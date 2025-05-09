// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "sql.h"

#include "actor-state.h"

#include <workerd/io/io-context.h>
#include <workerd/jsg/function.h>
#include <workerd/util/sqlite.h>

namespace workerd::api {

// Initialize the thread-local error storage
thread_local kj::Maybe<SqlStorage::SerializedJsError> SqlStorage::currentJsError;

// Maximum total size of all cached statements (measured in size of the SQL code). If cached
// statements exceed this, we remove the LRU statement(s).
//
// Hopefully most apps don't ever hit this, but it's important to have a limit in case of
// queries containing dynamic content or excessively large one-off queries.
static constexpr uint SQL_STATEMENT_CACHE_MAX_SIZE = 1024 * 1024;

SqlStorage::SqlStorage(jsg::Ref<DurableObjectStorage> storage)
    : storage(kj::mv(storage)),
      statementCache(IoContext::current().addObject(kj::heap<StatementCache>())) {}

SqlStorage::~SqlStorage() {
  // Nothing to clean up - the jsFunctions HashMap is cleaned up automatically
}

jsg::Ref<SqlStorage::Cursor> SqlStorage::exec(
    jsg::Lock& js, jsg::JsString querySql, jsg::Arguments<BindingValue> bindings) {
  // Internalize the string, so that the cache can be keyed by string identity rather than content.
  // Any string we put into the cache is expected to live there for a while anyway, so even if it
  // is a one-off, internalizing it (which moves it to the old generation) shouldn't hurt.
  querySql = querySql.internalize(js);

  auto& db = getDb(js);
  auto& statementCache = *this->statementCache;

  kj::Rc<CachedStatement>& slot = statementCache.map.findOrCreate(querySql, [&]() {
    auto result = kj::rc<CachedStatement>(js, *this, db, querySql, js.toString(querySql));
    statementCache.totalSize += result->statementSize;
    return result;
  });

  // Move cached statement to end of LRU queue.
  if (slot->lruLink.isLinked()) {
    statementCache.lru.remove(*slot.get());
  }
  statementCache.lru.add(*slot.get());

  if (slot->isShared()) {
    // Oops, this CachedStatement is currently in-use (presumably by a Cursor).
    //
    // SQLite only allows one instance of a statement to run at a time, so we will have to compile
    // the statement again as a one-off.
    //
    // In theory we could try to cache multiple copies of the statement, but as this is probably
    // exceedingly rare, it is not worth the added code complexity.
    SqliteDatabase::Regulator& regulator = *this;
    return js.alloc<Cursor>(js, db, regulator, js.toString(querySql), kj::mv(bindings));
  }

  auto result = js.alloc<Cursor>(js, slot.addRef(), kj::mv(bindings));

  // If the statement cache grew too big, drop the least-recently-used entry.
  while (statementCache.totalSize > SQL_STATEMENT_CACHE_MAX_SIZE) {
    auto& toRemove = *statementCache.lru.begin();
    auto oldQuery = jsg::JsString(toRemove.query.getHandle(js));
    statementCache.totalSize -= toRemove.statementSize;
    statementCache.lru.remove(toRemove);
    KJ_ASSERT(statementCache.map.eraseMatch(oldQuery));
  }

  return result;
}

SqlStorage::IngestResult SqlStorage::ingest(jsg::Lock& js, kj::String querySql) {
  SqliteDatabase::Regulator& regulator = *this;
  auto result = getDb(js).ingestSql(regulator, querySql);
  return IngestResult(
      kj::str(result.remainder), result.rowsRead, result.rowsWritten, result.statementCount);
}

void SqlStorage::setMaxPageCountForTest(jsg::Lock& js, int count) {
  auto& db = getDb(js);
  db.run(SqliteDatabase::TRUSTED, kj::str("PRAGMA max_page_count = ", count));
}

jsg::Ref<SqlStorage::Statement> SqlStorage::prepare(jsg::Lock& js, jsg::JsString query) {
  return js.alloc<Statement>(js, JSG_THIS, query);
}

void SqlStorage::ensureBuiltInFunctionsInitialized(jsg::Lock& js) {
  // Only initialize if the set is empty
  if (builtInFunctions.size() == 0) {
    auto& db = getDb(js);

    // Query the database for all built-in functions
    auto query = db.run(SqliteDatabase::TRUSTED, "PRAGMA function_list;");

    // Process each row
    while (!query.isDone()) {
      auto name = query.getText(0);  // First column is the function name

      // Convert to lowercase for case-insensitive checks
      auto lowercase = kj::heapString(name.size());
      for (size_t i = 0; i < name.size(); i++) {
        lowercase[i] = tolower(name[i]);
      }

      // Only insert if not already present
      if (builtInFunctions.find(lowercase) == kj::none) {
        builtInFunctions.insert(kj::mv(lowercase));
      }

      query.nextRow();
    }
  }
}

void SqlStorage::createScalarFunction(
    jsg::Lock& js, jsg::JsString name, jsg::JsValue functionValue) {
  // Validate that the function parameter is actually a function
  JSG_REQUIRE(functionValue.isFunction(), TypeError, "Expected a function as the second argument");

  // Convert to a string for use in the registration
  auto nameStr = js.toString(name);

  // Check the function name length - SQLite has a limit of 255 bytes for function names
  JSG_REQUIRE(nameStr.size() <= MAX_UDF_NAME_LENGTH, Error,
      kj::str("Function name exceeds maximum length (", MAX_UDF_NAME_LENGTH, ")"));

  // First ensure we've initialized our built-in functions list
  ensureBuiltInFunctionsInitialized(js);

  // Convert name to lowercase for case-insensitive check
  auto lowercaseName = kj::heapString(nameStr.size());
  for (size_t i = 0; i < nameStr.size(); i++) {
    lowercaseName[i] = tolower(nameStr[i]);
  }

  // Check against built-in functions
  if (builtInFunctions.find(lowercaseName) != kj::none) {
    JSG_FAIL_REQUIRE(Error, kj::str("Cannot override built-in SQLite function '", nameStr, "'"));
  }

  // Check if the function is already registered
  if (jsFunctions.find(nameStr) != kj::none) {
    // Unregister existing function first
    unregisterFunction(js, name);
  } else {
    // If this is a new function, check UDF count limit
    JSG_REQUIRE(jsFunctions.size() < MAX_UDF_COUNT, Error,
        kj::str("Maximum number of user-defined functions (", MAX_UDF_COUNT, ") exceeded"));
  }

  // Get the database
  auto& db = getDb(js);

  // Create a callback function that will be called when the SQL function is executed
  // Helper function to capture JavaScript errors from UDFs with stack traces
  // This function extracts error information from a V8 TryCatch object, including the stack trace,
  // serializes the error, and prepares it for later use when propagating through the SQLite error
  // handling system. It also sets an appropriate error message in the SQLite context.
  //
  // Parameters:
  //   jsLock - The current JavaScript lock
  //   isolate - The V8 isolate
  //   tryCatch - The V8 TryCatch object containing the caught exception
  //   funcName - The name of the UDF function where the error occurred
  //   context - The SQLite context to report the error to
  //
  // Returns:
  //   SerializedJsError containing the serialized error and stack trace information
  auto captureJsError = [](jsg::Lock& jsLock, v8::Isolate* isolate, v8::TryCatch& tryCatch,
                            kj::StringPtr funcName,
                            SqliteDatabase::SqliteContext& context) -> SerializedJsError {
    // Get the exception from the tryCatch
    v8::Local<v8::Value> exception = tryCatch.Exception();

    // Create a serializer for the error
    jsg::Serializer serializer(jsLock);

    // If we have a valid exception, use it, otherwise create a generic one
    v8::Local<v8::Value> errorValue;
    kj::String stackTrace;

    if (!exception.IsEmpty() && exception->IsObject()) {
      errorValue = exception;

      // Extract stack trace if available
      v8::Local<v8::Context> v8Context = isolate->GetCurrentContext();
      v8::Local<v8::Object> exceptionObj = exception.As<v8::Object>();

      // Try to get the stack property
      v8::Local<v8::String> stackKey = v8::String::NewFromUtf8(isolate, "stack").ToLocalChecked();
      v8::MaybeLocal<v8::Value> maybeStack = exceptionObj->Get(v8Context, stackKey);

      if (!maybeStack.IsEmpty()) {
        v8::Local<v8::Value> stackValue = maybeStack.ToLocalChecked();
        if (stackValue->IsString()) {
          v8::String::Utf8Value stackStr(isolate, stackValue);
          if (*stackStr) {
            // Store the stack trace
            stackTrace = kj::heapString(*stackStr, stackStr.length());
          } else {
            stackTrace = kj::str("Error in UDF function: ", funcName, " (stack unavailable)");
          }
        } else {
          stackTrace = kj::str("Error in UDF function: ", funcName, " (stack not a string)");
        }
      } else {
        stackTrace = kj::str("Error in UDF function: ", funcName, " (no stack property)");
      }

      // Try to get a message for the SQLite error
      kj::String sqliteErrorMsg = kj::str("JavaScript error in UDF ", funcName);
      v8::Local<v8::String> msgKey = v8::String::NewFromUtf8(isolate, "message").ToLocalChecked();
      v8::MaybeLocal<v8::Value> maybeMsg = exceptionObj->Get(v8Context, msgKey);

      if (!maybeMsg.IsEmpty()) {
        v8::Local<v8::Value> msgValue = maybeMsg.ToLocalChecked();
        if (msgValue->IsString()) {
          v8::String::Utf8Value msgStr(isolate, msgValue);
          if (*msgStr) {
            sqliteErrorMsg = kj::str("JavaScript error in UDF ", funcName, ": ", *msgStr);
          }
        }
      }

      // Return the error message to SQLite
      context.resultError(sqliteErrorMsg);
    } else {
      // Create a generic error if we couldn't get the actual error
      errorValue = v8::Exception::Error(
          v8::String::NewFromUtf8(isolate, "JavaScript error in UDF function").ToLocalChecked());
      stackTrace = kj::str("Error in UDF function: ", funcName, " (exception details unavailable)");

      // Return a generic error to SQLite
      context.resultError(kj::str("JavaScript error in UDF ", funcName, ": <JavaScript error>"));
    }

    // Serialize the error
    serializer.write(jsLock, jsg::JsValue(errorValue));
    auto serializedData = serializer.release();

    // Return the serialized error with stack trace information
    return SerializedJsError(kj::mv(serializedData.data), kj::str(funcName), kj::mv(stackTrace));
  };

  auto callback =
      [funcRef = jsg::JsRef<jsg::JsValue>(js, functionValue), isolate = js.v8Isolate,
          funcName = kj::str(nameStr)](
          kj::ArrayPtr<const SqliteDatabase::ValuePtr> udfArgs) -> SqliteDatabase::ValuePtr {
    // Get the current JavaScript context
    jsg::Lock& jsLock = jsg::Lock::from(isolate);
    v8::HandleScope handleScope(isolate);

    // Get the function from the reference
    v8::Local<v8::Value> funcValue = funcRef.getHandle(jsLock);

    // Make sure it's a function
    if (!funcValue->IsFunction()) {
      // TODO: Handle error more elegantly
      KJ_FAIL_REQUIRE("UDF is not a function");
    }

    // Use a TryCatch to capture any exceptions thrown during function execution
    v8::TryCatch tryCatch(isolate);

    // Convert the KJ values to JavaScript values
    auto jsArgs = kj::heapArray<v8::Local<v8::Value>>(udfArgs.size());

    for (size_t i = 0; i < udfArgs.size(); i++) {
      // Convert each KJ value to a JS value
      KJ_SWITCH_ONEOF(udfArgs[i]) {
        KJ_CASE_ONEOF(blobPtr, kj::ArrayPtr<const byte>) {
          // Convert blob to ArrayBuffer
          auto arrayBuffer = v8::ArrayBuffer::New(isolate, blobPtr.size());
          if (blobPtr.size() > 0) {
            memcpy(arrayBuffer->GetBackingStore()->Data(), blobPtr.begin(), blobPtr.size());
          }
          jsArgs[i] = arrayBuffer;
        }
        KJ_CASE_ONEOF(text, kj::StringPtr) {
          // Convert string to JS string
          jsArgs[i] =
              v8::String::NewFromUtf8(isolate, text.cStr(), v8::NewStringType::kNormal, text.size())
                  .ToLocalChecked();
        }
        KJ_CASE_ONEOF(intValue, int64_t) {
          // Convert integer to JS number
          if (intValue >= std::numeric_limits<int>::min() &&
              intValue <= std::numeric_limits<int>::max()) {
            jsArgs[i] = v8::Integer::New(isolate, static_cast<int>(intValue));
          } else {
            jsArgs[i] = v8::Number::New(isolate, static_cast<double>(intValue));
          }
        }
        KJ_CASE_ONEOF(doubleValue, double) {
          // Convert double to JS number
          jsArgs[i] = v8::Number::New(isolate, doubleValue);
        }
        KJ_CASE_ONEOF(nullValue, decltype(nullptr)) {
          // Convert null to JS null
          jsArgs[i] = v8::Null(isolate);
        }
      }
    }

    // Call the function
    v8::Local<v8::Function> func = v8::Local<v8::Function>::Cast(funcValue);
    v8::Local<v8::Value> thisObj = v8::Undefined(isolate);
    v8::Local<v8::Context> v8Context = isolate->GetCurrentContext();

    v8::MaybeLocal<v8::Value> maybeResult = func->Call(
        v8Context, thisObj, udfArgs.size(), udfArgs.size() > 0 ? jsArgs.begin() : nullptr);

    // Handle errors from function call
    if (maybeResult.IsEmpty()) {
      // An exception was thrown - propagate it
      throw tryCatch.Exception();
    }

    // Convert the JavaScript result to a SqlValue
    v8::Local<v8::Value> jsResult = maybeResult.ToLocalChecked();
    v8::Local<v8::Context> v8Context2 = isolate->GetCurrentContext();

    // Convert based on the JS type
    if (jsResult->IsNull() || jsResult->IsUndefined()) {
      return nullptr;
    } else if (jsResult->IsInt32() || jsResult->IsUint32()) {
      return static_cast<int64_t>(jsResult->IntegerValue(v8Context2).ToChecked());
    } else if (jsResult->IsNumber()) {
      double num = jsResult->NumberValue(v8Context2).ToChecked();
      if (num == static_cast<int64_t>(num)) {
        return static_cast<int64_t>(num);
      } else {
        return num;
      }
    } else if (jsResult->IsString()) {
      v8::String::Utf8Value str(isolate, jsResult);
      if (*str) {
        // Return a string - note that we can't return a heap string here as
        // it won't live long enough, so we rely on SQLITE_TRANSIENT in the bridge
        return kj::StringPtr(*str, str.length());
      } else {
        return kj::StringPtr("<string conversion failed>");
      }
    } else if (jsResult->IsBoolean()) {
      return static_cast<int64_t>(jsResult->BooleanValue(isolate) ? 1 : 0);
    } else if (jsResult->IsArrayBuffer()) {
      v8::Local<v8::ArrayBuffer> arrayBuffer = v8::Local<v8::ArrayBuffer>::Cast(jsResult);
      auto backingStore = arrayBuffer->GetBackingStore();
      // Similar to strings, we can't return a heap array here
      return kj::ArrayPtr<const byte>(
          static_cast<const byte*>(backingStore->Data()), backingStore->ByteLength());
    } else {
      // For anything else, convert to string
      v8::Local<v8::String> strValue;
      if (jsResult->ToString(v8Context2).ToLocal(&strValue)) {
        v8::String::Utf8Value str(isolate, strValue);
        if (*str) {
          return kj::StringPtr(*str, str.length());
        }
      }
      return kj::StringPtr("<string conversion failed>");
    }
  };

  // Store in our function map for GC tracking
  JsFunction jsFunc = {.function = jsg::JsRef<jsg::JsValue>(js, functionValue)};
  jsFunctions.insert(kj::heapString(nameStr), kj::mv(jsFunc));

  // Register with the modern callback API
  bool success = db.registerFunctionCallback(*this, nameStr, kj::mv(callback));

  if (!success) {
    // Clean up if registration failed
    jsFunctions.erase(nameStr);
    JSG_REQUIRE(false, Error, kj::str("Failed to register SQL function: ", nameStr));
  }
}

void SqlStorage::unregisterFunction(jsg::Lock& js, jsg::JsString name) {
  auto nameStr = js.toString(name);

  // Get the database
  auto& db = getDb(js);

  // Remove function from SQLite
  db.unregisterFunction(*this, nameStr);

  // Remove function from our map
  jsFunctions.erase(nameStr);

  // Clear any stored serialized error
  SqlStorage::currentJsError = kj::none;
}

double SqlStorage::getDatabaseSize(jsg::Lock& js) {
  auto& db = getDb(js);
  int64_t pages = execMemoized(db, pragmaPageCount,
      "select (select * from pragma_page_count) - (select * from pragma_freelist_count);")
                      .getInt64(0);
  return pages * getPageSize(db);
}

bool SqlStorage::isAllowedName(kj::StringPtr name) const {
  return !name.startsWith("_cf_");
}

bool SqlStorage::isAllowedTrigger(kj::StringPtr name) const {
  return true;
}

bool SqlStorage::isUserDefinedFunction(kj::StringPtr name) const {
  // Case-insensitive function lookup - check if we find a case-insensitive match
  for (const auto& entry: jsFunctions) {
    // Compare case-insensitively
    if (strcasecmp(entry.key.cStr(), name.cStr()) == 0) {
      return true;
    }
  }

  // No match found
  return false;
}

// Called by SQLite when an error occurs during SQL execution. This method checks if the error
// originated from a JavaScript UDF and, if so, reconstructs the original JavaScript error
// with its stack trace and throws it back to JavaScript code. This preserves the full context
// of where the error occurred in the user's JavaScript code.
//
// Thread safety: This method uses a thread-local variable (SqlStorage::currentJsError) to pass
// error information from the UDF execution context to this error handler. This approach is
// safe as long as UDF execution and error handling occur on the same thread, which is
// guaranteed by SQLite's execution model for the current implementation.
void SqlStorage::onError(kj::Maybe<int> sqliteErrorCode, kj::StringPtr message) const {
  // Error handler called

  // Check if we have a stored serialized JavaScript error from a UDF
  KJ_IF_SOME(serializedError, SqlStorage::currentJsError) {
    // Found serialized JS error

    // Get the current JavaScript context
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    jsg::Lock& js = jsg::Lock::from(isolate);
    v8::HandleScope handleScope(isolate);

    try {
      // Get the function name
      kj::StringPtr udfName = serializedError.functionName;

      // Create a descriptive error message that includes both the UDF name
      // and the SQLite error message for better context
      kj::String errorMsg = kj::str("JavaScript error in UDF ", udfName, ": ", message);

      // Create a new Error object with our message
      v8::Local<v8::String> errorMsgV8 = v8::String::NewFromUtf8(
          isolate, errorMsg.cStr(), v8::NewStringType::kNormal, errorMsg.size())
                                             .ToLocalChecked();

      v8::Local<v8::Value> newError = v8::Exception::Error(errorMsgV8);

      // Make sure it's an object
      if (newError->IsObject()) {
        v8::Local<v8::Object> errorObj = newError.As<v8::Object>();
        v8::Local<v8::Context> v8Context = isolate->GetCurrentContext();

        // Set the stack trace from the original error to preserve the call location
        // But add a synthetic frame for the SQL execution context

        // Create an enhanced stack trace that includes the SQL context
        kj::StringPtr origStack = serializedError.stackTrace;

        // We need to insert our synthetic frame at the point where the UDF execution
        // transitions to the SQL execution context.
        //
        // Use a simpler approach that doesn't require complex string manipulation.
        // We'll consider a few heuristics to find a good place to insert our frame.

        kj::String enhancedStack;

        // Get the function name that was called
        kj::StringPtr funcName = serializedError.functionName;

        // We'll insert our frame at a position based on the UDF name
        const char* stackStr = origStack.cStr();

        // Find an occurrence of the UDF name in the stack trace
        const char* funcNamePos = strstr(stackStr, funcName.cStr());

        if (funcNamePos == nullptr) {
          // UDF name not found in stack - look for common execution patterns
          const char* executePos = strstr(stackStr, "exec");
          if (executePos != nullptr) {
            funcNamePos = executePos;
          } else {
            // Look for "at" which prefixes stack frames
            const char* atPos = strstr(stackStr, "at ");
            if (atPos != nullptr) {
              funcNamePos = atPos;
            }
          }
        }

        if (funcNamePos == nullptr) {
          // Still not found - just append our frame at the end
          enhancedStack =
              kj::str(origStack, "\n    at SQLite.exec [SQL UDF invocation] (sql.c++:internal)");
        } else {
          // Let's try to find an appropriate stack frame boundary near our UDF name

          // First, find the line where the UDF name appears
          // Look for a newline before this position
          const char* lineStart = funcNamePos;
          while (lineStart > stackStr && *(lineStart - 1) != '\n') {
            lineStart--;
          }

          // Now find the end of this line
          const char* lineEnd = strchr(funcNamePos, '\n');
          if (lineEnd == nullptr) {
            lineEnd = stackStr + origStack.size();
          }

          // Find the next line after this one
          const char* nextLine = lineEnd;
          if (*nextLine == '\n') {
            nextLine++;
          }

          // Calculate our insertion point - put our frame after the function containing the UDF name
          size_t insertPos = nextLine - stackStr;

          // Construct the enhanced stack trace
          auto prefix = origStack.slice(0, insertPos);
          auto suffix = origStack.slice(insertPos);

          enhancedStack = kj::str(
              prefix, "    at SQLite.exec [SQL UDF invocation] (sql.c++:internal)\n", suffix);
        }

        v8::Local<v8::String> stackKey = v8::String::NewFromUtf8(isolate, "stack").ToLocalChecked();
        v8::Local<v8::String> stackValue = v8::String::NewFromUtf8(
            isolate, enhancedStack.cStr(), v8::NewStringType::kNormal, enhancedStack.size())
                                               .ToLocalChecked();

        // Set the stack property on the new error
        errorObj->Set(v8Context, stackKey, stackValue).Check();
      }

      // Clear the stored error to prevent memory leaks and ensure
      // the error doesn't accidentally get used for a future unrelated error
      SqlStorage::currentJsError = kj::none;

      // Throw the new error with the original stack trace
      js.v8Isolate->ThrowException(newError);
      throw jsg::JsExceptionThrown();
    } catch (jsg::JsExceptionThrown&) {
      // If the exception was already thrown, just let it propagate
      throw;
    } catch (...) {
      // If error creation fails, fall back to a basic error
      JSG_ASSERT(false, Error, message);
    }
  } else {
    // Regular SQLite error with no JavaScript context
    JSG_ASSERT(false, Error, message);
  }
}

bool SqlStorage::allowTransactions() const {
  JSG_FAIL_REQUIRE(Error,
      "To execute a transaction, please use the state.storage.transaction() or "
      "state.storage.transactionSync() APIs instead of the SQL BEGIN TRANSACTION or SAVEPOINT "
      "statements. The JavaScript API is safer because it will automatically roll back on "
      "exceptions, and because it interacts correctly with Durable Objects' automatic atomic "
      "write coalescing.");
}

bool SqlStorage::shouldAddQueryStats() const {
  // Bill for queries executed from JavaScript.
  return true;
}

SqlStorage::StatementCache::~StatementCache() noexcept(false) {
  for (auto& entry: lru) {
    lru.remove(entry);
  }
}

jsg::JsValue SqlStorage::wrapSqlValue(jsg::Lock& js, SqlValue value) {
  KJ_IF_SOME(v, value) {
    KJ_SWITCH_ONEOF(v) {
      KJ_CASE_ONEOF(bytes, kj::Array<byte>) {
        return jsg::JsValue(js.wrapBytes(kj::mv(bytes)));
      }
      KJ_CASE_ONEOF(text, kj::StringPtr) {
        return js.str(text);
      }
      KJ_CASE_ONEOF(number, double) {
        return js.num(number);
      }
    }
    KJ_UNREACHABLE;
  } else {
    return js.null();
  }
}

SqlStorage::Cursor::State::State(SqliteDatabase& db,
    SqliteDatabase::Regulator& regulator,
    kj::StringPtr sqlCode,
    kj::Array<BindingValue> bindingsParam)
    : bindings(kj::mv(bindingsParam)),
      query(db.run(regulator, sqlCode, mapBindings(bindings).asPtr())) {}

SqlStorage::Cursor::State::State(
    kj::Rc<CachedStatement> cachedStatementParam, kj::Array<BindingValue> bindingsParam)
    : bindings(kj::mv(bindingsParam)),
      query(cachedStatement.emplace(kj::mv(cachedStatementParam))
                ->statement.run(mapBindings(bindings).asPtr())) {}

SqlStorage::Cursor::~Cursor() noexcept(false) {
  // If this Cursor was created from a Statement, clear the Statement's currentCursor weak ref.
  KJ_IF_SOME(s, selfRef) {
    KJ_IF_SOME(p, s) {
      if (&p == this) {
        s = kj::none;
      }
    }
  }
}

void SqlStorage::Cursor::initColumnNames(jsg::Lock& js, State& stateRef) {
  // TODO(cleanup): Make `js.withinHandleScope` understand `jsg::JsValue` types in addition to
  //   `v8::Local`.
  KJ_IF_SOME(cached, stateRef.cachedStatement) {
    reusedCachedQuery = cached->useCount++ > 0;
  }

  js.withinHandleScope([&]() {
    v8::LocalVector<v8::Value> vec(js.v8Isolate);
    for (auto i: kj::zeroTo(stateRef.query.columnCount())) {
      vec.push_back(js.str(stateRef.query.getColumnName(i)));
    }
    auto array = jsg::JsArray(v8::Array::New(js.v8Isolate, vec.data(), vec.size()));
    columnNames = jsg::JsRef<jsg::JsArray>(js, array);
  });
}

double SqlStorage::Cursor::getRowsRead() {
  KJ_IF_SOME(st, state) {
    return static_cast<double>(st->query.getRowsRead());
  } else {
    return static_cast<double>(rowsRead);
  }
}

double SqlStorage::Cursor::getRowsWritten() {
  KJ_IF_SOME(st, state) {
    return static_cast<double>(st->query.getRowsWritten());
  } else {
    return static_cast<double>(rowsWritten);
  }
}

SqlStorage::Cursor::RowIterator::Next SqlStorage::Cursor::next(jsg::Lock& js) {
  auto self = JSG_THIS;
  auto maybeRow = rowIteratorNext(js, self);
  bool done = maybeRow == kj::none;
  return {
    .done = done,
    .value = kj::mv(maybeRow),
  };
}

jsg::JsArray SqlStorage::Cursor::toArray(jsg::Lock& js) {
  auto self = JSG_THIS;
  v8::LocalVector<v8::Value> results(js.v8Isolate);
  for (;;) {
    auto maybeRow = rowIteratorNext(js, self);
    KJ_IF_SOME(row, maybeRow) {
      results.push_back(row);
    } else {
      break;
    }
  }

  return jsg::JsArray(v8::Array::New(js.v8Isolate, results.data(), results.size()));
}

jsg::JsValue SqlStorage::Cursor::one(jsg::Lock& js) {
  auto self = JSG_THIS;
  auto result = JSG_REQUIRE_NONNULL(rowIteratorNext(js, self), Error,
      "Expected exactly one result from SQL query, but got no results.");

  KJ_IF_SOME(s, state) {
    // It appears that the query had more results, otherwise we would have set `state` to `none`
    // inside `iteratorImpl()`.
    endQuery(*s);
    JSG_FAIL_REQUIRE(
        Error, "Expected exactly one result from SQL query, but got multiple results.");
  }

  return result;
}

jsg::Ref<SqlStorage::Cursor::RowIterator> SqlStorage::Cursor::rows(jsg::Lock& js) {
  return js.alloc<RowIterator>(JSG_THIS);
}

kj::Maybe<jsg::JsObject> SqlStorage::Cursor::rowIteratorNext(jsg::Lock& js, jsg::Ref<Cursor>& obj) {
  KJ_IF_SOME(values, iteratorImpl(js, obj)) {
    auto names = obj->columnNames.getHandle(js);
    jsg::JsObject result = js.obj();
    KJ_ASSERT(names.size() == values.size());
    for (auto i: kj::zeroTo(names.size())) {
      result.set(js, names.get(js, i), jsg::JsValue(values[i]));
    }
    return result;
  } else {
    return kj::none;
  }
}

jsg::Ref<SqlStorage::Cursor::RawIterator> SqlStorage::Cursor::raw(jsg::Lock& js) {
  return js.alloc<RawIterator>(JSG_THIS);
}

// Returns the set of column names for the current Cursor. An exception will be thrown if the
// iterator has already been fully consumed. The resulting columns may contain duplicate entries,
// for instance a `SELECT *` across a join of two tables that share a column name.
jsg::JsArray SqlStorage::Cursor::getColumnNames(jsg::Lock& js) {
  return columnNames.getHandle(js);
}

kj::Maybe<jsg::JsArray> SqlStorage::Cursor::rawIteratorNext(jsg::Lock& js, jsg::Ref<Cursor>& obj) {
  KJ_IF_SOME(values, iteratorImpl(js, obj)) {
    return jsg::JsArray(v8::Array::New(js.v8Isolate, values.data(), values.size()));
  } else {
    return kj::none;
  }
}

kj::Maybe<v8::LocalVector<v8::Value>> SqlStorage::Cursor::iteratorImpl(
    jsg::Lock& js, jsg::Ref<Cursor>& obj) {
  auto& state = *KJ_UNWRAP_OR(obj->state, {
    if (obj->canceled) {
      JSG_FAIL_REQUIRE(Error,
          "SQL cursor was closed because the same statement was executed again. If you need to "
          "run multiple copies of the same statement concurrently, you must create multiple "
          "prepared statement objects.");
    } else {
      // Query already done.
      return kj::none;
    }
  });

  auto& query = state.query;

  if (query.isDone()) {
    obj->endQuery(state);
    return kj::none;
  }

  auto n = query.columnCount();
  v8::LocalVector<v8::Value> results(js.v8Isolate);
  results.reserve(n);
  for (auto i: kj::zeroTo(n)) {
    SqlValue value;
    KJ_SWITCH_ONEOF(query.getValue(i)) {
      KJ_CASE_ONEOF(data, kj::ArrayPtr<const byte>) {
        value.emplace(kj::heapArray(data));
      }
      KJ_CASE_ONEOF(text, kj::StringPtr) {
        value.emplace(text);
      }
      KJ_CASE_ONEOF(i, int64_t) {
        // int64 will become BigInt, but most applications won't want all their integers to be
        // BigInt. We will coerce to a double here.
        // TODO(someday): Allow applications to request that certain columns use BigInt.
        value.emplace(static_cast<double>(i));
      }
      KJ_CASE_ONEOF(d, double) {
        value.emplace(d);
      }
      KJ_CASE_ONEOF(_, decltype(nullptr)) {
        // leave value null
      }
    }
    results.push_back(wrapSqlValue(js, kj::mv(value)));
  }

  // Proactively iterate to the next row and, if it turns out the query is done, discard it. This
  // is an optimization to make sure that the statement can be returned to the statement cache once
  // the application has iterated over all results, even if the application fails to call next()
  // one last time to get `{done: true}`. A common case where this could happen is if the app is
  // expecting zero or one results, so it calls `exec(...).next()`. In the case that one result
  // was returned, the application may not bother calling `next()` again. If we hadn't proactively
  // iterated ahead by one, then the statement would not be returned to the cache until it was
  // GC'ed, which might prevent the cache from being effective in the meantime.
  //
  // Unfortunately, this does not help with the case where the application stops iterating with
  // results still available from the cursor. There's not much we can do about that case since
  // there's no way to know if the app might come back and try to use the cursor again later.
  query.nextRow();
  if (query.isDone()) {
    obj->endQuery(state);
  }

  return kj::mv(results);
}

void SqlStorage::Cursor::endQuery(State& stateRef) {
  // Save off row counts before the query goes away.
  rowsRead = stateRef.query.getRowsRead();
  rowsWritten = stateRef.query.getRowsWritten();

  // Clean up the query proactively.
  state = kj::none;
}

kj::Array<const SqliteDatabase::Query::ValuePtr> SqlStorage::Cursor::mapBindings(
    kj::ArrayPtr<BindingValue> values) {
  return KJ_MAP(value, values) -> SqliteDatabase::Query::ValuePtr {
    KJ_IF_SOME(v, value) {
      KJ_SWITCH_ONEOF(v) {
        KJ_CASE_ONEOF(data, kj::Array<const byte>) {
          return data.asPtr();
        }
        KJ_CASE_ONEOF(text, kj::String) {
          return text.asPtr();
        }
        KJ_CASE_ONEOF(d, double) {
          return d;
        }
      }
    } else {
      return nullptr;
    }
    KJ_UNREACHABLE;
  };
}

jsg::Ref<SqlStorage::Cursor> SqlStorage::Statement::run(
    jsg::Lock& js, jsg::Arguments<BindingValue> bindings) {
  return sqlStorage->exec(js, jsg::JsString(query.getHandle(js)), kj::mv(bindings));
}

void SqlStorage::visitForMemoryInfo(jsg::MemoryTracker& tracker) const {
  tracker.trackField("storage", storage);
  tracker.trackFieldWithSize("IoPtr<SqliteDatabase>", sizeof(IoPtr<SqliteDatabase>));
  if (pragmaPageCount != kj::none) {
    tracker.trackFieldWithSize(
        "IoPtr<SqllitDatabase::Statement>", sizeof(IoPtr<SqliteDatabase::Statement>));
  }
  if (pragmaGetMaxPageCount != kj::none) {
    tracker.trackFieldWithSize(
        "IoPtr<SqllitDatabase::Statement>", sizeof(IoPtr<SqliteDatabase::Statement>));
  }

  // Track JavaScript UDFs
  for (auto& entry: jsFunctions) {
    tracker.trackField("jsFunction", entry.value.function);
  }
}

}  // namespace workerd::api
