// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "sql.h"

#include "actor-state.h"

#include <workerd/io/io-context.h>
#include <workerd/jsg/function.h>
#include <workerd/jsg/util.h>
#include <workerd/util/sqlite.h>

namespace workerd::api {

// Thread-local storage for JavaScript error message and stack
thread_local kj::String SqlStorage::lastErrorMessage;
thread_local kj::String SqlStorage::lastErrorStack;
thread_local bool SqlStorage::hasJsException = false;

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

  // Get the function as a V8 function object and verify it's not async
  v8::Local<v8::Value> funcValue = functionValue;
  v8::Local<v8::Function> funcObj = v8::Local<v8::Function>::Cast(funcValue);
  JSG_REQUIRE(!funcObj->IsAsyncFunction(), TypeError,
      "Async functions are not supported in SQL UDFs. Please use a regular synchronous function.");

  // Also check if the function returns a Promise by calling it with no arguments
  // and checking the result. This catches regular functions that return Promises.
  {
    v8::TryCatch tryCatch(js.v8Isolate);
    v8::Local<v8::Context> context = js.v8Isolate->GetCurrentContext();
    v8::Local<v8::Value> thisArg = v8::Undefined(js.v8Isolate);
    v8::MaybeLocal<v8::Value> result = funcObj->Call(context, thisArg, 0, nullptr);

    // Only check for Promise return if the call didn't throw
    if (!tryCatch.HasCaught() && !result.IsEmpty()) {
      v8::Local<v8::Value> returnValue = result.ToLocalChecked();
      if (returnValue->IsPromise()) {
        JSG_FAIL_REQUIRE(TypeError,
            "Functions that return Promises are not supported in SQL UDFs. "
            "SQL UDFs must return values synchronously.");
      }
    }
  }

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

  auto callback = [funcRef = jsg::JsRef<jsg::JsValue>(js, functionValue), isolate = js.v8Isolate,
                      funcName = kj::str(nameStr)](
                      kj::ArrayPtr<const SqliteDatabase::ValuePtr> udfArgs)
      -> kj::OneOf<kj::Array<byte>, kj::String, int64_t, double, decltype(nullptr)> {
    // Get the current JavaScript context
    jsg::Lock& js = jsg::Lock::from(isolate);
    v8::HandleScope handleScope(isolate);

    // Get the function from the reference
    v8::Local<v8::Value> funcValue = funcRef.getHandle(js);

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
      // First convert the ValuePtr to SqlValue (similar to what happens in iteratorImpl)
      SqlValue value;

      KJ_SWITCH_ONEOF(udfArgs[i]) {
        KJ_CASE_ONEOF(blobPtr, kj::ArrayPtr<const byte>) {
          // Clone the blob data into a new array - this may be necessary for longevity
          value.emplace(kj::heapArray(blobPtr));
        }
        KJ_CASE_ONEOF(text, kj::StringPtr) {
          // StringPtr is valid during the callback and immediately converted to JS
          value.emplace(text);
        }
        KJ_CASE_ONEOF(intValue, int64_t) {
          // Convert to double like we do in iteratorImpl for consistency
          value.emplace(static_cast<double>(intValue));
        }
        KJ_CASE_ONEOF(doubleValue, double) {
          value.emplace(doubleValue);
        }
        KJ_CASE_ONEOF(nullValue, decltype(nullptr)) {
          // Leave value as null
        }
      }

      // Then use the existing wrapSqlValue function to convert to JS
      jsArgs[i] = wrapSqlValue(js, kj::mv(value));
    }

    // Call the function
    v8::Local<v8::Function> func = v8::Local<v8::Function>::Cast(funcValue);
    v8::Local<v8::Value> thisObj = v8::Undefined(isolate);
    v8::Local<v8::Context> v8Context = isolate->GetCurrentContext();

    v8::MaybeLocal<v8::Value> maybeResult = func->Call(
        v8Context, thisObj, udfArgs.size(), udfArgs.size() > 0 ? jsArgs.begin() : nullptr);

    // Handle any Javascript exceptions that were thrown and uncaught
    if (maybeResult.IsEmpty()) {
      v8::Local<v8::Value> jsException = tryCatch.Exception();

      // Extract message and stack from the exception
      v8::Local<v8::Context> context = isolate->GetCurrentContext();
      v8::Local<v8::String> messageKey =
          v8::String::NewFromUtf8(isolate, "message").ToLocalChecked();
      v8::Local<v8::String> stackKey = v8::String::NewFromUtf8(isolate, "stack").ToLocalChecked();
      v8::Local<v8::String> nameKey = v8::String::NewFromUtf8(isolate, "name").ToLocalChecked();

      // Store UDF name for better error messages
      kj::String functionNameCopy = kj::heapString(funcName);

      // Store error message and additional context
      if (jsException->IsObject()) {
        v8::Local<v8::Object> jsExceptionObj = jsException->ToObject(context).ToLocalChecked();

        // Get error type/name
        kj::String errorType = kj::heapString("Error");
        if (jsExceptionObj->Has(context, nameKey).FromMaybe(false)) {
          v8::Local<v8::Value> nameValue = jsExceptionObj->Get(context, nameKey).ToLocalChecked();
          if (nameValue->IsString()) {
            v8::Local<v8::String> nameStr = nameValue.As<v8::String>();
            v8::String::Utf8Value name(isolate, nameStr);
            errorType = kj::heapString(*name, name.length());
          }
        }

        // Get message
        kj::String errorMessage = kj::heapString("Unknown error");
        if (jsExceptionObj->Has(context, messageKey).FromMaybe(false)) {
          v8::Local<v8::Value> messageValue =
              jsExceptionObj->Get(context, messageKey).ToLocalChecked();
          if (messageValue->IsString()) {
            v8::Local<v8::String> messageStr = messageValue.As<v8::String>();
            v8::String::Utf8Value message(isolate, messageStr);
            errorMessage = kj::heapString(*message, message.length());
          }
        }

        // Create a more detailed error message that includes the UDF name and error details
        lastErrorMessage =
            kj::str(errorType, " in SQL UDF '", functionNameCopy, "': ", errorMessage);

        // Make sure to output the message to the console for debugging
        fprintf(stderr, "SQL UDF Error: %s\n", lastErrorMessage.cStr());

        // Get stack trace
        if (jsExceptionObj->Has(context, stackKey).FromMaybe(false)) {
          v8::Local<v8::Value> stackValue = jsExceptionObj->Get(context, stackKey).ToLocalChecked();
          if (stackValue->IsString()) {
            v8::Local<v8::String> stackStr = stackValue.As<v8::String>();
            v8::String::Utf8Value stack(isolate, stackStr);
            lastErrorStack = kj::heapString(*stack, stack.length());
          }
        }
      } else {
        // Fallback if it's not an object
        v8::String::Utf8Value exceptionStr(isolate, jsException);
        lastErrorMessage = kj::str("Error in SQL UDF '", functionNameCopy,
            "': ", kj::heapString(*exceptionStr, exceptionStr.length()));
        lastErrorStack = kj::heapString("");
      }

      hasJsException = true;

      // Throw a simple KJ exception to signal that an error occurred
      // We'll create a proper JS error with the saved details in onError
      kj::throwFatalException(kj::Exception(kj::Exception::Type::FAILED, "SQL UDF", __LINE__,
          kj::str("JavaScript error in UDF '", functionNameCopy, "'")));
    }

    // Get the JavaScript result
    v8::Local<v8::Value> jsResult = maybeResult.ToLocalChecked();

    // Convert based on the JS type
    if (jsResult->IsNull() || jsResult->IsUndefined()) {
      return nullptr;
    } else if (jsResult->IsInt32() || jsResult->IsUint32()) {
      return static_cast<int64_t>(jsResult->IntegerValue(v8Context).ToChecked());
    } else if (jsResult->IsNumber()) {
      double num = jsResult->NumberValue(v8Context).ToChecked();
      if (num == static_cast<int64_t>(num)) {
        return static_cast<int64_t>(num);
      } else {
        return num;
      }
    } else if (jsResult->IsString()) {
      // Convert JS string to owned kj::String for safe lifetime management
      return js.toString(jsResult);
    } else if (jsResult->IsBoolean()) {
      return static_cast<int64_t>(jsResult->BooleanValue(isolate) ? 1 : 0);
    } else if (jsResult->IsArrayBuffer()) {
      v8::Local<v8::ArrayBuffer> arrayBuffer = v8::Local<v8::ArrayBuffer>::Cast(jsResult);
      auto backingStore = arrayBuffer->GetBackingStore();

      // Create an owned copy of the ArrayBuffer data
      auto size = backingStore->ByteLength();
      auto copy = kj::heapArray<byte>(size);

      if (size > 0) {
        memcpy(copy.begin(), backingStore->Data(), size);
      }

      return kj::mv(copy);
    } else if (jsResult->IsUint8Array() || jsResult->IsInt8Array() ||
        jsResult->IsUint8ClampedArray()) {
      v8::Local<v8::TypedArray> typedArray = v8::Local<v8::TypedArray>::Cast(jsResult);
      auto buffer = typedArray->Buffer();
      auto backingStore = buffer->GetBackingStore();
      auto byteOffset = typedArray->ByteOffset();
      auto byteLength = typedArray->ByteLength();

      // Create an owned copy of the TypedArray data
      auto copy = kj::heapArray<byte>(byteLength);

      if (byteLength > 0) {
        memcpy(
            copy.begin(), static_cast<const byte*>(backingStore->Data()) + byteOffset, byteLength);
      }

      return kj::mv(copy);
    } else {
      // TODO: Throw a more specific error
      return nullptr;
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

// Called by SQLite when an error occurs during SQL execution. This method directly throws
// a JavaScript exception with the SQLite error message.
//
// For errors from UDFs, we check if we have a stored JavaScript exception in thread-local storage
// and throw that instead of a generic error message. This preserves the full stack trace.
void SqlStorage::onError(kj::Maybe<int> sqliteErrorCode,
    kj::StringPtr message,
    kj::Maybe<const kj::Exception&> exception) const {
  auto& ioContext = IoContext::current();
  jsg::Lock& js = ioContext.getCurrentLock();

  // Check if we have a stored JavaScript exception from a UDF
  if (hasJsException) {
    // Clear the flag to prevent leaking to next operation
    hasJsException = false;

    // Create a new error object with the stored message and stack
    v8::Isolate* isolate = js.v8Isolate;
    v8::HandleScope handleScope(isolate);
    v8::Local<v8::Context> context = isolate->GetCurrentContext();

    // Create an Error object with our stored message
    v8::Local<v8::String> errorMsg = v8::String::NewFromUtf8(
        isolate, lastErrorMessage.begin(), v8::NewStringType::kNormal, lastErrorMessage.size())
                                         .ToLocalChecked();

    v8::Local<v8::Value> error = v8::Exception::Error(errorMsg);

    // Add the stack property if we have it
    if (lastErrorStack.size() > 0) {
      v8::Local<v8::Object> errorObj = error->ToObject(context).ToLocalChecked();
      v8::Local<v8::String> stackKey = v8::String::NewFromUtf8(isolate, "stack").ToLocalChecked();
      v8::Local<v8::String> stackValue = v8::String::NewFromUtf8(
          isolate, lastErrorStack.begin(), v8::NewStringType::kNormal, lastErrorStack.size())
                                             .ToLocalChecked();

      // Set the stack property
      errorObj->Set(context, stackKey, stackValue).Check();
    }

    // Clear the stored data
    lastErrorMessage = kj::heapString("");
    lastErrorStack = kj::heapString("");

    // Throw the reconstructed error
    isolate->ThrowException(error);

    // Create and throw a JSG error with a more specific approach that preserves our message
    // Instead of using JSG_FAIL_REQUIRE which might not correctly use our message, we'll
    // throw directly from the V8 exception we already created
    throw jsg::JsExceptionThrown();

    // Don't need to explicitly return since JSG_FAIL_REQUIRE doesn't return
  } else if (exception != kj::none) {
    // If we have a regular KJ exception, convert it to a JS exception
    auto jsException = js.exceptionToJs(kj::Exception(KJ_REQUIRE_NONNULL(exception)));
    js.throwException(kj::mv(jsException));
  } else {
    // Just throw a basic error with the SQLite error message
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

kj::Array<const SqliteDatabase::ValuePtr> SqlStorage::Cursor::mapBindings(
    kj::ArrayPtr<BindingValue> values) {
  return KJ_MAP(value, values) -> SqliteDatabase::ValuePtr {
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
