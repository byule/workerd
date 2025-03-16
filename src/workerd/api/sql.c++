// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "sql.h"

#include "actor-state.h"

#include <workerd/io/io-context.h>

namespace workerd::api {

// Maximum total size of all cached statements (measured in size of the SQL code). If cached
// statements exceed this, we remove the LRU statement(s).
//
// Hopefully most apps don't ever hit this, but it's important to have a limit in case of
// queries containing dynamic content or excessively large one-off queries.
static constexpr uint SQL_STATEMENT_CACHE_MAX_SIZE = 1024 * 1024;

SqlStorage::SqlStorage(jsg::Ref<DurableObjectStorage> storage)
    : storage(kj::mv(storage)),
      statementCache(IoContext::current().addObject(kj::heap<StatementCache>())) {}

SqlStorage::~SqlStorage() {}

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
    return jsg::alloc<Cursor>(js, db, regulator, js.toString(querySql), kj::mv(bindings));
  }

  auto result = jsg::alloc<Cursor>(js, slot.addRef(), kj::mv(bindings));

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

jsg::Ref<SqlStorage::Statement> SqlStorage::prepare(jsg::Lock& js, jsg::JsString query) {
  return jsg::alloc<Statement>(js, JSG_THIS, query);
}

double SqlStorage::getDatabaseSize(jsg::Lock& js) {
  auto& db = getDb(js);
  int64_t pages = execMemoized(db, pragmaPageCount,
      "select (select * from pragma_page_count) - (select * from pragma_freelist_count);")
                      .getInt64(0);
  return pages * getPageSize(db);
}

// The SqlStorage class implementation

// Create a single object with lazy 'old' and 'new' properties 
jsg::JsObject SqlStorage::createRowValuesObject(jsg::Lock& js, 
                                               SqliteDatabase::LazyRowValues& oldValues,
                                               SqliteDatabase::LazyRowValues& newValues) {
  // Create the main values object
  jsg::JsObject result = js.obj();
  
  // Set up V8 context and isolate
  auto context = js.v8Context();
  auto isolate = js.v8Isolate;
  
  // Create a struct to hold pointers to both value sets
  struct LazyRowData {
    SqliteDatabase::LazyRowValues* oldValues;
    SqliteDatabase::LazyRowValues* newValues;
    
    LazyRowData(SqliteDatabase::LazyRowValues* old, SqliteDatabase::LazyRowValues* nw)
      : oldValues(old), newValues(nw) {}
  };
  
  // Pass both values to JavaScript
  auto lazyRowData = new LazyRowData(&oldValues, &newValues);
  
  // Define the 'old' property getter
  v8::Local<v8::Name> oldKey = v8::String::NewFromUtf8(isolate, "old").ToLocalChecked();
  v8::Local<v8::Function> oldGetter = v8::Function::New(
    context,
    [](const v8::FunctionCallbackInfo<v8::Value>& info) {
      v8::Isolate* isolate = info.GetIsolate();
      v8::HandleScope scope(isolate);
      v8::Local<v8::Context> context = isolate->GetCurrentContext();
      
      // Get the values pointer from the external data
      v8::Local<v8::External> external = info.Data().As<v8::External>();
      auto data = static_cast<LazyRowData*>(external->Value());
      
      // Create an object with the values
      v8::Local<v8::Object> oldObj = v8::Object::New(isolate);
      
      // Only extract if there are values
      if (!data->oldValues->isEmpty()) {
        // Extract all columns lazily
        auto allColumns = data->oldValues->getAllColumns();
        
        // Add each column to the result object
        for (auto& column : allColumns) {
          // Get a v8 string for the column name
          v8::Local<v8::String> colName = v8::String::NewFromUtf8(
            isolate, column.name.cStr()).ToLocalChecked();
          
          // Convert the value to a v8 value
          v8::Local<v8::Value> jsValue;
          
          KJ_SWITCH_ONEOF(column.value) {
            KJ_CASE_ONEOF(str, kj::String) {
              jsValue = v8::String::NewFromUtf8(isolate, str.cStr()).ToLocalChecked();
            }
            KJ_CASE_ONEOF(num, double) {
              jsValue = v8::Number::New(isolate, num);
            }
            KJ_CASE_ONEOF(blob, kj::Array<const byte>) {
              auto buffer = v8::ArrayBuffer::New(isolate, blob.size());
              auto backingStore = buffer->GetBackingStore();
              memcpy(backingStore->Data(), blob.begin(), blob.size());
              jsValue = buffer;
            }
            KJ_CASE_ONEOF(null, decltype(nullptr)) {
              jsValue = v8::Null(isolate);
            }
          }
          
          // Set the property
          oldObj->Set(context, colName, jsValue).Check();
        }
      }
      
      // Return the old values object
      info.GetReturnValue().Set(oldObj);
    },
    v8::External::New(isolate, lazyRowData)
  ).ToLocalChecked();
  
  // Define the 'new' property getter
  v8::Local<v8::Name> newKey = v8::String::NewFromUtf8(isolate, "new").ToLocalChecked();
  v8::Local<v8::Function> newGetter = v8::Function::New(
    context,
    [](const v8::FunctionCallbackInfo<v8::Value>& info) {
      v8::Isolate* isolate = info.GetIsolate();
      v8::HandleScope scope(isolate);
      v8::Local<v8::Context> context = isolate->GetCurrentContext();
      
      // Get the values pointer from the external data
      v8::Local<v8::External> external = info.Data().As<v8::External>();
      auto data = static_cast<LazyRowData*>(external->Value());
      
      // Create an object with the values
      v8::Local<v8::Object> newObj = v8::Object::New(isolate);
      
      // Only extract if there are values
      if (!data->newValues->isEmpty()) {
        // Extract all columns lazily
        auto allColumns = data->newValues->getAllColumns();
        
        // Add each column to the result object
        for (auto& column : allColumns) {
          // Get a v8 string for the column name
          v8::Local<v8::String> colName = v8::String::NewFromUtf8(
            isolate, column.name.cStr()).ToLocalChecked();
          
          // Convert the value to a v8 value
          v8::Local<v8::Value> jsValue;
          
          KJ_SWITCH_ONEOF(column.value) {
            KJ_CASE_ONEOF(str, kj::String) {
              jsValue = v8::String::NewFromUtf8(isolate, str.cStr()).ToLocalChecked();
            }
            KJ_CASE_ONEOF(num, double) {
              jsValue = v8::Number::New(isolate, num);
            }
            KJ_CASE_ONEOF(blob, kj::Array<const byte>) {
              auto buffer = v8::ArrayBuffer::New(isolate, blob.size());
              auto backingStore = buffer->GetBackingStore();
              memcpy(backingStore->Data(), blob.begin(), blob.size());
              jsValue = buffer;
            }
            KJ_CASE_ONEOF(null, decltype(nullptr)) {
              jsValue = v8::Null(isolate);
            }
          }
          
          // Set the property
          newObj->Set(context, colName, jsValue).Check();
        }
      }
      
      // Return the new values object
      info.GetReturnValue().Set(newObj);
    },
    v8::External::New(isolate, lazyRowData)
  ).ToLocalChecked();
  
  // Set up the property descriptors on the values object
  v8::Local<v8::Object> valuesObj = result;
  
  // Define the 'old' and 'new' properties
  v8::PropertyDescriptor oldDesc(oldGetter, v8::Undefined(isolate));
  oldDesc.set_enumerable(true);
  oldDesc.set_configurable(true);
  valuesObj->DefineProperty(context, oldKey, oldDesc).Check();
  
  v8::PropertyDescriptor newDesc(newGetter, v8::Undefined(isolate));
  newDesc.set_enumerable(true);
  newDesc.set_configurable(true);
  valuesObj->DefineProperty(context, newKey, newDesc).Check();
  
  return result;
}

void SqlStorage::setUpdateHook(jsg::Lock& js, jsg::V8Ref<v8::Function> callback) {
  // Store the JavaScript callback function
  updateHookCallback = kj::mv(callback);

  // Get the SQLite database
  auto& db = getDb(js);

  // Set the update hook with a direct callback that executes immediately
  // Re-entrancy protection is now handled at the SQLite level
  db.setUpdateHook(SqliteDatabase::UpdateHookCallback(
      [this](SqliteDatabase::UpdateOperation op, kj::StringPtr table, int64_t rowid,
             SqliteDatabase::LazyRowValues& oldValues, SqliteDatabase::LazyRowValues& newValues) {
    // Create a copy of the table name string
    auto tableCopy = kj::str(table);

    // Get current IoContext to access the JS isolate
    auto& context = IoContext::current();
    jsg::Lock& lock = context.getCurrentLock();

    // Try to get the callback from this object
    KJ_IF_SOME(callback, updateHookCallback) {
      // Make a copy of the callback for use in this function scope
      auto callbackCopy = callback.addRef(lock);

      // Build the arguments for the JavaScript callback
      v8::Local<v8::Value> args[4];

      // Wrap everything in a try-catch to handle JavaScript exceptions
      try {
        // Convert enum to string for JS friendly API
        if (op == SqliteDatabase::UpdateOperation::INSERT) {
          args[0] = lock.str(kj::StringPtr("insert"));
        } else if (op == SqliteDatabase::UpdateOperation::UPDATE) {
          args[0] = lock.str(kj::StringPtr("update"));
        } else {
          args[0] = lock.str(kj::StringPtr("delete"));
        }

        args[1] = lock.str(tableCopy);                   // table name
        args[2] = lock.num(static_cast<double>(rowid));  // rowid as double
        
        // Create a single values object with lazy 'old' and 'new' properties
        args[3] = createRowValuesObject(lock, oldValues, newValues);
        
      } catch (jsg::JsExceptionThrown& e) {
        // Catch JavaScript exceptions but allow the transaction to continue
        // This prevents the worker from crashing when JS exceptions occur in update hooks
        KJ_LOG(WARNING, "JavaScript exception while preparing update hook callback arguments");
        return;  // Exit the callback without calling the JavaScript function
      }

      // Call the JavaScript callback with the arguments
      auto handle = callbackCopy.getHandle(lock);
      auto undefined = v8::Undefined(lock.v8Isolate);

      // Use v8::TryCatch to catch JavaScript exceptions - this is the V8-recommended way to handle JS exceptions
      v8::TryCatch tryCatch(lock.v8Isolate);
      
      // Call the callback and ignore the result
      handle->Call(lock.v8Context(), undefined, 4, args);
      
      // Check if an exception occurred
      if (tryCatch.HasCaught()) {
        // An exception was thrown in JavaScript - log it but continue with the transaction
        KJ_LOG(WARNING, "JavaScript exception in SQL update hook callback");
        
        // Get exception details for logging (if possible)
        if (tryCatch.CanContinue()) {
          auto exception = tryCatch.Exception();
          if (!exception.IsEmpty()) {
            // Try to get the exception message for logging
            v8::Local<v8::Message> message = tryCatch.Message();
            if (!message.IsEmpty()) {
              v8::String::Utf8Value messageStr(lock.v8Isolate, message->Get());
              if (*messageStr) {
                KJ_LOG(INFO, "Exception message:", *messageStr);
              }
            }
          }
        }
        
        // Clear the exception so it doesn't propagate and crash the worker
        tryCatch.Reset();
      }
    }
  }));
}

void SqlStorage::executePendingUpdateHooks() {
  // This is now a stub function as we're executing hooks directly
  // Kept for API compatibility
}

void SqlStorage::clearUpdateHook(jsg::Lock& js) {
  // Clear the JavaScript callback
  updateHookCallback = kj::none;

  // Clear the SQLite update hook
  auto& db = getDb(js);
  db.setUpdateHook(kj::none);
}


bool SqlStorage::isAllowedName(kj::StringPtr name) const {
  return !name.startsWith("_cf_");
}

bool SqlStorage::isAllowedTrigger(kj::StringPtr name) const {
  return true;
}

void SqlStorage::onError(kj::Maybe<int> sqliteErrorCode, kj::StringPtr message) const {
  JSG_ASSERT(false, Error, message);
}

bool SqlStorage::allowTransactions() const {
  JSG_FAIL_REQUIRE(Error,
      "To execute a transaction, please use the state.storage.transaction() or "
      "state.storage.transactionSync() APIs instead of the SQL BEGIN TRANSACTION or SAVEPOINT "
      "statements. The JavaScript API is safer because it will automatically roll back on "
      "exceptions, and because it interacts correctly with Durable Objects' automatic atomic "
      "write coalescing.");
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
  return jsg::alloc<RowIterator>(JSG_THIS);
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

jsg::Ref<SqlStorage::Cursor::RawIterator> SqlStorage::Cursor::raw(jsg::Lock&) {
  return jsg::alloc<RawIterator>(JSG_THIS);
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
  tracker.trackField("updateHookCallback", updateHookCallback);
}

}  // namespace workerd::api
