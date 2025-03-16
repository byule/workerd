// Copyright (c) 2024 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

import * as assert from 'node:assert';
import { DurableObject } from 'cloudflare:workers';

// Test basic update hook functionality (INSERT, UPDATE, DELETE operations)
async function testBasicHooks(sql) {
  console.log("Testing basic update hook operations");
  
  // Set up test table
  sql.exec(`CREATE TABLE basic_hook_test (id INTEGER PRIMARY KEY, value TEXT);`);
  
  // Array to capture update hook callbacks
  const updates = [];
  
  // Register a hook callback
  sql.setUpdateHook((rowid, tableName, operation) => {
    updates.push({rowid, tableName, operation});
  });
  
  // Insert a row - should trigger the hook
  sql.exec(`INSERT INTO basic_hook_test (id, value) VALUES (1, 'test');`);
  
  // Verify hook was called for INSERT
  assert.equal(updates.length, 1);
  assert.equal(Number(updates[0].rowid), 1); // Convert BigInt to Number for comparison
  assert.equal(updates[0].tableName, 'basic_hook_test');
  assert.equal(updates[0].operation, 'INSERT');
  
  // Update the row - should trigger hook again
  sql.exec(`UPDATE basic_hook_test SET value = 'updated' WHERE id = 1;`);
  
  // Verify hook was called for UPDATE
  assert.equal(updates.length, 2);
  assert.equal(Number(updates[1].rowid), 1);
  assert.equal(updates[1].tableName, 'basic_hook_test');
  assert.equal(updates[1].operation, 'UPDATE');
  
  // Delete the row - should trigger hook again
  sql.exec(`DELETE FROM basic_hook_test WHERE id = 1;`);
  
  // Verify hook was called for DELETE
  assert.equal(updates.length, 3);
  assert.equal(Number(updates[2].rowid), 1);
  assert.equal(updates[2].tableName, 'basic_hook_test');
  assert.equal(updates[2].operation, 'DELETE');
  
  // Clear the hook
  sql.clearUpdateHook();
  
  // Reset updates array
  updates.length = 0;
  
  // Insert another row - should NOT trigger the hook
  sql.exec(`INSERT INTO basic_hook_test (id, value) VALUES (2, 'test2');`);
  
  // Verify hook was not called
  assert.equal(updates.length, 0);
  
  // Cleanup
  sql.exec(`DROP TABLE basic_hook_test;`);
}

// Test exception handling in update hooks
async function testHookExceptions(sql) {
  console.log("Testing update hook exception handling");
  
  // Set up test table
  sql.exec(`CREATE TABLE exception_hook_test (id INTEGER PRIMARY KEY, value TEXT);`);
  
  // Register a hook that throws an exception
  let exceptionHookCalled = false;
  let sqlOperationSucceeded = false;
  
  sql.setUpdateHook((rowid, tableName, operation) => {
    exceptionHookCalled = true;
    // Throw an exception from the hook
    throw new Error('Test exception from update hook');
  });
  
  try {
    // This should succeed even though the hook throws an exception
    sql.exec(`INSERT INTO exception_hook_test (id, value) VALUES (1, 'test');`);
    sqlOperationSucceeded = true;
  } catch (e) {
    // If we catch an exception here, that means the update hook exception
    // propagated out of the SQL operation, which is what we're trying to prevent
    sqlOperationSucceeded = false;
    console.error('SQL operation failed due to update hook exception:', e);
  }
  
  // Verify that the hook was called (causing the exception)
  assert.equal(exceptionHookCalled, true, 'Exception-throwing hook should be called');
  
  // Verify that the SQL operation succeeded despite the hook exception
  assert.equal(sqlOperationSucceeded, true, 'SQL operation should succeed despite hook exception');
  
  // Verify that the database is still in a valid state by querying it
  const result = sql.exec(`SELECT id, value FROM exception_hook_test WHERE id = 1;`).toArray();
  assert.equal(result.length, 1, 'Should be able to query after hook exception');
  assert.equal(result[0].value, 'test', 'Data should be inserted despite hook exception');
  
  // Clean up
  sql.clearUpdateHook();
  sql.exec(`DROP TABLE exception_hook_test;`);
}

// Test re-entrancy prevention
async function testReentrancyPrevention(sql) {
  console.log("Testing re-entrancy prevention");
  
  // Set up test table
  sql.exec(`CREATE TABLE reentrancy_test (id INTEGER PRIMARY KEY, value TEXT);`);
  
  // Test re-entrancy prevention for exec()
  let reEntrancyHookCalled = false;
  let reEntrancyExceptionThrown = false;
  
  sql.setUpdateHook((rowid, tableName, operation) => {
    reEntrancyHookCalled = true;
    
    try {
      // This should throw an exception since we're inside a hook
      sql.exec(`INSERT INTO reentrancy_test (id, value) VALUES (2, 'should-fail');`);
    } catch (e) {
      // We should catch an exception about re-entrancy
      reEntrancyExceptionThrown = true;
      assert.ok(e.message.includes('SQLite operations are not allowed inside update hook callbacks'), 
                'Exception should mention re-entrancy: ' + e.message);
    }
  });
  
  // Execute a statement that will trigger the hook
  sql.exec(`INSERT INTO reentrancy_test (id, value) VALUES (1, 'test');`);
  
  // Verify that the hook was called
  assert.equal(reEntrancyHookCalled, true, 'Re-entrancy test hook should be called');
  
  // Verify that the re-entrancy exception was thrown
  assert.equal(reEntrancyExceptionThrown, true, 'Re-entrancy exception should be thrown');
  
  // Verify that the first INSERT worked but the nested one did not
  const reEntrancyResult = sql.exec(`SELECT id, value FROM reentrancy_test WHERE id = 1;`).toArray();
  assert.equal(reEntrancyResult.length, 1, 'First INSERT should still work');
  assert.equal(reEntrancyResult[0].value, 'test', 'Value should be from the first INSERT');

  // Clean up
  sql.clearUpdateHook();
  sql.exec(`DROP TABLE reentrancy_test;`);
}

// Test prepared statement usage inside update hooks
async function testPreparedStatements(sql) {
  console.log("Testing prepared statements with update hooks");
  
  // Set up test tables
  sql.exec(`CREATE TABLE stmt_test (id INTEGER PRIMARY KEY, value TEXT);`);
  sql.exec(`INSERT INTO stmt_test (id, value) VALUES (1, 'test1'), (2, 'test2');`);
  
  // Test prepared statement usage inside hook
  let statementBlocked = false;
  
  // Create a prepared statement
  const statement = sql.prepare("SELECT * FROM stmt_test WHERE id = ?;");
  
  sql.setUpdateHook((rowid, tableName, operation) => {
    try {
      // Try to use the prepared statement inside the hook
      statement.run(1);
    } catch (e) {
      statementBlocked = true;
      assert.ok(e.message.includes("SQLite operations are not allowed inside update hook callbacks"),
                `Expected re-entrancy error but got: ${e.message}`);
    }
  });
  
  // Trigger the hook
  sql.exec(`INSERT INTO stmt_test (id, value) VALUES (3, 'test3');`);
  
  // Verify statement usage was blocked
  assert.equal(statementBlocked, true, "Prepared statement execution should have been blocked");
  
  // Clean up
  sql.clearUpdateHook();
  
  // Consume all rows from the statement to finalize it
  try {
    statement.run(1).toArray();
  } catch (e) {
    console.log(`Note: Could not finalize statement: ${e.message}`);
  }
  
  sql.exec(`DROP TABLE stmt_test;`);
}

// Test cursor operations inside update hooks
async function testCursorOperations(sql) {
  console.log("Testing cursor operations with update hooks");
  
  // Set up test table
  sql.exec(`CREATE TABLE cursor_test (id INTEGER PRIMARY KEY, value TEXT);`);
  sql.exec(`INSERT INTO cursor_test (id, value) VALUES (1, 'test1'), (2, 'test2');`);
  
  // Test cursor operations inside hook
  let cursorOpBlocked = false;
  
  // Create a cursor
  const cursor = sql.exec("SELECT * FROM cursor_test ORDER BY id;");
  
  sql.setUpdateHook((rowid, tableName, operation) => {
    try {
      // Try to call next() on the cursor
      cursor.next();
    } catch (e) {
      cursorOpBlocked = true;
      assert.ok(e.message.includes("SQLite operations are not allowed inside update hook callbacks"),
                `Expected re-entrancy error but got: ${e.message}`);
    }
  });
  
  // Trigger the hook
  sql.exec(`INSERT INTO cursor_test (id, value) VALUES (3, 'test3');`);
  
  // Verify the cursor operation was blocked
  assert.equal(cursorOpBlocked, true, "Cursor operation should have been blocked");
  
  // Clean up
  sql.clearUpdateHook();
  
  // Explicitly finalize cursor by consuming all rows
  cursor.toArray();
  
  sql.exec(`DROP TABLE cursor_test;`);
}

// Test other SQL operations (like databaseSize) inside update hooks
async function testMiscOperations(sql) {
  console.log("Testing misc SQL operations with update hooks");
  
  // Set up test table
  sql.exec(`CREATE TABLE misc_test (id INTEGER PRIMARY KEY, value TEXT);`);
  
  // Test databaseSize property access inside hook
  let dbSizeBlocked = false;
  
  sql.setUpdateHook((rowid, tableName, operation) => {
    try {
      // Try to get database size inside the hook
      sql.databaseSize;
    } catch (e) {
      dbSizeBlocked = true;
      assert.ok(e.message.includes("SQLite operations are not allowed inside update hook callbacks"),
                `Expected re-entrancy error but got: ${e.message}`);
    }
  });
  
  // Trigger the hook
  sql.exec(`INSERT INTO misc_test (id, value) VALUES (1, 'test1');`);
  
  // Verify database size operation was blocked
  assert.equal(dbSizeBlocked, true, "databaseSize property should have been blocked");
  
  // Clean up
  sql.clearUpdateHook();
  sql.exec(`DROP TABLE misc_test;`);
}

// Run all the tests
async function test(state) {
  const storage = state.storage;
  const sql = storage.sql;
  
  await testBasicHooks(sql);
  await testHookExceptions(sql);
  await testReentrancyPrevention(sql);
  await testPreparedStatements(sql);
  await testCursorOperations(sql);
  await testMiscOperations(sql);
  
  console.log("All SQL hooks tests completed successfully");
}

export class DurableObjectExample extends DurableObject {
  constructor(state, env) {
    super(state, env);
    this.state = state;
  }

  async fetch(req) {
    if (req.url.endsWith('/sql-hooks-test')) {
      await test(this.state);
      return Response.json({ ok: true });
    } else if (req.url.endsWith('/sql-hooks-basic')) {
      await testBasicHooks(this.state.storage.sql);
      return Response.json({ ok: true });
    } else if (req.url.endsWith('/sql-hooks-exceptions')) {
      await testHookExceptions(this.state.storage.sql);
      return Response.json({ ok: true });
    } else if (req.url.endsWith('/sql-hooks-reentrancy')) {
      await testReentrancyPrevention(this.state.storage.sql);
      return Response.json({ ok: true });
    } else if (req.url.endsWith('/sql-hooks-prepared-statements')) {
      await testPreparedStatements(this.state.storage.sql);
      return Response.json({ ok: true });
    } else if (req.url.endsWith('/sql-hooks-cursor-operations')) {
      await testCursorOperations(this.state.storage.sql);
      return Response.json({ ok: true });
    } else if (req.url.endsWith('/sql-hooks-misc-operations')) {
      await testMiscOperations(this.state.storage.sql);
      return Response.json({ ok: true });
    }
    
    throw new Error('unknown url: ' + req.url);
  }
}

export default {
  async fetch(request, env, ctx) {
    const id = env.ns.idFromName("test");
    const obj = env.ns.get(id);
    const response = await obj.fetch(request);
    
    // Assert the response was ok
    const result = await response.json();
    assert.deepEqual(result, { ok: true });
    
    return new Response("SQL hooks test passed!");
  }
};

// Export individual test modules that can be run separately
export let testSqlHooksBasic = {
  async test(ctrl, env, ctx) {
    const id = env.ns.idFromName("test");
    const obj = env.ns.get(id);
    const response = await obj.fetch(new Request("https://example.com/sql-hooks-basic"));
    
    const result = await response.json();
    assert.deepEqual(result, { ok: true });
  }
};

export let testSqlHooksExceptions = {
  async test(ctrl, env, ctx) {
    const id = env.ns.idFromName("test");
    const obj = env.ns.get(id);
    const response = await obj.fetch(new Request("https://example.com/sql-hooks-exceptions"));
    
    const result = await response.json();
    assert.deepEqual(result, { ok: true });
  }
};

export let testSqlHooksReentrancy = {
  async test(ctrl, env, ctx) {
    const id = env.ns.idFromName("test");
    const obj = env.ns.get(id);
    const response = await obj.fetch(new Request("https://example.com/sql-hooks-reentrancy"));
    
    const result = await response.json();
    assert.deepEqual(result, { ok: true });
  }
};

export let testSqlHooksPreparedStatements = {
  async test(ctrl, env, ctx) {
    const id = env.ns.idFromName("test");
    const obj = env.ns.get(id);
    const response = await obj.fetch(new Request("https://example.com/sql-hooks-prepared-statements"));
    
    const result = await response.json();
    assert.deepEqual(result, { ok: true });
  }
};

export let testSqlHooksCursorOperations = {
  async test(ctrl, env, ctx) {
    const id = env.ns.idFromName("test");
    const obj = env.ns.get(id);
    const response = await obj.fetch(new Request("https://example.com/sql-hooks-cursor-operations"));
    
    const result = await response.json();
    assert.deepEqual(result, { ok: true });
  }
};

export let testSqlHooksMiscOperations = {
  async test(ctrl, env, ctx) {
    const id = env.ns.idFromName("test");
    const obj = env.ns.get(id);
    const response = await obj.fetch(new Request("https://example.com/sql-hooks-misc-operations"));
    
    const result = await response.json();
    assert.deepEqual(result, { ok: true });
  }
};

// Main test entry point that runs all tests
export let testSqlHooks = {
  async test(ctrl, env, ctx) {
    const id = env.ns.idFromName("test");
    const obj = env.ns.get(id);
    const response = await obj.fetch(new Request("https://example.com/sql-hooks-test"));
    
    const result = await response.json();
    assert.deepEqual(result, { ok: true });
  }
};