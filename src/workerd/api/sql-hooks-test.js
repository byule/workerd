// Copyright (c) 2024 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

import * as assert from 'node:assert';
import { DurableObject } from 'cloudflare:workers';

async function test(state) {
  const storage = state.storage;
  const sql = storage.sql;
  
  // Set up test table
  sql.exec(`CREATE TABLE update_hook_test (id INTEGER PRIMARY KEY, value TEXT);`);
  
  // Array to capture update hook callbacks
  const updates = [];
  
  // Register a hook callback
  sql.setUpdateHook((rowid, tableName, operation) => {
    updates.push({rowid, tableName, operation});
  });
  
  // Insert a row - should trigger the hook
  sql.exec(`INSERT INTO update_hook_test (id, value) VALUES (1, 'test');`);
  
  // Verify hook was called for INSERT
  assert.equal(updates.length, 1);
  assert.equal(Number(updates[0].rowid), 1); // Convert BigInt to Number for comparison
  assert.equal(updates[0].tableName, 'update_hook_test');
  assert.equal(updates[0].operation, 'INSERT');
  
  // Update the row - should trigger hook again
  sql.exec(`UPDATE update_hook_test SET value = 'updated' WHERE id = 1;`);
  
  // Verify hook was called for UPDATE
  assert.equal(updates.length, 2);
  assert.equal(Number(updates[1].rowid), 1); // Convert BigInt to Number for comparison
  assert.equal(updates[1].tableName, 'update_hook_test');
  assert.equal(updates[1].operation, 'UPDATE');
  
  // Delete the row - should trigger hook again
  sql.exec(`DELETE FROM update_hook_test WHERE id = 1;`);
  
  // Verify hook was called for DELETE
  assert.equal(updates.length, 3);
  assert.equal(Number(updates[2].rowid), 1); // Convert BigInt to Number for comparison
  assert.equal(updates[2].tableName, 'update_hook_test');
  assert.equal(updates[2].operation, 'DELETE');
  
  // Clear the hook
  sql.clearUpdateHook();
  
  // Reset updates array
  updates.length = 0;
  
  // Insert another row - should NOT trigger the hook
  sql.exec(`INSERT INTO update_hook_test (id, value) VALUES (2, 'test2');`);
  
  // Verify hook was not called
  assert.equal(updates.length, 0);
  
  // Test exception handling
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
    sql.exec(`INSERT INTO update_hook_test (id, value) VALUES (3, 'test3');`);
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
  const result = sql.exec(`SELECT id, value FROM update_hook_test WHERE id = 3;`).toArray();
  assert.equal(result.length, 1, 'Should be able to query after hook exception');
  assert.equal(result[0].value, 'test3', 'Data should be inserted despite hook exception');
  
  // Clear the hook
  sql.clearUpdateHook();
  
  // Cleanup
  sql.exec(`DROP TABLE update_hook_test;`);
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

export let testSqlHooks = {
  async test(ctrl, env, ctx) {
    const id = env.ns.idFromName("test");
    const obj = env.ns.get(id);
    const response = await obj.fetch(new Request("https://example.com/sql-hooks-test"));
    
    // Assert the response was ok
    const result = await response.json();
    assert.deepEqual(result, { ok: true });
  }
};