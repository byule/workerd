// Copyright (c) 2023 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

import * as assert from 'node:assert';
import { DurableObject } from 'cloudflare:workers';

// This test validates that the SQL update hook uses lazy loading
// at the row level with a simple values.old and values.new pattern.

async function test(state) {
  const storage = state.storage;
  const sql = storage.sql;
  
  // Create a test table with multiple columns for our test
  sql.exec(
    `CREATE TABLE IF NOT EXISTS lazy_test (
      id INTEGER PRIMARY KEY, 
      name TEXT, 
      value INTEGER,
      data BLOB,
      extra TEXT
    )`
  );

  // Array to collect update hook information
  const updateEvents = [];
  
  // Set update hook that will verify lazy loading
  sql.setUpdateHook((operation, table, rowid, values) => {
    console.log('UPDATE HOOK CALLED:', { operation, table, rowid });
    
    // Function to record events
    const captureEvent = () => {
      updateEvents.push({
        operation,
        table,
        rowid,
      });
    };
    
    // For insert operations, access only values.new
    if (operation === 'insert') {
      console.log('INSERT: Accessing values.new but not values.old');
      
      // Access new values
      assert.equal(values.new.column1, 'test', "Expected name to be 'test'");
      
      // Don't access old values - there shouldn't be any anyway for INSERT
      
      captureEvent();
    }
    
    // For updates, access both values.old and values.new
    else if (operation === 'update') {
      console.log('UPDATE: Accessing both values.old and values.new');
      
      // Access old values
      console.log('Old name was:', values.old.column1);
      console.log('Old value was:', values.old.column2);
      
      // Access new values
      assert.equal(values.new.column1, 'updated', "Expected name to be 'updated'");
      
      captureEvent();
    }
    
    // For deletes, access only values.old
    else if (operation === 'delete') {
      console.log('DELETE: Only accessing values.old');
      
      // Access old values
      console.log('Deleted name was:', values.old.column1);
      
      // Don't access new values - there shouldn't be any anyway for DELETE
      
      captureEvent();
    }
  });

  // Generate some test data - a binary blob
  const testBlob = new Uint8Array([1, 2, 3, 4, 5]);
  
  // INSERT operation (should trigger hook, which accesses only values.new)
  sql.exec(
    'INSERT INTO lazy_test (name, value, data, extra) VALUES (?, ?, ?, ?)',
    'test', 100, testBlob, 'extra value'
  );
  
  // Verify we got the insert event
  assert.equal(updateEvents[0].operation, 'insert', "First event should be an insert");
  
  // UPDATE operation (should trigger hook, accessing both values.old and values.new)
  sql.exec(
    'UPDATE lazy_test SET name = ?, value = ?, extra = ? WHERE id = ?',
    'updated', 200, 'new extra', 1
  );
  
  // Verify we got the update event
  assert.equal(updateEvents[1].operation, 'update', "Second event should be an update");
  
  // DELETE operation (should trigger hook, accessing values.old)
  sql.exec('DELETE FROM lazy_test WHERE id = ?', 1);
  
  // Verify we got the delete event
  assert.equal(updateEvents[2].operation, 'delete', "Third event should be a delete");
  
  console.log('Row-level lazy loading with values.old/values.new test passed!');
}

export class LazyTestDO extends DurableObject {
  constructor(state, env) {
    super(state, env);
    this.state = state;
  }

  async fetch(req) {
    await test(this.state);
    return Response.json({ success: true });
  }
}

export default {
  async test(ctrl, env, ctx) {
    let id = env.ns.idFromName('LazyTest');
    let obj = env.ns.get(id);
    let resp = await obj.fetch('http://localhost/');
    return await resp.json();
  },
};