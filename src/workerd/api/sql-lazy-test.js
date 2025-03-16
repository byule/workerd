// Copyright (c) 2023 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

import * as assert from 'node:assert';
import { DurableObject } from 'cloudflare:workers';

// This test validates that the SQL update hook uses lazy loading
// for row value extraction, only loading the columns that are actually used.

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
  
  // Set update hook that will capture extraction counts
  sql.setUpdateHook((operation, table, rowid, oldValues, newValues) => {
    console.log('UPDATE HOOK CALLED:', { operation, table, rowid });
    // Get extraction counts, accounting for empty objects
    const oldCount = oldValues.__extractionCount ? oldValues.__extractionCount() : 0;
    const newCount = newValues.__extractionCount ? newValues.__extractionCount() : 0;
    console.log('Extraction counts:', {
      oldValuesExtracted: oldCount,
      newValuesExtracted: newCount
    });

    // Do extraction count recording after we access the columns
    // to capture the right counts
    const captureEvent = () => {
      updateEvents.push({
        operation,
        table,
        rowid,
        oldValuesExtractedCount: oldValues.__extractionCount ? oldValues.__extractionCount() : 0,
        newValuesExtractedCount: newValues.__extractionCount ? newValues.__extractionCount() : 0,
      });
    };
    
    // For insert operations, only access a single column from new values
    if (operation === 'insert') {
      console.log('INSERT: Only accessing name column:');
      assert.equal(newValues.column1(), 'test', "Expected name to be 'test'");
      captureEvent();
    }
    
    // For updates, access all columns from oldValues but only one from newValues
    else if (operation === 'update') {
      console.log('UPDATE: Accessing all old columns');
      console.log('Old name was:', oldValues.column1());
      console.log('Old value was:', oldValues.column2());
      console.log('Old data was:', oldValues.column3() ? 'BLOB' : 'null');
      console.log('Old extra was:', oldValues.column4());
      
      // Only access one column from the new values
      console.log('UPDATE: Only accessing new name:');
      assert.equal(newValues.column1(), 'updated', "Expected name to be 'updated'");
      captureEvent();
    }
    
    // For deletes, don't access any columns at all
    else if (operation === 'delete') {
      console.log('DELETE: Not accessing any columns');
      // Intentionally don't access any column values
      captureEvent();
    }
  });

  // Generate some test data - a binary blob
  const testBlob = new Uint8Array([1, 2, 3, 4, 5]);
  
  // INSERT operation (should trigger hook, which accesses only 1 column)
  sql.exec(
    'INSERT INTO lazy_test (name, value, data, extra) VALUES (?, ?, ?, ?)',
    'test', 100, testBlob, 'extra value'
  );
  
  // Verify the first event
  assert.equal(updateEvents[0].operation, 'insert', "First event should be an insert");
  // Since we only accessed column0 (id) in the update hook, extraction count should be 1
  assert.equal(updateEvents[0].newValuesExtractedCount, 1, 
    "Insert should have extracted exactly 1 column since we only accessed ID");
  
  // UPDATE operation (should trigger hook, accessing all old columns but only 1 new column)
  sql.exec(
    'UPDATE lazy_test SET name = ?, value = ?, extra = ? WHERE id = ?',
    'updated', 200, 'new extra', 1
  );
  
  // Verify the second event
  assert.equal(updateEvents[1].operation, 'update', "Second event should be an update");
  // We accessed all 4 old columns in the update hook
  assert.equal(updateEvents[1].oldValuesExtractedCount, 4, 
    "Update should have extracted all 4 old columns");
  // We only accessed column1 (name) from the new values
  assert.equal(updateEvents[1].newValuesExtractedCount, 1, 
    "Update should have extracted only 1 new column (name)");
  
  // DELETE operation (should trigger hook, not accessing any columns)
  sql.exec('DELETE FROM lazy_test WHERE id = ?', 1);
  
  // Verify the third event
  assert.equal(updateEvents[2].operation, 'delete', "Third event should be a delete");
  // We didn't access any columns in the delete hook
  assert.equal(updateEvents[2].oldValuesExtractedCount, 0, 
    "Delete should have extracted 0 columns since we didn't access any");
  
  console.log('Lazy loading test passed!');
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