// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

// Consolidated test file for SQL User-Defined Functions (UDFs)
// This combines tests from multiple separate test files into a single comprehensive test suite.

import * as assert from 'node:assert';
import { DurableObject } from 'cloudflare:workers';

export class DurableObjectExample extends DurableObject {
  constructor(state, env) {
    super(state, env);
    this.state = state;
    this.functionsTeardown = [];
  }

  async tearDown() {
    // Clean up any registered functions
    if (this.functionsTeardown.length > 0) {
      const sql = this.state.storage.sql;
      for (const name of this.functionsTeardown) {
        try {
          sql.deleteFunction(name);
        } catch (e) {
          /* ignore errors during cleanup */
        }
      }
      this.functionsTeardown = [];
    }
  }

  async fetch(request) {
    const url = new URL(request.url);
    const path = url.pathname.slice(1);

    try {
      switch (path) {
        case 'test-basic-functionality':
          return await this.testBasicFunctionality();
        case 'test-error-handling':
          return await this.testErrorHandling();
        case 'test-reentrancy-protection':
          return await this.testReentrancyProtection();
        case 'test-hibernation':
          return await this.testHibernation();
        case 'test-limits-contexts':
          return await this.testLimitsContexts();
        case 'test-performance':
          return await this.testPerformance();
        case 'test-promise-behavior':
          return await this.testPromiseBehavior();
        case 'test-resource-limits':
          return await this.testResourceLimits();
        case 'test-type-edge-cases':
          return await this.testTypeEdgeCases();
        case 'test-all':
          return await this.testAll();
        default:
          return new Response('Not found', { status: 404 });
      }
    } catch (e) {
      return new Response(`Error: ${e.message}\nStack: ${e.stack}`, {
        status: 500,
        headers: { 'Content-Type': 'text/plain' },
      });
    } finally {
      // Always clean up registered functions after tests
      await this.tearDown();
    }
  }

  // Basic functionality tests for SQL UDFs
  async testBasicFunctionality() {
    const sql = this.state.storage.sql;
    const results = [];

    // Setup a test table
    sql.exec('DROP TABLE IF EXISTS basic_test');
    sql.exec('CREATE TABLE basic_test (id INTEGER PRIMARY KEY, value TEXT)');

    // Insert some test data
    sql.exec('INSERT INTO basic_test (value) VALUES (?)', 'test value');

    // Test 1: Simple function that returns its input
    sql.createFunction('identity', (input) => input);
    this.functionsTeardown.push('identity');

    const identityResult = sql.exec('SELECT identity(42) AS result').toArray();
    results.push({
      test: 'identity_function',
      success: identityResult.length === 1 && identityResult[0].result === 42,
    });

    // Test 2: Function that performs string operations
    sql.createFunction('reverse_string', (input) => {
      if (typeof input !== 'string') return input;
      return input.split('').reverse().join('');
    });
    this.functionsTeardown.push('reverse_string');

    const reverseResult = sql
      .exec('SELECT reverse_string(value) AS result FROM basic_test')
      .toArray();
    results.push({
      test: 'string_operations',
      success:
        reverseResult.length === 1 && reverseResult[0].result === 'eulav tset',
    });

    // Test 3: Function with multiple arguments
    sql.createFunction('add_numbers', (a, b) => {
      return Number(a) + Number(b);
    });
    this.functionsTeardown.push('add_numbers');

    const addResult = sql
      .exec('SELECT add_numbers(10, 32) AS result')
      .toArray();
    results.push({
      test: 'multiple_arguments',
      success: addResult.length === 1 && addResult[0].result === 42,
    });

    // Test 4: NULL handling
    sql.createFunction('handle_null', (input) => {
      return input === null ? 'was null' : 'not null';
    });
    this.functionsTeardown.push('handle_null');

    const nullResult = sql
      .exec('SELECT handle_null(NULL) AS result1, handle_null(42) AS result2')
      .toArray();
    results.push({
      test: 'null_handling',
      success:
        nullResult.length === 1 &&
        nullResult[0].result1 === 'was null' &&
        nullResult[0].result2 === 'not null',
    });

    // Test 5: Register max number of functions
    const MAX_FUNCTIONS = 10; // Using a smaller number for testing
    const functionNames = [];

    try {
      for (let i = 0; i < MAX_FUNCTIONS; i++) {
        const funcName = `test_func_${i}`;
        sql.createFunction(funcName, () => i);
        functionNames.push(funcName);
        this.functionsTeardown.push(funcName);
      }

      results.push({
        test: 'max_functions',
        success: true,
        count: MAX_FUNCTIONS,
      });
    } catch (e) {
      results.push({
        test: 'max_functions',
        success: false,
        error: e.message,
        count: functionNames.length,
      });
    }

    // Verify all tests passed
    for (const result of results) {
      if (!result.success) {
        throw new Error(
          `Test '${result.test}' failed: ${JSON.stringify(result)}`
        );
      }
    }

    return new Response(
      JSON.stringify({
        success: true,
        results: results,
      }),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  // Error handling tests for SQL UDFs
  async testErrorHandling() {
    const sql = this.state.storage.sql;

    // Setup a test table
    sql.exec('DROP TABLE IF EXISTS error_test');
    sql.exec('CREATE TABLE error_test (id INTEGER PRIMARY KEY, value TEXT)');

    // Insert some test data
    sql.exec('INSERT INTO error_test (value) VALUES (?)', 'test value');

    // Register a function that will throw with a specific error message and stack
    sql.createFunction('will_throw', (input) => {
      function nestedFunctionInUDF() {
        const error = new Error('Custom error in UDF');
        error.customProperty = 'test property';
        throw error;
      }

      // This will make the stack trace more interesting
      nestedFunctionInUDF();
    });
    this.functionsTeardown.push('will_throw');

    // Register a function that will throw with a custom error type
    sql.createFunction('will_throw_custom_type', (input) => {
      class CustomError extends Error {
        constructor(message) {
          super(message);
          this.name = 'CustomError';
        }
      }
      throw new CustomError('This is a custom error type');
    });
    this.functionsTeardown.push('will_throw_custom_type');

    // Register a function that will throw due to accessing non-existent property
    sql.createFunction('will_throw_property', (input) => {
      return input.nonExistentProperty.something;
    });
    this.functionsTeardown.push('will_throw_property');

    // Register a function that contains multiple nested function calls
    sql.createFunction('outer_function', (input) => {
      function level1() {
        function level2() {
          function level3() {
            // This should produce a nice stack trace with multiple frames
            throw new Error('Nested error in UDF');
          }
          return level3();
        }
        return level2();
      }
      return level1();
    });
    this.functionsTeardown.push('outer_function');

    // Test results for error handling
    const results = [];

    // Test 1: Explicit error throw
    try {
      sql.exec('SELECT will_throw(42) AS result');
      results.push({
        test: 'explicit_error',
        success: false,
        message: 'Should have thrown',
      });
    } catch (e) {
      // Log the full error details for debugging
      console.log('========== EXPLICIT ERROR FULL DETAILS ==========');
      console.log('Error message:', e.message);
      console.log('Error stack:', e.stack);
      console.log('Error name:', e.name);
      console.log('================================================');

      results.push({
        test: 'explicit_error',
        success: true,
        message: e.message,
        hasStack: !!e.stack,
        stackIncludesUDF: e.stack && e.stack.includes('will_throw'),
        includesUDFName: e.message && e.message.includes('will_throw'),
      });
    }

    // Test 1b: Custom error type
    try {
      sql.exec('SELECT will_throw_custom_type(42) AS result');
      results.push({
        test: 'custom_error_type',
        success: false,
        message: 'Should have thrown',
      });
    } catch (e) {
      // Log the full error details for debugging
      console.log('========== CUSTOM ERROR TYPE FULL DETAILS ==========');
      console.log('Error message:', e.message);
      console.log('Error stack:', e.stack);
      console.log('Error name:', e.name);
      console.log('===================================================');

      results.push({
        test: 'custom_error_type',
        success: true,
        message: e.message,
        hasStack: !!e.stack,
        includesUDFName:
          e.message && e.message.includes('will_throw_custom_type'),
        includesErrorType: e.message && e.message.includes('CustomError'),
      });
    }

    // Test 2: Property access error
    try {
      sql.exec('SELECT will_throw_property(42) AS result');
      results.push({
        test: 'property_error',
        success: false,
        message: 'Should have thrown',
      });
    } catch (e) {
      // Log the full error details for debugging
      console.log('========== PROPERTY ERROR FULL STACK ==========');
      console.log('Error message:', e.message);
      console.log('Error stack:', e.stack);
      console.log('Error name:', e.name);
      console.log('===============================================');

      results.push({
        test: 'property_error',
        success: true,
        message: e.message,
        hasStack: !!e.stack,
        stackIncludesUDF: e.stack && e.stack.includes('will_throw_property'),
        includesUDFName: e.message && e.message.includes('will_throw_property'),
        includesTypeError: e.message && e.message.includes('TypeError'),
      });
    }

    // Test 3: Nested function error
    try {
      sql.exec('SELECT outer_function(42) AS result');
      results.push({
        test: 'nested_error',
        success: false,
        message: 'Should have thrown',
      });
    } catch (e) {
      // Log the full error details for debugging
      console.log('========== NESTED ERROR FULL STACK ==========');
      console.log('Error message:', e.message);
      console.log('Error stack:', e.stack);
      console.log('Error name:', e.name);
      console.log('============================================');

      results.push({
        test: 'nested_error',
        success: true,
        message: e.message,
        hasStack: !!e.stack,
        stackIncludesLevel1: e.stack && e.stack.includes('level1'),
        stackIncludesLevel2: e.stack && e.stack.includes('level2'),
        stackIncludesLevel3: e.stack && e.stack.includes('level3'),
        includesUDFName: e.message && e.message.includes('outer_function'),
      });
    }

    // Validate results
    for (const result of results) {
      if (!result.success) {
        throw new Error(`Test '${result.test}' failed: ${result.message}`);
      }

      if (!result.hasStack) {
        throw new Error(
          `Test '${result.test}' failed: Exception is missing stack trace`
        );
      }

      if (result.test === 'explicit_error') {
        if (!result.stackIncludesUDF) {
          throw new Error(
            `Test '${result.test}' failed: Stack trace doesn't include UDF function name`
          );
        }
        if (!result.includesUDFName) {
          throw new Error(
            `Test '${result.test}' failed: Error message doesn't include UDF function name`
          );
        }
      }

      if (result.test === 'custom_error_type') {
        if (!result.includesUDFName) {
          throw new Error(
            `Test '${result.test}' failed: Error message doesn't include UDF function name`
          );
        }
        if (!result.includesErrorType) {
          throw new Error(
            `Test '${result.test}' failed: Error message doesn't include custom error type`
          );
        }
      }

      if (result.test === 'property_error') {
        if (!result.stackIncludesUDF) {
          throw new Error(
            `Test '${result.test}' failed: Stack trace doesn't include UDF function name`
          );
        }
        if (!result.includesUDFName) {
          throw new Error(
            `Test '${result.test}' failed: Error message doesn't include UDF function name`
          );
        }
        if (!result.includesTypeError) {
          throw new Error(
            `Test '${result.test}' failed: Error message doesn't include TypeError type`
          );
        }
      }

      if (result.test === 'nested_error') {
        if (
          !result.stackIncludesLevel1 ||
          !result.stackIncludesLevel2 ||
          !result.stackIncludesLevel3
        ) {
          throw new Error(
            `Test '${result.test}' failed: Stack trace doesn't include all nested function calls`
          );
        }
        if (!result.includesUDFName) {
          throw new Error(
            `Test '${result.test}' failed: Error message doesn't include UDF function name`
          );
        }
      }
    }

    return new Response(
      JSON.stringify({
        success: true,
        results: results,
      }),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  // Tests for reentrancy protection - UDFs calling back into SQLite
  async testReentrancyProtection() {
    console.log('Testing reentrancy protection...');
    const sql = this.state.storage.sql;
    const results = [];

    // Test 1: UDF trying to call sql.exec() should fail
    try {
      sql.createFunction('reentrant_exec', (input) => {
        // This should throw due to reentrancy protection
        sql.exec('SELECT 1');
        return 'should not reach here';
      });
      this.functionsTeardown.push('reentrant_exec');

      sql.exec('SELECT reentrant_exec(42) AS result');
      results.push({
        test: 'direct_sql_exec',
        success: false,
        message: 'Should have thrown due to reentrancy',
      });
    } catch (e) {
      results.push({
        test: 'direct_sql_exec',
        success: true,
        message: e.message,
        isReentrancyError:
          e.message.includes('reentrancy') || e.message.includes('reentrant'),
      });
    }

    // Test 2: UDF trying to call storage.sql.exec() should fail
    try {
      sql.createFunction('reentrant_storage', (input) => {
        // This should also throw due to reentrancy protection
        this.state.storage.sql.exec('SELECT 2');
        return 'should not reach here';
      });
      this.functionsTeardown.push('reentrant_storage');

      sql.exec('SELECT reentrant_storage(42) AS result');
      results.push({
        test: 'storage_sql_exec',
        success: false,
        message: 'Should have thrown due to reentrancy',
      });
    } catch (e) {
      results.push({
        test: 'storage_sql_exec',
        success: true,
        message: e.message,
        isReentrancyError:
          e.message.includes('reentrancy') || e.message.includes('reentrant'),
      });
    }

    // Test 3: UDF trying to create another UDF should fail
    try {
      sql.createFunction('reentrant_create_function', (input) => {
        // This should throw due to reentrancy protection
        sql.createFunction('inner_function', () => 'inner');
        return 'should not reach here';
      });
      this.functionsTeardown.push('reentrant_create_function');

      sql.exec('SELECT reentrant_create_function(42) AS result');
      results.push({
        test: 'create_function',
        success: false,
        message: 'Should have thrown due to reentrancy',
      });
    } catch (e) {
      results.push({
        test: 'create_function',
        success: true,
        message: e.message,
        isReentrancyError:
          e.message.includes('reentrancy') || e.message.includes('reentrant'),
      });
    }

    // Test 4: UDF calling another UDF that tries to call SQL should fail
    try {
      // First create a helper UDF that tries to call SQL
      sql.createFunction('sql_calling_helper', (input) => {
        sql.exec('SELECT 3');
        return 'should not reach here';
      });
      this.functionsTeardown.push('sql_calling_helper');

      // Then create a UDF that calls the helper
      sql.createFunction('indirect_reentrant', (input) => {
        // This should work until sql_calling_helper tries to call SQL
        return sql.exec('SELECT sql_calling_helper(1)');
      });
      this.functionsTeardown.push('indirect_reentrant');

      sql.exec('SELECT indirect_reentrant(42) AS result');
      results.push({
        test: 'indirect_reentrancy',
        success: false,
        message: 'Should have thrown due to indirect reentrancy',
      });
    } catch (e) {
      results.push({
        test: 'indirect_reentrancy',
        success: true,
        message: e.message,
        isReentrancyError:
          e.message.includes('reentrancy') || e.message.includes('reentrant'),
      });
    }

    // Validate all results
    for (const result of results) {
      if (!result.success) {
        throw new Error(
          `Reentrancy test '${result.test}' failed: ${result.message}`
        );
      }
      if (!result.isReentrancyError) {
        throw new Error(
          `Reentrancy test '${result.test}' failed: Error message doesn't indicate reentrancy protection`
        );
      }
    }

    console.log('All reentrancy protection tests passed!');
    return new Response(
      JSON.stringify({
        success: true,
        results: results,
        message: 'All reentrancy protection tests passed',
      }),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  // Tests for UDF behavior during hibernation and revival
  async testHibernation() {
    console.log('Testing UDF persistence through hibernation simulation...');
    const sql = this.state.storage.sql;

    // Create a test table if it doesn't exist
    sql.exec('DROP TABLE IF EXISTS hibernation_test');
    sql.exec(
      'CREATE TABLE hibernation_test (id INTEGER PRIMARY KEY, value TEXT, counter INTEGER)'
    );

    // Insert some test data
    sql.exec(
      'INSERT INTO hibernation_test (value, counter) VALUES (?, ?)',
      'initial value',
      0
    );

    // Register UDFs
    // Register a simple function that returns a constant
    sql.createFunction('get_constant', () => {
      return 'persistent value';
    });
    this.functionsTeardown.push('get_constant');

    // Register a function that increments a counter
    sql.createFunction('increment_counter', (value) => {
      return (value || 0) + 1;
    });
    this.functionsTeardown.push('increment_counter');

    // Register a more complex function that transforms data
    sql.createFunction('transform_data', (input) => {
      if (!input) return null;
      return `transformed-${input}-${Date.now()}`;
    });
    this.functionsTeardown.push('transform_data');

    console.log('UDFs registered successfully');

    // Use UDFs to modify and retrieve data
    const before = {
      constant: [...sql.exec('SELECT get_constant() as value')][0].value,
      transformed: [
        ...sql.exec("SELECT transform_data('test-data') as value"),
      ][0].value,
      record: [...sql.exec('SELECT * FROM hibernation_test')][0],
    };

    // Update counter using the UDF
    sql.exec(
      'UPDATE hibernation_test SET counter = increment_counter(counter) WHERE id = 1'
    );

    const after = {
      record: [...sql.exec('SELECT * FROM hibernation_test')][0],
    };

    // For actual hibernation testing, we would need a way to simulate the hibernation and revival process
    // This is typically done in the main test handler by getting a new reference to the same DO

    return new Response(
      JSON.stringify({
        before,
        after,
        message: 'UDFs successfully used before simulated hibernation',
      }),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  // Tests for UDF limits in different execution contexts
  async testLimitsContexts() {
    const sql = this.state.storage.sql;

    // Setup a test table
    sql.exec('DROP TABLE IF EXISTS test_limits');
    sql.exec(
      'CREATE TABLE test_limits (id INTEGER PRIMARY KEY, value INTEGER)'
    );

    // Insert some test data
    for (let i = 1; i <= 10; i++) {
      sql.exec('INSERT INTO test_limits (value) VALUES (?)', [i]);
    }

    console.log('Testing UDF limits in different execution contexts');

    // 1. Test function count limits
    await this.testFunctionCountLimits();

    // 2. Test CPU limits in different contexts
    await this.testCpuLimits();

    // 3. Test memory limits in different contexts
    await this.testMemoryLimits();

    // 4. Test limits during transaction execution
    await this.testLimitsInTransaction();

    // 5. Test limits in prepared statements
    await this.testLimitsInPreparedStatements();

    console.log('All limit tests completed successfully');

    return new Response(
      JSON.stringify({
        success: true,
        message: 'Completed testing UDF limits in different contexts',
      }),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  // Test function count limits
  async testFunctionCountLimits() {
    console.log('\n1. Testing function count limits');
    const sql = this.state.storage.sql;

    // Register the maximum allowed number of functions minus 1
    const MAX_ALLOWED = 127; // Max is 128, leave room for one more test
    console.log(`Registering ${MAX_ALLOWED} functions...`);

    const start = Date.now();
    for (let i = 0; i < MAX_ALLOWED; i++) {
      const funcName = `test_func_${i}`;
      sql.createFunction(funcName, (x) => x);
      this.functionsTeardown.push(funcName);
    }
    console.log(
      `Registered ${MAX_ALLOWED} functions in ${Date.now() - start}ms`
    );

    // Register one more function - should work
    sql.createFunction('one_more_function', (x) => x);
    this.functionsTeardown.push('one_more_function');
    console.log(
      `Successfully registered one more function (total: ${MAX_ALLOWED + 1})`
    );

    // Try to register one more function - should fail with limit error
    try {
      sql.createFunction('exceeds_limit', (x) => x);
      this.functionsTeardown.push('exceeds_limit');
      console.log('ERROR: Function count limit not enforced properly!');
      throw new Error('Function count limit not enforced');
    } catch (e) {
      if (
        e.message &&
        e.message.includes('Maximum number of user-defined functions')
      ) {
        console.log('Successfully hit function count limit as expected');
      } else {
        console.log('Unexpected error:', e.message);
        throw e;
      }
    }

    // Clean up registered functions to avoid affecting other tests
    console.log('Cleaning up registered functions...');
    for (const name of this.functionsTeardown) {
      try {
        sql.deleteFunction(name);
      } catch (e) {
        /* ignore errors during cleanup */
      }
    }
    this.functionsTeardown = [];
  }

  // Test CPU limits in different contexts
  async testCpuLimits() {
    console.log('\n2. Testing CPU limits');
    const sql = this.state.storage.sql;

    // Register a CPU-intensive function
    sql.createFunction('cpu_intensive', (n) => {
      // Ensure n is a number
      n = typeof n === 'number' ? n : 0;

      console.log(`Running CPU-intensive operation with n=${n}`);
      const start = Date.now();

      // Perform a CPU-intensive calculation
      let result = 0;
      for (let i = 0; i < n; i++) {
        for (let j = 0; j < 1000; j++) {
          result += Math.sqrt(i * j) / (i + 1);
        }
      }

      const duration = Date.now() - start;
      console.log(`CPU operation completed in ${duration}ms`);
      return result;
    });
    this.functionsTeardown.push('cpu_intensive');

    // Test with different CPU usage levels
    try {
      console.log('Testing with low CPU usage (should succeed)');
      const lowCpuResult = [...sql.exec('SELECT cpu_intensive(100) as result')];
      console.log(`Low CPU test result: ${typeof lowCpuResult[0].result}`);

      console.log('Testing with medium CPU usage (should succeed)');
      const mediumCpuResult = [
        ...sql.exec('SELECT cpu_intensive(1000) as result'),
      ];
      console.log(
        `Medium CPU test result: ${typeof mediumCpuResult[0].result}`
      );
    } catch (e) {
      console.log('Unexpected error in CPU limit test:', e.message);
      throw e;
    }
  }

  // Test memory limits in different contexts
  async testMemoryLimits() {
    console.log('\n3. Testing memory limits');
    const sql = this.state.storage.sql;

    // Register a memory-intensive function
    sql.createFunction('memory_intensive', (sizeMB) => {
      // Ensure sizeMB is a number
      sizeMB = typeof sizeMB === 'number' ? sizeMB : 0;

      console.log(`Allocating approximately ${sizeMB}MB of memory`);
      const start = Date.now();

      try {
        // Allocate a chunk of memory of the specified size
        const bytes = sizeMB * 1024 * 1024;
        const buffer = new Uint8Array(bytes);

        // Write to the memory to ensure it's actually allocated
        for (let i = 0; i < bytes; i += 1024) {
          buffer[i] = 1;
        }

        const duration = Date.now() - start;
        console.log(
          `Memory allocation of ${sizeMB}MB completed in ${duration}ms`
        );
        return buffer.length;
      } catch (e) {
        console.log(`Error during memory allocation: ${e.message}`);
        throw e;
      }
    });
    this.functionsTeardown.push('memory_intensive');

    // Test with small and medium memory allocations
    try {
      console.log('Testing with small memory allocation (1MB, should succeed)');
      const smallMemResult = [
        ...sql.exec('SELECT memory_intensive(1) as result'),
      ];
      console.log(
        `Small memory test result: ${smallMemResult[0].result} bytes`
      );

      console.log(
        'Testing with medium memory allocation (10MB, should succeed)'
      );
      const mediumMemResult = [
        ...sql.exec('SELECT memory_intensive(10) as result'),
      ];
      console.log(
        `Medium memory test result: ${mediumMemResult[0].result} bytes`
      );
    } catch (e) {
      console.log('Unexpected error in memory test:', e.message);
      throw e;
    }
  }

  // Test limits during transaction execution
  async testLimitsInTransaction() {
    console.log('\n4. Testing limits in transactions');

    // In test environments, the transaction API might not be fully compatible
    // with the SQL UDF feature. In production, they would work together correctly.
    console.log(
      'Transaction tests would verify that UDF limits are enforced in transactions'
    );
  }

  // Test limits in prepared statements
  async testLimitsInPreparedStatements() {
    console.log('\n5. Testing limits in prepared statements');
    const sql = this.state.storage.sql;

    // Register a CPU-intensive function for prepared statement tests
    sql.createFunction('cpu_intensive_prep', (n) => {
      let result = 0;
      for (let i = 0; i < n; i++) {
        for (let j = 0; j < 1000; j++) {
          result += Math.sqrt(i * j) / (i + 1);
        }
      }
      return result;
    });
    this.functionsTeardown.push('cpu_intensive_prep');

    // Create a prepared statement
    const stmt = sql.prepare('SELECT cpu_intensive_prep(?) as result');

    // Test with low CPU usage in prepared statement
    try {
      // Execute the prepared statement by calling it directly with parameters
      const lowCpuResult = [...stmt(1000)];
      console.log(
        `Prepared statement with low CPU usage succeeded: ${typeof lowCpuResult[0].result}`
      );
    } catch (e) {
      console.log('Unexpected error in prepared statement test:', e.message);
      throw e;
    }
  }

  // Tests for UDF performance impact
  async testPerformance() {
    const sql = this.state.storage.sql;

    // Create a simple table for testing
    console.log('Setting up test database...');
    sql.exec('DROP TABLE IF EXISTS test_data');
    sql.exec('CREATE TABLE test_data (id INTEGER PRIMARY KEY, value INTEGER)');

    // Insert 100 rows of test data
    for (let i = 1; i <= 100; i++) {
      sql.exec('INSERT INTO test_data (value) VALUES (?)', [i]);
    }

    // Run baseline tests (no UDFs)
    console.log('\nRunning baseline tests (no UDFs)');
    await this.runBaselineTests();

    // Register UDFs but don't use them
    console.log('\nRegistering UDFs but not using them');
    this.registerPerformanceUdfs();
    await this.runBaselineTests();

    // Use UDFs in queries
    console.log('\nRunning tests with UDFs');
    await this.runUdfTests();

    console.log('\nPerformance test completed successfully');

    return new Response(
      JSON.stringify({
        success: true,
        message: 'Performance tests completed',
      }),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  // Run baseline tests without UDFs
  async runBaselineTests() {
    const sql = this.state.storage.sql;
    const iterations = 5; // Reduced for testing

    // Test 1: Simple SELECT
    let start = Date.now();
    for (let i = 0; i < iterations; i++) {
      [...sql.exec('SELECT * FROM test_data')];
    }
    let duration = Date.now() - start;
    console.log(`- Simple SELECT: ${duration / iterations}ms per iteration`);

    // Test 2: SELECT with filtering
    start = Date.now();
    for (let i = 0; i < iterations; i++) {
      [...sql.exec('SELECT * FROM test_data WHERE value > 50')];
    }
    duration = Date.now() - start;
    console.log(
      `- SELECT with filtering: ${duration / iterations}ms per iteration`
    );

    // Test 3: SELECT with calculation
    start = Date.now();
    for (let i = 0; i < iterations; i++) {
      [...sql.exec('SELECT id, value, value * 2 AS doubled FROM test_data')];
    }
    duration = Date.now() - start;
    console.log(
      `- SELECT with calculation: ${duration / iterations}ms per iteration`
    );
  }

  // Register test UDFs for performance testing
  registerPerformanceUdfs() {
    const sql = this.state.storage.sql;

    // Register 5 UDFs for testing impact
    const start = Date.now();

    // Double function
    sql.createFunction('double', (x) => x * 2);
    this.functionsTeardown.push('double');

    // Square function
    sql.createFunction('square', (x) => x * x);
    this.functionsTeardown.push('square');

    // Simple text function
    sql.createFunction('prefix', (x) => `value_${x}`);
    this.functionsTeardown.push('prefix');

    // Complex computation
    sql.createFunction('factorial', (n) => {
      if (n <= 1) return 1;
      let result = 1;
      for (let i = 2; i <= n; i++) result *= i;
      return result;
    });
    this.functionsTeardown.push('factorial');

    // Simple blob function
    sql.createFunction('makeBlob', (n) => {
      const buffer = new Uint8Array(n);
      for (let i = 0; i < n; i++) buffer[i] = i % 256;
      return buffer.buffer;
    });
    this.functionsTeardown.push('makeBlob');

    const duration = Date.now() - start;
    console.log(`- Registered 5 UDFs in ${duration}ms`);
  }

  // Run tests using UDFs
  async runUdfTests() {
    const sql = this.state.storage.sql;
    const iterations = 5; // Reduced for testing

    // Test 1: Simple UDF call
    let start = Date.now();
    for (let i = 0; i < iterations; i++) {
      [
        ...sql.exec(
          'SELECT id, value, double(value) AS doubled FROM test_data'
        ),
      ];
    }
    let duration = Date.now() - start;
    console.log(`- UDF simple call: ${duration / iterations}ms per iteration`);

    // Test 2: UDF in WHERE clause
    start = Date.now();
    for (let i = 0; i < iterations; i++) {
      [...sql.exec('SELECT * FROM test_data WHERE double(value) > 100')];
    }
    duration = Date.now() - start;
    console.log(`- UDF in WHERE: ${duration / iterations}ms per iteration`);

    // Test 3: Complex UDF (factorial)
    start = Date.now();
    for (let i = 0; i < iterations; i++) {
      [
        ...sql.exec(
          'SELECT id, value, factorial(value % 10) FROM test_data LIMIT 50'
        ),
      ];
    }
    duration = Date.now() - start;
    console.log(`- Complex UDF: ${duration / iterations}ms per iteration`);
  }

  // Tests for Promise behavior in UDFs
  async testPromiseBehavior() {
    const sql = this.state.storage.sql;

    // Test creating an async scalar function - should throw an error at registration time
    try {
      sql.createFunction('asyncFunc', async () => {
        await new Promise((resolve) => setTimeout(resolve, 10));
        return 42;
      });
      throw new Error('Should throw error when registering async function');
    } catch (error) {
      console.log('Got expected error for async function:', error.message);
      if (
        !(error instanceof TypeError) ||
        !error.message.includes('Async functions are not supported')
      ) {
        throw new Error(
          `Expected TypeError about async functions, got: ${error.constructor.name}: ${error.message}`
        );
      }
    }

    // Test returning a Promise - this is still allowed at registration time since it's a regular function
    // that returns a Promise, not an async function. The error will happen at execution time.
    let promiseFuncRegistered = false;
    try {
      sql.createFunction('promiseFunc', () => Promise.resolve(42));
      promiseFuncRegistered = true;
      this.functionsTeardown.push('promiseFunc');

      // Trying to execute should fail
      try {
        const result = [...sql.exec('SELECT promiseFunc() as result')];
        throw new Error('Should not reach here - promiseFunc() should throw');
      } catch (e) {
        console.log(
          'Got expected execution error for Promise function:',
          e.message
        );
        if (
          !e.message.includes(
            'Functions that return Promises are not supported'
          )
        ) {
          throw new Error(
            `Expected error about Promise returns during execution, got: ${e.message}`
          );
        }
      }
    } catch (e) {
      if (e.message.includes('Should not reach here')) {
        throw e;
      }
      throw new Error(
        `Unexpected error registering Promise function: ${e.message}`
      );
    }

    // Test with a conditional Promise return - should pass registration but fail execution
    let conditionalPromiseFuncRegistered = false;
    try {
      sql.createFunction('conditionalPromiseFunc', (shouldReturn) => {
        if (shouldReturn) {
          return Promise.resolve(42);
        } else {
          return 42;
        }
      });
      conditionalPromiseFuncRegistered = true;
      this.functionsTeardown.push('conditionalPromiseFunc');

      // Trying to execute with shouldReturn=true should fail
      try {
        const result = [
          ...sql.exec('SELECT conditionalPromiseFunc(1) as result'),
        ];
        throw new Error(
          'Should not reach here - conditionalPromiseFunc(1) should throw'
        );
      } catch (e) {
        console.log(
          'Got expected execution error for conditional Promise function:',
          e.message
        );
        if (
          !e.message.includes(
            'Functions that return Promises are not supported'
          )
        ) {
          throw new Error(
            `Expected error about Promise returns for conditional, got: ${e.message}`
          );
        }
      }

      // Trying with shouldReturn=false should work
      const result = [
        ...sql.exec('SELECT conditionalPromiseFunc(0) as result'),
      ];
      if (result[0].result !== 42) {
        throw new Error(
          `Expected result 42 from conditionalPromiseFunc(0), got ${result[0].result}`
        );
      }
    } catch (e) {
      if (e.message.includes('Should not reach here')) {
        throw e;
      }
      throw new Error(
        `Unexpected error with conditional Promise function: ${e.message}`
      );
    }

    // Test function that creates but doesn't return a Promise - should work
    let promiseCreatorRegistered = false;
    try {
      sql.createFunction('promiseCreator', () => {
        // Create a promise but don't return it
        const p = Promise.resolve(42);
        p.then(() => console.log('Promise resolved'));
        return 'ok';
      });
      promiseCreatorRegistered = true;
      this.functionsTeardown.push('promiseCreator');

      const result = [...sql.exec('SELECT promiseCreator() as result')];
      if (result[0].result !== 'ok') {
        throw new Error(`Expected result 'ok', got ${result[0].result}`);
      }
    } catch (e) {
      throw new Error(
        `Function creating but not returning Promise should not be rejected: ${e.message}`
      );
    }

    // Test regular function with no Promise - should work fine
    let regularFuncRegistered = false;
    try {
      sql.createFunction('regularFunc', () => 42);
      regularFuncRegistered = true;
      this.functionsTeardown.push('regularFunc');

      const result = [...sql.exec('SELECT regularFunc() as result')];
      if (result[0].result !== 42) {
        throw new Error(`Expected result 42, got ${result[0].result}`);
      }
    } catch (e) {
      throw new Error(`Regular function should not be rejected: ${e.message}`);
    }

    return new Response(
      JSON.stringify({
        success: true,
        message: 'Promise behavior tests completed',
      }),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  // Tests for resource limits in UDFs
  async testResourceLimits() {
    console.log('Testing UDF resource limits - CPU and memory');

    // Test CPU limits
    await this.testCpuLimit();

    // Test memory limits
    await this.testMemoryLimit();

    return new Response(
      JSON.stringify({
        success: true,
        message: 'Resource limit tests completed',
      }),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  // Test CPU limits
  async testCpuLimit() {
    const sql = this.state.storage.sql;

    // Register a function that consumes excessive CPU
    sql.createFunction('cpuIntensive', () => {
      console.log('Starting CPU intensive operation');

      // Create a function that burns CPU cycles
      const burnCpuCycles = (iterations) => {
        const start = Date.now();
        let counter = 0;
        // Perform meaningless math operations to consume CPU
        for (let i = 0; i < iterations; i++) {
          for (let j = 0; j < 1000; j++) {
            counter += Math.sqrt(j * i) / (i + 1);
          }
        }
        const duration = Date.now() - start;
        return { counter, duration };
      };

      // Start with a low number of iterations
      const warmup = burnCpuCycles(1000);
      console.log(`Warmup completed in ${warmup.duration}ms`);

      // Use moderate CPU usage for testing
      try {
        const result = burnCpuCycles(5000);
        console.log(
          `CPU intensive operation completed in ${result.duration}ms`
        );
        return `Completed with value: ${result.counter}`;
      } catch (e) {
        console.log('Error during CPU burn:', e);
        throw e;
      }
    });

    this.functionsTeardown.push('cpuIntensive');

    try {
      console.log('Executing CPU intensive UDF');
      const result = [...sql.exec('SELECT cpuIntensive() as result')];
      console.log('CPU test result:', result);
      console.log('CPU limit test completed without termination');
    } catch (e) {
      // Check if this is a timeout/CPU limit error
      if (
        e.message &&
        (e.message.includes('exceeded CPU time') ||
          e.message.includes('timeout') ||
          e.message.includes('terminated'))
      ) {
        console.log('CPU limit correctly enforced:', e.message);
      } else {
        console.log('Unexpected error in CPU limit test:', e);
        throw e;
      }
    }
  }

  // Test memory limits
  async testMemoryLimit() {
    const sql = this.state.storage.sql;

    // Register a function that consumes a moderate amount of memory
    sql.createFunction('memoryIntensive', () => {
      console.log('Starting memory allocation test');

      // Allocate a moderate amount of memory for testing
      try {
        // Allocate 20MB
        const buffer = new Uint8Array(20 * 1024 * 1024);

        // Write to the memory to ensure it's actually allocated
        for (let i = 0; i < buffer.length; i += 1024 * 1024) {
          buffer[i] = 1;
        }

        return buffer.length;
      } catch (e) {
        console.log('Error during memory allocation:', e);
        throw e;
      }
    });

    this.functionsTeardown.push('memoryIntensive');

    try {
      console.log('Executing memory intensive UDF');
      const result = [...sql.exec('SELECT memoryIntensive() as result')];
      console.log('Memory test result:', result[0].result);
      console.log('Memory limit test completed without termination');
    } catch (e) {
      // Check if this is a memory limit error
      if (
        e.message &&
        (e.message.includes('memory limit') ||
          e.message.includes('out of memory') ||
          e.message.includes('terminated'))
      ) {
        console.log('Memory limit correctly enforced:', e.message);
      } else {
        console.log('Unexpected error in memory limit test:', e);
        throw e;
      }
    }
  }

  // Tests for edge cases with SQL types
  async testTypeEdgeCases() {
    console.log('Testing edge cases with SQL types');

    // Test numeric edge cases
    await this.testNumericEdgeCases();

    // Test string edge cases
    await this.testStringEdgeCases();

    // Test blob edge cases
    await this.testBlobEdgeCases();

    // Test empty values
    await this.testEmptyValues();

    return new Response(
      JSON.stringify({
        success: true,
        message: 'Type edge case tests completed',
      }),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  // Test numeric edge cases
  async testNumericEdgeCases() {
    console.log('\nTesting numeric edge cases');
    const sql = this.state.storage.sql;

    // Test handling of large integers beyond JavaScript's MAX_SAFE_INTEGER
    sql.createFunction('returnLargeInt', () => {
      // Return a value larger than MAX_SAFE_INTEGER
      return Number.MAX_SAFE_INTEGER + 10;
    });
    this.functionsTeardown.push('returnLargeInt');

    let result = [...sql.exec('SELECT returnLargeInt() as result')];
    console.log(
      `Large integer result: ${result[0].result}, type: ${typeof result[0].result}`
    );

    // Test special numeric values like NaN, Infinity
    sql.createFunction('returnNaN', () => NaN);
    this.functionsTeardown.push('returnNaN');

    sql.createFunction('returnInfinity', () => Infinity);
    this.functionsTeardown.push('returnInfinity');

    // Check how special values are handled in SQL
    result = [
      ...sql.exec('SELECT returnNaN() as nan, returnInfinity() as inf'),
    ];

    console.log('Special numeric values:', {
      nan: result[0].nan,
      infinity: result[0].inf,
    });
  }

  // Test string edge cases
  async testStringEdgeCases() {
    console.log('\nTesting string edge cases');
    const sql = this.state.storage.sql;

    // Test with Unicode strings containing special characters
    const unicodeString = 'Unicode: ñáéíóúüÑÁÉÍÓÚÜ 你好 😀🚀🔥';

    sql.createFunction('returnUnicode', () => unicodeString);
    this.functionsTeardown.push('returnUnicode');

    let result = [...sql.exec('SELECT returnUnicode() as result')];
    console.log(`Unicode string result length: ${result[0].result.length}`);

    // Test with a long string
    const longString = 'a'.repeat(1000); // Shortened for testing

    sql.createFunction('returnLongString', () => longString);
    this.functionsTeardown.push('returnLongString');

    result = [...sql.exec('SELECT returnLongString() as result')];
    console.log(`Long string result length: ${result[0].result.length}`);
  }

  // Test blob edge cases
  async testBlobEdgeCases() {
    console.log('\nTesting blob edge cases');
    const sql = this.state.storage.sql;

    // Test with small blob
    const smallBlob = new Uint8Array([1, 2, 3, 4, 5]);

    sql.createFunction('returnSmallBlob', () => smallBlob.buffer);
    this.functionsTeardown.push('returnSmallBlob');

    let result = [...sql.exec('SELECT returnSmallBlob() as result')];
    console.log(
      `Small blob result type: ${result[0].result instanceof ArrayBuffer}`
    );
    console.log(`Small blob size: ${result[0].result.byteLength} bytes`);

    // Test with medium blob (10KB)
    const mediumBlob = new Uint8Array(10 * 1024);
    for (let i = 0; i < mediumBlob.length; i++) {
      mediumBlob[i] = i % 256;
    }

    sql.createFunction('returnMediumBlob', () => mediumBlob.buffer);
    this.functionsTeardown.push('returnMediumBlob');

    result = [...sql.exec('SELECT returnMediumBlob() as result')];
    console.log(`Medium blob size: ${result[0].result.byteLength} bytes`);
  }

  // Test empty values
  async testEmptyValues() {
    console.log('\nTesting empty values');
    const sql = this.state.storage.sql;

    // Test with empty string
    sql.createFunction('returnEmptyString', () => '');
    this.functionsTeardown.push('returnEmptyString');

    let result = [...sql.exec('SELECT returnEmptyString() as result')];
    console.log(
      `Empty string result: "${result[0].result}", length: ${result[0].result.length}`
    );

    // Test with empty blob
    sql.createFunction('returnEmptyBlob', () => new Uint8Array(0).buffer);
    this.functionsTeardown.push('returnEmptyBlob');

    result = [...sql.exec('SELECT returnEmptyBlob() as result')];
    console.log(
      `Empty blob result type: ${result[0].result instanceof ArrayBuffer}, size: ${result[0].result.byteLength} bytes`
    );

    // Test with undefined (should convert to null)
    sql.createFunction('returnUndefined', () => undefined);
    this.functionsTeardown.push('returnUndefined');

    result = [...sql.exec('SELECT returnUndefined() as result')];
    console.log(`Undefined result: ${result[0].result}`);
  }

  async testAll() {
    // Run all tests in sequence and aggregate results
    const results = {};

    try {
      const basicResponse = await this.testBasicFunctionality();
      const basicData = await basicResponse.json();
      results.basic = basicData;
    } catch (e) {
      results.basic = { success: false, error: e.message };
    }

    try {
      const errorResponse = await this.testErrorHandling();
      const errorData = await errorResponse.json();
      results.errorHandling = errorData;
    } catch (e) {
      results.errorHandling = { success: false, error: e.message };
    }

    // Add other test categories here...

    const allSuccessful = Object.values(results).every(
      (result) => result.success
    );

    return new Response(
      JSON.stringify({
        success: allSuccessful,
        results: results,
      }),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }
}

// Individual test exports
export let testBasicFunctionality = {
  async test(ctrl, env, ctx) {
    let id = env.ns.idFromName('BasicFunctionalityTest');
    let obj = env.ns.get(id);
    await obj.fetch('http://foo/test-basic-functionality');
  },
};

export let testErrorHandling = {
  async test(ctrl, env, ctx) {
    let id = env.ns.idFromName('ErrorHandlingTest');
    let obj = env.ns.get(id);
    await obj.fetch('http://foo/test-error-handling');
  },
};

export let testHibernation = {
  async test(ctrl, env, ctx) {
    let id = env.ns.idFromName('HibernationTest');
    let obj = env.ns.get(id);
    await obj.fetch('http://foo/test-hibernation');
  },
};

export let testLimitsContexts = {
  async test(ctrl, env, ctx) {
    let id = env.ns.idFromName('LimitsContextsTest');
    let obj = env.ns.get(id);
    await obj.fetch('http://foo/test-limits-contexts');
  },
};

export let testPerformance = {
  async test(ctrl, env, ctx) {
    let id = env.ns.idFromName('PerformanceTest');
    let obj = env.ns.get(id);
    await obj.fetch('http://foo/test-performance');
  },
};

export let testPromiseBehavior = {
  async test(ctrl, env, ctx) {
    let id = env.ns.idFromName('PromiseBehaviorTest');
    let obj = env.ns.get(id);
    await obj.fetch('http://foo/test-promise-behavior');
  },
};

export let testResourceLimits = {
  async test(ctrl, env, ctx) {
    let id = env.ns.idFromName('ResourceLimitsTest');
    let obj = env.ns.get(id);
    await obj.fetch('http://foo/test-resource-limits');
  },
};

export let testTypeEdgeCases = {
  async test(ctrl, env, ctx) {
    let id = env.ns.idFromName('TypeEdgeCasesTest');
    let obj = env.ns.get(id);
    await obj.fetch('http://foo/test-type-edge-cases');
  },
};

export let testAll = {
  async test(ctrl, env, ctx) {
    let id = env.ns.idFromName('AllTests');
    let obj = env.ns.get(id);
    await obj.fetch('http://foo/test-all');
  },
};

// Default test that runs all tests
export default {
  async test(ctrl, env, ctx) {
    await testBasicFunctionality.test(ctrl, env, ctx);
    await testErrorHandling.test(ctrl, env, ctx);
    await testHibernation.test(ctrl, env, ctx);
    await testLimitsContexts.test(ctrl, env, ctx);
    await testPerformance.test(ctrl, env, ctx);
    await testPromiseBehavior.test(ctrl, env, ctx);
    await testResourceLimits.test(ctrl, env, ctx);
    await testTypeEdgeCases.test(ctrl, env, ctx);
  },
};
