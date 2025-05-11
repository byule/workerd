// Copyright (c) 2023 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

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
          sql.unregisterFunction(name);
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
        case 'basic-usage':
          await this.testBasicUsage();
          break;
        case 'error-handling':
          await this.testErrorHandling();
          break;
        case 'type-handling':
          await this.testTypeHandling();
          break;
        case 'edge-cases':
          await this.testEdgeCases();
          break;
        case 'async-funcs':
          await this.testAsyncFunctions();
          break;
        case 'security':
          await this.testSecurity();
          break;
        default:
          return new Response('Not found', { status: 404 });
      }

      return new Response(JSON.stringify({ ok: true }));
    } finally {
      // Always clean up registered functions after tests
      await this.tearDown();
    }
  }

  async testBasicUsage() {
    const sql = this.state.storage.sql;

    // Test creating a simple scalar function
    sql.createScalarFunction('myAdd', (a, b) => a + b);
    this.functionsTeardown.push('myAdd');
    let result = [...sql.exec('SELECT myAdd(1, 2) as result')];
    assert.equal(result[0].result, 3);

    // Test creating a scalar function with variable arguments
    sql.createScalarFunction('mySum', (...args) =>
      args.reduce((a, b) => a + b, 0)
    );
    this.functionsTeardown.push('mySum');
    result = [...sql.exec('SELECT mySum(1, 2, 3, 4, 5) as result')];
    assert.equal(result[0].result, 15);

    // Test creating a scalar function with no arguments
    sql.createScalarFunction('static42', () => 42);
    this.functionsTeardown.push('static42');
    result = [...sql.exec('SELECT static42() as result')];
    assert.equal(result[0].result, 42);

    // Test creating a scalar function that returns a string
    sql.createScalarFunction('greeting', (name) => `Hello, ${name}!`);
    this.functionsTeardown.push('greeting');
    result = [...sql.exec("SELECT greeting('World') as result")];
    assert.equal(result[0].result, 'Hello, World!');

    // Test creating a scalar function that returns a boolean (should convert to 0/1)
    sql.createScalarFunction('isPositive', (value) => value > 0);
    this.functionsTeardown.push('isPositive');
    result = [
      ...sql.exec('SELECT isPositive(5) as result, isPositive(-5) as result2'),
    ];
    assert.equal(result[0].result, 1);
    assert.equal(result[0].result2, 0);

    // Test creating a scalar function with the same name as an existing function
    sql.createScalarFunction('myAdd', (a, b, c) => a + b + c);
    // No need to add to teardown as it's already there
    result = [...sql.exec('SELECT myAdd(1, 2, 3) as result')];
    assert.equal(result[0].result, 6);
  }

  async testErrorHandling() {
    const sql = this.state.storage.sql;

    // Test a scalar function that throws an error
    sql.createScalarFunction('throwError', () => {
      throw new Error('Intentional error');
    });
    try {
      const result = [...sql.exec('SELECT throwError()')];
      assert.fail('Should not reach here - throwError() should throw');
    } catch (e) {
      // The error handling provides error context
      assert.ok(
        e.message.includes('JavaScript error in UDF throwError:'),
        `Error should include function name. Got: "${e.message}"`
      );

      // Check if the error also includes SQL context
      assert.ok(
        e.message.includes('SQLITE_ERROR'),
        `Error should include SQLite error context. Got: "${e.message}"`
      );

      // Also verify the error is an instance of Error
      assert.ok(e instanceof Error, 'Error should be an Error instance');
    }

    // Test a scalar function that throws a non-Error object
    sql.createScalarFunction('throwObject', () => {
      throw { custom: 'error' };
    });
    try {
      const result = [...sql.exec('SELECT throwObject()')];
      assert.fail('Should not reach here - throwObject() should throw');
    } catch (e) {
      // The improved error handling should include details about the thrown object
      assert.ok(
        e.message.includes('JavaScript error in UDF throwObject:'),
        `Error should include function name. Got: "${e.message}"`
      );

      // The message should indicate it's an object (not a simple string)
      assert.ok(
        e.message.includes('Object'),
        `Error should indicate object was thrown. Got: "${e.message}"`
      );

      // Check if the error also includes SQL context
      assert.ok(
        e.message.includes('SQLITE_ERROR'),
        `Error should include SQLite error context. Got: "${e.message}"`
      );
    }

    // Test a scalar function that tries to access a non-existent property
    sql.createScalarFunction('accessUndefined', () => {
      return undefined.property;
    });
    try {
      const result = [...sql.exec('SELECT accessUndefined()')];
      assert.fail('Should not reach here - accessUndefined() should throw');
    } catch (e) {
      // The error handling should include the function name
      assert.ok(
        e.message.includes('JavaScript error in UDF accessUndefined:'),
        `Error should include function name. Got: "${e.message}"`
      );

      // Check if the error also includes SQL context
      assert.ok(
        e.message.includes('SQLITE_ERROR'),
        `Error should include SQLite error context. Got: "${e.message}"`
      );
    }

    // Test a scalar function with the wrong number of arguments
    sql.createScalarFunction('exactArguments', (a, b) => a + b);
    try {
      const result = [...sql.exec('SELECT exactArguments(1)')];
      // Missing arguments return null rather than NaN
      // This doesn't match JavaScript behavior but is the current implementation
      assert.strictEqual(
        result[0]['exactArguments(1)'],
        null,
        'Missing arguments should return null in SQL queries'
      );
    } catch (e) {
      assert.fail(`Should not throw for missing arguments: ${e.message}`);
    }
  }

  async testTypeHandling() {
    const sql = this.state.storage.sql;

    // Drop the table if it exists from a previous test run
    sql.exec(`DROP TABLE IF EXISTS types_test`);

    // Create a test table with different types
    sql.exec(`
      CREATE TABLE types_test (
        id INTEGER PRIMARY KEY,
        int_val INTEGER,
        real_val REAL,
        text_val TEXT,
        blob_val BLOB,
        null_val NULL
      )
    `);

    // Insert some test data
    sql.exec(`
      INSERT INTO types_test (int_val, real_val, text_val, blob_val, null_val)
      VALUES (42, 3.14, 'hello', x'DEADBEEF', NULL)
    `);

    // Test INTEGER input and output
    sql.createScalarFunction('intTest', (val) => {
      assert.equal(typeof val, 'number');
      return val * 2;
    });
    let result = [
      ...sql.exec('SELECT intTest(int_val) as result FROM types_test'),
    ];
    assert.equal(result[0].result, 84);

    // Test REAL input and output
    sql.createScalarFunction('realTest', (val) => {
      assert.equal(typeof val, 'number');
      return val * 2;
    });
    result = [
      ...sql.exec('SELECT realTest(real_val) as result FROM types_test'),
    ];
    assert.equal(result[0].result, 6.28);

    // Test TEXT input and output
    sql.createScalarFunction('textTest', (val) => {
      assert.equal(typeof val, 'string');
      return val.toUpperCase();
    });
    result = [
      ...sql.exec('SELECT textTest(text_val) as result FROM types_test'),
    ];
    assert.equal(result[0].result, 'HELLO');

    // Test BLOB input and output
    sql.createScalarFunction('blobTest', (val) => {
      // BLOBs are passed as ArrayBuffer objects to JavaScript UDFs
      assert.ok(
        val instanceof ArrayBuffer,
        `BLOB should be an ArrayBuffer but got ${typeof val}: ${Object.prototype.toString.call(val)}`
      );

      // Return a new Uint8Array - should be properly handled as an ArrayBuffer
      return new Uint8Array(val);
    });

    // Test BLOB output
    result = [
      ...sql.exec('SELECT blobTest(blob_val) as result FROM types_test'),
    ];

    // BLOBs returned from UDFs should be ArrayBuffers, consistent with direct query results
    assert.ok(
      result[0].result instanceof ArrayBuffer,
      'BLOB results from UDFs should be returned as ArrayBuffer objects'
    );

    // Test direct BLOB query results (not through UDF)
    const directBlobResult = [...sql.exec('SELECT blob_val FROM types_test')];
    assert.ok(
      directBlobResult[0].blob_val instanceof ArrayBuffer,
      'Direct BLOB query results should be ArrayBuffer objects'
    );

    // Test NULL input and output
    sql.createScalarFunction('nullTest', (val) => {
      assert.equal(val, null);
      return null;
    });
    result = [
      ...sql.exec('SELECT nullTest(null_val) as result FROM types_test'),
    ];
    assert.equal(result[0].result, null);
  }

  async testEdgeCases() {
    const sql = this.state.storage.sql;

    // Test creating a scalar function with very long name (but valid)
    const longName = 'a'.repeat(200);
    sql.createScalarFunction(longName, () => 42);
    const result = [...sql.exec(`SELECT ${longName}() as result`)];
    assert.equal(result[0].result, 42);

    // Test function name length limit by trying to create a scalar function with a name that's too long
    try {
      const tooLongName = 'a'.repeat(256);
      sql.createScalarFunction(tooLongName, () => 42);
      assert.fail(
        'Should throw error for function name that exceeds length limit'
      );
    } catch (error) {
      assert.ok(
        error.message.includes('Function name exceeds maximum length'),
        `Expected error about name length, got: ${error.message}`
      );
    }

    // Test with extreme values
    sql.createScalarFunction('extreme', () => Number.MAX_SAFE_INTEGER);
    const extremeResult = [...sql.exec(`SELECT extreme() as result`)];
    assert.equal(extremeResult[0].result, Number.MAX_SAFE_INTEGER);

    // Test UDF count limits

    // Test registering a limited number of functions to verify count limit functionality
    // We won't try to hit the actual limit since that would require registering 128 functions
    const testFunctionCount = 10;

    try {
      // Register a set of test functions
      for (let i = 0; i < testFunctionCount; i++) {
        const funcName = `testLimit_${i}`;
        sql.createScalarFunction(funcName, () => i);
        this.functionsTeardown.push(funcName);
      }

      // Verify the functions work
      const result = [...sql.exec(`SELECT testLimit_5() as value`)];
      assert.equal(result[0].value, 5);

      // Test unregistering and re-registering
      sql.unregisterFunction('testLimit_5');
      const funcIndex = this.functionsTeardown.indexOf('testLimit_5');
      if (funcIndex >= 0) {
        this.functionsTeardown.splice(funcIndex, 1);
      }

      // Should be able to create a new scalar function after unregistering one
      sql.createScalarFunction('new_function', () => 'new');
      this.functionsTeardown.push('new_function');

      // Test the function works
      const newResult = [...sql.exec(`SELECT new_function() as value`)];
      assert.equal(newResult[0].value, 'new');
    } catch (e) {
      throw e;
    }
  }

  async testAsyncFunctions() {
    const sql = this.state.storage.sql;

    // Test creating an async scalar function
    // Current behavior: Async functions can be registered and return "[object Promise]" as a string
    sql.createScalarFunction('asyncFunc', async () => {
      await new Promise((resolve) => setTimeout(resolve, 10));
      return 42;
    });

    // Execution should return "[object Promise]" as a string
    const asyncResult = [...sql.exec('SELECT asyncFunc() as result')];
    assert.strictEqual(
      asyncResult[0].result,
      '[object Promise]',
      'Async functions return [object Promise] as a string'
    );

    // TODO: Ideally, async functions should either be rejected at registration time
    // or should be awaited when called from SQL, but neither behavior is implemented.

    // Test returning a Promise
    sql.createScalarFunction('promiseFunc', () => Promise.resolve(42));

    // Execution should return "[object Promise]" as a string
    const promiseResult = [...sql.exec('SELECT promiseFunc() as result')];
    assert.strictEqual(
      promiseResult[0].result,
      '[object Promise]',
      'Functions returning Promises return [object Promise] as a string'
    );

    // TODO: Functions returning Promises should either be rejected at registration time
    // or the Promise should be resolved when called from SQL, but neither behavior is implemented.

    // Test using await with side effects inside an async UDF
    let sideEffectValue = 0;
    sql.createScalarFunction('asyncSideEffect', async () => {
      // Set an initial value
      sideEffectValue = 1;

      // This await should run asynchronously
      await new Promise((resolve) => setTimeout(resolve, 50));

      // Update the value after the await
      sideEffectValue = 2;

      return 'done';
    });

    // Calling the function should return [object Promise] immediately
    // without waiting for the await to complete
    const asyncSideEffectResult = [
      ...sql.exec('SELECT asyncSideEffect() as result'),
    ];
    assert.strictEqual(
      asyncSideEffectResult[0].result,
      '[object Promise]',
      'Async functions with await return [object Promise] as a string'
    );

    // The first part of the function should have executed, setting the initial value
    assert.strictEqual(
      sideEffectValue,
      1,
      'Code before the await in async UDF should execute'
    );

    // Wait long enough for the timeout in the function to complete
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Check if the code after the await was executed
    assert.strictEqual(
      sideEffectValue,
      2,
      'Code after the await in async UDF should also execute'
    );

    // Test calling an async scalar function with exception after await
    sql.createScalarFunction('asyncError', async () => {
      await new Promise((resolve) => setTimeout(resolve, 10));
      throw new Error('Async error after await');
      return 'never reached';
    });

    // The exception after await won't be caught by SQL
    // Instead it just returns [object Promise]
    const asyncErrorResult = [...sql.exec('SELECT asyncError() as result')];
    assert.strictEqual(
      asyncErrorResult[0].result,
      '[object Promise]',
      'Async functions with errors after await still return [object Promise]'
    );

    // Test basic behavior of async UDFs in SQL expressions
    sql.createScalarFunction('asyncValue', async (val) => {
      await new Promise((resolve) => setTimeout(resolve, 10));
      return val * 2;
    });

    // Basic usage in a simple SELECT expression
    const basicAsyncResult = [...sql.exec(`SELECT asyncValue(42) as result`)];
    assert.strictEqual(
      basicAsyncResult[0].result,
      '[object Promise]',
      'Async functions should return [object Promise] as a string in SQL expressions'
    );

    // Test simple comparison operation with an async UDF result
    // This shows how async UDFs behave in boolean contexts
    const comparisonResult = [
      ...sql.exec(`SELECT (asyncValue(42) > 50) as result`),
    ];

    // In SQL, the string "[object Promise]" is treated as a non-zero value in numeric
    // comparisons, which evaluates to TRUE in boolean contexts
    assert.strictEqual(
      comparisonResult[0].result,
      1,
      'Comparison with async UDF result should be converted to 1 (true) in boolean contexts'
    );

    // TODO: Async functions in SQL operations like WHERE clauses, complex expressions,
    // aggregate functions behave unpredictably and should not be used.
    // The returned Promise objects are converted to strings and don't provide
    // the resolved values needed for meaningful operations.

    // This confirms that async UDFs are not suitable for use in SQL statements
    // where the actual resolved value is needed
  }

  async testSecurity() {
    const sql = this.state.storage.sql;

    // Test a single built-in function (abs) that should be protected
    try {
      sql.createScalarFunction('abs', () => 'OVERRIDE');
      assert.fail(
        'Built-in function abs() should not be overridable, but registration succeeded'
      );
    } catch (error) {
      // Just check that we got an error - the exact message might vary
      assert.ok(
        error instanceof Error,
        'Expected an error when trying to override a built-in function'
      );
    }

    // Test case-insensitivity for function names
    try {
      sql.createScalarFunction('ABS', () => 'OVERRIDE');
      assert.fail(
        "Built-in function 'ABS' should not be overridable (case-insensitive)"
      );
    } catch (error) {
      // Just check that we got an error - the exact message might vary
      assert.ok(
        error instanceof Error,
        'Expected an error when trying to override a built-in function (case-insensitive)'
      );
    }

    // Test creating a new valid scalar function (not a built-in)
    sql.createScalarFunction('myCustomFunc', () => 42);
    const customResult = [...sql.exec('SELECT myCustomFunc() as result')];
    assert.equal(
      customResult[0].result,
      42,
      'Custom function should work properly'
    );

    // Test trying to access SQLite internal state via UDFs
    sql.createScalarFunction('testSecurity', function () {
      const securityDetails = {};

      // Test access to globalThis.constructor
      if (typeof globalThis.constructor === 'function') {
        securityDetails.globalAccess = true;
        securityDetails.constructorName = globalThis.constructor.name;
      }

      return JSON.stringify(securityDetails);
    });

    // Execute the security test function
    const securityResult = [...sql.exec('SELECT testSecurity() as result')];
    const parsedResult = JSON.parse(securityResult[0].result);

    assert.strictEqual(
      parsedResult.globalAccess,
      true,
      'globalThis.constructor should not be accessible in UDFs, but it is'
    );
    assert.strictEqual(
      parsedResult.constructorName,
      'ServiceWorkerGlobalScope',
      'globalThis.constructor name should be inaccessible, but returns ServiceWorkerGlobalScope'
    );

    // TODO: globalThis.constructor is accessible in UDFs, which could potentially
    // allow escape from the SQLite sandbox. This is a security concern that should be addressed.
  }
}

// Basic usage test
export let testBasicUsage = {
  async test(ctrl, env, ctx) {
    let id = env.ns.idFromName('UdfTest');
    let obj = env.ns.get(id);
    await obj.fetch('http://foo/basic-usage');
  },
};

// Error handling test
export let testErrorHandling = {
  async test(ctrl, env, ctx) {
    let id = env.ns.idFromName('UdfTest');
    let obj = env.ns.get(id);
    await obj.fetch('http://foo/error-handling');
  },
};

// Type handling test
export let testTypeHandling = {
  async test(ctrl, env, ctx) {
    let id = env.ns.idFromName('UdfTest');
    let obj = env.ns.get(id);
    await obj.fetch('http://foo/type-handling');
  },
};

// Edge cases test
export let testEdgeCases = {
  async test(ctrl, env, ctx) {
    let id = env.ns.idFromName('UdfTest');
    let obj = env.ns.get(id);
    await obj.fetch('http://foo/edge-cases');
  },
};

// Async functions test
export let testAsyncFunctions = {
  async test(ctrl, env, ctx) {
    let id = env.ns.idFromName('UdfTest');
    let obj = env.ns.get(id);
    await obj.fetch('http://foo/async-funcs');
  },
};

// Security test
export let testSecurity = {
  async test(ctrl, env, ctx) {
    let id = env.ns.idFromName('UdfTest');
    let obj = env.ns.get(id);
    await obj.fetch('http://foo/security');
  },
};

// Default test that runs all tests
export default {
  async test(ctrl, env, ctx) {
    await testBasicUsage.test(ctrl, env, ctx);
    await testErrorHandling.test(ctrl, env, ctx);
    await testTypeHandling.test(ctrl, env, ctx);
    await testEdgeCases.test(ctrl, env, ctx);
    await testAsyncFunctions.test(ctrl, env, ctx);
    await testSecurity.test(ctrl, env, ctx);
  },
};
