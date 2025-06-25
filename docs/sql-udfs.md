# SQL User-Defined Functions (UDFs) in Workers

Workers D1 and Durable Objects allow you to define custom SQL functions using JavaScript. This feature enables you to extend SQL's capabilities with your own functions, making complex operations more efficient and expressive.

## Overview

SQL User-Defined Functions (UDFs) let you create custom functions in JavaScript that can be called from SQL queries. These functions can:

- Process data in ways not supported by built-in SQL functions
- Implement complex business logic directly in SQL queries
- Reduce the need for post-processing query results in JavaScript
- Improve performance by minimizing data transfer between SQL and JavaScript

## Basic Usage

To create a UDF, use the `createScalarFunction` method on the SQL interface:

```javascript
// In a Durable Object
storage.sql.createScalarFunction('myFunction', (arg1, arg2) => {
  // Your JavaScript logic here
  return result;
});
```

Once defined, you can use your function in SQL queries:

```javascript
const results = [...storage.sql.exec('SELECT myFunction(column1, column2) FROM my_table')];
```

## Supported Types

UDFs support the following types for arguments and return values:

| SQL Type | JavaScript Type      |
|----------|----------------------|
| INTEGER  | Number (Integer)     |
| REAL     | Number (Float)       |
| TEXT     | String               |
| BLOB     | ArrayBuffer/Uint8Array |
| NULL     | null/undefined       |

Type conversion happens automatically in both directions:

- SQL to JavaScript: Values are automatically converted to the appropriate JavaScript types
- JavaScript to SQL: Return values are converted to the closest SQL type

## Example

```javascript
// Create a function to calculate distance between two points
storage.sql.createScalarFunction('distance', (x1, y1, x2, y2) => {
  return Math.sqrt(Math.pow(x2 - x1, 2) + Math.pow(y2 - y1, 2));
});

// Use it in a query
const results = [...storage.sql.exec(`
  SELECT 
    id, 
    name, 
    distance(latitude, longitude, 34.052235, -118.243683) AS distance_to_la 
  FROM 
    locations
  ORDER BY 
    distance_to_la
  LIMIT 10
`)];
```

## Limitations and Constraints

### Synchronous Execution

UDFs must be synchronous. Async functions and functions that return Promises are explicitly rejected:

```javascript
// ❌ This will fail - async functions are not supported
storage.sql.createScalarFunction('asyncFunc', async (value) => {
  const result = await computeExpensiveValue(value);
  return result;
});

// ❌ This will also fail - functions that return Promises
storage.sql.createScalarFunction('promiseFunc', (value) => {
  return Promise.resolve(value * 2);
});
```

### Resource Limits

UDFs are subject to the same CPU time and memory limits as the rest of your Worker code. Long-running or memory-intensive functions may be terminated if they exceed these limits.

### Built-in Function Protection

You cannot override built-in SQLite functions. Attempts to register a UDF with the same name as a built-in function will fail:

```javascript
// ❌ This will fail - 'abs' is a built-in SQLite function
storage.sql.createScalarFunction('abs', (value) => {
  return Math.abs(value);
});
```

### Maximum Number of UDFs

There is a limit of 128 UDFs that can be registered per SQL instance.

### Function Name Length

Function names must be 255 bytes or less.

## Error Handling

Errors thrown in UDFs are propagated back to the SQL query and will cause the query to fail:

```javascript
// Define a function that might throw errors
storage.sql.createScalarFunction('validateEmail', (email) => {
  if (!email || !email.includes('@')) {
    throw new Error('Invalid email format');
  }
  return email;
});

// Using the function - will throw an error if any email is invalid
try {
  const results = [...storage.sql.exec('SELECT validateEmail(email) FROM users')];
} catch (e) {
  console.error('Email validation failed:', e);
}
```

## Best Practices

### Keep Functions Simple

UDFs should be small, focused functions that perform specific tasks. Complex logic is better handled outside SQL queries.

### Minimize Side Effects

UDFs should avoid side effects like modifying global state or making external calls. Pure functions that only depend on their inputs are ideal.

### Use for Computational Tasks

UDFs are best for computational tasks that can be expressed concisely in JavaScript.

### Consider Performance

UDFs add overhead compared to built-in SQL functions. For performance-critical queries, benchmark to ensure UDFs don't create bottlenecks.

### Unregister When Done

If you no longer need a UDF, unregister it to free up resources:

```javascript
storage.sql.unregisterFunction('myFunction');
```

## Advanced Usage

### Register Functions During Initialization

Register frequently-used UDFs during Durable Object initialization:

```javascript
export class MyDurableObject {
  constructor(state, env) {
    this.state = state;
    
    // Register UDFs during initialization
    this.registerCustomFunctions();
  }
  
  registerCustomFunctions() {
    const sql = this.state.storage.sql;
    
    sql.createScalarFunction('formatCurrency', (amount, currency = 'USD') => {
      return new Intl.NumberFormat('en-US', { 
        style: 'currency', 
        currency 
      }).format(amount);
    });
    
    sql.createScalarFunction('calculateTax', (amount, rate = 0.0725) => {
      return amount * rate;
    });
  }
}
```

### Type Coercion

Be aware of type coercion between SQL and JavaScript:

```javascript
// Example of type handling
storage.sql.createScalarFunction('typeInfo', (value) => {
  return `Type: ${typeof value}, Value: ${value}`;
});

// Results will demonstrate the automatic type conversion
const results = [...storage.sql.exec(`
  SELECT 
    typeInfo(42) as int_type,
    typeInfo(3.14) as float_type,
    typeInfo('hello') as string_type,
    typeInfo(NULL) as null_type,
    typeInfo(x'DEADBEEF') as blob_type
`)];
```

## Security Considerations

### SQL Injection

While UDFs themselves don't introduce SQL injection vulnerabilities, be careful when constructing SQL queries that use UDFs with dynamic input:

```javascript
// ❌ Vulnerable to SQL injection
const userInput = request.params.search;
storage.sql.createScalarFunction('highlight', (text) => `<mark>${text}</mark>`);
const results = [...storage.sql.exec(`SELECT highlight(${userInput}) FROM documents`)];

// ✅ Safe - use parameterized queries
const results = [...storage.sql.exec(`SELECT highlight(?) FROM documents`, userInput)];
```

### Data Access

UDFs have access to all data passed to them. Ensure sensitive data isn't leaked through UDF operations.

## Debugging Tips

### Test UDFs Independently

Test your UDFs independently before using them in complex queries:

```javascript
// Define the function
storage.sql.createScalarFunction('complexCalculation', (a, b, c) => {
  // Complex logic here
  return result;
});

// Test with simple, predictable inputs
const testResult = [...storage.sql.exec(`
  SELECT 
    complexCalculation(1, 2, 3) as test1,
    complexCalculation(0, 0, 0) as test2,
    complexCalculation(-1, -2, -3) as test3
`)];
console.log('Test results:', testResult);
```

### Logging in UDFs

You can use `console.log` within UDFs for debugging:

```javascript
storage.sql.createScalarFunction('debugFunc', (value) => {
  console.log('debugFunc called with:', value);
  const result = value * 2;
  console.log('debugFunc returning:', result);
  return result;
});
```

## Experimental Status

SQL UDFs are currently an experimental feature and require the "experimental" flag to be enabled:

```javascript
// Worker with experimental flag enabled
export default {
  fetch(request, env, ctx) {
    // UDFs are available here
  }
};
```

The API may change before becoming generally available.

## Conclusion

SQL User-Defined Functions provide a powerful way to extend SQL capabilities with JavaScript in your Workers applications. They bridge the gap between the database and application logic, enabling more expressive and efficient queries.