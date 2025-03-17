// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

/**
 * SuperSql is a higher-level wrapper for the workerd SQL API.
 * It provides a more developer-friendly interface for working with SQL hooks
 * and implements common patterns for working with the SQL API.
 */
export class SuperSql {
  /**
   * Create a new SuperSql instance
   * @param {Object} sql - The raw sql object from state.storage.sql
   */
  constructor(sql) {
    this._sql = sql;
    this._hookRegistry = {
      INSERT: new Map(),
      UPDATE: new Map(),
      DELETE: new Map(),
      '*': new Map()
    };
    this._hookInstalled = false;
    this._pendingEvents = [];
    this._columnCache = new Map(); // Cache for column names by table
  }

  /**
   * Get the column names for a table
   * @param {string} tableName - The name of the table
   * @returns {Array<string>} - Array of column names
   * @private
   */
  _getColumnNames(tableName) {
    // Check the cache first
    if (this._columnCache.has(tableName)) {
      return this._columnCache.get(tableName);
    }

    try {
      const tableInfo = this._sql.exec(`PRAGMA table_info(${tableName})`).toArray();
      const columns = tableInfo.map(col => col.name);

      // Cache the result
      this._columnCache.set(tableName, columns);

      return columns;
    } catch (e) {
      return [];
    }
  }

  /**
   * Convert a row array to an object with column names as keys
   * @param {Array} row - Row data array
   * @param {Array<string>} columnNames - Array of column names
   * @returns {Object} - Row as an object
   * @private
   */
  _rowArrayToObject(row, columnNames) {
    if (!row) return null;
    const obj = {};
    for (let i = 0; i < columnNames.length && i < row.length; i++) {
      obj[columnNames[i]] = row[i];
    }
    return obj;
  }

  /**
   * Set up the update hook
   * @private
   */
  _installHook() {
    if (this._hookInstalled) return;

    this._sql.setUpdateHook((rowid, tableName, operation, values) => {
      // Just queue the event - no SQL operations in the hook!
      this._pendingEvents.push({
        rowid,
        tableName,
        operation,
        values: {
          old: values.old ? [...values.old] : null,
          new: values.new ? [...values.new] : null
        }
      });
    });

    this._hookInstalled = true;
  }

  /**
   * Process the pending events queue
   * @private
   */
  _processPendingEvents() {
    if (this._pendingEvents.length === 0) return;

    // Group events by table and operation for batching
    const groupedEvents = new Map();

    // Process all pending events
    for (const event of this._pendingEvents) {
      const { tableName, operation, values } = event;

      // Create a key for this table/operation combination
      const key = `${operation}:${tableName}`;

      if (!groupedEvents.has(key)) {
        groupedEvents.set(key, {
          operation,
          tableName,
          oldRows: [],
          newRows: []
        });
      }

      const group = groupedEvents.get(key);

      // Add the row data
      if (values.old) {
        group.oldRows.push(values.old);
      }

      if (values.new) {
        group.newRows.push(values.new);
      }
    }

    // Get column names for each table (now safe to do outside hook)
    for (const [key, group] of groupedEvents.entries()) {
      const { tableName, operation } = group;
      const columnNames = this._getColumnNames(tableName);

      // Convert row arrays to objects with column names
      const oldRowObjects = group.oldRows.map(row => this._rowArrayToObject(row, columnNames));
      const newRowObjects = group.newRows.map(row => this._rowArrayToObject(row, columnNames));

      // Get hooks for exact match
      const exactHooks = this._getHooks(operation, tableName);

      // Get hooks for wildcards
      const tableWildcardHooks = this._getHooks(operation, '*');
      const operationWildcardHooks = this._getHooks('*', tableName);
      const allWildcardHooks = this._getHooks('*', '*');

      // Combine all matching hooks
      const allHooks = [
        ...exactHooks,
        ...tableWildcardHooks,
        ...operationWildcardHooks,
        ...allWildcardHooks
      ];

      // Call each hook with the batched data
      for (const hook of allHooks) {
        try {
          hook(operation, tableName, oldRowObjects, newRowObjects);
        } catch (error) {
          console.error(`Error in SQL hook for ${operation} on ${tableName}:`, error);
        }
      }
    }

    // Clear the pending events
    this._pendingEvents = [];
  }

  /**
   * Get hooks for a specific operation and table
   * @param {string} operation - The operation type
   * @param {string} tableName - The table name
   * @returns {Array<Function>} - Array of matching hooks
   * @private
   */
  _getHooks(operation, tableName) {
    if (!this._hookRegistry[operation]) return [];
    const hooks = this._hookRegistry[operation].get(tableName);
    return hooks || [];
  }

  /**
   * Register a hook for a specific operation and table
   * @param {string} operation - The operation type (INSERT, UPDATE, DELETE)
   * @param {string} tableName - The table name or wildcard pattern
   * @param {Function} callback - Function to call when the hook is triggered
   * @returns {SuperSql} - Returns this for chaining
   */
  on(operation, tableName, callback) {
    // Normalize operation to uppercase
    operation = operation.toUpperCase();

    // Make sure the hook registry is properly initialized
    if (!this._hookRegistry[operation]) {
      this._hookRegistry[operation] = new Map();
    }

    // Register the hook for the specified operation and table
    if (!this._hookRegistry[operation].has(tableName)) {
      this._hookRegistry[operation].set(tableName, []);
    }

    this._hookRegistry[operation].get(tableName).push(callback);

    // Install the base hook if needed
    this._installHook();

    return this;
  }

  /**
   * Execute a SQL query with parameters
   * @param {string} query - The SQL query to execute
   * @param {...any} params - Query parameters
   * @returns {Object} - Result with any cursor data and changes
   */
  exec(query, ...params) {
    try {
      // Clear pending events before execution
      this._pendingEvents = [];

      // Execute the SQL query
      const result = this._sql.exec(query, ...params);

      // Process any events that were queued during execution
      this._processPendingEvents();

      // Add changes info to the result for the test API
      result.changes = {};

      // Group events by table and operation
      const eventsByTable = new Map();

      for (const event of this._pendingEvents) {
        const { tableName, operation, values } = event;

        if (!eventsByTable.has(tableName)) {
          eventsByTable.set(tableName, {
            operation,
            oldRows: [],
            newRows: []
          });
        }

        const group = eventsByTable.get(tableName);

        if (values.old) {
          group.oldRows.push(values.old);
        }

        if (values.new) {
          group.newRows.push(values.new);
        }
      }

      // Add information about changes to the result
      for (const [tableName, group] of eventsByTable.entries()) {
        const columnNames = this._getColumnNames(tableName);

        result.changes[tableName] = {
          operation: group.operation,
          oldRows: group.oldRows.map(row => this._rowArrayToObject(row, columnNames)),
          newRows: group.newRows.map(row => this._rowArrayToObject(row, columnNames))
        };
      }

      return result;
    } catch (error) {
      // Clear pending events on error
      this._pendingEvents = [];
      throw error;
    }
  }

  /**
   * Prepare a SQL statement
   * @param {string} query - The SQL query to prepare
   * @returns {Function} - Prepared statement function
   */
  prepare(query) {
    const preparedStmt = this._sql.prepare(query);

    // Return a wrapper function
    return (...params) => {
      try {
        // Clear pending events before execution
        this._pendingEvents = [];

        // Execute the prepared statement
        const result = preparedStmt(...params);

        // Process any events that were queued during execution
        this._processPendingEvents();

        return result;
      } catch (error) {
        // Clear pending events on error
        this._pendingEvents = [];
        throw error;
      }
    };
  }

  /**
   * Clear all registered hooks
   * @returns {SuperSql} - Returns this for chaining
   */
  clearHooks() {
    // Reset all hook registries
    this._hookRegistry = {
      INSERT: new Map(),
      UPDATE: new Map(),
      DELETE: new Map(),
      '*': new Map()
    };

    // Clear the SQL update hook
    if (this._hookInstalled) {
      this._sql.clearUpdateHook();
      this._hookInstalled = false;
    }

    // Clear any pending events
    this._pendingEvents = [];

    return this;
  }

  /**
   * Parse SQL and ingest statements
   * @param {string} sql - SQL statements to ingest
   * @returns {Object} - Result of ingest operation
   */
  ingest(sql) {
    try {
      // Clear pending events before execution
      this._pendingEvents = [];

      // Execute the ingest
      const result = this._sql.ingest(sql);

      // Process any events that were queued during execution
      this._processPendingEvents();

      return result;
    } catch (error) {
      // Clear pending events on error
      this._pendingEvents = [];
      throw error;
    }
  }

  /**
   * Get the database size
   * @returns {number} - Database size
   */
  get databaseSize() {
    return this._sql.databaseSize;
  }

  /**
   * Get the underlying SQL object
   * @returns {Object} - The raw SQL object
   */
  get raw() {
    return this._sql;
  }
}
