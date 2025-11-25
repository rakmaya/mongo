/**
 *    Copyright (C) 2024-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include "mongo/base/string_data.h"
#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/timestamp.h"
#include "mongo/db/operation_context.h"
#include "mongo/util/uuid.h"

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <shared_mutex>

namespace mongo::timeseries::hcindex {

// FORWARD DECLARATIONS
enum class DictionaryGranularity;
class SymbolDictionary;
class HCIndexReader;
class HCIndexWriter;

/**
 * Result of inserting a row into an attribute table. Contains the row ID and
 * a flag indicating whether this is a new row (true) or a duplicate (false).
 */
struct InsertRowResult {
    int64_t rowId;
    bool isNewRow;
};

/**
 * State machine for AttributeTable lifecycle:
 * - NOP: Initial state, no operations allowed
 * - Reconstruction: Table is being reconstructed from stored operations
 * - ReadWrite: Table is in normal write mode (new rows can be added)
 * - ReadOnly: Table is locked, no modifications allowed
 *
 * State transitions:
 * - NOP -> Reconstruction (via changeState)
 * - NOP -> ReadWrite (via changeState)
 * - Reconstruction -> ReadWrite (via changeState) - allows accepting new data after reconstruction
 * - Reconstruction -> ReadOnly (via changeState)
 * - ReadWrite -> ReadOnly (via changeState)
 * - ReadOnly -> (no transitions allowed)
 */
enum class AttributeTableState {
    NOP,
    Reconstruction,
    ReadWrite,
    ReadOnly
};

/**
 * Represents a query predicate for filtering rows in an attribute table. The
 * predicate can be specified either by column index (for internal use) or by
 * field name (for external use). A row matches the predicate if all specified
 * columns have the matching symbol indices.
 */
struct AttributeTablePredicate {
    // Map: column index -> symbol index to match
    // If a column is not in this map, it is not part of the predicate
    std::map<size_t, uint32_t> columnMatches;

    // Map: field name -> symbol index to match
    // Used when constructing predicates from metadata
    std::map<std::string, uint32_t> fieldMatches;
};

/**
 * Represents a single attribute table for a specific time window. An attribute
 * table is an evolving columnar structure that stores metadata as symbol indices.
 *
 * Key properties:
 * - Append-only: Rows are never modified after insertion
 * - Schema evolution: New columns can be added dynamically
 * - Missing values: 0 is reserved to denote missing values in cells
 * - Row IDs: Stable identifiers for rows. IDs are not reused
 * - Thread-safe: Uses shared_mutex for concurrent access
 * - Dictionary-backed: Uses a SymbolDictionary to convert metadata values to indices
 */
class AttributeTable {
public:
    /**
     * Construct an AttributeTable that uses the specified symbolDictionary to
     * convert metadata values to symbol indices. The symbolDictionary and writer
     * must remain valid for the lifetime of this AttributeTable.
     * The table starts in NOP state. Use changeState() to transition to
     * Reconstruction, ReadWrite, or ReadOnly states.
     */
    AttributeTable(SymbolDictionary* symbolDictionary,
                   HCIndexWriter* writer,
                   const Timestamp& windowStart = Timestamp(),
                   const Timestamp& windowEnd = Timestamp());

    /**
     * Insert a new row into the attribute table with the specified metadata and
     * return a stable row ID along with a flag indicating if it's a new row.
     * The metadata parameter is a BSON object containing field names and their
     * corresponding string values. The method looks up each value in the symbol
     * dictionary to get its index, automatically evolves the schema if new fields
     * appear in the metadata, and returns the existing row ID if a row with the
     * exact same metadata already exists (with isNewRow=false). Returns an error
     * if the row cannot be inserted or if any value lookup fails.
     */
    StatusWith<InsertRowResult> insertRow(const BSONObj& metadata);

    /**
     * Insert a new row with the specified symbol indices and return a stable row
     * ID. The row parameter contains symbol indices for each column in the current
     * schema. If the row has fewer elements than the current schema, missing
     * columns are filled with 0 (missing value indicator). This is a lower-level
     * method primarily used internally. Returns an error if the row cannot be
     * inserted.
     */
    StatusWith<int64_t> insertRowDirect(const std::vector<uint32_t>& row);

    /**
     * Retrieve the row with the specified rowId from the attribute table. Returns
     * the vector of symbol indices for that row if found, or boost::none if the
     * rowId does not exist.
     */
    boost::optional<std::vector<uint32_t>> getRow(int64_t rowId) const;

    /**
     * Query the attribute table and return a vector of row IDs that match the
     * specified predicate. The predicate specifies which columns to match and
     * what symbol indices they should contain. A row matches if all specified
     * columns have the matching indices.
     */
    std::vector<int64_t> queryRows(const AttributeTablePredicate& predicate) const;

    /**
     * Add a new column to the schema with the specified fieldName. All existing
     * rows are extended with 0 (missing value indicator) for the new column.
     * Returns an error if the column already exists or if the column cannot be
     * added.
     */
    Status addColumn(StringData fieldName);

    /**
     * Change the state of this table. Transitions are restricted:
     * - From NOP: can transition to Reconstruction or ReadWrite
     * - From Reconstruction: can transition to ReadWrite (to accept new data) or ReadOnly
     * - From ReadWrite: can transition to ReadOnly
     * - From ReadOnly: no transitions allowed
     * Returns an error if the transition is invalid.
     */
    Status changeState(AttributeTableState newState);

    /**
     * Set the writer for this table. This allows a table to be updated later.
     */
    void setWriter(HCIndexWriter* w) {
        writer = w;
    }

    /**
     * Flush any pending operations to the database via the writer.
     */
    void flush();

    /**
     * Return the current schema as a vector of column names in order.
     */
    const std::vector<std::string>& getSchema() const;

    /**
     * Return the total number of rows in this attribute table.
     */
    size_t getRowCount() const;

    /**
     * Return the memory usage of this attribute table in bytes.
     */
    size_t getMemoryUsageBytes() const;

private:
    /**
     * Internal method to convert metadata BSON object to a row vector. For each
     * field in the metadata, looks up the string value in the symbol dictionary to
     * get its index. Evolves the schema if new fields appear. Returns the row vector
     * with symbol indices in schema order, with 0 for missing fields. Returns an
     * error if any value lookup fails.
     */
    StatusWith<std::vector<uint32_t>> metadataToRow(const BSONObj& metadata);

    /**
     * Internal method to check if a row with the same symbol indices already
     * exists. Returns the row ID if found, or boost::none if not found.
     */
    boost::optional<int64_t> findDuplicateRow(const std::vector<uint32_t>& row) const;

    /**
     * Internal method to write a schema field to the writer.
     */
    Status writeSchema(const std::string& fieldName, size_t columnIndex);

    /**
     * Internal method to write a row to the writer.
     */
    Status writeRow(const std::vector<uint32_t>& row);

    // Symbol dictionary for converting metadata values to indices
    // Must remain valid for the lifetime of this AttributeTable
    SymbolDictionary* symbolDictionary;

    // Writer for writing new attribute operations
    // Must remain valid for the lifetime of this AttributeTable
    HCIndexWriter* writer;

    // Current state of the table
    AttributeTableState _state = AttributeTableState::NOP;

    // Whether there are pending changes to flush
    bool _isDirty = false;

    // Window boundaries for this attribute table
    Timestamp _windowStart;
    Timestamp _windowEnd;

    // Rows stored as vectors of symbol indices
    // Each row has the same number of columns as the schema
    std::vector<std::vector<uint32_t>> rows;

    // Schema: column names in order
    std::vector<std::string> schema;

    // Map: field name -> column index (for fast lookups)
    std::map<std::string, size_t> fieldToColumnIndex;

    // Next row ID to assign (starts at 0)
    int64_t nextRowId = 0;

    // Synchronization
    mutable std::shared_mutex mutex;
};

/**
 * Manages temporal attribute tables for a timeseries collection. Creates and
 * maintains separate attribute tables for each time window, allowing for natural
 * schema evolution and bounded memory usage.
 *
 * Key features:
 * - Time-window scoped: Each window has its own attribute table
 * - Configurable granularity: DAILY, HOURLY, THIRTY_MIN, TEN_MIN, FIVE_MIN
 * - Automatic window management: Creates tables on-demand
 * - Cleanup support: Can remove old tables to free memory
 * - Thread-safe: Safe for concurrent access from multiple threads
 * - Dictionary-backed: Uses a TemporalSymbolDictionary for value-to-index conversion
 */
class TemporalAttributeTable {
public:
    /**
     * Create a new temporal attribute table manager for managing attribute tables
     * for timeseries collections having the specified 'collectionUUID' with the
     * given 'granularity'. The symbolDictionary is used to convert metadata values
     * to symbol indices, the reader is used to read existing attribute operations,
     * and the writer is used to write new attribute operations. All parameters must
     * remain valid for the lifetime of this object.
     * Behavior is undefined unless 'collectionUUID', 'symbolDictionary',
     * and 'writer' are valid through the lifetime of this object.
     * The 'writer' can be nullptr if this table is being constructed by a reader
     * (in which case no new operations will be written).
     */
    TemporalAttributeTable(const UUID& collectionUUID,
                           DictionaryGranularity granularity,
                           class TemporalSymbolDictionary* symbolDictionary,
                           class HCIndexWriter* writer,
                           class HCIndexReader* reader = nullptr);

    /**
     * Returns a pointer to the attribute table covering the time window that
     * includes the specified 'timestamp' if found. Otherwise, create a new
     * table for the time window covering the 'timestamp' and return a pointer
     * to that table. Note that the returned pointer is valid for the lifetime
     * of this TemporalAttributeTable. Returns an error if the table for the
     * time window covering the 'timestamp' cannot be created. opCtx is required
     * for reconstruction from disk if the table is not in memory.
     */
    StatusWith<AttributeTable*> getOrCreateTableForTimestamp(OperationContext* opCtx,
                                                             const Timestamp& timestamp);

    /**
     * Returns a pointer to the attribute table covering the time window that
     * includes the specified 'timestamp' if found. Otherwise, return an error.
     * Note that the returned pointer is valid for the lifetime of this
     * TemporalAttributeTable. This method does not attempt reconstruction from disk.
     */
    StatusWith<AttributeTable*> getTableForTimestamp(const Timestamp& timestamp) const;

    /**
     * Check if an attribute table exists for the time window that includes
     * the specified 'timestamp'.
     */
    bool tableExists(const Timestamp& timestamp) const;

    /**
     * Create or reconstruct an attribute table for the time window that includes
     * the specified 'timestamp'. If the table already exists in memory, return it.
     * Otherwise, try to reconstruct it from disk using the reader if available.
     * Returns an error if the table cannot be created or reconstructed.
     */
    StatusWith<AttributeTable*> createTableForTimestamp(const Timestamp& timestamp);

    /**
     * Insert a new row with the specified metadata into the attribute table for
     * the time window that includes the specified 'timestamp' and return a stable
     * row ID along with a flag indicating if it's a new row. The metadata parameter
     * is a BSON object containing field names and their corresponding string values.
     * The method looks up each value in the symbol dictionary to get its index,
     * automatically evolves the schema if new fields appear in the metadata, and
     * returns the existing row ID if a row with the exact same metadata already
     * exists (with isNewRow=false). Returns an error if the row cannot be inserted
     * or if any value lookup fails. opCtx is required for reconstruction from disk
     * if the table is not in memory.
     */
    StatusWith<InsertRowResult> insertRow(OperationContext* opCtx,
                                          const BSONObj& metadata,
                                          const Timestamp& timestamp);

    /**
     * Insert a new row with the specified symbol indices into the attribute
     * table for the time window that includes the specified 'timestamp' and
     * return a stable row ID. This is a lower-level method primarily used
     * internally. Returns an error if the row cannot be inserted. opCtx is
     * required for reconstruction from disk if the table is not in memory.
     */
    StatusWith<int64_t> insertRowDirect(OperationContext* opCtx,
                                        const std::vector<uint32_t>& row,
                                        const Timestamp& timestamp);

    /**
     * Retrieve the row with the specified rowId from the attribute table for
     * the time window that includes the specified 'timestamp'. Returns the
     * vector of symbol indices for that row if found, or boost::none if the
     * rowId does not exist.
     */
    boost::optional<std::vector<uint32_t>> getRow(int64_t rowId,
                                                   const Timestamp& timestamp) const;

    /**
     * Query the attribute table for the time window that includes the specified
     * 'timestamp' and return a vector of row IDs that match the specified
     * predicate. The predicate specifies which columns to match and what symbol
     * indices they should contain.
     */
    std::vector<int64_t> queryRows(const AttributeTablePredicate& predicate,
                                   const Timestamp& timestamp) const;

    /**
     * Return the time window boundaries for the specified 'timestamp'.
     */
    std::pair<Timestamp, Timestamp> getWindowForTimestamp(const Timestamp& timestamp) const;

    /**
     * Remove attribute tables serving time windows older than the specified
     * 'beforeTimestamp'. This is used for cleanup to free memory from old
     * tables.
     */
    Status cleanupOldTables(const Timestamp& beforeTimestamp);

    /**
     * Flush all pending operations to the database.
     */
    void flush();

    /**
     * Statistics about this temporal attribute table.
     * TODO: Find out how stats is done in mongodb! For now this is for
     * debug/testing purpose.
     */
    struct Stats {
        size_t totalTables;
        size_t totalRows;
        size_t memoryUsageBytes;
    };

    /**
     * Return the usage statistics.
     */
    Stats getStats() const;

private:
    /**
     * Create or fetch the attribute table for the time window that starts at
     * the specified 'windowStart' timestamp. opCtx is required for reconstruction
     * from disk if the table is not in memory.
     */
    StatusWith<AttributeTable*> getOrCreateTable(OperationContext* opCtx,
                                                 const Timestamp& windowStart);

    /**
     * Return the window start timestamp for the time window that includes the
     * specified 'timestamp'.
     */
    Timestamp calculateWindowStart(const Timestamp& timestamp) const;

    /**
     * Return the window end timestamp for the time window that starts at the
     * specified 'windowStart' timestamp.
     */
    Timestamp calculateWindowEnd(const Timestamp& windowStart) const;

    // Map: windowStart -> AttributeTable
    // We want to clean up older tables.
    // TODO: In future, we can create a projection of this map to an LRU
    // iterator to eject unused tables.
    std::map<Timestamp, std::unique_ptr<AttributeTable>> tables;

    // Collection UUID for this temporal attribute table
    UUID collectionUUID;

    // Granularity level
    DictionaryGranularity granularity;

    // Temporal symbol dictionary for encoding metadata values
    class TemporalSymbolDictionary* temporalSymbolDictionary;

    // Writer for writing new attribute operations (can be nullptr if constructed by reader)
    class HCIndexWriter* writer;

    // Reader for reconstructing attribute operations from disk (can be nullptr)
    class HCIndexReader* reader;

    // Synchronization
    mutable std::shared_mutex mutex;
};

}  // namespace mongo::timeseries::hcindex

