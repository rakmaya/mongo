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

#include <memory>
#include <shared_mutex>
#include <unordered_set>

#include "mongo/db/exec/timeseries/hcindex/attribute_table.h"
#include "mongo/db/operation_context.h"
#include "mongo/util/uuid.h"

namespace mongo::timeseries::hcindex {

// FORWARD DECLARATIONS
class HCIndexReader;
class TemporalSymbolDictionary;
class TemporalBitmapIndex;

/**
 * Manages temporal attribute tables for a timeseries collection. Creates and
 * maintains separate attribute tables for each time window, allowing for natural
 * schema evolution and bounded memory usage.
 *
 * Key features:
 * - Time-window scoped: Each window has its own attribute table
 * - Configurable period and frequency: Hour/Minute/Second with custom frequencies
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
     * given 'period' and 'frequency'. The symbolDictionary is used to convert metadata values
     * to symbol indices, the reader is used to read existing attribute operations,
     * and the writer is used to write new attribute operations. All parameters must
     * remain valid for the lifetime of this object.
     * Behavior is undefined unless 'collectionUUID', 'symbolDictionary',
     * and 'writer' are valid through the lifetime of this object.
     * The 'writer' can be nullptr if this table is being constructed by a reader
     * (in which case no new operations will be written).
     */
    TemporalAttributeTable(const UUID& collectionUUID,
                           HCIndexPeriodEnum period,
                           int32_t frequency,
                           class TemporalSymbolDictionary* symbolDictionary,
                           class TemporalBitmapIndex* bitmapIndex,
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
     *
     * If bitmapIndex is provided, it will be used to pre-filter candidate rows
     * before performing the full scan in the attribute table.
     */
    std::vector<int64_t> queryRows(const AttributeTablePredicate& predicate,
                                   const Timestamp& timestamp,
                                   BitmapIndex* bitmapIndex = nullptr) const;

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
     * Set Excluded columns
     */
    void setExcludedIndexColumns(std::unordered_set<std::string> excludedColumns);

    /**
     * Set Included columns
     */
    void setIncludedIndexColumns(std::unordered_set<std::string> includedColumns);

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

    std::unordered_set<std::string> excludedIndexColumns;
    std::unordered_set<std::string> includedIndexColumns;

    // Collection UUID for this temporal attribute table
    UUID collectionUUID;

    // Period (hour, minute, second)
    HCIndexPeriodEnum period;

    // Frequency (1-24 for hour, 1-59 for minute/second)
    int32_t frequency;

    // Temporal symbol dictionary for encoding metadata values
    class TemporalSymbolDictionary* temporalSymbolDictionary;

    class TemporalBitmapIndex* temporalBitmapIndex;

    // Writer for writing new attribute operations (can be nullptr if constructed by reader)
    class HCIndexWriter* writer;

    // Reader for reconstructing attribute operations from disk (can be nullptr)
    class HCIndexReader* reader;

    // Synchronization
    mutable std::shared_mutex mutex;
};

}  // namespace mongo::timeseries::hcindex

