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

#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/bson/timestamp.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/timeseries/timeseries_gen.h"
#include "mongo/util/uuid.h"

#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

namespace mongo::timeseries::hcindex {

// Forward declarations
class HCIndexReader;
class HCIndexWriter;
class AttributeTable;

/**
 * State machine for BitmapIndex lifecycle:
 * - NOP: Initial state, no operations allowed
 * - Reconstruction: Index is being reconstructed from stored operations
 * - ReadWrite: Index is in normal write mode (new entries can be added)
 * - ReadOnly: Index is locked, no modifications allowed
 *
 * State transitions:
 * - NOP -> Reconstruction (via changeState)
 * - NOP -> ReadWrite (via changeState)
 * - Reconstruction -> ReadOnly (via changeState)
 * - ReadWrite -> ReadOnly (via changeState)
 * - ReadOnly -> (no transitions allowed)
 */
enum class BitmapIndexState {
    NOP,
    Reconstruction,
    ReadWrite,
    ReadOnly
};

/**
 * Represents a bitmap index for a specific time window.
 *
 * The bitmap index maps (columnIndex, symbolIndex) -> set of RowIDs.
 * This enables fast lookup of all rows that have a specific value in a specific column.
 *
 * Key properties:
 * - Append-only: Entries are only added, never removed (except via window eviction)
 * - Thread-safe: Uses shared_mutex for concurrent access
 * - Column-value indexed: Each unique (column, value) pair has its own bitmap
 *
 * Implementation note: For Phase 1 MVP, we use std::set<int64_t> as a simple
 * bitmap representation. This can be optimized with Roaring Bitmaps in Phase 2.
 */
class BitmapIndex {
public:
    /**
     * Create a new bitmap index for the specified time window.
     * The index starts in NOP state. Use changeState() to transition to
     * Reconstruction, ReadWrite, or ReadOnly states.
     */
    BitmapIndex(HCIndexPeriodEnum period,
                int32_t frequency,
                Timestamp windowStart,
                Timestamp windowEnd,
                HCIndexWriter* writer = nullptr);

    /**
     * Add a rowId to the bitmap for the specified (columnIndex, symbolIndex) pair.
     * If the bitmap doesn't exist, it is created.
     * Returns an error if the index is not in ReadWrite or Reconstruction state.
     */
    Status addEntry(size_t columnIndex, uint32_t symbolIndex, int64_t rowId);

    /**
     * Add all entries for a row. For each column in the row vector, if the
     * symbol index is non-zero (not missing), add the rowId to that (column, value) bitmap.
     * Returns an error if the index is not in ReadWrite or Reconstruction state.
     */
    Status addRow(int64_t rowId, const std::vector<uint32_t>& row);

    /**
     * Get all rowIds that have the specified symbolIndex in the specified column.
     * Returns an empty set if no matching entries exist.
     */
    std::set<int64_t> getRowIds(size_t columnIndex, uint32_t symbolIndex) const;

    /**
     * Get all rowIds that match ALL of the specified (columnIndex, symbolIndex) pairs.
     * This performs an intersection of all matching bitmaps.
     * The predicate is a vector where predicate[columnIndex] = symbolIndex to match.
     * A symbolIndex of 0 means "don't care" (skip this column in the intersection).
     * Returns an empty set if any required bitmap doesn't exist.
     */
    std::set<int64_t> queryRowIdsAnd(const std::vector<uint32_t>& predicate) const;

    /**
     * Get all rowIds that match ANY of the specified predicates.
     * This performs a union of the AND results for each predicate.
     * Each predicate is a vector where predicate[columnIndex] = symbolIndex to match.
     */
    std::set<int64_t> queryRowIdsOr(const std::vector<std::vector<uint32_t>>& predicates) const;

    /**
     * Change the state of this bitmap index. Transitions are restricted:
     * - From NOP: can transition to Reconstruction or ReadWrite
     * - From Reconstruction: can transition to ReadOnly
     * - From ReadWrite: can transition to ReadOnly
     * - From ReadOnly: no transitions allowed
     * Returns an error if the transition is invalid.
     */
    Status changeState(BitmapIndexState newState);

    /**
     * Get the current state of this bitmap index.
     */
    BitmapIndexState getState() const;

    /**
     * Set the writer for this index.
     */
    void setWriter(HCIndexWriter* writer) {
        _writer = writer;
    }

    /**
     * Returns true if there is an index for column at the specified
     * columnIndex. Otherwise, return false.
     */
    bool hasIndexForColumn(size_t columnIndex) const;

    /**
     * Set Excluded columns
     */
    void setExcludedColumns(std::unordered_set<std::size_t> excludedColumns);

    /**
     * Set Included columns
     */
    void setIncludedColumns(std::unordered_set<std::size_t> includedColumns);

    /**
     * Flush any pending operations to the database via the writer.
     */
    void flush();

    /**
     * Return true if the bitmap is empty. Otherwise, return false.
     */
    bool isEmpty() const;

    /**
     * Return the total number of unique (column, value) pairs indexed.
     */
    size_t getEntryCount() const;

    /**
     * Return the total number of rowId entries across all bitmaps.
     */
    size_t getTotalRowIdCount() const;

    /**
     * Return the memory usage of this bitmap index in bytes (approximate).
     */
    size_t getMemoryUsageBytes() const;

    /**
     * Get the window start timestamp.
     */
    Timestamp getWindowStart() const {
        return _windowStart;
    }

    /**
     * Get the window end timestamp.
     */
    Timestamp getWindowEnd() const {
        return _windowEnd;
    }

private:

    Status addEntryHelper(size_t columnIndex, uint32_t symbolIndex, int64_t rowId);

    // Two-level bitmap structure:
    // First level: columnIndex -> inner map
    // Second level: symbolIndex -> set of RowIDs
    // This structure enables efficient column-based scans and eliminates the need
    // for a separate _indexedColumns set.
    // Using std::set for Phase 1 MVP. Can be replaced with Roaring Bitmap later.
    using SymbolBitmaps = std::unordered_map<uint32_t, std::set<int64_t>>;
    std::unordered_map<size_t, SymbolBitmaps> _bitmaps;

    // Period and frequency for time window calculation
    HCIndexPeriodEnum _period;
    int32_t _frequency;

    // Range that we cover
    Timestamp _windowStart;
    Timestamp _windowEnd;

    // Writer (can be nullptr if not persisting)
    HCIndexWriter* _writer = nullptr;

    // Current state of the index
    BitmapIndexState _state = BitmapIndexState::NOP;

    // Whether there are any pending operations
    bool _isDirty = false;

    // Columns that are excluded, even if information-gain says otherwise.
    std::unordered_set<std::size_t> _excludedColumns;

    // Columns that are allowed to have indexes.
    std::unordered_set<std::size_t> _indexedColumns;

    // Synchronization
    mutable std::shared_mutex _mutex;
};

/**
 * Manages temporal bitmap indexes for a timeseries collection.
 *
 * Creates and maintains separate bitmap indexes for each time window,
 * allowing for efficient time-scoped queries and bounded memory usage.
 *
 * Key features:
 * - Time-window scoped: Each window has its own bitmap index
 * - Configurable period and frequency: Hour/Minute/Second with custom frequencies
 * - Automatic window management: Creates indexes on-demand
 * - Cleanup support: Can remove old indexes to free memory
 * - Thread-safe: Safe for concurrent access from multiple threads
 */
class TemporalBitmapIndex {
public:
    /**
     * Create a new temporal bitmap index manager for managing bitmap indexes
     * for timeseries collections having the specified 'collectionUUID' with the
     * given 'period' and 'frequency'.
     * The 'writer' can be nullptr if this index is being constructed by a reader.
     * The 'reader' can be nullptr if reconstruction from disk is not needed.
     */
    TemporalBitmapIndex(const UUID& collectionUUID,
                        HCIndexPeriodEnum period,
                        int32_t frequency,
                        HCIndexWriter* writer = nullptr,
                        HCIndexReader* reader = nullptr);

    /**
     * Returns a pointer to the bitmap index covering the time window that
     * includes the specified 'timestamp' if found. Otherwise, create a new
     * index for the time window covering the 'timestamp' and return a pointer
     * to that index. Note that the returned pointer is valid for the lifetime
     * of this TemporalBitmapIndex. Returns an error if the index for the time
     * window covering the 'timestamp' cannot be created.
     */
    StatusWith<BitmapIndex*> getOrCreateIndexForTimestamp(OperationContext* opCtx,
                                                          const Timestamp& timestamp);

    /**
     * Returns a pointer to the bitmap index covering the time window that
     * includes the specified 'timestamp' if found. Otherwise, return an error.
     */
    StatusWith<BitmapIndex*> getIndexForTimestamp(const Timestamp& timestamp) const;

    /**
     * Add all entries for a row to the bitmap index for the appropriate time window.
     * For each column in the row vector, if the symbol index is non-zero,
     * add the rowId to that (column, value) bitmap.
     */
    Status addRow(OperationContext* opCtx,
                  int64_t rowId,
                  const std::vector<uint32_t>& row,
                  const Timestamp& timestamp);

    /**
     * Query rowIds that match the predicate in the time window containing timestamp.
     * The predicate is a vector where predicate[columnIndex] = symbolIndex to match.
     * A symbolIndex of 0 means "don't care" (skip this column).
     */
    std::set<int64_t> queryRowIds(const std::vector<uint32_t>& predicate,
                                  const Timestamp& timestamp) const;


    /**
     * Get all rowIds that have the specified symbolIndex in the specified column.
     * Returns an empty set if no matching entries exist.
     */
    std::set<int64_t> queryRowIds(size_t columnIndex, uint32_t symbolIndex, const Timestamp& timestamp) const;

    /**
     * Return the time window boundaries for a given 'timestamp'.
     */
    std::pair<Timestamp, Timestamp> getWindowForTimestamp(const Timestamp& timestamp) const;

    /**
     * Remove indexes serving time windows older than the specified
     * 'beforeTimestamp'. This is used for cleanup to free memory from old indexes.
     */
    Status cleanupOldIndexes(const Timestamp& beforeTimestamp);

    /**
     * Returns true if the bitmap index for the time window containing
     * 'timestamp' has an index for the specified columnInde. Otherwise, return
     * false.
     */
    bool hasIndexForColumn(size_t columnIndex, const Timestamp& timestamp) const;

    /**
     * Flush all pending operations to the database.
     */
    void flush();

    /**
     * Set Excluded columns
     */
    void setExcludedColumns(std::unordered_set<std::size_t> excludedColumns);

    /**
     * Set Included columns
     */
    void setIncludedColumns(std::unordered_set<std::size_t> includedColumns);

    /**
     * Statistics about this temporal bitmap index.
     */
    struct Stats {
        size_t totalIndexes;
        size_t totalEntries;
        size_t memoryUsageBytes;
    };

    /**
     * Return the usage statistics.
     */
    Stats getStats() const;

private:
    /**
     * Create or fetch the bitmap index for the time window that starts at the
     * specified 'windowStart' timestamp.
     */
    StatusWith<BitmapIndex*> getOrCreateIndex(OperationContext* opCtx,
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

    // Map: windowStart → BitmapIndex
    std::map<Timestamp, std::unique_ptr<BitmapIndex>> _indexes;

    // Collection UUID for this temporal bitmap index
    UUID _collectionUUID;

    // Period (hour, minute, second)
    HCIndexPeriodEnum _period;

    // Frequency (1-24 for hour, 1-59 for minute/second)
    int32_t _frequency;

    // Writer (can be nullptr if constructed by reader)
    HCIndexWriter* _writer = nullptr;

    // Reader (can be nullptr if reconstruction from disk is not needed)
    HCIndexReader* _reader = nullptr;

    // Bitmap index options
    bool _buildMetadataIndex;
    double _sparseIndexThreshold;
    double _denseIndexThreshold;
    bool _dynamicIndexBuild;

    bool _doRecomputeIndexedColumns;

    // Columns that are excluded according to user preference
    std::unordered_set<std::size_t> _excludedColumns;

    // Columns that are included according to user preference
    std::unordered_set<std::size_t> _includedColumns;

    // Synchronization
    mutable std::shared_mutex _mutex;
};

}  // namespace mongo::timeseries::hcindex

