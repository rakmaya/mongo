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
#include "mongo/util/roaring_bitmaps.h"
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

                        // =====================
                        // enum BitmapIndexState
                        // =====================

/**
 * State machine for BitmapIndex lifecycle:
 * - NOP: Initial state, no operations allowed
 * - Reconstruction: Index is being reconstructed from stored operations
 * - ReadWrite: Index is in normal read and write mode (new entries can be added)
 * - ReadOnly: Index is locked, no modifications allowed
 *
 * State transitions:
 * - NOP -> Reconstruction or ReadWrite
 * - Reconstruction -> ReadWrite - to accept new data after reconstruction
 * - Reconstruction -> ReadOnly
 * - ReadWrite -> ReadOnly
 * - ReadOnly -> (no transitions allowed)
 */
enum class BitmapIndexState {
    NOP,
    Reconstruction,
    ReadWrite,
    ReadOnly
};

                        // =================
                        // class BitmapIndex
                        // =================

/**
 * Represents an append-only bitmap index for a specific time window. The bitmap index maps
 * (columnIndex, symbolIndex) -> set of RowIDs and facilites inverted index operations for
 * exact match queries.
 */
class BitmapIndex {

public:

    //- CONSTRUCTORS


    /**
     * Constructs a bitmap index for the time window [`windowStart`, `windowEnd`),
     * configured with the given `period` and `frequency`, optionally persisting
     * updates via `writer`. The index is initialized in the NOP state.
     */
    BitmapIndex(HCIndexPeriodEnum period,
                int32_t frequency,
                Timestamp windowStart,
                Timestamp windowEnd,
                HCIndexWriter* writer = nullptr);


    //- ACCESSORS


    /**
     * Return all rowIds that have the specified symbol in the given column.
     */
    std::set<int64_t> getRowIds(size_t column, uint32_t symbol) const;

    /**
     * Return all rowIds that match ALL non-zero values of respective columns indices of the
     * specified predicate. This performs an intersection of all matching bitmaps. Behavior is
     * undefined unless the predicate is a vector where predicate[columnIndex] = symbolIndex to
     * match. A symbolIndex of 0 means "skip this column in the intersection".
     */
    std::set<int64_t> queryRowIdsAnd(const std::vector<uint32_t>& predicate) const;

    /**
     * Return all rowIds that match ANY of the specified predicates. This performs a union of the
     * AND results for each predicate. Behavior is undefined unless the predicate is a vector where
     * predicate[columnIndex] = symbolIndex to match.
     */
    std::set<int64_t> queryRowIdsOr(const std::vector<std::vector<uint32_t>>& predicates) const;

    /**
     * Return the current state of this bitmap index.
     */
    BitmapIndexState getState() const;

    /**
     * Returns true if there is an index for column at the specified columnIndex. Otherwise,
     * return false.
     */
    bool hasIndexForColumn(size_t columnIndex) const;

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


    //- MODIFIERS


    /**
     * Add a bitmap entry for the specified (columnIndex, symbolIndex) pair with the given rowId.
     * If the bitmap doesn't exist, it is created. Returns an error if the index is not in
     * ReadWrite or Reconstruction state.
     */
    Status addEntry(size_t columnIndex, uint32_t symbolIndex, int64_t rowId);

    /**
     * Add all entries for a row. For each column in the row vector, if the symbol index is
     * non-zero (not missing), add the rowId to that column. Returns an error if the index is not
     * in ReadWrite or Reconstruction state.
     */
    Status addRow(int64_t rowId, const std::vector<uint32_t>& row);

    /**
     * Change the state of this bitmap index. Returns an error if transition is invalid. Valid
     * transition are:
     * - From NOP: can transition to Reconstruction or ReadWrite
     * - From Reconstruction: can transition to ReadOnly
     * - From ReadWrite: can transition to ReadOnly
     * - From ReadOnly: no transitions allowed
     */
    Status changeState(BitmapIndexState newState);

    /**
     * Set the writer for this index.
     */
    void setWriter(HCIndexWriter* writer) {
        _writer = writer;
    }

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

private:
    Status addEntryHelper(size_t columnIndex, uint32_t symbolIndex, int64_t rowId);

    // Two-level bitmap structure:
    // First level: columnIndex -> inner map
    // Second level: symbolIndex -> Roaring64BTree of RowIDs
    using SymbolBitmaps = std::unordered_map<uint32_t, Roaring64BTree>;
    std::unordered_map<size_t, SymbolBitmaps> _bitmaps;

    // Period and frequency for time window calculation
    HCIndexPeriodEnum _period;
    int32_t _frequency;

    // Range that we cover
    Timestamp _windowStart;
    Timestamp _windowEnd;

    // Writer
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


}  // namespace mongo::timeseries::hcindex

