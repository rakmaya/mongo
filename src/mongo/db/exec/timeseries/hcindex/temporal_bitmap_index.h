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
#include "mongo/db/exec/timeseries/hcindex/bitmap_index.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/timeseries/timeseries_gen.h"
#include "mongo/util/uuid.h"

#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <shared_mutex>
#include <unordered_set>
#include <vector>

namespace mongo::timeseries::hcindex {

// Forward declarations
class HCIndexReader;
class HCIndexWriter;
class BitmapIndex;

                        // =========================
                        // class TemporalBitmapIndex
                        // =========================

/**
 * Manages temporal bitmap indexes for a timeseries collection. Creates and maintains separate
 * bitmap indexes for each time window, allowing for efficient time-scoped queries.
 */
class TemporalBitmapIndex {

public:

    //- PUBLIC TYPES

    /**
     * Statistics about this temporal bitmap index.
     * TODO: Find out how stats is done in mongodb! For now this is for
     * debug/testing purpose.
     */
    struct Stats {
        size_t totalIndexes;
        size_t totalEntries;
        size_t memoryUsageBytes;
    };

public:

    //- CONSTRUCTORS


    /**
     * Constructs a temporal bitmap index manager for the given `collectionUUID`,
     * configured with the specified `period` and `frequency`, using `writer` for
     * persistence (if provided) and `reader` for reconstruction (if provided).
     */
    TemporalBitmapIndex(const UUID& collectionUUID,
                        HCIndexPeriodEnum period,
                        int32_t frequency,
                        HCIndexWriter* writer = nullptr,
                        HCIndexReader* reader = nullptr);


    //- ACCESSORS

    /**
     * Query rowIds that match the predicate in the time window containing timestamp. Behavior is
     * undifined unless the predicate is a vector where predicate[columnIndex] = symbolIndex to
     * match. A symbolIndex of 0 idicates "skip this column".
     */
    std::set<int64_t> queryRowIds(const std::vector<uint32_t>& predicate,
                                  const Timestamp& timestamp) const;

    /**
     * Returns row IDs in the window covering `timestamp` where the rows contain the specified
     * `symbolIndex` in the given `columnIndex`, or an empty set if none exist.
     */
    std::set<int64_t> queryRowIds(size_t columnIndex,
                                  uint32_t symbolIndex,
                                  const Timestamp& timestamp) const;

    /**
     * Returns true if the bitmap index for the time window containing 'timestamp' has an index
     * for the specified columnInde. Otherwise, return false.
     */
    bool hasIndexForColumn(size_t columnIndex, const Timestamp& timestamp) const;

    /**
     * Returns the bitmap index covering `timestamp`, or an error if none exists. The returned
     * point is valid for the lifetime of this `TemporalBitmapIndex`.
     */
    StatusWith<BitmapIndex*> getIndexForTimestamp(const Timestamp& timestamp) const;

    /**
     * Return the time window boundaries that covers the specified `timestamp`.
     */
    std::pair<Timestamp, Timestamp> getWindowForTimestamp(const Timestamp& timestamp) const;

    /**
     * Return the usage statistics.
     */
    Stats getStats() const;


    //- MODIFIERS


    /**
     * Returns the bitmap index covering `timestamp`, creating it if absent, with the
     * returned pointer valid for the lifetime of this `TemporalBitmapIndex`, or an
     * error if the index cannot be created.
     */
    StatusWith<BitmapIndex*> getOrCreateIndexForTimestamp(OperationContext* opCtx,
                                                          const Timestamp& timestamp);

    /**
     * Adds the row to the bitmap index for the window covering `timestamp`, inserting
     * `rowId` into each (column, value) bitmap for non-zero symbol indices.
     */
    Status addRow(OperationContext* opCtx,
                  int64_t rowId,
                  const std::vector<uint32_t>& row,
                  const Timestamp& timestamp);

    /**
     * Remove indexes serving time windows older than the specified 'beforeTimestamp'.
     */
    Status cleanupOldIndexes(const Timestamp& beforeTimestamp);

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

private:

    //- PRIVATE METHODS


    /**
     * Return the window start timestamp for the time window that includes the specified
     * 'timestamp'.
     */
    Timestamp calculateWindowStart(const Timestamp& timestamp) const;

    /**
     * Return the window end timestamp for the time window that starts at the specified
     * 'windowStart' timestamp.
     */
    Timestamp calculateWindowEnd(const Timestamp& windowStart) const;

    /**
     * Create or fetch the bitmap index for the time window that starts at the specified
     * 'windowStart' timestamp.
     */
    StatusWith<BitmapIndex*> getOrCreateIndex(OperationContext* opCtx,
                                              const Timestamp& windowStart);


    //- DATA


    // From windowStart -to- BitmapIndex
    std::map<Timestamp, std::unique_ptr<BitmapIndex>> _indexes;

    // Collection UUID for this temporal bitmap index
    UUID _collectionUUID;

    // Period
    HCIndexPeriodEnum _period;

    // Frequency
    int32_t _frequency;

    // Writer (this is optional)
    HCIndexWriter* _writer = nullptr;

    // Reader (this is optinal)
    HCIndexReader* _reader = nullptr;

    bool _buildMetadataIndex;
    double _sparseIndexThreshold;
    double _denseIndexThreshold;
    bool _dynamicIndexBuild;

    bool _doRecomputeIndexedColumns;

    // Excluded columns
    std::unordered_set<std::size_t> _excludedColumns;

    // Included columns
    std::unordered_set<std::size_t> _includedColumns;

    mutable std::shared_mutex _mutex;
};

}  // namespace mongo::timeseries::hcindex
