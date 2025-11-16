/*
 * Copyright (C) 2024-present MongoDB, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the Server Side Public License, version 1,
 * as published by MongoDB, Inc.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * Server Side Public License for more details.
 *
 * You should have received a copy of the Server Side Public License
 * along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 * As a special exception, the copyright holders give you permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and upon the terms of
 * the Server Side Public License, version 1, as published by MongoDB, Inc.
 */

#pragma once

#include "mongo/db/timeseries/hcindex/hc_batch.h"
#include "mongo/db/timeseries/hcindex/hcindex_collection_manager.h"
#include "mongo/db/timeseries/timeseries_options.h"
#include "mongo/db/operation_id.h"
#include "mongo/db/namespace_string.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/base/status.h"
#include "mongo/base/status_with.h"

#include <memory>
#include <map>
#include <vector>

namespace mongo::timeseries::hcindex {

/**
 * HCBucketCatalog manages time-window-based batching for HCIndex.
 *
 * Unlike the traditional BucketCatalog which groups measurements by metadata uniqueness,
 * HCBucketCatalog groups measurements purely by time window. This is essential for
 * high-cardinality metadata indexing where metadata changes should NOT trigger new batches.
 *
 * Key responsibilities:
 * 1. Group measurements by time window boundaries
 * 2. Encode each measurement's metadata to rowId using HCIndexCollectionManager
 * 3. Create HCBatch instances with measurements and rowIds
 * 4. Manage batch lifecycle (creation, staging, commit, abort)
 * 5. Track active batches per collection and time window
 */
class HCBucketCatalog {
public:
    /**
     * Constructor for HCBucketCatalog.
     *
     * @param opCtx - OperationContext for database operations
     * @param collectionUUID - UUID of the timeseries collection
     * @param timeseriesOptions - TimeseriesOptions for the collection
     * @param hcindexMgr - HCIndexCollectionManager for metadata encoding/decoding
     */
    HCBucketCatalog(OperationContext* opCtx,
                    const UUID& collectionUUID,
                    const TimeseriesOptions& timeseriesOptions,
                    std::shared_ptr<HCIndexCollectionManager> hcindexMgr);

    HCBucketCatalog(const HCBucketCatalog&) = delete;
    HCBucketCatalog& operator=(const HCBucketCatalog&) = delete;

    /**
     * Stages a batch of measurements into HCBatches grouped by time window.
     *
     * This method:
     * 1. Groups measurements by time window
     * 2. Encodes each measurement's metadata to rowId
     * 3. Creates HCBatch instances for each time window
     * 4. Returns vector of HCBatches ready for encoding
     *
     * @param opId - OperationId for this batch
     * @param measurements - Vector of measurement BSONObj to stage
     * @param bucketId - OID for the bucket
     * @return StatusWith<std::vector<std::shared_ptr<HCBatch>>> - HCBatches or error
     */
    StatusWith<std::vector<std::shared_ptr<HCBatch>>> stageInsertBatch(
        const OperationId& opId,
        const std::vector<BSONObj>& measurements,
        const OID& bucketId);

    /**
     * Prepares an HCBatch for commit, transitioning it to inactive state.
     *
     * @param batch - HCBatch to prepare
     * @return Status - OK if successful, error otherwise
     */
    Status prepareCommit(std::shared_ptr<HCBatch> batch);

    /**
     * Finishes committing an HCBatch and notifies waiting threads.
     *
     * @param batch - HCBatch that was committed
     * @return Status - OK if successful, error otherwise
     */
    Status finish(std::shared_ptr<HCBatch> batch);

    /**
     * Aborts an HCBatch with the given status.
     *
     * @param batch - HCBatch to abort
     * @param status - Error status for the abort
     * @return Status - OK if successful, error otherwise
     */
    Status abort(std::shared_ptr<HCBatch> batch, const Status& status);

    /**
     * Clears all batches for this catalog.
     *
     * @return Status - OK if successful, error otherwise
     */
    Status clear();

private:
    OperationContext* _opCtx;
    UUID _collectionUUID;
    TimeseriesOptions _timeseriesOptions;
    std::shared_ptr<HCIndexCollectionManager> _hcindexMgr;

    // Map of time window start timestamp to active HCBatches
    std::map<Timestamp, std::vector<std::shared_ptr<HCBatch>>> _activeBatches;

    /**
     * Extracts the timestamp from a measurement BSONObj.
     *
     * @param measurement - Measurement BSONObj
     * @return StatusWith<Timestamp> - Extracted timestamp or error
     */
    StatusWith<Timestamp> _extractTimestamp(const BSONObj& measurement);

    /**
     * Calculates the time window start for a given timestamp.
     *
     * @param timestamp - Timestamp to calculate window for
     * @return Timestamp - Start of the time window
     */
    Timestamp _getTimeWindowStart(const Timestamp& timestamp);

    /**
     * Calculates the time window end for a given timestamp.
     *
     * @param timestamp - Timestamp to calculate window for
     * @return Timestamp - End of the time window
     */
    Timestamp _getTimeWindowEnd(const Timestamp& timestamp);

    /**
     * Gets the window size in seconds for the current granularity.
     *
     * @return uint32_t - Window size in seconds
     */
    uint32_t _getWindowSizeSeconds() const;
};

}  // namespace mongo::timeseries::hcindex

