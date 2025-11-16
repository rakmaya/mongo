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

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/oid.h"
#include "mongo/db/operation_id.h"
#include "mongo/util/future.h"
#include "mongo/util/future_impl.h"

#include <cstdint>
#include <vector>

#include <boost/container/small_vector.hpp>
#include <boost/optional/optional.hpp>

namespace mongo::timeseries::hcindex {

/**
 * HCBatch represents a batch of measurements grouped by time window for HCIndex.
 *
 * Unlike WriteBatch which groups measurements by metadata uniqueness, HCBatch groups
 * measurements purely by time window. Each measurement's metadata is encoded to a rowId
 * and stored separately, allowing high-cardinality metadata to be handled efficiently.
 *
 * The key differences from WriteBatch:
 * - Batching is based on time window, not metadata uniqueness
 * - Stores rowIds (int64_t) instead of full metadata objects
 * - Measurements are grouped by time window boundaries
 */
struct HCBatch {
    HCBatch() = delete;
    HCBatch(const OperationId& opId,
            const OID& bucketId,
            StringData timeField);

    // Operation ID for this batch
    const OperationId opId;

    // Bucket identifier
    const OID bucketId;

    // Time field name (e.g., "timestamp")
    StringData timeField;

    // Time window boundaries
    boost::optional<Timestamp> timeWindowStart;
    boost::optional<Timestamp> timeWindowEnd;

    // Measurements in this batch (grouped by time window)
    static constexpr std::size_t kNumStaticBatchMeasurements = 10;
    using BatchMeasurements = boost::container::small_vector<BSONObj, kNumStaticBatchMeasurements>;
    BatchMeasurements measurements;

    // Corresponding rowIds for each measurement (encoded metadata)
    std::vector<int64_t> rowIds;

    // Min and max values for this batch
    BSONObj min;
    BSONObj max;

    // Promise for batch completion
    SharedPromise<void> promise;

    // User batch indices for retryability
    std::vector<size_t> userBatchIndices;
    std::vector<int32_t> stmtIds;
};

}  // namespace mongo::timeseries::hcindex

