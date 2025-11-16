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

#include "mongo/base/status_with.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/timeseries/timeseries_gen.h"
#include "mongo/db/timeseries/write_ops/timeseries_write_ops_utils_internal.h"
#include "mongo/db/timeseries/hcindex/hc_batch.h"

#include <memory>

namespace mongo::timeseries::hcindex {

// Use BucketDocument from write_ops_utils namespace
using BucketDocument = mongo::timeseries::write_ops_utils::BucketDocument;

/**
 * Encodes timeseries bucket documents using HCIndex (High Cardinality Indexing).
 *
 * Instead of storing full metadata in the bucket's 'meta' field, HCIndex encoding:
 * 1. Takes an HCBatch with pre-encoded rowIds for each measurement
 * 2. Creates a BucketDocument with rowIds stored in the 'meta' field
 * 3. Handles data field population from measurements
 *
 * Thread-safe: The encoder itself is stateless.
 */
class HCIndexBucketEncoder {
public:
    /**
     * Encode a bucket document using HCIndex.
     *
     * Takes an HCBatch containing measurements and their corresponding rowIds,
     * and returns a BucketDocument with the rowIds stored in the 'meta' field.
     *
     * Parameters:
     * - nss: The namespace of the bucket collection
     * - batch: The HCBatch containing measurements and rowIds
     * - options: The timeseries options for the collection
     *
     * Returns StatusWith<BucketDocument> containing the encoded bucket on success,
     * or an error status if encoding fails.
     */
    static StatusWith<BucketDocument> encode(
        const NamespaceString& nss,
        std::shared_ptr<HCBatch> batch,
        const TimeseriesOptions& options);

private:
    HCIndexBucketEncoder() = delete;
};

}  // namespace mongo::timeseries::hcindex

