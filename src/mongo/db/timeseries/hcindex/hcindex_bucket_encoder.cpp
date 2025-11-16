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

#include "mongo/db/timeseries/hcindex/hcindex_bucket_encoder.h"

#include "mongo/bson/bsonobjbuilder.h"

namespace mongo::timeseries::hcindex {

StatusWith<BucketDocument> HCIndexBucketEncoder::encode(
    const NamespaceString& nss,
    std::shared_ptr<HCBatch> batch,
    const TimeseriesOptions& options) {

    if (!batch) {
        return Status(ErrorCodes::BadValue, "HCBatch cannot be null");
    }

    if (batch->measurements.empty()) {
        return Status(ErrorCodes::BadValue, "HCBatch must contain at least one measurement");
    }

    if (batch->rowIds.size() != batch->measurements.size()) {
        return Status(ErrorCodes::BadValue,
                      "HCBatch rowIds and measurements must have the same size");
    }

    // Create BucketDocument with rowIds in 'meta' field
    BSONObjBuilder builder;
    builder.append("_id", batch->bucketId);

    {
        BSONObjBuilder controlBuilder(builder.subobjStart("control"));
        controlBuilder.append("version", 1);  // Uncompressed version
        controlBuilder.append("min", batch->min);
        controlBuilder.append("max", batch->max);
    }

    // Store rowIds array in meta field instead of full metadata
    {
        BSONArrayBuilder metaBuilder(builder.subarrayStart("meta"));
        for (int64_t rowId : batch->rowIds) {
            metaBuilder.append(rowId);
        }
    }

    // TODO: Add data field with measurements
    // This will be implemented when integrating with the write path
    {
        BSONObjBuilder dataBuilder(builder.subobjStart("data"));
        // Data fields will be populated from measurements
    }

    BucketDocument bucketDoc{builder.obj()};
    return bucketDoc;
}

}  // namespace mongo::timeseries::hcindex

