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
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/timestamp.h"

namespace mongo::timeseries::hcindex {
class HCIndexCollectionManager;
}

namespace mongo::timeseries::hcindex {

/**
 * Decodes timeseries bucket metadata that was encoded using HCIndex.
 *
 * When a bucket's 'meta' field contains a rowId (int64_t) instead of full metadata,
 * this decoder reconstructs the original metadata by:
 * 1. Reconstructing the TemporalSymbolDictionary from operations
 * 2. Reconstructing the TemporalAttributeTable from operations
 * 3. Looking up the rowId in the attribute table
 * 4. Converting symbol indices back to string values
 * 5. Returning the full metadata BSONObj
 *
 * This decoder enables transparent query execution on HCIndex-encoded buckets.
 * The query engine receives full metadata and can apply filters as normal.
 *
 * Thread-safe: The decoder itself is stateless; thread-safety depends on the
 * HCIndexCollectionManager passed to decode().
 */
class HCIndexBucketDecoder {
public:
    /**
     * Decode a rowId back to full metadata.
     *
     * Reconstructs the HCIndex structures from operations up to the specified
     * timestamp, then retrieves and decodes the metadata for the given rowId.
     *
     * Parameters:
     * - rowId: The row ID to decode (stored in bucket's 'meta' field)
     * - timestamp: The timestamp to use for partial reconstruction of structures
     * - hcindexMgr: The HCIndexCollectionManager for this collection
     *
     * Returns StatusWith<BSONObj> containing the decoded metadata on success,
     * or an error status if decoding fails.
     *
     * The returned BSONObj contains the full metadata with all fields and values
     * restored from the symbol dictionary and attribute table.
     */
    static StatusWith<BSONObj> decode(
        int64_t rowId,
        const Timestamp& timestamp,
        HCIndexCollectionManager* hcindexMgr);

private:
    HCIndexBucketDecoder() = delete;
};

}  // namespace mongo::timeseries::hcindex

