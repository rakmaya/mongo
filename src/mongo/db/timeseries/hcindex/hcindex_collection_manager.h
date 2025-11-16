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

#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/timestamp.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/timeseries/hcindex/temporal_symbol_dictionary.h"
#include "mongo/db/timeseries/hcindex/temporal_attribute_table.h"
#include "mongo/db/timeseries/hcindex/hcindex_writer.h"
#include "mongo/db/timeseries/hcindex/hcindex_reader.h"
#include "mongo/util/uuid.h"

#include <memory>

namespace mongo::timeseries::hcindex {

/**
 * Manages the lifecycle and operations of HCIndex structures for a single timeseries collection.
 *
 * This manager coordinates the creation, initialization, and usage of:
 * - TemporalSymbolDictionary: Maps metadata values to integer indices
 * - TemporalAttributeTable: Stores metadata as rows of integer indices
 * - HCIndexWriter: Writes operations to timeseries collections
 * - HCIndexReader: Reads and reconstructs structures from operations
 *
 * The manager is responsible for:
 * - Initializing HCIndex structures when a collection is created with HCIndex enabled
 * - Encoding metadata to rowIds during write operations
 * - Decoding rowIds back to metadata during read operations
 * - Cleaning up resources when the collection is dropped
 *
 * Thread-safe: Safe for concurrent access from multiple threads.
 */
class HCIndexCollectionManager {
public:
    /**
     * Create a new HCIndex manager for the specified collection.
     *
     * The manager will manage HCIndex structures for the collection identified by
     * collectionUUID, using the specified granularity for time-window scoping.
     */
    HCIndexCollectionManager(OperationContext* opCtx,
                            const UUID& collectionUUID,
                            DictionaryGranularity granularity);

    /**
     * Initialize HCIndex structures for this collection.
     *
     * This creates the necessary operations collections and initializes the
     * TemporalSymbolDictionary and TemporalAttributeTable. Must be called
     * before any encode/decode operations.
     *
     * Returns Status::OK() on success, or an error status if initialization fails.
     */
    Status initialize();

    /**
     * Encode metadata to a rowId.
     *
     * Extracts fields from the metadata BSONObj, encodes them using the
     * TemporalSymbolDictionary, and inserts a row into the TemporalAttributeTable.
     * Returns the rowId for the encoded metadata.
     *
     * If the same metadata is encoded multiple times, the same rowId is returned
     * (deduplication).
     *
     * Returns StatusWith<int64_t> containing the rowId on success, or an error
     * status if encoding fails.
     */
    StatusWith<int64_t> encodeMetadata(const BSONObj& metadata, const Timestamp& timestamp);

    /**
     * Decode a rowId back to full metadata.
     *
     * Reconstructs the TemporalSymbolDictionary and TemporalAttributeTable from
     * operations up to the specified timestamp, then retrieves the metadata for
     * the given rowId.
     *
     * Returns StatusWith<BSONObj> containing the decoded metadata on success, or
     * an error status if decoding fails.
     */
    StatusWith<BSONObj> decodeMetadata(int64_t rowId, const Timestamp& timestamp);

    /**
     * Clean up HCIndex structures for this collection.
     *
     * Drops the operations collections and releases all resources. Should be
     * called when the collection is dropped.
     *
     * Returns Status::OK() on success, or an error status if cleanup fails.
     */
    Status cleanup();

    /**
     * Get the dictionary granularity for this collection.
     *
     * Returns the DictionaryGranularity used for time-window scoping.
     */
    DictionaryGranularity getGranularity() const {
        return granularity;
    }

private:
    // Operation context for database operations
    OperationContext* opCtx;

    // Collection UUID for this manager
    UUID collectionUUID;

    // Dictionary granularity for time-window scoping
    DictionaryGranularity granularity;

    // Temporal symbol dictionary for encoding metadata values
    std::unique_ptr<TemporalSymbolDictionary> symbolDictionary;

    // Temporal attribute table for storing metadata rows
    std::unique_ptr<TemporalAttributeTable> attributeTable;

    // Writer for operations
    std::unique_ptr<HCIndexWriter> writer;

    // Reader for operations
    std::unique_ptr<HCIndexReader> reader;
};

}  // namespace mongo::timeseries::hcindex

