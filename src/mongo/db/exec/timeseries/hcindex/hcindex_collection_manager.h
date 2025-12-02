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
#include "mongo/db/database_name.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/exec/timeseries/hcindex/temporal_symbol_dictionary.h"
#include "mongo/db/exec/timeseries/hcindex/temporal_attribute_table.h"
#include "mongo/db/exec/timeseries/hcindex/hcindex_writer.h"
#include "mongo/db/exec/timeseries/hcindex/hcindex_reader.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/util/uuid.h"

#include <functional>
#include <memory>
#include <string>
#include <vector>

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
     * Get the namespace for the symbol operations collection.
     *
     * Returns the NamespaceString for the symbol operations collection that should be created
     * for the given collection UUID in the specified database.
     */
    static NamespaceString getSymbolOperationsNamespace(const DatabaseName& dbName, const UUID& collectionUUID);

    /**
     * Get the namespace for the attribute operations collection.
     *
     * Returns the NamespaceString for the attribute operations collection that should be created
     * for the given collection UUID in the specified database.
     */
    static NamespaceString getAttributeOperationsNamespace(const DatabaseName& dbName, const UUID& collectionUUID);

    /**
     * Create a new HCIndex manager for the specified collection.
     *
     * The manager will manage HCIndex structures for the collection identified by
     * collectionUUID in the specified database, using the specified granularity for time-window scoping.
     *
     * Parameters:
     * - opCtx: Operation context for database operations
     * - dbName: Database name where the timeseries collection resides
     * - collectionUUID: UUID of the timeseries collection
     * - granularity: Time-window granularity for the HCIndex structures
     */
    HCIndexCollectionManager(OperationContext* opCtx,
                            const DatabaseName& dbName,
                            const UUID& collectionUUID,
                            DictionaryGranularity granularity);

    /**
     * Initialize the reader for read operations by acquiring collections.
     *
     * This acquires collections for symbol and attribute operations and caches them
     * for reuse in all subsequent reconstruction operations, avoiding lock cycles during
     * query execution.
     *
     * This is called lazily on first use (e.g., during queryRows()) to avoid issues with
     * stashed transaction resources during pipeline cleanup.
     *
     * Parameters:
     * - opCtx: Operation context for database operations
     *
     * Returns Status::OK() on success, or an error status if initialization fails.
     */
    Status initializeForRead(OperationContext* opCtx);

    /**
     * Close and release acquired collections.
     *
     * Resets the acquired collections and clears the initialization flag, allowing
     * the manager to be re-initialized if needed.
     */
    void close();

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
     * Parameters:
     * - opCtx: Operation context for database operations
     * - metadata: The metadata BSONObj to encode
     * - timestamp: The timestamp for this operation
     *
     * Returns StatusWith<int64_t> containing the rowId on success, or an error
     * status if encoding fails.
     */
    StatusWith<int64_t> encodeMetadata(OperationContext* opCtx, const BSONObj& metadata, const Timestamp& timestamp);

    /**
     * Decode a rowId back to full metadata.
     *
     * Reconstructs the TemporalSymbolDictionary and TemporalAttributeTable from
     * operations up to the specified timestamp, then retrieves the metadata for
     * the given rowId.
     *
     * Parameters:
     * - opCtx: Operation context for database operations
     * - rowId: The rowId to decode
     * - timestamp: The timestamp for this operation
     *
     * Returns StatusWith<BSONObj> containing the decoded metadata on success, or
     * an error status if decoding fails.
     */
    StatusWith<BSONObj> decodeMetadata(OperationContext* opCtx, int64_t rowId, const Timestamp& timestamp);

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

    /**
     * Query the attribute table for rows matching a predicate at a given timestamp.
     *
     * This method converts a MatchExpression to an AttributeTablePredicate and queries
     * the attribute table for the time window containing the specified timestamp.
     *
     * Parameters:
     * - opCtx: Operation context for database operations
     * - matchExpr: The MatchExpression predicate to match (can be null for empty predicate)
     * - timestamp: The timestamp for determining which time window to query
     *
     * Returns StatusWith<std::vector<int64_t>> containing the matching rowIds on success,
     * or an error status if the query fails.
     */
    StatusWith<std::vector<int64_t>> queryRows(OperationContext* opCtx,
                                               const ::mongo::MatchExpression* matchExpr,
                                               const Timestamp& timestamp);

    /**
     * Flush pending operations accumulated by the writer.
     *
     * This method retrieves all pending operations from the writer and invokes the
     * provided callback for each collection type (symbol and attribute operations).
     *
     * The callback function receives:
     * - collectionName: The target collection name (hcindex.ops.symbols.* or hcindex.ops.attributes.*)
     * - operations: Vector of InsertStatement objects to be flushed
     *
     * The caller is responsible for implementing the actual database insert logic
     * in the callback. This design allows maximum flexibility in how operations
     * are persisted (e.g., batching, transaction handling, etc.).
     *
     * Callback signature:
     *   std::function<Status(const std::string& collectionName, const std::vector<InsertStatement>& operations)>
     *
     * Returns Status::OK() on success, or an error status if flushing fails.
     */
    Status flushPendingOperations(
        std::function<Status(const std::string&, const std::vector<InsertStatement>&)> flushCallback);

private:
    // Database name where the timeseries collection resides
    DatabaseName dbName;

    // Collection UUID for this manager
    UUID collectionUUID;

    // Dictionary granularity for time-window scoping
    DictionaryGranularity granularity;

    // Writer for operations
    std::unique_ptr<HCIndexWriter> writer;

    // Reader for operations
    std::unique_ptr<HCIndexReader> reader;

    // Temporal symbol dictionary for encoding metadata values
    std::unique_ptr<TemporalSymbolDictionary> symbolDictionary;

    // Temporal attribute table for storing metadata rows
    std::unique_ptr<TemporalAttributeTable> attributeTable;

    // Flag to track if we've initialized the reader for read operations
    bool initializedForRead = false;
};

}  // namespace mongo::timeseries::hcindex

