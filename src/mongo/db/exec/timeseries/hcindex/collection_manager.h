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
#include "mongo/db/exec/timeseries/hcindex/bitmap_index.h"
#include "mongo/db/exec/timeseries/hcindex/temporal_bitmap_index.h"
#include "mongo/db/exec/timeseries/hcindex/writer.h"
#include "mongo/db/exec/timeseries/hcindex/reader.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/timeseries/timeseries_gen.h"
#include "mongo/db/timeseries/hcindex_options.h"
#include "mongo/util/uuid.h"

#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace mongo::timeseries::hcindex {

                        // ==============================
                        // class HCIndexCollectionManager
                        // ==============================

/**
 * Manages the lifecycle and operation of HCIndex components for a single timeseries collection,
 * coordinating the creation and use of the temporal symbol dictionary, attribute table, and index
 * reader/writer to support metadata encoding, decoding, and write operations.
 */
class HCIndexCollectionManager {

public:

    //- CLASS METHODS

    /**
     * Returns the NamespaceString for the symbol operations collection that should be created
     * for the given `collectionUUID` in the specified database.
     */
    static NamespaceString getSymbolOperationsNamespace(const DatabaseName& dbName, const UUID& collectionUUID);

    /**
     * Returns the NamespaceString for the attribute operations collection that should be created
     * for the given `collectionUUID` in the specified database.
     */
    static NamespaceString getAttributeOperationsNamespace(const DatabaseName& dbName, const UUID& collectionUUID);

    /**
     * Returns the NamespaceString for the bitmap index collection that should be created
     * for the given `collectionUUID` in the specified database.
     */
    static NamespaceString getBitmapIndexNamespace(const DatabaseName& dbName, const UUID& collectionUUID);


    //- CONSTRUCTORS


    /**
     * Constructs an HCIndex collection manager for the specified timeseries collection,
     * configuring time-window scoping, metadata bitmap indexing behavior, and column
     * inclusion/exclusion policies for index construction and maintenance.
     * TODO: Allow some of these to be changed dynamically (e.g. excluded/included
     * columns and other timeseries options).
     */
    HCIndexCollectionManager(OperationContext* opCtx,
                            const DatabaseName& dbName,
                            const UUID& collectionUUID,
                            HCIndexPeriodEnum period,
                            int32_t frequency,
                            bool buildMetadataIndex,
                            double sparseIndexThreshold,
                            double denseIndexThreshold,
                            bool dynamicIndexBuild,
                            std::vector<std::string> excludedColumns,
                            std::vector<std::string> includedColumns);


    //- MODIFIERS


    /**
     * Initialize the reader for read operations by acquiring collections. This acquires
     * collections for symbol and attribute operations and caches them for reuse in all
     * subsequent reconstruction operations, avoiding lock cycles during query execution.
     * Note that we call this function on-demand if the collection has not been initialized
     * on first use (e.g., during queryRows()) to avoid issues with stashed transaction
     * resources during pipeline cleanup. Returns Status::OK() on success, or an error
     * if initialization fails.
     */
    Status initializeForRead(OperationContext* opCtx);

    /**
     * Close and release acquired collections. Resets the acquired collections and clears
     * the initialization flag, allowing the manager to be reinitialized if needed.
     */
    void close();

    /**
     * Prepare for yielding by releasing collection pointers. Called during doSaveState()
     * before a yield point. This releases the collection pointers held by the reader.
     * The collections can be restored later by calling restoreForYield().
     */
    void prepareForYield();

    /**
     * Restore collection pointers after yielding. Called during doRestoreState() after a
     * yield point. This re-acquires the collection pointers that were released by
     * prepareForYield(). Returns Status::OK() on success, or an error status if
     * restoration fails.
     */
    Status restoreForYield(OperationContext* opCtx);

    /**
     * Encode metadata to a rowId and returns rowId on success, or an error if encoding
     * fails. We extract the fields from the metadata BSONObj, encodes them using the
     * TemporalSymbolDictionary, and inserts a row into the TemporalAttributeTable.
     * Returns the rowId for the encoded metadata. If the same metadata is encoded
     * multiple times, the same rowId is returned (deduplication).
     */
    StatusWith<int64_t> encodeMetadata(OperationContext* opCtx, const BSONObj& metadata, const Timestamp& timestamp);

    /**
     * Returns the BSONObj containing the decoded the data in a row at the specified `rowId` back
     * to full metadata. Internally, the function will reconstructs the TemporalSymbolDictionary
     * and TemporalAttributeTable from operations up to the specified timestamp, then retrieves
     * the metadata for the given rowId. Returns an error status if the rowId does not exist or
     * decoding fails.
     */
    StatusWith<BSONObj> decodeMetadata(OperationContext* opCtx, int64_t rowId, const Timestamp& timestamp);

    /**
     * Clean up HCIndex structures for this collection. Drops the operations collections and
     * releases all resources.
     */
    Status cleanup();

    /**
     * Query the attribute table for rows matching a predicate at a given timestamp and returns
     * StatusWith<std::vector<int64_t>> containing the matching rowIds on success, or an error
     * status if the query fails.
     */
    StatusWith<std::vector<int64_t>> queryRows(OperationContext* opCtx,
                                               const ::mongo::MatchExpression* matchExpr,
                                               const Timestamp& timestamp);

    /**
     * Flush pending operations accumulated by the writer. This method retrieves all pending
     * operations from the writer and invokes the provided callback for each collection type
     * (symbol, attribute, bitmap-index operations).
     *
     * The callback function receives:
     * - collectionName: The target collection name (hcindex.ops.symbols.*, hcindex.ops.attributes.*,...)
     * - operations: Vector of InsertStatement representing the operations to be flushed.
     *
     * The caller is responsible for implementing the actual database insert logic in the
     * callback.
     *
     * Callback signature:
     *   std::function<Status(const std::string& collectionName, const std::vector<InsertStatement>& operations)>
     *
     * Returns Status::OK() on success, or an error status if flushing fails.
     */
    Status flushPendingOperations(
        std::function<Status(const std::string&, const std::vector<InsertStatement>&)> flushCallback);

private:

    //- DATA


    // Database name where the timeseries collection resides
    DatabaseName dbName;

    // Collection UUID
    UUID collectionUUID;

    // Period
    HCIndexPeriodEnum period;

    // Frequency
    int32_t frequency;

    bool _buildMetadataIndex;
    double _sparseIndexThreshold;
    double _denseIndexThreshold;
    bool _dynamicIndexBuild;
    std::vector<std::string> _excludedColumns;
    std::vector<std::string> _includedColumns;

    // Writer
    std::unique_ptr<HCIndexWriter> writer;

    // Reader
    std::unique_ptr<HCIndexReader> reader;

    // Temporal symbol dictionary for encoding metadata values
    std::unique_ptr<TemporalSymbolDictionary> symbolDictionary;

    // Temporal bitmap index for fast metadata predicate lookups
    std::unique_ptr<TemporalBitmapIndex> bitmapIndex;

    // Temporal attribute table for storing metadata rows
    std::unique_ptr<TemporalAttributeTable> attributeTable;

    // Init tracker
    bool initializedForRead = false;
};


}  // namespace mongo::timeseries::hcindex

