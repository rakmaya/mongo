/*
 *    Copyright 2024-present MongoDB, Inc.
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
 *    conditions as described in each individual source file and upon
 *    distribution of the following conditions are met:
 *
 *    - The source code is distributed subject to the license terms in the
 *      LICENSE file in the root directory of this source tree.
 *    - Neither the name of MongoDB, Inc. nor the names of its contributors
 *      may be used to endorse or promote products derived from this software
 *      without specific prior written permission.
 */

#pragma once

#include "mongo/base/status_with.h"
#include "mongo/bson/timestamp.h"
#include "mongo/db/database_name.h"
#include "mongo/db/exec/timeseries/hcindex/bitmap_index.h"
#include "mongo/db/exec/timeseries/hcindex/temporal_attribute_table.h"
#include "mongo/db/exec/timeseries/hcindex/temporal_symbol_dictionary.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/shard_role/shard_role.h"
#include "mongo/db/timeseries/timeseries_gen.h"
#include "mongo/util/uuid.h"

#include <memory>

namespace mongo::timeseries::hcindex {

/**
 * Result of constructing a symbol dictionary from operations.
 * Contains either:
 * - A SymbolDictionary (base dictionary with no reference)
 * - A DeltaSymbolDictionary (references a base dictionary from another window)
 *
 * The caller is responsible for storing these in the appropriate maps
 * in TemporalSymbolDictionary.
 */
struct SymbolDictionaryConstructionResult {
    // The base dictionary - only set if this is a new base (INIT without REF)
    std::unique_ptr<SymbolDictionary> baseDictionary;

    // The delta dictionary - only set if this references a base (INIT with REF)
    std::unique_ptr<DeltaSymbolDictionary> deltaDictionary;

    // The window start of the referenced base dictionary (if deltaDictionary is set)
    boost::optional<Timestamp> refBaseDictionaryWindowStart;

    // Returns true if this result contains a delta dictionary (references a base)
    bool isDelta() const {
        return deltaDictionary != nullptr;
    }

    // Returns the dictionary as an ISymbolDictionary pointer
    ISymbolDictionary* getDictionary() const {
        if (deltaDictionary) {
            return deltaDictionary.get();
        }
        return baseDictionary.get();
    }
};

/**
 * Reads and constructs time-parametrized index structures from operations timeseries.
 * Replays operations to construct dictionaries and attribute tables for specific windows.
 *
 * Supports partial construction: to interpret data from 09:00-09:25, only operations
 * up to 09:25 are replayed. No need to wait for FIN at 09:59:59. This enables efficient
 * streaming queries on partial time ranges.
 *
 * Operations are read from two separate collections in the same database as the timeseries
 * collection:
 * - Symbol operations: hcindex.ops.symbols.<collectionUUID>
 * - Attribute operations: hcindex.ops.attributes.<collectionUUID>
 */
class HCIndexReader {
public:
    /**
     * Create a new operations reader for the specified collection.
     *
     * Parameters:
     * - dbName: Database name where the timeseries collection resides
     * - collectionUUID: UUID of the timeseries collection
     */
    HCIndexReader(const DatabaseName& dbName, const UUID& collectionUUID);

    /**
     * Initialize the reader by acquiring collections for symbol and attribute operations.
     * This must be called once before calling constructSymbolDictionary or constructAttributeTable.
     * This acquires locks on the ops collections, which are then reused for all subsequent
     * reconstruction operations, avoiding lock cycles during query execution.
     *
     * Parameters:
     * - opCtx: Operation context for database operations
     *
     * Returns OK if initialization succeeds, or an error status if collection acquisition fails.
     */
    Status initializeCollections(OperationContext* opCtx);

    /**
     * Construct a SymbolDictionary by replaying operations up to the specified timestamp.
     * Only reads operations up to the given timestamp, enabling partial construction.
     *
     * This allows efficient queries on partial time ranges without waiting for window
     * completion (FIN operation).
     *
     * Parameters:
     * - opCtx: Operation context for database operations
     * - period: Time-window period (hour, minute, second)
     * - frequency: Time-window frequency (1-24 for hour, 1-59 for minute/second)
     */
    StatusWith<std::unique_ptr<SymbolDictionary>> constructSymbolDictionary(
        OperationContext* opCtx,
        const Timestamp& windowStart,
        const Timestamp& windowEnd,
        HCIndexPeriodEnum period,
        int32_t frequency,
        const Timestamp& upToTimestamp);

    /**
     * Construct a symbol dictionary (either base or delta) by replaying operations.
     *
     * This is the preferred method for constructing symbol dictionaries as it handles
     * both base dictionaries (INIT without REF) and delta dictionaries (INIT with REF).
     *
     * The result contains:
     * - baseDictionary: Set if INIT has no REF (this is a new base dictionary)
     * - deltaDictionary: Set if INIT has REF (references a base from another window)
     * - refBaseDictionaryWindowStart: The window start of the referenced base
     *
     * The caller (TemporalSymbolDictionary) is responsible for:
     * 1. Looking up the referenced base dictionary if refBaseDictionaryWindowStart is set
     * 2. Setting the base dictionary pointer on the delta dictionary
     * 3. Storing the dictionaries in the appropriate maps
     *
     * Parameters:
     * - opCtx: Operation context for database operations
     * - windowStart: Start timestamp of the time window
     * - windowEnd: End timestamp of the time window
     * - period: Time-window period (hour, minute, second)
     * - frequency: Time-window frequency (1-24 for hour, 1-59 for minute/second)
     * - upToTimestamp: Only replay operations up to this timestamp
     * - baseDictionary: Optional base dictionary to use for delta construction.
     *                   If provided and INIT has REF, this base will be used.
     *                   If nullptr and INIT has REF, a delta will be created without base
     *                   (caller must set base later).
     */
    StatusWith<SymbolDictionaryConstructionResult> constructSymbolDictionaryWithDelta(
        OperationContext* opCtx,
        const Timestamp& windowStart,
        const Timestamp& windowEnd,
        HCIndexPeriodEnum period,
        int32_t frequency,
        const Timestamp& upToTimestamp,
        SymbolDictionary* baseDictionary = nullptr);

    /**
     * Construct an AttributeTable by replaying operations up to the specified timestamp.
     * Only reads operations up to the given timestamp, enabling partial construction.
     *
     * This allows efficient queries on partial time ranges without waiting for window
     * completion (FIN operation).
     *
     * The symbolDictionary parameter is required to convert string values to symbol indices
     * during construction.
     *
     * Parameters:
     * - opCtx: Operation context for database operations
     * - period: Time-window period (hour, minute, second)
     * - frequency: Time-window frequency (1-24 for hour, 1-59 for minute/second)
     */
    StatusWith<std::unique_ptr<AttributeTable>> constructAttributeTable(
        OperationContext* opCtx,
        const Timestamp& windowStart,
        const Timestamp& windowEnd,
        HCIndexPeriodEnum period,
        int32_t frequency,
        const Timestamp& upToTimestamp,
        ISymbolDictionary* symbolDictionary);

    /**
     * Construct a BitmapIndex by replaying operations up to the specified timestamp.
     * Only reads operations up to the given timestamp, enabling partial construction.
     *
     * This allows efficient queries on partial time ranges without waiting for window
     * completion (FIN operation).
     *
     *
     * Parameters:
     * - opCtx: Operation context for database operations
     * - period: Time-window period (hour, minute, second)
     * - frequency: Time-window frequency (1-24 for hour, 1-59 for minute/second)
     */
    StatusWith<std::unique_ptr<BitmapIndex>> constructBitmapIndex(OperationContext* opCtx,
                                                                  const Timestamp& windowStart,
                                                                  const Timestamp& windowEnd,
                                                                  HCIndexPeriodEnum period,
                                                                  int32_t frequency,
                                                                  const Timestamp& upToTimestamp);

    /**
     * Release acquired collections to allow lock release and prevent stashed transaction resource
     * issues during pipeline cleanup.
     */
    void close();

    /**
     * Prepare for yielding by releasing collection pointers.
     *
     * Called during doSaveState() before a yield point. This releases the collection
     * pointers held by the acquisitions, allowing locks to be yielded safely.
     *
     * The collections can be restored later by calling restoreForYield().
     */
    void prepareForYield();

    /**
     * Restore collection pointers after yielding.
     *
     * Called during doRestoreState() after a yield point. This re-acquires the
     * collection pointers that were released by prepareForYield().
     *
     * Parameters:
     * - opCtx: Operation context for database operations
     *
     * Returns Status::OK() on success, or an error status if restoration fails.
     */
    Status restoreForYield(OperationContext* opCtx);

private:
    /**
     * Helper method to acquire both symbol and attribute operations collections.
     * Used by both initializeCollections() and restoreForYield().
     */
    void acquireCollections(OperationContext* opCtx);

    /**
     * Deserialize a Roaring64BTree from delta-encoded BinData format.
     * Reverses the encoding done by HCIndexWriter::_serializeRoaring64BTree().
     *
     * Format: [count:8][delta1:varint][delta2:varint]...
     *
     * Returns a Roaring64BTree containing all the rowIds.
     */
    Roaring64BTree _deserializeRoaring64BTree(const char* data, size_t size) const;

    DatabaseName dbName;
    UUID collectionUUID;
    boost::optional<CollectionAcquisition> symbolOpsCollection;
    boost::optional<CollectionAcquisition> attributeOpsCollection;
    boost::optional<CollectionAcquisition> bitmapIndexCollection;
    bool collectionsInitialized = false;
};

}  // namespace mongo::timeseries::hcindex

