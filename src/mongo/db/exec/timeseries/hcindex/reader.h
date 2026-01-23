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

                        // ========================================
                        // class SymbolDictionaryConstructionResult
                        // ========================================

/**
 * Result of constructing a symbol dictionary from operations. It will
 * either contain a base dictionary or a delta dictionary.
 * TODO: Construction semantics for Dictionary could be eiether completely
 * moved into the Reader or the Dictionary.
 */
struct SymbolDictionaryConstructionResult {
    // The base dictionary
    std::unique_ptr<SymbolDictionary> baseDictionary;

    // The delta dictionary
    std::unique_ptr<DeltaSymbolDictionary> deltaDictionary;

    // The window start of the referenced base dictionary
    boost::optional<Timestamp> refBaseDictionaryWindowStart;

    // Returns true if this result contains a delta dictionary
    bool isDelta() const;

    // Returns the dictionary as an ISymbolDictionary pointer
    ISymbolDictionary* getDictionary();
};


/**
 * Reads and constructs time-parametrized index structures from operations timeseries. Replays
 * operations to construct dictionaries, attribute tables and bitmap index for specific windows.
 * Supports partial construction functions to interpret data from 09:00-09:25, only operations up
 * to 09:25 are replayed. Note that the implementation of the read assumes that the data is
 * written using the HCIndexWriter.
 */
class HCIndexReader {
public:

    //- CONSTRUCTORS


    /**
     * Constructs an HCIndex operations reader for the specified timeseries collection.
     */
    HCIndexReader(const DatabaseName& dbName, const UUID& collectionUUID);


    //- MODIFIERS


    /**
     * Initializes the reader by acquiring and holding the required ops collections, and must be
     * called once before any reconstruction methods are used. Returns OK if initialization
     * succeeds, or an error status if collection acquisition fails.
     */
    Status initializeCollections(OperationContext* opCtx);

    /**
     * Reconstructs a symbol dictionary for the window [`windowStart`, `windowEnd`) by replaying
     * persisted operations up to `upToTimestamp`. `period` and `frequency` are forwarded to the
     * constructed dictionary. Behavior is undefined unless `opCtx` remain valid for the duration
     * of the read.
     */
    StatusWith<std::unique_ptr<SymbolDictionary>> constructSymbolDictionary(
        OperationContext* opCtx,
        const Timestamp& windowStart,
        const Timestamp& windowEnd,
        HCIndexPeriodEnum period,
        int32_t frequency,
        const Timestamp& upToTimestamp);

    /**
     * Reconstructs a symbol dictionary for the window [`windowStart`, `windowEnd`) by replaying
     * operations up to `upToTimestamp`. Specified `period` and `frequency` are forwarded to the
     * constructed dictionary. Returns either a base dictionary or a delta dictionary (with
     * optional reference metadata). Uses the the specified `baseDictionary` if provided.
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
     * Reconstructs an AttributeTable for the window [`windowStart`, `windowEnd`) by replaying
     * operations up to `upToTimestamp`. Specified `period`, `prequency` and `symbolDictionary`
     * are forwarded to the AttributeTable. Behavior is undefined unless `opCtx` is valid for
     * the duration of the construction.
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
     * Reconstructs a BitmapIndex for the window [`windowStart`, `windowEnd`) by replaying
     * operations up to `upToTimestamp`. `period` and `frequency` is forwarded to the
     * constructed BitmapIndex. Behavior is undefined unless `opCtx` remains valid for the
     * duration of the construction.
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
     * Releases acquired collection pointers prior to a yield. Collections restorable via
     * `restoreForYield()`.
     */
    void prepareForYield();

    /**
     * Re-acquires collection pointers after a yield using the given `opCtx`, restoring the state
     * released by `prepareForYield()`.
     */
    Status restoreForYield(OperationContext* opCtx);

private:

    //- PRIVATE METHODS


    /**
     * Helper method to acquire both symbol and attribute operations collections. Used by both
     * initializeCollections() and restoreForYield().
     */
    void acquireCollections(OperationContext* opCtx);

    /**
     * Deserialize a Roaring64BTree from delta-encoded BinData format. Reverses the encoding done
     * by HCIndexWriter::serializeRoaring64BTree().
     */
    Roaring64BTree deserializeRoaring64BTree(const char* data, size_t size) const;

    DatabaseName dbName;
    UUID collectionUUID;
    boost::optional<CollectionAcquisition> symbolOpsCollection;
    boost::optional<CollectionAcquisition> attributeOpsCollection;
    boost::optional<CollectionAcquisition> bitmapIndexCollection;
    bool collectionsInitialized = false;
};

}  // namespace mongo::timeseries::hcindex

