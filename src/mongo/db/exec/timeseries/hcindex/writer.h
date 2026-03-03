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

#include "mongo/base/status.h"
#include "mongo/bson/timestamp.h"
#include "mongo/db/database_name.h"
#include "mongo/db/exec/timeseries/hcindex/temporal_symbol_dictionary.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/timeseries/timeseries_gen.h"
#include "mongo/util/roaring_bitmaps.h"
#include "mongo/util/uuid.h"

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace mongo::timeseries::hcindex {

                        // ===================
                        // class HCIndexWriter
                        // ===================

/**
 * Builds operations for the time-parametrized index structure timeseries collections. Accumulates
 * INIT, opADD, FIN, and REF operations as InsertStatements. This component does NOT perform actual
 * inserts. Instead, it builds InsertStatement objects that can be flushed by the user.
 */
class HCIndexWriter {
public:

    //- PUBLIC TYPES

    /**
     * Indicates whether a symbol is part of the base dictionary or local/delta dictionary.
     */
    enum class SymbolType {
        // Symbol is from base disctionay
        Base,

        // Symbol is from local disctionay
        Local
    };

    /**
     * Represents a pending operation.
     */
    struct PendingOperation {
        InsertStatement statement;
        bool isSymbolOps;
    };


    //- CONSTRUCTORS


    /**
     * Constructs an HCIndex operations writer for the specified timeseries collection.
     */
    HCIndexWriter(const UUID& collectionUUID, const DatabaseName& dbName);


    //- ACCESSORS


    /**
     * Return all pending symbol operations accumulated so far.
     */
    std::vector<InsertStatement> getPendingSymbolOperations() const;

    /**
     * Return all pending attribute operations accumulated so far.
     */
    std::vector<InsertStatement> getPendingAttributeOperations() const;

    /**
     * Return all pending bitmap operations accumulated so far.
     */
    std::vector<InsertStatement> getPendingBitmapOperations() const;

    /**
     * Get the namespace string for symbol operations collection.
     * Format: hcindex.ops.symbols.<collectionUUID>
     */
    std::string getSymbolOperationsCollectionName() const;

    /**
     * Get the namespace string for attribute operations collection.
     * Format: hcindex.ops.attributes.<collectionUUID>
     */
    std::string getAttributeOperationsCollectionName() const;

    /**
     * Get the namespace string for bitmap operations collection.
     * Format: hcindex.ops.bitmaps.<collectionUUID>
     */
    std::string getBitmapOperationsCollectionName() const;

    /**
     * Get the collection UUID this writer is associated with.
     */
    const UUID& getCollectionUUID() const { return collectionUUID; }

    /**
     * Get the database name this writer is associated with.
     */
    const DatabaseName& getDbName() const { return dbName; }


    //- MODIFIERS


    /**
     * Enters INIT mode for symbol dictionary operations for the window
     * [`windowStart`, `windowEnd`), optionally referencing a base dictionary (`refBaseDictionary`)
     * and setting the local index offset (`localIndexOffset`). After flush() is called, the writer
     * automatically transitions back to ADD mode for this window.
     */
    Status initSymbolDictionary(const Timestamp& windowStart,
                                const Timestamp& windowEnd,
                                const boost::optional<Timestamp>& refBaseDictionary,
                                uint32_t localIndexOffset);

    /**
     * Enters INIT mode for attribute table operations for the window [`windowStart`, `windowEnd`).
     * After flush() is called, the writer automatically transitions back to ADD mode for this
     * window.
     */
    Status initAttributeTable(const Timestamp& windowstart, const Timestamp& windowend);

    /**
     * Accumulates a symbol for the window [`windowStart`, `windowEnd`) by adding the specified
     * `word` at given `index`, using `symbolType` to determine whether the symbol is written as
     * a base or local (delta) add operation, to be flushed later.
     */
    Status addSymbol(const Timestamp& windowStart,
                     const Timestamp& windowEnd,
                     const std::string& word,
                     uint32_t index,
                     SymbolType symbolType);

    /**
     * Accumulates the specified attribute table `row` for the window [`windowStart`, `windowEnd`).
     * Can be used for both INIT and opADD operations.
     */
    Status addAttributeRow(const Timestamp& windowStart,
                           const Timestamp& windowEnd,
                           const std::vector<uint32_t>& row);

    /**
     * Accumulates the specified schema field for the window [`windowStart`, `windowEnd`). Can be
     * used for both INIT and opADD operations.
     */
    Status addSchemaField(const Timestamp& windowStart,
                          const Timestamp& windowEnd,
                          const std::string& fieldName);

    /**
     * Accumulates an attribute mapping from `fieldName` to `columnIndex` for the window
     * [`windowStart`, `windowEnd`). Can be used for both INIT and opADD operations.
     */
    Status addAttribute(const Timestamp& windowStart,
                        const Timestamp& windowEnd,
                        const std::string& fieldName,
                        size_t columnIndex);

    /**
     * Mark the beginning of bitmap index initialization for the specified window.
     * Sets the writer to INIT mode for bitmap operations for this window.
     * After flush() is called, the writer automatically returns to ADD mode for this window.
     */
    Status initBitmapIndex(const Timestamp& windowStart, const Timestamp& windowEnd);

    /**
     * Add a bitmap entry to the accumulation buffer for the specified window. Each entry maps
     * (columnIndex, symbolIndex) -> set of rowIds. Can be used for both INIT and opADD operations.
     * Multiple calls accumulate entries that will be flushed together.
     */
    Status addBitmapEntry(const Timestamp& windowStart,
                          const Timestamp& windowEnd,
                          size_t columnIndex,
                          uint32_t symbolIndex,
                          const std::set<int64_t>& rowIds);

    /**
     * Add a bitmap entry using Roaring64BTree directly. This avoids the std::set intermediate
     * representation. TODO: A strongly typed RowIdSet could be the way to go here.
     */
    Status addBitmapEntryRoaring(const Timestamp& windowStart,
                                 const Timestamp& windowEnd,
                                 size_t columnIndex,
                                 uint32_t symbolIndex,
                                 const Roaring64BTree& roaringBitmap);

    /**
     * Flush accumulated operations grouped by time window. Creates BSON documents for INIT or
     * opADD operations and adds them to pendingOperations. The operation type (INIT or opADD)
     * is determined by the internal state set by initSymbolDictionary() or initAttributeTable().
     * After flush(), the writer returns to ADD mode.
     */
    Status flush(const Timestamp& windowStart,
                 const Timestamp& windowEnd,
                 HCIndexPeriodEnum period,
                 int32_t frequency,
                 bool isSymbolOps);

    /**
     * Flush accumulated bitmap operations grouped by time window. Creates BSON documents for INIT
     * or opADD operations and adds them to pendingBitmapOperations. The operation type
     * (INIT or opADD) is determined by the internal state set by initBitmapIndex(). After
     * flushBitmaps(), the writer returns to ADD mode.
     */
    Status flushBitmaps(const Timestamp& windowStart,
                        const Timestamp& windowEnd,
                        HCIndexPeriodEnum period,
                        int32_t frequency);

    /**
     * Build a FIN operation to mark the window as complete and immutable.
     */
    Status buildFin(const Timestamp& windowStart,
                    const Timestamp& windowEnd,
                    HCIndexPeriodEnum period,
                    int32_t frequency,
                    bool isSymbolOps);

    /**
     * Build a REF operation to indicate dictionary reuse from a previous window.
     */
    Status buildRef(const Timestamp& windowStart,
                    const Timestamp& windowEnd,
                    HCIndexPeriodEnum period,
                    int32_t frequency,
                    const Timestamp& refWindowStart);

    /**
     * Clear all pending operations for symbol, attribute, and bitmap.
     */
    void clearPendingOperations();

private:

    //- PRIVATE TYPES


    using WindowKey = std::pair<Timestamp, Timestamp>;
    using BitmapKey = std::pair<size_t, uint32_t>;

    // Symbol entry
    struct SymbolEntry {
        std::string word;
        uint32_t index;
        SymbolType type;
    };

    // Init Mode Parameters
    struct SymbolInitParams {
        boost::optional<Timestamp> refBaseDictionary;
        uint32_t localIndexOffset = 0;
    };

    /**
     * Operation type
     */
    enum class OpType {
        Symbol,
        Attribute,
        Bitmap
    };


    //- PRIVATE METHODS


    /**
     * Build an operation document and add it to pending operations.
     */
    void addPendingOperation(const BSONObj& doc, OpType opType);

    /**
     * Serialize a Roaring64BTree to binary format for storage. Returns a vector containing
     * the serialized data.
     */
    std::vector<char> serializeRoaring64BTree(const Roaring64BTree& bitmap) const;

    /**
     * Build and flush accumulated symbols as an operation.
     */
    Status flushSymbols(const Timestamp& windowStart,
                         const Timestamp& windowEnd,
                         HCIndexPeriodEnum period,
                         int32_t frequency);

    /**
     * Build and flush accumulated attributes as an operation.
     */
    Status flushAttributes(const Timestamp& windowStart,
                            const Timestamp& windowEnd,
                            HCIndexPeriodEnum period,
                            int32_t frequency);


    /**
     * Build and flush accumulated bitmap entries as an operation.
     */
    Status flushBitmapsImpl(const Timestamp& windowStart,
                            const Timestamp& windowEnd,
                            HCIndexPeriodEnum period,
                            int32_t frequency);


    //- DATA


    UUID collectionUUID;
    DatabaseName dbName;
    std::vector<InsertStatement> pendingSymbolOperations;
    std::vector<InsertStatement> pendingAttributeOperations;
    std::vector<InsertStatement> pendingBitmapOperations;


    // Accumulation buffers for incremental building, keyed by window (windowStart, windowEnd)
    std::map<WindowKey, std::vector<SymbolEntry>> accumulatedSymbols;
    std::map<WindowKey, std::vector<std::string>> accumulatedSchema;
    std::map<WindowKey, std::vector<std::vector<uint32_t>>> accumulatedRows;
    std::map<WindowKey, std::vector<std::pair<std::string, size_t>>> accumulatedAttributes;
    // Bitmap entries: (columnIndex, symbolIndex) -> Roaring64BTree of rowIds
    // Using Roaring64BTree directly for efficient delta encoding during serialization
    std::map<WindowKey, std::map<BitmapKey, Roaring64BTree>> accumulatedBitmaps;

    // TODO: ugly! need to refactor this!
    // State tracking for INIT vs ADD mode, keyed by window
    // Separate maps for symbol, attribute, and bitmap operations since they can be in different
    // modes. Defaults to ADD mode. Set to INIT by initSymbolDictionary(), initAttributeTable(), or
    // initBitmapIndex(). Resets to ADD after flush() for the corresponding operation type.
    std::map<WindowKey, bool> isSymbolInitMode;
    std::map<WindowKey, bool> isAttributeInitMode;
    std::map<WindowKey, bool> isBitmapInitMode;
    std::map<WindowKey, SymbolInitParams> symbolInitParams;
};

}  // namespace mongo::timeseries::hcindex

