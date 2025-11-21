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
#include "mongo/util/uuid.h"

#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

namespace mongo::timeseries::hcindex {

/**
 * Builds operations for the time-parametrized index structure timeseries collections.
 * Accumulates INIT, opADD, FIN, and REF operations as InsertStatements.
 *
 * Operations are stored in two separate collections in the same database as the timeseries collection:
 * - Symbol operations: hcindex.ops.symbols.<collectionUUID>
 * - Attribute operations: hcindex.ops.attributes.<collectionUUID>
 *
 * This class does NOT perform actual inserts. Instead, it builds InsertStatement objects
 * that can be flushed by the caller through HCIndexCollectionManager. This design avoids
 * circular dependencies by keeping HCIndexWriter independent of collection_crud.
 */
class HCIndexWriter {
public:
    /**
     * Represents a pending operation to be inserted.
     */
    struct PendingOperation {
        InsertStatement statement;
        bool isSymbolOps;  // true for symbol ops, false for attribute ops
    };

    /**
     * Create a new operations writer for the specified collection.
     *
     * Parameters:
     * - collectionUUID: UUID of the timeseries collection
     * - dbName: Database name where the timeseries collection resides
     */
    HCIndexWriter(const UUID& collectionUUID, const DatabaseName& dbName);

    /**
     * Mark the beginning of symbol dictionary initialization for the specified window.
     * Sets the writer to INIT mode for symbol operations for this window.
     * After flush() is called, the writer automatically returns to ADD mode for this window.
     */
    Status initSymbolDictionary(const Timestamp& windowStart, const Timestamp& windowEnd);

    /**
     * Mark the beginning of attribute table initialization for the specified window.
     * Sets the writer to INIT mode for attribute operations for this window.
     * After flush() is called, the writer automatically returns to ADD mode for this window.
     */
    Status initAttributeTable(const Timestamp& windowStart, const Timestamp& windowEnd);

    /**
     * Add a symbol to the accumulation buffer for the specified window.
     * Can be used for both INIT and opADD operations.
     * Multiple calls accumulate symbols that will be flushed together.
     */
    Status addSymbol(const Timestamp& windowStart,
                     const Timestamp& windowEnd,
                     const std::string& word,
                     uint32_t index);

    /**
     * Add a row to the accumulation buffer for the specified window.
     * Can be used for both INIT and opADD operations.
     * Multiple calls accumulate rows that will be flushed together.
     */
    Status addAttributeRow(const Timestamp& windowStart,
                           const Timestamp& windowEnd,
                           const std::vector<uint32_t>& row);

    /**
     * Add a schema field to the accumulation buffer for the specified window.
     * Can be used for both INIT and opADD operations.
     * Multiple calls accumulate schema fields that will be flushed together.
     */
    Status addSchemaField(const Timestamp& windowStart,
                          const Timestamp& windowEnd,
                          const std::string& fieldName);

    /**
     * Add an attribute (field name and column index) to the accumulation buffer for the specified window.
     * Can be used for both INIT and opADD operations.
     * Multiple calls accumulate attributes that will be flushed together.
     */
    Status addAttribute(const Timestamp& windowStart,
                        const Timestamp& windowEnd,
                        const std::string& fieldName,
                        size_t columnIndex);

    /**
     * Flush accumulated operations grouped by time window.
     * Creates BSON documents for INIT or opADD operations and adds them to pendingOperations.
     * The operation type (INIT or opADD) is determined by the internal state set by
     * initSymbolDictionary() or initAttributeTable(). After flush(), the writer returns to ADD mode.
     *
     * @param windowStart Start timestamp of the time window
     * @param windowEnd End timestamp of the time window
     * @param granularity Dictionary granularity for this window
     * @param isSymbolOps true for symbol operations, false for attribute operations
     */
    Status flush(const Timestamp& windowStart,
                 const Timestamp& windowEnd,
                 DictionaryGranularity granularity,
                 bool isSymbolOps);

    /**
     * Build a FIN operation to mark the window as complete and immutable.
     */
    Status buildFin(const Timestamp& windowStart,
                   const Timestamp& windowEnd,
                   DictionaryGranularity granularity,
                   bool isSymbolOps);

    /**
     * Build a REF operation to indicate dictionary reuse from a previous window.
     */
    Status buildRef(const Timestamp& windowStart,
                   const Timestamp& windowEnd,
                   DictionaryGranularity granularity,
                   const Timestamp& refWindowStart);

    /**
     * Get all pending symbol operations accumulated so far.
     * Returns a vector of InsertStatement objects ready to be flushed.
     */
    std::vector<InsertStatement> getPendingSymbolOperations() const;

    /**
     * Get all pending attribute operations accumulated so far.
     * Returns a vector of InsertStatement objects ready to be flushed.
     */
    std::vector<InsertStatement> getPendingAttributeOperations() const;

    /**
     * Clear all pending operations (both symbol and attribute).
     */
    void clearPendingOperations();

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

private:
    /**
     * Window key for accumulation buffers: (windowStart, windowEnd) pair
     */
    using WindowKey = std::pair<Timestamp, Timestamp>;

    /**
     * Helper method to build an operation document and add it to pending operations.
     */
    void _addPendingOperation(const BSONObj& doc, bool isSymbolOps);

    /**
     * Helper method to build and flush accumulated symbols as an operation.
     */
    Status _flushSymbols(const Timestamp& windowStart,
                         const Timestamp& windowEnd,
                         DictionaryGranularity granularity);

    /**
     * Helper method to build and flush accumulated attributes as an operation.
     */
    Status _flushAttributes(const Timestamp& windowStart,
                            const Timestamp& windowEnd,
                            DictionaryGranularity granularity);

    UUID collectionUUID;
    DatabaseName dbName;
    std::vector<InsertStatement> pendingSymbolOperations;
    std::vector<InsertStatement> pendingAttributeOperations;

    // Accumulation buffers for incremental building, keyed by window (windowStart, windowEnd)
    std::map<WindowKey, std::vector<std::pair<std::string, uint32_t>>> accumulatedSymbols;
    std::map<WindowKey, std::vector<std::string>> accumulatedSchema;
    std::map<WindowKey, std::vector<std::vector<uint32_t>>> accumulatedRows;
    std::map<WindowKey, std::vector<std::pair<std::string, size_t>>> accumulatedAttributes;

    // State tracking for INIT vs ADD mode, keyed by window
    // Separate maps for symbol and attribute operations since they can be in different modes.
    // Defaults to ADD mode. Set to INIT by initSymbolDictionary() or initAttributeTable().
    // Resets to ADD after flush() for the corresponding operation type.
    std::map<WindowKey, bool> isSymbolInitMode;
    std::map<WindowKey, bool> isAttributeInitMode;
};

}  // namespace mongo::timeseries::hcindex

