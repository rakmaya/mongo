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
#include "mongo/db/operation_context.h"
#include "mongo/db/timeseries/hcindex/temporal_symbol_dictionary.h"
#include "mongo/util/uuid.h"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace mongo::timeseries::hcindex {

/**
 * Writes operations to the time-parametrized index structure timeseries collections.
 * Manages writing INIT, opADD, FIN, and REF operations to the appropriate collections.
 *
 * Operations are stored in two separate timeseries collections:
 * - Symbol operations: system.hcindex.ops.symbols.<collectionUUID>
 * - Attribute operations: system.hcindex.ops.attributes.<collectionUUID>
 */
class HCIndexWriter {
public:
    /**
     * Create a new operations writer for the specified collection.
     */
    HCIndexWriter(OperationContext* opCtx, const UUID& collectionUUID);

    /**
     * Write an INIT operation to the symbol dictionary operations collection.
     * Initializes the dictionary with the given symbols for the specified window.
     */
    Status writeSymbolInit(const Timestamp& windowStart,
                          const Timestamp& windowEnd,
                          DictionaryGranularity granularity,
                          const std::vector<std::pair<std::string, uint32_t>>& symbols);

    /**
     * Write an INIT operation to the attribute table operations collection.
     * Initializes the table with the given schema and rows for the specified window.
     */
    Status writeAttributeInit(const Timestamp& windowStart,
                             const Timestamp& windowEnd,
                             DictionaryGranularity granularity,
                             const std::vector<std::string>& schema,
                             const std::vector<std::vector<uint32_t>>& rows);

    /**
     * Write an opADD operation to add new symbols to the dictionary.
     */
    Status writeSymbolAdd(const Timestamp& windowStart,
                         const Timestamp& windowEnd,
                         DictionaryGranularity granularity,
                         const std::vector<std::pair<std::string, uint32_t>>& symbols);

    /**
     * Write an opADD operation to add new attributes to the table schema.
     */
    Status writeAttributeAdd(const Timestamp& windowStart,
                            const Timestamp& windowEnd,
                            DictionaryGranularity granularity,
                            const std::vector<std::pair<std::string, size_t>>& attributes);

    /**
     * Write a FIN operation to mark the window as complete and immutable.
     */
    Status writeFin(const Timestamp& windowStart,
                   const Timestamp& windowEnd,
                   DictionaryGranularity granularity,
                   bool isSymbolOps);

    /**
     * Write a REF operation to indicate dictionary reuse from a previous window.
     */
    Status writeRef(const Timestamp& windowStart,
                   const Timestamp& windowEnd,
                   DictionaryGranularity granularity,
                   const Timestamp& refWindowStart);

private:
    /**
     * Helper method to insert an operation document into the appropriate operations collection.
     * If isSymbolOps is true, inserts into system.hcindex.ops.symbols.<collectionUUID>.
     * Otherwise, inserts into system.hcindex.ops.attributes.<collectionUUID>.
     */
    Status _insertOperation(const BSONObj& doc, bool isSymbolOps);

    OperationContext* opCtx;
    UUID collectionUUID;
};

}  // namespace mongo::timeseries::hcindex

