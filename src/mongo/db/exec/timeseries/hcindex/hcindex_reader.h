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
#include "mongo/db/operation_context.h"
#include "mongo/db/exec/timeseries/hcindex/temporal_symbol_dictionary.h"
#include "mongo/db/exec/timeseries/hcindex/temporal_attribute_table.h"
#include "mongo/util/uuid.h"

#include <memory>

namespace mongo::timeseries::hcindex {

/**
 * Reads and constructs time-parametrized index structures from operations timeseries.
 * Replays operations to construct dictionaries and attribute tables for specific windows.
 *
 * Supports partial construction: to interpret data from 09:00-09:25, only operations
 * up to 09:25 are replayed. No need to wait for FIN at 09:59:59. This enables efficient
 * streaming queries on partial time ranges.
 *
 * Operations are read from two separate collections in the same database as the timeseries collection:
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
     * Construct a SymbolDictionary by replaying operations up to the specified timestamp.
     * Only reads operations up to the given timestamp, enabling partial construction.
     *
     * This allows efficient queries on partial time ranges without waiting for window
     * completion (FIN operation).
     *
     * Parameters:
     * - opCtx: Operation context for database operations
     */
    StatusWith<std::unique_ptr<SymbolDictionary>> constructSymbolDictionary(
        OperationContext* opCtx,
        const Timestamp& windowStart,
        const Timestamp& windowEnd,
        DictionaryGranularity granularity,
        const Timestamp& upToTimestamp);

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
     */
    StatusWith<std::unique_ptr<AttributeTable>> constructAttributeTable(
        OperationContext* opCtx,
        const Timestamp& windowStart,
        const Timestamp& windowEnd,
        DictionaryGranularity granularity,
        const Timestamp& upToTimestamp,
        SymbolDictionary* symbolDictionary);

private:
    DatabaseName dbName;
    UUID collectionUUID;
};

}  // namespace mongo::timeseries::hcindex

