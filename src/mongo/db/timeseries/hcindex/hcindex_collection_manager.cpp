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

#include "mongo/db/timeseries/hcindex/hcindex_collection_manager.h"

#include "mongo/util/str.h"

namespace mongo::timeseries::hcindex {

HCIndexCollectionManager::HCIndexCollectionManager(OperationContext* opCtx,
                                                   const UUID& collectionUUID,
                                                   DictionaryGranularity granularity)
    : opCtx(opCtx),
      collectionUUID(collectionUUID),
      granularity(granularity),
      symbolDictionary(std::make_unique<TemporalSymbolDictionary>(opCtx, collectionUUID, granularity)),
      attributeTable(std::make_unique<TemporalAttributeTable>(opCtx, collectionUUID, granularity, symbolDictionary.get())),
      writer(std::make_unique<HCIndexWriter>(opCtx, collectionUUID)),
      reader(std::make_unique<HCIndexReader>(opCtx, collectionUUID)) {}

Status HCIndexCollectionManager::initialize() {
    // The structures are already initialized in the constructor
    // In the future, this could create the operations collections if needed
    return Status::OK();
}

StatusWith<int64_t> HCIndexCollectionManager::encodeMetadata(const BSONObj& metadata,
                                                             const Timestamp& timestamp) {
    if (!symbolDictionary || !attributeTable) {
        return Status(ErrorCodes::InternalError, "HCIndex structures not initialized");
    }

    // Insert the metadata row into the attribute table
    auto rowIdStatus = attributeTable->insertRow(metadata, timestamp);
    if (!rowIdStatus.isOK()) {
        return rowIdStatus.getStatus();
    }

    return rowIdStatus.getValue();
}

StatusWith<BSONObj> HCIndexCollectionManager::decodeMetadata(int64_t rowId,
                                                             const Timestamp& timestamp) {
    if (!symbolDictionary || !attributeTable) {
        return Status(ErrorCodes::InternalError, "HCIndex structures not initialized");
    }

    // Get the attribute table for this timestamp
    auto tableResult = attributeTable->getTableForTimestamp(timestamp);
    if (!tableResult.isOK()) {
        return tableResult.getStatus();
    }
    auto* table = tableResult.getValue();

    // Get the row from the attribute table (returns vector of symbol indices)
    auto rowIndices = attributeTable->getRow(rowId, timestamp);
    if (!rowIndices) {
        return Status(ErrorCodes::NoSuchKey,
                      str::stream() << "Row not found for rowId: " << rowId);
    }

    // Get the schema from the attribute table
    const auto& schema = table->getSchema();

    // Decode each symbol index using the symbol dictionary
    BSONObjBuilder builder;
    const auto& indices = rowIndices.value();

    for (size_t i = 0; i < indices.size() && i < schema.size(); ++i) {
        uint32_t symbolIndex = indices[i];

        // Skip missing values (0)
        if (symbolIndex == 0) {
            continue;
        }

        // Decode the symbol
        auto symbolOpt = symbolDictionary->decodeSymbol(symbolIndex, timestamp);
        if (!symbolOpt) {
            return Status(ErrorCodes::BadValue,
                          str::stream() << "Invalid symbol index: " << symbolIndex);
        }

        // Add to BSON with the correct field name from schema
        builder.append(schema[i], symbolOpt.value());
    }

    return builder.obj();
}

Status HCIndexCollectionManager::cleanup() {
    // In the future, this could drop the operations collections
    // For now, just clear the structures
    symbolDictionary.reset();
    attributeTable.reset();
    writer.reset();
    reader.reset();
    return Status::OK();
}

}  // namespace mongo::timeseries::hcindex

