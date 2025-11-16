/**
 *    Copyright (C) 2024-present MongoDB, Inc.
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
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you do not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/db/timeseries/hcindex/hcindex_reader.h"
#include "mongo/db/timeseries/hcindex/hcindex_writer.h"
#include "mongo/db/local_catalog/catalog_test_fixture.h"
#include "mongo/unittest/unittest.h"

namespace mongo::timeseries::hcindex {

class HCIndexReaderTest : public CatalogTestFixture {
protected:
    UUID getTestCollectionUUID() {
        return UUID::gen();
    }

    NamespaceString getSymbolOpsNamespace(const UUID& collectionUUID) {
        std::string collName = "system.hcindex.ops.symbols." + collectionUUID.toString();
        return NamespaceString::createNamespaceString_forTest("config", collName);
    }

    NamespaceString getAttributeOpsNamespace(const UUID& collectionUUID) {
        std::string collName = "system.hcindex.ops.attributes." + collectionUUID.toString();
        return NamespaceString::createNamespaceString_forTest("config", collName);
    }

    void createOpsCollection(const NamespaceString& nss) {
        ASSERT_OK(storageInterface()->createCollection(operationContext(), nss, CollectionOptions()));
    }
};

TEST_F(HCIndexReaderTest, ConstructSymbolDictionaryFromInit) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    auto nss = getSymbolOpsNamespace(collectionUUID);
    createOpsCollection(nss);

    // Write initial symbols
    HCIndexWriter writer(opCtx, collectionUUID);
    Timestamp windowStart(1, 0);
    Timestamp windowEnd(2, 0);
    std::vector<std::pair<std::string, uint32_t>> symbols = {
        {"region", 1},
        {"zone", 2},
        {"pod", 3}
    };

    auto writeStatus = writer.writeSymbolInit(windowStart, windowEnd, DictionaryGranularity::HOURLY, symbols);
    ASSERT_OK(writeStatus);

    // Construct dictionary
    HCIndexReader reader(opCtx, collectionUUID);
    auto dictStatus = reader.constructSymbolDictionary(
        windowStart, windowEnd, DictionaryGranularity::HOURLY, windowEnd);
    ASSERT_OK(dictStatus);

    auto dict = std::move(dictStatus.getValue());
    ASSERT(dict);

    // Verify symbols were constructed
    ASSERT_EQ(dict->getSymbolCount(), 3);
    ASSERT_EQ(dict->getSymbolIndex("region").value_or(0), 1);
    ASSERT_EQ(dict->getSymbolIndex("zone").value_or(0), 2);
    ASSERT_EQ(dict->getSymbolIndex("pod").value_or(0), 3);
}

TEST_F(HCIndexReaderTest, ConstructSymbolDictionaryFromInitAndAdd) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    auto nss = getSymbolOpsNamespace(collectionUUID);
    createOpsCollection(nss);

    // Write INIT and ADD operations
    HCIndexWriter writer(opCtx, collectionUUID);
    Timestamp windowStart(1, 0);
    Timestamp windowEnd(2, 0);

    std::vector<std::pair<std::string, uint32_t>> initSymbols = {
        {"region", 1},
        {"zone", 2}
    };
    auto initStatus = writer.writeSymbolInit(windowStart, windowEnd, DictionaryGranularity::HOURLY, initSymbols);
    ASSERT_OK(initStatus);

    std::vector<std::pair<std::string, uint32_t>> addSymbols = {
        {"pod", 3},
        {"instance", 4}
    };
    auto addStatus = writer.writeSymbolAdd(windowStart, windowEnd, DictionaryGranularity::HOURLY, addSymbols);
    ASSERT_OK(addStatus);

    // Construct dictionary
    HCIndexReader reader(opCtx, collectionUUID);
    auto dictStatus = reader.constructSymbolDictionary(
        windowStart, windowEnd, DictionaryGranularity::HOURLY, windowEnd);
    ASSERT_OK(dictStatus);

    auto dict = std::move(dictStatus.getValue());
    ASSERT(dict);

    // Verify all symbols were constructed
    ASSERT_EQ(dict->getSymbolCount(), 4);
    ASSERT_EQ(dict->getSymbolIndex("region").value_or(0), 1);
    ASSERT_EQ(dict->getSymbolIndex("zone").value_or(0), 2);
    ASSERT_EQ(dict->getSymbolIndex("pod").value_or(0), 3);
    ASSERT_EQ(dict->getSymbolIndex("instance").value_or(0), 4);
}

}  // namespace mongo::timeseries::hcindex

