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

#include "mongo/db/exec/timeseries/hcindex/hcindex_reader.h"
#include "mongo/db/exec/timeseries/hcindex/hcindex_writer.h"
#include "mongo/db/local_catalog/catalog_test_fixture.h"
#include "mongo/unittest/unittest.h"

namespace mongo::timeseries::hcindex {

class HCIndexReaderTest : public CatalogTestFixture {
protected:
    UUID getTestCollectionUUID() {
        return UUID::gen();
    }

    NamespaceString getSymbolOpsNamespace(const UUID& collectionUUID) {
        std::string collName = "hcindex.ops.symbols." + collectionUUID.toString();
        return NamespaceString::createNamespaceString_forTest("test", collName);
    }

    NamespaceString getAttributeOpsNamespace(const UUID& collectionUUID) {
        std::string collName = "hcindex.ops.attributes." + collectionUUID.toString();
        return NamespaceString::createNamespaceString_forTest("test", collName);
    }

    void createOpsCollection(const NamespaceString& nss) {
        ASSERT_OK(storageInterface()->createCollection(operationContext(), nss, CollectionOptions()));
    }

    void flushPendingOperations(HCIndexWriter& writer, const UUID& collectionUUID) {
        auto opCtx = operationContext();

        // Flush symbol operations
        auto symbolOps = writer.getPendingSymbolOperations();
        if (!symbolOps.empty()) {
            auto symbolNss = getSymbolOpsNamespace(collectionUUID);
            ASSERT_OK(storageInterface()->insertDocuments(opCtx, symbolNss, symbolOps));
        }

        // Flush attribute operations
        auto attrOps = writer.getPendingAttributeOperations();
        if (!attrOps.empty()) {
            auto attrNss = getAttributeOpsNamespace(collectionUUID);
            ASSERT_OK(storageInterface()->insertDocuments(opCtx, attrNss, attrOps));
        }

        writer.clearPendingOperations();
    }
};

TEST_F(HCIndexReaderTest, ConstructSymbolDictionaryFromInit) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    auto nss = getSymbolOpsNamespace(collectionUUID);
    createOpsCollection(nss);

    // Build initial symbols
    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    HCIndexWriter writer(collectionUUID, dbName);
    Timestamp windowStart(1, 0);
    Timestamp windowEnd(2, 0);

    // Initialize symbol dictionary and add symbols
    ASSERT_OK(writer.initSymbolDictionary(windowStart, windowEnd));
    ASSERT_OK(writer.addSymbol(windowStart, windowEnd, "region", 1));
    ASSERT_OK(writer.addSymbol(windowStart, windowEnd, "zone", 2));
    ASSERT_OK(writer.addSymbol(windowStart, windowEnd, "pod", 3));
    auto buildStatus = writer.flush(windowStart, windowEnd, DictionaryGranularity::HOURLY, true);
    ASSERT_OK(buildStatus);

    // Flush pending operations to database
    flushPendingOperations(writer, collectionUUID);

    // Construct dictionary
    HCIndexReader reader(opCtx, dbName, collectionUUID);
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

    // Build INIT and ADD operations
    DatabaseName dbName2 = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    HCIndexWriter writer(collectionUUID, dbName2);
    Timestamp windowStart(1, 0);
    Timestamp windowEnd(2, 0);

    // Initialize symbol dictionary and add initial symbols
    ASSERT_OK(writer.initSymbolDictionary(windowStart, windowEnd));
    ASSERT_OK(writer.addSymbol(windowStart, windowEnd, "region", 1));
    ASSERT_OK(writer.addSymbol(windowStart, windowEnd, "zone", 2));
    auto initStatus = writer.flush(windowStart, windowEnd, DictionaryGranularity::HOURLY, true);
    ASSERT_OK(initStatus);

    // Add more symbols (in ADD mode)
    ASSERT_OK(writer.addSymbol(windowStart, windowEnd, "pod", 3));
    ASSERT_OK(writer.addSymbol(windowStart, windowEnd, "instance", 4));
    auto addStatus = writer.flush(windowStart, windowEnd, DictionaryGranularity::HOURLY, true);
    ASSERT_OK(addStatus);

    // Flush pending operations to database
    flushPendingOperations(writer, collectionUUID);

    // Construct dictionary
    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    HCIndexReader reader(dbName, collectionUUID);
    auto dictStatus = reader.constructSymbolDictionary(
        opCtx, windowStart, windowEnd, DictionaryGranularity::HOURLY, windowEnd);
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

