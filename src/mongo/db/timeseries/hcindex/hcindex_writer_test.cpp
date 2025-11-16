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

#include "mongo/db/timeseries/hcindex/hcindex_writer.h"
#include "mongo/db/local_catalog/catalog_test_fixture.h"
#include "mongo/unittest/unittest.h"

namespace mongo::timeseries::hcindex {

class HCIndexWriterTest : public CatalogTestFixture {
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

    // Helper to read documents from collection
    std::vector<BSONObj> readCollectionDocuments(const NamespaceString& nss) {
        std::vector<BSONObj> docs;
        auto opCtx = operationContext();

        CollectionAcquisitionRequest acquisitionRequest(
            nss,
            PlacementConcern::kPretendUnsharded,
            repl::ReadConcernArgs::get(opCtx),
            AcquisitionPrerequisites::kRead);

        auto collection = acquireCollection(opCtx, acquisitionRequest, MODE_IS);

        if (!collection.exists()) {
            return docs;
        }

        auto cursor = collection.getCollectionPtr()->getCursor(opCtx);
        while (auto record = cursor->next()) {
            docs.push_back(record->data.toBson());
        }

        return docs;
    }
};

TEST_F(HCIndexWriterTest, WriteSymbolInitCreatesValidDocument) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    auto nss = getSymbolOpsNamespace(collectionUUID);
    createOpsCollection(nss);

    HCIndexWriter writer(opCtx, collectionUUID);

    Timestamp windowStart(1, 0);
    Timestamp windowEnd(2, 0);
    std::vector<std::pair<std::string, uint32_t>> symbols = {
        {"nyc", 1},
        {"us-east", 2},
        {"prod", 3}
    };

    auto status = writer.writeSymbolInit(windowStart, windowEnd, DictionaryGranularity::HOURLY, symbols);
    ASSERT_OK(status);

    // Verify the document was written correctly
    auto docs = readCollectionDocuments(nss);
    ASSERT_EQ(docs.size(), 1);

    auto doc = docs[0];
    ASSERT(doc.hasField("_id"));
    ASSERT_EQ(doc.getStringField("op"), "INIT");
    ASSERT_EQ(doc.getField("windowStart").timestamp(), windowStart);
    ASSERT_EQ(doc.getField("windowEnd").timestamp(), windowEnd);
    ASSERT_EQ(doc.getIntField("granularity"), static_cast<int>(DictionaryGranularity::HOURLY));

    // Verify symbols are present
    auto symbolsObj = doc.getObjectField("symbols");
    ASSERT_EQ(symbolsObj.getIntField("nyc"), 1);
    ASSERT_EQ(symbolsObj.getIntField("us-east"), 2);
    ASSERT_EQ(symbolsObj.getIntField("prod"), 3);
}

TEST_F(HCIndexWriterTest, WriteAttributeInitCreatesValidDocument) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    auto nss = getAttributeOpsNamespace(collectionUUID);
    createOpsCollection(nss);

    HCIndexWriter writer(opCtx, collectionUUID);

    Timestamp windowStart(1, 0);
    Timestamp windowEnd(2, 0);
    std::vector<std::string> schema = {"cluster", "service", "pod"};
    std::vector<std::vector<uint32_t>> rows = {{1, 2, 3}, {1, 0, 4}};

    auto status = writer.writeAttributeInit(windowStart, windowEnd, DictionaryGranularity::HOURLY, schema, rows);
    ASSERT_OK(status);

    // Verify the document was written correctly
    auto docs = readCollectionDocuments(nss);
    ASSERT_EQ(docs.size(), 1);

    auto doc = docs[0];
    ASSERT(doc.hasField("_id"));
    ASSERT_EQ(doc.getStringField("op"), "INIT");
    ASSERT_EQ(doc.getField("windowStart").timestamp(), windowStart);
    ASSERT_EQ(doc.getField("windowEnd").timestamp(), windowEnd);

    // Verify schema
    auto schemaObj = doc.getObjectField("schema");
    ASSERT_EQ(schemaObj.getStringField("0"), "cluster");
    ASSERT_EQ(schemaObj.getStringField("1"), "service");
    ASSERT_EQ(schemaObj.getStringField("2"), "pod");

    // Verify rows
    auto rowsArr = doc.getObjectField("rows");
    ASSERT_EQ(rowsArr.nFields(), 2);
}

TEST_F(HCIndexWriterTest, WriteSymbolAddCreatesValidDocument) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    auto nss = getSymbolOpsNamespace(collectionUUID);
    createOpsCollection(nss);

    HCIndexWriter writer(opCtx, collectionUUID);

    Timestamp windowStart(1, 0);
    Timestamp windowEnd(2, 0);
    std::vector<std::pair<std::string, uint32_t>> symbols = {{"newSymbol", 4}};

    auto status = writer.writeSymbolAdd(windowStart, windowEnd, DictionaryGranularity::HOURLY, symbols);
    ASSERT_OK(status);

    // Verify the document was written correctly
    auto docs = readCollectionDocuments(nss);
    ASSERT_EQ(docs.size(), 1);

    auto doc = docs[0];
    ASSERT(doc.hasField("_id"));
    ASSERT_EQ(doc.getStringField("op"), "opADD");
    ASSERT_EQ(doc.getField("windowStart").timestamp(), windowStart);
    ASSERT_EQ(doc.getField("windowEnd").timestamp(), windowEnd);
    ASSERT_EQ(doc.getIntField("granularity"), static_cast<int>(DictionaryGranularity::HOURLY));

    // Verify new symbol is present
    auto symbolsObj = doc.getObjectField("symbols");
    ASSERT_EQ(symbolsObj.getIntField("newSymbol"), 4);
}

TEST_F(HCIndexWriterTest, WriteAttributeAddCreatesValidDocument) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    auto nss = getAttributeOpsNamespace(collectionUUID);
    createOpsCollection(nss);

    HCIndexWriter writer(opCtx, collectionUUID);

    Timestamp windowStart(1, 0);
    Timestamp windowEnd(2, 0);
    std::vector<std::pair<std::string, size_t>> attributes = {{"newField", 3}};

    auto status = writer.writeAttributeAdd(windowStart, windowEnd, DictionaryGranularity::HOURLY, attributes);
    ASSERT_OK(status);

    // Verify the document was written correctly
    auto docs = readCollectionDocuments(nss);
    ASSERT_EQ(docs.size(), 1);

    auto doc = docs[0];
    ASSERT(doc.hasField("_id"));
    ASSERT_EQ(doc.getStringField("op"), "opADD");
    ASSERT_EQ(doc.getField("windowStart").timestamp(), windowStart);
    ASSERT_EQ(doc.getField("windowEnd").timestamp(), windowEnd);
    ASSERT_EQ(doc.getIntField("granularity"), static_cast<int>(DictionaryGranularity::HOURLY));

    // Verify new attribute is present
    auto attrsObj = doc.getObjectField("attributes");
    ASSERT_EQ(attrsObj.getIntField("newField"), 3);
}

TEST_F(HCIndexWriterTest, WriteFinCreatesValidDocument) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    auto nss = getSymbolOpsNamespace(collectionUUID);
    createOpsCollection(nss);

    HCIndexWriter writer(opCtx, collectionUUID);

    Timestamp windowStart(1, 0);
    Timestamp windowEnd(2, 0);

    auto status = writer.writeFin(windowStart, windowEnd, DictionaryGranularity::HOURLY, true);
    ASSERT_OK(status);

    // Verify the document was written correctly
    auto docs = readCollectionDocuments(nss);
    ASSERT_EQ(docs.size(), 1);

    auto doc = docs[0];
    ASSERT(doc.hasField("_id"));
    ASSERT_EQ(doc.getStringField("op"), "FIN");
    ASSERT_EQ(doc.getField("windowStart").timestamp(), windowStart);
    ASSERT_EQ(doc.getField("windowEnd").timestamp(), windowEnd);
    ASSERT_EQ(doc.getIntField("granularity"), static_cast<int>(DictionaryGranularity::HOURLY));
    // Timestamp should be set to windowEnd for FIN operations
    ASSERT_EQ(doc.getField("timestamp").timestamp(), windowEnd);
}

TEST_F(HCIndexWriterTest, WriteRefCreatesValidDocument) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    auto nss = getSymbolOpsNamespace(collectionUUID);
    createOpsCollection(nss);

    HCIndexWriter writer(opCtx, collectionUUID);

    Timestamp windowStart(2, 0);
    Timestamp windowEnd(3, 0);
    Timestamp refWindowStart(1, 0);

    auto status = writer.writeRef(windowStart, windowEnd, DictionaryGranularity::HOURLY, refWindowStart);
    ASSERT_OK(status);

    // Verify the document was written correctly
    auto docs = readCollectionDocuments(nss);
    ASSERT_EQ(docs.size(), 1);

    auto doc = docs[0];
    ASSERT(doc.hasField("_id"));
    ASSERT_EQ(doc.getStringField("op"), "REF");
    ASSERT_EQ(doc.getField("windowStart").timestamp(), windowStart);
    ASSERT_EQ(doc.getField("windowEnd").timestamp(), windowEnd);
    ASSERT_EQ(doc.getIntField("granularity"), static_cast<int>(DictionaryGranularity::HOURLY));
    // Verify reference to previous window
    ASSERT_EQ(doc.getField("refWindowStart").timestamp(), refWindowStart);
}

TEST_F(HCIndexWriterTest, SymbolInitFollowedByAddCreatesSequence) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    auto nss = getSymbolOpsNamespace(collectionUUID);
    createOpsCollection(nss);

    HCIndexWriter writer(opCtx, collectionUUID);

    Timestamp windowStart(1, 0);
    Timestamp windowEnd(2, 0);

    // Write INIT operation with initial symbols
    std::vector<std::pair<std::string, uint32_t>> initSymbols = {
        {"region", 1},
        {"zone", 2}
    };
    auto initStatus = writer.writeSymbolInit(windowStart, windowEnd, DictionaryGranularity::HOURLY, initSymbols);
    ASSERT_OK(initStatus);

    // Write ADD operation with new symbols
    std::vector<std::pair<std::string, uint32_t>> addSymbols = {
        {"pod", 3},
        {"instance", 4}
    };
    auto addStatus = writer.writeSymbolAdd(windowStart, windowEnd, DictionaryGranularity::HOURLY, addSymbols);
    ASSERT_OK(addStatus);

    // Verify both documents were written
    auto docs = readCollectionDocuments(nss);
    ASSERT_EQ(docs.size(), 2);

    // Verify INIT document (first)
    auto initDoc = docs[0];
    ASSERT_EQ(initDoc.getStringField("op"), "INIT");
    ASSERT_EQ(initDoc.getField("windowStart").timestamp(), windowStart);
    ASSERT_EQ(initDoc.getField("windowEnd").timestamp(), windowEnd);
    auto initSymbolsObj = initDoc.getObjectField("symbols");
    ASSERT_EQ(initSymbolsObj.getIntField("region"), 1);
    ASSERT_EQ(initSymbolsObj.getIntField("zone"), 2);

    // Verify ADD document (second)
    auto addDoc = docs[1];
    ASSERT_EQ(addDoc.getStringField("op"), "opADD");
    ASSERT_EQ(addDoc.getField("windowStart").timestamp(), windowStart);
    ASSERT_EQ(addDoc.getField("windowEnd").timestamp(), windowEnd);
    auto addSymbolsObj = addDoc.getObjectField("symbols");
    ASSERT_EQ(addSymbolsObj.getIntField("pod"), 3);
    ASSERT_EQ(addSymbolsObj.getIntField("instance"), 4);
}

TEST_F(HCIndexWriterTest, AttributeInitFollowedByAddCreatesSequence) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    auto nss = getAttributeOpsNamespace(collectionUUID);
    createOpsCollection(nss);

    HCIndexWriter writer(opCtx, collectionUUID);

    Timestamp windowStart(1, 0);
    Timestamp windowEnd(2, 0);

    // Write INIT operation with initial schema and rows
    std::vector<std::string> initSchema = {"cluster", "service"};
    std::vector<std::vector<uint32_t>> initRows = {{1, 2}, {1, 3}};
    auto initStatus = writer.writeAttributeInit(windowStart, windowEnd, DictionaryGranularity::HOURLY, initSchema, initRows);
    ASSERT_OK(initStatus);

    // Write ADD operation with new attribute
    std::vector<std::pair<std::string, size_t>> addAttributes = {{"pod", 2}};
    auto addStatus = writer.writeAttributeAdd(windowStart, windowEnd, DictionaryGranularity::HOURLY, addAttributes);
    ASSERT_OK(addStatus);

    // Verify both documents were written
    auto docs = readCollectionDocuments(nss);
    ASSERT_EQ(docs.size(), 2);

    // Verify INIT document (first)
    auto initDoc = docs[0];
    ASSERT_EQ(initDoc.getStringField("op"), "INIT");
    ASSERT_EQ(initDoc.getField("windowStart").timestamp(), windowStart);
    ASSERT_EQ(initDoc.getField("windowEnd").timestamp(), windowEnd);
    auto initSchemaObj = initDoc.getObjectField("schema");
    ASSERT_EQ(initSchemaObj.getStringField("0"), "cluster");
    ASSERT_EQ(initSchemaObj.getStringField("1"), "service");
    auto initRowsArr = initDoc.getObjectField("rows");
    ASSERT_EQ(initRowsArr.nFields(), 2);

    // Verify ADD document (second)
    auto addDoc = docs[1];
    ASSERT_EQ(addDoc.getStringField("op"), "opADD");
    ASSERT_EQ(addDoc.getField("windowStart").timestamp(), windowStart);
    ASSERT_EQ(addDoc.getField("windowEnd").timestamp(), windowEnd);
    auto addAttrsObj = addDoc.getObjectField("attributes");
    ASSERT_EQ(addAttrsObj.getIntField("pod"), 2);
}

}  // namespace mongo::timeseries::hcindex

