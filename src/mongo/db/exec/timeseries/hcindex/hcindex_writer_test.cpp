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

#include "mongo/db/exec/timeseries/hcindex/hcindex_writer.h"
#include "mongo/db/local_catalog/catalog_test_fixture.h"
#include "mongo/unittest/unittest.h"

namespace mongo::timeseries::hcindex {

class HCIndexWriterTest : public CatalogTestFixture {
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

TEST_F(HCIndexWriterTest, InitSymbolDictionaryAndFlushCreatesINITOperation) {
    auto collectionUUID = getTestCollectionUUID();
    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    HCIndexWriter writer(collectionUUID, dbName);

    Timestamp windowStart(1, 0);
    Timestamp windowEnd(2, 0);

    // Mark as INIT mode
    ASSERT_OK(writer.initSymbolDictionary(windowStart, windowEnd, boost::none, 1));

    // Add symbols incrementally
    ASSERT_OK(writer.addSymbol(windowStart, windowEnd, "nyc", 1));
    ASSERT_OK(writer.addSymbol(windowStart, windowEnd, "us-east", 2));
    ASSERT_OK(writer.addSymbol(windowStart, windowEnd, "prod", 3));

    // Flush accumulated symbols
    auto status = writer.flush(windowStart, windowEnd, HCIndexPeriodEnum::Hour, 1, true);
    ASSERT_OK(status);

    // Verify the operation was accumulated
    auto pendingOps = writer.getPendingSymbolOperations();
    ASSERT_EQ(pendingOps.size(), 1);

    auto doc = pendingOps[0].doc;
    ASSERT(doc.hasField("_id"));
    ASSERT_EQ(doc.getStringField("op"), "INIT");
    ASSERT_EQ(doc.getField("windowStart").timestamp(), windowStart);
    ASSERT_EQ(doc.getField("windowEnd").timestamp(), windowEnd);
    // Period is stored as integer: Hour=0, Minute=1, Second=2
    ASSERT_EQ(doc.getIntField("period"), static_cast<int>(HCIndexPeriodEnum::Hour));
    ASSERT_EQ(doc.getIntField("frequency"), 1);

    // Verify symbols are present
    auto symbolsObj = doc.getObjectField("symbols");
    ASSERT_EQ(symbolsObj.getIntField("nyc"), 1);
    ASSERT_EQ(symbolsObj.getIntField("us-east"), 2);
    ASSERT_EQ(symbolsObj.getIntField("prod"), 3);
}

TEST_F(HCIndexWriterTest, InitAttributeTableAndFlushCreatesINITOperation) {
    auto collectionUUID = getTestCollectionUUID();
    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    HCIndexWriter writer(collectionUUID, dbName);

    Timestamp windowStart(1, 0);
    Timestamp windowEnd(2, 0);

    // Mark as INIT mode
    ASSERT_OK(writer.initAttributeTable(windowStart, windowEnd));

    // Add schema fields incrementally
    ASSERT_OK(writer.addSchemaField(windowStart, windowEnd, "cluster"));
    ASSERT_OK(writer.addSchemaField(windowStart, windowEnd, "service"));
    ASSERT_OK(writer.addSchemaField(windowStart, windowEnd, "pod"));

    // Add rows incrementally
    ASSERT_OK(writer.addAttributeRow(windowStart, windowEnd, {1, 2, 3}));
    ASSERT_OK(writer.addAttributeRow(windowStart, windowEnd, {1, 0, 4}));

    // Flush accumulated attributes
    auto status = writer.flush(windowStart, windowEnd, HCIndexPeriodEnum::Hour, 1, false);
    ASSERT_OK(status);

    // Verify the operation was accumulated
    auto pendingOps = writer.getPendingAttributeOperations();
    ASSERT_EQ(pendingOps.size(), 1);

    auto doc = pendingOps[0].doc;
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

TEST_F(HCIndexWriterTest, InitThenAddCreatesINITThenopADDOperations) {
    auto collectionUUID = getTestCollectionUUID();
    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    HCIndexWriter writer(collectionUUID, dbName);

    Timestamp windowStart(1, 0);
    Timestamp windowEnd(2, 0);

    // First flush: INIT operation with initial symbols
    ASSERT_OK(writer.initSymbolDictionary(windowStart, windowEnd, boost::none, 1));
    ASSERT_OK(writer.addSymbol(windowStart, windowEnd, "nyc", 1));
    ASSERT_OK(writer.addSymbol(windowStart, windowEnd, "us-east", 2));
    auto status1 = writer.flush(windowStart, windowEnd, HCIndexPeriodEnum::Hour, 1, true);
    ASSERT_OK(status1);

    // Second flush: opADD operation with additional symbols (no init call, defaults to ADD mode)
    ASSERT_OK(writer.addSymbol(windowStart, windowEnd, "prod", 3));
    auto status2 = writer.flush(windowStart, windowEnd, HCIndexPeriodEnum::Hour, 1, true);
    ASSERT_OK(status2);

    // Verify both operations were accumulated
    auto pendingOps = writer.getPendingSymbolOperations();
    ASSERT_EQ(pendingOps.size(), 2);

    // Verify first document is INIT
    auto doc1 = pendingOps[0].doc;
    ASSERT_EQ(doc1.getStringField("op"), "INIT");
    auto symbolsObj1 = doc1.getObjectField("symbols");
    ASSERT_EQ(symbolsObj1.getIntField("nyc"), 1);
    ASSERT_EQ(symbolsObj1.getIntField("us-east"), 2);

    // Verify second document is opADD
    auto doc2 = pendingOps[1].doc;
    ASSERT_EQ(doc2.getStringField("op"), "opADD");
    auto symbolsObj2 = doc2.getObjectField("symbols");
    ASSERT_EQ(symbolsObj2.getIntField("prod"), 3);
}

TEST_F(HCIndexWriterTest, InitAttributeThenAddCreatesINITThenopADDOperations) {
    auto collectionUUID = getTestCollectionUUID();
    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    HCIndexWriter writer(collectionUUID, dbName);

    Timestamp windowStart(1, 0);
    Timestamp windowEnd(2, 0);

    // First flush: INIT operation with initial schema and rows
    ASSERT_OK(writer.initAttributeTable(windowStart, windowEnd));
    ASSERT_OK(writer.addSchemaField(windowStart, windowEnd, "cluster"));
    ASSERT_OK(writer.addSchemaField(windowStart, windowEnd, "service"));
    ASSERT_OK(writer.addAttributeRow(windowStart, windowEnd, {1, 2}));
    auto status1 = writer.flush(windowStart, windowEnd, HCIndexPeriodEnum::Hour, 1, false);
    ASSERT_OK(status1);

    // Second flush: opADD operation with additional attribute (no init call, defaults to ADD mode)
    ASSERT_OK(writer.addAttribute(windowStart, windowEnd, "pod", 2));
    auto status2 = writer.flush(windowStart, windowEnd, HCIndexPeriodEnum::Hour, 1, false);
    ASSERT_OK(status2);

    // Verify both operations were accumulated
    auto pendingOps = writer.getPendingAttributeOperations();
    ASSERT_EQ(pendingOps.size(), 2);

    // Verify first document is INIT with schema and rows
    auto doc1 = pendingOps[0].doc;
    ASSERT_EQ(doc1.getStringField("op"), "INIT");
    ASSERT(doc1.hasField("schema"));
    ASSERT(doc1.hasField("rows"));

    // Verify second document is opADD with attributes
    auto doc2 = pendingOps[1].doc;
    ASSERT_EQ(doc2.getStringField("op"), "opADD");
    ASSERT(doc2.hasField("attributes"));
}

TEST_F(HCIndexWriterTest, BuildFinCreatesValidDocument) {
    auto collectionUUID = getTestCollectionUUID();
    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    HCIndexWriter writer(collectionUUID, dbName);

    Timestamp windowStart(1, 0);
    Timestamp windowEnd(2, 0);

    auto status = writer.buildFin(windowStart, windowEnd, HCIndexPeriodEnum::Hour, 1, true);
    ASSERT_OK(status);

    // Verify the operation was accumulated
    auto pendingOps = writer.getPendingSymbolOperations();
    ASSERT_EQ(pendingOps.size(), 1);

    auto doc = pendingOps[0].doc;
    ASSERT(doc.hasField("_id"));
    ASSERT_EQ(doc.getStringField("op"), "FIN");
    ASSERT_EQ(doc.getField("windowStart").timestamp(), windowStart);
    ASSERT_EQ(doc.getField("windowEnd").timestamp(), windowEnd);
    // Period is stored as integer: Hour=0, Minute=1, Second=2
    ASSERT_EQ(doc.getIntField("period"), static_cast<int>(HCIndexPeriodEnum::Hour));
    ASSERT_EQ(doc.getIntField("frequency"), 1);
    // Timestamp should be set to windowEnd for FIN operations
    ASSERT_EQ(doc.getField("timestamp").timestamp(), windowEnd);
}

TEST_F(HCIndexWriterTest, BuildRefCreatesValidDocument) {
    auto collectionUUID = getTestCollectionUUID();
    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    HCIndexWriter writer(collectionUUID, dbName);

    Timestamp windowStart(2, 0);
    Timestamp windowEnd(3, 0);
    Timestamp refWindowStart(1, 0);

    auto status = writer.buildRef(windowStart, windowEnd, HCIndexPeriodEnum::Hour, 1, refWindowStart);
    ASSERT_OK(status);

    // Verify the operation was accumulated
    auto pendingOps = writer.getPendingSymbolOperations();
    ASSERT_EQ(pendingOps.size(), 1);

    auto doc = pendingOps[0].doc;
    ASSERT(doc.hasField("_id"));
    ASSERT_EQ(doc.getStringField("op"), "REF");
    ASSERT_EQ(doc.getField("windowStart").timestamp(), windowStart);
    ASSERT_EQ(doc.getField("windowEnd").timestamp(), windowEnd);
    // Period is stored as integer: Hour=0, Minute=1, Second=2
    ASSERT_EQ(doc.getIntField("period"), static_cast<int>(HCIndexPeriodEnum::Hour));
    ASSERT_EQ(doc.getIntField("frequency"), 1);
    // Verify reference to previous window
    ASSERT_EQ(doc.getField("refWindowStart").timestamp(), refWindowStart);
}

TEST_F(HCIndexWriterTest, SymbolInitFollowedByAddCreatesSequence) {
    auto collectionUUID = getTestCollectionUUID();
    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    HCIndexWriter writer(collectionUUID, dbName);

    Timestamp windowStart(1, 0);
    Timestamp windowEnd(2, 0);

    // First flush: INIT operation with initial symbols
    ASSERT_OK(writer.initSymbolDictionary(windowStart, windowEnd, boost::none, 1));
    ASSERT_OK(writer.addSymbol(windowStart, windowEnd, "region", 1));
    ASSERT_OK(writer.addSymbol(windowStart, windowEnd, "zone", 2));
    auto initStatus = writer.flush(windowStart, windowEnd, HCIndexPeriodEnum::Hour, 1, true);
    ASSERT_OK(initStatus);

    // Second flush: opADD operation with new symbols (no init call, defaults to ADD mode)
    ASSERT_OK(writer.addSymbol(windowStart, windowEnd, "pod", 3));
    ASSERT_OK(writer.addSymbol(windowStart, windowEnd, "instance", 4));
    auto addStatus = writer.flush(windowStart, windowEnd, HCIndexPeriodEnum::Hour, 1, true);
    ASSERT_OK(addStatus);

    // Verify both operations were accumulated
    auto pendingOps = writer.getPendingSymbolOperations();
    ASSERT_EQ(pendingOps.size(), 2);

    // Verify INIT document (first)
    auto initDoc = pendingOps[0].doc;
    ASSERT_EQ(initDoc.getStringField("op"), "INIT");
    ASSERT_EQ(initDoc.getField("windowStart").timestamp(), windowStart);
    ASSERT_EQ(initDoc.getField("windowEnd").timestamp(), windowEnd);
    auto initSymbolsObj = initDoc.getObjectField("symbols");
    ASSERT_EQ(initSymbolsObj.getIntField("region"), 1);
    ASSERT_EQ(initSymbolsObj.getIntField("zone"), 2);

    // Verify ADD document (second)
    auto addDoc = pendingOps[1].doc;
    ASSERT_EQ(addDoc.getStringField("op"), "opADD");
    ASSERT_EQ(addDoc.getField("windowStart").timestamp(), windowStart);
    ASSERT_EQ(addDoc.getField("windowEnd").timestamp(), windowEnd);
    auto addSymbolsObj = addDoc.getObjectField("symbols");
    ASSERT_EQ(addSymbolsObj.getIntField("pod"), 3);
    ASSERT_EQ(addSymbolsObj.getIntField("instance"), 4);
}

TEST_F(HCIndexWriterTest, AttributeInitFollowedByAddCreatesSequence) {
    auto collectionUUID = getTestCollectionUUID();
    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    HCIndexWriter writer(collectionUUID, dbName);

    Timestamp windowStart(1, 0);
    Timestamp windowEnd(2, 0);

    // First flush: INIT operation with initial schema and rows
    ASSERT_OK(writer.initAttributeTable(windowStart, windowEnd));
    ASSERT_OK(writer.addSchemaField(windowStart, windowEnd, "cluster"));
    ASSERT_OK(writer.addSchemaField(windowStart, windowEnd, "service"));
    ASSERT_OK(writer.addAttributeRow(windowStart, windowEnd, {1, 2}));
    ASSERT_OK(writer.addAttributeRow(windowStart, windowEnd, {1, 3}));
    auto initStatus = writer.flush(windowStart, windowEnd, HCIndexPeriodEnum::Hour, 1, false);
    ASSERT_OK(initStatus);

    // Second flush: opADD operation with new attribute (no init call, defaults to ADD mode)
    ASSERT_OK(writer.addAttribute(windowStart, windowEnd, "pod", 2));
    auto addStatus = writer.flush(windowStart, windowEnd, HCIndexPeriodEnum::Hour, 1, false);
    ASSERT_OK(addStatus);

    // Verify both operations were accumulated
    auto pendingOps = writer.getPendingAttributeOperations();
    ASSERT_EQ(pendingOps.size(), 2);

    // Verify INIT document (first)
    auto initDoc = pendingOps[0].doc;
    ASSERT_EQ(initDoc.getStringField("op"), "INIT");
    ASSERT_EQ(initDoc.getField("windowStart").timestamp(), windowStart);
    ASSERT_EQ(initDoc.getField("windowEnd").timestamp(), windowEnd);
    auto initSchemaObj = initDoc.getObjectField("schema");
    ASSERT_EQ(initSchemaObj.getStringField("0"), "cluster");
    ASSERT_EQ(initSchemaObj.getStringField("1"), "service");
    auto initRowsArr = initDoc.getObjectField("rows");
    ASSERT_EQ(initRowsArr.nFields(), 2);

    // Verify ADD document (second)
    auto addDoc = pendingOps[1].doc;
    ASSERT_EQ(addDoc.getStringField("op"), "opADD");
    ASSERT_EQ(addDoc.getField("windowStart").timestamp(), windowStart);
    ASSERT_EQ(addDoc.getField("windowEnd").timestamp(), windowEnd);
    auto addAttrsObj = addDoc.getObjectField("attributes");
    ASSERT_EQ(addAttrsObj.getIntField("pod"), 2);
}

TEST_F(HCIndexWriterTest, FlushWithMinutePeriodStoresCorrectPeriodAndFrequency) {
    auto collectionUUID = getTestCollectionUUID();
    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    HCIndexWriter writer(collectionUUID, dbName);

    Timestamp windowStart(1, 0);
    Timestamp windowEnd(2, 0);

    // Mark as INIT mode
    ASSERT_OK(writer.initSymbolDictionary(windowStart, windowEnd, boost::none, 1));

    // Add a symbol
    ASSERT_OK(writer.addSymbol(windowStart, windowEnd, "test", 1));

    // Flush with Minute period and frequency 30
    auto status = writer.flush(windowStart, windowEnd, HCIndexPeriodEnum::Minute, 30, true);
    ASSERT_OK(status);

    // Verify the operation was accumulated
    auto pendingOps = writer.getPendingSymbolOperations();
    ASSERT_EQ(pendingOps.size(), 1);

    auto doc = pendingOps[0].doc;
    // Period is stored as integer: Hour=0, Minute=1, Second=2
    ASSERT_EQ(doc.getIntField("period"), static_cast<int>(HCIndexPeriodEnum::Minute));
    ASSERT_EQ(doc.getIntField("frequency"), 30);
}

TEST_F(HCIndexWriterTest, FlushWithSecondPeriodStoresCorrectPeriodAndFrequency) {
    auto collectionUUID = getTestCollectionUUID();
    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    HCIndexWriter writer(collectionUUID, dbName);

    Timestamp windowStart(1, 0);
    Timestamp windowEnd(2, 0);

    // Mark as INIT mode
    ASSERT_OK(writer.initSymbolDictionary(windowStart, windowEnd, boost::none, 1));

    // Add a symbol
    ASSERT_OK(writer.addSymbol(windowStart, windowEnd, "test", 1));

    // Flush with Second period and frequency 45
    auto status = writer.flush(windowStart, windowEnd, HCIndexPeriodEnum::Second, 45, true);
    ASSERT_OK(status);

    // Verify the operation was accumulated
    auto pendingOps = writer.getPendingSymbolOperations();
    ASSERT_EQ(pendingOps.size(), 1);

    auto doc = pendingOps[0].doc;
    // Period is stored as integer: Hour=0, Minute=1, Second=2
    ASSERT_EQ(doc.getIntField("period"), static_cast<int>(HCIndexPeriodEnum::Second));
    ASSERT_EQ(doc.getIntField("frequency"), 45);
}

TEST_F(HCIndexWriterTest, BuildFinStoresCorrectPeriodAndFrequency) {
    auto collectionUUID = getTestCollectionUUID();
    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    HCIndexWriter writer(collectionUUID, dbName);

    Timestamp windowStart(1, 0);
    Timestamp windowEnd(2, 0);

    // Build FIN operation with Minute period and frequency 15 for symbol operations
    auto status = writer.buildFin(windowStart, windowEnd, HCIndexPeriodEnum::Minute, 15, true);
    ASSERT_OK(status);

    // Verify the operation was accumulated
    auto pendingOps = writer.getPendingSymbolOperations();
    ASSERT_EQ(pendingOps.size(), 1);

    auto doc = pendingOps[0].doc;
    ASSERT_EQ(doc.getStringField("op"), "FIN");
    // Period is stored as integer: Hour=0, Minute=1, Second=2
    ASSERT_EQ(doc.getIntField("period"), static_cast<int>(HCIndexPeriodEnum::Minute));
    ASSERT_EQ(doc.getIntField("frequency"), 15);
}

TEST_F(HCIndexWriterTest, BuildRefStoresCorrectPeriodAndFrequency) {
    auto collectionUUID = getTestCollectionUUID();
    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    HCIndexWriter writer(collectionUUID, dbName);

    Timestamp windowStart(1, 0);
    Timestamp windowEnd(2, 0);
    Timestamp refWindowStart(0, 0);

    // Build REF operation with Second period and frequency 30
    auto status = writer.buildRef(windowStart, windowEnd, HCIndexPeriodEnum::Second, 30, refWindowStart);
    ASSERT_OK(status);

    // Verify the operation was accumulated
    auto pendingOps = writer.getPendingSymbolOperations();
    ASSERT_EQ(pendingOps.size(), 1);

    auto doc = pendingOps[0].doc;
    ASSERT_EQ(doc.getStringField("op"), "REF");
    // Period is stored as integer: Hour=0, Minute=1, Second=2
    ASSERT_EQ(doc.getIntField("period"), static_cast<int>(HCIndexPeriodEnum::Second));
    ASSERT_EQ(doc.getIntField("frequency"), 30);
    ASSERT_EQ(doc.getField("refWindowStart").timestamp(), refWindowStart);
}

}  // namespace mongo::timeseries::hcindex

