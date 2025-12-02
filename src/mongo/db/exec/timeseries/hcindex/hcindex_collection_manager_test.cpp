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

#include "mongo/db/exec/timeseries/hcindex/hcindex_collection_manager.h"
#include "mongo/db/local_catalog/catalog_test_fixture.h"
#include "mongo/unittest/unittest.h"

namespace mongo::timeseries::hcindex {

class HCIndexCollectionManagerTest : public CatalogTestFixture {
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

    void createOpsCollections(const UUID& collectionUUID) {
        auto symbolNss = getSymbolOpsNamespace(collectionUUID);
        auto attributeNss = getAttributeOpsNamespace(collectionUUID);
        ASSERT_OK(storageInterface()->createCollection(operationContext(), symbolNss, CollectionOptions()));
        ASSERT_OK(storageInterface()->createCollection(operationContext(), attributeNss, CollectionOptions()));
    }
};

TEST_F(HCIndexCollectionManagerTest, Initialize) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    createOpsCollections(collectionUUID);

    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    HCIndexCollectionManager manager(opCtx, dbName, collectionUUID, DictionaryGranularity::HOURLY);
    auto status = manager.initializeForRead(opCtx);
    ASSERT_OK(status);
}

TEST_F(HCIndexCollectionManagerTest, EncodeMetadata) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    createOpsCollections(collectionUUID);

    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    HCIndexCollectionManager manager(opCtx, dbName, collectionUUID, DictionaryGranularity::HOURLY);
    ASSERT_OK(manager.initializeForRead(opCtx));

    BSONObj metadata = BSON("region" << "us-east" << "env" << "prod");
    Timestamp ts(1, 0);

    auto result = manager.encodeMetadata(opCtx, metadata, ts);
    ASSERT_OK(result.getStatus());
    // rowId should be >= 0 (0 is reserved for missing values, but implementation may return 0 as placeholder)
    ASSERT(result.getValue() >= 0);
}

TEST_F(HCIndexCollectionManagerTest, Deduplication) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    createOpsCollections(collectionUUID);

    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    HCIndexCollectionManager manager(opCtx, dbName, collectionUUID, DictionaryGranularity::HOURLY);
    ASSERT_OK(manager.initializeForRead(opCtx));

    BSONObj metadata = BSON("region" << "us-east" << "env" << "prod");
    Timestamp ts(1, 0);

    auto result1 = manager.encodeMetadata(opCtx, metadata, ts);
    auto result2 = manager.encodeMetadata(opCtx, metadata, ts);

    ASSERT_OK(result1.getStatus());
    ASSERT_OK(result2.getStatus());
    ASSERT_EQ(result1.getValue(), result2.getValue());
}

}  // namespace mongo::timeseries::hcindex

