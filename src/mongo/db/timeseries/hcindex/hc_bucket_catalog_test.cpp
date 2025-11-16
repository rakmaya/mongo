/**
 *    Copyright (C) 2025-present MongoDB, Inc.
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
 *    all of the code used herein. If you modify file(s) with this exception,
 *    you may extend this exception to your version of the file(s), but you are
 *    not obligated to do so. If you do not wish to do so, delete this
 *    exception statement from your version of the file(s) and its LICENSE file.
 */

#include "mongo/db/timeseries/hcindex/hc_bucket_catalog.h"
#include "mongo/db/timeseries/hcindex/hcindex_collection_manager.h"
#include "mongo/db/timeseries/hcindex/hc_batch.h"
#include "mongo/db/local_catalog/catalog_test_fixture.h"
#include "mongo/unittest/unittest.h"
#include "mongo/bson/bson_validate.h"

namespace mongo::timeseries::hcindex {

class HCBucketCatalogTest : public CatalogTestFixture {
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

    void createOpsCollections(const UUID& collectionUUID) {
        auto symbolNss = getSymbolOpsNamespace(collectionUUID);
        auto attributeNss = getAttributeOpsNamespace(collectionUUID);
        ASSERT_OK(storageInterface()->createCollection(operationContext(), symbolNss, CollectionOptions()));
        ASSERT_OK(storageInterface()->createCollection(operationContext(), attributeNss, CollectionOptions()));
    }

    TimeseriesOptions getTimeseriesOptions() {
        TimeseriesOptions opts;
        opts.setTimeField("timestamp");
        opts.setMetaField(boost::optional<std::string>("metadata"));
        return opts;
    }

    std::vector<BSONObj> createTestMeasurements() {
        std::vector<BSONObj> measurements;
        // Use Date_t for timestamp field (milliseconds since epoch)
        Date_t date1 = Date_t::fromMillisSinceEpoch(1763254800000LL);
        Date_t date2 = Date_t::fromMillisSinceEpoch(1763254801000LL);
        Date_t date3 = Date_t::fromMillisSinceEpoch(1763254802000LL);

        // Metadata fields should be flat with string values (not nested objects)
        // The "metadata" field contains the actual metadata object with string values
        measurements.push_back(BSON("timestamp" << date1 << "metadata" << BSON("region" << "us-east" << "env" << "prod") << "value" << 100));
        measurements.push_back(BSON("timestamp" << date2 << "metadata" << BSON("region" << "us-west" << "env" << "prod") << "value" << 200));
        measurements.push_back(BSON("timestamp" << date3 << "metadata" << BSON("region" << "us-east" << "env" << "staging") << "value" << 150));
        return measurements;
    }
};

TEST_F(HCBucketCatalogTest, ConstructorInitializesMembers) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    createOpsCollections(collectionUUID);

    auto manager = std::make_shared<HCIndexCollectionManager>(opCtx, collectionUUID, DictionaryGranularity::HOURLY);
    ASSERT_OK(manager->initialize());

    auto opts = getTimeseriesOptions();
    HCBucketCatalog catalog(opCtx, collectionUUID, opts, manager);
    // Constructor should not throw
}

TEST_F(HCBucketCatalogTest, StageInsertBatchGroupsByTimeWindow) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    createOpsCollections(collectionUUID);

    auto manager = std::make_shared<HCIndexCollectionManager>(opCtx, collectionUUID, DictionaryGranularity::HOURLY);
    ASSERT_OK(manager->initialize());

    auto opts = getTimeseriesOptions();
    HCBucketCatalog catalog(opCtx, collectionUUID, opts, manager);

    auto measurements = createTestMeasurements();
    auto result = catalog.stageInsertBatch(OperationId(1), measurements, OID::gen());

    ASSERT_OK(result.getStatus());
    auto batches = result.getValue();
    ASSERT_GT(batches.size(), 0);
    // All measurements should be in same time window (HOURLY)
    ASSERT_EQ(batches.size(), 1);
}

TEST_F(HCBucketCatalogTest, StageInsertBatchEncodesMetadata) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    createOpsCollections(collectionUUID);

    auto manager = std::make_shared<HCIndexCollectionManager>(opCtx, collectionUUID, DictionaryGranularity::HOURLY);
    ASSERT_OK(manager->initialize());

    auto opts = getTimeseriesOptions();
    HCBucketCatalog catalog(opCtx, collectionUUID, opts, manager);

    auto measurements = createTestMeasurements();
    auto result = catalog.stageInsertBatch(OperationId(1), measurements, OID::gen());

    ASSERT_OK(result.getStatus());
    auto batches = result.getValue();
    ASSERT_EQ(batches.size(), 1);
    
    auto batch = batches[0];
    ASSERT_EQ(batch->measurements.size(), measurements.size());
    ASSERT_EQ(batch->rowIds.size(), measurements.size());
    
    // Verify rowIds are valid (non-negative)
    for (auto rowId : batch->rowIds) {
        ASSERT(rowId >= 0);
    }
}

TEST_F(HCBucketCatalogTest, StageInsertBatchRejectsEmptyMeasurements) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    createOpsCollections(collectionUUID);

    auto manager = std::make_shared<HCIndexCollectionManager>(opCtx, collectionUUID, DictionaryGranularity::HOURLY);
    ASSERT_OK(manager->initialize());

    auto opts = getTimeseriesOptions();
    HCBucketCatalog catalog(opCtx, collectionUUID, opts, manager);

    std::vector<BSONObj> emptyMeasurements;
    auto result = catalog.stageInsertBatch(OperationId(1), emptyMeasurements, OID::gen());

    ASSERT_NOT_OK(result.getStatus());
}

TEST_F(HCBucketCatalogTest, StageInsertBatchRejectsNullManager) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();

    auto opts = getTimeseriesOptions();
    HCBucketCatalog catalog(opCtx, collectionUUID, opts, nullptr);

    auto measurements = createTestMeasurements();
    auto result = catalog.stageInsertBatch(OperationId(1), measurements, OID::gen());

    ASSERT_NOT_OK(result.getStatus());
}

TEST_F(HCBucketCatalogTest, StageInsertBatchHandlesMissingTimeField) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    createOpsCollections(collectionUUID);

    auto manager = std::make_shared<HCIndexCollectionManager>(opCtx, collectionUUID, DictionaryGranularity::HOURLY);
    ASSERT_OK(manager->initialize());

    auto opts = getTimeseriesOptions();
    HCBucketCatalog catalog(opCtx, collectionUUID, opts, manager);

    // Measurement without timestamp field
    std::vector<BSONObj> measurements;
    measurements.push_back(BSON("value" << 100));

    auto result = catalog.stageInsertBatch(OperationId(1), measurements, OID::gen());
    ASSERT_NOT_OK(result.getStatus());
}

TEST_F(HCBucketCatalogTest, PrepareCommitTransitionsBatch) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    createOpsCollections(collectionUUID);

    auto manager = std::make_shared<HCIndexCollectionManager>(opCtx, collectionUUID, DictionaryGranularity::HOURLY);
    ASSERT_OK(manager->initialize());

    auto opts = getTimeseriesOptions();
    HCBucketCatalog catalog(opCtx, collectionUUID, opts, manager);

    auto measurements = createTestMeasurements();
    auto result = catalog.stageInsertBatch(OperationId(1), measurements, OID::gen());
    ASSERT_OK(result.getStatus());

    auto batches = result.getValue();
    auto status = catalog.prepareCommit(batches[0]);
    ASSERT_OK(status);
}

TEST_F(HCBucketCatalogTest, FinishCompletesPromise) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    createOpsCollections(collectionUUID);

    auto manager = std::make_shared<HCIndexCollectionManager>(opCtx, collectionUUID, DictionaryGranularity::HOURLY);
    ASSERT_OK(manager->initialize());

    auto opts = getTimeseriesOptions();
    HCBucketCatalog catalog(opCtx, collectionUUID, opts, manager);

    auto measurements = createTestMeasurements();
    auto result = catalog.stageInsertBatch(OperationId(1), measurements, OID::gen());
    ASSERT_OK(result.getStatus());

    auto batches = result.getValue();
    auto status = catalog.finish(batches[0]);
    ASSERT_OK(status);
}

TEST_F(HCBucketCatalogTest, AbortSetsErrorOnPromise) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    createOpsCollections(collectionUUID);

    auto manager = std::make_shared<HCIndexCollectionManager>(opCtx, collectionUUID, DictionaryGranularity::HOURLY);
    ASSERT_OK(manager->initialize());

    auto opts = getTimeseriesOptions();
    HCBucketCatalog catalog(opCtx, collectionUUID, opts, manager);

    auto measurements = createTestMeasurements();
    auto result = catalog.stageInsertBatch(OperationId(1), measurements, OID::gen());
    ASSERT_OK(result.getStatus());

    auto batches = result.getValue();
    Status errorStatus(ErrorCodes::InternalError, "Test error");
    auto status = catalog.abort(batches[0], errorStatus);
    ASSERT_OK(status);
}

TEST_F(HCBucketCatalogTest, ClearRemovesAllBatches) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    createOpsCollections(collectionUUID);

    auto manager = std::make_shared<HCIndexCollectionManager>(opCtx, collectionUUID, DictionaryGranularity::HOURLY);
    ASSERT_OK(manager->initialize());

    auto opts = getTimeseriesOptions();
    HCBucketCatalog catalog(opCtx, collectionUUID, opts, manager);

    auto measurements = createTestMeasurements();
    auto result = catalog.stageInsertBatch(OperationId(1), measurements, OID::gen());
    ASSERT_OK(result.getStatus());

    auto status = catalog.clear();
    ASSERT_OK(status);
}

TEST_F(HCBucketCatalogTest, TimeWindowCalculationHourly) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    createOpsCollections(collectionUUID);

    auto manager = std::make_shared<HCIndexCollectionManager>(opCtx, collectionUUID, DictionaryGranularity::HOURLY);
    ASSERT_OK(manager->initialize());

    auto opts = getTimeseriesOptions();
    HCBucketCatalog catalog(opCtx, collectionUUID, opts, manager);

    // Create measurements at different times within the same hour
    Date_t date1 = Date_t::fromMillisSinceEpoch(1763254800000LL);  // 00:00:00
    Date_t date2 = Date_t::fromMillisSinceEpoch(1763254801000LL);  // 00:00:01
    Date_t date3 = Date_t::fromMillisSinceEpoch(1763258399000LL);  // 00:59:59

    std::vector<BSONObj> measurements;
    measurements.push_back(BSON("timestamp" << date1 << "metadata" << BSON("region" << "us-east" << "env" << "prod") << "value" << 100));
    measurements.push_back(BSON("timestamp" << date2 << "metadata" << BSON("region" << "us-west" << "env" << "prod") << "value" << 200));
    measurements.push_back(BSON("timestamp" << date3 << "metadata" << BSON("region" << "us-east" << "env" << "staging") << "value" << 150));

    auto result = catalog.stageInsertBatch(OperationId(1), measurements, OID::gen());
    ASSERT_OK(result.getStatus());

    // All measurements should be in the same batch (same hour)
    auto batches = result.getValue();
    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0]->measurements.size(), 3);
}

TEST_F(HCBucketCatalogTest, TimeWindowCalculationDifferentHours) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    createOpsCollections(collectionUUID);

    auto manager = std::make_shared<HCIndexCollectionManager>(opCtx, collectionUUID, DictionaryGranularity::HOURLY);
    ASSERT_OK(manager->initialize());

    auto opts = getTimeseriesOptions();
    HCBucketCatalog catalog(opCtx, collectionUUID, opts, manager);

    // Create measurements in different hours
    Date_t date1 = Date_t::fromMillisSinceEpoch(1763254800000LL);  // Hour 1: 00:00:00
    Date_t date2 = Date_t::fromMillisSinceEpoch(1763258400000LL);  // Hour 2: 01:00:00

    std::vector<BSONObj> measurements;
    measurements.push_back(BSON("timestamp" << date1 << "metadata" << BSON("region" << "us-east" << "env" << "prod") << "value" << 100));
    measurements.push_back(BSON("timestamp" << date2 << "metadata" << BSON("region" << "us-west" << "env" << "prod") << "value" << 200));

    auto result = catalog.stageInsertBatch(OperationId(1), measurements, OID::gen());
    ASSERT_OK(result.getStatus());

    // Measurements should be in different batches (different hours)
    auto batches = result.getValue();
    ASSERT_EQ(batches.size(), 2);
    ASSERT_EQ(batches[0]->measurements.size(), 1);
    ASSERT_EQ(batches[1]->measurements.size(), 1);
}

TEST_F(HCBucketCatalogTest, TimeWindowCalculationFiveMinute) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    createOpsCollections(collectionUUID);

    auto manager = std::make_shared<HCIndexCollectionManager>(opCtx, collectionUUID, DictionaryGranularity::FIVE_MIN);
    ASSERT_OK(manager->initialize());

    auto opts = getTimeseriesOptions();
    HCBucketCatalog catalog(opCtx, collectionUUID, opts, manager);

    // Create measurements in different 5-minute windows
    Date_t date1 = Date_t::fromMillisSinceEpoch(1763254800000LL);  // 00:00:00 (window 0)
    Date_t date2 = Date_t::fromMillisSinceEpoch(1763255100000LL);  // 00:05:00 (window 1)
    Date_t date3 = Date_t::fromMillisSinceEpoch(1763255400000LL);  // 00:10:00 (window 2)

    std::vector<BSONObj> measurements;
    measurements.push_back(BSON("timestamp" << date1 << "metadata" << BSON("region" << "us-east" << "env" << "prod") << "value" << 100));
    measurements.push_back(BSON("timestamp" << date2 << "metadata" << BSON("region" << "us-west" << "env" << "prod") << "value" << 200));
    measurements.push_back(BSON("timestamp" << date3 << "metadata" << BSON("region" << "us-east" << "env" << "staging") << "value" << 150));

    auto result = catalog.stageInsertBatch(OperationId(1), measurements, OID::gen());
    ASSERT_OK(result.getStatus());

    // Measurements should be in different batches (different 5-minute windows)
    auto batches = result.getValue();
    ASSERT_EQ(batches.size(), 3);
    ASSERT_EQ(batches[0]->measurements.size(), 1);
    ASSERT_EQ(batches[1]->measurements.size(), 1);
    ASSERT_EQ(batches[2]->measurements.size(), 1);
}

TEST_F(HCBucketCatalogTest, EncodedRowIdsDecodeToOriginalMetadata) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    createOpsCollections(collectionUUID);

    auto manager = std::make_shared<HCIndexCollectionManager>(opCtx, collectionUUID, DictionaryGranularity::HOURLY);
    ASSERT_OK(manager->initialize());

    auto opts = getTimeseriesOptions();
    HCBucketCatalog catalog(opCtx, collectionUUID, opts, manager);

    // Create measurements with distinct metadata
    Date_t date1 = Date_t::fromMillisSinceEpoch(1763254800000LL);
    Date_t date2 = Date_t::fromMillisSinceEpoch(1763254801000LL);

    BSONObj metadata1 = BSON("region" << "us-east" << "env" << "prod");
    BSONObj metadata2 = BSON("region" << "us-west" << "env" << "staging");

    std::vector<BSONObj> measurements;
    measurements.push_back(BSON("timestamp" << date1 << "metadata" << metadata1 << "value" << 100));
    measurements.push_back(BSON("timestamp" << date2 << "metadata" << metadata2 << "value" << 200));

    auto result = catalog.stageInsertBatch(OperationId(1), measurements, OID::gen());
    ASSERT_OK(result.getStatus());

    auto batches = result.getValue();
    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0]->rowIds.size(), 2);

    // Decode the rowIds back to metadata
    Timestamp ts1(1763254800, 0);
    Timestamp ts2(1763254801, 0);

    auto decodedResult1 = manager->decodeMetadata(batches[0]->rowIds[0], ts1);
    ASSERT_OK(decodedResult1.getStatus());
    auto decodedMetadata1 = decodedResult1.getValue();

    auto decodedResult2 = manager->decodeMetadata(batches[0]->rowIds[1], ts2);
    ASSERT_OK(decodedResult2.getStatus());
    auto decodedMetadata2 = decodedResult2.getValue();

    // Verify decoded metadata matches original
    ASSERT_BSONOBJ_EQ(decodedMetadata1, metadata1);
    ASSERT_BSONOBJ_EQ(decodedMetadata2, metadata2);
}

TEST_F(HCBucketCatalogTest, DuplicateMetadataProducesSameRowId) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    createOpsCollections(collectionUUID);

    auto manager = std::make_shared<HCIndexCollectionManager>(opCtx, collectionUUID, DictionaryGranularity::HOURLY);
    ASSERT_OK(manager->initialize());

    auto opts = getTimeseriesOptions();
    HCBucketCatalog catalog(opCtx, collectionUUID, opts, manager);

    // Create measurements with duplicate metadata
    Date_t date1 = Date_t::fromMillisSinceEpoch(1763254800000LL);
    Date_t date2 = Date_t::fromMillisSinceEpoch(1763254801000LL);
    Date_t date3 = Date_t::fromMillisSinceEpoch(1763254802000LL);

    BSONObj metadata = BSON("region" << "us-east" << "env" << "prod");

    std::vector<BSONObj> measurements;
    measurements.push_back(BSON("timestamp" << date1 << "metadata" << metadata << "value" << 100));
    measurements.push_back(BSON("timestamp" << date2 << "metadata" << metadata << "value" << 200));
    measurements.push_back(BSON("timestamp" << date3 << "metadata" << metadata << "value" << 300));

    auto result = catalog.stageInsertBatch(OperationId(1), measurements, OID::gen());
    ASSERT_OK(result.getStatus());

    auto batches = result.getValue();
    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0]->rowIds.size(), 3);

    // All rowIds should be the same (deduplication)
    ASSERT_EQ(batches[0]->rowIds[0], batches[0]->rowIds[1]);
    ASSERT_EQ(batches[0]->rowIds[1], batches[0]->rowIds[2]);

    // Verify decoded metadata matches original
    Timestamp ts(1763254800, 0);
    auto decodedResult = manager->decodeMetadata(batches[0]->rowIds[0], ts);
    ASSERT_OK(decodedResult.getStatus());
    ASSERT_BSONOBJ_EQ(decodedResult.getValue(), metadata);
}

}  // namespace mongo::timeseries::hcindex

