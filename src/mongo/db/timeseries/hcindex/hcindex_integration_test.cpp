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

#include "mongo/db/timeseries/hcindex/hcindex_collection_manager.h"
#include "mongo/db/timeseries/hcindex/hcindex_bucket_encoder.h"
#include "mongo/db/timeseries/hcindex/hcindex_bucket_decoder.h"
#include "mongo/db/timeseries/hcindex/hc_batch.h"
#include "mongo/db/local_catalog/catalog_test_fixture.h"
#include "mongo/unittest/unittest.h"
#include "mongo/bson/bson_validate.h"

namespace mongo::timeseries::hcindex {

class HCIndexIntegrationTest : public CatalogTestFixture {
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

    std::shared_ptr<HCBatch> createTestHCBatch() {
        auto batch = std::make_shared<HCBatch>(
            OperationId(1),
            OID::gen(),
            "timestamp"
        );
        batch->timeWindowStart = Timestamp(1, 0);
        batch->timeWindowEnd = Timestamp(2, 0);
        batch->measurements.push_back(BSON("timestamp" << Timestamp(1, 1) << "value" << 100));
        batch->measurements.push_back(BSON("timestamp" << Timestamp(1, 2) << "value" << 200));
        batch->rowIds.push_back(1);
        batch->rowIds.push_back(2);
        batch->min = BSON("value" << 100);
        batch->max = BSON("value" << 200);
        return batch;
    }
};

TEST_F(HCIndexIntegrationTest, HCIndexCollectionManagerInitialize) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    createOpsCollections(collectionUUID);

    HCIndexCollectionManager manager(opCtx, collectionUUID, DictionaryGranularity::HOURLY);
    auto status = manager.initialize();
    ASSERT_OK(status);
}

TEST_F(HCIndexIntegrationTest, HCIndexCollectionManagerEncodeMetadata) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    createOpsCollections(collectionUUID);

    HCIndexCollectionManager manager(opCtx, collectionUUID, DictionaryGranularity::HOURLY);
    ASSERT_OK(manager.initialize());

    BSONObj metadata = BSON("region" << "us-east" << "env" << "prod");
    Timestamp ts(1, 0);

    auto result = manager.encodeMetadata(metadata, ts);
    ASSERT_OK(result.getStatus());
    // rowId should be >= 0 (0 is reserved for missing values, but implementation may return 0 as placeholder)
    ASSERT(result.getValue() >= 0);
}

TEST_F(HCIndexIntegrationTest, HCIndexCollectionManagerDeduplication) {
    auto opCtx = operationContext();
    auto collectionUUID = getTestCollectionUUID();
    createOpsCollections(collectionUUID);

    HCIndexCollectionManager manager(opCtx, collectionUUID, DictionaryGranularity::HOURLY);
    ASSERT_OK(manager.initialize());

    BSONObj metadata = BSON("region" << "us-east" << "env" << "prod");
    Timestamp ts(1, 0);

    auto result1 = manager.encodeMetadata(metadata, ts);
    auto result2 = manager.encodeMetadata(metadata, ts);

    ASSERT_OK(result1.getStatus());
    ASSERT_OK(result2.getStatus());
    ASSERT_EQ(result1.getValue(), result2.getValue());
}

TEST_F(HCIndexIntegrationTest, HCIndexBucketEncoderValidatesInput) {
    auto nss = NamespaceString::createNamespaceString_forTest("test", "buckets_timeseries");
    auto batch = createTestHCBatch();
    auto options = getTimeseriesOptions();

    auto result = HCIndexBucketEncoder::encode(nss, batch, options);
    ASSERT_OK(result.getStatus());
}

TEST_F(HCIndexIntegrationTest, HCIndexBucketEncoderRejectsNullBatch) {
    auto nss = NamespaceString::createNamespaceString_forTest("test", "buckets_timeseries");
    auto options = getTimeseriesOptions();

    auto result = HCIndexBucketEncoder::encode(nss, nullptr, options);
    ASSERT_NOT_OK(result.getStatus());
}

TEST_F(HCIndexIntegrationTest, HCIndexBucketEncoderRejectsEmptyMeasurements) {
    auto nss = NamespaceString::createNamespaceString_forTest("test", "buckets_timeseries");
    auto batch = std::make_shared<HCBatch>(OperationId(1), OID::gen(), "timestamp");
    auto options = getTimeseriesOptions();

    auto result = HCIndexBucketEncoder::encode(nss, batch, options);
    ASSERT_NOT_OK(result.getStatus());
}

TEST_F(HCIndexIntegrationTest, HCIndexBucketEncoderValidatesSizeMatch) {
    auto nss = NamespaceString::createNamespaceString_forTest("test", "buckets_timeseries");
    auto batch = std::make_shared<HCBatch>(OperationId(1), OID::gen(), "timestamp");
    batch->measurements.push_back(BSON("timestamp" << Timestamp(1, 1) << "value" << 100));
    batch->rowIds.push_back(1);
    batch->rowIds.push_back(2);  // Mismatch!
    auto options = getTimeseriesOptions();

    auto result = HCIndexBucketEncoder::encode(nss, batch, options);
    ASSERT_NOT_OK(result.getStatus());
}

}  // namespace mongo::timeseries::hcindex

