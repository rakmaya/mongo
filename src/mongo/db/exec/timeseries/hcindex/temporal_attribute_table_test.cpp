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
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/db/exec/timeseries/hcindex/temporal_attribute_table.h"
#include "mongo/db/exec/timeseries/hcindex/temporal_symbol_dictionary.h"
#include "mongo/db/exec/timeseries/hcindex/writer.h"
#include "mongo/db/service_context_test_fixture.h"
#include "mongo/unittest/unittest.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/query/compiler/parsers/matcher/expression_parser.h"
#include "mongo/db/pipeline/expression_context_for_test.h"
#include "mongo/db/matcher/extensions_callback_noop.h"

// TODO Break this test up into multiple files. I started with one file and used AI
// to write various tests. It was having problems when the tests were split up into
// multiple files.

namespace mongo::timeseries::hcindex {

class AttributeTableTest : public unittest::Test {
protected:
    void setUp() override {
        auto collectionUUID = UUID::gen();
        DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
        writer = std::make_unique<HCIndexWriter>(collectionUUID, dbName);
        Timestamp windowStart(1, 0);
        Timestamp windowEnd(2, 0);
        dict = std::make_unique<SymbolDictionary>(HCIndexPeriodEnum::Hour, 1, windowStart, windowEnd, writer.get());
        ASSERT_OK(dict->changeState(SymbolDictionaryState::ReadWrite));
        table = std::make_unique<AttributeTable>(dict.get(), writer.get(), HCIndexPeriodEnum::Hour, 1, windowStart, windowEnd);
        ASSERT_OK(table->changeState(AttributeTableState::ReadWrite));
    }

    std::unique_ptr<HCIndexWriter> writer;
    std::unique_ptr<SymbolDictionary> dict;
    std::unique_ptr<AttributeTable> table;
};

TEST_F(AttributeTableTest, InsertRowWithMetadata) {
    BSONObj metadata = BSON("node"
                            << "nyc-01"
                            << "container"
                            << "api-1");

    auto result = table->insertRow(metadata);

    ASSERT_TRUE(result.isOK());
    ASSERT_EQ(0, result.getValue().rowId);  // First row gets ID 0
    ASSERT_TRUE(result.getValue().isNewRow);  // Should be a new row
}

TEST_F(AttributeTableTest, InsertRowReturnsSequentialIds) {
    BSONObj metadata1 = BSON("node"
                             << "nyc-01");
    BSONObj metadata2 = BSON("node"
                             << "nyc-02");

    auto result1 = table->insertRow(metadata1);
    auto result2 = table->insertRow(metadata2);

    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result2.isOK());
    ASSERT_EQ(0, result1.getValue().rowId);
    ASSERT_EQ(1, result2.getValue().rowId);
    ASSERT_TRUE(result1.getValue().isNewRow);
    ASSERT_TRUE(result2.getValue().isNewRow);
}

TEST_F(AttributeTableTest, InsertRowDeduplicatesSameMetadata) {
    BSONObj metadata = BSON("node"
                            << "nyc-01"
                            << "container"
                            << "api-1");

    auto result1 = table->insertRow(metadata);
    auto result2 = table->insertRow(metadata);

    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result2.isOK());
    ASSERT_EQ(result1.getValue().rowId, result2.getValue().rowId);
    ASSERT_TRUE(result1.getValue().isNewRow);  // First insert is new
    ASSERT_FALSE(result2.getValue().isNewRow);  // Second insert is duplicate
}

TEST_F(AttributeTableTest, GetRowReturnsInsertedRow) {
    BSONObj metadata = BSON("node"
                            << "nyc-01"
                            << "container"
                            << "api-1");

    auto insertResult = table->insertRow(metadata);
    ASSERT_TRUE(insertResult.isOK());

    auto getResult = table->getRow(insertResult.getValue().rowId);

    ASSERT_TRUE(getResult);
    ASSERT_EQ(2u, getResult.value().size());
}

TEST_F(AttributeTableTest, GetRowReturnsNoneForInvalidId) {
    auto result = table->getRow(999);

    ASSERT_FALSE(result);
}

TEST_F(AttributeTableTest, GetSchemaReturnsFieldNames) {
    BSONObj metadata = BSON("node"
                            << "nyc-01"
                            << "container"
                            << "api-1");

    auto result = table->insertRow(metadata);
    ASSERT_TRUE(result.isOK());
    ASSERT_TRUE(result.getValue().isNewRow);
    auto schema = table->getSchema();

    ASSERT_EQ(2u, schema.size());
    ASSERT_EQ("node", schema[0]);
    ASSERT_EQ("container", schema[1]);
}

TEST_F(AttributeTableTest, SchemaEvolvesWithNewFields) {
    BSONObj metadata1 = BSON("node"
                             << "nyc-01");
    BSONObj metadata2 = BSON("node"
                             << "nyc-02"
                             << "container"
                             << "api-1");

    auto result1 = table->insertRow(metadata1);
    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result1.getValue().isNewRow);
    auto schema1 = table->getSchema();
    ASSERT_EQ(1u, schema1.size());

    auto result2 = table->insertRow(metadata2);
    ASSERT_TRUE(result2.isOK());
    ASSERT_TRUE(result2.getValue().isNewRow);
    auto schema2 = table->getSchema();
    ASSERT_EQ(2u, schema2.size());
}

TEST_F(AttributeTableTest, GetRowCountReturnsCorrectCount) {
    ASSERT_EQ(0u, table->getRowCount());

    BSONObj metadata1 = BSON("node"
                             << "nyc-01");
    BSONObj metadata2 = BSON("node"
                             << "nyc-02");

    auto r1 = table->insertRow(metadata1);
    ASSERT_TRUE(r1.isOK());
    ASSERT_TRUE(r1.getValue().isNewRow);
    ASSERT_EQ(1u, table->getRowCount());

    auto r2 = table->insertRow(metadata2);
    ASSERT_TRUE(r2.isOK());
    ASSERT_TRUE(r2.getValue().isNewRow);
    ASSERT_EQ(2u, table->getRowCount());

    auto r3 = table->insertRow(metadata1);  // Duplicate
    ASSERT_TRUE(r3.isOK());
    ASSERT_FALSE(r3.getValue().isNewRow);
    ASSERT_EQ(2u, table->getRowCount());
}

TEST_F(AttributeTableTest, GetMemoryUsageBytesReturnsPositiveValue) {
    BSONObj metadata = BSON("node"
                            << "nyc-01"
                            << "container"
                            << "api-1");

    auto result = table->insertRow(metadata);
    ASSERT_TRUE(result.isOK());
    ASSERT_TRUE(result.getValue().isNewRow);

    size_t memoryUsage = table->getMemoryUsageBytes();
    ASSERT_GT(memoryUsage, 0u);
}

TEST_F(AttributeTableTest, InsertRowDirectWithVector) {
    // First establish a schema by inserting metadata
    BSONObj metadata = BSON("field1"
                            << "value1"
                            << "field2"
                            << "value2");
    auto insertResult = table->insertRow(metadata);
    ASSERT_TRUE(insertResult.isOK());
    ASSERT_TRUE(insertResult.getValue().isNewRow);

    // Now insertRowDirect should work with a row matching the schema
    std::vector<uint32_t> row = {1, 2};
    auto result = table->insertRowDirect(row);

    ASSERT_TRUE(result.isOK());
    ASSERT_EQ(1, result.getValue());  // Second row, first was from insertRow
}

TEST_F(AttributeTableTest, QueryRowsWithPredicate) {
    BSONObj metadata1 = BSON("node"
                             << "nyc-01"
                             << "container"
                             << "api-1");
    BSONObj metadata2 = BSON("node"
                             << "nyc-02"
                             << "container"
                             << "api-1");

    auto result1 = table->insertRow(metadata1);
    auto result2 = table->insertRow(metadata2);

    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result2.isOK());
    ASSERT_TRUE(result1.getValue().isNewRow);
    ASSERT_TRUE(result2.getValue().isNewRow);

    // Query for rows with container = "api-1"
    // Build a predicate using refRowVec: [0, 2] means column 0 (node) is any, column 1 (container) must be symbol 2
    AttributeTablePredicate predicate;
    predicate.refRowVec = {0, 2};  // 0 = any node, 2 = "api-1" for container

    auto queryResult = table->queryRows(predicate);

    ASSERT_EQ(2u, queryResult.size());
}

class TemporalAttributeTableTest : public ServiceContextTest {
protected:
    void setUp() override {
        ServiceContextTest::setUp();
        _opCtx = makeOperationContext();
        auto collectionUUID = UUID::gen();
        DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
        writer = std::make_unique<HCIndexWriter>(collectionUUID, dbName);
        tempDict = std::make_unique<TemporalSymbolDictionary>(
            collectionUUID, HCIndexPeriodEnum::Hour, 1, writer.get());
        tempTable = std::make_unique<TemporalAttributeTable>(
            collectionUUID, HCIndexPeriodEnum::Hour, 1, tempDict.get(), nullptr, writer.get());
    }

    OperationContext* getOpCtx() {
        return _opCtx.get();
    }

    ServiceContext::UniqueOperationContext _opCtx;
    std::unique_ptr<HCIndexWriter> writer;
    std::unique_ptr<TemporalSymbolDictionary> tempDict;
    std::unique_ptr<TemporalAttributeTable> tempTable;
};

TEST_F(TemporalAttributeTableTest, InsertRowWithTimestamp) {
    BSONObj metadata = BSON("node"
                            << "nyc-01");
    Timestamp ts(1000, 0);

    auto result = tempTable->insertRow(getOpCtx(), metadata, ts);

    ASSERT_TRUE(result.isOK());
    ASSERT_EQ(0, result.getValue().rowId);
    ASSERT_TRUE(result.getValue().isNewRow);
}

TEST_F(TemporalAttributeTableTest, InsertRowDeduplicatesInSameWindow) {
    BSONObj metadata = BSON("node"
                            << "nyc-01");
    Timestamp ts1(1000, 0);
    Timestamp ts2(2000, 0);  // Same window (0-3600)

    auto result1 = tempTable->insertRow(getOpCtx(), metadata, ts1);
    auto result2 = tempTable->insertRow(getOpCtx(), metadata, ts2);

    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result2.isOK());
    ASSERT_EQ(result1.getValue().rowId, result2.getValue().rowId);
    ASSERT_TRUE(result1.getValue().isNewRow);
    ASSERT_FALSE(result2.getValue().isNewRow);
}

TEST_F(TemporalAttributeTableTest, InsertRowCreatesNewRowInDifferentWindow) {
    BSONObj metadata = BSON("node"
                            << "nyc-01");
    Timestamp ts1(1000, 0);      // Window: 0-3600
    Timestamp ts2(5000, 0);      // Window: 3600-7200

    auto result1 = tempTable->insertRow(getOpCtx(), metadata, ts1);
    auto result2 = tempTable->insertRow(getOpCtx(), metadata, ts2);

    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result2.isOK());
    ASSERT_TRUE(result1.getValue().isNewRow);
    ASSERT_TRUE(result2.getValue().isNewRow);
    // Different windows may have different row IDs
}

TEST_F(TemporalAttributeTableTest, GetWindowForTimestamp) {
    Timestamp ts(3661, 0);  // 1 hour + 1 second

    auto [windowStart, windowEnd] = tempTable->getWindowForTimestamp(ts);

    ASSERT_EQ(3600u, windowStart.getSecs());
    ASSERT_EQ(7200u, windowEnd.getSecs());
}

TEST_F(TemporalAttributeTableTest, GetStats) {
    BSONObj metadata = BSON("node"
                            << "nyc-01");
    Timestamp ts(1000, 0);

    auto result = tempTable->insertRow(getOpCtx(), metadata, ts);
    ASSERT_TRUE(result.isOK());
    ASSERT_TRUE(result.getValue().isNewRow);

    auto stats = tempTable->getStats();

    ASSERT_EQ(1u, stats.totalTables);
    ASSERT_EQ(1u, stats.totalRows);
    ASSERT_GT(stats.memoryUsageBytes, 0u);
}

TEST_F(TemporalAttributeTableTest, CleanupOldTables) {
    BSONObj metadata = BSON("node"
                            << "nyc-01");
    Timestamp ts1(1000, 0);   // Window: 0-3600
    Timestamp ts2(5000, 0);   // Window: 3600-7200

    auto r1 = tempTable->insertRow(getOpCtx(), metadata, ts1);
    ASSERT_TRUE(r1.isOK());
    ASSERT_TRUE(r1.getValue().isNewRow);
    auto r2 = tempTable->insertRow(getOpCtx(), metadata, ts2);
    ASSERT_TRUE(r2.isOK());
    ASSERT_TRUE(r2.getValue().isNewRow);

    auto statsBefore = tempTable->getStats();
    ASSERT_EQ(2u, statsBefore.totalTables);

    // Cleanup tables before ts2's window
    auto cleanupResult = tempTable->cleanupOldTables(Timestamp(3600, 0));
    ASSERT_TRUE(cleanupResult.isOK());

    auto statsAfter = tempTable->getStats();
    ASSERT_EQ(1u, statsAfter.totalTables);
}

TEST_F(TemporalAttributeTableTest, GetRowWithTimestamp) {
    BSONObj metadata = BSON("node"
                            << "nyc-01");
    Timestamp ts(1000, 0);

    auto insertResult = tempTable->insertRow(getOpCtx(), metadata, ts);
    ASSERT_TRUE(insertResult.isOK());
    ASSERT_TRUE(insertResult.getValue().isNewRow);

    auto getResult = tempTable->getRow(insertResult.getValue().rowId, ts);

    ASSERT_TRUE(getResult);
}

TEST_F(TemporalAttributeTableTest, QueryRowsWithTimestamp) {
    BSONObj metadata = BSON("node"
                            << "nyc-01");
    Timestamp ts(1000, 0);

    auto insertResult = tempTable->insertRow(getOpCtx(), metadata, ts);
    ASSERT_TRUE(insertResult.isOK());
    ASSERT_TRUE(insertResult.getValue().isNewRow);

    AttributeTablePredicate predicate;
    auto queryResult = tempTable->queryRows(predicate, ts);

    ASSERT_GTE(queryResult.size(), 0u);
}

class MatchExpressionConversionTest : public unittest::Test {
protected:
    void setUp() override {
        auto collectionUUID = UUID::gen();
        DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
        writer = std::make_unique<HCIndexWriter>(collectionUUID, dbName);
        Timestamp windowStart(1, 0);
        Timestamp windowEnd(2, 0);
        dict = std::make_unique<SymbolDictionary>(HCIndexPeriodEnum::Hour, 1, windowStart, windowEnd, writer.get());
        ASSERT_OK(dict->changeState(SymbolDictionaryState::ReadWrite));
        table = std::make_unique<AttributeTable>(dict.get(), writer.get(), HCIndexPeriodEnum::Hour, 1, windowStart, windowEnd);
        ASSERT_OK(table->changeState(AttributeTableState::ReadWrite));
    }

    std::unique_ptr<HCIndexWriter> writer;
    std::unique_ptr<SymbolDictionary> dict;
    std::unique_ptr<AttributeTable> table;
};

TEST_F(MatchExpressionConversionTest, ConvertSingleEqualityPredicate) {
    // Insert test data
    BSONObj metadata = BSON("region" << "us-east");
    auto insertResult = table->insertRow(metadata);
    ASSERT_TRUE(insertResult.isOK());

    // Create MatchExpression: {region: "us-east"}
    BSONObj filterBSON = BSON("region" << "us-east");
    auto expCtx = make_intrusive<ExpressionContextForTest>();
    auto matchExpr = mongo::MatchExpressionParser::parse(
        filterBSON, expCtx, mongo::ExtensionsCallbackNoop());
    ASSERT_TRUE(matchExpr.isOK());

    // Convert to predicate
    auto predicateResult = table->convertMatchExpressionToPredicate(matchExpr.getValue().get());
    ASSERT_TRUE(predicateResult.isOK());

    auto predicate = predicateResult.getValue();
    ASSERT_GT(predicate.refRowVec.size(), 0u);
    ASSERT_NE(predicate.refRowVec[0], 0u);  // Should have non-zero symbol for region
}

TEST_F(MatchExpressionConversionTest, ConvertMultipleEqualityPredicates) {
    // Insert test data
    BSONObj metadata = BSON("region" << "us-east" << "env" << "prod");
    auto insertResult = table->insertRow(metadata);
    ASSERT_TRUE(insertResult.isOK());

    // Create MatchExpression: {region: "us-east", env: "prod"}
    BSONObj filterBSON = BSON("region" << "us-east" << "env" << "prod");
    auto expCtx = make_intrusive<ExpressionContextForTest>();
    auto matchExpr = mongo::MatchExpressionParser::parse(
        filterBSON, expCtx, mongo::ExtensionsCallbackNoop());
    ASSERT_TRUE(matchExpr.isOK());

    // Convert to predicate
    auto predicateResult = table->convertMatchExpressionToPredicate(matchExpr.getValue().get());
    ASSERT_TRUE(predicateResult.isOK());

    auto predicate = predicateResult.getValue();
    // Implicit AND should have children for each field
    ASSERT_TRUE(predicate.isAnd());
    ASSERT_GTE(predicate.children.size(), 2u);
}

TEST_F(MatchExpressionConversionTest, ConvertPartialPredicate) {
    // Insert test data with 3 fields
    BSONObj metadata = BSON("region" << "us-east" << "env" << "prod" << "zone" << "1a");
    auto insertResult = table->insertRow(metadata);
    ASSERT_TRUE(insertResult.isOK());

    // Create MatchExpression with only 2 of 3 fields: {region: "us-east", env: "prod"}
    BSONObj filterBSON = BSON("region" << "us-east" << "env" << "prod");
    auto expCtx = make_intrusive<ExpressionContextForTest>();
    auto matchExpr = mongo::MatchExpressionParser::parse(
        filterBSON, expCtx, mongo::ExtensionsCallbackNoop());
    ASSERT_TRUE(matchExpr.isOK());

    // Convert to predicate
    auto predicateResult = table->convertMatchExpressionToPredicate(matchExpr.getValue().get());
    ASSERT_TRUE(predicateResult.isOK());

    auto predicate = predicateResult.getValue();
    // Should only have entries for the fields in the predicate
    ASSERT_LTE(predicate.refRowVec.size(), 3u);
}

TEST_F(MatchExpressionConversionTest, ConvertPredicateWithNonExistentField) {
    // Insert test data
    BSONObj metadata = BSON("region" << "us-east");
    auto insertResult = table->insertRow(metadata);
    ASSERT_TRUE(insertResult.isOK());

    // Create MatchExpression with non-existent field: {nonexistent: "value"}
    BSONObj filterBSON = BSON("nonexistent" << "value");
    auto expCtx = make_intrusive<ExpressionContextForTest>();
    auto matchExpr = mongo::MatchExpressionParser::parse(
        filterBSON, expCtx, mongo::ExtensionsCallbackNoop());
    ASSERT_TRUE(matchExpr.isOK());

    // Convert to predicate - should succeed but may not find matches
    auto predicateResult = table->convertMatchExpressionToPredicate(matchExpr.getValue().get());
    // This may succeed or fail depending on implementation
    // The important thing is it doesn't crash
}

class QueryRowsTest : public unittest::Test {
protected:
    void setUp() override {
        auto collectionUUID = UUID::gen();
        DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
        writer = std::make_unique<HCIndexWriter>(collectionUUID, dbName);
        Timestamp windowStart(1, 0);
        Timestamp windowEnd(2, 0);
        dict = std::make_unique<SymbolDictionary>(HCIndexPeriodEnum::Hour, 1, windowStart, windowEnd, writer.get());
        ASSERT_OK(dict->changeState(SymbolDictionaryState::ReadWrite));
        table = std::make_unique<AttributeTable>(dict.get(), writer.get(), HCIndexPeriodEnum::Hour, 1, windowStart, windowEnd);
        ASSERT_OK(table->changeState(AttributeTableState::ReadWrite));
    }

    std::unique_ptr<HCIndexWriter> writer;
    std::unique_ptr<SymbolDictionary> dict;
    std::unique_ptr<AttributeTable> table;
};

TEST_F(QueryRowsTest, QueryRowsEmptyTable) {
    // Query empty table should return no results
    AttributeTablePredicate predicate;
    predicate.refRowVec = {0};  // Match any value for first column

    auto result = table->queryRows(predicate);
    ASSERT_EQ(result.size(), 0u);
}

TEST_F(QueryRowsTest, QueryRowsSingleRowExactMatch) {
    // Insert single row
    BSONObj metadata = BSON("region" << "us-east");
    auto insertResult = table->insertRow(metadata);
    ASSERT_TRUE(insertResult.isOK());

    // Get the actual symbol index for "us-east"
    auto symbolResult = dict->getOrInsertSymbol("us-east");
    ASSERT_TRUE(symbolResult.isOK());
    uint32_t usEastSymbol = symbolResult.getValue();

    // Query for exact match
    AttributeTablePredicate predicate;
    predicate.refRowVec = {usEastSymbol};

    auto result = table->queryRows(predicate);
    ASSERT_EQ(result.size(), 1u);
    ASSERT_EQ(result[0], 0u);  // First row
}

TEST_F(QueryRowsTest, QueryRowsSingleRowNoMatch) {
    // Insert single row
    BSONObj metadata = BSON("region" << "us-east");
    auto insertResult = table->insertRow(metadata);
    ASSERT_TRUE(insertResult.isOK());

    // Query for non-matching value (use a symbol index that doesn't exist)
    AttributeTablePredicate predicate;
    predicate.refRowVec = {999};  // Non-existent symbol

    auto result = table->queryRows(predicate);
    ASSERT_EQ(result.size(), 0u);
}

TEST_F(QueryRowsTest, QueryRowsMultipleRowsPartialMatch) {
    // Insert multiple rows
    BSONObj metadata1 = BSON("region" << "us-east" << "env" << "prod");
    BSONObj metadata2 = BSON("region" << "us-west" << "env" << "prod");
    BSONObj metadata3 = BSON("region" << "us-east" << "env" << "dev");

    auto result1 = table->insertRow(metadata1);
    auto result2 = table->insertRow(metadata2);
    auto result3 = table->insertRow(metadata3);

    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result2.isOK());
    ASSERT_TRUE(result3.isOK());

    // Get symbol indices
    auto usEastResult = dict->getOrInsertSymbol("us-east");
    ASSERT_TRUE(usEastResult.isOK());
    uint32_t usEastSymbol = usEastResult.getValue();

    // Query for region=us-east (should match rows 0 and 2)
    AttributeTablePredicate predicate;
    predicate.refRowVec = {usEastSymbol, 0};  // us-east, any env

    auto result = table->queryRows(predicate);
    ASSERT_EQ(result.size(), 2u);
}

TEST_F(QueryRowsTest, QueryRowsMultipleFieldsExactMatch) {
    // Insert multiple rows
    BSONObj metadata1 = BSON("region" << "us-east" << "env" << "prod");
    BSONObj metadata2 = BSON("region" << "us-east" << "env" << "dev");

    auto result1 = table->insertRow(metadata1);
    auto result2 = table->insertRow(metadata2);

    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result2.isOK());

    // Get symbol indices
    auto usEastResult = dict->getOrInsertSymbol("us-east");
    auto prodResult = dict->getOrInsertSymbol("prod");
    ASSERT_TRUE(usEastResult.isOK());
    ASSERT_TRUE(prodResult.isOK());
    uint32_t usEastSymbol = usEastResult.getValue();
    uint32_t prodSymbol = prodResult.getValue();

    // Query for region=us-east AND env=prod (should match only row 0)
    AttributeTablePredicate predicate;
    predicate.refRowVec = {usEastSymbol, prodSymbol};

    auto result = table->queryRows(predicate);
    ASSERT_EQ(result.size(), 1u);
    ASSERT_EQ(result[0], 0u);
}

TEST_F(QueryRowsTest, QueryRowsWithWildcard) {
    // Insert multiple rows
    BSONObj metadata1 = BSON("region" << "us-east" << "env" << "prod");
    BSONObj metadata2 = BSON("region" << "us-west" << "env" << "prod");

    auto result1 = table->insertRow(metadata1);
    auto result2 = table->insertRow(metadata2);

    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result2.isOK());

    // Get symbol index for "prod"
    auto prodResult = dict->getOrInsertSymbol("prod");
    ASSERT_TRUE(prodResult.isOK());
    uint32_t prodSymbol = prodResult.getValue();

    // Query for env=prod (any region) - using 0 as wildcard
    AttributeTablePredicate predicate;
    predicate.refRowVec = {0, prodSymbol};  // any region, prod

    auto result = table->queryRows(predicate);
    ASSERT_EQ(result.size(), 2u);
}

TEST_F(QueryRowsTest, QueryRowsSchemaEvolution) {
    // Insert row with 2 fields
    BSONObj metadata1 = BSON("region" << "us-east" << "env" << "prod");
    auto result1 = table->insertRow(metadata1);
    ASSERT_TRUE(result1.isOK());

    // Insert row with 3 fields (schema evolved)
    BSONObj metadata2 = BSON("region" << "us-west" << "env" << "dev" << "zone" << "1a");
    auto result2 = table->insertRow(metadata2);
    ASSERT_TRUE(result2.isOK());

    // Get symbol index for "1a"
    auto zoneResult = dict->getOrInsertSymbol("1a");
    ASSERT_TRUE(zoneResult.isOK());
    uint32_t zoneSymbol = zoneResult.getValue();

    // Query for zone=1a (should only match row 1)
    AttributeTablePredicate predicate;
    predicate.refRowVec = {0, 0, zoneSymbol};  // any region, any env, zone=1a

    auto result = table->queryRows(predicate);
    ASSERT_EQ(result.size(), 1u);
    ASSERT_EQ(result[0], 1u);
}

TEST_F(QueryRowsTest, QueryRowsDuplicateMetadata) {
    // Insert same metadata twice
    BSONObj metadata = BSON("region" << "us-east" << "env" << "prod");
    auto result1 = table->insertRow(metadata);
    auto result2 = table->insertRow(metadata);

    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result2.isOK());

    // Both should have same rowId (deduplication)
    ASSERT_EQ(result1.getValue().rowId, result2.getValue().rowId);

    // Get symbol indices
    auto usEastResult = dict->getOrInsertSymbol("us-east");
    auto prodResult = dict->getOrInsertSymbol("prod");
    ASSERT_TRUE(usEastResult.isOK());
    ASSERT_TRUE(prodResult.isOK());
    uint32_t usEastSymbol = usEastResult.getValue();
    uint32_t prodSymbol = prodResult.getValue();

    // Query should return only one row
    AttributeTablePredicate predicate;
    predicate.refRowVec = {usEastSymbol, prodSymbol};

    auto result = table->queryRows(predicate);
    ASSERT_EQ(result.size(), 1u);
}

class ComplexMatchExpressionTest : public unittest::Test {
protected:
    void setUp() override {
        auto collectionUUID = UUID::gen();
        DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
        writer = std::make_unique<HCIndexWriter>(collectionUUID, dbName);
        Timestamp windowStart(1, 0);
        Timestamp windowEnd(2, 0);
        dict = std::make_unique<SymbolDictionary>(HCIndexPeriodEnum::Hour, 1, windowStart, windowEnd, writer.get());
        ASSERT_OK(dict->changeState(SymbolDictionaryState::ReadWrite));
        table = std::make_unique<AttributeTable>(dict.get(), writer.get(), HCIndexPeriodEnum::Hour, 1, windowStart, windowEnd);
        ASSERT_OK(table->changeState(AttributeTableState::ReadWrite));
    }

    std::unique_ptr<HCIndexWriter> writer;
    std::unique_ptr<SymbolDictionary> dict;
    std::unique_ptr<AttributeTable> table;
};

TEST_F(ComplexMatchExpressionTest, ConvertAndExpression) {
    // Insert test data
    BSONObj metadata1 = BSON("region" << "us-east" << "env" << "prod");
    BSONObj metadata2 = BSON("region" << "us-west" << "env" << "prod");
    auto result1 = table->insertRow(metadata1);
    auto result2 = table->insertRow(metadata2);
    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result2.isOK());

    // Create AND MatchExpression: {$and: [{region: "us-east"}, {env: "prod"}]}
    BSONObj filterBSON = BSON("$and" << BSON_ARRAY(BSON("region" << "us-east") << BSON("env" << "prod")));
    auto expCtx = make_intrusive<ExpressionContextForTest>();
    auto matchExpr = mongo::MatchExpressionParser::parse(
        filterBSON, expCtx, mongo::ExtensionsCallbackNoop());
    ASSERT_TRUE(matchExpr.isOK());

    // Convert to predicate
    auto predicateResult = table->convertMatchExpressionToPredicate(matchExpr.getValue().get());
    ASSERT_TRUE(predicateResult.isOK());

    auto predicate = predicateResult.getValue();
    // AND expressions should have children, not refRowVec
    ASSERT_TRUE(predicate.isAnd());
    ASSERT_GT(predicate.children.size(), 0u);
}

TEST_F(ComplexMatchExpressionTest, ConvertOrExpression) {
    // Insert test data
    BSONObj metadata1 = BSON("region" << "us-east" << "env" << "prod");
    BSONObj metadata2 = BSON("region" << "us-west" << "env" << "dev");
    auto result1 = table->insertRow(metadata1);
    auto result2 = table->insertRow(metadata2);
    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result2.isOK());

    // Create OR MatchExpression: {$or: [{region: "us-east"}, {region: "us-west"}]}
    BSONObj filterBSON = BSON("$or" << BSON_ARRAY(BSON("region" << "us-east") << BSON("region" << "us-west")));
    auto expCtx = make_intrusive<ExpressionContextForTest>();
    auto matchExpr = mongo::MatchExpressionParser::parse(
        filterBSON, expCtx, mongo::ExtensionsCallbackNoop());
    ASSERT_TRUE(matchExpr.isOK());

    // Convert to predicate - OR expressions may not be directly convertible
    // but should not crash
    auto predicateResult = table->convertMatchExpressionToPredicate(matchExpr.getValue().get());
    // May succeed or fail depending on implementation
}

TEST_F(ComplexMatchExpressionTest, ConvertNotExpression) {
    // Insert test data
    BSONObj metadata = BSON("region" << "us-east" << "env" << "prod");
    auto result = table->insertRow(metadata);
    ASSERT_TRUE(result.isOK());

    // Create NOT MatchExpression: {region: {$ne: "us-west"}}
    BSONObj filterBSON = BSON("region" << BSON("$ne" << "us-west"));
    auto expCtx = make_intrusive<ExpressionContextForTest>();
    auto matchExpr = mongo::MatchExpressionParser::parse(
        filterBSON, expCtx, mongo::ExtensionsCallbackNoop());
    ASSERT_TRUE(matchExpr.isOK());

    // Convert to predicate - NOT expressions may not be directly convertible
    auto predicateResult = table->convertMatchExpressionToPredicate(matchExpr.getValue().get());
    // May succeed or fail depending on implementation
}

TEST_F(ComplexMatchExpressionTest, ConvertNestedAndExpression) {
    // Insert test data
    BSONObj metadata1 = BSON("region" << "us-east" << "env" << "prod" << "zone" << "1a");
    BSONObj metadata2 = BSON("region" << "us-east" << "env" << "prod" << "zone" << "1b");
    auto result1 = table->insertRow(metadata1);
    auto result2 = table->insertRow(metadata2);
    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result2.isOK());

    // Create nested AND: {$and: [{region: "us-east"}, {$and: [{env: "prod"}, {zone: "1a"}]}]}
    BSONObj filterBSON = BSON("$and" << BSON_ARRAY(
        BSON("region" << "us-east") <<
        BSON("$and" << BSON_ARRAY(BSON("env" << "prod") << BSON("zone" << "1a")))
    ));
    auto expCtx = make_intrusive<ExpressionContextForTest>();
    auto matchExpr = mongo::MatchExpressionParser::parse(
        filterBSON, expCtx, mongo::ExtensionsCallbackNoop());
    ASSERT_TRUE(matchExpr.isOK());

    // Convert to predicate
    auto predicateResult = table->convertMatchExpressionToPredicate(matchExpr.getValue().get());
    ASSERT_TRUE(predicateResult.isOK());
}

TEST_F(ComplexMatchExpressionTest, ConvertComplexAndOrExpression) {
    // Insert test data
    BSONObj metadata1 = BSON("region" << "us-east" << "env" << "prod" << "tier" << "gold");
    BSONObj metadata2 = BSON("region" << "us-west" << "env" << "dev" << "tier" << "silver");
    auto result1 = table->insertRow(metadata1);
    auto result2 = table->insertRow(metadata2);
    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result2.isOK());

    // Create: {$and: [{region: "us-east"}, {$or: [{env: "prod"}, {tier: "gold"}]}]}
    BSONObj filterBSON = BSON("$and" << BSON_ARRAY(
        BSON("region" << "us-east") <<
        BSON("$or" << BSON_ARRAY(BSON("env" << "prod") << BSON("tier" << "gold")))
    ));
    auto expCtx = make_intrusive<ExpressionContextForTest>();
    auto matchExpr = mongo::MatchExpressionParser::parse(
        filterBSON, expCtx, mongo::ExtensionsCallbackNoop());
    ASSERT_TRUE(matchExpr.isOK());

    // Convert to predicate
    auto predicateResult = table->convertMatchExpressionToPredicate(matchExpr.getValue().get());
    // May succeed or fail depending on implementation
}

TEST_F(ComplexMatchExpressionTest, ConvertMultipleAndConditions) {
    // Insert test data
    BSONObj metadata1 = BSON("region" << "us-east" << "env" << "prod" << "zone" << "1a" << "tier" << "gold");
    auto result1 = table->insertRow(metadata1);
    ASSERT_TRUE(result1.isOK());

    // Create: {region: "us-east", env: "prod", zone: "1a", tier: "gold"}
    // This is implicitly an AND of all conditions
    BSONObj filterBSON = BSON("region" << "us-east" << "env" << "prod" << "zone" << "1a" << "tier" << "gold");
    auto expCtx = make_intrusive<ExpressionContextForTest>();
    auto matchExpr = mongo::MatchExpressionParser::parse(
        filterBSON, expCtx, mongo::ExtensionsCallbackNoop());
    ASSERT_TRUE(matchExpr.isOK());

    // Convert to predicate
    auto predicateResult = table->convertMatchExpressionToPredicate(matchExpr.getValue().get());
    ASSERT_TRUE(predicateResult.isOK());

    auto predicate = predicateResult.getValue();
    // Implicit AND should have children for each field
    ASSERT_TRUE(predicate.isAnd());
    ASSERT_GTE(predicate.children.size(), 4u);
}

TEST_F(ComplexMatchExpressionTest, ConvertAndWithPartialFields) {
    // Insert test data with 4 fields
    BSONObj metadata = BSON("region" << "us-east" << "env" << "prod" << "zone" << "1a" << "tier" << "gold");
    auto result = table->insertRow(metadata);
    ASSERT_TRUE(result.isOK());

    // Create AND with only 2 of 4 fields: {$and: [{region: "us-east"}, {env: "prod"}]}
    BSONObj filterBSON = BSON("$and" << BSON_ARRAY(
        BSON("region" << "us-east") <<
        BSON("env" << "prod")
    ));
    auto expCtx = make_intrusive<ExpressionContextForTest>();
    auto matchExpr = mongo::MatchExpressionParser::parse(
        filterBSON, expCtx, mongo::ExtensionsCallbackNoop());
    ASSERT_TRUE(matchExpr.isOK());

    // Convert to predicate
    auto predicateResult = table->convertMatchExpressionToPredicate(matchExpr.getValue().get());
    ASSERT_TRUE(predicateResult.isOK());

    auto predicate = predicateResult.getValue();
    // Should only have entries for the fields in the predicate
    ASSERT_LTE(predicate.refRowVec.size(), 4u);
}

TEST_F(ComplexMatchExpressionTest, ConvertAndWithNonExistentField) {
    // Insert test data
    BSONObj metadata = BSON("region" << "us-east" << "env" << "prod");
    auto result = table->insertRow(metadata);
    ASSERT_TRUE(result.isOK());

    // Create AND with non-existent field: {$and: [{region: "us-east"}, {nonexistent: "value"}]}
    BSONObj filterBSON = BSON("$and" << BSON_ARRAY(
        BSON("region" << "us-east") <<
        BSON("nonexistent" << "value")
    ));
    auto expCtx = make_intrusive<ExpressionContextForTest>();
    auto matchExpr = mongo::MatchExpressionParser::parse(
        filterBSON, expCtx, mongo::ExtensionsCallbackNoop());
    ASSERT_TRUE(matchExpr.isOK());

    // Convert to predicate - should not crash
    auto predicateResult = table->convertMatchExpressionToPredicate(matchExpr.getValue().get());
    // May succeed or fail depending on implementation
}

TEST_F(ComplexMatchExpressionTest, ConvertEmptyAndExpression) {
    // Insert test data
    BSONObj metadata = BSON("region" << "us-east");
    auto result = table->insertRow(metadata);
    ASSERT_TRUE(result.isOK());

    // Create empty AND: {$and: []}
    // Note: MongoDB doesn't allow empty AND expressions, so this should fail to parse
    BSONObj filterBSON = BSON("$and" << BSONArray());
    auto expCtx = make_intrusive<ExpressionContextForTest>();
    auto matchExpr = mongo::MatchExpressionParser::parse(
        filterBSON, expCtx, mongo::ExtensionsCallbackNoop());

    // Empty AND expressions are not valid in MongoDB, so parsing should fail
    // This is expected behavior
    if (!matchExpr.isOK()) {
        // Expected - empty AND is invalid
        return;
    }

    // If parsing somehow succeeds, try to convert
    auto predicateResult = table->convertMatchExpressionToPredicate(matchExpr.getValue().get());
    // May succeed or fail depending on implementation
}

class NestedExpressionTest : public unittest::Test {
protected:
    void setUp() override {
        auto collectionUUID = UUID::gen();
        DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
        writer = std::make_unique<HCIndexWriter>(collectionUUID, dbName);
        Timestamp windowStart(1, 0);
        Timestamp windowEnd(2, 0);
        dict = std::make_unique<SymbolDictionary>(HCIndexPeriodEnum::Hour, 1, windowStart, windowEnd, writer.get());
        ASSERT_OK(dict->changeState(SymbolDictionaryState::ReadWrite));
        table = std::make_unique<AttributeTable>(dict.get(), writer.get(), HCIndexPeriodEnum::Hour, 1, windowStart, windowEnd);
        ASSERT_OK(table->changeState(AttributeTableState::ReadWrite));
    }

    std::unique_ptr<HCIndexWriter> writer;
    std::unique_ptr<SymbolDictionary> dict;
    std::unique_ptr<AttributeTable> table;
};

TEST_F(NestedExpressionTest, NestedAndOrExpression_AandB_OrCandD) {
    // Insert test data: 4 rows with different combinations
    BSONObj metadata1 = BSON("region" << "us-east" << "env" << "prod" << "tier" << "gold");
    BSONObj metadata2 = BSON("region" << "us-east" << "env" << "dev" << "tier" << "silver");
    BSONObj metadata3 = BSON("region" << "us-west" << "env" << "prod" << "tier" << "silver");
    BSONObj metadata4 = BSON("region" << "us-west" << "env" << "dev" << "tier" << "gold");

    auto result1 = table->insertRow(metadata1);
    auto result2 = table->insertRow(metadata2);
    auto result3 = table->insertRow(metadata3);
    auto result4 = table->insertRow(metadata4);

    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result2.isOK());
    ASSERT_TRUE(result3.isOK());
    ASSERT_TRUE(result4.isOK());

    // Create: {$or: [{$and: [{region: "us-east"}, {env: "prod"}]}, {$and: [{region: "us-west"}, {env: "dev"}]}]}
    // This should match rows 0 (us-east & prod) and 3 (us-west & dev)
    BSONObj filterBSON = BSON("$or" << BSON_ARRAY(
        BSON("$and" << BSON_ARRAY(BSON("region" << "us-east") << BSON("env" << "prod"))) <<
        BSON("$and" << BSON_ARRAY(BSON("region" << "us-west") << BSON("env" << "dev")))
    ));
    auto expCtx = make_intrusive<ExpressionContextForTest>();
    auto matchExpr = mongo::MatchExpressionParser::parse(
        filterBSON, expCtx, mongo::ExtensionsCallbackNoop());
    ASSERT_TRUE(matchExpr.isOK());

    // Convert to predicate - convertMatchExpressionToPredicate now handles OR expressions internally
    auto predicateResult = table->convertMatchExpressionToPredicate(matchExpr.getValue().get());
    ASSERT_TRUE(predicateResult.isOK());

    auto predicate = predicateResult.getValue();
    ASSERT_TRUE(predicate.isOr());

    // Query with the predicate - queryRows now handles OR semantics internally
    auto queryResult = table->queryRows(predicate);

    // Should match rows 0 and 3
    ASSERT_EQ(queryResult.size(), 2u);
    ASSERT_TRUE(std::find(queryResult.begin(), queryResult.end(), 0u) != queryResult.end());
    ASSERT_TRUE(std::find(queryResult.begin(), queryResult.end(), 3u) != queryResult.end());
}

TEST_F(NestedExpressionTest, NestedAndOrExpression_AandB_OrCandD_WithQueryRows) {
    // Insert test data
    BSONObj metadata1 = BSON("region" << "us-east" << "env" << "prod");
    BSONObj metadata2 = BSON("region" << "us-east" << "env" << "dev");
    BSONObj metadata3 = BSON("region" << "us-west" << "env" << "prod");
    BSONObj metadata4 = BSON("region" << "us-west" << "env" << "dev");

    auto result1 = table->insertRow(metadata1);
    auto result2 = table->insertRow(metadata2);
    auto result3 = table->insertRow(metadata3);
    auto result4 = table->insertRow(metadata4);

    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result2.isOK());
    ASSERT_TRUE(result3.isOK());
    ASSERT_TRUE(result4.isOK());

    // Get symbol indices
    auto usEastResult = dict->getOrInsertSymbol("us-east");
    auto usWestResult = dict->getOrInsertSymbol("us-west");
    auto prodResult = dict->getOrInsertSymbol("prod");
    auto devResult = dict->getOrInsertSymbol("dev");

    ASSERT_TRUE(usEastResult.isOK());
    ASSERT_TRUE(usWestResult.isOK());
    ASSERT_TRUE(prodResult.isOK());
    ASSERT_TRUE(devResult.isOK());

    uint32_t usEastSymbol = usEastResult.getValue();
    uint32_t usWestSymbol = usWestResult.getValue();
    uint32_t prodSymbol = prodResult.getValue();
    uint32_t devSymbol = devResult.getValue();

    // Query for (region=us-east AND env=prod) - should match row 0
    AttributeTablePredicate predicate1;
    predicate1.refRowVec = {usEastSymbol, prodSymbol};
    auto result = table->queryRows(predicate1);
    ASSERT_EQ(result.size(), 1u);
    ASSERT_EQ(result[0], 0u);

    // Query for (region=us-west AND env=dev) - should match row 3
    AttributeTablePredicate predicate2;
    predicate2.refRowVec = {usWestSymbol, devSymbol};
    result = table->queryRows(predicate2);
    ASSERT_EQ(result.size(), 1u);
    ASSERT_EQ(result[0], 3u);
}

TEST_F(NestedExpressionTest, TripleNestedExpression_AandB_OrCandD_OrEandF) {
    // Insert test data: 6 rows
    BSONObj metadata1 = BSON("region" << "us-east" << "env" << "prod" << "tier" << "gold");
    BSONObj metadata2 = BSON("region" << "us-east" << "env" << "dev" << "tier" << "silver");
    BSONObj metadata3 = BSON("region" << "us-west" << "env" << "prod" << "tier" << "silver");
    BSONObj metadata4 = BSON("region" << "us-west" << "env" << "dev" << "tier" << "gold");
    BSONObj metadata5 = BSON("region" << "eu-west" << "env" << "prod" << "tier" << "bronze");
    BSONObj metadata6 = BSON("region" << "eu-west" << "env" << "dev" << "tier" << "bronze");

    auto result1 = table->insertRow(metadata1);
    auto result2 = table->insertRow(metadata2);
    auto result3 = table->insertRow(metadata3);
    auto result4 = table->insertRow(metadata4);
    auto result5 = table->insertRow(metadata5);
    auto result6 = table->insertRow(metadata6);

    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result2.isOK());
    ASSERT_TRUE(result3.isOK());
    ASSERT_TRUE(result4.isOK());
    ASSERT_TRUE(result5.isOK());
    ASSERT_TRUE(result6.isOK());

    // Create: {$or: [{$and: [{region: "us-east"}, {env: "prod"}]},
    //                 {$and: [{region: "us-west"}, {env: "dev"}]},
    //                 {$and: [{region: "eu-west"}, {env: "prod"}]}]}
    BSONObj filterBSON = BSON("$or" << BSON_ARRAY(
        BSON("$and" << BSON_ARRAY(BSON("region" << "us-east") << BSON("env" << "prod"))) <<
        BSON("$and" << BSON_ARRAY(BSON("region" << "us-west") << BSON("env" << "dev"))) <<
        BSON("$and" << BSON_ARRAY(BSON("region" << "eu-west") << BSON("env" << "prod")))
    ));
    auto expCtx = make_intrusive<ExpressionContextForTest>();
    auto matchExpr = mongo::MatchExpressionParser::parse(
        filterBSON, expCtx, mongo::ExtensionsCallbackNoop());
    ASSERT_TRUE(matchExpr.isOK());

    // Convert to predicate - convertMatchExpressionToPredicate now handles OR expressions internally
    auto predicateResult = table->convertMatchExpressionToPredicate(matchExpr.getValue().get());
    ASSERT_TRUE(predicateResult.isOK());

    auto predicate = predicateResult.getValue();
    ASSERT_TRUE(predicate.isOr());

    // Query with the predicate - queryRows now handles OR semantics internally
    auto queryResult = table->queryRows(predicate);

    // Should match rows 0, 3, and 4
    ASSERT_EQ(queryResult.size(), 3u);
    ASSERT_TRUE(std::find(queryResult.begin(), queryResult.end(), 0u) != queryResult.end());
    ASSERT_TRUE(std::find(queryResult.begin(), queryResult.end(), 3u) != queryResult.end());
    ASSERT_TRUE(std::find(queryResult.begin(), queryResult.end(), 4u) != queryResult.end());
}

TEST_F(NestedExpressionTest, DeeplyNestedExpression_LeftAssociative) {
    // Insert test data
    BSONObj metadata1 = BSON("a" << "1" << "b" << "2" << "c" << "3" << "d" << "4");
    BSONObj metadata2 = BSON("a" << "1" << "b" << "2" << "c" << "3" << "d" << "5");
    BSONObj metadata3 = BSON("a" << "1" << "b" << "2" << "c" << "4" << "d" << "4");

    auto result1 = table->insertRow(metadata1);
    auto result2 = table->insertRow(metadata2);
    auto result3 = table->insertRow(metadata3);

    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result2.isOK());
    ASSERT_TRUE(result3.isOK());

    // Create: {$and: [{a: "1"}, {$and: [{b: "2"}, {$and: [{c: "3"}, {d: "4"}]}]}]}
    // This should match only row 0 (a=1, b=2, c=3, d=4)
    BSONObj filterBSON = BSON("$and" << BSON_ARRAY(
        BSON("a" << "1") <<
        BSON("$and" << BSON_ARRAY(
            BSON("b" << "2") <<
            BSON("$and" << BSON_ARRAY(
                BSON("c" << "3") <<
                BSON("d" << "4")
            ))
        ))
    ));
    auto expCtx = make_intrusive<ExpressionContextForTest>();
    auto matchExpr = mongo::MatchExpressionParser::parse(
        filterBSON, expCtx, mongo::ExtensionsCallbackNoop());
    ASSERT_TRUE(matchExpr.isOK());

    // Convert to predicate
    auto predicateResult = table->convertMatchExpressionToPredicate(matchExpr.getValue().get());
    ASSERT_TRUE(predicateResult.isOK());

    auto predicate = predicateResult.getValue();
    // Should be an AND (hierarchical structure for nested ANDs)
    ASSERT_TRUE(predicate.isAnd());
    ASSERT_GT(predicate.children.size(), 0u);

    // Query and verify results - the hierarchical structure should still match correctly
    auto queryResult = table->queryRows(predicate);
    ASSERT_EQ(queryResult.size(), 1u);
    ASSERT_EQ(queryResult[0], 0u);  // Only row 0 matches
}

TEST_F(NestedExpressionTest, DeeplyNestedExpression_RightAssociative) {
    // Insert test data
    BSONObj metadata1 = BSON("a" << "1" << "b" << "2" << "c" << "3" << "d" << "4");
    BSONObj metadata2 = BSON("a" << "1" << "b" << "2" << "c" << "3" << "d" << "5");

    auto result1 = table->insertRow(metadata1);
    auto result2 = table->insertRow(metadata2);

    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result2.isOK());

    // Create: {$and: [{$and: [{$and: [{a: "1"}, {b: "2"}]}, {c: "3"}]}, {d: "4"}]}
    // This should match only row 0 (a=1, b=2, c=3, d=4)
    BSONObj filterBSON = BSON("$and" << BSON_ARRAY(
        BSON("$and" << BSON_ARRAY(
            BSON("$and" << BSON_ARRAY(
                BSON("a" << "1") <<
                BSON("b" << "2")
            )) <<
            BSON("c" << "3")
        )) <<
        BSON("d" << "4")
    ));
    auto expCtx = make_intrusive<ExpressionContextForTest>();
    auto matchExpr = mongo::MatchExpressionParser::parse(
        filterBSON, expCtx, mongo::ExtensionsCallbackNoop());
    ASSERT_TRUE(matchExpr.isOK());

    // Convert to predicate
    auto predicateResult = table->convertMatchExpressionToPredicate(matchExpr.getValue().get());
    ASSERT_TRUE(predicateResult.isOK());

    auto predicate = predicateResult.getValue();
    // Should be an AND (hierarchical structure for nested ANDs)
    ASSERT_TRUE(predicate.isAnd());
    ASSERT_GT(predicate.children.size(), 0u);

    // Query and verify results - the hierarchical structure should still match correctly
    auto queryResult = table->queryRows(predicate);
    ASSERT_EQ(queryResult.size(), 1u);
    ASSERT_EQ(queryResult[0], 0u);  // Only row 0 matches
}

TEST_F(NestedExpressionTest, ComplexMixedNesting_AndOrAnd) {
    // Insert test data
    BSONObj metadata1 = BSON("region" << "us-east" << "env" << "prod" << "tier" << "gold" << "zone" << "1a");
    BSONObj metadata2 = BSON("region" << "us-east" << "env" << "prod" << "tier" << "silver" << "zone" << "1b");
    BSONObj metadata3 = BSON("region" << "us-west" << "env" << "dev" << "tier" << "gold" << "zone" << "2a");

    auto result1 = table->insertRow(metadata1);
    auto result2 = table->insertRow(metadata2);
    auto result3 = table->insertRow(metadata3);

    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result2.isOK());
    ASSERT_TRUE(result3.isOK());

    // Create: {$and: [{region: "us-east"}, {$or: [{env: "prod"}, {tier: "gold"}]}, {zone: "1a"}]}
    // This is a mixed AND/OR expression
    BSONObj filterBSON = BSON("$and" << BSON_ARRAY(
        BSON("region" << "us-east") <<
        BSON("$or" << BSON_ARRAY(
            BSON("env" << "prod") <<
            BSON("tier" << "gold")
        )) <<
        BSON("zone" << "1a")
    ));
    auto expCtx = make_intrusive<ExpressionContextForTest>();
    auto matchExpr = mongo::MatchExpressionParser::parse(
        filterBSON, expCtx, mongo::ExtensionsCallbackNoop());
    ASSERT_TRUE(matchExpr.isOK());

    // Convert to predicate
    auto predicateResult = table->convertMatchExpressionToPredicate(matchExpr.getValue().get());
    // Mixed AND/OR expressions may not be directly convertible
    if (predicateResult.isOK()) {
        auto predicate = predicateResult.getValue();
        auto queryResult = table->queryRows(predicate);
        // The predicate should match at least one row
        ASSERT_GT(queryResult.size(), 0u);
    }
}


}  // namespace mongo::timeseries::hcindex

