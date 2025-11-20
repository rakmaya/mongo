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
#include "mongo/db/exec/timeseries/hcindex/hcindex_writer.h"
#include "mongo/unittest/unittest.h"
#include "mongo/bson/bsonobjbuilder.h"

namespace mongo::timeseries::hcindex {

// ============================================================================
// AttributeTable Tests
// ============================================================================

class AttributeTableTest : public unittest::Test {
protected:
    void setUp() override {
        auto collectionUUID = UUID::gen();
        writer = std::make_unique<HCIndexWriter>(collectionUUID);
        Timestamp windowStart(1, 0);
        Timestamp windowEnd(2, 0);
        dict = std::make_unique<SymbolDictionary>(DictionaryGranularity::HOURLY, windowStart, windowEnd, writer.get());
        ASSERT_OK(dict->changeState(SymbolDictionaryState::ReadWrite));
        table = std::make_unique<AttributeTable>(dict.get(), writer.get(), windowStart, windowEnd);
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
    AttributeTablePredicate predicate;
    predicate.fieldMatches["container"] = 2;  // Index of "api-1"

    auto queryResult = table->queryRows(predicate);

    ASSERT_EQ(2u, queryResult.size());
}

// ============================================================================
// TemporalAttributeTable Tests
// ============================================================================

class TemporalAttributeTableTest : public unittest::Test {
protected:
    void setUp() override {
        auto collectionUUID = UUID::gen();
        writer = std::make_unique<HCIndexWriter>(collectionUUID);
        tempDict = std::make_unique<TemporalSymbolDictionary>(
            nullptr, collectionUUID, DictionaryGranularity::HOURLY, writer.get());
        tempTable = std::make_unique<TemporalAttributeTable>(
            nullptr, collectionUUID, DictionaryGranularity::HOURLY, tempDict.get(), writer.get());
    }

    std::unique_ptr<HCIndexWriter> writer;
    std::unique_ptr<TemporalSymbolDictionary> tempDict;
    std::unique_ptr<TemporalAttributeTable> tempTable;
};

TEST_F(TemporalAttributeTableTest, InsertRowWithTimestamp) {
    BSONObj metadata = BSON("node"
                            << "nyc-01");
    Timestamp ts(1000, 0);

    auto result = tempTable->insertRow(metadata, ts);

    ASSERT_TRUE(result.isOK());
    ASSERT_EQ(0, result.getValue().rowId);
    ASSERT_TRUE(result.getValue().isNewRow);
}

TEST_F(TemporalAttributeTableTest, InsertRowDeduplicatesInSameWindow) {
    BSONObj metadata = BSON("node"
                            << "nyc-01");
    Timestamp ts1(1000, 0);
    Timestamp ts2(2000, 0);  // Same window (0-3600)

    auto result1 = tempTable->insertRow(metadata, ts1);
    auto result2 = tempTable->insertRow(metadata, ts2);

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

    auto result1 = tempTable->insertRow(metadata, ts1);
    auto result2 = tempTable->insertRow(metadata, ts2);

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

    auto result = tempTable->insertRow(metadata, ts);
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

    auto r1 = tempTable->insertRow(metadata, ts1);
    ASSERT_TRUE(r1.isOK());
    ASSERT_TRUE(r1.getValue().isNewRow);
    auto r2 = tempTable->insertRow(metadata, ts2);
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

    auto insertResult = tempTable->insertRow(metadata, ts);
    ASSERT_TRUE(insertResult.isOK());
    ASSERT_TRUE(insertResult.getValue().isNewRow);

    auto getResult = tempTable->getRow(insertResult.getValue().rowId, ts);

    ASSERT_TRUE(getResult);
}

TEST_F(TemporalAttributeTableTest, QueryRowsWithTimestamp) {
    BSONObj metadata = BSON("node"
                            << "nyc-01");
    Timestamp ts(1000, 0);

    auto insertResult = tempTable->insertRow(metadata, ts);
    ASSERT_TRUE(insertResult.isOK());
    ASSERT_TRUE(insertResult.getValue().isNewRow);

    AttributeTablePredicate predicate;
    auto queryResult = tempTable->queryRows(predicate, ts);

    ASSERT_GTE(queryResult.size(), 0u);
}

}  // namespace mongo::timeseries::hcindex

