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

#include "mongo/db/exec/timeseries/hcindex/bitmap_index.h"
#include "mongo/db/exec/timeseries/hcindex/temporal_bitmap_index.h"
#include "mongo/db/exec/timeseries/hcindex/writer.h"
#include "mongo/db/database_name.h"
#include "mongo/unittest/unittest.h"

namespace mongo::timeseries::hcindex {

std::unique_ptr<BitmapIndex> createTestBitmapIndex() {
    Timestamp windowStart(1000, 0);
    Timestamp windowEnd(2000, 0);
    auto index = std::make_unique<BitmapIndex>(
        HCIndexPeriodEnum::Hour, 1, windowStart, windowEnd, nullptr);
    ASSERT_OK(index->changeState(BitmapIndexState::Reconstruction));
    // Set up included columns for testing (columns 0, 1, 2, 3)
    index->setIncludedColumns({0, 1, 2, 3});
    return index;
}

TEST(BitmapIndexTest, InitialStateIsNOP) {
    Timestamp windowStart(1000, 0);
    Timestamp windowEnd(2000, 0);
    BitmapIndex index(HCIndexPeriodEnum::Hour, 1, windowStart, windowEnd, nullptr);
    ASSERT_EQ(BitmapIndexState::NOP, index.getState());
}

TEST(BitmapIndexTest, CanTransitionFromNOPToReadWrite) {
    Timestamp windowStart(1000, 0);
    Timestamp windowEnd(2000, 0);
    BitmapIndex index(HCIndexPeriodEnum::Hour, 1, windowStart, windowEnd, nullptr);
    ASSERT_OK(index.changeState(BitmapIndexState::ReadWrite));
    ASSERT_EQ(BitmapIndexState::ReadWrite, index.getState());
}

TEST(BitmapIndexTest, CanTransitionFromNOPToReconstruction) {
    Timestamp windowStart(1000, 0);
    Timestamp windowEnd(2000, 0);
    BitmapIndex index(HCIndexPeriodEnum::Hour, 1, windowStart, windowEnd, nullptr);
    ASSERT_OK(index.changeState(BitmapIndexState::Reconstruction));
    ASSERT_EQ(BitmapIndexState::Reconstruction, index.getState());
}

TEST(BitmapIndexTest, CanTransitionFromReadWriteToReadOnly) {
    auto index = createTestBitmapIndex();
    ASSERT_OK(index->changeState(BitmapIndexState::ReadOnly));
    ASSERT_EQ(BitmapIndexState::ReadOnly, index->getState());
}

TEST(BitmapIndexTest, CanTransitionFromReconstructionToReadOnly) {
    Timestamp windowStart(1000, 0);
    Timestamp windowEnd(2000, 0);
    BitmapIndex index(HCIndexPeriodEnum::Hour, 1, windowStart, windowEnd, nullptr);
    ASSERT_OK(index.changeState(BitmapIndexState::Reconstruction));
    ASSERT_OK(index.changeState(BitmapIndexState::ReadOnly));
    ASSERT_EQ(BitmapIndexState::ReadOnly, index.getState());
}

TEST(BitmapIndexTest, CannotTransitionFromReadOnly) {
    auto index = createTestBitmapIndex();
    ASSERT_OK(index->changeState(BitmapIndexState::ReadOnly));
    ASSERT_NOT_OK(index->changeState(BitmapIndexState::ReadWrite));
    ASSERT_NOT_OK(index->changeState(BitmapIndexState::NOP));
}

TEST(BitmapIndexTest, CannotTransitionFromNOPToReadOnly) {
    Timestamp windowStart(1000, 0);
    Timestamp windowEnd(2000, 0);
    BitmapIndex index(HCIndexPeriodEnum::Hour, 1, windowStart, windowEnd, nullptr);
    ASSERT_NOT_OK(index.changeState(BitmapIndexState::ReadOnly));
}

TEST(BitmapIndexTest, AddEntrySucceeds) {
    auto index = createTestBitmapIndex();
    ASSERT_OK(index->addEntry(0, 1, 100));
    ASSERT_EQ(1u, index->getEntryCount());
}

TEST(BitmapIndexTest, AddEntrySkipsMissingValues) {
    auto index = createTestBitmapIndex();
    // symbolIndex 0 means "missing" - should be skipped
    ASSERT_OK(index->addEntry(0, 0, 100));
    ASSERT_EQ(0u, index->getEntryCount());
}

TEST(BitmapIndexTest, AddEntryFailsInNOPState) {
    Timestamp windowStart(1000, 0);
    Timestamp windowEnd(2000, 0);
    BitmapIndex index(HCIndexPeriodEnum::Hour, 1, windowStart, windowEnd, nullptr);
    ASSERT_NOT_OK(index.addEntry(0, 1, 100));
}

TEST(BitmapIndexTest, AddEntryFailsInReadOnlyState) {
    auto index = createTestBitmapIndex();
    ASSERT_OK(index->changeState(BitmapIndexState::ReadOnly));
    ASSERT_NOT_OK(index->addEntry(0, 1, 100));
}

TEST(BitmapIndexTest, AddMultipleRowIdsToSameBitmap) {
    auto index = createTestBitmapIndex();
    ASSERT_OK(index->addEntry(0, 1, 100));
    ASSERT_OK(index->addEntry(0, 1, 101));
    ASSERT_OK(index->addEntry(0, 1, 102));

    auto rowIds = index->getRowIds(0, 1);
    ASSERT_EQ(3u, rowIds.size());
    ASSERT_TRUE(rowIds.count(100) == 1);
    ASSERT_TRUE(rowIds.count(101) == 1);
    ASSERT_TRUE(rowIds.count(102) == 1);
}

TEST(BitmapIndexTest, AddRowSucceeds) {
    auto index = createTestBitmapIndex();
    // Row with 3 columns: column0=symbol1, column1=symbol2, column2=symbol3
    std::vector<uint32_t> row = {1, 2, 3};
    ASSERT_OK(index->addRow(100, row));
    ASSERT_EQ(3u, index->getEntryCount());  // 3 unique (column, value) pairs
}

TEST(BitmapIndexTest, AddRowSkipsMissingValues) {
    auto index = createTestBitmapIndex();
    // Row with missing value in column 1 (symbolIndex = 0)
    std::vector<uint32_t> row = {1, 0, 3};
    ASSERT_OK(index->addRow(100, row));
    ASSERT_EQ(2u, index->getEntryCount());  // Only 2 entries (column0 and column2)
}

TEST(BitmapIndexTest, GetRowIdsReturnsEmptyForNonExistentKey) {
    auto index = createTestBitmapIndex();
    auto rowIds = index->getRowIds(0, 1);
    ASSERT_TRUE(rowIds.empty());
}

TEST(BitmapIndexTest, GetRowIdsReturnsMatchingRowIds) {
    auto index = createTestBitmapIndex();
    ASSERT_OK(index->addEntry(0, 1, 100));
    ASSERT_OK(index->addEntry(0, 1, 200));
    ASSERT_OK(index->addEntry(0, 2, 300));

    auto rowIds = index->getRowIds(0, 1);
    ASSERT_EQ(2u, rowIds.size());
    ASSERT_TRUE(rowIds.count(100) == 1);
    ASSERT_TRUE(rowIds.count(200) == 1);
}

TEST(BitmapIndexTest, QueryRowIdsAndReturnsIntersection) {
    auto index = createTestBitmapIndex();
    // Add rows with different column values
    // Row 100: column0=1, column1=2
    // Row 101: column0=1, column1=3
    // Row 102: column0=2, column1=2
    ASSERT_OK(index->addRow(100, {1, 2}));
    ASSERT_OK(index->addRow(101, {1, 3}));
    ASSERT_OK(index->addRow(102, {2, 2}));

    // Query for column0=1 AND column1=2 (should return only row 100)
    std::vector<uint32_t> predicate = {1, 2};
    auto rowIds = index->queryRowIdsAnd(predicate);
    ASSERT_EQ(1u, rowIds.size());
    ASSERT_TRUE(rowIds.count(100) == 1);
}

TEST(BitmapIndexTest, QueryRowIdsAndWithDontCare) {
    auto index = createTestBitmapIndex();
    ASSERT_OK(index->addRow(100, {1, 2}));
    ASSERT_OK(index->addRow(101, {1, 3}));
    ASSERT_OK(index->addRow(102, {2, 2}));

    // Query for column0=1 (column1=0 means "don't care")
    std::vector<uint32_t> predicate = {1, 0};
    auto rowIds = index->queryRowIdsAnd(predicate);
    ASSERT_EQ(2u, rowIds.size());
    ASSERT_TRUE(rowIds.count(100) == 1);
    ASSERT_TRUE(rowIds.count(101) == 1);
}

TEST(BitmapIndexTest, QueryRowIdsAndReturnsEmptyForNoMatch) {
    auto index = createTestBitmapIndex();
    ASSERT_OK(index->addRow(100, {1, 2}));
    ASSERT_OK(index->addRow(101, {1, 3}));

    // Query for column0=1 AND column1=5 (no match)
    std::vector<uint32_t> predicate = {1, 5};
    auto rowIds = index->queryRowIdsAnd(predicate);
    ASSERT_TRUE(rowIds.empty());
}

TEST(BitmapIndexTest, QueryRowIdsOrReturnsUnion) {
    auto index = createTestBitmapIndex();
    ASSERT_OK(index->addRow(100, {1, 2}));
    ASSERT_OK(index->addRow(101, {1, 3}));
    ASSERT_OK(index->addRow(102, {2, 2}));

    // Query for (column0=1 AND column1=2) OR (column0=2 AND column1=2)
    std::vector<std::vector<uint32_t>> predicates = {{1, 2}, {2, 2}};
    auto rowIds = index->queryRowIdsOr(predicates);
    ASSERT_EQ(2u, rowIds.size());
    ASSERT_TRUE(rowIds.count(100) == 1);
    ASSERT_TRUE(rowIds.count(102) == 1);
}

TEST(BitmapIndexTest, GetTotalRowIdCount) {
    auto index = createTestBitmapIndex();
    ASSERT_OK(index->addEntry(0, 1, 100));
    ASSERT_OK(index->addEntry(0, 1, 101));
    ASSERT_OK(index->addEntry(0, 2, 200));

    ASSERT_EQ(2u, index->getEntryCount());      // 2 unique (column, value) pairs
    ASSERT_EQ(3u, index->getTotalRowIdCount()); // 3 total rowIds
}

TEST(BitmapIndexTest, GetMemoryUsageBytes) {
    auto index = createTestBitmapIndex();
    size_t initialMemory = index->getMemoryUsageBytes();

    ASSERT_OK(index->addEntry(0, 1, 100));
    size_t afterAddMemory = index->getMemoryUsageBytes();

    ASSERT_GT(afterAddMemory, initialMemory);
}

TEST(BitmapIndexTest, GetWindowBoundaries) {
    Timestamp windowStart(1000, 0);
    Timestamp windowEnd(2000, 0);
    BitmapIndex index(HCIndexPeriodEnum::Hour, 1, windowStart, windowEnd, nullptr);

    ASSERT_EQ(windowStart, index.getWindowStart());
    ASSERT_EQ(windowEnd, index.getWindowEnd());
}

// a helper construct to hold both TemporalBitmapIndex and writer for testing
struct TestTemporalBitmapIndexContext {
    std::unique_ptr<HCIndexWriter> writer;
    std::unique_ptr<TemporalBitmapIndex> index;
};

TestTemporalBitmapIndexContext createTestTemporalBitmapIndexWithWriter() {
    UUID collectionUUID = UUID::gen();
    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    auto writer = std::make_unique<HCIndexWriter>(collectionUUID, dbName);
    auto index = std::make_unique<TemporalBitmapIndex>(
        collectionUUID, HCIndexPeriodEnum::Hour, 1, writer.get(), nullptr);
    // Set up included columns for testing (columns 0, 1, 2, 3)
    index->setIncludedColumns({0, 1, 2, 3});
    return {std::move(writer), std::move(index)};
}

TEST(TemporalBitmapIndexTest, GetWindowForTimestampHourly) {
    auto ctx = createTestTemporalBitmapIndexWithWriter();

    // Timestamp at 1:30:00 (5400 seconds)
    Timestamp ts(5400, 0);
    auto [windowStart, windowEnd] = ctx.index->getWindowForTimestamp(ts);

    // Should be aligned to hour boundary: 1:00:00 - 2:00:00 (3600 - 7200)
    ASSERT_EQ(Timestamp(3600, 0), windowStart);
    ASSERT_EQ(Timestamp(7200, 0), windowEnd);
}

TEST(TemporalBitmapIndexTest, GetWindowForTimestampMinutely) {
    UUID collectionUUID = UUID::gen();
    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    HCIndexWriter writer(collectionUUID, dbName);
    TemporalBitmapIndex index(collectionUUID, HCIndexPeriodEnum::Minute, 1, &writer, nullptr);

    // Timestamp at 30 seconds into minute 5 (330 seconds)
    Timestamp ts(330, 0);
    auto [windowStart, windowEnd] = index.getWindowForTimestamp(ts);

    // Should be aligned to minute boundary: 5:00 - 6:00 (300 - 360)
    ASSERT_EQ(Timestamp(300, 0), windowStart);
    ASSERT_EQ(Timestamp(360, 0), windowEnd);
}

TEST(TemporalBitmapIndexTest, GetOrCreateIndexForTimestampCreatesIndex) {
    auto ctx = createTestTemporalBitmapIndexWithWriter();

    Timestamp ts(3600, 0);
    auto result = ctx.index->getOrCreateIndexForTimestamp(nullptr, ts);
    ASSERT_OK(result);
    ASSERT_NE(nullptr, result.getValue());
}

TEST(TemporalBitmapIndexTest, GetOrCreateIndexForTimestampReturnsSameIndex) {
    auto ctx = createTestTemporalBitmapIndexWithWriter();

    Timestamp ts1(3700, 0);  // In first hour
    Timestamp ts2(3800, 0);  // Also in first hour

    auto result1 = ctx.index->getOrCreateIndexForTimestamp(nullptr, ts1);
    auto result2 = ctx.index->getOrCreateIndexForTimestamp(nullptr, ts2);

    ASSERT_OK(result1);
    ASSERT_OK(result2);
    ASSERT_EQ(result1.getValue(), result2.getValue());  // Same index
}

TEST(TemporalBitmapIndexTest, GetOrCreateIndexForTimestampCreatesDifferentIndexes) {
    auto ctx = createTestTemporalBitmapIndexWithWriter();

    Timestamp ts1(3600, 0);   // First hour
    Timestamp ts2(7200, 0);   // Second hour

    auto result1 = ctx.index->getOrCreateIndexForTimestamp(nullptr, ts1);
    auto result2 = ctx.index->getOrCreateIndexForTimestamp(nullptr, ts2);

    ASSERT_OK(result1);
    ASSERT_OK(result2);
    ASSERT_NE(result1.getValue(), result2.getValue());  // Different indexes
}

TEST(TemporalBitmapIndexTest, GetIndexForTimestampReturnsErrorIfNotFound) {
    auto ctx = createTestTemporalBitmapIndexWithWriter();

    Timestamp ts(3600, 0);
    auto result = ctx.index->getIndexForTimestamp(ts);
    ASSERT_NOT_OK(result);
}

TEST(TemporalBitmapIndexTest, AddRowAndQuery) {
    auto ctx = createTestTemporalBitmapIndexWithWriter();

    Timestamp ts(3600, 0);
    std::vector<uint32_t> row = {1, 2, 3};

    ASSERT_OK(ctx.index->addRow(nullptr, 100, row, ts));
    ASSERT_OK(ctx.index->addRow(nullptr, 101, {1, 3, 3}, ts));
    ASSERT_OK(ctx.index->addRow(nullptr, 102, {2, 2, 3}, ts));

    // Query for column0=1 (rows 100, 101)
    auto rowIds = ctx.index->queryRowIds({1, 0, 0}, ts);
    ASSERT_EQ(2u, rowIds.size());
    ASSERT_TRUE(rowIds.count(100) == 1);
    ASSERT_TRUE(rowIds.count(101) == 1);
}

TEST(TemporalBitmapIndexTest, QueryReturnsEmptyForDifferentWindow) {
    auto ctx = createTestTemporalBitmapIndexWithWriter();

    Timestamp ts1(3600, 0);   // First hour
    Timestamp ts2(7200, 0);   // Second hour

    ASSERT_OK(ctx.index->addRow(nullptr, 100, {1, 2}, ts1));

    // Query in different window should return empty
    auto rowIds = ctx.index->queryRowIds({1, 2}, ts2);
    ASSERT_TRUE(rowIds.empty());
}

TEST(TemporalBitmapIndexTest, CleanupOldIndexes) {
    auto ctx = createTestTemporalBitmapIndexWithWriter();

    // Add rows to multiple windows
    ASSERT_OK(ctx.index->addRow(nullptr, 100, {1, 2}, Timestamp(3600, 0)));   // Hour 1
    ASSERT_OK(ctx.index->addRow(nullptr, 101, {1, 2}, Timestamp(7200, 0)));   // Hour 2
    ASSERT_OK(ctx.index->addRow(nullptr, 102, {1, 2}, Timestamp(10800, 0)));  // Hour 3

    auto statsBefore = ctx.index->getStats();
    ASSERT_EQ(3u, statsBefore.totalIndexes);

    // Cleanup indexes before hour 2
    ASSERT_OK(ctx.index->cleanupOldIndexes(Timestamp(7200, 0)));

    auto statsAfter = ctx.index->getStats();
    ASSERT_EQ(2u, statsAfter.totalIndexes);  // Hours 2 and 3 remain
}

TEST(TemporalBitmapIndexTest, GetStatsReturnsCorrectValues) {
    auto ctx = createTestTemporalBitmapIndexWithWriter();

    ASSERT_OK(ctx.index->addRow(nullptr, 100, {1, 2, 3}, Timestamp(3600, 0)));
    ASSERT_OK(ctx.index->addRow(nullptr, 101, {1, 2, 4}, Timestamp(3600, 0)));

    auto stats = ctx.index->getStats();
    ASSERT_EQ(1u, stats.totalIndexes);
    ASSERT_GT(stats.totalEntries, 0u);
    ASSERT_GT(stats.memoryUsageBytes, 0u);
}

}  // namespace mongo::timeseries::hcindex

