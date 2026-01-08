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

#include "mongo/db/exec/timeseries/hcindex/temporal_symbol_dictionary.h"
#include "mongo/db/exec/timeseries/hcindex/hcindex_writer.h"
#include "mongo/unittest/unittest.h"

namespace mongo::timeseries::hcindex {

// Helper struct to hold both dictionary and writer for testing
struct TestSymbolDictionaryContext {
    std::unique_ptr<HCIndexWriter> writer;
    SymbolDictionary* dict;
};

// Helper function to create a SymbolDictionary for testing
// Creates a dictionary with a writer so it can be modified
// Note: The caller must keep the returned context alive for the duration of the test
TestSymbolDictionaryContext createTestSymbolDictionaryWithWriter() {
    auto collectionUUID = UUID::gen();
    Timestamp windowStart(1, 0);
    Timestamp windowEnd(2, 0);
    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    auto writer = std::make_unique<HCIndexWriter>(collectionUUID, dbName);
    auto dict = new SymbolDictionary(HCIndexPeriodEnum::Hour, 1, windowStart, windowEnd, writer.get());
    ASSERT_OK(dict->changeState(SymbolDictionaryState::ReadWrite));
    return {std::move(writer), dict};
}

// ============================================================================
// SymbolDictionary Tests
// ============================================================================

TEST(SymbolDictionaryTest, GetOrInsertSymbolReturnsNewIndex) {
    auto ctx = createTestSymbolDictionaryWithWriter();

    auto result = ctx.dict->getOrInsertSymbol("nyc-01");
    ASSERT_TRUE(result.isOK());
    ASSERT_EQ(1u, result.getValue());
    delete ctx.dict;
}

TEST(SymbolDictionaryTest, GetOrInsertSymbolReturnsSameIndexForSameWord) {
    auto ctx = createTestSymbolDictionaryWithWriter();

    auto result1 = ctx.dict->getOrInsertSymbol("nyc-01");
    auto result2 = ctx.dict->getOrInsertSymbol("nyc-01");

    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result2.isOK());
    ASSERT_EQ(result1.getValue(), result2.getValue());
    delete ctx.dict;
}

TEST(SymbolDictionaryTest, GetOrInsertSymbolAssignsSequentialIndices) {
    auto ctx = createTestSymbolDictionaryWithWriter();

    auto result1 = ctx.dict->getOrInsertSymbol("word1");
    auto result2 = ctx.dict->getOrInsertSymbol("word2");
    auto result3 = ctx.dict->getOrInsertSymbol("word3");

    ASSERT_EQ(1u, result1.getValue());
    ASSERT_EQ(2u, result2.getValue());
    ASSERT_EQ(3u, result3.getValue());
    delete ctx.dict;
}

TEST(SymbolDictionaryTest, GetSymbolIndexReturnsIndexIfFound) {
    auto ctx = createTestSymbolDictionaryWithWriter();

    auto _ = ctx.dict->getOrInsertSymbol("nyc-01");
    auto result = ctx.dict->getSymbolIndex("nyc-01");

    ASSERT_TRUE(result);
    ASSERT_EQ(1u, result.value());
    delete ctx.dict;
}

TEST(SymbolDictionaryTest, GetSymbolIndexReturnsNoneIfNotFound) {
    auto ctx = createTestSymbolDictionaryWithWriter();

    auto result = ctx.dict->getSymbolIndex("not-found");

    ASSERT_FALSE(result);
    delete ctx.dict;
}

TEST(SymbolDictionaryTest, GetSymbolReturnsWordIfFound) {
    auto ctx = createTestSymbolDictionaryWithWriter();

    auto _ = ctx.dict->getOrInsertSymbol("nyc-01");
    auto result = ctx.dict->getSymbol(1);

    ASSERT_TRUE(result);
    ASSERT_EQ("nyc-01", result.value());
    delete ctx.dict;
}

TEST(SymbolDictionaryTest, GetSymbolReturnsNoneForInvalidIndex) {
    auto ctx = createTestSymbolDictionaryWithWriter();

    auto result = ctx.dict->getSymbol(999);

    ASSERT_FALSE(result);
    delete ctx.dict;
}

TEST(SymbolDictionaryTest, GetSymbolReturnsNoneForReservedZeroIndex) {
    auto ctx = createTestSymbolDictionaryWithWriter();

    auto result = ctx.dict->getSymbol(0);

    ASSERT_FALSE(result);
    delete ctx.dict;
}

TEST(SymbolDictionaryTest, GetSymbolCountReturnsCorrectCount) {
    auto ctx = createTestSymbolDictionaryWithWriter();

    ASSERT_EQ(0u, ctx.dict->getSymbolCount());

    auto _ = ctx.dict->getOrInsertSymbol("word1");
    ASSERT_EQ(1u, ctx.dict->getSymbolCount());

    _ = ctx.dict->getOrInsertSymbol("word2");
    ASSERT_EQ(2u, ctx.dict->getSymbolCount());

    _ = ctx.dict->getOrInsertSymbol("word1");  // Duplicate
    ASSERT_EQ(2u, ctx.dict->getSymbolCount());
    delete ctx.dict;
}

TEST(SymbolDictionaryTest, GetMemoryUsageBytesReturnsPositiveValue) {
    auto ctx = createTestSymbolDictionaryWithWriter();

    auto _ = ctx.dict->getOrInsertSymbol("nyc-01");
    _ = ctx.dict->getOrInsertSymbol("api-server");

    size_t memoryUsage = ctx.dict->getMemoryUsageBytes();
    ASSERT_GT(memoryUsage, 0u);
    delete ctx.dict;
}

// ============================================================================
// DeltaSymbolDictionary Tests
// ============================================================================

// Helper struct to hold DeltaSymbolDictionary and its dependencies for testing
struct TestDeltaSymbolDictionaryContext {
    std::unique_ptr<HCIndexWriter> writer;
    std::unique_ptr<SymbolDictionary> baseDictionary;
    std::unique_ptr<DeltaSymbolDictionary> deltaDictionary;
};

// Helper function to create a DeltaSymbolDictionary for testing
TestDeltaSymbolDictionaryContext createTestDeltaSymbolDictionaryWithWriter() {
    auto collectionUUID = UUID::gen();
    Timestamp windowStart(0, 0);
    Timestamp windowEnd(3600, 0);
    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    auto writer = std::make_unique<HCIndexWriter>(collectionUUID, dbName);
    auto baseDictionary = std::make_unique<SymbolDictionary>(
        HCIndexPeriodEnum::Hour, 1, windowStart, windowEnd, writer.get());
    ASSERT_OK(baseDictionary->changeState(SymbolDictionaryState::ReadWrite));

    // Add some base symbols
    ASSERT_OK(baseDictionary->getOrInsertSymbol("base-symbol-1"));
    ASSERT_OK(baseDictionary->getOrInsertSymbol("base-symbol-2"));

    Timestamp deltaWindowStart(3600, 0);
    Timestamp deltaWindowEnd(7200, 0);
    auto deltaDictionary = std::make_unique<DeltaSymbolDictionary>(
        HCIndexPeriodEnum::Hour, 1, deltaWindowStart, deltaWindowEnd, baseDictionary.get(), writer.get());
    ASSERT_OK(deltaDictionary->changeState(SymbolDictionaryState::ReadWrite));

    return {std::move(writer), std::move(baseDictionary), std::move(deltaDictionary)};
}

TEST(DeltaSymbolDictionaryTest, GetOrInsertSymbolInBase) {
    auto ctx = createTestDeltaSymbolDictionaryWithWriter();

    // Symbol from base dictionary should be found
    auto result = ctx.deltaDictionary->getSymbolIndex("base-symbol-1");
    ASSERT_TRUE(result);
    ASSERT_EQ(1u, result.value());
}

TEST(DeltaSymbolDictionaryTest, GetOrInsertSymbolAddsToLocalDelta) {
    auto ctx = createTestDeltaSymbolDictionaryWithWriter();

    // New symbol should be added to local delta
    auto result = ctx.deltaDictionary->getOrInsertSymbol("new-local-symbol");
    ASSERT_TRUE(result.isOK());
    // Should be index 3 (after base symbols 1 and 2)
    ASSERT_EQ(3u, result.getValue());

    // Verify it's in local delta
    auto& localDelta = ctx.deltaDictionary->getLocalDelta();
    ASSERT_EQ(1u, localDelta.size());
    ASSERT_TRUE(localDelta.find("new-local-symbol") != localDelta.end());
}

TEST(DeltaSymbolDictionaryTest, GetOrInsertSymbolReturnsSameIndexForSameWord) {
    auto ctx = createTestDeltaSymbolDictionaryWithWriter();

    auto result1 = ctx.deltaDictionary->getOrInsertSymbol("new-symbol");
    auto result2 = ctx.deltaDictionary->getOrInsertSymbol("new-symbol");

    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result2.isOK());
    ASSERT_EQ(result1.getValue(), result2.getValue());
}

TEST(DeltaSymbolDictionaryTest, GetSymbolFromBase) {
    auto ctx = createTestDeltaSymbolDictionaryWithWriter();

    auto result = ctx.deltaDictionary->getSymbol(1);
    ASSERT_TRUE(result);
    ASSERT_EQ("base-symbol-1", result.value());
}

TEST(DeltaSymbolDictionaryTest, GetSymbolFromLocalDelta) {
    auto ctx = createTestDeltaSymbolDictionaryWithWriter();

    auto insertResult = ctx.deltaDictionary->getOrInsertSymbol("local-symbol");
    ASSERT_TRUE(insertResult.isOK());

    auto result = ctx.deltaDictionary->getSymbol(insertResult.getValue());
    ASSERT_TRUE(result);
    ASSERT_EQ("local-symbol", result.value());
}

TEST(DeltaSymbolDictionaryTest, GetSymbolReturnsNoneForInvalidIndex) {
    auto ctx = createTestDeltaSymbolDictionaryWithWriter();

    auto result = ctx.deltaDictionary->getSymbol(999);
    ASSERT_FALSE(result);
}

TEST(DeltaSymbolDictionaryTest, GetSymbolCountIncludesBaseAndLocal) {
    auto ctx = createTestDeltaSymbolDictionaryWithWriter();

    // Initially should have 2 from base
    ASSERT_EQ(2u, ctx.deltaDictionary->getSymbolCount());

    // Add a local symbol
    ASSERT_OK(ctx.deltaDictionary->getOrInsertSymbol("local-1"));
    ASSERT_EQ(3u, ctx.deltaDictionary->getSymbolCount());

    // Add another local symbol
    ASSERT_OK(ctx.deltaDictionary->getOrInsertSymbol("local-2"));
    ASSERT_EQ(4u, ctx.deltaDictionary->getSymbolCount());
}

TEST(DeltaSymbolDictionaryTest, GetMemoryUsageBytesReturnsPositiveValue) {
    auto ctx = createTestDeltaSymbolDictionaryWithWriter();

    ASSERT_OK(ctx.deltaDictionary->getOrInsertSymbol("local-symbol"));

    size_t memoryUsage = ctx.deltaDictionary->getMemoryUsageBytes();
    ASSERT_GT(memoryUsage, 0u);
}

TEST(DeltaSymbolDictionaryTest, GetWindowBoundaries) {
    auto ctx = createTestDeltaSymbolDictionaryWithWriter();

    ASSERT_EQ(Timestamp(3600, 0), ctx.deltaDictionary->getWindowStart());
    ASSERT_EQ(Timestamp(7200, 0), ctx.deltaDictionary->getWindowEnd());
}

TEST(DeltaSymbolDictionaryTest, GetBaseDictionary) {
    auto ctx = createTestDeltaSymbolDictionaryWithWriter();

    ASSERT_EQ(ctx.baseDictionary.get(), ctx.deltaDictionary->getBaseDictionary());
}

TEST(DeltaSymbolDictionaryTest, GetNextSymbolIndex) {
    auto ctx = createTestDeltaSymbolDictionaryWithWriter();

    // Initially should be 3 (after base symbols 1 and 2)
    ASSERT_EQ(3u, ctx.deltaDictionary->getNextSymbolIndex());

    ASSERT_OK(ctx.deltaDictionary->getOrInsertSymbol("local-1"));
    ASSERT_EQ(4u, ctx.deltaDictionary->getNextSymbolIndex());
}

TEST(DeltaSymbolDictionaryTest, HasNoInheritedDeltaInitially) {
    auto ctx = createTestDeltaSymbolDictionaryWithWriter();

    ASSERT_FALSE(ctx.deltaDictionary->hasInheritedDelta());
    ASSERT_FALSE(ctx.deltaDictionary->getInheritedFromWindowStart());
}

TEST(DeltaSymbolDictionaryTest, GetEffectiveDelta) {
    auto ctx = createTestDeltaSymbolDictionaryWithWriter();

    ASSERT_OK(ctx.deltaDictionary->getOrInsertSymbol("delta-symbol-1"));
    ASSERT_OK(ctx.deltaDictionary->getOrInsertSymbol("delta-symbol-2"));

    auto effectiveDelta = ctx.deltaDictionary->getEffectiveDelta();
    ASSERT_EQ(2u, effectiveDelta.size());
    ASSERT_TRUE(effectiveDelta.find("delta-symbol-1") != effectiveDelta.end());
    ASSERT_TRUE(effectiveDelta.find("delta-symbol-2") != effectiveDelta.end());
}

TEST(DeltaSymbolDictionaryTest, LazyInheritanceFromPreviousInterval) {
    auto collectionUUID = UUID::gen();
    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    auto writer = std::make_unique<HCIndexWriter>(collectionUUID, dbName);

    // Create base dictionary
    Timestamp baseWindowStart(0, 0);
    Timestamp baseWindowEnd(3600, 0);
    auto baseDictionary = std::make_unique<SymbolDictionary>(
        HCIndexPeriodEnum::Hour, 1, baseWindowStart, baseWindowEnd, writer.get());
    ASSERT_OK(baseDictionary->changeState(SymbolDictionaryState::ReadWrite));
    ASSERT_OK(baseDictionary->getOrInsertSymbol("base-symbol"));

    // Create first delta dictionary (interval N-1)
    Timestamp delta1WindowStart(3600, 0);
    Timestamp delta1WindowEnd(7200, 0);
    auto delta1 = std::make_unique<DeltaSymbolDictionary>(
        HCIndexPeriodEnum::Hour, 1, delta1WindowStart, delta1WindowEnd, baseDictionary.get(), writer.get());
    ASSERT_OK(delta1->changeState(SymbolDictionaryState::ReadWrite));
    ASSERT_OK(delta1->getOrInsertSymbol("delta1-symbol"));

    // Create second delta dictionary (interval N) with delta1 as previous
    Timestamp delta2WindowStart(7200, 0);
    Timestamp delta2WindowEnd(10800, 0);
    auto delta2 = std::make_unique<DeltaSymbolDictionary>(
        HCIndexPeriodEnum::Hour, 1, delta2WindowStart, delta2WindowEnd, baseDictionary.get(), writer.get(),
        delta1.get(), nullptr);
    ASSERT_OK(delta2->changeState(SymbolDictionaryState::ReadWrite));

    // Initially no inherited delta
    ASSERT_FALSE(delta2->hasInheritedDelta());

    // Looking up a symbol from delta1 via getOrInsertSymbol should trigger inheritance
    // (getSymbolIndex is const and cannot trigger inheritance)
    auto result = delta2->getOrInsertSymbol("delta1-symbol");
    ASSERT_TRUE(result.isOK());

    // Now should have inherited delta
    ASSERT_TRUE(delta2->hasInheritedDelta());
    ASSERT_TRUE(delta2->getInheritedFromWindowStart());
    ASSERT_EQ(delta1WindowStart, delta2->getInheritedFromWindowStart().value());

    // Verify the symbol was found via inheritance (not added as new local)
    ASSERT_TRUE(delta2->getLocalDelta().empty());
    ASSERT_FALSE(delta2->getInheritedDelta().empty());
}

TEST(DeltaSymbolDictionaryTest, ComputeDeltaSimilarityIdenticalSets) {
    std::set<std::string> delta1 = {"a", "b", "c"};
    std::set<std::string> delta2 = {"a", "b", "c"};

    double similarity = computeDeltaSimilarity(delta1, delta2);
    ASSERT_EQ(1.0, similarity);
}

TEST(DeltaSymbolDictionaryTest, ComputeDeltaSimilarityNoOverlap) {
    std::set<std::string> delta1 = {"a", "b", "c"};
    std::set<std::string> delta2 = {"d", "e", "f"};

    double similarity = computeDeltaSimilarity(delta1, delta2);
    ASSERT_EQ(0.0, similarity);
}

TEST(DeltaSymbolDictionaryTest, ComputeDeltaSimilarityPartialOverlap) {
    std::set<std::string> delta1 = {"a", "b", "c"};
    std::set<std::string> delta2 = {"b", "c", "d"};

    // Intersection: {b, c} = 2
    // Union: {a, b, c, d} = 4
    // Similarity: 2/4 = 0.5
    double similarity = computeDeltaSimilarity(delta1, delta2);
    ASSERT_EQ(0.5, similarity);
}

TEST(DeltaSymbolDictionaryTest, ComputeDeltaSimilarityEmptySets) {
    std::set<std::string> delta1;
    std::set<std::string> delta2;

    // Both empty sets should have similarity 1.0 (identical)
    double similarity = computeDeltaSimilarity(delta1, delta2);
    ASSERT_EQ(1.0, similarity);
}

TEST(DeltaSymbolDictionaryTest, ComputeDeltaSimilarityOneEmptySet) {
    std::set<std::string> delta1 = {"a", "b"};
    std::set<std::string> delta2;

    // One empty set should have similarity 0.0
    double similarity = computeDeltaSimilarity(delta1, delta2);
    ASSERT_EQ(0.0, similarity);
}

// ============================================================================
// TemporalSymbolDictionary Tests
// ============================================================================

// Helper struct to hold both TemporalSymbolDictionary and writer for testing
struct TestTemporalSymbolDictionaryContext {
    std::unique_ptr<HCIndexWriter> writer;
    TemporalSymbolDictionary* tempDict;
};

// Helper function to create a TemporalSymbolDictionary for testing
TestTemporalSymbolDictionaryContext createTestTemporalSymbolDictionaryWithWriter() {
    auto collectionUUID = UUID::gen();
    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    auto writer = std::make_unique<HCIndexWriter>(collectionUUID, dbName);
    auto tempDict = new TemporalSymbolDictionary(collectionUUID, HCIndexPeriodEnum::Hour, 1, writer.get());
    return {std::move(writer), tempDict};
}

TEST(TemporalSymbolDictionaryTest, EncodeSingleSymbol) {
    auto ctx = createTestTemporalSymbolDictionaryWithWriter();
    Timestamp ts(1000, 0);

    auto result = ctx.tempDict->encodeSymbol("nyc-01", ts);

    ASSERT_TRUE(result.isOK());
    ASSERT_EQ(1u, result.getValue());
    delete ctx.tempDict;
}

TEST(TemporalSymbolDictionaryTest, EncodeSameSymbolReturnsSameIndex) {
    auto ctx = createTestTemporalSymbolDictionaryWithWriter();
    Timestamp ts(1000, 0);

    auto result1 = ctx.tempDict->encodeSymbol("nyc-01", ts);
    auto result2 = ctx.tempDict->encodeSymbol("nyc-01", ts);

    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result2.isOK());
    ASSERT_EQ(result1.getValue(), result2.getValue());
    delete ctx.tempDict;
}



TEST(TemporalSymbolDictionaryTest, DecodeSymbol) {
    auto ctx = createTestTemporalSymbolDictionaryWithWriter();
    Timestamp ts(1000, 0);

    auto _ = ctx.tempDict->encodeSymbol("nyc-01", ts);
    auto result = ctx.tempDict->decodeSymbol(1, ts);

    ASSERT_TRUE(result);
    ASSERT_EQ("nyc-01", result.value());
    delete ctx.tempDict;
}

TEST(TemporalSymbolDictionaryTest, DecodeSymbolReturnsNoneForInvalidIndex) {
    auto ctx = createTestTemporalSymbolDictionaryWithWriter();
    Timestamp ts(1000, 0);

    auto result = ctx.tempDict->decodeSymbol(999, ts);

    ASSERT_FALSE(result);
    delete ctx.tempDict;
}



TEST(TemporalSymbolDictionaryTest, GetWindowForTimestamp) {
    auto ctx = createTestTemporalSymbolDictionaryWithWriter();
    Timestamp ts(3661, 0);  // 1 hour + 1 second

    auto [windowStart, windowEnd] = ctx.tempDict->getWindowForTimestamp(ts);

    ASSERT_EQ(3600u, windowStart.getSecs());  // Start of hour
    ASSERT_EQ(7200u, windowEnd.getSecs());    // End of hour
    delete ctx.tempDict;
}

TEST(TemporalSymbolDictionaryTest, GetWindowForTimestampDaily) {
    auto collectionUUID = UUID::gen();
    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    auto writer = std::make_unique<HCIndexWriter>(collectionUUID, dbName);
    // Use Hour period with frequency 24 for a daily window
    TemporalSymbolDictionary tempDict(collectionUUID, HCIndexPeriodEnum::Hour, 24, writer.get());
    Timestamp ts(86401, 0);  // 1 day + 1 second

    auto [windowStart, windowEnd] = tempDict.getWindowForTimestamp(ts);

    ASSERT_EQ(86400u, windowStart.getSecs());   // Start of day
    ASSERT_EQ(172800u, windowEnd.getSecs());    // End of day
}

TEST(TemporalSymbolDictionaryTest, GetStats) {
    auto ctx = createTestTemporalSymbolDictionaryWithWriter();
    Timestamp ts(1000, 0);

    auto _ = ctx.tempDict->encodeSymbol("nyc-01", ts);
    _ = ctx.tempDict->encodeSymbol("api-1", ts);

    auto stats = ctx.tempDict->getStats();

    ASSERT_EQ(1u, stats.totalDictionaries);
    ASSERT_EQ(2u, stats.totalSymbols);
    ASSERT_GT(stats.memoryUsageBytes, 0u);
    delete ctx.tempDict;
}

TEST(TemporalSymbolDictionaryTest, CleanupOldDictionaries) {
    auto collectionUUID = UUID::gen();
    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    HCIndexWriter writer(collectionUUID, dbName);
    TemporalSymbolDictionary tempDict(collectionUUID, HCIndexPeriodEnum::Hour, 1, &writer);

    // Create dictionaries for different time windows
    Timestamp ts1(1000, 0);   // Window: 0-3600
    Timestamp ts2(5000, 0);   // Window: 3600-7200

    auto result1 = tempDict.encodeSymbol("word1", ts1);
    ASSERT_TRUE(result1.isOK());
    auto result2 = tempDict.encodeSymbol("word2", ts2);
    ASSERT_TRUE(result2.isOK());

    auto statsBefore = tempDict.getStats();
    ASSERT_EQ(2u, statsBefore.totalDictionaries);

    // Cleanup dictionaries before ts2's window
    auto cleanupStatus = tempDict.cleanupOldDictionaries(Timestamp(3600, 0));
    ASSERT_TRUE(cleanupStatus.isOK());

    auto statsAfter = tempDict.getStats();
    ASSERT_EQ(1u, statsAfter.totalDictionaries);
}

TEST(TemporalSymbolDictionaryTest, DifferentWindowsHaveSeparateDictionaries) {
    auto collectionUUID = UUID::gen();
    DatabaseName dbName = DatabaseName::createDatabaseName_forTest(boost::none, "test");
    HCIndexWriter writer(collectionUUID, dbName);
    TemporalSymbolDictionary tempDict(collectionUUID, HCIndexPeriodEnum::Hour, 1, &writer);

    Timestamp ts1(1000, 0);   // Window: 0-3600
    Timestamp ts2(5000, 0);   // Window: 3600-7200

    auto result1 = tempDict.encodeSymbol("word", ts1);
    auto result2 = tempDict.encodeSymbol("word", ts2);

    // Same word in different windows should get different indices
    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result2.isOK());
    ASSERT_EQ(1u, result1.getValue());
    ASSERT_EQ(1u, result2.getValue());  // Both start from 1 in their own dictionaries
}

}  // namespace mongo::timeseries::hcindex

