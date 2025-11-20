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
    auto writer = std::make_unique<HCIndexWriter>(collectionUUID);
    auto dict = new SymbolDictionary(DictionaryGranularity::HOURLY, windowStart, windowEnd, writer.get());
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
    auto writer = std::make_unique<HCIndexWriter>(collectionUUID);
    auto tempDict = new TemporalSymbolDictionary(nullptr, collectionUUID, DictionaryGranularity::HOURLY, writer.get());
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
    auto writer = std::make_unique<HCIndexWriter>(collectionUUID);
    TemporalSymbolDictionary tempDict(nullptr, collectionUUID, DictionaryGranularity::DAILY, writer.get());
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
    HCIndexWriter writer(collectionUUID);
    TemporalSymbolDictionary tempDict(nullptr, collectionUUID, DictionaryGranularity::HOURLY, &writer);

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
    HCIndexWriter writer(collectionUUID);
    TemporalSymbolDictionary tempDict(nullptr, collectionUUID, DictionaryGranularity::HOURLY, &writer);

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

