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

#include "mongo/db/timeseries/hcindex/temporal_symbol_dictionary.h"
#include "mongo/unittest/unittest.h"

namespace mongo::timeseries::hcindex {

// ============================================================================
// SymbolDictionary Tests
// ============================================================================

TEST(SymbolDictionaryTest, GetOrInsertSymbolReturnsNewIndex) {
    SymbolDictionary dict;

    auto result = dict.getOrInsertSymbol("nyc-01");
    ASSERT_TRUE(result.isOK());
    ASSERT_EQ(1u, result.getValue());
}

TEST(SymbolDictionaryTest, GetOrInsertSymbolReturnsSameIndexForSameWord) {
    SymbolDictionary dict;

    auto result1 = dict.getOrInsertSymbol("nyc-01");
    auto result2 = dict.getOrInsertSymbol("nyc-01");

    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result2.isOK());
    ASSERT_EQ(result1.getValue(), result2.getValue());
}

TEST(SymbolDictionaryTest, GetOrInsertSymbolAssignsSequentialIndices) {
    SymbolDictionary dict;

    auto result1 = dict.getOrInsertSymbol("word1");
    auto result2 = dict.getOrInsertSymbol("word2");
    auto result3 = dict.getOrInsertSymbol("word3");

    ASSERT_EQ(1u, result1.getValue());
    ASSERT_EQ(2u, result2.getValue());
    ASSERT_EQ(3u, result3.getValue());
}

TEST(SymbolDictionaryTest, GetSymbolIndexReturnsIndexIfFound) {
    SymbolDictionary dict;

    auto _ = dict.getOrInsertSymbol("nyc-01");
    auto result = dict.getSymbolIndex("nyc-01");

    ASSERT_TRUE(result);
    ASSERT_EQ(1u, result.value());
}

TEST(SymbolDictionaryTest, GetSymbolIndexReturnsNoneIfNotFound) {
    SymbolDictionary dict;

    auto result = dict.getSymbolIndex("not-found");

    ASSERT_FALSE(result);
}

TEST(SymbolDictionaryTest, GetSymbolReturnsWordIfFound) {
    SymbolDictionary dict;

    auto _ = dict.getOrInsertSymbol("nyc-01");
    auto result = dict.getSymbol(1);

    ASSERT_TRUE(result);
    ASSERT_EQ("nyc-01", result.value());
}

TEST(SymbolDictionaryTest, GetSymbolReturnsNoneForInvalidIndex) {
    SymbolDictionary dict;

    auto result = dict.getSymbol(999);

    ASSERT_FALSE(result);
}

TEST(SymbolDictionaryTest, GetSymbolReturnsNoneForReservedZeroIndex) {
    SymbolDictionary dict;

    auto result = dict.getSymbol(0);

    ASSERT_FALSE(result);
}

TEST(SymbolDictionaryTest, GetSymbolCountReturnsCorrectCount) {
    SymbolDictionary dict;

    ASSERT_EQ(0u, dict.getSymbolCount());

    auto _ = dict.getOrInsertSymbol("word1");
    ASSERT_EQ(1u, dict.getSymbolCount());

    _ = dict.getOrInsertSymbol("word2");
    ASSERT_EQ(2u, dict.getSymbolCount());

    _ = dict.getOrInsertSymbol("word1");  // Duplicate
    ASSERT_EQ(2u, dict.getSymbolCount());
}

TEST(SymbolDictionaryTest, GetMemoryUsageBytesReturnsPositiveValue) {
    SymbolDictionary dict;

    auto _ = dict.getOrInsertSymbol("nyc-01");
    _ = dict.getOrInsertSymbol("api-server");

    size_t memoryUsage = dict.getMemoryUsageBytes();
    ASSERT_GT(memoryUsage, 0u);
}

// ============================================================================
// TemporalSymbolDictionary Tests
// ============================================================================

TEST(TemporalSymbolDictionaryTest, EncodeSingleSymbol) {
    TemporalSymbolDictionary tempDict(nullptr, UUID::gen(), DictionaryGranularity::HOURLY);
    Timestamp ts(1000, 0);

    auto result = tempDict.encodeSymbol("nyc-01", ts);

    ASSERT_TRUE(result.isOK());
    ASSERT_EQ(1u, result.getValue());
}

TEST(TemporalSymbolDictionaryTest, EncodeSameSymbolReturnsSameIndex) {
    TemporalSymbolDictionary tempDict(nullptr, UUID::gen(), DictionaryGranularity::HOURLY);
    Timestamp ts(1000, 0);

    auto result1 = tempDict.encodeSymbol("nyc-01", ts);
    auto result2 = tempDict.encodeSymbol("nyc-01", ts);

    ASSERT_TRUE(result1.isOK());
    ASSERT_TRUE(result2.isOK());
    ASSERT_EQ(result1.getValue(), result2.getValue());
}



TEST(TemporalSymbolDictionaryTest, DecodeSymbol) {
    TemporalSymbolDictionary tempDict(nullptr, UUID::gen(), DictionaryGranularity::HOURLY);
    Timestamp ts(1000, 0);

    auto _ = tempDict.encodeSymbol("nyc-01", ts);
    auto result = tempDict.decodeSymbol(1, ts);

    ASSERT_TRUE(result);
    ASSERT_EQ("nyc-01", result.value());
}

TEST(TemporalSymbolDictionaryTest, DecodeSymbolReturnsNoneForInvalidIndex) {
    TemporalSymbolDictionary tempDict(nullptr, UUID::gen(), DictionaryGranularity::HOURLY);
    Timestamp ts(1000, 0);

    auto result = tempDict.decodeSymbol(999, ts);

    ASSERT_FALSE(result);
}



TEST(TemporalSymbolDictionaryTest, GetWindowForTimestamp) {
    TemporalSymbolDictionary tempDict(nullptr, UUID::gen(), DictionaryGranularity::HOURLY);
    Timestamp ts(3661, 0);  // 1 hour + 1 second

    auto [windowStart, windowEnd] = tempDict.getWindowForTimestamp(ts);

    ASSERT_EQ(3600u, windowStart.getSecs());  // Start of hour
    ASSERT_EQ(7200u, windowEnd.getSecs());    // End of hour
}

TEST(TemporalSymbolDictionaryTest, GetWindowForTimestampDaily) {
    TemporalSymbolDictionary tempDict(nullptr, UUID::gen(), DictionaryGranularity::DAILY);
    Timestamp ts(86401, 0);  // 1 day + 1 second

    auto [windowStart, windowEnd] = tempDict.getWindowForTimestamp(ts);

    ASSERT_EQ(86400u, windowStart.getSecs());   // Start of day
    ASSERT_EQ(172800u, windowEnd.getSecs());    // End of day
}

TEST(TemporalSymbolDictionaryTest, GetStats) {
    TemporalSymbolDictionary tempDict(nullptr, UUID::gen(), DictionaryGranularity::HOURLY);
    Timestamp ts(1000, 0);

    auto _ = tempDict.encodeSymbol("nyc-01", ts);
    _ = tempDict.encodeSymbol("api-1", ts);

    auto stats = tempDict.getStats();

    ASSERT_EQ(1u, stats.totalDictionaries);
    ASSERT_EQ(2u, stats.totalSymbols);
    ASSERT_GT(stats.memoryUsageBytes, 0u);
}

TEST(TemporalSymbolDictionaryTest, CleanupOldDictionaries) {
    TemporalSymbolDictionary tempDict(nullptr, UUID::gen(), DictionaryGranularity::HOURLY);

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
    TemporalSymbolDictionary tempDict(nullptr, UUID::gen(), DictionaryGranularity::HOURLY);

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

