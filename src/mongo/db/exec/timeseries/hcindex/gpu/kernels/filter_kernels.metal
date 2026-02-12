/**
 *    Copyright (C) 2024-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    Metal shaders for HCIndex GPU filtering operations.
 */

#include <metal_stdlib>
using namespace metal;

/**
 * Predicate operations (must match PredicateOp enum in C++)
 * TODO: Enforce build-time checks! Unlike the HIP implementation
 * there is no include-files that we can bring in here to enforce
 * this in a platform independent-way. Metal's support here is ify.
 * One way would be to have these fundamental definitions in a
 * include file and then **generate** the metal and HIP compliant
 * headers from the bazel build. It is a bit too much for PoC/reference
 * implementation.
 */
enum PredicateOp : uint32_t {
    // Equal
    EQ = 0,

    // Not equal
    NE = 1,

    // Less than
    LT = 2,

    // Less than or equal
    LE = 3,

    // Greater than
    GT = 4,

    // Greater than or equal
    GE = 5,
};

/**
 * Evaluate a single predicate.
 */
inline bool evaluatePredicate(uint32_t columnValue, uint32_t predicateValue, uint32_t op) {
    switch (op) {
        case EQ: return columnValue == predicateValue;
        case NE: return columnValue != predicateValue;
        case LT: return columnValue < predicateValue;
        case LE: return columnValue <= predicateValue;
        case GT: return columnValue > predicateValue;
        case GE: return columnValue >= predicateValue;
        default: return false;
    }
}

/**
 * Filter column kernel - produces a bitmap of matching rows.
 *
 * Each thread processes 32 rows (one word of the output bitmap).
 *
 * @param column Input column data (uint32_t per row)
 * @param resultBitmap Output bitmap (1 bit per row)
 * @param value Predicate value to compare against
 * @param numRows Total number of rows
 * @param op Predicate operation
 * @param wordIdx Thread position (which 32-row chunk to process)
 */
kernel void filterColumnKernel(
    device const uint32_t* column [[buffer(0)]],
    device uint32_t* resultBitmap [[buffer(1)]],
    constant uint32_t& value [[buffer(2)]],
    constant uint32_t& numRows [[buffer(3)]],
    constant uint32_t& op [[buffer(4)]],
    uint wordIdx [[thread_position_in_grid]]
) {
    uint32_t startRow = wordIdx * 32;
    if (startRow >= numRows) {
        return;
    }

    uint32_t bits = 0;
    uint32_t endRow = min(startRow + 32, numRows);

    for (uint32_t row = startRow; row < endRow; ++row) {
        if (evaluatePredicate(column[row], value, op)) {
            bits |= (1u << (row - startRow));
        }
    }

    resultBitmap[wordIdx] = bits;
}

/**
 * AND two bitmaps together (accumulator &= operand).
 * Each thread processes one word (TODO: fix implied magic number!).
 */
kernel void andBitmapsKernel(
    device uint32_t* accumulator [[buffer(0)]],
    device const uint32_t* operand [[buffer(1)]],
    uint wordIdx [[thread_position_in_grid]]
) {
    accumulator[wordIdx] &= operand[wordIdx];
}

/**
 * OR two bitmaps together (accumulator |= operand).
 * Each thread processes one word (TODO: fix implied magic number!).
 */
kernel void orBitmapsKernel(
    device uint32_t* accumulator [[buffer(0)]],
    device const uint32_t* operand [[buffer(1)]],
    uint wordIdx [[thread_position_in_grid]]
) {
    accumulator[wordIdx] |= operand[wordIdx];
}

/**
 * Count set bits in a bitmap (reduction).
 *
 * Each thread counts bits in one word. Final reduction is done on CPU
 * (TODO: fix implied magic number!).
 */
kernel void countBitsKernel(
    device const uint32_t* bitmap [[buffer(0)]],
    device atomic_uint* partialCounts [[buffer(1)]],
    uint wordIdx [[thread_position_in_grid]]
) {
    uint32_t word = bitmap[wordIdx];
    uint32_t count = popcount(word);
    atomic_fetch_add_explicit(partialCounts, count, memory_order_relaxed);
}

