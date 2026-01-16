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
 *    As a coverage coverage for the above, the following government rights notice and
 *    disclaimer must be included:
 *
 *    "SPDX-License-Identifier: SSPL-1.0 AND LicenseRef-Proprietary-MongoDB"
 */

#pragma once

#include <cstdint>

#include <boost/optional.hpp>

#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"

namespace mongo::timeseries::hcindex {

/**
 * Interface for symbol dictionaries used by AttributeTable.
 *
 * This interface abstracts the symbol encoding/decoding operations,
 * allowing AttributeTable to work with different dictionary implementations:
 * - SymbolDictionary: Simple per-interval dictionary
 * - DeltaSymbolDictionary: Delta-based dictionary with base + inherited + local layers
 *
 * Key properties:
 * - Symbol indices start from 1 (0 is reserved for missing values)
 * - Once assigned, symbol indices never change
 * - Thread-safety is implementation-specific
 */
class ISymbolDictionary {
public:
    virtual ~ISymbolDictionary() = default;

    /**
     * Look up or insert a symbol.
     *
     * If the symbol exists, returns its index.
     * If the symbol doesn't exist, inserts it and returns the new index.
     *
     * @param word The symbol string to look up or insert
     * @return The symbol index (>= 1) or an error status
     */
    virtual StatusWith<uint32_t> getOrInsertSymbol(StringData word) = 0;

    /**
     * Look up a symbol (read-only, no insertion).
     *
     * @param word The symbol string to look up
     * @return The symbol index if found, boost::none otherwise
     */
    virtual boost::optional<uint32_t> getSymbolIndex(StringData word) const = 0;

    /**
     * Decode a symbol index back to its string value.
     *
     * @param index The symbol index to decode
     * @return The symbol string if found, boost::none otherwise
     */
    virtual boost::optional<StringData> getSymbol(uint32_t index) const = 0;

    /**
     * Return the total number of symbols in this dictionary.
     * For delta dictionaries, this includes base + inherited + local symbols.
     */
    virtual size_t getSymbolCount() const = 0;
};

}  // namespace mongo::timeseries::hcindex
