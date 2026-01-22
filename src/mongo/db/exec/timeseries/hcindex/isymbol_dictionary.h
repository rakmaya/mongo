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

                        // =======================
                        // class ISymbolDictionary
                        // =======================

/**
 * Interface for symbol dictionaries. This interface abstracts the symbol encoding/decoding
 * operations, allowing AttributeTable to work with differen dictionary implementations. Also
 * see SymbolDictionary and DeltaSymbolDictionary
 */
class ISymbolDictionary {
public:
    virtual ~ISymbolDictionary() = default;

    /**
     * Look up or insert the specified 'word' in the dictionary. If the symbol exists, returns its
     * index. If the symbol doesn't exist, inserts it and returns the new index. Returns a symbol
     * index >= 1 on success. Otherwise, returns an error status.
     */
    virtual StatusWith<uint32_t> getOrInsertSymbol(StringData word) = 0;

    /**
     * Look up the specified 'word' in the dictionary and if found, return the symbol index.
     * Otherwise, return boost::none.
     */
    virtual boost::optional<uint32_t> getSymbolIndex(StringData word) const = 0;

    /**
     * Return the decoded string value associated with the specified symbol 'index' if found.
     * Otherwise, return boost::none.
     */
    virtual boost::optional<StringData> getSymbol(uint32_t index) const = 0;

    /**
     * Return the total number of symbols in this dictionary.
     */
    virtual size_t getSymbolCount() const = 0;
};


}  // namespace mongo::timeseries::hcindex
