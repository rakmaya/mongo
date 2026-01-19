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

#pragma once

#include <boost/optional.hpp>

#include <cstdint>
#include <map>
#include <shared_mutex>
#include <string>
#include <vector>

#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/timestamp.h"
#include "mongo/db/exec/timeseries/hcindex/isymbol_dictionary.h"
#include "mongo/db/timeseries/timeseries_gen.h"

// Forward declaration from mongo namespace
namespace mongo {
class MatchExpression;
}  // namespace mongo

namespace mongo::timeseries::hcindex {

// FORWARD DECLARATIONS
class HCIndexWriter;
class BitmapIndex;
class AttributeTable;

/**
 * Result of inserting a row into an attribute table. Contains the row ID,
 * a flag indicating whether this is a new row (true) or a duplicate (false),
 * and the row vector (symbol indices for each column).
 */
struct InsertRowResult {
    int64_t rowId;
    bool isNewRow;
    bool hasSchemaChanged;
    AttributeTable* table;
    std::vector<uint32_t> row;  // Symbol indices for each column
};

/**
 * State machine for AttributeTable lifecycle:
 * - NOP: Initial state, no operations allowed
 * - Reconstruction: Table is being reconstructed from stored operations
 * - ReadWrite: Table is in normal write mode (new rows can be added)
 * - ReadOnly: Table is locked, no modifications allowed
 *
 * State transitions:
 * - NOP -> Reconstruction or ReadWrite
 * - Reconstruction -> ReadWrite - to accept new data after reconstruction
 * - Reconstruction -> ReadOnly
 * - ReadWrite -> ReadOnly
 * - ReadOnly -> (no transitions allowed)
 */

enum class AttributeTableState {
    NOP,
    Reconstruction,
    ReadWrite,
    ReadOnly
};

/**
 * Represents a hierarchical prdicate structure for filtering rows.
 *
 * - LEAF: A simple equality predicate (refRowVec with symbol indices)
 * - AND: All children must match (intersection of results)
 * - OR: Any child can match (union of results)
 *
 * Examples:
 * - Simple AND: {a: 1, b: 2} -> LEAF with refRowVec=[sym_a, sym_b]
 * - Simple OR: {$or: [{a: 1}, {b: 2}]} -> OR with 2 LEAF children
 * - Mixed: {$or: [{a: 1}, {$and: [{b: 2}, {c: 3}]}]} -> OR with 1 LEAF and 1 AND child
 *
 * Note: The reason for reRowVec is that we can pack the columns in
 * attribute tables into SIMD optimized load and rip through them quickly. The
 * actual vectorized load and compare is not implemented yet in the PoC.
 */
struct AttributeTablePredicate {
    enum Type { LEAF, AND, OR };

    Type type = LEAF;

    // For LEAF: symbol indices in schema order (0 <=> not part of predicate).
    // The size is only as large as necessary to encode the predicate (i.e no
    // padding with 0s) and is also in the order of the attribute table schema.
    // A row mathes if for all non-zero entries in refRowVec, the corresponding
    // column is in the row has the same sybol index.
    std::vector<uint32_t> refRowVec;

    // For AND/OR: child predicates
    std::vector<AttributeTablePredicate> children;

    /**
     * Return true if this predicate is a leaf. Otherwise, return false.
     */
    bool isLeaf() const {
        return type == LEAF;
    }

    /**
     * Return true if this predicate is an AND node. Otherwise, return false.
     */
    bool isAnd() const {
        return type == AND;
    }

    /**
     * Return true if this predicate is an OR node. Otherwise, return false.
     */
    bool isOr() const {
        return type == OR;
    }
};


/**
 * Table of just integer values represents a single attribute table for a
 * specific time window. An attribute table is an evolving columnar
 * structure that stores metadata as symbol indices.
 *
 * The memory representation is columnar to ensure that we can cache out
 * unused columns accordingly. The storage aspect is maintained by the
 * HCIndexWriter which captures the operations.
 *
 * The structure allows for an append-only model and provides
 * schema evolution so new columns can be added with near-0 overhead.
 * Missing values are denoted by 0, which is reserved in the symbol dictionary.
 * RowIDs are unique (within the table) and is stable.
 */
class AttributeTable {

public:


    //- CONSTRUCTORS


    /**
     * Constructs an AttributeTable for the time window [`windowStart`, `windowEnd`) that uses the
     * provided `symbolDictionary` to translate user metadata to and from attribute table encoded
     * integers. The table is configured with the given `period` and `frequency`, and its persistent
     * encoding lifecycle is managed by `writer`. The attribute table is initiatized in the NOP
     * state.
     */
    AttributeTable(ISymbolDictionary* symbolDictionary,
                   HCIndexWriter* writer,
                   HCIndexPeriodEnum period,
                   int32_t frequency,
                   const Timestamp& windowStart = Timestamp(),
                   const Timestamp& windowEnd = Timestamp());


    //- ACCESSORS


    /**
     * Return the row having the specified `rowId` from the attribute table, if found.
     * Otherwise, return boost::none.
     */
    boost::optional<std::vector<uint32_t>> getRow(int64_t rowId) const;

    /**
     * Evaluates the specified  hierarchical `predicate` against the attribute table and returns the
     * row IDs that satisfy the predicate.
     *
     * A row matches a LEAF predicate if all specified columns contain the required symbol indices,
     * matches an OR predicate if it satisfies any child predicate (set union), and matches an AND
     * predicate if it satisfies all child predicates (set intersection). If a `bitmapIndex` is
     * provided, it is used to pre-filter candidate rows prior to the full evaluation. Note that
     * bitmapIndex can only be used when a LEAF if of a strict equality expression.
     */
    std::vector<int64_t> queryRows(const AttributeTablePredicate& predicate,
                                   BitmapIndex* bitmapIndex = nullptr) const;

    /**
     * Converts the specified `matchExpressio` and returns an `AttributeTablePredicate` by resolving
     * equality constraints to symbol and column indices, recursively handling AND/OR expressions
     * with union and intersection semantics, and returning an error if the expression cannot be
     * converted or any symbol lookup fails. TODO: Handle inequality and regular expressions
     */
    StatusWith<AttributeTablePredicate> convertMatchExpressionToPredicate(
        const ::mongo::MatchExpression* matchExpression) const;

    /**
     * Return the current schema as a vector of column names in the order it was inserted into the
     * table.
     */
    const std::vector<std::string>& getSchema() const;

    /**
     * Return the column name-to-index mapping.
     */
    const std::map<std::string, size_t>& getFieldToColumnIndexMap() const;

    /**
     * Return the total number of rows in this attribute table.
     */
    size_t getRowCount() const;

    /**
     * Return the approximate memory usage of this attribute table in bytes.
     */
    size_t getMemoryUsageBytes() const;


    //- MODIFIERS


    /**
     * Insert a new row into this attribute table having values specified within the given
     * `metadata` and return a stable row ID along with a flag indicating if it is a new row.
     * Behavior is undefined unless the metadata parameter is a BSON object containing field names
     * and their corresponding string values. The method looks up each value in the symbol
     * dictionary to get its index, automatically evolves the schema if new fields appear in the
     * metadata, and returns the existing row ID if a row with the exact same metadata already
     * exists (with isNewRow=false). Returns an error if the row cannot be inserted or if any value
     * lookup fails.
     */
    StatusWith<InsertRowResult> insertRow(const BSONObj& metadata);

    /**
     * Insert a new row with the specified symbol indices and return a stable row ID. The row
     * parameter contains symbol indices for each column in the current schema. If the row has fewer
     * elements than the current schema, missing columns are filled with 0 (missing value
     * indicator). This is a lower-level method primarily used internally. Returns an error if the
     * row cannot be inserted.
     * TODO: Remove this method from the public API (currently, the HCIndexReader needs it).
     */
    StatusWith<int64_t> insertRowDirect(const std::vector<uint32_t>& row);

    /**
     * Add a new column to the schema with the specified fieldName. Return an error if the column
     * already exists or if the column cannot be added.
     */
    Status addColumn(StringData fieldName);

    /**
     * Change the state of this table.
     * - NOP: Initial state, no operations allowed
     * - Reconstruction: Table was reconstructed from stored operations
     * - ReadWrite: Table is in the read+write mode (new rows can be added)
     * - ReadOnly: Table is locked, no modifications allowed
     *
     * Only the following transitions are allowed:
     * - NOP -> Reconstruction or ReadWrite
     * - Reconstruction -> ReadWrite - to accept new data after reconstruction
     * - Reconstruction -> ReadOnly (indicates table from from immutable source)
     * - ReadWrite -> ReadOnly
     * - ReadOnly -> (no transitions allowed)
     */
    Status changeState(AttributeTableState newState);

    /**
     * Set the writer for this table. This allows a table to be updated later.
     */
    void setWriter(HCIndexWriter* w);

    /**
     * Flush any pending operations to the database via the writer.
     */
    void flush();


private:


    //- PRIVATE METHODS


    /**
     * Queries matching row IDs for a LEAF predicate using simple equality matching, optionally
     * pre-filtering candidates via `bitmapIndex` before validating against the attribute table.
     */
    std::vector<int64_t> queryRowsLeaf(const std::vector<uint32_t>& refRowVec,
                                       BitmapIndex* bitmapIndex = nullptr) const;

    /**
     * Internal method to check if a row with the same symbol indices already exists. Returns the
     * row ID if found, or boost::none if not found.
     */
    boost::optional<int64_t> findDuplicateRow(const std::vector<uint32_t>& row) const;

    /**
     * Convert metadata BSON object to a row vector. For each field in the metadata, looks up the
     * string value in the symbol dictionary to get its index. Evolves the schema if new fields
     * appear. Returns the row vector with symbol indices in schema order, with 0 for missing
     * fields. Returns an error if any value lookup fails.
     */
    StatusWith<std::vector<uint32_t>> metadataToRow(const BSONObj& metadata);

    /**
     * Internal method to write a schema field to the writer.
     */
    Status writeSchema(const std::string& fieldName, size_t columnIndex);

    /**
     * Internal method to write a row to the writer.
     */
    Status writeRow(const std::vector<uint32_t>& row);


    //- DATA


    // Symbol dictionary.
    ISymbolDictionary* _symbolDictionary;

    // Writer for writing new attribute operations.
    HCIndexWriter* _writer;

    // Period and frequency for time window calculation.
    HCIndexPeriodEnum _period;
    int32_t _frequency;

    // Current state of the table.
    AttributeTableState _state = AttributeTableState::NOP;

    // Whether there are pending changes to flush.
    bool _isDirty = false;

    // Window boundaries for this attribute table.
    Timestamp _windowStart;
    Timestamp _windowEnd;

    // Each level-2 vector represents a column of symbol indices
    std::vector<std::vector<uint32_t>> _columns;

    // Track at which row ID each column was added (for schema evolution)
    std::vector<int64_t> _columnAddedAtRowId;

    // Column names in order
    std::vector<std::string> _schema;

    // Map: field name -> column index (for fast lookups)
    std::map<std::string, size_t> _fieldToColumnIndex;

    // TODO: Allow exclusive and shared access mode. Currenly, we just do a full lock regardless of
    // the operation type.
    mutable std::shared_mutex _mutex;
};


}  // namespace mongo::timeseries::hcindex
