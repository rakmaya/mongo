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

#include "mongo/db/exec/timeseries/hcindex/attribute_table.h"

#include <chrono>
#include <cstdlib>
#include <set>

#include "mongo/base/error_codes.h"
#include "mongo/db/exec/timeseries/hcindex/bitmap_index.h"
#include "mongo/db/exec/timeseries/hcindex/gpu/gpu_attribute_table.h"
#include "mongo/db/exec/timeseries/hcindex/gpu/gpu_attribute_table_manager.h"
#include "mongo/db/exec/timeseries/hcindex/writer.h"
#include "mongo/db/matcher/expression.h"
#include "mongo/db/update/path_support.h"
#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

namespace mongo::timeseries::hcindex {

// Static variable definition - force CPU path for benchmarking
bool AttributeTable::_forceCpuPath = (std::getenv("MONGO_HCINDEX_FORCE_CPU") != nullptr);

                        // --------------------
                        // class AttributeTable
                        // --------------------

//- CONSTRUCTORS


AttributeTable::AttributeTable(ISymbolDictionary* symbolDictionary,
                               HCIndexWriter* writer,
                               HCIndexPeriodEnum period,
                               int32_t frequency,
                               const Timestamp& windowStart,
                               const Timestamp& windowEnd)
    : _symbolDictionary(symbolDictionary),
      _writer(writer),
      _period(period),
      _frequency(frequency),
      _isDirty(false),
      _windowStart(windowStart),
      _windowEnd(windowEnd) {
}

AttributeTable::~AttributeTable() = default;


//- ACCESSORS


boost::optional<std::vector<uint32_t>> AttributeTable::getRow(int64_t rowId) const {
    std::shared_lock<std::shared_mutex> lock(_mutex);

    // Check if rowId is valid
    if (_columns.empty() || rowId < 0 || rowId >= static_cast<int64_t>(_columns[0].size())) {
        return boost::none;
    }

    // Reconstruct row from columns
    std::vector<uint32_t> row;
    for (const auto& column : _columns) {
        row.push_back(column[rowId]);
    }

    return row;
}


std::vector<int64_t> AttributeTable::queryRows(const AttributeTablePredicate& predicate,
                                               BitmapIndex* bitmapIndex) const {
    std::shared_lock<std::shared_mutex> lock(_mutex);

    // At a leeaf
    if (predicate.isLeaf()) {
        return queryRowsLeaf(predicate.refRowVec, bitmapIndex);
    }

    // At a node. Handle OR (union) and AND (intersection) reccursively.

    if (predicate.isOr()) {
        std::set<int64_t> uniqueMatches;
        for (const auto& child : predicate.children) {
            auto childResults = queryRows(child, bitmapIndex);
            for (auto rowId : childResults) {
                uniqueMatches.insert(rowId);
            }
        }
        std::vector<int64_t> result(uniqueMatches.begin(), uniqueMatches.end());
        return result;
    }

    if (predicate.isAnd()) {
        if (predicate.children.empty()) {
            return {};
        }

        // Create a combined result that contains rows that appear in both left and
        // right expressions.

        auto result = queryRows(predicate.children[0], bitmapIndex);
        for (size_t i = 1; i < predicate.children.size(); ++i) {
            auto childResults = queryRows(predicate.children[i], bitmapIndex);

            std::set<int64_t> childSet(childResults.begin(), childResults.end());
            std::vector<int64_t> intersection;
            for (auto rowId : result) {
                if (childSet.count(rowId) > 0) {
                    intersection.push_back(rowId);
                }
            }
            result = intersection;

            if (result.empty()) {
                break;
            }
        }
        return result;
    }

    // Defense: should never reach here
    return {};
}


StatusWith<AttributeTablePredicate> AttributeTable::convertMatchExpressionToPredicate(
    const ::mongo::MatchExpression* matchExpr) const {

    // TODO: Some of the errors are absorbed since the PoC implementation does not make these
    // parsing into a separate PlanStage. This is an area that need to be changed.

    if (!matchExpr) {
        return Status(ErrorCodes::BadValue, "matchExpr cannot be null");
    }

    // Handle OR expressions: create an OR node with children
    if (matchExpr->matchType() == ::mongo::MatchExpression::OR) {
        AttributeTablePredicate orPredicate;
        orPredicate.type = AttributeTablePredicate::OR;

        for (size_t i = 0; i < matchExpr->numChildren(); ++i) {
            auto branchResult = convertMatchExpressionToPredicate(matchExpr->getChild(i));
            if (!branchResult.isOK()) {
                // Failed! skip it. TODO: this should be an error!
                continue;
            }
            orPredicate.children.push_back(branchResult.getValue());
        }

        if (!orPredicate.children.empty()) {
            return orPredicate;
        }
        return AttributeTablePredicate();
    }

    // Handle AND expressions: create an AND node with children
    if (matchExpr->matchType() == ::mongo::MatchExpression::AND) {
        AttributeTablePredicate andPredicate;
        andPredicate.type = AttributeTablePredicate::AND;

        for (size_t i = 0; i < matchExpr->numChildren(); ++i) {
            auto branchResult = convertMatchExpressionToPredicate(matchExpr->getChild(i));
            if (!branchResult.isOK()) {
                // Failed! skip it. TODO: this should be an error!
                continue;
            }
            andPredicate.children.push_back(branchResult.getValue());
        }

        if (!andPredicate.children.empty()) {
            return andPredicate;
        }
        return AttributeTablePredicate();
    }

    // Now we are at a leaf of the expression tree.

    mongo::pathsupport::EqualityMatches equalities;
    auto status = mongo::pathsupport::extractEqualityMatches(*matchExpr, &equalities);
    if (!status.isOK()) {
        return status;
    }

    // Normalize the reference vector by padding missing columns with 0s up to the highest column
    // index used in the expression (no padding beyond that).

    std::vector<uint32_t> refRowVec;
    size_t maxColumnIndex = 0;

    // First maximum index necessary.
    for (const auto& [fieldPath, eqExpr] : equalities) {
        std::string schemaFieldPath = std::string(fieldPath);
        if (schemaFieldPath.find("metadata.") == 0) {
            schemaFieldPath = schemaFieldPath.substr(9);
        }

        auto it = _fieldToColumnIndex.find(schemaFieldPath);
        if (it != _fieldToColumnIndex.end()) {
            maxColumnIndex = std::max(maxColumnIndex, it->second);
        }
    }

    refRowVec.resize(maxColumnIndex + 1, 0);
    // Fill in the symbol indices for matching fields
    for (const auto& [fieldPath, eqExpr] : equalities) {
        std::string schemaFieldPath = std::string(fieldPath);
        if (schemaFieldPath.find("metadata.") == 0) {
            schemaFieldPath = schemaFieldPath.substr(9);
        }

        auto fieldIt = _fieldToColumnIndex.find(schemaFieldPath);
        if (fieldIt == _fieldToColumnIndex.end()) {
            // Field not in schema - no rows will match
            // TODO Handle NULL and NotPresent distinctly.
            return AttributeTablePredicate();
        }

        const BSONElement& data = eqExpr->getData();

        if (data.eoo()) {
            return Status(ErrorCodes::BadValue,
                          "HCIndex: Predicate value for field '" + std::string(fieldPath) +
                              "' is invalid (EOO element)");
        }

        if (data.type() != BSONType::string) {
            return Status(ErrorCodes::BadValue,
                          "HCIndex: Predicate value for field '" + std::string(fieldPath) +
                              "' must be a string, got: " + typeName(data.type()));
        }

        StringData value = data.valueStringData();

        auto symbolIndex = _symbolDictionary->getSymbolIndex(value);
        if (!symbolIndex) {
            // Return empty predicate (no rows will match)
            return AttributeTablePredicate();
        }
        refRowVec[fieldIt->second] = *symbolIndex;
    }

    // Trim trailing zeros.
    while (!refRowVec.empty() && refRowVec.back() == 0) {
        refRowVec.pop_back();
    }

    AttributeTablePredicate leafPredicate;
    leafPredicate.type = AttributeTablePredicate::LEAF;
    leafPredicate.refRowVec = refRowVec;
    return leafPredicate;
}

const std::vector<std::string>& AttributeTable::getSchema() const {
    std::shared_lock<std::shared_mutex> lock(_mutex);
    return _schema;
}

const std::map<std::string, size_t>& AttributeTable::getFieldToColumnIndexMap() const {
    std::shared_lock<std::shared_mutex> lock(_mutex);
    return _fieldToColumnIndex;
}

size_t AttributeTable::getRowCount() const {
    std::shared_lock<std::shared_mutex> lock(_mutex);
    return _columns.empty() ? 0 : _columns[0].size();
}

size_t AttributeTable::getMemoryUsageBytes() const {
    std::shared_lock<std::shared_mutex> lock(_mutex);

    size_t totalBytes = 0;

    // Schema memory
    for (const auto& fieldName : _schema) {
        totalBytes += fieldName.size();
    }
    totalBytes += _schema.capacity() * sizeof(std::string);

    // Columns memory
    for (const auto& column : _columns) {
        totalBytes += column.capacity() * sizeof(uint32_t);
    }
    totalBytes += _columns.capacity() * sizeof(std::vector<uint32_t>);

    // columnAddedAtRowId memory
    totalBytes += _columnAddedAtRowId.capacity() * sizeof(int64_t);

    // Field to column index map
    for (const auto& [fieldName, colIdx] : _fieldToColumnIndex) {
        totalBytes += fieldName.size();
    }

    // Row hash map.
    for (const auto& [hash, rowIds] : _rowHashToRowIds) {
        totalBytes += sizeof(size_t) + rowIds.capacity() * sizeof(int64_t);
    }
    totalBytes += _rowHashToRowIds.bucket_count() * sizeof(void*);

    return totalBytes;
}


//- MODIFIERS


StatusWith<InsertRowResult> AttributeTable::insertRow(const BSONObj& metadata) {
    std::unique_lock<std::shared_mutex> lock(_mutex);

    auto prevSchemaSize = _schema.size();

    // Convert metadata to row vector
    auto rowResult = metadataToRow(metadata);
    if (!rowResult.isOK()) {
        return rowResult.getStatus();
    }

    std::vector<uint32_t> row = rowResult.getValue();

    // Check for duplicate row
    auto duplicateRowId = findDuplicateRow(row);
    if (duplicateRowId) {
        return InsertRowResult{duplicateRowId.value(), false, false, this, row};
    }

    auto insertResult = insertRowDirect(row);
    if (!insertResult.isOK()) {
        return insertResult.getStatus();
    }

    // Mark GPU data as stale since we've added a new row
    _gpuDataStale = true;

    return InsertRowResult{
        insertResult.getValue(), true, prevSchemaSize != _schema.size(), this, row};
}

StatusWith<int64_t> AttributeTable::insertRowDirect(const std::vector<uint32_t>& row) {
    // Note: Caller must hold the mutex lock. This method does not acquire the lock
    // to avoid deadlock when called from insertRow() which already holds the lock.

    // Pad row with 0s if it's shorter than schema
    std::vector<uint32_t> paddedRow = row;
    while (paddedRow.size() < _schema.size()) {
        paddedRow.push_back(0);
    }

    // Validate row size
    if (paddedRow.size() > _schema.size()) {
        return Status(ErrorCodes::BadValue, "Row has more columns than schema");
    }

    // Get current row count (all columns have same size)
    int64_t rowId = _columns.empty() ? 0 : _columns[0].size();

    // Append values to each column
    for (size_t colIdx = 0; colIdx < paddedRow.size(); ++colIdx) {
        // Ensure column exists
        if (colIdx >= _columns.size()) {
            _columns.push_back(std::vector<uint32_t>());
            _columnAddedAtRowId.push_back(rowId);
        }
        _columns[colIdx].push_back(paddedRow[colIdx]);
    }

    // Add to hash map for fast duplicate detection
    size_t rowHash = computeRowHash(paddedRow);
    _rowHashToRowIds[rowHash].push_back(rowId);

    return rowId;
}

Status AttributeTable::addColumn(StringData fieldName) {
    std::unique_lock<std::shared_mutex> lock(_mutex);

    // Check if column already exists
    if (_fieldToColumnIndex.find(std::string(fieldName)) != _fieldToColumnIndex.end()) {
        return Status(ErrorCodes::DuplicateKey,
                      "Column already exists: " + std::string(fieldName));
    }

    // Add to schema
    std::string fieldNameStr = std::string(fieldName);
    size_t columnIndex = _schema.size();
    _fieldToColumnIndex[fieldNameStr] = columnIndex;
    _schema.push_back(fieldNameStr);

    // Get current row count
    int64_t currentRowCount = _columns.empty() ? 0 : _columns[0].size();

    // Create new column with 0s for all existing rows (missing value indicator)
    auto newColumn = std::make_unique<std::vector<uint32_t>>(currentRowCount, 0);
    _columns.push_back(*newColumn);
    _columnAddedAtRowId.push_back(currentRowCount);

    // Notify writer of the new schema field (must be done after schema is updated)
    // Note: writeSchema will check state and call writer->addSchemaField if in ReadWrite mode
    auto status = writeSchema(fieldNameStr, columnIndex);
    if (!status.isOK()) {
        return status;
    }

    return Status::OK();
}

Status AttributeTable::changeState(AttributeTableState newState) {
    std::unique_lock<std::shared_mutex> lock(_mutex);

    // Once in ReadOnly state, no transitions are allowed
    if (_state == AttributeTableState::ReadOnly) {
        return Status(ErrorCodes::InternalError, "Cannot change state of read-only table");
    }

    // Validate state transitions
    if (_state == AttributeTableState::NOP) {
        if (newState != AttributeTableState::Reconstruction &&
            newState != AttributeTableState::ReadWrite) {
            return Status(ErrorCodes::InternalError,
                          "Invalid state transition from NOP to " +
                              std::to_string(static_cast<int>(newState)));
        }
    } else if (_state == AttributeTableState::Reconstruction) {
        // From Reconstruction, can transition to ReadWrite (to accept new data) or ReadOnly
        if (newState != AttributeTableState::ReadWrite &&
            newState != AttributeTableState::ReadOnly) {
            return Status(ErrorCodes::InternalError,
                          "Invalid state transition from Reconstruction to " +
                              std::to_string(static_cast<int>(newState)));
        }
    } else if (_state == AttributeTableState::ReadWrite) {
        // From ReadWrite, can only transition to ReadOnly
        if (newState != AttributeTableState::ReadOnly) {
            return Status(ErrorCodes::InternalError,
                          "Invalid state transition from ReadWrite to " +
                              std::to_string(static_cast<int>(newState)));
        }
    }

    _state = newState;
    return Status::OK();
}

void AttributeTable::setWriter(HCIndexWriter* writer) {
    std::unique_lock<std::shared_mutex> lock(_mutex);
    _writer = writer;
}

void AttributeTable::flush() {
    std::unique_lock<std::shared_mutex> lock(_mutex);

    if (!_isDirty || _writer == nullptr) {
        return;  // Nothing to flush
    }

    // Flush pending operations via the writer
    auto status = _writer->flush(_windowStart, _windowEnd, _period, _frequency, false);
    if (!status.isOK()) {
        return;  // Could not flush
    }

    _isDirty = false;
}


//- PRIVATE METHODS


std::vector<int64_t> AttributeTable::queryRowsLeaf(const std::vector<uint32_t>& refRowVec,
                                                   BitmapIndex* bitmapIndex) const {
    std::vector<int64_t> matchingRowIds;

    // No columns or no predicates to match
    if (refRowVec.empty() || _columns.empty()) {
        return matchingRowIds;
    }

    size_t rowCount = _columns[0].size();

    // Count non-zero predicates
    size_t numPredicates = 0;
    for (uint32_t sym : refRowVec) {
        if (sym != 0) {
            ++numPredicates;
        }
    }

    // First, identify which predicate columns lack a bitmap index (need full scan)
    // These are candidates for GPU acceleration
    std::vector<size_t> columnsNeedingScan;
    for (size_t colIdx = 0; colIdx < refRowVec.size(); ++colIdx) {
        uint32_t expectedSymbol = refRowVec[colIdx];
        if (expectedSymbol == 0) {
            continue;  // Not part of predicate
        }
        if (colIdx >= _columns.size()) {
            continue;  // Column doesn't exist
        }
        // Check if this column has a bitmap index
        if (bitmapIndex == nullptr || !bitmapIndex->hasIndexForColumn(colIdx)) {
            columnsNeedingScan.push_back(colIdx);
        }
    }

    bool gpuWouldHelp = !columnsNeedingScan.empty() && shouldUseGpu(rowCount, numPredicates);

    /*
    LOGV2(9999992, "HCIndex: GPU check",
          "columnsNeedingScan"_attr = columnsNeedingScan.size(),
          "gpuWouldHelp"_attr = gpuWouldHelp,
          "forceCpuPath"_attr = _forceCpuPath,
          "rowCount"_attr = rowCount,
          "numPredicates"_attr = numPredicates,
          "hasGpuTable"_attr = (_gpuTable != nullptr),
          "gpuAvailable"_attr = (_gpuTable ? _gpuTable->isGpuAvailable() : false),
          "gpuDataStale"_attr = _gpuDataStale);
    */

    // GPU acceleration: Only use GPU if:
    // 1. There are columns without bitmap indexes (need full scan)
    // 2. GPU would help (table large enough, reasonable predicate count)
    // 3. GPU data is ALREADY uploaded (to avoid blocking during query)
    // 4. Not forcing CPU path for benchmarking
    if (gpuWouldHelp && !_forceCpuPath) {
        if (_gpuTable && _gpuTable->isGpuAvailable() && !_gpuDataStale) {
            /*
            LOGV2(9999992, "HCIndex: OPTIMIZATION - using GPU for query",
                  "rowCount"_attr = rowCount,
                  "numPredicates"_attr = numPredicates,
                  "columnsNeedingScan"_attr = columnsNeedingScan.size());
            */

            // Record access for LAL ordering
            if (!_gpuTableId.empty()) {
                gpu::GpuAttributeTableManager::instance().recordAccess(_gpuTableId);
            }

            // Time GPU execution
            auto gpuStart = std::chrono::high_resolution_clock::now();
            auto gpuResult = queryRowsLeafGpu(refRowVec);
            auto gpuEnd = std::chrono::high_resolution_clock::now();
            auto gpuMicros = std::chrono::duration_cast<std::chrono::microseconds>(gpuEnd - gpuStart).count();

            LOGV2(9999991, "HCIndex: GPU query completed",
                  "durationMicros"_attr = gpuMicros,
                  "rowCount"_attr = rowCount,
                  "matchCount"_attr = gpuResult.size());

            return gpuResult;
        }
        // GPU not ready - fall through to CPU path
        // Mark that GPU upload should be triggered after query completes
        LOGV2(9999992, "HCIndex: GPU not ready, marking for deferred upload",
              "columnsNeedingScan"_attr = columnsNeedingScan.size());
        _gpuUploadNeeded = true;
    }

    // Time CPU execution
    auto cpuStart = std::chrono::high_resolution_clock::now();

    // CPU path: Iterate through each column with a predicate (column-major order)
    // and use bitmapIndex wherever possible to filter rows.
    std::vector<bool> rowMatches(rowCount, true);

    for (size_t colIdx = 0; colIdx < refRowVec.size(); ++colIdx) {
        uint32_t expectedSymbol = refRowVec[colIdx];

        // 0 <==> not part of the predicate, skip it
        if (expectedSymbol == 0) {
            continue;
        }

        // If required column does exist, no rows can match.
        // TODO: NULL and NotAvailable distinction.
        if (colIdx >= _columns.size()) {
            std::fill(rowMatches.begin(), rowMatches.end(), false);
            break;
        }

        // columnAddedAtRowId[colIdx] is the rowId where the column was added
        // and we will treat all earlier rows as missing (0)
        // TODO: NULL and NotAvailable distinction.
        size_t firstValidRow = static_cast<size_t>(_columnAddedAtRowId[colIdx]);
        std::fill(rowMatches.begin(), rowMatches.begin() + firstValidRow, false);

        // If bitmap index exists for this column, then use it.
        if (bitmapIndex != nullptr && bitmapIndex->hasIndexForColumn(colIdx)) {
            auto columnRowIds = bitmapIndex->getRowIds(colIdx, expectedSymbol);

            // Mark rows not in columnRowIds as non-matching
            for (size_t rowIdx = firstValidRow; rowIdx < rowCount; ++rowIdx) {
                if (rowMatches[rowIdx] &&
                    columnRowIds.find(static_cast<int64_t>(rowIdx)) == columnRowIds.end()) {
                    rowMatches[rowIdx] = false;
                }
            }
            continue;
        }

        // We are here ==> No index could be used. Do a full scan.

        auto& column = _columns[colIdx];
        for (size_t rowIdx = firstValidRow; rowIdx < rowCount; ++rowIdx) {
            if (!rowMatches[rowIdx]) {
                continue;
            }
            if (column[rowIdx] != expectedSymbol) {
                rowMatches[rowIdx] = false;
            }
        }
    }

    // Collect all rows that matched all predicates
    for (size_t rowIdx = 0; rowIdx < rowCount; ++rowIdx) {
        if (rowMatches[rowIdx]) {
            matchingRowIds.push_back(rowIdx);
        }
    }

    auto cpuEnd = std::chrono::high_resolution_clock::now();
    auto cpuMicros = std::chrono::duration_cast<std::chrono::microseconds>(cpuEnd - cpuStart).count();

    LOGV2(9999991, "HCIndex: CPU query completed",
          "durationMicros"_attr = cpuMicros,
          "rowCount"_attr = rowCount,
          "matchCount"_attr = matchingRowIds.size(),
          "usedBitmapIndex"_attr = (bitmapIndex != nullptr));

    return matchingRowIds;
}

size_t AttributeTable::computeRowHash(const std::vector<uint32_t>& row) {
    // FNV-1a hash - fast and good distribution for integer sequences
    size_t hash = 14695981039346656037ULL;  // FNV offset basis
    for (uint32_t val : row) {
        hash ^= static_cast<size_t>(val);
        hash *= 1099511628211ULL;  // FNV prime
    }
    return hash;
}

boost::optional<int64_t> AttributeTable::findDuplicateRow(
    const std::vector<uint32_t>& row) const {

    if (_columns.empty()) {
        return boost::none;
    }

    size_t rowHash = computeRowHash(row);
    auto it = _rowHashToRowIds.find(rowHash);
    if (it == _rowHashToRowIds.end()) {
        return boost::none;
    }

    // Check each candidate row ID (handle hash collisions)
    for (int64_t candidateRowId : it->second) {
        bool matches = true;
        for (size_t colIdx = 0; colIdx < row.size() && colIdx < _columns.size(); ++colIdx) {
            if (_columns[colIdx][candidateRowId] != row[colIdx]) {
                matches = false;
                break;
            }
        }

        if (matches) {
            return candidateRowId;
        }
    }

    return boost::none;
}

StatusWith<std::vector<uint32_t>> AttributeTable::metadataToRow(const BSONObj& metadata) {

    // First, evolve the schema if necessary. Then build the row vector that meets the
    // ordered-schema.

    for (const auto& elem : metadata) {
        std::string fieldName = elem.fieldName();

        if (elem.type() != BSONType::string) {
            return Status(ErrorCodes::BadValue,
                          "Metadata field '" + fieldName +
                              "' must be a string, got: " + typeName(elem.type()));
        }

        // Check if field is in schema
        auto it = _fieldToColumnIndex.find(fieldName);
        if (it == _fieldToColumnIndex.end()) {
            // Add to schema
            _fieldToColumnIndex[fieldName] = _schema.size();
            _schema.push_back(fieldName);

            // Write the schema
            auto schemaStatus = writeSchema(fieldName, _schema.size() - 1);
            if (!schemaStatus.isOK()) {
                return schemaStatus;
            }

            // Create new column with 0s for all existing rows
            int64_t currentRowCount = _columns.empty() ? 0 : _columns[0].size();
            _columns.push_back(std::vector<uint32_t>(currentRowCount, 0));
            _columnAddedAtRowId.push_back(currentRowCount);
        }
    }

    // Build row according to schema order
    std::vector<uint32_t> row;
    for (const auto& fieldName : _schema) {
        auto elem = metadata[StringData(fieldName)];

        if (elem.eoo()) {
            // Field not in metadata - use 0 to indicate.
            row.push_back(0);
        } else {
            std::string fieldValue = elem.String();
            auto symbolResult = _symbolDictionary->getOrInsertSymbol(fieldValue);
            if (!symbolResult.isOK()) {
                return symbolResult.getStatus();
            }
            uint32_t symbolIndex = symbolResult.getValue();
            row.push_back(symbolIndex);
        }
    }

    // Write the row
    auto rowStatus = writeRow(row);
    if (!rowStatus.isOK()) {
        return rowStatus;
    }

    return row;
}


Status AttributeTable::writeSchema(const std::string& fieldName, size_t columnIndex) {

    if (_state == AttributeTableState::NOP) {
        return Status(ErrorCodes::InternalError, "Table is in NOP state, cannot add schema");
    }
    else if (_state == AttributeTableState::ReadOnly) {
        return Status(ErrorCodes::InternalError, "Table is ReadOnly, cannot add schema");
    }
    else if (_state == AttributeTableState::ReadWrite && _writer == nullptr) {
        return Status(ErrorCodes::InternalError, "Table in ReadWrite mode requires a writer");
    }

    if (_state == AttributeTableState::ReadWrite) {
        // If this is the first schema field, we need to initialize the attribute table.
        // Check if schema had any fields BEFORE this one was added (columnIndex == 0 means first
        // field)
        if (columnIndex == 0) {
            auto stat = _writer->initAttributeTable(_windowStart, _windowEnd);
            if (!stat.isOK()) {
                return stat;
            }
        }

        auto stat = _writer->addSchemaField(_windowStart, _windowEnd, fieldName);
        if (!stat.isOK()) {
            return stat;
        }

        _isDirty = true;
        auto attrStat = _writer->addAttribute(_windowStart, _windowEnd, fieldName, columnIndex);
        if (!attrStat.isOK()) {
            return attrStat;
        }
    }

    // Defense: should never reach here
    return Status::OK();
}

Status AttributeTable::writeRow(const std::vector<uint32_t>& row) {
    if (_state == AttributeTableState::NOP) {
        return Status(ErrorCodes::InternalError, "Table is in NOP state, cannot add row");
    }
    else if (_state == AttributeTableState::ReadOnly) {
        return Status(ErrorCodes::InternalError, "Table is ReadOnly, cannot add row");
    }
    else if (_state == AttributeTableState::ReadWrite && _writer == nullptr) {
        return Status(ErrorCodes::InternalError, "Table in ReadWrite mode requires a writer");
    }

    if (_state == AttributeTableState::ReadWrite) {
        // If this is the first row, we need to initialize the attribute table.
        bool isFirstRow = _columns.empty() || _columns[0].empty();
        if (isFirstRow && _schema.empty()) {
            auto stat = _writer->initAttributeTable(_windowStart, _windowEnd);
            if (!stat.isOK()) {
                return stat;
            }
        }

        _isDirty = true;
        return _writer->addAttributeRow(_windowStart, _windowEnd, row);
    }

    // Defense: should never reach here
    return Status::OK();
}


//- GPU ACCELERATION


bool AttributeTable::prepareGpuTable() const {
    // This is the public method to be called during initialization/idle time
    ensureGpuTableUploaded();
    return _gpuTable && _gpuTable->isGpuAvailable() && !_gpuDataStale;
}


void AttributeTable::triggerDeferredGpuUploadIfNeeded() const {
    // Check if GPU upload was requested during a query
    if (_gpuUploadNeeded) {
        _gpuUploadNeeded = false;  // Reset flag
        LOGV2(9999991, "HCIndex: Triggering deferred GPU upload after query completion");
        ensureGpuTableUploaded();
    }
}


void AttributeTable::ensureGpuTableUploaded() const {
    // Only attempt upload once
    if (_gpuUploadAttempted && !_gpuDataStale) {
        LOGV2(9999991, "HCIndex: GPU upload already attempted, skipping");
        return;
    }

    _gpuUploadAttempted = true;

    // Get the GPU table manager
    auto& manager = gpu::GpuAttributeTableManager::instance();

    // Check if GPU is available at all
    if (!manager.isGpuAvailable()) {
        LOGV2(9999991, "HCIndex: GPU not available via manager, skipping upload");
        return;
    }

    // Build unique table ID from collection UUID + window timestamps
    if (_gpuTableId.empty() && _writer) {
        _gpuTableId = _writer->getCollectionUUID().toString() + "_" +
                      _windowStart.toString() + "_" + _windowEnd.toString();
    } else if (_gpuTableId.empty()) {
        // Fallback: use window timestamps only (less unique but works without writer)
        _gpuTableId = "table_" + _windowStart.toString() + "_" + _windowEnd.toString();
    }

    // Get or create GPU table from manager
    if (!_gpuTable) {
        _gpuTable = manager.getOrCreate(_gpuTableId);
        if (!_gpuTable) {
            LOGV2(9999991, "HCIndex: Failed to get GPU table from manager");
            return;
        }
        LOGV2(9999991, "HCIndex: Got GPU table from manager",
              "tableId"_attr = _gpuTableId,
              "backendName"_attr = _gpuTable->getDeviceInfo().name);
    }

    // Check if GPU is available for this table
    if (!_gpuTable->isGpuAvailable()) {
        LOGV2(9999991, "HCIndex: GPU not available for table, skipping upload",
              "backendName"_attr = _gpuTable->getDeviceInfo().name);
        return;
    }

    // Upload current data to GPU
    LOGV2(9999991, "HCIndex: Uploading data to GPU...",
          "tableId"_attr = _gpuTableId,
          "rowCount"_attr = _columns.empty() ? 0 : _columns[0].size(),
          "columnCount"_attr = _columns.size());

    _gpuTable->uploadFromCpu(_columns);
    _gpuDataStale = false;

    // Record access for LAL ordering
    manager.recordAccess(_gpuTableId);

    LOGV2(9999991,
          "HCIndex: Successfully uploaded AttributeTable to GPU",
          "tableId"_attr = _gpuTableId,
          "rowCount"_attr = _columns.empty() ? 0 : _columns[0].size(),
          "columnCount"_attr = _columns.size(),
          "device"_attr = _gpuTable->getDeviceInfo().name);
}


bool AttributeTable::shouldUseGpu(size_t rowCount, size_t numPredicates) const {
    // Heuristics for when GPU is beneficial:
    // 1. Table must be large enough to amortize GPU overhead
    // 2. Simple predicates work best (equality on single columns)
    // 3. Not too many predicates (GPU AND chain has overhead)

    // Minimum rows to consider GPU (tunable based on benchmarks)
    constexpr size_t kMinRowsForGpu = 10000;

    // Maximum predicates for GPU (beyond this, CPU may be faster)
    constexpr size_t kMaxPredicatesForGpu = 10;

    // Must have predicates and rows
    if (numPredicates == 0 || rowCount == 0) {
        return false;
    }

    // GPU beneficial for large tables with reasonable predicate count
    return rowCount >= kMinRowsForGpu && numPredicates <= kMaxPredicatesForGpu;
}


std::vector<int64_t> AttributeTable::queryRowsLeafGpu(
    const std::vector<uint32_t>& refRowVec) const {

    // Convert AttributeTablePredicate format to GPU predicate format
    std::vector<gpu::GpuColumnPredicate> gpuPredicates;

    for (size_t colIdx = 0; colIdx < refRowVec.size(); ++colIdx) {
        uint32_t expectedSymbol = refRowVec[colIdx];

        // 0 means not part of predicate
        if (expectedSymbol == 0) {
            continue;
        }

        // Skip if column doesn't exist (no rows can match)
        if (colIdx >= _columns.size()) {
            return {};  // No matches possible
        }

        // Add equality predicate for this column
        gpuPredicates.push_back({
            colIdx,
            gpu::PredicateOp::EQ,
            expectedSymbol
        });
    }

    if (gpuPredicates.empty()) {
        return {};
    }

    // Execute filter on GPU
    return _gpuTable->filter(gpuPredicates);
}


}  // namespace mongo::timeseries::hcindex
