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
#include "mongo/db/exec/timeseries/hcindex/hcindex_reader.h"
#include "mongo/base/error_codes.h"
#include "mongo/db/matcher/expression.h"
#include "mongo/db/update/path_support.h"
#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

namespace mongo::timeseries::hcindex {

// ============================================================================
// AttributeTable Implementation
// ============================================================================

AttributeTable::AttributeTable(SymbolDictionary* symbolDictionary,
                               HCIndexWriter* writer,
                               HCIndexPeriodEnum period,
                               int32_t frequency,
                               const Timestamp& windowStart,
                               const Timestamp& windowEnd)
    : symbolDictionary(symbolDictionary)
    , writer(writer)
    , _period(period)
    , _frequency(frequency)
    , _windowStart(windowStart)
    , _windowEnd(windowEnd)
    , _isDirty(false)
{
}

Status AttributeTable::changeState(AttributeTableState newState) {
    std::unique_lock<std::shared_mutex> lock(mutex);

    // Once in ReadOnly state, no transitions are allowed
    if (_state == AttributeTableState::ReadOnly) {
        return Status(ErrorCodes::InternalError, "Cannot change state of read-only table");
    }

    // Validate state transitions
    if (_state == AttributeTableState::NOP) {
        if (newState != AttributeTableState::Reconstruction &&
            newState != AttributeTableState::ReadWrite) {
            return Status(ErrorCodes::InternalError,
                "Invalid state transition from NOP to " + std::to_string(static_cast<int>(newState)));
        }
    } else if (_state == AttributeTableState::Reconstruction) {
        // From Reconstruction, can transition to ReadWrite (to accept new data) or ReadOnly
        if (newState != AttributeTableState::ReadWrite &&
            newState != AttributeTableState::ReadOnly) {
            return Status(ErrorCodes::InternalError,
                "Invalid state transition from Reconstruction to " + std::to_string(static_cast<int>(newState)));
        }
    } else if (_state == AttributeTableState::ReadWrite) {
        // From ReadWrite, can only transition to ReadOnly
        if (newState != AttributeTableState::ReadOnly) {
            return Status(ErrorCodes::InternalError,
                "Invalid state transition from ReadWrite to " + std::to_string(static_cast<int>(newState)));
        }
    }

    _state = newState;
    return Status::OK();
}

StatusWith<InsertRowResult> AttributeTable::insertRow(const BSONObj& metadata) {
    std::unique_lock<std::shared_mutex> lock(mutex);

    // Convert metadata to row vector
    auto rowResult = metadataToRow(metadata);
    if (!rowResult.isOK()) {
        return rowResult.getStatus();
    }

    std::vector<uint32_t> row = rowResult.getValue();

    // Check for duplicate row
    auto duplicateRowId = findDuplicateRow(row);
    if (duplicateRowId) {
        return InsertRowResult{duplicateRowId.value(), false};
    }

    // Insert new row (note: insertRowDirect does not acquire lock, caller must hold it)
    auto insertResult = insertRowDirect(row);
    if (!insertResult.isOK()) {
        return insertResult.getStatus();
    }

    return InsertRowResult{insertResult.getValue(), true};
}

StatusWith<int64_t> AttributeTable::insertRowDirect(const std::vector<uint32_t>& row) {
    // Note: Caller must hold the mutex lock. This method does not acquire the lock
    // to avoid deadlock when called from insertRow() which already holds the lock.

    // Pad row with 0s if it's shorter than schema
    std::vector<uint32_t> paddedRow = row;
    while (paddedRow.size() < schema.size()) {
        paddedRow.push_back(0);
    }

    // Validate row size
    if (paddedRow.size() > schema.size()) {
        return Status(ErrorCodes::BadValue,
                      "Row has more columns than schema");
    }

    // Get current row count (all columns have same size)
    int64_t rowId = columns.empty() ? 0 : columns[0].size();

    // Append values to each column
    for (size_t colIdx = 0; colIdx < paddedRow.size(); ++colIdx) {
        // Ensure column exists
        if (colIdx >= columns.size()) {
            columns.push_back(std::vector<uint32_t>());
            columnAddedAtRowId.push_back(rowId);
        }
        columns[colIdx].push_back(paddedRow[colIdx]);
    }

    return rowId;
}

boost::optional<std::vector<uint32_t>> AttributeTable::getRow(int64_t rowId) const {
    std::shared_lock<std::shared_mutex> lock(mutex);

    // Check if rowId is valid
    if (columns.empty() || rowId < 0 || rowId >= static_cast<int64_t>(columns[0].size())) {
        return boost::none;
    }

    // Reconstruct row from columns
    std::vector<uint32_t> row;
    for (const auto& column : columns) {
        row.push_back(column[rowId]);
    }

    return row;
}

std::vector<int64_t> AttributeTable::queryRowsLeaf(
    const std::vector<uint32_t>& refRowVec) const {
    std::vector<int64_t> matchingRowIds;

    // If refRowVec is empty or no columns, no predicates to match
    if (refRowVec.empty() || columns.empty()) {
        return matchingRowIds;
    }

    // Get row count from first column
    size_t rowCount = columns[0].size();

    // Initialize a bitmap to track which rows match all predicates
    // Start with all rows as potential matches
    std::vector<bool> rowMatches(rowCount, true);

    // Iterate through each column with a predicate (column-major order for better vectorization)
    for (size_t colIdx = 0; colIdx < refRowVec.size(); ++colIdx) {
        uint32_t expectedSymbol = refRowVec[colIdx];

        // 0 means this field is not part of the predicate, skip it
        if (expectedSymbol == 0) {
            continue;
        }

        // Check if column exists
        if (colIdx >= columns.size()) {
            // Column doesn't exist, treat all rows as missing (0)
            // Since expectedSymbol != 0, no rows can match
            std::fill(rowMatches.begin(), rowMatches.end(), false);
            break;
        }


        // columnAddedAtRowId[colIdx] is the rowId where the column was added
        // and we will treat all earlier rows as missing (0)
        // TODO: Later we should allow null checks.
        size_t firstValidRow = static_cast<size_t>(columnAddedAtRowId[colIdx]);
        std::fill(rowMatches.begin(), rowMatches.begin() + firstValidRow, false);

        auto &column = columns[colIdx];
        // For this column, check each row (inner loop is now vectorizable)
        for (size_t rowIdx = firstValidRow; rowIdx < rowCount; ++rowIdx) {
            // Skip rows that already don't match
            if (!rowMatches[rowIdx]) {
                continue;
            }

            // Check if the row's symbol at this column matches
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

    return matchingRowIds;
}

std::vector<int64_t> AttributeTable::queryRows(
    const AttributeTablePredicate& predicate) const {
    std::shared_lock<std::shared_mutex> lock(mutex);

    // Handle LEAF predicates: simple equality matching
    if (predicate.isLeaf()) {
        return queryRowsLeaf(predicate.refRowVec);
    }

    // Handle OR predicates: union of all child results
    if (predicate.isOr()) {
        std::set<int64_t> uniqueMatches;
        for (const auto& child : predicate.children) {
            auto childResults = queryRows(child);
            for (auto rowId : childResults) {
                uniqueMatches.insert(rowId);
            }
        }
        std::vector<int64_t> result(uniqueMatches.begin(), uniqueMatches.end());
        return result;
    }

    // Handle AND predicates: intersection of all child results
    if (predicate.isAnd()) {
        if (predicate.children.empty()) {
            return {};
        }

        // Start with results from first child
        auto result = queryRows(predicate.children[0]);

        // Intersect with results from remaining children
        for (size_t i = 1; i < predicate.children.size(); ++i) {
            auto childResults = queryRows(predicate.children[i]);

            // Convert to set for efficient intersection
            std::set<int64_t> childSet(childResults.begin(), childResults.end());

            // Keep only rows that are in both sets
            std::vector<int64_t> intersection;
            for (auto rowId : result) {
                if (childSet.count(rowId) > 0) {
                    intersection.push_back(rowId);
                }
            }
            result = intersection;

            // Early exit if no matches
            if (result.empty()) {
                break;
            }
        }
        return result;
    }

    // Should not reach here
    return {};
}

StatusWith<AttributeTablePredicate> AttributeTable::convertMatchExpressionToPredicate(
    const ::mongo::MatchExpression* matchExpr) const {

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
                // If any branch fails to convert, skip it
                continue;
            }
            orPredicate.children.push_back(branchResult.getValue());
        }

        // If we collected any children, return the OR predicate
        if (!orPredicate.children.empty()) {
            return orPredicate;
        }
        // If no children were collected, return empty predicate
        return AttributeTablePredicate();
    }

    // Handle AND expressions: create an AND node with children
    if (matchExpr->matchType() == ::mongo::MatchExpression::AND) {
        AttributeTablePredicate andPredicate;
        andPredicate.type = AttributeTablePredicate::AND;

        for (size_t i = 0; i < matchExpr->numChildren(); ++i) {
            auto branchResult = convertMatchExpressionToPredicate(matchExpr->getChild(i));
            if (!branchResult.isOK()) {
                // If any branch fails to convert, skip it
                continue;
            }
            andPredicate.children.push_back(branchResult.getValue());
        }

        // If we collected any children, return the AND predicate
        if (!andPredicate.children.empty()) {
            return andPredicate;
        }
        // If no children were collected, return empty predicate
        return AttributeTablePredicate();
    }

    // Extract equality matches from the MatchExpression (for AND expressions)
    mongo::pathsupport::EqualityMatches equalities;
    auto status = mongo::pathsupport::extractEqualityMatches(*matchExpr, &equalities);
    if (!status.isOK()) {
        return status;
    }

    LOGV2(9999900, "HCIndex: convertMatchExpressionToPredicate",
          "equalitiesCount"_attr = equalities.size(),
          "schemaSize"_attr = schema.size(),
          "matchExprType"_attr = matchExpr->matchType());

    // Log the equalities we extracted
    for (const auto& [fieldPath, eqExpr] : equalities) {
        LOGV2(9999908, "HCIndex: Extracted equality",
              "fieldPath"_attr = fieldPath,
              "exprType"_attr = eqExpr->matchType());
    }

    // Build reference row vector: symbol indices in schema order
    // 0 means field is not part of predicate, non-zero means match this symbol
    std::vector<uint32_t> refRowVec;
    size_t maxColumnIndex = 0;

    // First pass: find the maximum column index needed
    for (const auto& [fieldPath, eqExpr] : equalities) {
        // Strip the metadata field prefix if present
        // The schema only contains the field names without the metadata prefix
        std::string schemaFieldPath = std::string(fieldPath);
        if (schemaFieldPath.find("metadata.") == 0) {
            schemaFieldPath = schemaFieldPath.substr(9);  // Remove "metadata." prefix
        }

        auto it = fieldToColumnIndex.find(schemaFieldPath);
        LOGV2(9999901, "HCIndex: Processing field",
              "fieldPath"_attr = fieldPath,
              "schemaFieldPath"_attr = schemaFieldPath,
              "found"_attr = (it != fieldToColumnIndex.end()));
        if (it != fieldToColumnIndex.end()) {
            maxColumnIndex = std::max(maxColumnIndex, it->second);
        }
    }

    // Initialize refRowVec with 0s up to maxColumnIndex
    refRowVec.resize(maxColumnIndex + 1, 0);

    // Second pass: fill in the symbol indices for matching fields
    for (const auto& [fieldPath, eqExpr] : equalities) {
        // Strip the metadata field prefix if present
        std::string schemaFieldPath = std::string(fieldPath);
        if (schemaFieldPath.find("metadata.") == 0) {
            schemaFieldPath = schemaFieldPath.substr(9);  // Remove "metadata." prefix
        }

        auto fieldIt = fieldToColumnIndex.find(schemaFieldPath);
        if (fieldIt == fieldToColumnIndex.end()) {
            // Field not in schema - no rows will match
            LOGV2(9999902, "HCIndex: Field not in schema, returning empty predicate",
                  "fieldPath"_attr = fieldPath,
                  "schemaFieldPath"_attr = schemaFieldPath);
            return AttributeTablePredicate();
        }

        // Get the symbol value from the BSON element
        const BSONElement& data = eqExpr->getData();

        LOGV2(9999907, "HCIndex: Got data element",
              "fieldPath"_attr = fieldPath,
              "dataType"_attr = typeName(data.type()),
              "dataEOO"_attr = data.eoo());

        if (data.eoo()) {
            // Element is EOO (end of object), which means it's invalid
            return Status(ErrorCodes::BadValue,
                          "HCIndex: Predicate value for field '" + std::string(fieldPath) +
                              "' is invalid (EOO element)");
        }

        if (data.type() != mongo::BSONType::string) {
            return Status(ErrorCodes::BadValue,
                          "HCIndex: Predicate value for field '" + std::string(fieldPath) +
                              "' must be a string, got: " + typeName(data.type()));
        }

        StringData value = data.valueStringData();

        // Look up the symbol index in the dictionary
        auto symbolIndex = symbolDictionary->getSymbolIndex(value);
        LOGV2(9999903, "HCIndex: Symbol lookup",
              "fieldPath"_attr = fieldPath,
              "value"_attr = value,
              "symbolIndex"_attr = (symbolIndex ? *symbolIndex : 0),
              "found"_attr = symbolIndex.has_value());

        if (!symbolIndex) {
            // Symbol not found in dictionary - this field value doesn't exist in this table
            // Return empty predicate (no rows will match)
            LOGV2(9999904, "HCIndex: Symbol not found in dictionary, returning empty predicate",
                  "fieldPath"_attr = fieldPath,
                  "value"_attr = value);
            return AttributeTablePredicate();
        }

        // Set the symbol index at the appropriate column position
        refRowVec[fieldIt->second] = *symbolIndex;
        LOGV2(9999905, "HCIndex: Set refRowVec",
              "columnIndex"_attr = fieldIt->second,
              "symbolIndex"_attr = *symbolIndex);
    }

    // Trim trailing zeros from refRowVec (don't pad at the end)
    while (!refRowVec.empty() && refRowVec.back() == 0) {
        refRowVec.pop_back();
    }

    LOGV2(9999906, "HCIndex: Final refRowVec",
          "size"_attr = refRowVec.size());

    // Create a LEAF predicate with the refRowVec
    AttributeTablePredicate leafPredicate;
    leafPredicate.type = AttributeTablePredicate::LEAF;
    leafPredicate.refRowVec = refRowVec;
    return leafPredicate;
}

Status AttributeTable::addColumn(StringData fieldName) {
    std::unique_lock<std::shared_mutex> lock(mutex);

    // Check if column already exists
    if (fieldToColumnIndex.find(std::string(fieldName)) != fieldToColumnIndex.end()) {
        return Status(ErrorCodes::DuplicateKey,
                      "Column already exists: " + std::string(fieldName));
    }

    // Add to schema
    std::string fieldNameStr = std::string(fieldName);
    size_t columnIndex = schema.size();
    fieldToColumnIndex[fieldNameStr] = columnIndex;
    schema.push_back(fieldNameStr);

    // Get current row count
    int64_t currentRowCount = columns.empty() ? 0 : columns[0].size();

    // Create new column with 0s for all existing rows (missing value indicator)
    auto newColumn = std::make_unique<std::vector<uint32_t>>(currentRowCount, 0);
    columns.push_back(*newColumn);
    columnAddedAtRowId.push_back(currentRowCount);

    // Notify writer of the new schema field (must be done after schema is updated)
    // Note: writeSchema will check state and call writer->addSchemaField if in ReadWrite mode
    auto status = writeSchema(fieldNameStr, columnIndex);
    if (!status.isOK()) {
        return status;
    }

    return Status::OK();
}

void AttributeTable::flush()
{
    std::unique_lock<std::shared_mutex> lock(mutex);

    if (!_isDirty || writer == nullptr) {
        return;  // Nothing to flush
    }

    // Flush pending operations via the writer
    auto status = writer->flush(_windowStart, _windowEnd, _period, _frequency, false);
    if (!status.isOK()) {
        return;  // Could not flush
    }

    _isDirty = false;
}

const std::vector<std::string>& AttributeTable::getSchema() const {
    std::shared_lock<std::shared_mutex> lock(mutex);
    return schema;
}

size_t AttributeTable::getRowCount() const {
    std::shared_lock<std::shared_mutex> lock(mutex);
    return columns.empty() ? 0 : columns[0].size();
}

size_t AttributeTable::getMemoryUsageBytes() const {
    std::shared_lock<std::shared_mutex> lock(mutex);

    size_t totalBytes = 0;

    // Schema memory
    for (const auto& fieldName : schema) {
        totalBytes += fieldName.size();
    }
    totalBytes += schema.capacity() * sizeof(std::string);

    // Columns memory
    for (const auto& column : columns) {
        totalBytes += column.capacity() * sizeof(uint32_t);
    }
    totalBytes += columns.capacity() * sizeof(std::vector<uint32_t>);

    // columnAddedAtRowId memory
    totalBytes += columnAddedAtRowId.capacity() * sizeof(int64_t);

    // Field to column index map
    for (const auto& [fieldName, colIdx] : fieldToColumnIndex) {
        totalBytes += fieldName.size();
    }

    return totalBytes;
}

StatusWith<std::vector<uint32_t>> AttributeTable::metadataToRow(
    const BSONObj& metadata) {
    // First pass: identify all fields in metadata and add new ones to schema
    for (const auto& elem : metadata) {
        std::string fieldName = elem.fieldName();

        // Extract string value from BSON element
        if (elem.type() != BSONType::string) {
            return Status(ErrorCodes::BadValue,
                          "Metadata field '" + fieldName +
                              "' must be a string, got: " +
                              typeName(elem.type()));
        }

        // Check if field is in schema
        auto it = fieldToColumnIndex.find(fieldName);
        if (it == fieldToColumnIndex.end()) {
            // New field - add to schema
            fieldToColumnIndex[fieldName] = schema.size();
            schema.push_back(fieldName);

            // Write the schema
            auto schemaStatus = writeSchema(fieldName, schema.size() - 1);
            if (!schemaStatus.isOK()) {
                return schemaStatus;
            }

            // Create new column with 0s for all existing rows
            int64_t currentRowCount = columns.empty() ? 0 : columns[0].size();
            columns.push_back(std::vector<uint32_t>(currentRowCount, 0));
            columnAddedAtRowId.push_back(currentRowCount);
        }
    }

    // Second pass: build row according to schema order
    std::vector<uint32_t> row;
    for (const auto& fieldName : schema) {
        auto elem = metadata[StringData(fieldName)];

        if (elem.eoo()) {
            // Field not in metadata - use missing value sentinel
            row.push_back(0);
        } else {
            // Look up value in symbol dictionary
            std::string fieldValue = elem.String();
            auto symbolResult = symbolDictionary->getOrInsertSymbol(fieldValue);
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

boost::optional<int64_t> AttributeTable::findDuplicateRow(
    const std::vector<uint32_t>& row) const {
    // If no columns, no rows to search
    if (columns.empty()) {
        return boost::none;
    }

    // Get row count from first column
    size_t rowCount = columns[0].size();

    // Linear search through existing rows
    for (size_t rowIdx = 0; rowIdx < rowCount; ++rowIdx) {
        bool matches = true;

        // Compare each column value
        for (size_t colIdx = 0; colIdx < row.size() && colIdx < columns.size(); ++colIdx) {
            if (columns[colIdx][rowIdx] != row[colIdx]) {
                matches = false;
                break;
            }
        }

        if (matches) {
            return rowIdx;
        }
    }

    return boost::none;
}

Status AttributeTable::writeSchema(const std::string& fieldName, size_t columnIndex)
{
    // Check state: modifications only allowed in Reconstruction or ReadWrite modes
    if (_state == AttributeTableState::NOP) {
        return Status(ErrorCodes::InternalError, "Table is in NOP state, cannot add schema");
    }
    if (_state == AttributeTableState::ReadOnly) {
        return Status(ErrorCodes::InternalError, "Table is read-only, cannot add schema");
    }

    // In ReadWrite mode, writer must be set for modifications
    if (_state == AttributeTableState::ReadWrite && writer == nullptr) {
        return Status(ErrorCodes::InternalError, "Table in ReadWrite mode requires a writer");
    }

    // In Reconstruction mode, we don't need a writer
    // In ReadWrite mode, we need to call the writer

    if (_state == AttributeTableState::ReadWrite) {
        // If this is the first schema field, we need to initialize the attribute table.
        // Check if schema had any fields BEFORE this one was added (columnIndex == 0 means first field)
        if (columnIndex == 0) {
            auto stat = writer->initAttributeTable(_windowStart, _windowEnd);
            if (!stat.isOK()) {
                return stat;
            }
        }

        auto stat = writer->addSchemaField(_windowStart, _windowEnd, fieldName);
        if (!stat.isOK()) {
            return stat;
        }

        _isDirty = true;

        auto attrStat = writer->addAttribute(_windowStart, _windowEnd, fieldName, columnIndex);
        if (!attrStat.isOK()) {
            return attrStat;
        }
    }

    return Status::OK();
}

Status AttributeTable::writeRow(const std::vector<uint32_t>& row)
{
    // Check state: modifications only allowed in Reconstruction or ReadWrite modes
    if (_state == AttributeTableState::NOP) {
        return Status(ErrorCodes::InternalError, "Table is in NOP state, cannot add row");
    }
    if (_state == AttributeTableState::ReadOnly) {
        return Status(ErrorCodes::InternalError, "Table is read-only, cannot add row");
    }

    // In ReadWrite mode, writer must be set for modifications
    if (_state == AttributeTableState::ReadWrite && writer == nullptr) {
        return Status(ErrorCodes::InternalError, "Table in ReadWrite mode requires a writer");
    }

    // In ReadWrite mode, notify writer of the new row
    if (_state == AttributeTableState::ReadWrite) {
        // If this is the first row, we need to initialize the attribute table.
        bool isFirstRow = columns.empty() || columns[0].empty();
        if (isFirstRow && schema.empty()) {
            auto stat = writer->initAttributeTable(_windowStart, _windowEnd);
            if (!stat.isOK()) {
                return stat;
            }
        }

        _isDirty = true;
        return writer->addAttributeRow(_windowStart, _windowEnd, row);
    }

    return Status::OK();
}


// ============================================================================
// TemporalAttributeTable Implementation
// ============================================================================

TemporalAttributeTable::TemporalAttributeTable(const UUID& collectionUUID,
                                               HCIndexPeriodEnum period,
                                               int32_t frequency,
                                               TemporalSymbolDictionary* symbolDictionary,
                                               HCIndexWriter* writer,
                                               HCIndexReader* reader)
    : collectionUUID(collectionUUID),
      period(period),
      frequency(frequency),
      temporalSymbolDictionary(symbolDictionary),
      writer(writer),
      reader(reader) {}

StatusWith<AttributeTable*> TemporalAttributeTable::getTableForTimestamp(
    const Timestamp& timestamp) const {
    std::shared_lock<std::shared_mutex> lock(mutex);

    Timestamp windowStart = calculateWindowStart(timestamp);
    auto it = tables.find(windowStart);

    if (it == tables.end()) {
        return Status(ErrorCodes::NoSuchKey,
                      "No attribute table found for timestamp");
    }

    return it->second.get();
}

bool TemporalAttributeTable::tableExists(const Timestamp& timestamp) const {
    std::shared_lock<std::shared_mutex> lock(mutex);

    Timestamp windowStart = calculateWindowStart(timestamp);
    return tables.find(windowStart) != tables.end();
}

StatusWith<AttributeTable*> TemporalAttributeTable::getOrCreateTableForTimestamp(
    OperationContext* opCtx,
    const Timestamp& timestamp) {
    Timestamp windowStart = calculateWindowStart(timestamp);
    Timestamp windowEnd = calculateWindowEnd(windowStart);

    // Check if table already exists
    {
        std::shared_lock<std::shared_mutex> lock(mutex);
        auto it = tables.find(windowStart);
        if (it != tables.end()) {
            return it->second.get();
        }
    }

    // Table doesn't exist in memory. Try to reconstruct from disk if reader is available.
    if (!reader) {
        // No reader available, create a new empty table
        return getOrCreateTable(opCtx, windowStart);
    }

    LOGV2(9999923, "HCIndex: Reconstructing attribute table for window", "windowStart"_attr = windowStart);

    // Try to reconstruct the table from disk
    // First, get or create the symbol dictionary for this window
    auto dictResult = temporalSymbolDictionary->getOrCreateDictionaryForTimestamp(opCtx, timestamp);
    if (!dictResult.isOK()) {
        return dictResult.getStatus();
    }
    auto* dictPtr = dictResult.getValue();

    // Reconstruct the attribute table from disk
    auto tableResult = reader->constructAttributeTable(
        opCtx, windowStart, windowEnd, period, frequency, timestamp, dictPtr);
    if (!tableResult.isOK()) {
        return tableResult.getStatus();
    }

    auto* tablePtr = tableResult.getValue().get();
    // If the table is empty, then we need a new table for this window
    if (tablePtr->getRowCount() == 0) {
        return getOrCreateTable(opCtx, windowStart);
    }

    // Store the reconstructed table in memory for future use
    std::unique_lock<std::shared_mutex> writeLock(mutex);

    // Set the writer on the reconstructed table so it can accept new data
    if (writer) {
        tablePtr->setWriter(writer);
    }

    tables[windowStart] = std::move(tableResult.getValue());

    return tablePtr;
}

StatusWith<InsertRowResult> TemporalAttributeTable::insertRow(OperationContext* opCtx,
                                                              const BSONObj& metadata,
                                                              const Timestamp& timestamp) {
    auto tableResult = getOrCreateTableForTimestamp(opCtx, timestamp);
    if (!tableResult.isOK()) {
        return tableResult.getStatus();
    }

    return tableResult.getValue()->insertRow(metadata);
}

StatusWith<int64_t> TemporalAttributeTable::insertRowDirect(
    OperationContext* opCtx,
    const std::vector<uint32_t>& row,
    const Timestamp& timestamp) {
    auto tableResult = getOrCreateTableForTimestamp(opCtx, timestamp);
    if (!tableResult.isOK()) {
        return tableResult.getStatus();
    }

    return tableResult.getValue()->insertRowDirect(row);
}

boost::optional<std::vector<uint32_t>> TemporalAttributeTable::getRow(
    int64_t rowId,
    const Timestamp& timestamp) const {
    auto tableResult = getTableForTimestamp(timestamp);
    if (!tableResult.isOK()) {
        return boost::none;
    }

    return tableResult.getValue()->getRow(rowId);
}

std::vector<int64_t> TemporalAttributeTable::queryRows(
    const AttributeTablePredicate& predicate,
    const Timestamp& timestamp) const {
    auto tableResult = getTableForTimestamp(timestamp);
    if (!tableResult.isOK()) {
        return {};
    }

    return tableResult.getValue()->queryRows(predicate);
}

std::pair<Timestamp, Timestamp> TemporalAttributeTable::getWindowForTimestamp(
    const Timestamp& timestamp) const {
    Timestamp windowStart = calculateWindowStart(timestamp);
    Timestamp windowEnd = calculateWindowEnd(windowStart);
    return {windowStart, windowEnd};
}

Status TemporalAttributeTable::cleanupOldTables(const Timestamp& beforeTimestamp) {
    std::unique_lock<std::shared_mutex> lock(mutex);

    auto it = tables.begin();
    while (it != tables.end()) {
        Timestamp windowEnd = calculateWindowEnd(it->first);
        if (windowEnd <= beforeTimestamp) {
            it = tables.erase(it);
        } else {
            ++it;
        }
    }

    return Status::OK();
}


void TemporalAttributeTable::flush()
{
    std::unique_lock<std::shared_mutex> lock(mutex);

    // Flush all tables
    for (auto& [windowStart, table] : tables) {
        // Flush the table
        table->flush();
    }
}

TemporalAttributeTable::Stats TemporalAttributeTable::getStats() const {
    std::shared_lock<std::shared_mutex> lock(mutex);

    Stats stats{0, 0, 0};

    for (const auto& [windowStart, table] : tables) {
        stats.totalTables++;
        stats.totalRows += table->getRowCount();
        stats.memoryUsageBytes += table->getMemoryUsageBytes();
    }

    return stats;
}

StatusWith<AttributeTable*> TemporalAttributeTable::getOrCreateTable(
    OperationContext* opCtx,
    const Timestamp& windowStart) {
    std::unique_lock<std::shared_mutex> lock(mutex);

    auto it = tables.find(windowStart);
    if (it != tables.end()) {
        return it->second.get();
    }

    // Get or create the symbol dictionary for this window
    auto dictResult = temporalSymbolDictionary->getOrCreateDictionaryForTimestamp(opCtx, windowStart);
    if (!dictResult.isOK()) {
        return dictResult.getStatus();
    }

    LOGV2(9999922, "HCIndex: Creating new attribute table for window", "windowStart"_attr = windowStart);

    // Create new table with the symbol dictionary for this window
    Timestamp windowEnd = calculateWindowEnd(windowStart);
    auto table = std::make_unique<AttributeTable>(dictResult.getValue(), writer, period, frequency, windowStart, windowEnd);

    // Change state to ReadWrite for new tables created by TemporalAttributeTable
    auto stateStatus = table->changeState(AttributeTableState::ReadWrite);
    if (!stateStatus.isOK()) {
        return stateStatus;
    }

    auto* tablePtr = table.get();
    tables[windowStart] = std::move(table);

    return tablePtr;
}

Timestamp TemporalAttributeTable::calculateWindowStart(const Timestamp& timestamp) const {
    uint32_t windowSizeSeconds = 0;

    switch (period) {
        case HCIndexPeriodEnum::Hour:
            windowSizeSeconds = frequency * 60 * 60;  // frequency hours in seconds
            break;
        case HCIndexPeriodEnum::Minute:
            windowSizeSeconds = frequency * 60;  // frequency minutes in seconds
            break;
        case HCIndexPeriodEnum::Second:
            windowSizeSeconds = frequency;  // frequency seconds
            break;
    }

    uint32_t seconds = timestamp.getSecs();
    uint32_t windowStartSeconds = (seconds / windowSizeSeconds) * windowSizeSeconds;
    return Timestamp(windowStartSeconds, 0);
}

Timestamp TemporalAttributeTable::calculateWindowEnd(const Timestamp& windowStart) const {
    uint32_t windowSizeSeconds = 0;

    switch (period) {
        case HCIndexPeriodEnum::Hour:
            windowSizeSeconds = frequency * 60 * 60;  // frequency hours in seconds
            break;
        case HCIndexPeriodEnum::Minute:
            windowSizeSeconds = frequency * 60;  // frequency minutes in seconds
            break;
        case HCIndexPeriodEnum::Second:
            windowSizeSeconds = frequency;  // frequency seconds
            break;
    }

    uint32_t seconds = windowStart.getSecs();
    return Timestamp(seconds + windowSizeSeconds, 0);
}

}  // namespace mongo::timeseries::hcindex

