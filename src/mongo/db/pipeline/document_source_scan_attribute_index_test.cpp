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
 *    all of the code used herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version of the file(s), but you
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used herein.
 */

#include "mongo/db/pipeline/document_source_scan_attribute_index.h"

#include "mongo/db/exec/agg/document_source_to_stage_registry.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {

using DocumentSourceScanAttributeIndexTest = AggregationContextFixture;

TEST_F(DocumentSourceScanAttributeIndexTest, ShouldCreateStageWithNullMatchExpression) {
    auto stage = DocumentSourceScanAttributeIndex::create(getExpCtx(), nullptr);
    ASSERT(stage);
    ASSERT_EQ(std::string(stage->getSourceName()),
              std::string(DocumentSourceScanAttributeIndex::kStageName.data()));
}

TEST_F(DocumentSourceScanAttributeIndexTest, ShouldCloneCorrectly) {
    auto stage = DocumentSourceScanAttributeIndex::create(getExpCtx(), nullptr);
    auto cloned = stage->clone(getExpCtx());
    ASSERT(cloned);
    ASSERT_EQ(std::string(cloned->getSourceName()),
              std::string(DocumentSourceScanAttributeIndex::kStageName.data()));
}

TEST_F(DocumentSourceScanAttributeIndexTest, ShouldSerializeCorrectly) {
    auto stage = DocumentSourceScanAttributeIndex::create(getExpCtx(), nullptr);
    auto serialized = stage->serialize();
    ASSERT_FALSE(serialized.missing());
}

TEST_F(DocumentSourceScanAttributeIndexTest, ShouldConvertToStageCorrectly) {
    auto docSource = DocumentSourceScanAttributeIndex::create(getExpCtx(), nullptr);
    auto stage = exec::agg::buildStage(docSource);
    ASSERT(stage);
}

}  // namespace
}  // namespace mongo

