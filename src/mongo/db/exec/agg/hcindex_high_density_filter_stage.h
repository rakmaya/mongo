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
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include "mongo/db/exec/agg/stage.h"
#include "mongo/db/matcher/expression.h"
#include "mongo/util/modules.h"

namespace mongo::exec::agg {

/**
 * Placeholder stage for HCIndex high-density bucket-level filtering.
 * 
 * This stage is a placeholder for future optimization that will use bucket-level
 * statistics for low-cardinality-high-density fields to quickly filter buckets
 * without extracting the whole bucket.
 * 
 * Currently, this stage is a no-op that passes all buckets through unchanged.
 * In the future, it will be enhanced to:
 * 1. Check bucket-level statistics for high-density fields
 * 2. Filter out buckets that cannot possibly match the predicate
 * 3. Avoid unpacking buckets that don't match
 */
class HCIndexHighDensityFilterStage final : public Stage {
public:
    HCIndexHighDensityFilterStage(StringData stageName,
                                  const boost::intrusive_ptr<ExpressionContext>& pExpCtx,
                                  std::unique_ptr<MatchExpression> matchExpr);

private:
    GetNextResult doGetNext() final;

    std::unique_ptr<MatchExpression> _matchExpr;
};

}  // namespace mongo::exec::agg

