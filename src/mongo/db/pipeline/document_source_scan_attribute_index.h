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

#pragma once

#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/matcher/expression.h"

namespace mongo {

/**
 * Scans the HCIndex attribute table to find rowIds matching a predicate.
 * This stage is used internally for HCIndex-enabled timeseries collections
 * to efficiently filter measurements by metadata predicates.
 */
class DocumentSourceScanAttributeIndex final : public DocumentSource {
public:
    static constexpr StringData kStageName = "$_internalScanAttributeIndex"_sd;

    /**
     * Create a new scan attribute index stage with a match expression.
     */
    static boost::intrusive_ptr<DocumentSourceScanAttributeIndex> create(
        const boost::intrusive_ptr<ExpressionContext>& expCtx,
        std::unique_ptr<MatchExpression> matchExpr);

    /**
     * Parse from BSON (for testing/serialization).
     */
    static boost::intrusive_ptr<DocumentSource> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& expCtx);

    DocumentSourceScanAttributeIndex(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                     std::unique_ptr<MatchExpression> matchExpr);

    StageConstraints constraints(PipelineSplitState pipeState) const final {
        return StageConstraints{StreamType::kStreaming,
                                PositionRequirement::kNone,
                                HostTypeRequirement::kNone,
                                DiskUseRequirement::kNoDiskUse,
                                FacetRequirement::kNotAllowed,
                                TransactionRequirement::kNotAllowed,
                                LookupRequirement::kNotAllowed,
                                UnionRequirement::kNotAllowed};
    }

    const char* getSourceName() const final {
        return kStageName.data();
    }

    static const Id& id;

    Id getId() const override {
        return id;
    }

    Value serialize(const SerializationOptions& opts = SerializationOptions{}) const final;

    DepsTracker::State getDependencies(DepsTracker* deps) const final {
        return DepsTracker::State::EXHAUSTIVE_ALL;
    }

    boost::optional<DistributedPlanLogic> distributedPlanLogic() final {
        return boost::none;
    }

    void addVariableRefs(std::set<Variables::Id>* refs) const final {}

    GetModPathsReturn getModifiedPaths() const final {
        return {GetModPathsReturn::Type::kFiniteSet, OrderedPathSet{}, {}};
    }

    boost::intrusive_ptr<DocumentSource> clone(
        const boost::intrusive_ptr<ExpressionContext>& newExpCtx) const final;

    // Public for access by stage conversion function
    std::unique_ptr<MatchExpression> _matchExpr;
};

}  // namespace mongo

