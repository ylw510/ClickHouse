#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Aggregator.h>

namespace DB
{

class ASTSelectQuery;

/// Fork pipeline into hits (projection + sort + limit) and aggregates (aggregation) streams.
class HitAndAggregateStep : public ITransformingStep
{
public:
    HitAndAggregateStep(
        SharedHeader input_header_,
        SharedHeader hits_header_,
        SharedHeader aggregates_header_,
        std::optional<ActionsDAG> hits_projection_actions_,
        SortDescription hits_sort_,
        UInt64 hits_limit_,
        UInt64 hits_offset_,
        Aggregator::Params agg_params_,
        size_t max_block_size_,
        size_t merge_threads_);

    String getName() const override { return "HitAndAggregate"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return false; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    const SharedHeader & getAggregatesHeader() const { return aggregates_header; }

private:
    void updateOutputHeader() override;

    SharedHeader aggregates_header;
    std::optional<ActionsDAG> hits_projection_actions;
    SortDescription hits_sort;
    UInt64 hits_limit;
    UInt64 hits_offset;
    Aggregator::Params agg_params;
    size_t max_block_size;
    size_t merge_threads;
};

Aggregator::Params buildAggregatorParamsFromAggregatesSubquery(
    const ASTSelectQuery & aggregates_query,
    const Block & input_header,
    const ContextPtr & context);

}
