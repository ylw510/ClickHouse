#include <Storages/GenericSampling.h>

#include <Columns/ColumnConst.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatFilterInfo.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>

#include <memory>
#include <numeric>

namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{
    using Rational = TableExpressionModifiers::Rational;

    bool isZero(const Rational & r) { return r.numerator == 0; }
    bool isOne(const Rational & r) { return r.denominator != 0 && r.numerator == r.denominator; }
    bool isGreaterThanOne(const Rational & r) { return r.denominator != 0 && r.numerator > r.denominator; }

    Float64 toFloat64(const Rational & r)
    {
        if (r.denominator == 0)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "SAMPLE denominator is zero");
        return static_cast<Float64>(r.numerator) / static_cast<Float64>(r.denominator);
    }
}

bool GenericSampleInterval::contains(Float64 x) const
{
    const Float64 end = offset + size;
    if (end <= 1.0)
        return x >= offset && x < end;
    return x >= offset || x < (end - 1.0);
}

bool GenericSampleInterval::containsHash(UInt64 hash) const
{
    constexpr Float64 scale = 0x1p-64;
    return contains(static_cast<Float64>(hash) * scale);
}

std::optional<GenericSampleInterval> GenericSampling::fromQueryInfo(const SelectQueryInfo & query_info)
{
    const auto sample_size_ratio = query_info.getSampleSizeRatio();
    if (!sample_size_ratio)
        return {};

    if (sample_size_ratio->denominator == 0)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "SAMPLE denominator is zero");

    const auto sample_offset_ratio = query_info.getSampleOffsetRatio();
    if (sample_offset_ratio && sample_offset_ratio->denominator == 0)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "SAMPLE OFFSET denominator is zero");

    if (isGreaterThanOne(*sample_size_ratio) || (sample_offset_ratio && isGreaterThanOne(*sample_offset_ratio)))
        throw Exception(
            ErrorCodes::ARGUMENT_OUT_OF_BOUND,
            "SAMPLE size greater than 1 (absolute number of rows) is not supported without SAMPLE BY. Use a ratio in (0, 1]");

    const bool size_is_one = isOne(*sample_size_ratio);
    const bool size_is_zero = isZero(*sample_size_ratio);
    const bool has_offset = sample_offset_ratio && !isZero(*sample_offset_ratio);

    /// SAMPLE 1 and SAMPLE 0 are no-ops, matching MergeTree (relative size 0 means "do not sample").
    if ((size_is_one || size_is_zero) && !has_offset)
        return {};

    if (has_offset && (size_is_one || size_is_zero))
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Sampling offset is incorrect because no sampling");

    GenericSampleInterval interval;
    interval.size = toFloat64(*sample_size_ratio);
    interval.offset = sample_offset_ratio ? toFloat64(*sample_offset_ratio) : 0;
    if (interval.size < 0)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Negative sample size");
    if (interval.offset < 0)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Negative sample offset");
    return interval;
}

bool GenericSampling::isActive(const SelectQueryInfo & query_info)
{
    auto sample = fromQueryInfo(query_info);
    return sample.has_value() && !sample->isFull();
}

bool GenericSampling::formatSupportsRowGroupSampling(const String & format)
{
    return format == "Parquet";
}

void GenericSampling::markAppliedIfPushedDown(SelectQueryInfo & query_info, const String & format)
{
    if (!formatSupportsRowGroupSampling(format) || !isActive(query_info))
        return;
    query_info.generic_sample_applied = true;
}

void GenericSampling::attachToFormatFilter(FormatFilterInfo & info, const SelectQueryInfo & query_info)
{
    if (!query_info.generic_sample_applied)
        return;
    auto sample = fromQueryInfo(query_info);
    if (sample)
        info.generic_sample = std::make_shared<GenericSampleInterval>(*sample);
}

void GenericSampling::adjustReadFlags(SelectQueryInfo & query_info, const String & format, bool & need_only_count)
{
    if (!isActive(query_info))
        return;
    need_only_count = false;
    markAppliedIfPushedDown(query_info, format);
}

UInt64 GenericSampling::hashRowGroup(std::string_view path, size_t row_group_id)
{
    SipHash hash;
    hash.update(path);
    hash.update(row_group_id);
    return hash.get64();
}

std::optional<std::vector<size_t>> GenericSampling::filterRowGroups(
    std::string_view path,
    size_t num_row_groups,
    const GenericSampleInterval & sample,
    const std::optional<std::vector<size_t>> & existing)
{
    if (sample.isFull())
        return existing;

    std::vector<size_t> candidates;
    if (existing)
        candidates = *existing;
    else
    {
        candidates.resize(num_row_groups);
        std::iota(candidates.begin(), candidates.end(), 0);
    }

    std::vector<size_t> kept;
    kept.reserve(candidates.size());
    for (size_t id : candidates)
    {
        if (sample.containsHash(hashRowGroup(path, id)))
            kept.push_back(id);
    }
    return kept;
}

ActionsDAG GenericSampling::makeRowFilterDAG(
    const NamesAndTypesList & header,
    const GenericSampleInterval & sample,
    const ContextPtr & context)
{
    ActionsDAG dag(header);

    auto float64 = std::make_shared<DataTypeFloat64>();
    const auto & rand_node = dag.addFunction(
        FunctionFactory::instance().get("randCanonical", context),
        {},
        "__generic_sample_rand");

    const auto & offset_node = dag.addColumn(
        float64->createColumnConst(1, sample.offset),
        float64,
        "__generic_sample_offset");

    const Float64 end = sample.offset + sample.size;
    const bool wraps = end > 1.0;
    const auto & bound_node = dag.addColumn(
        float64->createColumnConst(1, wraps ? (end - 1.0) : end),
        float64,
        "__generic_sample_bound");

    const auto & ge = dag.addFunction(
        FunctionFactory::instance().get("greaterOrEquals", context),
        {&rand_node, &offset_node},
        "__generic_sample_ge");
    const auto & lt = dag.addFunction(
        FunctionFactory::instance().get("less", context),
        {&rand_node, &bound_node},
        "__generic_sample_lt");
    const auto & filter = dag.addFunction(
        FunctionFactory::instance().get(wraps ? "or" : "and", context),
        {&ge, &lt},
        "__generic_sample");
    dag.addOrReplaceInOutputs(filter);
    return dag;
}

void GenericSampling::addFilterStepIfNeeded(
    QueryPlan & query_plan,
    const IStorage & storage,
    SelectQueryInfo & query_info,
    const ContextPtr & context)
{
    if (!storage.supportsGenericSampling() || query_info.generic_sample_applied || !query_plan.isInitialized())
        return;

    auto sample = fromQueryInfo(query_info);
    if (!sample)
        return;

    auto filter_step = std::make_unique<FilterStep>(
        query_plan.getCurrentHeader(),
        makeRowFilterDAG(query_plan.getCurrentHeader()->getNamesAndTypesList(), *sample, context),
        "__generic_sample",
        true);
    filter_step->setStepDescription("Generic SAMPLE");
    query_plan.addStep(std::move(filter_step));
    query_info.generic_sample_applied = true;
}

}
