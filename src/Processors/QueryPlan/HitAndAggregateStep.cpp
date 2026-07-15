#include <Processors/QueryPlan/HitAndAggregateStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/CopyTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/HitsAggregatesGlueTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Processors/LimitTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Interpreters/ExpressionActions.h>
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <Interpreters/Context.h>
#include <Interpreters/Aggregator.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Common/JSONBuilder.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool collect_hash_table_stats_during_aggregation;
    extern const SettingsUInt64 max_size_to_preallocate_for_aggregation;
    extern const SettingsNonZeroUInt64 temporary_files_buffer_size;
    extern const SettingsString temporary_files_codec;
    extern const SettingsUInt64 max_rows_to_group_by;
    extern const SettingsOverflowModeGroupBy group_by_overflow_mode;
    extern const SettingsUInt64 group_by_two_level_threshold;
    extern const SettingsUInt64 group_by_two_level_threshold_bytes;
    extern const SettingsUInt64 max_bytes_before_external_group_by;
    extern const SettingsDouble max_bytes_ratio_before_external_group_by;
    extern const SettingsBool empty_result_for_aggregation_by_empty_set;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsUInt64 min_free_disk_space_for_temporary_data;
    extern const SettingsBool compile_aggregate_expressions;
    extern const SettingsUInt64 min_count_to_compile_aggregate_expression;
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsBool enable_software_prefetch_in_aggregation;
    extern const SettingsBool optimize_group_by_constant_keys;
    extern const SettingsFloat min_hit_rate_to_use_consecutive_keys_optimization;
    extern const SettingsBool enable_producing_buckets_out_of_order_in_aggregation;
    extern const SettingsBool serialize_string_in_memory_with_zero_byte;
}

namespace ServerSetting
{
    extern const ServerSettingsUInt64 max_entries_for_hash_table_stats;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int BAD_ARGUMENTS;
}

namespace
{

std::optional<String> resolveColumnNameInHeader(const Block & header, const String & name)
{
    if (header.has(name))
        return name;

    for (const auto & column : header)
    {
        auto dot_pos = column.name.rfind('.');
        if (dot_pos != String::npos && column.name.substr(dot_pos + 1) == name)
            return column.name;
    }

    return std::nullopt;
}

const ColumnWithTypeAndName * findColumnInHeader(const Block & header, const String & name)
{
    if (const auto * column = header.findByName(name))
        return column;

    if (auto resolved_name = resolveColumnNameInHeader(header, name))
        return header.findByName(*resolved_name);

    return nullptr;
}

String resolveColumnNameInHeaderOrThrow(const Block & header, const String & name, const String & context)
{
    if (auto resolved = resolveColumnNameInHeader(header, name))
        return *resolved;

    throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unknown identifier '{}' in WITH AGGREGATES {}", name, context);
}

}

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

Aggregator::Params buildAggregatorParamsFromAggregatesSubquery(
    const ASTSelectQuery & aggregates_query,
    const Block & input_header,
    const ContextPtr & context)
{
    if (aggregates_query.tables())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "WITH AGGREGATES subquery must not have FROM clause");

    if (aggregates_query.groupBy() && aggregates_query.group_by_with_totals)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "WITH TOTALS is not supported in WITH AGGREGATES subquery");

    Names keys;
    NameSet key_names;
    if (auto group_by = aggregates_query.groupBy())
    {
        for (const auto & elem : group_by->children)
        {
            const auto & key_name = elem->getColumnName();
            keys.push_back(resolveColumnNameInHeaderOrThrow(input_header, key_name, "GROUP BY"));
            key_names.insert(keys.back());
        }
    }

    AggregateDescriptions aggregates;
    auto select_list = aggregates_query.select();
    if (!select_list)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "WITH AGGREGATES subquery must have SELECT clause");

    for (const auto & elem : select_list->children)
    {
        const auto * func = elem->as<ASTFunction>();
        if (!func || !AggregateFunctionFactory::instance().isAggregateFunctionName(func->name))
        {
            const auto resolved_name = resolveColumnNameInHeader(input_header, elem->getColumnName());
            if (!resolved_name || !key_names.contains(*resolved_name))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "WITH AGGREGATES subquery SELECT must contain only aggregate functions or GROUP BY keys, got: {}", elem->formatForErrorMessage());
            continue;
        }

        AggregateDescription aggregate;
        aggregate.column_name = elem->getColumnName();

        const ASTs & arguments = func->arguments ? func->arguments->children : ASTs();
        aggregate.argument_names.resize(arguments.size());
        DataTypes types(arguments.size());

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const auto & arg_name = arguments[i]->getColumnName();
            const auto * col = findColumnInHeader(input_header, arg_name);
            if (!col)
                throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unknown identifier '{}' in aggregate function '{}'", arg_name, func->formatForErrorMessage());
            types[i] = col->type;
            aggregate.argument_names[i] = col->name;
        }

        aggregate.parameters = func->parameters ? getAggregateFunctionParametersArray(func->parameters, "", context) : Array();
        AggregateFunctionProperties properties;
        aggregate.function = AggregateFunctionFactory::instance().get(func->name, func->getNullsAction(), types, aggregate.parameters, properties);
        aggregates.push_back(std::move(aggregate));
    }

    const auto & settings = context->getSettingsRef();
    const auto stats_collecting_params = StatsCollectingParams(
        /*key_=*/ 0,
        settings[Setting::collect_hash_table_stats_during_aggregation],
        context->getServerSettings()[ServerSetting::max_entries_for_hash_table_stats],
        settings[Setting::max_size_to_preallocate_for_aggregation]);

    auto tmp_data_scope = context->getTempDataOnDisk();
    if (tmp_data_scope)
        tmp_data_scope = tmp_data_scope->childScope(/* metrics */{}, settings[Setting::temporary_files_buffer_size], settings[Setting::temporary_files_codec]);

    return Aggregator::Params(
        keys,
        aggregates,
        /* overflow_row_ = */ false,
        settings[Setting::max_rows_to_group_by],
        settings[Setting::group_by_overflow_mode],
        settings[Setting::group_by_two_level_threshold],
        settings[Setting::group_by_two_level_threshold_bytes],
        Aggregator::Params::getMaxBytesBeforeExternalGroupBy(
            settings[Setting::max_bytes_before_external_group_by], settings[Setting::max_bytes_ratio_before_external_group_by]),
        settings[Setting::empty_result_for_aggregation_by_empty_set],
        tmp_data_scope,
        settings[Setting::max_threads],
        settings[Setting::min_free_disk_space_for_temporary_data],
        settings[Setting::compile_aggregate_expressions],
        settings[Setting::min_count_to_compile_aggregate_expression],
        settings[Setting::max_block_size],
        settings[Setting::enable_software_prefetch_in_aggregation],
        /* only_merge = */ false,
        settings[Setting::optimize_group_by_constant_keys],
        settings[Setting::min_hit_rate_to_use_consecutive_keys_optimization],
        stats_collecting_params,
        settings[Setting::enable_producing_buckets_out_of_order_in_aggregation],
        settings[Setting::serialize_string_in_memory_with_zero_byte]);
}

HitAndAggregateStep::HitAndAggregateStep(
    SharedHeader input_header_,
    SharedHeader hits_header_,
    SharedHeader aggregates_header_,
    std::optional<ActionsDAG> hits_projection_actions_,
    SortDescription hits_sort_,
    UInt64 hits_limit_,
    UInt64 hits_offset_,
    Aggregator::Params agg_params_,
    size_t max_block_size_,
    size_t merge_threads_)
    : ITransformingStep(input_header_, hits_header_, getTraits())
    , aggregates_header(std::move(aggregates_header_))
    , hits_projection_actions(std::move(hits_projection_actions_))
    , hits_sort(std::move(hits_sort_))
    , hits_limit(hits_limit_)
    , hits_offset(hits_offset_)
    , agg_params(std::move(agg_params_))
    , max_block_size(max_block_size_)
    , merge_threads(merge_threads_)
{
}

void HitAndAggregateStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    pipeline.dropTotalsAndExtremes();
    pipeline.resize(1);

    auto input_header = pipeline.getSharedHeader();

    pipeline.transform([&](const OutputPortRawPtrs & ports)
    {
        Processors processors;
        chassert(ports.size() == 1);

        auto copy = std::make_shared<CopyTransform>(input_header, 2);
        connect(*ports[0], copy->getInputPort());
        processors.push_back(copy);

        OutputPort * hits = &copy->getOutputs().front();
        OutputPort * aggs_src = &(*std::next(copy->getOutputs().begin()));

        if (hits_projection_actions)
        {
            auto expression_actions = std::make_shared<ExpressionActions>(hits_projection_actions->clone(), settings.getActionsSettings());
            auto expression = std::make_shared<ExpressionTransform>(hits->getSharedHeader(), expression_actions);
            connect(*hits, expression->getInputPort());
            hits = &expression->getOutputPort();
            processors.push_back(expression);
        }

        if (!hits_sort.empty())
        {
            UInt64 limit_for_sort = hits_limit ? hits_limit + hits_offset : 0;
            auto sorting = std::make_shared<PartialSortingTransform>(hits->getSharedHeader(), hits_sort, limit_for_sort);
            connect(*hits, sorting->getInputPort());
            hits = &sorting->getOutputPort();
            processors.push_back(sorting);
        }

        if (hits_limit || hits_offset)
        {
            auto limit = std::make_shared<LimitTransform>(hits->getSharedHeader(), hits_limit, hits_offset, 1, /* always_read_till_end = */ true);
            connect(*hits, limit->getInputPort());
            hits = &limit->getOutputPort();
            processors.push_back(limit);
        }

        auto transform_params = std::make_shared<AggregatingTransformParams>(input_header, agg_params, /* final = */ true);
        auto aggregating = std::make_shared<AggregatingTransform>(input_header, transform_params, nullptr);
        connect(*aggs_src, aggregating->getInputs().front());
        processors.push_back(aggregating);

        auto glue = std::make_shared<HitsAggregatesGlueTransform>(
            hits->getSharedHeader(),
            aggregates_header);
        connect(*hits, glue->getInputs().front());
        connect(aggregating->getOutputs().front(), *std::next(glue->getInputs().begin()));
        processors.push_back(glue);

        return processors;
    },
    /* check_ports = */ true,
    /* check_output_headers = */ false);

    pipeline.setAggregatesSharedHeader(aggregates_header);
    pipeline.extractAggregatesPort(pipeline.getNumStreams() - 1);
}

void HitAndAggregateStep::describeActions(FormatSettings & settings) const
{
    settings.out << settings.detail_prefix << "Hits limit: " << hits_limit << ", offset: " << hits_offset
                 << ", max block size: " << max_block_size << ", merge threads: " << merge_threads << '\n';
}

void HitAndAggregateStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Hits limit", hits_limit);
    map.add("Hits offset", hits_offset);
}

void HitAndAggregateStep::updateOutputHeader()
{
    output_header = input_headers.front();
    if (hits_projection_actions)
        output_header = std::make_shared<const Block>(hits_projection_actions->updateHeader(*input_headers.front()));
}

void HitAndAggregateStep::serialize(Serialization &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Serialization of HitAndAggregateStep is not implemented");
}

QueryPlanStepPtr HitAndAggregateStep::deserialize(Deserialization &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Deserialization of HitAndAggregateStep is not implemented");
}

void registerHitAndAggregateStep(QueryPlanStepRegistry & registry);
void registerHitAndAggregateStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("HitAndAggregate", HitAndAggregateStep::deserialize);
}

}
