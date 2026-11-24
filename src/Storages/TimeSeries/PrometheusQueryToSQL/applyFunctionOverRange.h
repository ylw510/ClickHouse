#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>

#include <optional>
#include <Parsers/IAST_fwd.h>


namespace DB::PrometheusQueryToSQL
{

/// Returns whether the specified string is the name of a prometheus function taking a range vector.
/// Examples: rate(), idelta(), last_over_time().
bool isFunctionOverRange(std::string_view function_name);

/// Applies a prometheus function taking a range vector.
SQLQueryPiece applyFunctionOverRange(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

/// `drop_metric_name` overrides the function's own metric-name policy. Internal callers that reuse a
/// translation for a private intermediate (e.g. absent_over_time's presence grid) pass `false`: dropping
/// the name there could only manufacture duplicate label sets, which the public path rejects.
SQLQueryPiece applyFunctionOverRange(
    const Node * node,
    std::string_view function_name,
    std::vector<SQLQueryPiece> && arguments,
    ConverterContext & context,
    std::optional<bool> drop_metric_name = std::nullopt);

/// Reads the minute rollup table when `function_name` can be answered from it and the range argument is a stored selector.
/// `quantile_phi` is the literal phi for `quantile_over_time`; other functions pass nullptr.
std::optional<SQLQueryPiece> tryApplyMinuteRollup(
    const Node * node,
    std::string_view function_name,
    const SQLQueryPiece & range_argument,
    ConverterContext & context,
    const NodeEvaluationRange & aggregation_range,
    Decimal64 window,
    ASTPtr quantile_phi,
    bool drop_metric_name);

}
