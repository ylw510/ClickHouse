#include <base/defines.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypeDateTime.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/QuantileTDigest.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHelpers.h>
#include <AggregateFunctions/TimeSeries/TimeSeriesVarianceState.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/NaNUtils.h>
#include <Common/assert_cast.h>
#include <Core/DecimalFunctions.h>

#include <cmath>
#include <limits>
#include <map>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

enum class RollupKind : UInt8
{
    Max,
    Min,
    Sum,
    Count,
    Avg,
    Present,
    Stddev,
    Stdvar,
    Quantile,
};

struct RollupMinute
{
    bool has = false;
    Float64 max_v = 0;
    Float64 min_v = 0;
    Float64 sum = 0;
    UInt64 count = 0;
    TimeSeriesVarianceState variance;
    QuantileTDigest<Float64> digest;
};

using RollupMinutes = std::map<Int64, RollupMinute>;

/// Slides minute rollup rows onto a PromQL grid. A minute stored under `bucket` covers `(bucket, bucket + 60s]`.
/// Partial rows of the same minute are merged. A minute that only touches a window is included whole.
class AggregateFunctionTimeSeriesRollupToGrid final : public IAggregateFunctionDataHelper<
    RollupMinutes,
    AggregateFunctionTimeSeriesRollupToGrid>
{
public:
    using Minute = RollupMinute;
    using Minutes = RollupMinutes;

    AggregateFunctionTimeSeriesRollupToGrid(
        const DataTypes & argument_types_,
        const Array & parameters_,
        RollupKind kind_,
        Int64 start_,
        Int64 end_,
        Int64 step_,
        Int64 window_,
        Int64 scale_multiplier_,
        Int64 minute_ticks_,
        Float64 phi_)
        : IAggregateFunctionDataHelper<Minutes, AggregateFunctionTimeSeriesRollupToGrid>(
            argument_types_, parameters_,
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>())))
        , kind(kind_)
        , start(start_)
        , end(end_)
        , step(step_)
        , window(window_)
        , scale_multiplier(scale_multiplier_)
        , minute_ticks(minute_ticks_)
        , phi(phi_)
        , grid_size(computeGridSize(start_, end_, step_))
    {
    }

    String getName() const override
    {
        switch (kind)
        {
            case RollupKind::Max: return "timeSeriesRollupMaxToGrid";
            case RollupKind::Min: return "timeSeriesRollupMinToGrid";
            case RollupKind::Sum: return "timeSeriesRollupSumToGrid";
            case RollupKind::Count: return "timeSeriesRollupCountToGrid";
            case RollupKind::Avg: return "timeSeriesRollupAvgToGrid";
            case RollupKind::Present: return "timeSeriesRollupPresentToGrid";
            case RollupKind::Stddev: return "timeSeriesRollupStddevToGrid";
            case RollupKind::Stdvar: return "timeSeriesRollupStdvarToGrid";
            case RollupKind::Quantile: return "timeSeriesRollupQuantileToGrid";
        }
        UNREACHABLE();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const Int64 bucket = readBucket(*columns[0], row_num) * scale_multiplier;
        Minute & minute = this->data(place)[bucket];

        switch (kind)
        {
            case RollupKind::Max:
                mergeExtreme(minute, readFloat(*columns[1], row_num), /* is_min = */ false);
                break;
            case RollupKind::Min:
                mergeExtreme(minute, readFloat(*columns[1], row_num), /* is_min = */ true);
                break;
            case RollupKind::Sum:
            {
                const Float64 value = readFloat(*columns[1], row_num);
                minute.sum += value;
                minute.has = true;
                break;
            }
            case RollupKind::Count:
            case RollupKind::Present:
                minute.count += readCount(*columns[1], row_num);
                minute.has = minute.count > 0;
                break;
            case RollupKind::Avg:
                minute.sum += readFloat(*columns[1], row_num);
                minute.count += readCount(*columns[2], row_num);
                minute.has = minute.count > 0;
                break;
            case RollupKind::Stddev:
            case RollupKind::Stdvar:
            {
                const auto & column = assert_cast<const ColumnAggregateFunction &>(*columns[1]);
                const auto & other = *reinterpret_cast<const TimeSeriesVarianceState *>(column.getData()[row_num]);
                minute.variance.merge(other);
                minute.has = minute.variance.count > 0;
                break;
            }
            case RollupKind::Quantile:
            {
                const auto & column = assert_cast<const ColumnAggregateFunction &>(*columns[1]);
                const auto & other = *reinterpret_cast<const QuantileTDigest<Float64> *>(column.getData()[row_num]);
                minute.digest.merge(other);
                minute.has = true;
                break;
            }
        }
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        for (const auto & [bucket, minute] : this->data(rhs))
            mergeMinute(this->data(place)[bucket], minute);
    }

    void serialize(ConstAggregateDataPtr, WriteBuffer &, std::optional<size_t>) const override
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} does not support serialization", getName());
    }

    void deserialize(AggregateDataPtr, ReadBuffer &, std::optional<size_t>, Arena *) const override
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} does not support deserialization", getName());
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & array = assert_cast<ColumnArray &>(to);
        auto & nullable = assert_cast<ColumnNullable &>(array.getData());
        auto & values = assert_cast<ColumnFloat64 &>(nullable.getNestedColumn()).getData();
        auto & nulls = nullable.getNullMapData();

        const auto & minutes = this->data(place);
        for (size_t index = 0; index < grid_size; ++index)
        {
            const Int64 point = (step == 0) ? start : start + static_cast<Int64>(index) * step;
            Minute combined;
            for (const auto & [bucket, minute] : minutes)
            {
                if (intersects(bucket, point))
                    mergeMinute(combined, minute);
            }

            Float64 result = 0;
            bool is_null = !combined.has;
            if (!is_null)
                is_null = !compute(combined, result);

            values.push_back(result);
            nulls.push_back(is_null ? 1 : 0);
        }
        array.getOffsets().push_back(values.size());
    }

private:
    static size_t computeGridSize(Int64 start_, Int64 end_, Int64 step_)
    {
        if (step_ == 0)
            return 1;
        if (step_ < 0 || end_ < start_)
            return 0;
        const Int128 span = static_cast<Int128>(end_) - start_;
        const Int128 size = span / step_ + 1;
        if (size > 16 * 1024 * 1024)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Time grid is too large");
        return static_cast<size_t>(size);
    }

    Int64 readBucket(const IColumn & column, size_t row) const
    {
        if (scale_multiplier != 1 && typeid_cast<const ColumnUInt32 *>(&column))
            return static_cast<Int64>(column.getUInt(row));
        if (typeid_cast<const ColumnUInt32 *>(&column) || typeid_cast<const ColumnUInt64 *>(&column))
            return static_cast<Int64>(column.getUInt(row));
        return column.getInt(row);
    }

    static Float64 readFloat(const IColumn & column, size_t row)
    {
        return assert_cast<const ColumnFloat64 &>(column).getData()[row];
    }

    static UInt64 readCount(const IColumn & column, size_t row)
    {
        if (const auto * floats = typeid_cast<const ColumnFloat64 *>(&column))
            return static_cast<UInt64>(floats->getData()[row]);
        return column.getUInt(row);
    }

    [[nodiscard]] bool intersects(Int64 bucket, Int64 point) const
    {
        /// Minute `(bucket, bucket + minute_ticks]`, window `(point - window, point]`.
        const Int128 minute_end = static_cast<Int128>(bucket) + minute_ticks;
        const Int128 window_start = static_cast<Int128>(point) - window;
        return static_cast<Int128>(bucket) < point && minute_end > window_start;
    }

    static void mergeExtreme(Minute & minute, Float64 incoming, bool is_min)
    {
        if (!minute.has)
        {
            minute.max_v = incoming;
            minute.min_v = incoming;
            minute.has = true;
            return;
        }
        Float64 & slot = is_min ? minute.min_v : minute.max_v;
        if (isNaN(slot))
        {
            if (!isNaN(incoming))
                slot = incoming;
            return;
        }
        if (isNaN(incoming))
            return;
        if (is_min)
        {
            if (incoming < slot)
                slot = incoming;
        }
        else if (incoming > slot)
        {
            slot = incoming;
        }
    }

    void mergeMinute(Minute & into, const Minute & from) const
    {
        if (!from.has && from.count == 0 && from.variance.count == 0)
        {
            if (kind != RollupKind::Quantile)
                return;
        }

        switch (kind)
        {
            case RollupKind::Max:
                if (from.has)
                    mergeExtreme(into, from.max_v, false);
                break;
            case RollupKind::Min:
                if (from.has)
                    mergeExtreme(into, from.min_v, true);
                break;
            case RollupKind::Sum:
                into.sum += from.sum;
                into.has = true;
                break;
            case RollupKind::Count:
            case RollupKind::Present:
            case RollupKind::Avg:
                into.sum += from.sum;
                into.count += from.count;
                into.has = into.count > 0 || (kind == RollupKind::Avg && from.has);
                if (kind != RollupKind::Avg)
                    into.has = into.count > 0;
                break;
            case RollupKind::Stddev:
            case RollupKind::Stdvar:
                into.variance.merge(from.variance);
                into.has = into.variance.count > 0;
                break;
            case RollupKind::Quantile:
                into.digest.merge(from.digest);
                into.has = true;
                break;
        }
    }

    [[nodiscard]] bool compute(const Minute & minute, Float64 & result) const
    {
        switch (kind)
        {
            case RollupKind::Max:
                result = minute.max_v;
                return minute.has;
            case RollupKind::Min:
                result = minute.min_v;
                return minute.has;
            case RollupKind::Sum:
                result = minute.sum;
                return minute.has;
            case RollupKind::Count:
                result = static_cast<Float64>(minute.count);
                return minute.count > 0;
            case RollupKind::Avg:
                if (minute.count == 0)
                    return false;
                result = minute.sum / static_cast<Float64>(minute.count);
                return true;
            case RollupKind::Present:
                if (minute.count == 0)
                    return false;
                result = 1;
                return true;
            case RollupKind::Stddev:
            case RollupKind::Stdvar:
            {
                if (minute.variance.count == 0)
                    return false;
                const Float64 variance = minute.variance.m2 / static_cast<Float64>(minute.variance.count);
                result = (kind == RollupKind::Stddev) ? std::sqrt(variance) : variance;
                return true;
            }
            case RollupKind::Quantile:
            {
                if (std::isnan(phi))
                {
                    result = std::numeric_limits<Float64>::quiet_NaN();
                    return true;
                }
                if (phi < 0)
                {
                    result = -std::numeric_limits<Float64>::infinity();
                    return true;
                }
                if (phi > 1)
                {
                    result = std::numeric_limits<Float64>::infinity();
                    return true;
                }
                result = minute.digest.get<Float64>(phi);
                return !std::isnan(result);
            }
        }
        return false;
    }

    RollupKind kind;
    Int64 start;
    Int64 end;
    Int64 step;
    Int64 window;
    Int64 scale_multiplier;
    Int64 minute_ticks;
    Float64 phi;
    size_t grid_size;
};

UInt32 columnScale(const DataTypePtr & type)
{
    if (const auto * date_time64 = typeid_cast<const DataTypeDateTime64 *>(type.get()))
        return date_time64->getScale();
    return 0;
}

AggregateFunctionPtr createRollup(
    const std::string & name,
    const DataTypes & argument_types,
    const Array & parameters,
    const Settings * settings,
    RollupKind kind,
    size_t value_arguments)
{
    checkTimeseriesAggregateFunctionsEnabled(name, settings);
    const size_t expected_parameters = (kind == RollupKind::Quantile) ? 5 : 4;
    const size_t expected_arguments = 1 + value_arguments;
    if (parameters.size() != expected_parameters)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires {} parameters, got {}", name, expected_parameters, parameters.size());
    if (argument_types.size() != expected_arguments)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires {} arguments, got {}", name, expected_arguments, argument_types.size());

    WhichDataType timestamp_which(argument_types[0]);
    if (!timestamp_which.isDateTime64() && !timestamp_which.isDateTime() && !timestamp_which.isUInt32())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of the bucket timestamps for aggregate function {}", argument_types[0]->getName(), name);

    const UInt32 column_scale = columnScale(argument_types[0]);
    const UInt32 grid_scale = std::max(column_scale, getTimeseriesParametersScale(
        kind == RollupKind::Quantile ? Array(parameters.begin() + 1, parameters.end()) : parameters));

    size_t parameter_index = 0;
    Float64 phi = 0.5;
    if (kind == RollupKind::Quantile)
        phi = extractTimeseriesFloatParameter(name, "phi", parameters[parameter_index++]);

    const DateTime64 grid_start = extractTimeseriesTimestampParameter(name, "start", parameters[parameter_index++], grid_scale);
    const DateTime64 grid_end = extractTimeseriesTimestampParameter(name, "end", parameters[parameter_index++], grid_scale);
    const Decimal64 grid_step = extractTimeseriesDurationParameter(name, "step", parameters[parameter_index++], grid_scale);
    const Decimal64 grid_window = extractTimeseriesDurationParameter(name, "window", parameters[parameter_index++], grid_scale);

    const Int64 scale_multiplier = DecimalUtils::scaleMultiplier<Int64>(grid_scale - column_scale);
    const Int64 minute_ticks = 60 * DecimalUtils::scaleMultiplier<Int64>(grid_scale);

    return std::make_shared<AggregateFunctionTimeSeriesRollupToGrid>(
        argument_types, parameters, kind,
        grid_start.value, grid_end.value, grid_step.value, grid_window.value,
        scale_multiplier, minute_ticks, phi);
}

}

void registerAggregateFunctionTimeSeriesRollupToGrid(AggregateFunctionFactory & factory)
{
    auto documentation_for = [](const char * name, const char * syntax)
    {
        FunctionDocumentation::Description description = "Slide minute rollup rows of a TimeSeries table onto a PromQL time grid.";
        FunctionDocumentation::Syntax syntax_doc = syntax;
        FunctionDocumentation::Arguments arguments = {
            {"bucket", "Start of the right-closed minute.", {"DateTime", "DateTime64", "UInt32"}},
            {"state", "The rollup column for this function.", {"Float64", "UInt64", "AggregateFunction"}},
        };
        FunctionDocumentation::Parameters parameters = {
            {"start", "Grid start.", {"DateTime64"}},
            {"end", "Grid end.", {"DateTime64"}},
            {"step", "Grid step.", {"Decimal64"}},
            {"window", "Window length.", {"Decimal64"}},
        };
        FunctionDocumentation::ReturnedValue returned_value = {"One nullable value per grid point.", {"Array(Nullable(Float64))"}};
        FunctionDocumentation::Examples examples = {};
        FunctionDocumentation::IntroducedIn introduced_in = {26, 11};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
        return FunctionDocumentation{description, syntax_doc, arguments, parameters, returned_value, examples, introduced_in, category};
    };

    auto add = [&](const char * name, RollupKind kind, size_t value_arguments, const char * syntax)
    {
        factory.registerFunction(name, {[=](const std::string & function_name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
        {
            return createRollup(function_name, argument_types, parameters, settings, kind, value_arguments);
        }, documentation_for(name, syntax)});
    };

    add("timeSeriesRollupMaxToGrid", RollupKind::Max, 1, "timeSeriesRollupMaxToGrid(start, end, step, window)(bucket, max)");
    add("timeSeriesRollupMinToGrid", RollupKind::Min, 1, "timeSeriesRollupMinToGrid(start, end, step, window)(bucket, min)");
    add("timeSeriesRollupSumToGrid", RollupKind::Sum, 1, "timeSeriesRollupSumToGrid(start, end, step, window)(bucket, sum)");
    add("timeSeriesRollupCountToGrid", RollupKind::Count, 1, "timeSeriesRollupCountToGrid(start, end, step, window)(bucket, count)");
    add("timeSeriesRollupAvgToGrid", RollupKind::Avg, 2, "timeSeriesRollupAvgToGrid(start, end, step, window)(bucket, sum, count)");
    add("timeSeriesRollupPresentToGrid", RollupKind::Present, 1, "timeSeriesRollupPresentToGrid(start, end, step, window)(bucket, count)");
    add("timeSeriesRollupStddevToGrid", RollupKind::Stddev, 1, "timeSeriesRollupStddevToGrid(start, end, step, window)(bucket, variance)");
    add("timeSeriesRollupStdvarToGrid", RollupKind::Stdvar, 1, "timeSeriesRollupStdvarToGrid(start, end, step, window)(bucket, variance)");
    add("timeSeriesRollupQuantileToGrid", RollupKind::Quantile, 1, "timeSeriesRollupQuantileToGrid(phi, start, end, step, window)(bucket, quantile)");
}

}
