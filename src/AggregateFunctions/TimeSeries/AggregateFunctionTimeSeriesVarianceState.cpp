#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/TimeSeries/TimeSeriesVarianceState.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/assert_cast.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

class AggregateFunctionTimeSeriesVarianceState final : public IAggregateFunctionDataHelper<
    TimeSeriesVarianceState, AggregateFunctionTimeSeriesVarianceState>
{
public:
    explicit AggregateFunctionTimeSeriesVarianceState(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<TimeSeriesVarianceState, AggregateFunctionTimeSeriesVarianceState>(
            argument_types_, {}, std::make_shared<DataTypeFloat64>())
    {
        if (argument_types_.size() != 1 || argument_types_[0]->getTypeId() != TypeIndex::Float64)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Aggregate function {} requires one Float64 argument", getName());
    }

    String getName() const override { return "timeSeriesVariance"; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).add(assert_cast<const ColumnFloat64 &>(*columns[0]).getData()[row_num]);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & state = this->data(place);
        writeBinaryLittleEndian(state.count, buf);
        writeBinaryLittleEndian(state.mean, buf);
        writeBinaryLittleEndian(state.m2, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        auto & state = this->data(place);
        readBinaryLittleEndian(state.count, buf);
        readBinaryLittleEndian(state.mean, buf);
        readBinaryLittleEndian(state.m2, buf);
    }

    /// The minute rollup column keeps the state. `insertResultInto` is the population variance, used if the state is finalized.
    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        const auto & state = this->data(place);
        Float64 result = 0;
        if (state.count != 0)
            result = state.m2 / static_cast<Float64>(state.count);
        assert_cast<ColumnFloat64 &>(to).getData().push_back(result);
    }
};

AggregateFunctionPtr create(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    return std::make_shared<AggregateFunctionTimeSeriesVarianceState>(argument_types);
}

}

void registerAggregateFunctionTimeSeriesVarianceState(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Welford/Chan population moments (`count`, `mean`, `m2`) of `Float64` samples. The TimeSeries minute rollup stores this state and merges it with `timeSeriesVarianceMerge`.
)";
    FunctionDocumentation::Syntax syntax = "timeSeriesVariance(value)";
    FunctionDocumentation::Arguments arguments = {{"value", "Sample value.", {"Float64"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Population variance `m2 / count` when the state is finalized.", {"Float64"}};
    FunctionDocumentation::Examples examples = {};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction("timeSeriesVariance", {create, documentation});
}

}
