#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/NaNUtils.h>
#include <Common/assert_cast.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

template <bool is_min>
struct TimeSeriesPromQLMinMaxData
{
    Float64 value = 0;
    bool has_value = false;
};

/// Float64 min/max with the PromQL rule that NaN loses to any real value.
/// The state is larger than Float64; `SimpleAggregateFunction` stores only the result of `insertResultInto`.
template <bool is_min>
class AggregateFunctionTimeSeriesPromQLMinMax final : public IAggregateFunctionDataHelper<
    TimeSeriesPromQLMinMaxData<is_min>,
    AggregateFunctionTimeSeriesPromQLMinMax<is_min>>
{
public:
    using Data = TimeSeriesPromQLMinMaxData<is_min>;

    explicit AggregateFunctionTimeSeriesPromQLMinMax(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionTimeSeriesPromQLMinMax<is_min>>(
            argument_types_, {}, std::make_shared<DataTypeFloat64>())
    {
        if (argument_types_.size() != 1 || argument_types_[0]->getTypeId() != TypeIndex::Float64)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Aggregate function {} requires one Float64 argument", getName());
    }

    String getName() const override
    {
        return is_min ? "timeSeriesMin" : "timeSeriesMax";
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        consider(this->data(place), assert_cast<const ColumnFloat64 &>(*columns[0]).getData()[row_num]);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        const auto & other = this->data(rhs);
        if (other.has_value)
            consider(this->data(place), other.value);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinary(this->data(place).has_value, buf);
        writeBinaryLittleEndian(this->data(place).value, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        readBinary(this->data(place).has_value, buf);
        readBinaryLittleEndian(this->data(place).value, buf);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        Float64 result = 0;
        if (this->data(place).has_value)
            result = this->data(place).value;
        assert_cast<ColumnFloat64 &>(to).getData().push_back(result);
    }

private:
    static void consider(Data & data, Float64 incoming)
    {
        if (!data.has_value)
        {
            data.value = incoming;
            data.has_value = true;
            return;
        }

        if (isNaN(data.value))
        {
            if (!isNaN(incoming))
                data.value = incoming;
            return;
        }
        if (isNaN(incoming))
            return;

        if constexpr (is_min)
        {
            if (incoming < data.value)
                data.value = incoming;
        }
        else
        {
            if (incoming > data.value)
                data.value = incoming;
        }
    }
};

template <bool is_min>
AggregateFunctionPtr createAggregateFunctionTimeSeriesPromQLMinMax(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    return std::make_shared<AggregateFunctionTimeSeriesPromQLMinMax<is_min>>(argument_types);
}

}

void registerAggregateFunctionTimeSeriesPromQLMinMax(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
PromQL `min`/`max` over `Float64` samples: a NaN loses to any real value. Used as `SimpleAggregateFunction` in the TimeSeries minute rollup table.
)";
    FunctionDocumentation::Syntax syntax = R"(
timeSeriesMax(value)
timeSeriesMin(value)
)";
    FunctionDocumentation::Arguments arguments = {{"value", "Sample value.", {"Float64"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"The minimum or maximum.", {"Float64"}};
    FunctionDocumentation::Examples examples = {};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction("timeSeriesMax", {createAggregateFunctionTimeSeriesPromQLMinMax<false>, documentation});
    factory.registerFunction("timeSeriesMin", {createAggregateFunctionTimeSeriesPromQLMinMax<true>, documentation});
}

}
