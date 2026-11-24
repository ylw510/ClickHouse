#pragma once

#include <base/types.h>

namespace DB
{

/// Welford/Chan population moments. `merge` matches `AggregateFunctionTimeseriesVarianceTraits::Summary::merge`.
struct TimeSeriesVarianceState
{
    UInt64 count = 0;
    Float64 mean = 0;
    Float64 m2 = 0;

    void add(Float64 x)
    {
        ++count;
        const Float64 delta = x - mean;
        mean += delta / static_cast<Float64>(count);
        m2 += delta * (x - mean);
    }

    void merge(const TimeSeriesVarianceState & other)
    {
        if (other.count == 0)
            return;
        if (count == 0)
        {
            *this = other;
            return;
        }

        const Float64 na = static_cast<Float64>(count);
        const Float64 nb = static_cast<Float64>(other.count);
        const Float64 total = na + nb;
        const Float64 delta = other.mean - mean;

        mean += delta * nb / total;
        m2 += other.m2 + delta * delta * na * nb / total;
        count += other.count;
    }
};

}
