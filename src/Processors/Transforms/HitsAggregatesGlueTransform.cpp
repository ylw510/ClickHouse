#include <Processors/Transforms/HitsAggregatesGlueTransform.h>

#include <Processors/Port.h>

namespace DB
{

HitsAggregatesGlueTransform::HitsAggregatesGlueTransform(SharedHeader hits_header, SharedHeader aggregates_header)
    : IProcessor(
        InputPorts{hits_header, aggregates_header},
        OutputPorts{hits_header, aggregates_header})
{
}

IProcessor::Status HitsAggregatesGlueTransform::prepare()
{
    bool need_data = false;
    bool port_full = false;
    bool all_finished = true;

    auto input_it = inputs.begin();
    auto output_it = outputs.begin();

    for (size_t i = 0; i < 2; ++i, ++input_it, ++output_it)
    {
        auto & input = *input_it;
        auto & output = *output_it;

        if (output.isFinished())
            continue;

        all_finished = false;

        if (!data[i].isEmpty())
            continue;

        if (input.isFinished())
        {
            output.finish();
            continue;
        }

        input.setNeeded();

        if (!input.hasData())
        {
            need_data = true;
            continue;
        }

        data[i] = input.pullData();
    }

    output_it = outputs.begin();
    for (size_t i = 0; i < 2; ++i, ++output_it)
    {
        if (data[i].isEmpty())
            continue;

        auto & output = *output_it;

        if (output.isFinished())
        {
            data[i] = {};
            continue;
        }

        if (!output.canPush())
        {
            port_full = true;
            continue;
        }

        if (data[i].exception)
            output.pushException(data[i].exception);
        else
            output.push(data[i].chunk.clone());

        data[i] = {};
    }

    if (all_finished)
        return Status::Finished;

    if (port_full)
        return Status::PortFull;

    if (need_data)
        return Status::NeedData;

    return Status::Ready;
}

}
