#pragma once

#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>

namespace DB
{

/// Processor with 2 inputs and 2 outputs that pass through hits and aggregates streams independently.
/// Headers may differ between the two pairs.
class HitsAggregatesGlueTransform final : public IProcessor
{
public:
    HitsAggregatesGlueTransform(SharedHeader hits_header, SharedHeader aggregates_header);

    String getName() const override { return "HitsAggregatesGlue"; }

    void work() override
    {
    }

    Status prepare() override;

    OutputPort & getHitsPort() { return outputs.front(); }
    OutputPort & getAggregatesPort() { return outputs.back(); }

private:
    std::array<Port::Data, 2> data;
};

}
