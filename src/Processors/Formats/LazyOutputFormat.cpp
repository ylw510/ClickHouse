#include <IO/NullWriteBuffer.h>
#include <Processors/Formats/LazyOutputFormat.h>
#include <Processors/Port.h>
#include <Processors/Transforms/AggregatingTransform.h>


namespace DB
{

NullWriteBuffer LazyOutputFormat::out;

LazyOutputFormat::LazyOutputFormat(SharedHeader header)
    : IOutputFormat(header, out)
    , queue(2)
{
}

LazyOutputFormat::LazyOutputFormat(SharedHeader header, SharedHeader aggregates_header)
    : IOutputFormat(header, out, aggregates_header)
    , queue(2)
{
}

Chunk LazyOutputFormat::getChunk(UInt64 milliseconds)
{
    if (isFinished())
        return {};

    Chunk chunk;
    if (milliseconds)
    {
        if (!queue.tryPop(chunk, milliseconds))
            return {};
    }
    else
    {
        if (!queue.pop(chunk))
            return {};
    }

    if (chunk)
        info.update(chunk.getNumRows(), chunk.allocatedBytes());

    return chunk;
}

Chunk LazyOutputFormat::getTotals()
{
    return std::move(totals);
}

Chunk LazyOutputFormat::getExtremes()
{
    return std::move(extremes);
}

Chunks LazyOutputFormat::getAggregates()
{
    return std::move(aggregates);
}

void LazyOutputFormat::setRowsBeforeLimit(size_t rows_before_limit)
{
    info.setRowsBeforeLimit(rows_before_limit);
}

void LazyOutputFormat::setRowsBeforeAggregation(size_t rows_before_aggregation)
{
    info.setRowsBeforeAggregation(rows_before_aggregation);
}

}
