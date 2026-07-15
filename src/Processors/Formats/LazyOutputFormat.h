#pragma once
#include <Processors/Formats/IOutputFormat.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <QueryPipeline/ProfileInfo.h>

namespace DB
{

class NullWriteBuffer;

/// LazyOutputFormat is used to retrieve ready data from executing pipeline.
/// You can periodically call `getChunk` from separate thread.
/// Used in PullingAsyncPipelineExecutor.
class LazyOutputFormat : public IOutputFormat
{

public:
    explicit LazyOutputFormat(SharedHeader header);
    LazyOutputFormat(SharedHeader header, SharedHeader aggregates_header);

    String getName() const override { return "LazyOutputFormat"; }

    Chunk getChunk(UInt64 milliseconds = 0);
    Chunk getTotals();
    Chunk getExtremes();
    Chunks getAggregates();

    bool isFinished() { return queue.isFinishedAndEmpty(); }

    ProfileInfo & getProfileInfo() { return info; }

    void setRowsBeforeLimit(size_t rows_before_limit) override;
    void setRowsBeforeAggregation(size_t rows_before_aggregation) override;

    void onCancel() noexcept override
    {
        queue.clearAndFinish();
    }

    void finalizeImpl() override
    {
        queue.finish();
    }

    bool expectMaterializedColumns() const override { return false; }
    bool supportsSpecialSerializationKinds() const override { return true; }

protected:
    void consume(Chunk chunk) override
    {
        (void)(queue.emplace(std::move(chunk)));
    }

    void consumeTotals(Chunk chunk) override { totals = std::move(chunk); }
    void consumeExtremes(Chunk chunk) override { extremes = std::move(chunk); }
    void consumeAggregates(Chunk chunk) override { aggregates.emplace_back(std::move(chunk)); }

private:

    ConcurrentBoundedQueue<Chunk> queue;
    Chunk totals;
    Chunk extremes;
    Chunks aggregates;

    /// Is not used.
    static NullWriteBuffer out;

    ProfileInfo info;
};

}
