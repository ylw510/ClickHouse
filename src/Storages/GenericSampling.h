#pragma once

#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context_fwd.h>

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace DB
{

class IStorage;
class QueryPlan;
struct FormatFilterInfo;
struct SelectQueryInfo;

/// Half-open interval on [0, 1), wrapping when `offset + size > 1`.
/// Matches MergeTree `SAMPLE k OFFSET m` as a Bernoulli / hash-range predicate for storages
/// that have no SAMPLE BY key.
struct GenericSampleInterval
{
    Float64 offset = 0;
    Float64 size = 1;

    bool isFull() const { return offset <= 0 && size >= 1; }
    bool contains(Float64 x) const;
    bool containsHash(UInt64 hash) const;
};

/** SAMPLE for table functions and file engines (`numbers` / `file` / `url` / `s3`).
  *
  * Row-level: a `FilterStep` on `randCanonical()` (Bernoulli).
  * Parquet: skip whole row groups by `sipHash(path, row_group_id)` so SAMPLE can avoid I/O;
  * the planner then does not add a second FilterStep (`SelectQueryInfo::generic_sample_applied`).
  */
class GenericSampling
{
public:
    static std::optional<GenericSampleInterval> fromQueryInfo(const SelectQueryInfo & query_info);
    static bool isActive(const SelectQueryInfo & query_info);

    static bool formatSupportsRowGroupSampling(const String & format);
    static void markAppliedIfPushedDown(SelectQueryInfo & query_info, const String & format);
    static void attachToFormatFilter(FormatFilterInfo & info, const SelectQueryInfo & query_info);
    static void adjustReadFlags(SelectQueryInfo & query_info, const String & format, bool & need_only_count);

    static UInt64 hashRowGroup(std::string_view path, size_t row_group_id);
    static std::optional<std::vector<size_t>> filterRowGroups(
        std::string_view path,
        size_t num_row_groups,
        const GenericSampleInterval & sample,
        const std::optional<std::vector<size_t>> & existing);

    static ActionsDAG makeRowFilterDAG(
        const NamesAndTypesList & header,
        const GenericSampleInterval & sample,
        const ContextPtr & context);

    static void addFilterStepIfNeeded(
        QueryPlan & query_plan,
        const IStorage & storage,
        SelectQueryInfo & query_info,
        const ContextPtr & context);
};

}
