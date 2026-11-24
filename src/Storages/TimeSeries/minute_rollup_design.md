# TimeSeries 分钟 rollup 内表实现方案

`samples` 保持不变，继续保存原始点。分钟 rollup 是多写的一份摘要，和现有的 `recent samples` 是同一类东西：同一批点，再写一张更适合一类查询的内表。`tags` 和 `time ranges` 不重写，`id` 仍是同一套。

## 查询路由

下面这句是路由的依据：

> 窗口不对齐、步长小于 1 分钟、或函数不是 `rate` / `deriv` 时，仍然查询这张聚合表。

「这张聚合表」指本方案新增的分钟 rollup 内表。三个条件是「或」：

| 窗口与整分钟对齐 | 步长 | 函数 | 读取 |
| --- | --- | --- | --- |
| 否 | 任意 | 任意 | 分钟 rollup |
| 任意 | `< 1` 分钟 | 任意 | 分钟 rollup |
| 任意 | 任意 | 不是 `rate`，也不是 `deriv` | 分钟 rollup |
| 是 | `>= 1` 分钟，且为整分钟 | `rate` 或 `deriv` | `samples`（或 TTL 内的 `recent samples`） |

因此只有同时满足下面三条才读原始点：

1. 窗口与存储的分钟对齐。
2. 步长大于或等于 1 分钟，并且是整分钟。步长为 0 的单点求值看求值时刻是否落在整分钟上；落在整分钟上则窗口条件按对齐处理。
3. 函数是 `rate` 或 `deriv`。

`max_over_time`、`min_over_time`、`sum_over_time`、`avg_over_time`、`count_over_time`、`present_over_time`、`quantile_over_time`、`irate`、`increase`、`histogram_quantile`、精确时间戳，以及其他不是 `rate` / `deriv` 的函数，不论窗口是否对齐、步长是否小于 1 分钟，都读分钟 rollup。

`rate` 和 `deriv` 在窗口不对齐或步长小于 1 分钟时也读分钟 rollup。只有对齐且步长至少 1 分钟时，才走现在的 `timeSeriesRateToGrid` / `timeSeriesDerivToGrid`，读原始点。

表设置或查询设置把 rollup 关掉时，没有这张表可读，所有函数仍走现有的 `samples` 路径。这是功能开关，不是查询失败后的另一条算法。

## 表

新的 TimeSeries 版本为 12。`ViewTarget::Kind` 增加 `MinuteRollups`，关键字 `MINUTE ROLLUPS`，内表名 `.inner_id.minuterollups.<uuid>`。

- 引擎：`AggregatingMergeTree`。复制表沿用现有内表的 `Replicated` / `Shared` 前缀。
- `ORDER BY (id, bucket)`。排序键就是聚合键，同一 `(id, bucket)` 的多批写入在后台合成一行。
- 分区默认 `toYYYYMM(toDateTime(bucket, 'UTC'))`，与 `samples` 相同。
- 行很窄，`index_granularity` 用 8192。
- 不设 TTL，保留时间与 `samples` 相同。
- 不存 tags、metric name、time range。

| 列 | 类型 |
| --- | --- |
| `id` | 与 `tags.id` 相同 |
| `bucket` | 与 `samples.bucket` 相同的时间类型 |
| `max` | `SimpleAggregateFunction(timeSeriesMax, Float64)` |
| `min` | `SimpleAggregateFunction(timeSeriesMin, Float64)` |
| `sum` | `SimpleAggregateFunction(sum, Float64)` |
| `count` | `SimpleAggregateFunction(sum, UInt64)` |
| `quantile` | `AggregateFunction(quantileTDigest, Float64)` |

`timeSeriesMax` 和 `timeSeriesMin` 的状态仍是 `Float64`，比较规则与 `timeSeriesMaxToGrid` / `timeSeriesMinToGrid` 相同：NaN 输给任何实数。裸的 `SimpleAggregateFunction(max, Float64)` 对 NaN 的比较与这条规则不同，所以不用它。

`quantile` 存的是可合并状态，不存算好的 `Float64`。十五个分钟上的分位数再取分位数，并不是这十五分钟原始点的分位数。查询使用 `quantileTDigestMerge`。t-digest 是近似值，和 PromQL `quantile_over_time` 的精确分位数不同；按上一节的路由，`quantile_over_time` 不是 `rate` / `deriv`，所以读这一列。不能合并的分位数没有单独的精确状态列，`quantileExact` 会把原始点再存一遍，失去少读的意义，因此不建这一列。

`sum` 是普通 `Float64` 求和。`timeSeriesSumToGrid` 使用 Kahan 补偿，先按分钟相加再相加窗口，低位会和逐点补偿和差几个 ulp。`max`、`min`、`count` 可以逐位相等；`sum` 和 `avg` 的测试用相对误差。

## bucket

PromQL 和 `AggregateFunctionTimeseriesBase` 的窗口都是 `(t - window, t]`。`fromSelector` 把它写成闭区间 `[start - window + 1 tick, end]`。

`bucket` 是分钟起点，覆盖右闭区间 `(bucket, bucket + 60s]`：

- `12:00:01` 到 `12:01:00` 的 `bucket` 是 `12:00:00`。
- 正好落在 `12:00:00` 的点属于上一分钟，`bucket` 是 `11:59:00`。

刻度跟随 timestamp 类型，1 tick 与该类型的最小单位相同。`DateTime64(3)` 的 1 tick 是 1 毫秒。计算是 `roundDownToMultiple(timestamp - 1 tick, minute_step)`，向负无穷取整，与 `TimeSeriesSink::getBucket` 相同。类型下界单独处理，避免 `timestamp - 1 tick` 下溢。

网格点 `T` 落在整分钟、窗口是 `N` 个整分钟时，`(T - N 分钟, T]` 正好等于这 `N` 行：`bucket` 从 `T - N 分钟` 到 `T - 1 分钟`。`12:15:00` 上的 15 分钟窗口读 `12:00:00` … `12:14:00` 这 15 行，`12:15:00` 这一点在 `bucket = 12:14:00` 里。

左闭的墙钟分钟 `[M, M + 60s)` 会在两端各错一个边界点，所以不用。

## 写入

写在 `TimeSeriesSink` 里，`makeSamplesBlocksForTimestampType` 调用 `sortSamples` 之后。这一步已经按 series 排序，并且同一时间戳只保留值最大的点，NaN 输给实数，与 `timeSeriesGroupArray` 合并时的去重一致。rollup 使用这批下标，不再对未去重的原始数组聚合。

对每个 series，按上面的右闭分钟切分，每个 `(id, 分钟)` 写一行：`max`、`min`、`sum`、`count`，以及该分钟内点的 `quantileTDigest` 状态。

写入前丢掉 stale marker（`0x7ff0000000000002`）。range selector 在进入 `timeSeries*ToGrid` 之前会用 `arrayFilter` 去掉它。某一分钟只剩 stale marker 时不写行。

`samples` 块、`recent samples` 块、`tags`、`time ranges` 的构造不变。rollup 只增加一条与 `recent_samples_pipeline` 并列的 pipeline。

同一分钟跨多个 insert 时，由 `AggregatingMergeTree` 按 `(id, bucket)` 合并。`max` / `min` 对重复写入幂等。`samples` 在合并 `timeSeriesGroupArray` 时会按时间戳再去重；rollup 的 `sum`、`count`、`quantile` 已经把点压扁，同一次时间戳若出现在两个 insert 里（例如 remote write 重试），会算两次。这是这三列与 `samples` 的差别。

## 查询

判定放在 PromQL 转 SQL 的路径上（`applyFunctionOverRange` 及其同类），不放在 `StorageTimeSeriesSelector`。selector 只看到时间范围，看不到函数、步长和网格是否对齐。

读 rollup 时不读 `samples`。`tags` 和 `time ranges` 仍先得到 `id`。`timeSeriesSelector` 增加返回列模式 `minute_rollups`，复用现有的 id 过滤，选出 `id, bucket, max, min, sum, count, quantile`。

不使用 `FINAL`。同一分钟尚未合并的部分行由查询侧的聚合再合并一次。

网格点 `t`、窗口 `(t - window, t]` 取所有与该区间相交的分钟行，边界上的那一分钟整行计入。缺行表示这一分钟没有点，不补 0。

窗口对齐且是整分钟时，相交的分钟集合就等于窗口本身。`max_over_time(cpu_usage{job="node"}[15m])` 在步长 1 分钟、网格落在整分钟上时，每个网格点是这 15 行的 `max` 再取 `max`。`min`、`sum`、`count` 同样。`avg` 是这 15 行的 `sum` 之和除以 `count` 之和。即时查询在一个求值点上读约 `window / 60` 行。`query_range` 按 `(id, bucket)` 扫过覆盖所有网格窗口的分钟，相邻窗口复用同一行。

窗口不对齐，或步长小于 1 分钟时，仍然扫这张表。边界分钟整行计入，所以实际覆盖比 `(t - window, t]` 稍宽，宽度最多不到 1 分钟。步长不是整分钟（例如 90 秒）时同样处理：步长小于 1 分钟已经要求读 rollup；步长大于 1 分钟但不是整分钟时，窗口与存储分钟对不齐，也走「窗口不对齐」这一条。

`GROUP BY` 之后把结果 `CAST` 成 `Array(Nullable(Float64))`，并按现有规则丢掉 metric name。

建议的滑动聚合是一组新函数，输入已经按分钟聚合好的列，参数仍是网格的 `start`、`end`、`step`、`window`：

```sql
SELECT timeSeriesIdToGroup(id) AS group,
       timeSeriesRollupMaxToGrid(bucket, max)(start, end, step, window) AS values
FROM timeSeriesSelector(db, table, selector, min_time, max_time, 'minute_rollups')
GROUP BY group
```

`min` 读 `min`。`sum` 读 `sum`。`count` 读 `count`。`avg` 读 `sum` 与 `count`。`present_over_time` 在窗口内 `count` 之和大于 0 时为 1。`quantile_over_time` 对相交分钟的 `quantile` 状态做 `quantileTDigestMerge`，再取分位数。

函数与列的对应关系：

| 函数 | 数据源 | 计算 |
| --- | --- | --- |
| `max_over_time` | 分钟 rollup | 相交分钟的 `max` 再取最大 |
| `min_over_time` | 分钟 rollup | 相交分钟的 `min` 再取最小 |
| `sum_over_time` | 分钟 rollup | 相交分钟的 `sum` 相加 |
| `avg_over_time` | 分钟 rollup | 相交分钟的 `sum` 之和 / `count` 之和 |
| `count_over_time` | 分钟 rollup | 相交分钟的 `count` 相加 |
| `present_over_time` | 分钟 rollup | 相交分钟的 `count` 之和大于 0 则为 1 |
| `quantile_over_time` | 分钟 rollup | 相交分钟的 t-digest 状态 `-Merge` |
| `rate`、`deriv` | 窗口对齐且步长 ≥ 1 分钟时读 `samples`；否则读分钟 rollup | 读 `samples` 时保持现有 ToGrid。读 rollup 时表上没有计数器重置和端点，不能从 `max` / `min` / `sum` / `count` 还原 PromQL 的 `rate` / `deriv`。此分支的扫描目标仍是分钟表，网格点结果为空，直到以后为计数器增加可合并状态 |
| `irate`、`increase`、`delta`、`idelta`、`changes`、`resets`、`predict_linear`、`first_over_time`、`last_over_time`、`ts_of_*`、`stddev_over_time`、`stdvar_over_time`、`mad_over_time` | 分钟 rollup | 这些函数不是 `rate` / `deriv`，扫描目标是分钟表。现有列还原不了原始点的顺序、端点时间戳和重置。网格点结果为空，不为它们改读 `samples` |
| `histogram_quantile` | 取决于内层 | 它本身吃的是 instant vector。内层若是对齐且步长 ≥ 1 分钟的 `rate` / `deriv`，内层读 `samples`。内层若是其他函数，或 `rate` / `deriv` 的窗口不对齐、步长小于 1 分钟，内层读分钟 rollup |
| 精确时间戳（`timestamp`） | 分钟 rollup | 不是 `rate` / `deriv`，读分钟表。能返回的时间是 `bucket` 边界，不是原始样本的时间戳 |

空窗口返回 `NULL`，与 ToGrid 在窗口内没有点时一致。

## 设置

- 表设置 `store_minute_rollups`，`Bool`，默认 `false`。为 `true` 时创建内表，`TimeSeriesSink` 写入分钟行。要求 `version >= 12`。值在 `CREATE` 时钉死。已有表没有历史分钟行，创建之后不能再打开。
- 查询设置 `time_series_use_minute_rollups`，`Bool`，默认 `true`。表里有这张内表时，按上一节的路由使用它。设为 `false` 时，所有函数读 `samples`。表没有这张内表时，该设置不改变结果。

这与 `recent_samples_ttl_seconds` 配合 `time_series_prefer_recent_samples_table` 的方式相同。

## 代码落点

- `src/Parsers/Prometheus/TimeSeriesVersion.h`：`LATEST` 升到 12，增加 `MIN_WITH_MINUTE_ROLLUPS`。
- `src/Parsers/ASTViewTargets.h`、`ASTViewTargets.cpp`、`ParserViewTargets.cpp`：`ViewTarget::MinuteRollups` 与关键字 `MINUTE ROLLUPS`。
- `src/Storages/TimeSeries/TimeSeriesSettings.cpp`：`store_minute_rollups`，以及版本、开关的一致性检查。
- `src/Storages/TimeSeries/normalizeTimeSeriesDefinitionImpl.cpp`：生成内表列、引擎、排序键、分区。目标列表今天是 `Samples`、`RecentSamples`、`Tags`、`TimeRanges`、`MetricFamilies`。
- `src/Storages/TimeSeries/TimeSeriesSink.cpp`：在 `sortSamples` 之后切分钟并推入新的 pipeline。`samples` 的小时桶逻辑保持原样。
- `src/Core/Settings.cpp`：`time_series_use_minute_rollups`。
- `src/Storages/StorageTimeSeriesSelector.cpp` 与 `PrometheusQueryToSQL/fromSelector.cpp`：`minute_rollups` 返回模式，id 过滤与时间范围沿用 tags / time ranges。
- `src/Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.cpp`：按本文的真值表选择分钟表或 `samples`，并换成 `timeSeriesRollup*ToGrid`。
- 新的聚合函数：按分钟行做滑动窗口，先合并同一 `bucket` 的部分行，再合并相交分钟。

`samples` 的列、`SimpleAggregateFunction(timeSeriesGroupArray, ...)` 和小时桶都不改。

## 测试

用 `./tests/queries/0_stateless/add-test` 新增 `.sql` 测试，不扩已有测试文件。

- `store_minute_rollups = 1` 时，对齐的 `max_over_time` / `min_over_time` / `count_over_time` 与关掉查询设置时的原始点结果逐值相同。数据包含整分钟边界上的点、边界内 1 tick 的点、stale marker，以及同一时间戳的两个值。
- `sum_over_time` 和 `avg_over_time` 允许相对误差。
- 窗口不对齐、步长 30 秒、步长 90 秒时，`EXPLAIN` 中 `max_over_time` 的读取目标是 `minuterollups`。
- 窗口对齐且步长 1 分钟时，`rate` 和 `deriv` 的读取目标是 `samples`，结果与关掉 rollup 时相同。
- 窗口不对齐或步长小于 1 分钟时，`rate` 和 `deriv` 的读取目标是 `minuterollups`。
- `time_series_use_minute_rollups = 0` 时读取目标回到 `samples`。
- `store_minute_rollups = 0` 时不创建 `minuterollups` 内表。
