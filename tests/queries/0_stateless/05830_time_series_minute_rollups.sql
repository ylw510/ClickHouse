-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

SET allow_experimental_time_series_table = 1;
SET enable_time_series_aggregate_functions = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_off;
DROP TABLE IF EXISTS ts_on;

CREATE TABLE ts_off ENGINE = TimeSeries SETTINGS store_minute_rollups = 0;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '%.minuterollups.%';

CREATE TABLE ts_on ENGINE = TimeSeries SETTINGS store_minute_rollups = 1;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '%.minuterollups.%';

INSERT INTO ts_off (metric_name, tags, samples) VALUES
    ('cpu', map('job', 'node'), [
        (toDateTime64(0, 3), 100.),
        (toDateTime64(60, 3), 1.),
        (toDateTime64(60.001, 3), 4.),
        (toDateTime64(120, 3), 2.),
        (toDateTime64(120, 3), 8.),
        (toDateTime64(180, 3), reinterpretAsFloat64(reverse(unhex('7FF0000000000002')))),
        (toDateTime64(900, 3), 5.)
    ]);
INSERT INTO ts_on (metric_name, tags, samples) VALUES
    ('cpu', map('job', 'node'), [
        (toDateTime64(0, 3), 100.),
        (toDateTime64(60, 3), 1.),
        (toDateTime64(60.001, 3), 4.),
        (toDateTime64(120, 3), 2.),
        (toDateTime64(120, 3), 8.),
        (toDateTime64(180, 3), reinterpretAsFloat64(reverse(unhex('7FF0000000000002')))),
        (toDateTime64(900, 3), 5.)
    ]);

SELECT 'aligned max min count sum avg';
SELECT count() FROM
(
    SELECT * FROM prometheusQuery(ts_on, 'max_over_time(cpu{job="node"}[15m])', toDateTime64(900, 3))
    EXCEPT
    SELECT * FROM prometheusQuery(ts_off, 'max_over_time(cpu{job="node"}[15m])', toDateTime64(900, 3))
);
SELECT count() FROM
(
    SELECT * FROM prometheusQuery(ts_on, 'min_over_time(cpu{job="node"}[15m])', toDateTime64(900, 3))
    EXCEPT
    SELECT * FROM prometheusQuery(ts_off, 'min_over_time(cpu{job="node"}[15m])', toDateTime64(900, 3))
);
SELECT count() FROM
(
    SELECT * FROM prometheusQuery(ts_on, 'count_over_time(cpu{job="node"}[15m])', toDateTime64(900, 3))
    EXCEPT
    SELECT * FROM prometheusQuery(ts_off, 'count_over_time(cpu{job="node"}[15m])', toDateTime64(900, 3))
);
SELECT count() FROM
(
    SELECT * FROM prometheusQuery(ts_on, 'sum_over_time(cpu{job="node"}[15m])', toDateTime64(900, 3))
    EXCEPT
    SELECT * FROM prometheusQuery(ts_off, 'sum_over_time(cpu{job="node"}[15m])', toDateTime64(900, 3))
);
SELECT count() FROM
(
    SELECT * FROM prometheusQuery(ts_on, 'avg_over_time(cpu{job="node"}[15m])', toDateTime64(900, 3))
    EXCEPT
    SELECT * FROM prometheusQuery(ts_off, 'avg_over_time(cpu{job="node"}[15m])', toDateTime64(900, 3))
);

SELECT 'rollup read vs samples read';
SELECT countIf(explain LIKE '%minuterollups%') > 0 FROM (EXPLAIN SELECT * FROM prometheusQuery(ts_on, 'max_over_time(cpu{job="node"}[15m])', toDateTime64(900, 3)));
SELECT countIf(explain LIKE '%minuterollups%') > 0 FROM (EXPLAIN SELECT * FROM prometheusQuery(ts_on, 'rate(cpu{job="node"}[15m])', toDateTime64(900, 3)));

SELECT 'setting off reads samples';
SELECT countIf(explain LIKE '%minuterollups%') = 0
FROM (EXPLAIN SELECT * FROM prometheusQuery(ts_on, 'max_over_time(cpu{job="node"}[15m])', toDateTime64(900, 3)) SETTINGS time_series_use_minute_rollups = 0);

SELECT 'rate matches samples';
SELECT count() FROM
(
    SELECT * FROM prometheusQuery(ts_on, 'rate(cpu{job="node"}[15m])', toDateTime64(900, 3))
    EXCEPT
    SELECT * FROM prometheusQuery(ts_off, 'rate(cpu{job="node"}[15m])', toDateTime64(900, 3))
);

SELECT 'unaligned max still names the rollup';
SELECT countIf(explain LIKE '%minuterollups%') > 0
FROM (EXPLAIN SELECT * FROM prometheusQuery(ts_on, 'max_over_time(cpu{job="node"}[90s])', toDateTime64(90, 3)));

DROP TABLE ts_off;
DROP TABLE ts_on;
