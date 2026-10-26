-- SAMPLE for table functions and files (https://github.com/ClickHouse/ClickHouse/issues/126468).
-- Row-level Bernoulli on numbers()/CSV; Parquet additionally samples whole row groups.

SELECT count() FROM numbers(1000) SAMPLE 1;
SELECT count() FROM numbers(1000) SAMPLE 0;
SELECT count() BETWEEN 1 AND 9999 FROM numbers(10000) SAMPLE 0.1;
SELECT count() BETWEEN 1 AND 9999 FROM numbers(10000) SAMPLE 1/10;
SELECT count() BETWEEN 1 AND 9999 FROM numbers(10000) SAMPLE 0.5 OFFSET 0.5;
SELECT count() FROM numbers(10) SAMPLE 100; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT count() FROM numbers(10) SAMPLE 1 OFFSET 0.1; -- { serverError ARGUMENT_OUT_OF_BOUND }

DROP TABLE IF EXISTS t_05429_mt;
CREATE TABLE t_05429_mt (x UInt64) ENGINE = MergeTree ORDER BY x;
SELECT count() FROM t_05429_mt SAMPLE 0.1; -- { serverError SAMPLING_NOT_SUPPORTED }
DROP TABLE t_05429_mt;

INSERT INTO FUNCTION file(currentDatabase() || '_05429.csv', CSV, 'n UInt64')
SELECT number FROM numbers(10000)
SETTINGS engine_file_truncate_on_insert = 1;

SELECT count() FROM file(currentDatabase() || '_05429.csv', CSV, 'n UInt64') SAMPLE 1;
SELECT count() BETWEEN 1 AND 9999 FROM file(currentDatabase() || '_05429.csv', CSV, 'n UInt64') SAMPLE 0.1;

INSERT INTO FUNCTION file(currentDatabase() || '_05429.parquet', Parquet, 'n UInt64')
SELECT number FROM numbers(100000)
SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;

SELECT count() FROM file(currentDatabase() || '_05429.parquet', Parquet) SAMPLE 1;
SELECT count() BETWEEN 1 AND 99999 FROM file(currentDatabase() || '_05429.parquet', Parquet) SAMPLE 0.1;
SELECT count() BETWEEN 1 AND 99999 FROM file(currentDatabase() || '_05429.parquet', Parquet) SAMPLE 0.5 OFFSET 0.5;
