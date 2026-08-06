#!/usr/bin/env bash
# Tags: no-fasttest

# Round-trip ClickHouse JSON columns through Arrow / ArrowStream with the
# canonical `arrow.json` extension metadata (native ArrowIPC writer/reader).

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ORIG="${CLICKHOUSE_TMP}/arrow_json_orig.tsv"
BACK_ARROW="${CLICKHOUSE_TMP}/arrow_json_back_arrow.tsv"
BACK_STREAM="${CLICKHOUSE_TMP}/arrow_json_back_stream.tsv"
ARROW_FILE="${CLICKHOUSE_TMP}/arrow_json.roundtrip.arrow"
STREAM_FILE="${CLICKHOUSE_TMP}/arrow_json.roundtrip.arrows"

$CLICKHOUSE_LOCAL -n > "${ORIG}" <<'SQL'
CREATE TABLE example
(
    id Int32,
    json_val JSON
) ENGINE = Memory;

INSERT INTO example VALUES
    (1, '{"user":"alice","age":30}'),
    (2, '{"items":[1,2,3],"flag":true}'),
    (3, NULL);

SELECT id, toJSONString(json_val) FROM example ORDER BY id FORMAT TSV;
SQL

$CLICKHOUSE_LOCAL -n > "${ARROW_FILE}" <<'SQL'
CREATE TABLE example
(
    id Int32,
    json_val JSON
) ENGINE = Memory;

INSERT INTO example VALUES
    (1, '{"user":"alice","age":30}'),
    (2, '{"items":[1,2,3],"flag":true}'),
    (3, NULL);

SELECT * FROM example ORDER BY id FORMAT Arrow;
SQL

$CLICKHOUSE_LOCAL -q "
    SET input_format_arrow_enable_json_parsing = 1;
    SELECT id, toJSONString(json_val) FROM file('${ARROW_FILE}', Arrow)
    ORDER BY id FORMAT TSV" > "${BACK_ARROW}"

echo "diff_arrow_native:"
diff "${ORIG}" "${BACK_ARROW}"

$CLICKHOUSE_LOCAL -n > "${STREAM_FILE}" <<'SQL'
CREATE TABLE example
(
    id Int32,
    json_val JSON
) ENGINE = Memory;

INSERT INTO example VALUES
    (1, '{"user":"alice","age":30}'),
    (2, '{"items":[1,2,3],"flag":true}'),
    (3, NULL);

SELECT * FROM example ORDER BY id FORMAT ArrowStream;
SQL

$CLICKHOUSE_LOCAL -q "
    SET input_format_arrow_enable_json_parsing = 1;
    SELECT id, toJSONString(json_val) FROM file('${STREAM_FILE}', ArrowStream)
    ORDER BY id FORMAT TSV" > "${BACK_STREAM}"

echo "diff_arrow_stream:"
diff "${ORIG}" "${BACK_STREAM}"

$CLICKHOUSE_LOCAL -q "
    SET input_format_arrow_enable_json_parsing = 1;
    SELECT toTypeName(json_val) FROM file('${ARROW_FILE}', Arrow) LIMIT 1;"

$CLICKHOUSE_LOCAL -q "
    SET input_format_arrow_enable_json_parsing = 0;
    SELECT toTypeName(json_val) FROM file('${ARROW_FILE}', Arrow) LIMIT 1;"
