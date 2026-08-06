#!/usr/bin/env bash
# Tags: no-fasttest

# Phase 2 export strategy: ClickHouse JSON continues to export as canonical
# `arrow.json` (not Spark `arrow.parquet.variant`). Variant binary import is
# covered by unit tests (gtest_parquet_variant_to_json).

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ARROW_FILE="${CLICKHOUSE_TMP}/arrow_json_not_variant.arrow"

$CLICKHOUSE_LOCAL -n > "${ARROW_FILE}" <<'SQL'
CREATE TABLE example
(
    id Int32,
    json_val JSON
) ENGINE = Memory;

INSERT INTO example VALUES (1, '{"a":1}');

SELECT * FROM example FORMAT Arrow;
SQL

python3 - <<PY
import struct
path = "${ARROW_FILE}"
data = open(path, "rb").read()
assert data[:6] == b"ARROW1", data[:16]
footer_len = struct.unpack_from("<i", data, len(data) - 10)[0]
footer = data[-(10 + footer_len):-10]
assert b"arrow.json" in footer, "expected arrow.json extension metadata in Arrow footer"
assert b"arrow.parquet.variant" not in footer, "JSON export must not use parquet variant"
assert b"parquet.variant" not in footer, "JSON export must not use parquet variant"
print("export_arrow_json_ok")
PY

$CLICKHOUSE_LOCAL -q "
    SET input_format_arrow_enable_json_parsing = 1;
    SELECT id, toJSONString(json_val) FROM file('${ARROW_FILE}', Arrow)
    ORDER BY id FORMAT TSV"
