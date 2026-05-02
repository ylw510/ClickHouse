#!/usr/bin/env python3
# =============================================================================
# Stress test for StorageBuffer under concurrency (MultiVersion snapshot reads).
#
# Requirements:
#   pip install clickhouse-connect
#
# Use one clickhouse_connect client per thread — sharing one session causes:
#   ProgrammingError: concurrent queries same session
#
# Example (local server):
#   python3 stress_buf_snap_concurrent.py --duration 30 --writers 4 --readers 8
# =============================================================================

from __future__ import annotations

import argparse
import random
import sys
import threading
import time
import traceback


class StressStats:
    """Thread-safe counters for throughput and latency summaries."""

    __slots__ = ("_lock", "rows_inserted", "insert_batches", "insert_time_sec", "queries", "query_time_sec", "optimize_calls", "optimize_time_sec")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.rows_inserted = 0
        self.insert_batches = 0
        self.insert_time_sec = 0.0
        self.queries = 0
        self.query_time_sec = 0.0
        self.optimize_calls = 0
        self.optimize_time_sec = 0.0

    def add_insert(self, rows: int, elapsed_sec: float) -> None:
        with self._lock:
            self.rows_inserted += rows
            self.insert_batches += 1
            self.insert_time_sec += elapsed_sec

    def add_query(self, elapsed_sec: float) -> None:
        with self._lock:
            self.queries += 1
            self.query_time_sec += elapsed_sec

    def add_optimize(self, elapsed_sec: float) -> None:
        with self._lock:
            self.optimize_calls += 1
            self.optimize_time_sec += elapsed_sec


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Concurrent INSERT/SELECT/OPTIMIZE against a Buffer table.")
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=8123)
    p.add_argument("--username", default="default")
    p.add_argument("--password", default="")
    p.add_argument("--secure", action="store_true", help="Use TLS (native protocol).")
    p.add_argument("--database", default="default")
    p.add_argument("--dest-table", default="stress_buf_dest", help="MergeTree destination table name.")
    p.add_argument("--buffer-table", default="stress_buf_layer", help="Buffer engine table name.")
    p.add_argument("--duration", type=float, default=20.0, help="Run time in seconds.")
    p.add_argument("--writers", type=int, default=4)
    p.add_argument("--readers", type=int, default=8)
    p.add_argument("--optimize-threads", type=int, default=1, help="OPTIMIZE TABLE buffer threads (0 to disable).")
    p.add_argument("--batch-rows", type=int, default=500, help="Rows per INSERT batch.")
    p.add_argument("--no-setup", action="store_true", help="Skip CREATE TABLE (tables must exist).")
    return p.parse_args()


def get_client(args: argparse.Namespace):
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=args.host,
        port=args.port,
        username=args.username,
        password=args.password,
        secure=args.secure,
        database=args.database,
    )


def setup_schema(client, args: argparse.Namespace) -> None:
    db = args.database
    dest = args.dest_table
    buf = args.buffer_table
    # Small thresholds so background flush and races trigger often during stress.
    # Buffer(db, table, num_buckets, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes[, flush_time, flush_rows, flush_bytes])
    ddl_dest = f"""
        CREATE TABLE IF NOT EXISTS `{db}`.`{dest}` (
            id UInt64,
            s String
        ) ENGINE = MergeTree ORDER BY id
    """
    # Same pattern as tests/queries/0_stateless/03094_virtual_column_table_name.sql:
    # CREATE TABLE buf AS dest ENGINE = Buffer(database, dest_table, ...)
    ddl_buf = f"""
        CREATE TABLE IF NOT EXISTS `{db}`.`{buf}` AS `{db}`.`{dest}`
        ENGINE = Buffer(
            `{db}`, `{dest}`, 16,
            1, 60,
            100, 100000,
            1000000, 1000000000,
            1, 50, 10000000
        )
    """
    client.command(ddl_dest)
    client.command(ddl_buf)


def writer_loop(wid: int, args: argparse.Namespace, stop: threading.Event, errs: list, stats: StressStats) -> None:
    try:
        c = get_client(args)
        buf = f"{args.database}.{args.buffer_table}"
        n = 0
        while not stop.is_set():
            batch = args.batch_rows
            base = (int(time.time() * 1e9) ^ wid) & 0xFFFFFFFFFFFFFFFF
            rows = []
            for i in range(batch):
                rid = base + n * batch + i
                rows.append([rid, "x" * (8 + (rid % 40))])
            n += 1
            t0 = time.perf_counter()
            c.insert(buf, rows, column_names=["id", "s"])
            stats.add_insert(batch, time.perf_counter() - t0)
    except Exception:
        errs.append(traceback.format_exc())


def reader_loop(rid: int, args: argparse.Namespace, stop: threading.Event, errs: list, stats: StressStats) -> None:
    try:
        c = get_client(args)
        buf = f"{args.database}.{args.buffer_table}"
        queries = [
            f"SELECT count() FROM {buf}",
            f"SELECT sum(length(s)) FROM {buf}",
            f"SELECT min(id), max(id) FROM {buf}",
            f"SELECT uniqExact(id) FROM {buf}",
        ]
        while not stop.is_set():
            q = random.choice(queries)
            t0 = time.perf_counter()
            c.query(q)
            stats.add_query(time.perf_counter() - t0)
    except Exception:
        errs.append(traceback.format_exc())


def optimize_loop(args: argparse.Namespace, stop: threading.Event, errs: list, stats: StressStats) -> None:
    try:
        c = get_client(args)
        buf = f"{args.database}.{args.buffer_table}"
        while not stop.is_set():
            t0 = time.perf_counter()
            c.command(f"OPTIMIZE TABLE {buf}")
            stats.add_optimize(time.perf_counter() - t0)
            time.sleep(0.2)
    except Exception:
        errs.append(traceback.format_exc())


def fetch_destination_row_count(args: argparse.Namespace):
    """Best-effort server-side row count on the MergeTree destination (may include prior runs)."""
    try:
        c = get_client(args)
        db = args.database
        dest = args.dest_table
        res = c.query(f"SELECT count() FROM `{db}`.`{dest}`")
        return int(res.result_rows[0][0])
    except Exception:
        return None


def print_report(stats: StressStats, wall_sec: float, args: argparse.Namespace) -> None:
    """Print totals and averages; throughput uses wall-clock duration of the stress phase."""

    ins_rows = stats.rows_inserted
    ins_batches = stats.insert_batches
    ins_t = stats.insert_time_sec
    q_n = stats.queries
    q_t = stats.query_time_sec
    opt_n = stats.optimize_calls
    opt_t = stats.optimize_time_sec

    print("", flush=True)
    print("=== stress summary ===", flush=True)
    print(f"  wall_time_sec:        {wall_sec:.3f}", flush=True)
    print(f"  writers / readers:   {args.writers} / {args.readers}", flush=True)
    print(f"  optimize_threads:    {args.optimize_threads}", flush=True)
    print("", flush=True)
    print("  INSERT:", flush=True)
    print(f"    rows_inserted:      {ins_rows}", flush=True)
    print(f"    batches:            {ins_batches}", flush=True)
    if ins_batches:
        print(f"    avg_batch_ms:       {1000.0 * ins_t / ins_batches:.3f}", flush=True)
    if wall_sec > 0:
        print(f"    rows_per_sec:       {ins_rows / wall_sec:.1f}", flush=True)
        print(f"    batches_per_sec:    {ins_batches / wall_sec:.2f}", flush=True)

    print("", flush=True)
    print("  SELECT (queries):", flush=True)
    print(f"    query_count:        {q_n}", flush=True)
    if q_n:
        print(f"    avg_latency_ms:     {1000.0 * q_t / q_n:.3f}", flush=True)
        print(f"    sum_query_time_sec: {q_t:.3f}", flush=True)
    if wall_sec > 0:
        print(f"    queries_per_sec:    {q_n / wall_sec:.1f}", flush=True)

    print("", flush=True)
    print("  OPTIMIZE:", flush=True)
    print(f"    calls:              {opt_n}", flush=True)
    if opt_n:
        print(f"    avg_latency_ms:     {1000.0 * opt_t / opt_n:.3f}", flush=True)
    if wall_sec > 0 and opt_n:
        print(f"    optimize_per_sec:   {opt_n / wall_sec:.2f}", flush=True)

    srv = fetch_destination_row_count(args)
    print("", flush=True)
    print("  Server (MergeTree destination):", flush=True)
    if srv is not None:
        print(f"    dest_row_count:       {srv}  (includes any data already in dest before this run)", flush=True)
    else:
        print("    dest_row_count:       (query failed — check table exists and permissions)", flush=True)

    print("", flush=True)


def main() -> int:
    args = parse_args()
    try:
        import clickhouse_connect as _cc  # noqa: F401
    except ImportError:
        print("Install: pip install clickhouse-connect", file=sys.stderr)
        return 1

    errs: list = []
    stats = StressStats()
    admin = get_client(args)
    if not args.no_setup:
        setup_schema(admin, args)

    stop = threading.Event()
    threads: list[threading.Thread] = []

    for i in range(args.writers):
        t = threading.Thread(target=writer_loop, args=(i, args, stop, errs, stats), daemon=True)
        threads.append(t)
    for i in range(args.readers):
        t = threading.Thread(target=reader_loop, args=(i, args, stop, errs, stats), daemon=True)
        threads.append(t)
    for i in range(args.optimize_threads):
        t = threading.Thread(target=optimize_loop, args=(args, stop, errs, stats), daemon=True)
        threads.append(t)

    print(
        f"Starting stress: duration={args.duration}s writers={args.writers} "
        f"readers={args.readers} optimize_threads={args.optimize_threads} "
        f"db={args.database} buffer={args.buffer_table}",
        flush=True,
    )
    for t in threads:
        t.start()

    wall_begin = time.monotonic()
    try:
        while time.monotonic() - wall_begin < args.duration:
            time.sleep(0.5)
            if errs:
                break
    finally:
        stop.set()

    for t in threads:
        t.join(timeout=10.0)

    wall_sec = time.monotonic() - wall_begin

    if errs:
        print("FAIL: exceptions in worker threads:", file=sys.stderr)
        for e in errs:
            print(e, file=sys.stderr)
        print_report(stats, wall_sec, args)
        return 2

    print("OK: completed without worker exceptions.", flush=True)
    print_report(stats, wall_sec, args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
