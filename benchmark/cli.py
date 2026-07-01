"""
Benchmark CLI: run a query against one or all engines and record timing.

Usage:
    python -m benchmark.cli run --query daily_revenue --engine pandas
    python -m benchmark.cli run --query daily_revenue --engine all
    python -m benchmark.cli report
"""

import argparse
import sys
from pathlib import Path

from benchmark import runners, storage

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOCAL_INPUT = PROJECT_ROOT / "data" / "raw" / "trips_2024-01.csv"
LOCAL_ZONES = PROJECT_ROOT / "data" / "raw" / "taxi_zone_lookup.csv"
HDFS_INPUT = "hdfs://namenode:9000/benchmark/input/trips.csv"
HDFS_ZONES = "hdfs://namenode:9000/benchmark/input/zones.csv"
HDFS_OUTPUT_PREFIX = "/benchmark/output"
# path as seen from inside the resourcemanager container (workspace bind mount),
# used for hadoop streaming's -files distributed-cache argument
CONTAINER_ZONES = "/workspace/data/raw/taxi_zone_lookup.csv"

ENGINES = ["pandas", "spark", "hadoop_mapreduce"]
QUERIES_NEEDING_ZONES = {"top_zones"}


def _data_stats():
    size_mb = LOCAL_INPUT.stat().st_size / (1024 * 1024)
    with open(LOCAL_INPUT, encoding="utf-8") as f:
        row_count = sum(1 for _ in f) - 1
    return size_mb, row_count


def run_one(engine: str, query: str):
    size_mb, row_count = _data_stats()
    print(f"[{engine}] running '{query}' ({row_count:,} rows, {size_mb:.1f} MB)...")

    needs_zones = query in QUERIES_NEEDING_ZONES

    if engine == "pandas":
        elapsed = runners.run_pandas(
            query, str(LOCAL_INPUT), str(LOCAL_ZONES) if needs_zones else None
        )
    elif engine == "spark":
        elapsed = runners.run_spark(
            query, HDFS_INPUT, HDFS_ZONES if needs_zones else None
        )
    elif engine == "hadoop_mapreduce":
        elapsed = runners.run_hadoop_mapreduce(
            query, HDFS_INPUT, f"{HDFS_OUTPUT_PREFIX}/{query}",
            CONTAINER_ZONES if needs_zones else None
        )
    else:
        raise ValueError(f"Unknown engine: {engine}")

    storage.record(engine, query, size_mb, row_count, elapsed)
    print(f"[{engine}] done in {elapsed:.2f}s")
    return elapsed


def cmd_run(args):
    engines = ENGINES if args.engine == "all" else [args.engine]
    results = {}
    for engine in engines:
        results[engine] = run_one(engine, args.query)

    print("\n--- summary ---")
    baseline = results.get("pandas")
    for engine, elapsed in results.items():
        note = ""
        if baseline and engine != "pandas":
            ratio = elapsed / baseline
            note = f"  ({ratio:.1f}x {'slower' if ratio > 1 else 'faster'} than pandas)"
        print(f"{engine:>18}: {elapsed:8.2f}s{note}")


def cmd_report(args):
    rows = storage.load_all()
    if not rows:
        print("No results recorded yet. Run `python -m benchmark.cli run` first.")
        return
    for row in rows:
        print(
            f"{row['timestamp']}  {row['engine']:>18}  {row['query']:<20} "
            f"{row['elapsed_seconds']:>8}s  ({row['row_count']} rows)"
        )


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="Run a query against one or all engines")
    run_p.add_argument("--query", required=True)
    run_p.add_argument("--engine", choices=ENGINES + ["all"], default="all")
    run_p.set_defaults(func=cmd_run)

    report_p = sub.add_parser("report", help="Print all recorded results")
    report_p.set_defaults(func=cmd_report)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    sys.exit(main())
