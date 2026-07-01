"""Append-only CSV store for benchmark run results."""

import csv
from datetime import datetime, timezone
from pathlib import Path

RESULTS_CSV = Path(__file__).resolve().parent.parent / "results" / "results.csv"
FIELDS = ["timestamp", "engine", "query", "data_size_mb", "row_count", "elapsed_seconds"]


def record(engine: str, query: str, data_size_mb: float, row_count: int, elapsed_seconds: float):
    RESULTS_CSV.parent.mkdir(parents=True, exist_ok=True)
    is_new = not RESULTS_CSV.exists()
    with open(RESULTS_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if is_new:
            writer.writeheader()
        writer.writerow({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "engine": engine,
            "query": query,
            "data_size_mb": round(data_size_mb, 1),
            "row_count": row_count,
            "elapsed_seconds": round(elapsed_seconds, 2),
        })


def load_all():
    if not RESULTS_CSV.exists():
        return []
    with open(RESULTS_CSV, newline="") as f:
        return list(csv.DictReader(f))
