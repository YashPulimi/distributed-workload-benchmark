"""
Generates a bar chart comparing median wall-clock time per engine per
query, annotated with speedup relative to the pandas baseline.

Usage:
    python -m benchmark.chart
"""

import statistics
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from benchmark import storage

ENGINE_LABELS = {
    "pandas": "Pandas\n(single machine)",
    "spark": "Spark\n(standalone cluster)",
    "hadoop_mapreduce": "Hadoop MapReduce\n(YARN, disk shuffle)",
}
ENGINE_ORDER = ["pandas", "spark", "hadoop_mapreduce"]
ENGINE_COLORS = {"pandas": "#4f8cff", "spark": "#e8622c", "hadoop_mapreduce": "#3ddc97"}

PLOTS_DIR = Path(__file__).resolve().parent.parent / "results" / "plots"


def medians_by_engine_query(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row["query"], row["engine"])].append(float(row["elapsed_seconds"]))
    medians = {key: statistics.median(values) for key, values in grouped.items()}
    counts = {key: len(values) for key, values in grouped.items()}
    return medians, counts


def plot_query(query: str, medians: dict, counts: dict):
    engines = [e for e in ENGINE_ORDER if (query, e) in medians]
    if not engines:
        return
    times = [medians[(query, e)] for e in engines]
    baseline = medians.get((query, "pandas"))
    run_counts = sorted({counts[(query, e)] for e in engines})
    n_label = f"n={run_counts[0]}" if len(run_counts) == 1 else f"n={min(run_counts)}-{max(run_counts)}"

    fig, ax = plt.subplots(figsize=(7, 5))
    bars = ax.bar(
        [ENGINE_LABELS[e] for e in engines],
        times,
        color=[ENGINE_COLORS[e] for e in engines],
    )

    for bar, engine, t in zip(bars, engines, times):
        label = f"{t:.1f}s"
        if baseline and engine != "pandas":
            ratio = t / baseline
            label += f"\n({ratio:.1f}x pandas)"
        ax.text(
            bar.get_x() + bar.get_width() / 2, bar.get_height(),
            label, ha="center", va="bottom", fontsize=10,
        )

    ax.set_ylabel("Median wall-clock time (seconds)")
    ax.set_title(f"Engine comparison — {query}\n(median per engine, {n_label} runs, single-node cluster)")
    ax.set_ylim(0, max(times) * 1.25)
    fig.tight_layout()

    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = PLOTS_DIR / f"{query}_comparison.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"wrote {out_path}")


def main():
    rows = storage.load_all()
    if not rows:
        print("No results recorded yet. Run `python -m benchmark.cli run` first.")
        return

    medians, counts = medians_by_engine_query(rows)
    queries = sorted({q for q, _ in medians})
    for query in queries:
        plot_query(query, medians, counts)


if __name__ == "__main__":
    sys.exit(main())
