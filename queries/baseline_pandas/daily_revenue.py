"""
Baseline: single-machine pandas implementation of the daily revenue query.

For each pickup date: trip count, total fare revenue, total tips,
total revenue, and average trip distance.
"""

import argparse
import sys

import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to trips CSV")
    args = parser.parse_args()

    df = pd.read_csv(
        args.input,
        usecols=["pickup_date", "fare_amount", "tip_amount", "total_amount", "trip_distance"],
    )

    result = (
        df.groupby("pickup_date")
        .agg(
            trip_count=("fare_amount", "size"),
            total_fare=("fare_amount", "sum"),
            total_tips=("tip_amount", "sum"),
            total_revenue=("total_amount", "sum"),
            avg_distance=("trip_distance", "mean"),
        )
        .reset_index()
        .sort_values("pickup_date")
    )

    print(result.to_string(index=False))
    print(f"\n{len(result)} distinct dates, {len(df):,} total trips")


if __name__ == "__main__":
    sys.exit(main())
