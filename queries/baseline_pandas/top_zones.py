"""
Baseline: pandas implementation of the top pickup zones query.

Joins trips against the (small) zone lookup table, then ranks zones by
trip count and total revenue.
"""

import argparse
import sys
from pathlib import Path

import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to trips CSV")
    parser.add_argument("--zones", required=True, help="Path to zone lookup CSV")
    parser.add_argument("--top", type=int, default=15)
    args = parser.parse_args()

    trips = pd.read_csv(args.input, usecols=["pu_location_id", "fare_amount", "total_amount"])
    zones = pd.read_csv(args.zones)

    merged = trips.merge(zones, left_on="pu_location_id", right_on="LocationID", how="left")

    result = (
        merged.groupby("Zone")
        .agg(trip_count=("fare_amount", "size"), total_revenue=("total_amount", "sum"))
        .reset_index()
        .sort_values("trip_count", ascending=False)
        .head(args.top)
    )

    print(result.to_string(index=False))


if __name__ == "__main__":
    sys.exit(main())
