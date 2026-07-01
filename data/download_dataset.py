"""
Downloads NYC Yellow Taxi trip data (TLC Trip Record Data) and the taxi
zone lookup table, then converts the trip data to CSV so it can be fed
identically into pandas, Hadoop Streaming, and Spark.

Usage:
    python data/download_dataset.py --month 2024-01
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
import requests

TAXI_DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

RAW_DIR = Path(__file__).parent / "raw"

KEEP_COLUMNS = {
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "fare_amount": "fare_amount",
    "tip_amount": "tip_amount",
    "total_amount": "total_amount",
}


def download(url: str, dest: Path):
    if dest.exists():
        print(f"  already have {dest.name}, skipping download")
        return
    print(f"  downloading {url}")
    resp = requests.get(url, stream=True, timeout=60)
    resp.raise_for_status()
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1 << 20):
            f.write(chunk)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", default="2024-01", help="YYYY-MM, e.g. 2024-01")
    args = parser.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = RAW_DIR / f"yellow_tripdata_{args.month}.parquet"
    csv_path = RAW_DIR / f"trips_{args.month}.csv"
    zone_path = RAW_DIR / "taxi_zone_lookup.csv"

    print(f"[1/3] Fetching trip data for {args.month}")
    download(TAXI_DATA_URL.format(month=args.month), parquet_path)

    print("[2/3] Fetching zone lookup table")
    download(ZONE_LOOKUP_URL, zone_path)

    print("[3/3] Converting parquet -> CSV (subset of columns, cleaned)")
    if csv_path.exists():
        print(f"  already have {csv_path.name}, skipping conversion")
    else:
        df = pd.read_parquet(parquet_path, columns=list(KEEP_COLUMNS.keys()))
        df = df.rename(columns=KEEP_COLUMNS)

        before = len(df)
        df = df.dropna(subset=["pickup_datetime", "pu_location_id", "fare_amount"])
        df = df[(df["fare_amount"] > 0) & (df["fare_amount"] < 500)]
        df = df[(df["trip_distance"] > 0) & (df["trip_distance"] < 100)]
        after = len(df)
        print(f"  cleaned {before:,} -> {after:,} rows ({before - after:,} dropped)")

        df["pickup_date"] = df["pickup_datetime"].dt.date.astype(str)
        df.to_csv(csv_path, index=False)

    size_mb = csv_path.stat().st_size / (1024 * 1024)
    row_count = sum(1 for _ in open(csv_path, encoding="utf-8")) - 1
    print(f"\nDone. {csv_path} — {row_count:,} rows, {size_mb:.1f} MB")


if __name__ == "__main__":
    sys.exit(main())
