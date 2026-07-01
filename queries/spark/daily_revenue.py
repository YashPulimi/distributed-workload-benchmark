"""
Spark implementation of the daily revenue query — same logic as
baseline_pandas/daily_revenue.py, run as a distributed DataFrame job
against the data in HDFS.
"""

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

TRIPS_SCHEMA = StructType([
    StructField("pickup_datetime", StringType()),
    StructField("dropoff_datetime", StringType()),
    StructField("pu_location_id", IntegerType()),
    StructField("do_location_id", IntegerType()),
    StructField("passenger_count", DoubleType()),
    StructField("trip_distance", DoubleType()),
    StructField("fare_amount", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("pickup_date", StringType()),
])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="HDFS path to trips CSV")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("daily_revenue").getOrCreate()

    # Explicit schema avoids Spark doing a full extra pass over the file
    # to infer types, which otherwise roughly doubles I/O for this job.
    df = spark.read.csv(args.input, header=True, schema=TRIPS_SCHEMA)

    result = (
        df.groupBy("pickup_date")
        .agg(
            F.count("fare_amount").alias("trip_count"),
            F.sum("fare_amount").alias("total_fare"),
            F.sum("tip_amount").alias("total_tips"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("trip_distance").alias("avg_distance"),
        )
        .orderBy("pickup_date")
    )

    rows = result.collect()
    for r in rows:
        print(
            f"{r['pickup_date']:>12} {r['trip_count']:>8} "
            f"{r['total_fare']:>12.2f} {r['total_tips']:>10.2f} "
            f"{r['total_revenue']:>12.2f} {r['avg_distance']:>8.4f}"
        )

    total_trips = df.count()
    print(f"\n{len(rows)} distinct dates, {total_trips:,} total trips")

    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
