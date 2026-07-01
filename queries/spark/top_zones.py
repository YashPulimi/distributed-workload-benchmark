"""
Spark implementation of the top pickup zones query — trips joined
against the zone lookup table. The lookup table is a few hundred KB,
far under Spark's broadcast threshold, so we broadcast it explicitly
rather than relying on autodetection - this turns the join into a
map-side hash join with no shuffle for the trips side.
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

ZONES_SCHEMA = StructType([
    StructField("LocationID", IntegerType()),
    StructField("Borough", StringType()),
    StructField("Zone", StringType()),
    StructField("service_zone", StringType()),
])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="HDFS path to trips CSV")
    parser.add_argument("--zones", required=True, help="HDFS path to zone lookup CSV")
    parser.add_argument("--top", type=int, default=15)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("top_zones").getOrCreate()

    trips = spark.read.csv(args.input, header=True, schema=TRIPS_SCHEMA)
    zones = spark.read.csv(args.zones, header=True, schema=ZONES_SCHEMA)

    joined = trips.join(F.broadcast(zones), trips.pu_location_id == zones.LocationID, "left")

    result = (
        joined.groupBy("Zone")
        .agg(
            F.count("fare_amount").alias("trip_count"),
            F.sum("total_amount").alias("total_revenue"),
        )
        .orderBy(F.desc("trip_count"))
        .limit(args.top)
    )

    for r in result.collect():
        print(f"{r['Zone']:>30} {r['trip_count']:>8} {r['total_revenue']:>14.2f}")

    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
