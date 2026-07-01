#!/usr/bin/env python3
"""
Mapper for the daily revenue MapReduce job.

Reads trip CSV rows from stdin, emits one line per trip:
    pickup_date \t fare_amount,tip_amount,total_amount,trip_distance

Hadoop Streaming feeds each input split to a separate mapper process,
so this only ever sees a slice of the file - it has no visibility into
totals, unlike the pandas/Spark versions which can see the whole frame.
"""

import sys

HEADER_PREFIX = "pickup_datetime,"


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line or line.startswith(HEADER_PREFIX):
            continue  # skip the CSV header line (only present in the first split)

        fields = line.split(",")
        if len(fields) != 10:
            continue  # malformed row, skip

        (
            _pickup_datetime, _dropoff_datetime, _pu, _do,
            _passengers, trip_distance, fare_amount, tip_amount,
            total_amount, pickup_date,
        ) = fields

        try:
            float(fare_amount)
            float(tip_amount)
            float(total_amount)
            float(trip_distance)
        except ValueError:
            continue

        print("{}\t{},{},{},{}".format(pickup_date, fare_amount, tip_amount, total_amount, trip_distance))


if __name__ == "__main__":
    main()
