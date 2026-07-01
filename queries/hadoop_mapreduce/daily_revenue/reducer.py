#!/usr/bin/env python3
"""
Reducer for the daily revenue MapReduce job.

Hadoop guarantees all values for the same key (pickup_date) arrive at
the same reducer, sorted by key - so we can aggregate with a simple
running total per date as the sorted stream comes in, without loading
the whole dataset into memory.
"""

import sys


def emit(date, count, total_fare, total_tips, total_revenue, total_distance):
    avg_distance = total_distance / count if count else 0.0
    print("{}\t{}\t{:.2f}\t{:.2f}\t{:.2f}\t{:.4f}".format(
        date, count, total_fare, total_tips, total_revenue, avg_distance
    ))


def main():
    current_date = None
    count = 0
    total_fare = total_tips = total_revenue = total_distance = 0.0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        date, values = line.split("\t", 1)
        fare, tip, total, distance = (float(v) for v in values.split(","))

        if date != current_date:
            if current_date is not None:
                emit(current_date, count, total_fare, total_tips, total_revenue, total_distance)
            current_date = date
            count = 0
            total_fare = total_tips = total_revenue = total_distance = 0.0

        count += 1
        total_fare += fare
        total_tips += tip
        total_revenue += total
        total_distance += distance

    if current_date is not None:
        emit(current_date, count, total_fare, total_tips, total_revenue, total_distance)


if __name__ == "__main__":
    main()
