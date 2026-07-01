#!/usr/bin/env python3
"""
Reducer for the top pickup zones MapReduce job.

Emits total trip count and revenue per zone. MapReduce has no cheap
built-in "top-N" primitive, so ranking/truncation happens as a tiny
post-processing step over this reducer's output (at most ~265 zones)
rather than a second MapReduce job.
"""

import sys


def emit(zone, count, total_revenue):
    print("{}\t{}\t{:.2f}".format(zone, count, total_revenue))


def main():
    current_zone = None
    count = 0
    total_revenue = 0.0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        zone, value = line.split("\t", 1)
        _one, total_amount = value.split(",")
        total_amount = float(total_amount)

        if zone != current_zone:
            if current_zone is not None:
                emit(current_zone, count, total_revenue)
            current_zone = zone
            count = 0
            total_revenue = 0.0

        count += 1
        total_revenue += total_amount

    if current_zone is not None:
        emit(current_zone, count, total_revenue)


if __name__ == "__main__":
    main()
