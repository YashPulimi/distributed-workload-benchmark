#!/usr/bin/env python3
"""
Mapper for the top pickup zones MapReduce job.

Implements a map-side (replicated) join: the zone lookup table is tiny
(~12KB) so it's shipped to every mapper via the distributed cache and
loaded into memory once, instead of paying for a shuffle-based join
against the multi-hundred-MB trips file.
"""

import csv
import sys

HEADER_PREFIX = "pickup_datetime,"


def load_zone_lookup(path="zones.csv"):
    lookup = {}
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            lookup[row["LocationID"]] = row["Zone"]
    return lookup


def main():
    zones = load_zone_lookup()

    for line in sys.stdin:
        line = line.strip()
        if not line or line.startswith(HEADER_PREFIX):
            continue

        fields = line.split(",")
        if len(fields) != 10:
            continue

        pu_location_id = fields[2]
        total_amount = fields[8]

        zone = zones.get(pu_location_id)
        if zone is None:
            continue

        try:
            float(total_amount)
        except ValueError:
            continue

        print("{}\t1,{}".format(zone, total_amount))


if __name__ == "__main__":
    main()
