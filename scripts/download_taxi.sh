#!/usr/bin/env bash
# Download one year of NYC yellow taxi trips (one Parquet file per month) and the zone table.
# Safe to run again: finished files are skipped, broken downloads continue where they stopped.
set -euo pipefail

YEAR="${1:-2024}"
BASE="https://d37ci6vzurychx.cloudfront.net"
OUT="data/taxi/raw"
mkdir -p "$OUT"

for MONTH in 01 02 03 04 05 06 07 08 09 10 11 12; do
    FILE="yellow_tripdata_${YEAR}-${MONTH}.parquet"
    echo "downloading $FILE"
    curl -fL --retry 3 -C - -o "$OUT/$FILE" "$BASE/trip-data/$FILE"
done

curl -fL --retry 3 -o "$OUT/taxi_zone_lookup.csv" "$BASE/misc/taxi_zone_lookup.csv"
echo "done"
ls -lh "$OUT"