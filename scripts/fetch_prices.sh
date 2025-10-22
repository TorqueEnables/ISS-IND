#!/usr/bin/env bash
set -euo pipefail

mkdir -p data/prices

found=""
for i in 1 2 3 4 5; do
  d=$(date -u -d "$i day ago" +%d%m%Y)
  url="https://archives.nseindia.com/products/content/sec_bhavdata_full_${d}.csv"
  echo "Trying $url"
  if curl -f -sS "$url" -o "/tmp/bhav_${d}.csv"; then
    echo "Downloaded ${url}"
    found="$d"
    cp "/tmp/bhav_${d}.csv" "data/prices/bhav_${d}.csv"
    cp "/tmp/bhav_${d}.csv" "data/prices/bhav_latest.csv"
    break
  fi
done

if [ -z "$found" ]; then
  echo "Could not find a bhavcopy in the last 5 days." >&2
  exit 1
fi
