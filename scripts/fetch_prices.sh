#!/usr/bin/env bash
set -euo pipefail

# Ensure date math is in IST so "1 day ago" matches India market day
export TZ="Asia/Kolkata"

mkdir -p data/prices

found=""
for i in 1 2 3 4 5; do
  d=$(date -d "$i day ago" +%d%m%Y)
  url="https://archives.nseindia.com/products/content/sec_bhavdata_full_${d}.csv"
  echo "Trying $url"
  if curl -f -sS -L --retry 5 --retry-delay 2 \
        -A "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36" \
        "$url" -o "/tmp/bhav_${d}.csv"; then
    echo "Downloaded ${url}"
    found="$d"
    # keep a dated copy and refresh the rolling latest
    install -m 0644 "/tmp/bhav_${d}.csv" "data/prices/bhav_${d}.csv"
    cp "data/prices/bhav_${d}.csv" "data/prices/bhav_latest.csv"
    break
  fi
done

if [ -z "$found" ]; then
  echo "Could not find a bhavcopy in the last 5 days." >&2
  exit 1
fi
