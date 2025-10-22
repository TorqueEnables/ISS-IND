#!/usr/bin/env bash
set -euo pipefail

# Build a 90-day rolling bhavcopy by concatenating dated files we already store.
out="data/prices/bhav_last_90d.csv"
tmp="/tmp/bhav_last_90d.csv"
: > "$tmp"

header_written=0
# Walk back ~140 calendar days to safely cover 90 trading days (holidays/weekends).
for i in $(seq 1 140); do
  d=$(date -u -d "$i day ago" +%d%m%Y)
  f="data/prices/bhav_${d}.csv"
  if [[ -f "$f" ]]; then
    if [[ $header_written -eq 0 ]]; then
      head -n 1 "$f" >> "$tmp"
      header_written=1
    fi
    tail -n +2 "$f" >> "$tmp"
  fi
done

# Only replace if we actually assembled something.
if [[ $header_written -eq 1 ]]; then
  mv "$tmp" "$out"
  echo "Built $out"
else
  echo "No historical bhav files found to assemble." >&2
  exit 1
fi
