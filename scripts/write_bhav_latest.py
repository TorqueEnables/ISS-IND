#!/usr/bin/env python3
# Writes data/prices/bhav_latest.csv from bhav_hist.csv
# Usage: python scripts/write_bhav_latest.py --schema normalized|cm

import argparse
import pandas as pd
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument("--hist", default="data/prices/bhav_hist.csv")
p.add_argument("--out",  default="data/prices/bhav_latest.csv")
p.add_argument("--schema", choices=["normalized","cm"], default="normalized")
args = p.parse_args()

hist_path = Path(args.hist)
if not hist_path.exists():
    raise SystemExit(f"Missing {hist_path}")

df = pd.read_csv(hist_path, parse_dates=["Date"])
if df.empty:
    raise SystemExit("bhav_hist.csv is empty; aborting.")

last = df["Date"].max()
snap = df[df["Date"] == last].copy()

norm_cols = ['Date','Symbol','Series','Open','High','Low','Close','PrevClose','Volume','Turnover']
missing = [c for c in norm_cols if c not in snap.columns]
if missing:
    raise SystemExit(f"bhav_hist.csv missing columns: {missing}")

if args.schema == "normalized":
    out = snap[norm_cols].sort_values(['Symbol','Series'])
else:
    # classic cm*bhav style columns
    out = pd.DataFrame({
        'SYMBOL':     snap['Symbol'],
        'SERIES':     snap['Series'],
        'OPEN':       snap['Open'],
        'HIGH':       snap['High'],
        'LOW':        snap['Low'],
        'CLOSE':      snap['Close'],
        'LAST':       snap['Close'],
        'PREVCLOSE':  snap['PrevClose'],
        'TOTTRDQTY':  snap['Volume'],
        'TOTTRDVAL':  snap['Turnover'],
        'TIMESTAMP':  snap['Date'].dt.strftime('%d-%b-%Y').str.upper(),
        'TOTALTRADES': 0,
        'ISIN':        "",
    }).sort_values(['SYMBOL','SERIES'])

out_path = Path(args.out)
out_path.parent.mkdir(parents=True, exist_ok=True)
out.to_csv(out_path, index=False)

print(f"Wrote {out_path} for {last.date()} with {len(out)} rows.")

# Sanity: if normalized, ensure Date matches hist max
if args.schema == "normalized":
    d_latest = pd.read_csv(out_path, parse_dates=["Date"])["Date"].max().date()
    if d_latest != last.date():
        raise SystemExit(f"bhav_latest date {d_latest} != hist max {last.date()}")
    print(f"OK: latest date {d_latest}")
