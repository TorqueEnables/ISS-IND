#!/usr/bin/env python3
"""
Generate a clean weekly-style brief from out/WEEK_PACK.csv.
Neutral tone, crisp reasoning, no fluff.
"""

import argparse, textwrap
from pathlib import Path
import pandas as pd
from datetime import datetime

p = argparse.ArgumentParser()
p.add_argument("--pack", required=True)
p.add_argument("--out", required=True)
args = p.parse_args()

pack_path = Path(args.pack)
if not pack_path.exists():
    raise SystemExit(f"Missing {pack_path}")

pack = pd.read_csv(pack_path)
if pack.empty:
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("# Weekly Brief\n\n_No eligible candidates today._\n", encoding="utf-8")
    print("Pack empty; wrote placeholder brief.")
    raise SystemExit(0)

# Determine as-of
as_of = None
if "AsOf" in pack.columns:
    try: as_of = pd.to_datetime(pack["AsOf"].iloc[0]).date()
    except: pass
as_of = as_of or datetime.now().date()

# Take top 8 for readability
sub = pack.sort_values("R_SCORE", ascending=False).head(8).copy()

def fmtline(r):
    why = r.get("WHY","")
    fam = r.get("SETUP_FAMILY","")
    entry = r.get("Entry","")
    sl    = r.get("SL","")
    parts = [
        f"**{r['Symbol']}** — score {r['R_SCORE']}, last close {r['Close']}.",
        f"Plan: Entry {entry}, Stop {sl}.",
    ]
    if fam: parts.append(f"Setup: {fam}.")
    if why: parts.append(f"Signals: {why}.")
    return " ".join(parts)

lines = []
lines.append(f"# StakeLens Insider — Weekly Brief (auto-updated)")
lines.append(f"**As of:** {as_of}\n")

lines.append("**How to use this brief:**")
lines.append(textwrap.fill(
    "This list is refreshed after each new upload. Focus on the listed entries and stops as a starting plan. "
    "Respect risk limits: size positions so a stop-out costs a small, fixed share of capital. "
    "Avoid chasing extended gaps; prefer controlled entries around the stated triggers.",
    width=96
))
lines.append("")

lines.append("## Top candidates")
for _, r in sub.iterrows():
    lines.append(f"- {fmtline(r)}")
lines.append("")

lines.append("## Notes on selection")
lines.append(textwrap.fill(
    "Names pass strict universe and liquidity gates, then rank on relative strength, power signals "
    "(pocket pivots or valid breakouts), squeeze conditions, moving-average structure, and turnover quality. "
    "Illiquid tickers and ETFs are excluded. Deliverable value is used when available; turnover floors are "
    "always enforced.",
    width=96
))
lines.append("")

# Save
out_path = Path(args.out)
out_path.parent.mkdir(parents=True, exist_ok=True)
out_path.write_text("\n".join(lines), encoding="utf-8")
print(f"Wrote {out_path} ({len(sub)} items).")
