#!/usr/bin/env python3
"""
Weekly brief generator — neutral tone, shows GO section first, then Watchlist.
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
out_path = Path(args.out)
out_path.parent.mkdir(parents=True, exist_ok=True)

if pack.empty:
    out_path.write_text("# Weekly Brief\n\n_No eligible candidates today._\n", encoding="utf-8")
    print("Pack empty; wrote placeholder brief.")
    raise SystemExit(0)

as_of = None
if "AsOf" in pack.columns:
    try: as_of = pd.to_datetime(pack["AsOf"].iloc[0]).date()
    except: pass
as_of = as_of or datetime.now().date()

# Split GO and WATCH
pack["GO"] = pack["GO"].astype(bool) if "GO" in pack.columns else False
go_df = pack[pack["GO"]].sort_values("R_SCORE", ascending=False)
wt_df = pack[~pack["GO"]].sort_values("R_SCORE", ascending=False).head(8)

def fline(r):
    parts = [
        f"**{r['Symbol']}** — score {r['R_SCORE']}, last close {r['Close']}.",
        f"Plan: Entry {r['Entry']}, Stop {r['SL']}.",
    ]
    if "EntryMode" in r: parts.append(f"Mode: {r['EntryMode']}.")
    if "TriggerDistATR" in r: parts.append(f"Proximity: {r['TriggerDistATR']} ATR.")
    if "WHY" in r and isinstance(r['WHY'], str) and r['WHY']: parts.append(f"Signals: {r['WHY']}.")
    if "GO_REASONS" in r and isinstance(r['GO_REASONS'], str) and r['GO_REASONS'] != "OK":
        parts.append(f"Note: {r['GO_REASONS']}.")
    return " ".join(parts)

lines = []
lines.append(f"# StakeLens Insider — Weekly Brief (auto-updated)")
lines.append(f"**As of:** {as_of}")
if "MKT_BREADTH20" in pack.columns and pack["MKT_BREADTH20"].notna().any():
    try:
        breadth = round(float(pack["MKT_BREADTH20"].dropna().iloc[0])*100, 1)
        lines.append(f"**Market breadth (above 20-DMA):** {breadth}%")
    except Exception:
        pass
lines.append("")

lines.append("**How to use this brief:**")
lines.append(textwrap.fill(
    "Trade only GO names for higher conviction. GO requires proximity to trigger, strong closing behaviour, "
    "and power confirmation, subject to a minimum market breadth. Watchlist items need further improvement; "
    "prefer a controlled reclaim or a decisive breakout on strong volume before acting.",
    width=96
))
lines.append("")

if len(go_df):
    lines.append("## GO (ready/high-conviction)")
    for _, r in go_df.iterrows():
        lines.append(f"- {fline(r)}")
    lines.append("")
else:
    lines.append("## GO (ready/high-conviction)")
    lines.append("- No names passed the Go/No-Go guard today.")
    lines.append("")

if len(wt_df):
    lines.append("## Watchlist (needs confirm)")
    for _, r in wt_df.iterrows():
        lines.append(f"- {fline(r)}")
    lines.append("")

out_path.write_text("\n".join(lines), encoding="utf-8")
print(f"Wrote {out_path} (GO={len(go_df)}; Watch={len(wt_df)}).")
