#!/usr/bin/env python3
# Rebuilds a weekly-style brief every time new data arrives.
# Inputs:
#   --pack  : out/WEEK_PACK.csv   (ranked universe, with WHY tags & triggers)
#   --tech  : data/tech_latest.csv (for simple breadth/context)
#   --out   : out/WEEK_BRIEF.md

import argparse, sys, textwrap
import pandas as pd
from datetime import datetime
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument("--pack", required=True)
p.add_argument("--tech", required=False)
p.add_argument("--out", required=True)
args = p.parse_args()

pack_path = Path(args.pack)
if not pack_path.exists():
    sys.exit(f"Missing {pack_path}")

# Load WEEK_PACK
pack = pd.read_csv(pack_path)
if pack.empty:
    sys.exit("WEEK_PACK.csv is empty — cannot produce a brief.")

# Try to load tech_latest for context
breadth_info = ""
try:
    tech = pd.read_csv(args.tech, parse_dates=["Date"])
    # Simple breadth snapshot: % above SMA20/SMA50
    def pct(colname):
        if colname not in tech.columns: return None
        return round(100.0 * tech[colname].mean(), 1)
    # If we have boolean flags (AboveSMA20/50) compute percentages
    above20 = pct("AboveSMA20") if "AboveSMA20" in tech.columns else None
    above50 = pct("AboveSMA50") if "AboveSMA50" in tech.columns else None

    # Fallback breadth using price vs SMA columns if available
    if above20 is None and "Close" in tech.columns and "SMA20" in tech.columns:
        above20 = round(100.0 * (tech["Close"] > tech["SMA20"]).mean(), 1)
    if above50 is None and "Close" in tech.columns and "SMA50" in tech.columns:
        above50 = round(100.0 * (tech["Close"] > tech["SMA50"]).mean(), 1)

    parts = []
    if above20 is not None: parts.append(f"{above20}% above 20-DMA")
    if above50 is not None: parts.append(f"{above50}% above 50-DMA")
    if parts:
        breadth_info = " | ".join(parts)
except Exception:
    pass

# Decide the as-of date from WEEK_PACK
asof_field = None
for col in ("AsOf","Date","DATE","asof"):
    if col in pack.columns:
        asof_field = col; break
if asof_field is not None:
    try:
        as_of = pd.to_datetime(pack[asof_field].iloc[0]).date()
    except Exception:
        as_of = datetime.now().date()
else:
    as_of = datetime.now().date()

# Choose top N
score_col = None
for c in ("R_SCORE","Score","score","TOTAL_SCORE"):
    if c in pack.columns: score_col = c; break
if score_col is None:
    sys.exit("WEEK_PACK.csv missing a score column (e.g., R_SCORE).")

pack = pack.sort_values(score_col, ascending=False)
topN = pack.head(8).copy()  # show up to 8 names

# Friendly accessors with fallbacks
def g(row, names, default=""):
    for n in names:
        if n in row and pd.notna(row[n]): return row[n]
    return default

def fmt_trigger(row):
    entry = g(row, ["Entry","ENTRY","BuyAbove","BUY_ABOVE"])
    sl    = g(row, ["SL","Stop","STOP"])
    atr   = g(row, ["ATR14","ATR","atr14"])
    if entry and sl:
        r = f"Entry {entry}, Stop {sl}"
        if atr: r += f", Risk guide ≈ {atr} ATR"
        return r
    return "Trigger details in the pack."

def fmt_why(row):
    why = g(row, ["WHY","Why","Tags","REASONS"])
    if why:
        # Keep it compact and readable
        return " — " + str(why).strip()
    return ""

lines = []

# Header
lines.append(f"# StakeLens Insider — Weekly Brief (auto-updated)\n")
lines.append(f"**As of:** {as_of}\n")
if breadth_info:
    lines.append(f"**Market breadth:** {breadth_info}\n")

lines.append("**How to use this brief:**")
lines.append(textwrap.fill(
    "This is a ranked weekly view refreshed after every data upload. "
    "Focus on the top names. Use the entry and stop levels as a starting plan and apply position sizing discipline. "
    "If a symbol gaps far beyond the entry band, wait for a controlled pullback before acting.",
    width=96
))
lines.append("")

# Top picks
lines.append("## Top candidates")
for _, r in topN.iterrows():
    sym = g(r, ["Symbol","SYMBOL","symbol"])
    close = g(r, ["Close","CLOSE","close"])
    score = g(r, [score_col])
    trig  = fmt_trigger(r)
    why   = fmt_why(r)
    lines.append(f"- **{sym}** — score {score}, last close {close}. {trig}{why}")

lines.append("")

# Execution notes (succinct, not chatty)
lines.append("## Execution notes")
notes = [
 "Enter on strength: use a stop order slightly above recent highs or the stated entry band.",
 "Place the initial stop near the most recent 2-bar low or the stated stop; size positions so a single stop-out costs a small, fixed fraction of capital.",
 "If the setup invalidates (closes below 20-DMA after entry or the stop is hit), exit and wait for a fresh signal.",
]
for n in notes: lines.append(f"- {n}")
lines.append("")

# Full table link hint
lines.append("## Full list and details")
lines.append("See `out/WEEK_PACK.csv` for the full ranked list with indicator context, WHY tags, and numeric fields used to compute scores.")
lines.append("")

# Save
out_path = Path(args.out)
out_path.parent.mkdir(parents=True, exist_ok=True)
out_path.write_text("\n".join(lines), encoding="utf-8")
print(f"Wrote {out_path} with {len(topN)} top entries.")
