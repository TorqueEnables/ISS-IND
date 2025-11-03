#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
StakeLens — Weekly Brief (Markdown)

Inputs:
  - out/WEEK_PACK.csv      (from score_week.py)

Outputs:
  - out/WEEK_BRIEF.md
  - data/brief_status.json   (diagnostics)

Logic:
  - Detect latest 'Date' inside WEEK_PACK.csv (as_of)
  - Pick top N by R_SCORE, with a minimum score threshold (tunable)
  - Summaries: tag mix (INSIDER+, BULK+, etc.), counts, data freshness
  - For each top pick: compact line with WHY & Trigger

Zero network. Robust to missing columns (fills blanks gracefully).
"""

from __future__ import annotations
import os, json
from datetime import datetime
import pandas as pd
import numpy as np

PACK_PATH = "out/WEEK_PACK.csv"
OUT_DIR   = "out"
STATUS_F  = "data/brief_status.json"
BRIEF_F   = os.path.join(OUT_DIR, "WEEK_BRIEF.md")

# Tunables
TOP_N = 20
MIN_SCORE = 65  # adjust as you want

def read_pack(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    for opts in ({}, {"encoding":"utf-8-sig"}, {"encoding":"latin-1"}):
        try:
            df = pd.read_csv(path, **opts)
            return df
        except Exception:
            pass
    return pd.DataFrame()

def safe_num(x):
    try:
        return float(x)
    except Exception:
        return np.nan

def format_row(r) -> str:
    sym   = r.get("Symbol","?")
    sc    = r.get("R_SCORE", np.nan)
    why   = r.get("WHY","").strip().replace(",,",",").strip(",")
    trig  = r.get("Trigger","").strip()
    close = r.get("Close", np.nan)
    atr   = r.get("ATR14", np.nan)
    s20   = r.get("SMA20", np.nan)
    s50   = r.get("SMA50", np.nan)

    sc_s  = f"{safe_num(sc):.0f}" if pd.notna(sc) else "-"
    cl_s  = f"{safe_num(close):.2f}" if pd.notna(close) else "-"
    atr_s = f"{safe_num(atr):.2f}" if pd.notna(atr) else "-"
    s20_s = f"{safe_num(s20):.2f}" if pd.notna(s20) else "-"
    s50_s = f"{safe_num(s50):.2f}" if pd.notna(s50) else "-"

    line = f"- **{sym}** — Score **{sc_s}** • Close {cl_s} • ATR14 {atr_s} • SMA20 {s20_s} • SMA50 {s50_s}\n" \
           f"  - WHY: {why if why else '—'}\n" \
           f"  - Trigger: {trig if trig else '—'}"
    return line

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    status = {"ok": False, "when": datetime.utcnow().isoformat()+"Z"}

    pack = read_pack(PACK_PATH)
    if pack.empty:
        md = "# StakeLens Weekly Brief\n\nNo WEEK_PACK.csv found or it is empty.\n"
        with open(BRIEF_F, "w", encoding="utf-8") as f: f.write(md)
        status.update({"ok": False, "error": "missing WEEK_PACK.csv"})
        with open(STATUS_F, "w", encoding="utf-8") as f: json.dump(status, f, indent=2)
        print(json.dumps(status, indent=2))
        return 1

    # coerce types
    if "Date" in pack.columns:
        pack["Date"] = pd.to_datetime(pack["Date"], errors="coerce")
        as_of = pack["Date"].max()
    else:
        as_of = pd.NaT

    # ensure R_SCORE numeric
    if "R_SCORE" in pack.columns:
        pack["R_SCORE"] = pd.to_numeric(pack["R_SCORE"], errors="coerce")
    else:
        pack["R_SCORE"] = np.nan

    # Filter by threshold & sort
    eligible = pack.copy()
    eligible = eligible[eligible["R_SCORE"] >= MIN_SCORE].copy()
    eligible = eligible.sort_values(["R_SCORE","Symbol"], ascending=[False, True])

    top = eligible.head(TOP_N).copy()

    # Tag counts from WHY
    def has_tag(s, tag):
        s = str(s) if pd.notna(s) else ""
        return tag in s

    tags = {
        "TREND":       int(top["WHY"].apply(lambda s: has_tag(s,"TREND")).sum()),
        "PULLBACK55":  int(top["WHY"].apply(lambda s: has_tag(s,"PULLBACK55")).sum()),
        "NR7":         int(top["WHY"].apply(lambda s: has_tag(s,"NR7")).sum()),
        "ATR_LOW":     int(top["WHY"].apply(lambda s: has_tag(s,"ATR_LOW")).sum()),
        "DELIV_Q4":    int(top["WHY"].apply(lambda s: has_tag(s,"DELIV_Q4")).sum()),
        "INSIDER+":    int(top["WHY"].apply(lambda s: has_tag(s,"INSIDER+")).sum()),
        "BULK+":       int(top["WHY"].apply(lambda s: has_tag(s,"BULK+")).sum()),
        "BLOCK+":      int(top["WHY"].apply(lambda s: has_tag(s,"BLOCK+")).sum()),
        "NEAR_52W":    int(top["WHY"].apply(lambda s: has_tag(s,"NEAR_52W")).sum()),
        "CALM_52W":    int(top["WHY"].apply(lambda s: has_tag(s,"CALM_52W")).sum()),
        "EXTENDED":    int(top["WHY"].apply(lambda s: has_tag(s,"EXTENDED")).sum()),
        "SUB_SMA20":   int(top["WHY"].apply(lambda s: has_tag(s,"SUB_SMA20")).sum()),
    }

    # Build Markdown
    as_of_s = as_of.strftime("%d %b %Y") if pd.notna(as_of) else "unknown"
    head = [
        "# StakeLens — Weekly Brief",
        "",
        f"**As of:** {as_of_s}",
        f"**Universe:** {len(pack):,} rows in WEEK_PACK • **Threshold:** R_SCORE ≥ {MIN_SCORE} • **Top N:** {TOP_N}",
        "",
        "## Snapshot (What’s dominating the leaders)",
        f"- TREND: {tags['TREND']}  ·  PULLBACK55: {tags['PULLBACK55']}  ·  NR7: {tags['NR7']}  ·  ATR_LOW: {tags['ATR_LOW']}",
        f"- Delivery (Q4): {tags['DELIV_Q4']}  ·  INSIDER+: {tags['INSIDER+']}  ·  BULK+: {tags['BULK+']}  ·  BLOCK+: {tags['BLOCK+']}",
        f"- NEAR_52W: {tags['NEAR_52W']}  ·  CALM_52W: {tags['CALM_52W']}",
        "",
        "## Top Picks (with triggers)",
    ]

    lines = [format_row(r) for _, r in top.iterrows()]

    tail = [
        "",
        "## Playbook",
        "- Position **after** price confirms the trigger (Close > Entry).",
        "- Initial stop below the listed **Stop**; risk sized ≈ ATR (see line).",
        "- Avoid fresh entries on names with `EXTENDED` or `SUB_SMA20` tags until they reset.",
        "",
        "## Data Notes",
        "- Indicators computed from your local bhav history; no internet calls.",
        "- If a field is blank for a name, that name may lack the full lookback (rare with July→Oct seed).",
        "",
        "_Generated by scripts/generate_weekly_brief.py_",
        ""
    ]

    md = "\n".join(head + lines + tail)
    with open(BRIEF_F, "w", encoding="utf-8") as f:
        f.write(md)

    status.update({
        "ok": True,
        "when": datetime.utcnow().isoformat()+"Z",
        "as_of": as_of_s,
        "rows_pack": int(len(pack)),
        "rows_eligible": int(len(eligible)),
        "rows_top": int(len(top)),
        "top_symbols": top["Symbol"].head(10).tolist(),
        "threshold": MIN_SCORE,
        "out_md": BRIEF_F
    })
    with open(STATUS_F, "w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)

    print(json.dumps(status, indent=2))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
