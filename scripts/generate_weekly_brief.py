#!/usr/bin/env python3
"""
StakeLens Insider — Weekly Brief generator (with Plan-A/B/C rendering).

Inputs:
  --pack  out/WEEK_PACK.csv  (columns include: Symbol, R_SCORE, Entry/SL/EntryMode,
                              Entry2/SL2/Mode2, Entry3/SL3/Mode3, WHY, GO, MKT_BREADTH20, DISP_4W, AsOf)
  --out   out/WEEK_BRIEF.md

Behaviour:
  - Prints GO (ready) and Watchlist (needs confirm)
  - Shows Plan A (primary) + optional Plan B (Reclaim) + optional Plan C (Inside-day)
"""

import argparse
from pathlib import Path
import pandas as pd
import numpy as np

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--pack", required=True)
    p.add_argument("--out", required=True)
    return p.parse_args()

def bfmt(x):
    return "—" if (pd.isna(x) or x == "" or (isinstance(x, float) and not np.isfinite(x))) else f"{x:.2f}"

def normalize_bool(s):
    if s.dtype == bool:
        return s
    return s.astype(str).str.lower().isin(["true","1","yes"])

def main():
    args = parse_args()
    df = pd.read_csv(args.pack)
    if df.empty:
        Path(args.out).write_text("Weekly Brief\nNo eligible candidates today.\n", encoding="utf-8")
        return

    # Robust types
    if "GO" in df.columns:
        df["GO"] = normalize_bool(df["GO"])
    else:
        df["GO"] = False

    asof = df["AsOf"].iloc[0] if "AsOf" in df.columns else ""
    mb   = df["MKT_BREADTH20"].iloc[0] if "MKT_BREADTH20" in df.columns else np.nan
    disp = df["DISP_4W"].iloc[0] if "DISP_4W" in df.columns else np.nan

    # Sorts
    go  = df[df["GO"]].sort_values(["R_SCORE","Close"], ascending=[False, False])
    wt  = df[~df["GO"]].sort_values(["R_SCORE","Close"], ascending=[False, False]).head(12)

    lines = []
    lines.append("StakeLens Insider — Weekly Brief (auto-updated)")
    lines.append(f"As of: {asof} Market breadth (>20-DMA): {mb*100:.1f}%  Dispersion(4W σ): {disp*100:.1f}%")
    lines.append("")
    lines.append("How to use this brief: Prefer GO names. If they don’t trigger, consider the Reclaim or Inside-day alternatives with their stated stops. Size positions so a stop-out costs a small, fixed share of capital.")
    lines.append("")

    def render_block(title, frame):
        lines.append(title)
        if frame.empty:
            lines.append("No names passed this section today.")
            lines.append("")
            return
        for _, r in frame.iterrows():
            sigs = (r.get("WHY","") or "").replace(",", ",")
            entry = bfmt(r.get("Entry", np.nan)); sl = bfmt(r.get("SL", np.nan))
            mode  = r.get("EntryMode","")
            e2 = bfmt(r.get("Entry2", np.nan)); s2 = bfmt(r.get("SL2", np.nan)); m2 = r.get("Mode2","")
            e3 = bfmt(r.get("Entry3", np.nan)); s3 = bfmt(r.get("SL3", np.nan)); m3 = r.get("Mode3","")

            lines.append(f"{r['Symbol']} — score {r['R_SCORE']:.1f}, last close {r['Close']:.2f}. "
                         f"Plan: Entry {entry}, Stop {sl}. Mode: {mode}. Signals: {sigs}.")
            alt_bits = []
            if m2:
                alt_bits.append(f"Alt: Entry {e2}, Stop {s2}. Mode: {m2}.")
            if m3:
                alt_bits.append(f"Inside: Entry {e3}, Stop {s3}. Mode: {m3}.")
            if alt_bits:
                lines.append(" ".join(alt_bits))
        lines.append("")

    render_block("GO (ready/high-conviction)", go)
    render_block("Watchlist (needs confirm)", wt)

    Path(args.out).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {args.out} (GO={len(go)}; Watch={len(wt)}).")

if __name__ == "__main__":
    main()
