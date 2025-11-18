#!/usr/bin/env python3
"""
Generate a readable Weekly Brief markdown from WEEK_PACK.csv.

- Clear headings and bullet lists
- Plan A/B/C shown on separate indented bullets
- Plain, neutral tone (no slang)
- Robust to missing optional columns (Entry2/Mode2/etc.)
"""

from pathlib import Path
import argparse
import math
import pandas as pd
import numpy as np

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--pack", default="out/WEEK_PACK.csv")
    p.add_argument("--out",  default="out/WEEK_BRIEF.md")
    p.add_argument("--max_watch", type=int, default=15, help="Cap watchlist length")
    return p.parse_args()

def f2(x):
    try:
        if x is None or (isinstance(x, float) and not math.isfinite(x)): return "—"
        return f"{float(x):.2f}"
    except Exception:
        return "—"

def f1p(x):
    try:
        if x is None or (isinstance(x, float) and not math.isfinite(x)): return "—"
        return f"{float(x)*100:.1f}%"
    except Exception:
        return "—"

def clean_signals(s: str) -> str:
    if not isinstance(s, str) or not s.strip():
        return "—"
    # Keep concise; replace underscores with thin spaces for readability
    toks = [t.strip() for t in s.split(",") if t.strip()]
    return ", ".join(toks[:8])

def plan_line(tag, entry, sl, mode):
    if (entry is None or (isinstance(entry, float) and not np.isfinite(entry))) \
       and (sl is None or (isinstance(sl, float) and not np.isfinite(sl))) \
       and (not mode):
        return None
    return f"- {tag}: Entry **{f2(entry)}**, Stop **{f2(sl)}**, Mode **{mode if mode else '—'}**"

def row_block(r):
    sym   = r.get("Symbol","?")
    score = f2(r.get("R_SCORE"))
    close = f2(r.get("Close"))
    prox  = r.get("TriggerDistATR", np.nan)
    prox_s= f"{prox:.2f} ATR" if isinstance(prox, (float,int)) and np.isfinite(prox) else "—"
    why   = clean_signals(r.get("WHY",""))
    reason= r.get("GO_REASONS","").strip() or "Needs confirmation"

    # Plans
    a = plan_line("Plan A", r.get("Entry"),  r.get("SL"),  r.get("EntryMode"))
    b = plan_line("Plan B", r.get("Entry2"), r.get("SL2"), r.get("Mode2"))
    c = plan_line("Plan C", r.get("Entry3"), r.get("SL3"), r.get("Mode3"))

    lines = [
        f"**{sym}** — score **{score}**, last close **{close}**",
        a if a else None,
        b if b else None,
        c if c else None,
        f"- Signals: {why}",
        f"- Proximity: {prox_s}",
        f"- Go reason: {reason}",
    ]
    # Indent plan/details bullets
    pretty = [lines[0]]
    for ln in lines[1:]:
        if ln: pretty.append(f"  {ln}")
    return "\n".join(pretty)

def main():
    args = parse_args()
    pack_path = Path(args.pack)
    out_path  = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not pack_path.exists():
        out_path.write_text("Weekly Brief\n\nNo eligible candidates today.\n", encoding="utf-8")
        return

    d = pd.read_csv(pack_path)
    if d.empty:
        out_path.write_text("Weekly Brief\n\nNo eligible candidates today.\n", encoding="utf-8")
        return

    # Safe header fields
    asof = None
    if "AsOf" in d.columns and d["AsOf"].notna().any():
        try:
            asof = pd.to_datetime(d["AsOf"]).max().date().isoformat()
        except Exception:
            asof = None
    asof_str = asof or "—"

    mb = None
    if "MKT_BREADTH20" in d.columns and d["MKT_BREADTH20"].notna().any():
        try: mb = float(d["MKT_BREADTH20"].iloc[0])
        except Exception: mb = None
    disp = None
    if "DISP_4W" in d.columns and d["DISP_4W"].notna().any():
        try: disp = float(d["DISP_4W"].iloc[0])
        except Exception: disp = None

    # Split GO vs Watchlist
    go = d[d.get("GO", False) == True].copy()
    wl = d[d.get("GO", False) != True].copy()

    # Sorts
    go  = go.sort_values(["R_SCORE","Symbol"], ascending=[False, True])
    wl  = wl.sort_values(["R_SCORE","Symbol"], ascending=[False, True]).head(args.max_watch)

    # Build header
    header = [
        "# StakeLens Insider — Weekly Brief (auto-updated)",
        "",
        f"**As of:** {asof_str}  ",
        f"**Market breadth (>20-DMA):** {f1p(mb) if mb is not None else '—'}  ",
        f"**Dispersion (4W σ):** {f1p(disp) if disp is not None else '—'}",
        "",
        "### How to read this",
        "- Start with **Plan A**. If it does not trigger, consider **Plan B (reclaim)** or **Plan C (inside-day)**, when shown.",
        "- Keep risk fixed per trade. Avoid wide gaps; prefer orderly triggers.",
        "- If breadth is weak, prioritize names with a strong Go reason or sector support.",
        "",
    ]

    # GO section
    go_lines = ["## GO (ready / high conviction)"]
    if go.empty:
        go_lines.append("No names passed the Go checks today.")
    else:
        for _, r in go.iterrows():
            go_lines.append(row_block(r))
            go_lines.append("")  # blank line between items

    # Watchlist section
    wl_lines = ["## Watchlist (needs confirmation)"]
    if wl.empty:
        wl_lines.append("No additional candidates today.")
    else:
        for _, r in wl.iterrows():
            wl_lines.append(row_block(r))
            wl_lines.append("")

    # Definitions block
    defs = [
        "### Plan definitions",
        "- **Plan A:** Breakout or ready trigger with a volatility- and tick-aware buffer.",
        "- **Plan B (Reclaim):** Reclaim over short moving average with power confirmation.",
        "- **Plan C (Inside-day):** Break of inside-day high with stop at the inside-day low.",
        "",
    ]

    md = "\n".join(header + go_lines + [""] + wl_lines + [""] + defs)
    out_path.write_text(md, encoding="utf-8")
    print(f"Wrote {out_path} (GO={int(go.shape[0])}, Watch={int(wl.shape[0])})")

if __name__ == "__main__":
    main()
