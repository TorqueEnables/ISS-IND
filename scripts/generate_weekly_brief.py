#!/usr/bin/env python3
"""
StakeLens Insider — Weekly Brief generator.

Reads WEEK_PACK.csv and emits a human-readable weekly brief:
- Header: date, breadth, dispersion
- GO list with Plans A/B/C, reasons, and sector context
- Watchlist (top non-GO names by score)

This version is aware of:
- Sector momentum (SEC_RS_Z, SECTOR_BREADTH20, SECTOR_MOM_OK)
- Stock vs sector 10D edge (RET10_EDGE_SEC, STOCK_VS_SEC_OK)
- Flow / structural reasons kept via WHY + GO_REASONS
"""

import argparse
from pathlib import Path
import pandas as pd
import numpy as np


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--pack", default="out/WEEK_PACK.csv")
    p.add_argument("--out",  default="out/WEEK_BRIEF.md")
    p.add_argument("--watch_limit", type=int, default=15)
    return p.parse_args()


def _fmt_pct(x, decimals=1, factor=100.0, suffix="%"):
    if pd.isna(x):
        return "NA"
    return f"{x*factor:.{decimals}f}{suffix}"


def _fmt_z(x):
    if pd.isna(x):
        return "NA"
    return f"{x:+.2f}"


def _fmt_ret_edge(x):
    if pd.isna(x):
        return "NA"
    return f"{x*100:+.1f}%"


def _fmt_price(x):
    if pd.isna(x):
        return "NA"
    return f"{x:.2f}"


def _fmt_score(x):
    if pd.isna(x):
        return "NA"
    return f"{x:.2f}"


def _plan_block(row):
    """Format Plan A/B/C text for a single row."""
    lines = []

    def plan_line(label, e_col, s_col, m_col):
        e = row.get(e_col, np.nan)
        s = row.get(s_col, np.nan)
        m = row.get(m_col, "")
        if pd.isna(e) or pd.isna(s) or (not isinstance(m, str)) or m == "":
            return f"{label}: Entry —, Stop —, Mode —"
        return f"{label}: Entry {_fmt_price(e)}, Stop {_fmt_price(s)}, Mode {m}"

    lines.append(plan_line("Plan A", "Entry", "SL", "EntryMode"))
    lines.append(plan_line("Plan B", "Entry2", "SL2", "Mode2"))
    lines.append(plan_line("Plan C", "Entry3", "SL3", "Mode3"))
    return "\n".join(lines)


def _sector_context(row):
    """Build one short sector/momentum line per stock."""
    sector = row.get("Sector", "OTHER")
    sb    = row.get("SECTOR_BREADTH20", np.nan)
    sz    = row.get("SEC_RS_Z", np.nan)
    edge  = row.get("RET10_EDGE_SEC", np.nan)
    sec_ok   = bool(row.get("SECTOR_MOM_OK", True))
    stock_ok = bool(row.get("STOCK_VS_SEC_OK", True))

    parts = [f"Sector: {sector}"]

    if not pd.isna(sb):
        parts.append(f"breadth {_fmt_pct(sb, 0)}")
    if not pd.isna(sz):
        parts.append(f"RS z {_fmt_z(sz)}")
    if not pd.isna(edge):
        parts.append(f"10D vs sector {_fmt_ret_edge(edge)}")

    # Qualitative flags
    qual = []
    if sec_ok and stock_ok:
        qual.append("riding sector + stock momentum")
    elif sec_ok and not stock_ok:
        qual.append("sector ok, stock lagging (be picky on entries)")
    elif (not sec_ok) and stock_ok:
        qual.append("stock outperforming a weak sector (elite only)")
    else:
        qual.append("both sector and stock are under pressure")

    parts.append(" | " + "; ".join(qual))
    return " ".join(parts)


def _slippage_hint(row):
    """
    Very rough slippage / execution hint:
    - High ATR% or low turnover => call out as potentially jumpy.
    """
    atr_pct = row.get("ATR_PCT", np.nan)
    t20     = row.get("TurnoverCr_med20", np.nan)

    msgs = []
    if pd.isna(atr_pct) or pd.isna(t20):
        return "Execution: normal; respect your max risk per trade."

    if atr_pct >= 0.08:
        msgs.append("High volatility (ATR% elevated)")
    if t20 < 10:
        msgs.append("Thinner liquidity (<10cr med turnover)")
    elif t20 < 20:
        msgs.append("Moderate liquidity (~10–20cr med turnover)")

    if not msgs:
        return "Execution: decent liquidity and moderate ATR – standard position sizing."
    return "Execution: " + "; ".join(msgs) + ". Size down / use limit orders."


def build_header(df):
    asof = str(df["AsOf"].iloc[0]) if "AsOf" in df.columns else "NA"
    breadth = df["MKT_BREADTH20"].iloc[0] if "MKT_BREADTH20" in df.columns else np.nan
    disp    = df["DISP_4W"].iloc[0] if "DISP_4W" in df.columns else np.nan

    lines = []
    lines.append("StakeLens Insider — Weekly Brief (auto-updated)")
    lines.append(f"As of: {asof}")
    lines.append(f"Market breadth (>20-DMA): {_fmt_pct(breadth)}")
    lines.append(f"Dispersion (4W σ): {_fmt_pct(disp, 1)}")
    lines.append("")
    return "\n".join(lines)


def build_how_to(df):
    breadth = df["MKT_BREADTH20"].iloc[0] if "MKT_BREADTH20" in df.columns else np.nan

    lines = []
    lines.append("How to read this")
    lines.append(
        "Start with Plan A. If it does not trigger, consider Plan B (reclaim) or Plan C (inside-day), when shown."
    )
    lines.append("Keep risk fixed per trade. Avoid wide gaps; prefer orderly triggers.")

    if not pd.isna(breadth):
        if breadth < 0.40:
            lines.append("Breadth is weak: treat GO as elite only and be strict with entries.")
        elif breadth > 0.60:
            lines.append("Breadth is strong: you can be slightly more open with quality setups.")
        else:
            lines.append("Breadth is mixed: prioritize names with strong GO reasons or clear sector support.")
    else:
        lines.append("Prioritize names with strong GO reasons or clear sector support.")

    lines.append("")
    return "\n".join(lines)


def build_go_section(go_df):
    lines = []
    lines.append("GO (ready / high conviction)")
    if go_df.empty:
        lines.append("No GO setups this run. Use the watchlist as your hunting ground for emerging ideas.")
        lines.append("")
        return "\n".join(lines)

    for _, r in go_df.iterrows():
        sym   = r["Symbol"]
        score = _fmt_score(r.get("R_SCORE", np.nan))
        close = _fmt_price(r.get("Close", np.nan))
        prox  = r.get("TriggerDistATR", np.nan)
        prox_txt = f"{prox:.2f} ATR" if pd.notna(prox) else "NA"
        go_reason = r.get("GO_REASONS", "NA")
        why_raw   = str(r.get("WHY", "") or "")
        signals   = [w for w in why_raw.split(",") if w]

        lines.append(f"{sym} — score {score}, last close {close}")
        lines.append("")
        lines.append(_plan_block(r))
        lines.append("")

        if signals:
            lines.append("Signals: " + ", ".join(signals))
        else:
            lines.append("Signals: —")

        lines.append(f"Proximity: {prox_txt}")
        lines.append(f"Go reason: {go_reason}")

        # Sector + slippage context
        lines.append(_sector_context(r))
        lines.append(_slippage_hint(r))

        lines.append("")  # blank line between names

    lines.append("")
    return "\n".join(lines)


def build_watch_section(watch_df, limit):
    lines = []
    lines.append("Watchlist (developing setups)")
    if watch_df.empty:
        lines.append("None this run.")
        lines.append("")
        return "\n".join(lines)

    watch_df = watch_df.sort_values("R_SCORE", ascending=False).head(limit)

    for _, r in watch_df.iterrows():
        sym   = r["Symbol"]
        score = _fmt_score(r.get("R_SCORE", np.nan))
        close = _fmt_price(r.get("Close", np.nan))
        reason = r.get("GO_REASONS", "Needs confirm")
        prox = r.get("TriggerDistATR", np.nan)
        prox_txt = f"{prox:.2f} ATR" if pd.notna(prox) else "NA"

        lines.append(f"{sym} — score {score}, last close {close}")
        lines.append(f"Proximity: {prox_txt}")
        lines.append(f"Notes: {reason}")
        lines.append(_sector_context(r))
        lines.append("")  # spacing

    lines.append("")
    return "\n".join(lines)


def main():
    args = parse_args()
    pack_path = Path(args.pack)
    out_path  = Path(args.out)

    if not pack_path.exists():
        raise SystemExit(f"Missing pack file: {pack_path}")

    df = pd.read_csv(pack_path)

    if df.empty:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text("StakeLens Insider — Weekly Brief\n\nNo data in WEEK_PACK.csv.\n", encoding="utf-8")
        print(f"Wrote empty brief to {out_path}")
        return

    # Robust boolean handling
    if "GO" in df.columns:
        df["GO"] = df["GO"].astype(bool)
    else:
        df["GO"] = False

    go_df    = df[df["GO"]].sort_values("R_SCORE", ascending=False)
    watch_df = df[~df["GO"]].copy()

    parts = []
    parts.append(build_header(df))
    parts.append(build_how_to(df))
    parts.append(build_go_section(go_df))
    parts.append(build_watch_section(watch_df, args.watch_limit))

    text = "\n".join(parts)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text, encoding="utf-8")
    print(f"Wrote {out_path} (GO={len(go_df)}, Watch={min(len(watch_df), args.watch_limit)})")


if __name__ == "__main__":
    main()
