#!/usr/bin/env python3
"""
StakeLens Insider — Brief generator.

Reads WEEK_PACK.csv and emits a human-friendly weekly brief:

- Header: date, breadth, dispersion
- GO list: one block per stock with clear bullets and indentation
- Watchlist: top non-GO names with concise context

Uses the enriched fields from score_week.py:
- Sector: Sector, SECTOR_BREADTH20, SEC_RS_Z
- Stock vs sector: RET_10, SEC_RET_10, RET10_EDGE_SEC
- Momentum flags: SECTOR_MOM_OK, STOCK_VS_SEC_OK
"""

import argparse
from pathlib import Path
import pandas as pd
import numpy as np


# ---------- CLI ----------

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--pack", default="out/WEEK_PACK.csv")
    p.add_argument("--out",  default="out/WEEK_BRIEF.md")
    p.add_argument("--watch_limit", type=int, default=15)
    return p.parse_args()


# ---------- Formatting helpers ----------

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
        return "—"
    return f"{x:.2f}"


def _fmt_score(x):
    if pd.isna(x):
        return "—"
    return f"{x:.2f}"


def _safe_bool(val, default=False):
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(val)
    return default


def _plan_block(row):
    """
    Return markdown with indented bullets:

      - **Plan A**: Entry X, Stop Y, Mode Z
      - **Plan B**: ...
      - **Plan C**: ...
    """
    lines = []

    def one_plan(label, e_col, s_col, m_col):
        e = row.get(e_col, np.nan)
        s = row.get(s_col, np.nan)
        m = row.get(m_col, "")
        if pd.isna(e) or pd.isna(s) or not isinstance(m, str) or m.strip() == "":
            return f"  - **{label}**: Entry —, Stop —, Mode —"
        return f"  - **{label}**: Entry {_fmt_price(e)}, Stop {_fmt_price(s)}, Mode {m}"

    # These match the columns written by score_week.py
    lines.append(one_plan("Plan A", "Entry",  "SL",  "EntryMode"))
    lines.append(one_plan("Plan B", "Entry2", "SL2", "Mode2"))
    lines.append(one_plan("Plan C", "Entry3", "SL3", "Mode3"))
    return "\n".join(lines)


def _sector_context(row):
    """
    One short, readable line summarising sector & momentum.

    Example:
    Sector: FINANCIALS — breadth 62%, RS z +0.80, 10D vs sector +1.2%
      - View: riding sector + stock momentum
    """
    sector = row.get("Sector", "OTHER")
    sb     = row.get("SECTOR_BREADTH20", np.nan)
    sz     = row.get("SEC_RS_Z", np.nan)
    edge   = row.get("RET10_EDGE_SEC", np.nan)

    sec_ok   = _safe_bool(row.get("SECTOR_MOM_OK", True), True)
    stock_ok = _safe_bool(row.get("STOCK_VS_SEC_OK", True), True)

    headline_parts = [f"Sector: {sector}"]
    if not pd.isna(sb):
        headline_parts.append(f"breadth {_fmt_pct(sb, 0)}")
    if not pd.isna(sz):
        headline_parts.append(f"RS z {_fmt_z(sz)}")
    if not pd.isna(edge):
        headline_parts.append(f"10D vs sector {_fmt_ret_edge(edge)}")

    # Qualitative view
    if sec_ok and stock_ok:
        view = "riding sector + stock momentum"
    elif sec_ok and not stock_ok:
        view = "sector ok, stock lagging (be picky on entries)"
    elif (not sec_ok) and stock_ok:
        view = "stock outperforming a weak sector (elite only)"
    else:
        view = "both sector and stock are under pressure"

    lines = []
    lines.append("  - " + " — ".join(headline_parts))
    lines.append(f"  - View: {view}")
    return "\n".join(lines)


def _slippage_hint(row):
    """
    Execution hint:
      - flags high ATR% / low turnover
    """
    atr_pct = row.get("ATR_PCT", np.nan)
    t20     = row.get("TurnoverCr_med20", np.nan)

    if pd.isna(atr_pct) or pd.isna(t20):
        return "  - Execution: normal; keep risk per trade fixed."

    msgs = []
    if atr_pct >= 0.08:
        msgs.append("high volatility (ATR% elevated)")
    if t20 < 10:
        msgs.append("thin liquidity (<10cr 20D median turnover)")
    elif t20 < 20:
        msgs.append("moderate liquidity (~10–20cr 20D median turnover)")

    if not msgs:
        return "  - Execution: decent liquidity and moderate ATR – standard sizing."

    return "  - Execution: " + "; ".join(msgs) + " — size down / use limit orders."


# ---------- Sections ----------

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
    lines.append("")
    lines.append("- Start with **Plan A**. If it does not trigger, look at Plan B (reclaim) or Plan C (inside-day), when present.")
    lines.append("- Keep **risk per trade fixed**. Avoid wide gap-ups; prefer orderly triggers.")
    if not pd.isna(breadth):
        if breadth < 0.40:
            lines.append("- Breadth is weak → treat GO as **elite only**, be strict with entries and avoid late chases.")
        elif breadth > 0.60:
            lines.append("- Breadth is strong → you can be slightly more open with **high-quality** setups.")
        else:
            lines.append("- Breadth is mixed → prioritize names with **clear GO reasons** or **strong sector support**.")
    else:
        lines.append("- Prioritize names with **clear GO reasons** or **strong sector support**.")
    lines.append("")
    return "\n".join(lines)

def build_tv_buy_section(df):
    """
    Section: names where the Python engine thinks Plan A is close enough
    and strong enough that tomorrow's bar could realistically trigger
    your TradingView buy script.
    """
    lines = []
    lines.append("Next-bar TV buy setup candidates")
    lines.append("")

    # If the pack is old / column missing, fail soft
    if "TV_BUY_SETUP" not in df.columns:
        lines.append("- TV_BUY_SETUP flag not available in this pack; rerun scoring with the updated score_week.py.")
        lines.append("")
        return "\n".join(lines)

    tv = df[df["TV_BUY_SETUP"].astype(bool)].copy()
    if tv.empty:
        lines.append("- None this run. Focus on the GO list and Watchlist instead.")
        lines.append("")
        return "\n".join(lines)

    tv = tv.sort_values("R_SCORE", ascending=False)

    for _, r in tv.iterrows():
        sym   = r["Symbol"]
        score = _fmt_score(r.get("R_SCORE", np.nan))
        close = _fmt_price(r.get("Close", np.nan))
        prox  = r.get("TriggerDistATR", np.nan)
        prox_txt = f"{prox:.2f} ATR above" if pd.notna(prox) else "NA"

        lines.append(f"- **{sym}** — score {score}, last close {close}")
        lines.append(f"  - Plan A trigger distance: {prox_txt}")
        # Reuse the existing helpers
        lines.append(_plan_block(r))
        lines.append(_sector_context(r))
        lines.append(_slippage_hint(r))
        lines.append("")  # spacing between names

    lines.append("")
    return "\n".join(lines)

def build_go_section(go_df):
    lines = []
    lines.append("GO (ready / high conviction)")
    lines.append("")

    if go_df.empty:
        lines.append("- No GO setups this run.")
        lines.append("- Use the watchlist as your hunting ground for emerging ideas.")
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

        lines.append(f"**{sym}** — score {score}, last close {_fmt_price(r.get('Close', np.nan))}")
        lines.append("")
        # Plans
        lines.append(_plan_block(r))
        lines.append("")
        # Signals / GO reason
        if signals:
            lines.append("  - Signals: " + ", ".join(signals))
        else:
            lines.append("  - Signals: —")
        lines.append(f"  - Proximity to trigger: {prox_txt}")
        lines.append(f"  - Go reason: {go_reason}")
        # Sector + slippage
        lines.append(_sector_context(r))
        lines.append(_slippage_hint(r))
        lines.append("")  # blank line between stocks

    lines.append("")
    return "\n".join(lines)


def build_watch_section(watch_df, limit):
    lines = []
    lines.append("Watchlist (developing setups)")
    lines.append("")

    if watch_df.empty:
        lines.append("- None this run.")
        lines.append("")
        return "\n".join(lines)

    watch_df = watch_df.sort_values("R_SCORE", ascending=False).head(limit)

    for _, r in watch_df.iterrows():
        sym   = r["Symbol"]
        score = _fmt_score(r.get("R_SCORE", np.nan))
        close = _fmt_price(r.get("Close", np.nan))
        prox  = r.get("TriggerDistATR", np.nan)
        prox_txt = f"{prox:.2f} ATR" if pd.notna(prox) else "NA"
        reason = r.get("GO_REASONS", "Needs confirm")

        lines.append(f"- **{sym}** — score {score}, last close {close}")
        lines.append(f"  - Proximity: {prox_txt}")
        lines.append(f"  - Note: {reason}")
        lines.append(_sector_context(r))
        lines.append("")  # spacing between watch names

    lines.append("")
    return "\n".join(lines)


# ---------- Main ----------

def main():
    args = parse_args()
    pack_path = Path(args.pack)
    out_path  = Path(args.out)

    if not pack_path.exists():
        raise SystemExit(f"Missing pack file: {pack_path}")

    df = pd.read_csv(pack_path)

    if df.empty:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            "StakeLens Insider — Weekly Brief\n\nNo data in WEEK_PACK.csv.\n",
            encoding="utf-8",
        )
        print(f"Wrote empty brief to {out_path}")
        return

    # Robust boolean for GO
    if "GO" in df.columns:
        df["GO"] = df["GO"].astype(bool)
    else:
        df["GO"] = False

        parts = []
    parts.append(build_header(df))
    parts.append(build_how_to(df))
    parts.append(build_tv_buy_section(df))                 # NEW: next-bar candidates
    parts.append(build_go_section(go_df))
    parts.append(build_watch_section(watch_df, args.watch_limit))

    text = "\n".join(parts)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text, encoding="utf-8")
    print(f"Wrote {out_path} (GO={len(go_df)}, Watch={min(len(watch_df), args.watch_limit)})")


if __name__ == "__main__":
    main()
