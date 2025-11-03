#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
StakeLens — Weekly Scoring & Ranking (local-only, zero network)

Inputs (local only):
  - data/prices/bhav_hist.csv               (time-series)
  - data/tech_latest.csv                    (last-day indicators, from process_eod.py)
  - data/highlow_52w.csv                    (52W levels)
  - data/deliverables_latest.csv            (optional; may already be merged into tech_latest)
  - data/bulk_deals_latest.csv              (optional)
  - data/block_deals_latest.csv             (optional)
  - data/CF-Insider-Trading-equities-latest.csv (optional)

Output:
  - out/WEEK_PACK.csv       # ranked universe with reasons & triggers
  - data/score_status.json  # diagnostics

Scoring philosophy (simple, robust, adjustable):
  • Trend alignment (Close > SMA20 & SMA50; SMA20 rising)
  • Pullback after recent HI55 (<=10 bars since HI55 and price pulled back 2–8%)
  • NR7 compression and/or narrow ATR% (ATR14/Close low percentile)
  • Delivery% high (day’s top quartile)
  • Corporate flows (recent Insider Buy, Bulk-BUY, Block-BUY)
  • 52W proximity (<=2%) OR fresh 52W (not extended)
  • Liquidity sanity (Volume >= 0.8×VolAvg20)

Weights are defined at the top and easy to tweak.
This produces a compact WHY tag list and a simple “Go/Stop” trigger.
"""

from __future__ import annotations
import os, json, re
from datetime import datetime, timedelta
from typing import Optional, Dict
import pandas as pd
import numpy as np

# ---------- constants & weights ----------
DATA_DIR = "data"
PRICES_DIR = os.path.join(DATA_DIR, "prices")
OUT_DIR = "out"
os.makedirs(OUT_DIR, exist_ok=True)

WEIGHTS = {
    "trend_align": 15,     # Close>SMA20 & SMA50 + SMA20 slope up
    "pullback_hi55": 20,   # <=10d since HI55 and 2–8% below that HI55
    "nr7": 10,             # NR7 day compression
    "atr_low": 8,          # ATR% in 10–40th pctile
    "delivery_q4": 10,     # Delivery% in top quartile
    "insider_buy": 10,     # net insider positive in 45 days
    "bulk_buy": 8,         # any Bulk BUY in 15 days
    "block_buy": 5,        # any Block BUY in 15 days
    "hi52w_near": 10,      # within 2% of 52W high
    "hi52w_fresh_calm": 8, # fresh 52W with ATR% <=3%
    "liquidity": 5,        # Vol >= 0.8×VolAvg20
    # penalties
    "below_sma20": -20,    # trend violation
    "extended_bb": -15,    # Close > BB_Upper + 1×ATR (too stretched)
}

# recencies
DAYS_HI55    = 10
DAYS_BULK    = 15
DAYS_BLOCK   = 15
DAYS_INSIDER = 45

# pullback zone from recent HI55
PULLBACK_MIN = 0.92  # 8% below
PULLBACK_MAX = 0.98  # 2% below

def read_csv(path: str) -> Optional[pd.DataFrame]:
    try:
        return pd.read_csv(path)
    except Exception:
        return None

def pct_rank(s: pd.Series) -> pd.Series:
    return s.rank(pct=True, method="average")

def load_inputs() -> Dict[str, Optional[pd.DataFrame]]:
    hist  = read_csv(os.path.join(PRICES_DIR, "bhav_hist.csv"))
    tech  = read_csv(os.path.join(DATA_DIR, "tech_latest.csv"))
    hi52  = read_csv(os.path.join(DATA_DIR, "highlow_52w.csv"))
    deliv = read_csv(os.path.join(DATA_DIR, "deliverables_latest.csv"))
    bulk  = read_csv(os.path.join(DATA_DIR, "bulk_deals_latest.csv"))
    block = read_csv(os.path.join(DATA_DIR, "block_deals_latest.csv"))
    ins   = None
    # try multiple insider file names (be lenient)
    for cand in [
        "CF-Insider-Trading-equities-latest.csv",
        "cf-insider-trading-equities-latest.csv",
    ]:
        p = os.path.join(DATA_DIR, cand)
        if os.path.exists(p):
            ins = read_csv(p)
            break
    return dict(hist=hist, tech=tech, hi52=hi52, deliv=deliv, bulk=bulk, block=block, ins=ins)

def compute_recent_hi55(hist: pd.DataFrame) -> pd.DataFrame:
    """Add HI55_DATE and HI55_PX per symbol for the last bar of each symbol."""
    df = hist.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values(["Symbol","Date"])
    g = df.groupby("Symbol", group_keys=False)
    roll_hi55 = g["High"].rolling(55, min_periods=55).max().reset_index(level=0, drop=True)
    df["HI55_Flag"] = (df["High"] == roll_hi55)
    # last HI55 date & price per symbol (forward-fill)
    last_hi_date = df.groupby("Symbol")["Date"].apply(lambda s: s.where(df.loc[s.index, "HI55_Flag"]).ffill())
    # Build a helper to capture last HI55 on each row
    df["HI55_LastDate"] = pd.NaT
    df.loc[last_hi_date.index, "HI55_LastDate"] = last_hi_date.values
    df["HI55_LastDate"] = df.groupby("Symbol")["HI55_LastDate"].ffill()
    # HI55 price (the rolling max) — we can re-use roll_hi55 series aligned
    df["HI55_Px"] = roll_hi55
    # keep only the last row per symbol
    last = df.sort_values(["Symbol","Date"]).groupby("Symbol").tail(1)
    return last[["Symbol","Date","HI55_LastDate","HI55_Px"]]

def coerce_date(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if df is None or col not in df.columns: return df
    df[col] = pd.to_datetime(df[col], errors="coerce")
    return df

def tag_bulk_block_recent(df: pd.DataFrame, kind: str, today: pd.Timestamp) -> pd.DataFrame:
    """Return per-symbol flags of recent BUY presence within N days."""
    if df is None or df.empty: 
        return pd.DataFrame(columns=["Symbol", f"{kind}_recent_buy"])
    d = df.copy()
    # best-effort column picks
    cols = {c.lower(): c for c in d.columns}
    symc = cols.get("symbol") or list(d.columns)[0]
    d.rename(columns={symc: "Symbol"}, inplace=True)
    # date
    dtc = None
    for k in ["date","deal_date","traded_date"]:
        if k in cols: dtc = cols[k]; break
    if dtc is None:
        return pd.DataFrame(columns=["Symbol", f"{kind}_recent_buy"])
    d[dtc] = pd.to_datetime(d[dtc], errors="coerce")
    # buy/sell field
    bsc = None
    for k in ["buy_sell","buy_or_sell","buysell","deal_type"]:
        if k in cols: bsc = cols[k]; break
    if bsc is not None:
        d[bsc] = d[bsc].astype(str).str.upper()
    # consider BUY only
    if bsc is not None:
        d = d[d[bsc].str.contains("BUY", na=False)]
    # recency window
    horizon = DAYS_BULK if kind=="bulk" else DAYS_BLOCK
    recent = d[d[dtc] >= (today - pd.Timedelta(days=horizon))]
    flg = (recent.groupby("Symbol").size() > 0).rename(f"{kind}_recent_buy").reset_index()
    return flg

def tag_insider_recent(ins: Optional[pd.DataFrame], today: pd.Timestamp) -> pd.DataFrame:
    if ins is None or ins.empty:
        return pd.DataFrame(columns=["Symbol","insider_net_sign","insider_recent"])
    d = ins.copy()
    # sanitize columns
    low = {c.lower(): c for c in d.columns}
    symc = low.get("symbol") or low.get("security_symbol") or low.get("companysymbol") or list(d.columns)[0]
    d.rename(columns={symc: "Symbol"}, inplace=True)
    # date
    dtc = None
    for k in ["date","txn_dt","transaction_date","intimation_date","post_date"]:
        if k in low: dtc = low[k]; break
    if dtc is None:
        return pd.DataFrame(columns=["Symbol","insider_net_sign","insider_recent"])
    d[dtc] = pd.to_datetime(d[dtc], errors="coerce")

    # direction (BUY/SELL)
    dirc = None
    for k in ["transaction_type","mode","buy_sell","type","acq_disp","buy_or_sell"]:
        if k in low: dirc = low[k]; break
    sign = pd.Series(0, index=d.index)
    if dirc is not None:
        x = d[dirc].astype(str).str.lower()
        buy_mask = x.str.contains("acq|buy|purchase|allot", regex=True)
        sell_mask = x.str.contains("sell|sale|dispose|dispos", regex=True)
        sign = sign.mask(buy_mask, 1).mask(sell_mask, -1)
    # fallback: positive if pledged revocation/ESOP allotments (optional)
    # window
    recent = d[d[dtc] >= (today - pd.Timedelta(days=DAYS_INSIDER))].copy()
    grp = recent.groupby("Symbol")[sign.name if sign.name else dirc].sum() if dirc else recent.groupby("Symbol").size()
    if not isinstance(grp, pd.Series):
        grp = pd.Series(dtype=float)
    out = grp.rename("insider_net_sign").reset_index()
    out["insider_recent"] = (out["insider_net_sign"] > 0)
    return out[["Symbol","insider_net_sign","insider_recent"]]

def main():
    status = {"ok": False, "when": datetime.utcnow().isoformat()+"Z"}
    inp = load_inputs()
    tech = inp["tech"]; hist = inp["hist"]

    if tech is None or tech.empty or hist is None or hist.empty:
        status["error"] = "Missing tech_latest or bhav_hist"
        print(json.dumps(status, indent=2)); 
        return 1

    # parse dates
    tech["Date"] = pd.to_datetime(tech["Date"], errors="coerce")
    hist["Date"] = pd.to_datetime(hist["Date"], errors="coerce")
    latest_day = pd.to_datetime(tech["Date"].max())

    # enrich with HI55 recency
    hi_rec = compute_recent_hi55(hist)
    base = tech.merge(hi_rec[["Symbol","HI55_LastDate","HI55_Px"]], on="Symbol", how="left")
    base["DaysSinceHI55"] = (latest_day - pd.to_datetime(base["HI55_LastDate"])).dt.days

    # ATR%, delivery%
    base["ATR_pct"] = (base["ATR14"] / base["Close"]).replace([np.inf,-np.inf], np.nan)
    if "Delivery_Pct" not in base.columns:
        # try to merge from deliverables_latest
        deliv = inp["deliv"]
        if deliv is not None and not deliv.empty:
            deliv["Date"] = pd.to_datetime(deliv["Date"], errors="coerce")
            d1 = deliv[deliv["Date"] == latest_day][["Symbol","Delivery_Pct"]]
            base = base.merge(d1, on="Symbol", how="left")

    # percentiles (robust to NaN)
    base["ATR_pct_pct"] = pct_rank(base["ATR_pct"].fillna(base["ATR_pct"].median()))
    base["Delivery_pct_pct"] = pct_rank(base["Delivery_Pct"].fillna(base["Delivery_Pct"].median()))

    # corporate recency
    bulk_f = tag_bulk_block_recent(inp["bulk"], "bulk", latest_day)
    block_f= tag_bulk_block_recent(inp["block"], "block", latest_day)
    ins_f  = tag_insider_recent(inp["ins"], latest_day)

    base = base.merge(bulk_f, on="Symbol", how="left")
    base = base.merge(block_f, on="Symbol", how="left")
    base = base.merge(ins_f,   on="Symbol", how="left")

    base["bulk_recent_buy"]  = base["bulk_recent_buy"].fillna(False)
    base["block_recent_buy"] = base["block_recent_buy"].fillna(False)
    base["insider_recent"]   = base["insider_recent"].fillna(False)
    base["insider_net_sign"] = base["insider_net_sign"].fillna(0)

    # trend tests
    base["trend_align"] = (base["Close"] > base["SMA20"]) & (base["Close"] > base["SMA50"]) & (base["SMA20"].diff().fillna(0) >= 0)
    # pullback-zone vs HI55
    base["pullback_hi55"] = (
        (base["DaysSinceHI55"].between(0, DAYS_HI55, inclusive="both")) &
        (base["HI55_Px"].notna()) &
        (base["Close"] >= base["HI55_Px"] * PULLBACK_MIN) &
        (base["Close"] <= base["HI55_Px"] * PULLBACK_MAX)
    )
    # NR7 flag is in tech_latest (bool)
    base["nr7_flag"] = base.get("NR7", False).astype(bool)
    # ATR low band (10–40th pctile)
    base["atr_low"] = (base["ATR_pct_pct"].between(0.10, 0.40, inclusive="both"))
    # Delivery top quartile
    base["delivery_q4"] = (base["Delivery_pct_pct"] >= 0.75)
    # 52W proximity / calm breakout
    # If we have highlow_52w, use its High52W for proximity
    hi52 = inp["hi52"]
    if hi52 is not None and not hi52.empty and "High52W" in hi52.columns:
        prox = base[["Symbol","Close"]].merge(hi52[["Symbol","High52W"]], on="Symbol", how="left")
        base = base.merge(prox[["Symbol","High52W"]], on="Symbol", how="left", suffixes=("",""))
        base["hi52w_near"] = (base["High52W"].notna()) & ((base["High52W"] - base["Close"]) / base["High52W"] <= 0.02)
    else:
        base["hi52w_near"] = False

    base["hi52w_fresh_calm"] = (base.get("HI55_Recent", False).astype(bool)) & (base["ATR_pct"] <= 0.03)

    # Liquidity sanity
    base["liq_ok"] = (base["VolAvg20"] > 0) & (base["Volume"] >= 0.8 * base["VolAvg20"])

    # Overextended penalty (above BB upper by >1×ATR)
    base["extended_bb"] = (base["Close"] > (base["BB_Upper"] + base["ATR14"].fillna(0)))

    # Below SMA20 penalty
    base["below_sma20"] = (base["Close"] < base["SMA20"])

    # ----- score -----
    score = np.zeros(len(base), dtype=float)
    def add(mask, w): 
        nonlocal score
        score = score + (mask.astype(float) * w)

    add(base["trend_align"], WEIGHTS["trend_align"])
    add(base["pullback_hi55"], WEIGHTS["pullback_hi55"])
    add(base["nr7_flag"], WEIGHTS["nr7"])
    add(base["atr_low"], WEIGHTS["atr_low"])
    add(base["delivery_q4"], WEIGHTS["delivery_q4"])
    add(base["insider_recent"], WEIGHTS["insider_buy"])
    add(base["bulk_recent_buy"], WEIGHTS["bulk_buy"])
    add(base["block_recent_buy"], WEIGHTS["block_buy"])
    add(base["hi52w_near"], WEIGHTS["hi52w_near"])
    add(base["hi52w_fresh_calm"], WEIGHTS["hi52w_fresh_calm"])
    add(base["liq_ok"], WEIGHTS["liquidity"])
    # penalties
    add(base["below_sma20"], WEIGHTS["below_sma20"])
    add(base["extended_bb"], WEIGHTS["extended_bb"])

    base["R_SCORE"] = np.clip(score, 0, 100)

    # Why tags (compact, human)
    tags = []
    for i, r in base.iterrows():
        t = []
        if r["trend_align"]: t.append("TREND")
        if r["pullback_hi55"]: t.append("PULLBACK55")
        if r["nr7_flag"]: t.append("NR7")
        if r["atr_low"]: t.append("ATR_LOW")
        if r["delivery_q4"]: t.append("DELIV_Q4")
        if r["insider_recent"]: t.append("INSIDER+")
        if r["bulk_recent_buy"]: t.append("BULK+")
        if r["block_recent_buy"]: t.append("BLOCK+")
        if r["hi52w_near"]: t.append("NEAR_52W")
        if r["hi52w_fresh_calm"]: t.append("CALM_52W")
        if r["extended_bb"]: t.append("EXTENDED")
        if r["below_sma20"]: t.append("SUB_SMA20")
        tags.append(",".join(t))
    base["WHY"] = tags

    # Triggers (simple, rule-of-thumb)
    # Long setup: buy > max(High of last 2) with stop < min(Low of last 2); risk ~0.8*ATR
    # We only have last-day row here; we’ll derive “prior 2 bars” from hist
    # Build small side-lookup:
    hq = hist.sort_values(["Symbol","Date"])
    prev2 = hq.groupby("Symbol").tail(3).groupby("Symbol")  # last 3 days per symbol
    max2 = prev2["High"].apply(lambda s: s.iloc[:-1].max() if len(s)>=2 else np.nan)  # exclude last day
    min2 = prev2["Low"].apply(lambda s: s.iloc[:-1].min() if len(s)>=2 else np.nan)
    trig = pd.DataFrame({"Symbol": max2.index, "TrigHigh2": max2.values, "TrigLow2": min2.values})
    base = base.merge(trig, on="Symbol", how="left")
    base["Entry"] = np.where(base["TrigHigh2"].notna(), base["TrigHigh2"], base["High"])
    base["Stop"]  = np.where(base["TrigLow2"].notna(),  base["TrigLow2"],  base["Low"])
    base["Risk"]  = (base["Entry"] - base["Stop"]).clip(lower=0.5*base["ATR14"], upper=1.2*base["ATR14"]).round(2)
    base["Trigger"] = (
        "Go long if Close>Entry; Stop below " + base["Stop"].round(2).astype(str) +
        "; Risk≈" + base["Risk"].astype(str)
    )

    # Final select + sort
    cols = [
        "Date","Symbol","Close","ATR14","SMA20","SMA50","BB_Upper","BB_Lower",
        "Volume","VolAvg20","Delivery_Pct","DaysSinceHI55","HI55_Px","High52W",
        "R_SCORE","WHY","Trigger"
    ]
    # merge High52W if present
    if "High52W" not in base.columns and inp["hi52"] is not None and not inp["hi52"].empty:
        base = base.merge(inp["hi52"][["Symbol","High52W"]], on="Symbol", how="left")

    out = base.copy()
    # avoid KeyErrors if some columns missing
    for c in cols:
        if c not in out.columns:
            out[c] = np.nan

    out = out[cols].sort_values(["R_SCORE","Symbol"], ascending=[False, True])

    # save
    out_path = os.path.join(OUT_DIR, "WEEK_PACK.csv")
    out.to_csv(out_path, index=False)

    status.update({
        "ok": True,
        "latest_day": str(latest_day.date()),
        "rows": int(len(out)),
        "top5": out.head(5)["Symbol"].tolist(),
        "note": "R_SCORE clipped [0,100]; weights in script WEIGHTS{}.",
        "out": out_path
    })
    with open(os.path.join(DATA_DIR, "score_status.json"), "w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)
    print(json.dumps(status, indent=2))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
