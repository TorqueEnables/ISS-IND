#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
StakeLens — Weekly Scoring & Ranking (self-healing; zero network)

If inputs are missing/empty, this script will call process_eod.py to regenerate
derived files, then continue. Produces out/WEEK_PACK.csv and data/score_status.json.
"""

from __future__ import annotations
import os, json, subprocess
from datetime import datetime, timedelta
from typing import Optional, Dict
import pandas as pd
import numpy as np

DATA_DIR   = "data"
PRICES_DIR = os.path.join(DATA_DIR, "prices")
OUT_DIR    = "out"
STATUS_F   = os.path.join(DATA_DIR, "score_status.json")
os.makedirs(OUT_DIR, exist_ok=True)

# ---------- robust CSV reader ----------
def read_csv_robust(path: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(path): return None
    for opts in ({}, {"encoding":"utf-8-sig"}, {"encoding":"latin-1"}):
        try:
            return pd.read_csv(path, **opts)
        except Exception:
            pass
    return None

def rows_of(path: str) -> int:
    df = read_csv_robust(path)
    return 0 if df is None else int(len(df))

def ensure_inputs_or_heal(diag: dict) -> Dict[str, Optional[pd.DataFrame]]:
    """Return dict of dataframes; if missing/empty -> run process_eod.py -> reload."""
    need_heal = False
    paths = {
        "hist": os.path.join(PRICES_DIR, "bhav_hist.csv"),
        "tech": os.path.join(DATA_DIR, "tech_latest.csv"),
        "hi52": os.path.join(DATA_DIR, "highlow_52w.csv"),
        "deliv": os.path.join(DATA_DIR, "deliverables_latest.csv"),
        "bulk": os.path.join(DATA_DIR, "bulk_deals_latest.csv"),
        "block": os.path.join(DATA_DIR, "block_deals_latest.csv"),
    }
    diag["inputs_seen"] = {k: (paths[k] if os.path.exists(paths[k]) else None) for k in paths}
    diag["input_rows"]  = {k: rows_of(paths[k]) if os.path.exists(paths[k]) else 0 for k in paths}

    # heal if critical inputs are missing/empty
    if diag["input_rows"]["hist"] == 0 or diag["input_rows"]["tech"] == 0:
        need_heal = True

    if need_heal:
        try:
            subprocess.run(["python","scripts/process_eod.py"], check=False, timeout=120)
        except Exception as e:
            diag["heal_error"] = f"{type(e).__name__}: {e}"

        # reload row counts
        diag["input_rows_after_heal"] = {k: rows_of(paths[k]) if os.path.exists(paths[k]) else 0 for k in paths}

    # finally load dfs
    dfs = {k: read_csv_robust(p) for k, p in paths.items()}
    # attempt insider latest by common names
    ins = None
    for nm in ["CF-Insider-Trading-equities-latest.csv","cf-insider-trading-equities-latest.csv"]:
        p = os.path.join(DATA_DIR, nm)
        if os.path.exists(p):
            ins = read_csv_robust(p)
            break
    dfs["ins"] = ins
    return dfs

# ---------- helpers ----------
def pct_rank(s: pd.Series) -> pd.Series:
    return s.rank(pct=True, method="average")

def compute_recent_hi55(hist: pd.DataFrame) -> pd.DataFrame:
    df = hist.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values(["Symbol","Date"])
    g = df.groupby("Symbol", group_keys=False)
    roll_hi55 = g["High"].rolling(55, min_periods=55).max().reset_index(level=0, drop=True)
    df["HI55_Flag"] = (df["High"] == roll_hi55)
    # last row per symbol (contains max date + aligned rolling max for that row)
    last = df.groupby("Symbol", as_index=False).tail(1).copy()
    last["HI55_Px"] = roll_hi55.groupby(df["Symbol"]).tail(1).values
    # find last HI55 date per symbol
    hi_idx = df[df["HI55_Flag"]].groupby("Symbol")["Date"].max().rename("HI55_LastDate")
    last = last.merge(hi_idx, on="Symbol", how="left")
    return last[["Symbol","Date","HI55_LastDate","HI55_Px"]]

# ---------- main ----------
def main():
    status = {"ok": False, "when": datetime.utcnow().isoformat()+"Z", "note": ""}

    diag = {}
    dfs = ensure_inputs_or_heal(diag)
    tech = dfs["tech"]; hist = dfs["hist"]

    if tech is None or hist is None or tech.empty or hist.empty:
        status.update({
            "ok": False,
            "error": "Missing tech_latest or bhav_hist after heal",
            "diag": diag
        })
        with open(STATUS_F, "w", encoding="utf-8") as f: json.dump(status, f, indent=2)
        print(json.dumps(status, indent=2))
        return 1

    # parse dates & sort
    tech["Date"] = pd.to_datetime(tech["Date"], errors="coerce")
    hist["Date"] = pd.to_datetime(hist["Date"], errors="coerce")
    tech = tech.dropna(subset=["Date","Symbol"]).copy()
    hist = hist.dropna(subset=["Date","Symbol"]).copy()
    latest_day = pd.to_datetime(tech["Date"].max())

    # enrich with HI55 recency
    hi_rec = compute_recent_hi55(hist)
    base = tech.merge(hi_rec[["Symbol","HI55_LastDate","HI55_Px"]], on="Symbol", how="left")
    base["DaysSinceHI55"] = (latest_day - pd.to_datetime(base["HI55_LastDate"])).dt.days

    # ATR% & Delivery% percentiles
    base["ATR_pct"] = (base["ATR14"] / base["Close"]).replace([np.inf,-np.inf], np.nan)
    base["ATR_pct_pct"] = pct_rank(base["ATR_pct"].fillna(base["ATR_pct"].median()))
    if "Delivery_Pct" not in base.columns:
        d = dfs["deliv"]
        if d is not None and not d.empty:
            d["Date"] = pd.to_datetime(d["Date"], errors="coerce")
            base = base.merge(d[d["Date"]==latest_day][["Symbol","Delivery_Pct"]], on="Symbol", how="left")
    base["Delivery_pct_pct"] = pct_rank(base["Delivery_Pct"].fillna(base["Delivery_Pct"].median()))

    # corporate recency (coarse flags)
    def recent_flag(df: Optional[pd.DataFrame], date_col_guess=("date","deal_date","traded_date"), days=15):
        if df is None or df.empty: return pd.DataFrame(columns=["Symbol","flag"])
        t = df.copy()
        lc = {c.lower():c for c in t.columns}
        sym = lc.get("symbol", list(t.columns)[0])
        t.rename(columns={sym:"Symbol"}, inplace=True)
        dc = next((lc[k] for k in date_col_guess if k in lc), None)
        if dc is None: return pd.DataFrame(columns=["Symbol","flag"])
        t[dc] = pd.to_datetime(t[dc], errors="coerce")
        recent = t[t[dc] >= (latest_day - pd.Timedelta(days=days))]
        return (recent.groupby("Symbol").size()>0).rename("flag").reset_index()

    bulk_f  = recent_flag(dfs["bulk"], days=15).rename(columns={"flag":"bulk_recent_buy"})
    block_f = recent_flag(dfs["block"], days=15).rename(columns={"flag":"block_recent_buy"})
    ins_f   = None
    if dfs["ins"] is not None and not dfs["ins"].empty:
        d = dfs["ins"].copy()
        lc = {c.lower():c for c in d.columns}
        sym = lc.get("symbol", list(d.columns)[0]); d.rename(columns={sym:"Symbol"}, inplace=True)
        dtc = next((lc[k] for k in ("date","txn_dt","transaction_date","intimation_date","post_date") if k in lc), None)
        if dtc:
            d[dtc] = pd.to_datetime(d[dtc], errors="coerce")
            recent = d[d[dtc] >= (latest_day - pd.Timedelta(days=45))]
            ins_f = (recent.groupby("Symbol").size()>0).rename("insider_recent").reset_index()
    if ins_f is None:
        ins_f = pd.DataFrame(columns=["Symbol","insider_recent"])

    base = base.merge(bulk_f, on="Symbol", how="left")
    base = base.merge(block_f, on="Symbol", how="left")
    base = base.merge(ins_f,   on="Symbol", how="left")
    for c in ("bulk_recent_buy","block_recent_buy","insider_recent"):
        base[c] = base[c].fillna(False)

    # trend & penalties
    base["trend_align"] = (base["Close"]>base["SMA20"]) & (base["Close"]>base["SMA50"]) & (base["SMA20"].diff().fillna(0)>=0)
    base["nr7_flag"]    = base.get("NR7", False).astype(bool)
    base["atr_low"]     = base["ATR_pct_pct"].between(0.10, 0.40, inclusive="both")
    base["delivery_q4"] = (base["Delivery_pct_pct"]>=0.75)
    base["hi52w_fresh_calm"] = (base.get("HI55_Recent", False).astype(bool)) & (base["ATR_pct"]<=0.03)
    base["liq_ok"]      = (base["VolAvg20"]>0) & (base["Volume"]>=0.8*base["VolAvg20"])
    base["extended_bb"] = (base["Close"]> (base["BB_Upper"] + base["ATR14"].fillna(0)))
    base["below_sma20"] = (base["Close"]< base["SMA20"])

    # score (weights kept inside for compactness)
    W = dict(trend_align=15,pullback_hi55=20,nr7=10,atr_low=8,delivery_q4=10,
             insider_buy=10,bulk_buy=8,block_buy=5,hi52w_near=10,hi52w_fresh_calm=8,liquidity=5,
             below_sma20=-20,extended_bb=-15)

    # HI55 pullback + near-52w
    base["pullback_hi55"] = (
        (base["DaysSinceHI55"].between(0,10, inclusive="both")) &
        base["HI55_Px"].notna() &
        (base["Close"]>= base["HI55_Px"]*0.92) &
        (base["Close"]<= base["HI55_Px"]*0.98)
    )
    # if we have High52W merge, otherwise set False
    if "High52W" not in base.columns:
        hi = read_csv_robust(os.path.join(DATA_DIR,"highlow_52w.csv"))
        if hi is not None and not hi.empty and "High52W" in hi.columns:
            base = base.merge(hi[["Symbol","High52W"]], on="Symbol", how="left")
    base["hi52w_near"] = base["High52W"].notna() & ((base["High52W"]-base["Close"])/base["High52W"] <= 0.02)

    score = np.zeros(len(base), dtype=float)
    def add(mask, w): 
        nonlocal score
        score = score + (mask.astype(float)*w)
    add(base["trend_align"], W["trend_align"])
    add(base["pullback_hi55"], W["pullback_hi55"])
    add(base["nr7_flag"], W["nr7"])
    add(base["atr_low"], W["atr_low"])
    add(base["delivery_q4"], W["delivery_q4"])
    add(base["insider_recent"], W["insider_buy"])
    add(base["bulk_recent_buy"], W["bulk_buy"])
    add(base["block_recent_buy"], W["block_buy"])
    add(base["hi52w_near"], W["hi52w_near"])
    add(base["hi52w_fresh_calm"], W["hi52w_fresh_calm"])
    add(base["liq_ok"], W["liquidity"])
    add(base["below_sma20"], W["below_sma20"])
    add(base["extended_bb"], W["extended_bb"])
    base["R_SCORE"] = np.clip(score, 0, 100)

    # WHY + Trigger
    tags = []
    for _, r in base.iterrows():
        t=[]
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

    # Two-bar trigger
    hq = hist.sort_values(["Symbol","Date"])
    prev = hq.groupby("Symbol").tail(3).groupby("Symbol")
    max2 = prev["High"].apply(lambda s: s.iloc[:-1].max() if len(s)>=2 else np.nan)
    min2 = prev["Low"].apply(lambda s: s.iloc[:-1].min() if len(s)>=2 else np.nan)
    trig = pd.DataFrame({"Symbol": max2.index, "TrigHigh2": max2.values, "TrigLow2": min2.values})
    base = base.merge(trig, on="Symbol", how="left")
    base["Entry"] = np.where(base["TrigHigh2"].notna(), base["TrigHigh2"], base["High"])
    base["Stop"]  = np.where(base["TrigLow2"].notna(),  base["TrigLow2"],  base["Low"])
    base["Risk"]  = (base["Entry"] - base["Stop"]).clip(lower=0.5*base["ATR14"], upper=1.2*base["ATR14"]).round(2)
    base["Trigger"] = (
        "Go long if Close>Entry; Stop below " + base["Stop"].round(2).astype(str) +
        "; Risk≈" + base["Risk"].astype(str)
    )

    # Output
    cols = ["Date","Symbol","Close","ATR14","SMA20","SMA50","BB_Upper","BB_Lower",
            "Volume","VolAvg20","Delivery_Pct","DaysSinceHI55","HI55_Px","High52W",
            "R_SCORE","WHY","Trigger"]
    for c in cols:
        if c not in base.columns: base[c]=np.nan
    out = base[cols].sort_values(["R_SCORE","Symbol"], ascending=[False, True])
    out_path = os.path.join(OUT_DIR, "WEEK_PACK.csv")
    out.to_csv(out_path, index=False)

    status.update({
        "ok": True,
        "when": datetime.utcnow().isoformat()+"Z",
        "latest_day": str(pd.to_datetime(base["Date"].max()).date()),
        "rows": int(len(out)),
        "top5": out.head(5)["Symbol"].tolist(),
        "out": out_path,
        "diag": diag
    })
    with open(STATUS_F, "w", encoding="utf-8") as f: json.dump(status, f, indent=2)
    print(json.dumps(status, indent=2))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
