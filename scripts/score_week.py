#!/usr/bin/env python3
"""
StakeLens Insider — Weekly scorer with Sector Advantage + Macro Regimes.
Now includes Plan-B (Reclaim) and Plan-C (Inside-day) entry options and
a volatility/tick-aware breakout buffer, plus flow-aware scoring using
insider trades, bulk/block deals, and delivery strength.

Optional inputs (auto-skip if missing):
- ref/symbol_sector.csv   -> columns: Symbol,Sector
- data/index/NIFTY.csv    -> Date,Open,High,Low,Close (daily)
- data/index/INDIAVIX.csv -> Date,Close (daily)
"""

import argparse, re, os
from datetime import timedelta
from pathlib import Path
import numpy as np
import pandas as pd

# ---------------- Core thresholds ----------------
MIN_CLOSE            = 50.0
TURNOVER_CR_20_FLOOR = 5.0   # ₹ cr (20d median)
DELIV_CR_20_FLOOR    = 2.0   # ₹ cr (20d median)
ATR_PCT_MIN          = 0.02  # 2%
ATR_PCT_MAX          = 0.10  # 10% (slightly wider to include power names)
TOP_LIMIT            = 50

# Go/No-Go (base)
BREADTH20_MIN        = 0.45  # % of universe above 20DMA
PROX_ATR_MAX         = 0.80  # legacy (not directly used now; kept for compat)
CLOSE_LOC_MIN        = 0.60  # close in top 40% of daily range
UPVOL3_RATIO_MIN     = 1.20  # 3d up-vol / down-vol
SQUEEZE_MIN_RS       = 0.60  # RS continuation requires some coil

# Sector Advantage (SAP)
SECTOR_BREADTH_BONUS = 0.10  # sector breadth must exceed market breadth by ≥10pp
SECTOR_BREADTH_MIN   = 0.55  # or hit an absolute 55% above 20DMA
SECTOR_RS_Z_MIN      = 0.00  # sector median 4W RS z ≥ 0

# Dispersion relief
DISPERSION_4W_MIN    = 0.08  # if stdev of 4W returns across universe ≥ 8%, relax breadth rule

# Macro toggles (only applied if optional files available)
VIX_TIGHTEN_PCTL     = 0.80  # if VIX ≥ 80th pct and NIFTY < 50DMA, tighten rules
PROX_ATR_TIGHT       = 0.50  # legacy (unused now)
UPVOL3_RATIO_TIGHT   = 1.40  # stronger power requirement in high-vol regime

# Dynamic proximity caps & hybrid buffers
PROX_ATR_BASE        = 0.30  # baseline cap when tape is neutral/soft
PROX_ATR_RISK_ON     = 0.45  # allow farther when breadth/dispersion are strong
PROX_ATR_HIGHVOL     = 0.25  # tighten further in high-vol regime
HYBRID_BUF_ATR       = 0.15  # 15% of ATR as part of breakout buffer
HYBRID_BUF_PCT       = 0.0015 # 0.15% of price as minimal nudge
RECLAIM_CLOSELOC_MIN = 0.60
RECLAIM_UPVOL3_MIN   = 1.20

ETF_REGEX = re.compile(
    r'(ETF|BEES|MOM|GOLD|SILVER|CPSE|PSU|FUND|FOF|NIFTY|SENSEX|JUNIOR|NEXT|TRI)$',
    re.IGNORECASE
)

# ---------------- Utilities ----------------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--hist", default="data/prices/bhav_hist.csv")
    p.add_argument("--out",  default="out/WEEK_PACK.csv")
    p.add_argument("--min_close", type=float, default=MIN_CLOSE)
    p.add_argument("--turnover_cr_20", type=float, default=TURNOVER_CR_20_FLOOR)
    p.add_argument("--deliv_cr_20", type=float, default=DELIV_CR_20_FLOOR)
    p.add_argument("--atr_min", type=float, default=ATR_PCT_MIN)
    p.add_argument("--atr_max", type=float, default=ATR_PCT_MAX)
    p.add_argument("--top", type=int, default=TOP_LIMIT)
    return p.parse_args()

def pick(cands, cols):
    for c in cands:
        if c in cols: return c
    return None

def _pick_flex(cols, names):
    """
    Case-insensitive fuzzy column chooser for external CSVs (PIT / Bulk / Block),
    tolerant to NSE schema drift.
    """
    cols_list = list(cols)
    lower = {c.lower(): c for c in cols_list}
    names_lower = [n.lower() for n in names]
    # Exact case-insensitive match first
    for n in names_lower:
        if n in lower:
            return lower[n]
    # Fallback: substring search
    for c in cols_list:
        cl = c.lower()
        for n in names_lower:
            if n in cl:
                return c
    return None

def to_num(s):
    if s.dtype == "O":
        s = s.astype(str).str.replace(",", "", regex=False).str.replace(" ", "", regex=False)
    return pd.to_numeric(s, errors="coerce")

def zscore(x: pd.Series):
    mu = x.mean(); sd = x.std(ddof=0)
    if not np.isfinite(sd) or sd == 0: return (x*0).fillna(0)
    return (x - mu) / sd

def bb_width_20(s):
    ma = s.rolling(20, min_periods=20).mean()
    sd = s.rolling(20, min_periods=20).std(ddof=0)
    return (ma.add(2*sd) - (ma.sub(2*sd))) / ma

def percentile_of_last(series: pd.Series, lookback=120):
    s = series.dropna()
    if s.empty: return np.nan
    tail = s.tail(lookback)
    last = tail.iloc[-1]
    return float((tail <= last).sum()) / float(len(tail))

def try_read_csv(path: Path, parse_date_col="Date"):
    if not path.exists(): return None
    try:
        df = pd.read_csv(path, parse_dates=[parse_date_col])
        return df
    except Exception:
        return None

def tick_for_price(p: float) -> float:
    # NSE equity default tick is ₹0.05 for most scrips. Keep simple and deterministic.
    return 0.05

def hybrid_breakout_buffer(close: float, high: float, atr: float) -> float:
    """Volatility-smart + tick-aware buffer above High."""
    t   = tick_for_price(close)
    base = max(HYBRID_BUF_ATR*atr, HYBRID_BUF_PCT*close, 0.0)
    if close < 100:
        base = max(base, 10*t, 0.20*atr)
    elif close < 500:
        base = max(base, 5*t)
    # ≥500: ATR term dominates naturally
    return base

# ---------------- Flow & penalty helpers ----------------
def attach_insider_flags(last: pd.DataFrame) -> pd.DataFrame:
    """
    INSIDER_PLUS:
        1 if net insider direction over the last ~30 days is positive, else 0.
    """
    last["INSIDER_PLUS"] = 0
    pit_path = Path("data/CF-Insider-Trading-equities-latest.csv")
    if not pit_path.exists():
        return last

    pit = pd.read_csv(pit_path)
    if pit.empty:
        return last

    sym_col  = _pick_flex(pit.columns, ["symbol", "security_symbol", "ticker"])
    date_col = _pick_flex(pit.columns, ["date", "txn_dt", "transaction_date", "intimation_date"])
    dir_col  = _pick_flex(pit.columns, ["transaction_type", "mode", "buy_sell", "type"])

    if not sym_col or not date_col or not dir_col:
        return last  # fail soft; schema unexpected

    pit[date_col] = pd.to_datetime(pit[date_col], errors="coerce")
    if pit[date_col].max() is pd.NaT:
        return last

    cutoff = pit[date_col].max() - timedelta(days=30)
    recent = pit[pit[date_col] >= cutoff].copy()
    if recent.empty:
        return last

    d = recent[dir_col].astype(str).str.lower()
    recent["dir_sign"] = 0
    recent.loc[d.str.contains("buy") | d.str.contains("acq"), "dir_sign"] = 1
    recent.loc[d.str.contains("sell") | d.str.contains("disp"), "dir_sign"] = -1

    net = (
        recent
        .groupby(sym_col)["dir_sign"]
        .sum()
        .rename("InsiderNet30")
    )

    last = last.merge(net, left_on="Symbol", right_index=True, how="left")
    last["INSIDER_PLUS"] = (last["InsiderNet30"] > 0).astype(int).fillna(0)
    return last

def _recent_presence_flag(path: Path, last: pd.DataFrame, flag_name: str, days: int = 40) -> pd.DataFrame:
    last[flag_name] = 0
    if not path.exists():
        return last

    deals = pd.read_csv(path)
    if deals.empty:
        return last

    sym_col  = _pick_flex(deals.columns, ["symbol", "scrip", "security"])
    date_col = _pick_flex(deals.columns, ["date", "deal_date", "traded_date"])

    if not sym_col or not date_col:
        return last

    deals[date_col] = pd.to_datetime(deals[date_col], errors="coerce")
    if deals[date_col].max() is pd.NaT:
        return last

    cutoff = deals[date_col].max() - timedelta(days=days)
    recent = deals[deals[date_col] >= cutoff]
    if recent.empty:
        return last

    has_deal = (
        recent.dropna(subset=[sym_col])
              .groupby(sym_col)[date_col]
              .size()
              .rename(flag_name)
              .clip(lower=1)
    )

    last = last.merge(has_deal, left_on="Symbol", right_index=True, how="left")
    last[flag_name] = (last[flag_name] > 0).astype(int).fillna(0)
    return last

def attach_bulk_block_flags(last: pd.DataFrame) -> pd.DataFrame:
    last = _recent_presence_flag(Path("data/bulk_deals_latest.csv"),  last, "BULK_PLUS")
    last = _recent_presence_flag(Path("data/block_deals_latest.csv"), last, "BLOCK_PLUS")
    return last

def attach_flow_and_penalty_features(last: pd.DataFrame) -> pd.DataFrame:
    """
    Add:
        DELIV_Q4     – delivery value in top quartile cross-sectionally (last day)
        INSIDER_PLUS – recent insider net buy
        BULK_PLUS / BLOCK_PLUS – recent presence in bulk/block deals
        BELOW_SMA20  – Close below 20-DMA (trend penalty)
        EXTENDED_BB  – stretched: very strong close + high ATR% (extension penalty)
    """
    # Delivery strength (value, not %; robust to NSE schema quirks)
    if "DelivValCr" in last.columns:
        if last["DelivValCr"].notna().any():
            q75 = last["DelivValCr"].dropna().quantile(0.75)
        else:
            q75 = np.nan
        if np.isfinite(q75):
            last["DELIV_Q4"] = ((last["DelivValCr"] >= q75) & last["DelivValCr"].notna()).astype(int)
        else:
            last["DELIV_Q4"] = 0
    else:
        last["DELIV_Q4"] = 0

    # Insider / Bulk / Block
    last = attach_insider_flags(last)
    last = attach_bulk_block_flags(last)

    # Penalties: trend + extension (approximate BB extension)
    last["BELOW_SMA20"] = (last["Close"] < last["SMA20"]).astype(int)
    # Extended = very strong close in range AND high ATR%
    last["EXTENDED_BB"] = ((last["CloseLoc"] >= 0.9) & (last["ATR_PCT"] >= 0.08)).astype(int)
    return last

# ---------------- Main ----------------
def main():
    args = parse_args()
    out_path  = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    report_path = Path("out/WEEK_GATE_REPORT.md")

    hist_path = Path(args.hist)
    if not hist_path.exists(): raise SystemExit(f"Missing {hist_path}")

    df = pd.read_csv(hist_path, parse_dates=["Date"])
    if df.empty: raise SystemExit("bhav_hist.csv is empty")

    # Map columns
    cols = df.columns
    sym   = pick(["Symbol","SYMBOL","symbol"], cols)
    ser   = pick(["Series","SERIES"], cols)
    open_ = pick(["Open","OPEN"], cols)
    high  = pick(["High","HIGH"], cols)
    low   = pick(["Low","LOW"], cols)
    close = pick(["Close","CLOSE","LAST"], cols)
    prev  = pick(["PrevClose","PREVCLOSE"], cols)
    vol   = pick(["Volume","TOTTRDQTY"], cols)
    turn  = pick(["Turnover","TOTTRDVAL"], cols)
    dqty  = pick(["DelivQty","DELIV_QTY","DELIVQTY","DeliverableQty"], cols)
    need = [sym, ser, open_, high, low, close, prev, vol, turn]
    if any(c is None for c in need):
        raise SystemExit(f"bhav_hist.csv missing required columns; got {cols.tolist()}")

    keep = ["Date", sym, ser, open_, high, low, close, prev, vol, turn] + ([dqty] if dqty else [])
    df = df[keep].copy()
    df.columns = ["Date","Symbol","Series","Open","High","Low","Close","PrevClose","Volume","Turnover"] + (["DelivQty"] if dqty else [])
    df.sort_values(["Symbol","Date"], inplace=True)

    # Coerce numerics
    for c in ["Open","High","Low","Close","PrevClose","Volume","Turnover"]:
        df[c] = to_num(df[c])
    if "DelivQty" in df.columns:
        df["DelivQty"] = to_num(df["DelivQty"])

    last_day = df["Date"].max()

    # Universe hygiene
    snap = df[df["Date"]==last_day].copy()
    def is_equity_row(row):
        if str(row["Series"]).upper() != "EQ": return False
        if not np.isfinite(row["Close"]) or row["Close"] < args.min_close: return False
        if ETF_REGEX.search(str(row["Symbol"])): return False
        return True
    snap["UNIVERSE_OK"] = snap.apply(is_equity_row, axis=1)
    keep_syms = set(snap.loc[snap["UNIVERSE_OK"], "Symbol"])
    total_syms = snap["Symbol"].nunique()
    uni_syms   = len(keep_syms)

    df = df[df["Symbol"].isin(keep_syms)].copy()
    g  = df.groupby("Symbol", group_keys=False)

    # PrevHigh/PrevLow for inside-day logic
    df["PrevHigh"] = g["High"].shift(1)
    df["PrevLow"]  = g["Low"].shift(1)

    # Turnover to ₹ cr (auto-detect units; fallback to price*volume)
    df["TurnoverCr_est"] = (df["Close"] * df["Volume"]) / 1e7
    guess_rupees_cr = df["Turnover"] / 1e7
    guess_lakhs_cr  = df["Turnover"] / 100.0
    mask = (df["Turnover"] > 0) & (df["TurnoverCr_est"] > 0)
    def med_abs_log_ratio(a, b):
        r = (a[mask] / b[mask]).replace([np.inf, -np.inf], np.nan).dropna()
        if r.empty: return np.inf
        return np.median(np.abs(np.log(r)))
    err_r = med_abs_log_ratio(guess_rupees_cr, df["TurnoverCr_est"])
    err_l = med_abs_log_ratio(guess_lakhs_cr,  df["TurnoverCr_est"])
    chosen = "lakhs→cr (/100)" if err_l < err_r else "rupees→cr (/1e7)"
    df["TurnoverCr_raw"] = guess_lakhs_cr if err_l < err_r else guess_rupees_cr
    df.loc[~(df["TurnoverCr_raw"] > 0), "TurnoverCr_raw"] = df.loc[~(df["TurnoverCr_raw"] > 0), "TurnoverCr_est"]

    # Liquidity medians
    df["TurnoverCr_med20"] = g["TurnoverCr_raw"].transform(lambda s: s.rolling(20, min_periods=10).median())
    if "DelivQty" in df.columns:
        df["DelivValCr"] = (df["DelivQty"] * df["Close"]) / 1e7
        df["DelivValCr_med20"] = g["DelivValCr"].transform(lambda s: s.rolling(20, min_periods=10).median())
    else:
        df["DelivValCr_med20"] = np.nan

    # Technicals & structure
    df["SMA10"]  = g["Close"].transform(lambda s: s.rolling(10, min_periods=10).mean())
    df["SMA20"]  = g["Close"].transform(lambda s: s.rolling(20, min_periods=10).mean())
    df["SMA50"]  = g["Close"].transform(lambda s: s.rolling(50, min_periods=25).mean())
    df["SMA200"] = g["Close"].transform(lambda s: s.rolling(200, min_periods=100).mean())

    # ATR (vectorised)
    pc = df.groupby("Symbol")["Close"].shift(1)
    tr1 = df["High"] - df["Low"]
    tr2 = (df["High"] - pc).abs()
    tr3 = (df["Low"]  - pc).abs()
    df["TR"] = np.maximum.reduce([tr1, tr2, tr3])
    df["ATR14"] = df.groupby("Symbol")["TR"].transform(lambda s: s.rolling(14, min_periods=10).mean())
    df["ATR_PCT"]= df["ATR14"] / df["Close"]

    rng = (df["High"] - df["Low"]).replace(0, np.nan)
    df["CloseLoc"] = ((df["Close"] - df["Low"]) / rng).clip(0,1)

    # BB width & percentile
    df["BBWidth20"] = g["Close"].transform(bb_width_20)
    pct_rows = []
    for symb, sub in df.groupby("Symbol"):
        pct_rows.append((symb, percentile_of_last(sub["BBWidth20"], 120)))
    pct_df = pd.DataFrame(pct_rows, columns=["Symbol","BBWidth_pctile_20"])

    # RS
    df["RET_20"] = g["Close"].transform(lambda s: s.pct_change(20))
    df["RET_65"] = g["Close"].transform(lambda s: s.pct_change(65))
    last_rs = df[df["Date"]==last_day][["Symbol","RET_20","RET_65"]].copy()
    last_rs["RS_4W_Z"]  = zscore(last_rs["RET_20"].fillna(0))
    last_rs["RS_13W_Z"] = zscore(last_rs["RET_65"].fillna(0))

    # Volume signals (vectorised)
    df["UpBar"]   = df["Close"] > df["PrevClose"]
    df["DownBar"] = df["Close"] < df["PrevClose"]
    upv   = (df["UpBar"].astype(int)   * df["Volume"])
    downv = (df["DownBar"].astype(int) * df["Volume"])
    df["UpVol10"]   = upv.groupby(df["Symbol"]).transform(lambda s: s.rolling(10, min_periods=10).sum())
    df["DownVol10"] = downv.groupby(df["Symbol"]).transform(lambda s: s.rolling(10, min_periods=10).sum())
    df["UpVol3"]    = upv.groupby(df["Symbol"]).transform(lambda s: s.rolling(3,  min_periods=3 ).sum())
    df["DownVol3"]  = downv.groupby(df["Symbol"]).transform(lambda s: s.rolling(3,  min_periods=3 ).sum())

    # Snapshot (last day) ---------------------------------------------
    base_cols = [
        "Symbol","Series","Open","High","Low","Close","PrevClose",
        "SMA10","SMA20","SMA50","SMA200","ATR14","ATR_PCT",
        "BBWidth20","UpVol10","DownVol10","UpVol3","DownVol3",
        "TurnoverCr_med20","DelivValCr_med20","CloseLoc"
    ]
    opt_cols = []
    if "DelivValCr" in df.columns:
        opt_cols.append("DelivValCr")
    last = df[df["Date"]==last_day][base_cols + opt_cols].copy()
    if "DelivValCr" not in last.columns:
        last["DelivValCr"] = np.nan
    last = last.merge(pct_df, on="Symbol", how="left")
    last = last.merge(last_rs[["Symbol","RS_4W_Z","RS_13W_Z"]], on="Symbol", how="left")

    # ---- Derived fields needed later (define BEFORE PocketPivot10) ----
    last["UpVolDom10"]  = (last["UpVol10"] > last["DownVol10"]).fillna(False)
    last["UpVol3Ratio"] = (last["UpVol3"] / last["DownVol3"]).replace([np.inf, -np.inf], np.nan)
    last["SqueezeScore"] = (1.0 - last["BBWidth_pctile_20"].clip(0,1))
    last["MA_Aligned"]   = ((last["SMA20"] > last["SMA50"]) & (last["SMA50"] > last["SMA200"]))

    # ---- Inside-day detection on last bar (bring PrevHigh/PrevLow once) ----
    prev_hl = df[df["Date"] == last_day][["Symbol", "PrevHigh", "PrevLow"]].copy()
    last = last.merge(prev_hl, on="Symbol", how="left")
    last["InsideDay"]  = (last["High"] < last["PrevHigh"]) & (last["Low"] > last["PrevLow"])
    last["InsideDay"]  = last["InsideDay"].fillna(False)
    last["InsideHigh"] = last["High"]
    last["InsideLow"]  = last["Low"]

    # Near 52w proxy + breakout
    gmax = g["Close"].transform(lambda s: s.rolling(252, min_periods=60).max())
    mx   = df[df["Date"]==last_day][["Symbol","Close"]].copy()
    mx["MaxClose252"] = gmax[df["Date"]==last_day].values
    mx["Near52w"]     = (mx["Close"] >= 0.97 * mx["MaxClose252"])
    last = last.merge(mx[["Symbol","Near52w","MaxClose252"]], on="Symbol", how="left")
    roll_hi_40 = g["High"].transform(lambda s: s.rolling(40, min_periods=25).max())
    brk = df[df["Date"]==last_day][["Symbol","Close"]].copy()
    brk["Hi40"]     = roll_hi_40[df["Date"]==last_day].values
    brk["Breakout"] = (brk["Close"] >= 1.01 * brk["Hi40"])
    last = last.merge(brk[["Symbol","Breakout","Hi40"]], on="Symbol", how="left")

    # Families
    last["PocketPivot10"] = (
        (df[df["Date"]==last_day]["UpBar"].values.astype(bool)) &
        last["UpVolDom10"] & (last["Close"] > last["SMA10"])
    ).astype(int)
    last["FAM_RS"] = (
        (last["RS_4W_Z"] >= 0.5) &
        (last["RS_13W_Z"] >= 0.0) &
        (last["Near52w"]) &
        ((last["SqueezeScore"] >= SQUEEZE_MIN_RS) | (last["PocketPivot10"]==1))
    )
    last["FAM_PULL"] = ((last["MA_Aligned"]) & (last["Close"] <= last["SMA10"]) & (last["Close"] >= last["SMA20"]))
    last["FAM_BB"]   = (last["Breakout"] & (last["SqueezeScore"] >= 0.7))
    last["ANY_FAM"]  = last["FAM_RS"] | last["FAM_PULL"] | last["FAM_BB"]

    # ---- Flow & penalties on last snapshot ----
    last = last.merge(snap[["Symbol","UNIVERSE_OK"]], on="Symbol", how="left")
    last = attach_flow_and_penalty_features(last)

    # Baseline gates
    last["LQ_TURN_OK"]  = (last["TurnoverCr_med20"] >= args.turnover_cr_20)
    last["LQ_DELIV_OK"] = (last["DelivValCr_med20"] >= args.deliv_cr_20) | last["DelivValCr_med20"].isna()
    last["LQ_OK"]       = last["LQ_TURN_OK"] & last["LQ_DELIV_OK"]
    last["ATR_OK"]      = (last["ATR_PCT"] >= args.atr_min) & (last["ATR_PCT"] <= args.atr_max)

    # ---- Market breadth & dispersion ----
    snap_ma = df[df["Date"]==last_day][["Symbol","Close"]].merge(
        df[df["Date"]==last_day][["Symbol","SMA20"]], on="Symbol", how="left"
    ).merge(snap[["Symbol","UNIVERSE_OK"]], on="Symbol", how="left")
    breadth20 = float((snap_ma.query("UNIVERSE_OK == True")["Close"] > snap_ma.query("UNIVERSE_OK == True")["SMA20"]).mean())
    if not np.isfinite(breadth20): breadth20 = 0.0
    # dispersion: stdev of 4W returns among universe
    disp4w = float(df[df["Date"]==last_day].merge(snap[["Symbol","UNIVERSE_OK"]], on="Symbol")\
                   .query("UNIVERSE_OK == True")["RET_20"].std(ddof=0))
    if not np.isfinite(disp4w): disp4w = 0.0

    # ---- Sector mapping (optional) ----
    sector_path = Path("ref/symbol_sector.csv")
    if sector_path.exists():
        secmap = pd.read_csv(sector_path)
        if "Symbol" in secmap.columns and "Sector" in secmap.columns:
            secmap["Sector"] = secmap["Sector"].fillna("OTHER").astype(str)
        else:
            secmap = None
    else:
        secmap = None

    if secmap is not None:
        last = last.merge(secmap, on="Symbol", how="left")
        last["Sector"] = last["Sector"].fillna("OTHER")
        # sector breadth: % above 20DMA within sector
        tmp = df[df["Date"]==last_day][["Symbol","Close"]].merge(
            df[df["Date"]==last_day][["Symbol","SMA20"]], on="Symbol", how="left"
        ).merge(secmap, on="Symbol", how="left").fillna({"Sector":"OTHER"})
        tmp["ABV20"] = tmp["Close"] > tmp["SMA20"]
        sec_breadth = tmp.groupby("Sector")["ABV20"].mean().to_dict()
        last["SECTOR_BREADTH20"] = last["Sector"].map(sec_breadth).fillna(0.0)
        # sector RS: median 4W RS within sector (z-scored across sectors)
        rs_by_sec = df[df["Date"]==last_day].merge(secmap, on="Symbol", how="left").fillna({"Sector":"OTHER"}) \
                       .groupby("Sector")["RET_20"].median().rename("SEC_RET_20")
        rs_sec = rs_by_sec.to_frame().reset_index()
        rs_sec["SEC_RS_Z"] = zscore(rs_sec["SEC_RET_20"].fillna(0))
        last = last.merge(rs_sec[["Sector","SEC_RS_Z"]], on="Sector", how="left")
    else:
        last["Sector"] = "OTHER"
        last["SECTOR_BREADTH20"] = 0.0
        last["SEC_RS_Z"] = 0.0

    # ---- Macro regime (optional) ----
    vix_df   = try_read_csv(Path("data/index/INDIAVIX.csv"))
    idx_df   = try_read_csv(Path("data/index/NIFTY.csv"))
    vix_pctl = np.nan
    nifty_below_50 = False
    if vix_df is not None and "Close" in vix_df.columns:
        vix_df = vix_df.sort_values("Date")
        vix_past = vix_df.tail(180)["Close"]
        cur_vix  = vix_df["Close"].iloc[-1]
        if len(vix_past) >= 20:
            vix_pctl = float((vix_past <= cur_vix).mean())  # 0..1
    if idx_df is not None and "Close" in idx_df.columns:
        idx_df = idx_df.sort_values("Date")
        idx_df["SMA50"] = idx_df["Close"].rolling(50, min_periods=25).mean()
        cur_close = idx_df["Close"].iloc[-1]
        cur_sma50 = idx_df["SMA50"].iloc[-1]
        nifty_below_50 = bool(cur_close < cur_sma50)

    high_vol_tighten = (np.isfinite(vix_pctl) and vix_pctl >= VIX_TIGHTEN_PCTL and nifty_below_50)

    # ---- Candidate pool after core gates ----
    uni_ok = last[last["UNIVERSE_OK"]]
    lq_ok  = uni_ok[uni_ok["LQ_OK"]]
    atr_ok = lq_ok[lq_ok["ATR_OK"]]
    fam_ok = atr_ok[atr_ok["ANY_FAM"]].copy()

    # ---- Scoring ----
    def liquidity_score(row):
        tt = min(1.0, (row["TurnoverCr_med20"] or 0)/20.0)
        dd = 0.5 if pd.isna(row.get("DelivValCr_med20", np.nan)) else min(1.0, (row["DelivValCr_med20"] or 0)/8.0)
        return max(0.0, min(1.0, 0.5*tt + 0.5*dd))

    elig = fam_ok.copy()
    elig["PowerSignal"] = np.where((elig["PocketPivot10"]==1) | (elig["Breakout"]), 1.0,
                              np.where(elig["UpVolDom10"], 0.5, 0.0))
    rs4 = elig["RS_4W_Z"].clip(0,2).fillna(0)
    rs13= elig["RS_13W_Z"].clip(-1,2).fillna(0)
    poww= elig["PowerSignal"].fillna(0)
    sqz = elig["SqueezeScore"].clip(0,1).fillna(0)
    lqs = elig.apply(liquidity_score, axis=1)
    stru= (elig["MA_Aligned"].astype(float) + elig["Near52w"].astype(float))/2.0

    # Ensure flow/penalty fields exist
    for col in ["DELIV_Q4","INSIDER_PLUS","BULK_PLUS","BLOCK_PLUS","BELOW_SMA20","EXTENDED_BB"]:
        if col not in elig.columns:
            elig[col] = 0

    flow = (
        10*elig["DELIV_Q4"].fillna(0) +
        10*elig["INSIDER_PLUS"].fillna(0) +
         8*elig["BULK_PLUS"].fillna(0) +
         5*elig["BLOCK_PLUS"].fillna(0)
    )
    penalty = (
        20*elig["BELOW_SMA20"].fillna(0) +
        15*elig["EXTENDED_BB"].fillna(0)
    )

    elig["R_SCORE"] = (30*rs4 + 15*rs13 + 20*poww + 15*sqz + 10*lqs + 10*stru + flow - penalty).round(1)
    elig["R_SCORE"] = elig["R_SCORE"].clip(lower=0, upper=100)

    # ---- Plans & proximity ----
    def build_plans(r):
        """
        Returns (Entry1, SL1, Mode1, Entry2, SL2, Mode2, Entry3, SL3, Mode3)
        - Plan A (primary): BREAKOUT/READY with hybrid buffer
        - Plan B (reclaim): reclaim over SMA10 if power and close-location are decent
        - Plan C (inside-day): break of inside high with stop at inside low
        """
        close = float(r["Close"]); high = float(r["High"]); low = float(r["Low"])
        atr   = float(r["ATR14"]) if np.isfinite(r["ATR14"]) else 0.0
        sma10 = float(r["SMA10"]) if np.isfinite(r["SMA10"]) else np.nan
        sma20 = float(r["SMA20"]) if np.isfinite(r["SMA20"]) else np.nan

        # ---- Plan A: hybrid breakout/ready
        if r["FAM_BB"] or r["FAM_RS"]:
            buf   = hybrid_breakout_buffer(close, high, atr)
            entry = high + buf
            sl    = close - (1.10 if r["FAM_RS"] else 1.20)*atr
            mode  = "BREAKOUT"
            if r["FAM_RS"] and entry <= high + 1e-9:
                mode = "READY"
        else:
            # Pullback family → prefer reclaim over SMA10 with ATR pad
            entry = max(close, (sma10 if np.isfinite(sma10) else close) + 0.10*atr)
            sl    = min((sma20 if np.isfinite(sma20) else close) - 0.50*atr, low - 0.20*atr)
            mode  = "RECLAIM"
        A = (round(entry,2), round(sl,2), mode)

        # ---- Plan B: Reclaim (power + structure)
        B = (np.nan, np.nan, "")
        if (r.get("UpVol3Ratio", np.nan) or 0) >= RECLAIM_UPVOL3_MIN and (r.get("CloseLoc", np.nan) or 0) >= RECLAIM_CLOSELOC_MIN:
            if np.isfinite(sma10) and np.isfinite(sma20):
                e2 = (sma10 + 0.10*atr)
                s2 = (sma20 - 0.50*atr)
                B  = (round(e2,2), round(s2,2), "RECLAIM")

        # ---- Plan C: Inside-day (today compressed within yesterday)
        C = (np.nan, np.nan, "")
        if bool(r.get("InsideDay", False)):
            ih = float(r["InsideHigh"]); il = float(r["InsideLow"])
            if np.isfinite(ih) and np.isfinite(il):
                C = (round(ih,2), round(il,2), "INSIDE_DAY")

        return A + B + C

    # Dynamic proximity caps
    risk_on = (breadth20 >= 0.55) or (disp4w >= 0.10)
    base_prox_max   = PROX_ATR_RISK_ON if risk_on else PROX_ATR_BASE
    base_upvol3_min = UPVOL3_RATIO_MIN
    if high_vol_tighten:
        base_prox_max   = min(base_prox_max, PROX_ATR_HIGHVOL)
        base_upvol3_min = UPVOL3_RATIO_TIGHT

    rows = []
    for _, r in elig.iterrows():
        (entry, sl, mode,
         entry2, sl2, mode2,
         entry3, sl3, mode3) = build_plans(r)

        atr = r["ATR14"] if np.isfinite(r["ATR14"]) and r["ATR14"]>0 else np.nan
        prox = round(max(0.0, (entry - r["Close"]) / atr), 2) if np.isfinite(atr) else 9.99
        power_ok = (r["PocketPivot10"]==1) or ((r["UpVol3Ratio"] or 0) >= base_upvol3_min)
        loc_ok   = (r["CloseLoc"] or 0) >= CLOSE_LOC_MIN
        prox_ok  = prox <= base_prox_max
        coil_ok  = (r["SqueezeScore"] >= 0.60) if r["FAM_RS"] else True

        # ---- Three paths to GO ----
        go_reason = None
        market_ok = (breadth20 >= BREADTH20_MIN) or (disp4w >= DISPERSION_4W_MIN)
        sector_ok = False
        stock_only_ok = False

        if "SECTOR_BREADTH20" in r and "SEC_RS_Z" in r:
            sec_b = float(r.get("SECTOR_BREADTH20", 0.0))
            sec_z = float(r.get("SEC_RS_Z", 0.0))
            sector_ok = (sec_b >= max(SECTOR_BREADTH_MIN, breadth20 + SECTOR_BREADTH_BONUS)) and (sec_z >= SECTOR_RS_Z_MIN)

        stock_only_ok = (r["RS_4W_Z"] >= 1.0) and power_ok and coil_ok and prox_ok and loc_ok

        go_flag = False
        if market_ok and power_ok and prox_ok and loc_ok and coil_ok:
            go_flag = True; go_reason = "MARKET_OK"
        elif sector_ok and power_ok and prox_ok and loc_ok and coil_ok:
            go_flag = True; go_reason = "SECTOR_OK"
        elif stock_only_ok:
            go_flag = True; go_reason = "STOCK_ONLY"

        # Diagnostics
        reasons = []
        if not power_ok: reasons.append("Power weak")
        if not loc_ok:   reasons.append("Weak close")
        if not prox_ok:  reasons.append(f"Far ({prox:.2f} ATR)")
        if not coil_ok:  reasons.append("Not coiled")
        if "DELIV_Q4" in r and r.get("DELIV_Q4", 0) == 0:
            reasons.append("No delivery edge")
        if "INSIDER_PLUS" in r and r.get("INSIDER_PLUS", 0) == 0:
            reasons.append("No fresh insider")
        if not market_ok and not sector_ok and not stock_only_ok:
            reasons.append(f"Breadth {breadth20*100:.0f}%/Disp {disp4w*100:.1f}%")
        if "SECTOR_BREADTH20" in r and not sector_ok:
            reasons.append(f"Sector weak (b{r.get('SECTOR_BREADTH20',0):.2f}, z{r.get('SEC_RS_Z',0):+.2f})")

        rows.append({
            "Symbol": r["Symbol"], "Series": r["Series"], "Close": r["Close"],
            "R_SCORE": r["R_SCORE"],
            "Entry": entry, "SL": sl, "EntryMode": mode, "TriggerDistATR": prox,
            "Entry2": entry2, "SL2": sl2, "Mode2": mode2,
            "Entry3": entry3, "SL3": sl3, "Mode3": mode3,
            "WHY": ",".join([t for t,flag in [
                ("VCP_BREAKOUT", bool(r["FAM_BB"])),
                ("RS_LEADER", bool(r["FAM_RS"])),
                ("TREND_PULLBACK", bool(r["FAM_PULL"])),
                ("POCKET_PIVOT", r["PocketPivot10"]==1),
                ("UPVOL_DOM", bool(r["UpVolDom10"])),
                ("MA_ALIGNED", bool(r["MA_Aligned"])),
                ("NEAR_52W", bool(r["Near52w"])),
                ("COILED", bool(r["SqueezeScore"]>=0.7)),
                ("DELIV_Q4", bool(r.get("DELIV_Q4",0))),
                ("INSIDER+", bool(r.get("INSIDER_PLUS",0))),
                ("BULK+", bool(r.get("BULK_PLUS",0))),
                ("BLOCK+", bool(r.get("BLOCK_PLUS",0))),
                ("SUB_SMA20", bool(r.get("BELOW_SMA20",0))),
                ("EXTENDED", bool(r.get("EXTENDED_BB",0))),
            ] if flag]),
            "RS_4W_Z": r["RS_4W_Z"], "RS_13W_Z": r["RS_13W_Z"], "SqueezeScore": r["SqueezeScore"],
            "PocketPivot10": r["PocketPivot10"],
            "TurnoverCr_med20": r["TurnoverCr_med20"], "DelivValCr_med20": r.get("DelivValCr_med20", np.nan),
            "ATR_PCT": r["ATR_PCT"], "MA_Aligned": r["MA_Aligned"], "Near52w": r["Near52w"],
            "DELIV_Q4": r.get("DELIV_Q4",0),
            "INSIDER_PLUS": r.get("INSIDER_PLUS",0),
            "BULK_PLUS": r.get("BULK_PLUS",0),
            "BLOCK_PLUS": r.get("BLOCK_PLUS",0),
            "BELOW_SMA20": r.get("BELOW_SMA20",0),
            "EXTENDED_BB": r.get("EXTENDED_BB",0),
            "GO": bool(go_flag),
            "GO_REASONS": go_reason if go_flag else ("; ".join(reasons) if reasons else "Needs confirm"),
            "MKT_BREADTH20": round(breadth20,3),
            "DISP_4W": round(disp4w,4),
            "AsOf": pd.to_datetime(last_day).date(),
            "Sector": r.get("Sector","OTHER"),
            "SECTOR_BREADTH20": r.get("SECTOR_BREADTH20", 0.0),
            "SEC_RS_Z": r.get("SEC_RS_Z", 0.0)
        })

    out_df = pd.DataFrame(rows)
    out_df = out_df.sort_values(["GO","R_SCORE"], ascending=[False, False]).head(args.top)
    out_df.to_csv(out_path, index=False)

    # Safe GO count when empty
    go_count = int(out_df["GO"].sum()) if "GO" in out_df.columns else 0

    # Report (stage counts)
    lq_count  = int(len(lq_ok))
    atr_count = int(len(atr_ok))
    fam_count = int(len(fam_ok))

    report = [
        f"### WEEK GATE REPORT — {str(last_day)[:10]}",
        f"- Turnover unit chosen: {chosen}",
        f"- Total symbols (last-day snapshot): {total_syms}",
        f"- Universe OK (EQ, price ≥ {int(args.min_close)}, purge ETFs): {uni_syms}",
        f"- Liquidity OK (Turnover_20 ≥ {args.turnover_cr_20} cr; Deliverable_20 ≥ {args.deliv_cr_20} cr or NA): {lq_count}",
        f"- ATR% OK ({args.atr_min*100:.0f}%–{args.atr_max*100:.0f}%): {atr_count}",
        f"- Family match (RS/Pullback/Breakout): {fam_count}",
        f"- Market breadth (%% >20DMA): {breadth20*100:.1f}%",
        f"- Dispersion (stdev 4W returns): {disp4w*100:.1f}%",
        f"- Macro tighten active: {'YES' if high_vol_tighten else 'NO'}",
        f"- Sector map: {'present' if 'secmap' in locals() and secmap is not None else 'absent'}",
    ]
    Path("out").mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(report) + "\n", encoding="utf-8")
    print(f"Wrote {out_path} with {len(out_df)} rows for {pd.to_datetime(last_day).date()} (GO={go_count})")

if __name__ == "__main__":
    main()
