#!/usr/bin/env python3
"""
Score weekly candidates directly from bhav_hist.csv
- Robust: no external "tech_latest" required.
- Simple: one entry-point, one output (out/WEEK_PACK.csv).
- Quality: hard universe & liquidity gates + power/squeeze/RS structure.
"""

import argparse, re, math
from pathlib import Path
import numpy as np
import pandas as pd

# ---------------------------
# Config (defaults; override via CLI if you want)
# ---------------------------
MIN_CLOSE            = 50.0         # price floor
TURNOVER_CR_20_FLOOR = 5.0          # ₹ cr median over 20d
DELIV_CR_20_FLOOR    = 2.0          # ₹ cr median over 20d
ATR_PCT_MIN          = 0.02         # 2%
ATR_PCT_MAX          = 0.08         # 8%
TOP_LIMIT            = 50           # write this many ranked rows

ETF_REGEX = re.compile(
    r'(ETF|BEES|MOM|GOLD|SILVER|CPSE|PSU|FUND|FOF|NIFTY|SENSEX|JUNIOR|NEXT|TRI)$',
    re.IGNORECASE
)

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

# ---------- helpers ----------
def pick(colnames, cols):
    for c in colnames:
        if c in cols: return c
    return None

def zscore(x: pd.Series):
    mu = x.mean()
    sd = x.std(ddof=0)
    if sd == 0 or np.isnan(sd): return (x*0).fillna(0)
    return (x - mu) / sd

def true_range(df):
    prev_close = df["Close"].shift(1)
    tr = pd.concat([
        df["High"] - df["Low"],
        (df["High"] - prev_close).abs(),
        (df["Low"] - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr

def rolling_median_cr(series, window):
    # value is rupees → convert to ₹ cr
    s = series.rolling(window, min_periods=window).median() / 1e7
    return s

def rolling_percentile_last(series, lookback=120):
    """Percentile of the last value vs the last N values (0..1).
       Computes only for the final row per group to keep it light."""
    if len(series) < 3:
        return np.nan
    tail = series.tail(lookback)
    last = tail.iloc[-1]
    rank = (tail <= last).sum()
    return float(rank) / float(len(tail))

def safe_div(a, b):
    with np.errstate(divide='ignore', invalid='ignore'):
        r = np.where(b==0, np.nan, a/b)
    return r

def clip(x, lo, hi):
    return max(lo, min(hi, x))

# ---------- main ----------
def main():
    args = parse_args()
    hist_path = Path(args.hist)
    out_path  = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not hist_path.exists():
        raise SystemExit(f"Missing {hist_path}")

    df = pd.read_csv(hist_path, parse_dates=["Date"])
    if df.empty:
        raise SystemExit("bhav_hist.csv is empty")

    # Canonicalize columns (accept both normalized and cm* styles)
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

    # Deliverables (optional)
    dqty = pick(["DelivQty","DELIV_QTY","DELIV_QTY(ROLLING)","DeliverableQty","DELIVQTY"], cols)

    need = [sym, ser, open_, high, low, close, prev, vol, turn]
    if any(c is None for c in need):
        raise SystemExit(f"bhav_hist.csv missing required columns; got {cols.tolist()}")

    df = df[[ "Date", sym, ser, open_, high, low, close, prev, vol, turn ] + ([dqty] if dqty else [])].copy()
    df.columns = ["Date","Symbol","Series","Open","High","Low","Close","PrevClose","Volume","Turnover"] + ([ "DelivQty"] if dqty else [])
    df.sort_values(["Symbol","Date"], inplace=True)

    # Latest trading day
    last_day = df["Date"].max()
    # Universe hygiene (EQ only, equities only, price floor, ETF/index purge)
    # We compute this on the latest day snapshot
    snap = df[df["Date"]==last_day].copy()

    def is_equity_row(row):
        s = str(row["Series"]).upper()
        if s != "EQ": return False
        if float(row["Close"]) < args.min_close: return False
        if ETF_REGEX.search(str(row["Symbol"])): return False
        return True

    snap["UNIVERSE_OK"] = snap.apply(is_equity_row, axis=1)

    # Merge back to full df for rolling features (keep only symbols in universe on last day)
    keep_syms = set(snap.loc[snap["UNIVERSE_OK"], "Symbol"])
    df = df[df["Symbol"].isin(keep_syms)].copy()

    # Groupby for rolling features
    g = df.groupby("Symbol", group_keys=False)

    # Moving averages
    df["SMA10"]  = g["Close"].transform(lambda s: s.rolling(10, min_periods=10).mean())
    df["SMA20"]  = g["Close"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    df["SMA50"]  = g["Close"].transform(lambda s: s.rolling(50, min_periods=50).mean())
    df["SMA200"] = g["Close"].transform(lambda s: s.rolling(200, min_periods=200).mean())

    # ATR14 & ATR%
    df["TR"]     = g.apply(true_range)
    df["ATR14"]  = g["TR"].transform(lambda s: s.rolling(14, min_periods=14).mean())
    df["ATR_PCT"] = df["ATR14"] / df["Close"]

    # Bollinger bandwidth (20, 2σ)
    def bb_width(s):
        ma = s.rolling(20, min_periods=20).mean()
        sd = s.rolling(20, min_periods=20).std(ddof=0)
        upper = ma + 2*sd
        lower = ma - 2*sd
        width = (upper - lower) / ma
        return width
    df["BBWidth20"] = g["Close"].transform(bb_width)

    # BB width percentile (last vs its 120-day history), compute only for the last day rows
    last_rows = df[df["Date"]==last_day].copy()
    pct_vals = []
    for symb, sub in df.groupby("Symbol"):
        s = sub["BBWidth20"].dropna()
        if s.empty:
            pct_vals.append((symb, np.nan)); continue
        pct = rolling_percentile_last(s, lookback=120)
        pct_vals.append((symb, pct))
    pct_df = pd.DataFrame(pct_vals, columns=["Symbol","BBWidth_pctile_20"])

    # Liquidity metrics
    df["TurnoverCr"] = df["Turnover"] / 1e7
    df["TurnoverCr_med20"] = g["Turnover"].transform(lambda s: s.rolling(20, min_periods=20).median() / 1e7)
    if "DelivQty" in df.columns:
        df["DelivValCr"] = (df["DelivQty"] * df["Close"]) / 1e7
        df["DelivValCr_med20"] = g.apply(lambda d: (d["DelivQty"]*d["Close"]).rolling(20, min_periods=20).median() / 1e7)
    else:
        df["DelivValCr_med20"] = np.nan

    # RS metrics: 4W (~20d) and 13W (~65d) returns; z-scored across universe for last day
    df["RET_20"] = g["Close"].transform(lambda s: s.pct_change(20))
    df["RET_65"] = g["Close"].transform(lambda s: s.pct_change(65))

    last_rs = df[df["Date"]==last_day][["Symbol","RET_20","RET_65"]].copy()
    # Exclude NaNs for mean/std
    last_rs["RS_4W_Z"]  = zscore(last_rs["RET_20"].fillna(0))
    last_rs["RS_13W_Z"] = zscore(last_rs["RET_65"].fillna(0))

    # Up-volume dominance (10d), PocketPivot (today)
    # Mark up/down bars
    df["UpBar"] = df["Close"] > df["PrevClose"]
    df["DownBar"] = df["Close"] < df["PrevClose"]

    # 10d sums for last day per symbol
    df["UpVol10"]   = g.apply(lambda d: (d["UpBar"]*d["Volume"]).rolling(10, min_periods=10).sum())
    df["DownVol10"] = g.apply(lambda d: (d["DownBar"]*d["Volume"]).rolling(10, min_periods=10).sum())

    last_ex = df[df["Date"]==last_day][["Symbol","SMA10","SMA20","SMA50","SMA200","ATR14","ATR_PCT",
                                        "BBWidth20","TurnoverCr_med20","DelivValCr_med20",
                                        "UpVol10","DownVol10","Open","High","Low","Close","PrevClose"]].copy()
    last_ex = last_ex.merge(pct_df, on="Symbol", how="left")
    last_ex = last_ex.merge(last_rs[["Symbol","RS_4W_Z","RS_13W_Z"]], on="Symbol", how="left")

    # PocketPivot: today's up-vol > max down-vol last 10 AND Close > SMA10
    # We only have the sum of down-vol; an ok proxy: today's Volume > (DownVol10/10) * k and up bar
    # Better: recompute max down vol over last 10; approximate by (DownVol10/10)*2
    df["MaxDownVol10_proxy"] = g["DownBar"].transform(lambda s: s.rolling(10, min_periods=10).sum())  # count only; proxy
    # We'll compute a simpler PP flag using today's up-bar + UpVol10 > DownVol10 and Close > SMA10
    last_vol = df[df["Date"]==last_day][["Symbol","Volume","UpBar"]].copy()
    last_ex = last_ex.merge(last_vol, on="Symbol", how="left")
    last_ex["UpVolDom10"] = (last_ex["UpVol10"] > last_ex["DownVol10"]).fillna(False)
    last_ex["PocketPivot10"] = (last_ex["UpBar"] & last_ex["UpVolDom10"] & (last_ex["Close"] > last_ex["SMA10"])).astype(int)

    # Squeeze proxy via percentile (lower is tighter)
    last_ex["SqueezeScore"] = (1.0 - last_ex["BBWidth_pctile_20"].clip(0,1))  # 0..1

    # Structure flags
    last_ex["MA_Aligned"] = ((last_ex["SMA20"] > last_ex["SMA50"]) & (last_ex["SMA50"] > last_ex["SMA200"]))
    # Near 52w high proxy: Close within 3% of max Close in last 252d
    max_close_252 = g["Close"].transform(lambda s: s.rolling(252, min_periods=60).max())
    near52 = df[df["Date"]==last_day][["Symbol","Close"]].copy()
    near52["MaxClose252"] = max_close_252[df["Date"]==last_day].values
    near52["Near52w"] = (near52["Close"] >= 0.97 * near52["MaxClose252"])
    last_ex = last_ex.merge(near52[["Symbol","Near52w","MaxClose252"]], on="Symbol", how="left")

    # Family eligibility
    # 1) Momentum continuation (RS leaders)
    last_ex["FAM_RS"] = (
        (last_ex["RS_4W_Z"] >= 0.5) &
        (last_ex["RS_13W_Z"] >= 0.0) &
        (last_ex["Near52w"]) &
        (last_ex["SqueezeScore"] >= 0.65) | (last_ex["PocketPivot10"]==1)
    )

    # 2) Trend pullback to strength
    # Pullback proxy: Close <= SMA10 but >= SMA20, and MA aligned
    last_ex["FAM_PULL"] = (
        (last_ex["MA_Aligned"]) &
        (last_ex["Close"] <= last_ex["SMA10"]) &
        (last_ex["Close"] >= last_ex["SMA20"])
    )

    # 3) Base breakout (squeeze-release)
    # Breakout proxy: Close >= (rolling 40d high)*1.01 and squeeze tight
    roll_hi_40 = g["High"].transform(lambda s: s.rolling(40, min_periods=40).max())
    brk = df[df["Date"]==last_day][["Symbol","Close"]].copy()
    brk["Hi40"] = roll_hi_40[df["Date"]==last_day].values
    brk["Breakout"] = (brk["Close"] >= 1.01 * brk["Hi40"])
    last_ex = last_ex.merge(brk[["Symbol","Breakout"]], on="Symbol", how="left")
    last_ex["FAM_BB"] = (last_ex["Breakout"] & (last_ex["SqueezeScore"] >= 0.7))

    # Liquidity gate (deliverable value optional; if not present, allow via turnover alone)
    lq_turn = (last_ex["TurnoverCr_med20"] >= args.turnover_cr_20)
    if "DelivValCr_med20" in last_ex.columns:
        lq_deliv = (last_ex["DelivValCr_med20"] >= args.deliv_cr_20)
    else:
        lq_deliv = pd.Series([False]*len(last_ex), index=last_ex.index)

    last_ex["LQ_TURN_OK"]  = lq_turn.fillna(False)
    last_ex["LQ_DELIV_OK"] = lq_deliv.fillna(False)
    # Gate rule: turnover must pass; deliverable passes OR is missing (NaN) → treat missing as pass
    last_ex["LQ_OK"] = last_ex["LQ_TURN_OK"] & ( last_ex["LQ_DELIV_OK"] | last_ex["LQ_DELIV_OK"].isna() )

    # ATR% gate
    last_ex["ATR_OK"] = (last_ex["ATR_PCT"] >= args.atr_min) & (last_ex["ATR_PCT"] <= args.atr_max)

    # Final eligibility: universe + liquidity + ATR + any family
    snap_flags = snap[["Symbol","UNIVERSE_OK"]].copy()
    last_ex = last_ex.merge(snap_flags, on="Symbol", how="left")

    last_ex["ANY_FAM"] = (last_ex["FAM_RS"] | last_ex["FAM_PULL"] | last_ex["FAM_BB"])
    eligible = last_ex[last_ex["UNIVERSE_OK"] & last_ex["LQ_OK"] & last_ex["ATR_OK"] & last_ex["ANY_FAM"]].copy()

    if eligible.empty:
        # Write an empty file with headers for stability
        empty_cols = ["Symbol","Series","Close","R_SCORE","WHY","Entry","SL",
                      "RS_4W_Z","RS_13W_Z","SqueezeScore","PocketPivot10",
                      "TurnoverCr_med20","DelivValCr_med20","ATR_PCT","MA_Aligned","Near52w",
                      "SETUP_FAMILY"]
        pd.DataFrame(columns=empty_cols).to_csv(out_path, index=False)
        print("No eligible names today; wrote empty WEEK_PACK.csv")
        return

    # Scores
    def liquidity_score(row):
        tt = min(1.0, (row["TurnoverCr_med20"] or 0)/20.0)
        dd = min(1.0, ( (row["DelivValCr_med20"] or 0)/8.0 ) ) if not pd.isna(row.get("DelivValCr_med20", np.nan)) else 0.5
        return max(0.0, min(1.0, 0.5*tt + 0.5*dd))

    eligible["PowerSignal"] = np.where((eligible["PocketPivot10"]==1) | (eligible["Breakout"]), 1.0,
                                  np.where(eligible["UpVolDom10"], 0.5, 0.0))

    rs4 = eligible["RS_4W_Z"].clip(lower=0, upper=2).fillna(0)
    rs13 = eligible["RS_13W_Z"].clip(lower=-1, upper=2).fillna(0)
    poww = eligible["PowerSignal"].fillna(0)
    sqz  = eligible["SqueezeScore"].clip(0,1).fillna(0)
    lqs  = eligible.apply(liquidity_score, axis=1)
    stru = (eligible["MA_Aligned"].astype(float) + eligible["Near52w"].astype(float))/2.0

    eligible["R_SCORE"] = (
        30.0*rs4 + 15.0*rs13 + 20.0*poww + 15.0*sqz + 10.0*lqs + 10.0*stru
    ).round(1)

    # Family tag
    def fam_tag(r):
        if r["FAM_BB"]: return "BASE_BREAKOUT"
        if r["FAM_RS"]: return "RS_CONT"
        if r["FAM_PULL"]: return "PULLBACK"
        return "OTHER"

    # Entry/SL heuristics
    def entry_sl(r):
        atr = r["ATR14"] if not pd.isna(r["ATR14"]) else 0
        if r["FAM_BB"]:
            entry = round(float(r["High"]*1.01), 2)
            sl    = round(float(r["Close"] - 1.2*atr), 2)
        elif r["FAM_RS"]:
            entry = round(float(max(r["Close"], r["High"]*1.005)), 2)
            sl    = round(float(r["Close"] - 1.1*atr), 2)
        else: # pullback
            entry = round(float(max(r["Close"], r["SMA10"] + 0.5*atr)), 2)
            sl    = round(float(min(r["SMA20"] - 0.5*atr, r["Low"] - 0.2*atr)), 2)
        return entry, sl

    # Bring Series back for output
    latest_series = df[df["Date"]==last_day][["Symbol","Series"]].copy()
    eligible = eligible.merge(latest_series, on="Symbol", how="left")

    # Build WHY tags
    def why(r):
        tags = []
        if r["FAM_BB"]: tags.append("VCP_BREAKOUT")
        if r["FAM_RS"]: tags.append("RS_LEADER")
        if r["FAM_PULL"]: tags.append("TREND_PULLBACK")
        if r["PocketPivot10"]==1: tags.append("POCKET_PIVOT")
        if r["UpVolDom10"]: tags.append("UPVOL_DOM")
        if r["MA_Aligned"]: tags.append("MA_ALIGNED")
        if r["Near52w"]: tags.append("NEAR_52W")
        if r["SqueezeScore"]>=0.7: tags.append("COILED")
        return ",".join(tags)

    # Compose output rows
    out_cols = ["Symbol","Series","Close","High","Low","SMA10","SMA20","SMA50","SMA200",
                "RS_4W_Z","RS_13W_Z","SqueezeScore","PocketPivot10",
                "TurnoverCr_med20","DelivValCr_med20","ATR14","ATR_PCT",
                "MA_Aligned","Near52w","FAM_RS","FAM_PULL","FAM_BB","Breakout","UpVolDom10"]

    out_df = eligible[out_cols].copy()
    ents, sls, fams, whys = [], [], [], []
    for _, r in out_df.iterrows():
        e, s = entry_sl(r)
        ents.append(e); sls.append(s)
        fams.append(fam_tag(r))
        whys.append(why(r))

    out_df["Entry"] = ents
    out_df["SL"]    = sls
    out_df["SETUP_FAMILY"] = fams
    out_df["WHY"] = whys
    out_df["R_SCORE"] = eligible["R_SCORE"].values
    out_df["AsOf"] = pd.to_datetime(last_day).date()

    out_df = out_df.sort_values("R_SCORE", ascending=False).head(args.top)

    # keep concise columns in output
    final = out_df[[
        "Symbol","Series","Close","R_SCORE","Entry","SL","WHY",
        "RS_4W_Z","RS_13W_Z","SqueezeScore","PocketPivot10",
        "TurnoverCr_med20","DelivValCr_med20","ATR_PCT",
        "MA_Aligned","Near52w","SETUP_FAMILY","AsOf"
    ]].reset_index(drop=True)

    final.to_csv(out_path, index=False)
    print(f"Wrote {out_path} with {len(final)} rows for {last_day.date()}")

if __name__ == "__main__":
    main()
