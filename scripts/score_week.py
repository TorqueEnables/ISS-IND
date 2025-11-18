#!/usr/bin/env python3
"""
StakeLens Insider — Weekly scorer with Go/No-Go gating.

What’s new vs prior version:
- Robust numeric coercion and turnover unit auto-detect remain intact.
- Market breadth guard (percent of universe above 20-DMA).
- Proximity-to-trigger: require entry within ≤ 0.8 × ATR14 of Close.
- Power quality: pocket pivot or 3d up-volume dominance AND strong close in range.
- GO flag + reasons; entry mode classification (BREAKOUT | RECLAIM | READY).
- Extra outputs so the brief can show conviction and why.
"""

import argparse, re
from pathlib import Path
import numpy as np
import pandas as pd

# ---------------- Config ----------------
MIN_CLOSE            = 50.0
TURNOVER_CR_20_FLOOR = 5.0   # ₹ cr
DELIV_CR_20_FLOOR    = 2.0   # ₹ cr
ATR_PCT_MIN          = 0.02  # 2%
ATR_PCT_MAX          = 0.10  # widen a bit to not miss power names
TOP_LIMIT            = 50

# Go/No-Go thresholds
BREADTH20_MIN        = 0.45  # 45% of universe above 20-DMA
PROX_ATR_MAX         = 0.80  # entry distance ≤ 0.8 * ATR14
CLOSE_LOC_MIN        = 0.60  # close in top 40% of bar
UPVOL3_RATIO_MIN     = 1.20  # 3-day up-vol / down-vol
SQUEEZE_MIN_RS       = 0.60  # min squeeze score for RS continuation

ETF_REGEX = re.compile(
    r'(ETF|BEES|MOM|GOLD|SILVER|CPSE|PSU|FUND|FOF|NIFTY|SENSEX|JUNIOR|NEXT|TRI)$',
    re.IGNORECASE
)

# -------------- utils -------------------
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

def to_num(s):
    if s.dtype == "O":
        s = s.astype(str).str.replace(",", "", regex=False).str.replace(" ", "", regex=False)
    return pd.to_numeric(s, errors="coerce")

def zscore(x: pd.Series):
    mu = x.mean()
    sd = x.std(ddof=0)
    if not np.isfinite(sd) or sd == 0: return (x*0).fillna(0)
    return (x - mu) / sd

def true_range(df):
    pc = df["Close"].shift(1)
    tr = pd.concat([df["High"]-df["Low"], (df["High"]-pc).abs(), (df["Low"]-pc).abs()], axis=1).max(axis=1)
    return tr

def bb_width_20(s):
    ma = s.rolling(20, min_periods=20).mean()
    sd = s.rolling(20, min_periods=20).std(ddof=0)
    upper = ma + 2*sd
    lower = ma - 2*sd
    return (upper - lower) / ma

def percentile_of_last(series: pd.Series, lookback=120):
    s = series.dropna()
    if s.empty: return np.nan
    tail = s.tail(lookback)
    last = tail.iloc[-1]
    return float((tail <= last).sum()) / float(len(tail))

# -------------- main --------------------
def main():
    args = parse_args()
    out_path  = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    report_path = Path("out/WEEK_GATE_REPORT.md")

    hist_path = Path(args.hist)
    if not hist_path.exists():
        raise SystemExit(f"Missing {hist_path}")

    df = pd.read_csv(hist_path, parse_dates=["Date"])
    if df.empty:
        raise SystemExit("bhav_hist.csv is empty")

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

    # Technicals
    df["SMA10"]  = g["Close"].transform(lambda s: s.rolling(10, min_periods=10).mean())
    df["SMA20"]  = g["Close"].transform(lambda s: s.rolling(20, min_periods=10).mean())
    df["SMA50"]  = g["Close"].transform(lambda s: s.rolling(50, min_periods=25).mean())
    df["SMA200"] = g["Close"].transform(lambda s: s.rolling(200, min_periods=100).mean())
    df["TR"]     = g.apply(true_range)
    df["ATR14"]  = g["TR"].transform(lambda s: s.rolling(14, min_periods=10).mean())
    df["ATR_PCT"]= df["ATR14"] / df["Close"]

    # Close location in range (0..1), guard against div/0
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

    # Volume signals
    df["UpBar"]   = df["Close"] > df["PrevClose"]
    df["DownBar"] = df["Close"] < df["PrevClose"]
    g10_up   = g.apply(lambda d: (d["UpBar"]*d["Volume"]).rolling(10, min_periods=10).sum())
    g10_down = g.apply(lambda d: (d["DownBar"]*d["Volume"]).rolling(10, min_periods=10).sum())
    df["UpVol10"], df["DownVol10"] = g10_up, g10_down

    # 3-day up/down volume
    g3_up   = g.apply(lambda d: (d["UpBar"]*d["Volume"]).rolling(3, min_periods=3).sum())
    g3_down = g.apply(lambda d: (d["DownBar"]*d["Volume"]).rolling(3, min_periods=3).sum())
    df["UpVol3"], df["DownVol3"] = g3_up, g3_down

    # Snapshot (last day)
    last = df[df["Date"]==last_day][[
        "Symbol","Series","Open","High","Low","Close","PrevClose",
        "SMA10","SMA20","SMA50","SMA200","ATR14","ATR_PCT",
        "BBWidth20","UpVol10","DownVol10","UpVol3","DownVol3",
        "TurnoverCr_med20","DelivValCr_med20","CloseLoc"
    ]].copy()
    last = last.merge(pct_df, on="Symbol", how="left")
    last = last.merge(last_rs[["Symbol","RS_4W_Z","RS_13W_Z"]], on="Symbol", how="left")

    # Power & squeeze
    last["UpVolDom10"]  = (last["UpVol10"] > last["DownVol10"]).fillna(False)
    last["UpVol3Ratio"] = (last["UpVol3"] / last["DownVol3"]).replace([np.inf, -np.inf], np.nan)
    last["SqueezeScore"] = (1.0 - last["BBWidth_pctile_20"].clip(0,1))
    last["MA_Aligned"]   = ((last["SMA20"] > last["SMA50"]) & (last["SMA50"] > last["SMA200"]))

    # Near 52w proxy
    gmax = g["Close"].transform(lambda s: s.rolling(252, min_periods=60).max())
    mx   = df[df["Date"]==last_day][["Symbol","Close"]].copy()
    mx["MaxClose252"] = gmax[df["Date"]==last_day].values
    mx["Near52w"]     = (mx["Close"] >= 0.97 * mx["MaxClose252"])
    last = last.merge(mx[["Symbol","Near52w","MaxClose252"]], on="Symbol", how="left")

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

    last["FAM_PULL"] = (
        (last["MA_Aligned"]) &
        (last["Close"] <= last["SMA10"]) &
        (last["Close"] >= last["SMA20"])
    )

    roll_hi_40 = g["High"].transform(lambda s: s.rolling(40, min_periods=25).max())
    brk = df[df["Date"]==last_day][["Symbol","Close"]].copy()
    brk["Hi40"]     = roll_hi_40[df["Date"]==last_day].values
    brk["Breakout"] = (brk["Close"] >= 1.01 * brk["Hi40"])
    last = last.merge(brk[["Symbol","Breakout","Hi40"]], on="Symbol", how="left")
    last["FAM_BB"] = (last["Breakout"] & (last["SqueezeScore"] >= 0.7))

    # Gates
    last = last.merge(snap[["Symbol","UNIVERSE_OK"]], on="Symbol", how="left")
    last["LQ_TURN_OK"]  = (last["TurnoverCr_med20"] >= args.turnover_cr_20)
    last["LQ_DELIV_OK"] = (last["DelivValCr_med20"] >= args.deliv_cr_20) | last["DelivValCr_med20"].isna()
    last["LQ_OK"]       = last["LQ_TURN_OK"] & last["LQ_DELIV_OK"]
    last["ATR_OK"]      = (last["ATR_PCT"] >= args.atr_min) & (last["ATR_PCT"] <= args.atr_max)
    last["ANY_FAM"]     = last["FAM_RS"] | last["FAM_PULL"] | last["FAM_BB"]

    # ---- Market breadth (universe snapshot) ----
    snap_ma = df[df["Date"]==last_day][["Symbol","Close"]].merge(
        df[df["Date"]==last_day][["Symbol","SMA20"]], on="Symbol", how="left"
    )
    snap_ma = snap_ma.merge(snap[["Symbol","UNIVERSE_OK"]], on="Symbol", how="left")
    breadth20 = float((snap_ma.query("UNIVERSE_OK == True")["Close"] > snap_ma.query("UNIVERSE_OK == True")["SMA20"]).mean())
    if not np.isfinite(breadth20): breadth20 = 0.0

    # Diagnostics
    uni_ok = last[last["UNIVERSE_OK"]]
    lq_ok  = uni_ok[uni_ok["LQ_OK"]]
    atr_ok = lq_ok[lq_ok["ATR_OK"]]
    fam_ok = atr_ok[atr_ok["ANY_FAM"]]

    report = [
        f"### WEEK GATE REPORT — {str(last_day)[:10]}",
        f"- Turnover unit chosen: {chosen}",
        f"- Total symbols (last-day snapshot): {total_syms}",
        f"- Universe OK (EQ, price ≥ {int(args.min_close)}, purge ETFs): {uni_syms}",
        f"- Liquidity OK (Turnover_20 ≥ {args.turnover_cr_20} cr; Deliverable_20 ≥ {args.deliv_cr_20} cr or NA): {len(lq_ok)}",
        f"- ATR% OK ({args.atr_min*100:.0f}%–{args.atr_max*100:.0f}%): {len(atr_ok)}",
        f"- Family match (RS/Pullback/Breakout): {len(fam_ok)}",
        f"- Market breadth (%% above 20-DMA): {breadth20*100:.1f}%",
    ]
    Path("out").mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(report) + "\n", encoding="utf-8")

    if fam_ok.empty:
        pd.DataFrame(columns=[
            "Symbol","Series","Close","R_SCORE","Entry","SL","WHY",
            "RS_4W_Z","RS_13W_Z","SqueezeScore","PocketPivot10",
            "TurnoverCr_med20","DelivValCr_med20","ATR_PCT",
            "MA_Aligned","Near52w","SETUP_FAMILY","AsOf",
            "GO","GO_REASONS","EntryMode","TriggerDistATR","CloseLoc","MKT_BREADTH20"
        ]).to_csv(out_path, index=False)
        print("No eligible names; wrote empty WEEK_PACK.csv and gate report.")
        return

    # ---- Scoring (unchanged weights) ----
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

    elig["R_SCORE"] = (30*rs4 + 15*rs13 + 20*poww + 15*sqz + 10*lqs + 10*stru).round(1)

    # ---- Entry/SL + proximity and modes ----
    def entry_sl_mode(r):
        atr = r["ATR14"] if np.isfinite(r["ATR14"]) else 0.0
        if r["FAM_BB"]:
            entry = float(r["High"]*1.01)
            sl    = float(r["Close"] - 1.2*atr)
            mode  = "BREAKOUT"
        elif r["FAM_RS"]:
            entry = float(max(r["Close"], r["High"]*1.005))
            sl    = float(r["Close"] - 1.1*atr)
            mode  = "BREAKOUT" if entry > r["High"] else "READY"
        else:
            entry = float(max(r["Close"], (r["SMA10"] or 0) + 0.5*atr))
            sl    = float(min((r["SMA20"] or r["Close"]) - 0.5*atr, r["Low"] - 0.2*atr))
            mode  = "RECLAIM"
        entry = round(entry, 2); sl = round(sl, 2)
        return entry, sl, mode

    out_cols = ["Symbol","Series","Close","High","Low","SMA10","SMA20","SMA50","SMA200",
                "RS_4W_Z","RS_13W_Z","SqueezeScore","PocketPivot10",
                "TurnoverCr_med20","DelivValCr_med20","ATR14","ATR_PCT",
                "MA_Aligned","Near52w","FAM_RS","FAM_PULL","FAM_BB",
                "Breakout","UpVolDom10","UpVol3Ratio","CloseLoc"]
    out_df = elig[out_cols].copy()

    ents, sls, modes, prox, go, go_reasons, whys = [], [], [], [], [], [], []
    for _, r in out_df.iterrows():
        e, s, m = entry_sl_mode(r)
        ents.append(e); sls.append(s); modes.append(m)

        # Proximity (in ATRs)
        if np.isfinite(r["ATR14"]) and r["ATR14"] > 0:
            td = max(0.0, (e - r["Close"]) / r["ATR14"])
        else:
            td = 9.99
        prox.append(round(td, 2))

        # WHY tags
        tags=[]
        if r["FAM_BB"]: tags.append("VCP_BREAKOUT")
        if r["FAM_RS"]: tags.append("RS_LEADER")
        if r["FAM_PULL"]: tags.append("TREND_PULLBACK")
        if r["PocketPivot10"]==1: tags.append("POCKET_PIVOT")
        if r["UpVolDom10"]: tags.append("UPVOL_DOM")
        if r["MA_Aligned"]: tags.append("MA_ALIGNED")
        if r["Near52w"]: tags.append("NEAR_52W")
        if r["SqueezeScore"]>=0.7: tags.append("COILED")
        whys.append(",".join(tags))

        # Go/No-Go decision
        reasons = []
        power_ok = (r["PocketPivot10"]==1) or ( (r["UpVol3Ratio"] or 0) >= UPVOL3_RATIO_MIN )
        if not power_ok:
            reasons.append("Power weak (no PP/3d up-vol)")
        loc_ok = (r["CloseLoc"] or 0) >= CLOSE_LOC_MIN
        if not loc_ok:
            reasons.append("Weak close-in-range")
        prox_ok = (td <= PROX_ATR_MAX)
        if not prox_ok:
            reasons.append(f"Far from entry ({td:.2f} ATR)")
        sq_ok = (r["SqueezeScore"] >= 0.60) if r["FAM_RS"] else True
        if not sq_ok:
            reasons.append("Not coiled enough")

        go_flag = (power_ok and loc_ok and prox_ok and sq_ok and (breadth20 >= BREADTH20_MIN))
        if breadth20 < BREADTH20_MIN:
            reasons.append(f"Market breadth low ({breadth20*100:.0f}%>20DMA)")

        go.append(bool(go_flag))
        go_reasons.append("OK" if go_flag else "; ".join(reasons) if reasons else "OK")

    out_df["Entry"] = ents
    out_df["SL"]    = sls
    out_df["EntryMode"] = modes
    out_df["TriggerDistATR"] = prox
    out_df["WHY"]   = whys
    out_df["R_SCORE"] = elig["R_SCORE"].values
    out_df["GO"]    = go
    out_df["GO_REASONS"] = go_reasons
    out_df["AsOf"]  = pd.to_datetime(last_day).date()
    out_df["MKT_BREADTH20"] = round(breadth20, 3)

    # Sort: GO first, then score
    out_df = out_df.sort_values(["GO","R_SCORE"], ascending=[False, False]).head(args.top)

    final = out_df[[
        "Symbol","Series","Close","R_SCORE","Entry","SL","WHY",
        "RS_4W_Z","RS_13W_Z","SqueezeScore","PocketPivot10",
        "TurnoverCr_med20","DelivValCr_med20","ATR_PCT",
        "MA_Aligned","Near52w","EntryMode","TriggerDistATR",
        "GO","GO_REASONS","MKT_BREADTH20","AsOf"
    ]].reset_index(drop=True)

    final.to_csv(out_path, index=False)
    print(f"Wrote {out_path} with {len(final)} rows for {pd.to_datetime(last_day).date()} (GO={final['GO'].sum()})")

if __name__ == "__main__":
    main()
