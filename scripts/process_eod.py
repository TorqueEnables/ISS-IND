#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
StakeLens — Manual Upload Processor (local-only, duplicate-safe)
- No network calls.
- Ingest raw NSE bhav CSVs you drop under data/prices/.
- Normalize + append to data/prices/bhav_hist.csv (de-dup on Date+Symbol; ~15 months kept).
- Copy the latest raw bhav to data/prices/bhav_latest.csv (to keep V1 compatibility).
- Derive:
    - data/deliverables_latest.csv (from bhav's DELIV columns when present)
    - data/highlow_52w.csv
    - data/tech_latest.csv (ATR14/SMA/BB/NR7 + Delivery_Pct)
- Normalize corporates if present:
    - data/Bulk-Deals-*.csv  -> data/bulk_deals_latest.csv
    - data/Block-Deals-*.csv -> data/block_deals_latest.csv
- Write data/process_status.json with diagnostics.

Key change vs previous: column picking is duplicate-safe (DATE1/TIMESTAMP won’t collide).
"""

from __future__ import annotations
import os, glob, io, json, re
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import pandas as pd

DATA_DIR   = "data"
PRICES_DIR = os.path.join(DATA_DIR, "prices")
FILES = {
    "status":        os.path.join(DATA_DIR, "process_status.json"),
    "bhav_hist":     os.path.join(PRICES_DIR, "bhav_hist.csv"),
    "bhav_latest":   os.path.join(PRICES_DIR, "bhav_latest.csv"),  # raw copy of latest daily CSV
    "hi52w":         os.path.join(DATA_DIR, "highlow_52w.csv"),
    "tech_latest":   os.path.join(DATA_DIR, "tech_latest.csv"),
    "deliverables":  os.path.join(DATA_DIR, "deliverables_latest.csv"),
    "bulk_latest":   os.path.join(DATA_DIR, "bulk_deals_latest.csv"),
    "block_latest":  os.path.join(DATA_DIR, "block_deals_latest.csv"),
}

KEEP_COLS = ["Date","Symbol","Series","Open","High","Low","Close","PrevClose","Volume","Turnover"]

def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(PRICES_DIR, exist_ok=True)

def atomic_write_df(df: pd.DataFrame, path: str):
    tmp = path + ".tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)

def atomic_write_text(txt: str, path: str):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(txt)
    os.replace(tmp, path)

# ---------- tolerant CSV read ----------
def read_csv_any(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path, dtype=str, skipinitialspace=True)
    except Exception:
        return pd.read_csv(path, dtype=str, skipinitialspace=True, encoding="latin-1")

# ---------- header sanitation & picking ----------
_SANITIZE_RE = re.compile(r"[^a-z0-9_]+")

def _clean_col(name: str) -> str:
    key = str(name).replace("\ufeff","").strip().lower()
    key = key.replace("-", "_").replace(" ", "_")
    key = _SANITIZE_RE.sub("_", key)
    key = re.sub(r"_+", "_", key).strip("_")
    return key

def build_sanitized_map(df: pd.DataFrame) -> Dict[str, List[str]]:
    """Return mapping: sanitized_key -> [original_col_names in order]"""
    m: Dict[str, List[str]] = {}
    for c in df.columns:
        k = _clean_col(c)
        m.setdefault(k, []).append(c)
    return m

def colpick(df: pd.DataFrame, smap: Dict[str, List[str]], *keys: str) -> Optional[str]:
    """Pick the first present column among candidate sanitized keys; return original name."""
    for k in keys:
        if k in smap and len(smap[k]) > 0:
            return smap[k][0]
    return None

def seriespick(df: pd.DataFrame, smap: Dict[str, List[str]], *keys: str) -> Optional[pd.Series]:
    c = colpick(df, smap, *keys)
    return None if c is None else df[c]

def parse_date_series(s: pd.Series) -> pd.Series:
    if s is None:
        return pd.Series([pd.NaT]*0, dtype="datetime64[ns]")
    d = pd.to_datetime(s, errors="coerce")
    if d.notna().sum() >= max(1, int(0.7*len(s))):
        return d
    d2 = pd.to_datetime(s, errors="coerce", dayfirst=True)
    d = d.fillna(d2)
    # last-attempt explicit patterns
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        d3 = pd.to_datetime(s, format=fmt, errors="coerce")
        d = d.fillna(d3)
    return d

# ---------- bhav normalization (duplicate-safe) ----------
def normalize_bhav(raw: pd.DataFrame) -> pd.DataFrame:
    smap = build_sanitized_map(raw)

    s_sym = seriespick(raw, smap, "symbol", "security_symbol", "securityname", "security_name")
    s_ser = seriespick(raw, smap, "series")
    s_dt  = seriespick(raw, smap, "date1", "date", "timestamp", "tradedate", "trade_date", "traded_date")

    s_open  = seriespick(raw, smap, "open_price", "open")
    s_high  = seriespick(raw, smap, "high_price", "high")
    s_low   = seriespick(raw, smap, "low_price", "low")
    s_close = seriespick(raw, smap, "close_price", "last_price", "close")

    s_prev  = seriespick(raw, smap, "prev_close", "prevclose", "previous_close", "previousclose")
    s_vol   = seriespick(raw, smap, "ttl_trd_qnty", "tottrdqty", "totaltradedquantity", "volume")
    s_tov   = seriespick(raw, smap, "turnover_lacs", "tottrdval", "turnover")

    df = pd.DataFrame({
        "Date":      parse_date_series(s_dt).dt.strftime("%Y-%m-%d") if s_dt is not None else None,
        "Symbol":    s_sym.astype(str).str.upper().str.strip() if s_sym is not None else None,
        "Series":    s_ser.astype(str).str.upper().str.strip() if s_ser is not None else None,
        "Open":      pd.to_numeric(s_open,  errors="coerce"),
        "High":      pd.to_numeric(s_high,  errors="coerce"),
        "Low":       pd.to_numeric(s_low,   errors="coerce"),
        "Close":     pd.to_numeric(s_close, errors="coerce"),
        "PrevClose": pd.to_numeric(s_prev,  errors="coerce"),
        "Volume":    pd.to_numeric(s_vol,   errors="coerce"),
        "Turnover":  pd.to_numeric(s_tov,   errors="coerce"),
    })

    df = df.dropna(subset=["Date","Symbol"]).copy()
    # Keep only EQ/BE/SM etc. if Series present; otherwise pass through
    if "Series" in df.columns and df["Series"].notna().any():
        df["Series"] = df["Series"].fillna("")
    return df[KEEP_COLS]

# ---------- Discover local bhav files ----------
def bhav_candidates() -> List[str]:
    out = []
    for p in sorted(set(glob.glob(os.path.join(PRICES_DIR, "*.csv")))):
        name = os.path.basename(p).lower()
        if name in {"bhav_hist.csv", "bhav_latest.csv"}:
            continue
        # quick sniff to ensure bhav-ish schema
        try:
            head = read_csv_any(p).head(1)
        except Exception:
            continue
        cols = {_clean_col(c) for c in head.columns}
        if "symbol" in cols and "series" in cols and {"date","date1","timestamp"} & cols:
            out.append(p)
    return out

# ---------- Corporates normalization ----------
def normalize_corporates(csv_path: str, kind: str) -> Optional[pd.DataFrame]:
    try:
        raw = read_csv_any(csv_path)
    except Exception:
        return None
    smap = build_sanitized_map(raw)
    s_sym = seriespick(raw, smap, "symbol", "security_name", "securitysymbol")
    s_bs  = seriespick(raw, smap, "buy_sell", "buy_or_sell", "buysell", "deal_type")
    s_qty = seriespick(raw, smap, "quantity", "qty", "quantity_traded")
    s_pr  = seriespick(raw, smap, "price", "avg_price")
    s_dt  = seriespick(raw, smap, "date", "deal_date", "traded_date")
    s_cn  = seriespick(raw, smap, "client_name", "buyer_name", "seller_name")

    base = {
        "Date":   parse_date_series(s_dt).dt.strftime("%Y-%m-%d") if s_dt is not None else None,
        "Symbol": s_sym.astype(str).str.upper().str.strip() if s_sym is not None else None,
        "Client_Name": s_cn if s_cn is not None else None,
        "Buy_Sell": s_bs.astype(str).str.upper().str.replace("PURCHASE","BUY") if s_bs is not None else None,
        "Quantity": pd.to_numeric(s_qty, errors="coerce") if s_qty is not None else None,
        "Price":    pd.to_numeric(s_pr,  errors="coerce") if s_pr  is not None else None,
        "Source":   os.path.basename(csv_path),
    }
    df = pd.DataFrame(base).dropna(subset=["Date","Symbol"], how="any")
    return df if not df.empty else None

# ---------- Indicators ----------
def compute_indicators_and_latest(deliv_latest: Optional[pd.DataFrame]) -> Dict[str, Any]:
    try:
        hist = pd.read_csv(FILES["bhav_hist"])
    except Exception:
        hist = pd.DataFrame(columns=KEEP_COLS)
    if hist.empty:
        atomic_write_df(pd.DataFrame(columns=[
            "Date","Symbol","Series","Open","High","Low","Close","PrevClose","Volume",
            "Range","TR","ATR14","SMA10","SMA20","SMA50","BB_Mid","BB_Upper","BB_Lower",
            "BB_Bandwidth","CLV","RangeAvg20","VolAvg20","HI55","HI55_Recent","NR7","Delivery_Pct"
        ]), FILES["tech_latest"])
        return {"ok": False, "rows": 0, "note": "hist empty"}

    df = hist.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values(["Symbol","Date"])
    df["Range"] = df["High"] - df["Low"]
    tr1 = df["Range"]
    tr2 = (df["High"] - df["PrevClose"]).abs()
    tr3 = (df["Low"]  - df["PrevClose"]).abs()
    df["TR"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    g = df.groupby("Symbol", group_keys=False)
    df["ATR14"] = g["TR"].rolling(14, min_periods=14).mean().reset_index(level=0, drop=True)
    df["SMA10"] = g["Close"].rolling(10, min_periods=10).mean().reset_index(level=0, drop=True)
    df["SMA20"] = g["Close"].rolling(20, min_periods=20).mean().reset_index(level=0, drop=True)
    df["SMA50"] = g["Close"].rolling(50, min_periods=50).mean().reset_index(level=0, drop=True)
    std20 = g["Close"].rolling(20, min_periods=20).std(ddof=0).reset_index(level=0, drop=True)
    df["BB_Mid"] = df["SMA20"]
    df["BB_Upper"] = df["BB_Mid"] + 2*std20
    df["BB_Lower"] = df["BB_Mid"] - 2*std20
    df["BB_Bandwidth"] = (df["BB_Upper"] - df["BB_Lower"]) / df["BB_Mid"]
    df["CLV"] = 0.5
    nz = (df["High"] != df["Low"])
    df.loc[nz, "CLV"] = (df.loc[nz, "Close"] - df.loc[nz, "Low"]) / (df.loc[nz, "High"] - df.loc[nz, "Low"])
    df["RangeAvg20"] = g["Range"].rolling(20, min_periods=20).mean().reset_index(level=0, drop=True)
    df["VolAvg20"] = g["Volume"].rolling(20, min_periods=20).mean().reset_index(level=0, drop=True)
    roll_hi55 = g["High"].rolling(55, min_periods=55).max().reset_index(level=0, drop=True)
    df["HI55"] = (df["High"] == roll_hi55)
    df["HI55_Recent"] = g["HI55"].rolling(5, min_periods=1).max().reset_index(level=0, drop=True).astype(bool)
    rng7min = g["Range"].rolling(7, min_periods=7).min().reset_index(level=0, drop=True)
    df["NR7"] = (df["Range"] == rng7min)

    last_date = df["Date"].max()
    latest = df[df["Date"] == last_date].copy()
    if deliv_latest is not None and not deliv_latest.empty:
        dl = deliv_latest.copy()
        dl["Date"] = pd.to_datetime(dl["Date"])
        dl = dl[dl["Date"] == last_date][["Symbol","Delivery_Pct"]]
        latest = latest.merge(dl, on="Symbol", how="left")
    else:
        latest["Delivery_Pct"] = None

    cols = ["Date","Symbol","Series","Open","High","Low","Close","PrevClose","Volume",
            "Range","TR","ATR14","SMA10","SMA20","SMA50","BB_Mid","BB_Upper","BB_Lower",
            "BB_Bandwidth","CLV","RangeAvg20","VolAvg20","HI55","HI55_Recent","NR7","Delivery_Pct"]
    atomic_write_df(latest[cols], FILES["tech_latest"])
    return {"ok": True, "rows": int(len(latest)), "last_date": last_date.strftime("%Y-%m-%d")}

# ---------- Main ----------
def main():
    ensure_dirs()
    # Load existing hist
    try:
        hist = pd.read_csv(FILES["bhav_hist"])
    except Exception:
        hist = pd.DataFrame(columns=KEEP_COLS)

    files = []
    try:
        files = bhav_candidates()
    except Exception:
        files = []

    diagnostics = []
    total_added = 0
    last_day_seen = None
    last_raw_path = None
    last_raw_date = None

    for path in files:
        entry = {"file": os.path.basename(path)}
        try:
            raw = read_csv_any(path)
            entry["raw_rows"] = int(len(raw))
            entry["raw_cols"] = list(map(str, list(raw.columns)[:12]))

            # detect file date from the best available date-like col (duplicate-safe)
            smap = build_sanitized_map(raw)
            s_dt = seriespick(raw, smap, "date1", "date", "timestamp", "tradedate", "trade_date", "traded_date")
            file_date = parse_date_series(s_dt)
            fdate = None
            if file_date.notna().any():
                fdate = file_date.dropna().iloc[0]
                entry["detected_date"] = str(fdate.date())

            df = normalize_bhav(raw)
            entry["norm_rows"] = int(len(df))
            entry["norm_null_date"] = int(df["Date"].isna().sum()) if "Date" in df.columns else None
            entry["norm_null_symbol"] = int(df["Symbol"].isna().sum()) if "Symbol" in df.columns else None

            if not df.empty:
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
                if not hist.empty:
                    key = pd.MultiIndex.from_frame(hist[["Date","Symbol"]])
                    mkey = pd.MultiIndex.from_frame(df[["Date","Symbol"]])
                    mask_new = ~mkey.isin(key)
                    df_new = df.loc[mask_new]
                else:
                    df_new = df
                entry["new_rows"] = int(len(df_new))
                if len(df_new) > 0:
                    hist = pd.concat([hist, df_new], ignore_index=True)
                    total_added += len(df_new)

                # track latest raw (by detected file date)
                if fdate is not None and (last_raw_date is None or fdate > last_raw_date):
                    last_raw_date = fdate
                    last_raw_path = path
                    last_day_seen = fdate

        except Exception as e:
            entry["error"] = str(e)

        diagnostics.append(entry)

    # finalize hist & write
    if not hist.empty:
        hist["Date"] = pd.to_datetime(hist["Date"], errors="coerce")
        hist = hist.dropna(subset=["Date","Symbol"])
        hist = hist.drop_duplicates(subset=["Date","Symbol"], keep="last")
        cutoff = hist["Date"].max() - timedelta(days=460)
        hist = hist[hist["Date"] >= cutoff].copy()
        hist_out = hist.sort_values(["Date","Symbol"]).assign(Date=hist["Date"].dt.strftime("%Y-%m-%d"))
        atomic_write_df(hist_out[KEEP_COLS], FILES["bhav_hist"])

    # copy latest raw bhav to bhav_latest.csv (unmodified)
    if last_raw_path:
        with open(last_raw_path, "rb") as src, open(FILES["bhav_latest"], "wb") as dst:
            dst.write(src.read())

    # deliverables_latest from latest raw
    deliv_rows = 0
    try:
        if last_raw_path:
            raw = read_csv_any(last_raw_path)
            smap = build_sanitized_map(raw)
            s_sym = seriespick(raw, smap, "symbol")
            s_ser = seriespick(raw, smap, "series")
            s_dt  = seriespick(raw, smap, "date1", "date", "timestamp", "tradedate", "trade_date", "traded_date")
            s_dq  = seriespick(raw, smap, "deliv_qty", "deliv_qty_shares_")
            s_tq  = seriespick(raw, smap, "ttl_trd_qnty", "tottrdqty", "volume")
            s_dp  = seriespick(raw, smap, "deliv_per")

            if s_sym is not None and s_ser is not None and s_dt is not None and (s_dq is not None or s_dp is not None):
                df = pd.DataFrame({
                    "Date":   parse_date_series(s_dt).dt.strftime("%Y-%m-%d"),
                    "Symbol": s_sym.astype(str).str.upper().str.strip(),
                    "Series": s_ser.astype(str).str.upper().str.strip(),
                    "Deliverable_Qty": pd.to_numeric(s_dq, errors="coerce") if s_dq is not None else None,
                    "Traded_Qty":      pd.to_numeric(s_tq, errors="coerce") if s_tq is not None else None,
                    "Delivery_Pct":    pd.to_numeric(s_dp, errors="coerce") if s_dp is not None else None,
                }).dropna(subset=["Date","Symbol"])
                if not df.empty:
                    atomic_write_df(df, FILES["deliverables"])
                    deliv_rows = int(len(df))
    except Exception:
        pass

    # corporates snapshots
    def latest_by_mtime(glob_pat: str) -> Optional[str]:
        paths = glob.glob(glob_pat)
        if not paths: return None
        paths.sort(key=lambda p: os.path.getmtime(p))
        return paths[-1]

    bulk_src  = latest_by_mtime(os.path.join(DATA_DIR, "Bulk-Deals-*.csv"))
    block_src = latest_by_mtime(os.path.join(DATA_DIR, "Block-Deals-*.csv"))
    bulk_rows = block_rows = 0
    if bulk_src:
        bdf = normalize_corporates(bulk_src, "bulk")
        if bdf is not None and not bdf.empty:
            atomic_write_df(bdf, FILES["bulk_latest"]); bulk_rows = int(len(bdf))
    if block_src:
        bdf = normalize_corporates(block_src, "block")
        if bdf is not None and not bdf.empty:
            atomic_write_df(bdf, FILES["block_latest"]); block_rows = int(len(bdf))

    # 52w & indicators
    hi_rows = 0
    try:
        dfh = pd.read_csv(FILES["bhav_hist"])
        if not dfh.empty:
            dfh["Date"] = pd.to_datetime(dfh["Date"])
            cutoff = dfh["Date"].max() - timedelta(days=370)
            dfw = dfh[dfh["Date"] >= cutoff].copy()
            agg = dfw.groupby("Symbol").agg(High52W=("High","max"), Low52W=("Low","min")).reset_index()
            hi = dfw.merge(agg[["Symbol","High52W"]], on="Symbol").query("High==High52W")
            hi = hi.sort_values(["Symbol","Date"]).groupby("Symbol").tail(1)[["Symbol","Date"]].rename(columns={"Date":"High52W_Date"})
            lo = dfw.merge(agg[["Symbol","Low52W"]], on="Symbol").query("Low==Low52W")
            lo = lo.sort_values(["Symbol","Date"]).groupby("Symbol").tail(1)[["Symbol","Date"]].rename(columns={"Date":"Low52W_Date"})
            out = agg.merge(hi, on="Symbol", how="left").merge(lo, on="Symbol", how="left")
            for c in ["High52W_Date","Low52W_Date"]:
                out[c] = pd.to_datetime(out[c], errors="coerce").dt.strftime("%Y-%m-%d")
            atomic_write_df(out[["Symbol","High52W","High52W_Date","Low52W","Low52W_Date"]], FILES["hi52w"])
            hi_rows = int(len(out))
    except Exception:
        pass

    tech_res = compute_indicators_and_latest(
        pd.read_csv(FILES["deliverables"]) if os.path.exists(FILES["deliverables"]) else None
    )

    # status dump
    last_hist_date = None
    hist_rows = 0
    if os.path.exists(FILES["bhav_hist"]):
        try:
            h = pd.read_csv(FILES["bhav_hist"])
            hist_rows = int(len(h))
            if not h.empty:
                last_hist_date = str(pd.to_datetime(h["Date"]).max().date())
        except Exception:
            pass

    status = {
        "ok": True,
        "when": datetime.utcnow().isoformat()+"Z",
        "inputs": {
            "bhav_files_seen": [os.path.basename(p) for p in files],
            "latest_raw_file": None if last_raw_path is None else os.path.basename(last_raw_path),
            "latest_raw_date": None if last_raw_date is None else str(last_raw_date.date()),
            "bulk_src":  None if bulk_src  is None else os.path.basename(bulk_src),
            "block_src": None if block_src is None else os.path.basename(block_src),
        },
        "outputs": {
            "bhav_hist_rows": hist_rows,
            "bhav_hist_last_date": last_hist_date,
            "deliverables_rows": deliv_rows,
            "bulk_rows": bulk_rows,
            "block_rows": block_rows,
            "hi52w_rows": hi_rows,
            "tech_latest_rows": tech_res.get("rows",0),
            "tech_latest_last_date": tech_res.get("last_date"),
        },
        "diagnostics": diagnostics
    }
    atomic_write_text(json.dumps(status, indent=2), FILES["status"])
    print(json.dumps(status, indent=2))

if __name__ == "__main__":
    raise SystemExit(main())
