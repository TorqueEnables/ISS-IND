#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
StakeLens — Manual Upload Processor (local-only)
- No network calls.
- Ingest raw NSE bhav CSVs you drop under data/prices/.
- Normalize + append to data/prices/bhav_hist.csv (de-dup on Date+Symbol; ~15 months kept).
- Copy the latest raw bhav to data/prices/bhav_latest.csv (to keep V1 compatibility).
- Derive:
    - data/deliverables_latest.csv (from bhav's DELIV columns)
    - data/highlow_52w.csv
    - data/tech_latest.csv (ATR14/SMA/BB/NR7 + Delivery_Pct)
- Normalize corporates if present:
    - data/Bulk-Deals-*.csv  -> data/bulk_deals_latest.csv
    - data/Block-Deals-*.csv -> data/block_deals_latest.csv
- Write data/process_status.json with deep diagnostics.
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
    "bhav_latest":   os.path.join(PRICES_DIR, "bhav_latest.csv"),  # raw copy of the latest daily CSV
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

# ---------- Header sanitation ----------
_SANITIZE_RE = re.compile(r"[^a-z0-9_]+")

def _clean_col(name: str) -> str:
    key = str(name).replace("\ufeff","").strip().lower()
    key = key.replace("-", "_").replace(" ", "_")
    key = _SANITIZE_RE.sub("_", key)
    key = re.sub(r"_+", "_", key).strip("_")
    return key

CANON = {
    "date": "Date", "date1": "Date", "timestamp":"Date","tradedate":"Date","trade_date":"Date","traded_date":"Date",
    "symbol":"Symbol", "series":"Series",
    "open":"Open","open_price":"Open",
    "high":"High","high_price":"High",
    "low":"Low","low_price":"Low",
    "close":"Close","close_price":"Close","last_price":"Close",
    "prevclose":"PrevClose","prev_close":"PrevClose","previousclose":"PrevClose","previous_close":"PrevClose",
    "volume":"Volume","tottrdqty":"Volume","ttl_trd_qnty":"Volume","totaltradedquantity":"Volume",
    "turnover":"Turnover","tottrdval":"Turnover","turnover_lacs":"Turnover",
    # deliverables in sec_bhavdata_full
    "deliv_qty":"DELIV_QTY","deliv_per":"DELIV_PER","deliv_qty_shares_":"DELIV_QTY",
}

def _parse_date_series(s: pd.Series) -> pd.Series:
    d = pd.to_datetime(s, errors="coerce")
    if d.notna().sum() >= max(1, int(0.7*len(s))): return d
    d2 = pd.to_datetime(s, errors="coerce", dayfirst=True); d = d.fillna(d2)
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        d3 = pd.to_datetime(s, format=fmt, errors="coerce"); d = d.fillna(d3)
    return d

def normalize_bhav(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()
    # map columns
    new_cols = []
    for c in df.columns:
        key = _clean_col(c)
        new_cols.append(CANON.get(key, str(c).strip()))
    df.columns = new_cols
    # ensure required columns
    for k in KEEP_COLS:
        if k not in df.columns:
            df[k] = None
    # coerce
    df["Symbol"] = df["Symbol"].astype(str).str.upper().str.strip()
    df["Series"] = df["Series"].astype(str).str.upper().str.strip()
    df["Date"]   = _parse_date_series(df["Date"]).dt.strftime("%Y-%m-%d")
    for c in ["Open","High","Low","Close","PrevClose","Volume","Turnover"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    out = df[KEEP_COLS].dropna(subset=["Date","Symbol"])
    return out

# ---------- Discover local bhav files ----------
def bhav_candidates() -> List[str]:
    out = []
    for p in sorted(set(glob.glob(os.path.join(PRICES_DIR, "*.csv")))):
        name = os.path.basename(p).lower()
        if name == "bhav_hist.csv" or name == "bhav_latest.csv":
            continue
        try:
            head = pd.read_csv(p, nrows=1, dtype=str, skipinitialspace=True)
        except Exception:
            try:
                head = pd.read_csv(p, nrows=1, dtype=str, skipinitialspace=True, encoding="latin-1")
            except Exception:
                continue
        cols = list(map(str, head.columns))
        cols_sanitized = set(_clean_col(c) for c in cols)
        # minimally expect symbol+series and a date-ish column
        if "symbol" in cols_sanitized and "series" in cols_sanitized and ({"date","date1","timestamp"} & cols_sanitized):
            out.append(p)
    return out

# ---------- Corporates normalization ----------
def normalize_corporates(csv_path: str, kind: str) -> Optional[pd.DataFrame]:
    try:
        raw = pd.read_csv(csv_path, dtype=str, skipinitialspace=True)
    except Exception:
        try:
            raw = pd.read_csv(csv_path, dtype=str, skipinitialspace=True, encoding="latin-1")
        except Exception:
            return None
    # sanitize headers
    m = {}
    for c in raw.columns:
        key = _clean_col(c)
        m[c] = key
    df = raw.rename(columns=m)
    # map common fields seen in NSE downloads
    sym  = df.filter(regex=r"^symbol$|^security_name$|^securitysymbol$").copy()
    if sym.shape[1]==0: return None
    df["Symbol"] = sym.iloc[:,0].astype(str).str.upper().str.strip()

    if kind == "bulk":
        bs = df.filter(regex=r"^buy_sell$|^buy_or_sell$|^buysell$|^deal_type$").copy()
        qty= df.filter(regex=r"^quantity$|^qty$|^quantity_traded$").copy()
        pr = df.filter(regex=r"^price$|^avg_price$").copy()
        dt = df.filter(regex=r"^date$|^deal_date$|^traded_date$").copy()
        cn = df.filter(regex=r"^client_name$|^buyer_name$|^seller_name$").copy()
        out = pd.DataFrame({
            "Date": pd.to_datetime(dt.iloc[:,0], errors="coerce").dt.strftime("%Y-%m-%d") if dt.shape[1] else None,
            "Symbol": df["Symbol"],
            "Client_Name": cn.iloc[:,0] if cn.shape[1] else None,
            "Buy_Sell": bs.iloc[:,0].str.upper().str.replace("PURCHASE","BUY") if bs.shape[1] else None,
            "Quantity": pd.to_numeric(qty.iloc[:,0], errors="coerce") if qty.shape[1] else None,
            "Price": pd.to_numeric(pr.iloc[:,0], errors="coerce") if pr.shape[1] else None,
            "Source": os.path.basename(csv_path),
        })
        return out

    if kind == "block":
        bs = df.filter(regex=r"^buy_sell$|^deal_type$|^buysell$").copy()
        qty= df.filter(regex=r"^quantity$|^qty$").copy()
        pr = df.filter(regex=r"^price$|^avg_price$").copy()
        dt = df.filter(regex=r"^date$|^deal_date$|^traded_date$").copy()
        cn = df.filter(regex=r"^client_name$|^buyer_name$|^seller_name$").copy()
        out = pd.DataFrame({
            "Date": pd.to_datetime(dt.iloc[:,0], errors="coerce").dt.strftime("%Y-%m-%d") if dt.shape[1] else None,
            "Symbol": df["Symbol"],
            "Client_Name": cn.iloc[:,0] if cn.shape[1] else None,
            "Buy_Sell": bs.iloc[:,0].str.upper().str.replace("PURCHASE","BUY") if bs.shape[1] else None,
            "Quantity": pd.to_numeric(qty.iloc[:,0], errors="coerce") if qty.shape[1] else None,
            "Price": pd.to_numeric(pr.iloc[:,0], errors="coerce") if pr.shape[1] else None,
            "Source": os.path.basename(csv_path),
        })
        return out

    return None

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

    # Discover bhav CSVs you dropped
    files = bhav_candidates()
    diagnostics = []
    total_added = 0
    last_day_seen = None
    last_raw_path = None
    last_raw_date = None

    for path in files:
        entry = {"file": os.path.basename(path)}
        # tolerant read
        try:
            try:
                raw = pd.read_csv(path, dtype=str, skipinitialspace=True)
            except Exception:
                raw = pd.read_csv(path, dtype=str, skipinitialspace=True, encoding="latin-1")

            entry["raw_rows"] = int(len(raw))
            entry["raw_cols"] = list(map(str, list(raw.columns)[:12]))

            # try to detect the date of this file for “latest raw” copy
            dt_col = None
            for c in raw.columns:
                key = _clean_col(c)
                if key in {"date","date1","timestamp","tradedate","trade_date","traded_date"}:
                    dt_col = c; break
            file_date = None
            if dt_col is not None:
                dts = pd.to_datetime(raw[dt_col], errors="coerce", dayfirst=True)
                if dts.notna().any():
                    file_date = dts.dropna().iloc[0]
            entry["detected_date"] = None if file_date is None else str(file_date.date())

            df = normalize_bhav(raw)
            entry["norm_rows"] = int(len(df))
            entry["norm_null_date"] = int(df["Date"].isna().sum()) if "Date" in df.columns else None
            entry["norm_null_symbol"] = int(df["Symbol"].isna().sum()) if "Symbol" in df.columns else None

            # new rows vs hist
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

                # track latest raw
                if file_date is not None and (last_raw_date is None or file_date > last_raw_date):
                    last_raw_date = file_date
                    last_raw_path = path
                    last_day_seen = file_date

        except Exception as e:
            entry["error"] = str(e)

        diagnostics.append(entry)

    # finalize hist window & write
    if not hist.empty:
        hist["Date"] = pd.to_datetime(hist["Date"], errors="coerce")
        hist = hist.dropna(subset=["Date","Symbol"])
        hist = hist.drop_duplicates(subset=["Date","Symbol"], keep="last")
        cutoff = hist["Date"].max() - timedelta(days=460)
        hist = hist[hist["Date"] >= cutoff].copy()
        hist_out = hist.sort_values(["Date","Symbol"]).assign(Date=hist["Date"].dt.strftime("%Y-%m-%d"))
        atomic_write_df(hist_out[KEEP_COLS], FILES["bhav_hist"])

    # copy latest raw bhav to bhav_latest.csv (unmodified) for V1 compatibility
    if last_raw_path:
        with open(last_raw_path, "rb") as src, open(FILES["bhav_latest"], "wb") as dst:
            dst.write(src.read())

    # deliverables_latest from latest raw
    deliv_rows = 0
    try:
        if last_raw_path:
            try:
                raw = pd.read_csv(last_raw_path, dtype=str, skipinitialspace=True)
            except Exception:
                raw = pd.read_csv(last_raw_path, dtype=str, skipinitialspace=True, encoding="latin-1")
            # map columns
            colmap = {_clean_col(c): c for c in raw.columns}
            def colpick(*keys):
                for k in keys:
                    if k in colmap: return colmap[k]
                return None
            c_sym = colpick("symbol")
            c_ser = colpick("series")
            c_dt  = colpick("date","date1","timestamp","tradedate","trade_date","traded_date")
            c_dq  = colpick("deliv_qty","deliv_qty_shares_")
            c_tq  = colpick("ttl_trd_qnty","tottrdqty","volume")
            c_dp  = colpick("deliv_per")
            if c_sym and c_ser and c_dt and (c_dq or c_dp):
                df = pd.DataFrame({
                    "Date": pd.to_datetime(raw[c_dt], errors="coerce", dayfirst=True).dt.strftime("%Y-%m-%d"),
                    "Symbol": raw[c_sym].astype(str).str.upper().str.strip(),
                    "Series": raw[c_ser].astype(str).str.upper().str.strip(),
                    "Deliverable_Qty": pd.to_numeric(raw[c_dq], errors="coerce") if c_dq else None,
                    "Traded_Qty": pd.to_numeric(raw[c_tq], errors="coerce") if c_tq else None,
                    "Delivery_Pct": pd.to_numeric(raw[c_dp], errors="coerce") if c_dp else None,
                })
                df = df.dropna(subset=["Date","Symbol"])
                atomic_write_df(df, FILES["deliverables"])
                deliv_rows = int(len(df))
    except Exception:
        pass

    # corporates snapshots (optional)
    # pick the "latest modified" Bulk/Block file, normalize, write *_latest.csv
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
            "latest_raw_date": None if last_day_seen is None else str(last_day_seen.date()),
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
