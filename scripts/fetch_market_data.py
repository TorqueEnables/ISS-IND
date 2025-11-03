#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
StakeLens Option A — Small, Fast, Daily Append (header-driven local ingest)
- Network try (tiny budget). OK if it fails.
- Robust local ingest from data/prices/ by HEADER SHAPE, not filename:
    accepts bhav-style CSVs (SYMBOL/SERIES + DATE/DATE1 + price/volume cols)
- Tolerant normalization (skip-initial-space, dtype=str, multi-format date parsing).
- Append into data/prices/bhav_hist.csv (de-dup by Date+Symbol; rolling window).
- Derive data/highlow_52w.csv and data/tech_latest.csv.
- Write data/fetch_status.json with deep diagnostics (including per-file ingest stats).
"""

from __future__ import annotations
import os, io, json, zipfile, time, glob
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------- Config ----------
IST = timezone(timedelta(hours=5, minutes=30))
DATA_DIR = "data"
PRICES_DIR = os.path.join(DATA_DIR, "prices")
FILES = {
    "status":              os.path.join(DATA_DIR, "fetch_status.json"),
    "deliverables_latest": os.path.join(DATA_DIR, "deliverables_latest.csv"),
    "deliverables_hist":   os.path.join(DATA_DIR, "deliverables_hist.csv"),
    "bulk_latest":         os.path.join(DATA_DIR, "bulk_deals_latest.csv"),
    "block_latest":        os.path.join(DATA_DIR, "block_deals_latest.csv"),
    "hi52w":               os.path.join(DATA_DIR, "highlow_52w.csv"),
    "bhav_hist":           os.path.join(PRICES_DIR, "bhav_hist.csv"),
    "tech_latest":         os.path.join(DATA_DIR, "tech_latest.csv"),
}

HTTP_TIMEOUT = 8
HTTP_RETRIES = 1
TOTAL_BHAV_BUDGET = 45
CORP_TIMEOUT = 6

def now_ist_dt(): return datetime.now(IST)
def now_ist_iso(): return now_ist_dt().isoformat()

def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True); os.makedirs(PRICES_DIR, exist_ok=True)

def atomic_write_df(df: pd.DataFrame, path: str):
    tmp = path + ".tmp"; df.to_csv(tmp, index=False); os.replace(tmp, path)

def atomic_write_text(txt: str, path: str):
    tmp = path + ".tmp"; open(tmp, "w", encoding="utf-8").write(txt); os.replace(tmp, path)

def ensure_headers(path: str, headers: List[str]):
    if not os.path.exists(path):
        atomic_write_df(pd.DataFrame(columns=headers), path)

def is_weekend(d: datetime) -> bool: return d.weekday() >= 5
def prev_biz_days(n: int, ref: Optional[datetime]=None) -> List[datetime]:
    ref = ref or now_ist_dt(); out, d = [], ref
    while len(out) < n:
        d = d - timedelta(days=1)
        if not is_weekend(d): out.append(d.replace(hour=0, minute=0, second=0, microsecond=0))
    return out
def biz_age_days(from_date: datetime, to_date: Optional[datetime]=None) -> int:
    to_date = to_date or now_ist_dt(); d, cnt = from_date, 0
    while d.date() < to_date.date():
        d += timedelta(days=1)
        if not is_weekend(d): cnt += 1
        if cnt > 10000: break
    return cnt

# ---------- HTTP ----------
def session_fast() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Accept": "*/*", "Accept-Language": "en-US,en;q=0.7", "Connection": "keep-alive",
    })
    retry = Retry(total=HTTP_RETRIES, connect=HTTP_RETRIES, read=HTTP_RETRIES,
                  status=HTTP_RETRIES, backoff_factor=0.3,
                  status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=frozenset(["GET"]))
    ad = HTTPAdapter(max_retries=retry)
    s.mount("https://", ad); s.mount("http://", ad)
    return s

# ---------- Normalization ----------
CANON = {
    "date": "Date", "timestamp": "Date", "date1": "Date", "tradedate": "Date", "traded_date": "Date",
    "symbol": "Symbol", "series": "Series",
    "open": "Open", "open_price": "Open",
    "high": "High", "high_price": "High",
    "low":  "Low",  "low_price":  "Low",
    "close": "Close", "close_price": "Close", "last_price": "Close",
    "prevclose": "PrevClose", "prev_close": "PrevClose", "previousclose": "PrevClose", "previous_close": "PrevClose",
    "volume": "Volume", "tottrdqty": "Volume", "ttl_trd_qnty": "Volume", "totaltradedquantity": "Volume",
    "turnover": "Turnover", "tottrdval": "Turnover", "turnover_lacs": "Turnover",
}
KEEP_COLS = ["Date","Symbol","Series","Open","High","Low","Close","PrevClose","Volume","Turnover"]

def _parse_date_series(s: pd.Series) -> pd.Series:
    d = pd.to_datetime(s, errors="coerce")
    if d.notna().sum() >= max(1, int(0.7*len(s))): return d
    d2 = pd.to_datetime(s, errors="coerce", dayfirst=True); d = d.fillna(d2)
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        d3 = pd.to_datetime(s, format=fmt, errors="coerce"); d = d.fillna(d3)
    return d

def normalize_bhav(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=KEEP_COLS)
    df = df.copy()
    # normalize headers: strip → lower → replace spaces/dashes → map via CANON
    new_cols = []
    for c in df.columns:
        key = str(c).strip().lower().replace(" ", "_").replace("-", "_")
        new_cols.append(CANON.get(key, str(c).strip()))
    df.columns = new_cols
    # ensure required cols
    for k in KEEP_COLS:
        if k not in df.columns: df[k] = None
    # clean
    df["Symbol"] = df["Symbol"].astype(str).str.upper().str.strip()
    df["Series"] = df["Series"].astype(str).str.upper().str.strip()
    # date
    dts = _parse_date_series(df["Date"])
    df["Date"] = dts.dt.strftime("%Y-%m-%d")
    # numerics
    for c in ["Open","High","Low","Close","PrevClose","Volume","Turnover"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    out = df[KEEP_COLS].dropna(subset=["Date","Symbol"])
    return out

# ---------- Network latest (fast) ----------
def try_fetch_bhav_for_date(s: requests.Session, d: datetime) -> Dict[str, Any]:
    ddmmyyyy = d.strftime("%d%m%Y"); DD, MON, YYYY = d.strftime("%d"), d.strftime("%b").upper(), d.strftime("%Y")
    urls = [
        f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{ddmmyyyy}.csv",
        f"http://archives.nseindia.com/products/content/sec_bhavdata_full_{ddmmyyyy}.csv",
        f"https://archives.nseindia.com/content/historical/EQUITIES/{YYYY}/{MON}/cm{DD}{MON}{YYYY}bhav.csv.zip",
        f"http://archives.nseindia.com/content/historical/EQUITIES/{YYYY}/{MON}/cm{DD}{MON}{YYYY}bhav.csv.zip",
    ]
    tried = []
    for u in urls[:2]:
        tried.append(u)
        try:
            r = s.get(u, timeout=HTTP_TIMEOUT, headers={"Referer":"https://www.nseindia.com/market-data"})
            if r.status_code == 200 and r.text and "SYMBOL" in r.text.upper():
                raw = pd.read_csv(io.StringIO(r.text), dtype=str, skipinitialspace=True)
                out = normalize_bhav(raw)
                if not out.empty:
                    day = out["Date"].dropna().iloc[0]
                    return {"ok": True, "date": day, "rows": int(len(out)), "df": out, "tried": tried}
        except Exception:
            pass
    for u in urls[2:]:
        tried.append(u)
        try:
            r = s.get(u, timeout=HTTP_TIMEOUT, headers={"Referer":"https://www.nseindia.com/market-data"})
            if r.status_code != 200 or not r.content: continue
            with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), None)
                if not name: continue
                raw_text = zf.read(name).decode("utf-8", errors="ignore")
            raw = pd.read_csv(io.StringIO(raw_text), dtype=str, skipinitialspace=True)
            out = normalize_bhav(raw)
            if not out.empty:
                day = out["Date"].dropna().iloc[0]
                return {"ok": True, "date": day, "rows": int(len(out)), "df": out, "tried": tried}
        except Exception:
            pass
    return {"ok": False, "date": d.strftime("%Y-%m-%d"), "rows": 0, "df": None, "tried": tried}

def fetch_latest_bhav_failfast() -> Dict[str, Any]:
    start = time.time(); s = session_fast()
    candidates = [now_ist_dt()] + prev_biz_days(2)
    tried_all = []
    for d in candidates:
        if is_weekend(d): continue
        if time.time() - start > TOTAL_BHAV_BUDGET: break
        res = try_fetch_bhav_for_date(s, d)
        tried_all.extend(res.get("tried", []))
        if res["ok"]:
            return {"ok": True, "date": res["date"], "rows": res["rows"], "df": res["df"], "tried": tried_all, "budget_s": int(time.time()-start)}
    return {"ok": False, "date": None, "rows": 0, "df": None, "tried": tried_all, "budget_s": int(time.time()-start)}

# ---------- Corporates (best-effort) ----------
def fetch_corporates_one(kind: str) -> Dict[str, Any]:
    s = session_fast()
    urls = (
        ["https://www.nseindia.com/api/corporates-bulk-deals?index=equities",
         "https://www.nseindia.com/api/corporates/bulk-deals?index=equities",
         "https://www.nseindia.com/api/corporates-pit?index=bulk-deals"]
        if kind=="bulk" else
        ["https://www.nseindia.com/api/corporates-block-deals?index=equities",
         "https://www.nseindia.com/api/corporates/block-deals?index=equities",
         "https://www.nseindia.com/api/corporates-pit?index=block-deals"]
    )
    keymap = {"symbol":"Symbol","clientName":"Client_Name","buySell":"Buy_Sell","quantity":"Quantity","avgPrice":"Price","price":"Price","dealDate":"Date","date":"Date"}
    tried, recs = [], []
    for u in urls:
        tried.append(u)
        try:
            r = s.get(u, timeout=CORP_TIMEOUT, headers={"Accept":"application/json, text/plain, */*","Referer":"https://www.nseindia.com/report-detail/display-bulk-and-block-deals"})
            if r.status_code != 200: continue
            obj = r.json()
            rows = obj.get("data") if isinstance(obj, dict) else (obj if isinstance(obj, list) else [])
            if not rows and isinstance(obj, dict):
                for v in obj.values():
                    if isinstance(v, list): rows = v; break
            if not rows: continue
            for rec in rows:
                out = {}
                for src, dst in keymap.items():
                    if src in rec: out[dst] = rec[src]
                recs.append(out)
            break
        except Exception:
            continue
    if recs:
        df = pd.DataFrame(recs)
        if not df.empty:
            df["Symbol"] = df.get("Symbol", "").astype(str).str.upper().str.strip()
            if "Buy_Sell" in df.columns:
                df["Buy_Sell"] = df["Buy_Sell"].astype(str).str.upper().str.replace("PURCHASE","BUY").str.replace("SELLL","SELL")
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
            for c in ("Quantity","Price"):
                if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
            keep = ["Date","Symbol","Client_Name","Buy_Sell","Quantity","Price","Source"]
            df["Source"] = urls[0]
            for k in keep:
                if k not in df.columns: df[k] = None
            return {"ok": True, "rows": int(len(df)), "df": df[keep], "tried": tried}
    return {"ok": False, "rows": 0, "df": None, "tried": tried}

def fetch_mto_latest() -> Dict[str, Any]:
    s = session_fast(); rec = None; tried=[]
    for d in [now_ist_dt()] + prev_biz_days(6):
        if is_weekend(d): continue
        ddmmyyyy = d.strftime("%d%m%Y")
        for u in [f"https://archives.nseindia.com/archives/equities/mto/MTO_{ddmmyyyy}.DAT",
                  f"http://archives.nseindia.com/archives/equities/mto/MTO_{ddmmyyyy}.DAT"]:
            tried.append(u)
            try:
                r = s.get(u, timeout=HTTP_TIMEOUT, headers={"Referer":"https://www.nseindia.com/market-data"})
                if r.status_code == 200 and "SYMBOL" in r.text.upper():
                    rec = (d, r.text); break
            except Exception:
                continue
        if rec: break
    if not rec: return {"ok": False, "rows": 0, "tried": tried}
    d, txt = rec
    lines = [ln.strip() for ln in txt.replace("\t", ",").splitlines() if ln.strip()]
    hdr_idx = None
    for i, ln in enumerate(lines):
        up = ln.upper()
        if "SYMBOL" in up and "SERIES" in up: hdr_idx = i; break
    if hdr_idx is None: return {"ok": False, "rows": 0, "tried": tried}
    body = lines[hdr_idx+1:]
    rows = []
    for ln in body:
        if ln.upper().startswith(("TOTAL","GRAND")): break
        parts = [p.strip() for p in ln.split(",")]
        if len(parts) < 3: continue
        symbol = parts[0].upper().strip()
        series = parts[1].upper().strip()
        numeric = [p for p in parts if any(ch.isdigit() for ch in p)]
        dq, tq, pct = None, None, None
        for p in numeric:
            if "%" in p: pct = p
        if len(numeric) >= 2:
            tq = numeric[-1]
            dq = numeric[-2]
        rows.append([d.strftime("%Y-%m-%d"), symbol, series, _num(dq), _num(tq), _num(pct)])
    if not rows: return {"ok": False, "rows": 0, "tried": tried}
    df = pd.DataFrame(rows, columns=["Date","Symbol","Series","Deliverable_Qty","Traded_Qty","Delivery_Pct"])
    return {"ok": True, "rows": int(len(df)), "df": df, "tried": tried}
def _num(v):
    if v is None: return None
    s = str(v).replace(",","").replace("%","").strip()
    try: return float(s)
    except: return None

# ---------- Local ingest (HEADER-based) ----------
REQUIRED_ANY_DATE = {"Date","DATE","DATE1","Timestamp","TradeDate","TRADE_DATE"}
REQUIRED_SYMBOL = {"Symbol","SYMBOL"}
REQUIRED_SERIES = {"Series","SERIES"}
HINT_COLS = {"Open","OPEN","OPEN_PRICE","High","HIGH","HIGH_PRICE","Low","LOW","LOW_PRICE",
             "Close","CLOSE","CLOSE_PRICE","LAST_PRICE","PrevClose","PREV_CLOSE",
             "TTL_TRD_QNTY","TOTTRDQTY","VOLUME","TURNOVER_LACS","TOTTRDVAL"}

def looks_like_bhav_header(cols: List[str]) -> bool:
    cset = set(cols)
    has_date  = bool(cset & REQUIRED_ANY_DATE)
    has_sym   = bool(cset & REQUIRED_SYMBOL)
    has_series= bool(cset & REQUIRED_SERIES)
    has_hint  = bool(cset & HINT_COLS)
    return has_date and has_sym and has_series and has_hint

def discover_local_bhav_files() -> List[str]:
    out = []
    for p in sorted(set(glob.glob(os.path.join(PRICES_DIR, "*.csv")))):
        name = os.path.basename(p).lower()
        if name == "bhav_hist.csv":   # never ingest our own hist
            continue
        # quick header peek
        try:
            head = pd.read_csv(p, nrows=1, dtype=str, skipinitialspace=True)
            if looks_like_bhav_header(list(map(str, head.columns))):
                out.append(p)
        except Exception:
            # try latin-1
            try:
                head = pd.read_csv(p, nrows=1, dtype=str, skipinitialspace=True, encoding="latin-1")
                if looks_like_bhav_header(list(map(str, head.columns))):
                    out.append(p)
            except Exception:
                continue
    return out

def ingest_local_bhav_candidates() -> Dict[str, Any]:
    files = discover_local_bhav_files()
    used, diag = [], []
    total_rows_added = 0
    last_local_date = None

    try:
        hist = pd.read_csv(FILES["bhav_hist"])
    except Exception:
        hist = pd.DataFrame(columns=KEEP_COLS)
    if not hist.empty:
        hist["Date"] = pd.to_datetime(hist["Date"], errors="coerce")

    for path in files:
        entry = {"file": os.path.basename(path)}
        try:
            # tolerant read full
            try:
                raw = pd.read_csv(path, dtype=str, skipinitialspace=True)
            except Exception:
                raw = pd.read_csv(path, dtype=str, skipinitialspace=True, encoding="latin-1")
            entry["raw_rows"] = int(len(raw))
            entry["raw_cols"] = list(map(str, list(raw.columns)[:12]))

            df = normalize_bhav(raw)
            entry["norm_rows"] = int(len(df))
            entry["norm_null_date"] = int(df["Date"].isna().sum()) if "Date" in df.columns else None
            entry["norm_null_symbol"] = int(df["Symbol"].isna().sum()) if "Symbol" in df.columns else None

            if not df.empty:
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
                # dedupe vs hist
                if not hist.empty:
                    key = pd.MultiIndex.from_frame(hist[["Date","Symbol"]])
                    mkey = pd.MultiIndex.from_frame(df[["Date","Symbol"]])
                    mask_new = ~mkey.isin(key)
                    df_new = df.loc[mask_new]
                else:
                    df_new = df
                entry["new_rows"] = int(len(df_new))
                if not df_new.empty:
                    hist = pd.concat([hist, df_new], ignore_index=True)
                    used.append(entry["file"])
                    total_rows_added += len(df_new)
                    cand_date = df_new["Date"].max()
                    if last_local_date is None or cand_date > last_local_date:
                        last_local_date = cand_date
            else:
                entry["new_rows"] = 0
        except Exception as e:
            entry["error"] = str(e)
        diag.append(entry)

    if used:
        hist = hist.drop_duplicates(subset=["Date","Symbol"], keep="last")
        hist["Date"] = pd.to_datetime(hist["Date"], errors="coerce")
        cutoff = hist["Date"].max() - timedelta(days=460)
        hist = hist[hist["Date"] >= cutoff].copy()
        atomic_write_df(hist.sort_values(["Date","Symbol"]).assign(Date=hist["Date"].dt.strftime("%Y-%m-%d")), FILES["bhav_hist"])
        return {
            "used": True, "files": used,
            "added_rows": int(total_rows_added),
            "date": None if last_local_date is None else last_local_date.strftime("%Y-%m-%d"),
            "diagnostics": diag
        }
    else:
        return {"used": False, "files": [], "added_rows": 0, "date": None, "reason": "no_matching_or_empty", "diagnostics": diag}

# ---------- Indicators ----------
def compute_indicators_and_latest(deliv_latest: Optional[pd.DataFrame]) -> Dict[str, Any]:
    try:
        hist = pd.read_csv(FILES["bhav_hist"])
    except Exception:
        hist = pd.DataFrame(columns=KEEP_COLS)
    if hist.empty:
        ensure_headers(FILES["tech_latest"], ["Date","Symbol","Series","Open","High","Low","Close","PrevClose","Volume",
                                              "Range","TR","ATR14","SMA10","SMA20","SMA50","BB_Mid","BB_Upper","BB_Lower",
                                              "BB_Bandwidth","CLV","RangeAvg20","VolAvg20","HI55","HI55_Recent","NR7","Delivery_Pct"])
        return {"ok": False, "rows": 0, "note": "hist empty"}

    df = hist.copy()
    df["Date"] = pd.to_datetime(df["Date"]); df = df.sort_values(["Symbol","Date"])
    df["Range"] = df["High"] - df["Low"]
    tr1 = df["Range"]; tr2 = (df["High"] - df["PrevClose"]).abs(); tr3 = (df["Low"] - df["PrevClose"]).abs()
    df["TR"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    g = df.groupby("Symbol", group_keys=False)
    df["ATR14"] = g["TR"].rolling(14, min_periods=14).mean().reset_index(level=0, drop=True)
    df["SMA10"] = g["Close"].rolling(10, min_periods=10).mean().reset_index(level=0, drop=True)
    df["SMA20"] = g["Close"].rolling(20, min_periods=20).mean().reset_index(level=0, drop=True)
    df["SMA50"] = g["Close"].rolling(50, min_periods=50).mean().reset_index(level=0, drop=True)
    std20 = g["Close"].rolling(20, min_periods=20).std(ddof=0).reset_index(level=0, drop=True)
    df["BB_Mid"] = df["SMA20"]; df["BB_Upper"] = df["BB_Mid"] + 2*std20; df["BB_Lower"] = df["BB_Mid"] - 2*std20
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
    # headers
    ensure_headers(FILES["deliverables_latest"], ["Date","Symbol","Series","Deliverable_Qty","Traded_Qty","Delivery_Pct"])
    ensure_headers(FILES["deliverables_hist"],   ["Date","Symbol","Series","Deliverable_Qty","Traded_Qty","Delivery_Pct"])
    ensure_headers(FILES["bulk_latest"],         ["Date","Symbol","Client_Name","Buy_Sell","Quantity","Price","Source"])
    ensure_headers(FILES["block_latest"],        ["Date","Symbol","Client_Name","Buy_Sell","Quantity","Price","Source"])
    ensure_headers(FILES["hi52w"],               ["Symbol","High52W","High52W_Date","Low52W","Low52W_Date"])
    ensure_headers(FILES["bhav_hist"],           KEEP_COLS)
    ensure_headers(FILES["tech_latest"],         ["Date","Symbol","Series","Open","High","Low","Close","PrevClose","Volume",
                                                  "Range","TR","ATR14","SMA10","SMA20","SMA50","BB_Mid","BB_Upper","BB_Lower",
                                                  "BB_Bandwidth","CLV","RangeAvg20","VolAvg20","HI55","HI55_Recent","NR7","Delivery_Pct"])

    prev_last_date = None
    try:
        hh = pd.read_csv(FILES["bhav_hist"])
        if not hh.empty: prev_last_date = pd.to_datetime(hh["Date"]).max()
    except Exception:
        pass

    # 1) tiny network attempt
    bhav_res = fetch_latest_bhav_failfast()

    # If network succeeded, merge first
    if bhav_res["ok"]:
        try: hist = pd.read_csv(FILES["bhav_hist"])
        except Exception: hist = pd.DataFrame(columns=KEEP_COLS)
        new_day = bhav_res["df"].copy()
        new_day["Date"] = pd.to_datetime(new_day["Date"], errors="coerce")
        merged = pd.concat([hist, new_day], ignore_index=True).drop_duplicates(subset=["Date","Symbol"], keep="last")
        if not merged.empty:
            merged["Date"] = pd.to_datetime(merged["Date"], errors="coerce")
            cutoff = merged["Date"].max() - timedelta(days=460)
            merged = merged[merged["Date"] >= cutoff].copy()
            atomic_write_df(merged.sort_values(["Date","Symbol"]).assign(Date=merged["Date"].dt.strftime("%Y-%m-%d")), FILES["bhav_hist"])

    # 2) local ingest (header-driven)
    local_res = ingest_local_bhav_candidates()

    # current hist state
    hist_rows_after = 0; new_last_date = prev_last_date
    try:
        hist = pd.read_csv(FILES["bhav_hist"])
        if not hist.empty:
            hist_rows_after = int(len(hist))
            new_last_date = pd.to_datetime(hist["Date"]).max()
    except Exception:
        pass

    # 3) corporates
    bulk_res  = fetch_corporates_one("bulk")
    if bulk_res["ok"]: atomic_write_df(bulk_res["df"], FILES["bulk_latest"])
    block_res = fetch_corporates_one("block")
    if block_res["ok"]: atomic_write_df(block_res["df"], FILES["block_latest"])
    mto_res   = fetch_mto_latest()
    if mto_res["ok"]:
        atomic_write_df(mto_res["df"], FILES["deliverables_latest"])
        try: mh = pd.read_csv(FILES["deliverables_hist"])
        except Exception: mh = pd.DataFrame(columns=mto_res["df"].columns)
        allm = pd.concat([mh, mto_res["df"]], ignore_index=True).drop_duplicates(subset=["Date","Symbol","Series"], keep="last")
        if not allm.empty:
            allm["Date"] = pd.to_datetime(allm["Date"]); cutoff = allm["Date"].max() - timedelta(days=120)
            allm = allm[allm["Date"] >= cutoff].copy()
            atomic_write_df(allm.assign(Date=allm["Date"].dt.strftime("%Y-%m-%d")), FILES["deliverables_hist"])

    # 4) derive 52W (from hist)
    try:
        df = pd.read_csv(FILES["bhav_hist"])
        if not df.empty:
            df["Date"] = pd.to_datetime(df["Date"])
            cutoff = df["Date"].max() - timedelta(days=370)
            df = df[df["Date"] >= cutoff].copy()
            agg = df.groupby("Symbol").agg(High52W=("High","max"), Low52W=("Low","min")).reset_index()
            hi = df.merge(agg[["Symbol","High52W"]], on="Symbol").query("High==High52W")
            hi = hi.sort_values(["Symbol","Date"]).groupby("Symbol").tail(1)[["Symbol","Date"]].rename(columns={"Date":"High52W_Date"})
            lo = df.merge(agg[["Symbol","Low52W"]], on="Symbol").query("Low==Low52W")
            lo = lo.sort_values(["Symbol","Date"]).groupby("Symbol").tail(1)[["Symbol","Date"]].rename(columns={"Date":"Low52W_Date"})
            out = agg.merge(hi, on="Symbol", how="left").merge(lo, on="Symbol", how="left")
            for c in ["High52W_Date","Low52W_Date"]:
                out[c] = pd.to_datetime(out[c], errors="coerce").dt.strftime("%Y-%m-%d")
            atomic_write_df(out[["Symbol","High52W","High52W_Date","Low52W","Low52W_Date"]], FILES["hi52w"])
    except Exception:
        pass

    # 5) indicators (latest-day slice)
    tech_res = compute_indicators_and_latest(mto_res["df"] if locals().get("mto_res") and mto_res.get("ok") else None)

    # 6) status (+ deep local diagnostics)
    local_debug = {
        "prices_dir_exists": os.path.isdir(PRICES_DIR),
        "csv_in_prices": sorted([os.path.basename(p) for p in glob.glob(os.path.join(PRICES_DIR, "*.csv"))]),
        "local_candidates": sorted([os.path.basename(p) for p in discover_local_bhav_files()]),
        "local_ingest": local_res.get("diagnostics", []),
    }
    status = {
        "ok": True,
        "when_ist": now_ist_iso(),
        "steps": {
            "bhav": {"ok": bool(bhav_res["ok"]), "date": bhav_res.get("date"),
                     "rows": bhav_res.get("rows", 0), "budget_s": bhav_res.get("budget_s", 0)},
            "local_bhav": {k:v for k,v in local_res.items() if k != "diagnostics"},
            "bhav_hist": {"rows": hist_rows_after,
                          "prev_last_date": None if prev_last_date is None else prev_last_date.strftime("%Y-%m-%d"),
                          "last_date": None if new_last_date is None else new_last_date.strftime("%Y-%m-%d")},
            "bulk": {"ok": bool(bulk_res["ok"]), "rows": bulk_res.get("rows", 0)},
            "block": {"ok": bool(block_res["ok"]), "rows": block_res.get("rows", 0)},
            "deliverables": {"ok": bool(mto_res.get("ok", False)), "rows": mto_res.get("rows", 0) if locals().get("mto_res") else 0},
            "tech_latest": tech_res,
        },
        "debug": {
            "bhav_urls_tried": bhav_res.get("tried", [])[:50],
            "local_files": local_debug
        }
    }
    stale = True
    if new_last_date is not None:
        age = biz_age_days(new_last_date); stale = age >= 2
    status["stale_2biz_days"] = bool(stale)

    atomic_write_text(json.dumps(status, indent=2), FILES["status"])
    print(json.dumps(status, indent=2))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
