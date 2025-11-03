#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
StakeLens Option A — Small, Fast, Daily Append (robust local ingest)
- Try to fetch ONLY the latest trading day's bhav (fail fast; tiny budget).
- ALSO auto-ingest any local CSVs in data/prices/:
    * sec_bhavdata_full_*.csv
    * cm*bhav*.csv
    * bhav_latest.csv
- Append into data/prices/bhav_hist.csv (keep last-good; de-dup).
- Derive data/highlow_52w.csv, compute data/tech_latest.csv.
- Bulk/Block/MTO best-effort with tight timeouts (no stalls).
- Write data/fetch_status.json with observability and staleness guard.
"""

from __future__ import annotations
import os, io, json, zipfile, time, glob
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------- Config ----------------
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

# Tight budgets (fail fast)
HTTP_TIMEOUT = 8          # seconds per HTTP request
HTTP_RETRIES = 1          # minimal retries
TOTAL_BHAV_BUDGET = 45    # seconds to attempt latest bhav overall
CORP_TIMEOUT = 6          # per corporates request

# ---------------- Utils ----------------
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

def is_weekend(d: datetime) -> bool:
    return d.weekday() >= 5  # 5=Sat, 6=Sun

def prev_biz_days(n: int, ref: Optional[datetime]=None) -> List[datetime]:
    ref = ref or now_ist_dt()
    out, d = [], ref
    while len(out) < n:
        d = d - timedelta(days=1)
        if not is_weekend(d):
            out.append(d.replace(hour=0, minute=0, second=0, microsecond=0))
    return out

def biz_age_days(from_date: datetime, to_date: Optional[datetime]=None) -> int:
    to_date = to_date or now_ist_dt()
    d, cnt = from_date, 0
    step = timedelta(days=1)
    while d.date() < to_date.date():
        d += step
        if not is_weekend(d): cnt += 1
        if cnt > 10000: break
    return cnt

# ---------------- HTTP ----------------
def session_fast() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.7",
        "Connection": "keep-alive",
    })
    retry = Retry(
        total=HTTP_RETRIES, connect=HTTP_RETRIES, read=HTTP_RETRIES,
        status=HTTP_RETRIES, backoff_factor=0.3,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"])
    )
    ad = HTTPAdapter(max_retries=retry)
    s.mount("https://", ad); s.mount("http://", ad)
    return s

# ---------------- Normalization ----------------
CANON = {
    # date
    "date": "Date", "timestamp": "Date", "date1": "Date", "tradedate": "Date", "traded_date": "Date",

    # symbol / series
    "symbol": "Symbol", "series": "Series",

    # open/high/low/close variants
    "open": "Open", "open_price": "Open",
    "high": "High", "high_price": "High",
    "low":  "Low",  "low_price":  "Low",
    "close": "Close", "close_price": "Close", "last_price": "Close",

    # prev close variants
    "prevclose": "PrevClose", "prev_close": "PrevClose", "previousclose": "PrevClose", "previous_close": "PrevClose",

    # volume variants
    "volume": "Volume", "tottrdqty": "Volume", "ttl_trd_qnty": "Volume", "totaltradedquantity": "Volume",

    # turnover variants
    "turnover": "Turnover", "tottrdval": "Turnover", "turnover_lacs": "Turnover",
}

KEEP_COLS = ["Date","Symbol","Series","Open","High","Low","Close","PrevClose","Volume","Turnover"]

def normalize_bhav(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=KEEP_COLS)

    # make a safe copy and rename columns using CANON (case-insensitive)
    df = df.copy()
    new_cols = []
    for c in df.columns:
        key = c.strip().lower()
        key = key.replace(" ", "_").replace("-", "_")
        new_cols.append(CANON.get(key, c.strip()))
    df.columns = new_cols

    # If key columns missing, try to fill from close/last_price, etc.
    for k in KEEP_COLS:
        if k not in df.columns:
            df[k] = None

    # Cast/clean
    df["Symbol"] = df["Symbol"].astype(str).str.upper().str.strip()
    df["Series"] = df["Series"].astype(str).str.upper().str.strip()

    # Date normalization (DATE1, TIMESTAMP, etc.)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Numeric casts
    for c in ["Open","High","Low","Close","PrevClose","Volume","Turnover"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Keep only standard columns in the right order
    out = df[KEEP_COLS].dropna(subset=["Date","Symbol"])
    # If Close is missing but Open or Last existed, keep row; indicators will handle NaNs gracefully
    return out

# ---------------- Latest bhav (fast) ----------------
def try_fetch_bhav_for_date(s: requests.Session, d: datetime) -> Dict[str, Any]:
    ddmmyyyy = d.strftime("%d%m%Y")
    DD, MON, YYYY = d.strftime("%d"), d.strftime("%b").upper(), d.strftime("%Y")
    urls = [
        f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{ddmmyyyy}.csv",
        f"http://archives.nseindia.com/products/content/sec_bhavdata_full_{ddmmyyyy}.csv",
        f"https://archives.nseindia.com/content/historical/EQUITIES/{YYYY}/{MON}/cm{DD}{MON}{YYYY}bhav.csv.zip",
        f"http://archives.nseindia.com/content/historical/EQUITIES/{YYYY}/{MON}/cm{DD}{MON}{YYYY}bhav.csv.zip",
    ]
    tried = []
    # CSV first
    for u in urls[:2]:
        tried.append(u)
        try:
            r = s.get(u, timeout=HTTP_TIMEOUT, headers={"Referer":"https://www.nseindia.com/market-data"})
            if r.status_code == 200 and r.text and "SYMBOL" in r.text.upper():
                df = pd.read_csv(io.StringIO(r.text))
                out = normalize_bhav(df)
                if not out.empty:
                    day = out["Date"].dropna().iloc[0]
                    return {"ok": True, "date": day, "rows": int(len(out)), "df": out, "tried": tried}
        except Exception:
            pass
    # ZIP fallback
    for u in urls[2:]:
        tried.append(u)
        try:
            r = s.get(u, timeout=HTTP_TIMEOUT, headers={"Referer":"https://www.nseindia.com/market-data"})
            if r.status_code != 200 or not r.content: continue
            with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), None)
                if not name: continue
                raw = zf.read(name).decode("utf-8", errors="ignore")
            df = pd.read_csv(io.StringIO(raw))
            out = normalize_bhav(df)
            if not out.empty:
                day = out["Date"].dropna().iloc[0]
                return {"ok": True, "date": day, "rows": int(len(out)), "df": out, "tried": tried}
        except Exception:
            pass
    return {"ok": False, "date": d.strftime("%Y-%m-%d"), "rows": 0, "df": None, "tried": tried}

def fetch_latest_bhav_failfast() -> Dict[str, Any]:
    start = time.time()
    s = session_fast()
    candidates = [now_ist_dt()] + prev_biz_days(2)  # today + last 2 biz days
    tried_all = []
    for d in candidates:
        if is_weekend(d): continue
        if time.time() - start > TOTAL_BHAV_BUDGET: break
        res = try_fetch_bhav_for_date(s, d)
        tried_all.extend(res.get("tried", []))
        if res["ok"]:
            return {"ok": True, "date": res["date"], "rows": res["rows"], "df": res["df"], "tried": tried_all, "budget_s": int(time.time()-start)}
    return {"ok": False, "date": None, "rows": 0, "df": None, "tried": tried_all, "budget_s": int(time.time()-start)}

# ---------------- Corporates (best-effort) ----------------
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
            r = s.get(u, timeout=CORP_TIMEOUT, headers={
                "Accept":"application/json, text/plain, */*",
                "Referer":"https://www.nseindia.com/report-detail/display-bulk-and-block-deals"
            })
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
    s = session_fast()
    rec = None; tried=[]
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
    header = [h.strip() for h in lines[hdr_idx].split(",")]
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
        rows.append([d.strftime("%Y-%m-%d"), symbol, series,
                     _num(dq), _num(tq), _num(pct)])
    if not rows: return {"ok": False, "rows": 0, "tried": tried}
    df = pd.DataFrame(rows, columns=["Date","Symbol","Series","Deliverable_Qty","Traded_Qty","Delivery_Pct"])
    return {"ok": True, "rows": int(len(df)), "df": df, "tried": tried}

def _num(v):
    if v is None: return None
    s = str(v).replace(",","").replace("%","").strip()
    try: return float(s)
    except: return None

# ---------------- Local ingest (robust) ----------------
def ingest_local_bhav_candidates() -> Dict[str, Any]:
    """
    Ingest any local bhav CSVs und
