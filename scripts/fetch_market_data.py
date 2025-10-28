#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ISS-IND robust EOD fetcher (final hardened, multi-source archives, self-contained)

Outputs (atomic writes under data/):
  - deliverables_latest.csv, deliverables_hist.csv  (best-effort MTO)
  - bulk_deals_latest.csv, block_deals_latest.csv   (best-effort APIs)
  - prices/bhav_hist.csv     (cold-seeded from NSE archives if empty)
  - highlow_52w.csv          (derived from bhav_hist)
  - tech_latest.csv          (latest-day technical indicators per symbol)
  - fetch_status.json        (diagnostics incl. URLs tried)

Zero-budget. Survives flaky endpoints. Last-good semantics. Never fails the workflow.
"""

from __future__ import annotations
import os, sys, io, json, time, zipfile
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ----------------- Paths/Constants -----------------
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
    "bhav_latest":         os.path.join(PRICES_DIR, "bhav_latest.csv"),
    "bhav_hist":           os.path.join(PRICES_DIR, "bhav_hist.csv"),
    "tech_latest":         os.path.join(DATA_DIR, "tech_latest.csv"),
}

# Optional GH raw fallback if your repo already has a bhav_latest.csv
GH_RAW_BHAV_URL = os.environ.get(
    "GH_RAW_BHAV_URL",
    "https://raw.githubusercontent.com/TorqueEnables/ISS-IND/main/data/prices/bhav_latest.csv"
)

# ----------------- FS helpers -----------------
def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(PRICES_DIR, exist_ok=True)

def atomic_write_text(txt: str, path: str):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(txt)
    os.replace(tmp, path)

def atomic_write_df(df: pd.DataFrame, path: str):
    tmp = path + ".tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)

def ensure_file_with_headers(path: str, headers: list[str]):
    if not os.path.exists(path):
        atomic_write_df(pd.DataFrame(columns=headers), path)

def now_ist() -> datetime:
    return datetime.now(IST)

def is_weekend(d: datetime) -> bool:
    return d.weekday() >= 5

def previous_business_days(n: int, ref: Optional[datetime] = None) -> List[datetime]:
    ref = ref or now_ist()
    out, d = [], ref
    while len(out) < n:
        d = d - timedelta(days=1)
        if not is_weekend(d):
            out.append(d)
    return out

def backoff(call, tries=4, base=2, **kwargs):
    last = None
    for i in range(tries):
        try:
            return call(**kwargs)
        except Exception as e:
            last = e
            time.sleep(base * (2 ** i))
    if last:
        raise last

# ----------------- HTTP session with robust retries -----------------
def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.7",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    })
    retry = Retry(
        total=5, connect=5, read=5,
        status=5, backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"])
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    # Warm cookies / referers NSE expects
    for url in [
        "https://www.nseindia.com",
        "https://www.nseindia.com/market-data",
        "https://www.nseindia.com/companies-listing/corporate-filings-bulk-deals",
        "https://www.nseindia.com/companies-listing/corporate-filings-block-deals",
    ]:
        try: s.get(url, timeout=20)
        except Exception: pass
    return s

# ----------------- Deliverables (MTO, best-effort) -----------------
def parse_mto_text(txt: str) -> pd.DataFrame:
    lines = [ln.strip() for ln in txt.replace("\t", ",").splitlines() if ln.strip()]
    hdr_idx = None
    for i, ln in enumerate(lines):
        up = ln.upper()
        if "SYMBOL" in up and "SERIES" in up and ("DELIV" in up or "DELIVERABLE" in up):
            hdr_idx = i; break
    if hdr_idx is None:
        raise ValueError("MTO header not found")
    header = [h.strip().replace(" ", "_") for h in lines[hdr_idx].split(",")]
    rows = []
    for ln in lines[hdr_idx+1:]:
        up = ln.upper()
        if up.startswith("TOTAL") or up.startswith("GRAND"): break
        parts = [p.strip() for p in ln.split(",")]
        if len(parts) < 5: continue
        parts = (parts + [""] * len(header))[:len(header)]
        rows.append(parts)
    df = pd.DataFrame(rows, columns=header)
    colmap = {}
    for c in df.columns:
        cu = c.upper()
        if cu.startswith("DATE"): colmap[c]="Date"
        elif cu.startswith("SYMBOL"): colmap[c]="Symbol"
        elif cu.startswith("SERIES"): colmap[c]="Series"
        elif "TRAD" in cu and "QTY" in cu: colmap[c]="Traded_Qty"
        elif "DELIV" in cu and "QTY" in cu: colmap[c]="Deliverable_Qty"
        elif ("DELIV" in cu and "%") or "PER" in cu: colmap[c]="Delivery_Pct"
    df = df.rename(columns=colmap)
    keep = ["Date","Symbol","Series","Deliverable_Qty","Traded_Qty","Delivery_Pct"]
    for k in keep:
        if k not in df.columns: df[k] = None
    df = df[keep]
    df["Symbol"] = df["Symbol"].astype(str).str.upper().str.strip()
    df["Series"] = df["Series"].astype(str).str.upper().str.strip()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True).dt.strftime("%Y-%m-%d")
    for c in ["Deliverable_Qty","Traded_Qty","Delivery_Pct"]:
        df[c] = (df[c].astype(str).str.replace(",", "", regex=False).str.replace("%","",regex=False).str.strip())
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.dropna(subset=["Date","Symbol"])

def try_mto_for_date(s: requests.Session, d: datetime) -> Optional[pd.DataFrame]:
    ddmmyyyy = d.strftime("%d%m%Y")
    # try multiple hosts
    urls = [
        f"https://archives.nseindia.com/archives/equities/mto/MTO_{ddmmyyyy}.DAT",
        f"http://archives.nseindia.com/archives/equities/mto/MTO_{ddmmyyyy}.DAT",
        f"https://www.nseindia.com/archives/equities/mto/MTO_{ddmmyyyy}.DAT",
        f"https://www1.nseindia.com/archives/equities/mto/MTO_{ddmmyyyy}.DAT",
    ]
    for u in urls:
        try:
            r = s.get(u, timeout=40, headers={"Referer": "https://www.nseindia.com/market-data"}, allow_redirects=True)
            if r.status_code == 200 and "SYMBOL" in r.text.upper():
                return parse_mto_text(r.text)
        except Exception:
            continue
    return None

def fetch_deliverables_latest(s: requests.Session, status_urls: Dict[str, Any]) -> Optional[pd.DataFrame]:
    for dt in [now_ist()] + previous_business_days(6):
        if is_weekend(dt): continue
        ddmmyyyy = dt.strftime("%d%m%Y")
        status_urls.setdefault("mto_tried", []).extend([
            f"https://archives.nseindia.com/archives/equities/mto/MTO_{ddmmyyyy}.DAT",
            f"http://archives.nseindia.com/archives/equities/mto/MTO_{ddmmyyyy}.DAT",
            f"https://www.nseindia.com/archives/equities/mto/MTO_{ddmmyyyy}.DAT",
            f"https://www1.nseindia.com/archives/equities/mto/MTO_{ddmmyyyy}.DAT",
        ])
        try:
            df = try_mto_for_date(s, dt)
            if df is not None and not df.empty:
                return df
        except Exception:
            continue
    return None

# ----------------- Corporates (Bulk/Block best-effort) -----------------
def fetch_corporates(s: requests.Session, kind: str, status_urls: Dict[str, Any]) -> Optional[pd.DataFrame]:
    # Primary JSON APIs (they often work without CSV export)
    eps = {
        "bulk": [
            "https://www.nseindia.com/api/corporates-bulk-deals?index=equities",
            "https://www.nseindia.com/api/corporates/bulk-deals?index=equities",
            "https://www.nseindia.com/api/corporates-pit?index=bulk-deals",
        ],
        "block": [
            "https://www.nseindia.com/api/corporates-block-deals?index=equities",
            "https://www.nseindia.com/api/corporates/block-deals?index=equities",
            "https://www.nseindia.com/api/corporates-pit?index=block-deals",
        ],
    }
    referer = {
        "bulk":  "https://www.nseindia.com/report-detail/display-bulk-and-block-deals",
        "block": "https://www.nseindia.com/report-detail/display-bulk-and-block-deals",
    }
    keymap = {
        "symbol":"Symbol", "clientName":"Client_Name", "buySell":"Buy_Sell",
        "quantity":"Quantity", "avgPrice":"Price", "price":"Price",
        "dealDate":"Date", "date":"Date",
    }
    for url in eps[kind]:
        status_urls.setdefault(f"{kind}_tried", []).append(url)
        try:
            r = s.get(url, timeout=40, headers={"Accept":"application/json, text/plain, */*", "Referer": referer[kind]}, allow_redirects=True)
            if r.status_code != 200:
                continue
            data = r.json()
            rows = None
            if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
                rows = data["data"]
            elif isinstance(data, list):
                rows = data
            else:
                for v in (data.values() if isinstance(data, dict) else []):
                    if isinstance(v, list): rows = v; break
            if not rows:
                continue
            recs = []
            for rec in rows:
                out = {}
                for src, dst in keymap.items():
                    if src in rec and rec[src] not in (None, ""):
                        out[dst] = rec[src]
                recs.append(out)
            df = pd.DataFrame(recs)
            if df.empty:
                continue
            df["Symbol"] = df["Symbol"].astype(str).str.upper().str.strip()
            if "Buy_Sell" in df.columns:
                df["Buy_Sell"] = (df["Buy_Sell"].astype(str).str.upper()
                                  .str.replace("PURCHASE","BUY").str.replace("SELLL","SELL"))
            for c in ("Quantity","Price"):
                if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True).dt.strftime("%Y-%m-%d")
            df["Source"] = url
            keep = ["Date","Symbol","Client_Name","Buy_Sell","Quantity","Price","Source"]
            for k in keep:
                if k not in df.columns: df[k] = None
            return df[keep].dropna(subset=["Symbol"])
        except Exception:
            continue
    return None

# ----------------- Bhav: loaders and normalizers -----------------
def load_bhav_latest_local_or_remote() -> Optional[pd.DataFrame]:
    p = FILES["bhav_latest"]
    if os.path.exists(p):
        try: return pd.read_csv(p)
        except Exception: pass
    try:
        r = requests.get(GH_RAW_BHAV_URL, timeout=30, headers={"User-Agent":"curl/8.0"})
        if r.status_code == 200 and r.text:
            return pd.read_csv(io.StringIO(r.text))
    except Exception:
        pass
    return None

def normalize_bhav(df: pd.DataFrame) -> pd.DataFrame:
    canon = {
        "date":"Date","timestamp":"Date",
        "symbol":"Symbol","series":"Series",
        "open":"Open","high":"High","low":"Low","close":"Close",
        "prevclose":"PrevClose","prev_close":"PrevClose","previousclose":"PrevClose",
        "volume":"Volume","tottrdqty":"Volume",
        "turnover":"Turnover","tottrdval":"Turnover","turnover_lacs":"Turnover",
    }
    df = df.copy()
    df.columns = [canon.get(c.strip().lower(), c.strip()) for c in df.columns]
    keep = ["Date","Symbol","Series","Open","High","Low","Close","PrevClose","Volume","Turnover"]
    for k in keep:
        if k not in df.columns: df[k] = None
    df = df[keep]
    df["Symbol"] = df["Symbol"].astype(str).str.upper().str.strip()
    df["Series"] = df["Series"].astype(str).str.upper().str.strip()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    for c in ["Open","High","Low","Close","PrevClose","Volume","Turnover"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.dropna(subset=["Date","Symbol","Close"])

def fetch_bhav_sec_full_csv_for_date(s: requests.Session, d: datetime) -> Optional[pd.DataFrame]:
    # Primary daily CSV, no zip
    ddmmyyyy = d.strftime("%d%m%Y")
    urls = [
        f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{ddmmyyyy}.csv",
        f"http://archives.nseindia.com/products/content/sec_bhavdata_full_{ddmmyyyy}.csv",
    ]
    for url in urls:
        try:
            r = s.get(url, timeout=50, headers={"Referer":"https://www.nseindia.com/market-data"}, allow_redirects=True)
            if r.status_code != 200 or not r.text:
                continue
            df = pd.read_csv(io.StringIO(r.text))
            # normalize
            canon = {
                "symbol":"Symbol","series":"Series","open":"Open","high":"High","low":"Low","close":"Close",
                "prev. close":"PrevClose","prev_close":"PrevClose","previousclose":"PrevClose",
                "ttl_trd_qnty":"Volume","tottrdqty":"Volume","turnover_lacs":"Turnover","tottrdval":"Turnover",
                "date1":"Date","timestamp":"Date"
            }
            df.columns = [canon.get(c.strip().lower(), c.strip()) for c in df.columns]
            keep = ["Date","Symbol","Series","Open","High","Low","Close","PrevClose","Volume","Turnover"]
            for k in keep:
                if k not in df.columns: df[k] = None
            out = df[keep].copy()
            out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
            for c in ["Open","High","Low","Close","PrevClose","Volume","Turnover"]:
                out[c] = pd.to_numeric(out[c], errors="coerce")
            out["Symbol"] = out["Symbol"].astype(str).str.upper().str.strip()
            out["Series"] = out["Series"].astype(str).str.upper().str.strip()
            out = out.dropna(subset=["Date","Symbol","Close"])
            if not out.empty:
                return out
        except Exception:
            continue
    return None

def fetch_bhav_archive_zip_for_date(s: requests.Session, d: datetime) -> Optional[pd.DataFrame]:
    # Zipped bhav copy on 3 hosts, both https and http (fallback)
    MON = d.strftime("%b").upper()
    DD = d.strftime("%d"); YYYY = d.strftime("%Y")
    urls = [
        f"https://archives.nseindia.com/content/historical/EQUITIES/{YYYY}/{MON}/cm{DD}{MON}{YYYY}bhav.csv.zip",
        f"https://www.nseindia.com/content/historical/EQUITIES/{YYYY}/{MON}/cm{DD}{MON}{YYYY}bhav.csv.zip",
        f"https://www1.nseindia.com/content/historical/EQUITIES/{YYYY}/{MON}/cm{DD}{MON}{YYYY}bhav.csv.zip",
        f"http://archives.nseindia.com/content/historical/EQUITIES/{YYYY}/{MON}/cm{DD}{MON}{YYYY}bhav.csv.zip",
    ]
    for url in urls:
        try:
            r = s.get(url, timeout=60, headers={"Referer":"https://www.nseindia.com/market-data"}, allow_redirects=True)
            if r.status_code != 200 or not r.content:
                continue
            try:
                with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                    name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), None)
                    if not name: 
                        continue
                    raw = zf.read(name).decode("utf-8", errors="ignore")
                df = pd.read_csv(io.StringIO(raw))
            except Exception:
                continue
            canon = {
                "symbol":"Symbol","series":"Series","open":"Open","high":"High","low":"Low","close":"Close",
                "prevclose":"PrevClose","previousclose":"PrevClose","tottrdqty":"Volume","tottrdval":"Turnover","timestamp":"Date"
            }
            df.columns = [canon.get(c.strip().lower(), c.strip()) for c in df.columns]
            keep = ["Date","Symbol","Series","Open","High","Low","Close","PrevClose","Volume","Turnover"]
            for k in keep:
                if k not in df.columns: df[k] = None
            out = df[keep].copy()
            out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
            for c in ["Open","High","Low","Close","PrevClose","Volume","Turnover"]:
                out[c] = pd.to_numeric(out[c], errors="coerce")
            out["Symbol"] = out["Symbol"].astype(str).str.upper().str.strip()
            out["Series"] = out["Series"].astype(str).str.upper().str.strip()
            out = out.dropna(subset=["Date","Symbol","Close"])
            if not out.empty:
                return out
        except Exception:
            continue
    return None

def cold_seed_bhav_hist_if_empty(s: requests.Session, status_urls: Dict[str, Any], min_days:int=90) -> int:
    """If bhav_hist is empty, walk back through up to 240 prior business days,
       trying sec_bhavdata_full first, then zipped cm..bhav.csv.zip on 3 hosts."""
    try:
        hist = pd.read_csv(FILES["bhav_hist"])
        current_rows = len(hist)
    except Exception:
        hist = pd.DataFrame(columns=["Date","Symbol","Series","Open","High","Low","Close","PrevClose","Volume","Turnover"])
        current_rows = 0
    if current_rows > 0:
        return current_rows

    frames = []
    seen_dates = set()

    for d in previous_business_days(240):
        # 1) sec_bhavdata_full
        sec_url_https = f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{d.strftime('%d%m%Y')}.csv"
        sec_url_http  = f"http://archives.nseindia.com/products/content/sec_bhavdata_full_{d.strftime('%d%m%Y')}.csv"
        status_urls.setdefault("bhav_tried", []).extend([sec_url_https, sec_url_http])

        df = fetch_bhav_sec_full_csv_for_date(s, d)

        # 2) zipped archives if sec_full missing
        if df is None or df.empty:
            for base in ("archives.nseindia.com","www.nseindia.com","www1.nseindia.com"):
                status_urls.setdefault("bhav_tried", []).append(
                    f"https://{base}/content/historical/EQUITIES/{d.strftime('%Y')}/{d.strftime('%b').upper()}/cm{d.strftime('%d%b%Y').upper()}bhav.csv.zip"
                )
            df = fetch_bhav_archive_zip_for_date(s, d)

        if df is not None and not df.empty:
            day = df["Date"].iloc[0]
            if day not in seen_dates:
                frames.append(df)
                seen_dates.add(day)

        time.sleep(0.4)
        if len(seen_dates) >= min_days:
            break

    if frames:
        allb = pd.concat(frames, ignore_index=True)
        allb = allb.drop_duplicates(subset=["Date","Symbol"], keep="last")
        allb["Date"] = pd.to_datetime(allb["Date"]).dt.strftime("%Y-%m-%d")
        atomic_write_df(allb, FILES["bhav_hist"])
        return len(allb)

    return 0

# ----------------- Indicators -----------------
def compute_indicators_and_write_latest(deliv_latest: Optional[pd.DataFrame]) -> dict:
    try:
        hist = pd.read_csv(FILES["bhav_hist"])
    except Exception:
        hist = pd.DataFrame(columns=["Date","Symbol","Series","Open","High","Low","Close","PrevClose","Volume","Turnover"])

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
    tr3 = (df["Low"] - df["PrevClose"]).abs()
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

    out_cols = ["Date","Symbol","Series","Open","High","Low","Close","PrevClose","Volume",
                "Range","TR","ATR14","SMA10","SMA20","SMA50","BB_Mid","BB_Upper","BB_Lower",
                "BB_Bandwidth","CLV","RangeAvg20","VolAvg20","HI55","HI55_Recent","NR7","Delivery_Pct"]
    atomic_write_df(latest[out_cols], FILES["tech_latest"])
    return {"ok": True, "rows": int(len(latest)), "last_date": last_date.strftime("%Y-%m-%d")}

# ----------------- Main -----------------
def main():
    ensure_dirs()
    # Ensure files exist (headers) so commits pick them up even on first run
    ensure_file_with_headers(FILES["deliverables_latest"], ["Date","Symbol","Series","Deliverable_Qty","Traded_Qty","Delivery_Pct"])
    ensure_file_with_headers(FILES["deliverables_hist"],   ["Date","Symbol","Series","Deliverable_Qty","Traded_Qty","Delivery_Pct"])
    ensure_file_with_headers(FILES["bulk_latest"],         ["Date","Symbol","Client_Name","Buy_Sell","Quantity","Price","Source"])
    ensure_file_with_headers(FILES["block_latest"],        ["Date","Symbol","Client_Name","Buy_Sell","Quantity","Price","Source"])
    ensure_file_with_headers(FILES["hi52w"],               ["Symbol","High52W","High52W_Date","Low52W","Low52W_Date"])
    ensure_file_with_headers(FILES["bhav_hist"],           ["Date","Symbol","Series","Open","High","Low","Close","PrevClose","Volume","Turnover"])
    ensure_file_with_headers(FILES["tech_latest"],         ["Date","Symbol","Series","Open","High","Low","Close","PrevClose","Volume",
                                                            "Range","TR","ATR14","SMA10","SMA20","SMA50","BB_Mid","BB_Upper","BB_Lower",
                                                            "BB_Bandwidth","CLV","RangeAvg20","VolAvg20","HI55","HI55_Recent","NR7","Delivery_Pct"])

    s = get_session()
    status = {"ok": True, "when_ist": now_ist().isoformat(), "steps": {}, "debug": {}}

    # Deliverables (MTO) best-effort
    try:
        ddf = fetch_deliverables_latest(s, status["debug"])
        if ddf is not None and not ddf.empty:
            atomic_write_df(ddf, FILES["deliverables_latest"])
            try: hist = pd.read_csv(FILES["deliverables_hist"])
            except Exception: hist = pd.DataFrame(columns=ddf.columns)
            hist = (pd.concat([hist, ddf], ignore_index=True)
                      .drop_duplicates(subset=["Date","Symbol","Series"], keep="last"))
            if not hist.empty:
                hist["Date"] = pd.to_datetime(hist["Date"], errors="coerce")
                cutoff = hist["Date"].max() - timedelta(days=120)
                hist = hist[hist["Date"] >= cutoff].copy()
                hist["Date"] = hist["Date"].dt.strftime("%Y-%m-%d")
            atomic_write_df(hist, FILES["deliverables_hist"])
            status["steps"]["deliverables"] = {"ok": True, "rows": int(len(ddf))}
        else:
            status["ok"] = False
            status["steps"]["deliverables"] = {"ok": False, "error": "MTO not available for recent days"}
    except Exception as e:
        status["ok"] = False
        status["steps"]["deliverables"] = {"ok": False, "error": str(e)}

    # Bulk & Block best-effort
    try:
        bdf = fetch_corporates(s, "bulk", status["debug"])
        if bdf is not None and not bdf.empty:
            atomic_write_df(bdf, FILES["bulk_latest"])
            status["steps"]["bulk"] = {"ok": True, "rows": int(len(bdf))}
        else:
            status["steps"]["bulk"] = {"ok": False, "error": "No bulk data"}
    except Exception as e:
        status["steps"]["bulk"] = {"ok": False, "error": str(e)}
    try:
        kdf = fetch_corporates(s, "block", status["debug"])
        if kdf is not None and not kdf.empty:
            atomic_write_df(kdf, FILES["block_latest"])
            status["steps"]["block"] = {"ok": True, "rows": int(len(kdf))}
        else:
            status["steps"]["block"] = {"ok": False, "error": "No block data"}
    except Exception as e:
        status["steps"]["block"] = {"ok": False, "error": str(e)}

    # Bhav hist: use bhav_latest if present, else cold-seed archives
    seeded_rows = 0
    try:
        bl = load_bhav_latest_local_or_remote()
        if bl is not None and not bl.empty:
            bl = normalize_bhav(bl)
            try:
                hist = pd.read_csv(FILES["bhav_hist"])
            except Exception:
                hist = pd.DataFrame(columns=bl.columns)
            allb = (pd.concat([hist, bl], ignore_index=True)
                      .drop_duplicates(subset=["Date","Symbol"], keep="last"))
            if not allb.empty:
                allb["Date"] = pd.to_datetime(allb["Date"])
                cutoff = allb["Date"].max() - timedelta(days=460)
                allb = allb[allb["Date"] >= cutoff].copy()
                allb["Date"] = allb["Date"].dt.strftime("%Y-%m-%d")
            atomic_write_df(allb, FILES["bhav_hist"])
        seeded_rows = cold_seed_bhav_hist_if_empty(s, status["debug"], min_days=90)
    except Exception as e:
        status["debug"]["bhav_seed_error"] = str(e)

    # Derive 52W
    try:
        hist2 = pd.read_csv(FILES["bhav_hist"])
        if not hist2.empty:
            df = hist2.copy()
            df["Date"] = pd.to_datetime(df["Date"])
            cutoff = df["Date"].max() - timedelta(days=370)
            df = df[df["Date"] >= cutoff].copy()
            agg = df.groupby("Symbol").agg(High52W=("High","max"), Low52W=("Low","min")).reset_index()
            hi = df.merge(agg[["Symbol","High52W"]], on=["Symbol"]).query("High==High52W")
            hi = hi.sort_values(["Symbol","Date"]).groupby("Symbol").tail(1)[["Symbol","Date"]].rename(columns={"Date":"High52W_Date"})
            lo = df.merge(agg[["Symbol","Low52W"]], on=["Symbol"]).query("Low==Low52W")
            lo = lo.sort_values(["Symbol","Date"]).groupby("Symbol").tail(1)[["Symbol","Date"]].rename(columns={"Date":"Low52W_Date"})
            out = agg.merge(hi, on="Symbol", how="left").merge(lo, on="Symbol", how="left")
            for c in ["High52W_Date","Low52W_Date"]:
                out[c] = pd.to_datetime(out[c], errors="coerce").dt.strftime("%Y-%m-%d")
            atomic_write_df(out[["Symbol","High52W","High52W_Date","Low52W","Low52W_Date"]], FILES["hi52w"])
    except Exception as e:
        status["debug"]["hi52w_error"] = str(e)

    # Indicators → tech_latest.csv
    try:
        step = compute_indicators_and_write_latest(ddf if 'ddf' in locals() else None)
        status["steps"]["tech_latest"] = step
    except Exception as e:
        status["steps"]["tech_latest"] = {"ok": False, "error": str(e)}

    # Status
    try:
        try:
            hh = pd.read_csv(FILES["bhav_hist"])
            status["steps"]["bhav_hist_52w"] = {"ok": bool(len(hh) > 0), "hist_rows": int(len(hh)), "seeded_rows": int(seeded_rows)}
        except Exception:
            status["steps"]["bhav_hist_52w"] = {"ok": False, "hist_rows": 0, "seeded_rows": int(seeded_rows)}
        atomic_write_text(json.dumps(status, indent=2), FILES["status"])
    except Exception:
        pass

    print(json.dumps(status, indent=2))
    return 0

if __name__ == "__main__":
    sys.exit(main())
