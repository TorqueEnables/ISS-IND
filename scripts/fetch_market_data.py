#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Robust EOD fetcher for ISS-IND (zero-budget, last-good semantics)

Outputs (atomic writes to data/):
  - deliverables_latest.csv        (NSE MTO)
  - deliverables_hist.csv          (rolling ~120d)
  - bulk_deals_latest.csv          (NSE corporates)
  - block_deals_latest.csv         (NSE corporates)
  - prices/bhav_hist.csv           (grown from bhav_latest.csv)
  - highlow_52w.csv                (derived from bhav_hist)
  - fetch_status.json              (diagnostics; never fails the Action)

Design:
  - Warm cookies & set Referer -> lower 403/anti-bot odds
  - Retry with exponential backoff
  - Multi-day fallback for MTO (walk back past business days)
  - Multi-endpoint fallback for bulk/block
  - If a step fails, keep last-good file; never write empty files
  - Exit 0 (let downstream gating handle DataOK)
"""

from __future__ import annotations
import os, sys, io, json, time, shutil
from typing import List, Dict, Optional
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests

# ---- Constants & paths -------------------------------------------------------

IST = timezone(timedelta(hours=5, minutes=30))
DATA_DIR = "data"
PRICES_DIR = os.path.join(DATA_DIR, "prices")

FILES = {
    "deliverables_latest": os.path.join(DATA_DIR, "deliverables_latest.csv"),
    "deliverables_hist":   os.path.join(DATA_DIR, "deliverables_hist.csv"),
    "bulk_latest":         os.path.join(DATA_DIR, "bulk_deals_latest.csv"),
    "block_latest":        os.path.join(DATA_DIR, "block_deals_latest.csv"),
    "bhav_hist":           os.path.join(PRICES_DIR, "bhav_hist.csv"),
    "bhav_latest":         os.path.join(PRICES_DIR, "bhav_latest.csv"),
    "hi52w":               os.path.join(DATA_DIR, "highlow_52w.csv"),
    "status":              os.path.join(DATA_DIR, "fetch_status.json"),
}

GH_RAW_BHAV_URL = os.environ.get(
    "GH_RAW_BHAV_URL",
    "https://raw.githubusercontent.com/TorqueEnables/ISS-IND/main/data/prices/bhav_latest.csv"
)

# ---- Utils -------------------------------------------------------------------

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

def now_ist() -> datetime:
    return datetime.now(IST)

def is_weekend(d: datetime) -> bool:
    return d.weekday() >= 5  # Sat 5 / Sun 6

def previous_business_days(n: int, ref: Optional[datetime] = None) -> List[datetime]:
    ref = ref or now_ist()
    days = []
    d = ref
    while len(days) < n:
        d = d - timedelta(days=1)
        if not is_weekend(d):
            days.append(d)
    return days

def with_backoff(func, tries=4, base_delay=2, *args, **kwargs):
    last = None
    for i in range(tries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last = e
            time.sleep(base_delay * (2 ** i))
    if last:
        raise last

# ---- HTTP session prep -------------------------------------------------------

def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.7",
        "Connection": "keep-alive",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
    })
    # Warm cookies
    try:
        s.get("https://www.nseindia.com", timeout=20)
        s.get("https://www.nseindia.com/companies-listing/corporate-filings-bulk-deals", timeout=20)
        s.get("https://www.nseindia.com/companies-listing/corporate-filings-block-deals", timeout=20)
    except Exception:
        pass
    return s

# ---- Deliverables (MTO) ------------------------------------------------------

def parse_mto_csv_text(txt: str) -> pd.DataFrame:
    # Normalize to CSV-like lines
    lines = [ln.strip() for ln in txt.replace("\t", ",").splitlines() if ln.strip()]
    # Find header; MTO often has a CSV header row (comma-separated)
    hdr_idx = None
    for i, ln in enumerate(lines):
        up = ln.upper()
        if "SYMBOL" in up and "SERIES" in up and ("DELIV" in up or "DELIVERABLE" in up):
            hdr_idx = i
            break
    if hdr_idx is None:
        raise ValueError("MTO header not found")
    header = [h.strip().replace(" ", "_") for h in lines[hdr_idx].split(",")]
    rows = []
    for ln in lines[hdr_idx+1:]:
        up = ln.upper()
        if up.startswith("TOTAL") or up.startswith("GRAND"):
            break
        parts = [p.strip() for p in ln.split(",")]
        if len(parts) < 5:  # too short
            continue
        # pad / trim
        parts = (parts + [""] * len(header))[:len(header)]
        rows.append(parts)
    df = pd.DataFrame(rows, columns=header)

    # Map to canonical columns
    colmap = {}
    for c in df.columns:
        cu = c.upper()
        if cu.startswith("DATE"):
            colmap[c] = "Date"
        elif cu.startswith("SYMBOL"):
            colmap[c] = "Symbol"
        elif cu.startswith("SERIES"):
            colmap[c] = "Series"
        elif "TRAD" in cu and "QTY" in cu:
            colmap[c] = "Traded_Qty"
        elif "DELIV" in cu and "QTY" in cu:
            colmap[c] = "Deliverable_Qty"
        elif ("DELIV" in cu and "%") or "PER" in cu:
            colmap[c] = "Delivery_Pct"
    df = df.rename(columns=colmap)

    keep = ["Date","Symbol","Series","Deliverable_Qty","Traded_Qty","Delivery_Pct"]
    for k in keep:
        if k not in df.columns:
            df[k] = None
    df = df[keep]

    # Clean types
    df["Symbol"] = df["Symbol"].astype(str).str.upper().str.strip()
    df["Series"] = df["Series"].astype(str).str.upper().str.strip()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True).dt.strftime("%Y-%m-%d")
    for numcol in ["Deliverable_Qty","Traded_Qty","Delivery_Pct"]:
        df[numcol] = (
            df[numcol]
            .astype(str).str.replace(",", "", regex=False)
            .str.replace("%", "", regex=False)
            .str.strip()
        )
        df[numcol] = pd.to_numeric(df[numcol], errors="coerce")
    df = df.dropna(subset=["Date","Symbol"])
    return df

def try_fetch_mto_for_date(s: requests.Session, dt: datetime) -> Optional[pd.DataFrame]:
    ddmmyyyy = dt.strftime("%d%m%Y")
    candidates = [
        f"https://www.nseindia.com/archives/equities/mto/MTO_{ddmmyyyy}.DAT",
        f"https://www.nseindia.com/archives/equities/mto/mto_{ddmmyyyy}.DAT",
        f"https://www.nseindia.com/content/equities/mto/MTO_{ddmmyyyy}.DAT",
    ]
    for url in candidates:
        try:
            r = s.get(url, timeout=30, headers={"Referer": "https://www.nseindia.com/market-data"})
            if r.status_code == 200 and r.text and "SYMBOL" in r.text.upper():
                return parse_mto_csv_text(r.text)
        except Exception:
            continue
    return None

def fetch_deliverables_latest(s: requests.Session) -> Optional[pd.DataFrame]:
    # Walk back up to 6 recent business days; return first success
    for dt in [now_ist()] + previous_business_days(6):
        if is_weekend(dt):
            continue
        try:
            df = with_backoff(try_fetch_mto_for_date, 3, 2, s, dt)
            if df is not None and not df.empty:
                return df
        except Exception:
            continue
    return None

# ---- Corporates: Bulk / Block ------------------------------------------------

def fetch_corporates_table(s: requests.Session, kind: str) -> Optional[pd.DataFrame]:
    """
    kind in {"bulk","block"}
    """
    # Endpoint fallbacks (NSE changes paths; we try a few)
    ep_sets = {
        "bulk": [
            "https://www.nseindia.com/api/corporates-bulk-deals?index=equities",
            "ht
