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
            "https://www.nseindia.com/api/corporates/bulk-deals?index=equities",
            "https://www.nseindia.com/api/corporates-pit?index=bulk-deals",
        ],
        "block": [
            "https://www.nseindia.com/api/corporates-block-deals?index=equities",
            "https://www.nseindia.com/api/corporates/block-deals?index=equities",
            "https://www.nseindia.com/api/corporates-pit?index=block-deals",
        ],
    }
    referers = {
        "bulk":  "https://www.nseindia.com/companies-listing/corporate-filings-bulk-deals",
        "block": "https://www.nseindia.com/companies-listing/corporate-filings-block-deals",
    }
    keymap = {
        "symbol": "Symbol",
        "clientName": "Client_Name",
        "buySell": "Buy_Sell",
        "quantity": "Quantity",
        "avgPrice": "Price",
        "price": "Price",
        "dealDate": "Date",
        "date": "Date",
    }

    for url in ep_sets[kind]:
        try:
            r = with_backoff(
                s.get, tries=4, base_delay=2,
                url=url,
                timeout=30,
                headers={"Accept": "application/json, text/plain, */*", "Referer": referers[kind]},
            )
            if r.status_code != 200:
                continue
            data = r.json()
            # Discover rows
            rows = None
            if isinstance(data, dict):
                if "data" in data and isinstance(data["data"], list):
                    rows = data["data"]
                else:
                    # pick first list value
                    for v in data.values():
                        if isinstance(v, list):
                            rows = v
                            break
            elif isinstance(data, list):
                rows = data

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

            # Normalize
            df["Symbol"] = df["Symbol"].astype(str).str.upper().str.strip()
            if "Buy_Sell" in df.columns:
                df["Buy_Sell"] = (df["Buy_Sell"].astype(str).str.upper()
                                  .str.replace("PURCHASE","BUY")
                                  .str.replace("SELLL","SELL"))
            for c in ("Quantity","Price"):
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True).dt.strftime("%Y-%m-%d")
            df["Source"] = url
            # Keep canonical cols
            keep = ["Date","Symbol","Client_Name","Buy_Sell","Quantity","Price","Source"]
            for k in keep:
                if k not in df.columns:
                    df[k] = None
            return df[keep].dropna(subset=["Symbol"])
        except Exception:
            continue
    return None

# ---- Bhav history & 52w ------------------------------------------------------

def load_bhav_latest_local_or_remote() -> Optional[pd.DataFrame]:
    path = FILES["bhav_latest"]
    if os.path.exists(path):
        try:
            return pd.read_csv(path)
        except Exception:
            pass
    # fallback to remote raw (same repo)
    try:
        r = requests.get(GH_RAW_BHAV_URL, timeout=30, headers={"User-Agent": "curl/7.88"})
        if r.status_code == 200 and r.text:
            return pd.read_csv(io.StringIO(r.text))
    except Exception:
        pass
    return None

def normalize_bhav(df: pd.DataFrame) -> pd.DataFrame:
    canon = {
        "date":"Date", "symbol":"Symbol", "series":"Series", "open":"Open", "high":"High",
        "low":"Low", "close":"Close", "prevclose":"PrevClose", "prev_close":"PrevClose",
        "volume":"Volume", "tottrdqty":"Volume", "turnover":"Turnover", "tottrdval":"Turnover"
    }
    df = df.copy()
    df.columns = [canon.get(c.strip().lower(), c.strip()) for c in df.columns]
    keep = ["Date","Symbol","Series","Open","High","Low","Close","PrevClose","Volume","Turnover"]
    for k in keep:
        if k not in df.columns:
            df[k] = None
    df = df[keep]

    df["Symbol"] = df["Symbol"].astype(str).str.upper().str.strip()
    df["Series"] = df["Series"].astype(str).str.upper().str.strip()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    for c in ["Open","High","Low","Close","PrevClose","Volume","Turnover"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["Date","Symbol","Close"])
    return df

def grow_bhav_hist(df_latest: pd.DataFrame) -> Optional[pd.DataFrame]:
    hist_path = FILES["bhav_hist"]
    keep = ["Date","Symbol","Series","Open","High","Low","Close","PrevClose","Volume","Turnover"]
    if os.path.exists(hist_path):
        try:
            hist = pd.read_csv(hist_path)
        except Exception:
            hist = pd.DataFrame(columns=keep)
    else:
        hist = pd.DataFrame(columns=keep)
    allb = pd.concat([hist, df_latest], ignore_index=True)
    allb = allb.drop_duplicates(subset=["Date","Symbol"], keep="last")
    # keep ~18 months
    allb["Date"] = pd.to_datetime(allb["Date"], errors="coerce")
    if allb["Date"].notna().any():
        cutoff = allb["Date"].max() - timedelta(days=460)
        allb = allb[allb["Date"] >= cutoff].copy()
        allb["Date"] = allb["Date"].dt.strftime("%Y-%m-%d")
    atomic_write_df(allb[keep], hist_path)
    return allb

def derive_52w_from_hist(hist: pd.DataFrame):
    if hist is None or hist.empty:
        return
    df = hist.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    cutoff = df["Date"].max() - timedelta(days=370)
    df = df[df["Date"] >= cutoff].copy()
    agg = df.groupby("Symbol").agg(
        High52W=("High", "max"),
        Low52W=("Low", "min"),
    ).reset_index()

    # Dates of extremes (last occurrence)
    hi = df.merge(agg[["Symbol","High52W"]], on=["Symbol"], how="left")
    hi = hi[hi["High"] == hi["High52W"]].sort_values(["Symbol","Date"]).groupby("Symbol").tail(1)[["Symbol","Date"]]
    hi = hi.rename(columns={"Date": "High52W_Date"})

    lo = df.merge(agg[["Symbol","Low52W"]], on=["Symbol"], how="left")
    lo = lo[lo["Low"] == lo["Low52W"]].sort_values(["Symbol","Date"]).groupby("Symbol").tail(1)[["Symbol","Date"]]
    lo = lo.rename(columns={"Date": "Low52W_Date"})

    out = agg.merge(hi, on="Symbol", how="left").merge(lo, on="Symbol", how="left")
    for c in ["High52W_Date","Low52W_Date"]:
        out[c] = pd.to_datetime(out[c], errors="coerce").dt.strftime("%Y-%m-%d")
    atomic_write_df(out[["Symbol","High52W","High52W_Date","Low52W","Low52W_Date"]], FILES["hi52w"])

# ---- Main --------------------------------------------------------------------

def main():
    ensure_dirs()
    status = {"ok": True, "when_ist": now_ist().isoformat(), "steps": {}}

    s = get_session()

    # 1) Deliverables (MTO) with multi-day fallback
    try:
        ddf = fetch_deliverables_latest(s)
        if ddf is not None and not ddf.empty:
            atomic_write_df(ddf[["Date","Symbol","Series","Deliverable_Qty","Traded_Qty","Delivery_Pct"]], FILES["deliverables_latest"])
            # Grow small history
            try:
                hist = pd.read_csv(FILES["deliverables_hist"])
            except Exception:
                hist = pd.DataFrame(columns=["Date","Symbol","Series","Deliverable_Qty","Traded_Qty","Delivery_Pct"])
            hist = pd.concat([hist, ddf], ignore_index=True)
            hist = hist.drop_duplicates(subset=["Date","Symbol","Series"], keep="last")
            hist["Date"] = pd.to_datetime(hist["Date"], errors="coerce")
            if hist["Date"].notna().any():
                cutoff = hist["Date"].max() - timedelta(days=120)
                hist = hist[hist["Date"] >= cutoff].copy()
                hist["Date"] = hist["Date"].dt.strftime("%Y-%m-%d")
            atomic_write_df(hist, FILES["deliverables_hist"])
            status["steps"]["deliverables"] = {"ok": True, "rows": int(len(ddf))}
        else:
            status["ok"] = False
            status["steps"]["deliverables"] = {"ok": False, "error": "No recent MTO found (kept last-good if existed)"}
    except Exception as e:
        status["ok"] = False
        status["steps"]["deliverables"] = {"ok": False, "error": str(e)}

    # 2) Bulk deals (best-effort; keep last-good)
    try:
        bdf = fetch_corporates_table(s, "bulk")
        if bdf is not None and not bdf.empty:
            atomic_write_df(bdf, FILES["bulk_latest"])
            status["steps"]["bulk"] = {"ok": True, "rows": int(len(bdf))}
        else:
            status["steps"]["bulk"] = {"ok": False, "error": "No bulk data (kept last-good)"}
    except Exception as e:
        status["steps"]["bulk"] = {"ok": False, "error": str(e)}

    # 3) Block deals (best-effort; keep last-good)
    try:
        kdf = fetch_corporates_table(s, "block")
        if kdf is not None and not kdf.empty:
            atomic_write_df(kdf, FILES["block_latest"])
            status["steps"]["block"] = {"ok": True, "rows": int(len(kdf))}
        else:
            status["steps"]["block"] = {"ok": False, "error": "No block data (kept last-good)"}
    except Exception as e:
        status["steps"]["block"] = {"ok": False, "error": str(e)}

    # 4) Bhav history & 52w derive (self-seeding)
    try:
        bl = load_bhav_latest_local_or_remote()
        if bl is None or bl.empty:
            status["steps"]["bhav_hist_52w"] = {"ok": False, "error": "bhav_latest not found locally or remotely"}
        else:
            bl = normalize_bhav(bl)
            hist = grow_bhav_hist(bl)
            if hist is not None and not hist.empty:
                derive_52w_from_hist(hist)
                status["steps"]["bhav_hist_52w"] = {"ok": True, "hist_rows": int(len(hist))}
            else:
                status["steps"]["bhav_hist_52w"] = {"ok": False, "error": "hist empty after grow"}
    except Exception as e:
        status["steps"]["bhav_hist_52w"] = {"ok": False, "error": str(e)}

    # 5) Final status (never fail the run)
    try:
        atomic_write_text(json.dumps(status, indent=2), FILES["status"])
    except Exception:
        pass

    # Always green
    print(json.dumps(status, indent=2))
    return 0

if __name__ == "__main__":
    sys.exit(main())
