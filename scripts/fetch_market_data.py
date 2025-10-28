#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fetches & normalizes:
  - deliverables_latest.csv  (from NSE MTO file)
  - bulk_deals_latest.csv    (NSE corporates JSON)
  - block_deals_latest.csv   (NSE corporates JSON)
  - highlow_52w.csv          (derived from data/prices/bhav_hist.csv,
                              which is grown daily from data/prices/bhav_latest.csv)

All writes are atomic: write .tmp then replace.
Keeps a minimal history for bhav (bhav_hist.csv) and deliverables (deliverables_hist.csv).
"""

from __future__ import annotations
import os, io, sys, json, time, shutil
from datetime import datetime, timedelta, timezone
import pandas as pd
import requests

# ---------- Helpers ----------

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

def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(PRICES_DIR, exist_ok=True)

def atomic_write(df: pd.DataFrame, path: str):
    tmp = path + ".tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)

def atomic_write_text(txt: str, path: str):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(txt)
    os.replace(tmp, path)

def today_ist():
    return datetime.now(IST)

def previous_business_day(d: datetime):
    # Mon=0 .. Sun=6
    wd = d.weekday()
    if wd == 0:  # Monday -> previous Friday
        return d - timedelta(days=3)
    elif wd == 6:  # Sunday -> Friday
        return d - timedelta(days=2)
    elif wd == 5:  # Saturday -> Friday
        return d - timedelta(days=1)
    return d - timedelta(days=1)

def get_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "DNT": "1",
        "Referer": "https://www.nseindia.com/market-data/large-deals",
    })
    try:
        s.get("https://www.nseindia.com", timeout=20)
    except Exception:
        pass
    return s

def parse_mto_text(txt: str) -> pd.DataFrame:
    """
    Parses NSE MTO (security-wise deliverables) .DAT text.
    We find the header line with 'SYMBOL' and 'DELIV' then read until 'TOTAL'.
    """
    lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
    # Find header row index
    hdr_idx = None
    for i, ln in enumerate(lines):
        if all(k in ln.upper() for k in ["SYMBOL", "SERIES", "DELIV", "TRaded".upper()[:5]]):
            hdr_idx = i
            break
    if hdr_idx is None:
        # fallback: try common header
        for i, ln in enumerate(lines):
            if "SYMBOL" in ln.upper() and "DELIVERABLE" in ln.upper():
                hdr_idx = i
                break
    if hdr_idx is None:
        raise ValueError("Could not locate MTO header line")

    header = [h.strip().replace(" ", "_") for h in lines[hdr_idx].split(",")]
    rows = []
    for ln in lines[hdr_idx+1:]:
        up = ln.upper()
        if up.startswith("TOTAL") or up.startswith("GRAND"):
            break
        parts = [p.strip() for p in ln.split(",")]
        if len(parts) != len(header):
            # Some MTO lines may have extra commas in security names; skip weird rows
            continue
        rows.append(parts)

    df = pd.DataFrame(rows, columns=header)
    # Normalize expected columns
    # Common columns in MTO: DATE, SYMBOL, SERIES, TOTAL_TRADES, TOTAL_TRD_QTY, DELIV_QTY, DELIV_PER
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
        elif ("DELIV" in cu and "%") in c or "PER" in cu:
            colmap[c] = "Delivery_Pct"
    df = df.rename(columns=colmap)
    keep = ["Date","Symbol","Series","Deliverable_Qty","Traded_Qty","Delivery_Pct"]
    for k in keep:
        if k not in df.columns:
            df[k] = None
    df = df[keep]
    # Clean types
    df["Symbol"] = df["Symbol"].str.upper().str.strip()
    df["Series"] = df["Series"].str.upper().str.strip()
    for numcol in ["Deliverable_Qty","Traded_Qty","Delivery_Pct"]:
        df[numcol] = (
            df[numcol]
            .astype(str).str.replace(",", "", regex=False)
            .str.replace("%", "", regex=False)
            .str.strip()
        )
        df[numcol] = pd.to_numeric(df[numcol], errors="coerce")
    # Date: keep as YYYY-MM-DD string
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True).dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["Date","Symbol"])
    return df

def fetch_deliverables(s: requests.Session, ref_date: datetime) -> pd.DataFrame:
    # NSE archive path pattern: MTO_DDMMYYYY.DAT
    ddmmyyyy = ref_date.strftime("%d%m%Y")
    url = f"https://www.nseindia.com/archives/equities/mto/MTO_{ddmmyyyy}.DAT"
    r = s.get(url, timeout=30)
    if r.status_code != 200 or not r.text:
        raise RuntimeError(f"MTO fetch failed: {r.status_code}")
    return parse_mto_text(r.text)

def fetch_corporates_json(s: requests.Session, url: str, keymap: dict) -> pd.DataFrame:
    r = s.get(url, timeout=30, headers={"Accept": "application/json, text/plain, */*"})
    if r.status_code != 200:
        raise RuntimeError(f"Fetch failed {url}: {r.status_code}")
    data = r.json()
    # Try common containers
    if isinstance(data, dict) and "data" in data:
        rows = data["data"]
    elif isinstance(data, list):
        rows = data
    else:
        # Some endpoints return {'bulk_deals': [...]} etc.
        for v in data.values():
            if isinstance(v, list):
                rows = v
                break
        else:
            raise RuntimeError("Unknown JSON shape")
    recs = []
    for rec in rows:
        out = {}
        for src, dst in keymap.items():
            out[dst] = rec.get(src)
        recs.append(out)
    df = pd.DataFrame(recs)
    if df.empty:
        return df
    # Normalize
    df["Symbol"] = df["Symbol"].astype(str).str.upper().str.strip()
    df["Buy_Sell"] = df["Buy_Sell"].astype(str).str.upper().str.replace("PURCHASE","BUY").str.replace("SELLL","SELL")
    for c in ("Quantity","Price"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    # Date
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True).dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["Symbol"])
    # Add Source
    df["Source"] = url
    return df

def grow_bhav_hist_from_latest():
    latest_path = FILES["bhav_latest"]
    if not os.path.exists(latest_path):
        print("WARNING: bhav_latest.csv not found; skipping hist+52w derivation.")
        return None
    bhav_latest = pd.read_csv(latest_path)
    # Normalize columns
    # Expect: Date,Symbol,Series,Open,High,Low,Close,PrevClose,Volume,Turnover (order may vary)
    # Coerce names to title-case keys if needed
    canon = {
        "date":"Date", "symbol":"Symbol", "series":"Series", "open":"Open", "high":"High",
        "low":"Low", "close":"Close", "prevclose":"PrevClose", "prev_close":"PrevClose",
        "volume":"Volume", "tottrdqty":"Volume", "turnover":"Turnover", "tottrdval":"Turnover"
    }
    bhav_latest.columns = [canon.get(c.strip().lower(), c.strip()) for c in bhav_latest.columns]
    keep = ["Date","Symbol","Series","Open","High","Low","Close","PrevClose","Volume","Turnover"]
    for k in keep:
        if k not in bhav_latest.columns:
            bhav_latest[k] = None
    bhav_latest = bhav_latest[keep]
    # Clean
    bhav_latest["Symbol"] = bhav_latest["Symbol"].astype(str).str.upper().str.strip()
    bhav_latest["Series"] = bhav_latest["Series"].astype(str).str.upper().str.strip()
    bhav_latest["Date"] = pd.to_datetime(bhav_latest["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    numcols = ["Open","High","Low","Close","PrevClose","Volume","Turnover"]
    for c in numcols:
        bhav_latest[c] = pd.to_numeric(bhav_latest[c], errors="coerce")
    bhav_latest = bhav_latest.dropna(subset=["Date","Symbol","Close"])

    # Load/append to hist
    hist_path = FILES["bhav_hist"]
    if os.path.exists(hist_path):
        hist = pd.read_csv(hist_path)
    else:
        hist = pd.DataFrame(columns=keep)
    # Concat and de-dup by (Date, Symbol)
    allb = pd.concat([hist, bhav_latest], ignore_index=True)
    allb = allb.drop_duplicates(subset=["Date","Symbol"], keep="last")
    # Keep last ~400 trading days
    allb["Date"] = pd.to_datetime(allb["Date"])
    cutoff = allb["Date"].max() - timedelta(days=460)
    allb = allb[allb["Date"] >= cutoff].copy()
    allb["Date"] = allb["Date"].dt.strftime("%Y-%m-%d")
    atomic_write(allb, hist_path)
    return allb

def derive_52w(bhav_hist: pd.DataFrame):
    if bhav_hist is None or bhav_hist.empty:
        return
    df = bhav_hist.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    # last ~252 trading days ≈ 52 weeks
    cutoff = df["Date"].max() - timedelta(days=370)
    df = df[df["Date"] >= cutoff].copy()
    agg = df.groupby("Symbol").agg(
        High52W=("High", "max"),
        Low52W=("Low", "min"),
    ).reset_index()
    # Dates of those extrema
    m = df.merge(agg[["Symbol","High52W"]], on=["Symbol"], how="left")
    hi_dates = m.loc[m["High"]==m["High52W"]].sort_values(["Symbol","Date"]).groupby("Symbol").tail(1)[["Symbol","Date"]]
    hi_dates = hi_dates.rename(columns={"Date":"High52W_Date"})
    m2 = df.merge(agg[["Symbol","Low52W"]], on=["Symbol"], how="left")
    lo_dates = m2.loc[m2["Low"]==m2["Low52W"]].sort_values(["Symbol","Date"]).groupby("Symbol").tail(1)[["Symbol","Date"]]
    lo_dates = lo_dates.rename(columns={"Date":"Low52W_Date"})
    out = agg.merge(hi_dates, on="Symbol", how="left").merge(lo_dates, on="Symbol", how="left")
    # Format
    for c in ["High52W_Date","Low52W_Date"]:
        out[c] = pd.to_datetime(out[c], errors="coerce").dt.strftime("%Y-%m-%d")
    atomic_write(out[["Symbol","High52W","High52W_Date","Low52W","Low52W_Date"]], FILES["hi52w"])

def main():
    ensure_dirs()
    s = get_session()
    now = today_ist()
    ref = previous_business_day(now)

    status = {"ok": True, "when_ist": now.isoformat(), "steps": {}}

    # --- Deliverables (MTO) ---
    try:
      ddf = fetch_deliverables(s, ref)
      if not ddf.empty:
          # Write latest
          ddf2 = ddf[["Date","Symbol","Series","Deliverable_Qty","Traded_Qty","Delivery_Pct"]].copy()
          atomic_write(ddf2, FILES["deliverables_latest"])
          # Grow history (90 days)
          if os.path.exists(FILES["deliverables_hist"]):
              hist = pd.read_csv(FILES["deliverables_hist"])
          else:
              hist = pd.DataFrame(columns=ddf2.columns)
          hist = pd.concat([hist, ddf2], ignore_index=True)
          hist = hist.drop_duplicates(subset=["Date","Symbol","Series"], keep="last")
          hist["Date"] = pd.to_datetime(hist["Date"])
          cutoff = hist["Date"].max() - timedelta(days=120)
          hist = hist[hist["Date"] >= cutoff].copy()
          hist["Date"] = hist["Date"].dt.strftime("%Y-%m-%d")
          atomic_write(hist, FILES["deliverables_hist"])
          status["steps"]["deliverables"] = {"ok": True, "rows": int(len(ddf2))}
      else:
          raise RuntimeError("Deliverables empty")
    except Exception as e:
      status["ok"] = False
      status["steps"]["deliverables"] = {"ok": False, "error": str(e)}

    # --- Bulk deals ---
    try:
      # Try common NSE corporates bulk endpoint variant(s)
      keymap = {
        "symbol": "Symbol", "clientName": "Client_Name", "buySell": "Buy_Sell",
        "quantity": "Quantity", "avgPrice": "Price", "dealDate": "Date", "date": "Date", "price": "Price"
      }
      # Try a few known paths; first one that works wins
      urls_try = [
        "https://www.nseindia.com/api/corporates-bulk-deals?index=equities",
        "https://www.nseindia.com/api/corporates/bulk-deals?index=equities",
        "https://www.nseindia.com/api/corporates-pit?index=bulk-deals",
      ]
      bdf = pd.DataFrame()
      for u in urls_try:
          try:
              bdf = fetch_corporates_json(s, u, keymap)
              if not bdf.empty:
                  break
          except Exception:
              continue
      if bdf.empty:
          raise RuntimeError("No bulk deals data from tried endpoints")
      bdf = bdf[["Date","Symbol","Client_Name","Buy_Sell","Quantity","Price","Source"]].copy()
      atomic_write(bdf, FILES["bulk_latest"])
      status["steps"]["bulk"] = {"ok": True, "rows": int(len(bdf))}
    except Exception as e:
      status["ok"] = False
      status["steps"]["bulk"] = {"ok": False, "error": str(e)}

    # --- Block deals ---
    try:
      keymap = {
        "symbol": "Symbol", "clientName": "Client_Name", "buySell": "Buy_Sell",
        "quantity": "Quantity", "avgPrice": "Price", "dealDate": "Date", "date": "Date", "price": "Price"
      }
      urls_try = [
        "https://www.nseindia.com/api/corporates-block-deals?index=equities",
        "https://www.nseindia.com/api/corporates/block-deals?index=equities",
        "https://www.nseindia.com/api/corporates-pit?index=block-deals",
      ]
      kdf = pd.DataFrame()
      for u in urls_try:
          try:
              kdf = fetch_corporates_json(s, u, keymap)
              if not kdf.empty:
                  break
          except Exception:
              continue
      if kdf.empty:
          raise RuntimeError("No block deals data from tried endpoints")
      kdf = kdf[["Date","Symbol","Client_Name","Buy_Sell","Quantity","Price","Source"]].copy()
      atomic_write(kdf, FILES["block_latest"])
      status["steps"]["block"] = {"ok": True, "rows": int(len(kdf))}
    except Exception as e:
      status["ok"] = False
      status["steps"]["block"] = {"ok": False, "error": str(e)}

    # --- bhav history & 52w derivation ---
    try:
      hist = grow_bhav_hist_from_latest()
      if hist is not None and not hist.empty:
          derive_52w(hist)
          status["steps"]["bhav_hist_52w"] = {"ok": True, "hist_rows": int(len(hist))}
      else:
          status["steps"]["bhav_hist_52w"] = {"ok": False, "error": "bhav_hist empty or missing"}
    except Exception as e:
      status["ok"] = False
      status["steps"]["bhav_hist_52w"] = {"ok": False, "error": str(e)}

    # --- status file ---
    try:
      atomic_write_text(json.dumps(status, indent=2), FILES["status"])
    except Exception:
      pass

    # Final log
    print(json.dumps(status, indent=2))
    # Non-zero exit on failure so Actions shows red
    if not status["ok"]:
        sys.exit(1)

if __name__ == "__main__":
    main()
