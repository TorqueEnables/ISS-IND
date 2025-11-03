#!/usr/bin/env python3
# Rollback placeholder: no network, just ensure files & write a friendly status.
import os, json
import pandas as pd

DATA_DIR = "data"
PRICES_DIR = os.path.join(DATA_DIR, "prices")
FILES = {
    "status": os.path.join(DATA_DIR, "fetch_status.json"),
    "deliverables_latest": os.path.join(DATA_DIR, "deliverables_latest.csv"),
    "deliverables_hist":   os.path.join(DATA_DIR, "deliverables_hist.csv"),
    "bulk_latest":         os.path.join(DATA_DIR, "bulk_deals_latest.csv"),
    "block_latest":        os.path.join(DATA_DIR, "block_deals_latest.csv"),
    "hi52w":               os.path.join(DATA_DIR, "highlow_52w.csv"),
    "bhav_hist":           os.path.join(PRICES_DIR, "bhav_hist.csv"),
    "tech_latest":         os.path.join(DATA_DIR, "tech_latest.csv"),
}

def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(PRICES_DIR, exist_ok=True)

def ensure_headers(path, headers):
    if not os.path.exists(path):
        pd.DataFrame(columns=headers).to_csv(path, index=False)

def main():
    ensure_dirs()
    # Keep headers stable so Sheets never break
    ensure_headers(FILES["deliverables_latest"], ["Date","Symbol","Series","Deliverable_Qty","Traded_Qty","Delivery_Pct"])
    ensure_headers(FILES["deliverables_hist"],   ["Date","Symbol","Series","Deliverable_Qty","Traded_Qty","Delivery_Pct"])
    ensure_headers(FILES["bulk_latest"],         ["Date","Symbol","Client_Name","Buy_Sell","Quantity","Price","Source"])
    ensure_headers(FILES["block_latest"],        ["Date","Symbol","Client_Name","Buy_Sell","Quantity","Price","Source"])
    ensure_headers(FILES["hi52w"],               ["Symbol","High52W","High52W_Date","Low52W","Low52W_Date"])
    ensure_headers(FILES["bhav_hist"],           ["Date","Symbol","Series","Open","High","Low","Close","PrevClose","Volume","Turnover"])
    ensure_headers(FILES["tech_latest"],         ["Date","Symbol","Series","Open","High","Low","Close","PrevClose","Volume",
                                                  "Range","TR","ATR14","SMA10","SMA20","SMA50","BB_Mid","BB_Upper","BB_Lower",
                                                  "BB_Bandwidth","CLV","RangeAvg20","VolAvg20","HI55","HI55_Recent","NR7","Delivery_Pct"])
    status = {
        "ok": True,
        "mode": "rollback-placeholder",
        "note": "No network fetch performed. Safe baseline in place.",
    }
    with open(FILES["status"], "w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)
    print(json.dumps(status, indent=2))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
