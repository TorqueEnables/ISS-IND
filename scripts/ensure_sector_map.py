#!/usr/bin/env python3
"""
Ensure ref/symbol_sector.csv contains all symbols in the latest bhav snapshot.
- No schedule needed; run from on_upload_ingest.
- Downloads official Nifty 500/Midcap 150/Smallcap 250 only if new symbols exist.
- Respects manual overrides in ref/symbol_sector_overrides.csv (if present).
"""

import io, sys
from pathlib import Path
import pandas as pd
import urllib.request as u

HIST = Path("data/prices/bhav_hist.csv")
MAP_OUT = Path("ref/symbol_sector.csv")
OVR = Path("ref/symbol_sector_overrides.csv")

SOURCES = [
    ("NIFTY 500",        "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv"),
    ("NIFTY MIDCAP 150", "https://www.niftyindices.com/IndexConstituent/ind_niftymidcap150list.csv"),
    ("NIFTY SMALLCAP 250","https://www.niftyindices.com/IndexConstituent/ind_niftysmallcap250list.csv"),
]

def read_hist_symbols():
    if not HIST.exists():
        print(f"[ensure_sector_map] missing {HIST}, skip", file=sys.stderr)
        return set()
    df = pd.read_csv(HIST, parse_dates=["Date"])
    if df.empty: return set()
    # Column normalization
    cols = {c.lower(): c for c in df.columns}
    sym = cols.get("symbol") or cols.get("SYMBOL")
    series = cols.get("series") or cols.get("SERIES")
    close = cols.get("close") or cols.get("CLOSE") or cols.get("LAST")
    if sym is None or series is None or close is None:
        # fallback: return all symbols
        symset = set(df.iloc[df["Date"].idxmax()][sym].astype(str).str.upper().unique()) if "Date" in df.columns else set()
        return symset
    last = df["Date"].max()
    snap = df[df["Date"]==last][[sym,series,close]].copy()
    snap.columns = ["Symbol","Series","Close"]
    snap["Symbol"] = snap["Symbol"].astype(str).str.upper().str.strip()
    # only EQ + price>=50 (same universe hygiene as scorer)
    snap = snap[(snap["Series"].astype(str).str.upper()=="EQ") & (pd.to_numeric(snap["Close"], errors="coerce")>=50)]
    return set(snap["Symbol"].unique())

def load_map():
    if MAP_OUT.exists():
        m = pd.read_csv(MAP_OUT)
        if not {"Symbol","Sector"}.issubset(m.columns):
            m = pd.DataFrame(columns=["Symbol","Sector"])
    else:
        m = pd.DataFrame(columns=["Symbol","Sector"])
    m["Symbol"] = m["Symbol"].astype(str).str.upper().str.strip()
    m["Sector"] = m["Sector"].astype(str).str.strip()
    return m

def fetch_csv(url: str) -> pd.DataFrame:
    with u.urlopen(url, timeout=30) as r:
        raw = r.read()
    df = pd.read_csv(io.BytesIO(raw))
    cols = {c.lower(): c for c in df.columns}
    sym = cols.get("symbol")
    ind = cols.get("industry") or cols.get("sector") or cols.get("industry name")
    if sym is None or ind is None:
        raise RuntimeError(f"Unexpected headers from {url}: {df.columns.tolist()}")
    out = df[[sym, ind]].rename(columns={sym:"Symbol", ind:"Sector"})
    out["Symbol"] = out["Symbol"].astype(str).str.upper().str.strip()
    out["Sector"] = out["Sector"].astype(str).str.strip()
    return out

def apply_overrides(df):
    if not OVR.exists(): return df
    o = pd.read_csv(OVR)
    if not {"Symbol","Sector"}.issubset(o.columns): return df
    o["Symbol"] = o["Symbol"].astype(str).str.upper().str.strip()
    o["Sector"] = o["Sector"].astype(str).str.strip()
    base = df.set_index("Symbol")
    base.update(o.set_index("Symbol"))
    return base.reset_index()

def main():
    want = read_hist_symbols()
    if not want:
        print("[ensure_sector_map] nothing to do"); return

    cur = load_map()
    have = set(cur["Symbol"].unique())
    missing = sorted(list(want - have))
    if not missing:
        print("[ensure_sector_map] map already covers current universe"); return

    # Build a lookup from official lists
    print(f"[ensure_sector_map] resolving {len(missing)} new symbols…")
    rows = []
    seen = set()
    for label, url in SOURCES:
        try:
            df = fetch_csv(url)
        except Exception as e:
            print(f"[WARN] {label} fetch failed: {e}", file=sys.stderr); continue
        for _, r in df.iterrows():
            s = r["Symbol"]
            if s in seen: continue
            rows.append({"Symbol": s, "Sector": r["Sector"]})
            seen.add(s)
    lookup = pd.DataFrame(rows)

    # Take only the missing ones; default to OTHER when not found
    if not lookup.empty:
        add = lookup[lookup["Symbol"].isin(missing)].copy()
    else:
        add = pd.DataFrame(columns=["Symbol","Sector"])

    still_missing = sorted(list(set(missing) - set(add["Symbol"].unique())))
    if still_missing:
        add = pd.concat([add, pd.DataFrame({"Symbol": still_missing, "Sector": ["OTHER"]*len(still_missing)})], ignore_index=True)

    new_map = pd.concat([cur, add], ignore_index=True).drop_duplicates(subset=["Symbol"], keep="first")
    new_map = apply_overrides(new_map)
    new_map = new_map.sort_values("Symbol")
    MAP_OUT.parent.mkdir(parents=True, exist_ok=True)
    new_map.to_csv(MAP_OUT, index=False)
    print(f"[ensure_sector_map] wrote {MAP_OUT} (now {len(new_map)} symbols; added {len(add)})")

if __name__ == "__main__":
    main()
