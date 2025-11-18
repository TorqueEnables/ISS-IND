#!/usr/bin/env python3
# Creates ref/symbol_sector.csv (Symbol,Sector) from official Nifty indices CSVs.
# Zero deps beyond pandas. Idempotent, safe to re-run.

import io, sys
from pathlib import Path
import pandas as pd
import urllib.request as u

SOURCES = [
    # Priority order: first hit wins if a Symbol appears in multiple lists.
    ("NIFTY 500",      "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv"),
    ("NIFTY MIDCAP 150","https://www.niftyindices.com/IndexConstituent/ind_niftymidcap150list.csv"),
    ("NIFTY SMALLCAP 250","https://www.niftyindices.com/IndexConstituent/ind_niftysmallcap250list.csv"),
    # Optional: add sector lists if you want to override/clean special cases
    # ("NIFTY AUTO",    "https://www.niftyindices.com/IndexConstituent/ind_niftyautolist.csv"),
]

OUT = Path("ref/symbol_sector.csv")
OUT.parent.mkdir(parents=True, exist_ok=True)

def fetch_csv(url: str) -> pd.DataFrame:
    # The CSVs are plain text and consistent; no cookies required.
    with u.urlopen(url, timeout=30) as r:
        raw = r.read()
    df = pd.read_csv(io.BytesIO(raw))
    # Normalize expected columns across lists
    # Common headers observed: "Company Name","Industry","Symbol","Series","ISIN Code"
    # We only need Symbol + Industry (as Sector).
    cols = {c.lower(): c for c in df.columns}
    sym = cols.get("symbol")
    ind = cols.get("industry") or cols.get("sector") or cols.get("industry name")
    if sym is None or ind is None:
        raise RuntimeError(f"Unexpected headers in {url}: {df.columns.tolist()}")
    df = df[[sym, ind]].rename(columns={sym: "Symbol", ind: "Sector"})
    df["Symbol"] = df["Symbol"].astype(str).str.strip().str.upper()
    df["Sector"] = df["Sector"].astype(str).str.strip()
    return df

def main():
    seen = set()
    rows = []
    for label, url in SOURCES:
        try:
            df = fetch_csv(url)
        except Exception as e:
            print(f"[WARN] Failed {label}: {e}", file=sys.stderr); continue
        for _, r in df.iterrows():
            sym = r["Symbol"]
            if sym in seen:  # keep first occurrence (priority order)
                continue
            seen.add(sym)
            rows.append({"Symbol": sym, "Sector": r["Sector"]})
    if not rows:
        print("[ERR] No data assembled; aborting.", file=sys.stderr); sys.exit(1)
    out = pd.DataFrame(rows).sort_values("Symbol")
    # Minimal cleaning to keep a compact sector taxonomy if you like:
    # out["Sector"] = out["Sector"].str.replace(" and ", " & ", regex=False)
    out.to_csv(OUT, index=False)
    print(f"Wrote {OUT} with {len(out)} symbols")

if __name__ == "__main__":
    main()
