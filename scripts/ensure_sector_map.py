#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

BHAV_PATH   = Path("data/prices/bhav_latest.csv")
BASE_PATH   = Path("ref/symbol_sector.csv")
MANUAL_PATH = Path("ref/symbol_sector_manual.csv")

def main():
    if not BHAV_PATH.exists():
        raise SystemExit(f"Missing {BHAV_PATH}")

    bhav = pd.read_csv(BHAV_PATH)
    if "Symbol" not in bhav.columns:
        raise SystemExit("bhav_latest.csv missing Symbol column")

    # 1) Current traded universe
    universe = (
        bhav["Symbol"]
        .dropna()
        .astype(str)
        .unique()
    )
    universe_set = set(universe)

    # 2) Manual map (your file) — source of truth
    if not MANUAL_PATH.exists():
        raise SystemExit(f"Manual sector file not found: {MANUAL_PATH}")

    manual = pd.read_csv(MANUAL_PATH)

    # Be tolerant to column naming
    if "Sector" not in manual.columns:
        if "Industry" in manual.columns:
            manual = manual.rename(columns={"Industry": "Sector"})
        else:
            raise SystemExit("symbol_sector_manual.csv must have Sector or Industry column")

    if "Symbol" not in manual.columns:
        raise SystemExit("symbol_sector_manual.csv must have Symbol column")

    manual["Symbol"] = manual["Symbol"].astype(str)
    manual["Sector"] = manual["Sector"].fillna("OTHER").astype(str)

    # Keep only needed columns & dedupe
    manual = manual[["Symbol", "Sector"]].drop_duplicates(subset=["Symbol"])

    manual_syms = set(manual["Symbol"])
    covered = len(universe_set & manual_syms)
    missing = sorted(universe_set - manual_syms)

    print(f"[ensure_sector_map] manual map has {len(manual_syms)} symbols")
    print(f"[ensure_sector_map] coverage vs universe: {covered} / {len(universe_set)}")

    # 3) Start from manual map
    base = manual.copy()

    # 4) Add missing universe symbols as OTHER
    if missing:
        extra = pd.DataFrame({
            "Symbol": missing,
            "Sector": ["OTHER"] * len(missing),
        })
        base = pd.concat([base, extra], ignore_index=True)

    # 5) Final cleanup & write
    base = (
        base
        .drop_duplicates(subset=["Symbol"])
        .sort_values("Symbol")
        .reset_index(drop=True)
    )

    BASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    base.to_csv(BASE_PATH, index=False)

    other_count = (base["Sector"] == "OTHER").sum()
    print(f"[ensure_sector_map] wrote {BASE_PATH} with {len(base)} rows "
          f"(OTHER={other_count})")

if __name__ == "__main__":
    main()
