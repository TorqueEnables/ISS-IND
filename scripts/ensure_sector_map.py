#!/usr/bin/env python3
"""
Ensure ref/symbol_sector.csv exists and is aligned with the current universe.

- Reads latest bhav universe from data/prices/bhav_latest.csv
- Uses manual overrides from ref/symbol_sector_manual.csv (if present)
- Merges with existing ref/symbol_sector.csv (if present)
- Writes consolidated ref/symbol_sector.csv used by score_week.py
"""

from pathlib import Path
import sys
import pandas as pd


def _pick_flex(cols, names):
    """
    Case-insensitive fuzzy column chooser.
    Tries exact (case-insensitive) first, then substring match.
    """
    cols_list = list(cols)
    lower = {c.lower(): c for c in cols_list}
    names_lower = [n.lower() for n in names]

    # Exact case-insensitive match
    for n in names_lower:
        if n in lower:
            return lower[n]

    # Substring match
    for c in cols_list:
        cl = c.lower()
        for n in names_lower:
            if n in cl:
                return c
    return None


def load_universe_symbols(bhav_path: Path):
    if not bhav_path.exists():
        print(f"[ensure_sector_map] {bhav_path} not found; skipping sector map build", file=sys.stderr)
        return set()

    bhav = pd.read_csv(bhav_path)
    if bhav.empty:
        print(f"[ensure_sector_map] {bhav_path} is empty; skipping sector map build", file=sys.stderr)
        return set()

    sym_col = _pick_flex(
        bhav.columns,
        [
            "Symbol", "SYMBOL", "symbol",
            "Scrip", "Security", "Security Name",
            "SC_NAME", "SC_CODE", "TRADING_SYMBOL",
        ],
    )
    if not sym_col:
        print(
            f"[ensure_sector_map] could not detect symbol column in {bhav_path}; "
            f"columns={list(bhav.columns)}",
            file=sys.stderr,
        )
        return set()

    bhav["SymKey"] = bhav[sym_col].astype(str).str.strip().str.upper()
    universe = set(bhav["SymKey"].dropna().unique())
    print(f"[ensure_sector_map] detected {len(universe)} symbols in latest bhav universe")
    return universe


def load_existing_map(out_path: Path):
    if not out_path.exists():
        return {}

    try:
        cur = pd.read_csv(out_path)
    except Exception as e:
        print(f"[ensure_sector_map] failed to read existing {out_path}: {e}", file=sys.stderr)
        return {}

    sym_col = _pick_flex(cur.columns, ["Symbol", "SYMBOL", "symbol"])
    sec_col = _pick_flex(cur.columns, ["Sector", "SECTOR", "sector", "Industry", "INDUSTRY"])
    if not sym_col or not sec_col:
        return {}

    cur["SymKey"] = cur[sym_col].astype(str).str.strip().str.upper()
    mapping = {}
    for _, row in cur[["SymKey", sec_col]].dropna(subset=["SymKey"]).iterrows():
        mapping[row["SymKey"]] = str(row[sec_col])
    print(f"[ensure_sector_map] loaded {len(mapping)} existing symbol→sector entries")
    return mapping


def load_manual_map(manual_path: Path):
    if not manual_path.exists():
        print("[ensure_sector_map] no manual file ref/symbol_sector_manual.csv; skipping overrides")
        return {}

    try:
        man = pd.read_csv(manual_path)
    except Exception as e:
        print(f"[ensure_sector_map] failed to read manual map {manual_path}: {e}", file=sys.stderr)
        return {}

    if man.empty:
        print("[ensure_sector_map] manual file is empty; skipping overrides")
        return {}

    sym_col = _pick_flex(man.columns, ["Symbol", "SYMBOL", "symbol", "Scrip", "Security"])
    sec_col = _pick_flex(man.columns, ["Sector", "SECTOR", "sector", "Industry", "INDUSTRY"])
    if not sym_col or not sec_col:
        print(
            f"[ensure_sector_map] manual file missing Symbol/Sector-like columns; "
            f"cols={list(man.columns)}",
            file=sys.stderr,
        )
        return {}

    man["SymKey"] = man[sym_col].astype(str).str.strip().str.upper()
    mapping = {}
    for _, row in man[["SymKey", sec_col]].dropna(subset=["SymKey"]).iterrows():
        mapping[row["SymKey"]] = str(row[sec_col])
    print(f"[ensure_sector_map] loaded {len(mapping)} manual symbol→sector entries")
    return mapping


def main():
    bhav_path   = Path("data/prices/bhav_latest.csv")
    manual_path = Path("ref/symbol_sector_manual.csv")
    out_path    = Path("ref/symbol_sector.csv")

    universe = load_universe_symbols(bhav_path)
    if not universe:
        print("[ensure_sector_map] no universe symbols; nothing to do")
        return

    existing_map = load_existing_map(out_path)
    manual_map   = load_manual_map(manual_path)

    rows = []
    for sym in sorted(universe):
        if sym in manual_map:
            sector = manual_map[sym]
        elif sym in existing_map:
            sector = existing_map[sym]
        else:
            sector = "OTHER"
        rows.append((sym, sector))

    sec_df = pd.DataFrame(rows, columns=["Symbol", "Sector"])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    sec_df.to_csv(out_path, index=False)

    print(
        f"[ensure_sector_map] wrote {out_path} with {len(sec_df)} symbols "
        f"(manual={len(manual_map)}, existing={len(existing_map)})"
    )


if __name__ == "__main__":
    main()
