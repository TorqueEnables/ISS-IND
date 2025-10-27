#!/usr/bin/env python3
import csv, sys, os, time, glob, re
from pathlib import Path
from datetime import datetime, timedelta
from urllib.request import Request, urlopen, URLError, HTTPError

ROOT = Path(__file__).resolve().parents[1]  # repo root
PRICES_DIR = ROOT / "data" / "prices"
PRICES_DIR.mkdir(parents=True, exist_ok=True)

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
URL_TMPL = "https://archives.nseindia.com/products/content/sec_bhavdata_full_{ddmmyyyy}.csv"

DESIRED_COLS = 15
KEEP_DAYS_DEFAULT = 10      # for daily mirror
BACKFILL_DAYS_DEFAULT = 150 # for backfill
ROLLING_N = 90              # days to stitch

def dmy(n_days_ago: int) -> str:
    d = datetime.utcnow() - timedelta(days=n_days_ago)
    return d.strftime("%d%m%Y")  # DDMMYYYY

def fetch_day(ddmmyyyy: str) -> bool:
    """Download one day if missing; return True if saved."""
    out = PRICES_DIR / f"bhav_{ddmmyyyy}.csv"
    if out.exists() and out.stat().st_size > 0:
        return False
    url = URL_TMPL.format(ddmmyyyy=ddmmyyyy)
    req = Request(url, headers={"User-Agent": UA})
    # simple retry
    for attempt in range(5):
        try:
            with urlopen(req, timeout=20) as resp:
                if resp.status != 200:
                    raise HTTPError(url, resp.status, "HTTP status", hdrs=None, fp=None)
                text = resp.read().decode("utf-8", errors="replace")
            break
        except (URLError, HTTPError):
            if attempt == 4:
                return False
            time.sleep(2)
    # validate header
    first_line = text.splitlines()[0] if text else ""
    if not first_line.startswith("SYMBOL"):
        return False
    out.write_text(text, encoding="utf-8")
    return True

def list_daily_files_sorted():
    """Return per-day files sorted newest->oldest, with parsed date key."""
    files = []
    pat = re.compile(r"bhav_(\d{2})(\d{2})(\d{4})\.csv$")
    for p in PRICES_DIR.glob("bhav_*.csv"):
        m = pat.fullmatch(p.name)
        if not m: 
            continue
        dd, mm, yyyy = map(int, m.groups())
        files.append(((yyyy, mm, dd), p))
    files.sort(key=lambda x: x[0], reverse=True)  # newest first
    return files

def stitch_latest_90(files_newest_first):
    """Build bhav_latest_90d.csv (chronological, single header) and bhav_latest.csv."""
    if not files_newest_first:
        print("No daily files found to stitch", file=sys.stderr)
        return False
    keep = [p for _, p in files_newest_first[:ROLLING_N]]
    keep.reverse()  # oldest -> newest

    tmp = PRICES_DIR / "bhav_latest_90d.csv.tmp"
    with keep[0].open("r", encoding="utf-8", newline="") as fh0, tmp.open("w", encoding="utf-8", newline="") as out:
        r0 = csv.reader(fh0)
        header = next(r0, None)
        if not header:
            print("Oldest file missing header", file=sys.stderr)
            return False
        writer = csv.writer(out)
        writer.writerow(header[:DESIRED_COLS] + [""] * max(0, DESIRED_COLS - len(header)))
        # write rows from first file
        for row in r0:
            writer.writerow((row[:DESIRED_COLS] + [""] * max(0, DESIRED_COLS - len(row))))
        # append others skipping their headers
        for p in keep[1:]:
            with p.open("r", encoding="utf-8", newline="") as fh:
                rdr = csv.reader(fh)
                next(rdr, None)
                for row in rdr:
                    writer.writerow((row[:DESIRED_COLS] + [""] * max(0, DESIRED_COLS - len(row))))
    tmp.rename(PRICES_DIR / "bhav_latest_90d.csv")

    # latest single-day convenience copy
    (PRICES_DIR / "bhav_latest.csv").write_text(
        (files_newest_first[0][1]).read_text(encoding="utf-8"),
        encoding="utf-8"
    )
    return True

def split_eq_shards():
    """Make EQ-only shards: p1 header+rows, p2/p3 rows only; normalize to 15 cols."""
    latest90 = PRICES_DIR / "bhav_latest_90d.csv"
    if not latest90.exists():
        print("bhav_latest_90d.csv not found", file=sys.stderr)
        return False

    with latest90.open("r", encoding="utf-8", newline="") as fh:
        rdr = csv.reader(fh)
        header = next(rdr, None)
        if not header:
            return False
        header = header[:DESIRED_COLS] + [""] * max(0, DESIRED_COLS - len(header))
        rows = []
        for row in rdr:
            if not row:
                continue
            row = row[:DESIRED_COLS] + [""] * max(0, DESIRED_COLS - len(row))
            series = row[1].strip().strip('"').upper()
            if series == "EQ":
                rows.append(row)

    n = len(rows)
    p1 = rows[: n // 3]
    p2 = rows[n // 3 : 2 * n // 3]
    p3 = rows[2 * n // 3 : ]

    def write_part(name, part, header_flag):
        path = PRICES_DIR / name
        with path.open("w", encoding="utf-8", newline="") as out:
            w = csv.writer(out)
            if header_flag:
                w.writerow(header)
            w.writerows(part)

    write_part("bhav_latest_90d_p1.csv", p1, True)
    write_part("bhav_latest_90d_p2.csv", p2, False)
    write_part("bhav_latest_90d_p3.csv", p3, False)
    print(f"EQ-only shards: p1={len(p1)} p2={len(p2)} p3={len(p3)}", file=sys.stderr)
    return True

def run(days: int):
    saved = 0
    for i in range(days + 1):
        if fetch_day(dmy(i)):
            saved += 1
    print(f"Fetched {saved} new daily files", file=sys.stderr)
    files = list_daily_files_sorted()
    if not files:
        sys.exit("No daily files present after fetch")
    stitched = stitch_latest_90(files)
    if stitched:
        split_eq_shards()

if __name__ == "__main__":
    # flags: --days N (default 10), or --backfill N (default 150)
    days = KEEP_DAYS_DEFAULT
    args = sys.argv[1:]
    if "--backfill" in args:
        try:
            days = int(args[args.index("--backfill")+1])
        except Exception:
            days = BACKFILL_DAYS_DEFAULT
    elif "--days" in args:
        try:
            days = int(args[args.index("--days")+1])
        except Exception:
            days = KEEP_DAYS_DEFAULT
    run(days)
