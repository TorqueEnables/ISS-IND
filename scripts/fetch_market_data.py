def fetch_bhav_archive_zip_for_date(s: requests.Session, d: datetime) -> Optional[pd.DataFrame]:
    """
    Try zipped daily bhav from archives → www → www1.
    Fully exception-safe: any per-URL failure is caught and we continue.
    """
    MON = d.strftime("%b").upper()
    DD = d.strftime("%d"); YYYY = d.strftime("%Y")
    urls = [
        f"https://archives.nseindia.com/content/historical/EQUITIES/{YYYY}/{MON}/cm{DD}{MON}{YYYY}bhav.csv.zip",
        f"https://www.nseindia.com/content/historical/EQUITIES/{YYYY}/{MON}/cm{DD}{MON}{YYYY}bhav.csv.zip",
        f"https://www1.nseindia.com/content/historical/EQUITIES/{YYYY}/{MON}/cm{DD}{MON}{YYYY}bhav.csv.zip",
    ]
    for url in urls:
        try:
            r = s.get(url, timeout=50, headers={"Referer":"https://www.nseindia.com/market-data"}, allow_redirects=True)
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
            # swallow and continue to next host
            continue
    return None


def cold_seed_bhav_hist_if_empty(s: requests.Session, status_urls: Dict[str, Any], min_days:int=90) -> int:
    """
    If bhav_hist is empty, walk back through up to ~180 prior business days.
    For each day, try sec_bhavdata_full DDMMYYYY CSV, then zipped cmDDMONYYYYbhav.csv.zip (3 hosts).
    All exceptions are swallowed so a single TLS error cannot abort the scan.
    """
    # Load current hist (may be header-only)
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

    # Try up to 180 business days back; stop once we have >= min_days distinct trade dates
    for d in previous_business_days(180):
        # 1) sec_bhavdata_full
        sec_url = f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{d.strftime('%d%m%Y')}.csv"
        status_urls.setdefault("bhav_tried", []).append(sec_url)
        df = None
        try:
            r = s.get(sec_url, timeout=40, headers={"Referer":"https://www.nseindia.com/market-data"})
            if r.status_code == 200 and r.text:
                tmp = pd.read_csv(io.StringIO(r.text))
                # normalize to canonical
                canon = {
                    "symbol":"Symbol","series":"Series","open":"Open","high":"High","low":"Low","close":"Close",
                    "prev. close":"PrevClose","prev_close":"PrevClose","previousclose":"PrevClose",
                    "ttl_trd_qnty":"Volume","tottrdqty":"Volume","turnover_lacs":"Turnover","tottrdval":"Turnover",
                    "date1":"Date","timestamp":"Date"
                }
                tmp.columns = [canon.get(c.strip().lower(), c.strip()) for c in tmp.columns]
                keep = ["Date","Symbol","Series","Open","High","Low","Close","PrevClose","Volume","Turnover"]
                for k in keep:
                    if k not in tmp.columns: tmp[k] = None
                df = tmp[keep].copy()
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
                for c in ["Open","High","Low","Close","PrevClose","Volume","Turnover"]:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
                df["Symbol"] = df["Symbol"].astype(str).str.upper().str.strip()
                df["Series"] = df["Series"].astype(str).str.upper().str.strip()
                df = df.dropna(subset=["Date","Symbol","Close"])
        except Exception:
            df = None

        # 2) zipped archives if sec_full missing
        if df is None or df.empty:
            # for visibility, log the 3 hosts we will try
            for base in ("archives.nseindia.com","www.nseindia.com","www1.nseindia.com"):
                status_urls.setdefault("bhav_tried", []).append(
                    f"https://{base}/content/historical/EQUITIES/{d.strftime('%Y')}/{d.strftime('%b').upper()}/cm{d.strftime('%d%b%Y').upper()}bhav.csv.zip"
                )
            try:
                df = fetch_bhav_archive_zip_for_date(s, d)
            except Exception:
                df = None

        # If we got data for the day, collect it
        if df is not None and not df.empty:
            day = df["Date"].iloc[0]
            if day not in seen_dates:
                frames.append(df)
                seen_dates.add(day)

        # polite pause
        time.sleep(0.5)

        # stop once we have enough distinct days
        if len(seen_dates) >= min_days:
            break

    if frames:
        allb = pd.concat(frames, ignore_index=True)
        allb = allb.drop_duplicates(subset=["Date","Symbol"], keep="last")
        allb["Date"] = pd.to_datetime(allb["Date"]).dt.strftime("%Y-%m-%d")
        atomic_write_df(allb, FILES["bhav_hist"])
        return len(allb)

    # nothing fetched; keep header-only file
    return 0
