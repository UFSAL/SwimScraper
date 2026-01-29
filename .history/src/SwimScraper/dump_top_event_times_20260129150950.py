"""
dump_top_event_times.py

Given a CSV of recruits with swimmer_IDs,
pull Swimcloud times via API for each swimmer + event
and write a combined CSV.

Robust version:
- handles list-based API responses
- rate limits requests
- retries on 429
- skips 403 / 404 swimmers cleanly
- path-safe CSV loading
"""

import time
from typing import List, Dict, Optional
from pathlib import Path

import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parents[2]

INPUT_CSV = BASE_DIR / "recruits_2021_top100_M.csv"
OUTPUT_CSV = BASE_DIR / "recruits_2021_M_times.csv"

BASE = "https://www.swimcloud.com"

EVENTS = {
    "50 Free (Y)": "1|50|Y|1",
    "100 Free (Y)": "1|100|Y|1",
    "200 Free (Y)": "1|200|Y|1",
    "100 Fly (Y)": "3|100|Y|1",
}

REQUEST_SLEEP_SEC = 0.9 
RETRY_SLEEP_SEC = 5
MAX_RETRIES = 3
TIMEOUT = (10, 30)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/144.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Referer": "https://www.swimcloud.com/",
}

# --------------------------------------------------
# NETWORK HELPERS
# --------------------------------------------------

def safe_get(
    session: requests.Session,
    url: str,
    params: Dict,
) -> Optional[requests.Response]:
    """
    GET request with retries + 429 handling.
    Returns None for 403 / 404.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        r = session.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)

        if r.status_code == 429:
            print(f"      [RATE LIMIT] retrying in {RETRY_SLEEP_SEC}s (attempt {attempt})")
            time.sleep(RETRY_SLEEP_SEC)
            continue

        if r.status_code in (403, 404):
            return None

        r.raise_for_status()
        return r

    print("      [FAIL] Max retries exceeded")
    return None

# --------------------------------------------------
# FETCH
# --------------------------------------------------

def fetch_times(
    session: requests.Session,
    swimmer_id: int,
    event_code: str,
) -> Optional[List[Dict]]:
    """
    Fetch list of swims for swimmer + event.
    Swimcloud returns a LIST, not a dict.
    """
    url = f"{BASE}/api/swimmers/{swimmer_id}/times_by_event/"
    params = {"event": event_code}

    response = safe_get(session, url, params)
    time.sleep(REQUEST_SLEEP_SEC)

    if response is None:
        return None

    data = response.json()

    if not isinstance(data, list):
        print("      [WARN] Unexpected JSON shape")
        return None

    return data

# --------------------------------------------------
# EXTRACT
# --------------------------------------------------

def extract_rows(
    swims: List[Dict],
    swimmer_row: Dict,
    event_label: str,
) -> List[Dict]:
    """
    Normalize swims into flat rows.
    """
    rows: List[Dict] = []

    for s in swims:
        rows.append({
            "class_year": swimmer_row["class_year"],
            "gender": swimmer_row["gender"],
            "swimmer_ID": swimmer_row["swimmer_ID"],
            "swimmer_name": swimmer_row["swimmer_name"],
            "HS_power_index": swimmer_row["HS_power_index"],
            "event_label": event_label,
            "time": s.get("time"),
            "date": s.get("date"),
            "meet": s.get("meet", {}).get("name"),
            "team": s.get("team", {}).get("name"),
            "is_relay": s.get("relay", False),
        })

    return rows

# --------------------------------------------------
# MAIN
# --------------------------------------------------

def main():
    print(f"[INFO] Loading recruits from {INPUT_CSV}")
    recruits_df = pd.read_csv(INPUT_CSV)

    session = requests.Session()
    session.headers.update(HEADERS)

    all_rows: List[Dict] = []

    for _, swimmer in recruits_df.iterrows():
        swimmer_id = int(swimmer["swimmer_ID"])
        swimmer_name = swimmer["swimmer_name"]

        print(f"[INFO] Swimmer {swimmer_name} ({swimmer_id})")

        swimmer_row = swimmer.to_dict()

        for event_label, event_code in EVENTS.items():
            print(f"    {event_label} ...", end=" ")

            swims = fetch_times(session, swimmer_id, event_code)

            if not swims:
                print("no data")
                continue

            rows = extract_rows(swims, swimmer_row, event_label)
            all_rows.extend(rows)

            print(f"{len(rows)} swims")

    if not all_rows:
        print("[WARN] No data collected.")
        return

    df = pd.DataFrame(all_rows)

    df = df.sort_values(
        by=["swimmer_ID", "event_label", "time", "date"],
        ascending=[True, True, True, False],
        na_position="last",
    )

    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"[OK] Wrote {len(df)} rows to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
