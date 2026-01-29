"""
dump_swimmer_times_api_bulk.py

Given a CSV of recruits with swimmer_IDs,
pull Swimcloud times via API for each swimmer
and write a combined CSV.

Input: recruits_2021_top100_M.csv
Output: recruits_2021_M_times.csv
"""

import time
from typing import List, Dict

import pandas as pd
import requests

# -------------------------
# CONFIG
# -------------------------

BASE = "https://www.swimcloud.com"

INPUT_CSV = "../../recruits_2021_top100_M.csv"
OUTPUT_CSV = "recruits_2021_M_times.csv"

# event_label -> event_code
EVENTS = {
    "50 Free (Y)": "1|50|Y|1",
    "100 Free (Y)": "1|100|Y|1",
    "200 Free (Y)": "1|200|Y|1",
    "100 Fly (Y)": "3|100|Y|1",
    # add more events here
}

SLEEP_SEC = 0.4
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

# -------------------------
# FETCH
# -------------------------

def fetch_times_json(swimmer_id: int, event_code: str) -> Dict:
    url = f"{BASE}/api/swimmers/{swimmer_id}/times_by_event/"
    params = {"event": event_code}

    r = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

# -------------------------
# EXTRACT
# -------------------------

def extract_rows_from_json(
    api_data: Dict,
    swimmer_row: Dict,
    event_label: str,
) -> List[Dict]:
    rows: List[Dict] = []

    event_name = api_data.get("event", {}).get("name")
    course = api_data.get("event", {}).get("course")

    for r in api_data.get("results", []):
        rows.append({
            "class_year": swimmer_row["class_year"],
            "gender": swimmer_row["gender"],
            "swimmer_ID": swimmer_row["swimmer_ID"],
            "swimmer_name": swimmer_row["swimmer_name"],
            "HS_power_index": swimmer_row["HS_power_index"],
            "event_label": event_label,
            "event_name": event_name,
            "course": course,
            "time": r.get("time"),
            "date": r.get("date"),
            "meet": r.get("meet"),
            "team": r.get("team"),
            "is_relay": r.get("relay", False),
        })

    return rows

# -------------------------
# MAIN
# -------------------------

def main():
    print(f"[INFO] Loading recruits from {INPUT_CSV}")
    recruits_df = pd.read_csv(INPUT_CSV)

    all_rows: List[Dict] = []

    for _, swimmer in recruits_df.iterrows():
        swimmer_id = int(swimmer["swimmer_ID"])
        swimmer_name = swimmer["swimmer_name"]

        print(f"[INFO] Swimmer {swimmer_name} ({swimmer_id})")

        swimmer_row = swimmer.to_dict()

        for event_label, event_code in EVENTS.items():
            try:
                api_data = fetch_times_json(swimmer_id, event_code)
                rows = extract_rows_from_json(api_data, swimmer_row, event_label)
                all_rows.extend(rows)

                print(f"    {event_label}: {len(rows)} rows")
                time.sleep(SLEEP_SEC)

            except Exception as e:
                print(f"    [WARN] Failed {event_label}: {e}")
                continue

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
