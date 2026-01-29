"""
dump_swimmer_all_times.py

Read swimmer IDs (from rosters.csv) and dump *all* swims for all events
for each swimmer to swimmer_all_times.csv using JSON APIs.
"""

from pathlib import Path
import pandas as pd
import SwimScraper as ss

ROSTERS_CSV = Path(__file__).with_name("rosters.csv")
OUT_CSV = Path(__file__).with_name("swimmer_all_times.csv")


def load_swimmer_ids(path=ROSTERS_CSV):
    df = pd.read_csv(path)
    if "swimmer_ID" not in df.columns:
        raise ValueError(f"rosters.csv missing 'swimmer_ID' column (found: {df.columns})")

    ids = (
        df["swimmer_ID"]
        .dropna()
        .astype(str)
        .unique()
    )
    return ids


def main():
    swimmer_ids = load_swimmer_ids()
    print(f"[dump_swimmer_all_times] Loaded {len(swimmer_ids)} unique swimmers")

    all_dfs = []
    for i, sid in enumerate(swimmer_ids, start=1):
        print(f"  [{i}/{len(swimmer_ids)}] Swimmer {sid}...", end="", flush=True)
        try:
            df = ss.getSwimmerAllTimes(sid)
        except Exception as e:
            print(f" ERROR: {e}")
            continue

        if df.empty:
            print(" no data")
            continue

        all_dfs.append(df)
        print(f" {len(df)} rows")

    if not all_dfs:
        print("[dump_swimmer_all_times] No data collected.")
        return

    out = pd.concat(all_dfs, ignore_index=True)
    out.to_csv(OUT_CSV, index=False, encoding="utf-8")
    print(f"[dump_swimmer_all_times] Wrote {len(out)} rows to {OUT_CSV}")


if __name__ == "__main__":
    main()
