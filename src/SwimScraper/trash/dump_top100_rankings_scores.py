from pathlib import Path

import pandas as pd
from SwimScraper import build_swimcloud_session_from_env, get_swimmer_rankings_scores

BASE_DIR = Path(__file__).resolve().parents[3]
CSV_DIR = BASE_DIR / "csv"

TOP100_CSV = CSV_DIR / "recruits_2021_top100_M.csv"
OUT_CSV = CSV_DIR / "recruits_2021_M_rankings_scores.csv"

def main():
    df = pd.read_csv(TOP100_CSV)
    # adjust if your column name differs
    swimmer_ids = df["swimmer_ID"].astype(str).tolist()

    session = build_swimcloud_session_from_env()

    all_rows = []
    for i, sid in enumerate(swimmer_ids, 1):
        try:
            s_df = get_swimmer_rankings_scores(sid, session=session)
            if not s_df.empty:
                all_rows.append(s_df)
            print(f"[{i}/{len(swimmer_ids)}] ok swimmer {sid} rows={len(s_df)}")
        except Exception as e:
            print(f"[{i}/{len(swimmer_ids)}] FAIL swimmer {sid}: {e}")

    if all_rows:
        out = pd.concat(all_rows, ignore_index=True)
    else:
        out = pd.DataFrame()

    CSV_DIR.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, index=False)
    print(f"Wrote {OUT_CSV} rows={len(out)}")

if __name__ == "__main__":
    main()
