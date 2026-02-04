from pathlib import Path

import SwimScraper as sc

BASE_DIR = Path(__file__).resolve().parents[2]
CSV_DIR = BASE_DIR / "csv"

session = sc.build_swimcloud_session_from_env()
df = sc.get_swimmer_rankings_scores("1283295", session=session)  # Leon example
print(df[["season_label","season_id","context","score"]].head(20))
CSV_DIR.mkdir(parents=True, exist_ok=True)
out_csv = CSV_DIR / "rankings_scores_1283295.csv"
df.to_csv(out_csv, index=False)
print(f"Wrote {out_csv}")
