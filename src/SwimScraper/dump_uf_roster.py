import pandas as pd
import SwimScraper as ss

TEAM_ID = 117
TEAM_NAME = "University of Florida"
YEAR = 2025         
GENDERS = ["M", "F"]
OUT_CSV = "uf_roster_2024.csv"


def main():
    all_rows = []

    for gender in GENDERS:
        print(f"Fetching roster for {TEAM_NAME}, gender {gender}, year {YEAR}...")
        # team_ID is enough; we don't really need teams.csv for this
        roster = ss.getRoster(team=TEAM_NAME, gender=gender, year=YEAR, team_ID=TEAM_ID)
        print(f"  -> {len(roster)} swimmers")
        for r in roster:
            r["gender"] = gender
        all_rows.extend(roster)

    if not all_rows:
        print("No roster data found.")
        return

    df = pd.DataFrame(all_rows)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")
    print(f"Wrote {len(df)} rows to {OUT_CSV}")


if __name__ == "__main__":
    main()
