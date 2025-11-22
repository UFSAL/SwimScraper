"""
dump_recruiting_data.py

Batch script to build a roster dataset for selected teams and years.

- Reads teams_config.csv
- For each (team_id, team_name, gender) and year in [START_YEAR, END_YEAR],
  calls SwimScraper.getRoster(...)
- Writes:
    - teams.csv   (unique teams from the config)
    - rosters.csv (one row per swimmer-season)
"""

import csv
from pathlib import Path
from typing import List, Dict

import pandas as pd

import SwimScraper as ss

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------

# Where to read the team list from
CONFIG_PATH = Path(__file__).with_name("teams_config.csv")

# Where to write output CSVs
OUTPUT_TEAMS_CSV = Path(__file__).with_name("teams.csv")
OUTPUT_ROSTERS_CSV = Path(__file__).with_name("rosters.csv")

# Year range (inclusive). 2024 ≈ 2024–25 season (season_id=28).
START_YEAR = 2020
END_YEAR = 2024


# ---------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------

def read_team_config(path: Path = CONFIG_PATH) -> List[Dict]:
    """
    Read teams_config.csv and return a list of dicts with keys:
      team_id, team_name, gender
    """
    if not path.exists():
        raise FileNotFoundError(
            f"teams_config.csv not found at {path}. "
            "Create it with columns: team_id,team_name,gender"
        )

    teams = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {"team_id", "team_name", "gender"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(
                f"teams_config.csv is missing required columns: {', '.join(sorted(missing))}"
            )

        for row in reader:
            # Normalize fields
            team_id = row["team_id"].strip()
            team_name = row["team_name"].strip()
            gender = row["gender"].strip().upper()

            if not team_id or not team_name or gender not in {"M", "F"}:
                print(f"[WARN] Skipping invalid config row: {row}")
                continue

            teams.append(
                {
                    "team_id": team_id,
                    "team_name": team_name,
                    "gender": gender,
                }
            )

    if not teams:
        raise ValueError("No valid rows found in teams_config.csv")

    return teams


def write_teams_csv(teams: List[Dict], path: Path = OUTPUT_TEAMS_CSV) -> None:
    """
    Write a simple teams.csv with unique (team_id, team_name).
    """
    uniq = {}
    for t in teams:
        uniq[t["team_id"]] = t["team_name"]

    rows = [{"team_id": k, "team_name": v} for k, v in uniq.items()]
    df = pd.DataFrame(rows).sort_values(["team_name", "team_id"])
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"[dump_recruiting_data] Wrote {len(df)} teams to {path}")


# ---------------------------------------------------------------------
# MAIN ROSTER DUMP LOGIC
# ---------------------------------------------------------------------

def gather_rosters(team_config: List[Dict]) -> pd.DataFrame:
    """
    Loop over team_config and years, call getRoster, and accumulate results
    into a single pandas DataFrame.
    """
    all_rows = []

    for cfg in team_config:
        team_id = cfg["team_id"]
        team_name = cfg["team_name"]
        gender = cfg["gender"]

        for year in range(START_YEAR, END_YEAR + 1):
            print(f"Fetching roster for {team_name} ({gender}), year {year}...")

            try:
                roster = ss.getRoster(
                    team=team_name,
                    gender=gender,
                    year=year,
                    team_ID=team_id,
                )
            except Exception as e:
                print(
                    f"[ERROR] Failed to fetch roster for {team_name} ({gender}), "
                    f"year {year}: {e}"
                )
                continue

            if not roster:
                print(
                    f"[WARN] No roster rows returned for {team_name} ({gender}), "
                    f"year {year}"
                )
                continue

            # Add year info to each row
            for r in roster:
                r = dict(r)  # copy
                r["year"] = year
                all_rows.append(r)

    if not all_rows:
        print("[dump_recruiting_data] WARNING: No roster rows collected.")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    # Optional: sort rows for nicer CSV output
    sort_cols = [c for c in ["team_name", "gender", "year", "swimmer_name"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols)

    return df


def main():
    print(f"[dump_recruiting_data] Reading team config from {CONFIG_PATH}...")
    team_config = read_team_config(CONFIG_PATH)
    print(f"[dump_recruiting_data] Loaded {len(team_config)} team/gender entries.")

    # Write simple teams.csv
    write_teams_csv(team_config, OUTPUT_TEAMS_CSV)

    # Gather rosters
    roster_df = gather_rosters(team_config)

    if roster_df.empty:
        print("[dump_recruiting_data] No roster data to write.")
    else:
        roster_df.to_csv(OUTPUT_ROSTERS_CSV, index=False, encoding="utf-8")
        print(
            f"[dump_recruiting_data] Wrote {len(roster_df)} rows to {OUTPUT_ROSTERS_CSV}"
        )


if __name__ == "__main__":
    main()
