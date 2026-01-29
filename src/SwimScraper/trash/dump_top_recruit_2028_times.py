# dump_top_recruit_2028_times.py
#
# 1. Load recruits_2028.csv (created by dump_recruits_2028.py)
# 2. Find the top recruit (lowest HS_power_index)
# 3. Use SwimScraper's JSON helpers to pull ALL times for that swimmer
# 4. Save to top_recruit_2028_all_times.csv

import pandas as pd
import SwimScraper as ss


RECRUITS_CSV = "recruits_2028.csv"
OUT_CSV = "top_recruit_2028_all_times.csv"


def pick_top_recruit(path: str) -> tuple[str, str]:
    """Return (swimmer_id, swimmer_name) for the best recruit."""
    df = pd.read_csv(path)

    if "HS_power_index" not in df.columns or "swimmer_ID" not in df.columns:
        raise ValueError("recruits_2028.csv must have HS_power_index and swimmer_ID columns")

    # Lower HS_power_index is better
    df["HS_power_index"] = pd.to_numeric(df["HS_power_index"], errors="coerce")
    df = df.dropna(subset=["HS_power_index"])

    if df.empty:
        raise ValueError("No recruits with a valid HS_power_index found.")

    df = df.sort_values("HS_power_index")
    top = df.iloc[0]

    swimmer_id = str(top["swimmer_ID"])
    swimmer_name = str(top["swimmer_name"])

    return swimmer_id, swimmer_name


def main():
    # 1) Find the top recruit from the CSV
    swimmer_id, swimmer_name = pick_top_recruit(RECRUITS_CSV)
    print(f"Top recruit (by HS_power_index): {swimmer_name} (ID {swimmer_id})")

    # 2) Use the JSON helper in SwimScraper to fetch ALL times
    #    This should return a pandas DataFrame (per our earlier implementation)
    try:
        df = ss.getSwimmerAllTimes(swimmer_id)
    except Exception as e:
        print(f"[SwimScraper] Failed to fetch times for swimmer {swimmer_id}: {e}")
        return

    if df is None or df.empty:
        print("No times returned for this swimmer.")
        return

    # 3) Save to CSV
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")
    print(f"Wrote {len(df)} rows to {OUT_CSV}")


if __name__ == "__main__":
    main()
