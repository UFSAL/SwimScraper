# dump_recruits_2028.py
import pandas as pd
import SwimScraper as ss

CLASS_YEAR = 2028
GENDERS = ["M", "F"]
OUT_CSV = "recruits_2028.csv"


def main():
    all_rows = []

    for gender in GENDERS:
        print(f"Fetching HS recruits for class {CLASS_YEAR}, gender {gender}...")
        recs = ss.getHSRecruitRankings(CLASS_YEAR, gender)
        # getHSRecruitRankings already fetches up to 200 (4 pages)
        for r in recs:
            r["gender"] = gender
            r["class_year"] = CLASS_YEAR
        all_rows.extend(recs)
        print(f"  -> {len(recs)} recruits")

    if not all_rows:
        print("No recruits found.")
        return

    df = pd.DataFrame(all_rows)
    # Ensure HS_power_index is numeric for later analysis
    df["HS_power_index"] = pd.to_numeric(df["HS_power_index"], errors="coerce")
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")
    print(f"Wrote {len(df)} rows to {OUT_CSV}")


if __name__ == "__main__":
    main()
