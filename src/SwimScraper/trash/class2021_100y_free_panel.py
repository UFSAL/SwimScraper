"""
class2021_100y_free_panel_from_csv.py (MEN ONLY, TOP 100)

Like dump_top_recruit_2028_times.py, but:
- Loads recruits_2021.csv
- Takes TOP 100 recruits by HS_power_index (lower = better)
- Pulls 100Y Free swims for each swimmer from 2021–2025
- Aggregates season-best per year, ranks within cohort, and growth metrics
- Exports to SQLite + CSV

REQUIRES recruits_2021.csv with columns:
  swimmer_ID, swimmer_name, HS_power_index
"""

import os
import time
import math
import pandas as pd
from sqlalchemy import create_engine

import SwimScraper as ss  # uses your existing functions

# ----------------------------
# CONFIG
# ----------------------------
RECRUITS_CSV = "recruits_2021.csv"   # <-- you need this file like recruits_2028.csv
GENDER = "M"

TOP_N = 100
YEAR_START = 2021
YEAR_END = 2025

EVENT_DISTANCE = 100
EVENT_COURSE = "Y"
EVENT_STROKE_CODE_FREE = 1  # Free

SLEEP_BETWEEN_SWIMMERS_SEC = 0.25
MAX_RETRIES = 3
RETRY_BACKOFF_SEC = 1.0

SQL_URL = os.getenv("SQL_URL", "sqlite:///swim_progression.db")

WRITE_CSV = True
OUTDIR = "out"
os.makedirs(OUTDIR, exist_ok=True)


# ----------------------------
# HELPERS
# ----------------------------
def gender_code(g: str) -> int:
    return 1 if g == "M" else 2 if g == "F" else 0


def event_token_100y_free(g: str) -> str:
    return f"{gender_code(g)}|{EVENT_DISTANCE}|{EVENT_COURSE}|{EVENT_STROKE_CODE_FREE}"


def safe_fetch_times(swimmer_id: str, gender: str) -> pd.DataFrame:
    token = event_token_100y_free(gender)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            raw = ss.getSwimmerTimesByEventJSON(swimmer_id, token)
            df = ss.swimmer_times_to_dataframe(raw)
            if df is None or df.empty:
                return pd.DataFrame()
            return df
        except Exception as e:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SEC * attempt)
            else:
                print(f"[WARN] Failed times_by_event swimmer={swimmer_id}: {e}")
                return pd.DataFrame()

    return pd.DataFrame()


def infer_season_year(row: dict) -> int | None:
    season_id = row.get("season_id")
    if pd.notna(season_id):
        try:
            return int(ss.getYear(int(season_id)))
        except Exception:
            pass

    for k in ("dateofswim", "date_created", "date", "swim_date"):
        v = row.get(k)
        if v is None or (isinstance(v, float) and math.isnan(v)):
            continue
        dt = pd.to_datetime(v, errors="coerce", utc=True)
        if pd.isna(dt):
            continue
        return int(dt.year)

    return None


def pick_time_display(row: dict) -> str | None:
    for k in ("eventtime", "time", "display_time", "result_time"):
        v = row.get(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return None


def pick_time_seconds(row: dict) -> float | None:
    disp = pick_time_display(row)
    if disp is None:
        return None
    try:
        return ss.convertTime(disp)
    except Exception:
        return None


def sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    used = set()
    new_cols = []
    for i, c in enumerate(df.columns):
        if c == "" or c.lower().startswith("unnamed") or c.lower() == "none":
            c = f"col_{i}"
        base = c
        j = 2
        while c in used:
            c = f"{base}_{j}"
            j += 1
        used.add(c)
        new_cols.append(c)
    df.columns = new_cols
    return df


# ----------------------------
# COHORT LOADING (CSV-BASED, LIKE YOUR 2028 SCRIPT)
# ----------------------------
def load_top_100_recruits(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    required = {"swimmer_ID", "swimmer_name", "HS_power_index"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{path} missing columns: {missing}")

    df["HS_power_index"] = pd.to_numeric(df["HS_power_index"], errors="coerce")
    df = df.dropna(subset=["HS_power_index"])

    # men only (if file includes both). if it doesn't, this is harmless.
    if "gender" in df.columns:
        df = df[df["gender"].astype(str).str.upper() == GENDER]

    df = df.sort_values("HS_power_index").head(TOP_N).copy()

    df = df.rename(columns={"swimmer_ID": "swimmer_id"})
    df["swimmer_id"] = df["swimmer_id"].astype(str)
    df["gender"] = GENDER

    return df.reset_index(drop=True)


# ----------------------------
# PANEL BUILD
# ----------------------------
def build_panel_100y_free(cohort: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for i, r in cohort.iterrows():
        swimmer_id = str(r["swimmer_id"])
        swimmer_name = r.get("swimmer_name")

        df_times = safe_fetch_times(swimmer_id, GENDER)
        time.sleep(SLEEP_BETWEEN_SWIMMERS_SEC)

        if df_times.empty:
            continue

        for _, rec in df_times.iterrows():
            recd = rec.to_dict()
            season_year = infer_season_year(recd)
            if season_year is None or season_year < YEAR_START or season_year > YEAR_END:
                continue

            tsec = pick_time_seconds(recd)
            tdisp = pick_time_display(recd)
            if tsec is None or (isinstance(tsec, float) and math.isnan(tsec)):
                continue

            rows.append({
                "swimmer_id": swimmer_id,
                "swimmer_name": swimmer_name,
                "gender": GENDER,
                "class_year": 2021,
                "hs_power_index": r.get("HS_power_index"),
                "event": "100 Y Free",
                "season_year": int(season_year),
                "time_seconds": float(tsec),
                "time_display": tdisp,
                "meet_name": recd.get("meet_name") or recd.get("name"),
                "dateofswim": recd.get("dateofswim") or recd.get("date_created") or recd.get("date"),
                "place": recd.get("place"),
            })

    return pd.DataFrame(rows)


def season_best_and_ranks(panel: pd.DataFrame) -> pd.DataFrame:
    if panel.empty:
        return pd.DataFrame()

    best = (
        panel.sort_values(["swimmer_id", "season_year", "time_seconds"])
        .groupby(["class_year", "gender", "swimmer_id", "swimmer_name", "hs_power_index", "event", "season_year"], as_index=False)
        .agg(
            best_time_seconds=("time_seconds", "min"),
            best_time_display=("time_display", "first"),
            swims_count=("time_seconds", "count"),
        )
    )

    # rank within the TOP 100 cohort for each year
    best["cohort_rank"] = (
        best.groupby(["class_year", "gender", "event", "season_year"])["best_time_seconds"]
        .rank(method="min", ascending=True)
        .astype(int)
    )

    # Keep exactly 2021-2025 rows only; if missing years, they just won't appear
    best = best.sort_values(["swimmer_id", "season_year"])
    return best


def growth_summary(best: pd.DataFrame) -> pd.DataFrame:
    """
    One row per swimmer summarizing change from first observed year to last observed year.
    """
    if best.empty:
        return pd.DataFrame()

    g = best.sort_values(["swimmer_id", "season_year"]).groupby(["swimmer_id", "swimmer_name"], as_index=False)

    first = g.first()[["swimmer_id", "swimmer_name", "season_year", "best_time_seconds", "cohort_rank"]].rename(
        columns={
            "season_year": "first_year",
            "best_time_seconds": "first_best_time_seconds",
            "cohort_rank": "first_rank",
        }
    )
    last = g.last()[["swimmer_id", "swimmer_name", "season_year", "best_time_seconds", "cohort_rank"]].rename(
        columns={
            "season_year": "last_year",
            "best_time_seconds": "last_best_time_seconds",
            "cohort_rank": "last_rank",
        }
    )

    out = first.merge(last, on=["swimmer_id", "swimmer_name"], how="inner")
    out["time_drop_seconds"] = out["first_best_time_seconds"] - out["last_best_time_seconds"]  # positive = faster
    out["rank_change"] = out["first_rank"] - out["last_rank"]  # positive = improved rank (smaller number)
    return out.sort_values(["rank_change", "time_drop_seconds"], ascending=False)


def export_all(cohort: pd.DataFrame, panel: pd.DataFrame, best: pd.DataFrame, summary: pd.DataFrame):
    cohort = sanitize_columns(cohort)
    panel = sanitize_columns(panel)
    best = sanitize_columns(best)
    summary = sanitize_columns(summary)

    # CSV
    if WRITE_CSV:
        cohort.to_csv(os.path.join(OUTDIR, "recruit_cohort_top100_2021_M.csv"), index=False)
        panel.to_csv(os.path.join(OUTDIR, "swims_raw_100y_free_2021_2025_M.csv"), index=False)
        best.to_csv(os.path.join(OUTDIR, "season_best_100y_free_2021_2025_M.csv"), index=False)
        summary.to_csv(os.path.join(OUTDIR, "growth_summary_100y_free_2021_2025_M.csv"), index=False)

    # SQL
    engine = create_engine(SQL_URL)
    cohort.to_sql("recruit_cohort_top100_2021_M", engine, if_exists="replace", index=False)
    panel.to_sql("swimmer_event_swims_raw_100y_free_2021_2025_M", engine, if_exists="replace", index=False)
    best.to_sql("swimmer_event_season_best_100y_free_2021_2025_M", engine, if_exists="replace", index=False)
    summary.to_sql("swimmer_growth_summary_100y_free_2021_2025_M", engine, if_exists="replace", index=False)

    print(f"[OK] Exported tables to {SQL_URL}")
    print(f"[OK] Also wrote CSVs to ./{OUTDIR}/")


def main():
    print("[1/4] Loading top 100 recruits from CSV ...")
    cohort = load_top_100_recruits(RECRUITS_CSV)
    print(f"      Cohort size: {len(cohort)}")

    print("[2/4] Fetching 100Y Free swims 2021–2025 for cohort ...")
    panel = build_panel_100y_free(cohort)
    print(f"      Raw swims rows: {len(panel)}")

    print("[3/4] Computing season best + cohort ranks ...")
    best = season_best_and_ranks(panel)
    print(f"      Season-best rows: {len(best)}")

    print("[4/4] Building growth summary + exporting ...")
    summary = growth_summary(best)
    export_all(cohort, panel, best, summary)

    if not summary.empty:
        print("\nTop 10 biggest improvers (rank_change, time_drop):")
        print(summary.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
