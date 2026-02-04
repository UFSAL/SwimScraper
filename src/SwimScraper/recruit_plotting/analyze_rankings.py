import re
import time
import random
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import SwimScraper as sc


# =========================
# PATHS / CONFIG (robust)
# =========================

def find_project_root(start: Path) -> Path:
    """
    Walk upward from `start` until we find a folder containing `csv/`.
    Falls back to `start` if not found.
    """
    start = start.resolve()
    for p in [start, *start.parents]:
        if (p / "csv").is_dir():
            return p
    return start


SCRIPT_DIR = Path(__file__).resolve().parent
BASE_DIR = find_project_root(SCRIPT_DIR)
CSV_DIR = BASE_DIR / "csv"

# Inputs/outputs in <project_root>/csv
TOP100_CSV = CSV_DIR / "recruits_2021_top100_M.csv"
OUT_RAW_CSV = CSV_DIR / "rankings_scores_top100_2021.csv"
OUT_PIECEWISE_CSV = CSV_DIR / "rankings_scores_top100_2021_piecewise.csv"
OUT_SUMMARY_CSV = CSV_DIR / "rankings_scores_top100_2021_summary.csv"
FAILURES_CSV = CSV_DIR / "rankings_scores_failures.csv"

# Save plots to: <project_root>/top100_plot_2017-college
PLOTS_DIR = BASE_DIR / "top100_plot_2017-college"

# Columns autodetect
CANDIDATE_ID_COLS = ["swimmer_ID", "swimmer_id", "id", "SwimmerID"]
CANDIDATE_NAME_COLS = ["swimmer_name", "name", "SwimmerName"]
CANDIDATE_RANK_COLS = ["rank", "Rank", "class_rank", "recruit_rank"]

# Contexts for the piecewise story
HS_CONTEXT = "National"
NCAA_CONTEXT = "USA College"

# Track HS starting at 2017
HS_MIN_YEAR = 2017

# polite scraping
SLEEP_MIN = 0.7
SLEEP_MAX = 1.6

# How many swimmers to run (None = all)
LIMIT_N = None

# =========================
# NEW FILTER RULE (your request)
# =========================
REQUIRED_YEARS = list(range(2017, 2025 + 1))  # must have at least one score per year for every year in this list
REQUIRE_NCAA = True  # must have at least one USA College row (drop swimmers who never swam NCAA)


# =========================
# HELPERS
# =========================

def pick_col(df: pd.DataFrame, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def normalize_rank_col(df: pd.DataFrame, rank_col: str) -> pd.Series:
    """
    Ensures rank is numeric-ish (1..100). If missing, generate 1..N by row order.
    """
    if rank_col and rank_col in df.columns:
        s = df[rank_col].copy()
        s = s.astype(str).str.replace("#", "", regex=False)
        s = pd.to_numeric(s, errors="coerce")
        if s.isna().all():
            return pd.Series(range(1, len(df) + 1), index=df.index)
        s = s.fillna(pd.Series(range(1, len(df) + 1), index=df.index))
        return s.astype(int)
    return pd.Series(range(1, len(df) + 1), index=df.index)


def parse_start_year(season_label: str) -> int | None:
    m = re.search(r"(\d{4})", str(season_label))
    return int(m.group(1)) if m else None


def safe_filename(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "_", str(s)).strip("_")


def fit_slope(x_years: np.ndarray, y_scores: np.ndarray) -> float | None:
    if len(x_years) < 2:
        return None
    if np.unique(x_years).size < 2:
        return None
    return float(np.polyfit(x_years, y_scores, 1)[0])


def summarize_phase(g: pd.DataFrame) -> dict:
    g = g.sort_values("start_year")
    if g.empty:
        return {
            "n": 0,
            "first_year": None,
            "last_year": None,
            "first_score": None,
            "last_score": None,
            "change": None,
            "slope": None,
            "mean": None,
            "std": None,
        }

    x = g["start_year"].to_numpy()
    y = g["score"].to_numpy()

    first_score = float(y[0])
    last_score = float(y[-1])

    return {
        "n": int(len(g)),
        "first_year": int(x[0]),
        "last_year": int(x[-1]),
        "first_score": first_score,
        "last_score": last_score,
        "change": float(last_score - first_score) if len(y) >= 2 else None,
        "slope": fit_slope(x, y),
        "mean": float(np.mean(y)),
        "std": float(np.std(y)),
    }


def classify(hs_slope, ncaa_slope, hs_change, ncaa_change):
    """
    Simple, interpretable buckets:
      - sustained improver: hs up + ncaa up
      - hs improver -> ncaa drop
      - late developer: hs flat/down but ncaa up
      - decliner: both down
      - unknown: insufficient data
    """
    def sign(v):
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return None
        if v > 0:
            return 1
        if v < 0:
            return -1
        return 0

    hs = sign(hs_slope if hs_slope is not None else hs_change)
    nc = sign(ncaa_slope if ncaa_slope is not None else ncaa_change)

    if hs is None or nc is None:
        return "insufficient_data"

    if hs >= 0 and nc > 0:
        return "sustained_improver"
    if hs > 0 and nc < 0:
        return "hs_improver_ncaa_drop"
    if hs <= 0 and nc > 0:
        return "late_developer"
    if hs < 0 and nc <= 0:
        return "decliner"
    return "mixed"


def has_all_required_years(g: pd.DataFrame, required_years: list[int]) -> bool:
    years = set(pd.to_numeric(g["start_year"], errors="coerce").dropna().astype(int).unique().tolist())
    return all(y in years for y in required_years)


def has_any_ncaa(g: pd.DataFrame) -> bool:
    return g["context"].astype(str).str.contains(NCAA_CONTEXT, na=False).any()


def build_yearly_series(g: pd.DataFrame, years: list[int]) -> pd.DataFrame:
    """
    Create one point per swimmer per year (clean line).
    Preference per year:
      1) NCAA context if present
      2) HS context if present
      3) otherwise max score available that year
    """
    rows = []
    g = g.copy()
    g["context"] = g["context"].astype(str)

    for y in years:
        gy = g[g["start_year"] == y].copy()
        if gy.empty:
            continue

        nc = gy[gy["context"].str.contains(NCAA_CONTEXT, na=False)]
        hs = gy[gy["context"].str.contains(HS_CONTEXT, na=False)]

        if not nc.empty:
            pick = nc.loc[nc["score"].idxmax()]
        elif not hs.empty:
            pick = hs.loc[hs["score"].idxmax()]
        else:
            pick = gy.loc[gy["score"].idxmax()]

        rows.append({
            "swimmer_id": pick["swimmer_id"],
            "start_year": int(y),
            "score": float(pick["score"]),
            "context": pick["context"],
            "recruit_rank_2021": pick.get("recruit_rank_2021", None),
            "recruit_name": pick.get("recruit_name", ""),
            "recruit_label": pick.get("recruit_label", pick["swimmer_id"]),
        })

    return pd.DataFrame(rows)


# =========================
# MAIN
# =========================

def main():
    print("BASE_DIR =", BASE_DIR)
    print("CSV_DIR  =", CSV_DIR)
    print("TOP100_CSV exists?", TOP100_CSV.exists(), TOP100_CSV)

    # =========================
    # 1) LOAD TOP100
    # =========================
    if not TOP100_CSV.exists():
        raise FileNotFoundError(f"Could not find {TOP100_CSV}")

    recruits = pd.read_csv(TOP100_CSV)

    id_col = pick_col(recruits, CANDIDATE_ID_COLS)
    name_col = pick_col(recruits, CANDIDATE_NAME_COLS)
    rank_col = pick_col(recruits, CANDIDATE_RANK_COLS)

    if id_col is None:
        raise ValueError(
            f"Could not find swimmer id column. Looked for: {CANDIDATE_ID_COLS}. "
            f"Columns found: {list(recruits.columns)}"
        )

    recruits["__rank__"] = normalize_rank_col(recruits, rank_col)
    recruits["__id__"] = recruits[id_col].astype(str)
    recruits["__name__"] = recruits[name_col].astype(str) if name_col else ""

    if LIMIT_N:
        recruits = recruits.head(LIMIT_N).copy()

    # =========================
    # 2) SCRAPE RANKINGS (raw)
    # =========================
    session = sc.build_swimcloud_session_from_env()
    all_rows = []
    failures = []

    for _, row in recruits.iterrows():
        swimmer_id = row["__id__"]
        swimmer_name = row["__name__"]
        swimmer_rank = int(row["__rank__"])
        label = f"#{swimmer_rank} {swimmer_name}".strip()

        print(f"[{swimmer_rank:>3}] scraping {swimmer_id} {swimmer_name}")

        try:
            s_df = sc.get_swimmer_rankings_scores(swimmer_id, session=session)

            if s_df.empty:
                failures.append((swimmer_id, swimmer_name, "empty"))
                continue

            s_df["recruit_rank_2021"] = swimmer_rank
            s_df["recruit_name"] = swimmer_name
            s_df["recruit_label"] = label

            all_rows.append(s_df)

        except Exception as e:
            failures.append((swimmer_id, swimmer_name, str(e)))

        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

    if not all_rows:
        raise RuntimeError("No data scraped. Check cookies / blocking.")

    raw = pd.concat(all_rows, ignore_index=True)
    raw["start_year"] = raw["season_label"].apply(parse_start_year)
    raw["score"] = pd.to_numeric(raw["score"], errors="coerce")
    raw = raw.dropna(subset=["start_year", "score"])

    # Make sure these exist / are strings
    raw["swimmer_id"] = raw["swimmer_id"].astype(str)
    raw["context"] = raw["context"].astype(str)

    CSV_DIR.mkdir(parents=True, exist_ok=True)
    raw.to_csv(OUT_RAW_CSV, index=False)
    print(f"\nWrote raw: {OUT_RAW_CSV} rows={len(raw):,}")

    if failures:
        pd.DataFrame(failures, columns=["swimmer_id", "name", "error"]).to_csv(
            FAILURES_CSV, index=False
        )
        print(f"Wrote failures: {FAILURES_CSV} count={len(failures)}")

    # =========================
    # 2.5) FILTER TO "COMPLETE" SWIMMERS (2017-2025 + NCAA)
    # =========================
    keep_ids = []
    for swimmer_id, g in raw.groupby("swimmer_id"):
        if not has_all_required_years(g, REQUIRED_YEARS):
            continue
        if REQUIRE_NCAA and not has_any_ncaa(g):
            continue
        keep_ids.append(swimmer_id)

    raw_complete = raw[raw["swimmer_id"].isin(keep_ids)].copy()
    print(f"\nKeeping swimmers with full coverage {REQUIRED_YEARS[0]}-{REQUIRED_YEARS[-1]} "
          f"and NCAA={REQUIRE_NCAA}: {len(keep_ids)} swimmers "
          f"(rows={len(raw_complete):,})")

    # =========================
    # 3) BUILD PIECEWISE TRACK (for summaries, optional)
    # =========================
    piece_rows = []
    summary_rows = []

    for swimmer_id, g in raw_complete.groupby("swimmer_id"):
        g = g.copy()

        # detect NCAA entry year: first year with USA College context
        ncaa_g = g[g["context"].str.contains(NCAA_CONTEXT, na=False)]
        ncaa_start_year = int(ncaa_g["start_year"].min()) if not ncaa_g.empty else None

        # HS phase: National only, from 2017 up to before ncaa_start_year (if exists)
        hs = g[g["context"].str.contains(HS_CONTEXT, na=False)].copy()
        hs = hs[hs["start_year"] >= HS_MIN_YEAR]
        if ncaa_start_year is not None:
            hs = hs[hs["start_year"] < ncaa_start_year]

        # NCAA phase: USA College only, from ncaa_start_year onward
        ncaa = g[g["context"].str.contains(NCAA_CONTEXT, na=False)].copy()
        if ncaa_start_year is not None:
            ncaa = ncaa[ncaa["start_year"] >= ncaa_start_year]

        # tag phase for plotting
        if not hs.empty:
            hs["phase"] = "HS_National"
            piece_rows.append(hs)
        if not ncaa.empty:
            ncaa["phase"] = "NCAA_USA_College"
            piece_rows.append(ncaa)

        base = g.iloc[0]
        hs_feat = summarize_phase(hs)
        ncaa_feat = summarize_phase(ncaa)

        summary_rows.append({
            "swimmer_id": swimmer_id,
            "recruit_rank_2021": base.get("recruit_rank_2021", None),
            "recruit_name": base.get("recruit_name", ""),
            "recruit_label": base.get("recruit_label", swimmer_id),
            "ncaa_start_year": ncaa_start_year,
            **{f"hs_{k}": v for k, v in hs_feat.items()},
            **{f"ncaa_{k}": v for k, v in ncaa_feat.items()},
            "classification": classify(
                hs_feat["slope"], ncaa_feat["slope"], hs_feat["change"], ncaa_feat["change"]
            ),
        })

    piece = pd.concat(piece_rows, ignore_index=True) if piece_rows else pd.DataFrame()
    piece.to_csv(OUT_PIECEWISE_CSV, index=False)
    print(f"Wrote piecewise: {OUT_PIECEWISE_CSV} rows={len(piece):,}")

    summary = pd.DataFrame(summary_rows)
    summary.to_csv(OUT_SUMMARY_CSV, index=False)
    print(f"Wrote summary: {OUT_SUMMARY_CSV} rows={len(summary):,}")

    # =========================
    # 4) PLOTS
    # =========================
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    # NEW Plot: ONLY complete swimmers, one continuous line per swimmer across 2017-2025
    if not raw_complete.empty:
        yearly_rows = []
        for swimmer_id, g in raw_complete.groupby("swimmer_id"):
            ys = build_yearly_series(g, REQUIRED_YEARS)
            if not ys.empty:
                yearly_rows.append(ys)

        yearly = pd.concat(yearly_rows, ignore_index=True) if yearly_rows else pd.DataFrame()

        # save legend table (easy lookup)
        legend_df = (
            yearly[["swimmer_id", "recruit_rank_2021", "recruit_name", "recruit_label"]]
            .drop_duplicates()
            .sort_values(["recruit_rank_2021", "recruit_name"], na_position="last")
        )
        legend_csv = CSV_DIR / "complete_swimmers_2017_2025_legend.csv"
        legend_df.to_csv(legend_csv, index=False)
        print(f"Saved legend table: {legend_csv} rows={len(legend_df):,}")

        # plot
        plt.figure(figsize=(16, 8))
        handles = []
        labels = []

        for swimmer_id, g in yearly.groupby("swimmer_id"):
            g = g.sort_values("start_year")
            label = str(g["recruit_label"].iloc[0])

            (ln,) = plt.plot(
                g["start_year"],
                g["score"],
                marker="o",
                linewidth=1.2,
                alpha=0.85,
                label=label,
            )
            handles.append(ln)
            labels.append(label)

        plt.title("Top 100 (Class of 2021): ONLY swimmers with complete data 2017–2025 (continuous line)")
        plt.xlabel("Season start year")
        plt.ylabel("Score")
        plt.xticks(REQUIRED_YEARS)
        plt.grid(True, alpha=0.25)

        # Big legend outside the plot
        # If you have many swimmers even after filtering, this keeps it readable.
        plt.legend(
            handles=handles,
            labels=labels,
            bbox_to_anchor=(1.02, 1),
            loc="upper left",
            borderaxespad=0.0,
            fontsize=7,
            ncol=1,
            title="Swimmers",
        )

        outpath = PLOTS_DIR / "complete_swimmers_2017_2025_with_legend.png"
        plt.tight_layout(rect=[0, 0, 0.78, 1])  # leave space on right for legend
        plt.savefig(outpath, dpi=200)
        plt.close()
        print(f"Saved: {outpath}")

    else:
        print("No swimmers matched the strict 2017–2025 + NCAA filter. "
              "Loosen REQUIRED_YEARS or REQUIRE_NCAA if needed.")

    print("\nDone.")


if __name__ == "__main__":
    main()