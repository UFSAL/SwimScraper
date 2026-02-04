import os
import re
import time
import random
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

import SwimScraper as sc


# =========================
# CONFIG
# =========================

BASE_DIR = Path(__file__).resolve().parents[3]
CSV_DIR = BASE_DIR / "csv"

TOP100_CSV = CSV_DIR / "recruits_2021_top100_M.csv"   # <-- change if needed
OUT_CSV = CSV_DIR / "rankings_scores_top100_2021.csv"
PLOTS_DIR = Path("rankings_plots_top100_2021")

# Column autodetect (works with most of your files)
CANDIDATE_ID_COLS = ["swimmer_ID", "swimmer_id", "id", "SwimmerID"]
CANDIDATE_NAME_COLS = ["swimmer_name", "name", "SwimmerName"]
CANDIDATE_RANK_COLS = ["rank", "Rank", "class_rank", "recruit_rank"]

# Context filter: set to None to plot ALL contexts separately
# Otherwise you can do: CONTEXT_WHITELIST = ["National"]
CONTEXT_WHITELIST = None

# How many swimmers to run (None = all)
LIMIT_N = None

# polite scraping
SLEEP_MIN = 0.7
SLEEP_MAX = 1.6


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
        # handle strings like "#12"
        s = s.astype(str).str.replace("#", "", regex=False)
        s = pd.to_numeric(s, errors="coerce")
        if s.isna().all():
            return pd.Series(range(1, len(df) + 1), index=df.index)
        # fill missing with row order
        s = s.fillna(pd.Series(range(1, len(df) + 1), index=df.index))
        return s.astype(int)
    return pd.Series(range(1, len(df) + 1), index=df.index)


def parse_start_year(season_label: str) -> int | None:
    m = re.search(r"(\d{4})", str(season_label))
    return int(m.group(1)) if m else None


def main():
    if not Path(TOP100_CSV).exists():
        raise FileNotFoundError(f"Could not find {TOP100_CSV} in current folder.")

    df = pd.read_csv(TOP100_CSV)

    id_col = pick_col(df, CANDIDATE_ID_COLS)
    name_col = pick_col(df, CANDIDATE_NAME_COLS)
    rank_col = pick_col(df, CANDIDATE_RANK_COLS)

    if id_col is None:
        raise ValueError(
            f"Could not find swimmer id column. Looked for: {CANDIDATE_ID_COLS}. "
            f"Columns found: {list(df.columns)}"
        )
    if name_col is None:
        # still works; labels will just be "#rank swimmer_id"
        print("[WARN] Could not find swimmer name column; labels will omit names.")

    df["__rank__"] = normalize_rank_col(df, rank_col)
    df["__id__"] = df[id_col].astype(str)
    df["__name__"] = df[name_col].astype(str) if name_col else ""

    if LIMIT_N:
        df = df.head(LIMIT_N).copy()

    session = sc.build_swimcloud_session_from_env()

    all_rows = []
    failures = []

    for i, row in df.iterrows():
        swimmer_id = row["__id__"]
        swimmer_name = row["__name__"]
        swimmer_rank = int(row["__rank__"])

        label = f"#{swimmer_rank} {swimmer_name}".strip()
        print(f"[{swimmer_rank:>3}] scraping {swimmer_id} {swimmer_name}")

        try:
            s_df = sc.get_swimmer_rankings_scores(swimmer_id, session=session)

            if s_df.empty:
                print(f"   -> no rows returned")
                failures.append((swimmer_id, swimmer_name, "empty"))
                continue

            s_df["recruit_rank_2021"] = swimmer_rank
            s_df["recruit_name"] = swimmer_name
            s_df["recruit_label"] = label

            all_rows.append(s_df)

        except Exception as e:
            print(f"   -> ERROR: {e}")
            failures.append((swimmer_id, swimmer_name, str(e)))

        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

    if not all_rows:
        raise RuntimeError("No data scraped. Check cookies / CSV columns / blocking.")

    out = pd.concat(all_rows, ignore_index=True)
    out["start_year"] = out["season_label"].apply(parse_start_year)

    CSV_DIR.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, index=False)
    print(f"\nWrote: {OUT_CSV}  rows={len(out):,}")

    if failures:
        failures_csv = CSV_DIR / "rankings_scores_failures.csv"
        pd.DataFrame(failures, columns=["swimmer_id", "name", "error"]).to_csv(
            failures_csv, index=False
        )
        print(f"Wrote: {failures_csv}  count={len(failures)}")

    # =========================
    # PLOTS
    # =========================
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    plot_df = out.dropna(subset=["start_year", "score"]).copy()
    plot_df["score"] = pd.to_numeric(plot_df["score"], errors="coerce")
    plot_df = plot_df.dropna(subset=["score"])

    if CONTEXT_WHITELIST:
        plot_df = plot_df[plot_df["context"].isin(CONTEXT_WHITELIST)]

    # One plot per context
    for context, g in plot_df.groupby("context"):
        plt.figure(figsize=(11, 7))

        # one line per swimmer
        for swimmer_id, sg in g.groupby("swimmer_id"):
            sg = sg.sort_values("start_year")
            label = sg["recruit_label"].iloc[0] if "recruit_label" in sg.columns else swimmer_id
            plt.plot(sg["start_year"], sg["score"], marker="o", linewidth=1.2, label=label)

        plt.title(f"SwimCloud Score Trend (Class of 2021 Top 100) — {context}")
        plt.xlabel("Season start year")
        plt.ylabel("Score")
        plt.grid(True, alpha=0.3)

        # legend can get huge; put outside and shrink font
        plt.legend(loc="center left", bbox_to_anchor=(1.02, 0.5), fontsize=7, ncol=1)
        plt.tight_layout()

        safe = re.sub(r"[^a-zA-Z0-9]+", "_", context).strip("_")
        outpath = PLOTS_DIR / f"scores_trend_{safe}.png"
        plt.savefig(outpath, dpi=200)
        plt.close()
        print(f"Saved plot: {outpath}")

    print("\nDone.")


if __name__ == "__main__":
    main()
