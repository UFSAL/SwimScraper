import pandas as pd
import matplotlib.pyplot as plt
import SwimScraper as sc

session = sc.build_swimcloud_session_from_env()
df = sc.get_swimmer_rankings_scores("1283295", session=session)

# pick ONE context to plot (usually National)
ctx = "National"
d = df[df["context"].str.contains(ctx, case=False, na=False)].copy()

# convert season_label like "2025-2026" -> start_year 2025
d["start_year"] = d["season_label"].str.extract(r"(\d{4})").astype(int)
d = d.sort_values("start_year")

plt.figure()
plt.plot(d["start_year"], d["score"], marker="o")
plt.title(f"SwimCloud Score Trend ({ctx}) - swimmer 1283295")
plt.xlabel("Season start year")
plt.ylabel("Score")
plt.grid(True, alpha=0.3)
plt.show()