import SwimScraper as ss

swimmer_id = 2361690  # from your screenshot

print("=== Fastest times JSON ===")
fast = ss.getSwimmerProfileFastestTimes(swimmer_id)

print("type(fast):", type(fast))
print("num records:", len(fast))

# Show the first record to inspect the fields
if fast:
    print("First record:", fast[0])

# Turn fastest-times list into a DataFrame
import pandas as pd
fast_df = pd.DataFrame(fast)
print("\nFastest-times DataFrame head:")
print(fast_df.head())


print("\n=== Times for one event (JSON) ===")
event_token = "1|50|Y|1"  # adjust if needed
times_json = ss.getSwimmerTimesByEventJSON(swimmer_id, event_token)

print("type(times_json):", type(times_json))
print("num records:", len(times_json))

if times_json:
    print("First record:", times_json[0])

# Try DataFrame flatten
try:
    df = ss.swimmer_times_to_dataframe(times_json)
    print("\nTimes DataFrame head:")
    print(df.head())
except Exception as e:
    print("\nCould not flatten JSON automatically:", e)

