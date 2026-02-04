# dump_top100_recruits_2021.py
#
# Pull top 100 recruits from Swimcloud recruiting rankings (2021 Men)
# by scraping the HTML table (no __NEXT_DATA__, no API needed).
#
# Output: recruits_2021_top100_M.csv

import re
import time
from typing import List, Dict, Optional
from pathlib import Path

import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parents[3]
CSV_DIR = BASE_DIR / "csv"
BASE = "https://www.swimcloud.com"
OUT_CSV = CSV_DIR / "recruits_2021_top100_M.csv"

CLASS_YEAR = 2021
GENDER = "M"
TARGET_N = 100

SLEEP_SEC = 0.4
TIMEOUT = (10, 30)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.swimcloud.com/",
}

def fetch_html(page: int) -> str:
    url = f"{BASE}/recruiting/rankings/{CLASS_YEAR}/{GENDER}/"
    if page > 1:
        url += f"?page={page}"
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text

def extract_rows_from_html(html: str) -> List[Dict]:
    """
    Extract recruits from the recruiting rankings table HTML.

    We rely on patterns that are clearly present in your saved HTML:
      - swimmer link: <a href="/swimmer/404823">
      - name: <h2 ...>Sam Hoover</h2>
      - power index: <td ...>1.00</td> in the last column
    """
    rows: List[Dict] = []

    # Split into table rows
    # This is simple + robust enough for this page structure.
    tr_blocks = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.S | re.I)

    for tr in tr_blocks:
        m_id = re.search(r'href="/swimmer/(\d+)"', tr, flags=re.I)
        if not m_id:
            continue

        swimmer_id = m_id.group(1)

        # Name is in the <h2 ...>NAME</h2>
        m_name = re.search(r"<h2[^>]*>(.*?)</h2>", tr, flags=re.S | re.I)
        swimmer_name = None
        if m_name:
            # remove HTML entities / tags inside name if any
            swimmer_name = re.sub(r"<.*?>", "", m_name.group(1)).strip()
            swimmer_name = swimmer_name.replace("&#x27;", "'")

        # Power index is last numeric-looking td in the row (works well here)
        tds = re.findall(r"<td[^>]*>(.*?)</td>", tr, flags=re.S | re.I)
        power_index: Optional[float] = None
        if tds:
            # try from end backwards
            for td in reversed(tds):
                txt = re.sub(r"<.*?>", "", td).strip()
                txt = txt.replace("&nbsp;", " ").strip()
                # power index looks like 1.00, 1.21, 6.54 etc
                if re.fullmatch(r"\d+(\.\d+)?", txt):
                    try:
                        power_index = float(txt)
                        break
                    except:
                        pass

        rows.append({
            "class_year": CLASS_YEAR,
            "gender": GENDER,
            "swimmer_ID": swimmer_id,
            "swimmer_name": swimmer_name,
            "HS_power_index": power_index,
        })

    return rows

def main():
    all_rows: List[Dict] = []
    seen = set()

    page = 1
    while len(all_rows) < TARGET_N:
        print(f"[INFO] Fetch page {page} ...")
        html = fetch_html(page)

        page_rows = extract_rows_from_html(html)
        print(f"[INFO] extracted {len(page_rows)} candidates on page {page}")

        # Keep order, dedupe by swimmer_ID
        for r in page_rows:
            sid = r["swimmer_ID"]
            if sid in seen:
                continue
            seen.add(sid)
            all_rows.append(r)
            if len(all_rows) >= TARGET_N:
                break

        page += 1
        time.sleep(SLEEP_SEC)

        # Safety stop (should never hit)
        if page > 20:
            print("[WARN] Hit page limit while trying to collect top 100.")
            break

    df = pd.DataFrame(all_rows).head(TARGET_N)
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")
    print(f"[OK] Wrote {len(df)} recruits to {OUT_CSV}")

if __name__ == "__main__":
    main()
