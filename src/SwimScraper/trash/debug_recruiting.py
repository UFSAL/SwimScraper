# debug_recruiting_page_save.py
#
# Downloads ONE recruiting rankings page and saves the raw HTML so we can inspect
# whether the data is in __NEXT_DATA__ or loaded by JS/API.

from pathlib import Path
import requests

URL = "https://www.swimcloud.com/recruiting/rankings/2021/M/?page=1"
OUTFILE = Path(__file__).with_name("debug_page_1.html")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.6367.60 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.swimcloud.com/",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

def main():
    print(f"[INFO] Fetching {URL}")
    r = requests.get(URL, headers=HEADERS, timeout=(10, 30))
    print(f"[INFO] status={r.status_code} bytes={len(r.text)} final_url={r.url}")

    OUTFILE.write_text(r.text, encoding="utf-8")
    print(f"[OK] Saved HTML to: {OUTFILE}")

    has_next = "__NEXT_DATA__" in r.text
    print(f"[CHECK] __NEXT_DATA__ present? {has_next}")

    # Extra quick checks:
    print(f"[CHECK] contains '/swimmer/' links? {'/swimmer/' in r.text}")
    print(f"[CHECK] contains 'Power Index'? {'Power Index' in r.text}")

if __name__ == "__main__":
    main()
