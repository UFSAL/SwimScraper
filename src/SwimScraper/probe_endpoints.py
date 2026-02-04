from __future__ import annotations

import requests

BASE = "https://swimcloud.com"
SWIMMER_ID = 404823  # Sam Hoover

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/144.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://swimcloud.com/",
    "X-Requested-With": "XMLHttpRequest",
}

TIMEOUT = (10, 30)

def try_url(session: requests.Session, url: str):
    r = session.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=False)
    ct = r.headers.get("Content-Type", "")
    try:
        j = r.json()
        is_json = True
        j_type = type(j).__name__
    except Exception:
        is_json = False
        j_type = None

    snippet = (r.text or "")[:140].replace("\n", " ").replace("\r", " ")
    print(f"{r.status_code} json={is_json} type={j_type} ct={ct} url={url}")
    if not is_json or r.status_code != 200:
        print(f"    snippet='{snippet}...'")

def main():
    session = requests.Session()

    candidates = [
        f"{BASE}/auth_info/",
        f"{BASE}/swimmer/{SWIMMER_ID}/times/profile_fastest_times/",
        f"{BASE}/swimmer/{SWIMMER_ID}/times/profile_fastest_times/?course=All&season=All&sort=Best",
        f"{BASE}/api/swimmers/{SWIMMER_ID}/profile_fastest_times/",
        f"{BASE}/api/swimmers/{SWIMMER_ID}/times/profile_fastest_times/",
    ]

    print(f"[INFO] Probing endpoints for swimmer_id={SWIMMER_ID} BASE={BASE}\n")
    for url in candidates:
        try_url(session, url)

if __name__ == "__main__":
    main()