"""
dump_best_event_progressions.py

Reads your Top-100 recruits CSV (with swimmer ids/urls),
finds each swimmer's BEST events (from Personal Bests sorted by "Best"),
then downloads the Event Progression (times history) for those best events.

Outputs:
  - best_event_progressions.csv   (all event-history rows)
  - best_events.csv               (the chosen "best events" per swimmer)

Why you were getting HTTP 404:
Swimcloud often returns 404 to these XHR endpoints unless you have a valid
logged-in browser session (cookies + csrf). DevTools works because your browser
is authenticated; Python requests isn't.

Solution:
This script uses Playwright to:
  - (first run) open a real browser so you can log in
  - save cookies/session to swimcloud_state.json
  - reuse that session to call the same XHR JSON endpoints reliably.

Install once:
  pip install pandas requests playwright
  python -m playwright install

Run:
  python dump_best_event_progressions.py
"""

from __future__ import annotations

import re
import time
import random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from pathlib import Path

import pandas as pd

# Playwright is required for authenticated XHR (fixes the 404s)
from playwright.sync_api import sync_playwright


# =========================
# CONFIG
# =========================

BASE_DIR = Path(__file__).resolve().parents[3]
CSV_DIR = BASE_DIR / "csv"

TOP100_CSV = CSV_DIR / "recruits_2021_top100_M.csv"   # <-- set to your csv filename
OUT_PROGRESS_CSV = CSV_DIR / "best_event_progressions.csv"
OUT_BESTEVENTS_CSV = CSV_DIR / "best_events.csv"

BEST_EVENTS_PER_SWIMMER = 1  # set to 3 if you want top 3 best events per swimmer

STATE_FILE = "swimcloud_state.json"  # saved login cookies/session
BASE = "https://www.swimcloud.com"

SLEEP_BETWEEN_REQUESTS_SEC = (0.30, 0.75)
RETRIES = 3
TIMEOUT_MS = 30_000

POSSIBLE_SWIMMER_ID_COLS = ["swimmer_id", "swimmerid", "id"]
POSSIBLE_SWIMMER_URL_COLS = ["swimmer_url", "swimmerurl", "profile_url", "url"]
POSSIBLE_NAME_COLS = ["name", "swimmer_name", "swimmername"]


# =========================
# Data helpers
# =========================

def sleep_a_bit():
    lo, hi = SLEEP_BETWEEN_REQUESTS_SEC
    time.sleep(random.uniform(lo, hi))


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def extract_swimmer_id(row: pd.Series) -> Optional[int]:
    # 1) direct id columns
    for col in POSSIBLE_SWIMMER_ID_COLS:
        if col in row and pd.notna(row[col]):
            try:
                return int(str(row[col]).strip())
            except Exception:
                pass

    # 2) parse from url columns: .../swimmer/404823/...
    for col in POSSIBLE_SWIMMER_URL_COLS:
        if col in row and pd.notna(row[col]):
            m = re.search(r"/swimmer/(\d+)", str(row[col]))
            if m:
                return int(m.group(1))

    # 3) scan all values
    for v in row.values:
        if pd.isna(v):
            continue
        m = re.search(r"/swimmer/(\d+)", str(v))
        if m:
            return int(m.group(1))

    return None


def extract_name(row: pd.Series) -> str:
    for col in POSSIBLE_NAME_COLS:
        if col in row and pd.notna(row[col]):
            return str(row[col]).strip()
    return ""


@dataclass
class BestEvent:
    swimmer_id: int
    swimmer_name: str
    event_key: str
    event_label: str
    best_time: str
    best_date: str


def parse_fastest_times_payload(payload: Any) -> List[Dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, dict):
        for k in ("results", "data", "items"):
            if k in payload and isinstance(payload[k], list):
                return payload[k]
        for v in payload.values():
            if isinstance(v, dict):
                for k in ("results", "data", "items"):
                    if k in v and isinstance(v[k], list):
                        return v[k]
    if isinstance(payload, list):
        return payload
    return []


def parse_times_by_event_payload(payload: Any) -> List[Dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, dict):
        for k in ("results", "data", "items", "times"):
            if k in payload and isinstance(payload[k], list):
                return payload[k]
        for v in payload.values():
            if isinstance(v, dict):
                for k in ("results", "data", "items", "times"):
                    if k in v and isinstance(v[k], list):
                        return v[k]
    if isinstance(payload, list):
        return payload
    return []


def pick_best_events(swimmer_id: int, swimmer_name: str, fastest_rows: List[Dict[str, Any]], n: int) -> List[BestEvent]:
    best: List[BestEvent] = []

    for row in fastest_rows:
        # event_key candidates
        event_key = (
            row.get("event_key")
            or row.get("eventKey")
            or row.get("event_value")
            or row.get("value")
        )

        # event sometimes is directly the key (string)
        if not event_key and isinstance(row.get("event"), str):
            event_key = row.get("event")

        # event sometimes is object {value,label}
        if not event_key and isinstance(row.get("event"), dict):
            ev = row["event"]
            event_key = ev.get("value") or ev.get("key")

        event_label = (
            row.get("event_name")
            or row.get("eventName")
            or row.get("label")
            or row.get("event_label")
            or row.get("eventLabel")
            or row.get("event_display")
            or row.get("eventDisplay")
            or ""
        )
        if not event_label and isinstance(row.get("event"), dict):
            event_label = row["event"].get("label") or ""

        best_time = str(row.get("time") or row.get("best_time") or row.get("bestTime") or "").strip()
        best_date = str(row.get("date") or row.get("best_date") or row.get("bestDate") or "").strip()

        if event_key:
            best.append(BestEvent(
                swimmer_id=swimmer_id,
                swimmer_name=swimmer_name,
                event_key=str(event_key),
                event_label=str(event_label),
                best_time=best_time,
                best_date=best_date,
            ))

        if len(best) >= n:
            break

    return best


# =========================
# Swimcloud URLs
# =========================

def swimmer_times_page(swimmer_id: int) -> str:
    return f"{BASE}/swimmer/{swimmer_id}/times/"


def fastest_times_url(swimmer_id: int) -> str:
    return f"{BASE}/swimmer/{swimmer_id}/times/profile_fastest_times/"


def times_by_event_url(swimmer_id: int, event_key: str) -> str:
    # event key must be URL-encoded exactly like Swimcloud expects (e.g. 1|7 -> 1%7C7)
    # We'll let the request layer encode it by building the full URL ourselves:
    from urllib.parse import quote
    return f"{BASE}/swimmer/{swimmer_id}/times/times_by_event/?event={quote(str(event_key), safe='')}"


# =========================
# Playwright XHR fetcher
# =========================

def ensure_logged_in_state(pw) -> str:
    """
    If swimcloud_state.json doesn't exist or you want to refresh it:
      - opens a visible browser
      - you log in
      - press Enter in terminal
      - saves state file
    """
    import os

    if os.path.exists(STATE_FILE):
        return STATE_FILE

    print("\n[SETUP] No swimcloud_state.json found.")
    print("[SETUP] A browser will open. Log into Swimcloud in that window.")
    print("[SETUP] After you're logged in, come back here and press Enter.\n")

    browser = pw.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    page.goto(BASE, wait_until="domcontentloaded", timeout=TIMEOUT_MS)

    input("Press Enter AFTER you have logged in to Swimcloud in the opened browser... ")

    context.storage_state(path=STATE_FILE)
    browser.close()

    print(f"[SETUP] Saved authenticated session to {STATE_FILE}\n")
    return STATE_FILE


def xhr_json_with_retries(context, url: str, referer: str) -> Any:
    """
    Use the authenticated browser context to call Swimcloud XHR endpoints.
    """
    last_err: Optional[Exception] = None

    headers = {
        "Accept": "application/json, text/plain, */*",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": referer,
    }

    for attempt in range(1, RETRIES + 1):
        try:
            resp = context.request.get(url, headers=headers, timeout=TIMEOUT_MS)
            status = resp.status
            if status == 200:
                return resp.json()

            # 304 sometimes happens; try reading body as json anyway (may fail)
            if status == 304:
                try:
                    return resp.json()
                except Exception:
                    raise RuntimeError(f"HTTP 304 but non-JSON for {url}")

            raise RuntimeError(f"HTTP {status} for {url}")

        except Exception as e:
            last_err = e
            if attempt < RETRIES:
                time.sleep(0.8 * attempt)
            else:
                raise

    raise last_err if last_err else RuntimeError("Unknown XHR error")


# =========================
# Main
# =========================

def main():
    df = pd.read_csv(TOP100_CSV)
    df = normalize_columns(df)

    if df.empty:
        raise RuntimeError(f"{TOP100_CSV} is empty or unreadable.")

    all_best_events: List[BestEvent] = []
    all_progress_rows: List[Dict[str, Any]] = []

    with sync_playwright() as pw:
        state_path = ensure_logged_in_state(pw)

        # Use saved state for authenticated requests (headless is fine now)
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(storage_state=state_path)

        for idx, row in df.iterrows():
            swimmer_id = extract_swimmer_id(row)
            swimmer_name = extract_name(row)

            if not swimmer_id:
                print(f"[SKIP] row {idx}: could not determine swimmer_id")
                continue

            tp = swimmer_times_page(swimmer_id)

            print(f"[INFO] {idx+1}/{len(df)} fastest_times swimmer={swimmer_id} {swimmer_name}".strip())
            ft_url = fastest_times_url(swimmer_id)

            try:
                payload = xhr_json_with_retries(context, ft_url, referer=tp)
            except Exception as e:
                print(f"[WARN] fastest_times failed swimmer={swimmer_id} err={e}")
                continue

            sleep_a_bit()

            fastest_rows = parse_fastest_times_payload(payload)
            best_events = pick_best_events(swimmer_id, swimmer_name, fastest_rows, BEST_EVENTS_PER_SWIMMER)

            if not best_events:
                print(f"[WARN] no best events found for swimmer {swimmer_id}")
                continue

            all_best_events.extend(best_events)

            for be in best_events:
                print(f"      -> event: {be.event_label or be.event_key}")
                te_url = times_by_event_url(swimmer_id, be.event_key)

                try:
                    te_payload = xhr_json_with_retries(context, te_url, referer=tp)
                except Exception as e:
                    print(f"[WARN] times_by_event failed swimmer={swimmer_id} event={be.event_key} err={e}")
                    continue

                sleep_a_bit()

                hist_rows = parse_times_by_event_payload(te_payload)
                for h in hist_rows:
                    out = {
                        "swimmer_id": swimmer_id,
                        "swimmer_name": swimmer_name,
                        "event_key": be.event_key,
                        "event": be.event_label,
                        "time": h.get("time") or h.get("resultTime") or h.get("result_time") or "",
                        "date": h.get("date") or h.get("resultDate") or h.get("result_date") or "",
                        "meet": h.get("meet") or h.get("meetName") or h.get("meet_name") or "",
                        "course": h.get("course") or h.get("pool") or "",
                        "is_pb": h.get("isPB") or h.get("pb") or "",
                    }
                    all_progress_rows.append(out)

        browser.close()

    best_df = pd.DataFrame([{
        "swimmer_id": b.swimmer_id,
        "swimmer_name": b.swimmer_name,
        "event_key": b.event_key,
        "event": b.event_label,
        "best_time": b.best_time,
        "best_date": b.best_date,
    } for b in all_best_events])

    prog_df = pd.DataFrame(all_progress_rows)

    CSV_DIR.mkdir(parents=True, exist_ok=True)
    best_df.to_csv(OUT_BESTEVENTS_CSV, index=False)
    prog_df.to_csv(OUT_PROGRESS_CSV, index=False)

    print(f"\n[OK] wrote {OUT_BESTEVENTS_CSV} rows={len(best_df)}")
    print(f"[OK] wrote {OUT_PROGRESS_CSV} rows={len(prog_df)}")


if __name__ == "__main__":
    main()
