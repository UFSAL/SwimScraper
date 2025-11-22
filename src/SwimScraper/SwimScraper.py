import requests
import csv
from bs4 import BeautifulSoup as bs
import pandas as pd
import time as _time

# Selenium is only used by a few legacy functions.
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
)

from pathlib import Path
from datetime import datetime
SWIMCLOUD_SWIMMER_API = "https://www.swimcloud.com/api/swimmers"

# ---------------------------------------------------------------------------
# JSON-BASED HELPERS (NEW STYLE)
# ---------------------------------------------------------------------------
STROKE_CODES = {
    "1": "Free",
    "2": "Back",
    "3": "Breast",
    "4": "Fly",
    "5": "IM",
}

def _gender_code(eventgender: str) -> int:
    """Map SwimCloud eventgender ('M'/'F') to numeric code used in event token."""
    if eventgender == "M":
        return 1
    if eventgender == "F":
        return 2
    # fallback, just in case
    return 0

def _stroke_name(code) -> str:
    return STROKE_CODES.get(str(code), str(code))

def _event_label_from_record(rec: dict) -> str:
    """e.g. '50 Y Free'."""
    dist = rec.get("eventdistance")
    course = rec.get("eventcourse")
    stroke = _stroke_name(rec.get("eventstroke"))
    return f"{dist} {course} {stroke}"

def _event_token_from_record(rec: dict) -> str:
    """
    Build the event token used by /times_by_event/ from a fastest-times record.
    Pattern inferred from DevTools:
        gender_code|distance|course|stroke_code
    Example: '1|50|Y|1' == M 50y Free
    """
    gender_code = _gender_code(rec.get("eventgender"))
    distance = rec.get("eventdistance")
    course = rec.get("eventcourse")
    stroke_code = rec.get("eventstroke")
    return f"{gender_code}|{distance}|{course}|{stroke_code}"

def getTeamPerformance(team_id, gender="M", event_course="Y", rank_type="D", limit=200):
    """
    Fetch performance rankings for a team from SwimCloud's JSON API.

    This uses:
        https://www.swimcloud.com/api/performances/get_for_team/

    Returns a list of dicts with fields like:
        id, score, gender, agegroup, team_id, season_id, rank_type,
        event_course, place, updated_at, created_at, ...
    """
    url = "https://www.swimcloud.com/api/performances/get_for_team/"
    params = {
        "event_course": event_course,
        "gender": gender,
        "limit": limit,
        "rank_type": rank_type,
        "team_id": team_id,
    }

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
    }

    r = requests.get(url, params=params, headers=headers)
    r.raise_for_status()
    data = r.json()
    return data.get("results", [])


def getSwimmerProfileFastestTimes(swimmer_ID):
    """
    JSON API: fastest times per event for a swimmer.

    Hits:
      https://www.swimcloud.com/api/swimmers/<ID>/profile_fastest_times/

    Returns the raw JSON dict from SwimCloud. Use this when you want
    'one row per event' best time (good for a quick performance snapshot).
    """
    url = f"{SWIMCLOUD_SWIMMER_API}/{swimmer_ID}/profile_fastest_times/"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
    }
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()


def getSwimmerTimesByEventJSON(swimmer_ID, event_token):
    """
    JSON API: all swims for a single event for this swimmer.

    event_token is the *decoded* 'event=' query value from DevTools, e.g.:

        '1|50|Y|1'

    which corresponds to this URL:

        https://www.swimcloud.com/api/swimmers/<ID>/times_by_event/?event=1%7C50%7CY%7C1
    """
    url = f"{SWIMCLOUD_SWIMMER_API}/{swimmer_ID}/times_by_event/"
    params = {"event": event_token}  # requests will URL-encode the '|'
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
    }
    r = requests.get(url, params=params, headers=headers)
    r.raise_for_status()
    return r.json()


def swimmer_times_to_dataframe(times_json):
    """
    Best-effort converter from times_by_event JSON into a flat pandas DataFrame.

    This assumes the JSON has a top-level list under either 'results' or 'times'.
    If the schema is different, tweak this function after printing times_json.keys().
    """
    import pandas as pd

    if isinstance(times_json, dict):
        if "results" in times_json and isinstance(times_json["results"], list):
            rows = times_json["results"]
        elif "times" in times_json and isinstance(times_json["times"], list):
            rows = times_json["times"]
        else:
            lists = [v for v in times_json.values() if isinstance(v, list)]
            if len(lists) == 1:
                rows = lists[0]
            else:
                raise ValueError(
                    "Could not locate a list of times in JSON. "
                    "Inspect the structure and adjust swimmer_times_to_dataframe()."
                )
    elif isinstance(times_json, list):
        rows = times_json
    else:
        raise TypeError("times_json must be a dict or list")

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# TEAMS TABLE MANAGEMENT (collegeSwimmingTeams.csv)
# ---------------------------------------------------------------------------

def getSwimmerEventTokens(swimmer_ID):
    """
    Use profile_fastest_times JSON to infer which events this swimmer has.

    Returns a list of dicts with:
      - swimmer_id
      - event_token  (for times_by_event API)
      - event_label  (e.g. '50 Y Free')
      - eventdistance, eventstroke, eventcourse, eventgender
    """
    fastest = getSwimmerProfileFastestTimes(swimmer_ID)
    tokens = []

    seen = set()
    for rec in fastest:
        token = _event_token_from_record(rec)
        if token in seen:
            continue
        seen.add(token)
        tokens.append({
            "swimmer_id": swimmer_ID,
            "event_token": token,
            "event_label": _event_label_from_record(rec),
            "eventdistance": rec.get("eventdistance"),
            "eventstroke": rec.get("eventstroke"),
            "eventcourse": rec.get("eventcourse"),
            "eventgender": rec.get("eventgender"),
        })

    return tokens

def getSwimmerAllTimes(swimmer_ID):
    """
    Fetch *all* swims for this swimmer across all events, using JSON APIs.

    Returns a pandas DataFrame with one row per swim and simplified columns.

    Columns:
      swimmer_id, event_label, eventdistance, eventcourse, eventstroke_name,
      eventgender, eventtime, dateofswim, meet_name, season_id, heat, lane, place
    """
    import pandas as pd

    event_tokens = getSwimmerEventTokens(swimmer_ID)
    all_rows = []

    for et in event_tokens:
        token = et["event_token"]
        label = et["event_label"]
        try:
            event_times = getSwimmerTimesByEventJSON(swimmer_ID, token)
        except Exception as e:
            print(f"[SwimScraper] Error fetching times for swimmer {swimmer_ID}, "
                  f"event {label} ({token}): {e}")
            continue

        for rec in event_times:
            # Choose sensible, stable fields; fall back where needed
            distance = rec.get("eventdistance")
            course = rec.get("eventcourse")
            stroke_code = rec.get("eventstroke")
            stroke_name = _stroke_name(stroke_code)
            gender = rec.get("eventgender")

            # time & date / meet fields (names inferred from fastest-times JSON)
            eventtime = rec.get("eventtime") or rec.get("time")
            dateofswim = rec.get("dateofswim") or rec.get("date_created")
            meet_name = rec.get("meet_name") or rec.get("name")
            season_id = rec.get("season_id")
            heat = rec.get("heat")
            lane = rec.get("lane")
            place = rec.get("place")

            all_rows.append({
                "swimmer_id": swimmer_ID,
                "event_label": label,
                "eventdistance": distance,
                "eventcourse": course,
                "eventstroke_name": stroke_name,
                "eventgender": gender,
                "eventtime": eventtime,
                "dateofswim": dateofswim,
                "meet_name": meet_name,
                "season_id": season_id,
                "heat": heat,
                "lane": lane,
                "place": place,
            })

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df = df.sort_values(["swimmer_id", "event_label", "dateofswim"], na_position="last")
    return df

def getSwimmerFastestTimesClean(swimmer_ID):
    """
    Return a simplified DataFrame of fastest times per event for this swimmer.

    Columns:
      swimmer_id, event_label, eventdistance, eventcourse, eventstroke_name,
      eventgender, eventtime, dateofswim, meet_name, season_id
    """
    import pandas as pd

    fastest = getSwimmerProfileFastestTimes(swimmer_ID)
    rows = []
    for rec in fastest:
        label = _event_label_from_record(rec)
        stroke_name = _stroke_name(rec.get("eventstroke"))
        rows.append({
            "swimmer_id": swimmer_ID,
            "event_label": label,
            "eventdistance": rec.get("eventdistance"),
            "eventcourse": rec.get("eventcourse"),
            "eventstroke_name": stroke_name,
            "eventgender": rec.get("eventgender"),
            "eventtime": rec.get("eventtime") or rec.get("time"),
            "dateofswim": rec.get("dateofswim") or rec.get("date_created"),
            "meet_name": rec.get("meet_name") or rec.get("name"),
            "season_id": rec.get("season_id"),
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df = df.sort_values(["swimmer_id", "event_label"])
    return df


def _default_teams_path():
    """Return default location of collegeSwimmingTeams.csv (next to this file)."""
    return Path(__file__).with_name("collegeSwimmingTeams.csv")


import pandas as pd  # already there at the top

def load_teams(path=None):
    """
    Load the college teams table from a CSV path or the default location.

    If the file is missing, empty, or has no columns, return an empty
    DataFrame with the expected columns instead of crashing.
    """
    if path is None:
        path = _default_teams_path()
    path = Path(path)

    expected_cols = [
        "team_name",
        "team_ID",
        "team_state",
        "team_division",
        "team_division_ID",
        "team_conference",
        "team_conference_ID",
    ]

    if not path.exists():
        print(f"[SwimScraper] Warning: teams CSV not found at {path}, using empty table.")
        return pd.DataFrame(columns=expected_cols)

    if path.stat().st_size == 0:
        print(f"[SwimScraper] Warning: teams CSV at {path} is empty, using empty table.")
        return pd.DataFrame(columns=expected_cols)

    try:
        df = pd.read_csv(path)
    except pd.errors.EmptyDataError:
        print(f"[SwimScraper] Warning: teams CSV at {path} has no columns, using empty table.")
        return pd.DataFrame(columns=expected_cols)

    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    return df[expected_cols]



# Global teams table used by a few helper functions
teams = load_teams()


def set_teams_csv(path):
    """Override the default teams CSV path at runtime."""
    global teams
    teams = load_teams(path)


# ---------------------------------------------------------------------------
# CONSTANTS / MAPPINGS
# ---------------------------------------------------------------------------

events = {
    # yards
    "25 Y Free": "125Y",
    "25 Y Back": "225 Y",
    "25 Y Breast": "325Y",
    "25 Y Fly": "425Y",
    "50 Y Free": "150Y",
    "75 Y Free": "175Y",
    "100 Y Free": "1100Y",
    "125 Y Free": "1125Y",
    "200 Y Free": "1200Y",
    "400 Y Free": "1400Y",
    "500 Y Free": "1500Y",
    "800 Y Free": "1800Y",
    "1000 Y Free": "11000Y",
    "1500 Y Free": "11500Y",
    "1650 Y Free": "11650Y",
    "50 Y Back": "250Y",
    "100 Y Back": "2100Y",
    "200 Y Back": "2200Y",
    "50 Y Breast": "350Y",
    "100 Y Breast": "3100Y",
    "200 Y Breast": "3200Y",
    "50 Y Fly": "450Y",
    "100 Y Fly": "4100Y",
    "200 Y Fly": "4200Y",
    "100 Y IM": "5100Y",
    "200 Y IM": "5200Y",
    "400 Y IM": "5400Y",
    "200 Free Relay": 6200,
    "400 Free Relay": 6400,
    "800 Free Relay": 6800,
    "200 Medley Relay": 7200,
    "400 Medley Relay": 7400,
    "1 M Diving": "H1",
    "1 M Diving (6 dives)": "H16",
    "3 M Diving ": "H3",
    "3 M Diving (6 dives)": "H36",
    "7M Diving": "H75",
    "7M Diving (5 dives)": "H75Y",
    "Platform Diving": "H2",
    "50 Individual": "H50",
    "100 Individual": "H100",
    "200 Individual": "H200",
    # short course meters
    "25 S Free": "125S",
    "25 S Back": "225 S",
    "25 S Breast": "325S",
    "25 S Fly": "425S",
    "50 S Free": "150S",
    "75 S Free": "175S",
    "100 S Free": "1100S",
    "125 S Free": "1125S",
    "200 S Free": "1200S",
    "400 S Free": "1400S",
    "500 S Free": "1500S",
    "800 S Free": "1800S",
    "1000 S Free": "11000S",
    "1500 S Free": "11500S",
    "1650 S Free": "11650S",
    "50 S Back": "250S",
    "100 S Back": "2100S",
    "200 S Back": "2200S",
    "50 S Breast": "350S",
    "100 S Breast": "3100S",
    "200 S Breast": "3200S",
    "50 S Fly": "450S",
    "100 S Fly": "4100S",
    "200 S Fly": "4200S",
    "100 S IM": "5100S",
    "200 S IM": "5200S",
    "400 S IM": "5400S",
    # long course meters
    "50 L Free": "150L",
    "100 L Free": "1100L",
    "200 L Free": "1200L",
    "400 L Free": "1400L",
    "500 L Free": "1500L",
    "800 L Free": "1800L",
    "1000 L Free": "11000L",
    "1500 L Free": "11500L",
    "1650 L Free": "11650L",
    "50 L Back": "250L",
    "100 L Back": "2100L",
    "200 L Back": "2200L",
    "50 L Breast": "350L",
    "100 L Breast": "3100L",
    "200 L Breast": "3200L",
    "50 L Fly": "450L",
    "100 L Fly": "4100L",
    "200 L Fly": "4200L",
    "100 L IM": "5100L",
    "200 L IM": "5200L",
    "400 L IM": "5400L",
}

us_states = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "District of Columbia": "DC",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
}

# ---------------------------------------------------------------------------
# HELPER FUNCTIONS (string parsing, IDs, etc.)
# ---------------------------------------------------------------------------

def cleanName(name: str) -> str:
    """
    Normalize swimmer names.

    Handles:
      - 'Last, First'  -> 'First Last'
      - 'First Last'   -> 'First Last' (unchanged)
      - single tokens  -> returned as-is
    """
    if not name:
        return ""

    name = name.strip()

    if "," in name:
        parts = [p.strip() for p in name.split(",") if p.strip()]
        if len(parts) == 2:
            last, first = parts
            return f"{first} {last}".strip()
        return " ".join(parts)

    tokens = name.split()
    if len(tokens) <= 1:
        return tokens[0]
    return " ".join(tokens)


def getTeamID(team_name):
    """Get team_ID for a given team_name from the teams table."""
    team_number = -1
    for _, row in teams.iterrows():
        if row["team_name"] == team_name:
            team_number = row["team_ID"]
    return team_number


def getTeamName(team_ID):
    """Get team_name for a given team_ID from the teams table."""
    team_name = ""
    for _, row in teams.iterrows():
        if row["team_ID"] == team_ID:
            team_name = row["team_name"]
    return team_name


def getSeasonID(year):
    return year - 1996


def getYear(season_ID):
    return season_ID + 1996


def getEventName(event_ID):
    return list(events.keys())[list(events.values()).index(event_ID)]


def getEventID(event_name):
    return events.get(event_name)


def getState(hometown):
    home = hometown.split(",")[-1].strip()
    if home.isalpha():
        return home
    return "NONE"


def getCity(hometown):
    home = hometown.split(",")
    home.pop()  # remove state/country
    city = " ".join([c.strip() for c in home])
    return city


def convertTime(display_time):
    """Convert strings like 'M:SS.s' or 'SS.s' to total seconds (float).

    Returns None for non-numeric codes like DNS/DQ.
    """
    display_time = str(display_time).strip()

    if ":" in display_time:
        minutes, seconds = display_time.split(":", 1)
        return float(minutes) * 60 + float(seconds)
    elif display_time.isalpha():
        return None
    else:
        return float(display_time)


def getIndexes(data):
    """For EVENT PROGRESSION tables; find meet / date / extra-info columns."""
    meet_name_index = -1
    date_index = -1
    additional_info_index = -1

    i = 0
    for td in data:
        text = td.text.strip()
        if text == "Meet":
            meet_name_index = i
        elif text == "Date":
            date_index = i
        elif (
            text == ""
            and td.has_attr("class")
            and "c-table-clean__col-fit" in td["class"]
        ):
            additional_info_index = i
        i += 1

    return {
        "meet_name_index": meet_name_index,
        "date_index": date_index,
        "additional_info_index": additional_info_index,
    }


# ---------------------------------------------------------------------------
# "SURFACE" HTML SCRAPERS (lists: teams, rosters, HS recruits)
# ---------------------------------------------------------------------------

def getCollegeTeams(team_names=["NONE"], conference_names=["NONE"], division_names=["NONE"]):
    """
    Return a list of teams matching given filters, using the local teams table.

    This uses collegeSwimmingTeams.csv (loaded into `teams`) – it does NOT
    hit SwimCloud directly. Use getTeamList.py to regenerate the CSV if needed.
    """
    team_df = pd.DataFrame()
    if team_names != ["NONE"]:
        team_df = teams[teams["team_name"].isin(team_names)].reset_index(drop=True)
    elif division_names != ["NONE"]:
        team_df = teams[teams["team_division"].isin(division_names)].reset_index(drop=True)
    elif conference_names != ["NONE"]:
        team_df = teams[teams["team_conference"].isin(conference_names)].reset_index(
            drop=True
        )
    else:
        team_df = teams

    return team_df.to_dict("records")


def getRoster(team, gender, team_ID=-1, season_ID=-1, year=-1, pro=False):
    """
    Scrape SwimCloud roster HTML for a team/gender/season.

    NOTE: For now this only collects:
        swimmer_name, swimmer_ID, grade, hometown_state, hometown_city,
        team_name, team_ID, HS_power_index=None

    We deliberately DO NOT call getPowerIndex() here to avoid one HTTP
    request per swimmer. HSPowerIndex can later be filled from a JSON
    swimmer endpoint.
    """
    roster = []

    if gender not in ("M", "F"):
        print("ERROR: need to input either M or F for gender")
        return []

    if team_ID != -1:
        team_number = team_ID
        team = getTeamName(team_ID) or team
    else:
        team_number = getTeamID(team)

    if year != -1:
        season_ID = getSeasonID(year)

    if season_ID == -1 and year == -1:
        current_year = datetime.now().year
        season_ID = getSeasonID(current_year)

    roster_url = (
        f"https://www.swimcloud.com/team/{team_number}/roster/"
        f"?page=1&gender={gender}&season_id={season_ID}"
    )

    resp = requests.get(
        roster_url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                " AppleWebKit/537.36 (KHTML, like Gecko)"
                " Chrome/81.0.4044.138 Safari/537.36"
            ),
            "Referer": "https://google.com/",
        },
    )
    resp.encoding = "utf-8"
    soup = bs(resp.text, "html.parser")

    try:
        rows = (
            soup.find(
                "table",
                attrs={
                    "class": "c-table-clean c-table-clean--middle table table-hover"
                },
            )
            .find_all("tr")[1:]
        )
    except AttributeError:
        print("An invalid team was entered, causing the following error:")
        raise

    for row in rows:
        swimmer_name = cleanName(row.find("a").text.strip())
        id_array = row.find_all("a")
        swimmer_ID = id_array[0]["href"].split("/")[-1]

        cols = row.find_all("td")
        hometown = cols[2].text.strip()
        state = getState(hometown)
        city = getCity(hometown)

        if not pro:
            grade = cols[3].text.strip()
        else:
            grade = "None"

        # IMPORTANT: no per-swimmer HTTP here; placeholder only.
        HS_power_index = None

        roster.append(
            {
                "swimmer_name": swimmer_name,
                "swimmer_ID": swimmer_ID,
                "team_name": team,
                "team_ID": team_number,
                "grade": grade,
                "hometown_state": state,
                "hometown_city": city,
                "HS_power_index": HS_power_index,
            }
        )

    return roster


def getHSRecruitRankings(
    class_year, gender, state="none", state_abbreviation="none", international=False
):
    """
    Scrape SwimCloud high school recruiting rankings (HTML).

    Returns a list of dicts with:
      swimmer_name, swimmer_ID, team_name, team_ID,
      hometown_state, hometown_city, HS_power_index
    """
    recruits = []

    if state != "none" and state_abbreviation == "none":
        state_abbreviation = us_states.get(state)

    if gender not in ("M", "F"):
        print("ERROR: need to input either M or F for gender")
        return []

    base = f"https://www.swimcloud.com/recruiting/rankings/{class_year}/{gender}/"

    if international:
        recruiting_url = base + "2/"
    elif state_abbreviation not in (None, "none"):
        recruiting_url = base + f"1/{state_abbreviation}/"
    elif state != "none":
        recruiting_url = base + f"1/{us_states.get(state)}/"
    else:
        recruiting_url = base

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.6367.60 Safari/537.36"
        ),
        "Referer": "https://www.google.com/",
    }

    for page in range(1, 5):
        page_url = f"{recruiting_url}?page={page}"
        resp = requests.get(page_url, headers=headers)
        if resp.status_code != 200:
            break

        soup = bs(resp.text, "html.parser")
        table_div = soup.find("div", class_="c-table-clean--responsive")
        if not table_div:
            break

        rows = table_div.find_all("tr")
        if len(rows) <= 1:
            break

        for row in rows[1:]:
            # swimmer name + ID
            name_link = row.find("a", href=lambda h: h and "/swimmer/" in h)
            if not name_link:
                continue
            swimmer_name = name_link.get_text(strip=True)
            href = name_link["href"].rstrip("/")
            swimmer_ID = href.split("/")[-1]

            # hometown info
            hometown_state = hometown_city = None
            hometown_td = row.find("td", class_="u-color-mute")
            if hometown_td:
                hometown_info = hometown_td.get_text(strip=True)
                hometown_state = getState(hometown_info)
                hometown_city = getCity(hometown_info)

            # HS power index (as shown in table)
            hs_power_index = None
            power_td = row.find("td", class_="u-text-end")
            if power_td:
                hs_power_index = power_td.get_text(strip=True)

            # committed college (if any)
            team_name = "None"
            team_ID = "None"
            team_link = row.find("a", href=lambda h: h and "/team/" in h)
            if team_link:
                team_href = team_link["href"].rstrip("/")
                team_ID = team_href.split("/")[-1]

                img = team_link.find("img")
                if img and img.get("alt"):
                    parts = img["alt"].split()
                    if parts and parts[-1].lower() == "logo":
                        parts = parts[:-1]
                    team_name = " ".join(parts).strip() or team_name
                else:
                    team_name = team_link.get_text(strip=True) or team_name

            recruits.append(
                {
                    "swimmer_name": swimmer_name,
                    "swimmer_ID": swimmer_ID,
                    "team_name": team_name,
                    "team_ID": team_ID,
                    "hometown_state": hometown_state,
                    "hometown_city": hometown_city,
                    "HS_power_index": hs_power_index,
                }
            )

    return recruits


# ---------------------------------------------------------------------------
# LEGACY / HEAVY FUNCTIONS (HTML + Selenium)
# ---------------------------------------------------------------------------
# These are left mostly unchanged for backwards compatibility but are NOT
# used in your new JSON+HTML pipeline. Use with caution for big jobs.

def getTeamRankingsList(gender, season_ID=-1, year=-1):
    """Legacy: scrape national team rankings with Selenium."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)
    ignored_exceptions = (NoSuchElementException, StaleElementReferenceException)

    teams_out = []

    if gender not in ("M", "F"):
        print("ERROR: need to input either M or F for gender")
        return []

    if year != -1:
        season_ID = getSeasonID(year)
    elif season_ID == -1 and year == -1:
        current_year = datetime.now().year
        season_ID = getSeasonID(current_year)

    page_url = (
        "https://swimcloud.com/team/rankings/?eventCourse=L"
        f"?gender={gender}&page=1&region&seasonId={season_ID}"
    )
    driver.get(page_url)
    _time.sleep(3)

    html = driver.page_source
    soup = bs(html, "html.parser")

    teams_list = (
        soup.find("table", attrs={"class": "c-table-clean"})
        .find("tbody")
        .find_all("tr")
    )

    for team in teams_list:
        data = team.find_all("td")
        team_name = data[1].find("strong").text.strip()
        team_ID = data[1].find("a")["href"].split("/")[-1]
        swimcloud_points = data[2].find("a").text.strip()
        teams_out.append(
            {
                "team_name": team_name,
                "team_ID": team_ID,
                "swimcloud_points": swimcloud_points,
            }
        )

    driver.close()
    return teams_out


def getPowerIndex(swimmer_ID):
    """
    TEMP STUB: high school power index lookup.

    The original implementation made multiple HTML requests per swimmer.
    For the new pipeline we avoid that; this function just returns None.

    Once we have a swimmer JSON endpoint, we can implement this properly.
    """
    return None


