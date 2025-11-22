import requests
import csv
from bs4 import BeautifulSoup as bs
import pandas as pd
import time
from pathlib import Path
from typing import Union

TEAM_PAGE_RANGE = 32  # number of pages under /team/?page=

states = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]

team_list = []


def getTeamList():
    """
    Scrape SwimCloud's /team/ pages to get all college teams.

    This is an HTML-only "surface" scraper used to build collegeSwimmingTeams.csv.
    """
    global team_list
    team_list = []

    for page in range(1, TEAM_PAGE_RANGE):
        team_list_url = f"https://www.swimcloud.com/team/?page={page}"
        resp = requests.get(
            team_list_url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
        )
        resp.encoding = "utf-8"
        soup = bs(resp.content, "html.parser")

        teams_html = soup.find_all("tr")[1:]  # first row is header

        for row in teams_html:
            infoList = row.find_all("td")
            if len(infoList) < 4:
                continue

            team_name = infoList[0].find("a").text.strip()
            team_ID = infoList[0].find("a")["href"].split("/")[-2]

            team_state = infoList[1].text.strip()
            if team_state not in states:
                team_state = "NA"

            team_division = "NONE"
            team_division_ID = "NONE"
            team_conference = "NONE"
            team_conference_ID = "NONE"

            if infoList[2].find("a") is not None:
                div_link = infoList[2].find("a")
                team_division = div_link["title"].strip()
                team_division_ID = div_link["href"].split("/")[-2]

            if infoList[3].find("a") is not None:
                conf_link = infoList[3].find("a")
                team_conference = conf_link["title"].strip()
                team_conference_ID = conf_link["href"].split("/")[-2]

            team_list.append(
                {
                    "team_name": team_name,
                    "team_ID": team_ID,
                    "team_state": team_state,
                    "team_division": team_division,
                    "team_division_ID": team_division_ID,
                    "team_conference": team_conference,
                    "team_conference_ID": team_conference_ID,
                }
            )

        # be semi-polite to the server
        time.sleep(0.05)


def teamListToCSV(filename="collegeSwimmingTeams.csv"):
    """Write the global team_list to a CSV."""
    with open(filename, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "team_name",
                "team_ID",
                "team_state",
                "team_division",
                "team_division_ID",
                "team_conference",
                "team_conference_ID",
            ]
        )

        for team in team_list:
            writer.writerow(
                [
                    team["team_name"],
                    team["team_ID"],
                    team["team_state"],
                    team["team_division"],
                    team["team_division_ID"],
                    team["team_conference"],
                    team["team_conference_ID"],
                ]
            )


def build_team_dataframe():
    """Scrape SwimCloud for all college teams and return a pandas DataFrame."""
    getTeamList()
    return pd.DataFrame(team_list)


def regenerate_teams_csv(path: Union[str, Path] = "collegeSwimmingTeams.csv") -> None:
    """
    Scrape SwimCloud and write the college teams table to a CSV.

    The resulting CSV can be loaded by SwimScraper.py via load_teams().
    """
    df = build_team_dataframe()
    path = Path(path)
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"[getTeamList] Wrote {len(df)} teams to {path}")


if __name__ == "__main__":
    regenerate_teams_csv()
