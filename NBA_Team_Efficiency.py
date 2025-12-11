#!/usr/bin/env python
import argparse
import csv
from datetime import datetime
import json
import re
from pathlib import Path
from time import sleep, time
from typing import Dict, Tuple, Any, Sequence, Union

from bs4 import BeautifulSoup, element
import requests
import pandas as pd
# Removed pyarrow and ipdb imports

DEBUG = True
MIN_YEAR = 2010
MAX_YEAR = 2026

# a player key is a 3-tuple of (bb_ref_id, team, year)
PlayerKey = Tuple[str, str, str]
PlayerStats = Dict[str, Any]
StatDict = Dict[PlayerKey, PlayerStats]


def log(msg: str) -> None:
    if DEBUG:
        print(msg)


def tryint(mayben: str) -> Union[str, float, int]:
    """
    Try to turn `mayben` into a number.
    (Function remains unchanged)
    """
    try:
        return int(mayben)
    except ValueError:
        try:
            return round(float(mayben), 4)
        except ValueError:
            return mayben


def stale(fname: Path) -> bool:
    """
    Return whether a file is stale and should be re-downloaded
    (Function remains unchanged)
    """
    if not fname.is_file():
        return True
    return (time() - fname.stat().st_mtime) / (60 * 60) > 1


def save(url: str, fname: Path) -> None:
    """save the contents of url to fname (Function remains unchanged)"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:106.0) Gecko/20100101 Firefox/106.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "cross-site",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }

    sleeps = 1
    res = requests.get(url, headers=headers)
    sleep(sleeps)

    retries = 0
    while res.status_code != 200:
        res = requests.get(url)
        sleeps *= 2
        print(f"sleeping {sleeps}, retrying {url}")
        sleep(sleeps)
        retries += 1
        if retries > 6:
            raise Exception(res.text, url, fname)

    with open(fname, "w") as f:
        f.write(res.text)


def get_bbref_data(year: str, datadir: Path) -> None:
    # ... (function remains unchanged) ...
    if not datadir.is_dir():
        datadir.mkdir(parents=True)

    log(f"getting {year} data in {datadir}")

    dir_ = Path(f"data/{year}")

    pages = [
        "totals",
        "per_game",
        "per_minute",
        "per_poss",
        "play-by-play",
        "advanced",
        "shooting",
        "adj_shooting",
        "rookies",
    ]

    for page in pages:
        save(
            f"https://www.basketball-reference.com/leagues/NBA_{year}_{page}.html",
            dir_ / f"{page}.html",
        )
    save(
        f"https://www.basketball-reference.com/leagues/NBA_{year}.html",
        dir_ / "teams.html",
    )

TEAMRE = re.compile("teams/(.*?)/")


def parse_team_stats(year: str) -> Dict[str, Any]:
    """Parse team stats and return a dictionary mapping team shortname to stats."""
    datadir = Path(f"data/{year}")
    teamdata = {}

    # Check if the teams.html file exists before trying to open it
    team_file_path = datadir / "teams.html"
    if not team_file_path.is_file():
        log(f"Team data file not found for {year}: {team_file_path}")
        return {}

    soup = BeautifulSoup(open(team_file_path), "html.parser")

    # 1. Totals Team Stats
    team_stats = soup.find("div", id="all_totals_team-opponent")
    if isinstance(team_stats, element.Tag):
        for team in team_stats.find_all("tr")[1:31]:
            teamstats = {
                t["data-stat"].replace("-", "_"): tryint(
                    "".join(str(c) for c in t.children)
                )
                for t in team.find_all("td")
            }
            assert isinstance(teamstats["team"], str)
            name = BeautifulSoup(teamstats["team"], "html.parser").text.rstrip("*")

            teamshort_match = TEAMRE.search(teamstats["team"])
            assert teamshort_match
            shortname = teamshort_match.group(1)

            teamstats["name"] = name
            teamstats["shortname"] = shortname
            
            # Use 'team_id' (e.g., MIA) as the key for later lookup/merge
            teamdata[shortname] = teamstats

    # 2. Misc/Advanced Stats
    misc_stats = soup.find("div", id="all_advanced_team")
    if isinstance(misc_stats, element.Tag):
        for team in misc_stats.find_all("tr")[2:32]:
            miscstats = {
                t["data-stat"].replace("-", "_"): tryint(
                    "".join(str(c) for c in t.children)
                )
                for t in team.find_all("td")
            }
            assert isinstance(miscstats["team"], str)
            
            shortname_match = TEAMRE.search(miscstats["team"])
            assert shortname_match
            shortname = shortname_match.group(1)

            if shortname in teamdata:
                 # Merge advanced stats into the existing entry
                 teamdata[shortname] = {**teamdata[shortname], **miscstats}
            else:
                 log(f"Warning: Team {shortname} found in advanced but not totals for {year}")


    # Prepare the data to be returned as a list of dicts, including the year
    final_team_list = []
    for team_key, stats in teamdata.items():
        stats['year'] = year
        final_team_list.append(stats)
        
    return final_team_list


def parse_bbref_row(players: StatDict, player: element.Tag, year: str) -> None:
    # ... (function remains unchanged) ...
    ignore = ["bpm-dum", "ws-dum", "DUMMY"]
    stats = {
        t["data-stat"].replace("-", "_"): tryint("".join(str(c) for c in t.children))
        for t in player.find_all("td")
        if t["data-stat"] and t["data-stat"] not in ignore
    }

    assert isinstance(stats["player"], str)
    assert isinstance(stats["team_id"], str)
    stats["name"] = BeautifulSoup(stats["player"], "html.parser").text
    stats["team"] = BeautifulSoup(stats["team_id"], "html.parser").text

    player_id = player.find("td")
    assert isinstance(player_id, element.Tag)
    stats["bb_ref_id"] = player_id.attrs["data-append-csv"]
    stats["year"] = year

    key = (stats["bb_ref_id"], stats["team"], year)
    if key not in players:
        players[key] = stats
    else:
        players[key] = {**players[key], **stats}


PLAYER_ROW = re.compile(r".*\b(full_table|partial_table)\b.*")


def parse_player_stats(year: str) -> StatDict:
    # ... (function remains unchanged) ...
    datadir = f"data/{year}"
    players: StatDict = {}

    for page in ["totals", "advanced", "per_minute", "per_poss", "per_game"]:
        soup = BeautifulSoup(open(f"{datadir}/{page}.html"), "html.parser")
        for player in soup.find_all("tr", {"class": PLAYER_ROW}):
            parse_bbref_row(players, player, year)

    return players


def download_data(years: Sequence[str], force_download: bool = False) -> None:
    # ... (function remains unchanged) ...
    for year in years:
        datadir = Path(f"data/{year}")
        if not datadir.is_dir():
            datadir.mkdir(parents=True)

        # we want to download the year's data if:
        # - the force_download flag was set
        # - the data is stale
        if force_download or stale(datadir / "totals.html"):
            get_bbref_data(year, datadir)
        else:
            continue

    # 538 Raptor data (remains unchanged)
    raptord = Path("data/raptor")
    if (
        not raptord.is_dir()
        or not (raptord / "historical_RAPTOR.csv").is_file()
        or not (raptord / "modern_RAPTOR.csv").is_file()
        or force_download
    ):
        log("downloading raptor")
        raptord.mkdir(parents=True, exist_ok=True)

        save(
            "https://raw.githubusercontent.com/fivethirtyeight/data/master/nba-raptor/historical_RAPTOR_by_team.csv",
            raptord / "historical_RAPTOR.csv",
        )

        save(
            "https://raw.githubusercontent.com/fivethirtyeight/data/master/nba-raptor/modern_RAPTOR_by_team.csv",
            raptord / "modern_RAPTOR.csv",
        )

    latest_raptor = raptord / "latest_RAPTOR.csv"
    if not (latest_raptor).is_file() or stale(latest_raptor) or force_download:
        log("downloading latest raptor")
        save(
            "https://projects.fivethirtyeight.com/nba-model/2022/latest_RAPTOR_by_team.csv",
            latest_raptor,
        )


# 538 has some different team names than bbref
def fix_team(team: str, year: str) -> str:
    # ... (function remains unchanged) ...
    if team == "CHA" and int(year) > 2014:
        return "CHO"
    return team


def parse_raptor_stats(data: StatDict, years: Sequence[str]) -> None:
    # ... (function remains unchanged) ...
    raptord = Path("data/raptor")
    for raptorfile in ("latest_RAPTOR", "modern_RAPTOR", "historical_RAPTOR"):
        for row in csv.DictReader(open(raptord / f"{raptorfile}.csv")):
            if row["season_type"] != "RS":
                # we're currently not handling playoff data, so skip
                continue
            pid = row["player_id"]
            year = row["season"]
            if year not in years:
                continue

            team = fix_team(row["team"], year)
            for key in set(row.keys()) - set(
                ["player_name", "player_id", "team", "season", "season_type"]
            ):
                if (pid, team, year) not in data:
                    raise Exception(f"Couldn't find ({pid}, {team}, {year}) in data")
                if key in data[(pid, team, year)]:
                    continue
                data[(pid, team, year)][key] = tryint(row[key])


def process_data(years: Sequence[str]) -> None:
    """Process the requested years' data and write it out as a CSV file"""
    player_data: StatDict = {}
    all_team_data: Dict[str, Any] = {}
    
    # 1. Process player stats first
    for year in years:
        log(f"processing {year} player data")
        player_data = {**parse_player_stats(year), **player_data}
    
    log("processing raptor")
    parse_raptor_stats(player_data, years)
    
    # 2. Process team stats
    team_list_of_dicts = []
    for year in years:
        log(f"processing {year} team data")
        team_list_of_dicts.extend(parse_team_stats(year))
        
    log("creating dataframes")
    
    # --- COMBINE ALL DATA INTO ONE CSV ---
    
    # Create the Player Data Frame
    player_df = pd.DataFrame(player_data.values())

    # Create the Team Data Frame (This is the efficiency data you requested)
    team_df = pd.DataFrame(team_list_of_dicts)
    
    # Merge Player and Team data (optional, but shows how to combine)
    # The team stats are duplicated for every player on that team in that year
    
    # Rename columns in team_df for merge safety
    team_df = team_df.rename(columns={'shortname': 'team_id'})
    
    # Select key team stats columns to merge, avoiding conflicts
    team_cols_to_merge = ['year', 'team_id', 'off_rtg', 'def_rtg', 'net_rtg'] 
    
    # Perform the merge. Merges team data into player data
    # final_df = pd.merge(player_df, 
    #                     team_df[team_cols_to_merge], 
    #                     on=['year', 'team_id'], 
    #                     how='left',
    #                     suffixes=('_player', '_team'))
                        
    # For Power BI, let's focus ONLY on the clean Team Stats DataFrame
    final_df = team_df
    
    log("converting columns to proper type")

    # This section remains the same for type safety in Pandas
    for col in final_df.columns:
        column = final_df[col]
        assert column is not None
        colHead = column.head(20)
        for elt in colHead:
            if isinstance(elt, (float, int)):
                column.replace("", 0.0, inplace=True)
                break
        if column.dtype == "int64":
            final_df[col] = column.astype("int32")

    log("creating CSV file")

    output = "nba_team_efficiency.csv"
    
    # Save the final data as a CSV file (This is the key change!)
    final_df.to_csv(output, index=False)
    
    log(f"Data saved to {output}")


def main(args) -> None:
    # ... (function remains unchanged) ...
    if args.year_only:
        years = [str(args.year_only)]
    else:
        years = [str(year) for year in range(MIN_YEAR, MAX_YEAR)]

    if not args.no_download:
        download_data(years, args.force_download)
    process_data(years)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="download basketball statistics")
    parser.add_argument(
        "--no-download",
        "-n",
        dest="no_download",
        action="store_true",
        required=False,
        help="do not download any data",
    )
    parser.add_argument(
        "--force-download",
        "-f",
        dest="force_download",
        action="store_true",
        required=False,
        help="download all data regardless of the cache",
    )
    parser.add_argument(
        "--year-only",
        "-y",
        dest="year_only",
        type=int,
        action="store",
        required=False,
        help="only process a particular year",
    )
    parser.add_argument(
        "--force-reprocess",
        dest="force_reprocess",
        action="store_true",
        required=False,
        help="Force reprocessing of all years",
    )
    args = parser.parse_args()

    # The original was "with ipdb.launch_ipdb_on_exception(): main(args)"
    # We use a simple try/except block now that ipdb is removed
    try:
        main(args)
    except Exception as e:
        print(f"An error occurred during execution: {e}")
        # Optionally, re-raise the exception if needed for debugging
