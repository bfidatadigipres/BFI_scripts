#!/usr/bin/env python3

"""
Fetch JSON metadata from PA Media for streaming platforms.
CSV for programmes must be supplied for metadata retrieval
** Must be supplied CSV path as argv[1] to operate **

main():
1. Open platform year CSV and read list of article, title, level and platform
2. Build PA Media URL with title and platform catalogue ID
   url = os.path.join(URL, f"asset?Anatomy%20of%20a%20Scandal&apikey={os.environ['PATV_KEY']}")
3. Call the API for title, using fetch('title')
4. When downloaded iterates the returned assets identifying which approach to take:
   Episodic: Handling multiple episodes per season
   Monographic: Handling single instances
5. Extract asset_id from ['link'][0]['href'][0], slicing the last part of the web address
6. From this you can build the folder structure.
7. Extract (mono/season and series episodes), catalogue and contributors data to JSON dictionary.
7. Place relevant JSON dumped metadata into each folder when not already in place.

Platform agnostic, this data take from CSV input

2024
"""

import datetime
import json
import logging
import os
import sys
from typing import Any, Final, Optional

import pandas
import requests
import tenacity

# Local package
sys.path.append(os.environ["CODE"])
import utils

# Global variables
STORAGE = os.environ["QNAP_IMAGEN"]
LOG_PATH = os.environ["LOG_PATH"]
TODAY = datetime.date.today()
TWO_WEEKS = TODAY - datetime.timedelta(days=140)
START = f"{TWO_WEEKS.strftime('%Y-%m-%d')}T00:00:00"
END = f"{TODAY.strftime('%Y-%m-%d')}T23:59:00"
UPDATE_AFTER = "2019-07-01T00:00:00"

# Setup logging
LOGGER = logging.getLogger("fetch_platforms_augmented")
HDLR = logging.FileHandler(os.path.join(LOG_PATH, "fetch_platforms_augmented.log"))
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

# PATV API details including unique identifiers for Netflix catalogue
URL = os.environ.get("PATV_STREAM_URL")
HEADERS = {"accept": "application/json", "apikey": os.environ.get("PATV_KEY")}
STREAM_KEYS = {
    "Netflix": os.environ.get("PA_NETFLIX"),
    "Amazon": os.environ.get("PA_AMAZON"),
}


def read_csv_to_dict(csv_path: str) -> dict[str, list[str]]:
    """
    Make set of all entries
    with title as key, and value
    to contain all other entries
    as a list (use pandas)
    """

    data = pandas.read_csv(csv_path)
    data_dct = data.to_dict(orient="list")
    print(data)
    return data_dct


@tenacity.retry(stop=tenacity.stop_after_attempt(3))
def fetch(
    cat_id: str, search_type: str, search_id: str, title: str
) -> Optional[dict[str, str]]:
    """
    Fetch data from PATV URL
    """
    url_title = title.replace(" ", "%20")
    url_title = f"%27{url_title}%27"
    if search_type == "title":
        try:
            url_all = os.path.join(
                URL,
                f"catalogue/{cat_id}/asset?title={url_title}&apikey={os.environ['PATV_KEY']}",
            )
            print(url_all)
            req = requests.get(url_all, headers=HEADERS)
            dct = json.loads(req.text)
            return dct
        except Exception as err:
            print("fetch(): **** PROBLEM: Cannot fetch EPG metadata.")
            LOGGER.critical("**** PROBLEM: Cannot fetch EPG metadata. **** \n%s", err)
            raise tenacity.TryAgain
    elif search_type == "cat_asset":
        try:
            req = requests.get(
                os.path.join(URL, f"catalogue/{cat_id}/asset/{search_id}"),
                headers=HEADERS,
            )
            dct = json.loads(req.text)
            return dct
        except Exception as err:
            print("fetch(): **** PROBLEM: Cannot fetch EPG metadata.")
            LOGGER.critical("**** PROBLEM: Cannot fetch EPG metadata. **** \n%s", err)
            raise tenacity.TryAgain
    elif search_type == "asset":
        try:
            req = requests.get(os.path.join(URL, f"asset/{search_id}"), headers=HEADERS)
            dct = json.loads(req.text)
            return dct
        except Exception as err:
            print("fetch(): **** PROBLEM: Cannot fetch EPG metadata.")
            LOGGER.critical("**** PROBLEM: Cannot fetch EPG metadata. **** \n%s", err)
            raise tenacity.TryAgain
    elif search_type == "contributors":
        try:
            req = requests.get(
                os.path.join(URL, f"asset/{search_id}/contributor"), headers=HEADERS
            )
            dct = json.loads(req.text)
            return dct
        except Exception as err:
            print("fetch(): **** PROBLEM: Cannot fetch EPG metadata.")
            LOGGER.critical("**** PROBLEM: Cannot fetch EPG metadata. **** \n%s", err)
            raise tenacity.TryAgain
    return None


def json_dump(json_path: str, dct=None) -> None:
    """
    Take a catalogue dictionary
    and output to file for read/processing
    """
    if dct is None:
        dct = {}

    with open(json_path, "w") as file:
        json.dump(dct, file, indent=4)
        file.close()


def get_cat_assets(asset=None) -> tuple[str, str, Optional[str], str]:
    """
    Retrieve asset information for logs/processing
    """
    if asset is None:
        asset = {}
    try:
        episode_title = asset["title"]
    except (IndexError, KeyError):
        episode_title = ""
    try:
        cat_id = asset["id"]
    except (IndexError, KeyError):
        cat_id = ""
    try:
        ep_num = asset["meta"]["episode"]
    except (IndexError, KeyError):
        ep_num = None
    try:
        linked_content = asset["link"][0]["href"]
    except (IndexError, KeyError):
        linked_content = ""
    try:
        episode_asset_id = linked_content.split("/")[-1]
    except Exception:
        episode_asset_id = ""

    return (episode_title, cat_id, ep_num, episode_asset_id)


def get_series_title(asset=None) -> Optional[str]:
    """
    Get series title data
    """
    if asset is None:
        asset = {}
    try:
        title = asset["title"]
        title = (
            title.replace("/", "")
            .replace("'", "")
            .replace("&", "and")
            .replace("(", "")
            .replace(")", "")
        )
        return title.replace(" ", "_")
    except (IndexError, KeyError):
        return None


def main() -> None:
    """
    Grab last two weeks catalogue items, output to JSON for storage
    Iterate list and build asset_dict of TV items, then process
    any new items placing in programme led folder structures
    """
    if not utils.check_control("stora"):
        LOGGER.info("Script run prevented by downtime_control.json. Script exiting.")
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")

    LOGGER.info(
        "========== Fetch augmented metadata script STARTED ======================"
    )

    # Fetch CSV data for title/article
    csv_path = sys.argv[1]
    if not os.path.isfile(csv_path):
        sys.exit(f"Problem with supplied CSV path {csv_path}")

    prog_dct = read_csv_to_dict(csv_path)
    csv_range = len(prog_dct["title"])
    for num in range(0, csv_range):
        # Capture CSV supplied data to vars
        platform = prog_dct["platform"][num]
        title = prog_dct["title"][num]
        article = prog_dct["article"][num]
        level = prog_dct["level"][num]
        season_num = int(prog_dct["series_number"][num])
        total_episode = int(prog_dct["episode_number"][num])
        print(platform, article, title, level, season_num, total_episode)
        LOGGER.info("** Processing item: %s %s", article, title)

        # Fetch title information from REST API
        if "-" not in article:
            title_retrieve = f"{article} {title}"
        else:
            title_retrieve = title

        cat_id = ""
        storage_path = os.path.join(STORAGE, platform.upper())
        for key, value in STREAM_KEYS.items():
            if platform == key:
                cat_id = value

        if len(cat_id) == 0:
            LOGGER.warning("Unable to retrieve Platform catalogue ID: %s", platform)
            continue

        LOGGER.info(
            "Requests will now attempt to retrieve the %s metadata for title %s",
            platform,
            title_retrieve,
        )
        json_dct = fetch(cat_id, "title", "", title_retrieve)
        if not json_dct:
            continue
        print(json_dct)
        LOGGER.info("Fetched JSON data successfully.")
        catalogue_path = os.path.join(
            storage_path,
            f"catalogue/{title_retrieve.replace(' ', '_')}_{END.replace(':', '-')}_catalogue.json",
        )
        json_dump(catalogue_path, json_dct)
        print("Downloaded catalogue info...")

        # Iterate all data retrieved from catalogue for date range
        asset_dict = {}
        items = json_dct["item"]

        if level == "Series":
            for asset in items:
                episode_title, catalogue_id, num, episode_asset_id = get_cat_assets(
                    asset
                )
                print(episode_title, catalogue_id, episode_asset_id)
                if not num:
                    print(
                        f"Skipping asset {episode_title}, no 'episode' data in 'meta'"
                    )
                    LOGGER.info(
                        "Skipping asset %s %s, as does not have series/season data",
                        episode_title,
                        episode_asset_id,
                    )
                    continue
                asset_dict[episode_asset_id.strip()] = f"{catalogue_id.strip()}, {num}"
                print(f"Added {episode_asset_id} and {catalogue_id} to dict")

            # Clean up and check for valid entries
            json_dct = None
            if len(asset_dict) == 0:
                LOGGER.warning(
                    "Skipping: No items retrieved from JSON catalogue for %s.",
                    title_retrieve,
                )
                continue

            # Iterate asset_dict, using show_asset_id to identify season/series data
            LOGGER.info(
                "%s new assets found from metadata JSON retrieval: %s",
                len(asset_dict),
                catalogue_path,
            )
            for ep_asset_id, cat_details in asset_dict.items():
                cat_deets = cat_details.split(",")
                ep_cat_id = cat_deets[0]
                ep_num = cat_deets[-1]

                # Fetch all assetIDs to build folders
                episode_dct = fetch(cat_id, "asset", ep_asset_id, "")
                episode_cat_dct = fetch(cat_id, "cat_asset", ep_cat_id, "")
                season_asset_id, series_asset_id = retrieve_dct_data(episode_dct)
                episode_folder = f"episode_{ep_num.strip()}_{ep_asset_id}"
                print(f"************ EPISODE FOLDER: {episode_folder}")
                print(f"SERIES ID: {series_asset_id}, SEASON_ID: {season_asset_id}")
                print(episode_dct)
                print("********************")
                print(episode_cat_dct)

                # Series data
                if not series_asset_id:
                    LOGGER.warning(
                        "Skipping: Series ID absent for episode asset %s", ep_asset_id
                    )
                    continue
                series_dct = fetch(cat_id, "asset", series_asset_id, "")
                series_title = get_series_title(series_dct)
                if not series_title:
                    series_title = episode_dct["related"][1]["title"]
                series_folder = f"{series_title}_{series_asset_id}"
                print(f"SERIES TITLE: {series_title}, SERIES_FOLDER: {series_folder}")

                # Season data
                if not season_asset_id:
                    LOGGER.warning(
                        "Skipping: Season ID absent for episode asset %s", ep_asset_id
                    )
                    continue
                season_dct = fetch(cat_id, "asset", season_asset_id, "")
                season_num = season_dct["number"]
                season_folder = f"season_{season_num}_{season_asset_id}"
                print(f"SEASON_FOLDER: {season_folder}")

                # Create path to new episode
                series_path = os.path.join(storage_path, series_folder)
                season_path = os.path.join(series_path, season_folder)
                episode_path = os.path.join(season_path, episode_folder)
                if not os.path.exists(episode_path):
                    LOGGER.info("* New episode to be added: %s", episode_path)
                    os.makedirs(episode_path, mode=0o777, exist_ok=True)

                # Check for all JSON contents
                series_json = os.path.join(
                    series_path, f"series_{series_asset_id}.json"
                )
                if not os.path.exists(series_json):
                    LOGGER.info("New Series JSON: %s", f"series_{series_asset_id}.json")
                    json_dump(series_json, series_dct)
                season_json = os.path.join(
                    season_path, f"season_{season_asset_id}.json"
                )
                if not os.path.exists(season_json):
                    LOGGER.info("New Season JSON: %s", f"season_{season_asset_id}.json")
                    json_dump(season_json, season_dct)
                episode_json = os.path.join(episode_path, f"episode_{ep_asset_id}.json")
                if not os.path.exists(episode_json):
                    LOGGER.info(
                        "New Episode AssetID JSON: %s", f"episode_{ep_asset_id}.json"
                    )
                    json_dump(episode_json, episode_dct)
                episode_cat_json = os.path.join(
                    episode_path, f"episode_catalogue_{ep_cat_id}.json"
                )
                if not os.path.exists(episode_cat_json):
                    LOGGER.info(
                        "New Episode catalogue JSON: %s",
                        f"episode_catalogue_{ep_cat_id}.json",
                    )
                    json_dump(episode_cat_json, episode_cat_dct)
                contributors_json = os.path.join(
                    episode_path, f"contributors_{ep_asset_id}.json"
                )
                if not os.path.exists(contributors_json):
                    contributors_dct = fetch(cat_id, "contributors", ep_asset_id, "")
                    if len(contributors_dct["item"]) >= 1:
                        LOGGER.info("New Contributors JSON: %s", contributors_json)
                        json_dump(contributors_json, contributors_dct)
        else:
            for asset in items:
                episode_title, catalogue_id, num, episode_asset_id = get_cat_assets(
                    asset
                )
                print(episode_title, catalogue_id, episode_asset_id)
                if num:
                    print(
                        f"NO MONOGRAPH: Skipping asset {episode_title}, 'episode' data in 'meta'"
                    )
                    LOGGER.info(
                        "Skipping asset %s %s, has series/season data",
                        episode_title,
                        episode_asset_id,
                    )
                    continue
                asset_dict[episode_asset_id.strip()] = (
                    f"{catalogue_id.strip()}, {episode_title}"
                )
                print(f"Added {episode_asset_id} and {catalogue_id} to dict")

            # Clean up and check for valid entries
            json_dct = None
            if len(asset_dict) == 0:
                LOGGER.warning(
                    "Skipping: No items retrieved from JSON catalogue for %s.",
                    title_retrieve,
                )
                continue

            # Iterate asset_dict, using show_asset_id to identify season/series data
            LOGGER.info(
                "%s new assets found from metadata JSON retrieval: %s",
                len(asset_dict),
                catalogue_path,
            )
            for ep_asset_id, cat_details in asset_dict.items():
                cat_deets = cat_details.split(",")
                ep_cat_id = cat_deets[0]
                ep_num = cat_deets[-1]

                LOGGER.info("Monographic item found: %s", title)
                # Fetch all assetIDs to build folder
                episode_dct = fetch(cat_id, "asset", ep_asset_id, "")
                episode_cat_dct = fetch(cat_id, "cat_asset", ep_cat_id, "")

                if not title:
                    title = episode_dct["title"]
                episode_folder = f"{title.strip().replace(' ', '_')}_{ep_asset_id}"

                # Create path to new episode
                mono_path = os.path.join(storage_path, episode_folder)
                if not os.path.exists(mono_path):
                    LOGGER.info("* New episode to be added: %s", mono_path)
                    os.makedirs(mono_path, mode=0o777, exist_ok=True)

                # Check for all JSON contents
                mono_json = os.path.join(mono_path, f"monographic_{ep_asset_id}.json")
                if not os.path.exists(mono_json):
                    LOGGER.info(
                        "New Monographic AssetID JSON: %s", f"mono_{ep_asset_id}.json"
                    )
                    json_dump(mono_json, episode_dct)
                mono_cat_json = os.path.join(
                    mono_path, f"mono_catalogue_{ep_cat_id}.json"
                )
                if not os.path.exists(mono_cat_json):
                    LOGGER.info(
                        "New Episode catalogue JSON: %s",
                        f"episode_catalogue_{ep_cat_id}.json",
                    )
                    json_dump(mono_cat_json, episode_cat_dct)
                contributors_json = os.path.join(
                    mono_path, f"contributors_{ep_asset_id}.json"
                )
                if not os.path.exists(contributors_json):
                    contributors_dct = fetch(cat_id, "contributors", ep_asset_id, "")
                    if len(contributors_dct["item"]) > 1:
                        LOGGER.info(
                            "New Contributors JSON: %s",
                            f"contributors_{ep_asset_id}.json",
                        )
                        json_dump(contributors_json, contributors_dct)

    LOGGER.info(
        "========== Fetch augmented metadata script ENDED ================================================"
    )


def retrieve_dct_data(dct=None) -> tuple[Optional[str], Optional[str]]:
    """
    Check if DCT data is None, if not retrieve season/series IDs
    """
    if dct is None:
        dct = {}
    try:
        first_type = dct["related"][0]["type"]
        season_id = dct["related"][0]["id"]
    except (TypeError, IndexError, KeyError):
        first_type = ""
        season_id = ""
    try:
        second_type = dct["related"][1]["type"]
        series_id = dct["related"][1]["id"]
    except (TypeError, IndexError, KeyError):
        second_type = ""
        series_id = ""
    if "season" in first_type and "series" in second_type:
        return season_id, series_id
    if "season" in second_type and "series" in first_type:
        return series_id, season_id
    return None, None


if __name__ == "__main__":
    main()
