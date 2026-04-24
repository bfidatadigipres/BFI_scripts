import pandas as pd
import csv
import json
import datetime
import yaml
import pandas as pd
from zoneinfo import ZoneInfo
from datetime import datetime, timezone
import csv
import datetime
import os
import sys

sys.path.append(os.environ["CODE"])
import adlib_v3 as adlib

CODE_PATH = os.environ["CODE"]
GENRE_MAP = os.path.join(CODE_PATH, "document_en_15907/EPG_genre_mapping.yaml")
SUBS_PTH = os.environ["SUBS_PATH2"]
GENRE_PTH = SUBS_PTH.split("subtitles_not_in_cid/")[0]


def get_description(info_json):
    description = []
    d_desc = {}
    try:
        d_short = info_json["item"][0]["summary"]["short"]
        d_short = d_short.replace("\xe2\x80\x99", "'").replace("\xe2\x80\x93", "-")
        description.append(d_short)
        d_desc["d_short"] = d_short
    except (IndexError, KeyError, TypeError) as err:
        print(err)
    try:
        d_medium = info_json["item"][0]["summary"]["medium"]
        d_medium = d_medium.replace("\xe2\x80\x99", "'").replace("\xe2\x80\x93", "-")
        description.append(d_medium)
        d_desc["d_medium"] = d_medium
    except (IndexError, KeyError, TypeError) as err:
        print(err)
    try:
        d_long = info_json["item"][0]["summary"]["long"]
        d_long = d_long.replace("\xe2\x80\x99", "'").replace("\xe2\x80\x93", "-")
        description.append(d_long)
        d_desc["d_long"] = d_long
    except (IndexError, KeyError, TypeError) as err:
        print(err)

    # Sorts to longest first which populates description var
    description.sort(key=len, reverse=True)
    print(description)
    if len(description) > 0:
        description = description[0]
    else:
        description = ""
    return description, d_desc


def genre_retrieval(category_code, description, title):
    """
    Retrieve genre data, return as list
    """
    with open(GENRE_MAP, "r", encoding="utf8") as files:
        data = yaml.load(files, Loader=yaml.FullLoader)
        print(
            f"genre_retrieval(): The genre data is being retrieved for: {category_code}"
        )
    for _ in data:
        if category_code in data["genres"]:
            genre_one = {}
            genre_two = {}
            subject_one = {}
            subject_two = {}

            genre_one = data["genres"][category_code.strip("u")]["Genre"]
            print(f"genre_retrieval(): Genre one: {genre_one}")
            if "Undefined" in str(genre_one):
                print(
                    f"genre_retrieval(): Undefined category_code discovered: {category_code}"
                )
                with open(
                    os.path.join(GENRE_PTH, "redux_undefined_genres.txt"), "a"
                ) as genre_log:
                    genre_log.write("\n")
                    genre_log.write(
                        f"Category: {category_code}     Title: {title}     Description: {description}"
                    )
                genre_one_priref = ""
            else:
                for _, val in genre_one.items():
                    genre_one_priref = val
                print(
                    f"genre_retrieval(): Key value for genre_one_priref: {genre_one_priref}"
                )
            try:
                genre_two = data["genres"][category_code.strip("u")]["Genre2"]
                for _, val in genre_two.items():
                    genre_two_priref = val
            except (IndexError, KeyError):
                genre_two_priref = ""

            try:
                subject_one = data["genres"][category_code.strip("u")]["Subject"]
                for _, val in subject_one.items():
                    subject_one_priref = val
                print(
                    f"genre_retrieval(): Key value for subject_one_priref: {subject_one_priref}"
                )
            except (IndexError, KeyError):
                subject_one_priref = ""

            try:
                subject_two = data["genres"][category_code.strip("u")]["Subject2"]
                for _, val in subject_two.items():
                    subject_two_priref = val
                print(
                    f"genre_retrieval(): Key value for subject_two_priref: {subject_two_priref}"
                )
            except (IndexError, KeyError):
                subject_two_priref = ""

            return [
                genre_one_priref,
                genre_two_priref,
                subject_one_priref,
                subject_two_priref,
            ]

        else:
            print(f"{category_code} -- New category not in EPG_genre_map.yaml: {title}")
            with open(
                os.path.join(GENRE_PTH, "redux_undefined_genres.txt"), "a"
            ) as genre_log:
                genre_log.write("\n")
                genre_log.write(
                    f"Category: {category_code}     Title: {title}     Description: {description}"
                )
            return []


if __name__ == "__main__":
    file = sys.argv[1]
    output_file = sys.argv[2]
    results_csv = []
    results = {}
    descriptions_results = {}
    amend_df = pd.read_csv(file)
    df = amend_df[
        [
            "filepath",
            "priref",
            "title.article",
            "title",
            "title.language",
            "title.type",
            "title.article_cid",
            "title_cid",
            "title.language_cid",
            "title.type_cid",
        ]
    ]
    for index, row in df.iterrows():
        # print(f"index: {index}")
        # print(f"row: {row}")
        print(row["priref"])
        df_row_dict = row[
            [
                "title.article",
                "title",
                "title.language",
                "title.type",
                "title.article_cid",
                "title_cid",
                "title.language_cid",
                "title.type_cid",
            ]
        ].to_dict()
        df_row_dict = {k: "" if pd.isna(v) else v for k, v in df_row_dict.items()}

        search = f'priref="{row["priref"]}"'
        hit, record = adlib.retrieve_record(
            os.environ.get("CID_API4"), "manifestations", search, "1"
        )
        part_of_reference_result = adlib.retrieve_field_name(
            record[0], "part_of_reference.lref"
        )
        search = f'priref="{part_of_reference_result[0]}"'
        hit, work_record = adlib.retrieve_record(
            os.environ.get("CID_API4"), "works", search, "1"
        )
        if work_record == None:
            work_description_type, work_description, work_description_date = "", "", ""

        else:
            work_description_type = adlib.retrieve_field_name(
                work_record[0], "description.type"
            )
            work_description = adlib.retrieve_field_name(work_record[0], "description")
            if work_description == []:
                work_description = ""
            work_description_date = adlib.retrieve_field_name(
                work_record[0], "description.date"
            )
            print(work_description_date)
            print(work_record[0])

        with open(row["filepath"], "r") as file:
            info_json = json.load(file)
            description, d_desc = get_description(info_json)
            print(f"description: {description}")
            episode_total = (
                info_json.get("item")[0].get("asset").get("meta").get("episodeTotal")
            )
            episode_number = (
                info_json.get("item")[0].get("asset").get("meta").get("episode")
            )
            series_part_unit_value = (
                info_json.get("item")[0].get("asset").get("related")
            )
            if series_part_unit_value == []:
                series_part_unit_value = None
            else:
                series_part_unit_value = (
                    info_json.get("item")[0]
                    .get("asset")
                    .get("related")[0]
                    .get("number")
                )
            asset_title = info_json.get("item")[0].get("asset").get("title")
            if asset_title is None:
                asset_title = ""
            category_codes = []
            try:
                category_code_one = (
                    info_json.get("item")[0].get("asset").get("category")[0].get("code")
                )
                category_codes.append(category_code_one)
            except (IndexError, KeyError, TypeError) as err:
                print(err)
            try:
                category_code_two = (
                    info_json.get("item")[0].get("asset").get("category")[1].get("code")
                )
                category_codes.append(category_code_two)
            except (IndexError, KeyError, TypeError) as err:
                print(err)
            category_codes.sort(key=len, reverse=True)
            if len(category_codes) > 1:
                category_code = category_codes[0]
            else:
                category_code = category_codes

            if episode_number is None:
                episode_number = info_json.get("item")[0].get("asset").get("number")
                episode_total = info_json.get("item")[0].get("asset").get("total")
            if "d_short" in d_desc:
                descriptions_results.update(
                    {
                        "label.type_short": "EPGSHORT",
                        "label.date_short": str(datetime.datetime.now())[:10],
                        "label.source_short": "EBS augmented EPG supply",
                        "label.text_short": d_desc["d_short"],
                    }
                )

            else:
                descriptions_results.update(
                    {
                        "label.type_short": "",
                        "label.date_short": "",
                        "label.source_short": "",
                        "label.text_short": "",
                    }
                )

            if "d_medium" in d_desc:
                descriptions_results.update(
                    {
                        "label.type_med": "EPGMEDIUM",
                        "label.date_med": str(datetime.datetime.now())[:10],
                        "label.source_med": "EBS augmented EPG supply",
                        "label.text_med": d_desc["d_medium"],
                    }
                )
            else:
                descriptions_results.update(
                    {
                        "label.type_med": "",
                        "label.date_med": "",
                        "label.source_med": "",
                        "label.text_med": "",
                    }
                )

            if "d_long" in d_desc:
                descriptions_results.update(
                    {
                        "label.type_long": "EPGLONG",
                        "label.date_long": str(datetime.datetime.now())[:10],
                        "label.source_long": "EBS augmented EPG supply",
                        "label.text_long": d_desc["d_long"],
                    }
                )
            else:
                descriptions_results.update(
                    {
                        "label.type_long": "",
                        "label.date_long": "",
                        "label.source_long": "",
                        "label.text_long": "",
                    }
                )
            print(f"category code: {category_code}")
            print(f"description: {description}")
            print(f"asset title: {asset_title}")
            if category_code == []:
                print(f"There's no category_code")
                genre_outcome = None
                (
                    genre_one_priref,
                    genre_two_priref,
                    subject_one_priref,
                    subject_two_priref,
                ) = ("", "", "", "")

            else:
                genre_outcome = genre_retrieval(category_code, description, asset_title)

            if genre_outcome is None or genre_outcome == []:
                print(f"category_code: {category_code} not in epg_genre_map")
                (
                    genre_one_priref,
                    genre_two_priref,
                    subject_one_priref,
                    subject_two_priref,
                ) = ("", "", "", "")
            else:
                (
                    genre_one_priref,
                    genre_two_priref,
                    subject_one_priref,
                    subject_two_priref,
                ) = genre_retrieval(category_code, description, asset_title)

            results_one = {"priref": part_of_reference_result[0]}
            results = {
                "part_unit.value": episode_number,
                "part_unit.valuetotal": episode_total,
                "part_unit": "EPISODE",
                "part_unit.value_series": series_part_unit_value,
                "part_unit.valuetotal_series": "",
                "part_unit_series": "SERIES",
                "content.genre.lref": genre_one_priref,
                "content.genre.lref_2": genre_two_priref,
                "content.subject.lref": subject_one_priref,
                "content.subject.lref_2": subject_two_priref,
            }
            if episode_number is None:
                results.update(
                    {
                        "part_unit.value": "",
                        "part_unit.valuetotal": "",
                        "part_unit": "",
                    }
                )
            if series_part_unit_value is None:
                results.update(
                    {
                        "part_unit.value_series": "",
                        "part_unit.valuetotal_series": "",
                        "part_unit_series": "",
                    }
                )

            results_two = {
                "description.type": "Synopsis",
                "description.date": str(datetime.datetime.now())[:10],
                "description": description,
                "description.type_work": (
                    work_description_type[0]
                    if type(work_description_type) == list
                    else ""
                ),
                "description_work": (
                    work_description[0] if work_description != "" else ""
                ),
                "description_date_work": (
                    work_description_date[0]
                    if type(work_description_date) == list
                    else ""
                ),
            }
            if description == "":
                results_two.update(
                    {"description.type": "", "description.date": "", "description": ""}
                )
            # results.update(descriptions_results)
            # print(df_dict[0])
            final = {
                **results_one,
                **(df_row_dict),
                **results_two,
                **descriptions_results,
                **results,
            }
            print(f"Final: {final}")
            results_csv.append(final)

    # print(results_csv[0].keys())
    with open(output_file, "w+", newline="") as file:
        fieldnames = results_csv[0].keys()
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results_csv)
