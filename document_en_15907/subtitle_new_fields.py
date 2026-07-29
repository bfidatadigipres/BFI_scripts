import os
import sys
from datetime import datetime, timedelta

sys.path.append(os.environ["CODE"])
import utils
import adlib_v3 as adlib
import adlib_v3_sess as adlib_sess
import shutil

CID_API = os.environ["CID_API3"]
SUBTITLE_FOLDER = os.path.join(
    os.environ.get("ADMIN"), "off_air_tv/subtitles_not_in_cid"
)


def main():
    object_number_list = []
    # iterate through folder
    list_files = os.listdir(SUBTITLE_FOLDER)[5:10]
    # get object number
    for file in list_files:
        object_number = utils.get_object_number(file)
        # print(f"object_number={object_number} Filename: {file}")
        object_search = f"object_number='{object_number}'"
        _, item_record = adlib.retrieve_record(
            CID_API, "items", object_search, "1", fields=None
        )
        if item_record is None:
            print("orginal search failed, trying new search with different title")
            continue
        # print(f"record: {record[0]}")
        item_priref = adlib.retrieve_field_name(item_record[0], "priref")
        print(item_priref)
        object_number_list.append(item_priref[0])
        search_manifest = f"priref='{item_priref[0]}'"
        _, item_manifest_record = adlib.retrieve_record(
            CID_API, "items", search_manifest, "1", fields=None
        )
        if item_manifest_record is None:
            print("orginal search failed")
            continue
        # print(f"\n mani record: {record_items}")
        mani_priref = adlib.retrieve_field_name(
            item_manifest_record[0], "part_of_reference.lref"
        )[0]
        print(f"manifestation priref: {mani_priref}")
        manifestation_priref_search = f"priref='{mani_priref}'"
        _, record_manifestation = adlib.retrieve_record(
            CID_API,
            "items",
            manifestation_priref_search,
            "1",
            fields=[
                "transmission_date",
                "transmission_end_time",
                "transmission_start_time",
            ],
        )
        print(f"\n {record_manifestation}")
        transmission_dates = adlib.retrieve_field_name(
            record_manifestation[0], "transmission_date"
        )
        if not transmission_dates:
            print(f"Missing transmission_date for {file}")
            continue
        transmission_date = transmission_dates[0]
        transmission_end_time = adlib.retrieve_field_name(
            record_manifestation[0], "transmission_end_time"
        )[0]
        transmission_start_time = adlib.retrieve_field_name(
            record_manifestation[0], "transmission_start_time"
        )[0]
        # convert to datetime
        time_format = "%H:%M:%S"
        date_format = "%Y-%m-%d"
        end_time = datetime.strptime(transmission_end_time, time_format)
        start_time = datetime.strptime(transmission_start_time, time_format)
        if end_time < start_time:
            print("Show ran past midnight!!")
            date_datetime = datetime.strptime(
                transmission_date, date_format
            ) + timedelta(days=1)
            transmission_date = date_datetime.strftime(date_format)

        file_vtt = os.path.join(SUBTITLE_FOLDER, file)
        with open(file_vtt, encoding="utf-8") as webvtt_file:
            webvtt_payload = webvtt_file.read()
        # subtitle.type, subtitle.date, subtitle.text
        item_edit_data = [
            {"edit.date": str(datetime.now())[0:10]},
            {"edit.name": "datadigipres"},
            {"edit.notes": "Automated subtitle relocation project"},
            {"edit.time": str(datetime.now())[11:19]},
            {"subtitle.date": transmission_date},
            {"subtitle.text": webvtt_payload},
            {"subtitle.type": "WEBVTT_C"},
        ]
        edit_xml = adlib.create_grouped_data(item_priref[0], "Edit", [item_edit_data])
        print(f"xml: {edit_xml}")

        try:
            post_resp = adlib_sess.post(
                CID_API, edit_xml, "items", "updaterecord", None
            )
        except Exception as err:
            if hasattr(err, "__cause__"):
                print(f"Cause: {err.__cause__}")
            if hasattr(err, "last_attempt"):
                # tenacity-style RetryError
                print(f"Underlying exception: {err.last_attempt.exception()}")
            print(err)

        # move file to subtitles/ folder
        # shutil.move(file_vtt, #Use os.environ.get())

    print(object_number_list)

    # print(f"webvtt payload: {webvtt_payload}")


if __name__ == "__main__":
    main()
