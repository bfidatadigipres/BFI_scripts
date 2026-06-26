#!/usr/bin/ python3

"""
Script to retrieve folders of
Film Fund different audio files named
after CID Item record object_number.

1. Looks for audio processing folders in FF_STORAGE
   and find list of subfolders within, add to list
   and iterate through the folder names
2. Extract object number from folder name
   and processes single files within (not multiple files)
3. Extracts enclosed file name and completes stages:
   a/ Build dictionary for new Item record
   b/ Convert to XML using adlib_v3
   c/ Push data to CID to create item record
   d/ Rename the file one by one with partwhole 01of01
      and move to autoingest
   e/ Update all digital.acquired_filenames to CID item
      record and append quality_comments also
4. When all files in a folder processed the
   folder is checked as empty and deleted

2026
"""

# Public packages
import os
import sys
import ffmpeg
import shutil
import logging
import datetime
from typing import Any, Iterable, Final, Optional

# Local packages
sys.path.append(os.environ.get("CODE"))
import adlib_v3 as adlib
import utils

# Global variables
LOGS: Final = os.environ.get("LOG_PATH")
CONTROL_JSON: Final = os.path.join(LOGS, "downtime_control.json")
FF_STORAGE: Final = os.path.join(os.environ.get("QNAP_11"))
AUTOINGEST: Final = os.path.join(
    os.environ.get("AUTOINGEST_QNAP11"), "ingest/autodetect/"
)

STORAGE: Final = os.path.join(FF_STORAGE, 'automation/audio_description')
CID_API = utils.get_current_api()

# Setup logging
LOGGER = logging.getLogger("document_augmented_film_fund_separate_audio")
HDLR = logging.FileHandler(
    os.path.join(LOGS, "document_augmented_film_fund_separate_audio.log")
)
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def cid_check_ob_num(object_number: str) -> Optional[dict[str, Optional[Any]]]:
    """
    Looks up object_number and retrieves title
    and other data for new separate 5.1 audio record
    """
    search = f"object_number='{object_number}'"
    hits, record = adlib.retrieve_record(CID_API, "items", search, "0")
    if hits is None:
        raise Exception(f"CID API was unreachable for Items search: {search}")
    if hits == 0:
        return None
    return record


def main():
    """
    Search for folders named after CID item records
    Check for contents and create new CID item record
    for each audio file within. Rename and move for ingest.
    """
    if not utils.check_control("pause_scripts"):
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")

    folders = [x for x in os.listdir(STORAGE) if os.path.isdir(os.path.join(STORAGE, x))]
    if not folders:
        sys.exit("No folders found at this time")

    LOGGER.info(
        "== Document augmented film fund separate audio start ==================="
    )
    for folder in folders:
        if not utils.check_control("pause_scripts"):
            LOGGER.info(
                "Script run prevented by downtime_control.json. Script exiting."
            )
            sys.exit("Script run prevented by downtime_control.json. Script exiting.")

        if not utils.check_storage(AUTOINGEST):
            LOGGER.info("Skipping path - prevented by storage_control.json.")
            continue
        if not utils.cid_check(CID_API):
            LOGGER.critical("* Cannot establish CID session, exiting script")
            sys.exit("* Cannot establish CID session, exiting script")

        fpath = os.path.join(STORAGE, folder)
        if not os.path.exists(fpath):
            LOGGER.warning("Folder path is not valid: %s", fpath)
            continue

        object_number = folder
        file_list = os.listdir(fpath)
        if not file_list:
            LOGGER.warning("Skipping. No files found in folder path: %s", fpath)
            continue
        if len(file_list) != 1:
            LOGGER.warning("More than one file found... problem?")
            continue
        wav_type = ""
        file = file_list[0]
        ext = file.split(".")[-1]
        filepath = os.path.join(fpath, file)
        if ext.lower() != "wav":
            LOGGER.warning("File found is not WAV file. Skipping")
            continue
        mdata = ffmpeg.probe(filepath)
        if not len(mdata.get("streams")) == 1:
            LOGGER.warning("File has more than one stream. Skipping")
            continue
        channels = mdata.get("streams")[0].get("channels")
        if int(channels) == 1:
            wav_type = "mono"
        elif int(channels) == 2:
            wav_type = "stereo"

        if not wav_type:
            LOGGER.warning("No WAV type identified, skipping file")
            continue

        LOGGER.info("File being processed %s in folder %s", file, folder)

        # Check object number valid
        record = cid_check_ob_num(object_number)
        if record is None:
            LOGGER.warning(
                "Skipping: Record could not be matched with object_number"
            )
            continue

        source_priref = adlib.retrieve_field_name(record[0], "priref")[0]
        if not source_priref:
            continue
        print(f"Priref matched with retrieved folder name: {source_priref}")
        LOGGER.info(
            "Priref %s matched with folder name: %s", source_priref, folder
        )

        # Create CID item record for mono/stereo audio files in folder
        item_record = create_new_item_record(source_priref, wav_type, record, ext)
        if item_record is None:
            continue

        print(item_record)
        new_priref = adlib.retrieve_field_name(item_record, "priref")[0]
        new_ob_num = adlib.retrieve_field_name(item_record, "object_number")[0]
        LOGGER.info("** CID Item record created: %s - %s", new_priref, new_ob_num)
        print(f"CID Item record created: {new_priref}, {new_ob_num}")

        # Rename file and move to autoingest
        new_file = f"{new_ob_num.replace("-", "_")}_01of01.{ext}"
        new_filepath = os.path.join(fpath, new_file)
        success = rename_or_move("rename", filepath, new_filepath)
        if success is False:
            if not os.path.exists(new_filepath):
                LOGGER.warning("File was not renamed successfully. Manual assistance needed.")
                continue
        elif success == "Path error":
            LOGGER.warning("Path error: %s", os.path.join(filepath, new_filepath))
            continue
        LOGGER.info("File successfully renamed. Moving to %s ingest path", AUTOINGEST)
        move_success = rename_or_move(
            "move", new_filepath, os.path.join(AUTOINGEST, new_file)
        )
        if move_success is False:
            LOGGER.warning(
                "Error with file move to autoingest, leaving in place for manual assistance"
            )
        elif move_success is True:
            LOGGER.info(
                "File %s successfully moved to ingest path: %s\n",
                new_file,
                AUTOINGEST,
            )
        elif move_success == "Path error":
            LOGGER.warning("Manual help needed: Path error %s", new_filepath)
            continue

        # Write all dict names to digital.acquired_filename in CID item record
        success = create_digital_original_filenames(new_priref, file)
        if not success:
            LOGGER.warning(
                "Skipping further actions. Digital acquired filenames not written to CID item record: %s",
                new_priref,
            )
            continue
        LOGGER.info(
            "Digital Acquired Filename data added to CID item record %s", new_priref
        )

        # Write quality comments to new CID item record
        if wav_type == "mono":
            qual_comm = (
                    "Mono audio supplied separately as WAV PCM file."
                )
        elif wav_type == "stereo":
            qual_comm = "Stereo audio supplied separately as WAV PCM file."
        else:
            qual_comm = ""
        success = adlib.add_quality_comments(CID_API, new_priref, qual_comm)
        if not success:
            LOGGER.warning(
                "Quality comments were not written to record: %s", new_priref
            )
        LOGGER.info("Quality comments added to CID item record %s", new_priref)

        # Check fpath is empty and delete
        if len(os.listdir(fpath)) == 0:
            LOGGER.info("All files processed in folder: %s", object_number)
            LOGGER.info("Deleting empty folder: %s", fpath)
            os.rmdir(fpath)
        else:
            LOGGER.warning(
                "Leaving folder %s in place as files still remaining in folder %s",
                object_number,
                os.listdir(fpath),
            )

    LOGGER.info(
        "== Document augmented film fund separate audio end =====================\n"
    )


def rename_or_move(arg: str, file_a: str, file_b: str) -> str | bool:
    """
    Use shutil or os to move/rename
    from file a to file b. Verify change
    before confirming success/failure
    """

    if not os.path.isfile(file_a):
        return "Path error"

    if arg == "move":
        try:
            shutil.move(file_a, file_b)
        except Exception as err:
            LOGGER.warning(
                "rename_or_move(): Failed to %s file to new destination: \n%s\n%s",
                arg,
                file_a,
                file_b,
            )
            print(err)
            return False

    if arg == "rename":
        try:
            os.rename(file_a, file_b)
        except Exception as err:
            LOGGER.warning(
                "rename_or_move(): Failed to %s file to new destination: \n%s\n%s",
                arg,
                file_a,
                file_b,
            )
            print(err)
            return False

    if os.path.isfile(file_b):
        return True
    return False


def make_item_record_dict(
    priref: str, source: str, record: list[dict[str, Optional[Any]]], ext: str
) -> Iterable[dict[str, str]]:
    """
    Get CID item record for source and borrow data
    for creation of new CID item record
    """

    item = [
        {"input.name": "datadigipres"},
        {"input.date": str(datetime.datetime.now())[:10]},
        {"input.time": str(datetime.datetime.now())[11:19]},
        {
            "input.notes": f"Film Fund - automated bulk documentation for separate audio"
        }
    ]
    item.append({"record_type": "ITEM"})
    item.append({"item_type": "DIGITAL"})
    item.append({"copy_status": "M"})
    item.append({"copy_usage.lref": "131560"})
    item.append({"accession_date": str(datetime.datetime.now())[:10]})

    if "Title" in str(record):
        title = adlib.retrieve_field_name(record[0], "title")[0]
        if source == "mono":
            item.append({"title": f"{title} (mono audio)"})
        elif source == "stereo":
            item.append({"title": f"{title} (stereo audio)"})

        if adlib.retrieve_field_name(record[0], "title_article")[0]:
            item.append(
                {
                    "title.article": adlib.retrieve_field_name(
                        record[0], "title_article"
                    )[0]
                }
            )
        item.append({"title.language": "English"})
        item.append({"title.type": "05_MAIN"})
    else:
        LOGGER.warning("No title data retrieved. Aborting record creation")
        return None
    if "Part_of" in str(record):
        parent_priref = adlib.retrieve_field_name(
            record[0]["Part_of"][0]["part_of_reference"][0], "priref"
        )[0]
        item.append({"part_of_reference.lref": parent_priref})
    else:
        LOGGER.warning("No part_of_reference data retrieved. Aborting record creation")
        return None
    item.append({"related_object.reference.lref": priref})
    if source == "mono":
        item.append({"related_object.notes": "Mono audio for"})
    elif source == "stereo":
        item.append({"related_object.notes": "Stereo audio for"})

    item.append({"file_type.ref": "99837"})
    item.append({"code_type": "99837"})
    item.append({"track_type": "PCM"})
    if "acquisition.date" in str(record):
        item.append(
            {
                "acquisition.date": adlib.retrieve_field_name(
                    record[0], "acquisition.date"
                )[0]
            }
        )
    if "acquisition.method" in str(record):
        item.append(
            {
                "acquisition.method": adlib.retrieve_field_name(
                    record[0], "acquisition.method"
                )[0]
            }
        )
    item.append({"acquisition.source.lref": "143463"})
    item.append({"acquisition.source.type": "DONOR"})
    item.append(
        {
            "access_conditions": "Access requests for this collection are subject to an approval process. "
            "Please raise a request via the Collections Systems Service Desk, describing your specific use."
        }
    )
    item.append({"access_conditions.date": str(datetime.datetime.now())[:10]})
    if "grouping" in str(record):
        item.append({"grouping": adlib.retrieve_field_name(record[0], "grouping")[0]})
    if "language" in str(record):
        item.append({"language": adlib.retrieve_field_name(record[0], "language")[0]})
        item.append(
            {"language.type": adlib.retrieve_field_name(record[0], "language.type")[0]}
        )

    return item


def create_digital_original_filenames(
    priref: str, asset_list_dct: dict[Any, Any]
) -> bool:
    """
    Create entries for digital.acquired_filename
    and append to the CID item record.
    """
    payload = f"<adlibXML><recordList><record priref='{priref}'>"
    for key, val in asset_list_dct.items():
        filename = f"{key} - Renamed to: {val}"
        LOGGER.info("Writing to digital.acquired_filename: %s", filename)
        pay_mid = f"<Acquired_filename><digital.acquired_filename>{filename}</digital.acquired_filename><digital.acquired_filename.type>FILE</digital.acquired_filename.type></Acquired_filename>"
        payload = payload + pay_mid

    pay_edit = f"<Edit><edit.name>datadigipres</edit.name><edit.date>{str(datetime.datetime.now())[:10]}</edit.date><edit.time>{str(datetime.datetime.now())[11:19]}</edit.time><edit.notes>Film Fund digital acquired filename update</edit.notes></Edit>"
    payload_end = "</record></recordList></adlibXML>"
    payload = payload + pay_edit + payload_end

    LOGGER.info("** Appending digital.acquired_filename data to item record now...")
    LOGGER.info(payload)

    try:
        result = adlib.post(CID_API, payload, "items", "updaterecord")
        print(f"Item appended successful! {priref}\n{result}")
        LOGGER.info(
            "Successfully appended digital.acquired_filenames to Item record %s", priref
        )
        print(result)
        return True
    except Exception as err:
        print(err)
        LOGGER.warning(
            "Failed to append digital.acquired_filenames to Item record %s", priref
        )
        print(f"CID item record append FAILED!! {priref}")
        return False


def create_new_item_record(
    priref: str, wav_type: str, record: dict[str, Optional[Any]], ext: str
):
    """
    Build new CID item record from existing data and make CID item record
    """
    item_dct = make_item_record_dict(priref, wav_type, record, ext)
    LOGGER.info(item_dct)
    item_xml = adlib.create_record_data(CID_API, "items", "", item_dct)
    new_record = adlib.post(CID_API, item_xml, "items", "insertrecord")
    if new_record is None:
        LOGGER.warning("Skipping: CID item record creation failed: %s", item_xml)
        return None
    LOGGER.info("New CID item record created: %s", new_record)
    return new_record


if __name__ == "__main__":
    main()
