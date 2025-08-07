#!/usr/bin/ python3

"""
Script to receive IMP
Netflix folder named with UID.
UID placed into Item record
digital.acquired_filename field
of CID item record.

Receives IMP folder complete with
MXF video and audio files, and XML
metadata files.

1. Searches STORAGE for 'rename_netflix'
   folders and checks for items with
   content returning as a folder_list.
2. Iterates, finds match for IMP folder name
   within folder, from CID item
   record, ie 'N-123456'. Creates N_123456_
   filename prefix
3. Opens each XML in folder looking
   for string match to '<PackingList'
4. Iterates <PackingList><AssetList><Asset>
   blocks extracting <OriginalFilmName language='en'>
   into list of items
5. Numbers each following order retrieved,
   ie, if 6 assets 'N_123456_01of06' for first item
   and 'N_123456_06of06' for last item, adds to dict.
   Checks same amount of items in PKL as in folder.
6. Iterates dictionary adding original filename and
   new name to CID item record 'digital.acquired_filename'
   field, which allows repeated entries. Formatting:
   "<Original Filename> - Renamed to: N_123456_01of06.mxf"
7. Open each XML and write content to the label.text
   and label.type field (possibly new field)
8. XML and MXF contents of IMP folder are renamed
   as per dictionary and moved to autoingest new
   black_pearl_ingest_netflix path (to be confirmed)
   where new put scripts ensure file is moved to
   the netflix01 bucket.

Note: Configured for adlib_v3 and will require API update

2023
"""
# Public packages
import datetime
import logging
import os
import shutil
import sys
from typing import Any, Final, Iterable, Optional

import xmltodict

# Local packages
sys.path.append(os.environ["CODE"])
import adlib_v3 as adlib
import utils

# Global variables
STORAGE_PTH: Final = os.environ.get("PLATFORM_INGEST_PTH")
NETFLIX_PTH: Final = os.environ.get("NETFLIX_PATH")
NET_INGEST: Final = os.environ.get("NETFLIX_INGEST")
AUTOINGEST: Final = os.path.join(STORAGE_PTH, NET_INGEST)
STORAGE: Final = os.path.join(STORAGE_PTH, NETFLIX_PTH)
LOGS: Final = os.environ.get("LOG_PATH")
CODE: Final = os.environ.get("CODE_PATH")
CONTROL_JSON: Final = os.path.join(LOGS, "downtime_control.json")
CID_API: Final = utils.get_current_api()

# Setup logging
LOGGER = logging.getLogger("document_augmented_netflix_renaming")
HDLR = logging.FileHandler(
    os.path.join(LOGS, "document_augmented_netflix_renaming.log")
)
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def cid_check_filename(imp_fname: str) -> tuple[str, str, str]:
    """
    Sends CID request for series_id data
    """
    search: str = f'digital.acquired_filename="{imp_fname}"'
    record: Optional[Iterable[dict[str, Any]]] = adlib.retrieve_record(
        CID_API, "items", search, "1"
    )[1]
    print(record)
    if not record:
        print(f"cid_check(): Unable to match IMP with Item record: {imp_fname}")
        LOGGER.info("Unable to match %s to digital.acquired_filename field", imp_fname)
        return None
    try:
        priref = adlib.retrieve_field_name(record[0], "priref")[0]
        print(f"cid_check(): Priref: {priref}")
    except (IndexError, KeyError, TypeError):
        priref = ""

    search = f'priref="{priref}"'
    record = adlib.retrieve_record(CID_API, "items", search, "1")[1]
    print(record)
    if not record:
        return None
    try:
        ob_num = adlib.retrieve_field_name(record[0], "object_number")[0]
        print(f"cid_check(): Object number: {ob_num}")
    except (IndexError, KeyError, TypeError):
        ob_num = ""
    try:
        file_type = adlib.retrieve_field_name(
            record[0], "digital.acquired_filename.type"
        )[0]
        print(f"cid_check(): File type: {file_type}")
    except (IndexError, KeyError, TypeError):
        file_type = ""

    return priref, ob_num, file_type.title()


def walk_netflix_folders() -> list[str]:
    """
    Collect list of folderpaths
    for files named rename_netflix
    """
    print(STORAGE)
    rename_folders: list[str] = []
    for root, dirs, _ in os.walk(STORAGE):
        for directory in dirs:
            if "rename_netflix" == directory:
                rename_folders.append(os.path.join(root, directory))
    print(f"{len(rename_folders)} rename folder(s) found")
    folder_list: list[str] = []
    for rename_folder in rename_folders:
        print(rename_folder)
        folders: list[str] = os.listdir(rename_folder)
        if not folders:
            print(f"Skipping, rename folder empty: {rename_folder}")
            continue
        for folder in folders:
            print(folder)
            fpath = os.path.join(rename_folder, folder)
            if os.path.isdir(fpath):
                folder_list.append(os.path.join(rename_folder, folder))
            else:
                LOGGER.warning(
                    "Netflix IMP renaming script. Non-folder item found in rename_netflix path: %s",
                    fpath,
                )

    return folder_list


def main():
    """
    Check watch folder for IMP folder
    look to match IMP folder name with
    CID item record.
    Where matched, process contents
    read PKL XML for part whole order
    and check contents match Asset list.
    """
    if not utils.check_control("pause_scripts"):
        LOGGER.info("Script run prevented by downtime_control.json. Script exiting.")
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")
    if not utils.check_storage(STORAGE):
        LOGGER.info("Script run prevented by storage_control.json. Script exiting.")
        sys.exit("Script run prevented by storage_control.json. Script exiting.")
    if not utils.cid_check(CID_API):
        LOGGER.critical("* Cannot establish CID session, exiting script")
        sys.exit("* Cannot establish CID session, exiting script")

    folder_list: list[str] = walk_netflix_folders()
    if len(folder_list) == 0:
        LOGGER.info("Netflix IMP renaming script. No folders found.")
        sys.exit()

    LOGGER.info("== Document augmented Netflix renaming start =================")
    for fpath in folder_list:
        folder = os.path.split(fpath)[1]
        LOGGER.info("Folder path found: %s", fpath)
        priref, ob_num, file_type = cid_check_filename(folder.strip())
        print(f"CID item record found: {priref} with matching {file_type.title()}")

        if not priref:
            LOGGER.warning("Cannot find CID Item record for this folder: %s", fpath)
            continue
        if file_type != "Folder":
            LOGGER.warning("Incorrect filename type retrieved in CID. Skipping.")
            continue

        LOGGER.info(
            "Folder matched to CID Item record: %s | %s | %s", folder, priref, ob_num
        )
        xml_list: list[str] = [
            x for x in os.listdir(fpath) if x.endswith((".xml", ".XML"))
        ]
        mxf_list: list[str] = [
            x for x in os.listdir(fpath) if x.endswith((".mxf", ".MXF"))
        ]
        all_items: list[str] = [
            x for x in os.listdir(fpath) if os.path.isfile(os.path.join(fpath, x))
        ]
        total_items: int = len(mxf_list) + len(xml_list)
        if total_items != len(all_items):
            LOGGER.warning("Folder contains files that are not XML or MXF: %s", fpath)
            continue
        packing_list: str = ""
        xml_content_all: list[str] = []
        # Read/write to CID item record, and identify the PackingList
        for xml in xml_list:
            with open(os.path.join(fpath, xml), "r") as xml_text:
                xml_content = xml_text.readlines()
            if "<PackingList" in xml_content[1]:
                packing_list = os.path.join(fpath, xml)
            lines = "".join(xml_content)
            xml_content_all.append(lines)
        print(xml_content_all)
        print(packing_list)
        success: bool = xml_item_append(priref, xml_content_all)
        if not success:
            LOGGER.warning("Problem writing to CID record %s", priref)
            LOGGER.warning("Skipping further actions: Failed write of XML data")
            LOGGER.info(xml_content_all)
            continue
        if not packing_list:
            LOGGER.warning("No PackingList found in folder: %s", fpath)
            continue
        LOGGER.info("PackingList located and XML data written to CID item record")

        # Extracting PackingList content to dict and count
        asset_dct: dict[str, str] = {}
        with open(packing_list, "r") as readfile:
            asset_text = readfile.read()
            asset_dct = xmltodict.parse(f"""{asset_text}""")
        asset_dct_list = asset_dct["PackingList"]["AssetList"]["Asset"]

        # If XML asset not in PKL, add here:
        asset_whole = len(asset_dct_list)
        if asset_whole != (total_items - 2):
            LOGGER.warning(
                "Folder contents does not match length of packing list: %s", fpath
            )
            LOGGER.warning(
                "PKL length %s -- Total MXF + CPL file in folder %s",
                asset_whole,
                total_items,
            )
            continue

        # Build asset_list, PKL order first, followed by remaining XML
        LOGGER.info(
            "PackingList returned %s items, matching MXF content + CPL XML.",
            asset_whole,
        )
        asset_items: dict[str, str] = {}
        object_num: int = 1
        new_filenum_prefix: str = ob_num.replace("-", "_")
        for asset in asset_dct_list:
            filename = asset["OriginalFileName"]["#text"]
            ext: str = os.path.splitext(filename)[1]
            if not filename:
                LOGGER.warning(
                    "Exiting processing this asset - Could not retrieve original filename: %s",
                    asset,
                )
                continue
            print(f"Filename found {filename}")
            new_filename: str = (
                f"{new_filenum_prefix}_{str(object_num).zfill(2)}of{str(total_items).zfill(2)}{ext}"
            )
            asset_items[filename] = new_filename
            object_num += 1
        for xml in xml_list:
            if xml not in asset_items.keys():
                new_filename: str = (
                    f"{new_filenum_prefix}_{str(object_num).zfill(2)}of{str(total_items).zfill(2)}.xml"
                )
                asset_items[xml] = new_filename
                object_num += 1

        if len(asset_items) != total_items:
            LOGGER.warning(
                "Failed to retrieve all filenames from PackingList Assets: %s",
                asset_dct_list,
            )
            continue

        # Write all dict names to digital.acquired_filename in CID item record, re-write folder name
        success: bool = create_digital_original_filenames(
            priref, folder.strip(), asset_items
        )
        if not success:
            LOGGER.warning(
                "Skipping further actions. Asset item list not written to CID item record: %s",
                priref,
            )
            continue
        LOGGER.info(
            "CID item record <%s> filenames appended to digital.acquired_filenamed field",
            priref,
        )

        # Rename all files in IMP folder
        LOGGER.info("Beginning renaming of IMP folder assets:")
        success_rename: bool = True
        for key, value in asset_items.items():
            filepath = os.path.join(fpath, key)
            new_filepath = os.path.join(fpath, value)
            if os.path.isfile(filepath):
                LOGGER.info("\t- Renaming %s to new filename %s", key, value)
                os.rename(filepath, new_filepath)
                if not os.path.isfile(new_filepath):
                    LOGGER.warning("\t-  Error renaming file %s!", key)
                    success_rename = False
                    break
        if not success_rename:
            LOGGER.warning("SKIPPING: Failure to rename files in IMP %s", fpath)
            continue

        # Move to local autoingest black_pearl_netflix_ingest (subfolder for netflix01 bucket put)
        LOGGER.info("ALL IMP %s FILES RENAMED SUCCESSFULLY", folder)
        LOGGER.info("Moving to autoingest:")
        for file in asset_items.values():
            moving_asset = os.path.join(fpath, file)
            LOGGER.info("\t- %s", moving_asset)
            shutil.move(moving_asset, AUTOINGEST)
            if os.path.isfile(moving_asset):
                LOGGER.warning(
                    "Movement of file %s to autoingest failed!", moving_asset
                )
                LOGGER.warning(" - Please move manually")

        # Check IMP folder is empty and delete - Is this stage wanted? Waiting to hear from Andy
        contents: list[str] = list(os.listdir(fpath))
        if len(contents) == 0:
            os.rmdir(fpath)
            LOGGER.info("IMP folder empty, deleting %s", fpath)
        else:
            LOGGER.warning("IMP not empty, leaving in place for checks: %s", fpath)

    LOGGER.info("== Document augmented Netflix renaming end ===================\n")


def build_defaults() -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """
    Build record and item defaults
    Not active, may not be needed
    Record contents may need review!
    """
    record = [
        {"input.name": "datadigipres"},
        {"input.date": str(datetime.datetime.now())[:10]},
        {"input.time": str(datetime.datetime.now())[11:19]},
        {"input.notes": "Netflix metadata integration - automated bulk documentation"},
        {"record_access.user": "BFIiispublic"},
        {"record_access.rights": "0"},
        {"record_access.reason": "SENSITIVE_LEGAL"},
        {"grouping.lref": "400947"},
        {"language.lref": "71429"},
        {"language.type": "DIALORIG"},
    ]

    item = [
        {"record_type": "ITEM"},
        {"item_type": "DIGITAL"},
        {"copy_status": "M"},
        {"copy_usage.lref": "131560"},
        {"file_type.lref": "401103"},  # IMP
        {"code_type.lref": "400945"},  # Mixed
        {"accession_date": str(datetime.datetime.now())[:10]},
        {
            "acquisition.method.lref": "132853"
        },  # Donation - with written agreement ACQMETH
        {"acquisition.source.lref": "143463"},  # Netflix
        {"acquisition.source.type": "DONOR"},
    ]

    return record, item


def create_digital_original_filenames(
    priref: str, folder_name: str, asset_list_dct: dict[str, str]
) -> Optional[bool]:
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

    pay_mid = f"<Acquired_filename><digital.acquired_filename>{folder_name}</digital.acquired_filename><digital.acquired_filename.type>FOLDER</digital.acquired_filename.type></Acquired_filename>"
    pay_edit = f"<Edit><edit.name>datadigipres</edit.name><edit.date>{str(datetime.datetime.now())[:10]}</edit.date><edit.time>{str(datetime.datetime.now())[11:19]}</edit.time><edit.notes>Netflix automated digital acquired filename update</edit.notes></Edit>"
    payload_end = "</record></recordList></adlibXML>"
    payload = payload + pay_mid + pay_edit + payload_end

    LOGGER.info("** Appending digital.acquired_filename data to item record now...")
    LOGGER.info(payload)

    try:
        result = adlib.post(CID_API, payload, "items", "updaterecord")
        print(f"Item appended successful! {priref}\n{result}")
        LOGGER.info(
            "Successfully appended IMP digital.acquired_filenames to Item record %s",
            priref,
        )
        print(result)
        return True
    except Exception as err:
        print(err)
        LOGGER.warning(
            "Failed to append IMP digital.acquired_filenames to Item record %s", priref
        )
        print(f"CID item record append FAILED!! {priref}")
        return False


def xml_item_append(priref: str, xml_data: list[str]) -> bool:
    """
    Write XML data to CID item record
    """
    num = 1
    payload = f"<adlibXML><recordList><record priref='{priref}'>"
    for xml_block in xml_data:
        text = f"Netflix XML data {num}"
        pay_mid = f"<Label><label.source>{text}</label.source><label.text><![CDATA[{xml_block}]]></label.text></Label>"
        payload = payload + pay_mid
        num += 1
    payload_end = "</record></recordList></adlibXML>"
    payload = payload + payload_end

    LOGGER.info("** Appending Label text data to item record now...")
    LOGGER.info(payload)

    try:
        result = adlib.post(CID_API, payload, "items", "updaterecord")
        print(f"Item appended successful! {priref}\n{result}")
        LOGGER.info("Successfully appended Label fields to Item record %s", priref)
        print(result)
        return True
    except Exception as err:
        print(err)
        LOGGER.warning("Failed to append Label fields to Item record %s", priref)
        print(f"CID item record append FAILED!! {priref}")
        return False


if __name__ == "__main__":
    main()
