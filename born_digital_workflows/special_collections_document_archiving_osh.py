#!/usr/bin/env python3

"""
Special Collections Document Archiving script for OSH

Script stages:
MUST BE SUPPLIED WITH SYS.ARGV[1] AT SUB-FOND LEVEL PATH
1. Iterate through supplied sys.argv[1] folder path
2. For each subfolder split folder name: ob_num / ISAD(G) level / Title
3. Create CID record for each folder following level from folder name
   - Only when subfolder starts with object number of parent folder
   - Only creating Series, Sub Series and Sub Sub Series (awaiting record_type for last)
   - For items (any digital document) create an Archive Item record (df='ITEM_ARCH')
4. Join to the parent/children records through the ob_num part/part_of
5. Once at bottom of folders in sub or sub sub series, order files by alphabetic order (sort)
6. Check for filename already in digital.acquired_filename in CID already (report where found)
7. CID archive item records are to be made for each, and linked to parent folder:
      Named GUR-2-1-1-1-1, GUR-2-1-1-1-2 etc based on parent's object number
      Original filename is to be captured into the Item record digital.acquired_filename
      Rename the file and move to autoingest.

NOTE:
Code assumption:
An Archival Item should never be ordered higher than
sub folders. Eg, all files within a folder are renamed to GUR- object numbers after
sub folder CID records have been created and object number assigned.

2025
"""

# Public packages
import csv
import datetime
import logging
import os
import sys
from typing import Any, Dict, List, Optional

import magic
import requests

# Private packages
sys.path.append(os.environ.get("CODE"))
import adlib_v3_sess as adlib
import utils

# Global path variables
AUTOINGEST = os.path.join(os.environ.get("AUTOINGEST_BP_SC"), "ingest/autodetect/")
LOG = os.path.join(
    os.environ.get("LOG_PATH"), "special_collections_document_archiving_osh.log"
)
MEDIAINFO_PATH = os.path.join(os.environ.get("LOG_PATH"), "cid_mediainfo/")
# CID_API = os.environ.get("CID_API4")
CID_API = utils.get_current_api()

LOGGER = logging.getLogger("sc_document_archiving_osh")
HDLR = logging.FileHandler(LOG)
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

FILE_TYPES = {
    "XLS": ["xls", "SS"],
    "XLSX": ["xlsx", "SS"],
    "DOC": ["doc", "D"],
    "DOCX": ["docx", "D"],
    "PDF": ["pdf", "D"],
    "PPT": ["ppt", "SL"],
    "PPTX": ["pptx", "SL"],
    "JPEG": ["jpg", "jpeg", "I"],
    "PNG": ["png", "I"],
    "TIFF": ["tiff", "tif", "I"],
    "EML": ["eml", "E"],
    "AI": ["ai", "D"],
    "PSD": ["psd", "D"],
    "FDX": ["fdx", "T"],
    "FDR": ["fdr", "T"],
    "PAGES": ["pages", "D"],
    "PSB": ["psb", "D"],
    "EPS": ["eps", "D"],
    "CR2": ["cr2", "I"],
    "HEIC": ["heic", "I"],
    "RTF": ["rtf", "T"],
    "CSV": ["csv", "SS"],
    "TXT": ["txt", "T"],
    "MSG": ["msg", "M"],
    "ZIP": ["zip", "D"],
    "BMP": ["bmp", "I"],
    "NUMBERS": ["numbers", "SS"],
    "CPGZ": ["cpgz", "D"],
    "INDD": ["indd", "D"],
    "JFIF": ["jfif", "I"],
    "PKGF": ["pkgf", "D"],
    "SVG": ["svg", "I"],
    "KEY": ["key", "SL"],
}


def cid_retrieve(
    fname: str, record_type: str, session
) -> Optional[tuple[str, str, str]]:
    """
    Receive filename and search in CID works dB
    Return selected data to main()
    """
    search: str = f'(object_number="{fname}" and record_type="{record_type}")'

    fields: list[str] = ["priref", "title", "title.article"]

    record = adlib.retrieve_record(
        CID_API, "archivescatalogue", search, "1", session, fields
    )[1]

    LOGGER.info("cid_retrieve(): Making CID query request with:\n%s", search)
    if not record:
        search: str = f'object_number="{fname}"'
        record = adlib.retrieve_record(
            CID_API, "archivescatalogue", search, "1", session, fields
        )[1]
        if not record:
            LOGGER.warning("cid_retrieve(): Unable to retrieve data for %s", fname)
            return None

    if "priref" in str(record):
        priref = adlib.retrieve_field_name(record[0], "priref")[0]
    else:
        priref = ""
    if "Title" in str(record):
        title = adlib.retrieve_field_name(record[0], "title")[0]
    else:
        title = ""
    if "title.article" in str(record):
        title_article = adlib.retrieve_field_name(record[0], "title.article")[0]
    else:
        title_article = ""

    return priref, title, title_article


def get_file_type(ext: str) -> Optional[str]:
    """
    Get file type from extension
    """

    for key, value in FILE_TYPES.items():
        if ext.lower() in value:
            return key, value[-1]
    return ext.upper(), ""


def record_hits(fname: str, session) -> Optional[Any]:
    """
    Count hits and return bool / NoneType
    """
    search: str = f'object_number="{fname}"'
    hits = adlib.retrieve_record(CID_API, "archivescatalogue", search, 1, session)[0]

    if hits is None:
        return None
    if int(hits) == 0:
        return False
    if int(hits) > 0:
        return True


def get_children_items(ppriref: str, session) -> Optional[List[str]]:
    """
    Get all children of a given priref
    """
    item_list = []
    search: str = f'part_of_reference.lref="{ppriref}"'
    fields: list[str] = ["priref", "object_number"]

    hits, records = adlib.retrieve_record(
        CID_API, "archivescatalogue", search, "0", session, fields
    )
    if hits is None:
        return None
    elif hits == 0:
        return item_list

    for r in records:
        item_list.append(adlib.retrieve_field_name(r, "object_number")[0])
    return item_list


def get_last_child(child_list):
    """
    Complete checks for last child in list
    Convert to integer and sort for last num
    """
    if child_list is None:
        return None
    elif len(child_list) == 0:
        return 0
    else:
        num_lst = [int(x.split("-")[-1]) for x in child_list]
        num_lst.sort()
        return num_lst[-1]


def sort_files(file_list: List[str], last_child_num: str) -> List[str]:
    """
    Get alphabetic order of files, and sort accordingly
    return with enumeration number
    """
    file_list.sort()
    enum_list = []
    for i, name in enumerate(file_list):
        i += last_child_num
        # enum_list.append(f"{name.split(' - ', 1)[-1]}, {i + 1}")
        enum_list.append(f"{name}, {i + 1}")
    print(f"Enumerated list: {enum_list}")
    return enum_list


def folder_split(fname):
    """
    Split folder name into parts
    """
    fsplit = fname.split("_", 2)
    if len(fsplit) != 3:
        LOGGER.warning("Folder has not split as anticipated: %s", fsplit)
        return None, None, None
    ob_num, record_type, title = fsplit
    if not ob_num.startswith(("GUR", "")):
        LOGGER.warning("Object number is not formatted as anticipated: %s", ob_num)
        return None, None, None

    return ob_num, record_type, title


def get_image_data(ipath: str) -> list[dict[str, str]]:
    """
    Create dictionary for Image
    metadata from Exif data source
    """
    ext = os.path.splitext(ipath)[1].replace(".", "")
    try:
        file_type, mime = get_file_type(ext)
    else:
        file_type = mime = ""
    print(f"**** {file_type} ****")
    exif_metadata = utils.exif_data(f"{ipath}")
    if exif_metadata is None:
        LOGGER.warning("File could not be read by ExifTool: %s", ipath)
        date = os.path.getmtime(ipath)
        metadata_dct = [
            {
                "production.date.notes": datetime.datetime.fromtimestamp(date).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            },
            {
                "production.date.end": datetime.datetime.fromtimestamp(date).strftime(
                    "%Y-%m-%d"
                )
            },
            {"filesize": str(os.path.getsize(ipath))},
            {"filesize.unit": "B (Byte)"},
            {"file_type": file_type},
            {"media_type": mime},
        ]
        return metadata_dct

    if "Corrupt data" in str(exif_metadata):
        LOGGER.info("Exif cannot read metadata for file: %s", ipath)
        date = os.path.getmtime(ipath)
        metadata_dct = [
            {
                "production.date.notes": datetime.datetime.fromtimestamp(date).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            },
            {
                "production.date.end": datetime.datetime.fromtimestamp(date).strftime(
                    "%Y-%m-%d"
                )
            },
            {"filesize": str(os.path.getsize(ipath))},
            {"filesize.unit": "B (Byte)"},
            {"file_type": file_type},
            {"media_type": mime},
        ]
        return metadata_dct
    print(exif_metadata)
    if not isinstance(exif_metadata, list):
        return None

    data = [
        "File Modification Date/Time, production.date.notes",
        "Software, source_software",
    ]

    image_dict = []
    for mdata in exif_metadata:
        if ":" not in str(mdata):
            continue
        field, value = mdata.split(":", 1)
        for d in data:
            exif_field, cid_field = d.split(", ")
            if "production.date.notes" in str(
                d
            ) and "File Modification Date/Time" in str(field):
                image_dict.append({f"{cid_field}": value.strip()})
                try:
                    date = value.strip().split(" ", 1)[0].replace(":", "-")
                    image_dict.append({"production.date.end": date})
                except IndexError as err:
                    LOGGER.warning("Error splitting date: %s", err)
            elif exif_field == field.strip():
                image_dict.append({f"{cid_field}": value.strip()})
    image_dict.append({"filesize": str(os.path.getsize(ipath))})
    image_dict.append({"filesize.unit": "B (Byte)"})
    image_dict.append({"file_type": file_type})
    image_dict.append({"media_type": mime})

    return image_dict


def build_defaults():
    """
    Use this function to just build standard defaults for all GUR records
    Discuss what specific record data they want in every record / some records
    """

    records_all = [
        {"record_access.user": "BFIiispublic"},
        {"record_access.rights": "0"},
        # ? {"record_access.reason": "Temporary restriction while OSH New Voices in the Archive project completes, to be removed for public access later in project"},
        {"institution.name.lref": "999570701"},
        {"analogue_or_digital": "DIGITAL"},
        {"digital.born_or_derived": "BORN_DIGITAL"},
        {"input.name": "datadigipres"},
        {"input.date": str(datetime.datetime.now())[:10]},
        {"input.time": str(datetime.datetime.now())[11:19]},
        {
            "input.notes": "Automated record creation for Our Screen Heritage OSH strand 3, to facilitate ingest to Archivematica."
        },
    ]

    return records_all


def main():
    """
    Iterate supplied folder, find image files in folders
    named after work and create analogue/digital item records
    for every photo. Clean up empty folders.
    """
    if not utils.check_control("power_off_all"):
        LOGGER.info("Script run prevented by downtime_control.json. Script exiting.")
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")

    if not utils.cid_check(CID_API):
        sys.exit("* Cannot establish CID session, exiting script")

    LOGGER.info(
        "=========== Special Collections rename - Document Archiving OSH START ============"
    )

    base_dir = sys.argv[1]  # Always sub_fond level path
    if not utils.check_storage(base_dir):
        LOGGER.info("Script run prevented by storage_control.json. Script exiting.")
        sys.exit("Script run prevented by storage_control.json. Script exiting.")

    sub_fond = os.path.basename(base_dir)
    sf_ob_num, sf_record_type, sf_title = folder_split(sub_fond)
    print(f"Sub fond data found:\n\n{sf_ob_num}\n\n{sf_record_type}\n\n{sf_title}")
    LOGGER.info("Sub fond data: %s, %s, %s", sf_ob_num, sf_record_type, sf_title)

    if not os.path.isdir(base_dir):
        sys.exit("Folder path is not a valid path")

    series = []
    sub_series = []
    sub_sub_series = []
    sub_sub_sub_series = []
    file = []
    for root, dirs, _ in os.walk(base_dir):
        for directory in dirs:
            if not str(directory).startswith(sf_ob_num):
                continue
            dpath = os.path.join(root, directory)
            if "_series_" in str(directory):
                series.append(dpath)
            elif "_sub-series_" in str(directory):
                sub_series.append(dpath)
            elif "_sub-sub-series_" in str(directory):
                sub_sub_series.append(dpath)
            elif "_sub-sub-sub-series_" in str(directory):
                sub_sub_sub_series.append(dpath)
            elif "_file_" in str(directory):
                file.append(dpath)

    session = adlib.create_session()
    defaults_all = build_defaults()

    # Process all directories first from series down to file
    # Series
    if not series:
        sys.exit("No series data found, exiting.")
    series.sort()
    LOGGER.info("Series found %s: %s", len(series), ", ".join(series))
    s_prirefs = create_folder_record(series, session, defaults_all)
    LOGGER.info("Series records created/identified:\n%s", s_prirefs)
    # Sub-series
    if not sub_series:
        sys.exit("No sub-series data found, exiting.")
    sub_series.sort()
    LOGGER.info("Sub-series found %s: %s", len(sub_series), ", ".join(sub_series))
    ss_prirefs = create_folder_record(sub_series, session, defaults_all)
    LOGGER.info("Sub-series records created/identified:\n%s", ss_prirefs)
    # Sub-sub-series
    if sub_sub_series:
        sub_sub_series.sort()
        sss_prirefs = create_folder_record(sub_sub_series, session, defaults_all)
        LOGGER.info("Sub-sub-series records created/identified:\n%s", sss_prirefs)
    # Sub-sub-sub-series
    if sub_sub_sub_series:
        sub_sub_sub_series.sort()
        ssss_prirefs = create_folder_record(sub_sub_sub_series, session, defaults_all)
        LOGGER.info("Sub-sub-sub-series records created/identified:\n%s", ssss_prirefs)
    # Files
    if file:
        file.sort()
        f_prirefs = create_folder_record(file, session, defaults_all)
        LOGGER.info("File records created/identified:\n%s", f_prirefs)

    # Create Archive Item records for all levels
    # Series
    if series:
        series_dcts, series_items = handle_repeat_folder_data(
            series, s_prirefs, session, defaults_all
        )
        LOGGER.info("Processed the following Series and Series items:")
        for s in series_dcts:
            LOGGER.info(s)
        if not series_items:
            LOGGER.info("No Archival Items found for Series.")
        else:
            for i in series_items:
                LOGGER.info(i)
    # Sub-series
    if sub_series:
        s_series_dcts, s_series_items = handle_repeat_folder_data(
            sub_series, ss_prirefs, session, defaults_all
        )
        LOGGER.info("Processed the following Sub series and Sub series items:")
        for s in s_series_dcts:
            LOGGER.info(s)
        if not s_series_items:
            LOGGER.info("No Archival Items found for Sub-series.")
        else:
            for i in s_series_items:
                LOGGER.info(i)
    # Sub-sub-series
    if sub_sub_series:
        ss_series_dcts, ss_series_items = handle_repeat_folder_data(
            sub_sub_series, sss_prirefs, session, defaults_all
        )
        LOGGER.info("Processed the following Sub-sub series and Sub-sub series items:")
        for s in ss_series_dcts:
            LOGGER.info(s)
        if not ss_series_items:
            LOGGER.info("No Archival Items found for Sub-sub-series.")
        else:
            for i in ss_series_items:
                LOGGER.info(i)
    # Sub-sub-sub-series
    if sub_sub_sub_series:
        sss_series_dcts, sss_series_items = handle_repeat_folder_data(
            sub_sub_sub_series, ssss_prirefs, session, defaults_all
        )
        LOGGER.info(
            "Processed the following Sub-sub-sub series and Sub-sub-sub series items:"
        )
        for s in sss_series_dcts:
            LOGGER.info(s)
        if not sss_series_items:
            LOGGER.info("No Archival Items found for Sub-sub-sub-series.")
        else:
            for i in sss_series_items:
                LOGGER.info(i)
    # Files
    if file:
        file_dcts, file_items = handle_repeat_folder_data(
            file, f_prirefs, session, defaults_all
        )
        LOGGER.info("Processed the following File and File items:")
        for s in file_dcts:
            LOGGER.info(s)
        if not file_items:
            LOGGER.info("No Archival Items found for any Files.")
        else:
            for i in file_items:
                LOGGER.info(i)

    LOGGER.info(
        "=========== Special Collections - Document Archiving OSH END =============="
    )


def handle_repeat_folder_data(record_type_list, priref_dct, session, defaults_all):
    """
    Get back dict of fpaths and prirefs, then
    look within each for documents that need recs.
    """
    print(
        f"Received data for handling repeated folder data:\n{record_type_list}\n\n{defaults_all}\n\n{priref_dct}"
    )

    # Check for item_archive files within folders
    item_prirefs = []
    for key, val in priref_dct.items():
        file_order = {}
        p_priref, p_ob_num = val.split(" - ")

        print(f"Folder path: {key} - priref {p_priref} - object number {p_ob_num}")
        # List all files in folder, but not if already named after parent ob_num
        file_list = [
            os.path.join(key, x)
            for x in os.listdir(key)
            if os.path.isfile(os.path.join(key, x)) and not x.startswith(p_ob_num)
        ]
        if len(file_list) == 0:
            LOGGER.info("No files found in path: %s", key)
            continue

        # Get last object numbers of parent priref children
        child_list = get_children_items(p_priref, session)
        print(f"Child list: {child_list}")
        last_child_num = get_last_child(child_list)
        if last_child_num is None:
            LOGGER.warning(
                "Failed to retrieve CID response, skipping this folder: %s.", p_ob_num
            )
            continue
        LOGGER.info(
            "Children of record found. Passing last number to enumeration: %s",
            str(last_child_num),
        )

        enum_files = sort_files(file_list, last_child_num)
        file_order[f"{key}"] = enum_files
        LOGGER.info(
            "%s files found to create Item Archive records: %s",
            len(file_order),
            ", ".join(file_order),
        )
        # Create ITEM_ARCH records and rename files / move to new subfolders?
        item_priref_group = create_archive_item_record(
            file_order, key, p_priref, session, defaults_all
        )
        item_prirefs.append(item_priref_group)

    return priref_dct, item_prirefs


def create_folder_record(
    folder_list: List[str], session: requests.Session, defaults: List[Dict[str, str]]
) -> Dict[str, str]:
    """
    Accept list of folder paths and create a record
    where none already exist, linking to the parent
    record
    """
    record_types = [
        "sub-fonds",
        "series",
        "sub-series",
        "sub-sub-series",
        "sub-sub-sub-series",
        "file",
    ]
    print(f"Received {folder_list}, {defaults}")
    priref_dct = {}
    for fpath in folder_list:
        root, folder = os.path.split(fpath)
        p_ob_num, p_record_type, _ = folder_split(os.path.basename(root))
        print(f"Parent folder: {root} - {p_ob_num} - {p_record_type}")
        print(f"Folder to be processed {folder}")
        ob_num, record_type, local_title = folder_split(folder)
        print(ob_num)
        print(record_type)
        print(local_title)

        if ob_num is None:
            continue
        # Skip file, it can sit any level in types
        if record_type != "file":
            idx = record_types.index(record_type)
            if isinstance(idx, int):
                print(
                    f"Record type match: {record_types[idx]} - checking parent record_type is correct."
                )
                pidx = idx - 1
                if record_types[pidx] != p_record_type:
                    LOGGER.warning(
                        "Problem with supplied record types in folder name, skipping"
                    )
                    continue

        # Check if parent already created to allow for repeat runs against folders
        p_exist = record_hits(p_ob_num, session)
        if p_exist is None:
            LOGGER.warning("API may not be available. Skipping for safety.")
            continue
        if p_exist is False:
            LOGGER.info(
                "Skipping creation of child record to %s, record not matched in CID",
                p_ob_num,
            )
            continue
        LOGGER.info("Parent record matched in CID: %s", p_ob_num)
        p_priref, title, title_art = cid_retrieve(
            p_ob_num, p_record_type.upper().replace("-", "_"), session
        )
        LOGGER.info("Parent priref %s, Title %s %s", p_priref, title_art, title)

        # Check if record already exists before creating new record
        exist = record_hits(ob_num, session)
        if exist is None:
            LOGGER.warning("API may not be available. Skipping for safety %s", folder)
            continue
        if exist is True:
            priref, title, title_art = cid_retrieve(
                ob_num, record_type.upper().replace("-", "_"), session
            )
            LOGGER.info("Skipping creation. Record for %s already exists", ob_num)
            priref_dct[fpath] = f"{priref} - {ob_num}"
            continue
        LOGGER.info("No record found. Proceeding.")

        # Create record here
        cid_record_type = record_type.upper().replace("-", "_")
        data = [
            {"record_type": cid_record_type},
            {"description_level_object": "ARCHIVE"},
            {"object_number": ob_num},
            {"part_of_reference": p_ob_num},
            {"archive_title.type": "01_orig"},
            {"title": local_title},
        ]
        data.extend(defaults)
        new_priref = post_record(session, data)
        if new_priref is None:
            LOGGER.warning(
                "Record failed to create using data: %s, %s, %s, %s,\n%s",
                ob_num,
                cid_record_type,
                p_priref,
                local_title,
                data,
            )
            continue

        LOGGER.info("New %s record_type created: %s", cid_record_type, new_priref)
        print(
            f"New series record created: {ob_num} - {new_priref} / Parent: {p_ob_num} / Record type: {cid_record_type} / {local_title}"
        )
        priref_dct[fpath] = f"{new_priref} - {ob_num}"

    return priref_dct


def post_record(session, record_data=None) -> Optional[Any]:
    """
    Receive dict of series data
    and create records for each
    and create CID records
    """
    if record_data is None:
        return None

    # Convert to XML
    print(record_data)
    record_xml = adlib.create_record_data(
        CID_API, "archivescatalogue", session, "", record_data
    )
    print(record_xml)
    try:
        rec = adlib.post(
            CID_API, record_xml, "archivescatalogue", "insertrecord", session
        )
        if rec is None:
            LOGGER.warning("Failed to create new record:\n%s", record_xml)
            return None
        if "priref" not in str(rec):
            LOGGER.warning("Failed to create new record:\n%s", record_xml)
            return None
        priref = adlib.retrieve_field_name(rec, "priref")[0]
        return priref
    except Exception as err:
        raise err


def create_archive_item_record(
    file_order, parent_path, parent_priref, session, defaults_all
):
    """
    Get data needed for creation of item archive record
    Receive item fpath, enumeration, parent priref/ob num and title
    """
    print("Create archive item record!")
    parent_ob_num, _, title = folder_split(os.path.basename(parent_path))
    LOGGER.info(
        "Processing files for parent %s in path: %s", parent_priref, parent_path
    )
    LOGGER.info("File order: %s", file_order)

    all_item_prirefs = {}
    for _, value in file_order.items():
        for ip in value:
            data = ip.rsplit(", ", 1)
            print(data)
            if not os.path.isfile(data[0]):
                LOGGER.warning("Corrupt file path supplied: %s", ipath)
                continue

            # Get particulars
            ipath = data[0]
            num = data[1]
            mime = magic.Magic(mime=True)
            mime_type = mime.from_file(ipath)
            iname = os.path.basename(ipath)
            LOGGER.info(
                "------ File: %s --- number %s --- mime %s ------",
                iname,
                num,
                mime_type,
            )
            ext = os.path.splitext(ipath)[1].lstrip(".")
            ob_num = f"{parent_ob_num}-{num.strip()}"
            new_name = f"{ob_num.replace('-', '_')}_01of01.{ext}"
            new_folder = f"{ob_num}_{iname.rsplit('.', 1)[0].replace(' ', '-')}"

            # Create exif metadata / checksum
            metadata_dct = {}
            try:
                metadata_dct = get_image_data(ipath)
                print(metadata_dct)
            except Exception as err:
                LOGGER.warning(
                    "File type not recognised by exiftool: %s\n%s", mime_type, err
                )

            checksum = utils.create_md5_65536(ipath)

            record_dct = [
                {"record_type": "ITEM_ARCH"},
                {"part_of_reference": parent_ob_num},
                {"archive_title.type": "01_orig"},
                {"title": iname},
                {"digital.acquired_filename": iname},
                {"digital.acquired_filename.type": "FILE"},
                {"object_number": ob_num},
                {"received_checksum.type": "MD5"},
                {"received_checksum.date": str(datetime.datetime.now())[:10]},
                {"received_checksum.value": checksum},
            ]

            if metadata_dct:
                record_dct.extend(metadata_dct)
            record_dct.extend(defaults_all)

            # Check record not already existing - then create record and receive priref
            exist = record_hits(ob_num, session)
            if exist is None:
                LOGGER.warning(
                    "API may not be available. Skipping record creation for safety %s",
                    iname,
                )
                continue
            if exist is True:
                priref, title, _ = cid_retrieve(ob_num, "ITEM_ARCH", session)
                LOGGER.warning(
                    "Skipping creation. Record %s / %s already exists: <%s>",
                    title,
                    ob_num,
                    priref,
                )
                continue

            # Create
            LOGGER.info("Data collated for record creation: %s", record_dct)
            new_priref = post_record(session, record_dct)
            if new_priref is None:
                LOGGER.warning("Record creation failed: %s", record_dct)
                return None

            all_item_prirefs[new_priref] = f"{iname} - {new_name}"
            LOGGER.info("New record created for Item Archive: %s", new_priref)

            # Create new folder to house file within
            LOGGER.info("Creating new folder for file: %s", new_folder)
            new_fpath = os.path.join(parent_path, new_folder)
            if not os.path.isdir(new_fpath):
                try:
                    os.makedirs(new_fpath, exist_ok=True)
                    LOGGER.info("New folder created: %s", new_fpath)
                except OSError as err:
                    LOGGER.warning("Folder creation error: %s", err)
            # Rename and move file into new folder
            new_filepath = os.path.join(new_fpath, new_name)
            try:
                LOGGER.info("File renaming:\n - %s\n - %s", ipath, new_filepath)
                os.rename(ipath, new_filepath)
                if os.path.isfile(new_filepath):
                    LOGGER.info("File renaming was successful.")
                else:
                    LOGGER.warning("File renaming failed:\n%s\n%s", ipath, new_fpath)
            except OSError as err:
                LOGGER.warning("File renaming error: %s", err)
            # Create metadata.csv file for new folder - filename objects/new_filename, dc.title - original filename
            success = create_metadata_csv(new_fpath, new_name, iname, ob_num)
            if not success:
                LOGGER.warning("Metadata file creation failed for %s", new_fpath)

    print(f"Item prirefs: {all_item_prirefs}")
    return all_item_prirefs


def create_metadata_csv(fpath, fname, title, ob_num):
    """
    Create new metadata folder, and fill with metadata.csv
    """
    metadata_path = os.path.join(fpath, "metadata/")
    os.makedirs(metadata_path, exist_ok=True)
    metadata_file = os.path.join(metadata_path, "metadata.csv")
    headers = ["filename", "dc.title", "dc.identifier"]
    with open(metadata_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        writer.writerow([f"objects/{fname}", title, ob_num])

    if os.path.getsize(metadata_file) > 0:
        return True


if __name__ == "__main__":
    main()
