#!/usr/bin/env python3

"""
Special Collections Document tranfsers for OSH
Moving renamed folders to SFTP / Archivematica

1. Iterates through CID records looking for Closed
   or Open record statements (access_status field), and
   where no alternative_number AIP UUID in place.
2. Where found extract CID metadata and priref and build
   metadata.csv including metadata.
   Overwrite into metadata.csv file.
3. SFTP the folder matched to the object_number of the record
   - may need to iterate over folders initially until new
     automations have digital.acquired_filepath fully populated
     Stages:
        i. Build SFTP command for each level and move to Archivematica Transfer Storage
        ii. When SFTP complete, configure an Archivematica package transfer with specific
           details including AtoM proposed 'slug', with each package set to appropriate
           config of Open or Closed
        iii. Check that the transfer status is complete
4. Depending on the 'Open' or 'Closed' status, the records
   are ingested to Archivematica with OpenRecords or ClosedRecords
   The slug should be formed from the parent object_number
   Check AIP transfer completed okay.
5. Update item record with data that shows this automation
   has completed - AIP UUID to alternative_number
6. Capture all outputs to logs

Assumption in code
1. That historical AIP uploads will be deleted before
   running this code finally

2025
"""

# Public packages
from time import sleep
import logging
import os
import sys
import csv
from tenacity import retry, wait_fixed
import archivematica_sip_utils as am_utils

sys.path.append(os.environ.get("CODE"))
import adlib_v3 as adlib
import utils

LOGS = os.environ.get("LOG_PATH")
LOG = os.path.join(LOGS, "special_collections_document_transfer_osh.log")
CID_API = utils.get_current_api()
print(CID_API)
STORAGE = os.environ.get("BP_OSH_CHADHA")

LOGGER = logging.getLogger("sc_document_transfer_osh")
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


def get_cid_records(status):
    """
    Check in CID for any new Archive Items
    with OPEN/CLOSED in access_status field
    and where no UUID for AIP present in
    alternative_number field
    status = OPEN / CLOSED
    """
    # search = f"(object_number='GUR-2-2-5-4-*' and access_status='{status}' and not alternative_number.type='Archivematica AIP UUID')"
    search = f"(object_number='GUR-2-*' and access_status='{status}' and not alternative_number.type='Archivematica AIP UUID')"
    LOGGER.info("get_cid_records(): Making CID query request with:\n%s", search)

    fields: list[str] = [
        "priref",
        "title",
        "title.article",
        "object_number",
        "creator",
        "production.date.end",
        "subject",
        "digital.acquired_filepath",
        "content.description",
        "dimension.free",
        "language",
        "access_category.notes",
        "file_type",
    ]

    hits, records = adlib.retrieve_record(
        CID_API, "archivescatalogue", search, 0, fields
    )
    LOGGER.info(
        "get_cid_records(): Number of matching Archive Item records found:\n%s", hits
    )
    if hits > 0:
        return records
    return None


def fetch_matching_folder(ob_num, ext):
    """
    Iterate STORAGE to find folder that
    matches the object_number
    """
    print(f"Matching {ob_num} / {ext} to directories!")
    for root, dirs, _ in os.walk(STORAGE):
        for directory in dirs:
            if directory.startswith(f"{ob_num}_"):
                print("++++++++++++++++++++++++++ MATCH! +++++++++++++++++++++++++")
                dpath = os.path.join(root, directory)
                file = [
                    x
                    for x in os.listdir(dpath)
                    if os.path.isfile(os.path.join(dpath, x))
                ]
                fpath = os.path.join(root, directory, file[0])
                print(fpath, dpath)
                return fpath, dpath
    return None, None


def get_top_level_folder(folder_path):
    """
    Iterate split folder path
    looking for first _series_ entry
    """
    fp_list = folder_path.split("/")
    for fp in fp_list:
        if "_series_" in fp:
            top_level_folder = fp
            return top_level_folder

    return None


def create_metadata_csv(mdata: dict, fname: str) -> bool:
    """
    Repopulate metadata.csv with new data
    """
    metadata_file = mdata.get("metadata.csv")

    headers = [
        "filename",
        "dc.title",
        "dc.identifier",
        "dc.creator",
        "dc.date",
        "dc.type",
        "dc.description",
        "dc.extent",
        "dc.format",
        "dc.source",
        "dc.language",
        "dc.rights",
        "dc.subject",
    ]

    mdata_list = [f"objects/{fname}"]
    if mdata.get("title") is not None:
        mdata_list.append(mdata.get("title").strip())
    else:
        mdata_list.append("")
    if mdata.get("object_number") is not None:
        mdata_list.append(mdata.get("object_number").strip())
    else:
        mdata_list.append("")
    if mdata.get("creator") is not None:
        mdata_list.append(mdata.get("creator").strip())
    else:
        mdata_list.append("")
    if mdata.get("production.date.end") is not None:
        mdata_list.append(mdata.get("production.date.end").strip())
    else:
        mdata_list.append("")
    if mdata.get("subject") is not None:
        mdata_list.append(mdata.get("subject").strip())
    else:
        mdata_list.append("")
    if mdata.get("content.description") is not None:
        mdata_list.append(mdata.get("content.description").strip())
    else:
        mdata_list.append("")
    if mdata.get("dimension.free") is not None:
        mdata_list.append(mdata.get("dimension.free").strip())
        mdata_list.append(mdata.get("dimension.free").strip())
    else:
        mdata_list.append("")
        mdata_list.append("")
    if mdata.get("digital.acquired_filepath") is not None:
        mdata_list.append(mdata.get("digital.acquired_filepath").strip())
    else:
        mdata_list.append("")
    if mdata.get("language") is not None:
        mdata_list.append(mdata.get("language").strip())
    else:
        mdata_list.append("")
    if mdata.get("access_category.notes") is not None:
        mdata_list.append(mdata.get("access_category.notes").strip())
    else:
        mdata_list.append("")
    if mdata.get("subject") is not None:
        mdata_list.append(mdata.get("subject").strip())
    else:
        mdata_list.append("")

    with open(metadata_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        writer.writerow(mdata_list)

    if os.path.getsize(metadata_file) > 150:
        return True
    return False


def main():
    """
    Iterate supplied folder
    and complete series of
    SFTP / transfer
    """

    if not utils.check_control("pause_scripts"):
        LOGGER.info("Script run prevented by downtime_control.json. Script exiting.")
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")
    if not utils.check_control("power_off_all"):
        LOGGER.info("Script run prevented by downtime_control.json. Script exiting.")
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")
    if not utils.cid_check(CID_API):
        sys.exit("* Cannot establish CID session, exiting script")
    if not utils.check_storage(STORAGE):
        LOGGER.info("Script run prevented by storage_control.json. Script exiting.")
        sys.exit("Script run prevented by storage_control.json. Script exiting.")
    if not os.path.exists(STORAGE):
        sys.exit(f"Exiting. Path could not be found: {STORAGE}")

    LOGGER.info(
        "=========== Special Collections Archivematica - Document Transfer OSH START ============"
    )

    statuses = ["OPEN", "CLOSED"]
    for status in statuses:
        print(f"***** Processing: {status}")
        LOGGER.info("Processing status: %s", status)
        recs = get_cid_records(status)
        if recs is None:
            LOGGER.info("No new records found for %s status", status)
            continue
        LOGGER.info("New '%s' status records found:\n%s items", status, len(recs))
        print(f"TOTAL recs found: {len(recs)}")

        # Start processing at folder level
        for rec in recs:
            mdata_dct = {}
            mdata_dct = iterate_record(rec, status)
            print(f"Metadata dictionary extracted from CID/record:\n{mdata_dct}")
            if mdata_dct is None:
                LOGGER.warning(
                    "Skipping. Failed to extract metadata for record:\n%s", rec
                )
                continue
            ob_num = mdata_dct.get("object_number")
            priref = mdata_dct.get("priref")
            LOGGER.info("** New record being processed: %s", priref)
            LOGGER.info("Metadata extracted:\n%s", mdata_dct)
            file_path = mdata_dct.get("file_path")
            file = os.path.basename(file_path)
            if not os.path.isfile(file_path):
                LOGGER.warning("Skipping. Could not file path: %s", file_path)
                print(f"Filepath not found: {file_path}")
                continue

            # Augment metadata
            LOGGER.info("Adding additional metadata to the metadata/metadata.csv")
            success = create_metadata_csv(mdata_dct, file)
            if not success:
                LOGGER.warning(
                    "Dublin core metadata enrichment failed for: %s / %s",
                    file,
                    mdata_dct.get("priref"),
                )
            else:
                LOGGER.info("Dublin core metadata enriched: %s", file)

            # Check if Archival Item already in SFTP
            top_level_folder = ""
            top_level_folder = get_top_level_folder(file_path)
            sftp = False
            folder_contents = am_utils.check_sftp_status(file_path, top_level_folder)
            if file in folder_contents:
                sftp = True

            # PUT Archival Items only to SFTP (no record_type in name)
            LOGGER.info("Starting SFTP transfer to Archivematica")
            if sftp is False:
                LOGGER.info("%s identified as top level folder", top_level_folder)
                sftp_files = am_utils.send_to_sftp(file_path, top_level_folder)
                if sftp_files is None:
                    LOGGER.warning(
                        "SFTP PUT failed for folder: %s %s",
                        mdata_dct.get("object_number"),
                        file_path,
                    )
                    continue
                if file not in sftp_files:
                    LOGGER.warning(
                        "Problem with files put in folder %s: %s", file, sftp_files
                    )
                    continue
                LOGGER.info(
                    "SFTP Put successful: %s moved to Archivematica", sftp_files
                )
            elif sftp is True:
                LOGGER.info(
                    "*** File already uploaded to SFTP, following potential failed attempt."
                )

            # MOVING ITEM TO AIP
            LOGGER.info("Starting transfer of SFTP item to Archivematica AIP")
            folder_path = mdata_dct.get("folderpath")
            fp_split = folder_path.split(top_level_folder)[-1]
            am_path = os.path.join(top_level_folder, fp_split)
            ob_num = mdata_dct.get("object_number")
            on_split = ob_num.split("-")[:-1]
            parent_ob_num = "-".join(on_split).lower()

            if status == "OPEN":
                processing_config = "OpenRecords"
            else:
                processing_config = "ClosedRecords"

            LOGGER.info(
                "Moving SFTP directory %s to Archivematica as %s - with slug %s",
                am_path,
                processing_config,
                parent_ob_num,
            )
            response = am_utils.send_as_package(
                am_path,
                top_level_folder,
                parent_ob_num,
                priref,
                processing_config,
                True,
            )
            LOGGER.info("Package send response: %s", response)
            if "id" not in response:
                LOGGER.warning(
                    "Possible failure for Archivematica creation: %s", response
                )
                continue
            transfer_uuid = sip_uuid = aip_uuid = ""
            transfer_uuid = response.get("id")
            sleep(10)
            LOGGER.info("Checking transfer status...")
            transfer_dict = check_transfer_status(transfer_uuid, ob_num)
            if not transfer_dict:
                LOGGER.warning(
                    "Transfer confirmation not found after 10 minutes for directory %s",
                    am_path,
                )
                LOGGER.warning(
                    "Manual assistance needed to update UUIDs to CID item record"
                )
                continue
            sip_uuid = transfer_dict.get("sip_uuid")
            LOGGER.info(transfer_dict)
            sleep(10)
            LOGGER.info("Checking ingest status...")
            ingest_dict = check_ingest_status(sip_uuid, ob_num)
            if not ingest_dict:
                LOGGER.warning(
                    "Ingest confirmation not found after 10 minutes for directory %s",
                    file,
                )
                LOGGER.warning(
                    "Manual assistance needed to update AIP UUID to CID item record"
                )
                continue
            aip_uuid = ingest_dict.get("uuid")

            # Update Alternative number at close
            LOGGER.info("Updating AIP UUID to CID record: %s", aip_uuid)
            success = update_alternative_number(aip_uuid, priref)
            if success:
                LOGGER.info("Updated AIP UUID to CID item archive record: %s", priref)
            else:
                LOGGER.warning(
                    "The AIP update to record %s failed - please append manually!",
                    priref,
                )
                LOGGER.warning(aip_uuid)

            LOGGER.info("Completed AIP update for file %s", am_path)

    LOGGER.info(
        "=========== Special Collections Archivematica - Document Transfer OSH END =============="
    )


def iterate_record(rec: list[dict], status: str) -> dict:
    """
    Handle OPEN or CLOSED record meta data retrieval
    """
    mdata = {}
    try:
        priref = adlib.retrieve_field_name(rec, "priref")[0]
        ob_num = adlib.retrieve_field_name(rec, "object_number")[0]
        ftype = adlib.retrieve_field_name(rec, "file_type")[0]
        LOGGER.info("** Process Item Archive record %s", priref)
    except (KeyError, TypeError, IndexError) as err:
        LOGGER.warning(
            "Skipping this record as Priref could not be acquired:\n%s\n%s", rec, err
        )
        return None

    ext = FILE_TYPES.get(ftype)[0]
    fpath, dirpath = fetch_matching_folder(ob_num, ext)
    if not fpath or not dirpath:
        print(f"Filepath could not be matched to object_number: {ob_num}")
        LOGGER.warning("Filepath could not be matched to object_number: %s", ob_num)
        return None
    mdata_path = os.path.join(dirpath, "metadata/metadata.csv")

    if not os.path.exists(dirpath):
        LOGGER.warning("Unable to find matching folder path for %s", ob_num)
        return None
    if not os.path.isfile(fpath):
        LOGGER.warning("Unable to find matching file path for %s", fpath)
        return None

    LOGGER.info("Directory path found for %s record: %s", status, dirpath)
    mdata["priref"] = priref
    mdata["object_number"] = ob_num
    mdata["folderpath"] = dirpath
    mdata["metadata.csv"] = mdata_path
    mdata["file_path"] = fpath

    try:
        title = adlib.retrieve_field_name(rec, "title")[0]
        mdata["title"] = title
    except (KeyError, TypeError, IndexError):
        pass
    try:
        creator = adlib.retrieve_field_name(rec, "creator")[0]
        mdata["creator"] = creator
    except (KeyError, TypeError, IndexError):
        pass
    try:
        pdate = adlib.retrieve_field_name(rec, "production.date.end")[0]
        mdata["production.date.end"] = pdate
    except (KeyError, TypeError, IndexError):
        pass
    try:
        subject = adlib.retrieve_field_name(rec, "subject")[0]
        mdata["subject"] = subject
    except (KeyError, TypeError, IndexError):
        pass
    try:
        fpath = adlib.retrieve_field_name(rec, "digital.acquired_filepath")[0]
        mdata["digital.acquired_filepath"] = fpath
    except (KeyError, TypeError, IndexError):
        pass
    try:
        desc = adlib.retrieve_field_name(rec, "content.description")[0]
        mdata["content.description"] = desc
    except (KeyError, TypeError, IndexError):
        pass
    try:
        dfree = adlib.retrieve_field_name(rec, "dimension.free")[0]
        mdata["dimension.free"] = dfree
    except (KeyError, TypeError, IndexError):
        pass
    try:
        language = adlib.retrieve_field_name(rec, "language")[0]
        mdata["language"] = language
    except (KeyError, TypeError, IndexError):
        pass
    try:
        access = adlib.retrieve_field_name(rec, "access_category.notes")[0]
        mdata["access_category.notes"] = access
    except (KeyError, TypeError, IndexError):
        pass

    return mdata


@retry(wait=wait_fixed(15))
def check_transfer_status(uuid, directory):
    """
    Check status of transfer up to 10
    times, or until retrieved
    """
    trans_dict = am_utils.get_transfer_status(uuid)

    if trans_dict.get("status") == "COMPLETE" and len(trans_dict.get("sip_uuid")) > 0:
        LOGGER.info(
            "Transfer of package completed: %s", trans_dict.get("directory", directory)
        )
        return trans_dict
    else:
        raise Exception


@retry(wait=wait_fixed(15))
def check_ingest_status(uuid, directory):
    """
    Check status of transfer up to 10
    times, or until retrieved
    """
    ingest_dict = am_utils.get_ingest_status(uuid)

    if ingest_dict.get("status") == "COMPLETE" and len(ingest_dict.get("uuid")) > 0:
        LOGGER.info(
            "Ingest of package completed: %s", ingest_dict.get("directory", directory)
        )
        return ingest_dict
    else:
        raise Exception


def update_alternative_number(uuid: str, priref: str) -> None:
    """
    For each item successfully PUT to sftp
    and progressed to AIP update alternative_number
    """
    dct = [
        {"alternative_number": uuid},
        {"alternative_number.type": "Archivematica AIP UUID"},
    ]

    record_xml = adlib.create_record_data(CID_API, "archivescatalogue", priref, dct)
    print(record_xml)
    try:
        rec = adlib.post(CID_API, record_xml, "archivescatalogue", "updaterecord")
        if rec is None:
            LOGGER.warning("Failed to update record: %s\n%s", priref, record_xml)
            return None
        if "priref" not in str(rec):
            LOGGER.warning("Failed to update record: %s\n%s", priref, record_xml)
            return None
        priref = adlib.retrieve_field_name(rec, "priref")[0]
        return priref
    except Exception as err:
        raise err


if __name__ == "__main__":
    main()
