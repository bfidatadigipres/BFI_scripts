#!/usr/bin/env python3
"""
Script to remove all AIPs from Archivematica
using data from Axiell database supplying
AIP UUID and status of record OPEN or CLOSED

2026
"""

import os
import sys
import logging
import requests
from typing import Optional, List, Any, Dict
import archivematica_sip_utils as ut

sys.path.append(os.environ.get("CODE"))
import adlib_v3_sess as adlib
import utils

AIP_DEST = os.path.join(
    os.environ.get("QNAP_05"), "Archivematica_Download"
)
ACCESS_DEST = os.path.join(
    os.environ.get("QNAP_05"), "Archivematica_Access"
)
LOG_PATH = os.environ.get("LOG_PATH")
CID_API = utils.get_current_api()

LOGGER = logging.getLogger("archivematica_aip_proxy_downloader")
HDLR = logging.FileHandler(os.path.join(
    LOG_PATH, "archivematica_aip_proxy_downloader.log"
))
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def cid_retrieve(session: requests.Session) -> Optional[List[Dict[Any]]]:
    """
    retrieve list of object numbers that are set
    to open/closed and have Archivematica AIP UUIDs
    in alternative_number field, but no download name
    in alternative number field
    """
    search: str = '(object_number="GUR-*" and alternative_number.type="Archivematica AIP UUID")'
    hits, recs = adlib.retrieve_record(
        CID_API, "archivescatalogue", search, "0", session
    )
    LOGGER.info("cid_retrieve(): Records found to process: %s", hits)
    if hits is None:
        return None
    if hits == 0:
        return []

    processing_dct = []
    for record in recs:
        local_data = {}
        priref = adlib.retrieve_field_name(record, "priref")[0]
        ob_num = adlib.retrieve_field_name(record, "object_number")[0]
        access_status = adlib.retrieve_field_name(record, "access_status")[0]
        alt_num_type_lst = adlib.retrieve_field_name(record, "alternative_number.type")
        alt_num_lst = adlib.retrieve_field_name(record, "alternative_number")
        local_data = {
                "priref": priref,
                "object_number": ob_num,
                "access_status": access_status,
        }
        local_data["alternative_number_type1"] = alt_num_type_lst[0]
        local_data["alternative_number1"] = alt_num_lst[0]
        if len(alt_num_type_lst) == 2:
            local_data["alternative_number_type2"] = alt_num_type_lst[1]
            local_data["alternative_number2"] = alt_num_lst[1]
        processing_dct.append(local_data)
    
    return processing_dct


def main():
    """
    Retrieve list of records for processing
    and work through them, updating records
    as needed to mark completion of downloads
    """

    sess = adlib.create_session()
    actionable_recs = cid_retrieve(sess)
    if actionable_recs is None:
        sys.exit("EXIT: Failure to reach ArchivesCatalogue data")
    if len(actional_recs) == 0:
        sys.exit("EXIT: No records found for this processing...")

    LOGGER.info("==== AIP Proxy Download START =================")
    for record in actionable_recs:
        if "alternative_number_type2" in record:
            check_processed = record.get("alternative_number_type2")
            check_name = record.get("alternative_number2")
            check_path = os.path.join(AIP_DEST, check_name)
            if check_processed == "AIP download filename":
                if os.path.exists(check_path):
                    LOGGER.info("Skipping record, already processed: %s\n%s", record.get("object_number"), record)
                    continue
                else:
                    LOGGER.info("Allowing to continue as record updated, but no downloaded file located:\n%s", check_path)

        ob_num = record.get("object_number")
        priref = record.get("priref")
        aip_uuid = record.get("alternative_number1")
        LOGGER.info("Downloading AIP with data: '%s' '%s'", ob_num, aip_uuid)
        download_path = ut.download_aip(aip_uuid, AIP_DEST, ob_num)
        aip_fname = os.path.basename(download_path)
        if os.path.exists(download_path):
            LOGGER.info("AIP TAR file downloaded successfully to path:\n%s", download_path)
        else:
            LOGGER.warning("Downloaded AIP not found in supplied path:\n%s", download_path)
            continue
        
        if record.get("access_status").strip() == "OPEN":
            LOGGER.info("Record has access_status <%s>, downloading access proxy file", record.get("access_status"))
            proxy_path = ut.download_normalised_file(ob_num, ACCESS_DEST)
            if os.path.isfile(proxy_path):
                LOGGER.info("Access rendition proxy successfully downloaded:\n%s", proxy_path)
            else:
                LOGGER.warning("Failed to download Access rendition file to supplied path:\n%s", proxy_path)
                continue

        if not aip_fname:
            LOGGER.error("Unable to retrieve filename from AIP download path:\n%s", download_path)
            LOGGER.error("Manual AIP download filename update required for record <%s>", ob_num)

        alt_num = [
            {"alternative_number.type": record.get("alternative_number_type1")},
            {"alternative_number": record.get("alternative_number1")},
            {"alternative_number.type": "AIP download filename"},
            {"alternative_number": aip_fname}
        ]
        xml_update = adlib.create_record_data(CID_API, "archivescatalogue", sess, priref, alt_num)
        updated_record = adlib.post(CID_API, xml_update, "archivescatalogue", "updaterecord" sess)
        if aip_fname in str(updated_record):
            LOGGER.info("CID record <%s> updated with Alternative Number data for AIP", priref)
        else:
            LOGGER.warning("CID record <%s> failed to update AIP download filename:\n%s", priref, aip_fname)
        sys.exit("Just trying one pass first")

    LOGGER.info("==== AIP Proxy Download END ===================")
