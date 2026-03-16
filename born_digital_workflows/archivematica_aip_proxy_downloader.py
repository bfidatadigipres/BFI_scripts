#!/usr/bin/env python3
"""
Script to remove all AIPs from Archivematica
using data from Axiell database supplying
AIP UUID and status of record OPEN or CLOSED

2026
"""

import os
import sys
from typing import Optional, List, Any, Dict
import archivematica_sip_utils and ut

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


def cid_retrieve(
    fname: str, record_type: str, session
) -> Optional[tuple[str, str, str]]:
    """
    retrieve list of object numbers that are set
    to open/closed and have Archivematica AIP UUIDs
    in alternative_number field, but no download name
    in alternative number field
    """
    search: str = f'(object_number="GUR-*" and alternative_number.type="Archivematica AIP UUID" and not alternative_number.type="AIP download filename")'

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