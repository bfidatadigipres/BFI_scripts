#!/usr/bin/env python3

"""
Special Collections Document tranfsers for OSH
Moving renamed folders to SFTP / Archivematica

Script stages:
MUST BE SUPPLIED WITH SYS.ARGV[1] AT SUB-FOND LEVEL PATH
AND SYS.ARGV[2] AT SFTP/ARCHIVEMATICA TOP FOLDER LEVEL

Iterate through supplied sys.argv[1] folder path completing:
1. For each subfolder split folder name: ob_num / ISAD(G) level / Title
2. Build SFTP command for each level and move to Archivematica Transfer Storage
   JMW: Do we need to move containing folders over as 'OpenRecords' to get slugs?
3. When SFTP complete, configure an Archivematica package transfer with specific
   details including AtoM proposed 'slug', with each package set to ClosedRecords
   status, blocking it's appearance in AtoM until moved to an Open status
   JMW: What are we doing about sub-sub-series / sub-sub-sub-series mapping?
4. Check that the transfer status is complete
5. Upload SIP UUID / AIP UUID to CID item record
   JMW: Field location to be identified? label.type (enum needed)
   JMW: Do we want other statement of AIP in Archivematica in CID?
6. Capture all outputs to logs

NOTES:
Some assumptions in code 
1. That to make AtoM slug we may need an additional stage  / manual work
2. That we do not PUT folders with 'fonds' to 'sub-sub-sub-series' levels, just
   use them to inform SFTP folder structures, and (when known) slug names for AtoM
3. That the slug will be named after the CID object number of parent folder
4. That we will write the transfer / aip UUIDs to the CID label text fields
5. AtoM records already existing, won't need recreating as CLOSED?
6. Slug creation needs considering where sub-sub-series and lower levels are not
   supported in AtoM

2025
"""

# Public packages
import datetime
import logging
import os
import sys
import tenacity
from time import sleep

# Private packages
import archivematica_sip_utils as am_utils
sys.path.append(os.environ.get("CODE"))
import adlib_v3 as adlib
import utils

LOG = os.path.join(
    os.environ.get("LOG_PATH"), "special_collections_document_transfer_osh.log"
)
CID_API = os.environ.get("CID_API4")
# CID_API = utils.get_current_api()

LOGGER = logging.getLogger("sc_document_transfer_osh")
HDLR = logging.FileHandler(LOG)
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

LEVEL = [
    '_fonds_',
    '_sub-fonds_',
    '_series_',
    '_sub-series_',
    '_sub-sub-series_',
    '_sub-sub-sub-series_',
    '_file_'
]


def top_folder_split(fname, prefix):
    """
    Split folder name into parts
    """
    fsplit = fname.split("_", 2)
    if len(fsplit) != 3:
        LOGGER.warning("Folder has not split as anticipated: %s", fsplit)
        return None, None, None
    ob_num, record_type, title = fsplit
    if not ob_num.startswith(prefix):
        LOGGER.warning("Object number is not formatted as anticipated: %s", ob_num)
        return None, None, None

    return ob_num, record_type, title


def main():
    """
    Iterate supplied folder
    and complete series of
    SFTP / transfer
    """
    if not utils.check_control("power_off_all"):
        LOGGER.info("Script run prevented by downtime_control.json. Script exiting.")
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")
    if not utils.cid_check(CID_API):
        sys.exit("* Cannot establish CID session, exiting script")
    if sys.argv < 3:
        print("Path has not been supplied to script. Exiting.")
    base_dir = sys.argv[1]  # Always sub_fond level path
    top_level_folder = sys.argv[2] # Specified SFTP top level folder

    if not os.path.exists(base_dir):
        sys.exit(f"Exiting. Path could not be found: {base_dir}")
    if top_level_folder not in base_dir:
        sys.exit("Exiting, folder name {top_level_folder} or path formatted incorrectly {base_dir}")

    LOGGER.info(
        "=========== Special Collections Archivematica - Document Transfer OSH START ============"
    )
    LOGGER.info("Path target: %s", base_dir)

    # Start processing paths
    for root, dirs, _ in os.walk(base_dir):
        for directory in dirs:
            dpath = os.path.join(root, directory)
            record_type = None
            if any(x in directory for x in LEVEL):
                ob_num, record_type, title = top_folder_split(directory, top_level_folder.split('-', 1)[0])
            else:
                ob_num, title = directory.split('_', 1)

            # PUT objects only to SFTP
            if record_type is None:
                LOGGER.info("Folder identified for ob num %s, title %s - type: %s", ob_num, title, record_type)
                put_files = am_utils.send_to_sftp(dpath, top_level_folder)
                if put_files is None:
                    LOGGER.warning("SFTP PUT failed for folder: %s %s", ob_num, dpath)
                    continue
                files = os.listdir(dpath)
                if put_files[0] in files:
                    LOGGER.info("SFTP Put successful: %s moved to Archivematica", put_files)
                else:
                    LOGGER.warning("Problem with files put in folder %s: %s", directory, put_files)
                    continue

                # Make vars for Archivematica / Slug
                dpath_split = dpath.split(top_level_folder)[-1]
                am_path = os.path.join(top_level_folder, dpath_split)
                # JMW: Decision still forthcoming regarding slug name / ob_num - if former matching may be needed
                atom_slug = os.path.basename(os.path.split(am_path)[0]).split('_', 1)[0].lower()
                if am_utils.get_slug_match(atom_slug) is False:
                    LOGGER.warning("Supposed slug cannot be found in AtoM objects.")
                    # JMW: Make slug here?
                    continue

                item_rec = adlib.retrieve_record(CID_API, 'archivescatalogue', f'object_number="{ob_num}"', 1, ['priref'])[1]
                item_priref = adlib.retrieve_field_name(item_rec[0], 'priref')[1]

                # Move to Archivematica
                processing_config = 'ClosedRecords' # Fixed as closed in this code
                LOGGER.info("Moving SFTP directory %s to Archivematica as %s", directory, processing_config)
                response = am_utils.send_as_package(am_path, atom_slug, item_priref, processing_config, True)
                if 'id' not in response:
                    LOGGER.warning("Possible failure for Archivematica creation: %s", response)
                    continue

                transfer_uuid = response.get('id')
                transfer_dict = check_transfer_status(transfer_uuid, directory)
                if not transfer_dict:
                    LOGGER.warning("Transfer confirmation not found after 10 minutes for directory %s", directory)
                    LOGGER.warning("Manual assistance needed to update UUIDs to CID item record")
                    continue
                sip_uuid = transfer_dict.get('sip_uuid')
                LOGGER.info(transfer_dict)
                ingest_dict = check_ingest_status(sip_uuid, directory)
                if not ingest_dict:
                    LOGGER.warning("Ingest confirmation not found after 10 minutes for directory %s", directory)
                    LOGGER.warning("Manual assistance needed to update AIP UUID to CID item record")
                    continue
                aip_uuid = ingest_dict.get('uuid')
                LOGGER.info(ingest_dict)

                # Update transfer, SIP and AIP UUID to CID item record
                # JMW: If adopted new enumeration needed for label.type
                uuid = [
                    {"label.type": "ARTEFACTUALUUID"},
                    {"label.source": "Transfer UUID"},
                    {"label.date":str(datetime.datetime.now())[:10]},
                    {"label.text": transfer_uuid},
                    {"label.type": "ARTEFACTUALUUID"},
                    {"label.source": "SIP UUID"},
                    {"label.date":str(datetime.datetime.now())[:10]},
                    {"label.text": sip_uuid}
                    {"label.type": "ARTEFACTUALUUID"},
                    {"label.source": "AIP UUID"},
                    {"label.date":str(datetime.datetime.now())[:10]},
                    {"label.text": aip_uuid}
                ]
                print(f"Label values:\n{uuid}")

                # Start creating CID Work Series record
                uuid_xml = adlib.create_record_data(CID_API, "archivescatalogue", item_priref, uuid)
                print(uuid_xml)
                try:
                    print("Attempting to create CID record")
                    rec = adlib.post(CID_API, uuid_xml, "archivescatalogue", "updaterecord")
                    if rec is None:
                        LOGGER.warning("Failed to update record:\n%s", uuid_xml)
                        return None
                    if "priref" not in str(rec):
                        LOGGER.warning("Failed to update new record:\n%s", item_priref)
                        return None

                except Exception as err:
                    LOGGER.warning("Unable to update UUID data to record: %s", err)

                LOGGER.info("Completed upload of %s\n", directory)
            else:
                LOGGER.info("Skipping PUT of %s - no objects enclosed", directory)

    LOGGER.info(
        "=========== Special Collections Archivematica - Document Transfer OSH END =============="
    )


@tenacity.retry(tenacity.stop_after_attempt(10))
def check_transfer_status(uuid, directory):
    '''
    Check status of transfer up to 10
    times, or until retrieved
    '''
    trans_dict = am_utils.get_transfer_status(uuid)

    if trans_dict.get('status') == 'COMPLETE' and len(trans_dict.get('sip_uuid')) > 0:
        LOGGER.info("Transfer of package completed: %s", trans_dict.get('directory', directory))
        return trans_dict
    else:
        sleep(60)
        raise Exception


@tenacity.retry(tenacity.stop_after_attempt(10))
def check_ingest_status(uuid, directory):
    '''
    Check status of transfer up to 10
    times, or until retrieved
    '''
    ingest_dict = am_utils.get_ingest_status(uuid)

    if ingest_dict.get('status') == 'COMPLETE' and len(ingest_dict.get('uuid')) > 0:
        LOGGER.info("Ingest of package completed: %s", ingest_dict.get('directory', directory))
        return ingest_dict
    else:
        sleep(60)
        raise Exception
    

if __name__ == "__main__":
    main()
