#!/usr/bin/env/ python3

"""
Move multi-item whole-tape digitisations if
the carried content has been documented and ingested.
Moves them into deletion folder, separate shell script deletes them.

Note: this script requires the BlackPearl creds loaded as
      as environment variables and the version of the
      SDK as installing /home/appdeploy/code/autoingest/v/

      From the current directory:
          source ../autoingest/creds.rc
          source ../autoingest/v/bin/activate
          python backup.py

Refactored for Python3
Updated for Adlib V3
2023
"""

import logging

# Public packages
import os
import shutil
import sys
from typing import Any, Final, Optional

from ds3 import ds3

# Private packages
sys.path.append(os.environ["CODE"])
import models

import adlib_v3 as adlib
import utils

# Global variables
LOG_PATH: Final = os.environ["LOG_PATH"]
CONTROL_JSON: Final = os.path.join(LOG_PATH, "downtime_control.json")
CID_API: Final = utils.get_current_api()
CLIENT: Final = ds3.createClientFromEnv()

TARGETS: Final = [
    os.path.join(os.environ["QNAP_H22"], "processing/"),
    os.path.join(os.environ["GRACK_H22"], "processing/"),
    os.path.join(os.environ["QNAP_08"], "processing/"),
    os.path.join(os.environ["QNAP_08"], "memnon_processing/"),
    os.path.join(os.environ["QNAP_10"], "processing/"),
]

# Setup logging, overwrite each time
logger = logging.getLogger("delete_post_split")
hdlr = logging.FileHandler(os.path.join(LOG_PATH, "delete_post_split.log"))
formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)


def get_object_list(fname: str) -> list[ds3.Ds3GetObject]:
    """
    Build a DS3 object list for some SDK queries
    """
    file_list = [fname]
    return [ds3.Ds3GetObject(name=x) for x in file_list]


def bp_physical_placement(fname: str, bucket: str) -> bool:
    """
    Retrieve the physical placement with object_list
    """
    object_list = get_object_list(fname)
    query = ds3.GetPhysicalPlacementForObjectsSpectraS3Request(bucket, object_list)
    result = CLIENT.get_physical_placement_for_objects_spectra_s3(query)
    data = result.result

    if not data["TapeList"]:
        return False
    try:
        persisted = data["TapeList"][0]["AssignedToStorageDomain"]
    except (IndexError, KeyError):
        return False

    if "true" in str(persisted):
        return True


def main():
    """
    Iterate media_targets looking for files
    to process.
    """
    for media_target in TARGETS:
        if not utils.check_control("split_control_delete") or not utils.check_control(
            "black_pearl"
        ):
            logger.info(
                "Script run prevented by downtime_control.json. Script exiting."
            )
            sys.exit("Script run prevented by downtime_control.json. Script exiting.")
        if not utils.cid_check(CID_API):
            print("* Cannot establish CID session, exiting script")
            logger.critical("* Cannot establish CID session, exiting script")
            sys.exit()

        # Path to source media
        root = os.path.join(media_target, "source")
        logger.info("%s\t** Processing files in \t%s", root, root)

        # List video files in recursive sub-directories
        files = []
        for directory, _, filenames in os.walk(root):
            for filename in [
                f
                for f in filenames
                if f.endswith((".mov", ".mxf", "mkv", ".MOV", ".MKV", ".MXF"))
            ]:
                files.append(os.path.join(directory, filename))

        # Track tapes processed total
        logger.info(files)

        # Process digitised tape files sequentially
        for filepath in files:
            f = os.path.split(filepath)[1]
            if f"source/{f}" in filepath:
                print(f"Skipping, file not in numbered subfolder: {filepath}")
                continue
            print(f"Current file: {filepath}\t{f}")

            # Expect can_ID or package_number filenames
            id_ = f.split(".")[0]

            # Model carrier
            # Label
            try:
                label = models.PhysicalIdentifier(id_)
                label_type = label.type
            except Exception as err:
                message = f"{filepath}\tUnable to determine label type\t{err}"
                print(message)
                logger.warning("%s\tUnable to determine label type", filepath)
                continue

            # Carrier
            try:
                d = {label_type: id_}
                t = models.Carrier(**d)
            except Exception as err:
                message = f"{filepath}\tUnable to model carrier\t{err}"
                print(message)
                logger.warning("%s\tUnable to model carrier\t%s", filepath, str(err))
                continue

            # Parse items carried, sort in logical order
            try:
                items = t.items
            except Exception:
                continue

            if not isinstance(items, list):
                items = [items]

            # Track BlackPearl-preserved objects
            preserved_objects = {}

            # Single- or multi-item
            whole = t.partwhole[1]
            if whole == 1:
                total_objects_expected = len(items)
            else:
                total_objects_expected = whole

            # Process each item on tape
            for item in items:
                object_number = adlib.retrieve_field_name(item, "object_number")[0]

                # Check expected number of media records have been created for correct grouping
                if "/qnap_h22/" in filepath or "/qnap_10/" in filepath:
                    grouping = "398385"
                elif "memnon_processing/" in filepath:
                    grouping = "401629"
                else:
                    grouping = "397987"

                record = get_results(filepath, grouping, object_number)
                if not record:
                    logger.warning(
                        "%s\tNo CID record found for object_number %s and grouping %s",
                        filepath,
                        object_number,
                        grouping,
                    )
                    continue

                # Check that each media record umid has been preserved to tape by BlackPearl
                for rec in record:
                    bp_umid = adlib.retrieve_field_name(rec, "reference_number")[0]
                    try:
                        bucket = adlib.retrieve_field_name(rec, "preservation_bucket")[
                            0
                        ]
                    except (IndexError, TypeError, KeyError):
                        bucket = "imagen"
                    original_filename = adlib.retrieve_field_name(
                        rec, "imagen.media.original_filename"
                    )[0]
                    print(
                        f"* CID Media record has reference number {bp_umid} and original filename {original_filename}"
                    )
                    logger.info(
                        "%s\tCID Media record has reference number %s and original filename %s",
                        filepath,
                        bp_umid,
                        original_filename,
                    )

                    # Check BlackPearl physical placement
                    if len(bucket) < 3:
                        bucket = "imagen"
                    placement = bp_physical_placement(bp_umid, bucket)
                    if placement:
                        logger.info(
                            "%s\tPersisted\t%s\t%s\tBucket: %s",
                            filepath,
                            object_number,
                            bp_umid,
                            bucket,
                        )
                        print(f"Persisted: {f}\t{object_number}\t{bp_umid}\t{bucket}")
                        preserved_objects[original_filename] = bp_umid
                        print(
                            f"* Preserved objects: {preserved_objects[original_filename]}"
                        )
                        print(f"* Len(preserved_objects) = {len(preserved_objects)}")
                        print(f"* Total objects expected = {total_objects_expected}")

            deleteable = len(preserved_objects) >= total_objects_expected
            logger.info(
                "%s\tPreserved objects: %s / Total objects expected: %s",
                filepath,
                len(preserved_objects),
                total_objects_expected,
            )
            print(
                f"{f}\tDeletable={deleteable}\t{len(preserved_objects)}/{total_objects_expected}"
            )

            # Set move destination
            dst = os.path.join(media_target, f"delete/{f}")

            if deleteable and total_objects_expected > 0:
                # Delete single-item tapes
                if total_objects_expected == 1:
                    print(
                        f"* Moving single item tape file to delete folder: {filepath}"
                    )
                    try:
                        shutil.move(filepath, dst)
                        logger.info(
                            "%s\tMoved single item tape file to delete folder", filepath
                        )
                    except Exception as err:
                        logger.warning(
                            "%s\tUnable to move file to delete folder", filepath
                        )
                        print(f"* Unable to move file to delete folder: {filepath}")
                        print(err)
                        raise

                # Delete multi-item tapes:
                elif total_objects_expected >= 2:
                    print(f"Moving multi-item tape file to delete folder: {filepath}")
                    try:
                        shutil.move(filepath, dst)
                        logger.info(
                            "%s\tMoved multi-item tape file to delete folder", filepath
                        )
                    except Exception as err:
                        print(
                            f"* Unable to move file to delete folder: {filepath}\t{err}"
                        )
                        logger.warning(
                            "%s\tUnable to move file to delete folder", filepath
                        )
                        raise

            else:
                print(
                    f"* Ignoring because not all Items are persisted: {filepath}, {len(preserved_objects)} persisted, {total_objects_expected} expected"
                )
                logger.warning(
                    "%s\tIgnored because not all Items are persisted: %s persisted, %s expected",
                    filepath,
                    len(preserved_objects),
                    total_objects_expected,
                )


def get_results(filepath: str, grouping: str, object_number: str) -> dict[str, str]:
    """
    Checks for cross-over period between 'datadigipres'
    and 'collectionssystems' in CID media record
    """

    search = f'(object.object_number->((grouping.lref="{grouping}") and (input.name="datadigipres" or input.name="collectionssystems") and (source_item->(object_number="{object_number}"))))'
    fields = [
        "reference_number",
        "imagen.media.original_filename",
        "preservation_bucket",
    ]
    print(f"* Querying for ingest status of CID Item record {object_number}")
    hits, record = adlib.retrieve_record(CID_API, "media", search, "0", fields)
    print(f"Hits: {hits}\nRecord: {record}")
    if hits is None or hits == 0:
        print(f"* CID query failed to obtain result using search:\n{search}")
        logger.warning(
            "%s\tCID query failed to obtain result with input.name datadigipres",
            filepath,
        )
        return None
    if hits >= 1:
        logger.info(
            "%s\tCID Item record found, with object number %s", filepath, object_number
        )
        return record


if __name__ == "__main__":
    main()
