#! /usr/bin/env /usr/local/bin/python3

"""
Script to sort files in segmented/rna_mkv folders
based on PAR/DAR/Height into the correct autoingest
folders. Also to make alterations to MKV metadata
when DAR found to be 1.26. Change to 1.29.

main():
1. Begin iterating list of FOLDERS
2. Skip any file names that are not named correctly
3. Extract DAR, PAR and Height of file
4. Check DAR is not 1.26, if so use mkvpropedit to adjust
5. Assess from height/DAR which autoingest path needed
6. Move file from folder to new autoingest target path

Converted from Py2 legacy code to Py3
October 2022
"""

# Public modules
import logging
import os
import re
import shutil
import subprocess
import sys
from typing import Final, Optional

# Public packages
sys.path.append(os.environ["CODE"])
import adlib_v3 as adlib
import utils

# Configure adlib
CID_API: Final = utils.get_current_api()

# Setup logging
LOGGER = logging.getLogger("aspect_ratio_triage")
LOGS = os.environ["SCRIPT_LOG"]
HDLR = logging.FileHandler(os.path.join(LOGS, "aspect_ratio_triage.log"))
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

FOLDERS: Final = {
    f"{os.environ['QNAP_H22']}/processing/segmented/": f"{os.environ['AUTOINGEST_QNAP02']}ingest/proxy/video/adjust/",
    f"{os.environ['QNAP_H22']}/processing/rna_mkv/": f"{os.environ['AUTOINGEST_QNAP02']}ingest/proxy/video/adjust/",
    f"{os.environ['GRACK_H22']}/processing/rna_mkv/": f"{os.environ['AUTOINGEST_H22']}ingest/proxy/video/adjust/",
    f"{os.environ['QNAP_08']}/processing/segmented/": f"{os.environ['AUTOINGEST_QNAP08']}ingest/proxy/video/adjust/",
    f"{os.environ['QNAP_08']}/memnon_processing/segmented/": f"{os.environ['AUTOINGEST_QNAP08']}ingest/proxy/video/adjust/",
    f"{os.environ['QNAP_10']}/processing/segmented/": f"{os.environ['AUTOINGEST_QNAP10']}ingest/proxy/video/adjust/",
    f"{os.environ['QNAP_VID']}/processing/segmented/": f"{os.environ['AUTOINGEST_QNAP01']}ingest/proxy/video/adjust/",
}


def get_dar(fullpath: str) -> str:
    """
    Retrieves metadata DAR info and returns as string
    trimmed to 5 spaces
    """
    dar = utils.get_metadata("Video", "DisplayAspectRatio", fullpath)
    if len(dar) <= 5:
        return dar[:5]


def get_par(fullpath: str) -> str:
    """
    Retrieves metadata PAR info and returns
    Checks if multiples from multi video tracks
    """
    par_full: str = utils.get_metadata("Video", "PixelAspectRatio", fullpath)
    if len(par_full) <= 5:
        return par_full
    return par_full[:5]


def get_height(fullpath: str) -> str:
    """
    Retrieves height information via mediainfo
    Using sampled height where original
    height and stored height differ (MXF samples)
    """

    sheight: int | str = utils.get_metadata("Video", "Sampled_height", fullpath)
    rheight: int | str = utils.get_metadata("Video", "Height", fullpath)

    try:
        int(sheight)
    except ValueError:
        sheight = 0

    if sheight:
        height: str | list[str] = [
            str(sheight) if int(sheight) > int(rheight) else str(rheight)
        ]
    else:
        height = str(rheight)

    if "480" in height:
        return "480"
    if "486" in height:
        return "486"
    if "576" in height:
        return "576"
    if "608" in height:
        return "608"
    if "720" in height:
        return "720"
    if "1080" in height or "1 080" in height:
        return "1080"

    height = height.split(" pixel", maxsplit=1)[0]
    return re.sub("[^0-9]", "", height)


def check_parent_aspect_ratio(object_number: str) -> Optional[str]:
    """
    Retrieve the DAR from the parent item record
    object_number supplied
    """
    if not object_number.startswith("N_"):
        return None

    search = f"object_number='{object_number}'"
    hits, record = adlib.retrieve_record(CID_API, "media", search, "1", ["priref"])
    print(f"Check media record response: {hits} hits\n{record}")
    if hits is None or hits == 0:
        return None

    priref = adlib.retrieve_field_name(record[0], "priref")[0]
    rec = adlib.retrieve_record(
        CID_API, "media", f'priref="{priref}"', "1", ["source_item.lref"]
    )[1]
    source_priref = adlib.retrieve_field_name(rec[0], "priref")[0]
    source_rec = adlib.retrieve_record(
        CID_API, "media", f'priref="{source_priref}"', "1", ["aspect_ratio"]
    )[1]
    return adlib.retrieve_field_name(source_rec[0], "aspect_ratio")[0]


def get_colour(fullpath: str) -> tuple[str, str]:
    """
    Retrieves colour information via mediainfo and returns in correct FFmpeg format
    """

    colour_prim = utils.get_metadata("Video", "colour_primaries", fullpath)
    col_matrix = utils.get_metadata("Video", "matrix_coefficients", fullpath)

    if "BT.709" in colour_prim:
        color_primaries = "bt709"
    elif "BT.601" in colour_prim and "NTSC" in colour_prim:
        color_primaries = "smpte170m"
    elif "BT.601" in colour_prim and "PAL" in colour_prim:
        color_primaries = "bt470bg"
    else:
        color_primaries = ""

    if "BT.709" in col_matrix:
        colormatrix = "bt709"
    elif "BT.601" in col_matrix:
        colormatrix = "smpte170m"
    elif "BT.470" in col_matrix:
        colormatrix = "bt470bg"
    else:
        colormatrix = ""

    return (color_primaries, colormatrix)


def adjust_par_metadata(filepath: str) -> bool:
    """
    Use MKVToolNix MKVPropEdit to
    adjust the metadata for PAR output
    check output correct
    """
    dar = get_dar(filepath)

    cmd: list[str] = [
        "mkvpropedit",
        filepath,
        "--edit",
        "track:v1",
        "--set",
        "display-width=295",
        "--set",
        "display-height=228",
    ]

    confirmed = subprocess.run(
        cmd,
        shell=False,
        check=True,
        universal_newlines=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    confirmed = str(confirmed.stdout)
    print(confirmed)

    if "The changes are written to the file." not in str(confirmed):
        LOGGER.warning("DAR conversion failed: %s", confirmed)
        return False

    new_dar = get_dar(filepath)
    if "1.29" in new_dar:
        return True


def fix_aspect_ratio(fpath: str) -> Optional[bool]:
    """
    Change AR 4x3 to 16x9
    Crop not needed
    Should always be MKV,
    reject if MOV found
    """
    if not fpath.endswith((".mkv", ".MKV")):
        LOGGER.warning(
            "Skipping: File that needs change to 16x9 is not an FFV1 Matroska: %s",
            fpath,
        )
        return False

    fpath_split: str = os.path.splitext(fpath)[0]
    replace: str = f"{fpath_split}_16x9.mkv"

    launch: list[str] = ["ffmpeg", "-i", fpath]

    cv: list[str] = ["-c", "copy", "-map", "0"]

    aspect: list[str] = ["-aspect", "16:9", replace]

    command: list[str] = launch + cv + aspect

    try:
        process = subprocess.run(command, shell=False, capture_output=True, text=True)
    except subprocess.CalledProcessError as err:
        LOGGER.warning(err)
        return False

    if process.returncode != 0:
        if os.path.exists(replace):
            LOGGER.warning(
                "Subprocess returncode was not 0, deleted failed transcode attempt"
            )
            os.remove(replace)
            return False
    else:
        if os.path.exists(replace):
            check_dar = get_dar(replace)
            if "1.77" in str(check_dar):
                orig_size = utils.get_size(fpath)
                new_size = utils.get_size(replace)
                if orig_size == new_size:
                    os.remove(fpath)
                    os.rename(replace, fpath)
                    return True
            else:
                LOGGER.warning(
                    "DAR incorrect OR file size mismatch, deleting failed transcode attempt"
                )
                os.remove(replace)
                return False
        else:
            LOGGER.info("Reshape of 4x3 file to 16x9 failed: %s.", fpath)
            LOGGER.info("Manual fix needed to complete process.")
            return False


def main():
    """
    Iterate folders, checking for files (not partial)
    extract metdata and filter to correct autoingest path
    """
    LOGGER.info("==== aspect.py START =================")

    for fol in FOLDERS:
        if not utils.check_storage(fol):
            LOGGER.info(
                "Skipping path %s - prevented by Storage Control document.", fol
            )
            continue
        LOGGER.info("Targeting folder: %s", fol)
        files = []
        for root, _, filenames in os.walk(fol):
            files += [os.path.join(root, file) for file in filenames]

        for f in files:
            if not utils.check_control("split_control_delete"):
                LOGGER.info(
                    "Script run prevented by downtime_control.json. Script exiting."
                )
                sys.exit(
                    "Script run prevented by downtime_control.json. Script exiting."
                )

            fn = os.path.basename(f)
            # Require N-* <object_number>
            if not fn.startswith("N_"):
                print(f"{f}\tFilename does not start with N_")
                continue

            # Require partWhole
            if "of" not in fn:
                print(f"{f}\tFilename does not contain _of_")
                LOGGER.warning("%s\tFilename does not contain _of_", f)
                continue

            # Ignore partials
            if "partial" in fn:
                print(f"Skipping: Partial in filename {fn}")
                continue

            ext = f.split(".")[-1]

            # Get height
            LOGGER.info(f"** Checking file: {f}")
            height = get_height(f)
            print(f"Height: {height}")

            # Test for 608 line height
            if not height:
                print(f"{f}\tCould not fetch frame height (px)")
                LOGGER.warning("%s\tCould not fetch frame height (px)", f)
                continue

            # Check aspect ratio of CID item record
            ob_num: str = utils.get_object_number(fn)
            aspect = check_parent_aspect_ratio(ob_num)
            print(aspect)
            if "16:9" in str(aspect):
                LOGGER.info(
                    "* Parent DAR: %s. File requires conversion to DAR 16:9", aspect
                )
                if not fix_aspect_ratio(f):
                    LOGGER.warning("Unsuccessful attempt to change DAR to 16:9 %s", fn)
                    continue
                LOGGER.info(
                    "File metadata updated to 16:9 and file replaced with new version: %s",
                    fn,
                )

            # Check PAR and DAR
            dar = get_dar(f)
            par = get_par(f)
            if not dar:
                print(f"{f}\tCould not fetch DAR from header")
                LOGGER.warning("%s\tCould not fetch DAR from header", f)
                continue
            if not par:
                print(f"{f}\tCould not fetch PAR from header")
                LOGGER.warning("%s\tCould not fetch PAR from header", f)
                continue
            # Update CID with DAR warning
            if "1.26" in dar:
                print(f"{f}\tFile found with 1.26 DAR. Converting to 1.29 DAR")
                LOGGER.info("%s\tFile found with 1.26 DAR. Converting to 1.29 DAR", f)
                confirmed = adjust_par_metadata(f)
                if not confirmed:
                    print(f"{f}\tCould not adjust DAR metdata. Skipping file.")
                    LOGGER.warning(
                        "%s\tCould not adjust DAR metadata. Skipping file.", f
                    )
                    continue
                print(f"{f}\tFile DAR header metadata changed to 1.29")
                LOGGER.info("%s\tFile DAR header metadata changed to 1.29", f)

            # Collect decimalised aspects
            aspects = []
            # for a, b in [i.split(':') for i in [dar, par]]:
            decimal = float(dar) / float(par)
            print(decimal)
            aspects.append(decimal)

            if not aspects:
                print(f"{f}\tCould not handle aspects")
                LOGGER.warning("%s\tCould not handle aspects", f)
                continue

            # Test aspects
            target_aspect = None
            if height == "608":
                target_aspect = None
                target_height = os.path.join("608")
                target_path = os.path.join(FOLDERS[fol], target_height)
                target = os.path.join(target_path, fn)
                print(f"Moving {f}\t to {target}")

                try:
                    shutil.move(f, target)
                    LOGGER.info("%s\tSuccessfully moved to target: %s\t", f, target)
                except Exception:
                    LOGGER.warning("%s\tCould not move to target: %s\t", f, target)
                    raise

            elif all(a > 1.42 for a in aspects):
                target_aspect = os.path.join("16x9", ext)
            elif all(a < 1.4 for a in aspects):
                target_aspect = os.path.join("4x3", ext)
            else:
                print(f"{f}\tCould not resolve aspects: {aspects}\t")
                LOGGER.warning("%s\tCould not resolve aspects: %s\t", f, aspects)
                continue

            if target_aspect:
                target_path = os.path.join(FOLDERS[fol], target_aspect)
                target = os.path.join(target_path, fn)
                print(f"Moving {f}\t to {target}")

                try:
                    shutil.move(f, target)
                    LOGGER.info("%s\tSuccessfully moved to target: %s\t", f, target)
                except Exception:
                    LOGGER.warning("%s\tCould not move to target: %s\t", f, target)
                    raise

    LOGGER.info("==== aspect.py END ===================\n")


if __name__ == "__main__":
    main()
