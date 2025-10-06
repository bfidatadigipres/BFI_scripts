#!/usr/bin/env python3

"""
VERSION WRITTEN FOR QNAP-04 STORA MP4 TRANSCODING
Script to be launched from parallel, requires sys.argv arguments
to determine correct transcode paths (RNA or BFI).

1. Receives script path from sys.argv()
   Checks in CID to see if the file is accessible:
   - Yes, retrieves input date and acquisition source.
   - No, assume item record has restricted content and skip (go to stage 14).
2. Checks metadata of each file:
   - If moving image file extracts DAR, height, duration
     (go to stage 3-end).
   - If still image go to stage 10-end.
   - If audio nothing required (go to stage 13-end).
   - If other nothing required (go to stage 13-end).
3. Video assets are checked in CID for RNA/BFI destination path for transcode.
4. Selects FFmpeg subprocess command based on DAR/height/standard with crop/stretch for SD.
5. Encodes with FFmpeg a progressive MP4 file to selected path.
6. Verifies MP4 passes mediaconch policy (therefore successful).
7. Look up in CID for MTQ yes/no and if yes, begin transcode of MP4 to HLS in specific path.
8. Uses duration to calculate how many seconds until 20% of total duration.
9. Extract JPEG image from MP4 file.
10. Uses 'gm' to generate full size(600x600ppi) and thumbnail(300x300ppi) from extracted JPEG.
11. Delete the first FFmpeg JPEG created from MP4 only.
12. Where JPEG or HLS assets (to follow) are created, write names to fields in CID media record.
13. Moves source file to completed folder for deletion.
14. Maintain log of all actions against file and dump in one lot to avoid log overlaps.

2022
Python 3.6+
"""
# Public packages
import logging
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Final, Iterable, Optional, Tuple, Union

import pytz
import tenacity

# Local packages
sys.path.append(os.environ["CODE"])
import adlib_v3 as adlib
import utils

# Global paths from environment vars
MP4_POLICY: Final = os.environ["MP4_POLICY"]
LOG_PATH: Final = os.environ["LOG_PATH"]
FLLPTH: Final = sys.argv[1].split("/")[:4]
LOG_PREFIX: Final = "_".join(FLLPTH)
LOG_FILE: Final = os.path.join(LOG_PATH, f"mp4_transcode{LOG_PREFIX}.log")
TRANSCODE: Final = os.environ["TRANSCODING"]
if not os.path.ismount(TRANSCODE):
    sys.exit(f"{TRANSCODE} path is not mounted. Script exiting.")
CID_API: Final = utils.get_current_api()
HOST: Final = os.uname()[1]

# Setup logging
if LOG_PREFIX != "_mnt_qnap_04_autoingest":
    sys.exit(f"Incorrect filepath received: {LOG_PREFIX}")
LOGGER = logging.getLogger("mp4_transcode_make_jpeg")
HDLR = logging.FileHandler(LOG_FILE)
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

SUPPLIERS: Final = {
    "East Anglian Film Archive": "eafa",
    "Imperial War Museum": "iwm",
    "London's Screen Archive": "lsa",
    "MACE": "mace",
    "North East Film Archive": "nefa",
    "Northern Ireland Screen": "nis",
    "Scottish Screen Archive": "nls",
    "National Screen and Sound Archive of Wales": "nssaw",
    "North West Film Archive": "nwfa",
    "Screen Archive South East": "sase",
    "Box, The": "thebox",
    "Wessex Film and Sound Archive": "wfsa",
    "Yorkshire Film Archive": "yfa",
}


def local_time() -> str:
    """
    Return strftime object formatted
    for London time (includes BST adjustment)
    """
    return datetime.now(pytz.timezone("Europe/London")).strftime("%Y-%m-%d %H:%M:%S")


def main():
    """
    Check sys.argv[1] populated
    Get ext, check filetype then process
    according to video, image or pass through
    audio and documents
    """
    if len(sys.argv) < 2:
        sys.exit("EXIT: Not enough arguments")

    fullpath = sys.argv[1]
    if not os.path.isfile(fullpath):
        sys.exit("EXIT: Supplied path is not a file")

    # Multiple instances of script so collecting logs for one burst output
    if not utils.check_control("mp4_transcode"):
        LOGGER.info("Script run prevented by downtime_control.json. Script exiting.")
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")
    if not utils.check_storage(fullpath) or not utils.check_storage(TRANSCODE):
        LOGGER.info("Script run prevented by Storage Control document. Script exiting.")
        sys.exit("Script run prevented by storage_control.json. Script exiting.")
    if not utils.cid_check(CID_API):
        LOGGER.critical("* Cannot establish CID session, exiting script")
        sys.exit("* Cannot establish CID session, exiting script")
    log_build = []

    filepath, file = os.path.split(fullpath)
    fname, ext = os.path.splitext(file)
    completed_pth = os.path.join(os.path.split(filepath)[0], "completed/", file)

    log_build.append(
        f"{local_time()}\tINFO\t================== START Transcode MP4 make JPEG {file} {HOST} =================="
    )
    print(f"File to be processed: {file}. Completed path: {completed_pth}")
    outpath, outpath2 = "", ""

    ext = ext.lstrip(".")
    print(file, fname, ext)
    # Check CID for Item record and extract transcode path
    object_number = utils.get_object_number(fname)
    if object_number is None or object_number is False:
        object_number = ""
    if object_number.startswith("CA_"):
        priref, source, groupings = check_item(object_number, "collectionsassets")
    else:
        priref, source, groupings = check_item(object_number, "items")
    # Check CID media record and extract input date for path
    media_priref, input_date, largeimage, thumbnail, access = get_media_priref(file)
    if not media_priref:
        log_build.append(
            f"{local_time()}\tCRITICAL\tDigital media record priref missing: {file}"
        )
        log_build.append(
            f"{local_time()}\tINFO\t==================== END Transcode MP4 and make JPEG {file} ==================="
        )
        log_output(log_build)
        sys.exit("EXITING: Digital media record missing. See logs.")
    if not priref and not input_date:
        # Record inaccessible (possible access restrictions)
        log_build.append(
            f"{local_time()}\tWARNING\tProblems accessing CID to retrieve Item record data: {object_number}"
        )
        log_build.append(
            f"{local_time()}\tINFO\t==================== END Transcode MP4 and make JPEG {file} ==================="
        )
        log_output(log_build)
        sys.exit(f"EXITING: Unable to retrieve item details from CID: {object_number}")

    date_pth = input_date.replace("-", "")[:6]
    if len(date_pth) <= 5:
        sys.exit(f"Error with date path: {date_pth}. Script exiting.")
    if "H22: Video Digitisation: Item Outcomes" in str(groupings) and source:
        log_build.append(
            f"{local_time()}\tINFO\t** Source for H22 video: {source} ****"
        )
        rna_pth = "bfi"
        for key, val in SUPPLIERS.items():
            if key in str(source):
                rna_pth = val
        transcode_pth = os.path.join(TRANSCODE, rna_pth, date_pth)
    else:
        transcode_pth = os.path.join(TRANSCODE, "bfi", date_pth)

    # Check if transcode already completed
    if fname in access and thumbnail and largeimage:
        log_build.append(
            f"{local_time()}\tINFO\tMedia record already has Imagen Media UMIDs. Checking for transcodes"
        )
        if os.path.exists(os.path.join(transcode_pth, fname)):
            log_build.append(
                f"{local_time()}\tINFO\tTranscode file already exists. Moving {file} to completed folder"
            )
            try:
                shutil.move(fullpath, completed_pth)
            except Exception:
                log_build.append(
                    f"{local_time()}\tINFO\tMove to completed/ path has failed. Script exiting."
                )
            log_output(log_build)
            sys.exit(f"EXITING: File {file} has already been processed.")
        else:
            log_build.append(
                f"{local_time()}\tWARNING\tCID UMIDs exist but no transcoding. Allowing files to proceed."
            )

    # Get file type, video or audio etc.
    ftype = utils.sort_ext(ext)
    if ftype == "audio":
        log_build.append(
            f"{local_time()}\tINFO\tItem is an audio file. No actions required at this time."
        )
        log_build.append(
            f"{local_time()}\tINFO\tMoving {file} to Autoingest completed folder: {completed_pth}"
        )
        shutil.move(fullpath, completed_pth)
        log_build.append(
            f"{local_time()}\tINFO\t==================== END Transcode MP4 and make JPEG {file} ==================="
        )
        log_output(log_build)
        sys.exit()

    elif ftype == "document":
        log_build.append(
            f"{local_time()}\tINFO\tItem is a document. No actions required at this time."
        )
        log_build.append(
            f"{local_time()}\tINFO\tMoving {file} to Autoingest completed folder: {completed_pth}"
        )
        shutil.move(fullpath, completed_pth)
        log_build.append(
            f"{local_time()}\tINFO\t==================== END Transcode MP4 and make JPEG {file} ==================="
        )
        log_output(log_build)
        sys.exit()

    elif ftype == "video":
        log_build.append(
            f"{local_time()}\tINFO\tItem is video. Checking for DAR, height and duration of video."
        )
        if not os.path.exists(transcode_pth):
            log_build.append(f"Creating new transcode path: {transcode_pth}")
            os.makedirs(transcode_pth, mode=0o777, exist_ok=True)

        audio, stream_default = check_audio(fullpath)
        dar = get_dar(fullpath)
        par = get_par(fullpath)
        height = get_height(fullpath)
        width = get_width(fullpath)
        duration, vs = get_duration(fullpath)
        log_build.append(
            f"{local_time()}\tINFO\tData retrieved: Audio {audio}, DAR {dar}, PAR {par}, Height {height}, Width {width}, Duration {duration} secs"
        )

        # CID transcode paths
        outpath = os.path.join(transcode_pth, f"{fname}.mp4")
        outpath2 = os.path.join(transcode_pth, fname)
        log_build.append(f"{local_time()}\tINFO\tMP4 destination will be: {outpath2}")

        retry = False

        # Final check file not parallel processed already
        if not os.path.isfile(fullpath):
            log_build.append(f"{local_time()}\tINFO\tFile for processing no longer in transcode/ path. Exiting")
            log_output(log_build)
            sys.exit("EXIT: Supplied path is not a file")

        # Check to ensure that the file isn't already being processed
        check_name = os.path.join(transcode_pth, fname)
        if os.path.exists(check_name):
            log_build.append(f"{local_time()}\tINFO\tFile has already been processed. Exiting")
            log_output(log_build)
            sys.exist("File has already completed processing. Skipping")
        if os.path.exists(f"{check_name}.mp4"):
            delete_confirm = check_mod_time(f"{check_name}.mp4")
            if delete_confirm is True:
                os.remove(f"{check_name}.mp4")
            else:
                log_build.append(f"{local_time()}\tINFO\tFile being processed concurrently. Exiting")
                log_output(log_build)
                sys.exit("File already being processed. Skipping.")

        # Build FFmpeg command based on dar/height
        ffmpeg_cmd = create_transcode(
            fullpath, outpath, height, width, dar, par, audio, stream_default, vs, retry
        )
        if not ffmpeg_cmd:
            log_build.append(
                f"{local_time()}\tWARNING\tFailed to build FFmpeg command with data: {fullpath}\nHeight {height} Width {width} DAR {dar}"
            )
            log_output(log_build)
            sys.exit("EXIT: Failure to create FFmpeg command. Please see logs")

        ffmpeg_call_neat = " ".join(ffmpeg_cmd)
        log_build.append(
            f"{local_time()}\tINFO\tFFmpeg call created:\n{ffmpeg_call_neat}"
        )

        try:
            subprocess.run(
                ffmpeg_cmd,
                shell=False,
                check=True,
                universal_newlines=True,
                stderr=subprocess.PIPE,
            ).stderr
        except Exception as err:
            log_build.append(
                f"{local_time()}\tWARNING\tFFmpeg command failed first pass. Retrying without video filters:\n{err}"
            )

        if os.path.exists(outpath):
            log_build.append("MP4 transcode completed successfully")
        else:
            retry = True
            ffmpeg_cmd_retry = create_transcode(
                fullpath,
                outpath,
                height,
                width,
                dar,
                par,
                audio,
                stream_default,
                vs,
                retry,
            )
            if not ffmpeg_cmd_retry:
                log_build.append(
                    f"{local_time()}\tWARNING\tFailed to build FFmpeg command for FFmpeg retry: {fullpath}\nHeight {height} Width {width} DAR {dar}"
                )
                log_output(log_build)
                sys.exit("EXIT: Failure to create FFmpeg command. Please see logs")
            ffmpeg_call_neat2 = " ".join(ffmpeg_cmd_retry)
            log_build.append(
                f"{local_time()}\tINFO\tFFmpeg retry call created with video filters:\n{ffmpeg_call_neat2}"
            )
            try:
                subprocess.run(
                    ffmpeg_cmd_retry,
                    shell=False,
                    check=True,
                    universal_newlines=True,
                    stderr=subprocess.PIPE,
                ).stderr
            except Exception as err:
                log_build.append(
                    f"{local_time()}\tCRITICAL\tFFmpeg command failed twice: {ffmpeg_call_neat}\n{err}"
                )
                log_build.append(
                    f"{local_time()}\tINFO\t==================== END Transcode MP4 and make JPEG {file} ==================="
                )
                print(err)
                log_output(log_build)
                sys.exit("FFmpeg command failed twice. Script exiting.")

        time.sleep(2)
        # Mediaconch conformance check file
        policy_check = conformance_check(outpath)
        if "PASS!" in policy_check:
            log_build.append(
                f"{local_time()}\tINFO\tMediaconch pass! MP4 transcode complete. Beginning JPEG image generation."
            )
        else:
            log_build.append(
                f"{local_time()}\tINFO\tWARNING: MP4 failed policy check: {policy_check}"
            )
            log_build.append(
                f"{local_time()}\tINFO\tDeleting transcoded MP4 and leaving file for repeated transcode attempt"
            )
            os.remove(outpath)
            log_build.append(
                f"{local_time()}\tINFO\t==================== END Transcode MP4 and make JPEG {file} ==================="
            )
            log_output(log_build)
            sys.exit("EXIT: Transcode failure. Please see logs")

        # Start JPEG extraction
        jpeg_location = os.path.join(transcode_pth, f"{fname}.jpg")
        print(f"JPEG output to go here: {jpeg_location}")

        # Calculate seconds mark to grab screen
        seconds = adjust_seconds(duration)
        if seconds is None:
            log_build.append(f"{local_time()}\tWARNING\tSeconds not found from duration: {duration}")
            log_build.append(f"{local_time()}\tWARNING\tCleaning up MP4 creation")
            log_output(log_build)
            sys.exit("Exiting: JPEG not created from MP4 file - duration data missing")

        log_build.append(f"{local_time()}\tINFO\tSeconds for JPEG cut: {seconds}")
        success = get_jpeg(seconds, outpath, jpeg_location)
        if not os.path.isfile(outpath):
            dif_secs = seconds // 2
            log_build.append(
                f"{local_time()}\tINFO\tSeconds for JPEG cut retry: {dif_secs}"
            )
            success = get_jpeg(dif_secs, outpath, jpeg_location)
            if not success:
                log_build.append(
                    f"{local_time()}\tWARNING\tFailed to create JPEG from MP4 file"
                )
                log_build.append(
                    f"{local_time()}\tINFO\t==================== END Transcode MP4 and make JPEG {file} ==================="
                )
                log_output(log_build)
                sys.exit("Exiting: JPEG not created from MP4 file")

        # Generate Full size 600x600, thumbnail 300x300
        full_jpeg = make_jpg(jpeg_location, "full", None, None)
        thumb_jpeg = make_jpg(jpeg_location, "thumb", None, None)
        print(full_jpeg, thumb_jpeg)

        if thumb_jpeg is None:
            thumb_jpeg = ""
        if full_jpeg is None:
            full_jpeg = ""

        log_build.append(
            f"{local_time()}\tINFO\tNew images created at {seconds} seconds into video:\n - {full_jpeg}\n - {thumb_jpeg}"
        )
        if os.path.isfile(full_jpeg) and os.path.isfile(thumb_jpeg):
            os.remove(jpeg_location)
        else:
            log_build.append(
                f"{local_time()}\tWARNING\tOne of the JPEG images hasn't created, please check outpath: {jpeg_location}"
            )

        # Clean up MP4 extension
        os.replace(outpath, outpath2)

    elif ftype == "image":

        oversize = False
        log_build.append(
            f"{local_time()}\tINFO\tItem is image. Generating large (full size copy) and thumbnail jpeg images."
        )
        size = os.path.getsize(fullpath)
        if 104857600 <= int(size) <= 209715200:
            log_build.append(
                f"{local_time()}\tINFO\tImage is over 100MB. Applying resize to large image."
            )
            percent = "75"
            oversize = True
        elif 209715201 <= int(size) <= 314572800:
            log_build.append(
                f"{local_time()}\tINFO\tImage is over 200MB. Applying resize to large image."
            )
            percent = "60"
            oversize = True
        elif 314572801 <= int(size) <= 419430400:
            log_build.append(
                f"{local_time()}\tINFO\tImage is over 300MB. Applying resize to large image."
            )
            percent = "45"
            oversize = True
        elif int(size) > 419430401:
            log_build.append(
                f"{local_time()}\tINFO\tImage is over 400MB. Applying resize to large image."
            )
            percent = "30"
            oversize = True

        # Create image files from source image
        if not os.path.exists(transcode_pth):
            log_build.append(f"Creating new transcode path: {transcode_pth}")
            os.makedirs(transcode_pth, mode=0o777, exist_ok=True)

        if not oversize:
            full_jpeg = make_jpg(fullpath, "full", transcode_pth, None)
        else:
            full_jpeg = make_jpg(fullpath, "oversize", transcode_pth, percent)

        thumb_jpeg = make_jpg(fullpath, "thumb", transcode_pth, None)

        if thumb_jpeg is None:
            thumb_jpeg = ""
        if full_jpeg is None:
            full_jpeg = ""
        if os.path.isfile(full_jpeg) and os.path.isfile(thumb_jpeg):
            log_build.append(
                f"{local_time()}\tINFO\tNew images created:\n - {full_jpeg}\n - {thumb_jpeg}"
            )
        else:
            log_build.append(
                f"{local_time()}\tERROR\tOne of both JPEG image creations failed for file %s",
                file,
            )

    else:
        log_build.append(
            f"{local_time()}\tCRITICAL\tFile extension type not recognised: {fullpath}"
        )
        error_path = os.path.join(filepath, "error/", file)
        shutil.move(fullpath, error_path)
        log_build.append(
            f"{local_time()}\tINFO\t==================== END Transcode MP4 and make JPEG {file} ==================="
        )
        log_output(log_build)
        sys.exit("Exiting as script does not recognised file type")

    # Post MPEG/JPEG creation updates to Media record
    media_data = []
    if full_jpeg:
        full_jpeg_file = os.path.splitext(full_jpeg)[0]
        print(full_jpeg, full_jpeg_file)
        os.replace(full_jpeg, full_jpeg_file)
        os.chmod(full_jpeg_file, 0o777)
        media_data.append(
            f"<access_rendition.largeimage>{os.path.split(full_jpeg_file)[1]}</access_rendition.largeimage>"
        )
    if thumb_jpeg:
        thumb_jpeg_file = os.path.splitext(thumb_jpeg)[0]
        os.replace(thumb_jpeg, thumb_jpeg_file)
        os.chmod(thumb_jpeg_file, 0o777)
        media_data.append(
            f"<access_rendition.thumbnail>{os.path.split(thumb_jpeg_file)[1]}</access_rendition.thumbnail>"
        )
    if outpath2:
        media_data.append(
            f"<access_rendition.mp4>{os.path.split(outpath2)[1]}</access_rendition.mp4>"
        )
        os.chmod(outpath2, 0o777)
    log_build.append(
        f"{local_time()}\tINFO\tWriting UMID data to CID Media record: {media_priref}"
    )
    LOGGER.info(media_data)
    success = cid_media_append(file, media_priref, media_data)
    if success:
        log_build.append(
            f"{local_time()}\tINFO\tJPEG/HLS filename data updated to CID media record"
        )
        log_build.append(
            f"{local_time()}\tINFO\tMoving preservation file to completed path: {completed_pth}"
        )
        shutil.move(fullpath, completed_pth)
    else:
        log_build.append(
            f"{local_time()}\tCRITICAL\tProblem writing UMID data to CID media record: {media_priref}"
        )
        log_build.append(
            f"{local_time()}\tWARNING\tLeaving files in transcode folder for repeat attempts to process"
        )

    log_build.append(
        f"{local_time()}\tINFO\t==================== END Transcode MP4 and make JPEG {file} ===================="
    )
    log_output(log_build)
    print(log_build)


def log_output(log_build: list[str]) -> None:
    """
    Collect up log list and output to log in one block
    """
    for log in log_build:
        LOGGER.info(log)


def adjust_seconds(duration: float) -> float:
    """
    Adjust second duration one third in
    """
    print(duration)
    LOGGER.info("adjust_seconds(): Received duration: %s", duration)
    if len(duration) == 0:
        return None
    if not isinstance(duration, float):
        return None
    return duration // 3


def get_jpeg(seconds: float, fullpath: str, outpath: str) -> bool:
    """
    Retrieve JPEG from MP4
    Seconds accepted as float
    """
    cmd: list[str] = [
        "ffmpeg",
        "-ss",
        str(seconds),
        "-i",
        fullpath,
        "-frames:v",
        "1",
        "-q:v",
        "2",
        outpath,
    ]

    command: str = " ".join(cmd)
    print("***********************")
    print(command)
    print("***********************")
    try:
        subprocess.call(cmd)
        return True
    except Exception as err:
        LOGGER.warning(
            "%s\tINFO\tget_jpeg(): failed to extract JPEG\n%s\n%s",
            local_time(),
            command,
            err,
        )
        return False


def check_item(ob_num: str, database: str) -> Optional[tuple[str, str, str]]:
    """
    Use requests to retrieve priref/RNA data for item object number
    """
    search: str = f"(object_number='{ob_num}')"
    record = adlib.retrieve_record(CID_API, database, search, "1")[1]
    if not record:
        record = adlib.retrieve_record(CID_API, "collect", search, "1")[1]
    if not record:
        return None

    priref = adlib.retrieve_field_name(record[0], "priref")[0]
    if not priref:
        priref = ""
    source = adlib.retrieve_field_name(record[0], "acquisition.source")[0]
    if not source:
        source = ""
    groupings = adlib.retrieve_field_name(record[0], "grouping")
    if not groupings:
        groupings = ""

    return priref, source, groupings


def get_media_priref(fname: str) -> Optional[tuple[str, str, str, str, str]]:
    """
    Retrieve priref from Digital record
    """
    search: str = f"(imagen.media.original_filename='{fname}')"
    record = adlib.retrieve_record(CID_API, "media", search, "1")[1]
    if not record:
        return None

    priref = adlib.retrieve_field_name(record[0], "priref")[0]
    if not priref:
        priref = ""
    input_date = adlib.retrieve_field_name(record[0], "input.date")[0]
    if not input_date:
        input_date = ""
    largeimage_umid = adlib.retrieve_field_name(
        record[0], "access_rendition.largeimage"
    )[0]
    if not largeimage_umid:
        largeimage_umid = ""
    thumbnail_umid = adlib.retrieve_field_name(record[0], "access_rendition.thumbnail")[
        0
    ]
    if not thumbnail_umid:
        thumbnail_umid = ""
    access_rendition = adlib.retrieve_field_name(record[0], "access_rendition.mp4")[0]
    if not access_rendition:
        access_rendition = ""

    return priref, input_date, largeimage_umid, thumbnail_umid, access_rendition


def get_dar(fullpath: str) -> Optional[str]:
    """
    Retrieves metadata DAR info and returns as string
    """

    dar_setting = utils.get_metadata("Video", "DisplayAspectRatio/String", fullpath)
    if "4:3" in dar_setting:
        return "4:3"
    if "16:9" in dar_setting:
        return "16:9"
    if "15:11" in dar_setting:
        return "4:3"


def get_par(fullpath: str) -> str:
    """
    Retrieves metadata PAR info and returns
    Checks if multiples from multi video tracks
    """
    par_setting = utils.get_metadata("Video", "PixelAspectRatio", fullpath)
    par_full = str(par_setting).rstrip("\n")

    if len(par_full) <= 5:
        return par_full
    return par_full[:5]


def get_height(fullpath: str) -> str:
    """
    Retrieves height information via mediainfo
    """

    sampled_height = utils.get_metadata("Video", "Sampled_Height", fullpath)
    reg_height = utils.get_metadata("Video", "Height", fullpath)

    try:
        int(sampled_height)
    except ValueError:
        sampled_height = 0

    if sampled_height == 0:
        height = str(reg_height)
    elif int(sampled_height) > int(reg_height):
        height = str(sampled_height)
    else:
        height = str(reg_height)

    if "480" == height:
        return "480"
    if "486" == height:
        return "486"
    if "576" == height:
        return "576"
    if "608" == height:
        return "608"
    if "720" == height:
        return "720"
    if "1080" == height or "1 080" == height:
        return "1080"
    height = height.split(" pixel", maxsplit=1)[0]
    return re.sub("[^0-9]", "", height)


def get_width(fullpath: str) -> str:
    """
    Retrieves height information via ffprobe
    """
    width = utils.get_metadata("Video", "Width/String", fullpath)

    if "720" == width:
        return "720"
    if "768" == width:
        return "768"
    if "1024" == width or "1 024" == width:
        return "1024"
    if "1280" == width or "1 280" == width:
        return "1280"
    if "1920" == width or "1 920" == width:
        return "1920"
    width = width.split(" pixel", maxsplit=1)[0]
    return re.sub("[^0-9]", "", width)


def get_duration(fullpath: str) -> Optional[tuple[Union[str, int], str]]:
    """
    Retrieves duration information via mediainfo
    where more than two returned, find longest of
    first two and return video stream info to main
    for update to ffmpeg map command
    """

    duration = utils.get_metadata("Video", "Duration", fullpath)
    if not duration:
        return ("", "")

    print(f"Mediainfo seconds: {duration}")

    if "." in duration:
        duration_list = duration.split(".")

    if isinstance(duration, str):
        try:
            second_duration = int(duration) // 1000
            return (second_duration, "0")
        except ValueError:
            return ("", "")
    elif len(duration_list) == 2:
        print("Just one duration returned")
        num = duration_list[0]
        second_duration = int(num) // 1000
        print(second_duration)
        return (second_duration, "0")
    elif len(duration_list) > 2:
        print("More than one duration returned")
        dur1 = f"{duration_list[0]}"
        dur2 = f"{duration_list[1][6:]}"
        print(dur1, dur2)
        if int(dur1) > int(dur2):
            second_duration = int(dur1) // 1000
            return (second_duration, "0")
        if int(dur1) < int(dur2):
            second_duration = int(dur2) // 1000
            return (second_duration, "1")


def check_audio(fullpath: str) -> Optional[tuple[str, Optional[str]]]:
    """
    Checking if audio in mov file ahead of transcode
    """

    ffprobe_cmd = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "a",
        "-show_entries",
        "stream=codec_type",
        "-of",
        "default=noprint_wrappers=1",
        fullpath,
    ]

    cmd0 = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "a:0",
        "-show_entries",
        "stream=index:stream_tags=language",
        "-of",
        "compact=p=0:nk=1",
        fullpath,
    ]

    cmd1 = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "a:1",
        "-show_entries",
        "stream=index:stream_tags=language",
        "-of",
        "compact=p=0:nk=1",
        fullpath,
    ]

    audio = subprocess.check_output(ffprobe_cmd)
    audio_str = audio.decode("utf-8")
    if not audio_str:
        return None

    lang0 = subprocess.check_output(cmd0)
    lang1 = subprocess.check_output(cmd1)
    try:
        lang0 = subprocess.check_output(cmd0)
        lang0_str = lang0.decode("utf-8")
    except Exception:
        lang0_str = ""
    try:
        lang1 = subprocess.check_output(cmd1)
        lang1_str = lang1.decode("utf-8")
    except Exception:
        lang1_str = ""

    print(f"**** LANGUAGES: Stream 0 {lang0_str} - Stream 1 {lang1_str}")

    if "nar" in str(lang0_str).lower():
        print("Narration stream 0 / English stream 1")
        return ("Audio", "1")
    if "nar" in str(lang1_str).lower():
        print("Narration stream 1 / English stream 0")
        return ("Audio", "0")
    return ("Audio", None)


def create_transcode(
    fullpath: str,
    output_path: str,
    height: Union[int, str],
    width: Union[int, str],
    dar: Optional[str],
    par: str,
    audio: str,
    default: Optional[str],
    vs: str,
    retry: bool,
) -> list[str]:
    """
    Builds FFmpeg command based on height/dar input
    """
    print(f"Received {dar} {par} {height} {width} {audio} {default}")

    ffmpeg_program_call: list[str] = ["ffmpeg"]

    input_video_file: list[str] = ["-i", fullpath]

    video_settings: list[str] = ["-c:v", "libx264", "-crf", "28"]

    pix: list[str] = ["-pix_fmt", "yuv420p"]

    fast_start: list[str] = ["-movflags", "faststart"]

    deinterlace: list[str] = ["-vf", "yadif"]

    crop_sd_608: list[str] = [
        "-vf",
        "yadif,crop=672:572:24:32,scale=734:576:flags=lanczos,pad=768:576:-1:-1",
    ]

    no_stretch_4x3: list[str] = ["-vf", "yadif,pad=768:576:-1:-1"]

    crop_sd_4x3: list[str] = [
        "-vf",
        "yadif,crop=672:572:24:2,scale=734:576:flags=lanczos,pad=768:576:-1:-1",
    ]

    crop_sd_15x11: list[str] = [
        "-vf",
        "yadif,crop=704:572,scale=768:576:flags=lanczos,pad=768:576:-1:-1",
    ]

    crop_ntsc_486: list[str] = [
        "-vf",
        "yadif,crop=672:480,scale=734:486:flags=lanczos,pad=768:486:-1:-1",
    ]

    crop_ntsc_486_16x9: list[str] = [
        "-vf",
        "yadif,crop=672:480,scale=1024:486:flags=lanczos",
    ]

    crop_ntsc_640x480: list[str] = ["-vf", "yadif,pad=768:480:-1:-1"]

    crop_sd_16x9: list[str] = [
        "-vf",
        "yadif,crop=704:572:8:2,scale=1024:576:flags=lanczos",
    ]

    scale_sd_16x9: list[str] = [
        "-vf",
        "yadif,scale=1024:576:flags=lanczos,blackdetect=d=0.05:pix_th=0.10",
    ]

    hd_16x9: list[str] = ["-vf", "yadif,scale=-1:720:flags=lanczos,pad=1280:720:-1:-1"]

    fhd_all: list[str] = [
        "-vf",
        "yadif,scale=-1:1080:flags=lanczos,pad=1920:1080:-1:-1",
    ]

    fhd_letters: list[str] = [
        "-vf",
        "yadif,scale=1920:-1:flags=lanczos,pad=1920:1080:-1:-1,blackdetect=d=0.05:pix_th=0.10",
    ]

    max_mux = ["-max_muxing_queue_size", "9999"]

    output = ["-nostdin", "-y", output_path, "-f", "null", "-"]

    if vs:
        map_video = [
            "-map",
            f"0:v:{vs}",
        ]
    else:
        map_video = [
            "-map",
            "0:v:0",
        ]

    if default and audio:
        map_audio = [
            "-map",
            "0:a?",
            "-c:a",
            "aac",
            f"-disposition:a:{default}",
            "default",
            "-dn",
        ]
    else:
        map_audio = ["-map", "0:a?", "-c:a", "aac", "-dn"]

    height = int(height)
    width = int(width)
    aspect = round(height / width, 3)
    cmd_mid = []

    if height <= 486 and dar == "16:9":
        cmd_mid = crop_ntsc_486_16x9
    elif height <= 486 and dar == "4:3":
        cmd_mid = crop_ntsc_486
    elif height <= 486 and width == 640:
        cmd_mid = crop_ntsc_640x480
    elif height <= 576 and dar == "16:9":
        cmd_mid = crop_sd_16x9
    elif height <= 576 and width == 768:
        cmd_mid = no_stretch_4x3
    elif height <= 576 and width == 1024:
        cmd_mid = scale_sd_16x9
    elif height <= 576 and par == "1.000":
        cmd_mid = no_stretch_4x3
    elif height <= 576 and dar == "4:3":
        cmd_mid = crop_sd_4x3
    elif height <= 576 and dar == "15:11":
        cmd_mid = crop_sd_15x11
    elif height == 608:
        cmd_mid = crop_sd_608
    elif height == 576 and dar == "1.85:1":
        cmd_mid = crop_sd_16x9
    elif height <= 720 and dar == "16:9":
        cmd_mid = hd_16x9
    elif width == 1920 and aspect >= 1.778:
        cmd_mid = fhd_letters
    elif height > 720 and width <= 1920:
        cmd_mid = fhd_all
    elif width >= 1920 and aspect < 1.778:
        cmd_mid = fhd_all
    elif height >= 1080 and aspect >= 1.778:
        cmd_mid = fhd_letters
    print(f"Middle command chose: {cmd_mid}")

    if retry:
        return (
            ffmpeg_program_call
            + input_video_file
            + map_video
            + map_audio
            + video_settings
            + pix
            + deinterlace
            + max_mux
            + fast_start
            + output
        )
    return (
        ffmpeg_program_call
        + input_video_file
        + map_video
        + map_audio
        + video_settings
        + pix
        + cmd_mid
        + max_mux
        + fast_start
        + output
    )


def make_jpg(
    filepath: str, arg: str, transcode_pth: Optional[str], percent: Optional[str]
) -> Optional[str]:
    """
    Create GM JPEG using command based on argument
    These command work. For full size don't use resize.
    """

    start_reduce = ["gm", "convert", "-density", "300x300", filepath, "-strip"]

    start = ["gm", "convert", "-density", "600x600", filepath, "-strip"]

    thumb = [
        "-resize",
        "x180",
    ]

    oversize = [
        "-resize",
        f"{percent}%x{percent}%",
    ]

    if not transcode_pth:
        out = os.path.splitext(filepath)[0]
    else:
        fname = os.path.split(filepath)[1]
        file = os.path.splitext(fname)[0]
        out = os.path.join(transcode_pth, file)

    if "thumb" in arg:
        outfile = f"{out}_thumbnail.jpg"
        cmd = start_reduce + thumb + [f"{outfile}"]
    elif "oversize" in arg:
        outfile = f"{out}_largeimage.jpg"
        cmd = start + oversize + [f"{outfile}"]
    else:
        outfile = f"{out}_largeimage.jpg"
        cmd = start + [f"{outfile}"]

    try:
        subprocess.call(cmd)
    except Exception as err:
        LOGGER.error(
            "%s\tERROR\tJPEG creation failed for filepath: %s\n%s",
            local_time(),
            filepath,
            err,
        )

    print(outfile)
    if os.path.exists(outfile):
        return outfile


def conformance_check(file: str) -> str:
    """
    Checks file against MP4 mediaconch policy
    Looks for essential items to ensure that
    the transcode was successful
    """
    success: tuple[bool, str] = utils.get_mediaconch(file, MP4_POLICY)
    if success[0] is True:
        return "PASS!"
    else:
        return f"FAIL! This policy has failed {success[1]}"


def check_mod_time(fpath: str) -> bool:
    """
    See if mod time over 5 hrs old
    """
    now = datetime.now().astimezone()
    local_tz = pytz.timezone("Europe/London")
    file_mod_time = os.stat(fpath).st_mtime
    modified = datetime.fromtimestamp(file_mod_time, tz=timezone.utc)
    mod = modified.replace(tzinfo=pytz.utc).astimezone(local_tz)

    diff = now - mod
    seconds = diff.seconds
    hours = (seconds / 60) // 60
    LOGGER.info("%s\tModified time is %s seconds ago. %s hours", fpath, seconds, hours)
    print(f"{fpath}\tModified time is {seconds} seconds ago")
    if seconds > 18000:
        print(f"*** Deleting file as old MP4: {fpath}")
        return True
    return False


@tenacity.retry(stop=tenacity.stop_after_attempt(10))
def cid_media_append(fname: str, priref: str, data: Iterable[str]) -> Optional[bool]:
    """
    Receive data and priref and append to CID media record
    """
    payload_head: str = f"<adlibXML><recordList><record priref='{priref}'>"
    payload_mid: str = "".join(data)
    payload_end: str = "</record></recordList></adlibXML>"
    payload: str = payload_head + payload_mid + payload_end

    rec = adlib.post(CID_API, payload, "media", "updaterecord")
    if not rec:
        return False
    data_priref = get_media_priref(fname)
    print("**************************************************************")
    print(data)
    print("**************************************************************")

    data_priref = get_media_priref(fname)
    if data_priref is None:
        data_priref = tuple("")
        return False
    file = fname.split(".")[0]
    if file == data_priref[4] or file in str(data_priref[2]):
        LOGGER.info(
            "cid_media_append(): Write of access_rendition data confirmed successful for %s - Priref %s",
            fname,
            priref,
        )
        return True


if __name__ == "__main__":
    main()
