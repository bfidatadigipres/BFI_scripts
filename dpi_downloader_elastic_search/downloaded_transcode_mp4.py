#!/usr/bin/python3

"""
Module script for BFI National Archive downloader app.

This script manages downloads for missing MP4 access proxy
files, it receives the downloaded source file path and
processes the file in situ before returning the new
encoded file path to the downloader app script, which sends
an email notification of the file's completed download
and transcode.

2023

"""
# Python packages
import datetime
import logging
import os
import re
import shutil
import subprocess
import sys
import time
from typing import Any, Final, Optional, Union

import pytz
import tenacity

# Private packages
sys.path.append(os.environ["CODE"])
import adlib_v3 as adlib
import utils

# Global paths from environment vars
MP4_POLICY: Final = os.environ["MP4_POLICY"]
LOG_PATH: Final = os.environ["LOG_PATH"]
LOG_FILE: Final = os.path.join(
    LOG_PATH, "scheduled_elasticsearch_downloader_transcode.log"
)
CID_API: Final = utils.get_current_api()
TRANSCODE: Final = os.environ["TRANSCODING"]
if not os.path.ismount(TRANSCODE):
    sys.exit(f"{TRANSCODE} path is not mounted. Script exiting.")
# TRANSCODE = os.path.join(os.environ['QNAP_REND1'], 'mp4_transcoding_backup/')

# Setup logging
logger = logging.getLogger("bp_downloader_mp4_transcode")
HDLR = logging.FileHandler(LOG_FILE)
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
logger.addHandler(HDLR)
logger.setLevel(logging.INFO)

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
    return datetime.datetime.now(pytz.timezone("Europe/London")).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def check_for_mixed_audio(fpath: str) -> Optional[dict[str, int]]:
    """
    For use where audio channels 6+ exist
    check for 'DL' and 'DR' and build different
    FFmpeg command that uses mixed audio only
    """
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "stream=channel_layout",
        "-of",
        "csv=p=0",
        fpath,
    ]
    audio = subprocess.check_output(cmd)
    audio_str = audio.decode("utf-8").lstrip("\n").rstrip("\n")
    audio_channels = audio_str.split("\n")
    if len(audio_channels) > 1:
        audio_downmix = {}
        for num in range(0, len(audio_channels)):
            if "(DL)" in audio_channels[num]:
                audio_downmix["DL"] = num
            if "(DR)" in audio_channels[num]:
                audio_downmix["DR"] = num
        if len(audio_downmix) == 2:
            return audio_downmix

    return None


def transcode_mp4(fpath: str) -> str:
    """
    Get ext, check filetype then process
    according to video, image or pass through
    audio and documents
    """
    fullpath = fpath
    if not os.path.isfile(fullpath):
        logger.warning(
            "%s\tWARNING\tSCRIPT EXITING: Error with file path supplied, not a file: %s",
            local_time(),
            fullpath,
        )
        return "failed transcode"

    if not utils.check_control("pause_scripts"):
        logger.info("Script run prevented by downtime_control.json. Script exiting.")
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")
    log_build = []

    filepath, file = os.path.split(fullpath)
    fname, ext = os.path.splitext(file)
    log_build.append(
        f"{local_time()}\tINFO\t================== START Download Transcode to MP4 proxy {file} {filepath} =================="
    )
    print(f"File to be processed: {file}")
    outpath, outpath2 = "", ""
    ext = ext.lstrip(".")
    print(file, fname, ext)
    # Check CID for Item record and extract transcode path
    object_number = make_object_number(fname)
    if object_number is None:
        log_build.append(f"Object number: {object_number} does not exists")
        object_number = ""
    if object_number.startswith("CA-"):
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
            f"{local_time()}\tINFO\t==================== END Download Transcode to MP4 proxy {file} ==================="
        )
        log_output(log_build)
        return "no media record"
    if not priref and not input_date:
        # Record inaccessible (possible access restrictions)
        log_build.append(
            f"{local_time()}\tWARNING\tProblems accessing CID to retrieve Item record data: {object_number}"
        )
        log_build.append(
            f"{local_time()}\tINFO\t==================== END Download Transcode to MP4 proxy {file} ==================="
        )
        log_output(log_build)
        return "no item record"

    date_pth = input_date.replace("-", "")[:6]
    if len(date_pth) <= 5:
        sys.exit(f"Error with date path: {date_pth}. Script exiting.")
    if "H22: Video Digitisation: Item Outcomes" in str(groupings) and source:
        log_build.append(
            f"{local_time()}\tINFO\t** Source for H22 video: {source} ****"
        )
        for key, val in SUPPLIERS.items():
            if key in str(source):
                rna_pth = val
                transcode_pth = os.path.join(TRANSCODE, rna_pth, date_pth)
    else:
        transcode_pth = os.path.join(TRANSCODE, "bfi", date_pth)

    # Check if transcode already completed and capture original access rendition names for replacement
    if access and thumbnail and largeimage:   
        log_build.append(
            f"{local_time()}\tINFO\tMedia record already has Imagen Media UMIDs. Checking for transcodes"
        )
        if fname != access:
            if os.path.exists(os.path.join(transcode_pth, access)):
                log_build.append(
                    f"{local_time()}\tINFO\tTranscode file already exists. Overwriting with new request."
                )
                maintain_names = [access, thumbnail, largeimage]
        elif os.path.exists(os.path.join(transcode_pth, fname)):
            log_build.append(
                f"{local_time()}\tINFO\tTranscode file already exists. Overwriting with new request."
            )
            maintain_names = []

        else:   
            log_build.append(
                f"{local_time()}\tWARNING\tCID UMIDs exist but no transcoding. Allowing files to proceed."
            )
            maintain_names = []

    # Get file type, video or audio etc.
    ftype = sort_ext(ext)
    if ftype == "audio":
        log_build.append(
            f"{local_time()}\tINFO\tItem is an audio file. No actions required at this time."
        )
        log_build.append(
            f"{local_time()}\tINFO\t==================== END Download Transcode to MP4 proxy {file} ==================="
        )
        log_output(log_build)
        return "audio"

    elif ftype == "document":
        log_build.append(
            f"{local_time()}\tINFO\tItem is a document. No actions required at this time."
        )
        log_build.append(
            f"{local_time()}\tINFO\t==================== END Download Transcode to MP4 proxy {file} ==================="
        )
        log_output(log_build)
        return "document"

    elif ftype == "video":
        log_build.append(
            f"{local_time()}\tINFO\tItem is video. Checking for DAR, height and duration of video."
        )
        if not os.path.exists(transcode_pth):
            log_build.append(f"Creating new transcode path: {transcode_pth}")
            os.makedirs(transcode_pth, mode=0o777, exist_ok=True)

        audio, stream_default, stream_count = check_audio(fullpath)
        dar = get_dar(fullpath)
        par = get_par(fullpath)
        height = get_height(fullpath)
        width = get_width(fullpath)
        duration, vs = get_duration(fullpath)
        log_build.append(
            f"Data retrieved: Audio {audio}, DAR {dar}, PAR {par}, Height {height}, Width {width}, Duration {duration} secs"
        )

        # CID transcode paths - maintain imagen era names where collected
        if maintain_names:
            outpath = os.path.join(transcode_pth, f"{maintain_names[0]}.mp4")
            outpath2 = os.path.join(transcode_pth, maintain_names[0])
        else:
            outpath = os.path.join(transcode_pth, f"{fname}.mp4")
            outpath2 = os.path.join(transcode_pth, fname)
        log_build.append(f"{local_time()}\tINFO\tMP4 destination will be: {outpath2}")

        # Check stream count and see if 'DL' 'DR' present
        if stream_count:
            if len(stream_count) > 6:
                mixed_dict = check_for_mixed_audio(fullpath)
            else:
                mixed_dict = None
        else:
            mixed_dict = None

        # Check if FL FR present
        fl_fr = check_for_fl_fr(fullpath)

        # Build FFmpeg command based on dar/height
        print("Launching create_transcode() function")
        ffmpeg_cmd = create_transcode(
            fullpath,
            outpath,
            height,
            width,
            dar,
            par,
            audio,
            stream_default,
            vs,
            mixed_dict,
            fl_fr,
        )
        if not ffmpeg_cmd:
            log_build.append(
                f"{local_time()}\tWARNING\tFailed to build FFmpeg command with data: {fullpath}\nHeight {height} Width {width} DAR {dar}"
            )
            log_output(log_build)
            return "transcode fail"

        print(ffmpeg_cmd)
        ffmpeg_call_neat = " ".join(ffmpeg_cmd)
        print(ffmpeg_call_neat)
        log_build.append(
            f"{local_time()}\tINFO\tFFmpeg call created:\n{ffmpeg_call_neat}"
        )

        # Capture transcode timings
        tic = time.perf_counter()
        try:
            data = subprocess.run(
                ffmpeg_cmd,
                shell=False,
                check=True,
                universal_newlines=True,
                stderr=subprocess.PIPE,
            ).stderr
        except Exception as e:
            log_build.append(
                f"{local_time()}\tCRITICAL\tFFmpeg command failed: {ffmpeg_call_neat}"
            )
            log_build.append(
                f"{local_time()}\tINFO\t==================== END Transcode MP4 and make JPEG {file} ==================="
            )
            print(e)
            log_output(log_build)
            return "transcode fail"
        toc = time.perf_counter()
        transcode_mins = (toc - tic) // 60
        log_build.append(
            f"{local_time()}\t** Transcode took {transcode_mins} minutes to complete for file: {fullpath}"
        )

        time.sleep(5)
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
            return "transcode fail"

        # Start JPEG extraction
        if maintain_names:
            jpeg_location = os.path.join(transcode_pth, f"{maintain_names[0]}.jpg")
        else:
            jpeg_location = os.path.join(transcode_pth, f"{fname}.jpg")
        print(f"JPEG output to go here: {jpeg_location}")

        # Calculate seconds mark to grab screen
        seconds = adjust_seconds(duration, data)
        print(f"Seconds for JPEG cut: {seconds}")
        success = get_jpeg(seconds, outpath, jpeg_location)
        if not success:
            log_build.append(
                f"{local_time()}\tWARNING\tFailed to create JPEG from MP4 file"
            )
            log_build.append(
                f"{local_time()}\tINFO\t==================== END Transcode MP4 and make JPEG {file} ==================="
            )
            log_output(log_build)
            return "jpeg fail"

        # Generate Full size 600x600, thumbnail 300x300 - pass UID original filenames from Imagen if captured
        full_jpeg = make_jpg(jpeg_location, "full", None, None, maintain_names)
        thumb_jpeg = make_jpg(jpeg_location, "thumb", None, None, maintain_names)

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

        if os.path.isfile(full_jpeg) and os.path.isfile(thumb_jpeg):
            log_build.append(
                f"{local_time()}\tINFO\tNew images created:\n - {full_jpeg}\n - {thumb_jpeg}"
            )
        else:
            log_build.append(
                f"{local_time()}\tERROR\tOne of both JPEG image creations failed for file {file}"
            )

    else:
        log_build.append(
            f"{local_time()}\tCRITICAL\tFile extension type not recognised: {fullpath}"
        )
        error_path = os.path.join(filepath, "error/")
        if not os.path.exists(error_path):
            os.makedirs(error_path, exist_ok=True)
        shutil.move(fullpath, os.path.join(error_path, file))
        log_build.append(
            f"{local_time()}\tINFO\t==================== END Transcode MP4 and make JPEG {file} ==================="
        )
        log_output(log_build)
        return "transcode fail"

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

    success = cid_media_append(media_priref, media_data)
    if success:
        log_build.append(
            f"{local_time()}\tINFO\tJPEG/HLS filename data updated to CID media record"
        )
    else:
        log_build.append(
            f"{local_time()}\tCRITICAL\tProblem writing UMID data to CID media record: {priref}"
        )
        log_build.append(
            f"{local_time()}\tWARNING\tLeaving files in transcode folder for repeat attempts to process"
        )
        # Any further clean up needed here?

    log_build.append(
        f"{local_time()}\tINFO\t==================== END Transcode MP4 and make JPEG {file} ===================="
    )
    log_output(log_build)
    return "True"


def log_output(log_build: list[str]) -> None:
    """
    Collect up log list and output to log in one block
    """
    log_clean = list(dict.fromkeys(log_build))
    for log in log_clean:
        logger.info(log)


def adjust_seconds(duration: int | str, data: str) -> int:
    """
    Adjust second durations within
    FFmpeg detected blackspace
    """
    blist = retrieve_blackspaces(data)
    print(f"*** BLACK GAPS: {blist}")
    if isinstance(duration, str):
        if "." in duration:
            duration = duration.split(".")[0]
        duration = int(duration)
    if not blist:
        return duration // 2

    secs = duration // 4
    clash = check_seconds(blist, secs)
    if not clash:
        return secs

    for num in range(2, 5):
        frame_secs = duration // num
        clash = check_seconds(blist, frame_secs)
        if not clash:
            return frame_secs

    if len(blist) > 2:
        first = blist[1].split(" - ")[1]
        second = blist[2].split(" - ")[0]
        frame_secs = int(first) + (int(second) - int(first)) // 2
        if int(first) < frame_secs < int(second):
            return frame_secs

    return duration // 2


def retrieve_blackspaces(data: str) -> list[str]:
    """
    Retrieve black detect log and check if
    second variable falls in blocks of blackdetected
    """
    data_list = data.splitlines()
    time_range = []
    for line in data_list:
        if "black_start" in line:
            split_line = line.split(":")
            split_start = split_line[1].split(".")[0]
            start = re.sub("[^0-9]", "", split_start)
            split_end = split_line[2].split(".")[0]
            end = re.sub("[^0-9]", "", split_end)
            # Round up to next second for cover
            end = str(int(end) + 1)
            time_range.append(f"{start} - {end}")
    return time_range


def check_seconds(blackspace: list[str], seconds: int) -> Optional[bool]:
    """
    Create range and check for second within
    """
    clash = []
    for item in blackspace:
        start, end = item.split(" - ")
        st = int(start) - 1
        ed = int(end) + 1
        if seconds in range(st, ed):
            clash.append(seconds)

    if len(clash) > 0:
        return True


def get_jpeg(seconds: int, fullpath: str, outpath: str) -> bool:
    """
    Retrieve JPEG from MP4
    Seconds accepted as float
    """
    cmd = [
        "ffmpeg",
        "-ss",
        str(seconds),
        "-i",
        fullpath,
        "-frames:v",
        "1",
        "-q:v",
        "2",
        "-y",
        outpath,
    ]

    command = " ".join(cmd)
    try:
        subprocess.call(cmd)
        return True
    except Exception as err:
        logger.warning(
            "%s\tINFO\tget_jpeg(): failed to extract JPEG\n%s\n%s",
            local_time(),
            command,
            err,
        )
        return False


def make_object_number(fname: str) -> Optional[str]:
    """
    Convert file or directory to CID object_number
    """
    name_split = fname.split("_")
    if len(name_split) == 3:
        return "-".join(name_split[:2])
    if len(name_split) == 4:
        return "-".join(name_split[:3])
    else:
        return None


def check_item(ob_num: str, database: str) -> Optional[tuple[str, str, list[str]]]:
    """
    Use adlib to retrieve priref/RNA data for item object number
    """
    search = f"(object_number='{ob_num}')"
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
    search = f"(imagen.media.original_filename='{fname}')"
    fields = [
        "priref",
        "input.date",
        "access_rendition.largeimage",
        "access_rendition.thumbnail",
        "access_rendition.mp4",
    ]
    record = adlib.retrieve_record(CID_API, "media", search, "0", fields)[1]
    if not record:
        return None

    if "priref" in str(record):
        priref = adlib.retrieve_field_name(record[0], "priref")[0]
    else:
        priref = ""
    if "input.date" in str(record):
        input_date = adlib.retrieve_field_name(record[0], "input.date")[0]
    else:
        input_date = ""
    if "access_rendition.largeimage" in str(record):
        largeimage_umid = adlib.retrieve_field_name(
            record[0], "access_rendition.largeimage"
        )[0]
    else:
        largeimage_umid = ""
    if "access_rendition.thumbail" in str(record):
        thumbnail_umid = adlib.retrieve_field_name(
            record[0], "access_rendition.thumbnail"
        )[0]
    else:
        thumbnail_umid = ""
    if "access_rendition.mp4" in str(record):
        access_rendition = adlib.retrieve_field_name(record[0], "access_rendition.mp4")[
            0
        ]
    else:
        access_rendition = ""

    return (priref, input_date, largeimage_umid, thumbnail_umid, access_rendition)


def sort_ext(ext: str) -> Optional[str]:
    """
    Decide on file type
    """
    mime_type = {
        "video": ["mxf", "mkv", "mov", "mp4", "avi", "ts", "mpeg", "mpg", "wmv"],
        "image": ["png", "gif", "jpeg", "jpg", "tif", "pct", "tiff"],
        "audio": ["wav", "flac", "mp3"],
        "document": ["docx", "pdf", "txt", "doc", "tar"],
    }

    ext = ext.lower()
    for key, val in mime_type.items():
        if str(ext) in str(val):
            return key


def get_dar(fullpath: str) -> str:
    """
    Retrieves metadata DAR info and returns as string
    """
    cmd = [
        "mediainfo",
        "--Language=raw",
        "--Full",
        '--Inform="Video;%DisplayAspectRatio/String%"',
        fullpath,
    ]

    cmd[3] = cmd[3].replace('"', "")
    dar_setting = subprocess.check_output(cmd)
    dar_setting_str = dar_setting.decode("utf-8")

    if "4:3" in dar_setting_str:
        return "4:3"
    if "16:9" in dar_setting_str:
        return "16:9"
    if "15:11" in dar_setting_str:
        return "4:3"
    if "1.85:1" in dar_setting_str:
        return "1.85:1"
    if "2.2:1" in dar_setting_str:
        return "2.2:1"

    return dar_setting_str


def get_par(fullpath: str) -> str:
    """
    Retrieves metadata PAR info and returns
    Checks if multiples from multi video tracks
    """
    cmd = [
        "mediainfo",
        "--Language=raw",
        "--Full",
        '--Inform="Video;%PixelAspectRatio%"',
        fullpath,
    ]

    cmd[3] = cmd[3].replace('"', "")
    par_setting = subprocess.check_output(cmd)
    par_full = par_setting.decode("utf-8").rstrip("\n")

    if len(par_full) <= 5:
        return par_full
    else:
        return par_full[:5]


def get_height(fullpath: str) -> str:
    """
    Retrieves height information via mediainfo
    Using sampled height where original
    height and stored height differ (MXF samples)
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

    if height.startswith("480 "):
        return "480"
    if height.startswith("486 "):
        return "486"
    if height.startswith("576 "):
        return "576"
    if height.startswith("608 "):
        return "608"
    if height.startswith("720 "):
        return "720"
    if height.startswith("1080 ") or height.startswith("1 080 "):
        return "1080"

    height = height.split(" pixel", maxsplit=1)[0]
    return re.sub("[^0-9]", "", height)


def get_width(fullpath):
    """
    Retrieves height information using mediainfo
    """

    width = utils.get_metadata("Video", "Width/String", fullpath)
    clap_width = utils.get_metadata("Video", "Width_CleanAperture/String", fullpath)
    print(width, clap_width)
    if width.startswith("720 ") and clap_width.startswith("703 "):
        return "703"
    if width.startswith("720 "):
        return "720"
    if width.startswith("768 "):
        return "768"
    if width.startswith("1024 ") or width.startswith("1 024 "):
        return "1024"
    if width.startswith("1280 ") or width.startswith("1 280 "):
        return "1280"
    if width.startswith("1920 ") or width.startswith("1 920 "):
        return "1920"
    if width.isdigit():
        return str(width)

    width = width.split(" p", maxsplit=1)[0]
    return re.sub("[^0-9]", "", width)


def get_duration(fullpath: str) -> tuple[int, str] | tuple[str, str]:
    """
    Retrieves duration information via mediainfo
    where more than two returned, file longest of
    first two and return video stream info to main
    for update to ffmpeg map command
    """

    duration = utils.get_metadata("Video", "Duration", fullpath)
    duration_list = []
    if not duration:
        return ("", "")
    if "." in duration:
        duration_list = duration.split(".")

    if len(duration_list) == 2:
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
        elif int(dur1) < int(dur2):
            second_duration = int(dur2) // 1000
            return (second_duration, "1")
    else:
        second_duration = int(duration) // 1000
        return (second_duration, "0")


def check_audio(
    fullpath: str,
) -> tuple[Optional[str], Optional[str], Optional[list[str]]]:
    """
    Mediainfo command to retrieve channels, identify
    stereo or mono, returned as 2 or 1 respectively
    """

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

    cmd2 = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "a",
        "-show_entries",
        "stream=index",
        "-of",
        "compact=p=0",
        fullpath,
    ]

    audio = utils.get_metadata("Audio", "Format", fullpath)
    if len(audio) == 0:
        return None, None, None

    try:
        lang0 = subprocess.check_output(cmd0)
        lang0_str = lang0.decode("utf-8")
    except Exception:
        lang0_str = ""
    try:
        lang1 = subprocess.check_output(cmd1)
        lang1_str = lang0.decode("utf-8")
    except Exception:
        lang1_str = ""
    try:
        streams = subprocess.check_output(cmd2)
        streams_str = streams.decode("utf-8").lstrip("\n").rstrip("\n").split("\n")
    except Exception:
        streams = None
    print(f"**** LANGUAGES: Stream 0 {lang0_str} - Stream 1 {lang1_str}")

    if "nar" in str(lang0_str).lower():
        print("Narration stream 0 / English stream 1")
        return ("Audio", "1", streams_str)
    elif "nar" in str(lang1_str).lower():
        print("Narration stream 1 / English stream 0")
        return ("Audio", "0", streams_str)
    else:
        return ("Audio", None, streams_str)


def create_transcode(
    fullpath: str,
    output_path: str,
    height: Union[int, str],
    width: Union[int, str],
    dar: str,
    par: str,
    audio: Optional[str],
    default: Optional[str],
    vs: str,
    mixed_dict: Optional[dict[str, int]],
    fl_fr: bool,
) -> Optional[list[str]]:
    """
    Builds FFmpeg command based on height/dar input
    """
    print(
        f"Received DAR {dar} PAR {par} H {height} W {width} Audio {audio} Default audio {default} Video stream {vs} Mixed audio {mixed_dict}"
    )
    print(f"Fullpath {fullpath} Output path {output_path}")

    ffmpeg_program_call = ["ffmpeg"]

    input_video_file = ["-i", fullpath]

    video_settings = [
        "-c:v",
        "libx264",
        "-crf",
        "28",
    ]

    pix = ["-pix_fmt", "yuv420p"]

    fast_start = ["-movflags", "faststart"]

    crop_sd_608 = [
        "-vf",
        "yadif,crop=672:572:24:32,scale=734:576:flags=lanczos,pad=768:576:-1:-1,blackdetect=d=0.05:pix_th=0.1",
    ]

    no_stretch_4x3 = ["-vf", "yadif,pad=768:576:-1:-1,blackdetect=d=0.05:pix_th=0.10"]

    crop_sd_4x3 = [
        "-vf",
        "yadif,crop=672:572:24:2,scale=734:576:flags=lanczos,pad=768:576:-1:-1,blackdetect=d=0.05:pix_th=0.10",
    ]

    upscale_sd_width = [
        "-vf",
        "yadif,scale=1024:-1:flags=lanczos,pad=1024:576:-1:-1,blackdetect=d=0.05:pix_th=0.10",
    ]

    upscale_sd_height = [
        "-vf",
        "yadif,scale=-1:576:flags=lanczos,pad=1024:576:-1:-1,blackdetect=d=0.05:pix_th=0.10",
    ]

    scale_sd_4x3 = [
        "-vf",
        "yadif,scale=768:576:flags=lanczos,blackdetect=d=0.05:pix_th=0.10",
    ]

    scale_sd_16x9 = [
        "-vf",
        "yadif,scale=1024:576:flags=lanczos,blackdetect=d=0.05:pix_th=0.10",
    ]

    crop_sd_15x11 = [
        "-vf",
        "yadif,crop=704:572,scale=768:576:flags=lanczos,pad=768:576:-1:-1,blackdetect=d=0.05:pix_th=0.10",
    ]

    crop_ntsc_486 = [
        "-vf",
        "yadif,crop=672:480,scale=734:486:flags=lanczos,pad=768:486:-1:-1,blackdetect=d=0.05:pix_th=0.10",
    ]

    crop_ntsc_486_16x9 = [
        "-vf",
        "yadif,crop=672:480,scale=1024:486:flags=lanczos,blackdetect=d=0.05:pix_th=0.10",
    ]

    crop_ntsc_640x480 = [
        "-vf",
        "yadif,pad=768:480:-1:-1,blackdetect=d=0.05:pix_th=0.10",
    ]

    crop_sd_16x9 = [
        "-vf",
        "yadif,crop=704:572:8:2,scale=1024:576:flags=lanczos,blackdetect=d=0.05:pix_th=0.10",
    ]

    sd_downscale_4x3 = [
        "-vf",
        "yadif,scale=768:576:flags=lanczos,blackdetect=d=0.05:pix_th=0.10",
    ]

    hd_16x9 = [
        "-vf",
        "yadif,scale=-1:720:flags=lanczos,pad=1280:720:-1:-1,blackdetect=d=0.05:pix_th=0.10",
    ]

    hd_16x9_letterbox = [
        "-vf",
        "yadif,scale=1280:-1:flags=lanczos,pad=1280:720:-1:-1,blackdetect=d=0.05:pix_th=0.10",
    ]

    fhd_all = [
        "-vf",
        "yadif,scale=-1:1080:flags=lanczos,pad=1920:1080:-1:-1,blackdetect=d=0.05:pix_th=0.10",
    ]

    fhd_letters = [
        "-vf",
        "yadif,scale=1920:-1:flags=lanczos,pad=1920:1080:-1:-1,blackdetect=d=0.05:pix_th=0.10",
    ]

    output = ["-nostdin", "-y", output_path, "-f", "null", "-"]

    if vs:
        print(f"VS {vs}")
        map_video = ["-map", f"0:v:{vs}"]
    else:
        map_video = ["-map", "0:v:0"]

    if mixed_dict:
        print(f"Mixed DL DR audio found: {mixed_dict}")
        map_audio = [
            "-map",
            f"0:a:{mixed_dict['DL']}",
            "-map",
            f"0:a:{mixed_dict['DR']}",
            "-ac",
            "2",
            "-c:a:0",
            "aac",
            "-ab:1",
            "320k",
            "-ar:1",
            "48000",
            "-ac:1",
            "2",
            "-disposition:a:0",
            "default",
            "-c:a:1",
            "aac",
            "-ab:2",
            "210k",
            "-ar:2",
            "48000",
            "-ac:2",
            "1",
            "-disposition:a:1",
            "0",
            "-strict",
            "2",
            "-async",
            "1",
            "-dn",
        ]
    elif fl_fr is True:
        map_audio = ["-map", "0:a?", "-c:a", "aac", "-ac", "2", "-dn"]
    elif default and audio:
        print(f"Default {default}, Audio {audio}")
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
    # Calculate height/width to decide HD scale path
    aspect = round(width / height, 3)
    cmd_mid = []

    if height < 480 and aspect >= 1.778:
        cmd_mid = upscale_sd_width
    elif height < 480 and aspect < 1.778:
        cmd_mid = upscale_sd_height
    elif height == 486 and dar == "16:9":
        cmd_mid = crop_ntsc_486_16x9
    elif height == 486 and dar == "4:3":
        cmd_mid = crop_ntsc_486
    elif height <= 486 and width == 640:
        cmd_mid = crop_ntsc_640x480
    elif height < 576 and width == 720 and dar == "4:3":
        cmd_mid = scale_sd_4x3
    elif height == 576 and width == 703 and dar != "16:9":
        cmd_mid = scale_sd_4x3
    elif height == 576 and width == 703 and dar == "16:9":
        cmd_mid = scale_sd_16x9
    elif height == 576 and width == 1024:
        cmd_mid = scale_sd_16x9
    elif height < 576 and width > 720 and dar == "16:9":
        cmd_mid = scale_sd_16x9
    elif height < 576 and width > 720 and dar == "4:3":
        cmd_mid = sd_downscale_4x3
    elif height <= 576 and dar == "16:9":
        cmd_mid = crop_sd_16x9
    elif height <= 576 and width == 768:
        cmd_mid = no_stretch_4x3
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
    elif height < 720 and dar == "16:9":
        cmd_mid = scale_sd_16x9
    elif height < 720 and dar == "4:3":
        cmd_mid = sd_downscale_4x3
    elif width == 1280 and height <= 720:
        cmd_mid = hd_16x9_letterbox
    elif height == 720 and dar == "16:9":
        cmd_mid = hd_16x9
    elif height == 720:
        cmd_mid = hd_16x9
    elif width == 1920 and aspect >= 1.778:
        cmd_mid = fhd_letters
    elif height > 720 and width <= 1920:
        cmd_mid = fhd_all
    elif width >= 1920 and aspect < 1.778:
        cmd_mid = fhd_all
    elif height >= 1080 and aspect >= 1.778:
        cmd_mid = fhd_letters
    print(f"Middle command chosen: {cmd_mid}")

    if audio is None:
        return (
            ffmpeg_program_call
            + input_video_file
            + map_video
            + video_settings
            + pix
            + fast_start
            + cmd_mid
            + output
        )
    if len(cmd_mid) > 0 and audio:
        return (
            ffmpeg_program_call
            + input_video_file
            + map_video
            + video_settings
            + pix
            + cmd_mid
            + map_audio
            + fast_start
            + output
        )
    if len(cmd_mid) > 0 and not audio:
        return (
            ffmpeg_program_call
            + input_video_file
            + map_video
            + video_settings
            + pix
            + cmd_mid
            + map_audio
            + fast_start
            + output
        )


def check_for_fl_fr(fpath: str) -> bool:
    """
    For use where audio is '1 channels (FL) or (FR)
    which is unsupported by FFmpeg, add -ac 2 to command
    """
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "stream=channel_layout",
        "-of",
        "csv=p=0",
        fpath,
    ]
    audio = subprocess.check_output(cmd)
    audio_str = audio.decode("utf-8").lstrip("\n").rstrip("\n")
    audio_channels = audio_str.split("\n")
    if "5.1(side)" in audio_channels:
        return True
    if len(audio_channels) > 1:
        audio_downmix = {}
        for num in range(0, len(audio_channels)):
            if "1 channels (FL)" in audio_channels[num]:
                audio_downmix["FL"] = num
            if "1 channels (FR)" in audio_channels[num]:
                audio_downmix["FR"] = num
        if len(audio_downmix) == 2:
            return True
    else:
        if "5.1" in audio_channels:
            return True
    return False


def make_jpg(filepath: str, arg: str, transcode_pth: Optional[str], percent: Optional[str], orig_names=None) -> str:
    """
    Create GM JPEG using command based on argument
    These command work. For full size don't use resize.
    """
    if orig_names is None:
        orig_names = []

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
    # Should only activate for video option transcode (with no transcode_pth)
    if orig_names:
        if not transcode_pth:
            out = os.path.split(filepath)[0]
            if "thumb" in arg:
                outfile = f"{out}/{orig_names[1]}.jpg"
                cmd = start_reduce + thumb + [f"{outfile}"]
            else:
                outfile = f"{out}/{orig_names[2]}.jpg"
                cmd = start + [f"{outfile}"]
    # Handles Image options and video not replacing old Imagen era filenames
    else:
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
        logger.error(
            "%s\tERROR\tJPEG creation failed for filepath: %s\n%s",
            local_time(),
            filepath,
            err,
        )

    if os.path.exists(outfile):
        return outfile


def conformance_check(file: str) -> str:
    """
    Checks file against MP4 mediaconch policy
    Looks for essential items to ensure that
    the transcode was successful
    """

    mediaconch_cmd = ["mediaconch", "--force", "-p", MP4_POLICY, file]

    try:
        success = subprocess.check_output(mediaconch_cmd)
        success_str = success.decode("utf-8")
    except Exception as err:
        success_str = ""
        logger.warning(
            "%s\tWARNING\tMediaconch policy retrieval failure for %s\n%s",
            local_time(),
            file,
            err,
        )

    if "pass!" in success_str:
        return "PASS!"
    elif success_str.startswith("fail!"):
        return f"FAIL! This policy has failed {success_str}"
    else:
        return "FAIL!"


@tenacity.retry(stop=tenacity.stop_after_attempt(10))
def cid_media_append(priref: str, data: list[str]) -> bool:
    """
    Receive data and priref and append to CID media record
    """
    payload_head = f"<adlibXML><recordList><record priref='{priref}'>"
    payload_mid = "".join(data)
    payload_end = "</record></recordList></adlibXML>"
    payload = payload_head + payload_mid + payload_end

    rec = adlib.post(CID_API, payload, "media", "updaterecord")
    if rec is None:
        logger.warning("cid_media_append(): Post of data failed: %s - %s", priref, rec)
        return False
    else:
        logger.info(
            "cid_media_append(): Write of access_rendition data appear successful for Priref %s",
            priref,
        )
        return True
