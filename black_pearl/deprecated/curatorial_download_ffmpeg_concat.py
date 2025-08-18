#!/usr/bin/python3

"""
Receive CSV from curatorial formatted:
Field names:
new_file      <- name for concatenated file, eg N_123456_01of01.mkv
object_number <- for source part file, eg N-123456
part_file     <- name of file, eg N_123456_01of02.mkv
in            <- start time code for beginning of cut, eg 00:01:10.000
out           <- end time code for finishing cut, eg 01:12:10.000

Imports all rows in CSV, where 'new_file' match place all contents into
on single dictionary:
{new_file: {object_number: [part_file, in, out]}, {object_number: [part_file, in, out]},
 new_file: {object_numner: [part_file, in, out]}}

When dictionary compiled, run through following steps:
1. Look up reference_number for each part_file and
   download from Black Pearl, placing into working folder named
   after object number for the parts being joined
2. Trim downloaded parts and ensure completed files are same
   as TC in/out durations supplied
3. Build concatenation list from supplied in/out fields and
   filenames, and ensure that the TC is formatted correctly
4. Run the FFmpeg concatenation command and save file to top
   level of concat folder in QNAP-04 alongside working folder
5. Leave working folder and downloaded / trimmed parts,
   to allow for manual review and clean up if satisfied

NOTES: Some BlueFish MKV files are being accessed for these
       concats and will need identifying/amending. The code
       will need adding (or external module calling) when
       this is found to be the case. Mediainfo check required
       for the particular interlacing message.

Joanna White
2023
"""

import hashlib
import json
import logging
# Python packages
import os
import subprocess
import sys

import requests
from ds3 import ds3, ds3Helpers

# Local package
CODE = os.environ["CODE"]
sys.path.append(CODE)
import adlib

# GLOBAL VARS
QNAP04 = os.environ["QNAP_IMAGEN"]
DESTINATION = os.path.join(QNAP04, "curatorial_concatenation/")
LOG_PATH = os.environ["LOG_PATH"]
CONTROL_JSON = os.path.join(LOG_PATH, "downtime_control.json")

# API VARIABLES
CID_API = os.environ["CID_API3"]
CID = adlib.Database(url=CID_API)
CUR = adlib.Cursor(CID)
CLIENT = ds3.createClientFromEnv()
HELPER = ds3Helpers.Helper(client=CLIENT)

# Set up logging
LOGGER = logging.getLogger("curatorial_ffmpeg_concat")
HDLR = logging.FileHandler(os.path.join(LOG_PATH, "curatorial_ffmpeg_concat.log"))
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def check_control():
    """
    Check control json for downtime requests
    """
    with open(CONTROL_JSON) as control:
        j = json.load(control)
        if not j["black_pearl"]:
            return False
        else:
            return True


def cid_check():
    """
    Test CID active
    """
    try:
        CUR = adlib.Cursor(CID)
    except KeyError:
        print("* Cannot establish CID session, script exiting")
        LOGGER.warning("* Cannot establish CID session, script exiting")
        sys.exit()


def get_duration(filepath):
    """
    Retrieve duration field if possible
    """
    cmd = ["mediainfo", "-f", "--Language=raw", "--Output=General;%Duration%", filepath]
    try:
        duration = subprocess.check_output(cmd)
    except Exception as err:
        LOGGER.warning(err)
        duration = ""

    return duration


def get_audio_stream_count(file_path):
    """
    Count streams and return number
    """
    cmd = ["mediainfo", "--Output=Audio;%Channels%", file_path]
    try:
        data = subprocess.check_output(cmd)
    except Exception as err:
        LOGGER.warning(err)
        data = ""

    data = data.decode("utf-8")
    stream_count = len(data)
    chnl_type = data[0]

    return stream_count, chnl_type


def append_new_tracks(fpath, audio_num, most_tracks, chnl_type):
    """
    Generate black audio track for same duration
    as file, then MKV merge into space audio track
    spaces
    """
    duration = get_duration(fpath).rstrip()
    duration = int(duration)
    clip_duration = duration // 1000
    tracks_needed = most_tracks - int(audio_num)
    audio_tracks = []

    for num in range(tracks_needed):
        audio_fpath = make_audio_track(fpath, num, clip_duration, chnl_type)
        if not audio_fpath:
            return None
        audio_tracks.append(audio_fpath)

    if len(audio_tracks) != tracks_needed:
        return None

    new_fpath = mkv_merge_source_file_and_audio(fpath, audio_tracks)
    if not new_fpath:
        return None

    return new_fpath


def mkv_merge_source_file_and_audio(fpath, audio_tracks):
    """
    MKVToolNix mkv merge to join all files together
    """
    print("******** Made it into audio merge section *************")
    pth, fname = os.path.split(fpath)
    fname2 = f"{fname}_merged.mkv"
    new_fpath = os.path.join(pth, fname2)

    cmd = ["mkvmerge", "--append-mode", "file", "-o", new_fpath, fpath]

    cmd_use = cmd + audio_tracks
    command_string = ", ".join(cmd_use)
    LOGGER.info(command_string)

    try:
        subprocess.call(cmd_use)
        print(f"MERGED!!! {new_fpath}")
    except Exception as err:
        LOGGER.warning("Failed to merge audio tracks to file %s: %s", fname, err)

    if os.path.isfile(new_fpath):
        return new_fpath


def make_audio_track(fpath, num, clip_duration, chnl_type):
    """
    Generate null audio track
    """
    pth = os.path.split(fpath)[0]
    number = str(num).zfill(2)
    audio_fpath = os.path.join(pth, f"audio_{number}.wav")
    cmd = [
        "ffmpeg",
        "-f",
        "lavfi",
        "-i",
        f"anullsrc=r=48000:cl={chnl_type}",
        "-t",
        clip_duration,
        "-c:a",
        "pcm_s16le",
        audio_fpath,
    ]
    try:
        subprocess.call(cmd)
        print("****** Audio file made ********")
    except Exception as err:
        LOGGER.warning("Audio file generation failed: %s", err)
    if os.path.isfile(audio_fpath):
        return audio_fpath


def find_media_original_filename(fname):
    """
    Retrieve the object number return all
    attached media record fnames
    """
    query = {
        "database": "media",
        "search": f"imagen.media.original_filename={fname}",
        "limit": "0",
        "output": "json",
        "fields": "reference_number, preservation_bucket",
    }

    try:
        query_result = requests.get(CID_API, params=query)
        results = query_result.json()
    except Exception as err:
        LOGGER.exception(
            "get_media_original_filename: Unable to match filename to CID media record: %s\n%s",
            fname,
            err,
        )
        print(err)

    try:
        priref = results["adlibJSON"]["recordList"]["record"][0]["priref"][0]
    except (IndexError, TypeError, KeyError) as exc:
        print(exc)
        priref = ""
    try:
        ref_num = results["adlibJSON"]["recordList"]["record"][0]["reference_number"][0]
    except (IndexError, TypeError, KeyError) as exc:
        print(exc)
        ref_num = ""
    try:
        bucket = results["adlibJSON"]["recordList"]["record"][0]["preservation_bucket"][
            0
        ]
    except (IndexError, TypeError, KeyError) as exc:
        print(exc)
        bucket = ""

    return priref, ref_num, bucket


def check_download_exists(download_fpath, orig_fname, fname, transcode):
    """
    Check if download already exists
    in path, return new filepath and bool
    for download existance
    """
    skip_download = False
    if str(orig_fname).strip() != str(fname).strip():
        check_pth = os.path.join(download_fpath, orig_fname)
    else:
        check_pth = os.path.join(download_fpath, fname)

    if os.path.isfile(check_pth) and transcode == "none":
        return None, None
    elif os.path.isfile(check_pth):
        skip_download = True

    if str(orig_fname).strip() != str(fname).strip():
        new_fpath = os.path.join(download_fpath, orig_fname)
    else:
        new_fpath = os.path.join(download_fpath, fname)

    return new_fpath, skip_download


def get_bp_md5(fname, bucket):
    """
    Fetch BP checksum to compare
    to new local MD5
    """
    md5 = ""
    query = ds3.HeadObjectRequest(bucket, fname)
    result = CLIENT.head_object(query)
    try:
        md5 = result.response.msg["ETag"]
    except Exception as err:
        print(err)
    if md5:
        return md5.replace('"', "")


def make_check_md5(path, fname):
    """
    Generate MD5 for fpath
    Locate matching file in CID/checksum_md5 folder
    and see if checksums match. If not, write to log
    """
    download_checksum = ""
    fpath = os.path.join(path, fname)
    try:
        hash_md5 = hashlib.md5()
        with open(fpath, "rb") as file:
            for chunk in iter(lambda: file.read(65536), b""):
                hash_md5.update(chunk)
        download_checksum = hash_md5.hexdigest()
    except Exception as err:
        print(err)

    print(f"Created from download: {download_checksum}")
    return str(download_checksum)


def download_bp_object(fname, outpath, bucket):
    """
    Download the BP object from SpectraLogic
    tape library and save to outpath
    """
    file_path = os.path.join(outpath, fname)
    if os.path.isfile(file_path):
        return "Already downloaded in path"
    get_objects = [ds3Helpers.HelperGetObject(fname, file_path)]
    try:
        get_job_id = HELPER.get_objects(get_objects, bucket)
        print(f"BP get job ID: {get_job_id}")
        return get_job_id
    except Exception as err:
        LOGGER.warning("Unable to retrieve file %s from Black Pearl: %s", fname, err)
        return "Failed"


def read_csv(csv_path):
    """
    Yield contents line by line
    """
    with open(csv_path, "r") as file:
        for line in file:
            if line.startswith("new_file"):
                continue
            yield line.split(",")


def main():
    """
    Receive CSV path, read lines and group into
    dictionary to process files in groups
    """
    if len(sys.argv) < 2:
        sys.exit("SYS ARGV missing CSV path")
    csv_path = sys.argv[1]
    if not os.path.isfile(csv_path):
        sys.exit("CSV path is not legitimate")

    cid_check()
    check_control()
    LOGGER.info("Curatorial download FFmpeg concat START =================")

    download_list = []
    files = []
    parts = []
    for item, fname, part, tcin, tcout in read_csv(csv_path):
        tcout = tcout.rstrip("\n")
        download_list.append(part)
        files.append(fname)
        parts.append({fname: [part, tcin, tcout, item]})
    print(f"Dictionary of parts extracted from CSV: {parts}")
    LOGGER.info("Dictionary of parts extracted from CSV:\n%s", parts)

    # Start download of all CSV parts to folder / MD5 check
    seconds_dict = {}
    stream_data = []
    for part in parts:
        total_seconds = 0
        for key, val in part.items():
            key_name = key
            fname = val[0]
            priref, ref_num, bucket = find_media_original_filename(fname)
            print(f"Priref: {priref}")
            print(f"Ref number: {ref_num}")
            if len(bucket) < 3:
                bucket = "imagen"
            LOGGER.info("Matched CID media record %s: ref %s", priref, ref_num)
            outpath = os.path.join(DESTINATION, f"{key}/")
            os.makedirs(outpath, mode=0o777, exist_ok=True)

        # Begin download of file
        download_fails = []
        job_id = download_bp_object(ref_num, outpath, bucket)
        if job_id == "Failed":
            LOGGER.warning("Download failure: %s", ref_num)
            download_fails.append(ref_num)
        else:
            LOGGER.info("Download confirmation: %s", job_id)
            LOGGER.info("%s downloaded to %s", ref_num, outpath)
            local_md5 = make_check_md5(outpath, ref_num)
            bp_etag = get_bp_md5(ref_num, bucket)
            if local_md5.strip() == bp_etag.strip():
                LOGGER.info("MD5 checksums match between local download and BP:")
                LOGGER.info("\t%s", local_md5)
                LOGGER.info("\t%s", bp_etag)
            else:
                LOGGER.warning("MD5 checksums match between local download and BP:")
                LOGGER.warning("\t%s", local_md5)
                LOGGER.warning("\t%s", bp_etag)

        file_path = os.path.join(outpath, ref_num)
        # Get audio tracks of each file here
        audio_num, chnl_type = get_audio_stream_count(file_path)
        if chnl_type == "2":
            audio_data = f"{audio_num} - stereo"
        else:
            audio_data = f"{audio_num} - mono"
        stream_data.append(audio_data)

        # Skip a batch where one of the downloads failed
        concat = True
        # Check for download failures
        if fname in download_fails:
            LOGGER.warning(
                " - %s failed download, concatenation cannot be completed for this group: %s",
                fname,
                part,
            )
            concat = False
        if not concat:
            continue

        # Create new files from tcin/tcout and downloads
        concat_path = os.path.join(outpath, "edited_parts/")
        os.makedirs(concat_path, mode=0o777, exist_ok=True)
        outfile, cname, duration_secs = create_edited_file(part, outpath, concat_path)
        total_seconds += duration_secs
        if not outfile:
            LOGGER.warning(
                "Part missing from concat edit, cannot complete concatenation for this group: %s",
                part,
            )
            continue
        LOGGER.info(
            "New edited file created: %s - %s seconds long", outfile, duration_secs
        )

        # Get duration of new clip in seconds and compare
        duration = get_duration(outfile)
        if duration:
            duration = int(duration)
            clip_duration = duration // 1000
            if abs(duration_secs - clip_duration) <= 10:
                LOGGER.info(
                    "Durations match between TC supplied and video clip: %s and %s seconds",
                    duration_secs,
                    clip_duration,
                )
            else:
                LOGGER.warning(
                    "Difference greater than 10 seconds in supplied tc in/out (%s seconds) and video clip (%s seconds)",
                    duration_secs,
                    clip_duration,
                )
        else:
            LOGGER.warning(
                "Duration could not be extracted from file, unable to calculate if in/out varies"
            )
        make_concat_list(cname, concat_path, f"file {outfile}\n")
        seconds_dict[key_name] = total_seconds

    # Concat items count up
    folders_to_process = []
    for fname in files:
        prefix = fname.replace("-", "_")
        text_file = ""
        for root, _, files_ in os.walk(DESTINATION):
            for f in files_:
                if f == f"{prefix}_concat.txt":
                    text_file = os.path.join(root, f)
        if os.path.isfile(text_file):
            entries = subprocess.check_output(f"wc -l {text_file}", shell=True)
            entries = entries.decode("utf-8").split(" /", maxsplit=1)[0]
        if int(entries) == files.count(fname):
            LOGGER.info(
                "%s - Correct number of clipped files found for concatenation %s",
                fname,
                entries,
            )
            if prefix not in folders_to_process:
                folders_to_process.append(prefix)
        else:
            LOGGER.warning(
                "%s - insufficient parts for concatenation of asset %s",
                text_file,
                fname,
            )

    # Add silent audio where needed
    stream_data.sort()
    least_tracks = int(stream_data[0].split(" - ")[0])
    most_tracks = int(stream_data[-1].split(" - ")[0])
    LOGGER.info(
        "Least audio tracks: %s - Most audio tracks %s", least_tracks, most_tracks
    )
    if least_tracks != most_tracks:
        LOGGER.warning(
            "Audio streams vary between files. File audio stream appending is necessary..."
        )
        audio_edit = True
    else:
        audio_edit = False

    # Iterate all new {file}_concat.txt files creating new items, clean up where durations match
    for folder in folders_to_process:
        total_seconds_concat = concat_fname = ""
        ob_num = folder.replace("_", "-")
        target_concat = os.path.join(
            DESTINATION, ob_num, f"edited_parts/{folder}_concat.txt"
        )
        print(f"TARGET FOR CONCATENATION: {target_concat}")
        if audio_edit:
            edits_path = os.path.join(DESTINATION, ob_num, "edited_parts/")
            for root, _, files in os.walk(edits_path):
                for file in files:
                    fpath = os.path.join(root, file)
                    audio_num, chnl_type = get_audio_stream_count(fpath)
                    if int(audio_num) != most_tracks:
                        LOGGER.info(
                            "%s - has %s audio tracks, but needs %s tracks. Appending silent tracks.",
                            file,
                            audio_num,
                            most_tracks,
                        )
                        new_fpath = append_new_tracks(
                            fpath, audio_num, most_tracks, chnl_type
                        )
                        if not new_fpath:
                            LOGGER.warning(
                                "Updating additional audio tracks failed. Skipping as transcode will fail."
                            )
                            continue
                        LOGGER.info(
                            "Successfully appended additional audio tracks. Renaming new file as old."
                        )
                        new_fpath_name = new_fpath.split("_merge", maxsplit=1)[0]
                        os.rename(fpath, f"{fpath}_old")
                        os.rename(new_fpath, new_fpath_name)

        for part in parts:
            for key, value in part.items():
                if ob_num == key:
                    concat_fname = value[3]
        if concat_fname:
            concat_fpath = os.path.join(DESTINATION, concat_fname)
            success = subprocess.call(
                f"ffmpeg -f concat -safe 0 -i {target_concat} -c copy -map 0 {concat_fpath}",
                shell=True,
            )
            if success != 0:
                LOGGER.warning(
                    "Subprocess call for concatenation failed for %s", target_concat
                )
                LOGGER.warning(
                    "Concatination failed for %s. Manual concat required: \n%s",
                    ob_num,
                    os.path.join(DESTINATION, ob_num),
                )
            else:
                LOGGER.info(
                    "Concatination complete for %s. Manual clean up working folder permitted: \n%s",
                    ob_num,
                    os.path.join(DESTINATION, ob_num),
                )
        for k, v in seconds_dict.items():
            print(k)
            if str(k) in str(concat_fname):
                total_seconds_concat = v
        if total_seconds_concat:
            concat_duration = get_duration(concat_fpath)
            if concat_duration:
                LOGGER.info(
                    "Totals for edits: %s. Total for concatenation: %s",
                    total_seconds_concat,
                    concat_duration,
                )
                if abs(total_seconds_concat - concat_duration) <= 10:
                    LOGGER.info(
                        "Totals for edits under 10 seconds different: Supplied edits: %s. Total for concatenation: %s",
                        total_seconds_concat,
                        concat_duration,
                    )
                else:
                    LOGGER.warning(
                        "Difference greater than 10 seconds in supplied tc in/out ({duration_secs} seconds) and video clip ({clip_duration} seconds)"
                    )
        else:
            LOGGER.warning(
                "Unable to retrieve durations to check concatenation is accurate to edited files"
            )

    LOGGER.info("Curatorial download FFmpeg concat END ===================")


def create_edited_file(part, outpath, cpath):
    """
    Receive dct containing supplied part
    tc in and tc out, plus outpath. Check
    for previous parts and append new number
    """
    for value in part.values():
        source_file = value[0]
        tcin = value[1]
        tcout = value[2]

    infile = os.path.join(outpath, source_file)
    fname = source_file.split("_")[:-1]
    fname = "_".join(fname)
    ext = source_file.split(".")[-1]
    check_file = os.path.join(cpath, fname)
    matches = [x for x in os.listdir(cpath) if x.endswith(ext)]

    if not matches:
        outfile = f"{check_file}_01.{ext}"
    else:
        matches.sort()
        matched = matches[-1]
        last_match = matched.split(".")[0][-2:]
        num = int(last_match) + 1
        outfile = f"{check_file}_{str(num).zfill(2)}.{ext}"

    print(outfile)
    ffmpeg_cmd = [
        "ffmpeg",
        "-ss",
        tcin.strip(),
        "-to",
        tcout.strip(),
        "-i",
        infile,
        "-c:v",
        "prores_ks",
        "-profile:v",
        "3",
        "-c:a",
        "copy",
        "-pix_fmt",
        "yuv422p10le",
        "-vendor",
        "ap10",
        "-flags",
        "+ildct",
        "-map",
        "0",
        "-r",
        "25",
        "-movflags",
        "+faststart",
        outfile,
    ]

    duration_seconds = get_clip_duration(tcin, tcout)

    try:
        code = subprocess.call(ffmpeg_cmd)
        if code != 0:
            LOGGER.warning("FFmpeg command failed: %s", " ".join(ffmpeg_cmd))
            return None, None, None
        else:
            LOGGER.info("FFmpeg command called: %s", " ".join(ffmpeg_cmd))
            return outfile, fname, duration_seconds
    except Exception as err:
        LOGGER.warning(err)
        return None, None, None


def get_clip_duration(tcin, tcout):
    """
    Calculate to the nearest second
    the length of the FFmpeg clip
    """
    h, m, s = tcin.split(".")[0].split(":")
    secs_in = int(h) * 3600 + int(m) * 60 + int(s)
    h, m, s = tcout.split(".")[0].split(":")
    secs_out = int(h) * 3600 + int(m) * 60 + int(s)
    print(f"Total duration of cut video = {secs_out - secs_in} seconds")
    if secs_out > secs_in:
        return secs_out - secs_in
    else:
        return None


def make_concat_list(fname, outpath, message):
    """
    Look for existing concat file if not present, create and write lines
    """
    file = os.path.splitext(fname)[0]
    concat_txt = os.path.join(outpath, f"{file}_concat.txt")

    if not os.path.isfile(concat_txt):
        with open(concat_txt, "w+") as out_file:
            out_file.close()

    with open(concat_txt, "a") as out_file:
        out_file.write(f"{message}")


if __name__ == "__main__":
    main()
