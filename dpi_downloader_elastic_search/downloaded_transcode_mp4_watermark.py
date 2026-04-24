#!/usr/bin/env python3

"""
Module script for BFI National Archive downloader app.

This script manages transcoding to MP4 access copy with/without watermark
of assets downloaded from the BFI National Archive file downloader.
It receives the downloaded source file path and
processes the file in situ before returning the new
encoded file path to the downloader app script, which sends
an email notification of the file's completed download
and transcode. Deletes source if successful.

2023
"""

import logging
import os
import re
import subprocess
import sys
import time
from typing import Any, Optional

import magic

# Local imports
sys.path.append(os.environ["CODE"])
import utils

# Global paths from server environmental variables
MP4_POLICY = os.environ["MP4_POLICY"]
LOG = os.environ["LOG_PATH"]
CONTROL_JSON = os.path.join(LOG, "downtime_control.json")
WATERMARK = os.environ.get("WATERMARK")

# Setup logging
logger = logging.getLogger("downloaded_transcode_prores")
hdlr = logging.FileHandler(
    os.path.join(LOG, "scheduled_database_downloader_transcode.log")
)
formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)


def check_mime_type(fpath: str) -> bool:
    """
    Checks the mime type is video
    and if stream media checks ffprobe
    """
    if fpath.endswith((".mxf", ".ts", ".mpg")):
        mime = "video"
    else:
        mime = magic.from_file(fpath, mime=True)
    try:
        type_ = mime.split("/")[0]
        print(f"* mime type is {type_}")
    except IOError:
        logger.warning("%s\tCannot open file, resource busy", fpath)
        return False
    if type_ != "video":
        print(f'* MIMEtype "{type_}" is not video...')
        return False
    if type_ == "video":
        cmd = ["ffprobe", "-i", fpath, "-loglevel", "-8"]
        try:
            code = subprocess.call(cmd)
            if code != 0:
                logger.warning(
                    "%s\tffprobe failed to read file: [%s] status", fpath, code
                )
                return False
            print("* ffprobe read file successfully - status 0")
        except Exception as err:
            logger.warning("%s\tffprobe failed to read file", fpath)
            print(err)
            return False
    return True


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


def get_height(fullpath):
    """
    Retrieves height information via mediainfo
    Using sampled height where original
    height and stored height differ (MXF samples)
    """

    cmd = [
        "mediainfo",
        "--Language=raw",
        "--Full",
        '--Inform="Video;%Sampled_Height%"',
        fullpath,
    ]

    cmd[3] = cmd[3].replace('"', "")
    sampled_height = subprocess.check_output(cmd)
    cmd2 = [
        "mediainfo",
        "--Language=raw",
        "--Full",
        '--Inform="Video;%Height%"',
        fullpath,
    ]

    cmd2[3] = cmd2[3].replace('"', "")
    reg_height = subprocess.check_output(cmd2)

    try:
        int(sampled_height)
    except ValueError:
        sampled_height = 0

    if int(sampled_height) > int(reg_height):
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
    else:
        height = height.split(" pixel", maxsplit=1)[0]
        return re.sub("[^0-9]", "", height)


def get_width(fullpath: str):
    """
    Retrieves height information using mediainfo
    """
    cmd = [
        "mediainfo",
        "--Language=raw",
        "--Full",
        '--Inform="Video;%Width/String%"',
        fullpath,
    ]

    cmd[3] = cmd[3].replace('"', "")
    width = subprocess.check_output(cmd)
    width_str = width.decode("utf-8")

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
    else:
        if width_str.isdigit():
            return width_str
        else:
            width_str = width_str.split(" p", maxsplit=1)[0]
            return re.sub("[^0-9]", "", width_str)


def get_duration(fullpath):
    """
    Retrieves duration information via mediainfo
    where more than two returned, file longest of
    first two and return video stream info to main
    for update to ffmpeg map command
    """

    cmd = [
        "mediainfo",
        "--Language=raw",
        "--Full",
        '--Inform="Video;%Duration%"',
        fullpath,
    ]

    cmd[3] = cmd[3].replace('"', "")
    duration = subprocess.check_output(cmd)
    if not duration:
        return ("", "")

    duration = duration.decode("utf-8").rstrip("\n")
    print(f"Mediainfo seconds: {duration}")

    if "." in duration:
        duration = duration.split(".")

    if isinstance(duration, str):
        second_duration = int(duration) // 1000
        return (second_duration, "0")
    elif len(duration) == 2:
        print("Just one duration returned")
        num = duration[0]
        second_duration = int(num) // 1000
        print(second_duration)
        return (second_duration, "0")
    elif len(duration) > 2:
        print("More than one duration returned")
        dur1 = f"{duration[0]}"
        dur2 = f"{duration[1][6:]}"
        print(dur1, dur2)
        if int(dur1) > int(dur2):
            second_duration = int(dur1) // 1000
            return (second_duration, "0")
        elif int(dur1) < int(dur2):
            second_duration = int(dur2) // 1000
            return (second_duration, "1")


def check_audio(fullpath: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Mediainfo command to retrieve channels, identify
    stereo or mono, returned as 2 or 1 respectively
    """

    cmd = [
        "mediainfo",
        "--Language=raw",
        "--Full",
        '--Inform="Audio;%Format%"',
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

    cmd2 = [
        "mediainfo",
        "--Language=raw",
        "--Full",
        '--Inform="Audio;%ChannelLayout%"',
        fullpath,
    ]

    cmd[3] = cmd[3].replace('"', "")
    audio = subprocess.check_output(cmd)
    audio_str = audio.decode("utf-8")
    if len(audio) == 0:
        return None, None, None

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

    cmd2[3] = cmd2[3].replace('"', "")
    chnl_layout = subprocess.check_output(cmd2)
    chnl_layout_str = chnl_layout.decode("utf-8")
    stereo_lr = False

    if "LR" in chnl_layout_str:
        stereo_lr = True

    if "NAR" in lang0_str:
        print("Narration stream 0 / English stream 1")
        if stereo_lr:
            return ("Audio", "1", "ac")
        return ("Audio", "1", None)
    elif "NAR" in lang1_str:
        print("Narration stream 1 / English stream 0")
        if stereo_lr:
            return ("Audio", "0", "ac")
        return ("Audio", "0", None)
    else:
        if stereo_lr:
            return ("Audio", None, "ac")
        return ("Audio", None, None)


def create_watermark_command(fullpath: str, output: str) -> list[Optional[str]]:
    """
    Subprocess command build, with variations
    added based on metadata extraction
    """

    ffmpeg_program_call = ["ffmpeg"]

    input_video = ["-i", fullpath]

    input_watermark = [
        "-i",
        WATERMARK,
    ]
    """
    # Top left
    filter_graph1 = [
        "-filter_complex",
        "[1][0]scale2ref=w='iw*20/100':h='ow/mdar'[wm][vid];[vid][wm]overlay=10:10"
    ]

    # Centre opaque
    filter_graph2 = [
        "-filter_complex",
        "[1]format=rgba,colorchannelmixer=aa=0.3[logo];[logo][0]scale2ref=oh*mdar:ih[logo][video];[video][logo]overlay=(main_w-overlay_w)/2:(main_h-overlay_h)/2"
    ]
    """
    # Top right
    filter_graph3 = [
        "-filter_complex",
        "[1][0]scale2ref=w='iw*20/100':h='ow/mdar'[wm][vid];[vid][wm]overlay=(main_w-overlay_w)-10:10",
    ]

    return (
        ffmpeg_program_call + input_video + input_watermark + filter_graph3 + [output]
    )


def create_ffmpeg_command(
    fullpath: str, output: str, video_data: list[Optional[str]]
) -> list[str]:
    """
    Subprocess command build, with variations
    added based on metadata extraction
    """

    ffmpeg_program_call = ["ffmpeg"]

    input_video_file = ["-i", fullpath]

    video_settings = ["-c:v", "libx264", "-crf", "22"]

    pix = ["-pix_fmt", "yuv420p"]

    fast_start = ["-movflags", "faststart"]

    crop_sd_608 = [
        "-vf",
        "yadif,crop=672:572:24:32,scale=734:576:flags=lanczos,pad=768:576:-1:-1",
    ]

    no_stretch_4x3 = ["-vf", "yadif,pad=768:576:-1:-1"]

    crop_sd_4x3 = [
        "-vf",
        "yadif,crop=672:572:24:2,scale=734:576:flags=lanczos,pad=768:576:-1:-1",
    ]

    crop_sd_15x11 = [
        "-vf",
        "yadif,crop=704:572,scale=768:576:flags=lanczos,pad=768:576:-1:-1",
    ]

    crop_ntsc_486 = [
        "-vf",
        "yadif,crop=672:480,scale=734:486:flags=lanczos,pad=768:486:-1:-1",
    ]

    crop_ntsc_486_16x9 = ["-vf", "yadif,crop=672:480,scale=1024:486:flags=lanczos"]

    crop_ntsc_640x480 = ["-vf", "yadif,pad=768:480:-1:-1"]

    crop_sd_16x9 = ["-vf", "yadif,crop=704:572:8:2,scale=1024:576:flags=lanczos"]

    hd_16x9 = ["-vf", "yadif,scale=-1:720:flags=lanczos,pad=1280:720:-1:-1"]

    fhd_all = ["-vf", "yadif,scale=-1:1080:flags=lanczos,pad=1920:1080:-1:-1"]

    fhd_letters = ["-vf", "yadif,scale=1920:-1:flags=lanczos,pad=1920:1080:-1:-1"]

    output_data = ["-nostdin", "-y", output, "-f", "null", "-"]

    if video_data[6]:
        print(f"VS {video_data[6]}")
        map_video = [
            "-map",
            f"0:v:{video_data[6]}",
        ]
    else:
        map_video = [
            "-map",
            "0:v:0",
        ]

    if video_data[5] and video_data[4] and not video_data[7]:
        print(f"Default {video_data[5]}, Audio {video_data[4]}")
        map_audio = [
            "-map",
            "0:a?",
            f"-disposition:a:{video_data[5]}",
            "default",
            "-dn",
        ]

    elif video_data[7] == "ac1":
        print(f"Stereo LR {video_data[7]}")
        map_audio = ["-map", "0:a?", "-ac", "1", "-dn"]

    elif video_data[7] == "ac2":
        print(f"Stereo C {video_data[7]}")
        map_audio = ["-map", "0:a?", "-ac", "2", "-dn"]
    else:
        map_audio = ["-map", "0:a?", "-dn"]

    height = int(video_data[0])
    width = int(video_data[1])
    dar = video_data[2]
    par = video_data[3]
    # Calculate height/width to decide HD scale path
    aspect = round(width / height, 3)
    cmd_mid = []

    if height <= 486 and dar == "16:9":
        cmd_mid = crop_ntsc_486_16x9
    elif height <= 486 and dar == "4:3":
        cmd_mid = crop_ntsc_486
    elif height <= 486 and width == 640:
        cmd_mid = crop_ntsc_640x480
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
    elif height <= 576 and dar == "16:9":
        cmd_mid = crop_sd_16x9
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

    if video_data[4] is None:
        return (
            ffmpeg_program_call
            + input_video_file
            + map_video
            + video_settings
            + pix
            + fast_start
            + cmd_mid
            + output_data
        )
    if len(cmd_mid) > 0 and video_data[4]:
        return (
            ffmpeg_program_call
            + input_video_file
            + map_video
            + map_audio
            + video_settings
            + pix
            + fast_start
            + cmd_mid
            + output_data
        )
    if len(cmd_mid) > 0 and not video_data[4]:
        return (
            ffmpeg_program_call
            + input_video_file
            + map_video
            + map_audio
            + video_settings
            + pix
            + fast_start
            + cmd_mid
            + output_data
        )


def check_policy(output_path: str):
    """
    Run mediaconch check against new prores
    """
    new_file = os.path.split(output_path)[1]
    if os.path.isfile(output_path):
        logger.info("Conformance check: comparing %s with policy", new_file)
        result = conformance_check(output_path)
        if "PASS!" in result:
            logger.info("%s passed the policy checker", new_file)
            return "pass!"
        else:
            logger.warning("FAIL: %s failed the policy checker", new_file)
            return result


def conformance_check(filepath: str) -> str:
    """
    Checks mediaconch policy against new V210 mov
    """

    mediaconch_cmd = ["mediaconch", "--force", "-p", MP4_POLICY, filepath]

    try:
        success = subprocess.check_output(mediaconch_cmd)
        success_str = success.decode("utf-8")
    except Exception:
        success_str = ""
        logger.exception("Mediaconch policy retrieval failure for %s", filepath)

    if "N/A!" in success_str:
        logger.info(
            "***** FAIL! Problem with the MediaConch policy suspected. Check <%s> manually *****\n%s",
            filepath,
            success,
        )
        return "FAIL!"
    elif "pass!" in success_str:
        logger.info("PASS: %s has passed the mediaconch policy", filepath)
        return "PASS!"
    elif "fail!" in success_str:
        logger.warning("FAIL! The policy has failed for %s:\n %s", filepath, success)
        return "FAIL!"
    else:
        logger.warning("FAIL! The policy has failed for %s", filepath)
        return "FAIL!"


def transcode_mp4_access(fpath, arg: str) -> str:
    """
    Receives fullpath and watermark boole from Python downloader script
    Passes to FFmpeg subprocess command, transcodes MP4 then checks
    finished encoding against custom H264 MP4 mediaconch policy
    """
    fullpath = fpath
    watermark = arg
    if not os.path.isfile(fullpath):
        logger.warning("SCRIPT EXITING: Error with file path:\n %s", fullpath)
        return "False"
    mime_true = check_mime_type(fullpath)
    if not mime_true:
        logger.warning(
            "SCRIPT EXITING: Supplied file is not mimetype video:\n %s", fullpath
        )
        return "not video"
    if not utils.check_control("pause_scripts"):
        logger.info("Script run prevented by downtime_control.json. Script exiting.")
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")

    logger.info(
        "================== START DPI download transcode to MP4 watermark START =================="
    )
    if watermark:
        watermark is True
        logger.info("File requested for transcode to MP4 with watermark: %s", fullpath)
    else:
        watermark is False
        logger.info(
            "File requested for transcode to MP4 with NO watermark: %s", fullpath
        )

    path_split = os.path.split(fullpath)
    file = path_split[1]
    if watermark:
        output_watermark_fullpath = os.path.join(
            path_split[0], f"{file.split('.')[0]}_watermark.mp4"
        )
        if os.path.isfile(output_watermark_fullpath):
            logger.warning("Watermarked file requested but already found in path.")
            return "exists"
    output_fullpath = os.path.join(path_split[0], f"{file.split('.')[0]}_access.mp4")
    if os.path.isfile(output_fullpath) and not watermark:
        logger.warning(
            "MP4 access file requested with out watermark but already found in path."
        )
        return "exists"

    # Collect data for transcode
    logger_data = []
    video_data = []

    audio, stream_default, stereo = check_audio(fullpath)
    dar = get_dar(fullpath)
    par = get_par(fullpath)
    height = get_height(fullpath)
    width = get_width(fullpath)
    duration, vs = get_duration(fullpath)
    video_data = [height, width, dar, par, audio, stream_default, vs, stereo]

    logger_data.append(f"** File being processed: {fullpath}")
    logger_data.append(
        f"Metadata retrieved:\nDAR {dar} PAR {par} Audio {audio} Height {height} Width {width} Duration {duration}"
    )

    # Execute FFmpeg subprocess call
    ffmpeg_call = create_ffmpeg_command(fullpath, output_fullpath, video_data)
    ffmpeg_call_neat = (" ".join(ffmpeg_call), "\n")
    logger_data.append(f"FFmpeg call: {ffmpeg_call_neat}")

    # tic/toc record encoding time
    tic = time.perf_counter()
    try:
        subprocess.call(ffmpeg_call)
        logger_data.append("Subprocess call for FFmpeg command successful")
    except subprocess.CalledProcessError as err:
        logger_data.append(f"WARNING: FFmpeg command failed: {ffmpeg_call_neat}\n{err}")
        log_clean = list(dict.fromkeys(logger_data))
        for line in log_clean:
            if "WARNING" in str(line):
                logger.warning("%s", line)
            else:
                logger.info("%s", line)
        return "transcode fail"
    toc = time.perf_counter()
    encoding_time = (toc - tic) // 60
    seconds_time = toc - tic
    logger_data.append(
        f"*** Encoding time for {file}: {encoding_time} minutes or as seconds: {seconds_time}"
    )
    logger_data.append("Checking if new MP4 access file passes Mediaconch policy")
    pass_policy = check_policy(output_fullpath)
    if pass_policy == "pass!":
        logger_data.append("New MP4 access file passed MediaConch policy")
    else:
        logger_data.append(
            f"MP4 access file failed the MediaConch policy:\n{pass_policy}"
        )
        log_clean = list(dict.fromkeys(logger_data))
        for line in log_clean:
            if "WARNING" in str(line):
                logger.warning("%s", line)
            else:
                logger.info("%s", line)
        return "transcode fail"

    # Create watermark if wanted
    if watermark:
        ffmpeg_call = create_watermark_command(
            output_fullpath, output_watermark_fullpath
        )
        ffmpeg_call_neat = (" ".join(ffmpeg_call), "\n")
        logger_data.append(f"FFmpeg watermark call: {ffmpeg_call_neat}")

        # tic/toc record encoding time
        tic = time.perf_counter()
        try:
            subprocess.call(ffmpeg_call)
            logger_data.append(
                "Subprocess call for FFmpeg watermark command successful"
            )
        except Exception as err:
            logger_data.append(f"WARNING: FFmpeg watermark command failed: {err}")
            log_clean = list(dict.fromkeys(logger_data))
            for line in log_clean:
                if "WARNING" in str(line):
                    logger.warning("%s", line)
                else:
                    logger.info("%s", line)
            return "transcode fail"
        toc = time.perf_counter()
        encoding_time = (toc - tic) // 60
        seconds_time = toc - tic
        os.remove(output_fullpath)
        logger_data.append(
            f"*** Encoding time for {file}: {encoding_time} minutes or as seconds: {seconds_time}"
        )

    # Output data to log
    log_clean = list(dict.fromkeys(logger_data))
    for line in log_clean:
        if "WARNING" in str(line):
            logger.warning("%s", line)
        else:
            logger.info("%s", line)

    if watermark:
        if os.path.isfile(output_watermark_fullpath):
            return "True"
        else:
            return "transcode fail"
    elif not watermark:
        if os.path.isfile(output_fullpath):
            return "True"
        else:
            return "transcode fail"

    logger.info(
        "==================== END DPI download transcode to MP4 watermark END ===================="
    )


if __name__ == "__main__":
    transcode_mp4_access(sys.argv)
