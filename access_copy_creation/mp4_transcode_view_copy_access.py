#!/usr/bin/env python3

"""
Script to be launched from watch folder entry.

1. Checks watch folder for file arrival, file named
   after job number and date, not CID item number
2. Checks metadata of each file extracting DAR, height, duration, audio options
3. Selects FFmpeg subprocess command based on DAR/height/standard with crop/stretch for SD.
4. Encodes with FFmpeg a progressive MP4 file to selected path.
5. Verifies MP4 passes mediaconch policy (therefore successful).
6. Moves access file to top folder and move source file for deletion.
7. Maintain log of all actions against file and dump in one lot to avoid log overlaps.

2026
"""

# Public packages
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Final, Optional, Union
import pytz

# Local packages
sys.path.append(os.environ["CODE"])
import utils

# Global paths from environment vars
MP4_POLICY: Final = os.environ["MP4_POLICY"]
LOG_PATH: Final = os.environ["LOG_PATH"]
LOG_FILE: Final = os.path.join(LOG_PATH, f"mp4_viewing_copy_access.log")
STORAGE_PATH: Final = os.path.join(os.environ.get("QNAP_11"), "bbc_access/")

# Setup logging
LOGGER = logging.getLogger("mp4_viewing_copy_access")
HDLR = logging.FileHandler(LOG_FILE)
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def local_time() -> str:
    """
    Return strftime object formatted
    for London time (includes BST adjustment)
    """
    return datetime.now(pytz.timezone("Europe/London")).strftime("%Y-%m-%d %H:%M:%S")


def main():
    """
    Check transform_path for new videos
    that need to be converted to access copies
    """

    if not utils.check_control("mp4_transcode"):
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")
    if not utils.check_storage(STORAGE_PATH):
        sys.exit("Script run prevented by storage_control.json. Script exiting.")

    file_list = []
    transform_path = os.path.join(STORAGE_PATH, "processing")
    for root, _, files in os.walk(transform_path):
        for file in files:
            if file.startswith("done_"):
                continue
            file_list.append(os.path.join(root, file))

    if file_list == []:
        sys.exit("No files found at this time")

    LOGGER.info("================== START Transcode MP4 Access Copy Creation - BBC ==================")
    for fullpath in file_list:
        root, file = os.path.split(fullpath)
        job_id = os.path.basename(root)

        complete_path = os.path.join(STORAGE_PATH, f"{job_id}_{file}")
        if os.path.exists(complete_path):
            policy_check = conformance_check(complete_path)
            if "PASS!" in policy_check:
                LOGGER.info("File already exists - and passes Mediaconch test. Will add 'done_' to file: %s", file)
                new_file = f"done_{file}"
                os.rename(fullpath, os.path.join(root, new_file))
                continue

        # Get file type, video or audio etc.
        ext = file.split(".")[-1]
        ftype = utils.sort_ext(ext)
        if ftype == "audio":
            LOGGER.info("Incorrect file supplied to transcode: %s", file)
            continue
        elif ftype == "document":
            LOGGER.info("Incorrect file type supplied to transcode: %s", file)
            continue
        elif ftype == "video":
            LOGGER.info("Item is video. Checking for DAR, height and duration of video.")
            audio, stream_default, stream_count = check_audio(fullpath)
            dar = get_dar(fullpath)
            par = get_par(fullpath)
            height = get_height(fullpath)
            width = get_width(fullpath)
            duration, vs = get_duration(fullpath)
            LOGGER.info("Audio data retrieved: %s - %s - %s", audio, stream_default, stream_count)
            LOGGER.info("Video data retrieved: %s - %s - %s - %s - %s - %s", dar, par, height, width, duration, vs)

            # Check stream count and see if DL/DR or FL/FR or 12 channels
            mixed_dict = check_for_mixed_audio(fullpath)
            fl_fr = check_for_fl_fr(fullpath)
            twelve_chnl = False
            discretes = utils.get_metadata("Audio", "ChannelLayout", fullpath)
            if "Discrete" in discretes:
                if discretes.count("Discrete") >= 12:
                    twelve_chnl = True
            audio_channels = utils.get_metadata("General", "Audio_Channels_Total", fullpath)
            audio_count = utils.get_metadata("General", "AudioCount", fullpath)
            if audio_count.strip() == "1" and audio_channels.strip() == "12":
                twelve_chnl = True

            # Build FFmpeg command based on dar/height
            ffmpeg_cmd = create_transcode(
                fullpath,
                complete_path,
                height,
                width,
                dar,
                par,
                audio,
                stream_default,
                vs,
                mixed_dict,
                fl_fr,
                twelve_chnl,
            )
            if not ffmpeg_cmd:
                LOGGER.warning("Failed to build FFmpeg command with data: %s Height %s Width %s DAR %s", file, height, width, dar)
                continue

            ffmpeg_call_neat = " ".join(ffmpeg_cmd)
            print(ffmpeg_call_neat)
            LOGGER.info("FFmpeg call created:\n%s", ffmpeg_call_neat)

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
            except subprocess.CalledProcessError as e:
                LOGGER.error("FFmpeg command failed: %s\n%s", ffmpeg_call_neat, e)
                LOGGER.error(data)
                continue

            transcode_mins = (toc - tic) // 60
            LOGGER.info("** Transcode took %s minutes to complete for file: %s", transcode_mins, fullpath)

            # Mediaconch conformance check file
            policy_check = conformance_check(complete_path)
            if "PASS!" in policy_check:
                LOGGER.info("Mediaconch pass! MP4 transcode complete. Renaming source 'done_%s.", file)
                new_file = f"done_{file}"
                os.rename(fullpath, os.path.join(transform_path, new_file))
            else:
                LOGGER.warning("MP4 failed policy check: %s", policy_check)
                os.remove(complete_path)
                continue

    LOGGER.info("==================== END Transcode MP4 Access Copy Creation - BBC ==================")


def get_dar(fullpath: str) -> str:
    """
    Retrieves metadata DAR info and returns as string
    """

    dar_setting = utils.get_metadata("Video", "DisplayAspectRatio/String", fullpath)
    if len(dar_setting) >= 6:
        print(f"Suspect height has multiple returned streams: {dar_setting}")
        dar_setting = remove_stream_repeats(dar_setting, fullpath)

    if "4:3" in str(dar_setting):
        return "4:3"
    if "16:9" in str(dar_setting):
        return "16:9"
    if "15:11" in str(dar_setting):
        return "4:3"
    if "1.85:1" in str(dar_setting):
        return "1.85:1"
    if "2.2:1" in str(dar_setting):
        return "2.2:1"

    return str(dar_setting)


def get_par(fullpath: str) -> str:
    """
    Retrieves metadata PAR info and returns
    Checks if multiples from multi video tracks
    """

    par_setting = utils.get_metadata("Video", "PixelAspectRatio", fullpath)
    par_full = str(par_setting).rstrip("\n")
    if len(par_full) >= 6:
        print(f"Suspect height has multiple returned streams: {par_full}")
        par_full = remove_stream_repeats(par_full, fullpath)

    if len(par_full) <= 5:
        return par_full
    return par_full[:5]


def remove_stream_repeats(value: str, fullpath: str) -> str:
    """
    Deals with instances where height/width/DAR/PAR return
    multiple values for multiple streams - Video stream only
    """

    count = utils.get_metadata("General", "VideoCount", fullpath)
    print(f"Video stream total found: {count}")
    if not count.isnumeric():
        return value
    elif int(count) > 1:
        if len(value) % len(count) == 0:
            chop_length = len(value) // int(count)
            return value[:chop_length]
    else:
        return value


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

    if len(height) >= 6:
        print(f"Suspect height has multiple returned streams: {height}")
        height = remove_stream_repeats(height, fullpath)

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


def get_width(fullpath: str) -> str:
    """
    Retrieves height information using mediainfo
    """

    width = utils.get_metadata("Video", "Width/String", fullpath)
    clap_width = utils.get_metadata("Video", "Width_CleanAperture/String", fullpath)

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

    if len(width) >= 6:
        print(f"Suspect width has multiple returned streams: {width}")
        width = remove_stream_repeats(width, fullpath)
    if width.isdigit():
        return str(width)

    width = width.split(" p", maxsplit=1)[0]
    return re.sub("[^0-9]", "", width)


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
    audio_str = str(audio.decode("utf-8").lstrip("\n").rstrip("\n"))
    audio_channels = str(audio_str).split("\n")
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
    audio_str = str(audio.decode("utf-8")).lstrip("\n").rstrip("\n")
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


def get_duration(fullpath: str) -> Optional[tuple[int, str]]:
    """
    Retrieves duration information via mediainfo
    where more than two returned, file longest of
    first two and return video stream info to main
    for update to ffmpeg map command
    """

    duration = utils.get_metadata("Video", "Duration", fullpath)
    if not duration:
        return (0, "")
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


def check_audio(
    fullpath: str,
) -> tuple[Optional[str], Optional[str], Optional[Union[bytes, list[str]]]]:
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
    except (subprocess.CalledProcessError, Exception):
        lang0_str = ""
    try:
        lang1 = subprocess.check_output(cmd1)
        lang1_str = lang1.decode("utf-8")
    except (subprocess.CalledProcessError, Exception):
        lang1_str = ""
    try:
        streams = subprocess.check_output(cmd2)
        streams_str = streams.decode("utf-8").lstrip("\n").rstrip("\n").split("\n")
    except (subprocess.CalledProcessError, Exception):
        streams_str = None
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
    twelve_chnl: bool,
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
        "23",
    ]

    pix = ["-pix_fmt", "yuv420p"]

    fast_start = ["-movflags", "faststart"]

    crop_sd_608 = [
        "-vf",
        "yadif,crop=672:572:24:32,scale=734:576:flags=lanczos,pad=768:576:-1:-1,drawtext=fontfile='/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf':fontsize=45:text='BFI Research Viewings':fontcolor=white:alpha=0.6:x=(w-text_w)/2:y=100",
    ]

    no_stretch_4x3 = [
        "-vf",
        "yadif,pad=768:576:-1:-1,drawtext=fontfile='/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf':fontsize=45:text='BFI Research Viewings':fontcolor=white:alpha=0.6:x=(w-text_w)/2:y=100"
    ]

    crop_sd_4x3 = [
        "-vf",
        "yadif,crop=672:572:24:2,scale=734:576:flags=lanczos,pad=768:576:-1:-1,drawtext=fontfile='/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf':fontsize=45:text='BFI Research Viewings':fontcolor=white:alpha=0.6:x=(w-text_w)/2:y=100",
    ]

    upscale_sd_width = [
        "-vf",
        "yadif,scale=1024:-1:flags=lanczos,pad=1024:576:-1:-1,drawtext=fontfile='/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf':fontsize=45:text='BFI Research Viewings':fontcolor=white:alpha=0.6:x=(w-text_w)/2:y=100",
    ]

    upscale_sd_height = [
        "-vf",
        "yadif,scale=-1:576:flags=lanczos,pad=1024:576:-1:-1,drawtext=fontfile='/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf':fontsize=45:text='BFI Research Viewings':fontcolor=white:alpha=0.6:x=(w-text_w)/2:y=100",
    ]

    scale_sd_4x3 = [
        "-vf",
        "yadif,scale=768:576:flags=lanczos,drawtext=fontfile='/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf':fontsize=45:text='BFI Research Viewings':fontcolor=white:alpha=0.6:x=(w-text_w)/2:y=100",
    ]

    scale_sd_16x9 = [
        "-vf",
        "yadif,scale=1024:576:flags=lanczos,drawtext=fontfile='/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf':fontsize=45:text='BFI Research Viewings':fontcolor=white:alpha=0.6:x=(w-text_w)/2:y=100",
    ]

    crop_sd_15x11 = [
        "-vf",
        "yadif,crop=704:572,scale=768:576:flags=lanczos,pad=768:576:-1:-1,drawtext=fontfile='/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf':fontsize=45:text='BFI Research Viewings':fontcolor=white:alpha=0.6:x=(w-text_w)/2:y=100",
    ]

    crop_ntsc_486 = [
        "-vf",
        "yadif,crop=672:480,scale=734:486:flags=lanczos,pad=768:486:-1:-1,drawtext=fontfile='/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf':fontsize=45:text='BFI Research Viewings':fontcolor=white:alpha=0.6:x=(w-text_w)/2:y=100",
    ]

    crop_ntsc_486_16x9 = [
        "-vf",
        "yadif,crop=672:480,scale=1024:486:flags=lanczos,drawtext=fontfile='/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf':fontsize=45:text='BFI Research Viewings':fontcolor=white:alpha=0.6:x=(w-text_w)/2:y=100",
    ]

    crop_ntsc_640x480 = [
        "-vf",
        "yadif,pad=768:480:-1:-1,drawtext=fontfile='/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf':fontsize=45:text='BFI Research Viewings':fontcolor=white:alpha=0.6:x=(w-text_w)/2:y=100",
    ]

    crop_sd_16x9 = [
        "-vf",
        "yadif,crop=704:572:8:2,scale=1024:576:flags=lanczos,drawtext=fontfile='/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf':fontsize=45:text='BFI Research Viewings':fontcolor=white:alpha=0.6:x=(w-text_w)/2:y=100",
    ]

    sd_downscale_4x3 = [
        "-vf",
        "yadif,scale=768:576:flags=lanczos,drawtext=fontfile='/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf':fontsize=45:text='BFI Research Viewings':fontcolor=white:alpha=0.6:x=(w-text_w)/2:y=100",
    ]

    hd_16x9 = [
        "-vf",
        "yadif,scale=-1:720:flags=lanczos,pad=1280:720:-1:-1,drawtext=fontfile='/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf':fontsize=60:text='BFI Research Viewings':fontcolor=white:alpha=0.6:x=(w-text_w)/2:y=100",
    ]

    hd_16x9_letterbox = [
        "-vf",
        "yadif,scale=1280:-1:flags=lanczos,pad=1280:720:-1:-1,drawtext=fontfile='/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf':fontsize=80:text='BFI Research Viewings':fontcolor=white:alpha=0.6:x=(w-text_w)/2:y=100",
    ]

    fhd_all = [
        "-vf",
        "yadif,scale=-1:1080:flags=lanczos,pad=1920:1080:-1:-1,drawtext=fontfile='/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf':fontsize=80:text='BFI Research Viewings':fontcolor=white:alpha=0.6:x=(w-text_w)/2:y=100",
    ]

    fhd_letters = [
        "-vf",
        "yadif,scale=1920:-1:flags=lanczos,pad=1920:1080:-1:-1,drawtext=fontfile='/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf':fontsize=80:text='BFI Research Viewings':fontcolor=white:alpha=0.6:x=(w-text_w)/2:y=100",
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
    elif twelve_chnl is True:
        map_audio = [
            "-map",
            "0:a?",
            "-af",
            "pan=stereo|c0=FL+0.707*FC|c1=FR+0.707*FC",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            "-dn",
        ]
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
    print(f"Audio command chosen: {map_audio}")

    # Calculate height/width to decide HD scale path
    height = int(height)
    width = int(width)
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
    elif height == 576 and aspect < 1.778:
        cmd_mid = scale_sd_4x3
    elif width <= 768 and aspect < 1.778:
        cmd_mid = scale_sd_4x3
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
    elif height > 720 and aspect >= 1.778:
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


def conformance_check(file: str) -> str:
    """
    Checks file against MP4 mediaconch policy
    Looks for essential items to ensure that
    the transcode was successful
    """
    success = utils.get_mediaconch(file, MP4_POLICY)
    if success[0] is True:
        return "PASS!"
    else:
        return f"FAIL! This policy has failed {success[1]}"


if __name__ == "__main__":
    main()
