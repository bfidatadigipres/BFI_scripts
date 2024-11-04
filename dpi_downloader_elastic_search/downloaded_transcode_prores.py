#!/usr/bin/env python3

'''
Module script for BFI National Archive downloader app.

This script manages transcoding to ProRes of assets downloaded
from the BFI National Archive file downloader.
It receives the downloaded source file path and
processes the file in situ before returning the new
encoded file path to the downloader app script, which sends
an email notification of the file's completed download
and transcode.

2023
'''

import os
import re
import sys
import time
import json
import logging
import subprocess
import magic

# Global paths from server environmental variables
PATH_POLICY = os.environ['MEDIACONCH']
PRORES_POLICY = os.path.join(PATH_POLICY, 'BFI_download_transcode_basic_prores.xml')
LOG = os.environ['LOG_PATH']
CONTROL_JSON = os.path.join(LOG, 'downtime_control.json')

# Setup logging
logger = logging.getLogger('downloaded_transcode_prores')
hdlr = logging.FileHandler(os.path.join(LOG, 'scheduled_database_downloader_transcode.log'))
formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)


def check_control():
    '''
    Check control json for downtime requests
    '''
    with open(CONTROL_JSON) as control:
        j = json.load(control)
        if not j['pause_scripts']:
            return False
        else:
            return True


def check_mime_type(fpath):
    '''
    Checks the mime type is video
    and if stream media checks ffprobe
    '''
    if fpath.endswith(('.ts', '.mxf', '.mpg')):
        mime = 'video'
    else:
        mime = magic.from_file(fpath, mime=True)
    try:
        type_ = mime.split('/')[0]
        print(f'* mime type is {type_}')
    except IOError:
        logger.warning('%s\tCannot open file, resource busy', fpath)
        return False
    if type_ != 'video':
        print(f'* MIMEtype "{type_}" is not video...')
        return False
    if type_ == 'video':
        cmd = ['ffprobe',
               '-i', fpath,
               '-loglevel', '-8']
        try:
            code = subprocess.call(cmd)
            if code != 0:
                logger.warning('%s\tffprobe failed to read file: [%s] status', fpath, code)
                return False
            print('* ffprobe read file successfully - status 0')
        except Exception as err:
            logger.warning('%s\tffprobe failed to read file', fpath)
            print(err)
            return False
    return True


def get_dar(fullpath):
    '''
    Retrieves metadata DAR info and returns as string
    '''
    cmd = [
        'mediainfo',
        '--Language=raw', '--Full',
        '--Inform="Video;%DisplayAspectRatio/String%"',
        fullpath
    ]

    cmd[3] = cmd[3].replace('"', '')
    dar_setting = subprocess.check_output(cmd)
    dar_setting = dar_setting.decode('utf-8')

    if '4:3' in str(dar_setting):
        return '4:3'
    if '16:9' in str(dar_setting):
        return '16:9'
    if '15:11' in str(dar_setting):
        return '4:3'
    if '1.85:1' in str(dar_setting):
        return '1.85:1'
    if '2.2:1' in str(dar_setting):
        return '2.2:1'

    return str(dar_setting)


def get_par(fullpath):
    '''
    Retrieves metadata PAR info and returns
    Checks if multiples from multi video tracks
    '''
    cmd = [
        'mediainfo',
        '--Language=raw', '--Full',
        '--Inform="Video;%PixelAspectRatio%"',
        fullpath
    ]

    cmd[3] = cmd[3].replace('"', '')
    par_setting = subprocess.check_output(cmd)
    par_setting = par_setting.decode('utf-8')
    par_full = str(par_setting).rstrip('\n')

    if len(par_full) <= 5:
        return par_full
    else:
        return par_full[:5]


def get_height(fullpath):
    '''
    Retrieves height information via mediainfo
    Using sampled height where original
    height and stored height differ (MXF samples)
    '''

    cmd = [
        'mediainfo',
        '--Language=raw', '--Full',
        '--Inform="Video;%Sampled_Height%"',
        fullpath
    ]

    cmd[3] = cmd[3].replace('"', '')
    sampled_height = subprocess.check_output(cmd)
    cmd2 = [
        'mediainfo',
        '--Language=raw', '--Full',
        '--Inform="Video;%Height%"',
        fullpath
    ]

    cmd2[3] = cmd2[3].replace('"', '')
    reg_height = subprocess.check_output(cmd2)

    try:
        int(sampled_height)
    except ValueError:
        sampled_height = 0

    if int(sampled_height) > int(reg_height):
        height = str(sampled_height)
    else:
        height = str(reg_height)

    if '480' == height:
        return '480'
    if '486' == height:
        return '486'
    if '576' == height:
        return '576'
    if '608' == height:
        return '608'
    if '720' == height:
        return '720'
    if '1080' == height or '1 080' == height:
        return '1080'
    else:
        height = height.split(' pixel', maxsplit=1)[0]
        return re.sub("[^0-9]", "", height)


def get_width(fullpath):
    '''
    Retrieves height information using mediainfo
    '''
    cmd = [
        'mediainfo',
        '--Language=raw', '--Full',
        '--Inform="Video;%Width/String%"',
        fullpath
    ]

    cmd[3] = cmd[3].replace('"', '')
    width = subprocess.check_output(cmd)
    width = str(width)

    if '720' == width:
        return '720'
    if '768' == width:
        return '768'
    if '1024' == width or '1 024' == width:
        return '1024'
    if '1280' == width or '1 280' == width:
        return '1280'
    if '1920' == width or '1 920' == width:
        return '1920'
    else:
        if width.isdigit():
            return str(width)
        else:
            width = width.split(' p', maxsplit=1)[0]
            return re.sub("[^0-9]", "", width)


def get_duration(fullpath):
    '''
    Retrieves duration information via mediainfo
    where more than two returned, file longest of
    first two and return video stream info to main
    for update to ffmpeg map command
    '''

    cmd = [
        'mediainfo', '--Language=raw',
        '--Full', '--Inform="Video;%Duration%"',
        fullpath
    ]

    cmd[3] = cmd[3].replace('"', '')
    duration = subprocess.check_output(cmd)
    if not duration:
        return ('', '')

    duration = duration.decode('utf-8').rstrip('\n')
    print(f"Mediainfo seconds: {duration}")

    if '.' in duration:
        duration = duration.split('.')

    if isinstance(duration, str):
        second_duration = int(duration) // 1000
        return (second_duration, '0')
    elif len(duration) == 2:
        print("Just one duration returned")
        num = duration[0]
        second_duration = int(num) // 1000
        print(second_duration)
        return (second_duration, '0')
    elif len(duration) > 2:
        print("More than one duration returned")
        dur1 = f"{duration[0]}"
        dur2 = f"{duration[1][6:]}"
        print(dur1, dur2)
        if int(dur1) > int(dur2):
            second_duration = int(dur1) // 1000
            return (second_duration, '0')
        elif int(dur1) < int(dur2):
            second_duration = int(dur2) // 1000
            return (second_duration, '1')


def check_audio(fullpath):
    '''
    Mediainfo command to retrieve channels, identify
    stereo or mono, returned as 2 or 1 respectively
    '''

    cmd = [
        'mediainfo', '--Language=raw',
        '--Full', '--Inform="Audio;%Format%"',
        fullpath
    ]

    cmd0 = [
        'ffprobe', '-v',
        'error', '-select_streams', 'a:0',
        '-show_entries', 'stream=index:stream_tags=language',
        '-of', 'compact=p=0:nk=1',
        fullpath
    ]

    cmd1 = [
        'ffprobe', '-v',
        'error', '-select_streams', 'a:1',
        '-show_entries', 'stream=index:stream_tags=language',
        '-of', 'compact=p=0:nk=1',
        fullpath
    ]

    cmd2 = [
        'mediainfo', '--Language=raw',
        '--Full', '--Inform="Audio;%ChannelLayout%"',
        fullpath
    ]

    cmd[3] = cmd[3].replace('"', '')
    audio = subprocess.check_output(cmd)
    audio = str(audio)

    if len(audio) == 0:
        return None, None, None

    try:
        lang0 = subprocess.check_output(cmd0)
    except Exception:
        lang0 = ''
    try:
        lang1 = subprocess.check_output(cmd1)
    except Exception:
        lang1 = ''

    print(f"**** LANGUAGES: Stream 0 {lang0} - Stream 1 {lang1}")

    cmd2[3] = cmd2[3].replace('"', '')
    chnl_layout = subprocess.check_output(cmd2)
    chnl_layout = str(chnl_layout)
    stereo_lr = False

    if 'LR' in chnl_layout:
        stereo_lr = True

    if 'NAR' in str(lang0):
        print("Narration stream 0 / English stream 1")
        if stereo_lr:
            return ('Audio', '1', 'ac')
        return('Audio', '1', None)
    elif 'NAR' in str(lang1):
        print("Narration stream 1 / English stream 0")
        if stereo_lr:
            return ('Audio', '0', 'ac')
        return ('Audio', '0', None)
    else:
        if stereo_lr:
            return ('Audio', None, 'ac')
        return ('Audio', None, None)


def create_ffmpeg_command(fullpath, output, video_data):
    '''
    Subprocess command build, with variations
    added based on metadata extraction
    '''

    # Build subprocess call from data list
    ffmpeg_program_call = [
        "ffmpeg"
    ]

    input_video_file = [
        "-i", fullpath,
        "-nostdin"
    ]

    # Map video stream that's longest to 0
    if video_data[0]:
        print(f"VS {video_data[0]}")
        map_video = [
            "-map", f"0:v:{video_data[0]}",
        ]
    else:
        map_video = [
            "-map", "0:v:0",
        ]


    video_settings = [
        "-c:v", "prores_ks",
        "-profile:v", "3"
    ]

    prores_build = [
        "-pix_fmt", "yuv422p10le",
        "-vendor", "ap10",
        "-movflags", "+faststart"
    ]

    if video_data[3] and video_data[2] and not video_data[4]:
        map_audio = [
            "-map", "0:a?",
            f"-disposition:a:{video_data[3]}",
            "default", "-dn"
        ]
    elif video_data[4]:
        map_audio = [
            "-map", "0:a?",
            "-ac", "1", "-dn"
        ]
    else:
        map_audio = [
            "-map", "0:a?",
            "-dn"
        ]

    crop_sd_608 = [
        "-vf",
        "bwdif=send_frame,crop=672:572:24:32,scale=734:576:flags=lanczos,pad=768:576:-1:-1"
    ]

    no_crop = [
        "-vf",
        "bwdif=send_frame"
    ]

    output_settings = [
        "-nostdin", "-y",
        output, "-f",
        "null", "-"
    ]

    height = int(video_data[1])
    if height == 608:
        cmd_mid = crop_sd_608
    else:
        cmd_mid = no_crop

    if video_data[2] is None:
        return ffmpeg_program_call + input_video_file + map_video + video_settings + cmd_mid + prores_build + output_settings
    elif video_data[2]:
        return ffmpeg_program_call + input_video_file + map_video + map_audio + video_settings + cmd_mid + prores_build + output_settings


def check_policy(output_path):
    '''
    Run mediaconch check against new prores
    '''
    new_file = os.path.split(output_path)[1]
    if os.path.isfile(output_path):
        logger.info("Conformance check: comparing %s with policy", new_file)
        result = conformance_check(output_path)
        if "PASS!" in result:
            return 'pass!'
        else:
            return result


def conformance_check(filepath):
    '''
    Checks mediaconch policy against new V210 mov
    '''

    mediaconch_cmd = [
        'mediaconch', '--force',
        '-p', PRORES_POLICY,
        filepath
    ]

    try:
        success = subprocess.check_output(mediaconch_cmd)
        success = str(success)
    except Exception:
        success = ""
        logger.exception("Mediaconch policy retrieval failure for %s", filepath)

    if 'N/A!' in success:
        return "FAIL!"
    elif 'pass!' in success:
        return "PASS!"
    elif 'fail!' in success:
        return "FAIL!"
    else:
        return "FAIL!"


def transcode_mov(fpath):
    '''
    Receives sys.argv[1] path to MOV from shell start script via GNU parallel
    Passes to FFmpeg subprocess command, transcodes ProRes mov then checks
    finished encoding against custom prores mediaconch policy
    If pass, cleans up files moving to finished_prores/ folder and deletes V210 mov (temp offline).
    '''
    fullpath = fpath
    if not os.path.isfile(fullpath):
        logger.warning("SCRIPT EXITING: Error with file path:\n %s", sys.argv)
        return False
    mime_true = check_mime_type(fullpath)
    if not mime_true:
        logger.warning("SCRIPT EXITING: Supplied file is not mimetype video:\n %s", sys.argv)
        return 'not video'
    running = check_control()
    if not running:
        logger.warning('Script run prevented by downtime_control.json. Script exiting.')
        return False

    logger.info("================== START DPI download transcode to prores START ==================")
    path_split = os.path.split(fullpath)
    file = path_split[1]
    output_fullpath = os.path.join(path_split[0], f"{file.split('.')[0]}_prores.mov")
    if os.path.isfile(output_fullpath):
        return 'exists'
    logger_data = []
    video_data = []

    # Collect data for downloaded file
    audio, stream_default, stereo = check_audio(fullpath)
    dar = get_dar(fullpath)
    par = get_par(fullpath)
    height = get_height(fullpath)
    width = get_width(fullpath)
    duration, vs = get_duration(fullpath)
    video_data = [vs, height, audio, stream_default, stereo]

    logger_data.append(f"** File being processed: {fullpath}")
    logger_data.append(f"Metadata retrieved:\nDAR {dar} PAR {par} Audio {audio} Height {height} Width {width} Duration {duration}")

    # Execute FFmpeg subprocess call
    ffmpeg_call = create_ffmpeg_command(fullpath, output_fullpath, video_data)
    ffmpeg_call_neat = (" ".join(ffmpeg_call), "\n")
    logger_data.append(f"FFmpeg call: {ffmpeg_call_neat}")

    # tic/toc record encoding time
    tic = time.perf_counter()
    try:
        subprocess.call(ffmpeg_call)
        logger_data.append("Subprocess call for FFmpeg command successful")
    except Exception as err:
        logger_data.append(f"WARNING: FFmpeg command failed: {ffmpeg_call_neat}\n{err}")
        log_clean = list(dict.fromkeys(logger_data))
        for line in log_clean:
            logger.info("%s", line)
        logger.info("==================== END DPI download transcode to prores END ====================")
        return 'transcode fail'
    toc = time.perf_counter()
    encoding_time = (toc - tic) // 60
    seconds_time = toc - tic
    logger_data.append(f"*** Encoding time for {file}: {encoding_time} minutes or as seconds: {seconds_time}")
    logger_data.append("Checking if new Prores file passes Mediaconch policy")
    pass_policy = check_policy(output_fullpath)
    if pass_policy == 'pass!':
        logger_data.append("New ProRes file passed MediaConch policy")
        log_clean = list(dict.fromkeys(logger_data))
        for line in log_clean:
            logger.info("%s", line)
        logger.info("==================== END DPI download transcode to prores END ====================")
        return 'True'
    else:
        logger_data.append(f"ProRes file failed the MediaConch policy:\n{pass_policy}")
        log_clean = list(dict.fromkeys(logger_data))
        for line in log_clean:
            logger.info("%s", line)
        logger.info("==================== END DPI download transcode to prores END ====================")
        return 'transcode fail'
