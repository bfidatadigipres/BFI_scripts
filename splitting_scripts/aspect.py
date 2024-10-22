#! /usr/bin/env /usr/local/bin/python3

'''
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
'''

# Public modules
import os
import re
import sys
import shutil
import logging
import subprocess

# Public packages
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib
import utils

# Configure adlib
CID_API = os.environ['CID_API4']

# Setup logging
LOGGER = logging.getLogger('aspect_ratio_triage')
LOGS = os.environ['SCRIPT_LOG']
HDLR = logging.FileHandler(os.path.join(LOGS, 'aspect_ratio_triage.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

FOLDERS = {
    f"{os.environ['QNAP_H22']}/processing/segmented/": f"{os.environ['AUTOINGEST_QNAP02']}ingest/proxy/video/adjust/",
    f"{os.environ['ISILON_VID']}/processing/segmented/": f"{os.environ['AUTOINGEST_IS_VID']}ingest/proxy/video/adjust/",
    f"{os.environ['QNAP_H22']}/processing/rna_mkv/": f"{os.environ['AUTOINGEST_QNAP02']}ingest/proxy/video/adjust/",
    f"{os.environ['GRACK_H22']}/processing/rna_mkv/": f"{os.environ['AUTOINGEST_H22']}ingest/proxy/video/adjust/",
    f"{os.environ['QNAP_08']}/processing/segmented/": f"{os.environ['AUTOINGEST_QNAP08']}ingest/proxy/video/adjust/",
    f"{os.environ['QNAP_10']}/processing/segmented/": f"{os.environ['AUTOINGEST_QNAP10']}ingest/proxy/video/adjust/",
    f"{os.environ['QNAP_VID']}/processing/segmented/": f"{os.environ['AUTOINGEST_QNAP01']}ingest/proxy/video/adjust/"
}


def get_dar(fullpath):
    '''
    Retrieves metadata DAR info and returns as string
    '''
    cmd = [
        'mediainfo',
        '--Language=raw', '--Full',
        '--Inform="Video;%DisplayAspectRatio%"',
        fullpath
    ]

    cmd[3] = cmd[3].replace('"', '')
    dar_setting = subprocess.check_output(cmd)
    dar_setting = dar_setting.decode('utf-8')
    dar = str(dar_setting).rstrip('\n')
    return dar


def get_aspect_ratio(fullpath):
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
    dar = str(dar_setting).rstrip('\n')
    return dar


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
    sampled_height = sampled_height.decode('utf-8')
    sheight = str(sampled_height).rstrip('\n')

    cmd2 = [
        'mediainfo',
        '--Language=raw', '--Full',
        '--Inform="Video;%Height%"',
        fullpath
    ]

    cmd2[3] = cmd2[3].replace('"', '')
    reg_height = subprocess.check_output(cmd2)
    reg_height = reg_height.decode('utf-8')
    rheight = str(reg_height).rstrip('\n')

    try:
        int(sheight)
    except ValueError:
        sheight = 0

    if sheight:
        height = [str(sheight) if int(sheight) > int(rheight) else str(rheight)]
    else:
        height = str(rheight)

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

    height = height.split(' pixel', maxsplit=1)[0]
    return re.sub("[^0-9]", "", height)


def check_parent_aspect_ratio(object_number):
    '''
    Retrieve the DAR from the parent item record
    object_number supplied
    '''
    if not object_number.startswith('N_'):
        return None
    
    search = f"object_number='{object_number}'"
    hits, record = adlib.retrieve_record(CID_API, 'media', search, '1', ['priref'])
    print(f"Check media record response: {hits} hits\n{record}")
    if hits is None or hits == 0:
        return None
    
    priref = adlib.retrieve_field_name(record[0], 'priref')[0]
    rec = adlib.retrieve_record(CID_API, 'media', f'priref="{priref}"', '1', ['source_item.lref'])[1]
    source_priref = adlib.retrieve_field_name(rec[0], 'priref')[0]
    source_rec = adlib.retrieve_record(CID_API, 'media', f'priref="{source_priref}"', '1', ['aspect_ratio'])[1]
    return adlib.retrieve_field_name(source_rec[0], 'aspect_ratio')[0]


def adjust_par_metadata(filepath):
    '''
    Use MKVToolNix MKVPropEdit to
    adjust the metadata for PAR output
    check output correct
    '''
    dar = get_dar(filepath)

    cmd = [
        'mkvpropedit', filepath,
        '--edit', 'track:v1',
        '--set', 'display-width=295',
        '--set', 'display-height=228'
    ]

    confirmed = subprocess.run(cmd, shell=False, check=True, universal_newlines=True, stdout=subprocess.PIPE, text=True)
    confirmed = str(confirmed.stdout)
    print(confirmed)

    if 'The changes are written to the file.' not in str(confirmed):
        LOGGER.warning("DAR conversion failed: %s", confirmed)
        return False

    new_dar = get_dar(filepath)
    if '1.29' in new_dar:
        LOGGER.info("DAR converted from %s to %s", dar, new_dar)
        return True


def fix_aspect_ratio(fpath, height):
    '''
    Trim if height 608 if needed
    Change DAR 4x3 to 16x9
    '''
    fpath_split, ext = os.path.splitext(fpath)
    replace = f"{fpath_split}_4x3.{ext}"

    launch = [
        'ffmpeg', '-i',
        fpath
    ]

    if str(height) == '608':
        crop = [
            '-vf',
            'crop=720:608-32:0:576'
        ]
    else:
        crop = []

    if 'mkv' in ext.lower():
        cv = [
            '-c:v', 'ffv1', '-level', '3',
            '-g', '1', '-slicecrc', '1'
        ]
    else:
        cv = [
            '-c:v', 'v210'
        ]

    aspect = [
        '-aspect', '16:9'
    ]

    audio_map = [
        '-c:a', 'copy',
        '-map', '0',
        replace
    ]
    command = launch + crop + cv + aspect + audio_map
    try:
        process = subprocess.run(command, shell=False, capture_output=True, text=True)            
    except subprocess.CalledProcessError as err:
        LOGGER.warning(err)
        return None

    if process.returncode != 0:
        if os.path.exists(replace):
            LOGGER.warning("Subprocess returncode was not 0, deleted failed transcode attempt")
            os.remove(replace)
            return False
    else:
        if os.path.exists(replace):
            check_dar = get_aspect_ratio(replace)
            if '16:9' in check_dar:
                os.rename(fpath, f"{fpath}_DELETE")
                os.rename(replace, fpath)
                return True
            else:
                LOGGER.warning("Aspect ratio of new file is not 16:9, deleted failed transcode attempt")
                os.remove(replace)
                return False     
        else:
            LOGGER.info("Renaming of file that replaces %s failed: %s.", fpath, replace)
            LOGGER.info("Manual fix needed to complete process.")
            return False


def main():
    '''
    Iterate folders, checking for files (not partial)
    extract metdata and filter to correct autoingest path
    '''
    LOGGER.info("==== aspect.py START =================")

    for fol in FOLDERS:
        LOGGER.info("Targeting folder: %s", fol)
        files = []
        for root, _, filenames in os.walk(fol):
            files += [os.path.join(root, file) for file in filenames]

        for f in files:
            fn = os.path.basename(f)
            # Require N-* <object_number>
            if not fn.startswith('N_'):
                print(f'{f}\tFilename does not start with N_')
                LOGGER.warning('%s\tFilename does not start with N_', f)
                continue

            # Require partWhole
            if 'of' not in fn:
                print(f'{f}\tFilename does not contain _of_')
                LOGGER.warning('%s\tFilename does not contain _of_', f)
                continue

            # Ignore partials
            if 'partial' in fn:
                print(f'Skipping: Partial in filename {fn}')
                continue

            ext = f.split('.')[-1]

            # Get height
            height = get_height(f)
            print(f'Height: {height}')

            # Test for 608 line height
            if not height:
                print(f'{f}\tCould not fetch frame height (px)')
                LOGGER.warning('%s\tCould not fetch frame height (px)', f)
                continue
            '''
            JMW - Requires test to ensure file cropped/encoded are not at fault
            # Check aspect ratio of CID item record
            ob_num = utils.get_object_number(fn)
            aspect = check_parent_aspect_ratio(ob_num)
            print(aspect)
            if '16:9' in str(aspect):
                LOGGER.info("File requires transcode to aspect ratio 16x9")
                if not fix_aspect_ratio(f, height):
                    LOGGER.warning("Unsuccessful attempt to change Aspect ratio to 4x3: %s", fn)
                    continue
                LOGGER.info("File metadata updated to 4x3 and file replaced with new version: %s", fn)
            '''
            # Check PAR and DAR
            dar = get_dar(f)
            par = get_par(f)
            if not dar:
                print(f'{f}\tCould not fetch DAR from header')
                LOGGER.warning('%s\tCould not fetch DAR from header', f)
                continue
            if not par:
                print(f'{f}\tCould not fetch PAR from header')
                LOGGER.warning('%s\tCould not fetch PAR from header', f)
                continue
            # Update CID with DAR warning
            if '1.26' in dar:
                print(f'{f}\tFile found with 1.26 DAR. Converting to 1.29 DAR')
                LOGGER.info('%s\tFile found with 1.26 DAR. Converting to 1.29 DAR', f)
                confirmed = adjust_par_metadata(f)
                if not confirmed:
                    print(f'{f}\tCould not adjust DAR metdata. Skipping file.')
                    LOGGER.warning('%s\tCould not adjust DAR metadata. Skipping file.', f)
                    continue
                print(f'{f}\tFile DAR header metadata changed to 1.29')
                LOGGER.info('%s\tFile DAR header metadata changed to 1.29', f)

            # Collect decimalised aspects
            aspects = []
            # for a, b in [i.split(':') for i in [dar, par]]:
            decimal = float(dar) / float(par)
            print(decimal)
            aspects.append(decimal)

            if not aspects:
                print(f'{f}\tCould not handle aspects')
                LOGGER.warning('%s\tCould not handle aspects', f)
                continue

            # Test aspects
            target_aspect = None
            if height == '608':
                target_aspect = None
                target_height = os.path.join('608')
                target_path = os.path.join(FOLDERS[fol], target_height)
                target = os.path.join(target_path, fn)
                print(f'Moving {f}\t to {target}')

                try:
                    shutil.move(f, target)
                except Exception:
                    LOGGER.warning('%s\tCould not move to target: %s\t', f, target)
                    raise

            elif all(a > 1.42 for a in aspects):
                target_aspect = os.path.join('16x9', ext)
            elif all(a < 1.4 for a in aspects):
                target_aspect = os.path.join('4x3', ext)
            else:
                print(f'{f}\tCould not resolve aspects: {aspects}\t')
                LOGGER.warning('%s\tCould not resolve aspects: %s\t', f, aspects)
                continue

            if target_aspect:
                target_path = os.path.join(FOLDERS[fol], target_aspect)
                target = os.path.join(target_path, fn)
                print(f'Moving {f}\t to {target}')

                try:
                    shutil.move(f, target)
                    LOGGER.info('%s\tSuccessfully moved to target: %s\t', f, target)
                except Exception:
                    LOGGER.warning('%s\tCould not move to target: %s\t', f, target)
                    raise

    LOGGER.info("==== aspect.py END ===================\n")


if __name__ == '__main__':
    main()
