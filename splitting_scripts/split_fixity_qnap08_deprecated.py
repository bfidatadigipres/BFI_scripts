#!/usr/bin/env python3

'''
MUST BE LAUNCHED FROM SHELL SCRIPT
FOR SYS.ARGV[1] Path and SYS.ARGV[2] '' / multi
Unique paths for QNAP-08 to QNAP-01 FFmpeg stream copy

Process F47 digitisation output:
   - expect can_ID or package_number file inputs
   - model item contents of whole tape digitisation
   - translate existing content segmentation timecode data
   - move single-item tapes to destination
   - split multi-item tapes into content segments
   - document new media objects in CID

 Use:
  # Single item tapes only
  $ python split_fixity.py <path_to_folder>

  # All tapes
  $ python split_fixity.py <path_to_folder> multi

Refactored for Python3
June 2022
'''

# Public packages
import os
import sys
import glob
import json
import time
import shutil
import logging
import datetime
import subprocess
from typing import Final, Optional, Any

# Private packages
sys.path.append(os.environ['CODE'])
import adlib
import utils
import document_item
import models
import clipmd5

# GLOBAL PATHS FROM SYS.ARGV
try:
    TARGET: Final = str(sys.argv[1])
    MULTI_ITEMS: Final = True if sys.argv[2] == 'multi' else False
except IndexError:
    MULTI_ITEMS = False

if not os.path.exists(TARGET):
    sys.exit(f"EXIT: Target path received not valid: {TARGET}")

# Path to split files destination
SOURCE, NUM = os.path.split(TARGET)
OUTPUT_08: Final = os.path.join(os.path.split(SOURCE)[0], 'segmented')
OUTPUT_01: Final = os.environ['QNAP01_SEGMENTED']
MEDIA_TARGET: Final = os.path.split(OUTPUT_08)[0]  # Processing folder
AUTOINGEST_01: Final = os.environ['AUTOINGEST_QNAP01']
AUTOINGEST_08: Final = os.environ['AUTOINGEST_QNAP08']
LOG_PATH: Final = os.environ['LOG_PATH']

# Setup CID
CID_API: Final = utils.get_current_api()
CID: Final = adlib.Database(url=CID_API)
CUR: Final = adlib.Cursor

# Setup logging, overwrite each time
logger = logging.getLogger(f'split_fixity_{NUM}')
#hdlr = logging.FileHandler(os.path.join(MEDIA_TARGET, f'log/split_{NUM}.log'))
hdlr = logging.FileHandler(os.path.join(MEDIA_TARGET, f'log/split_{NUM}.log'), mode='w')
formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)


def control_check() -> None:
    '''
    Check that `downtime_control.json` has not indicated termination
    '''
    with open(os.path.join(LOG_PATH, 'downtime_control.json')) as control:
        j = json.load(control)
        if not j['split_control_ofcom']:
            logger.info("Exit requested by downtime_control.json")
            sys.exit('Exit requested by downtime_control.json')


def get_duration(fullpath: str) -> int:
    '''
    Retrieve file duration using ffprobe
    '''

    cmd: list[str] = [
        'mediainfo', '--Language=raw',
        '--Full', '--Inform="Video;%Duration%"',
        fullpath
    ]

    cmd[3] = cmd[3].replace('"', '')
    duration = subprocess.check_output(cmd).decode('utf-8').rstrip('\n')
    if len(duration) <= 1:
        return None
    print(len(duration), duration)
    print(f"Mediainfo seconds: {duration}")

    if '.' in duration:
        duration = duration.split('.')
    if isinstance(duration, str):
        second_duration = int(duration) // 1000
        return second_duration
    if len(duration) == 2:
        print("Just one duration returned")
        num = duration[0]
        second_duration = int(num) // 1000
        return second_duration
    if len(duration) > 2:
        print("More than one duration returned")
        dur1 = f"{duration[0]}"
        dur2 = f"{duration[1][6:]}"
        if int(dur1) > int(dur2):
            second_duration = int(dur1) // 1000
            return second_duration
        if int(dur1) < int(dur2):
            second_duration = int(dur2) // 1000
            return second_duration
    return None


def check_media_record(fname: str) -> bool:
    '''
    Check if CID media record
    already created for filename
    '''
    search: str = f"imagen.media.original_filename='{fname}'"
    query: dict[str, str] = {
        'database': 'media',
        'search': search,
        'limit': '0',
        'output': 'json',
    }

    try:
        result = CID.get(query)
        if result.hits:
            return True
    except Exception as err:
        print(f"Unable to retrieve CID Media record {err}")
    return False


def match_in_autoingest(fname: str, autoingest: str) -> list[str]:
    '''
    Run a glob check of path
    '''
    match = glob.glob(f"{autoingest}/**/*/{fname}", recursive=True)
    if not match:
        print(f"No match found in {autoingest} : {fname}")
        return None
    print(f"Match: {fname} - {match}")
    return match


def main():
    '''
    Process file and complete segmentation
    generate CID record
    '''

    # List files in recursive sub-directories
    files = []
    for root, _, filenames in os.walk(TARGET):
        for filename in [f for f in filenames if f.endswith(('.mov', '.mxf', '.mkv', '.MOV', '.MKV', '.MXF'))]:
            files.append(os.path.join(root, filename))

    # Process digitised tape files sequentially
    print("----------------------------------------------------")
    print(files)
    for filepath in files:
        control_check()

        f = os.path.basename(filepath)
        logger.info('=== Current file: %s Multi item: %s', filepath, MULTI_ITEMS)

        # Require file extension
        try:
            extension = f.split('.')[-1]
        except Exception as exc:
            logger.warning('%s\tMissing extension: %s', filepath, exc)
            continue

        # Expect can_ID or package_number filename
        id_ = f.split('.')[0]

        # Model identifier
        try:
            i = models.PhysicalIdentifier(id_)
            logger.info(i)
            style = i.type
            logger.info('* Identifier is %s', style)
        except Exception as err:
            logger.warning('models.py error: %s\t%s\t%s', filepath, id_, err)
            continue

        # Model carrier
        try:
            c = models.Carrier(**{style: id_})
            logger.info('* Carrier modelled ok')
        except Exception as err:
            logger.warning('%s\t%s\t%s', filepath, id_, err)
            continue

        # Model segments
        try:
            segments = dict(c.segments)
            logger.info('* Segments modelled ok')
        except Exception as err:
            logger.warning('%s\t%s\t%s', filepath, id_, err)
            continue

        # Only process multi-item tapes if script was invoked with argument 'multi'
        logger.info('*** Checking number of Items from carrier modelling: %s', len(c.items))
        if len(c.items) > 1:
            if not MULTI_ITEMS:
                logger.info("*** Not multi item so skipping.")
                continue

        # Create carrier-level output directory
        qnap_01_carrier_directory = os.path.join(OUTPUT_01, id_)
        qnap_08_carrier_directory = os.path.join(OUTPUT_08, id_)
        logger.info('* Carrier directories created\nQNAP-01 %s / QNAP-08 %s', qnap_01_carrier_directory, qnap_08_carrier_directory)

        # Process each item on tape
        for item in c.items:
            item_priref = int(item['priref'][0])
            object_number = item['object_number'][0]
            logger.info('%s\t* Item priref is %s and object number is %s', filepath, item_priref, object_number)

            # Check whether object_number derivative has been documented already
            try:
                exists = document_item.already_exists(object_number)
            except Exception as err:
                logger.warning('%s\tUnable to determine if derived record already exists for\t%s\n%s', filepath, object_number, err)
                continue

            # Check to prevent progress if more than one CID item record exists
            if exists:
                print(f"Exists: {exists} Hits: {exists.hits}")

                # Avoid working with any file that has more than one derived item record
                if exists.hits > 1:
                    logger.info('%s\t* More than one item record for derived MKV already exists for\t%s. Skipping.', filepath, object_number)
                    print(f"{c.partwhole[0]} is 1 and more than one CID item record exists - this file split will be skipped.")
                    continue
                try:
                    existing_ob_num = exists.records[0]['object_number'][0]
                except (IndexError, TypeError, KeyError):
                    logger.info("Unable to get object_number for file checks. Skipping")
                    continue
                logger.info('%s\t* Item record for derived MKV already exists for\t%s', filepath, existing_ob_num)
                logger.info('%s\tChecking if file has already persisted to DPI, or is in autoingest paths', filepath)
                firstpart_check = f"{existing_ob_num.replace('-','_')}_01of{str(c.partwhole[1]).zfill(2)}.{extension}"
                check_filename = f"{existing_ob_num.replace('-','_')}_{str(c.partwhole[0]).zfill(2)}of{str(c.partwhole[1]).zfill(2)}.{extension}"
                print(f"Checking if {firstpart_check} or {check_filename} persisted to DPI or are in autoingest")

                # Check for first part before allowing next parts to advance
                print(AUTOINGEST_01)
                print(AUTOINGEST_08)
                if c.partwhole[0] != 1:
                    print(f"Checking if first part has already been created or has persisted to DPI: {firstpart_check}")
                    check_result = check_media_record(firstpart_check)
                    firstpart = False
                    if check_result:
                        print(f"First part {firstpart_check} exists in CID, proceeding to check for {check_filename}")
                        firstpart = True
                    match1 = match_in_autoingest(firstpart_check, AUTOINGEST_01)
                    match2 = match_in_autoingest(firstpart_check, AUTOINGEST_08)
                    if match1 or match2:
                        firstpart = True
                    if not firstpart:
                        logger.info("%s\tSkipping: First part has not yet been created, no CID match %s", filepath, firstpart_check)
                        continue
                    logger.info("%s\tPart 01of* in group found %s. Checking if part also ingested...", filepath, firstpart_check, check_filename)

                check_result = check_media_record(check_filename)
                if check_result:
                    print(f"SKIPPING: Filename {check_filename} matched with persisted CID media record")
                    logger.warning("%s\tPart found ingested to DPI: %s.", filepath, check_filename)
                    continue
                match01 = match_in_autoingest(check_filename, AUTOINGEST_01)
                match02 = match_in_autoingest(check_filename, AUTOINGEST_08)
                if match01 or match02:
                    print(f"SKIPPING: CID item record exists and file found in autoingest: {check_filename}")
                    logger.warning("%s\t* Skipping. Part found already in autoingest: %s.", filepath, check_filename)
                    continue
                logger.info("%s\t* Item %s not already created. Clear to continue progress.", filepath, check_filename)

            logger.info("%s\tNo derived CID item record already exists for object number %s.", filepath, object_number)
            # If destination file already exists, move on
            of_01 = os.path.join(qnap_01_carrier_directory, f"{object_number}.{extension}")
            of_08 = os.path.join(qnap_08_carrier_directory, f"{object_number}.{extension}")
            logger.info('%s\t* Destinations created for QNAP-01 and QNAP-08: \n%s\t%s', filepath, of_01, of_08)
            if os.path.isfile(of_01):
                if segments:
                    logger.warning('%s\tDestination file already exists for segmented file. Deleting: %s', filepath, of_01)
                    os.remove(of_01)
                else:
                    logger.warning('%s\tDestination file already exists for non-segmented file: %s', filepath, of_01)
                    continue
            elif os.path.isfile(of_08):
                logger.warning('%s\tDestination file already exists for file: %s', filepath, of_08)
                continue

            # Programme in/out (seconds)
            if segments:
                try:
                    a = segments[item_priref][0][0][0]
                    b = segments[item_priref][-1][-1][-1]
                    logger.info("%s\t* Segments\t%s\t%s", filepath, a, b)
                    print(f'* Segments\t{a}\t{b}')
                except (IndexError, KeyError) as exc:
                    # debug this
                    print(segments)
                    print(exc)
                    raise

            # Get duration of file
            item_duration = get_duration(filepath)
            if not item_duration:
                logger.warning("%s\t* Item has no duration, skipping this file.", filepath)
                continue
            # Get positon of item on tape
            if segments:
                pos = list(segments).index(item_priref)
                logger.info("%s\tSegment position: %s", filepath, pos)

                # Item is first on tape
                if pos == 0:
                    # Begin at tape head
                    in_ = 0
                    logger.info('%s\t* Item is first on tape, starting at tape head...', filepath)
                else:
                    # Begin at end of previous item
                    print('* Item is not first on tape, starting at end of previous item...')
                    logger.info("%s\t* Item is not first on tape, starting at end of previous item...", filepath)
                    k = list(segments.items())[pos-1][0]
                    in_ = segments[k][-1][-1][-1]

                    # Check that tc-in is after tc-out of preceding item
                    if a < in_:
                        logger.warning('%s\t%s\tInvalid video_part data: item begins before end of preceding item\t{}', filepath, id_, in_)
                        # Skip entire carrier
                        break

                # Item is last on tape
                if pos == len(segments)-1:
                    # End at tape tail
                    out = item_duration
                else:
                    # End at beginning of next item
                    k = list(segments.items())[pos+1][0]
                    out = segments[k][0][0][0]

            if not segments and len(c.items) == 1:
                logger.info("%s\t* Item is only one Item on the tape, no need to segment the file...", filepath)
                in_ = 0
                out = item_duration

            if in_ > out:
                logger.warning("%s\t* Duration of tape %s is less than timecode start %s. Skipping this split.", filepath, out, in_)
                continue

            # Format segment to HH:MM:SS timecodes
            tcin = time.strftime('%H:%M:%S', time.gmtime(float(in_)))
            tcout = time.strftime('%H:%M:%S', time.gmtime(float(out)))
            print(f"TC in {tcin} TC out {tcout}")

            # Translate new relative segment positions
            relative_segments = []
            file_duration = None
            content_duration = 0
            if segments:
                relative_in = float(a) - in_
                offset = a - relative_in

                for s in segments[item_priref][0]:
                    i = time.strftime('%H:%M:%S', time.gmtime(float(s[0]) - offset))
                    o = time.strftime('%H:%M:%S', time.gmtime(float(s[1]) - offset))
                    t = (i, o)
                    relative_segments.append(t)

                    # Calculate content duration (excludes inter-segment chunks)
                    content_duration += (s[1] - s[0])

                file_duration = time.strftime('%H:%M:%S', time.gmtime(content_duration))
                logger.info("%s\tFile duration: %s", filepath, file_duration)

            rel = '; '.join([('-'.join([i[0], i[1]])) for i in relative_segments])
            print('')
            print(f'Carrier: {id_}')
            print(f'Item: {item_priref}')
            print(f'Content duration: {file_duration}')
            print(f'Content chapters: {rel}')
            print(f'Extract from source: {tcin} -> {tcout}')
            print('')

            if len(c.items) == 1:
                make_segment = False
            elif len(c.items) >= 2:
                make_segment = True

            if not make_segment:
                # Create carrier-level output directory
                qnap_08 = True
                if not os.path.exists(qnap_08_carrier_directory):
                    os.mkdir(qnap_08_carrier_directory)

                # No need to stream-copy single item, simply move file to destination
                try:
                    print(f'* Move single-item file: {f} -> {of_08}')
                    logger.info('%s\tMoved single-item file\t%s -> %s', filepath, f, of_08)
                    shutil.move(filepath, of_08)
                except Exception:
                    logger.warning('%s\tUnable to move and rename file\t%s -> %s', filepath, f, of_08)
                    continue
            else:
                # Create carrier-level output directory
                qnap_08 = False
                if not os.path.exists(qnap_01_carrier_directory):
                    os.mkdir(qnap_01_carrier_directory)

                print(f'Start fixity segmentation of multi-Item file at {datetime.datetime.now().ctime()}')
                logger.info('%s\tStarting fixity segmentation of multi-Item file at\t%s', filepath, datetime.datetime.now().ctime())

                # Extract segment by stream copying with FFmpeg
                additional_args = ['-dn', '-map', '0', '-c', 'copy', '-copyts', '-avoid_negative_ts', 'make_zero']
                fixity = clipmd5.clipmd5(filepath, tcin, of_01, tcout, additional_args)
                logger.info('%s\tFixity confirmed: %s', filepath, fixity)

                if not fixity:
                    # Log FFmpeg error
                    ffmpeg_message = f'{filepath}\tFFmpeg failed to create Matroska file\t{of_01}'
                    logger.warning(ffmpeg_message)
                    print(ffmpeg_message)

                    # Clean up failed media object
                    try:
                        os.remove(of_01)
                        logger.info('%s\tDeleted invalid Matroska file\t%s', filepath, of_01)
                    except Exception as err:
                        logger.warning('%s\tFailed to delete invalid Matroska file\t%s\n%s', f, of_01, err)

                    # Next item
                    continue

            # Sense check that split file does exist before creation of Item record
            if os.path.isfile(of_01):
                print(f'End fixity segmentation at {datetime.datetime.now().ctime()}')
                logger.info('%s\tEnd fixity segmentation at %s', filepath, datetime.datetime.now().ctime())
            else:
                if os.path.isfile(of_08):
                    print(f'End fixity segmentation at {datetime.datetime.now().ctime()}')
                    logger.info('%s\tEnd fixity segmentation at %s', filepath, datetime.datetime.now().ctime())
                else:
                    print(f'* Split file does not exist, do not proceed with CID item record creation: {of_01} or {of_08}')
                    logger.info('%s\tSplit file does not exist. Moving to next item.\n%s\t%s', filepath, of_01, of_08)
                    continue

            print(f'End fixity segmentation at {datetime.datetime.now().ctime()}')
            logger.info('%s\tEnd fixity segmentation at %s', filepath, datetime.datetime.now().ctime())

            # Document new media object in CID
            note = 'autocreated'
            if make_segment:
                note = 'autocreated (segmented)'

            # Single-item tape
            if c.partwhole[1] == 1:
                try:
                    new_object = document_item.new_or_existing(object_number, relative_segments, file_duration, extension, note=note)
                    if not new_object:
                        document_message = f'{filepath}\tFailed to document Matroska file in CID: {id_}/{object_number}.{extension}'
                        logger.warning(document_message)
                        continue
                except Exception as err:
                    document_message = f'{filepath}\tFailed to document Matroska file in CID: {err}\t{id_}/{object_number}.{extension}'
                    logger.warning(document_message)
                    print(document_message)
                    continue
                # New object successfully retrieved / created
                logger.info('%s\tCreated new CID Item for Matroska file: %s', filepath, object_number)

            # Multi-part
            elif c.partwhole[1] > 1:
                try:
                    new_object = document_item.new_or_existing(object_number, relative_segments, file_duration, extension, note=note)
                    if not new_object:
                        document_message = f'{filepath}\tFailed to document Matroska file in CID: {id_}/{object_number}.{extension}'
                        logger.warning(document_message)
                        continue
                except Exception as err:
                    document_message = f'{filepath}\tFailed to document Matroska file in CID: {err}\t{id_}/{object_number}.{extension}'
                    logger.warning(document_message)
                    # consider deleting object even though only documentation stage failed,
                    # re-segmenting is probably more trivial than a new process for fixing
                    # the missing documentation
                    continue

                if not new_object:
                    object_message = f'{filepath}\tFailed to read object_number from newly created CID Item record'
                    logger.warning(object_message)
                    print(object_message)
                    continue

                # New object successfully retrieved / created
                logger.info('%s\tCreated / Retrieved CID Item for Matroska file: %s', filepath, object_number)

            #Rename media object with N-* object_number and partWhole
            new_object = new_object.replace('-', '_')
            logger.info("%s\tMultipart partwhole retrieved from models: %s", filepath, c.partwhole)
            part = str(c.partwhole[0]).zfill(2)
            whole = str(c.partwhole[1]).zfill(2)
            logger.info("%s\tPart whole script adjustments: %s of %s", filepath, part, whole)
            if qnap_08:
                ext = of_08.split('.')[-1]
                nf = f'{new_object}_{part}of{whole}.{ext}'
                dst = os.path.join(qnap_08_carrier_directory, nf)
                if os.path.isfile(dst):
                    logger.warning('%s\tFilename already exists in path, skipping renaming: %s', filepath, dst)
                    continue
                try:
                    logger.info('%s\tRenaming Matroska file with new Item object_number and partWhole: %s --> %s', filepath, f'{object_number}.{extension}', nf)
                    print(f'\t{of_08} --> {dst}')
                    os.rename(of_08, dst)
                except Exception as err:
                    logger.warning('%s\tFailed to rename Matroska file with new Item object_number and partWhole\t%s\n%s', filepath, of_08, err)
                    continue
            else:
                ext = of_01.split('.')[-1]
                nf = f'{new_object}_{part}of{whole}.{ext}'
                dst = os.path.join(qnap_01_carrier_directory, nf)
                if os.path.isfile(dst):
                    logger.warning('%s\tFilename already exists in path, skipping renaming: %s', filepath, dst)
                    continue
                try:
                    logger.info('%s\tRenaming Matroska file with new Item object_number and partWhole: %s --> %s', filepath, f'{object_number}.{extension}', nf)
                    print(f'\t{of_08} --> {dst}')
                    os.rename(of_01, dst)
                except Exception as err:
                    logger.warning('%s\tFailed to rename Matroska file with new Item object_number and partWhole\t%s\n%s', filepath, of_01, err)
                    continue


if __name__ == "__main__":
    main()
