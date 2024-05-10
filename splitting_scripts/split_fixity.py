#!/usr/bin/env python3

'''
MUST BE LAUNCHED FROM SHELL SCRIPT
FOR SYS.ARGV[1] Path and SYS.ARGV[2] '' / multi

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
import json
import time
import glob
import shutil
import logging
import datetime
import subprocess

# Private packages
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib
import document_item
import models
import clipmd5

# GLOBAL PATHS FROM SYS.ARGV
try:
    TARGET = str(sys.argv[1])
    MULTI_ITEMS = True if sys.argv[2] == 'multi' else False
except IndexError:
    MULTI_ITEMS = False

if not os.path.exists(TARGET):
    sys.exit(f"EXIT: Target path received not valid: {TARGET}")

# Path to split files destination
SOURCE, NUM = os.path.split(TARGET)  # Source folder
OUTPUT = os.path.join(os.path.split(SOURCE)[0], 'segmented')
MEDIA_TARGET = os.path.split(OUTPUT)[0]  # Processing folder
AUTOINGEST = os.path.join(os.path.split(MEDIA_TARGET)[0], 'autoingest')
LOG_PATH = os.environ['SCRIPT_LOG']
CID_API = os.environ['CID_API4']

# Setup logging, overwrite each time
logger = logging.getLogger(f'split_fixity_{NUM}')
hdlr = logging.FileHandler(os.path.join(MEDIA_TARGET, f'log/split_{NUM}.log'))
# hdlr = logging.FileHandler(os.path.join(MEDIA_TARGET, f'log/split_{NUM}.log'), mode=w)
formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)


def control_check():
    '''
    Check that `downtime_control.json` has not indicated termination
    '''
    with open(os.path.join(LOG_PATH, 'downtime_control.json')) as control:
        j = json.load(control)
        if not j['split_control_ofcom']:
            logger.info("Exit requested by downtime_control.json")
            sys.exit('Exit requested by downtime_control.json')


def get_duration(fullpath):
    '''
    Retrieve file duration using ffprobe
    '''

    cmd = [
        'mediainfo', '--Language=raw',
        '--Full', '--Inform="Video;%Duration%"',
        fullpath
    ]

    cmd[3] = cmd[3].replace('"', '')
    duration = subprocess.check_output(cmd)
    duration = duration.decode('utf-8').rstrip('\n')
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


def check_media_record(fname):
    '''
    Check if CID media record
    already created for filename
    '''
    search = f"imagen.media.original_filename='{fname}'"
    hits = adlib.retrieve_record(CID_API, 'media', search, '0')[0]
    print(f"Check media record response: {hits} hits")

    if hits >= 1:
        return True
    elif hits == 0:
        return False


def main():
    '''
    Process file and complete segmentation
    generate CID record
    '''

    # List files in recursive sub-directories
    files = []
    for root, _, filenames in os.walk(TARGET):
        for filename in [f for f in filenames if f.endswith(('.mov', '.mxf', '.mkv', '.MOV', '.MXF', '.MKV'))]:
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
        print(f"Modelling identifier from {id_}")
        try:
            i = models.PhysicalIdentifier(id_)
            logger.info(i)
            style = i.type
            logger.info('* Identifier is %s', style)
        except Exception as err:
            logger.warning('models.py error: %s\t%s\t%s', filepath, id_, err)
            print(err)
            continue

        # Model carrier
        print(f"Modelling carrier from '**{style}: {id_}'")
        try:
            c = models.Carrier(**{style: id_})
            logger.info('* Carrier modelled ok')
        except Exception as err:
            print(err)
            logger.warning('%s\t%s\n%s', filepath, id_, err)
            continue

        # Model segments
        print("Modelling segments from Carrier")
        try:
            segments = dict(c.segments)
            print(segments)
            logger.info('* Segments modelled ok')
        except Exception as err:
            logger.warning('%s\t%s\n%s', filepath, id_, err)
            print(err)
            continue

        # Only process multi-item tapes if script was invoked with argument 'multi'
        print("Checking number of Items from carrier modelling")
        logger.info('*** Checking number of Items from carrier modelling: %s', len(c.items))
        if len(c.items) > 1:
            print("Multi-item tape found")
            if not MULTI_ITEMS:
                print("Skipping as not running 'multi' pass")
                logger.info("*** Not multi item so skipping.")
                continue

        # Create carrier-level output directory
        carrier_directory = f'{OUTPUT}/{id_}'
        print(f"Creating carrier directory: {carrier_directory}")
        logger.info('* Carrier directory is %s/%s', OUTPUT, id_)

        # Process each item on tape
        print("Begin iterating items in Carrier")
        for item in c.items:
            item_priref = int(adlib.retrieve_field_name(item, 'priref')[0])
            object_number = adlib.retrieve_field_name(item, 'object_number')[0]
            logger.info('%s\t* Item priref is %s and object number is %s', filepath, item_priref, object_number)
            print(f"Item to be processed: {item_priref} {object_number}")

            # Check whether object_number derivative has been documented already
            print(f"Launching document_item.py to check if CID item record exists: {object_number}")
            try:
                hits, exists = document_item.already_exists(object_number)
            except Exception as err:
                logger.warning('%s\tUnable to determine if derived record already exists for\t%s\n%s', filepath, object_number, err)
                continue

            # Check to prevent progress if more than one CID item record exists
            if exists:
                print(f"Exists: {exists} Hits: {hits}")

                # Avoid working with any file that has more than one derived item record
                if hits > 1:
                    logger.info('%s\t* More than one item record for derived MKV already exists for\t%s. Skipping.', filepath, object_number)
                    print(f"{c.partwhole[0]} is 1 and more than one CID item record exists - this file split will be skipped.")
                    continue
                try:
                    existing_ob_num = adlib.retrieve_field_name(exists, 'object_number')[0]
                except (IndexError, TypeError, KeyError):
                    logger.info("Unable to get object_number for file checks. Skipping")
                    continue
                logger.info('%s\t* Item record for derived MKV already exists for\t%s', filepath, existing_ob_num)
                firstpart_check = f"{existing_ob_num.replace('-','_')}_01of{str(c.partwhole[1]).zfill(2)}.{extension}"
                check_filename = f"{existing_ob_num.replace('-','_')}_{str(c.partwhole[0]).zfill(2)}of{str(c.partwhole[1]).zfill(2)}.{extension}"
                print(firstpart_check, check_filename)

                # Check for first part before allowing next parts to advance
                if c.partwhole[0] != 1:
                    print(f"Checking if first part has already been created or has persisted to DPI: {firstpart_check}")
                    check_result = check_media_record(firstpart_check)
                    firstpart = False
                    if check_result is True:
                        print(f"First part {firstpart_check} exists in CID, proceeding to check for {check_filename}")
                        firstpart = True
                    if check_result is None:
                        logger.warning("Skipping: Unable to retrieve CID Media hit information, try again next pass")
                        continue
                    print(f"**** AUTOINGEST: {AUTOINGEST}")
                    match_path = glob.glob(f"{AUTOINGEST}/**/*/{firstpart_check}", recursive=True)
                    print(f"****** MATCH PATH {match_path}")
                    if firstpart_check in str(match_path):
                        firstpart = True
                    if not firstpart:
                        logger.info("%s\tSkipping: First part has not yet been created, no CID match %s", filepath, firstpart_check)
                        continue
                    logger.info("%s\tPart 01of* in group found %s. Checking if part also ingested...", filepath, firstpart_check, check_filename)

                check_result = check_media_record(check_filename)
                if check_result is True:
                    print(f"SKIPPING: Filename {check_filename} matched with persisted CID media record")
                    logger.warning("%s\tPart found ingested to DPI: %s.", filepath, check_filename)
                    continue
                if check_result is None:
                    print("SKIPPING: API hit attempt failed. Leaving for another pass.")
                    logger.warning("%s\tAPI attempt returned None. Leaving for another pass: %s.", filepath, check_filename)
                    continue
                matched = glob.glob(f"{AUTOINGEST}/**/*/{check_filename}", recursive=True)
                if check_filename in str(matched):
                    print(f"SKIPPING: CID item record exists and file found in autoingest: {check_filename}")
                    logger.warning("%s\t* Skipping. Part found already in autoingest: %s.", filepath, check_filename)
                    continue
                logger.info("%s\t* Item %s not already created. Clear to continue progress.", filepath, check_filename)

            # If destination file already exists, move on
            of = f'{OUTPUT}/{id_}/{object_number}.{extension}'
            logger.info('%s\t* Destination for new file: %s', filepath, of)
            if os.path.isfile(of):
                logger.warning('%s\tDestination file already exists for file: %s', filepath, of)
                continue

            # Programme in/out (seconds)
            print("Get in / out from segment data")
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
            print("Get file duration")
            item_duration = get_duration(filepath)
            print(f"*********** {item_duration} ***********")
            if not item_duration:
                print("No duration retrieved")
                logger.warning("%s\t* Item has no duration, skipping this file.", filepath)
                continue

            # Get positon of item on tape
            print("Get position of item on tape")
            if segments:
                pos = list(segments).index(item_priref)
                logger.info("%s\tSegment position: %s", filepath, pos)
                print(f"Segment position: {pos}")

                # Item is first on tape
                if pos == 0:
                    # Begin at tape head
                    in_ = 0
                    print("Item is first on tape, starting at tape head...")
                    logger.info('%s\t* Item is first on tape, starting at tape head...', filepath)
                else:
                    # Begin at end of previous item
                    print('* Item is not first on tape, starting at end of previous item...')
                    logger.info("%s\t* Item is not first on tape, starting at end of previous item...", filepath)
                    k = list(segments.items())[pos-1][0]
                    in_ = segments[k][-1][-1][-1]

                    # Check that tc-in is after tc-out of preceding item
                    print("Check that tc-in is after tc-out of preceding item")
                    if a < in_:
                        logger.warning('%s\t%s\tInvalid video_part data: item begins before end of preceding item\t%s', filepath, id_, in_)
                        print("Invalid video part data: item begins before end of preceding item")
                        # Skip entire carrier
                        break

                # Item is last on tape
                print("If item is last on tape")
                if pos == len(segments)-1:
                    # End at tape tail
                    out = item_duration
                    print(f"Out set to full duration: {out}")
                else:
                    # End at beginning of next item
                    k = list(segments.items())[pos+1][0]
                    out = segments[k][0][0][0]
                    print(k, out)

            print("If no segments, and length of all Carrier items is 1:")
            if not segments and len(c.items) == 1:
                print("Item is the only one on the tape, no segmenting and move only")
                logger.info("%s\t* Item is only one Item on the tape, no need to segment the file...", filepath)
                in_ = 0
                out = item_duration
                print(in_, out)

            print("If in is greater than out:")
            if in_ > out:
                print("Duration of tape is less than timecode start. Skipping")
                logger.warning("%s\t* Duration of tape %s is less than timecode start %s. Skipping this split.", filepath, out, in_)
                continue

            # Format segment to HH:MM:SS timecodes
            print("Converting segments to HH:MM:SS timecode")
            tcin = time.strftime('%H:%M:%S', time.gmtime(float(in_)))
            tcout = time.strftime('%H:%M:%S', time.gmtime(float(out)))
            print(f"TC in {tcin} TC out {tcout}")

            # Translate new relative segment positions
            print("Translate new relative segment positions")
            relative_segments = []
            file_duration = None
            content_duration = 0
            if segments:
                relative_in = float(a) - in_
                offset = a - relative_in
                print(f"Segments: {relative_in} {offset}")
                for s in segments[item_priref][0]:
                    i = time.strftime('%H:%M:%S', time.gmtime(float(s[0]) - offset))
                    o = time.strftime('%H:%M:%S', time.gmtime(float(s[1]) - offset))
                    t = (i, o)
                    print(t)
                    relative_segments.append(t)

                    # Calculate content duration (excludes inter-segment chunks)
                    content_duration += (s[1] - s[0])

                file_duration = time.strftime('%H:%M:%S', time.gmtime(content_duration))
                logger.info("%s\tFile duration: %s", filepath, file_duration)
                print(f"File duration calculated: {file_duration}")

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
            print(f"Making segment: {make_segment}")

            # Create carrier-level output directory
            print("Creating the Carrier directory: {carrier_directory}")
            if not os.path.exists(carrier_directory):
                os.mkdir(carrier_directory)
                # os.umask(0)
                # os.mkdir(carrier_directory)

            print("If not make_segment")
            if not make_segment:
                # No need to stream-copy single item, simply move file to destination
                try:
                    print(f'* Move single-item file: {f} -> {of}')
                    logger.info('%s\tMoved single-item file\t%s -> %s', filepath, f, of)
                    shutil.move(filepath, of)
                except Exception as err:
                    print(err)
                    logger.warning('%s\tUnable to move and rename file\t%s -> %s\n%s', filepath, f, of, err)
                    continue
            else:
                print(f'Start fixity segmentation of multi-Item file at {datetime.datetime.now().ctime()}')
                logger.info('%s\tStarting fixity segmentation of multi-Item file at\t%s', filepath, datetime.datetime.now().ctime())

                # Extract segment by stream copying with FFmpeg
                print("Extracting segment by stream copy with FFmpeg...")
                additional_args = ['-dn', '-map', '0', '-c', 'copy', '-copyts', '-avoid_negative_ts', 'make_zero']
                print(f"Call clipmd5.clipmd5({filepath}, {tcin}, {of}, {tcout}, {additional_args})")
                fixity = clipmd5.clipmd5(filepath, tcin, of, tcout, additional_args)
                print(f"Fixity confirmed: {fixity}")
                logger.info('%s\tFixity confirmed: %s', filepath, fixity)

                if not fixity:
                    # Log FFmepg error
                    ffmpeg_message = f'{filepath}\tFFmpeg failed to create Matroska file\t{of}'
                    logger.warning(ffmpeg_message)
                    print(f"Fixity failed: {ffmpeg_message}")

                    # Clean up failed media object
                    try:
                        os.remove(of)
                        print(f"Attempting deletion of asset: {of}")
                        logger.info('%s\tDeleted invalid Matroska file\t%s', filepath, of)
                    except Exception as err:
                        print(err)
                        logger.warning('%s\tFailed to delete invalid Matroska file\t%s\n%s', f, of, err)

                    # Next item
                    print("Skipping to next item")
                    continue

            # Sense check that split file does exist before creation of Item record
            print(f"Check if new split file exists: {of}")
            if not os.path.isfile(of):
                print(f'* Split file does not exist, do not proceed with CID item record creation: {of}')
                logger.info('%s\tSplit file does not exist. Moving to next item. %s', filepath, of)
                continue

            print(f'End fixity segmentation at {datetime.datetime.now().ctime()}')
            logger.info('%s\tEnd fixity segmentation at %s', filepath, datetime.datetime.now().ctime())

            # Document new media object in CID
            print("Begin process of creating new CID item record")
            note = 'autocreated'
            if make_segment:
                note = 'autocreated (segmented)'

            logger.info("LOG CHECK: document_item.new_or_existing(%s, %s, %s, %s, note=%s)", object_number, relative_segments, file_duration, extension, note)
            print(f"Launching document_item.new({object_number}, {relative_segments}, {file_duration}, {extension}, note={note})")

            # Single-item tape
            if c.partwhole[1] == 1:
                print("** Single item tape")
                try:
                    new_object = document_item.new_or_existing(object_number, relative_segments, file_duration, extension, note=note)
                    if not new_object:
                        print("Creation of new CID item record failed!")
                        document_message = f'{filepath}\tFailed to document Matroska file in CID: {of}'
                        logger.warning(document_message)
                        continue
                except Exception as err:
                    document_message = f'{filepath}\tFailed to document Matroska file in CID: {of}\t{err}'
                    logger.warning(document_message)
                    print(document_message)
                    continue
                # New object successfully retrieved / created
                logger.info('%s\tCreated/found CID Item for Matroska file: %s', filepath, object_number)
                print(f"Created/found CID item record: {object_number}")

            # Multi-part
            elif c.partwhole[1] > 1:
                print("** Multi-part tape")
                try:
                    new_object = document_item.new_or_existing(object_number, relative_segments, file_duration, extension, note=note)
                    if not new_object:
                        print("Creation of new CID item record failed!")
                        document_message = f'{filepath}\tFailed to document MKV in CID or read object_number of new CID Item record: {of}'
                        logger.warning(document_message)
                        continue
                except Exception as err:
                    document_message = f'{filepath}\tFailed to document MKV in CID or read object_number of new CID Item record: {of}\n{err}'
                    logger.warning(document_message)
                    print(document_message)
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
                logger.info('%s\tCID Item for Matroska file identified: %s / %s', filepath, object_number, new_object)
                print(f"CID item record created/retrieved and linked to source: {object_number} / {new_object}")

            # Rename media object with N-* object_number and partWhole
            print("Renaming the media object with 'N_' and partWhole")
            new_object = new_object.replace('-', '_')
            logger.info("%s\tPart whole retrieved from models: %s", filepath, c.partwhole)
            part = str(c.partwhole[0]).zfill(2)
            whole = str(c.partwhole[1]).zfill(2)
            print(new_object, part, whole)
            logger.info("%s\tPart whole script adjustments: %s of %s", filepath, part, whole)
            ext = of.split('.')[-1]
            nf = f'{new_object}_{part}of{whole}.{ext}'
            dst = f'{OUTPUT}/{id_}/{nf}'
            print(dst)

            if os.path.isfile(dst):
                logger.warning('%s\tFilename already exists in path, skipping renaming: %s', filepath, dst)
                continue

            try:
                logger.info('%s\tRenaming Matroska file with new Item object_number and partWhole: %s --> %s', filepath, os.path.basename(of), nf)
                print(f'\t{os.path.basename(of)} --> {nf}')
                os.rename(of, dst)
            except Exception as err:
                logger.warning('%s\tFailed to rename Matroska file with new Item object_number and partWhole\t%s\n%s', filepath, of, err)
                print(err)
                continue


if __name__ == "__main__":
    main()
