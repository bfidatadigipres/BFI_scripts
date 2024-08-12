#!/usr/bin/env python3

'''
Create a new CID Item record for a file and rename
the file with the new <object_number>

Converted to Python3
Joanna White
2023
'''
import os
import sys
import time
import shutil
import logging
import argparse
import subprocess

# Private modules
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib
import utils

DOWNTIME_CONTROL = os.environ['CONTROL_JSON']
TS_DAY = time.strftime("%Y-%m-%d")
TS_TIME = time.strftime("%H:%M:%S")
DUPE_ERRORS = os.path.join(os.environ['GRACK_H22'], 'duplicates/')
CID_API = os.environ['CID_API4']

# Setup logging
logger = logging.getLogger('rna_document')
hdlr = logging.FileHandler(os.path.join(os.environ['GRACK_H22'], '/mediaconch/rna_document_move.txt'))
formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)


RNA_PRIREF = {
    'EAFA': 20009,
    'IWM': 360,
    'LSA': 999697937,
    'MACE': 132860,
    'NEFA': 999711319,
    'NIS': 104087,
    'NSSAW': 999568451,
    'NWFA': 20893,
    'SASE': 999375090,
    'NLS': 131840,
    'WFSA': 92251,
    'TheBox': 999816052,
    'YFA': 999376880
}

FRAMEWORK_PRIREF = {
    'DC1': 999795134,
    'LMH': 999801915,
    'MX1': 999761305,
    'TVT': 155340,
    'VDM': 80878,
    'INN': 999825298,
    'ATG_Exceptions': 999570701,
    'CJP': 999875894,
    'IMES': 999875895
}


def default_record():
    '''
    Default values for an item record
    '''

    data = [{'input.date': TS_DAY},
            {'input.time': TS_TIME},
            {'input.name': 'datadigipres'},
            {'input.notes': 'Heritage 2022 videotape digitisation - automated documentation'},
            {'record_type': 'ITEM'},
            {'grouping': 'H22: Video Digitisation: Item Outcomes'},
            {'file_type': 'MKV'},
            {'code_type': 'FFV1 v3'},
            {'item_type': 'DIGITAL'},
            {'copy_status': 'M'},
            {'copy_usage': 'Restricted access to preserved digital file'},
            {'record_access.user': 'BFIiispublic'},
            {'record_access.rights': '0'}]

    return data


def make_item_record(rna, frame_work):
    '''
    Return item record data as a list of field-value pair dictionaries
    '''

    rna_priref = RNA_PRIREF[rna]
    framework_priref = FRAMEWORK_PRIREF[frame_work]

    new_record = default_record()
    new_record.append({'acquisition.source.lref': str(rna_priref)})
    new_record.append({'creator.lref': str(framework_priref)})
    new_record.append({'creator.role': 'Laboratory'})
    new_record.append({'production.reason': 'Heritage 2022 video digitisation project'})

    return new_record


def check_supplier(filepath):
    '''
    Match file path to supplier
    short form
    '''
    supplier = None

    if 'LMH' in filepath:
        supplier = 'LMH'
    if 'DC1' in filepath:
        supplier = 'DC1'
    if 'VDM' in filepath:
        supplier = 'VDM'
    if 'MX1' in filepath:
        supplier = 'MX1'
    if 'INN' in filepath:
        supplier = 'INN'
    if 'ATG' in filepath:
        supplier = 'ATG_Exceptions'
    if 'CJP' in filepath:
        supplier = 'CJP'
    if 'IMES' in filepath:
        supplier = 'IMES'

    return supplier


def get_duration(filepath):
    '''
    Replacing fffilter
    '''
    cmd = [
        'mediainfo',
        '--Language=raw', '-f',
        '--Output=General;%Duration%',
        filepath
    ]

    try:
        duration = subprocess.check_output(cmd)
        duration = duration.decode('utf-8')
    except Exception as err:
        logger.warning(err)
        duration = ''

    return duration


def main(filepath, frame_work, destination=None):
    '''
    Create a new item record and rename source file
    '''
    if not utils.check_control('pause_scripts'):
        logger.info('Script run prevented by downtime_control.json. Script exiting.')
        sys.exit('Script run prevented by downtime_control.json. Script exiting.')
    if not utils.cid_check(CID_API):
        logger.critical("* Cannot establish CID session, exiting script")
        sys.exit("* Cannot establish CID session, exiting script")

    # Parse filename string
    in_file = os.path.basename(filepath)
    rna = in_file.split('_')[0]
    filename = in_file.split('.')[0]
    ext = in_file.split('.')[-1]
    label = filename[len(rna)+1:]
    supplier = check_supplier(filepath)

    # Prepare new item record metadata
    record = make_item_record(rna, frame_work)

    # Append some file-specific field-values
    record.append({'title.language': 'English'})
    record.append({'title.type': '10_ARCHIVE'})
    record.append({'title': label})
    record.append({'digital.acquired_filename': in_file})
    record.append({'digital.acquired_filename.type': 'FILE'})
    print(record)

    # Read the duration
    d = get_duration(filepath)
    seconds = int(d) // 1000
    hhmmss = time.strftime('%H:%M:%S', time.gmtime(seconds))
    record.append({'video_duration': hhmmss})

    # Read the filesize and convert bytes to MB
    size = os.path.getsize(filepath)
    if isinstance(size, int):
        size = size / (1024 * 1024.0)
        record.append({'filesize': str(round(size, 1))})
        record.append({'filesize.unit': 'MB (Megabyte)'})

    search = f'digital.acquired_filename={in_file}'
    hit_count = adlib.retrieve_record(CID_API, 'items', search, '-1')[0]
    print(f"Number of CID records with filename {in_file} already = {hit_count}")
    logger.info("Number of CID records with filename %s already = %s", in_file, hit_count)

    if hit_count < 1:
        dupe = False
    else:
        dupe = True

    dupe_error_destination = os.path.join(DUPE_ERRORS, f"{supplier}_{in_file}")

    if dupe is True:
        shutil.move(filepath, dupe_error_destination)
        print(f'Dupe file {in_file} moved to error folder')
        logger.info('Dupe file %s moved to error folder', in_file)

    if dupe is False:
        # Create CID record
        record_xml = adlib.create_record_data('', record)
        result = adlib.post(CID_API, record_xml, 'items', 'insertrecord')
        if not result:
            logger.warning("Record creation failed with XML:\n%s", record_xml)
            sys.exit('CID item record creation failed')
        try:
            object_number = adlib.retrieve_field_name(result, 'object_number')[0]
        except Exception as err:
            object_number = None
            logger.warning(err)

        if not object_number:
            logger.warning("CID record creation failed, script exiting.")
            sys.exit("CID object number couldn't be extracted from new record")

        # Rename source
        ob_num_name = object_number.replace('-', '_')
        output_file = f'{ob_num_name}_01of01.{ext}'

        if not destination:
            # No destination path given, so rename in place
            dst = os.path.join(os.path.dirname(filepath), output_file)
        else:
            dst = os.path.join(destination, output_file)
        try:
            shutil.move(filepath, dst)
            logger.info('File successfully moved from %s to %s', filepath, dst)
        except Exception as err:
            logger.warning("File move failed: %s to %s", filepath, dst)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('file')
    parser.add_argument('-d', '--destination', type=str, help='folder path for renamed files to be moved to')
    args = parser.parse_args()

    path = os.path.abspath(args.file)
    directory = os.path.dirname(path)
    frame_work = os.path.split(directory)[-1]

    # Ignore partially uploaded files
    if 'partial' in args.file:
        sys.exit()

    # Document and rename
    try:
        main(path, frame_work, args.destination)
    except Exception as err:
        print(f'Encountered an error processing file {args.file}:\n{err}')
        logger.warning('Encountered an error processing file %s:\n%s', args.file, err)
