#!/usr/bin/env python3

'''
Special Collections Digital Derivative script
Creation of Analogue and Digital Item records

WIP 

Joanna White
2024
'''

# Public packages
import os
import sys
import shutil
import datetime


# Private packages
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib
from utils import logger, check_control, cid_check, get_metadata

# Global path variables
SCPATH = os.environ['SPECIAL_COLLECTIONS']
STORAGE = os.path.join(SCPATH, 'Uncatalogued_stills_digital_derivative/')
AUTOINGEST = os.path.join(SCPATH, os.environ['INGEST_SC'])
LOG = os.path.join(os.environ['LOG_PATH'], 'special_collections_rename_digital_derivatives.log')
CID_API = os.environ['CID_API4']

# Global variables
TODAY = str(datetime.datetime.now())
TODAY_DATE = TODAY[:10]
TODAY_TIME = TODAY[11:19]
DATE_TIME = (f"{TODAY_DATE} = {TODAY_TIME}")


def cid_retrieve(fname):
    '''
    Receive filename and search in CID items
    Return object number to main
    '''
    search = f'object_number="{fname}"'
    fields = [
        'priref',
        'object_number',
        'title_date_start',
        'title_date.type',
        'title',
        'title.article'
    ]

    record = adlib.retrieve_record(CID_API, 'works', search, '0', fields)[1]
    logger(LOG, 'info', f"cid_retrieve(): Making CID query request with:\n {search}")
    if not record:
        print(f"cid_retrieve(): Unable to retrieve data for {fname}")
        logger(LOG, 'exception', f"cid_retrieve(): Unable to retrieve data for {fname}")
        return None

    if 'priref' in str(record):
        priref = adlib.retrieve_field_name(record[0], 'priref')[0]
    else:
        priref = ""
    if 'object_number' in str(record):
        ob_num = adlib.retrieve_field_name(record[0], 'object_number')[0]
    else:
        ob_num = ""
    if 'Title' in str(record):
        title = adlib.retrieve_field_name(record[0], 'title')[0]
    else:
        title = ""
    if 'title.article' in str(record):
        title_article = adlib.retrieve_field_name(record[0], 'title.article')[0]
    else:
        title_article = ""
    if 'title_date_start' in str(record):
        title_date_start = adlib.retrieve_field_name(record[0], 'title_date_start')
    else:
        title_date_start = []
    if 'title_date.type' in str(record):
        title_date_type = adlib.retrieve_field_name(record[0], 'title_date.type')
    else:
        title_date_type = []

    tds = sort_date_types(title_date_start, title_date_type)
    return priref, ob_num, title, title_article, tds


def sort_date_types(title_date_start, title_date_type):
    '''
    Make sure only 'copyright' pair returned
    '''
    if len(title_date_start) != len(title_date_type):
        return None
    if 'Copyright' not in str(title_date_type):
        return None

    idx = title_date_start.index('Copyright')

    if isinstance(idx, int):
        return title_date_start[idx]
    return None


def main():
    '''
    search in CID Item for digital.acquired_filename
    Retrieve object number and use to build new filename for YACF file
    Update local log for YACF monitoring
    Move file to autoingest path
    '''
    if check_control('pause_scripts'):
        sys.exit("Script run prevented by downtime_control.json. Script exiting.")
    if cid_check(CID_API):
        sys.exit("* Cannot establish CID session, exiting script")

    logger(LOG, 'info', "=========== Special Collections rename - Digital Derivatives START ============")
    work_directories = [ x for x in os.listdir(STORAGE) if os.path.isdir(os.path.join(STORAGE, x)) ]
    for work in work_directories:
        wpath = os.path.join(STORAGE, work)
        logger(LOG, 'info', f"Work folder found: {work}")
        work_data = cid_retrieve(work.strip())
        if work_data is None:
            logger(LOG, 'warning', f"Please check folder name {work} as no CID match found")
            continue

        # Build file list of wpath contents
        images = [ x for x in os.listdir(wpath) if os.path.isfile(os.path.join(wpath, x)) ]
        sorted_images = sorted(images)
        for image in sorted_images:
            if not image.endswith(('.tiff', '.tif', '.TIFF', '.TIF', '.jpeg', '.jpg', '.JPEG', '.JPG')):
                logger(LOG, 'warning', f"Skipping: File found in folder {work} that is not image file: {image}")
                continue
            logger(LOG, 'info', f"Processing image file: {image}")
            ipath = os.path.join(wpath, image)

            # Analogue and Digital Derivative records to be made
            record_analogue = build_defaults(work_data, ipath, image, 'Analogue')
            analogue_priref, analogue_obj = create_new_image_record(record_analogue)
            logger(LOG, 'info', f"* New Item record created for image {image} Analogue {analogue_priref}")

            record_digital = build_defaults(work, ipath, image, 'Digital', analogue_obj)
            digi_priref, digi_obj = create_new_image_record(record_digital)
            logger(LOG, 'info', f"* New Item record created for image {image} Digital Derivative {digi_priref}")

            if len(digi_obj) > 0:
                logger(LOG, 'info', f"** Renumbering file {image} with object number {digi_obj}")
                new_filepath, new_file = rename(ipath, digi_obj)
                if os.path.exists(new_filepath):
                    logger(LOG, 'info', f"New filename generated: {new_file}")
                    logger(LOG, 'info', f"File renumbered and filepath updated to: {new_filepath}")
                    success = move(new_filepath, 'ingest')
                    if success:
                        logger(LOG, 'info', f"File {new_file} relocated to Autoingest {DATE_TIME}")
                    else:
                        logger(LOG, 'warning', f"FILE {new_file} DID NOT MOVE SUCCESSFULLY TO AUTOINGEST")
                else:
                    logger(LOG, 'warning', f"Problem creating new number for {image}")
            else:
                logger(LOG, 'warning', "Object number was not returned following creation of CID Item record for digital derivative.")
                logger(LOG, 'warning', "File was not renamed and will be left for manual intervention")
                continue

        # UP TO HERE JO

    logger(LOG, 'info', "=========== Special Collections rename - Digital Derivatives END ==============")


def build_defaults(work_data, ipath, image, arg, obj=None):
    '''
    Build up item record defaults
    '''
    records = [{
        'institution.name.lref': '999570701',
        'object_type': 'Single object',
        'description_level_object': 'Stills',
        'object_category': 'Photograph: Production',
    }]

    if len(work_data[1]) > 0:
        records.extend({'related_object.reference': work_data[1]})
    else:
        logger(LOG, 'warning', "No parent object number retrieved. Script exiting.")
        return None
    if len(work_data[2]) > 0:
        records.extend({'title': work_data[2]})
    else:
        logger(LOG, 'warning', "No title data retrieved. Script exiting.")
        return None
    if len(work_data[3]) > 0:
        records.extend({'title.article': work_data[3]})
    if len(work_data[4]) > 0:
        records.extend({'production.date.start': work_data[4]})

    if arg == 'analogue':
        records.extend({'analogue_or_digital': 'Analogue'})
    elif arg == 'digital':
        records.extend({'analogue_or_digital': 'Digital'})
        records.extend({'digital.born_or_derived': 'Digital derivative: Preservation'})
        records.extend({'digital.acquired_filename': image})
        if obj:
            records.extend({'source_item': obj})
        ext = image.split('.')[-1]
        if len(ext) > 0:
            records.extend({'file_type': ext.upper()})
        bitdepth = get_metadata('Image', 'BitDepth', ipath)
        if len(bitdepth) > 0:
            records.extend({'bit_depth': bitdepth})

    return records


def create_new_image_record(record_json):
    '''
    Function for creation of new CID records
    both Analogue and Digital, returning priref/obj
    '''
    record_xml = adlib.create_record_data('', record_json)
    print(record_xml)
    record = adlib.post(CID_API, record_xml, 'items', 'insertrecord')
    if not record:
        logger(LOG, 'warning', f"Adlib POST failed to create CID item record for data:\n{record_xml}")
        return None
    
    priref = adlib.retrieve_field_name(record, 'priref')[0]
    obj = adlib.retrieve_field_name(record, 'object_number')[0]
    return priref, obj
                

def rename(filepath, ob_num):
    '''
    Receive original file path and rename filename
    based on object number, return new filepath, filename
    '''
    new_filepath, new_filename = '', ''
    ipath, filename = os.path.split(filepath)
    ext = os.path.splitext(filename)[1]
    new_name = ob_num.replace('-', '_')
    new_filename = f"{new_name}_01of01{ext}"
    print(f"Renaming {filename} to {new_filename}")
    new_filepath = os.path.join(ipath, new_filename)

    try:
        os.rename(filepath, new_filepath)
    except OSError:
        logger(LOG, 'warning', f"There was an error renaming {filename} to {new_filename}")

    return (new_filepath, new_filename)


def move(filepath, arg):
    '''
    Move existing filepaths to Autoingest
    '''
    if os.path.exists(filepath) and 'fail' in arg:
        print(f"move(): Moving {filepath} to {YACF_NO_CID}")
        try:
            shutil.move(filepath, YACF_NO_CID)
            return True
        except Exception as err:
            logger(LOG, 'warning', f"Error trying to move file {filepath} to {YACF_NO_CID}. Error: {err}")
            return False
    elif os.path.exists(filepath) and 'ingest' in arg:
        print(f"move(): Moving {filepath} to {AUTOINGEST}")
        try:
            shutil.move(filepath, AUTOINGEST)
            return True
        except Exception:
            logger(LOG, 'warning', f"Error trying to move file {filepath} to {AUTOINGEST}")
            return False
    else:
        return False


if __name__ == '__main__':
    main()
