#!/usr/bin/env LANG=en_UK.UTF-8 /usr/local/bin/python3

'''
Special Collections Born Digital script
For creation of born digital CID Item records

NOTE: Copy of digital derivative script WIP

Joanna White
2024
'''

# Public packages
import os
import sys
import json
import shutil
import logging
import datetime
import subprocess

# Private packages
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib

# Global path variables
SCPATH = os.environ['SPECIAL_COLLECTIONS']
STORAGE = os.path.join(SCPATH, 'Uncatalogued_stills_digital_derivative/')
AUTOINGEST = os.path.join(SCPATH, os.environ['INGEST_SC'])
LOG_PATH = os.environ['LOG_PATH']
CONTROL_JSON = os.path.join(LOG_PATH, 'downtime_control.json')
CID_API = os.environ['CID_API3']

# Setup logging
LOGGER = logging.getLogger('special_collections_rename_digital_derivatives')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'special_collections_rename_digital_derivatives.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

# Global variables
TODAY = str(datetime.datetime.now())
TODAY_DATE = TODAY[:10]
TODAY_TIME = TODAY[11:19]
DATE_TIME = (f"{TODAY_DATE} = {TODAY_TIME}")


def check_control():
    '''
    Check control json for downtime requests
    '''
    with open(CONTROL_JSON) as control:
        j = json.load(control)
        if not j['pause_scripts']:
            LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
            sys.exit('Script run prevented by downtime_control.json. Script exiting.')


def cid_check():
    '''
    Tests if CID active before all other operations commence
    '''
    try:
        adlib.check(CID_API)
    except KeyError:
        print("* Cannot establish CID session, exiting script")
        LOGGER.critical("* Cannot establish CID session, exiting script")
        sys.exit()


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
    LOGGER.info("cid_retrieve(): Making CID query request with:\n %s", search)
    if not record:
        print(f"cid_retrieve(): Unable to retrieve data for {fname}")
        LOGGER.exception("cid_retrieve(): Unable to retrieve data for %s", fname)
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
    LOGGER.info("=========== SC rename digital derivatives script start ==========")
    check_control()
    cid_check()

    work_directories = [ x for x in os.listdir(STORAGE) if os.path.isdir(os.path.join(STORAGE, x)) ]
    for work in work_directories:
        wpath = os.path.join(STORAGE, work)
        LOGGER.info("Work folder found: %s", work)
        work_data = cid_retrieve(work.strip())
        if work_data is None:
            LOGGER.warning(f"Please check folder name <%s> as no CID match found", work)
            continue

        # Build file list of wpath contents
        images = [ x for x in os.listdir(wpath) if os.path.isfile(os.path.join(wpath, x)) ]
        sorted_images = sorted(images)
        for image in sorted_images:
            if not image.endswith(('.tiff', '.tif', '.TIFF', '.TIF', '.jpeg', '.jpg', '.JPEG', '.JPG')):
                LOGGER.warning("Skipping: File found in folder <%s> that is not image file: %s", work, image)
                continue
            LOGGER.info("Processing image file: %s", image)
            ipath = os.path.join(wpath, image)

            # Analogue and Digital Derivative records to be made
            record_analogue = build_defaults(work_data, ipath, image, 'Analogue')
            analogue_priref, analogue_obj = create_new_image_record(record_analogue)
            LOGGER.info("* New Item record created for image <%s> Analogue %s", image, analogue_priref)

            record_digital = build_defaults(work, ipath, image, 'Digital', analogue_obj)
            digi_priref, digi_obj = create_new_image_record(record_digital)
            LOGGER.info("* New Item record created for image <%s> Digital Derivative %s", image, digi_priref)

            if len(digi_obj) > 0:
                LOGGER.info("** Renumbering file %s with object number %s", image, digi_obj)
                new_filepath, new_file = rename(ipath, digi_obj)
                if os.path.exists(new_filepath):
                    LOGGER.info(f"New filename generated: {new_file}")
                    LOGGER.info(f"File renumbered and filepath updated to: {new_filepath}")
                    success = move(new_filepath, 'ingest')
                    if success:
                        LOGGER.info("File %s relocated to Autoingest %s", new_file, DATE_TIME)
                    else:
                        LOGGER.warning("FILE %s DID NOT MOVE SUCCESSFULLY TO AUTOINGEST", new_file)
                else:
                    LOGGER.warning("Problem creating new number for %s", image)
            else:
                LOGGER.warning("Object number was not returned following creation of CID Item record for digital derivative.")
                LOGGER.warning("File was not renamed and will be left for manual intervention")
                continue

        # UP TO HERE JO


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
        LOGGER.warning("No parent object number retrieved. Script exiting.")
        return None
    if len(work_data[2]) > 0:
        records.extend({'title': work_data[2]})
    else:
        LOGGER.warning("No title data retrieved. Script exiting.")
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
        bitdepth = get_bitdepth(ipath)
        if len(bitdepth) > 0:
            records.extend({'bit_depth': bitdepth})

    return records


def get_bitdepth(ipath):
    '''
    Use MediaInfo to retrieve bitdepth of image
    '''
    cmd = [
        'mediainfo',
        '--Language=raw',
        '--Output=Image;%BitDepth%',
        ipath
    ]

    bitdepth = subprocess.check_output(cmd)
    bitdepth = bitdepth.decode('utf-8')
    return bitdepth


def create_new_image_record(record_json):
    '''
    Function for creation of new CID records
    both Analogue and Digital, returning priref/obj
    '''
    record_xml = adlib.create_record_data('', record_json)
    print(record_xml)
    record = adlib.post(CID_API, record_xml, 'items', 'insertrecord')
    if not record:
        LOGGER.warning("Adlib POST failed to create CID item record for data:\n%s", record_xml)
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
        LOGGER.warning("There was an error renaming %s to %s", filename, new_filename)

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
            LOGGER.warning("Error trying to move file %s to %s. Error: %s", filepath, YACF_NO_CID, err)
            return False
    elif os.path.exists(filepath) and 'ingest' in arg:
        print(f"move(): Moving {filepath} to {AUTOINGEST}")
        try:
            shutil.move(filepath, AUTOINGEST)
            return True
        except Exception:
            LOGGER.warning("Error trying to move file %s to %s", filepath, AUTOINGEST)
            return False
    else:
        return False


if __name__ == '__main__':
    main()
