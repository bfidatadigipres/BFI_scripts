#!/usr/bin/env python3

'''
Special Collections Digital Derivative script
Creation of Analogue and Digital Item records

Script stages:
1. Searches STORAGE folder collecting list of 'works' folders
2. Iterates these works folders completing following steps:
    a. Extracts work data from folder name
    b. Gets list of image files within 'works' folder
    c. Iterates image files, skips if not accepted image extension
    d. For every image creates a CID Analogue image record,
       extracts priref/object number and links to work via related_object.reference
    e. Uses CID analogue object number to create CID Digital item record,
       linked to analogue record via source_item, and to work via related_object.reference
    f. Uses CID Digital item record object_number to rename the image file
    g. Moves renamed file to local autoingest path
    h. If any CID record creation fails the file is skipped and left in place
3. Checks if the 'works' folder is empty, and if so deletes empty folder
4. Continues iteration until all 'works' have been processed.

Notes: 
Waiting for metadata updates from SC teams for digital files
Uses requests.Sessions() for creation of works
within on session. Trial of sessions().

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
import adlib_v3_sess as adlib
import utils

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


def cid_retrieve(fname, session):
    '''
    Receive filename and search in CID works dB
    Return selected data to main()
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

    record = adlib.retrieve_record(CID_API, 'works', search, '0', session, fields)[1]
    utils.logger(LOG, 'info', f"cid_retrieve(): Making CID query request with:\n {search}")
    if not record:
        print(f"cid_retrieve(): Unable to retrieve data for {fname}")
        utils.logger(LOG, 'exception', f"cid_retrieve(): Unable to retrieve data for {fname}")
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
    Iterate folders in STORAGE, find image files in folders
    named after work and create analogue/digital item records
    for every photo. Clean up empty folders.
    '''
    if not utils.cid_check(CID_API):
        sys.exit("* Cannot establish CID session, exiting script")

    utils.logger(LOG, 'info', "=========== Special Collections rename - Digital Derivatives START ============")
    work_directories = [ x for x in os.listdir(STORAGE) if os.path.isdir(os.path.join(STORAGE, x)) ]
    session = adlib.create_session()
    for work in work_directories:
        if not utils.check_control('pause_scripts'):
            sys.exit("Script run prevented by downtime_control.json. Script exiting.")
        wpath = os.path.join(STORAGE, work)
        utils.logger(LOG, 'info', f"Work folder found: {work}")
        work_data = cid_retrieve(work.strip(), session)
        if work_data is None:
            utils.logger(LOG, 'warning', f"Please check folder name {work} as no CID match found")
            continue

        # Build file list of wpath contents
        images = [ x for x in os.listdir(wpath) if os.path.isfile(os.path.join(wpath, x)) ]
        sorted_images = sorted(images)
        for image in sorted_images:
            if not image.endswith(('.tiff', '.tif', '.TIFF', '.TIF', '.jpeg', '.jpg', '.JPEG', '.JPG')):
                utils.logger(LOG, 'warning', f"Skipping: File found in folder {work} that is not image file: {image}")
                continue
            utils.logger(LOG, 'info', f"Processing image file: {image}")
            ipath = os.path.join(wpath, image)

            # Analogue and Digital Derivative records to be made
            record_analogue = build_defaults(work_data, ipath, image, 'Analogue')[0]
            analogue_priref, analogue_obj = create_new_image_record(record_analogue, session)
            utils.logger(LOG, 'info', f"* New Item record created for image {image} Analogue {analogue_priref}")

            record_digital, metadata = build_defaults(work, ipath, image, 'Digital', analogue_obj)
            digi_priref, digi_obj = create_new_image_record(record_digital, session)
            utils.logger(LOG, 'info', f"* New Item record created for image {image} Digital Derivative {digi_priref}")

            if len(analogue_priref) == 0 or len(digi_priref) == 0:
                utils.logger(LOG, 'warning', f"Missing priref following record creation for {image}. Analogue priref {analogue_priref} / Digital priref {digi_priref}")
                utils.logger(LOG, 'warning', f"Moving file to failure folder. Manual clean up of records required.")
                move(ipath, 'fail')
                continue

            if len(digi_obj) > 0:
                # Append metadata to header tags
                if len(metadata) > 0:
                    header = f"<Header_tags><header_tags.parser>Exiftool</header_tags.parser><header_tags><![CDATA[{metadata}]]></header_tags></Header_tags>"
                    success = write_payload(digi_priref, header, session)
                    if not success:
                        utils.logger(LOG, 'warning', "Payload data was not written to CID record: {priref}\n{metadata}")

                utils.logger(LOG, 'info', f"** Renumbering file {image} with object number {digi_obj}")
                new_filepath, new_file = rename(ipath, digi_obj)
                if os.path.exists(new_filepath):
                    utils.logger(LOG, 'info', f"New filename generated: {new_file}")
                    utils.logger(LOG, 'info', f"File renumbered and filepath updated to: {new_filepath}")
                    success = move(new_filepath, 'ingest')
                    if success:
                        utils.logger(LOG, 'info', f"File {new_file} relocated to Autoingest {DATE_TIME}")
                    else:
                        utils.logger(LOG, 'warning', f"FILE {new_file} DID NOT MOVE SUCCESSFULLY TO AUTOINGEST")
                else:
                    utils.logger(LOG, 'warning', f"Problem creating new number for {image}")
            else:
                utils.logger(LOG, 'warning', "Object number was not returned following creation of CID Item record for digital derivative.")
                continue

        # Checking all processed and delete empty folder
        folder_empty = os.listdir(wpath)
        if len(folder_empty) == 0:
            utils.logger(LOG, 'info', f"All files in folder processed. Deleting folder: {work}")
            os.rmdir(wpath)
        else:
            utils.logger(LOG, 'warning', f"Not all items in folder processed, leaving folder in place for repeat attempt.")
            continue

    utils.logger(LOG, 'info', "=========== Special Collections rename - Digital Derivatives END ==============")


def build_defaults(work_data, ipath, image, arg, obj=None):
    '''
    Build up item record defaults
    '''
    metadata = None
    records = [{
        'institution.name.lref': '999570701',
        'object_type': 'Single object',
        'description_level_object': 'Stills',
        'object_category': 'Photograph: Production',
    }]

    if len(work_data[1]) > 0:
        records.extend({'related_object.reference': work_data[1]})
    else:
        utils.logger(LOG, 'warning', "No parent object number retrieved. Script exiting.")
        return None
    if len(work_data[2]) > 0:
        records.extend({'title': work_data[2]})
    else:
        utils.logger(LOG, 'warning', "No title data retrieved. Script exiting.")
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
        bitdepth = utils.get_metadata('Image', 'BitDepth', ipath)
        if len(bitdepth) > 0:
            records.extend({'bit_depth': bitdepth})

        metadata_rec, metadata = get_exifdata(ipath)
        if metadata_rec:
            records.append(metadata_rec)

    return records, metadata


def get_exifdata(dpath):
    '''
    Attempt to get metadata for record build
    Example dict below, waiting for confirmation

    born_digital_fields = {
        'File Size': '',
        'File Type': '',
        'MIME Type': '',
        'Image Width': 'dimension.type',
        'Image Height': 'dimension.type',
        'Bits Per Sample': '',
        'Compression': '',
        'Photometric Interpretation': '',
        'Strip Offsets': '',
        'Samples Per Pixel': '',
        'Rows Per Strip': '',
        'Strip Byte Counts': '',
        'Planar Configuration': '',
        'Software': '',
        'Modify Date': '',
        'Extra Samples': '',
    }

    data_list = data.split('\n')

    metadata = {}
    for key, value in born_digital_fields.items():
        for row in data_list:
            if row.startswith(key):
                field = row.split(': ', 1)[1]
                metadata[value] = field

    if len(metadata) > 0:
        return metadata, data
    return None, data
    '''
    data = utils.exif_data(dpath)
    return None, data


def create_new_image_record(record_json, session):
    '''
    Function for creation of new CID records
    both Analogue and Digital, returning priref/obj
    '''
    record_xml = adlib.create_record_data('', record_json)
    print(record_xml)
    record = adlib.post(CID_API, record_xml, 'internalobject', 'insertrecord', session)
    if not record:
        utils.logger(LOG, 'warning', f"Adlib POST failed to create CID item record for data:\n{record_xml}")
        return None
    
    priref = adlib.retrieve_field_name(record, 'priref')[0]
    obj = adlib.retrieve_field_name(record, 'object_number')[0]
    return priref, obj
                

def write_payload(priref, payload_header, session):
    '''
    Payload formatting per mediainfo output
    '''
    payload_head = f"<adlibXML><recordList><record priref='{priref}'>"
    payload_end = "</record></recordList></adlibXML>"
    payload = payload_head + payload_header + payload_end

    record = adlib.post(CID_API, payload, 'internalobject', 'updaterecord', session)
    if record is None:
        return False
    elif 'error' in str(record):
        return False
    else:
        return True


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
        utils.logger(LOG, 'warning', f"There was an error renaming {filename} to {new_filename}")

    return (new_filepath, new_filename)


def move(filepath, arg):
    '''
    Move existing filepaths to Autoingest
    '''
    if os.path.exists(filepath) and 'fail' in arg:
        pth = os.path.split(filepath)[0]
        failures = os.path.join(pth, 'failures/')
        os.makedirs(failures, mode=0o777, exist_ok=True)
        print(f"move(): Moving {filepath} to {failures}")
        try:
            shutil.move(filepath, failures)
            return True
        except Exception as err:
            utils.logger(LOG, 'warning', f"Error trying to move file {filepath} to {failures}. Error: {err}")
            return False
    elif os.path.exists(filepath) and 'ingest' in arg:
        print(f"move(): Moving {filepath} to {AUTOINGEST}")
        try:
            shutil.move(filepath, AUTOINGEST)
            return True
        except Exception:
            utils.logger(LOG, 'warning', f"Error trying to move file {filepath} to {AUTOINGEST}")
            return False
    else:
        return False


if __name__ == '__main__':
    main()
