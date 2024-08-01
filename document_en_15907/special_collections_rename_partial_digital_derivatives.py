#!/usr/bin/env python3

'''
Special Collections Digital Derivative script
Creation of Digital Item records only linked
to existing Analogue Items.

Script stages:
1. Searches STORAGE folder collecting list of 'analogue' item folders
2. Iterates these folders completing following steps:
    a. Extracts work/item data from folder name
    b. Gets list of image files within folder
    c. Iterates image files, skips if not accepted image extension
    d. Uses CID analogue object number to create CID Digital item record,
       linked to analogue record via source_item, and to work via related_object.reference
    e. Uses CID Digital item record object number to rename the image file
    f. Moves renamed file to local autoingest path
    g. If any CID record creation fails the file is skipped and left in place
3. Checks if the 'works' folder is empty, and if so deletes empty folder
4. Continues iteration until all 'works' have been processed.

Notes:  
Waiting for metadata updates from SC teams for digital files.
Uses requests.Sessions() for creation of works
within on session. Trial of sessions().

Joanna White
2024
'''

# Public packages
import os
import sys
import shutil
import logging
import datetime

# Private packages
sys.path.append(os.environ['CODE'])
import adlib_v3_sess as adlib
import utils

# Global path variables
SCPATH = os.environ['SPECIAL_COLLECTIONS']
STORAGE = os.path.join(SCPATH, 'Part_catalogued_stills_digital_derivative/')
AUTOINGEST = os.path.join(SCPATH, os.environ['INGEST_SC'])
LOG = os.path.join(os.environ['LOG_PATH'], 'special_collections_rename_partial_digital_derivatives.log')
MEDIAINFO_PATH = os.path.join(os.environ['LOG_PATH'], 'cid_mediainfo/')
CID_API = os.environ['CID_API4']

# Global variables
TODAY = str(datetime.datetime.now())
TODAY_DATE = TODAY[:10]
TODAY_TIME = TODAY[11:19]
DATE_TIME = (f"{TODAY_DATE} = {TODAY_TIME}")

LOGGER = logging.getLogger('sc_rename_partial_partial_digital_derivatives')
HDLR = logging.FileHandler(LOG)
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

BIT_DEPTHS = {
    '8': '401572',
    '10': '99796',
    '12': '392421',
    '16': '99797',
    '24': '395618',
    '32': '99838',
    '48': '95709'
}


def cid_retrieve(fname, session):
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
        'related_object.reference'
    ]

    record = adlib.retrieve_record(CID_API, 'internalobject', search, '0', session, fields)[1]
    LOGGER.info(f"cid_retrieve(): Making CID query request with:\n {search}")
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
    if 'related_object.reference' in str(record):
        related_obj = adlib.retrieve_field_name(record[0], 'related_object.reference')[0]
    else:
        related_obj = ""   
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
    return priref, ob_num, related_obj, title, title_article, tds


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
    named after analogue record
    '''
    if not utils.cid_check(CID_API):
        sys.exit("* Cannot establish CID session, exiting script")

    LOGGER.info("=========== Special Collections rename - Digital Derivatives START ============")
    rec_directories = [ x for x in os.listdir(STORAGE) if os.path.isdir(os.path.join(STORAGE, x)) ]
    session = adlib.create_session()
    for rec in rec_directories:
        if not utils.check_control('pause_scripts'):
            sys.exit("Script run prevented by downtime_control.json. Script exiting.")
        rpath = os.path.join(STORAGE, rec)
        LOGGER.info(f"Analogue record folder found: {rec}")
        record_data = cid_retrieve(rec.strip(), session)
        if record_data is None:
            LOGGER.warning(f"Please check folder name {rec} as no CID match found")
            continue

        # Build file list of wpath contents
        images = [ x for x in os.listdir(rpath) if os.path.isfile(os.path.join(rpath, x)) ]
        sorted_images = sorted(images)
        for image in sorted_images:
            if not image.endswith(('.tiff', '.tif', '.TIFF', '.TIF', '.jpeg', '.jpg', '.JPEG', '.JPG')):
                LOGGER.warning(f"Skipping: File found in folder {rec} that is not image file: {image}")
                continue
            LOGGER.info(f"Processing image file: {image}")
            ipath = os.path.join(rpath, image)

            if bool(utils.check_filename(image)):
                LOGGER.warning(f"Skipping: File passed filename checks and likely already renumbered: {image}")
                ob_num = utils.get_object_number(image)
                if not ob_num:
                    continue
                rec = adlib.retrieve_record(CID_API, 'internalobject', f'object_number="{ob_num}"', '1', session, ['digital.born_or_derived'])
                check = adlib.retrieve_field_name(rec[0], 'digital.born_or_derived')[0]
                if 'DIGITAL_DERIVATIVE_PRES' in check:
                    LOGGER.info(f'Moving to autoingest. File renumbered to matching Digital record: {ob_num}')
                    move(ipath, 'ingest')
                continue

            # Digital Derivative record to be made
            record_digital, metadata = build_defaults(record_data, ipath, image)
            digi_priref, digi_obj = create_new_image_record(record_digital, session)
            LOGGER.info(f"* New Item record created for image {image} Digital Derivative {digi_priref}")

            if len(digi_priref) == 0:
                LOGGER.warning(f"Missing Digital Derivative priref following record creation for {image}.")
                LOGGER.warning(f"Moving file to failure folder. Manual clean up of records required.")
                move(ipath, 'fail')
                continue

            if len(digi_obj) > 0:
                # Append metadata to header tags
                if len(metadata) > 0:
                    header = f"<Header_tags><header_tags.parser>Exiftool</header_tags.parser><header_tags><![CDATA[{metadata}]]></header_tags></Header_tags>"
                    success = write_payload(digi_priref, header, session)
                    if not success:
                        LOGGER.warning("Payload data was not written to CID record: {priref}\n{metadata}")

                LOGGER.info(f"** Renumbering file {image} with object number {digi_obj}")
                new_filepath, new_file = rename(ipath, digi_obj)
                if os.path.exists(new_filepath):
                    LOGGER.info(f"New filename generated: {new_file}")
                    LOGGER.info(f"File renumbered and filepath updated to: {new_filepath}")
                    success = move(new_filepath, 'ingest')
                    if success:
                        LOGGER.info(f"File {new_file} relocated to Autoingest {DATE_TIME}")
                    else:
                        LOGGER.warning(f"FILE {new_file} DID NOT MOVE SUCCESSFULLY TO AUTOINGEST")
                else:
                    LOGGER.warning(f"Problem creating new number for {image}")
                success = write_exif_to_file(image, metadata)
                if not success:
                    LOGGER.warning(f"Unable to create EXIF metadata file for image: {image}\n{metadata}")
            else:
                LOGGER.warning("Object number was not returned following creation of CID Item record for digital derivative.")
                continue

        # Checking all processed and delete empty folder
        folder_empty = os.listdir(rpath)
        if len(folder_empty) == 0:
            LOGGER.info(f"All files in folder processed. Deleting folder: {rec}")
            os.rmdir(rpath)
        else:
            LOGGER.warning(f"Not all items in folder processed, leaving folder in place for repeat attempt.")
            continue

    LOGGER.info("=========== Special Collections rename - Digital Derivatives END ==============")


def build_defaults(data, ipath, image):
    '''
    Build up item record defaults
    '''
    metadata = None
    records = ([
        {'institution.name.lref': '999570701'},
        {'object_type': 'OBJECT'},
        {'description_level_object': 'STILLS'},
        {'object_category.lref': '132885'}
    ])
    print(data)

    if data[2]:
        records.append({'related_object.reference': data[2]})
    else:
        LOGGER.warning("No parent object number retrieved. Script exiting.")
        return None
    if data[0]:
        records.append({'source_item.lref': data[0]})
    else:
        LOGGER.warning("No source_item object number retrieved. Script exiting.")
        return None
    if data[3]:
        records.append({'title': data[3]})
    else:
        LOGGER.warning("No title data retrieved. Script exiting.")
        return None
    if data[4]:
        records.append({'title.article': data[4]})
    if data[5]:
        records.append({'production.date.start': data[5]})

    records.append({'analogue_or_digital': 'DIGITAL'})
    records.append({'digital.born_or_derived': 'DIGITAL_DERIVATIVE_PRES'})
    records.append({'digital.acquired_filename': image})
    ext = image.split('.')[-1]
    if ext.lower() in ['jpeg', 'jpg']:
        records.append({'file_type.lref': '396310'})
    elif ext.lower() in ['tif', 'tiff']:
        records.append({'file_type.lref': '395395'})
    bitdepth = utils.get_metadata('Image', 'BitDepth', ipath)
    if bitdepth:
        for key, val in BIT_DEPTHS.items():
            if bitdepth == key:
                records.append({'bit_depth.lref': val})

    metadata_rec, metadata = get_exifdata(ipath)
    if metadata_rec:
        records.extend(metadata_rec)

    records.append({'input.name': 'datadigipres'})
    records.append({'input.date': str(datetime.datetime.now())[:10]})
    records.append({'input.time': str(datetime.datetime.now())[11:19]})
    records.append({'input.notes': 'Automated record creation for Special Collections, to facilitate ingest to DPI'})

    return records, metadata


def get_exifdata(dpath):
    '''
    Attempt to get metadata for record build
    Example dict below, waiting for confirmation
    '''
    metadata = ([])
    creator_data = []
    rights_data = []
    data = utils.exif_data(dpath)
    if not data:
        return None, None
    data_list = data.split('\n')
    for d in data_list:
        if d.startswith('File Size '):
            val = d.split(': ', 1)[-1]
            metadata.append({'filesize': val.split(' ')[0]})
            metadata.append({'filesize.unit': val.split(' ')[-1]})
        elif d.startswith('Image Height '):
            metadata.append({'dimension.type': 'Height'})
            metadata.append({'dimension.value': d.split(': ', 1)[-1]})
            metadata.append({'dimension.unit': 'Pixels'})
        elif d.startswith('Image Width '):
            metadata.append({'dimension.type': 'Width'})
            metadata.append({'dimension.value': d.split(': ', 1)[-1]})
            metadata.append({'dimension.unit': 'Pixels'})
        elif d.startswith('Compression '):
            metadata.append({'code_type': d.split(': ', 1)[-1]})
        elif d.startswith('Color Space Data '):
            metadata.append({'colour_space': d.split(': ', 1)[-1]})
#        elif d.startswith('Camera Model Name '):
#            metadata.append({'source_device': d.split(': ', 1)[-1]})
        elif d.startswith('Description '):
            metadata.append({'description': d.split(': ', 1)[-1]})
            metadata.append({'description.name': 'Digital file metadata'})
        elif d.startswith('Create Date '):
            try:
                val = d.split(': ', 1)[-1].split(' ', 1)[0].replace(':', '-')
                metadata.append({'production.date.start': val})
            except (KeyError, IndexError):
                pass
        if d.startswith('Creator '):
            creator_data = d.split(': ', 1)[-1]
        elif d.startswith('Artist '):
            creator_data = d.split(': ', 1)[-1]
        elif d.startswith('By-line '):
            creator_data = d.split(': ', 1)[-1]
        if d.startswith('Rights '):
            rights_data = d.split(': ', 1)[-1]
        elif d.startswith('Copyright Notice '):
            rights_data = d.split(': ', 1)[-1]
        elif d.startswith('Copyright '):
            rights_data = d.split(': ', 1)[-1]

    if len(creator_data) > 0 and len(rights_data) > 0:
        creator_data.sort(key=len, reverse=True)
        rights_data.sort(key=len, reverse=True)
        metadata.append({'production.notes': f"Photographer: {creator_data[0]}, Rights: {rights_data[0]}"})
    elif len(creator_data) > 0:
         creator_data.sort(key=len, reverse=True)
         metadata.append({'production.notes': f"Photographer: {creator_data[0]}"})
    elif len(rights_data) > 0:
         rights_data.sort(key=len, reverse=True)
         metadata.append({'production.notes': f"Rights: {rights_data[0]}"})
    density = utils.get_metadata('Image', 'Density/String', dpath)
    if density:
        metadata.append({'dimension.free': density})

    if len(metadata) > 0:
        return metadata, data
    return None, data


def write_exif_to_file(image, metadata):
    '''
    Create newline output to text file
    '''

    meta_dump = os.path.join(MEDIAINFO_PATH, f"{image}_EXIF.txt")

    with open(meta_dump, 'a+') as file:
        file.write(metadata)
        file.close()
    
    if os.path.isfile(meta_dump):
        return meta_dump
    return None


def create_new_image_record(record_json, session):
    '''
    Function for creation of new CID records
    both Analogue and Digital, returning priref/obj
    '''
    record_xml = adlib.create_record_data('', record_json)
    print(record_xml)
    record = adlib.post(CID_API, record_xml, 'internalobject', 'insertrecord', session)
    if not record:
        LOGGER.warning(f"Adlib POST failed to create CID item record for data:\n{record_xml}")
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
        LOGGER.warning(f"There was an error renaming {filename} to {new_filename}")

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
            LOGGER.warning(f"Error trying to move file {filepath} to {failures}. Error: {err}")
            return False
    elif os.path.exists(filepath) and 'ingest' in arg:
        print(f"move(): Moving {filepath} to {AUTOINGEST}")
        try:
            shutil.move(filepath, AUTOINGEST)
            return True
        except Exception:
            LOGGER.warning(f"Error trying to move file {filepath} to {AUTOINGEST}")
            return False
    else:
        return False


if __name__ == '__main__':
    main()
