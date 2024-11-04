#!/usr/bin/env python3

'''
** THIS SCRIPT MUST RUN FROM SHELL LAUNCH SCRIPT RUNNING PARALLEL MULTIPLE JOBS **
Script to clean up Mediainfo files that belong to filepaths deleted by autoingest,
writes mediainfo data to CID media record priref before deleting files.

1. Receive filepath of '*_TEXT.txt' file from sys.argv
2. Extract filename, and create paths for all metadata possibilities
3. Check CID Media record exists with imagen.media.original_filename matching filename
   There is no danger deleting before asset in autoingest, as validation occurs
   before CID media record creation now.
4. Capture priref of CID Media record
5. Extract each metadata file and write in XML block to CID media record
   in header_tags and header_parser fields,
7. Where data written successfully delete mediainfo file
8. Where there's no match leave file and may be needed later

2023
Python3.8+
'''

# Global packages
import os
import sys
import json
import logging

# Local packages
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib

# Global variables
LOG_PATH = os.environ['LOG_PATH']
MEDIAINFO_PATH = os.path.join(LOG_PATH, 'cid_mediainfo')
CSV_PATH = os.path.join(LOG_PATH, 'persistence_queue_copy.csv')
CONTROL_JSON = os.environ['CONTROL_JSON']
CID_API = os.environ['CID_API4']

# Setup logging
LOGGER = logging.getLogger('metadata_clean_up')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'metadata_clean_up.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def check_control():
    '''
    Check control json for downtime requests
    '''
    with open(CONTROL_JSON) as control:
        j = json.load(control)
        if not j['pause_scripts']:
            LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
            sys.exit('Script run prevented by downtime_control.json. Script exiting.')


def check_cid():
    '''
    Test CID online before script progresses
    '''
    try:
        adlib.check(CID_API)
    except KeyError:
        print("* Cannot establish CID session, exiting script")
        LOGGER.critical("* Cannot establish CID session, exiting script")
        sys.exit()


def cid_retrieve(fname):
    '''
    Retrieve priref for media record from imagen.media.original_filename
    '''
    priref = ''
    search = f"imagen.media.original_filename='{fname}'"
    record = adlib.retrieve_record(CID_API, 'media', search, '0')[1]
    if not record:
        return ''
    if 'priref' in str(record):
        priref = adlib.retrieve_field_name(record[0], 'priref')[0]
        return priref
    return ''


def read_extract(metadata_pth):
    '''
    Open mediainfo text files from path, read() and store to variable
    '''
    with open(metadata_pth, 'r') as data:
        readme = data.read()
    return readme


def main():
    '''
    Clean up scripts for checksum files that have been processed by autoingest
    and also mediainfo reports, writing text file dumps to media record in CID
    '''
    check_cid()
    check_control()
    if len(sys.argv) < 2:
        sys.exit()

    text_path = sys.argv[1]
    text_file = os.path.basename(text_path)
    filename = text_file.split("_TEXT.txt")[0]
    # Make all possible paths
    text_full_path = os.path.join(MEDIAINFO_PATH, f"{filename}_TEXT_FULL.txt")
    ebu_path = os.path.join(MEDIAINFO_PATH, f"{filename}_EBUCore.txt")
    pb_path = os.path.join(MEDIAINFO_PATH, f"{filename}_PBCore2.txt")
    xml_path = os.path.join(MEDIAINFO_PATH, f"{filename}_XML.xml")
    json_path = os.path.join(MEDIAINFO_PATH, f"{filename}_JSON.json")
    exif_path = os.path.join(MEDIAINFO_PATH, f"{filename}_EXIF.txt")

    if len(filename) > 0 and filename.endswith((".ini", ".DS_Store", ".mhl", ".json")):
        sys.exit('Incorrect media file detected.')

    # Checking for existence of Digital Media record
    print(text_path, filename)
    priref = cid_retrieve(filename)
    if len(priref) == 0:
        sys.exit('Script exiting. Priref could not be retrieved.')

    print(f"Priref retrieved: {priref}. Writing metadata to record")
    print(text_path)

    text = text_full = ebu = pb = xml = json = exif = ''
    # Processing metadata output for text path
    if os.path.exists(text_path):
        text_dump = read_extract(text_path)
        text = f"<Header_tags><header_tags.parser>MediaInfo text 0</header_tags.parser><header_tags><![CDATA[{text_dump}]]></header_tags></Header_tags>"

    # Processing metadata output for text full path
    if os.path.exists(text_full_path):
        text_dump = read_extract(text_full_path)
        text_full = f"<Header_tags><header_tags.parser>MediaInfo text 0 full</header_tags.parser><header_tags><![CDATA[{text_dump}]]></header_tags></Header_tags>"

    # Processing metadata output for ebucore path
    if os.path.exists(ebu_path):
        text_dump = read_extract(ebu_path)
        ebu = f"<Header_tags><header_tags.parser>MediaInfo ebucore 0</header_tags.parser><header_tags><![CDATA[{text_dump}]]></header_tags></Header_tags>"

    # Processing metadata output for pbcore path
    if os.path.exists(pb_path):
        text_dump = read_extract(pb_path)
        pb = f"<Header_tags><header_tags.parser>MediaInfo pbcore 0</header_tags.parser><header_tags><![CDATA[{text_dump}]]></header_tags></Header_tags>"

    # Processing metadata output for pbcore path
    if os.path.exists(xml_path):
        text_dump = read_extract(xml_path)
        xml = f"<Header_tags><header_tags.parser>MediaInfo xml 0</header_tags.parser><header_tags><![CDATA[{text_dump}]]></header_tags></Header_tags>"

    # Processing metadata output for json path
    if os.path.exists(json_path):
        text_dump = read_extract(json_path)
        json = f"<Header_tags><header_tags.parser>MediaInfo json 0</header_tags.parser><header_tags><![CDATA[{text_dump}]]></header_tags></Header_tags>"

    # Processing metadata output for special collections exif data
    if os.path.exists(exif_path):
        text_dump = read_extract(exif_path)
        exif = f"<Header_tags><header_tags.parser>Exiftool text</header_tags.parser><header_tags><![CDATA[{text_dump}]]></header_tags></Header_tags>"

    payload_data = text + text_full + ebu + pb + xml + json + exif

    # Write data
    success = write_payload(priref, payload_data)
    if success:
        LOGGER.info("Payload data successfully written to CID Media record: %s", priref)
        if os.path.exists(text_path):
            LOGGER.info("Deleting path: %s", text_path)
            os.remove(text_path)
        if os.path.exists(text_full_path):
            LOGGER.info("Deleting path: %s", text_full_path)
            os.remove(text_full_path)
        if os.path.exists(ebu_path):
            LOGGER.info("Deleting path: %s", ebu_path)
            os.remove(ebu_path)
        if os.path.exists(pb_path):
            LOGGER.info("Deleting path: %s", pb_path)
            os.remove(pb_path)
        if os.path.exists(xml_path):
            LOGGER.info("Deleting path: %s", xml_path)
            os.remove(xml_path)
        if os.path.exists(json_path):
            LOGGER.info("Deleting path: %s", json_path)
            os.remove(json_path)
        if os.path.exists(exif_path):
            LOGGER.info("Deleting path: %s", exif_path)
            os.remove(exif_path)
    else:
        LOGGER.warning("Payload data was not written to CID Media record: %s", priref)


def write_payload(priref, payload_data):
    '''
    Payload formatting per mediainfo output
    '''
    payload_head = f"<adlibXML><recordList><record priref='{priref}'>"
    payload_end = "</record></recordList></adlibXML>"
    payload = payload_head + payload_data + payload_end

    record = adlib.post(CID_API, payload, 'media', 'updaterecord')
    if record is None:
        return False
    elif "header_tags.parser" in str(record):
        return True
    else:
        return None


if __name__ == '__main__':
    main()
