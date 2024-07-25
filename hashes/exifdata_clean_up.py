#!/usr/bin/env python3

'''
Script to clean up Mediainfo files that belong to filepaths deleted by autoingest,
writes mediainfo data to CID media record priref before deleting files.

1. Receive filepath of '*_EXIF.txt' file from sys.argv
2. Extract filename, and create path for new file
3. Check CID Media record exists with imagen.media.original_filename matching filename
4. Capture priref of CID Media record
5. Extract Exiftool metadata file and write in XML block to CID media record
   in header_tag and header_parser field,
7. Where data written successfully delete mediainfo file
8. Where there's no match leave file and may be needed later

Joanna White
2024
'''

# Global packages
import os
import sys

# Local packages
sys.path.append(os.environ['CODE'])
import adlib_v3_sess as adlib
import utils

# Global variables
LOG_PATH = os.environ['LOG_PATH']
MEDIAINFO_PATH = os.path.join(LOG_PATH, 'cid_mediainfo')
CID_API = os.environ['CID_API4']
LOG = os.path.join(LOG_PATH, 'exifdata_clean_up.log')


def cid_retrieve(fname, session):
    '''
    Retrieve priref for media record from imagen.media.original_filename
    '''
    priref = ''
    search = f"imagen.media.original_filename='{fname}'"
    record = adlib.retrieve_record(CID_API, 'media', search, '0', session)[1]
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
    if not utils.cid_check(CID_API):
        utils.logger(LOG, 'critical', "* Cannot establish CID session, exiting script")
        sys.exit()
    if not utils.check_control('pause_scripts'):
        utils.logger(LOG, 'info', 'Script run prevented by downtime_control.json. Script exiting.')
        sys.exit('Script run prevented by downtime_control.json. Script exiting.')
    if len(sys.argv) < 2:
        sys.exit()

    exif_list = [ x for x in os.listdir(MEDIAINFO_PATH) if '_EXIF.txt' in str(x) ]
    if not exif_list:
        sys.exit()

    utils.logger(LOG, 'info' "============ EXIFDATA CLEAN UP SCRIPT START ===================")
    session = utils.create_session()

    for item in exif_list:
        filename = item.split("_EXIF.txt")[0]
        exif_path = os.path.join(MEDIAINFO_PATH, item)
        if len(filename) > 0 and filename.endswith((".ini", ".DS_Store", ".mhl", ".json")):
            continue

        # Checking for existence of Digital Media record
        utils.logger(LOG, 'info', f'Checking in CID for match to found filename: {filename}')
        priref = cid_retrieve(filename, session)
        if len(priref) == 0:
            utils.logger(LOG, 'warning', 'Skipping item. Priref could not be retrieved.')
            continue

        # Processing metadata output for exif path
        if os.path.exists(exif_path):
            metadata = read_extract(exif_path)
            payload_data = f"<Header_tags><header_tags.parser>Exiftool</header_tags.parser><header_tags><![CDATA[{metadata}]]></header_tags></Header_tags>"

        if not payload_data:
            utils.logger(LOG, 'warning', f"Skipping: Unable to retrieve metadata from file: {exif_path}")
            continue

        # Write data
        success = write_payload(priref, payload_data, session)
        if success:
            utils.logger(LOG, 'info', f"Payload data successfully written to CID Media record: {priref}")
            utils.logger(LOG, 'info', "Deleting all path data")
            if os.path.exists(exif_path):
                os.remove(exif_path)
        else:
            utils.logger(LOG, 'warning', f"Payload data was not written to CID Media record: {priref}")
            utils.logger(LOG, 'info', "Metadata records being left in place for repeat write attempt")

    utils.logger(LOG, 'info', "============ METADATA CLEAN UP SCRIPT COMPLETE ================")


def write_payload(priref, payload_data, session):
    '''
    Payload formatting per mediainfo output
    '''
    payload_head = f"<adlibXML><recordList><record priref='{priref}'>"
    payload_end = "</record></recordList></adlibXML>"
    payload = payload_head + payload_data + payload_end

    record = adlib.post(CID_API, payload, 'media', 'updaterecord', session)
    if record is None:
        return False
    elif 'error' in str(record):
        return False
    else:
        return True


if __name__ == '__main__':
    main()
