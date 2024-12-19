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
import csv
import sys
import json
import logging

# Local packages
sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib
import utils

# Global variables
LOG_PATH = os.environ['LOG_PATH']
MEDIAINFO_PATH = os.path.join(LOG_PATH, 'cid_mediainfo')
CSV_PATH = os.path.join(LOG_PATH, 'persistence_queue_copy.csv')
CID_API = os.environ['CID_API4']
ERROR_CSV = os.path.join(LOG_PATH, 'media_record_metadata_post_failures.csv')

# Setup logging
LOGGER = logging.getLogger('metadata_clean_up')
HDLR = logging.FileHandler(os.path.join(LOG_PATH, 'metadata_clean_up.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)

FIELDS = [
    {'container.duration': ['Duration_String1', 'Duration  ']},
    {'container.duration.milliseconds': ['Duration', '']},
    {'container.file_size.total_bytes': ['FileSize', 'File size  ']},
    {'container.file_size.total_gigabytes': ['FileSize_String4', 'File size  ']},
    {'container.commercial_name': ['Format_Commercial', 'Commercial name  ']},
    {'container.format': ['Format', 'Format  ']},
    {'container.audio_codecs': ['Audio_Codec_List', 'Audio codecs ']},
    {'container.audio_stream_count': ['AudioCount', 'Count of audio streams  ']},
    {'container.video_stream_count': ['VideoCount', 'Count of video streams  ']},
    {'container.format_profile': ['Format_Profile','Format profile  ']},
    {'container.format_version': ['Format_Version','Format version  ']},
    {'container.encoded_date': ['Encoded_Date','Encoded date  ']},
    {'container.frame_count': ['FrameCount', 'Frame count  ']},
    {'container.frame_rate': ['FrameRate', 'Frame rate  ']},
    {'container.overall_bit_rate': ['OverallBitRate_String', 'Overall bit rate  ']},
    {'container.overall_bit_rate_mode': ['OverallBitRate_Mode', 'Overall bit rate mode  ']},
    {'container.writing_application': ['Encoded_Application', 'Writing application  ']},
    {'container.writing_library': ['Encoded_Library', 'Writing library  ']},
    {'container.file_extension': ['FileExtension', 'File extension  ']},
    {'container.media_UUID': ['UniqueID', 'Unique ID  ']},
    {'container.truncated': ['IsTruncated','Is truncated  ']},
    {'video.duration': ['Duration_String1', '']},
    {'video.duration.milliseconds': ['Duration','Duration  ']}, 
    {'video.bit_depth': ['BitDepth', 'Bit depth  ']},
    {'video.bit_rate_mode': ['BitRate_Mode', 'Bit rate mode  ']},
    {'video.bit_rate': ['BitRate_String','Bit rate  ']},
    {'video.chroma_subsampling': ['ChromaSubsampling', 'Chroma subsampling']},
    {'video.compression_mode': ['Compression_Mode', 'Compression mode  ']},
    {'video.format_version': ['Format_Version', 'Format version  ']},
    {'video.frame_count': ['FrameCount', 'Frame count  ']},
    {'video.frame_rate': ['FrameRate', 'Frame rate  ']},
    {'video.frame_rate_mode': ['FrameRate_Mode', 'Frame rate mode  ']},
    {'video.height': ['Height', 'Height  ']},
    {'video.scan_order': ['ScanOrder_String', 'Scan order  ']},
    {'video.scan_type': ['ScanType','Scan type  ']},
    {'video.scan_type.store_method': ['ScanType_StoreMethod_String', 'Scan type, store method  ']},
    {'video.standard': ['Standard', 'Standard  ']},
    {'video.stream_size_bytes': ['StreamSize', 'Stream size  ']},
    {'video.stream_order': ['StreamOrder', 'StreamOrder  ']},
    {'video.width': ['Width', 'Width  ']},
    {'video.format_profile': ['Format_Profile', 'Format profile  ']},
    {'video.width_aperture': ['Width_CleanAperture', 'Width clean aperture  ']}, # Guessed second
    {'video.delay': ['Delay','Delay  ']},
    {'video.format_settings_GOP': ['Format_Settings_GOP', 'Format settings, GOP  ']},
    {'video.codec_id': ['CodecID','Codec ID  ']},
    {'video.colour_space': ['ColorSpace','Color space  ']},
    {'video.colour_primaries': ['colour_primaries', 'Color primaries  ']},
    {'video.commercial_name': ['Format_Commercial', 'Commercial name  ']},
    {'video.display_aspect_ratio': ['DisplayAspectRatio','Display aspect ratio  ']},
    {'video.format': ['Format', 'Format  ']},
    {'video.matrix_coefficients': ['matrix_coefficients', 'Matrix coefficients  ']},
    {'video.pixel_aspect_ratio': ['PixelAspectRatio', 'Pixel aspect ratio  ']},
    {'video.transfer_characteristics': ['transfer_characteristics', 'Transfer characteristics  ']},
    {'video.writing_library': ['Encoded_Library', 'Writing library  ']},
    {'video.stream_size': ['StreamSize_String', 'Stream size  ']},
    {'colour_range': ['colour_range', 'Color range  ']},
    {'max_slice_count': ['MaxSlicesCount', 'MaxSlicesCount  ']},
    {'audio.bit_depth': ['BitDepth', 'Bit depth  ']},
    {'audio.bit_rate': ['BitRate_String', 'Bit rate  ']},
    {'audio.bit_rate_mode': ['BitRate_Mode', 'Bit rate mode  ']},
    {'audio.channels': ['Channels', 'Channel(s)  ']},
    {'audio.codec_id': ['CodecID', 'Codec ID  ']},
    {'audio.duration': ['Duration_String1', 'Duration  ']},
    {'audio.channel_layout': ['ChannelLayout', 'Channel layout  ']},
    {'audio.channel_position': ['ChannelPositions', 'Channel positions  ']},
    {'audio.compression_mode': ['Compression_Mode', 'Compression mode  ']},
    {'audio.format_settings_endianness': ['Format_Settings_Endianness', 'Format settings, Endianness  ']},
    {'audio.format_settings_sign': ['Format_Settings_Sign', 'Format settings, Sign  ']},
    {'audio.frame_count': ['FrameCount','Frame count  ']},
    {'audio.language': ['Language_String', 'Language  ']},
    {'audio.stream_size_bytes': ['StreamSize', 'Stream size  ']},
    {'audio.stream_order': ['StreamOrder', 'StreamOrder  ']},
    {'audio.stream_size': ['StreamSize_String', 'Stream size  ']},
    {'audio.commercial_name': ['Format_Commercial', 'Commercial name  ']},
    {'audio.format': ['Format', 'Format  ']},
    {'audio.sampling_rate': ['SamplingRate_String', 'Sampling rate  ']},
    {'other.duration': ['Duration_String1', 'Duration  ']},
    {'other.frame_rate': ['FrameRate', 'Frame rate  ']},
    {'other.language': ['Language_String', 'Language  ']},
    {'other.type': ['Type', 'Type  ']},
    {'other.timecode_first_frame': ['TimeCode_FirstFrame', 'Time code of first frame  ']},
    {'other.stream_order': ['StreamOrder', 'StreamOrder  ']},
    {'other.format': ['Format', 'Format  ']},
    {'text.duration': ['Duration_String1', 'Duration  ']},
    {'text.stream_order': ['StreamOrder', 'StreamOrder  ']},
    {'text.format': ['Format', 'Format  ']},
    {'text.codec_id': ['CodecID', 'Codec ID  ']}
]

def make_paths(filename):
    '''
    Make all possible paths
    '''
    text_full_path = os.path.join(MEDIAINFO_PATH, f"{filename}_TEXT_FULL.txt")
    ebu_path = os.path.join(MEDIAINFO_PATH, f"{filename}_EBUCore.xml")
    pb_path = os.path.join(MEDIAINFO_PATH, f"{filename}_PBCore2.xml")
    xml_path = os.path.join(MEDIAINFO_PATH, f"{filename}_XML.xml")
    json_path = os.path.join(MEDIAINFO_PATH, f"{filename}_JSON.json")
    exif_path = os.path.join(MEDIAINFO_PATH, f"{filename}_EXIF.txt")

    return [text_full_path, ebu_path, pb_path, xml_path, json_path, exif_path]


def cid_retrieve(fname):
    '''
    Retrieve priref for media record from imagen.media.original_filename
    '''
    try:
        search = f"imagen.media.original_filename='{fname}'"
        record = adlib.retrieve_record(CID_API, 'media', search, '0')[1]
        if not record:
            return ''
        if 'priref' in str(record):
            priref = adlib.retrieve_field_name(record[0], 'priref')[0]
            return priref
        return ''
    except Exception as e:
        print(e)
    except AttributeError:
        print('Priref return None type')


def main():
    '''
    Clean up scripts for checksum files that have been processed by autoingest.
    Write all mediainfo reports to header_tags. Populate fields with specific data.
    Two POST strategy in place to manage development of thesaurus terms for linked fields
    '''
    if len(sys.argv) < 2:
        sys.exit('Missing arguments')
    if not utils.cid_check(CID_API):
        print("* Cannot establish CID session, exiting script")
        LOGGER.critical("* Cannot establish CID session, exiting script")
        sys.exit()
#    if not utils.check_control('pause_scripts'):
#        LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
#        sys.exit('Script run prevented by downtime_control.json. Script exiting.')

    text_path = sys.argv[1]
    text_file = os.path.basename(text_path)
    if text_file.endswith('_TEXT.txt'):
        filename = text_file.split("_TEXT.txt")[0]
        if len(filename) > 0 and filename.endswith((".ini", ".DS_Store", ".mhl", ".json")):
            sys.exit('Incorrect media file detected.')

        # Checking for existence of Digital Media record
        print(text_path, filename)
        priref = cid_retrieve(filename)
        if priref is None:
            sys.exit('Script exiting. Could not find matching Priref.')
        if len(priref) == 0:
            sys.exit('Script exiting. Priref could not be retrieved.')

        print(f"Priref retrieved: {priref}. Writing metadata to record")
        json_path = make_paths(filename)[4]
        json_use = True
        if not json_path:
            LOGGER.warning("JSON record is absent. Attempting recovery from TEXT data")
            json_use = False
        else:
            with open(json_path, 'r') as f:
                length = len(f.readlines())
                if not length >= 10:
                    LOGGER.warning("JSON record is absent. Attempting recovery from TEXT data")
                    json_use = False
        if json_use:
            mdata_xml = build_metadata_xml(json_path, priref)
        else:
            text_full_path = make_paths(filename)[0]
            mdata_xml = build_metadata_text_xml(text_path, text_full_path, priref)

        success = write_payload(mdata_xml)
        if success:
            LOGGER.info("** Digital Media metadata from JSON successfully written to CID Media record: %s", priref)
        else:
            LOGGER.warning("Failed to push regular metadata to the CID record. Writing to errors CSV")
            write_to_errors_csv('media', CID_API, priref, mdata_xml)

    elif text_file.endswith('_EXIF.txt'):
        filename = text_file.split("_EXIF.txt")[0]
        if len(filename) > 0 and filename.endswith((".ini", ".DS_Store", ".mhl", ".json")):
            sys.exit('Incorrect media file detected.')

        # Checking for existence of Digital Media record
        print(text_path, filename)
        priref = cid_retrieve(filename)
        if priref is None:
            sys.exit('Script exiting. Could not find matching Priref.')
        if len(priref) == 0:
            sys.exit('Script exiting. Priref could not be retrieved.')

        print(f"Priref retrieved: {priref}. Writing metadata to record")
        exif_path = make_paths(filename)[5]

        image_xml = build_exif_metadata_xml(exif_path, priref)

        success = write_payload(image_xml)
        if success:
            LOGGER.info("** Digital Media EXIF metadata from JSON successfully written to CID Media record: %s", priref)
        else:
            LOGGER.warning("Failed to push EXIF metadata to the CID record. Writing to errors CSV")
            write_to_errors_csv('media', CID_API, priref, image_xml)

    # Write remaining metadata to header_tags and clean up
    header_payload = make_header_data(text_path, filename, priref)
    if not header_payload:
        LOGGER.warning("Failed to compile header metadata tag. Writing to errors CSV")
        write_to_errors_csv('media', CID_API, priref, header_payload)
        sys.exit()

    success = write_payload(header_payload)
    if success:
        LOGGER.info("Payload data successfully written to CID Media record: %s", priref)
    else:
        LOGGER.warning("Failed to POST header tag data to CID record. Writing to errors CSV")
        # clean_up(filename)


def build_exif_metadata_xml(exif_path, priref):
    '''
    Open text file for any EXIF data
    and create XML for update record
    '''
    with open(exif_path, 'r') as metadata:
        mdata = metadata.readlines()

    img_xml = get_image_xml(mdata)
    print(img_xml)
    xml = adlib.create_record_data(CID_API, 'media', priref, img_xml)

    return xml


def build_metadata_xml(json_path, priref):
    '''
    Open JSON, dump to dict and create
    metadata XML for updaterecord
    '''
    videos = audio = other = text = ''
    with open(json_path, 'r') as metadata:
        mdata = json.load(metadata)

    for track in mdata['media']['track']:
        if track['@type'] == 'General':
            print(f"General track: {track}")
            gen = get_xml('container', track)
            gen_xml = wrap_as_xml('Container', gen)

        elif track['@type'] == 'Video':
            print(f"Video track: {track}")
            vid = get_video_xml(track)
            vid_xml = wrap_as_xml('Video', vid)
            if len(videos) > 0:
                videos += vid_xml
            else:
                videos = vid_xml
        elif track['@type'] == 'Audio':
            print(f"Audio track: {track}")
            aud = get_xml('audio', track)
            aud_xml = wrap_as_xml('Audio', aud)
            if len(audio) > 0:
                audio += aud_xml
            else:
                audio = aud_xml
        elif track['@type'] == 'Other':
            oth = get_xml('other', track)
            oth_xml = wrap_as_xml('Other', oth)
            if len(other) > 0:
                other += oth_xml
            else:
                other = oth_xml
        elif track['@type'] == 'Text':
            txt = get_xml('text', track)
            txt_xml = wrap_as_xml('Text', txt)
            if len(text) > 0:
                text += txt_xml
            else:
                text = txt_xml

    payload = gen_xml + videos + audio + other + text
    payload_start = f"<adlibXML><recordList><record priref='{priref}'>"
    payload_end = "</record></recordList></adlibXML>"

    return f"{payload_start}{payload}{payload_end}"


def get_text_rows(start, mdata):
    '''
    read lines and get data
    '''
    collection = []
    capture = False
    for row in mdata:
        if start in row:
            capture = True
            collection.append(row.strip())
        if row == '\n':
            capture = False
        if capture and ':' in str(row):
            collection.append(row.strip())

    return collection


def iterate_text_rows(data, match, key):
    '''
    Receive key to match, sort and
    clean data and return
    '''
    if match == '':
        return None

    matches = []
    for row in data:
        if match in str(row):
            field_entry = row.split(':', 1)[-1].strip()
            if 'MiB' in field_entry:
                continue
            matches.append(field_entry)
    if matches:
        if 'height' in key or 'width' in key:
            field_chosen = manipulate_data(key, sorted(matches, key=len)[0])
        else:
            field_chosen = manipulate_data(key, sorted(matches, key=len)[-1])
        if field_chosen is None:
            return None
        return {f'{key}': field_chosen}


def get_stream_count(gen_rows):
    '''
    Get counts of streams
    to isolate metadata
    '''
    for row in gen_rows:
        if row.startswith('Count of audio streams  '):
            aud_count = int(row.split(':')[-1].strip())
        if row.startswith('Count of video streams  '):
            vid_count = int(row.split(':')[-1].strip())

    return vid_count, aud_count


def build_metadata_text_xml(text_path, text_full_path, priref):
    '''
    Use supplied text file for metadata extraction
    '''
    if os.path.exists(text_full_path):
        with open(text_full_path, 'r') as metadata:
            mdata = metadata.readlines()
    else:
        with open(text_path, 'r') as metadata:
            mdata = metadata.readlines()

    gen = []
    payload = ''
    gen_rows = get_text_rows('General', mdata)
    for field in FIELDS:
        for key, val in field.items():
            if key.startswith('container.'):
                match = iterate_text_rows(gen_rows, val[1], key)
                if match is None:
                    continue
                gen.append(match)
    if len(gen) > 0:
        xml = wrap_as_xml('Container', gen)
        payload += xml

    vid_count, aud_count = get_stream_count(gen_rows)
    for num in range(1, vid_count+1):
        if vid_count == 1:
            vid_rows = get_text_rows('Video', mdata)
        else:
            vid_rows = get_text_rows(f'Video #{num}', mdata)
        vid = []
        for field in FIELDS:
            for key, val in field.items():
                if key.startswith('video.'):
                    match = iterate_text_rows(vid_rows, val[1], key)
                    if match is None:
                        continue
                    vid.append(match)
                if key.startswith('colour_range'):
                    match = iterate_text_rows(vid_rows, val[1], key)
                    if match is None:
                        continue
                    vid.append(match)
                if key.startswith('MaxSlicesCount'):
                    match = iterate_text_rows(vid_rows, val[1], key)
                    if match is None:
                        continue
                    vid.append(match)
        if len(vid) > 0:
            xml = wrap_as_xml('Video', vid)
            payload += xml

    for num in range(1, aud_count+1):
        if aud_count == 1:
            aud_rows = get_text_rows('Audio', mdata)
        else:
            aud_rows = get_text_rows(f'Audio #{num}', mdata)
        aud = []
        for field in FIELDS:
            for key, val in field.items():
                if key.startswith('audio.'):
                    match = iterate_text_rows(aud_rows, val[1], key)
                    if match is None:
                        continue
                    aud.append(match)
        if len(aud) > 0:
            xml = wrap_as_xml('Audio', aud)
            payload += xml

    oth = []
    oth_rows = get_text_rows('Other', mdata)
    for field in FIELDS:
        for key, val in field.items():
            if key.startswith('other.'):
                match = iterate_text_rows(oth_rows, val[1], key)
                if match is None:
                    continue
                oth.append(match)
    if len(oth) > 0:
        xml = wrap_as_xml('Other', oth)
        payload += xml

    txt = []
    txt_rows = get_text_rows('Text', mdata)
    for field in FIELDS:
        for key, val in field.items():
            if key.startswith('text.'):
                match = iterate_text_rows(txt_rows, val[1], key)
                if match is None:
                    continue
                txt.append(match)
    if len(txt) > 0:
        xml = wrap_as_xml('Text', txt)
        payload += xml

    payload_start = f"<adlibXML><recordList><record priref='{priref}'>"
    payload_end = "</record></recordList></adlibXML>"

    return f"{payload_start}{payload}{payload_end}"


def manipulate_data(key, selection):
    '''
    Sort and transform data where needed
    '''
    if '.sampling_rate' in key and selection.isnumeric():
        return None
    if '.stream_size_bytes' in key and selection.isnumeric():
        return selection
    if '.stream_size' in key and selection.isnumeric():
        return None
    if '.bit_rate' in key and selection.isnumeric():
        return None
    if selection == 'Variable':
        return 'VBR'
    if selection == 'Constant':
        return 'CBR'
    if '.total_gigabytes' in key and 'GiB' in selection:
        return selection.split(' GiB')[0]
    elif '.total_gigabytes' in key and 'MiB' in selection:
        return None
    elif '.total_gigabytes' in key and selection.isnumeric():
        return None
    if 'FPS' in selection:
        return selection.split(' FPS')[0]
    if '.milliseconds' in key and selection.isnumeric():
        return selection
    elif '.milliseconds' in key and ':' in selection:
        return None
    elif '.milliseconds' in key and 'min' in selection:
        return None
    if '.bit_depth' in key and ' bits' in selection:
        return selection.split(' bits')[0]
    if 'language' in key and selection == 'en':
        return 'English'
    if 'language' in key and 'nar' in selection:
        return None
    if '.height' in key and 'pixel' in selection:
        return None
    if '.width' in key and 'pixels' in selection:
        return None
    if 'audio.channels' in key and 'channel' in selection:
        return None
    return selection


def wrap_as_xml(grouping, field_pairs):
    '''
    Borrwed from Adlib
    but for specific need
    '''
    mid = ''
    for grouped in field_pairs:
        for key, val in grouped.items():
            xml_field = f'<{key}>{val}</{key}>'
            mid += xml_field

    return f'<{grouping}>{mid}</{grouping}>'


def get_xml(arg, track):
    '''
    Create dictionary for General
    metadata required
    '''
    dict = []
    for field in FIELDS:
        for k, v in field.items():
            if k.startswith(f'{arg}.'):
                if track.get(v[0]):
                    selected = manipulate_data(k, track.get(v[0]))
                    if selected is None:
                        continue
                    dict.append({f'{k}': selected.strip()})

    return dict


def get_video_xml(track):
    '''
    Create dictionary for Video
    metadata required
    '''

    video_dict = []
    for field in FIELDS:
        for k, v in field.items():
            if k.startswith('video.'):
                if track.get(v[0]):
                    selected = manipulate_data(k, track.get(v[0]))
                    print(type(selected))
                    if selected is None:
                        continue
                    video_dict.append({f'{k}': selected.strip()})
            if k.startswith('colour_range'):
                if track.get(v[0]):
                    selected = manipulate_data(k, track.get(v[0]))
                    if selected is None:
                        continue
                    video_dict.append({f'{k}': selected.strip()})
            if k.startswith('max_slice_count'):
                if track.get(v[0]):
                    selected = manipulate_data(k, track.get(v[0]))
                    if selected is None:
                        continue
                    video_dict.append({f'{k}': selected.strip()})
                elif track.get('extra'):
                    try:
                        selected = manipulate_data(k, track.get('extra').get(v[0]))
                        if selected is None:
                            continue
                        video_dict.append({f'{k}': selected.strip()})
                    except (KeyError, AttributeError, TypeError):
                        pass
    return video_dict


def get_image_xml(track):
    '''
    Create dictionary for Image
    metadata from Exif data source
    '''

    if not isinstance(track, list):
        return None

    data = [
        'File Size, file_size',
        'Bits Per Sample, bits_per_sample',
        'Color Components, colour_components',
        'Color Space, colour_space',
        'Compression, compression',
        'Encoding Process, encoding_process',
        'Exif Byte Order, exif_byte_order',
        'File Type, file_type',
        'Exif Image Height, height',
        'Exif Image Width, width',
        'Orientation, orientation',
        'Resolution Unit, resolution_unit',
        'Software, software',
        'X Resolution, x_resolution',
        'Y Cb Cr Sub Sampling, y_cb_cr_sub_sampling',
        'Y Resolution, y_resolution'
    ]

    image_dict = []
    for mdata in track:
        field, value = mdata.split(':', 1)
        for d in data:
            exif_field, cid_field = d.split(', ')
            if exif_field == field.strip():
                image_dict.append({f'image.{cid_field}': value.strip()})

    return image_dict


def clean_up(filename, text_path):
    '''
    Clean up metadata
    '''
    tfp, ep, pp, xp, jp, exfp = make_paths(filename)

    try:
        LOGGER.info("Deleting path: %s", text_path)
        os.remove(text_path)
    except Exception:
        LOGGER.warning("Unable to delete file: %s", text_path)
    try:
        LOGGER.info("Deleting path: %s", tfp)
        os.remove(tfp)
    except Exception:
        LOGGER.warning("Unable to delete file: %s", tfp)
    try:
        LOGGER.info("Deleting path: %s", ep)
        os.remove(ep)
    except Exception:
        LOGGER.warning("Unable to delete file: %s", ep)
    try:
        LOGGER.info("Deleting path: %s", pp)
        os.remove(pp)
    except Exception:
        LOGGER.warning("Unable to delete file: %s", pp)
    try:
        LOGGER.info("Deleting path: %s", xp)
        os.remove(xp)
    except Exception:
        LOGGER.warning("Unable to delete file: %s", xp)
    try:
        LOGGER.info("Deleting path: %s", jp)
        os.remove(jp)
    except Exception:
        LOGGER.warning("Unable to delete file: %s", jp)
    try:
        LOGGER.info("Deleting path: %s", exfp)
        os.remove(exfp)
    except Exception:
        LOGGER.warning("Unable to delete file: %s", exfp)


def make_header_data(text_path, filename, priref):
    '''
    Create the header tag data
    '''
    tfp, ep, pp, xp, jp, exfp = make_paths(filename)
    text = text_full = ebu = pb = xml = json = exif = ''
    if text_path.endswith('_EXIF.txt'):
        text_path = text_path.replace('_EXIF.txt', '_TEXT.txt')

    # Processing metadata output for text path
    try:
        text_dump = utils.read_extract(text_path)
        text = f"<Header_tags><header_tags.parser>MediaInfo text 0</header_tags.parser><header_tags><![CDATA[{text_dump}]]></header_tags></Header_tags>"
    except Exception as err:
        print(err)
        LOGGER.warning("Failed to write Text dump to record %s: %s", priref, text_path)

    # Processing metadata output for text full path
    try:
        text_dump = utils.read_extract(tfp)
        text_full = f"<Header_tags><header_tags.parser>MediaInfo text 0 full</header_tags.parser><header_tags><![CDATA[{text_dump}]]></header_tags></Header_tags>"
    except Exception as err:
        print(err)
        LOGGER.warning("Failed to write Text Full dump to record %s: %s", priref, tfp)

    # Processing metadata output for ebucore path
    try:
        text_dump = utils.read_extract(ep)
        ebu = f"<Header_tags><header_tags.parser>MediaInfo ebucore 0</header_tags.parser><header_tags><![CDATA[{text_dump}]]></header_tags></Header_tags>"
    except Exception as err:
        print(err)
        LOGGER.warning("Failed to write EBUCore dump to record %s: %s", priref, ep)

    # Processing metadata output for pbcore path
    try:
        text_dump = utils.read_extract(pp)
        pb = f"<Header_tags><header_tags.parser>MediaInfo pbcore 0</header_tags.parser><header_tags><![CDATA[{text_dump}]]></header_tags></Header_tags>"
    except Exception as err:
        print(err)
        LOGGER.warning("Failed to write PBCore dump to record %s: %s", priref, pb)

    # Processing metadata output for pbcore path
    try:
        text_dump = utils.read_extract(xp)
        xml = f"<Header_tags><header_tags.parser>MediaInfo xml 0</header_tags.parser><header_tags><![CDATA[{text_dump}]]></header_tags></Header_tags>"
    except Exception as err:
        print(err)
        LOGGER.warning("Failed to write XML dump to record %s: %s", priref, xp)

    # Processing metadata output for json path
    try:
        text_dump = utils.read_extract(jp)
        json = f"<Header_tags><header_tags.parser>MediaInfo json 0</header_tags.parser><header_tags><![CDATA[{text_dump}]]></header_tags></Header_tags>"
    except Exception as err:
        print(err)
        LOGGER.warning("Failed to write JSON dump to record %s: %s", priref, jp)

    # Processing metadata output for special collections exif data
    try:
        text_dump = utils.read_extract(exfp)
        exif = f"<Header_tags><header_tags.parser>Exiftool text</header_tags.parser><header_tags><![CDATA[{text_dump}]]></header_tags></Header_tags>"
    except Exception as err:
        print(err)
        LOGGER.warning("Failed to write Exif dump to record %s: %s", priref, exfp)

    payload_data = text + text_full + ebu + pb + xml + json + exif
    return f"<adlibXML><recordList><record priref='{priref}'>{payload_data}</record></recordList></adlibXML>"


def write_payload(payload):
    '''
    Payload formatting per mediainfo output
    '''

    record = adlib.post(CID_API, payload, 'media', 'updaterecord')
    print(record)
    if record is None:
        return False
    elif "'error': {'message':" in str(record):
        return False
    elif priref in str(record):
        return True
    else:
        return None


def write_to_errors_csv(dbase, api, priref, xml_dump):
    '''
    Keep a tab of problem POSTs as we expand
    thesaurus range for media record linked metadata
    '''
    data = [priref, dbase, api, xml_dump]

    with open(ERROR_CSV, 'a+', newline='') as csvfile:
        datawriter = csv.writer(csvfile)
        print(f"Adding to CSV error logs:\n{data}")
        datawriter.writerow(data)


if __name__ == '__main__':
    main()
