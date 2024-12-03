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
import utils

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


def cid_retrieve(fname):
    '''
    Retrieve priref for media record from imagen.media.original_filename
    '''
    try:
        priref = ''
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
    '''
    if len(sys.argv) < 2:
        sys.exit()
    if not utils.cid_check(CID_API):
        print("* Cannot establish CID session, exiting script")
        LOGGER.critical("* Cannot establish CID session, exiting script")
        sys.exit()
    if not utils.check_control('pause_scripts'):
        LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
        sys.exit('Script run prevented by downtime_control.json. Script exiting.')

    text_path = sys.argv[1]
    text_file = os.path.basename(text_path)
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
    mdata_xml = build_metadata_xml(json_path, priref)
    print(mdata_xml)
    sys.exit("Not writing to anywhere")
    success = write_payload(priref, mdata_xml, 'container.duration')
    if success:
        LOGGER.info("Digital Media metadata from JSON successfully written to CID Media record: %s", priref)

    # Write remaining metadata to header_tags and clean up
    header_payload = make_header_data(text_path, filename)
    if not header_payload:
        sys.exit()
    success = write_payload(priref, header_payload, "header_tags.parser")
    if success:
        LOGGER.info("Payload data successfully written to CID Media record: %s", priref)
        clean_up(filename)


def build_metadata_xml(json_path, priref):
    '''
    Open JSON, dump to dict and create
    metadata XML for updaterecord
    '''
    videos = []
    image = []
    audio = []
    other = []
    text = []

    with open(json_path, 'r') as metadata:
        mdata = json.loads(metadata)

    for track in mdata['media']['track']:
        if track['@type'] == 'General':
            gen_xml = get_general_xml(track)
        elif track['@type'] == 'Video':
            vid_xml = get_video_xml(track)
            videos = videos + vid_xml
        elif track['@type'] == 'Image':
            img_xml = get_image_xml(track)
            image = image + img_xml
        elif track['@type'] == 'Audio':
            aud_xml = get_audio_xml(track)
            audio = audio + aud_xml
        elif track['@type'] == 'Other':
            oth_xml = get_other_xml(track)
            other = other + oth_xml
        elif track['@type'] == 'Text':
            txt_xml = get_text_xml(track)
            text = text + txt_xml

    return gen_xml + videos + audio + other + text


def match_lref(arg, matched_data):
    '''
    Look up thesuarus data for argument supplied
    and match the matched_data supplied
    JMW -- to complete when understood better
    '''
    pass


def get_general_xml(track):
    '''
    Create dictionary for General
    metadata required
    '''
    data = [
        'Duration/String1, duration',
        'Duration, duration.milliseconds',
        'FileSize, file_size.total_bytes',
        'FileSize/String4, file_size.total_gigabytes',
        'AudioCount, audio_stream_count',
        'VideoCount, video_stream_count',
        'Format_Profile, format_profile',
        'Format_Version, format_version',
        'Encoded_Date, encoded_date',
        'FrameCount, frame_count',
        'FrameRate, frame_rate',
        'OverallBitRate, overall_bit_rate',
        'OverallBitRate_Mode, overall_bit_rate_mode',
        'Encoded_Application, writing_application',
        'Encoded_Library, writing_library',
        'FileExtension, file_extension',
        'UniqueID, media_UUID',
        'IsTruncated, truncated'
    ]

    general_dict = []
    for mdata in data:
        minfo, cid = mdata.split(', ')
        if track.get(minfo):
            general_dict.append({f'container.{cid}': track[minfo]})

    # Manage lref look up for some items
    if track.get('Format_Commercial'):
        cn = match_lref('container.commercial_name', track['Format_Commercial'])
        if cn:
            general_dict.append({'container.commercial_name.lref': cn})
    if track.get('Format'):
        cn = match_lref('container.format', track['Format'])
        if cn:
            general_dict.append({'container.format.lref': cn})
    if track.get('Audio_Codec_List'):
        cn = match_lref('container.audio_codecs', track['Audio_Codec_List'])
        if cn:
            general_dict.append({'container.audio_codecs.lref': cn})

    general_xml = adlib.create_grouped_data('', 'container', general_dict)
    return general_xml


def get_video_xml(track):
    '''
    Create dictionary for Video
    metadata required
    '''
    data = [
        'Duration/String1, duration',
        'Duration, duration.milliseconds',
        'BitDepth, bit_depth',
        'BitRate_Mode, bit_rate_mode',
        'BitRate, bit_rate',
        'ChromaSubsampling, chroma_subsampling',
        'colour_primaries, colour_primaries',
        'Compression_Mode, compression_mode',
        'Format_Version, format_version',
        'FrameCount, frame_count',
        'FrameRate, frame_rate',
        'FrameRate_Mode, frame_rate_mode',
        'Height, height',
        'ScanOrder, scan_order',
        'ScanType, scan_type',
        'ScanType_StoreMethod, scan_type_store_method',
        'Standard, standard',
        'StreamSize/String1, stream_size',
        'StreamSize, stream_size.bytes',
        'StreamOrder, stream_order',
        'Width, width',
        'Format_Profile, format_profile',
        'Width_CleanAperture, width_aperture'
        'Delay, delay',
        'Format_settings_GOP', 'format_settings_GOP'
    ]

    video_dict = []
    for mdata in data:
        minfo, cid = mdata.split(', ')
        if track.get(minfo):
            video_dict.append({f'video.{cid}': track[minfo]})

    # Handle items with thesaurus look up
    if track.get('CodecID'):
        cn = match_lref('video.codec_id', track['CodecID'])
        if cn:
            video_dict.append({'video.codec_id.lref': cn})
    if track.get('ColorSpace'):
        cn = match_lref('video.colour_space', track['ColorSpace'])
        if cn:
            video_dict.append({'video.colour_space.lref': cn})
    if track.get('Format_Commercial'):
        cn = match_lref('video.commercial_name', track['Format_Commercial'])
        if cn:
            video_dict.append({'video.commercial_name.lref': cn})
    if track.get('DisplayAspectRatio'):
        cn = match_lref('video.display_aspect_ratio', track['DisplayAspectRatio'])
        if cn:
            video_dict.append({'video.display_aspect_ratio.lref': cn})
    if track.get('Format'):
        cn = match_lref('video.format', track['Format'])
        if cn:
            video_dict.append({'video.format.lref': cn})
    if track.get('matrix_coefficients'):
        cn = match_lref('video.matrix_coefficients', track['matrix_coefficients'])
        if cn:
            video_dict.append({'video.matrix_coefficients.lref': cn})
    if track.get('PixelAspectRatio'):
        cn = match_lref('video.pixel_aspect_ratio', track['PixelAspectRatio'])
        if cn:
            video_dict.append({'video.pixel_aspect_ratio.lref': cn})
    if track.get('transfer_characteristics'):
        cn = match_lref('video.transfer_characteristics', track['transfer_characteristics'])
        if cn:
            video_dict.append({'video.transfer_characteristics.lref': cn})
    if track.get('Encoded_Library'):
        cn = match_lref('video.writing_library', track['Encoded_Library'])
        if cn:
            video_dict.append({'video.writing_library.lref': cn})

    # Handle grouped items with no video prefix
    if track['extra'].get('MaxSlicesCount'):
        video_dict.append({'max_slice_count': track['extra'].get('MaxSlicesCount')})
    if track.get('colour_range'):
        video_dict.append({'colour_range': track.get('colour_range')})


def get_image_xml(track):
    '''
    Create dictionary for Image
    metadata required
    JMW - To complete when metadata source identified
    '''
    data = [
        'Duration/String1, duration',
        'Duration, duration.milliseconds',

    ]

    image_dict = []
    for mdata in data:
        minfo, cid = mdata.split(', ')
        if track.get(minfo):
            image_dict.append({f'audio.{cid}': track[minfo]})

    if track.get('CodecID'):
        cn = match_lref('image.commercial_name', track['Format_Commercial'])
        if cn:
            image_dict.append({'image.commercial_name.lref': cn})


def get_audio_xml(track):
    '''
    If audio channel present,
    extract available metadata
    and XML format
    '''
    data = [
        'BitDepth, bit_depth',
        'BitRate_Mode, bit_rate_mode',
        'Channels, channels',
        'CodecID, codec_id',
        'Duration, duration',
        'BitRate, bit_rate',
        'ChannelLayout, channel_layout',
        'ChannelPositions, channel_positions',
        'Compression_Mode, compression_mode',
        'Format_Settings_Endianness, format_settings_endianness',
        'Format_Settings_Sign, format_settings_sign',
        'FrameCount, frame_count',
        'StreamSize/String3, stream_size',
        'StreamSize, stream_size_bytes',
        'StreamOrder, stream_order'
    ]

    audio_dict = []
    for mdata in data:
        minfo, cid = mdata.split(', ')
        if track.get(minfo):
            audio_dict.append({f'audio.{cid}': track[minfo]})

    # Handle lref look up items
    if track.get('Format_Commercial'):
        cn = match_lref('audio.commercial_name', track['Format_Commercial'])
        if cn:
            audio_dict.append({'audio.commercial_name.lref': cn})
    if track.get('Format'):
        data = match_lref('audio.format', track['Format'])
        if data:
            audio_dict.append({'audio.format.lref': data})
    if track.get('SamplingRate'):
        data = match_lref('audio.sampling_rate', track['SamplingRate'])
        if data:
            audio_dict.append({'audio.sampling_rate.lref': data})
    if track.get('Language'):
        data = match_lref('audio.language', track['SamplingRate'])
        if data:
            audio_dict.append({'audio.sampling_rate.lref': data})

    audio_xml = adlib.create_grouped_data('', 'container', audio_dict)
    return audio_xml


def get_other_xml(track):
    '''
    Create dictionary for Other
    metadata required
    '''
    data = [
        'Duration, duration',
        'FrameRate, frame_rate',
        'Type, type',
        'TimeCode_FirstFrame, timecode_first_frame',
        'StreamOrder, stream_order'
    ]

    other_dict = []
    for mdata in data:
        minfo, cid = mdata.split(', ')
        if track.get(minfo):
            other_dict.append({f'other.{cid}': track[minfo]})

    # Handle lref look up items
    if track.get('Format'):
        data = match_lref('other.format', track['Format'])
        if data:
            other_dict.append({'audio.format.lref': data})
    if track.get('Language'):
        data = match_lref('other.language', track['Language'])
        if data:
            other_dict.append({'audio.language.lref': data})


def get_text_xml(track):
    '''
    Create dictionary for Text
    metadata required
    '''
    data = [
        'Duration/String1, duration',
        'StreamOrder, stream_order',
        'Format, format'
    ]

    text_dict = []
    for mdata in data:
        minfo, cid = mdata.split(', ')
        if track.get(minfo):
            text_dict.append({f'audio.{cid}': track[minfo]})

    if track.get('CodecID'):
        cn = match_lref('text.codec_id', track['CodecID'])
        if cn:
            text_dict.append({'text.codec_id.lref': cn})


def clean_up(filename, text_path):
    '''
    Clean up metadata
    '''
    tfp, ep, pp, xp, jp, ep = make_paths(filename)

    if os.path.exists(text_path):
        LOGGER.info("Deleting path: %s", text_path)
        os.remove(text_path)
    if os.path.exists(tfp):
        LOGGER.info("Deleting path: %s", tfp)
        os.remove(tfp)
    if os.path.exists(ep):
        LOGGER.info("Deleting path: %s", ep)
        os.remove(ep)
    if os.path.exists(pp):
        LOGGER.info("Deleting path: %s", pp)
        os.remove(pp)
    if os.path.exists(xp):
        LOGGER.info("Deleting path: %s", xp)
        os.remove(xp)
    if os.path.exists(jp):
        LOGGER.info("Deleting path: %s", jp)
        os.remove(jp)
    if os.path.exists(ep):
        LOGGER.info("Deleting path: %s", ep)
        os.remove(ep)


def make_paths(filename):
    '''
    Make all possible paths
    '''
    text_full_path = os.path.join(MEDIAINFO_PATH, f"{filename}_TEXT_FULL.txt")
    ebu_path = os.path.join(MEDIAINFO_PATH, f"{filename}_EBUCore.txt")
    pb_path = os.path.join(MEDIAINFO_PATH, f"{filename}_PBCore2.txt")
    xml_path = os.path.join(MEDIAINFO_PATH, f"{filename}_XML.xml")
    json_path = os.path.join(MEDIAINFO_PATH, f"{filename}_JSON.json")
    exif_path = os.path.join(MEDIAINFO_PATH, f"{filename}_EXIF.txt")

    return [text_full_path, ebu_path, pb_path, xml_path, json_path, exif_path]


def make_header_data(text_path, filename):
    '''
    Create the header tag data
    '''
    tfp, ep, pp, xp, jp, ep = make_paths(filename)

    text = text_full = ebu = pb = xml = json = exif = ''
    # Processing metadata output for text path
    if os.path.exists(text_path):
        text_dump = utils.read_extract(text_path)
        text = f"<Header_tags><header_tags.parser>MediaInfo text 0</header_tags.parser><header_tags><![CDATA[{text_dump}]]></header_tags></Header_tags>"

    # Processing metadata output for text full path
    if os.path.exists(tfp):
        text_dump = utils.read_extract(tfp)
        text_full = f"<Header_tags><header_tags.parser>MediaInfo text 0 full</header_tags.parser><header_tags><![CDATA[{text_dump}]]></header_tags></Header_tags>"

    # Processing metadata output for ebucore path
    if os.path.exists(ep):
        text_dump = utils.read_extract(ep)
        ebu = f"<Header_tags><header_tags.parser>MediaInfo ebucore 0</header_tags.parser><header_tags><![CDATA[{text_dump}]]></header_tags></Header_tags>"

    # Processing metadata output for pbcore path
    if os.path.exists(pp):
        text_dump = utils.read_extract(pp)
        pb = f"<Header_tags><header_tags.parser>MediaInfo pbcore 0</header_tags.parser><header_tags><![CDATA[{text_dump}]]></header_tags></Header_tags>"

    # Processing metadata output for pbcore path
    if os.path.exists(xp):
        text_dump = utils.read_extract(xp)
        xml = f"<Header_tags><header_tags.parser>MediaInfo xml 0</header_tags.parser><header_tags><![CDATA[{text_dump}]]></header_tags></Header_tags>"

    # Processing metadata output for json path
    if os.path.exists(jp):
        text_dump = utils.read_extract(jp)
        json = f"<Header_tags><header_tags.parser>MediaInfo json 0</header_tags.parser><header_tags><![CDATA[{text_dump}]]></header_tags></Header_tags>"

    # Processing metadata output for special collections exif data
    if os.path.exists(ep):
        text_dump = utils.read_extract(ep)
        exif = f"<Header_tags><header_tags.parser>Exiftool text</header_tags.parser><header_tags><![CDATA[{text_dump}]]></header_tags></Header_tags>"

    payload_data = text + text_full + ebu + pb + xml + json + exif
    return payload_data


def write_payload(priref, payload_data, arg):
    '''
    Payload formatting per mediainfo output
    '''
    payload_head = f"<adlibXML><recordList><record priref='{priref}'>"
    payload_end = "</record></recordList></adlibXML>"
    payload = payload_head + payload_data + payload_end

    record = adlib.post(CID_API, payload, 'media', 'updaterecord')
    if record is None:
        return False
    elif arg in str(record):
        return True
    else:
        return None


if __name__ == '__main__':
    main()
