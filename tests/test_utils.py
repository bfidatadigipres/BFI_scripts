#!/usr/bin/env python3
import csv
import os
import sys
import pytest
sys.path.append(os.environ['CODE'])
import utils

@pytest.mark.skip(reason='no control json file found')
def test_check_control():
    true_response = utils.check_control('black_pearl')
    assert true_response is True
    false_response = utils.check_control('power_off_all')
    assert false_response is False


@pytest.mark.skip(reason='no api credentials')
def test_check_cid():
    true_response = utils.cid_check(os.environ['CID_DATA3'])
    assert true_response is True



@pytest.mark.parametrize("file_extension, expected_output",  [
    ('imp', 'mxf, xml'), ('tar', 'dpx, dcp, dcdm, wav'), 
    ('mxf', 'mxf, 50i, imp'), ('mpg', 'mpeg-1, mpeg-ps'), 
    ('mpeg', 'mpeg-1, mpeg-ps'), ('mp4', 'mp4'), ('mov', 'mov, prores'), 
    ('mkv', 'mkv, dpx'), ('wav', 'wav'), ('tif', 'tif, tiff'), 
    ('tiff', 'tif, tiff'), ('jpg', 'jpg, jpeg'), ('jpeg', 'jpg, jpeg'), 
    ('ts', 'mpeg-ts'), ('srt', 'srt'), ('xml', 'xml, imp'), ('scc', 'scc'),
    ('itt', 'itt'), ('stl', 'stl'), ('cap', 'cap'), ('dxfp', 'dxfp'),
    ('dfxp', 'dfxp'), ('csv', 'csv'), ('pdf', 'pdf'),
    ('', None), # no file extension
    ('<3', None), # invalid file type
    ('pfp', None), # invalid file type
    ('dp', None), # invalid file type
    ('mvp', None) # invlaid file type
])
def test_accepted_file_type(file_extension, expected_output):
    print(f"file_extension={file_extension}")
    print(f"expected_output={expected_output}")
    
    results = utils.accepted_file_type(file_extension)

    if expected_output is None:
        assert expected_output is None

    assert results == expected_output


def test_read_yaml(writing_yaml):

    # given a yaml file

    # when read yaml is called
    result = utils.read_yaml(writing_yaml)

    if result == [{}]:
        expected = [{}]
        assert result == expected

    else:
        expected = {
            "bfi": "British Film Institue",
            "bbc": "British Broadcasting Channel",
            "vue": "vue",
            "odeon": "odeon"
        }

        # then the result is true if the write result is outputted
    assert result == expected

def test_read_csv(writing_csv):

    result = utils.read_csv(writing_csv)

    with open(writing_csv, 'r') as file:
        csv_reader = csv.DictReader(file)
        results_data = [row for row in csv_reader]

    if results_data == []:
        expected = []
        assert results_data == expected

    else:
        expected =  [
        {"film_company": "bfi", "full_name": "British Film Institute"},
        {"film_company": "BBC", "full_name": "British Broadcasting Channel" },
        {"film_company": "vue", "full_name": "vue"},
        {"film_company": "Odeon", "full_name": "Odeon"}    
    ]
        assert results_data == expected
        

@pytest.mark.parametrize("filename, expected_results", [
    ("N_123456_01of01.mkv", True),
    ("C_345678_01of02.mp4", True),
    ("PBL_123456_02of05.ts", True),
    ("SCR?_846573_010f09.ts", True),
    ("Q_345678_01of02.mp", False),
    ("STL_987654_09of20.avi", False),
    (".DS_STORE", False),
    ("N_123456_01of02.gif", False),
    ("PD_376857_03of10.avi", False)
])

def test_check_filename(filename, expected_results):
    # given a filename

    # when check filename is called
    result = utils.check_filename(filename)

    # then the file return if its in the correct format
    assert result == expected_results

@pytest.mark.parametrize("filename, expected_result", [
    ("N_123456_01of01.mkv", (1,1)),
    ("N_123456_01of02.gif", (1,2)),
    ("PBL_123456_02of05.png", (2,5)),
    ("PD_376857_02of10.avi", (2,10)),
    ("STL_987654_09of20.avi", (9,20)),
    ("SCR_845673_01of09.ts", (1,9)),
    ("N_126Q4?_03of03.mkv", (3, 3)),
    ("N_126_03of01.mkv", (None, None)), # invalid format
    ("N_123456_03of01.ts", (None, None)), #part is larger than whole
    ("N_123456_0kof0k.mkv", (None, None)) # illegal characters involved
])

def test_check_part_whole(filename, expected_result):
    # given a file name

    # when check part whole function is called
    result = utils.check_part_whole(filename)

    # resuts the part whole part
    assert result == expected_result


@pytest.mark.parametrize("filename, expected_outcome", [
    ("N_123456_01of01.mkv", "N-123456"),
    ("PBL_123456_02of05.png", "PBL-123456"),
    ("PD_376857_02of10.avi", "PD-376857"),
    ("SCR_846573_01of09.ts", "SCR-846573"),
    ("Q_126_03of01.mkv", False), # invalid format
    ("?shjs_01.avi", False), # invalid format
    ("STL_987654_09of20.avi", False) # invalid format
])

def test_get_object_number(filename, expected_outcome):
    # given a file name

    # when get object is called
    result = utils.get_object_number(filename)

    # return object number and check if the result is the same as expected
    assert result == expected_outcome


@pytest.mark.parametrize("extension_type, expected_output", [
    ('mxf', 'video'),
    ('mkv', 'video'),
    ('mov', 'video'),
    ('mp4', 'video'),
    ('mpg', 'video'),
    ('ts', 'video'),
    ('mpeg', 'video'),
    ('png', 'image'),
    ('gif', 'image'),
    ('jpeg', 'image'),
    ('jpg', 'image'),
    ('tif', 'image'),
    ('pct', 'image'),
    ('tiff', 'image'),
    ('wav', 'audio'),
    ('flac', 'audio'),
    ('mp3', 'audio'),
    ('docx', 'document'),
    ('pdf', 'document'),
    ('txt', 'document'),
    ('doc', 'document'),
    ('tar', 'document'),
    ('srt', 'document'),
    ('scc', 'document'),
    ('itt', 'document'),
    ('stl', 'document'),
    ('stl', 'document'),
    ('cap', 'document'),
    ('dxfp', 'document'),
    ('xml', 'document'),
    ('dfxp', 'document'),
    ('bashrc', None), # not a mime type
    ('DS_STORE', None), # not a mime type
    (' ', None), # no extension
    ('', None), # no extension
    ('s', None), # invalid extension path
    ('jdhbfjdbjdbjd', None) # invalid extension path

])

def test_sort_ext(extension_type, expected_output):
    # given an extension

    # when the sort_ext function called
    result = utils.sort_ext(extension_type)

    # assert the file type to expected -> true
    assert result is expected_output


@pytest.mark.parametrize("stream, args, expected_result",[
('Video', 'Duration', '10000.000000'),
('Video', 'BitRate', '1781489'),
('Video', 'Width', '720'),
('Video', 'Height', '576')])
def test_get_metadata(stream, args, expected_result):
    # given a file name
    file_name = "tests/MKV_sample.mkv"

    # when get metadata is called
    result = utils.get_metadata(stream, args, file_name)


    # we should get duration
    assert result == expected_result

@pytest.mark.parametrize("dpath, policy, outcome", [
    ("tests/MKV_sample.mkv", "tests/test_policy.xml", (True, 'pass! tests/MKV_sample.mkv\n'))
])
def test_get_mediaconch(dpath, policy, outcome):
    result = utils.get_mediaconch(dpath=dpath, policy=policy)

    assert result == outcome

@pytest.mark.parametrize("file_name, expected_results", 
[
    ("tests/MKV_sample.mkv", "10.000000")
]
)
def test_get_ms(file_name, expected_results):
    # given a file name
    # when get ms is called
    result = utils.get_ms(file_name)


    # we should get duration in ms
    assert result == expected_results


@pytest.mark.parametrize("file_name, expected_results", 
[
    ("tests/MKV_sample.mkv", "0:00:10.000000")
]
)
def test_get_duration(file_name, expected_results):
    # given a file name
    # when get ms is called
    result = utils.get_duration(file_name)


    # we should get duration in ms
    assert result == expected_results

@pytest.mark.parametrize("file_name, expected_results", 
[
    ("tests/MKV_sample.mkv", "a249fba2c4a44a9354d2c3d6d0805dd6"),
    ("", None)
]
)
def test_create_md5_65536(file_name, expected_results):
    # given a file name/ folder 

    # when get md5 65536 is called
    result = utils.create_md5_65536(file_name)


    # we should get hash value of the file
    assert result == expected_results

@pytest.mark.parametrize("input, expected_output", [
    ("", None),
    ("tests/MKV_sample.mkv", 8149026)
    ])
def test_get_size(input, expected_output):
    
    result = utils.get_size(input)

    assert result == expected_output
    
@pytest.mark.slow
@pytest.mark.parametrize("filename, message, expected_output", [
    ("N_10307017_01of01.mkv", "Successfully deleted file", None),
     ("N_10306783_01of01.mkv", "Successfully deleted file", None)
    ])
def test_check_global_logs(filename, message, expected_output):
    
    result = utils.check_global_log(filename, message)

    assert result == expected_output

# PYTHONPATH=$(pwd) pytest -s -vv test/test_utils.py