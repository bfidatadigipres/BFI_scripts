import pytest
import datetime
import os

# custom libraries
from hashes import checksum_maker_mediainfo as cmm

TODAY = str(datetime.date.today())


@pytest.mark.parametrize("checksum_value, filepath, filename", [
    ('a249fba2c4a44a9354d2c3d6d0805dd6', 'tests/MKV_sample.mkv', 'MKV_sample.mkv'),
    ('a249fba2c4a44a9354d2c3d6d0805dd6', 'MKV_samplea.mkv', 'MKV_samplea.mkv')
])
def test_checksum_write(creating_checksum_path, checksum_value, filepath, filename):
    checksum_file = cmm.checksum_write(creating_checksum_path, checksum_value, filepath, filename)
    with open(checksum_file, 'r') as file_read:
        result = file_read.readlines()
        
    assert result[0] == f'{checksum_value} - {filepath} - {TODAY}'
    assert checksum_file.exists()

@pytest.mark.parametrize("filename, checksum, filepath", [
    ('MKV_sample.mkv', 'a249fba2c4a44a9354d2c3d6d0805dd6', 'tests/MKV_sample.mkv')
])
def test_checksum_exist(mocker, filename, checksum, filepath):

    mocker.patch('tenacity.retry', lambda x : x)
    checksum_file = cmm.checksum_exist(filename, checksum, filepath)

    # mock opening a function and checks if the same is the same
    with open(checksum_file, 'r') as file_read:
        result = file_read.readlines()
        
    assert result[0] == f'{checksum} - {filepath} - {TODAY}'



# do some logging testing
@pytest.mark.parametrize("filepath, filename, expected_outcome", [
    ('tests/MKV_sample.mkv', 'MKV_sample.mkv', 'a249fba2c4a44a9354d2c3d6d0805dd6'),
    ('tests/sample.mkv', 'sample.mkv', None)
])
def test_make_output_md5(filepath, filename, expected_outcome):
    results = cmm.make_output_md5(filepath, filename)

    assert results == expected_outcome

@pytest.mark.parametrize("arg, output_type, filepath, expected_filename", [
    ("-f", 'TEXT', 'tests/MKV_sample.mkv', 'MKV_sample.mkv_TEXT_FULL.txt'),
    ("", 'TEXT', 'tests/file2.mp4','file2.mp4_TEXT.txt')
])
def test_mediainfo_create(mocker, arg, output_type, filepath, expected_filename, tmp_path):

    mock_call = mocker.patch('subprocess.call', return_value=0)
    mocker.patch('tenacity.retry', lambda x : x)

    result = cmm.mediainfo_create(arg, output_type, filepath, tmp_path)

    expected_outcome = tmp_path / expected_filename


    assert result == str(expected_outcome)


@pytest.mark.parametrize('check, file_content, expected_outcome', [
    ('checksum_file.mkv.md5', '', None),
    ('files.img', '?', None),
    ('samples.mkv.md5', 'None', True)
])
def test_checksum_test(tmp_path, check, file_content, expected_outcome):
    check_sum_foler = tmp_path / 'checksum_folder'
    check_sum_foler.mkdir()
    file = check_sum_foler / check
    file.write_text(file_content)

    results = cmm.checksum_test(check_sum_foler, file)

    assert results == expected_outcome


# create a temp folder path and check if the file exists
@pytest.mark.parametrize('file_names', [
    ('MKV_sample.mkv')
])
def test_make_metadata(tmp_path, mocker, file_names):
    temp_dir_location = tmp_path / 'moving_folder'
    temp_dir_location.mkdir()

    temp_dir_metadata = tmp_path / 'metadata_folder'
    temp_dir_metadata.mkdir()

    
    mocker.patch('tenacity.retry', lambda x : x)
    cmm.make_metadata(temp_dir_location, file_names, temp_dir_metadata)

    assert temp_dir_location.is_dir()

    expected_format = ['txt', 'xml', 'json']

    for fmt in expected_format:
        assert (temp_dir_metadata / f"{file_names}.{fmt}").exists(), f"missing {fmt} metadata file"
