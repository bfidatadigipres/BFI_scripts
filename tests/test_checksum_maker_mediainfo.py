import datetime
import os

import pytest

# custom libraries
from hashes import checksum_maker_mediainfo as cmm

TODAY = str(datetime.date.today())


@pytest.mark.parametrize(
    "checksum_value, filepath, filename",
    [
        ("a249fba2c4a44a9354d2c3d6d0805dd6", "tests/MKV_sample.mkv", "MKV_sample.mkv"),
        ("a249fba2c4a44a9354d2c3d6d0805dd6", "MKV_samplea.mkv", "MKV_samplea.mkv"),
    ],
)
def test_checksum_write(creating_checksum_path, checksum_value, filepath, filename):
    """
    Tests the checksum_write function.

    This test checks the behaviour of the function that checks if the
    checksum data in the file is the correctly written into
    a md5 file. It uses paramterized inputs to validate various of cases.

    Notes:
    ------
    Creating the checksum_path is specified in conftest.py using tmp_path, to prevent creating an actual
    file
    """
    checksum_file = cmm.checksum_write(
        creating_checksum_path, checksum_value, filepath, filename
    )
    with open(checksum_file, "r") as file_read:
        result = file_read.readlines()

    assert result[0] == f"{checksum_value} - {filepath} - {TODAY}"
    assert checksum_file.exists()


@pytest.mark.parametrize(
    "filename, checksum",
    [
        ("MKV_sample.mkv", "a249fba2c4a44a9354d2c3d6d0805dd6"),
        ("MKV_sample.img", "abcdef5h789jn"),
    ],
)
def test_checksum_exist(mocker, filename, checksum, tmp_path):
    """
    Tests the checksum_exist function.

    This test checks the behaviour of the function that checks if the
    checksum data in the file is the correctly written into
    a md5 file and returns a path. It uses paramterized inputs to validate various of cases.
    Mocker is also used to 'mock' the tenacity decorator behaviour.

    """
    filepath = tmp_path / filename

    mocker.patch("tenacity.retry", lambda x: x)
    checksum_file = cmm.checksum_exist(tmp_path, filename, checksum, filepath)

    print(checksum_file)

    assert os.path.exists(checksum_file)


@pytest.mark.parametrize(
    "filepath, filename, expected_outcome",
    [
        ("tests/MKV_sample.mkv", "MKV_sample.mkv", "a249fba2c4a44a9354d2c3d6d0805dd6"),
        ("tests/sample.mkv", "sample.mkv", None),
    ],
)
def test_make_output_md5(filepath, filename, expected_outcome):
    """
    Tests make_output_md5 function

    this test checks the behaviour of the funtion where given a valid filename, should return
    the checksum value. It uses paramterized inputs to validate various of cases.

    """
    results = cmm.make_output_md5(filepath, filename)

    assert results == expected_outcome


@pytest.mark.parametrize(
    "arg, output_type, filepath, expected_filename",
    [
        ("-f", "TEXT", "tests/MKV_sample.mkv", "MKV_sample.mkv_TEXT_FULL.txt"),
        ("", "TEXT", "tests/file2.mp4", "file2.mp4_TEXT.txt"),
        ("", "JSON", "tests/bilb.mp4", "bilb.mp4_JSON.json"),
        ("", "EBUCore", "tests/N_123_4.mp4", "N_123_4.mp4_EBUCore.xml"),
    ],
)
def test_mediainfo_create(
    mocker, arg, output_type, filepath, expected_filename, tmp_path
):
    """
    Tests mediainfo_create function

    This test checks the behaviour of the function where given a valid filepath, argument for
    mediainfo, and output_type, should return
    the metadata of the file in different files for CID.
    It uses paramterized inputs to validate various of cases.

    """

    mock_call = mocker.patch("subprocess.call", return_value=0)
    mocker.patch("tenacity.retry", lambda x: x)

    result = cmm.mediainfo_create(arg, output_type, filepath, tmp_path)

    expected_outcome = tmp_path / expected_filename

    print(expected_outcome)
    assert result == str(expected_outcome)


@pytest.mark.parametrize(
    "check, file_content, expected_outcome",
    [
        ("checksum_file.mkv.md5", "", None),
        ("files.img", "?", None),
        ("files.wmv", "as2accdefbn98m", None),
        ("samples.mkv.md5", "None", True),
    ],
)
def test_checksum_test(tmp_path, check, file_content, expected_outcome):
    """
    Tests checksum_test function

    This test checks the behaviour of the function where given a valid filepath and filename,
    should return True if the file starts with 'None'.
    It uses paramterized inputs to validate various of cases.

    """

    check_sum_foler = tmp_path / "checksum_folder"
    check_sum_foler.mkdir()
    file = check_sum_foler / check
    file.write_text(file_content)

    results = cmm.checksum_test(check_sum_foler, file)

    assert results == expected_outcome


# create a temp folder path and check if the file exists
@pytest.mark.parametrize("file_names", [("MKV_sample.mkv"), ("make_temp_file.jpg")])
def test_make_metadata(tmp_path, mocker, file_names):
    """
    Tests make_metadata function

    This test checks the behaviour of the function where given a directory, filename and
    checksum_directory, the different format of media file's metadata
    is written to a specific directory. It uses paramterized inputs to validate various of cases.

    """

    temp_dir_location = tmp_path / "moving_folder"
    temp_dir_location.mkdir()

    test_file = tmp_path / file_names
    if file_names == "make_temp_file.jpg":
        test_file.write_text("dummy data")

    temp_dir_metadata = tmp_path / "metadata_folder"
    temp_dir_metadata.mkdir()

    mocker.patch("tenacity.retry", lambda x: x)

    cmm.make_metadata(str(temp_dir_location), test_file.name, str(temp_dir_metadata))

    print(list(temp_dir_metadata.iterdir()))

    assert temp_dir_location.is_dir()

    expected_out = [
        "TEXT_FULL.txt",
        "TEXT.txt",
        "EBUCore.xml",
        "PBCore2.xml",
        "XML.xml",
        "JSON.json",
    ]

    for fmt in expected_out:
        meta_file = temp_dir_metadata / f"{test_file.name}_{fmt}"

        assert meta_file.exists(), f"{list(temp_dir_metadata.iterdir())}"
