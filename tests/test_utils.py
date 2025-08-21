#!/usr/bin/env python3
import csv
import io
import json
import os
import subprocess
import sys

import pytest

sys.path.append(os.environ["CODE"])

# custom import
import utils


@pytest.mark.parametrize(
    "input, expected_output",
    [
        ("black_pearl", True),
        ("power_off_all", True),
        ("/mnt/isilon/film_operations/Finished", False),
    ],
)
def test_check_control(input, expected_output):
    """
    Tests the check control function.

    This test checks the behaviour of the function that verifies the ability to retrieve
    the correct value associated with a given key.
    It uses paramterized inputs to validate various of cases where:
    - the key exists and return the value

    """
    json_response = utils.check_control(input)
    assert json_response is expected_output


@pytest.mark.skip(reason="no api credentials")
def test_check_cid():
    true_response = utils.cid_check(os.environ["CID_DATA3"])
    assert true_response is True


@pytest.mark.parametrize(
    "file_extension, expected_output",
    [
        ("imp", "mxf, xml"),
        ("tar", "dpx, dcp, dcdm, wav"),
        ("mxf", "mxf, 50i, imp"),
        ("mpg", "mpeg-1, mpeg-ps"),
        ("mpeg", "mpeg-1, mpeg-ps"),
        ("mp4", "mp4"),
        ("mov", "mov, prores"),
        ("mkv", "mkv, dpx"),
        ("wav", "wav"),
        ("tif", "tif, tiff"),
        ("tiff", "tif, tiff"),
        ("jpg", "jpg, jpeg"),
        ("jpeg", "jpg, jpeg"),
        ("ts", "mpeg-ts"),
        ("srt", "srt"),
        ("xml", "xml, imp"),
        ("scc", "scc"),
        ("itt", "itt"),
        ("stl", "stl"),
        ("cap", "cap"),
        ("dxfp", "dxfp"),
        ("dfxp", "dfxp"),
        ("csv", "csv"),
        ("pdf", "pdf"),
        ("", None),  # no file extension
        ("<3", None),  # invalid file type
        ("pfp", None),  # invalid file type
        ("dp", None),  # invalid file type
        ("mvp", None),  # invlaid file type
    ],
)
def test_accepted_file_type(file_extension, expected_output):
    """
    Tests the accepted file type function.

    This test checks the behaviour of the function that checks
    the specific file extension is a valid type.
    It uses paramterized inputs to validate various of cases where:
    - the file_extension is valid
    - the file_extension is not valid

    """
    print(f"file_extension={file_extension}")
    print(f"expected_output={expected_output}")

    results = utils.accepted_file_type(file_extension)

    if expected_output is None:
        assert expected_output is None

    assert results == expected_output


def test_read_yaml(writing_yaml):
    """
    Tests the read_yaml function.

    This test checks the behaviour of the function that checks if the data in the
    yaml file is the correctly written into
    the file. It uses paramterized inputs to validate various of cases.

    Notes:
    ------
    Creating the file is specified in conftest.py using tmp_path, to prevent creating an actual
    file
    """

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
            "odeon": "odeon",
        }

        # then the result is true if the write result is outputted
    assert result == expected


def test_read_csv(writing_csv):
    """
    Tests the read_csv function.

    This test checks the behaviour of the function that checks if the data in the
    csv file is the correctly written into
    the file. It uses paramterized inputs to validate various of cases.

    Notes:
    ------
    Creating the file is specified in conftest.py using tmp_path, to prevent creating an actual
    file
    """

    result = utils.read_csv(writing_csv)

    with open(writing_csv, "r") as file:
        csv_reader = csv.DictReader(file)
        results_data = [row for row in csv_reader]

    if results_data == []:
        expected = []
        assert results_data == expected

    else:
        expected = [
            {"film_company": "bfi", "full_name": "British Film Institute"},
            {"film_company": "BBC", "full_name": "British Broadcasting Channel"},
            {"film_company": "vue", "full_name": "vue"},
            {"film_company": "Odeon", "full_name": "Odeon"},
        ]
        assert results_data == expected


def test_read_extract(writing_txt):
    result = utils.read_extract(writing_txt)

    if result == "":
        expected = ""
        assert result == expected

    else:
        expected = "hello world!"
        assert result == expected


@pytest.mark.parametrize(
    "filename, expected_results",
    [
        ("N_123456_01of01.mkv", True),
        ("C_345678_01of02.mp4", True),
        ("PBL_123456_02of05.ts", True),
        ("SCR_846573_010f09.ts", True),
        ("Q_345678_01of02.mp", False),
        ("STL_987654_09of20.avi", False),
        (".DS_STORE", False),
        ("N_123456_01of02.gif", False),
        ("PD_376857_03of10.avi", False),
    ],
)
def test_check_filename(filename, expected_results):
    """
    Tests the check_filename function.

    This test checks the behaviour of the function that the filename supplied is
    in the correct format. It uses paramterized inputs to validate various of cases.

    Examples:
    >>> test_check_filename(".DS_STORE", False) # not correct format
    >>> test_check_filename("N_123456_01of01.mkv", True) # in correct format

    """
    # given a filename

    # when check filename is called
    result = utils.check_filename(filename)

    # then the file return if its in the correct format
    assert result == expected_results


@pytest.mark.parametrize(
    "filename, expected_result",
    [
        ("N_123456_01of01.mkv", (1, 1)),
        ("N_123456_01of02.gif", (1, 2)),
        ("PBL_123456_02of05.png", (2, 5)),
        ("PD_376857_02of10.avi", (2, 10)),
        ("STL_987654_09of20.avi", (9, 20)),
        ("SCR_845673_01of09.ts", (1, 9)),
        ("N_126Q4?_03of03.mkv", (3, 3)),
        ("N_126_03of01.mkv", (None, None)),  # invalid format
        ("N_123456_03of01.ts", (None, None)),  # part is larger than whole
        ("N_123456_0kof0k.mkv", (None, None)),  # illegal characters involved
    ],
)
def test_check_part_whole(filename, expected_result):
    """
    Tests the check_part_whole function.

    This test checks the behaviour of the function that filename supplied is
    in the correct format and extract the whole and part
    . It uses paramterized inputs to validate various of cases.

    Examples:
    -------
    >>> test_check_part_whole("N_126_03of01.mkv", (None, None)) # part larger than whole
    >>> test_check_part_whole("N_126Q4?_03of03.mkv", (3, 3)) # valid

    Notes:
    -----
    For cases where the part is greater than the whole, it returns None,None

    """
    # given a file name

    # when check part whole function is called
    result = utils.check_part_whole(filename)

    # resuts the part whole part
    assert result == expected_result


@pytest.mark.parametrize(
    "filename, expected_outcome",
    [
        ("N_123456_01of01.mkv", "N-123456"),
        ("PBL_123456_02of05.png", "PBL-123456"),
        ("PD_376857_02of10.avi", "PD-376857"),
        ("SCR_846573_01of09.ts", "SCR-846573"),
        ("Q_126_03of01.mkv", False),  # invalid format
        ("?shjs_01.avi", False),  # invalid format
        ("STL_987654_09of20.avi", False),  # invalid format
    ],
)
def test_get_object_number(filename, expected_outcome):
    """
    Tests the get_object_number function.

    This test checks the behaviour of the function that filename supplied is
    in the correct format and extract the object number.
    It uses paramterized inputs to validate various of cases.

    Examples:
    ---------
    >>> test_get_object_number(Q_126_03of01.mkv, False) # not in a valid format
    >>> test_get_object_number(N_123456_01of01.mkv, N-123456) # first half in correct format
    """
    # given a file name

    # when get object is called
    result = utils.get_object_number(filename)

    # return object number and check if the result is the same as expected
    assert result == expected_outcome


@pytest.mark.parametrize(
    "extension_type, expected_output",
    [
        ("mxf", "video"),
        ("mkv", "video"),
        ("mov", "video"),
        ("mp4", "video"),
        ("mpg", "video"),
        ("ts", "video"),
        ("mpeg", "video"),
        ("png", "image"),
        ("gif", "image"),
        ("jpeg", "image"),
        ("jpg", "image"),
        ("tif", "image"),
        ("pct", "image"),
        ("tiff", "image"),
        ("wav", "audio"),
        ("flac", "audio"),
        ("mp3", "audio"),
        ("docx", "document"),
        ("pdf", "document"),
        ("txt", "document"),
        ("doc", "document"),
        ("tar", "document"),
        ("srt", "document"),
        ("scc", "document"),
        ("itt", "document"),
        ("stl", "document"),
        ("stl", "document"),
        ("cap", "document"),
        ("dxfp", "document"),
        ("xml", "document"),
        ("dfxp", "document"),
        ("bashrc", None),  # not a mime type
        ("DS_STORE", None),  # not a mime type
        (" ", None),  # no extension
        ("", None),  # no extension
        ("s", None),  # invalid extension path
        ("jdhbfjdbjdbjd", None),  # invalid extension path
    ],
)
def test_sort_ext(extension_type, expected_output):
    """
    Tests the sort_ext function.

    This test checks the behaviour of the function where the extension_type supplied
    and returns the mime type (document, audio, image, video).
    It uses paramterized inputs to validate various of cases.

    Examples:
    ---------
    >>> test_sort_ext(mkv, Video) # not in a valid format
    >>> test_sort_ext(<3, None) # not a mime type
    """
    # given an extension

    # when the sort_ext function called
    result = utils.sort_ext(extension_type)

    # assert the file type to expected -> true
    assert result is expected_output


def test_exif_data(mocker):
    """
    Tests the exif_data function.

    This test checks the behaviour of the function where the filename is supplied
    and returns the the file's metadata.
    It uses mocking and patching to replicate and isolate the command line process
    for testing purposes.

    """

    mock_output = (
        b"ExifTool Version Number         : 11.88\n"
        b"File Name                       : MKV_sample.mkv\n"
        b"Directory                       : tests\n"
        b"File Size                       : 7.8 MB\n"
        b"File Modification Date/Time     : 2024:10:31 11:08:20+00:00\n"
        b"File Access Date/Time           : 2024:11:12 13:20:32+00:00\n"
        b"File Inode Change Date/Time     : 2024:11:06 09:16:37+00:00\n"
        b"File Permissions                : rwxrwxrwx\n"
        b"File Type                       : MKV\n"
        b"File Type Extension             : mkv\n"
        b"MIME Type                       : video/x-matroska\n"
        b"EBML Version                    : 1\n"
        b"EBML Read Version               : 1\n"
        b"Doc Type                        : matroska\n"
        b"Doc Type Version                : 4\n"
        b"Doc Type Read Version           : 2\n"
        b"Timecode Scale                  : 1 ms\n"
        b"Muxing App                      : Lavf58.76.100\n"
        b"Writing App                     : Lavf58.76.100\n"
        b"Duration                        : 10.00 s\n"
        b"Codec ID                        : A_PCM/INT/LIT\n"
        b"Track Default                   : No\n"
        b"Audio Codec ID                  : A_PCM/INT/LIT\n"
        b"Audio Channels                  : 2\n"
        b"Audio Sample Rate               : 48000\n"
        b"Audio Bits Per Sample           : 24\n"
        b"Track Number                    : 3\n"
        b"Track Language                  : eng\n"
        b"Track Type                      : Video\n"
        b"Video Frame Rate                : 25\n"
        b"Video Codec ID                  : V_MS/VFW/FOURCC\n"
        b"Image Width                     : 720\n"
        b"Image Height                    : 576\n"
        b"Display Width                   : 20\n"
        b"Display Height                  : 11\n"
        b"Display Unit                    : Unknown (3)\n"
        b"Tag Name                        : DURATION\n"
        b"Tag String                      : 00:00:10.000000000\n"
        b"Image Size                      : 720x576\n"
        b"Megapixels                      : 0.415\n"
    )
    mocker.patch("subprocess.check_output", return_value=mock_output)
    # mocker.patch("subprocess.check_output", side_effect=subprocess.CalledProcessError(1, 'exiftool'))

    result = utils.exif_data("tests/MKV_sample.mkv")

    assert "File Name                       : MKV_sample.mkv\n" in result

    subprocess.check_output.assert_called_with(["exiftool", "tests/MKV_sample.mkv"])


@pytest.mark.parametrize(
    "stream, args, expected_result",
    [
        ("Video", "Duration", b"10000.000000"),
        ("Video", "BitRate", b"1781489"),
        ("Video", "Width", b"720"),
        ("Video", "Height", b"576"),
    ],
)
def test_get_metadata(mocker, stream, args, expected_result):
    """
    Tests the get_metadat function.

    This test checks the behaviour of the function where the stream,
    args and filename are supplied and returns the metadata of the file.
    It uses mocking and patching to replicate and isolate the command line process
    for testing purposes.

    """
    # given a file name
    file_name = "tests/MKV_sample.mkv"

    mocker.patch("subprocess.check_output", return_value=expected_result)

    # when get metadata is called
    result = utils.get_metadata(stream, args, file_name)

    # we should get duration
    assert result == expected_result.decode("latin-1")

    subprocess.check_output.assert_called_with(
        [
            "mediainfo",
            "--Full",
            "--Language=raw",
            f"--Output={stream};%{args}%",
            file_name,
        ]
    )


@pytest.mark.parametrize(
    "dpath, policy, outcome",
    [
        (
            "tests/MKV_sample.mkv",
            "tests/test_policy.xml",
            (True, "pass! tests/MKV_sample.mkv\n"),
        )
    ],
)
def test_get_mediaconch(dpath, policy, outcome):
    """
    Tests the get_mediaconch function.

    This test checks the behaviour of the function where the policy and filename are supplied
    and checks for 'pass! {path}' in mediaconch reponse.
    It uses paramterized inputs to validate various of cases.

    """
    result = utils.get_mediaconch(dpath=dpath, policy=policy)

    assert result == outcome


@pytest.mark.parametrize(
    "file_name, expected_results", [("tests/MKV_sample.mkv", "10.000000")]
)
def test_get_ms(file_name, expected_results):
    """
    Tests the get_ms function.

    This test checks the behaviour of the function where the filename is supplied
    and returns the duration of the file in milliseconds.
    It uses paramterized inputs to validate various of cases.

    """
    # given a file name
    # when get ms is called
    result = utils.get_ms(file_name)

    # we should get duration in ms
    assert result == expected_results


@pytest.mark.parametrize(
    "file_name, expected_results", [("tests/MKV_sample.mkv", "0:00:10.000000")]
)
def test_get_duration(file_name, expected_results):
    """
    Tests the get_duration function.

    This test checks the behaviour of the function where the filename is supplied
    and returns the duration of the file.
    It uses paramterized inputs to validate various of cases.

    """
    # given a file name
    # when get ms is called
    result = utils.get_duration(file_name)

    # we should get duration in ms
    assert result == expected_results


@pytest.mark.parametrize(
    "file_name, expected_results",
    [("tests/MKV_sample.mkv", "a249fba2c4a44a9354d2c3d6d0805dd6"), ("", None)],
)
def test_create_md5_65536(file_name, expected_results):
    """
    Tests the create_md5_65536 function.

    This test checks the behaviour of the function where the filename is supplied
    and returns the checksum_value of the file.
    It uses paramterized inputs to validate various of cases.

    Note:
    -----
    For empty files, it should return None.

    """
    # given a file name/ folder

    # when get md5 65536 is called
    result = utils.create_md5_65536(file_name)

    # we should get hash value of the file
    assert result == expected_results


@pytest.mark.parametrize(
    "filename, expected_output", [("", None), ("tests/MKV_sample.mkv", 8149026)]
)
def test_get_size(filename, expected_output):
    """
    Tests the get_size function.

    This test checks the behaviour of the function where the filename is supplied
    and returns the size of the file.
    It uses paramterized inputs to validate various of cases.

    Note:
    -----
    For empty files, it should return None.
    """

    result = utils.get_size(filename)

    assert result == expected_output


@pytest.mark.slow
@pytest.mark.parametrize(
    "filename, message, expected_output",
    [
        ("N_10307017_01of01.mkv", "Successfully deleted file", None),
        ("N_10306783_01of01.mkv", "Successfully deleted file", None),
    ],
)
def test_check_global_logs(filename, message, expected_output):
    """
    Tests the check_global_logs function.

    This test checks the behaviour of the function where the filename and message are supplied
    and returns a value if its present in the global logs.
    It uses paramterized inputs to validate various of cases.

    Note:
    -----
    This function takes a while to run.

    """

    result = utils.check_global_log(filename, message)

    assert result == expected_output


def test_probe_metadata():
    result = utils.probe_metadata("height", "video", "tests/MKV_sample.mkv")

    # assert the file type to expected -> true
    assert result == 576


def test_checksum_write(tmp_path):
    checksum_filepath = tmp_path / "sample.md5"

    path = utils.checksum_write(checksum_filepath, "...", "filename.mkv", "2025-05-14")

    assert path == checksum_filepath
    assert checksum_filepath.exists()


@pytest.mark.parametrize(
    "arg, output_type, out_pth_ext",
    [
        ("", "XML", ".xml"),
        ("", "EBUCore", ".xml"),
        ("", "PBCore", ".xml"),
        ("", "TEXT", ".txt"),
        ("-f", "TEXT", "_FULL.txt"),
        ("-f", "JSON", ".json"),
    ],
)
def test_mediainfo_create(
    mocker, arg, create_mediainfo_folder, output_type, out_pth_ext
):
    filename = os.path.basename(create_mediainfo_folder)
    dirname = os.path.dirname(create_mediainfo_folder)
    out_filename = f"{filename}_{output_type}{out_pth_ext}"

    out_path = os.path.join(dirname, out_filename)

    def fake_subprocess_call(cmd):
        os.makedirs(dirname, exist_ok=True)
        with open(out_path, "w") as f:
            f.write("<xml>dummy</xml>")
        return 0

    mocker_subprocess = mocker.patch(
        "subprocess.call", side_effect=fake_subprocess_call
    )

    result = utils.mediainfo_create(arg, output_type, create_mediainfo_folder, dirname)

    assert os.path.exists(result)
    assert result == out_path
    assert os.path.getsize(result) > 0
    if arg:
        mocker_subprocess.assert_called_once_with(
            [
                "mediainfo",
                arg,
                "--Details=0",
                f"--Output={output_type}",
                f"--LogFile={out_path}",
                create_mediainfo_folder,
            ]
        )
    else:
        mocker_subprocess.assert_called_once_with(
            [
                "mediainfo",
                "--Details=0",
                f"--Output={output_type}",
                f"--LogFile={out_path}",
                create_mediainfo_folder,
            ]
        )


@pytest.mark.parametrize(
    "fpath, fname, expected_results",
    [
        ("tests/", "MKV_sample.mkv", "tests/MKV_sample.mkv"),
        ("tests/", "non_existent_file.txt", None),
        ("no_existing_folder/", "file_exists.md5", None),
    ],
)
def test_local_file_search(fpath, fname, expected_results):
    """
    Tests 'local_file_search' function from utils.py

    This tests validates that the function correctly finds the file.

    Parameters:
    -----------
    fpath: str
        folder/file to look into
    fname: str
        the filename
    expected_results: str
        the expected outcome.
    """
    outcome = utils.local_file_search(fpath, fname)

    assert outcome == expected_results


def test_send_email(mocker, writing_csv):
    """
    Tests 'send_email' function in utils.py

    This test validates that the function sends the email
    with the txt file(under the limit).

    Parameters:
    -----------
    mocker: unittest.mocker
        built in mocker that mocks any behaviour.
    writing_csv: str
        the path to the temporary csv file.
    """
    # create an file
    mock_smtp = mocker.patch("smtplib.SMTP_SSL", autospec=True)
    mock_smtp_instance = mocker.MagicMock()
    mock_smtp.return_value.__enter__.return_value = mock_smtp_instance

    mocker.patch("utils.EMAIL", "test@bfi.org.uk")
    mocker.patch("utils.PASSWORD", "dumb_pass")
    mocker.patch("utils.SMTP_SERVER", "smtp.test.com")
    mocker.patch("utils.SMTP_PORT", 465)

    utils.send_email("reciver@email.com", "Test subject", "Test_body", writing_csv)
    mock_smtp_instance.login.assert_called_once_with("test@bfi.org.uk", "dumb_pass")
    mock_smtp_instance.sendmail.assert_called_once()
    args = mock_smtp_instance.sendmail.call_args[0]
    assert "Test subject" in args[2]


def test_send_email_oversized(mocker, oversized_file):
    """
    Tests 'send_email' function in utils.py

    This test validates that the function sends the email
    with an oversized file.

    Parameters:
    -----------
    mocker: unittest.mocker
        built in mocker that mocks any behaviour.
    oversized_file: str
        the path to the temporary file.
    """
    # create an file
    mock_smtp = mocker.patch("smtplib.SMTP_SSL", autospec=True)
    mock_smtp_instance = mocker.MagicMock()
    mock_smtp.return_value.__enter__.return_value = mock_smtp_instance

    mocker.patch("utils.EMAIL", "test@bfi.org.uk")
    mocker.patch("utils.PASSWORD", "dumb_pass")
    mocker.patch("utils.SMTP_SERVER", "smtp.test.com")
    mocker.patch("utils.SMTP_PORT", 465)

    utils.send_email("reciver@email.com", "Test subject", "Test_body", oversized_file)
    mock_smtp_instance.login.assert_called_once_with("test@bfi.org.uk", "dumb_pass")
    mock_smtp_instance.sendmail.assert_called_once()
    args = mock_smtp_instance.sendmail.call_args[0]
    assert "Test subject" in args[2]


def test_send_email_txt(mocker, writing_txt):
    """
    Tests 'send_email' function in utils.py

    This test validates that the function sends the email
    with the txt file(under the limit).

    Parameters:
    -----------
    mocker: unittest.mocker
        built in mocker that mocks any behaviour.
    writing_txt: str
        the path to the txt file.
    """
    # create an file
    mock_smtp = mocker.patch("smtplib.SMTP_SSL", autospec=True)
    mock_smtp_instance = mocker.MagicMock()
    mock_smtp.return_value.__enter__.return_value = mock_smtp_instance

    mocker.patch("utils.EMAIL", "test@bfi.org.uk")
    mocker.patch("utils.PASSWORD", "dumb_pass")
    mocker.patch("utils.SMTP_SERVER", "smtp.test.com")
    mocker.patch("utils.SMTP_PORT", 465)

    utils.send_email("reciver@email.com", "Test subject", "Test_body", writing_txt)
    mock_smtp_instance.login.assert_called_once_with("test@bfi.org.uk", "dumb_pass")
    mock_smtp_instance.sendmail.assert_called_once()
    args = mock_smtp_instance.sendmail.call_args[0]
    assert "Test subject" in args[2]


def test_get_current_api_failed(mocker):
    """
    testing get_current_api() function in utils where the json file doesnt exists
    """
    mocker.patch("json.load", side_effect=FileNotFoundError)

    result = utils.get_current_api()

    assert result is None


def test_get_current_api_no_env(mocker):
    """
    testing get_current_api() function in utils where the environmental variable
    doesnt exists
    """
    fake_json = {"current_api": "MY_API_KEYT"}

    mocker_control_json = io.StringIO(json.dumps(fake_json))
    mocker.patch("builtins.open", return_value=mocker_control_json)

    result = utils.get_current_api()
    assert result is None


def test_get_current_api_found(mocker):
    """
    testing get_current_api() function in utils
    """
    fake_json = {"current_api": "MY_API_KEYT"}
    mocker.patch("os.environ", {"MY_API_KEYT": "dummy_data"})

    mocker_control_json = io.StringIO(json.dumps(fake_json))
    mocker.patch("builtins.open", return_value=mocker_control_json)

    result = utils.get_current_api()
    assert result == "dummy_data"


@pytest.mark.parametrize(
    "time_input, bool_input, expected_outcome",
    [
        ("2023-10-31 01:30:00", False, ["2023-10-31", "01:30:00"]),
        ("2029-13-21 12:35:00", True, ValueError),
        ("2023-11-30 22:45:00", False, ["2023-11-30", "22:45:00"]),
        ("2023-09-32 02:30:00", True, ValueError),
    ],
)
def test_check_bst_adjustment(time_input, bool_input, expected_outcome):
    """ """

    if bool_input:
        with pytest.raises(expected_outcome):
            utils.check_bst_adjustment(time_input)
    else:
        result = utils.check_bst_adjustment(time_input)
        assert result == expected_outcome


@pytest.mark.parametrize(
    "file_input, bool_input, expected_outcome",
    [
        ("/mnt/folder_1", True, True),
        ("/mnt/folder_2/", False, False),
        ("/mnt/folder_3", True, True),
    ],
)
def test_check_storage(monkeypatch, tmp_path, file_input, bool_input, expected_outcome):
    test_file = tmp_path / "storage.json"
    with open(test_file, "w") as f:
        json.dump({file_input: bool_input, "all_storage_on": True}, f)
    monkeypatch.setattr("utils.STORAGE_JSON", str(test_file))
    result = utils.check_storage(file_input)
    assert result == expected_outcome


@pytest.mark.parametrize(
    "file_input, expected_outcome",
    [
        ("/mnt/folder_1", "Storage not found"),
        ("/mnt/folder_2/", "Storage not found"),
        ("/mnt/folder_3", "Storage not found"),
    ],
)
def test_check_storage_no_file(tmp_path, monkeypatch, file_input, expected_outcome):
    """
    Test the check_storage function when the file does not exist
    """
    test_file = tmp_path / "storage.json"
    with open(test_file, "w") as f:
        json.dump({"all_storage_on": True}, f)
    monkeypatch.setattr("utils.STORAGE_JSON", str(test_file))
    result = utils.check_storage(file_input)
    assert result == expected_outcome


def test_storage_status_errors(monkeypatch, tmp_path):
    # Test with a non-existent file
    non_existent = tmp_path / "doesnt_exist.json"
    monkeypatch.setattr("utils.STORAGE_JSON", str(non_existent))

    # Should raise FileNotFoundError
    with pytest.raises(FileNotFoundError):
        utils.check_storage(non_existent)

    # Test with invalid JSON
    invalid_json = tmp_path / "invalid.json"
    with open(invalid_json, "w") as f:
        f.write("This is not valid JSON")

    monkeypatch.setattr("utils.STORAGE_JSON", str(invalid_json))

    # Should raise JSONDecodeError
    with pytest.raises(json.JSONDecodeError):
        utils.check_storage(invalid_json)
