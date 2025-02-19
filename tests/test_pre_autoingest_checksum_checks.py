import pytest
import sys

from hashes.pre_autoingest_checksum_checks import *
from hashes.checksum_maker_mediainfo import *


def test_find_files_and_md5(tmp_path):
    files_to_create = {
        "file1.mkv.md5": '',
        "file1.mkv": '',
        'file3.jpg.md5': '',
        'file2.mkv': ''
    }
    ingest_folder = tmp_path / 'ingest_folder'
    ingest_folder.mkdir()

    for file_name, content in files_to_create.items():
        filepath = ingest_folder / file_name
        filepath.write_text(content)

    result = {
        'matches': [('file1.mkv', 'file1.mkv.md5')],
        'missing_md5_files': ['file2.mkv'],
        'missing_media': ['file3.jpg.md5']
    }

    expected_files = ['file1.mkv.md5', 'file1.mkv', 'file3.jpg.md5', 'file2.mkv']
    actual_files = [set(files_to_create.keys())]

    outcome = find_files_and_md5(ingest_folder)

    assert result == outcome
    #assert actual_files == expected_files
    assert ingest_folder.exists()
    assert ingest_folder.is_dir()

@pytest.mark.parametrize(
    'source_exists, expected_message', 
    [
        (True, 'File successfully moved from source to destination'),
        (False, 'file doesnt exists')
    ]
)
def test_move_files(tmp_path, source_exists, expected_message):
    source_folder = tmp_path /'source.txt'

    destination_folder = tmp_path / 'destination_folder'
    #destin_file = destination_folder / 'source.txt'

    if source_exists:
        source_folder.write_text("text file")

    result = move_files(source_folder, destination_folder)

    assert expected_message in result
   
    if source_exists:
        assert not source_folder.exists()

def test_pygrep(tmp_path):
    checksum_folder = tmp_path / 'checksum_folder'
    checksum_folder.mkdir()
    filepath = checksum_folder / 'hello.mkv.md5'

    result = pygrep(filepath, 'xxx', '')
    print(result)

    assert result == (False, None)
