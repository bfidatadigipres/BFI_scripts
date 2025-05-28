import pytest
import yaml

@pytest.fixture(params=[
    [{}],
    {
    "bfi": "British Film Institue",
    "bbc": "British Broadcasting Channel",
    "vue": "vue",
    "odeon": "odeon"
}
])
def yaml_data(request):
    return request.param

@pytest.fixture
def writing_yaml(tmp_path, yaml_data):
    '''
    This function writes the different typs of data into
    temporary csv file for testing purposes.

    Parameter:
    -----------

        tmp_path: pathlib.Path
            pytest builtin module to create paths

        csv_data: string
            the data to input to thre file

    Returns:
    --------
        csv_file
            return the csv path

    '''
    d = tmp_path / 'sub'
    d.mkdir()
    yaml_path = d / "hello.yaml"
    with open(yaml_path, 'w') as f:
        yaml.dump(yaml_data, f)
    return yaml_path

@pytest.fixture(params = [
    "hello world!",
    ""
])
def txt_data(request):
    return request.param

@pytest.fixture
def writing_txt(tmp_path, txt_data):
    d = tmp_path / 'sub'
    d.mkdir()
    txt_file = d / 'testting.txt'
    txt_file.write_text(txt_data)
    return txt_file

@pytest.fixture(params = [
    [
        {"film_company": "bfi", "full_name": "British Film Institute"},
        {"film_company": "BBC", "full_name": "British Broadcasting Channel" },
        {"film_company": "vue", "full_name": "vue"},
        {"film_company": "Odeon", "full_name": "Odeon"}    
    ],
    [
        {}
    ]
])

def csv_data(request):
    return request.param

@pytest.fixture()
def writing_csv(tmp_path, csv_data):
    '''
    This function writes the different types of data into
    temporary csv file for testing purposes.

    Parameter:
    -----------

        tmp_path: pathlib.Path
            pytest builtin module to create paths

        csv_data: string
            the data to input to thre file

    Returns:
    --------
        csv_file
            return the csv path
    '''
    d = tmp_path / "sub"
    d.mkdir()
    csv_file = d / "hello.csv"
    with open(csv_file, 'w') as f:
        f.write(','.join(csv_data[0].keys()))
        f.write("\n")
        for row in csv_data:
            f.write(",".join(str(x) for x in row.values()))
            f.write("\n")
    return csv_file

@pytest.fixture()
def creating_checksum_path(tmp_path):
    d = tmp_path / 'checksum_folder'
    d.mkdir()
    checksum_file_name = d / 'mkv_sample.mkv.md5'
    return checksum_file_name

@pytest.fixture()
def oversized_file(tmp_path):
    file = tmp_path / 'oversized_file.xml'
    with open(file, 'wb') as write_file:
        write_file.seek(55551073741824-1)
        write_file.write(b"\0\0\0\0\0\0\0\0\0\0\0\0")
    yield file

@pytest.fixture()
def create_mediainfo_folder(tmp_path):
    d = tmp_path / 'mediainfo_folder'
    d.mkdir()
    media_file = d / 'mkv_sample.mkv'
    media_file.write_text("dummy media content")
    return d  
            



# @pytest.fixture()
# def create_mediainfo_folder(tmp_path):
#     d = tmp_path / 'mediainfo_folder'
#     d.mkdir()
#     return d
            