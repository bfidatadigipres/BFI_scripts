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
    d = tmp_path / 'sub'
    d.mkdir()
    yaml_path = d / "hello.yaml"
    with open(yaml_path, 'w') as f:
        yaml.dump(yaml_data, f)
    return yaml_path


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


            