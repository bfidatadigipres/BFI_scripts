from hashes import metadata_clean_up as mcu
import pytest

@pytest.mark.parametrize('filename, output',
    [
        ('', '')
    ]
)
def test_cid_retrival(filename, output):
    results = mcu.cid_retrieve(filename)

    assert results == output