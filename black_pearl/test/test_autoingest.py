#!/usr/bin/env python3

import os
import sys

sys.path.append(os.path.join(os.environ["CODE"], "black_pearl"))
import autoingest


def test_check_filename():
    """
    Using hardcoded names to test naming checks
    """
    data1 = autoingest.check_filename("N_123456_new_01of01.mkv")
    data2 = autoingest.check_filename("n-123456_01of01.mkv")
    data3 = autoingest.check_filename("N_123456_2_01of01.mkv")
    data4 = autoingest.check_filename("z_123456_001of003.tif")
    data5 = autoingest.check_filename("N_123456_01of02.mov.mov")
    assert data1 is False
    assert data2 is False
    assert data3 is True
    assert data4 is False
    assert data5 is False


def test_check_mime_path():
    """
    Using test files in black_pearl/tests/
    """
    vid = os.path.join(os.environ["CODE"], "black_pearl/test/N_6783241_01of01.ts")
    image = os.path.join(
        os.environ["CODE"], "black_pearl/test/JAR_2_1_1_0001of0148.tif"
    )
    duff = os.path.join(os.environ["CODE"], "black_pearl/test/no_file.txt")
    assert autoingest.check_mime_type(vid, "logs") == True
    assert autoingest.check_mime_type(image, "logs") == True
    assert autoingest.check_mime_type(duff, "logs") == False


def test_get_ob_num():
    """
    Using hard coded fname examples
    """
    data1 = autoingest.get_object_number("C_123456_01of01.tif")
    data2 = autoingest.get_object_number("C-123456")
    data3 = autoingest.get_object_number("JAR_123456_0100of0834.tif")
    data4 = autoingest.get_object_number("M_123456_01of03.mkv")
    data5 = autoingest.get_object_number("PBM_49605_16of16.tif")
    data6 = autoingest.get_object_number("PBM_37429_19of20.tif")
    assert data1 == "C-123456"
    assert data2 == ""
    assert data3 is False
    assert data4 is False
    assert data5 == "PBM-49605"
    assert data6 == "PBM-37429"


def test_check_partwhole():
    """
    Using hardcoded names to test naming checks
    """
    data1 = autoingest.check_part_whole("N_1234_01of002.ts")
    data2 = autoingest.check_part_whole("N_1234_0002of1099.ts")
    data3 = autoingest.check_part_whole("N_1234_2_o1off02.ts")
    data4 = autoingest.check_part_whole("N_1234_01of99.mkv")
    data5 = autoingest.check_part_whole("N_I23456_0IofI0.mkv")
    assert data1[0] is None
    assert data2[0] == 2
    assert data3[0] is None
    assert data4[0] == 1
    assert data4[1] == 99
    assert data5[0] is None


def test_process_image_archive():
    """
    Hard coded names to test correct operations
    """
    data1 = autoingest.process_image_archive("JAR_123456_0100of0345.tif", "logs")
    data2 = autoingest.process_image_archive("JAR_123456_00100of00345.tif", "logs")
    data3 = autoingest.process_image_archive("JAR-123456_01of03.tif", "logs")
    data4 = autoingest.process_image_archive("JAR_2_1_1_001of300.tif", "logs")
    data5 = autoingest.process_image_archive("SPD_9439697_01of01.TIFF", "logs")
    data6 = autoingest.process_image_archive("AR_123456_0101of0100.jpg", "logs")
    data7 = autoingest.process_image_archive("AR_123456_00of20.jpg", "logs")
    assert data1[0] == "JAR-123456"
    assert data1[1] == 100
    assert data1[2] == 345
    assert data1[3] == "tif"
    assert data2[0] is None
    assert data3[0] is None
    assert data4[0] == "JAR-2-1-1"
    assert data4[1] == 1
    assert data4[2] == 300
    assert data5[0] == None
    assert data6[1] == None
    assert data7[1] == None


def test_get_item_priref():
    """
    Test real / fake data
    """
    data1 = autoingest.get_item_priref("N-9413393")
    data2 = autoingest.get_item_priref("M-123456")
    data3 = autoingest.get_item_priref("JMW-123456")
    data4 = autoingest.get_item_priref("PBM-49605 ")
    assert data1 == 157858140
    assert data2 == 150935379
    assert data3 == ""
    assert data4 == 110098020


def test_check_media_record():
    """
    Supply genuine/fake fname
    to test API return sufficient
    """
    data1 = autoingest.check_media_record("N_123456_01to02.mkv")
    data2 = autoingest.check_media_record(N_6923640_01of01.mov)
    data3 = autoingest.check_media_record("N_9732719_01of02.mov")
    assert data1 is False
    assert data2 is False
    assert data3 is True


def test_check_bp_status():
    """
    Check for correct bool response
    """
    data1 = autoingest.check_bp_status(
        "N_6923640_01of01.mov", ["imagen", "preservation01"]
    )
    data2 = autoingest.check_bp_status(
        "N_6923640_01of01.mkv", ["imagen", "preservation01"]
    )
    data3 = autoingest.check_bp_status("", ["imagen", "preservation01"])
    data4 = autoingest.check_bp_status("N_6923640_01of01.mov", [imagen, preservation01])
    assert data1 is True
    assert data2 is False
    assert data3 is False
    assert data4 is False


def test_ext_in_file_type():
    """
    Check for correct bool response
    """
    ext1 = autoingest.ext_in_file_type("mkv", "157386003", "pytest_false_log")
    ext2 = autoingest.ext_in_file_type("mov", "157386003", "pytest_false_log")
    ext3 = autoingest.ext_in_file_type("", "", "pytest_false_log")
    ext4 = autoingest.ext_in_file_type("")
    assert ext1 is False
    assert ext2 is True
    assert ext3 is False
    assert ext4 is False


def test_get_media_ingest():
    """
    Check for correct return of filenames
    of False where none found
    """
    fname1 = autoingest.get_media_ingests("N-6923640")
    fname2 = autoingest.get_media_ingests("N-6839629")
    fname3 = autoingest.get_media_ingests("N_1234_01of01.mkv")
    assert fname1 == ["N_6923640_01of01.mov"]
    assert fname2 == [
        "N_6839629_01of03.mkv",
        "N_6839629_02of03.mkv",
        "N_6839629_03of03.mkv",
    ]
    assert fname3 is False
