#!/usr/bin/env python3

import os
import sys
import pytz
import datetime
sys.path.append(os.path.join(os.environ['CODE'], 'black_pearl'))
import black_pearl_move_put as bp


def test_get_buckets():
    '''
    Using hardcoded names to test JSON
    retrieval of BP buckets
    '''
    bucket_list1 = bp.get_buckets('bfi')
    bucket_list2 = bp.get_buckets('netflix')
    bucket_list3 = bp.get_buckets('none')
    assert bucket_list1[0] == 'preservation01'
    assert bucket_list1[1] == ['preservation01', 'imagen']
    assert bucket_list2[0] == 'netflix01'
    assert bucket_list2[1] == ['netflix01']
    assert bucket_list3 is None


def test_get_size():
    '''
    Supply test file and check
    correct filesize returned
    '''
    file_size1 = bp.get_size('/mnt/fake_file/none.mkv')
    file_size2 = bp.get_size('/mnt/qnap_film/Public/test/MKV_sample')
    file_size3 = bp.get_size(False)
    assert file_size1 is None
    assert file_size2 == '814026'
    assert file_size3 is None


def test_check_bp_status():
    '''
    Check for correct bool response
    '''
    data1 = bp.check_bp_status('N_6923640_01of01.mov', ['imagen', 'preservation01'])
    data2 = bp.check_bp_status('N_6923640_01of01.mkv', ['imagen', 'preservation01'])
    data3 = bp.check_bp_status('', ['imagen', 'preservation01'])
    data4 = bp.check_bp_status('N_6923640_01of01.mov', [imagen, preservation01])
    assert data1 is True
    assert data2 is False
    assert data3 is False
    assert data4 is False


def test_format_dt():
    '''
    Supply datetime now and check
    it's formatted correctly
    '''
    dt_now = datetime.now(pytz.timezone('Europe/London'))
    dt_test = dt_now.strftime('%Y-%m-%d_%H-%M')
    dt_return1 = bp.format_dt()
    assert dt_test in dt_return1
    sleep(65)
    dt_return2 = bp.format_dt()
    assert dt_test not in dt_return2


def test_check_folder_age():
    '''
    Supply ingest folder names
    and check if they are + = dats based on 
    now time.
    ingest_2023-10-24_10-02-03
    '''
    fmt = "%Y-%m-%d %H:%M:%S.%f"

    day_diff1 = bp.check_folder_age('ingest_2023-10-24_00-01-00')
    
