#!/usr/bin/env python3

import os
import sys
sys.path.append(os.environ['CODE'])
import utils

def test_check_control():
    true_response = utils.check_control('black_pearl')
    assert true_response is True
    false_response = utils.check_control('power_off_all')
    assert false_response is False

def test_check_cid():
    true_response = utils.cid_check(os.environ['CID_DATA3'])
    assert true_response is True
