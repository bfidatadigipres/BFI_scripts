#!/usr/bin/env python3

import os
import sys

sys.path.append(os.environ["CODE"])
import adlib_v3 as adlib


def test_check_control():
    true_response = utils.check_control("black_pearl")
    assert true_response is True
    false_response = utils.check_control("power_off_all")
    assert false_response is False
