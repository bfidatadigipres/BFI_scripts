#!/usr/bin/env python3

import os
import sys

sys.path.append(os.path.join(os.environ["CODE"], "black_pearl/"))
import bp_utils


def test_check_control():
    true_response = utils.check_control("black_pearl")
    assert true_response is True
    false_response = utils.check_control("power_off_all")
    assert false_response is False
