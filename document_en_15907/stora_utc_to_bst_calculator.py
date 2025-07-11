#!/usr/bin/env python3

"""
Iterate through CID manifestations to identify STORA records
with broadcast date that matches BST_DCT dates. Where
found move broadcast time into UCT_timestamp field and add
+1 hour to the existing time in transmission_start_time.
Where a time moves beyond midnight, the transmission_date
will also need an extra day adding and updating the field.

Time formatted HH:MM:SS
Date formatted YYYY-MM-DD

DR-573

2025
"""

import datetime
import errno
import json
import logging
import os
import shutil
import sys
import time
from typing import Any, Final, Optional
import requests
import tenacity


BST_DCT = {
    "2015": ["2015-03-29", "2015-10-25"],
    "2016": ["2016-03-27", "2016-10-30"],
    "2017": ["2017-03-26", "2017-10-29"],
    "2018": ["2018-03-25", "2018-10-28"],
    "2019": ["2019-03-31", "2019-10-27"],
    "2020": ["2020-03-29", "2020-10-25"],
    "2021": ["2021-03-28", "2021-10-31"],
    "2022": ["2022-03-27", "2022-10-30"],
    "2023": ["2023-03-26", "2023-10-29"],
    "2024": ["2024-03-31", "2024-10-27"],
    "2025": ["2025-03-30", "2025-10-26"]
}
