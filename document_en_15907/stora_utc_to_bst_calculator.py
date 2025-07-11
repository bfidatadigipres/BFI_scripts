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

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Final, Optional

FORMAT = "%Y-%m-%d %H:%M:%S"
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



def check_bst_adjustment(date_utc: str, time_utc: str) -> bool:
    """
    Determines if a given UTC datetime string falls within BST
    adds +1 where needed
    """
    utc_datetime_str = f"{date_utc} {time_utc}"

    try:
        dt_utc = datetime.strptime(utc_datetime_str, FORMAT).replace(tzinfo=timezone.utc)
        print(dt_utc)
    except ValueError as e:
        raise ValueError(f"Invalid datetime string format: {e}. Expected '%Y-%m-%d %H:%M:%S'")

    london_tz = ZoneInfo("Europe/London")
    dt_london = dt_utc.astimezone(london_tz)
    string_bst = datetime.strftime(dt_london, FORMAT)
    return string_bst.split(" ")

