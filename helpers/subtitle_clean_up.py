#!/usr/bin/env python3

"""
Clean up of subtitles.vtt files that failed to be
renamed/moved after Log folder permissions changed
"""

# Public packages
import os
import shutil

sys.path.append(os.environ["CODE"])
import adlib_v3 as adlib


# Global variables
STORAGE = os.environ["STORA_PATH"]
SUBS_PTH = os.environ["SUBS_PATH2"]
CHECK_DATES = [
    "2025/09/02",
    "2025/09/03",
    "2025/09/04",
    "2025/09/05",
    "2025/09/06",
    "2025/09/07",
    "2025/09/08"
]


def main():
    """
    Target check dates looking for
    subtitles large than 10 bytes
    match to CID digital media recs
    and renaming / move to SUBS_PTH
    """
    for dt in CHECK_DATES:
        target_date = os.path.join(STORAGE, dt)
        if os.path.exists(target_date):
            print(f"******* NEW DATE {target_date} *********")
            check_for_subs(target_date)


def check_cid(root):
    """
    Search in digital.acquired_filename
    for partial match to root path
    then extract object_number
    """
    match = os.path.join(root, 'stream.mpeg2.ts')
    search = f'digital.acquired_filename="{match}"'
    hits, result = adlib.retrieve_record(CID_API, "items", search, "0", ["object_number"])

    print(f"*** check_cid(): {hits}\n{result}")
    if hits is None:
        print(f"CID API could not be reached for Manifestations search: {search}")
        return None
    if hits == 0:
        return 0
    try:
        ob_num = adlib.retrieve_field_name(result[0], "object_number")[0]
        return ob_num
    except (IndexError, TypeError, KeyError):
        return None


def check_for_subs(target_date):
    for root, _, files in os.walk(target_date):
        for file in files:
            if file.endswith(".vtt"):
                print(f"Path identified: {root}")
                sub_fpath = os.path.join(root, file)
                if os.stat(sub_fpath).st_size < 15:
                    print(f"SKIPPING: File too small for useful content: {sub_fpath}")
                    continue
                ob_num = check_cid(root)
                if not ob_num:
                    print(f"SKIPPING: Could not match: {root}")
                    continue
                fname = f"{ob_num.split("-", "_")}_01of01.vtt"
                new_fpath = os.path.join(SUBS_PTH, fname)

                print(f"shutil.move({sub_fpath}, {new_fpath})")


if __name__ == "__main__":
    main()