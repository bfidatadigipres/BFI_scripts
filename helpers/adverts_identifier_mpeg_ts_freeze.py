"""
Script to attempt to find start points for
TV adverts from MPEG-TS (or any) video input
using FFmpeg

2026
"""

import os
import sys
import subprocess
from datetime import timedelta


def get_silence_detection(fpath):
    """
    Use FFmpeg to capture date about
    silence and frozen image - no output
    """
    cmd = [
        "ffmpeg", "-i",
        fpath,
        "-af", "silencedetect=noise=-31dB:d=0.4",
        "-vf", "freezedetect=noise=-60dB:d=0.2",
        "-f", "null", "/dev/null"
    ]
    try:
        data = subprocess.run(
            cmd,
            shell=False,
            check=True,
            universal_newlines=True,
            stderr=subprocess.PIPE,
        ).stderr
    except subprocess.CalledProcessError as e:
        data = getattr(e, "stderr", "") or ""
        print(e)

    return data


def retrieve_silences(data):
    """
    Fetch from FFmpeg data output
    start/end points for filters
    """
    data_list = data.splitlines()
    time_range = []
    freeze_range = []

    for line in data_list:
        line = line.strip()
        if "silence_start" in line:
            start = line.split(":")[-1].strip()
        if "silence_end" in line:
            end = line.split(":")[-1].strip()
            if start and end:
                time_range.append((start, end))
            start = None
            end = None
        if "freeze_start" in line:
            freeze_start = line.split(":")[-1].strip()
        if "freeze_end" in line:
            freeze_end = line.split(":")[-1].strip()
            if freeze_start and freeze_end:
                freeze_range.append((freeze_start, freeze_end))
            freeze_start = None
            freeze_end = None

    return time_range, freeze_range


def find_advert_breaks(fpath):
    """
    Filter out any silences that do not
    correlate with visual freezes
    """
    audio_data = get_silence_detection(fpath)
    time_range, freeze_range = retrieve_silences(audio_data)
    print(freeze_range)
    if not time_range:
        return None
    filtered = []
    for s, e in time_range:
        print(s, e)
        if start_within_freezes(s, freeze_range):
            filtered.append((s, e))

    return filtered


def start_within_freezes(sstart, freeze_range):
    """
    Check ranges for previous func
    """
    for fstart, fend in freeze_range:
        if sstart >= fstart and sstart <= fend:
            return True
    return False


def parse_start_times(data):
    """
    Clean up and sort start times
    """
    starts = []
    for item in data:
        print(item)
        try:
            if isinstance(item, (tuple, list)):
                start = float(item[0])
            else:
                start_str, _ = item.split("-")
                start = float(start_str.strip())
            starts.append(start)
        except ValueError:
            continue  # skip bad entries
    return sorted(starts)


def format_time(seconds):
    """
    Return timestamp from seconds
    """
    return str(timedelta(seconds=int(round(seconds))))


def is_valid_gap(gap, tolerance=0.5):
    """
    Looks for gaps that 10 sec X divisble only
    """
    for base in range(10, 61, 10):  # 10,20,30,40,50, upto 60
        if abs(gap - base) <= tolerance:
            return True
    return False


def find_silence_clusters(data, tolerance=0.5, min_matches=3):
    """
    Only allow through clusters of 10x second
    gaps to identify advert blocks
    """
    starts = parse_start_times(data)
    if not starts:
        return []

    clusters = []
    current_cluster = [starts[0]]

    for i in range(1, len(starts)):
        gap = starts[i] - starts[i - 1]

        if is_valid_gap(gap, tolerance):
            current_cluster.append(starts[i])
        else:
            if len(current_cluster) >= min_matches:
                clusters.append(current_cluster)
            current_cluster = [starts[i]]

    if len(current_cluster) >= min_matches:
        clusters.append(current_cluster)

    result = []
    for cluster in clusters:
        result.extend(format_time(t) for t in cluster)

    return result


if __name__ == "__main__":
    input_file = sys.argv[1]
    if not os.path.isfile(input_file):
        sys.exit(f"Please ensure path to file is correct...\n{input_file}")
    advert_starts = find_advert_breaks(input_file)
    possible_ads = find_silence_clusters(advert_starts)
    print(possible_ads)
