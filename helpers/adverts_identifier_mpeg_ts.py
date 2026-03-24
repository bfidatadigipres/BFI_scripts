import os
import sys
import subprocess
from datetime import timedelta


def get_silence_detection(input_file):

    cmd = [
        "ffmpeg", "-i",
        input_file,
        "-af", "silencedetect=noise=-31dB:d=0.4",
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
        print(e)

    return data


def retrieve_silences(data):
    data_list = data.splitlines()
    time_range = []
    for line in data_list:
        if "silence_start" in line:
            print(line)
            start = line.split(":")[-1].strip()
        if "silence_end" in line:
            print(line)
            end = line.split(":")[-1].split("|")[0].strip()
            if start and end:
                time_range.append((start, end))
            start = None
            end = None

    return time_range


def find_advert_breaks(input_file):
    audio_data = get_silence_detection(input_file)
    time_range = retrieve_silences(audio_data)

    return time_range


def parse_start_times(data):
    starts = []
    for item in data:
        try:
            start_str, _ = item
            starts.append(float(start_str.strip()))
        except ValueError:
            continue  # skip bad entries
    return sorted(starts)


def format_time(seconds):
    return str(timedelta(seconds=int(round(seconds))))


def is_valid_gap(gap, tolerance=0.50):
    for base in range(10, 61, 10):  # 10,20,30,40,50, upto 60
        if abs(gap - base) <= tolerance:
            return True
    return False


def find_silence_clusters(data, tolerance=0.5, min_matches=3):
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
