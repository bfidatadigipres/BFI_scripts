#!/usr/bin/env python3

'''
Extract a clip with FFmpeg and determine if it maintains
frameMD5 fixity with its source

Use:
  $ clipmd5 /path/to/source.mkv --start 00:01:15 --end 00:03:00 --output clip.mkv
  $ clipmd5 source.mkv --start 00:05:00 --end 25 --output clip.mkv --ffmpeg -an -

Converted from Py2 legacy code to Py3
Joanna White
2022
'''

import sys
import argparse
import subprocess


def framemd5_manifest(cmd):
    ''' Generate a manifest of MD5 checksums: a list per stream '''

    # Append framemd5 arguments to given command
    cmd.extend(['-loglevel', 'quiet', '-hide_banner', '-f', 'framemd5', '-'])
    command = ' '.join(cmd)

    try:
        p = subprocess.check_output(command,shell=True,stderr=subprocess.STDOUT)
        return p
    except subprocess.CalledProcessError as err:
        raise RuntimeError(f"Command {err.cmd} returned with error (code {err.returncode}): {err.output}") from err


def segment(cmd):
    ''' Trim a new file from the given source '''

    command = ' '.join(cmd)
    status = subprocess.call(command,shell=True)
    if status == 0:
        return True


def error_message(message):
    ''' Print and close '''

    sys.stderr.write(message + '\n')
    sys.stderr.flush()
    sys.exit(1)


def create_clip(cmd):
    ''' Trim a clip from source given timecodes (hh:mm:ss) and compare its framemd5 with source '''

    # Generate framemd5s for source
    src_md5 = framemd5_manifest(cmd[:-1])

    # Create segment
    segment(cmd)

    # Remove in/out args from given command
    for i in ['-ss', '-to', '-t']:
        if i in cmd:
            pos = cmd.index(i)
            cmd.pop(pos+1)
            cmd.pop(pos)

    # Replace given input path with recent output
    pos = cmd.index('-i') + 1
    cmd[pos] = cmd[-1]
    print(cmd[:-1])
    # Generate framemd5s for destination
    dst_md5 = framemd5_manifest(cmd[:-1])

    # Check md5 manifests have executed correctly
    if '#hash: MD5' not in str(src_md5):
        print(f"create_clip(): Framemd5 source MKV formatting error \n{src_md5}")
        return False
    if '#hash: MD5' not in str(dst_md5):
        print(f"create_clip(): Framemd5 transcoded MKV formatting error \n{dst_md5}")
        return False

    src = framemd5_cut(src_md5)
    dst = framemd5_cut(dst_md5)

    if src and dst:
        result = bool(src == dst)
        return result

    return False


def framemd5_cut(framemd5):
    '''
    This function to chop up framemd5 so only checksum remain for comparison
    avoiding errors with non-matching duration columns, split on comma
    JMW new function 08/09/2022
    '''

    new_manifest = []
    framemd5_decode = framemd5.decode('utf-8')
    lines = framemd5_decode.split('\n')

    for line in lines:
        new_manifest.append(line.split(',')[-1])

    return ''.join(new_manifest)


def clipmd5(in_file, start, out_file, end=None, ffmpeg=None):
    ''' Wrapper '''

    cmd = construct_command(in_file, start, out_file, end, ffmpeg)
    status = create_clip(cmd)
    return status


def construct_command(in_file, start, out_file, end=None, ffmpeg=None):
    ''' Assemble an FFmpeg command as a list of parameters '''

    cmd = ['ffmpeg', '-ss', start, '-i', in_file]

    # Pass end str as [--to] or int as [-t]
    if end:
        try:
            int(end)
            out = '-t'
        except ValueError:
            out = '-to'

        cmd.extend([out, end])

    # Append any FFmpeg parameters; put output path at the end
    if ffmpeg:
        cmd.extend(ffmpeg + [out_file])
    else:
        cmd.extend([out_file])

    return cmd


def main():
    ''' Create a new clip, and confirm that its framemd5 manifest matches the source '''

    parser = argparse.ArgumentParser(description='Extract a clip with framemd5 fixity')
    parser.add_argument('file', nargs='?', type=argparse.FileType('r'), help='Input file')
    parser.add_argument('--start', type=str, required=True, help='Extract from [hh:mm:ss]')
    parser.add_argument('--end', help='Extract until position given as [hh:mm:ss] or [s]')
    parser.add_argument('--output', type=str, required=True, help='Create clip with filename')

    # Permit any FFmpeg arguments
    parser.add_argument('--ffmpeg',
                        nargs=argparse.REMAINDER,
                        default=['-map', '0',     # All streams
                                 '-c', 'copy',    # Stream copy, no transcode
                                 '-n'],           # Do not overwrite output
                        help='Any additional FFmpeg parameters')

    args = parser.parse_args()

    status = clipmd5(args.file.name, args.start, args.output, args.end, args.ffmpeg)

    if not status:
        error_message(f'{args.output}\tNO fixity')
    else:
        print(f'{args.output}\tFixity OK')


if __name__ == '__main__':
    main()
