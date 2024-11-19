'''
Create CSV_PATH with headers:
fname, bucket

To be run from a server where Spectra SDK installed
and run from ENV3, so I'd suggest:
BK-CI-DATA11:
`source /home/datadigipres/code/ENV3/bin/activate`
`python3 fetch_tv_size_pandas.py`
'''

from ds3 import ds3
import pandas as pd

CSV_PATH = ''
NEW_CSV = ''
CLIENT = ds3.createClientFromEnv()


def main():
    '''
    Read CSV, chunk one row at a time
    fetch DS3 length then write to 
    new CSV using pandas
    '''

    dataframe = pd.read_csv(CSV_PATH, chunksize=1)
    write_header = True

    for chunk in dataframe:
        file_size = fetch_length(chunk.bucket, chunk.fname)
        if not file_size or len(file_size) == 0:
            print(f"Unable to retrieve file size for file: {chunk.fname}")
            continue
        chunk['file_size'] = file_size
        chunk.to_csv(NEW_CSV, mode='a', header=write_header, index=False)
        write_header = False


def fetch_length(bucket, ref_num):
    '''
    Fetch length from Black Pearl using
    HeadObjectRequest
    '''
    r = ds3.HeadObjectRequest(bucket, ref_num)
    result = CLIENT.head_object(r)
    return result.response.msg['content-length']


if __name__ == '__main__':
    main()
