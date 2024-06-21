'''
Consolidate all BP activities
to one utility module

Joanna White
2024
'''

import os
import json
from ds3 import ds3, ds3Helpers

CLIENT = ds3.createClientFromEnv()
HELPER = ds3Helpers.Helper(client=CLIENT)
DPI_BUCKETS = os.environ['DPI_BUCKET']


def get_buckets(bucket_collection):
    '''
    Read JSON list return
    key_value and list of others
    '''
    bucket_list = []
    key_bucket = ''

    with open(DPI_BUCKETS) as data:
        bucket_data = json.load(data)
    if bucket_collection == 'bfi':
        for key, value in bucket_data.items():
            if 'preservationbucket' in str(key):
                pass
            elif 'preservation0' in str(key):
                if value is True:
                    key_bucket = key
                bucket_list.append(key)
            elif 'imagen' in str(key):
                bucket_list.append(key)
    else:
        for key, value in bucket_data.items():
            if f"{bucket_collection}0" in str(key):
                if value is True:
                    key_bucket = key
                bucket_list.append(key)

    return key_bucket, bucket_list


def check_bp_status(fname, bucket_list):
    '''
    Look up filename in BP to avoid
    multiple ingests of files
    '''

    for bucket in bucket_list:
        query = ds3.HeadObjectRequest(bucket, fname)
        result = CLIENT.head_object(query)

    if 'DOESNTEXIST' in str(result.result):
        return False
    else:
        md5 = result.response.msg['ETag']
        length = result.response.msg['Content-Length']
        if int(length) > 1 and len(md5) > 30:
            return True


def get_job_status(job_id):
    '''
    Fetch job status for specific ID
    '''
    cached = status = ''

    job_status = CLIENT.get_job_spectra_s3(
                   ds3.GetJobSpectraS3Request(job_id.strip()))

    if job_status.result['CachedSizeInBytes']:
        cached = job_status.result['CachedSizeInBytes']
    if job_status.result['Status']:
        status = job_status.result['Status']
    print(f"Status for JOB ID: {job_id}")
    print(f"{status}, {cached}")

    return status, cached


def get_bp_md5(fname, bucket):
    '''
    Fetch BP checksum to compare
    to new local MD5
    '''
    md5 = ''
    query = ds3.HeadObjectRequest(bucket, fname)
    result = CLIENT.head_object(query)
    try:
        md5 = result.response.msg['ETag']
    except Exception as err:
        print(err)
    if md5:
        return md5.replace('"', '')


def get_object_list(fname):
    '''
    Get all details to check file persisted
    '''

    request = ds3.GetObjectsWithFullDetailsSpectraS3Request(name=f"{fname}", include_physical_placement=True)
    try:
        result = CLIENT.get_objects_with_full_details_spectra_s3(request)
        data = result.result
    except Exception as err:
        return None

    if not data['ObjectList']:
        return None, None, None
    if "'TapeList': [{'AssignedToStorageDomain': 'true'" in str(data):
        confirmed = True
    elif "'TapeList': [{'AssignedToStorageDomain': 'false'" in str(data):
        confirmed = False
    try:
        md5 = data['ObjectList'][0]['ETag']
    except (TypeError, IndexError):
        md5 = None
    try:
        length = data['ObjectList'][0]['Blobs']['ObjectList'][0]['Length']
    except (TypeError, IndexError):
        length = None

    return confirmed, md5, length


def put_directory(directory_pth, bucket):
    '''
    Add the directory to black pearl using helper (no MD5)
    Retrieve job number and launch json notification
    '''
    try:
        put_job_ids = HELPER.put_all_objects_in_directory(source_dir=directory_pth, bucket=bucket, objects_per_bp_job=5000, max_threads=3)
    except Exception as err:
        print('Exception: %s', err)
        return None
    print("PUT COMPLETE - JOB ID retrieved: {put_job_ids}")
    job_list = []
    for job_id in put_job_ids:
        job_list.append(job_id)
    return job_list


def download_bp_object(fname, outpath, bucket):
    '''
    Download the BP object from SpectraLogic
    tape library and save to outpath
    '''
    if bucket == '':
        bucket = 'imagen'

    file_path = os.path.join(outpath, fname)
    get_objects = [ds3Helpers.HelperGetObject(fname, file_path)]
    try:
        get_job_id = HELPER.get_objects(get_objects, bucket)
        print(f"BP get job ID: {get_job_id}")
    except Exception as err:
        raise Exception(f"Unable to retrieve file {fname} from Black Pearl: {err}")

    return get_job_id


def get_buckets_blob(bucket_collection):
    '''
    Read JSON list return
    key_value and list of others
    '''
    key_bucket = ''

    with open(DPI_BUCKETS) as data:
        bucket_data = json.load(data)
    if bucket_collection == 'netflix':
        for key, value in bucket_data.items():
            if 'netflixblobbing' in key.lower():
                if value is True:
                    key_bucket = key
    elif bucket_collection == 'amazon':
        for key, value in bucket_data.items():
            if 'amazonblobbing' in key.lower():
                if value is True:
                    key_bucket = key
    elif bucket_collection == 'bfi':
        for key, value in bucket_data.items():
            if 'preservationblobbing' in key.lower():
                if value is True:
                    key_bucket = key

    return key_bucket


def put_single_file(fpath, ref_num, bucket_name):
    '''
    Add the file to black pearl using helper
    Fine for < or > 1TB
    '''
    file_size = os.path.getsize(fpath)
    put_obj = [ds3Helpers.HelperPutObject(object_name=ref_num, file_path=fpath, size=file_size)]
    try:
        put_job_id = HELPER.put_objects(put_objects=put_obj, bucket=bucket_name)
        print(f"PUT COMPLETE - JOB ID retrieved: {put_job_id}")
        return put_job_id
    except Exception as err:
        print('Exception: %s', err)
        return None


def delete_black_pearl_object(ref_num, version, bucket):
    '''
    Receive reference number and initiate
    deletion of object
    '''
    try:
        request = ds3.DeleteObjectRequest(bucket, ref_num, version_id=version)
        job_deletion = CLIENT.delete_object(request)
        return job_deletion
    except Exception as exc:
        print(exc)
        return None


def etag_deletion_confirmation(ref_num, bucket):
    '''
    Get confirmation of deletion
    '''
    resp = ds3.HeadObjectRequest(bucket, ref_num)
    result = CLIENT.head_object(resp)
    etag = result.response.msg['ETag']
    if etag is None:
        return 'Deleted'
    return etag
