"""
Consolidate all BP activities
to one utility module

2024
"""

import json
import os
from typing import Optional, Union

from ds3 import ds3, ds3Helpers

CLIENT = ds3.createClientFromEnv()
HELPER = ds3Helpers.Helper(client=CLIENT)
DPI_BUCKETS = os.environ["DPI_BUCKET"]
JSON_END = os.environ["JSON_END_POINT"]


def get_buckets(bucket_collection: str) -> tuple[str, list[str]]:
    """
    Read JSON list return
    key_value and list of others
    """
    bucket_list: list[str] = []
    key_bucket: str = ""

    with open(DPI_BUCKETS) as data:
        bucket_data: dict[str, str] = json.load(data)
    if bucket_collection == "bfi":
        for key, value in bucket_data.items():
            if "preservationblobbing" in str(key.lower()):
                continue
            elif "preservation0" in str(key.lower()):
                if value is True:
                    key_bucket = key
                bucket_list.append(key)
            elif "imagen" in str(key):
                bucket_list.append(key)
    else:
        for key, value in bucket_data.items():
            if f"{bucket_collection.strip()}blobbing" in key:
                continue
            elif f"{bucket_collection.strip()}0" in key:
                if value is True:
                    key_bucket = key
                bucket_list.append(key)

    return key_bucket, bucket_list


def check_no_bp_status(fname: str, bucket_list: list[str]) -> bool:
    """
    Look up filename in BP to avoid
    multiple ingests of files
    """
    exist_across_buckets: list[str] = []
    for bucket in bucket_list:
        try:
            query: ds3.HeadObjectRequest = ds3.HeadObjectRequest(bucket, fname)
            result: ds3.HeadObjectResponse = CLIENT.head_object(query)
            # Only return false if DOESNTEXIST is missing, eg file found
            if "DOESNTEXIST" in str(result.result):
                print(f"File {fname} NOT found in Black Pearl bucket {bucket}")
                exist_across_buckets.append("DOESNTEXIST")
            elif str(result.result) == "EXISTS":
                print(f"File {fname} found in Black Pearl bucket {bucket}")
                exist_across_buckets.append("PRESENT")
        except Exception as err:
            print(err)
    print(exist_across_buckets)
    if exist_across_buckets == []:
        return False
    if "PRESENT" in str(exist_across_buckets):
        return False
    if "DOESNTEXIST" in str(exist_across_buckets):
        return True
    return False


def get_job_status(job_id: str) -> tuple[str, str]:
    """
    Fetch job status for specific ID
    """
    cached = status = ""

    job_status: ds3.GetJobSpectraS3Request = CLIENT.get_job_spectra_s3(
        ds3.GetJobSpectraS3Request(job_id.strip())
    )

    if job_status.result["CachedSizeInBytes"]:
        cached = job_status.result["CachedSizeInBytes"]
    if job_status.result["Status"]:
        status = job_status.result["Status"]
    print(f"Status for JOB ID: {job_id}")
    print(f"{status}, {cached}")

    return status, cached


def get_bp_md5(fname: str, bucket: str) -> Optional[str]:
    """
    Fetch BP checksum to compare
    to new local MD5
    """
    md5: str = ""
    query: ds3.HeadObjectRequest = ds3.HeadObjectRequest(bucket, fname)
    result: ds3.HeadObjectResponse = CLIENT.head_object(query)
    try:
        md5: str = result.response.msg["ETag"]
    except Exception as err:
        print(err)
        return None
    if md5:
        return md5.replace('"', "")


def get_bp_length(fname: str, bucket: str) -> Optional[str]:
    """
    Fetch BP checksum to compare
    to new local MD5
    """
    size: str = ""
    query: ds3.HeadObjectRequest = ds3.HeadObjectRequest(bucket, fname)
    result: ds3.HeadObjectResponse = CLIENT.head_object(query)
    try:
        size = result.response.msg["Content-Length"]
    except Exception as err:
        print(err)
        return None
    if size:
        return size.replace('"', "")


def get_confirmation_length_md5(
    fname: str, bucket: str, bucket_list: list[str]
) -> Optional[tuple[Optional[Union[bool, str]], Optional[str], Optional[str]]]:
    """
    Alternative retrieval for get_object_list
    avoiding full_details requests
    """
    flist: list[str] = [fname]
    try:
        object_flist: list[ds3.Ds3GetObject] = list(
            [ds3.Ds3GetObject(name=fname) for fname in flist]
        )
        res = ds3.GetPhysicalPlacementForObjectsSpectraS3Request(bucket, object_flist)
        result = CLIENT.get_physical_placement_for_objects_spectra_s3(res)
        data = result.result
    except Exception as err:
        print(err)
        data = None

    if data is None:
        for buck in bucket_list:
            try:
                object_flist = list([ds3.Ds3GetObject(name=fname) for fname in flist])
                res = ds3.GetPhysicalPlacementForObjectsSpectraS3Request(
                    buck, object_flist
                )
                result = CLIENT.get_physical_placement_for_objects_spectra_s3(res)
                print(result.result)
                if len(result.result["TapeList"]) > 0:
                    data = result.result
                    bucket = buck
                    break
            except Exception as err:
                data = None
                print(err)

    if not data["TapeList"]:
        return "No tape list", None, None
    if result.result["TapeList"][0]["AssignedToStorageDomain"] == "true":
        confirmed = True
    elif result.result["TapeList"][0]["AssignedToStorageDomain"] == "false":
        confirmed = False
    else:
        return None, None, None

    md5 = get_bp_md5(fname, bucket)
    length = get_bp_length(fname, bucket)
    return confirmed, md5, length


def get_object_list(
    fname: str,
) -> Optional[tuple[Union[bool, str], Optional[str], Optional[str]]]:
    """
    Get all details to check file persisted
    """

    request = ds3.GetObjectsWithFullDetailsSpectraS3Request(
        name=f"{fname}", include_physical_placement=True
    )
    try:
        result = CLIENT.get_objects_with_full_details_spectra_s3(request)
        data = result.result
    except Exception as err:
        print(err)
        return None

    if not data["ObjectList"]:
        return "No object list", None, None
    if "'TapeList': [{'AssignedToStorageDomain': 'true'" in str(data):
        confirmed = True
    elif "'TapeList': [{'AssignedToStorageDomain': 'false'" in str(data):
        confirmed = False
    try:
        md5 = data["ObjectList"][0]["ETag"]
    except (TypeError, IndexError):
        md5 = None
    try:
        length = data["ObjectList"][0]["Blobs"]["ObjectList"][0]["Length"]
    except (TypeError, IndexError):
        length = None

    return confirmed, md5, length


def put_directory(directory_pth: str, bucket: str) -> Optional[list[str]]:
    """
    Add the directory to black pearl using helper (no MD5)
    Retrieve job number and launch json notification
    """
    try:
        put_job_ids: list[str] = HELPER.put_all_objects_in_directory(
            source_dir=directory_pth,
            bucket=bucket,
            objects_per_bp_job=5000,
            max_threads=3,
        )
    except Exception as err:
        print("Exception: %s", err)
        return None
    print(f"PUT COMPLETE - JOB ID retrieved: {put_job_ids}")
    job_list = []
    for job_id in put_job_ids:
        job_list.append(job_id)
    return job_list


def put_notification(job_id: str) -> str:
    """
    Ensure job notification is sent to Isilon/ BP NAS
    """
    job_completed_registration = (
        CLIENT.put_job_completed_notification_registration_spectra_s3(
            ds3.PutJobCompletedNotificationRegistrationSpectraS3Request(
                notification_end_point=JSON_END, format="JSON", job_id=job_id
            )
        )
    )

    return job_completed_registration.result["NotificationEndPoint"]


def download_bp_object(fname: str, outpath: str, bucket: str) -> str:
    """
    Download the BP object from SpectraLogic
    tape library and save to outpath
    """
    if bucket == "":
        bucket = "imagen"

    file_path: str = os.path.join(outpath, fname)
    get_objects: list[ds3Helpers.HelperGetObject] = [
        ds3Helpers.HelperGetObject(fname, file_path)
    ]
    try:
        get_job_id: str = HELPER.get_objects(get_objects, bucket, max_threads=1)
        print(f"BP get job ID: {get_job_id}")
    except Exception as err:
        raise Exception(f"Unable to retrieve file {fname} from Black Pearl: {err}")

    return get_job_id


def get_buckets_blob(bucket_collection: str) -> str:
    """
    Read JSON list return
    key_value and list of others
    """
    key_bucket: str = ""

    with open(DPI_BUCKETS) as data:
        bucket_data: dict[str, str] = json.load(data)
    if bucket_collection == "netflix":
        for key, value in bucket_data.items():
            if "netflixblobbing" in key.lower():
                if value is True:
                    key_bucket = key
    elif bucket_collection == "amazon":
        for key, value in bucket_data.items():
            if "amazonblobbing" in key.lower():
                if value is True:
                    key_bucket = key
    elif bucket_collection == "bfi":
        for key, value in bucket_data.items():
            if "preservationblobbing" in key.lower():
                if value is True:
                    key_bucket = key

    return key_bucket


def put_single_file(fpath: str, ref_num, bucket_name, check=False) -> Optional[str]:
    """
    Add the file to black pearl using helper
    Fine for < or > 1TB
    """
    file_size: int = os.path.getsize(fpath)
    put_obj: ds3Helpers.HelperPutObject = [
        ds3Helpers.HelperPutObject(object_name=ref_num, file_path=fpath, size=file_size)
    ]
    try:
        put_job_id: str = HELPER.put_objects(
            put_objects=put_obj,
            bucket=bucket_name,
            max_threads=1,
            calculate_checksum=bool(check),
        )
        print(f"PUT COMPLETE - JOB ID retrieved: {put_job_id}")
        return put_job_id
    except Exception as err:
        print("Exception: %s", err)
        return None


def delete_black_pearl_object(
    ref_num: str, version: Optional[str], bucket: str
) -> Optional[ds3.DeleteObjectResponse]:
    """
    Receive reference number and initiate
    deletion of object
    """
    try:
        request = ds3.DeleteObjectRequest(bucket, ref_num, version_id=version)
        job_deletion: str = CLIENT.delete_object(request)
        return job_deletion
    except Exception as exc:
        print(exc)
        return None


def etag_deletion_confirmation(ref_num: str, bucket: str) -> Optional[str]:
    """
    Get confirmation of deletion
    """
    resp = ds3.HeadObjectRequest(bucket, ref_num)
    result: ds3.HeadObjectResponse = CLIENT.head_object(resp)
    etag = result.response.msg["ETag"]
    if etag is None:
        return "Deleted"
    return etag


def get_version_id(ref_num: str) -> Optional[str]:
    """
    Call up Black Pearl ObjectList for each item
    using reference_number, and retrieve version_id
    ['ObjectList'][0]['Blobs']['ObjectList'][0]['VersionId']
    """
    resp: ds3.GetObjectsWithFullDetailsSpectraS3Request = (
        ds3.GetObjectsWithFullDetailsSpectraS3Request(
            name=ref_num, include_physical_placement=True
        )
    )
    result = CLIENT.get_objects_with_full_details_spectra_s3(resp)
    obj = result.result

    if not obj:
        return None
    if not len(obj) == 1:
        return None

    try:
        version_id: Optional[str] = obj["ObjectList"][0]["Blobs"]["ObjectList"][0][
            "VersionId"
        ]
    except (IndexError, TypeError, KeyError):
        version_id = None
    return version_id
