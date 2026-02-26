#!/usr/bin/env python3
"""
Utility script for API POST/GETs from Archivematica
SFTP and Archivemtica Storage Service API

Used for born digital workflow:
special_collections_document_transfers_osh.py

2025
"""

import base64
import json
import os
import sys
import mimetypes
from typing import Optional, List, Any, Dict
from urllib.parse import urlencode, urljoin
import paramiko
import requests


TS_UUID = os.environ.get("AM_TS_UUID")  # Archivematica Transfer Storage address uuid
SFTP_USR = os.environ.get("AM_SFTP_US")  # Transfer Storage user
SFTP_KEY = os.environ.get("AM_SFTP_PW")  # Transfer Storage password
ARCH_URL = os.environ.get("AM_URL")  # BFI Archivematica instance url
API_NAME = os.environ.get("AM_API")  # Authorisation user name
API_KEY = os.environ.get("AM_KEY")  # Authorisation user key
SS_PIPE = os.environ.get("AM_SS_UUID")  # Archivematica Storage Service uuid
SS_NAME = os.environ.get("AMSS_USR")  # Storage Service user name
SS_KEY = os.environ.get("AMSS_KEY")  # Storage service user key
ATOM_URL = os.environ.get("ATOM_URL")  # AtoM API URL for queries
ATOM_KEY = os.environ.get("ATOM_KEY_META")  # AtoM API key
ATOM_AUTH = os.environ.get("ATOM_AUTH")  # AtoM autorisation

# Header dicts
HEADER = {
    "Authorization": f"ApiKey {API_NAME}:{API_KEY}",
    "Content-Type": "application/json",
}
HEADER_META = {
    "Authorization": f"ApiKey {API_NAME}:{API_KEY}",
    "Content-Type": "application/x-www-form-urlencoded",
}
ATOM_HEADER = {"REST-API-Key": ATOM_KEY, "Accept": "application/json"}
SS_HEADER = {
    "Authorization": f"ApiKey {SS_NAME}:{SS_KEY}",
    "Content-Type": "application/json",
}

if not ARCH_URL or not API_NAME or not API_KEY or not SFTP_USR or not SFTP_KEY:
    sys.exit(
        "Error: Please set AM_URL, AM_API (username), and AM_KEY (API key) environment variables."
    )


def sftp_connect() -> paramiko.sftp_client.SFTPClient:
    """
    Make connection to Archivematica SFTP
    """
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(ARCH_URL.lstrip("https://"), "22", SFTP_USR, SFTP_KEY)
    return ssh_client.open_sftp()


def sftp_listdir(rpath: str) -> List[Any]:
    """
    Call SFTP connection's listdir data
    with exception handling
    """
    sftp = sftp_connect()
    try:
        check_folder = sftp.listdir(rpath)
    except FileNotFoundError as err:
        print(f"get_sftp_listdir(): {err}")
        check_folder = []

    return check_folder


def send_to_sftp(fpath: str, top_folder: str) -> Optional[List[Any]]:
    """
    Arg fpath must be to file level (not folder)
    Supply arg top_folder without trailing '/'
    Top folder represents the first folder sitting
    below SFTP 'API_Uploads'.
    Folder/files to be formatted using snake_case preferably

    Works through nested source paths ensuring all folder
    structure passed through to SFTP folder 'API_Uploads'
    Creates 'metadata/' folder containing metadata.csv
    with Dublin Core basic metadata, as recommended by
    Artefactual docs.
    """

    relpath = fpath.split(top_folder)[-1]
    whole_path, file = os.path.split(relpath)
    print(whole_path, file)
    root, container = os.path.split(whole_path)
    print(f"Root: {root}, Container: {container}")
    path_parts = root.lstrip("/").split("/")
    print(f"Total folder count to file: {len(path_parts)}")
    remote_path = f"sftp-transfer-source/API_Uploads/{top_folder}/{path_parts[0]}"
    print(remote_path)

    # Create folders where absent
    sftp = sftp_connect()
    check_folder = sftp_listdir("sftp-transfer-source/API_Uploads")
    print(f"Check folder contents: {check_folder}")
    if top_folder not in str(check_folder):
        success = sftp_mkdir(sftp, f"sftp-transfer-source/API_Uploads/{top_folder}")
        if not success:
            print(f"Failed to make new top level folder: {top_folder}")
            return None
    if path_parts[0] not in sftp_listdir(os.path.split(remote_path)[0]):
        success = sftp_mkdir(sftp, remote_path)
        if not success:
            print(f"Failed to make new directory for {remote_path}")
            return None
    for pth in path_parts[1:]:
        remote_path = os.path.join(remote_path, pth)
        print(remote_path)
        if pth not in sftp_listdir(os.path.split(remote_path)[0]):
            success = sftp_mkdir(sftp, remote_path)
            if not success:
                print(f"Failed to make new directory for {remote_path}")
                return None

    root_contents = sftp_listdir(remote_path)
    if container not in root_contents:
        success = sftp_mkdir(sftp, os.path.join(remote_path, container))
        if not success:
            print(
                f"Failed to make new directory for {os.path.join(remote_path, container)}"
            )
            return None
    print(f"Folder {container} found in Archivematica already")
    print(
        f"Moving file {file} into Archivematica path {os.path.join(remote_path, container)}"
    )
    response = sftp_put(sftp, fpath, os.path.join(remote_path, container, file))
    if response is False:
        print(f"Failed to send file to SFTP: {file} / {fpath}")
        return None
    print(
        f"File {file} successfully PUT to {os.path.join(remote_path, container, file)}"
    )
    print("Making CSV folder...")
    m_relpath = os.path.join(remote_path, container, "metadata/")
    mpath = os.path.join(os.path.split(fpath)[0], "metadata/")
    metadata_fpath = os.path.join(mpath, "metadata.csv")
    if os.path.exists(metadata_fpath):
        success = sftp_mkdir(sftp, m_relpath)
        if not success:
            print(f"Failed to make new directory for {m_relpath}")
            return None
        response = sftp_put(
            sftp, metadata_fpath, os.path.join(m_relpath, "metadata.csv")
        )
        if response is False:
            print(f"Failed to send file to SFTP: 'metadata.csv' / {m_relpath}")
            return None
        else:
            print(f"File 'metadata.csv' successfully PUT to {m_relpath}")

    files = sftp.listdir(os.path.join(remote_path, container))
    sftp.close()
    return files


def send_metadata_to_sftp(fpath: str, top_folder: str) -> Optional[List[Any]]:
    """
    Check for parent folder, if absent mkdir
    First step SFTP into Storage Service, then check
    content has made it into the folder
    Supply arg top_folder without trailing /
    Folder/files to be formatted using snake_case preferably
    Arg fpath must me to file level (not containing folder)
    """

    relpath = fpath.split(top_folder)[-1]
    whole_path, file = os.path.split(relpath)
    print(whole_path, file)
    root, container = os.path.split(whole_path)
    print(f"Root: {root}, Container: {container}")
    remote_path = f"sftp-transfer-source/API_Uploads/{top_folder}/{root}"
    print(remote_path)

    # Create ssh / sftp object
    sftp = sftp_connect()
    try:
        root_contents = sftp.listdir(remote_path)
        print(f"Root of remote_path: {root_contents}")
    except OSError as err:
        print(f"Error attempting to retrieve path {remote_path}\n{err}")
        root_contents = ""
        success = sftp_mkdir(sftp, remote_path)
        if not success:
            print(f"Failed to make new directory for {remote_path}")
            return None
    if container not in root_contents:
        success = sftp_mkdir(sftp, os.path.join(remote_path, container))
        if not success:
            print(
                f"Failed to make new directory for {os.path.join(remote_path, container)}"
            )
            return None
    else:
        print(f"Folder {container} found in Archivematica already")

    print(
        f"Moving file {file} into Archivematica path {os.path.join(remote_path, container)}"
    )

    m_relpath = os.path.join(remote_path, container, "metadata/")
    mpath = os.path.join(os.path.split(fpath)[0], "metadata/")
    metadata_fpath = os.path.join(mpath, "metadata.csv")
    if os.path.exists(metadata_fpath):
        success = sftp_mkdir(sftp, m_relpath)
        if not success:
            print(f"Failed to make new directory for {m_relpath}")
            return None
        response = sftp_put(
            sftp, metadata_fpath, os.path.join(m_relpath, "metadata.csv")
        )
        print(response)
        if response is False:
            print(f"Failed to send file to SFTP: 'metadata.csv' / {m_relpath}")
            return None
        print(f"File 'metadata.csv' successfully PUT to {m_relpath}")

    files = sftp.listdir(os.path.join(remote_path, container))
    sftp.close()
    return files


def sftp_put(sftp_object: paramiko.sftp_client.SFTPClient, fpath: str, relpath: str) -> bool:
    """
    Handle PUT to sftp using
    open SFTP connection
    """
    print(f"PUT request received:\n{fpath}\n{relpath}")

    try:
        data = sftp_object.put(fpath, relpath)
        print(str(data))
        if "SFTPAttributes" in str(data):
            return True
    except FileNotFoundError as err:
        print(f"File {fpath} was not found. {err}")
        return False
    except OSError as err:
        print(f"Error attempting to PUT {fpath} {err}")
        return False


def sftp_mkdir(sftp_object: paramiko.sftp_client.SFTPClient, relpath: str) -> Optional[List[str]]:
    """
    Handle making directory
    using open sftp connection
    """
    try:
        sftp_object.mkdir(relpath)
    except OSError as err:
        print(f"Error attempting to MKDIR {relpath}\n{err}")
        return None

    relpath = relpath.rstrip("/")
    root, fold = os.path.split(relpath)
    content = sftp_object.listdir(root)
    print(content)
    if fold in str(content):
        return content

    return None


def check_sftp_status(fpath: str, top_folder: str) -> List[str]:
    """
    Check if a file already been
    PUT to SFTP folder API_Uploads
    before intiating repeat upload
    """
    relpath = fpath.split(top_folder)[-1]
    whole_path, _ = os.path.split(relpath)
    remote_path = f"sftp-transfer-source/API_Uploads/{top_folder}/{whole_path}"
    print(f"Checking path: {remote_path}")

    sftp = sftp_connect()
    try:
        content_list = sftp.listdir(remote_path)
    except FileNotFoundError as err:
        print(f"check_sftp_status(): {err}")
        content_list = []

    return content_list


def send_as_package(
    fpath: str,
    top_folder: str,
    atom_slug: str,
    item_priref: str,
    process_config: str,
    auto_approve_arg: bool
) -> Optional[Dict[str, Any]]:
    """
    Send a package using v2 beta package, subject to change!
    Args: fpath from top level no trailing /, AToM slug if known,
    BFI unique ID item_priref, processing config, bool 'True'

    This is the only upload method that allows the API to supply an
    AtoM slug, used to build readable web URLs
    """
    # Build correct folder paths
    package_endpoint = os.path.join(ARCH_URL, "api/v2beta/package")
    folder_path = os.path.basename(fpath)
    path_str = (
        f"{TS_UUID}:/bfi-sftp/sftp-transfer-source/API_Uploads/{top_folder}/{fpath}"
    )
    print(path_str)
    encoded_path = base64.b64encode(path_str.encode("utf-8")).decode("utf-8")

    # Create payload and post
    data_payload = {
        "name": f"{folder_path}",
        "path": encoded_path,
        "type": "standard",
        "processing_config": process_config,
        "accession": item_priref,
        "access_system_id": atom_slug,
        "auto_approve": auto_approve_arg,
    }
    print(json.dumps(data_payload))
    print(f"Starting transfer of {path_str}")
    try:
        response = requests.post(
            package_endpoint, headers=HEADER, data=json.dumps(data_payload)
        )
        response.raise_for_status()
        print(f"Package transfer initiatied - status code {response.status_code}:")
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error: {err}")
    except requests.exceptions.ConnectionError as err:
        print(f"Connection error: {err}")
    except requests.exceptions.Timeout as err:
        print(f"Timeout error: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Request exception: {err}")
    except ValueError:
        print("Response not supplied in JSON format")
        print(f"Response as text:\n{response.text}")
    return None


def get_transfer_status(uuid: str) -> Optional[Dict[str, Any]]:
    """
    Look for transfer status of new transfer/package.
    Returns transfer dictionary with 'status': 'COMPLETED' and
    'sip_uuid': '<UID>', among other. The SIP UUID needed for
    ingest status check function following.
    """

    status_endpoint = os.path.join(ARCH_URL, f"api/transfer/status/{uuid.strip()}")
    try:
        response = requests.get(status_endpoint, headers=HEADER)
        response.raise_for_status()
        print(response.text)
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error: {err}")
    except requests.exceptions.ConnectionError as err:
        print(f"Connection error: {err}")
    except requests.exceptions.Timeout as err:
        print(f"Timeout error: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Request exception: {err}")
    except ValueError:
        print("Response not supplied in JSON format")
        print(f"Response as text:\n{response.text}")
    return None


def get_ingest_status(sip_uuid: str) -> Optional[Dict[str, Any]]:
    """
    Look for ingest status of new transfer/package.
    Returns directory name, message, 'status': 'COMPLETE',
    'type': 'SIP' and 'uuid': <UUID>'. This UUID represents
    the AIP UUID, which may be needed for reingest.
    """

    status_endpoint = os.path.join(ARCH_URL, f"api/ingest/status/{sip_uuid.strip()}")
    try:
        response = requests.get(status_endpoint, headers=HEADER)
        response.raise_for_status()
        print(response.text)
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error: {err}")
    except requests.exceptions.ConnectionError as err:
        print(f"Connection error: {err}")
    except requests.exceptions.Timeout as err:
        print(f"Timeout error: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Request exception: {err}")
    except ValueError:
        print("Response not supplied in JSON format")
        print(f"Response as text:\n{response.text}")
    return None


def get_transfer_list() -> Optional[Dict[str, Any]]:
    """
    Calls to retrieve UUID for
    transfers already in Archivematica
    """

    completed = os.path.join(ARCH_URL, "api/transfer/completed/")
    api_key = f"{API_NAME}:{API_KEY}"
    headers = {"Accept": "*/*", "Authorization": f"ApiKey {api_key}"}

    try:
        response = requests.get(completed, headers=headers)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)

        data = response.json()
        if data and "results" in data:
            return data["results"]
        else:
            print("Error: 'results' key not found in the response.")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
    return None


def get_location_uuids() -> Optional[List[Dict[str, Any]]]:
    """
    Call the v2 locations to retrieve
    UUID locations for different
    Archivematica services
    """
    ss_end = f"{ARCH_URL}:8000/api/v2/location/"
    api_key = f"{SS_NAME}:{SS_KEY}"
    headers = {
        "Authorization": f"ApiKey {api_key}",
        "Content-type": "application/json",
        "Accept": "*/*",
    }

    try:
        response = requests.get(ss_end, headers=headers)
        response.raise_for_status()
        data = json.loads(response.text)
        if "objects" in data:
            return data["objects"]
    except requests.exceptions.RequestException as err:
        print(err)
        return None


def get_atom_objects(skip_path: str) -> Optional[Dict[str, Any]]:
    """
    Return json dict containting
    list of all objects found in AtoM.
    Default max return is 10, skip_path
    returns alternativate return values.
    Called during next function.
    """

    try:
        response = requests.get(
            skip_path,
            auth=("bfi", ATOM_AUTH),
            headers=ATOM_HEADER,
            # accept_redirect=True,
        )
        objects = json.loads(response.text)
        print(f"Objects received: {objects}")
        if objects.get("results", None):
            return objects

    except requests.exceptions.HTTPError as err:
        print(f"HTTP error: {err}")
    except requests.exceptions.ConnectionError as err:
        print(f"Connection error: {err}")
    except requests.exceptions.Timeout as err:
        print(f"Timeout error: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Request exception: {err}")
    except ValueError:
        print("Response not supplied in JSON format")
        print(f"Response as text:\n{response.text}")
    return None


def get_all_atom_objects()-> Optional[List[Any]]:
    """
    Handle skip iteration through all available
    information objects, call get_atom_objects
    with interative skip numbers from totals.
    """

    endpoint = os.path.join(ATOM_URL, "informationobjects")
    objects = get_atom_objects(endpoint)
    if not objects:
        print("Warning, unable to find any informationobjects")
        return None

    total_obs = objects.get("total", None)
    if not isinstance(total_obs, int):
        print(f"Warning, unable to get total information objects from API: {objects}")
        return None

    all_list = []
    for item in objects["results"]:
        all_list.append(item)

    runs = total_obs // 10
    for num in range(1, runs + 1):
        skip_endpoint = f"{endpoint}?skip={num}0"
        new_ob = get_atom_objects(skip_endpoint)
        if not new_ob:
            continue
        for item in new_ob["results"]:
            all_list.append(item)
    if len(all_list) != total_obs:
        print("May not have retrieved all information objects correctly!")

    return all_list


def get_specific_atom_object(ob_num):
    """
    Handle skip iteration through all available
    information objects, call get_atom_objects
    with interative skip numbers from totals.
    """

    endpoint = os.path.join(ATOM_URL, "informationobjects")
    objects = get_atom_objects(endpoint)
    if not objects:
        print("Warning, unable to find any informationobjects")
        return None

    total_obs = objects.get("total", None)
    if not isinstance(total_obs, int):
        print(f"Warning, unable to get total information objects from API: {objects}")
        return None

    for item in objects["results"]:
        if ob_num in item.get("reference_code"):
            return item

    runs = total_obs // 10
    for num in range(1, runs + 1):
        skip_endpoint = f"{endpoint}?skip={num}0"
        new_ob = get_atom_objects(skip_endpoint)
        if not new_ob:
            continue
        for item in new_ob["results"]:
            if ob_num in item.get("reference_code"):
                return item
    return None


def get_slug_match(slug_match: str) -> bool:
    """
    Handles retrieval of all AtoM information objects
    then builds list of slugs and attempts to match to
    argument 'slug_match'.

    Slug must be formatted to match in lowercase and
    '-' instead of white spaces.
    This function has not been fully tested.
    """
    list_of_objects = get_all_atom_objects()
    if list_of_objects is None:
        return None

    for item in list_of_objects:
        try:
            slug = item.get("slug", None)
        except (KeyError, TypeError) as err:
            print(err)
            continue
        if slug is None:
            continue
        elif slug == slug_match:
            return True
    return False


def delete_sip(sip_uuid: str) -> Optional[str]:
    """
    Remove (hide) a SIP from Archivematica
    after it's been transfered in error.
    This function has not been fully tested.
    """
    endpoint = f"{ARCH_URL}/api/ingest/{sip_uuid}/delete/"
    try:
        response = requests.delete(endpoint, headers=HEADER)
        response.raise_for_status()
        print(f"Package deletion success: {response.text}:")
        return response.text
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error: {err}")
        print(f"Response status code: {response.status_code}")
        print(f"Response headers: {response.headers}")
    except requests.exceptions.ConnectionError as err:
        print(f"Connection error: {err}")
    except requests.exceptions.Timeout as err:
        print(f"Timeout error: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Request exception: {err}")
    except ValueError:
        print("Response not supplied in JSON format")
        print(f"Response as text:\n{response.text}")
    return None


def reingest_v2_aip(aip_uuid: str, re_type: str, process_config: str) -> Optional[Dict[str, Any]]:
    """
    Function for reingesting an AIP to create
    an open DIP for AtoM revision
    type = 'PARTIAL' / 'FULL'
    Full needed to supply processing_config update
    Partial used for metadata reingests only
    Returns 'reingest_uuid' key needed for metadata upload.
    You cannot supply slugs (access_system_id) using this endpoint
    """
    package_endpoint = f"{ARCH_URL}:8000/api/v2/file/{aip_uuid}/reingest/"

    # Create payload and post
    data_payload = {
        "pipeline": SS_PIPE,
        "reingest_type": re_type,
        "processing_config": process_config,
    }
    payload = json.dumps(data_payload)
    print(f"Starting reingest of AIP UUID: {aip_uuid}")
    try:
        response = requests.post(package_endpoint, headers=SS_HEADER, data=payload)
        response.raise_for_status()
        print(f"Package transfer initiatied - status code {response.status_code}:")
        print(response.text)
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error: {err}")
        print(f"Response status code: {response.status_code}")
        print(f"Response headers: {response.headers}")
    except requests.exceptions.ConnectionError as err:
        print(f"Connection error: {err}")
    except requests.exceptions.Timeout as err:
        print(f"Timeout error: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Request exception: {err}")
    except ValueError:
        print("Response not supplied in JSON format")
        print(f"Response as text:\n{response.text}")
    return None


def reingest_aip(aip_uuid_name: str, aip_uuid: str, ingest_type: str) -> Optional[Dict[str, Any]]:
    """
    Alternative endpoint for reingesting an AIP.
    Reingest type can be:
    - FULL (file and metadata)
    - PARTIAL (metadata only)
    You cannot supply slugs (access_system_id) using this endpoint
    """
    endpoint = f"{ARCH_URL}/api/transfer/reingest/"

    # Create payload and post
    data_payload = {
        "name": aip_uuid_name,
        "uuid": aip_uuid,
        "reingest_type": ingest_type,
    }
    payload = json.dumps(data_payload)
    print(f"Starting reingest of AIP UUID: {aip_uuid}")
    try:
        response = requests.post(endpoint, headers=HEADER, data=payload)
        response.raise_for_status()
        print(f"Package transfer initiatied - status code {response.status_code}:")
        print(response.text)
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error: {err}")
        print(f"Response status code: {response.status_code}")
        print(f"Response headers: {response.headers}")
    except requests.exceptions.ConnectionError as err:
        print(f"Connection error: {err}")
    except requests.exceptions.Timeout as err:
        print(f"Timeout error: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Request exception: {err}")
    except ValueError:
        print("Response not supplied in JSON format")
        print(f"Response as text:\n{response.text}")
    return None


def metadata_copy_reingest(sip_uuid: str, source_mdata_path: str) -> Optional[Dict[str, Any]]:
    """
    Arg source_mdata_path should be from top level folder
    to metadata only, not absolute path. Top level folder
    is the first folder in sftp root path after 'API_Uploads'

    Where metadata reingest occurs, set copy metadata
    call to requests. Path is to metadata.csv level
    for the given item's correlating aip uuid
    """
    from urllib.parse import urlencode

    mdata_endpoint = os.path.join(ARCH_URL, "api/ingest/copy_metadata_files/")
    mdata_path_str = (
        f"{TS_UUID}:/bfi-sftp/sftp-transfer-source/API_Uploads/{source_mdata_path}"
    )
    encoded_path = base64.b64encode(mdata_path_str.encode("utf-8")).decode("utf-8")

    data_payload = urlencode({"sip_uuid": sip_uuid, "source_paths[]": encoded_path})

    print(json.dumps(data_payload))
    print(f"Starting transfer of {mdata_path_str}")
    try:
        response = requests.post(mdata_endpoint, headers=HEADER_META, data=data_payload)
        response.raise_for_status()
        print(f"Metadata copy initiatied - status code {response.status_code}")
        print(response.text)
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error: {err}")
    except requests.exceptions.ConnectionError as err:
        print(f"Connection error: {err}")
    except requests.exceptions.Timeout as err:
        print(f"Timeout error: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Request exception: {err}")
    except ValueError:
        print("Response not supplied in JSON format")
        print(f"Response as text:\n{response.text}")
    return None


def approve_aip_reingest(uuid: str) -> Optional[Dict[str, Any]]:
    """
    Send approval for reingest.
    This cannot be automated in reingest functions.
    """
    endpoint = f"{ARCH_URL}/api/ingest/reingest/approve/"

    payload = urlencode({"uuid": uuid})

    try:
        response = requests.post(endpoint, headers=HEADER_META, data=payload)
        response.raise_for_status()
        print(f"AIP reingest started {response.status_code}")
        print(response.text)
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error: {err}")
    except requests.exceptions.ConnectionError as err:
        print(f"Connection error: {err}")
    except requests.exceptions.Timeout as err:
        print(f"Timeout error: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Request exception: {err}")
    except ValueError:
        print("Response not supplied in JSON format")
        print(f"Response as text:\n{response.text}")
    return None


def approve_transfer(dir_name: str) -> Optional[Dict[str, Any]]:
    """
    Find transfer that needs approval
    And approve if dir-name matches
    """
    get_unapproved = f"{ARCH_URL}/api/transfer/unapproved/"
    approve_transfer = f"{ARCH_URL}/api/transfer/approve/"

    try:
        response = requests.get(get_unapproved, headers=HEADER_META)
        response.raise_for_status()
        print(f"Tranfers unapproved: {response.status_code}")
        print(response.text)
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error: {err}")
    except requests.exceptions.ConnectionError as err:
        print(f"Connection error: {err}")
    except requests.exceptions.Timeout as err:
        print(f"Timeout error: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Request exception: {err}")
    except ValueError:
        print("Response not supplied in JSON format")
        print(f"Response as text:\n{response.text}")

    dct = json.loads(response.text)
    print(type(dct))
    for lst in dct["results"]:
        for key, value in lst.items():
            if key == "directory" and value.startswith(dir_name):
                payload = urlencode(
                    {
                        "directory": value,
                        "type": "standard",
                    }
                )
                try:
                    response = requests.post(
                        approve_transfer, headers=HEADER_META, data=payload
                    )
                    response.raise_for_status()
                    print(f"Tranfers unapproved: {response.status_code}")
                    print(response.text)
                    return response.json()
                except requests.exceptions.HTTPError as err:
                    print(f"HTTP error: {err}")
                except requests.exceptions.ConnectionError as err:
                    print(f"Connection error: {err}")
                except requests.exceptions.Timeout as err:
                    print(f"Timeout error: {err}")
                except requests.exceptions.RequestException as err:
                    print(f"Request exception: {err}")
                except ValueError:
                    print("Response not supplied in JSON format")
                    print(f"Response as text:\n{response.text}")


def download_aip(aip_uuid: str, dpath: str, fn: str) -> Optional[str]:
    """
    Fetch an AIP stream and 
    write to download path as TAR file
    """
    endpoint = f"{ARCH_URL}:8000/api/v2/file/{aip_uuid}/download/"

    try:
        with requests.get(endpoint, headers=SS_HEADER, stream=True) as response:
            content = response.headers.get("Content-Disposition")
            if content:
                fname = content.split('filename=')[-1].strip('"')
            else:
                fname = f"{fn}.tar"
            download_path = os.path.join(dpath, fname)

            with open(download_path, "wb") as file:
                for chunk in response.iter_content(8192):
                    if chunk:
                        file.write(chunk)
            if os.path.isfile(download_path):
                return download_path
    except requests.exceptions.RequestException as err:
        print(err)
        return None


def _filename_from_content_disposition(cd: str) -> Optional[str]:
    # Very small helper; handles: attachment; filename="x.pdf"
    if not cd:
        return None
    m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', cd)
    return m.group(1) if m else None


def download_normalised_file(ref_code: str, dpath: str) -> Optional[str]:
    """
    Build endpoint from slug (retrieve using ref_code)
    Attempt download of file to dpath, and check for
    extensions, rename file.
    """

    info = get_specific_atom_object(ref_code)
    if not info:
        return None

    slug = info.get("slug")
    if not slug:
        return None

    base = ATOM_URL if ATOM_URL.endswith("/") else ATOM_URL + "/"
    endpoint = urljoin(base, f"informationobjects/{slug}/digitalobject")
    os.makedirs(dpath, exist_ok=True)

    fn_base = ref_code.replace("-", "_")
    headers = dict(HEADER_META or {})
    headers.setdefault("REST-API-Key", API_KEY)
    tmp_path = os.path.join(dpath, fn_base + ".part")

    try:
        with requests.get(
            endpoint,
            headers=headers,
            auth=(SS_NAME, SS_KEY),
            stream=True,
            allow_redirects=True,
            timeout=(10, 300),
        ) as r:
            try:
                r.raise_for_status()
            except requests.HTTPError:
                body_snippet = (r.text or "")[:500]
                raise RuntimeError(
                    f"Download failed: {r.status_code} {r.reason}\n"
                    f"Final URL: {r.url}\n"
                    f"Content-Type: {r.headers.get('Content-Type')}\n"
                    f"Body (first 500 chars):\n{body_snippet}"
                )

            # Get extension
            cd = r.headers.get("Content-Disposition", "")
            server_name = _filename_from_content_disposition(cd)

            ext = ""
            if server_name:
                _, ext = os.path.splitext(server_name)

            if not ext:
                ctype = (r.headers.get("Content-Type") or "").split(";")[0].strip()
                ext = mimetypes.guess_extension(ctype) or ""

            final_path = os.path.join(dpath, fn_base + ext)

            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

            cl = r.headers.get("Content-Length")
            if cl is not None and os.path.getsize(tmp_path) != int(cl):
                raise RuntimeError(
                    f"Incomplete download: wrote {os.path.getsize(tmp_path)} bytes, "
                    f"expected {cl}"
                )

            os.replace(tmp_path, final_path)
            return final_path

    except Exception as err:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass
        print(err)
        return None
