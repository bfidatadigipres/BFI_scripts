#!/usr/bin/env python3

"""
Python interface for Adlib API v3.7.17094.1+
(http://api.adlibsoft.com/site/api)

2024
"""

from datetime import datetime, timedelta
import json
from time import sleep
from typing import Any, Optional, List, Dict, Tuple, Union
from tenacity import retry, stop_after_attempt
import xmltodict
from requests import Session, exceptions, request

HEADERS = {"Content-Type": "text/xml"}
TIMEOUT = 100


def check(api: str) -> dict[str, Any]:
    """
    Check API responds
    """
    query = {"command": "getversion", "limit": 0, "output": "jsonv1"}

    return get(api, query)


def create_session() -> Session:
    """
    Start a requests session and return
    """
    session = Session()
    return session


def retrieve_record(api: str, database: str, search: str, limit: Union[int, str], session: Optional[Session] = None, fields: Optional[list[str]] = None) -> tuple[Optional[int], Union[list[dict[str, Any]], dict[str, Any], None]]:
    """
    Retrieve data from CID using new API
    """
    if search.startswith("priref="):
        search_new = search
    else:
        if database == "items":
            search_new = f"(record_type=ITEM) and {search}"
        elif database == "works":
            search_new = f"(record_type=WORK) and {search}"
        elif database == "manifestations":
            search_new = f"(record_type=MANIFESTATION) and {search}"
        else:
            search_new = search

    query = {
        "database": database,
        "search": search_new,
        "limit": limit,
        "output": "jsonv1",
    }

    if fields:
        field_str = ", ".join(fields)
        query["fields"] = field_str

    record = get(api, query, session)
    if not record:
        return None, None
    if record["adlibJSON"]["diagnostic"]["hits"] == 0:
        return 0, None
    if "recordList" not in str(record):
        try:
            hits = int(record["adlibJSON"]["diagnostic"]["hits"])
            return hits, record
        except (IndexError, KeyError, TypeError) as err:
            print(err)
            return 0, record

    hits = int(record["adlibJSON"]["diagnostic"]["hits"])
    return hits, record["adlibJSON"]["recordList"]["record"]


@retry(stop=stop_after_attempt(10))
def get(api: str, query: dict[str, str], session: Optional[Session] = None) -> dict[str, Any]:
    """
    Send a GET request
    """
    if not session:
        session = create_session()
    try:
        req = session.get(api, headers=HEADERS, params=query, timeout=TIMEOUT)
        if req.status_code != 200:
            raise Exception
        dct = json.loads(req.text)
        return dct
    except exceptions.HTTPError as err:
        print(f"HTTP error: {err}")
        raise Exception from err
    except exceptions.ConnectionError as err:
        print(f"Connection error: {err}")
        raise Exception from err
    except exceptions.Timeout as err:
        print(f"Timeout error: {err}")
        raise Exception from err
    except exceptions.RequestException as err:
        print(f"Request exception: {err}")
        raise Exception from err
    except Exception as err:
        print(err)
        raise Exception from err


def _time_window_last_15min(type: str) -> str:
    """
    Return a search string for records created in the last 15 minutes.
    Type determines search against 'creation' or 'modification'
    """
    now = datetime.now()
    window_start = now - timedelta(minutes=15)
    return f"{type} > '{window_start.strftime('%Y-%m-%d %H:%M:%S')}'"


def post_with_verify(
    api: str,
    payload: str,
    database: str,
    method: str,
    session: Optional[Session] = None,
    search_value: str = "",
    max_retries: int = 3,
    retry_delay: int = 10,
) -> Optional[dict[str, Any]]:
    """
    POST a record, then verify with a GET if the POST appears to fail.
    Handles both network failures (no response) and false SQL failures
    search_value is the unique field to search for in the
    GET verification query. Returns the parsed record dict on success,
    None after all retries exhausted.
    """
    if not session:
        session = create_session()

    for attempt in range(1, max_retries + 1):
        result = post(api, payload, database, method, session)

        if result and '{"@attributes":{"priref":' in str(result):
            return result

        print(f"post_with_verify(): POST attempt {attempt} returned no priref, "
              f"waiting {retry_delay}s then checking via GET...")
        sleep(retry_delay)

        if method == 'updaterecord':
            search = f"{_time_window_last_15min('modification')} and {search_value}"
        else:
            search = f"{_time_window_last_15min('creation')} and {search_value}"
        try:
            hits, record = retrieve_record(api, database, search, 1, session)
            if hits and hits > 0:
                print(f"post_with_verify(): Record found on GET after POST failure "
                      f"(attempt {attempt}) — returning existing record")
                return record
        except Exception as err:
            print(f"post_with_verify(): GET verification failed: {err}")

        print(f"post_with_verify(): Record not found on GET, retrying POST "
              f"(attempt {attempt}/{max_retries})")

    print(f"post_with_verify(): All {max_retries} attempts exhausted for "
          f"{database}/{search_value}")
    return None


def post(api: str, payload: str, database: str, method: str, session: Optional[Session] = None) -> Union[dict[str, Any], bool, None]:
    """
    Send a POST request
    """
    params = {
        "command": method,
        "database": database,
        "xmltype": "grouped",
        "output": "jsonv1",
    }
    payload = payload.encode("utf-8")

    if not session:
        session = create_session()

    try:
        response = session.post(
            api, headers=HEADERS, params=params, data=payload, timeout=TIMEOUT
        )
    except exceptions.Timeout as err:
        print(f"POST timeout: {err}")
        return None
    except exceptions.ConnectionError as err:
        print(f"POST connection error: {err}")
        return None
    except exceptions.HTTPError as err:
        print(f"POST HTTP error: {err}")
        return None
    except exceptions.RequestException as err:
        print(f"POST request exception: {err}")
        return None
    except Exception as err:
        print(f"POST unexpected error: {err}")
        return None

    print("-------------------------------------")
    print(f"adlib_v3.POST(): {response.text}")
    print("-------------------------------------")

    boolean = check_response(response.text, api)
    if boolean is True:
        return False
    if "recordList" in response.text:
        record = json.loads(response.text)
        try:
            if isinstance(record["adlibJSON"]["recordList"]["record"], list):
                return record["adlibJSON"]["recordList"]["record"][0]
            return record["adlibJSON"]["recordList"]["record"]
        except (KeyError, IndexError, TypeError):
            return record
    elif "@attributes" in response.text:
        record = json.loads(response.text)
        return record
    elif "error" in response.text:
        return None

    return None


def retrieve_field_name(record: dict[str, Any], fieldname: str) -> list[Any]:
    """
    Retrieve record, check for language data
    Alter retrieval method. record ==
    ['adlibJSON']['recordList']['record'][0]
    """
    field_list = []

    try:
        for field in record[f"{fieldname}"]:
            if isinstance(field, str):
                field_list.append(field)
            elif "'@lang'" in str(field):
                field_list.append(field["value"][0]["spans"][0]["text"])
            else:
                field_list.append(field["spans"][0]["text"])
    except KeyError:
        field_list = group_check(record, fieldname)

    if not isinstance(field_list, list):
        return [field_list]
    return field_list


def retrieve_facet_list(record: dict[str, Any], fname: str) -> list[str]:
    """
    Retrieve list of facets
    """
    facets = []
    for value in record["adlibJSON"]["facetList"][0]["values"]:
        facets.append(value[fname]["spans"][0]["text"])

    return facets


def group_check(record: dict[str, Any], fname: str) -> Union[list[str], list[dict[str, Any]], None]:
    """
    Get group that contains field key
    """
    group_check = dict([(k, v) for k, v in record.items() if f"{fname}" in str(v)])
    fieldnames = []
    if len(group_check) == 1:
        first_key = next(iter(group_check))
        for entry in group_check[f"{first_key}"]:
            for key, val in entry.items():
                if str(key) == str(fname):
                    if "@lang" in str(val):
                        try:
                            fieldnames.append(val[0]["value"][0]["spans"][0]["text"])
                        except (IndexError, KeyError):
                            pass
                    else:
                        try:
                            fieldnames.append(val[0]["spans"][0]["text"])
                        except (IndexError, KeyError):
                            pass
        if fieldnames:
            return fieldnames

    elif len(group_check) > 1:
        all_vals = []
        for kname in group_check:
            for key, val in group_check[f"{kname}"][0].items():
                if key == fname:
                    dictionary = {}
                    dictionary[fname] = val
                    all_vals.append(dictionary)
        if len(all_vals) == 1:
            if "@lang" in str(all_vals):
                try:
                    return all_vals[0][fname][0]["value"][0]["spans"][0]["text"]
                except KeyError:
                    print(f"Failed to extract value: {all_vals}")
                    return None
            else:
                try:
                    return all_vals[0][fname][0]["spans"][0]["text"]
                except KeyError:
                    print(f"Failed to extract value: {all_vals}")
                    return None
        else:
            return all_vals
    else:
        return None


def get_grouped_items(api: str, database: str, session: Optional[Session] = None) -> Union[dict[str, list[str]], tuple[None, None]]:
    """
    Check dB for groupings and ensure
    these are added to XML configuration
    """
    query = {"command": "getmetadata", "database": database, "limit": 0}
    if not session:
        session = create_session()
    result = session.get(api, headers=HEADERS, params=query, timeout=TIMEOUT)
    metadata = xmltodict.parse(result.text)
    if not isinstance(metadata, dict):
        return None, None

    grouped: dict[str, list[str]] = {}
    mdata = metadata["adlibXML"]["recordList"]["record"]
    for num in range(0, len(mdata)):
        try:
            group = mdata[num]["group"]
            field_name = mdata[num]["fieldName"]["value"][0]["#text"]
            if group in grouped.keys():
                grouped[group].append(field_name)
            else:
                grouped[group] = [field_name]
        except KeyError:
            pass

    return grouped


def create_record_data(api: str, database: str, sess: Session, priref: Union[str, int], data: Optional[list[dict[str, str]]] = None) -> str:
    if data is None:
        data = []
    if not isinstance(data, list):
        data = [data]

    grouped = get_grouped_items(api, database, sess)
    new_grouping: Dict[str, List[Dict[str, str]]] = {}
    non_grouped_items: List[Dict[str, str]] = []

    for item in data:
        group_found = False
        for group_key, fields in grouped.items():
            if group_key not in new_grouping:
                new_grouping[group_key] = []

            access_record = {k: item[k] for k in item if k in fields}
            if access_record:
                new_grouping[group_key].append(access_record)
                group_found = True
                break
        if not group_found:
            non_grouped_items.append(item)

    for k, v in new_grouping.items():
        if v != []:
            print(f"Adjusted grouping data: {k}: {v}")

    # Build repeat blocks by detecting when a field name recurs
    record_data: Dict[str, List[List[Dict[str, str]]]] = {}
    for group_key, records in new_grouping.items():
        if not records:
            continue
        record_data[group_key] = []
        current_block: List[Dict[str, str]] = []
        seen_keys: set = set()

        for record_item in records:
            item_keys = set(record_item.keys())
            # If any key in this item was already seen in the current block,
            # we're starting a new repeat instance
            if item_keys & seen_keys:
                record_data[group_key].append(current_block)
                current_block = []
                seen_keys = set()

            current_block.append(record_item)
            seen_keys.update(item_keys)

        if current_block:
            record_data[group_key].append(current_block)

    # Build XML
    output_list = []
    output_list.append(f"<priref>{priref or 0}</priref>")

    for ng_item in non_grouped_items:
        for key, value in ng_item.items():
            output_list.append(f"<{key}>{escape_xml(value)}</{key}>")

    for group_key, blocks in record_data.items():
        for block in blocks:
            output_list.append(f"<{group_key}>")
            for record_item in block:
                for key, value in record_item.items():
                    output_list.append(f"<{key}>{escape_xml(value)}</{key}>")
            output_list.append(f"</{group_key}>")

    payload = "".join(output_list)
    return f"<adlibXML><recordList><record>{payload}</record></recordList></adlibXML>"


def escape_xml(s: str) -> str:
    """
    Escape characters that break
    XML POST to CID
    """
    if not isinstance(s, str):
        return s
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def create_grouped_data(priref: Optional[str], grouping: str, field_pairs: list[Union[list[dict[str, str]], dict[str, str]]]) -> Optional[str]:
    """
    Handle repeated groups of fields pairs, suppied as list of dcts per group
    along with grouping known in advance and priref for append
    """
    if not priref:
        return None

    payload_mid = ""
    for lst in field_pairs:
        mid = ""
        mid_fields = ""
        if isinstance(lst, list):
            for grouped in lst:
                for key, value in grouped.items():
                    xml_field = f"<{key}><![CDATA[{value}]]></{key}>"
                    mid += xml_field
        elif isinstance(lst, dict):
            for key, value in lst.items():
                xml_field = f"<{key}><![CDATA[{value}]]></{key}>"
                mid += xml_field
        mid_fields = f"<{grouping}>" + mid + f"</{grouping}>"
        payload_mid = payload_mid + mid_fields

    if len(priref) > 0:
        payload = f"<adlibXML><recordList><record priref='{priref}'>"
        payload_end = "</record></recordList></adlibXML>"
        return payload + payload_mid + payload_end
    else:
        return payload_mid


def add_quality_comments(api: str, priref: str, comments: str, session: Optional[Session] = None) -> bool:
    """
    Receive comments string
    convert to XML quality comments
    and updaterecord with data
    """

    p_start = f"<adlibXML><recordList><record priref='{priref}'><quality_comments>"
    date_now = str(datetime.now())[:10]
    p_comm = f"<quality_comments><![CDATA[{comments}]]></quality_comments>"
    p_date = f"<quality_comments.date>{date_now}</quality_comments.date>"
    p_writer = "<quality_comments.writer>datadigipres</quality_comments.writer>"
    p_end = "</quality_comments></record></recordList></adlibXML>"
    payload = p_start + p_comm + p_date + p_writer + p_end

    if not session:
        session = create_session()

    response = session.post(
        api,
        headers={"Content-Type": "text/xml"},
        params={
            "database": "items",
            "command": "updaterecord",
            "xmltype": "grouped",
            "output": "jsonv1",
        },
        data=payload,
        timeout=TIMEOUT,
    )
    if "error" in str(response.text):
        return False
    else:
        return True


def check_response(rec: str, api: str) -> Optional[bool]:
    """
    Collate list of received API failures
    and check for these reponses from post
    actions. Initiate recycle
    """
    failures = [
        "A severe error occurred on the current command.",
        "Execution Timout Expired. The timeout period elapsed",
    ]

    for warning in failures:
        if warning in str(rec):
            recycle_api(api)
            return True


def recycle_api(api: str) -> None:
    """
    Adds a search call to API which
    triggers Powershell recycle
    """
    search = "title=recycle.application.pool.data.test"
    req = request("GET", api, headers=HEADERS, params=search, timeout=TIMEOUT)
    print(f"Search to trigger recycle sent: {req}")
    print("Pausing for 2 minutes")
    sleep(120)
