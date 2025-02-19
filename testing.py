import os
import requests
import json
from typing import Optional, Any, Iterable

HEADERS = {
    'Content-Type': 'text/xml'
}
def retrieve_record(api: str, database: str, search: str, limit: str, fields=None) -> tuple[Optional[int], Optional[list[dict[str, Iterable[Any]]]]]:
    '''
    Retrieve data from CID using new API
    '''
    if search.startswith('priref='):
        search_new = search
    else:
        if database == 'items':
            search_new = f'(record_type=ITEM) and {search}'
        elif database == 'works':
            search_new = f'(record_type=WORK) and {search}'
        elif database == 'manifestations':
            search_new = f'(record_type=MANIFESTATION) and {search}'
        else:
            search_new = search

    query = {
        'database': database,
        'search': search_new,
        'limit': limit,
        'output': 'jsonv1'
    }

    if fields:
        field_str = ', '.join(fields)
        query['fields'] = field_str

    record = get(api, query)
    if not record:
        print(query)
        return None, None
    if record['adlibJSON']['diagnostic']['hits'] == 0:
        return 0, None
    if 'recordList' not in str(record):
        try:
            hits = int(record['adlibJSON']['diagnostic']['hits'])
            return hits, record
        except (IndexError, KeyError, TypeError) as err:
            print(err)
            return 0, record

    hits = int(record['adlibJSON']['diagnostic']['hits'])
    return hits, record['adlibJSON']['recordList']['record']


def get(api: str, query: dict[str, str]):
    '''
    Send a GET request
    '''
    try:
        req = requests.request('GET', api, headers=HEADERS, params=query)
        if req.status_code != 200:
            raise Exception
        dct = json.loads(req.text)
        return dct
    except requests.exceptions.Timeout as err:
        print(err)
        raise Exception from err
    except requests.exceptions.ConnectionError as err:
        print(err)
        raise Exception from err
    except requests.exceptions.HTTPError as err:
        print(err)
        raise Exception from err
    except Exception as err:
        print(err)
        raise Exception from err


if __name__ == '__main__':
    CID_API = os.environ['CID_API4']
    hit, record = retrieve_record(CID_API, 'works', 'priref=158884597', '0', ['grouping.lref', 'title', 'edit.name'])
    #print(retrieve_field_name(record[0], 'priref'))
    print(record[0])

    {'@attributes': {'priref': '158884597', 'created': '2025-02-14T06:01:25', 'modification': '2025-02-14T06:01:29', 'selected': 'False', 'deleted': 'False'}, 
    'Title': [{'title': [{'spans': [{'text': 'A&E After Dark'}]}]}], 
    'grouping.lref': [{'spans': [{'text': '398775'}]}]}