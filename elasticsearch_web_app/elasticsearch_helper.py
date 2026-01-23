import os
from functools import lru_cache

import pandas as pd
from elasticsearch import Elasticsearch

ELASTIC_PASS = os.environ["ELASTIC_PASS"]


@lru_cache(maxsize=1)
def get_autoingest_docs(es):
    indices = es.cat.indices(format="json")
    list_of_auto = [idx["index"] for idx in indices if "autoingest" in idx["index"]]
    return list_of_auto


def search_by_filepath(filepath, list_of_auto, es):
    query = {
        "bool": {
            "should": [
                {"term": {"filename.keyword": f"{filepath}"}},
                {"wildcard": {"filename.keyword": {"value": f"{filepath}*of*"}}},
            ],
            "minimum_should_match": 1,
        }
    }
    df = pd.DataFrame()

    for index in list_of_auto:
        response = es.search(index=index, query=query)
        df = pd.concat(
            [df, pd.json_normalize(x["_source"] for x in response["hits"]["hits"])],
            ignore_index=True,
        )

    return df
