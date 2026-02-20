#!/usr/bin/env python3

"""
Elastic search deletion
script to clean up after
index entries mapped from
CSV_PATH file

DEPENDENCY:
Must run in ENV with
elasticsearch7
"""

import os
import sys
import csv
import logging
from elasticsearch7 import Elasticsearch, TransportError, ConnectionError
from datetime import datetime

ES_HOST = os.environ.get("ES_PATH")
LOG_PATH = os.environ.get("LOG_PATH")
ADMIN = os.environ.get("ADMIN")
CSV_PATH = os.path.join(ADMIN, "code/elasticsearch/elasticsearch_deletion.csv")


# Setup logging
logger = logging.getLogger("elasticsearch_delete_items")
hdlr = logging.FileHandler(os.path.join(LOG_PATH, "elasticsearch_delete_items.log"))
formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)


def main():
    """
    Call up all clients
    from Elasticsearch
    compare to CSV entries
    delete those not needed
    """

    logger.info("Elasticsearch delete items start =======================")
    index_name = "dpi_items"
    try:
        client = Elasticsearch(ES_HOST)
        logger.info("Successfully connected to Elasticsearch.")
    except ConnectionError as err:
        logger.error("ConnectionError: %s", err)
        return

    try:
        client_count = client.count(index=index_name)
        logger.info(f"Index %s contains %s documents initially.", index_name, client_count.get("count"))
    except TransportError as err:
        logger.error("TransportError before deletion: %s", err.info)

    logger.info("Comparing Clients to CSV contents...")
    try:
        with open(CSV_PATH, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            success = 0
            skip = 0
            next(reader)

            for row in reader:
                if not row or not row[0].strip():
                    skip += 1
                    continue

                doc_id = row[0].strip()
                try:
                    res = client.delete(index=index_name, id=doc_id, ignore=[404])
                    if res.get('result') == 'deleted':
                        success += 1
                        logger.info("Successfully deleted %s", doc_id)
                    else:
                        logger.info("Skipped non-existent document: %s", doc_id)
                except TransportError as e:
                    logger.error("Error deleting %s: %s", doc_id, e.info)

        final_count = client.count(index=index_name)
        logger.info(f"Documents deleted: %s, Skipped (already deleted/empty): %s", success, skip)
        logger.info("Documents remaining in index: %s", final_count.get('count'))

    except FileNotFoundError:
        log_error(log_path, "CSV file not found.")
    logger.info("Elasticsearch delete items complete ====================")


if __name__ == "__main__":
    main()
