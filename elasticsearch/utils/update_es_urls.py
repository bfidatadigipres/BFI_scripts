#!/usr/bin/env python3
"""Bulk-update a URL prefix inside a field across an Elasticsearch index.

Replaces a prefix in every document of one index, e.g. pointing
`url_collections_search` at a new collections-search host. Run with `--dry-run`
first to see how many documents would be touched.

Example:

    python update_es_urls.py \
        --index dpi_screencraft \
        --field url_collections_search \
        --old "<old-url-prefix>" \
        --new "<new-url-prefix>" \
        --dry-run

The Elasticsearch endpoint defaults to $ES_SEARCH_PATH.
"""
from __future__ import annotations

import argparse
import os
import sys

from elasticsearch import Elasticsearch, helpers


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--es",
        default=os.environ.get("ES_SEARCH_PATH"),
        help="Elasticsearch endpoint (default: $ES_SEARCH_PATH)",
    )
    parser.add_argument("--index", required=True, help="Index to update")
    parser.add_argument(
        "--field",
        default="url_collections_search",
        help="Field holding the URL (default: url_collections_search)",
    )
    parser.add_argument("--old", required=True, help="URL prefix to replace")
    parser.add_argument("--new", required=True, help="Replacement URL prefix")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report how many documents match, then exit",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.es:
        print(
            "No Elasticsearch endpoint: pass --es or set ES_SEARCH_PATH",
            file=sys.stderr,
        )
        return 2

    es = Elasticsearch(
        args.es, request_timeout=60, max_retries=5, retry_on_timeout=True
    )

    # Exact prefix match against the keyword subfield
    query = {"query": {"prefix": {f"{args.field}.keyword": args.old}}}

    total = es.count(index=args.index, body=query)["count"]
    print(f"Found {total} documents matching {args.field} prefix {args.old!r}")
    if args.dry_run:
        print("Dry run — nothing written")
        return 0

    seen = 0

    def actions():
        nonlocal seen
        for hit in helpers.scan(
            es, index=args.index, query=query, _source=True, scroll="30m", size=1000
        ):
            old_url = hit["_source"][args.field]
            new_url = old_url.replace(args.old, args.new, 1)
            seen += 1
            if seen % 10000 == 0:
                print(f"  Scanned {seen}...")
            yield {
                "_op_type": "update",
                "_index": hit["_index"],
                "_id": hit["_id"],
                "doc": {args.field: new_url},
            }

    success, errors = helpers.bulk(
        es,
        actions(),
        chunk_size=500,
        max_retries=5,
        request_timeout=120,
        stats_only=True,
    )
    print(f"\nDone — updated {success} documents")
    if errors:
        print(f"Errors: {errors}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
