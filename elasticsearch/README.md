# Elasticsearch Indexing

Indexers that fetch records from the CID API (Collections Information Database),
convert the XML to JSON documents and bulk-index them into Elasticsearch, plus
supporting utilities.

Everything lives under `elasticsearch/indexer_scripts/` and reads its
configuration from the environment (see [Environment](#environment)) — no hosts,
paths or credentials are hard-coded in the code.

## Scripts

| Script | Purpose |
|--------|---------|
| `elasticsearch_index_items.py` | Index **item** records from CID into `dpi_items` |
| `elasticsearch_index_screencraft_objects.py` | Index screencraft **objects** into `dpi_screencraft` |
| `elasticsearch_index_screencraft_works.py` | Index screencraft **works** into `dpi_screencraft_works` |
| `elasticsearch_bulk_delete_items.py` | Bulk-delete documents from `dpi_items` by priref (CSV input) |
| `elasticsearch_cleanup_screencraft_works.py` | Detect and clean stale works in `dpi_screencraft_works` |
| `elasticsearch_check_missing_items.py` | Compare a CSV of prirefs with an index and list what is missing |
| `elasticsearch_index_shared.py` | Shared utilities (HTTP session, ES client, XML parsing, bulk pipeline, slow-record guard) |

## Requirements

- Python 3.12+
- `elasticsearch`, `requests`, `defusedxml`, `xmljson`

In this deployment a pre-provisioned virtualenv is used; the interpreter is
referenced by the `ELASTIC_ENV` environment variable (see below), so nothing is
installed from the repository at run time. To run from a different environment,
create a venv and install the packages above.

## Environment

The scripts read all configuration from the environment:

| Variable | Purpose |
|---|---|
| `CID_API1` | CID web-service base URL (`.../wwwopac.ashx`) |
| `ES_SEARCH_PATH` | Elasticsearch HTTP endpoint |
| `LOG_PATH` | Directory for run logs, dead-letter files and slow-record CSVs |
| `ELASTIC_ENV` | Python interpreter of the venv the jobs run with |
| `CODE` | Root of this repository checkout, used in job command lines |
| `SLOW_RECORD_TIMEOUT_SECONDS` | Per-record CID response cap in seconds (default `3`; `0` disables the guard) |
| `SLOW_RECORD_ABORT_CONSECUTIVE` | Abort the run after N consecutive slow records (default `0` = disabled) |
| `SLOW_RECORD_USE_CACHE` | Skip prirefs already listed in the slow-records CSV, without re-probing (default on) |

These are provided system-wide on the indexing host (loaded by PAM, so scheduled
jobs see them too). Values are environment-specific and are not kept in the repository.

## Running the indexers

Four jobs run in order overnight, each covering a recent window, so that only one
indexer is ever talking to CID at a time:

| Time | Script |
|---|---|
| 00:00 | `elasticsearch_cleanup_screencraft_works.py` |
| 02:00 | `elasticsearch_index_items.py` |
| 04:00 | `elasticsearch_index_screencraft_objects.py` |
| 06:00 | `elasticsearch_index_screencraft_works.py` |

Command lines use the environment variables, for example:

```cron
0 2 * * * datadigipres ${ELASTIC_ENV} ${CODE}elasticsearch/indexer_scripts/elasticsearch_index_items.py > /tmp/python_cron.log 2>&1
```

Each run appends a `SUMMARY` line to its own log under `${LOG_PATH}`. Do not run
two indexers at once, and avoid overlapping a backfill with the overnight jobs.

## Usage

### Items indexer

```sh
# Default: query both items + works for today-2
"$ELASTIC_ENV" elasticsearch/indexer_scripts/elasticsearch_index_items.py

# Specific date range (inclusive)
"$ELASTIC_ENV" elasticsearch/indexer_scripts/elasticsearch_index_items.py --date-from 2026-03-18 --date-to 2026-03-18

# Query mode: items, works or both (default)
"$ELASTIC_ENV" elasticsearch/indexer_scripts/elasticsearch_index_items.py --query items

# Direct priref lookup
"$ELASTIC_ENV" elasticsearch/indexer_scripts/elasticsearch_index_items.py --prirefs 123456,234567

# CSV priref input (no limit)
"$ELASTIC_ENV" elasticsearch/indexer_scripts/elasticsearch_index_items.py --prirefs-csv prirefs.csv
```

### Screencraft indexers

```sh
# Date range
"$ELASTIC_ENV" elasticsearch/indexer_scripts/elasticsearch_index_screencraft_objects.py --date-from 2026-03-01 --date-to 2026-03-31

# Custom CID search (replaces the date-range query)
"$ELASTIC_ENV" elasticsearch/indexer_scripts/elasticsearch_index_screencraft_objects.py --search "Df='archival item','digital derivative','internal object' and ..."
"$ELASTIC_ENV" elasticsearch/indexer_scripts/elasticsearch_index_screencraft_works.py --search "Df=work and ..."

# Direct prirefs
"$ELASTIC_ENV" elasticsearch/indexer_scripts/elasticsearch_index_screencraft_objects.py --prirefs 11,12,13

# CSV priref input (no limit)
"$ELASTIC_ENV" elasticsearch/indexer_scripts/elasticsearch_index_screencraft_objects.py --prirefs-csv prirefs.csv
"$ELASTIC_ENV" elasticsearch/indexer_scripts/elasticsearch_index_screencraft_works.py --prirefs-csv prirefs.csv
```

### Bulk delete

```sh
"$ELASTIC_ENV" elasticsearch/indexer_scripts/elasticsearch_bulk_delete_items.py --csv prirefs.csv --dry-run
"$ELASTIC_ENV" elasticsearch/indexer_scripts/elasticsearch_bulk_delete_items.py --csv prirefs.csv
```

### Screencraft works cleanup

Detects stale Work documents: for each changed screencraft object, it looks up
the works index by the object's media number, then for any stale Work doc it
checks CID for remaining in-scope screencraft links (linked objects that carry
media) — refreshing the doc from CID when in-scope links exist, deleting it
when none do.

```sh
# Date range (default: today-2)
"$ELASTIC_ENV" elasticsearch/indexer_scripts/elasticsearch_cleanup_screencraft_works.py --date-from 2026-09-01 --date-to 2026-09-03 --dry-run

# Direct prirefs / CSV input
"$ELASTIC_ENV" elasticsearch/indexer_scripts/elasticsearch_cleanup_screencraft_works.py --prirefs 11232350,11232352
"$ELASTIC_ENV" elasticsearch/indexer_scripts/elasticsearch_cleanup_screencraft_works.py --prirefs-csv prirefs.csv

# Run for real (writes to Elasticsearch)
"$ELASTIC_ENV" elasticsearch/indexer_scripts/elasticsearch_cleanup_screencraft_works.py --date-from 2026-09-01 --date-to 2026-09-03
```

Flow:

1. Fetch changed screencraft object prirefs (date query: `Df=...` + `modification` window)
2. For each object, read its live CID Work link (`moving_image_work.priref`)
3. Search the works index for docs whose `media_objects` contain the object number
4. Classify hits as **correct** (priref matches the live CID Work link) or **stale**
5. For each stale Work, query CID for records still linking to it, counting only
   in-scope links (linked objects that carry media)
6. In-scope links exist → refresh the Work doc from CID; none → delete the Work doc

`--dry-run` performs the full analysis and reports planned actions without writing to Elasticsearch.

### Missing-items check

Compares a CSV of prirefs (first column; a header row is fine) against an index
and writes the prirefs that are **not** present, so they can be re-indexed:

```sh
python3 elasticsearch/indexer_scripts/elasticsearch_check_missing_items.py --csv all_items.csv
# -> all_items_missing_from_es.csv

"$ELASTIC_ENV" elasticsearch/indexer_scripts/elasticsearch_index_items.py --prirefs-csv all_items_missing_from_es.csv
```

Options: `--index` (default `dpi_items`), `--es` (default `$ES_SEARCH_PATH`),
`--batch`, `--out`. Read-only against Elasticsearch; safe to re-run.

## Slow records

Some CID records are pathologically slow to render through the per-record lookup
(seconds to ~40s, versus the usual 0.05–0.5s), and a handful of them would hold
up a whole run. Each record fetch is therefore time-capped:

- a record slower than `SLOW_RECORD_TIMEOUT_SECONDS` (default 3) is skipped and
  appended to `<prefix>_slow_records.csv` as `priref,seconds`;
- the run continues; the `SUMMARY` line reports `slow_records_skipped` and
  `slow_records_cached`;
- prirefs already listed in the slow-records CSV are skipped without re-probing
  (`SLOW_RECORD_USE_CACHE=1` by default);
- `SLOW_RECORD_ABORT_CONSECUTIVE` can abort the run after N consecutive slow
  records — default `0` (disabled), because genuine slow clusters can run to
  thousands of records;
- the guarded fetch uses a session without automatic retries, so the cap is a
  hard bound. Other failures still go to the dead-letter file as before.

Re-index a slow set once the CID side is fixed:

```sh
"$ELASTIC_ENV" elasticsearch/indexer_scripts/elasticsearch_index_items.py --prirefs-csv item_slow_records.csv
```

## How it works

1. Resolve the requested date range or priref list
2. Fetch prirefs from the CID API (`prirefcollectraw`)
3. Write raw priref responses to a trace file (`*_prirefs.txt`)
4. Deduplicate prirefs
5. Fetch full XML for each unique priref (with the slow-record guard above)
6. Parse XML securely with `defusedxml`
7. Convert XML to a JSON document using `xmljson/parker`
8. Bulk index into Elasticsearch (priref used as `_id`)
9. Log failures to a dead-letter file (`*_dead_letter.jsonl`)

For date ranges larger than 2 days, the scripts automatically split into
per-day CID calls to avoid timeouts.

Individual item XML fetches are throttled to 250ms between requests to avoid
overwhelming the CID API.

## Output files

- `*_prirefs.txt` — raw CID priref responses (trace only)
- `*_indexing.log` / `*_cleanup.log` — run log (stdout + file)
- `*_dead_letter.jsonl` — JSONL failure records (indexers: cid_fetch, xml_parse, es_index; cleanup: cid_fetch, xml_parse, es_search, cid_reverse, es_review, es_update, es_delete)
- `*_slow_records.csv` — prirefs skipped for exceeding the slow-record timeout, with the measured seconds

## CID endpoints

- Base: `${CID_API1}` (`.../wwwopac.ashx`)
- Priref collection: `database=prirefcollectraw`
- Item XML lookup: `database=elasticsearchitems`
- Screencraft XML lookup: `database=elasticsearchscreencraft_objects` / `elasticsearchscreencraft_works`

## Elasticsearch targets

| Index | Script |
|-------|--------|
| `dpi_items` | `elasticsearch_index_items.py` |
| `dpi_screencraft` | `elasticsearch_index_screencraft_objects.py` |
| `dpi_screencraft_works` | `elasticsearch_index_screencraft_works.py` / `elasticsearch_cleanup_screencraft_works.py` |

## Utilities

- `utils/update_es_urls.py` — bulk-update a URL prefix in a field across an
  index (e.g. repointing links at a new collections-search host). Takes
  `--index`, `--field`, `--old`, `--new`, optional `--es`, and supports
  `--dry-run`.
