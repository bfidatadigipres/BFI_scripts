# Elasticsearch Indexing

Fetches records from the CID API (Collections Information Database), converts XML to JSON documents, and bulk indexes them into Elasticsearch.

## Scripts

| Script | Purpose |
|--------|---------|
| `elasticsearch_index_items.py` | Index **item** records from CID into `dpi_items` |
| `elasticsearch_index_screencraft_objects.py` | Index screencraft **objects** into `dpi_screencraft` |
| `elasticsearch_index_screencraft_works.py` | Index screencraft **works** into `dpi_screencraft_works` |
| `elasticsearch_bulk_delete_items.py` | Bulk-delete documents from `dpi_items` by priref (CSV input) |
| `elasticsearch_cleanup_screencraft_works.py` | Detect and clean stale works in `dpi_screencraft_works` |
| `elasticsearch_index_shared.py` | Shared utilities (HTTP session, ES client, XML parsing, bulk pipeline) |

## Dependencies

Python dependencies are declared in `pyproject.toml` and locked in `uv.lock`. Python version is pinned in `.python-version`.

- **Primary tool**: [uv](https://docs.astral.sh/uv/) — installs from the lockfile for reproducible builds.

The repository lives on a network filesystem (`/mnt/qnap_04`), which is far too slow (and fragile) to host a virtualenv. The venv is created on **local disk** instead, at `~/.venvs/elasticsearch-indexing`:

```sh
# With uv (recommended) — create/refresh the local venv from this repo
UV_PROJECT_ENVIRONMENT=~/.venvs/elasticsearch-indexing uv sync

# Or with pip (won't use the lockfile)
python3 -m venv ~/.venvs/elasticsearch-indexing
~/.venvs/elasticsearch-indexing/bin/pip install -e .
```

To add a dependency:
```sh
uv add <package>
```

This updates both `pyproject.toml` and `uv.lock` in one step (then re-run `uv sync` as above).

The venv is self-contained — no need for `pip install -e` in your crontab. Use the local venv's Python binary directly:

```cron
30 2 * * * cd /path/to/code/elasticsearch && /home/mcconnachies/.venvs/elasticsearch-indexing/bin/python elasticsearch_index_screencraft_objects.py >> /tmp/cron_screencraft.log 2>&1
```

## Usage

### Items indexer

```sh
# Default: query both items + works for today-2
python3 elasticsearch_index_items.py

# Specific date range
python3 elasticsearch_index_items.py --date-from 2026-03-18 --date-to 2026-03-18

# Query mode
python3 elasticsearch_index_items.py --query items

# Direct priref lookup
python3 elasticsearch_index_items.py --prirefs 123456,234567

# CSV priref input (no limit)
python3 elasticsearch_index_items.py --prirefs-csv prirefs.csv
```

### Screencraft indexers

```sh
# Date range
python3 elasticsearch_index_screencraft_objects.py --date-from 2026-03-01 --date-to 2026-03-31

# Custom CID search (replaces date-range query)
python3 elasticsearch_index_screencraft_objects.py --search "Df='archival item','digital derivative','internal object' and ..."
python3 elasticsearch_index_screencraft_works.py --search "Df=work and ..."

# Direct prirefs
python3 elasticsearch_index_screencraft_objects.py --prirefs 11,12,13

# CSV priref input (no limit)
python3 elasticsearch_index_screencraft_objects.py --prirefs-csv prirefs.csv
python3 elasticsearch_index_screencraft_works.py --prirefs-csv prirefs.csv
```

### Bulk delete

```sh
python3 elasticsearch_bulk_delete_items.py --csv prirefs.csv --dry-run
python3 elasticsearch_bulk_delete_items.py --csv prirefs.csv
```

### Screencraft works cleanup

Detects stale Work documents: for each changed screencraft object, it looks up
the works index by the object's media number, then for any stale Work doc it
checks CID for remaining in-scope screencraft links (linked objects that carry
media) — refreshing the doc from CID when in-scope links exist, deleting it
when none do.

```sh
# Date range (default: today-2)
python3 elasticsearch_cleanup_screencraft_works.py --date-from 2026-09-01 --date-to 2026-09-03 --dry-run

# Direct prirefs / CSV input
python3 elasticsearch_cleanup_screencraft_works.py --prirefs 11232350,11232352
python3 elasticsearch_cleanup_screencraft_works.py --prirefs-csv prirefs.csv

# Run for real (writes to Elasticsearch)
python3 elasticsearch_cleanup_screencraft_works.py --date-from 2026-09-01 --date-to 2026-09-03
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

## How it works

1. Resolve the requested date range or priref list
2. Fetch prirefs from the CID API (prirefcollectraw)
3. Write raw priref responses to a trace file (`*_prirefs.txt`)
4. Deduplicate prirefs
5. Fetch full XML for each unique priref
6. Parse XML securely with `defusedxml`
7. Convert XML to a JSON document using `xmljson/parker`
8. Bulk index into Elasticsearch (priref used as `_id`)
9. Log failures to a dead-letter file (`*_dead_letter.jsonl`)

For date ranges larger than 2 days, the scripts automatically split into per-day CID calls to avoid timeouts.

Individual item XML fetches are throttled to 250ms between requests to avoid overwhelming the CID API.

## Output files

- `*_prirefs.txt` — raw CID priref responses (trace only)
- `*_indexing.log` / `*_cleanup.log` — run log (stdout + file)
- `*_dead_letter.jsonl` — JSONL failure records (indexers: cid_fetch, xml_parse, es_index; cleanup: cid_fetch, xml_parse, es_search, cid_reverse, es_review, es_update, es_delete)

## CID endpoints

- Base: `http://212.114.101.119/CIDDataSandbox/wwwopac.ashx`
- Priref collection: `database=prirefcollectraw`
- Item XML lookup: `database=elasticsearchitems`
- Screencraft XML lookup: `database=elasticsearchscreencraft_objects` / `elasticsearchscreencraft_works`

## Elasticsearch targets

| Index | Script |
|-------|--------|
| `dpi_items` | `elasticsearch_index_items.py` |
| `dpi_screencraft` | `elasticsearch_index_screencraft_objects.py` |
| `dpi_screencraft_works` | `elasticsearch_index_screencraft_works.py` / `elasticsearch_cleanup_screencraft_works.py` |
