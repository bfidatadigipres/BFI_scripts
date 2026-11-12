# adlib_gateway

A single, central queue/controller for all Axiell (Adlib) CID API traffic.

Today every script under `BFI_scripts` imports `adlib_v3` / `adlib_v3_sess`
and calls the Axiell API directly. Cron launchers run them with GNU parallel
(up to `--jobs 20`) across several servers, so dozens of processes hit one
SQL-backed API at once. That concurrency is what produces the intermittent
SQL "Execution Timeout Expired" / "A severe error occurred" responses, lost
POST confirmations, orphan records and the 120s IIS app-pool recycles.

This folder contains:

* **`client/adlib.py`** - a new unified, session-based library that replaces
  both legacy libraries. Pure helpers (XML building, field extraction) run
  locally; every network call is proxied to the gateway.
* **`gateway/`** - a small Flask service (run under gunicorn + systemd on ONE
  host) that queues all calls, serialises writes, retries with backoff, and
  performs app-pool recycles centrally.
* **`systemd/`** - service unit + env file template for the gateway host.
* **`tests/`** - 54 unit/end-to-end tests (no network required).

Nothing outside this folder is modified by dropping it in. See
`~/Desktop/adlib_gateway_implementation.md` for the step-by-step rollout
(gateway host setup, env vars on every server, and the `adlib_v3*` shim swap).

## Architecture

```
[server A: cron + parallel]--+
[server B: cron + parallel]--+--> adlib_gateway (one host, systemd/gunicorn)
[Dagster hosts]--------------+      |- per-caller round-robin scheduler
        |                            |- POST queue -> 1 writer worker
   client/adlib.py                   |- GET  queue -> 3 reader workers
   (thin, session-based,             |- retry + backoff + jitter
    same function names)             |- post_with_verify (atomic, server-side)
                                     |- recycle + dispatch pause (centralised)
                                     \- request log: caller/depth/wait/outcome
                                                |
                                                v
                                      Axiell API -> SQL Server
```

Fairness: each caller (`script/pid@host`) gets its own FIFO sub-queue and the
scheduler rotates between callers, so a 2,400-call bulk job (e.g.
`document_augmented_stora`) never starves a small 5-call cron script.

## Layout

```
adlib_gateway/
  client/adlib.py            unified client library (deploy to repo root)
  gateway/app.py             Flask app (gunicorn entrypoint: app:app)
  gateway/service.py         auth + allowlist + admission + submit/wait + logging
  gateway/workers.py         writer/reader pool, upstream retry, recycle, verify
  gateway/scheduler.py       per-caller round-robin queue
  gateway/config.py          config loader (JSON + env overrides)
  gateway/config.json        default configuration
  gateway/requirements.txt   flask, gunicorn, requests
  systemd/adlib-gateway.service
  systemd/adlib-gateway.env.example
  tests/                     pytest suite
```

## Request envelope (client -> gateway)

`POST /request` with `Authorization: Bearer <token>` and a JSON body:

| op                 | key fields                                                        | returns                          |
|--------------------|-------------------------------------------------------------------|----------------------------------|
| `get`              | `api`, `params`                                                    | `{ok, status_code, text}`        |
| `post`             | `api`, `params`, `payload`                                         | `{ok, status_code, text}`        |
| `lock`             | `api`, `priref`, `database`, `command` (`lockrecord`/`unlockrecord`)| `{ok, locked, status_code, text}`|
| `post_with_verify` | `api`, `payload`, `database`, `method`, `search_value`, `max_retries`, `retry_delay` | `{ok, record}` |

`GET /healthz` returns queue depths (total + per caller), worker counts,
completed count, recycle count and whether dispatch is paused.

Gateway HTTP statuses: `200` executed (inspect body `ok`), `400` bad/disallowed
request, `401` bad token, `503` queue full or wait timeout (`Retry-After`
header set; the client backs off and retries), `502` internal error.

## Configuration reference (`gateway/config.json`)

| key                     | default | meaning                                                        |
|-------------------------|---------|----------------------------------------------------------------|
| `bind_host` / `bind_port`| `0.0.0.0` / `8080` | dev-server bind (gunicorn uses `--bind` instead)     |
| `token_env`             | `ADLIB_GATEWAY_TOKEN` | env var holding the shared secret                    |
| `writer_workers`        | `1`     | POST/verify/lock workers - keep at 1 to serialise writes        |
| `reader_workers`        | `3`     | concurrent GET workers                                          |
| `max_queue_depth`       | `500`   | per-lane cap; beyond this -> 503 + Retry-After                  |
| `max_queue_wait_s`      | `120`   | allowed queue wait before 503                                   |
| `op_budget_s`           | `900`   | allowed execution time for one op (covers post_with_verify)     |
| `api_timeout_s`         | `100`   | upstream Axiell timeout (matches legacy `TIMEOUT`)              |
| `upstream_retries`      | `5`     | upstream attempts for transient failures (backoff + jitter)     |
| `backoff_base_s` / `backoff_max_s` | `0.5` / `10.0` | retry backoff bounds                              |
| `recycle_pause_s`       | `120`   | dispatch pause while the app pool recycles                      |
| `recycle_search`        | `title=recycle...` | query string that triggers the PowerShell recycle     |
| `failure_strings`       | (2 SQL errors) | response substrings that trigger a recycle                |
| `allowed_target_hosts`  | `[]`    | allowlist; EMPTY = any http(s) target (log warning). FILL IN.   |
| `log_file`              | `""`    | request log path; empty = stderr                                |
| `verify.max_retries`    | `3`     | post_with_verify attempts                                       |
| `verify.retry_delay_s`  | `10`    | delay before the verification GET                               |
| `verify.verify_window_minutes` | `1`| creation/modification time window for verification             |

Every key can be overridden by env vars (`ADLIB_GW_*`, see `config.py`), which
is how the systemd env file tunes the running service.

## Security notes

* Set a strong `ADLIB_GATEWAY_TOKEN` (e.g. `openssl rand -hex 32`) identically
  on the gateway and every client. With no token, `/request` is unauthenticated.
* Populate `allowed_target_hosts` with your CID API hostnames so the gateway
  cannot be abused as an open proxy.
* Bind to the internal interface and firewall the port to your server subnet.

## Run locally (development)

```bash
python3 -m venv /tmp/adlibgw && /tmp/adlibgw/bin/pip install -r gateway/requirements.txt
cd gateway
ADLIB_GATEWAY_TOKEN=devtoken /tmp/adlibgw/bin/python app.py   # dev server on :8080
```

Production uses gunicorn with **exactly one worker process** (multiple workers
would each hold separate in-memory queues and break global serialisation):

```bash
gunicorn --workers 1 --threads 64 --timeout 1800 --bind 0.0.0.0:8080 app:app
```

## Tests

```bash
python3 -m venv /tmp/adlibgw && /tmp/adlibgw/bin/pip install -r gateway/requirements.txt
/tmp/adlibgw/bin/pip install pytest pytest-mock xmltodict tenacity
/tmp/adlibgw/bin/python -m pytest adlib_gateway/tests -q     # 54 passed
```

Flask-dependent tests are skipped automatically if Flask is not installed.
