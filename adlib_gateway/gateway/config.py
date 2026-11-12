#!/usr/bin/env python3

"""
Gateway configuration.

Loads ``config.json`` (next to this file by default) into a ``Config``
dataclass and applies environment overrides. No framework dependency so the
whole gateway core can be unit tested without Flask installed.

2026
"""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

DEFAULT_CONFIG_PATH = Path(__file__).parent / "config.json"


@dataclass
class VerifyConfig:
    max_retries: int = 3
    retry_delay_s: int = 10
    verify_window_minutes: int = 1


@dataclass
class Config:
    bind_host: str = "0.0.0.0"
    bind_port: int = 8080
    token_env: str = "ADLIB_GATEWAY_TOKEN"
    writer_workers: int = 1
    reader_workers: int = 3
    max_queue_depth: int = 500
    max_queue_wait_s: int = 120
    op_budget_s: int = 900
    api_timeout_s: int = 100
    upstream_retries: int = 5
    backoff_base_s: float = 0.5
    backoff_max_s: float = 10.0
    recycle_pause_s: int = 120
    recycle_search: str = "title=recycle.application.pool.data.test"
    failure_strings: List[str] = field(
        default_factory=lambda: [
            "A severe error occurred on the current command.",
            "Execution Timeout Expired. The timeout period elapsed",
        ]
    )
    allowed_target_hosts: List[str] = field(default_factory=list)
    log_file: str = ""
    verify: VerifyConfig = field(default_factory=VerifyConfig)

    @property
    def token(self) -> str:
        return os.environ.get(self.token_env, "")


def _coerce(default: Any, value: Any) -> Any:
    if isinstance(default, bool):
        return bool(value)
    if isinstance(default, int) and not isinstance(default, bool):
        return int(value)
    if isinstance(default, float):
        return float(value)
    return value


def load_config(path: Any = None) -> Config:
    cfg = Config()
    cfg_path = Path(path or os.environ.get("ADLIB_GW_CONFIG") or DEFAULT_CONFIG_PATH)
    data: Dict[str, Any] = {}
    if cfg_path.exists():
        data = json.loads(cfg_path.read_text())

    verify_data = data.pop("verify", {})
    for key, value in data.items():
        if hasattr(cfg, key):
            setattr(cfg, key, _coerce(getattr(cfg, key), value))
    for key, value in verify_data.items():
        if hasattr(cfg.verify, key):
            setattr(cfg.verify, key, _coerce(getattr(cfg.verify, key), value))

    _apply_env_overrides(cfg)
    return cfg


def _apply_env_overrides(cfg: Config) -> None:
    env_map = {
        "ADLIB_GW_BIND_HOST": "bind_host",
        "ADLIB_GW_BIND_PORT": "bind_port",
        "ADLIB_GW_WRITER_WORKERS": "writer_workers",
        "ADLIB_GW_READER_WORKERS": "reader_workers",
        "ADLIB_GW_MAX_QUEUE_DEPTH": "max_queue_depth",
        "ADLIB_GW_MAX_QUEUE_WAIT_S": "max_queue_wait_s",
        "ADLIB_GW_OP_BUDGET_S": "op_budget_s",
        "ADLIB_GW_API_TIMEOUT_S": "api_timeout_s",
        "ADLIB_GW_RECYCLE_PAUSE_S": "recycle_pause_s",
        "ADLIB_GW_LOG_FILE": "log_file",
    }
    for env_key, attr in env_map.items():
        raw = os.environ.get(env_key)
        if raw is None or raw == "":
            continue
        setattr(cfg, attr, _coerce(getattr(cfg, attr), raw))

    hosts = os.environ.get("ADLIB_GW_ALLOWED_HOSTS")
    if hosts:
        cfg.allowed_target_hosts = [h.strip() for h in hosts.split(",") if h.strip()]
