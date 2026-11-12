#!/usr/bin/env python3

"""
Flask HTTP wrapper around the framework-agnostic ``GatewayService``.

Run in production with gunicorn using EXACTLY ONE worker process so the
in-memory queues serialise calls globally (threads are fine, processes are
not)::

    gunicorn --workers 1 --threads 64 --timeout 1800 \
        --bind 0.0.0.0:8080 app:app

The module-level ``app`` is created on import for gunicorn's ``app:app``.

2026
"""

import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, jsonify, request  # noqa: E402

from config import Config, load_config  # noqa: E402
from service import GatewayService  # noqa: E402


def _configure_logging(cfg: Config) -> None:
    log = logging.getLogger("adlib_gateway")
    log.setLevel(logging.INFO)
    if not log.handlers:
        handler = (
            logging.FileHandler(cfg.log_file)
            if cfg.log_file
            else logging.StreamHandler()
        )
        handler.setFormatter(
            logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
        )
        log.addHandler(handler)


def create_app(config: Config = None, session=None) -> Flask:
    cfg = config or load_config()
    _configure_logging(cfg)
    log = logging.getLogger("adlib_gateway")
    if not cfg.allowed_target_hosts:
        log.warning(
            "allowed_target_hosts is EMPTY: the gateway will proxy to any "
            "http(s) target. On a shared network, populate "
            "allowed_target_hosts in config.json to prevent open-proxy abuse."
        )
    if not cfg.token:
        log.warning(
            "No gateway token configured (env %s is empty): /request is "
            "unauthenticated. Set a shared secret on the gateway and all "
            "clients.",
            cfg.token_env,
        )
    app = Flask(__name__)
    service = GatewayService(cfg, session=session)
    app.config["SERVICE"] = service

    @app.route("/healthz", methods=["GET"])
    def healthz():
        return jsonify(service.health()), 200

    @app.route("/request", methods=["POST"])
    def gateway_request():
        envelope = request.get_json(silent=True)
        if not isinstance(envelope, dict):
            return jsonify({"ok": False, "error": "invalid JSON body"}), 400
        auth = request.headers.get("Authorization", "")
        status, body, headers = service.handle(envelope, auth)
        resp = jsonify(body)
        resp.status_code = status
        for key, value in headers.items():
            resp.headers[key] = value
        return resp

    return app


app = create_app()


if __name__ == "__main__":
    cfg = load_config()
    app.run(host=cfg.bind_host, port=cfg.bind_port, threaded=True)
