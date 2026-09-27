"""Offline sample mode: a local stand-in for the four real endpoints.

`python run_pipeline.py --sample` starts this server on 127.0.0.1 and points the
normal configuration at it, so the SAME retrieval code runs: streamed file
downloads with Content-Length checks, the Open-Meteo JSON call, and Socrata-style
pagination ($select=count(*), $order, $limit, $offset). Nothing is special-cased
in the pipeline for sample mode except where outputs go and a lower zone threshold.

The files it serves live in data/sample/ and were produced by scripts/make_sample.py
from the real preserved raw inputs (see data/sample/SAMPLE_MANIFEST.json).
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


class SampleServer:
    def __init__(self, sample_dir: Path):
        self.dir = sample_dir
        self.manifest = json.loads((sample_dir / "SAMPLE_MANIFEST.json").read_text(encoding="utf-8"))
        month = self.manifest["month"]
        files = {
            f"/trip-data/fhvhv_tripdata_{month}.parquet": (self.manifest["files"]["hvfhv"], "application/octet-stream"),
            "/misc/taxi_zone_lookup.csv": (self.manifest["files"]["zone_lookup"], "text/csv"),
            "/misc/taxi_zones.zip": (self.manifest["files"]["zone_shapes"], "application/zip"),
            "/v1/archive": (self.manifest["files"]["weather"], "application/json"),
        }
        rows = json.loads((sample_dir / self.manifest["files"]["nyc311"]).read_text(encoding="utf-8"))
        rows.sort(key=lambda r: r["unique_key"])
        base = self.dir

        class Handler(BaseHTTPRequestHandler):
            def _send(self, status, body: bytes, ctype):
                self.send_response(status)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                url = urlparse(self.path)
                if url.path in files:
                    name, ctype = files[url.path]
                    return self._send(200, (base / name).read_bytes(), ctype)
                if url.path == "/resource/erm2-nwe9.json":
                    q = {k: v[0] for k, v in parse_qs(url.query).items()}
                    if q.get("$select") == "count(*)":
                        return self._send(200, json.dumps([{"count": str(len(rows))}]).encode(), "application/json")
                    offset, limit = int(q.get("$offset", 0)), int(q.get("$limit", 1000))
                    return self._send(200, json.dumps(rows[offset:offset + limit]).encode(), "application/json")
                self._send(404, b"not found", "text/plain")

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.server.server_close()

    def env(self) -> dict:
        """Configuration overrides that point the pipeline at this server."""
        return {
            "HVFHV_URL_TEMPLATE": self.base + "/trip-data/fhvhv_tripdata_{month}.parquet",
            "ZONE_LOOKUP_URL": self.base + "/misc/taxi_zone_lookup.csv",
            "ZONE_SHAPES_URL": self.base + "/misc/taxi_zones.zip",
            "WEATHER_API_URL": self.base + "/v1/archive",
            "NYC311_API_URL": self.base + "/resource/erm2-nwe9.json",
        }
