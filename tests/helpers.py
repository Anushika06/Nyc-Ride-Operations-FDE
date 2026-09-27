"""Fixtures + a local HTTP server, so tests need no network and no 500 MB file.

Reference geometry (zone lookup + shapefile) and 311 coordinates are the REAL files from
data/sample/, so geographic mapping is tested against real TLC polygons. Trips and 311
dates are synthetic so the tests can use a small, fully controlled month.
"""
from __future__ import annotations

import io
import json
import threading
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pyarrow as pa
import pyarrow.parquet as pq

SAMPLE = Path(__file__).resolve().parent.parent / "data" / "sample"
ZONES_CSV = (SAMPLE / "taxi_zone_lookup.csv").read_bytes()
ZONES_ZIP = (SAMPLE / "taxi_zones.zip").read_bytes()


def trip(req, on_scene, pickup, dropoff, pu=1, do=2, lic="HV0003", miles=2.0, fare=10.0, trip_time=None):
    if trip_time is None and pickup and dropoff:
        trip_time = int((dropoff - pickup).total_seconds())
    return dict(hvfhs_license_num=lic, request_datetime=req, on_scene_datetime=on_scene, pickup_datetime=pickup,
                dropoff_datetime=dropoff, PULocationID=pu, DOLocationID=do, trip_miles=miles, trip_time=trip_time,
                base_passenger_fare=fare)


SCHEMA = pa.schema([
    ("hvfhs_license_num", pa.string()), ("request_datetime", pa.timestamp("us")),
    ("on_scene_datetime", pa.timestamp("us")), ("pickup_datetime", pa.timestamp("us")),
    ("dropoff_datetime", pa.timestamp("us")), ("PULocationID", pa.int32()), ("DOLocationID", pa.int32()),
    ("trip_miles", pa.float64()), ("trip_time", pa.int64()), ("base_passenger_fare", pa.float64()),
])


def table(rows) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=SCHEMA)


def month_of_trips(year=2026, month=2, per_day=40, zones=(68, 161, 237)) -> pa.Table:
    """A clean month: every day covered, waits 2-9 min, on-scene observed, real Manhattan zones."""
    rows = []
    day = datetime(year, month, 1)
    while day.month == month:
        for i in range(per_day):
            req = day + timedelta(hours=i % 24, minutes=(i * 7) % 60, seconds=17)
            wait = 2 + (i % 8)
            on = req + timedelta(minutes=wait - 1)
            pk = req + timedelta(minutes=wait)
            rows.append(trip(req, on, pk, pk + timedelta(minutes=15), pu=zones[i % len(zones)],
                             do=zones[(i + 1) % len(zones)],
                             lic="HV0003" if i % 4 else "HV0005"))
        day += timedelta(days=1)
    return table(rows)


def weather_payload(year=2026, month=2, drop_hours=0) -> dict:
    start = datetime(year, month, 1) - timedelta(days=1)
    end = datetime(year + (month == 12), month % 12 + 1, 1)
    times, t = [], start
    while t < end:
        times.append(t)
        t += timedelta(hours=1)
    times = times[drop_hours:]
    return {
        "latitude": 40.78, "longitude": -73.97, "timezone": "America/New_York",
        "hourly_units": {"time": "iso8601", "temperature_2m": "C", "precipitation": "mm", "wind_speed_10m": "km/h"},
        "hourly": {
            "time": [x.strftime("%Y-%m-%dT%H:%M") for x in times],
            "temperature_2m": [5.0] * len(times),
            "precipitation": [(3.0 if x.hour == 8 else 0.5 if x.hour == 9 else 0.0) for x in times],
            "wind_speed_10m": [10.0] * len(times),
        },
    }


def complaints_311(year=2026, month=2, n=40) -> list[dict]:
    """Real FHV complaint records (real coordinates, descriptors) re-dated into the test month."""
    real = json.loads((SAMPLE / "nyc311_fhv_2026-07.json").read_text(encoding="utf-8"))
    rows = []
    for i, r in enumerate(real[:n]):
        r = dict(r)
        r["created_date"] = datetime(year, month, 1 + i % 28, 12, i % 60).strftime("%Y-%m-%dT%H:%M:%S.000")
        r.pop("closed_date", None)
        rows.append(r)
    return rows


def parquet_bytes(tbl: pa.Table) -> bytes:
    buf = io.BytesIO()
    pq.write_table(tbl, buf)
    return buf.getvalue()


class FakeSources:
    """Serves all five sources plus scripted errors. /resource/311 emulates Socrata paging."""

    def __init__(self, trips: pa.Table, weather: dict, rows311: list[dict] | None = None, count_override=None):
        self.rows311 = sorted(rows311 if rows311 is not None else complaints_311(), key=lambda r: r["unique_key"])
        self.count_override = count_override
        self.routes = {
            "/trips.parquet": (200, parquet_bytes(trips), "application/octet-stream"),
            "/zones.csv": (200, ZONES_CSV, "text/csv"),
            "/zones.zip": (200, ZONES_ZIP, "application/zip"),
            "/archive": (200, json.dumps(weather).encode(), "application/json"),
            "/always500": (500, b"boom", "text/plain"),
            "/missing": (404, b"nope", "text/plain"),
        }
        self.hits: dict = {}
        me = self

        class Handler(BaseHTTPRequestHandler):
            def _send(self, status, body, ctype, declared=None):
                self.send_response(status)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(declared if declared is not None else len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                url = urlparse(self.path)
                me.hits[url.path] = me.hits.get(url.path, 0) + 1
                if url.path == "/truncated":  # promises 1000 bytes, sends 10, closes
                    self.close_connection = True
                    return self._send(200, b"0123456789", "application/octet-stream", declared=1000)
                if url.path == "/311":
                    q = {k: v[0] for k, v in parse_qs(url.query).items()}
                    if q.get("$select") == "count(*)":
                        n = me.count_override if me.count_override is not None else len(me.rows311)
                        return self._send(200, json.dumps([{"count": str(n)}]).encode(), "application/json")
                    off, lim = int(q.get("$offset", 0)), int(q.get("$limit", 1000))
                    return self._send(200, json.dumps(me.rows311[off:off + lim]).encode(), "application/json")
                status, body, ctype = me.routes.get(url.path, (404, b"", "text/plain"))
                self._send(status, body, ctype)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def env(self) -> dict:
        return {
            "HVFHV_URL_TEMPLATE": self.base + "/trips.parquet?m={month}",
            "ZONE_LOOKUP_URL": self.base + "/zones.csv",
            "ZONE_SHAPES_URL": self.base + "/zones.zip",
            "WEATHER_API_URL": self.base + "/archive",
            "NYC311_API_URL": self.base + "/311",
        }

    def close(self):
        self.server.shutdown()
        self.server.server_close()
