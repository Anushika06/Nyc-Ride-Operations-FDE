"""EXTRACT + PRESERVE RAW + RETRIEVAL COMPLETENESS for the three sources.

Principle: a successful download is not proof of complete retrieval.
Every source is written byte-for-byte to data/raw (or data/reference) with a
retrieval manifest (url, bytes, sha256, time), and then proven complete with
checks appropriate to that source. Nothing in this module transforms data.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import math
from datetime import datetime, timezone, date, timedelta
from pathlib import Path
from urllib.parse import urlencode, urlsplit

import pyarrow.parquet as pq

from .checks import CheckResult, PASS, WARN, FAIL, CRITICAL, WEATHER
from .http_client import request_with_retries

HVFHV_REQUIRED_COLUMNS = [
    "hvfhs_license_num", "request_datetime", "on_scene_datetime", "pickup_datetime",
    "dropoff_datetime", "PULocationID", "DOLocationID", "trip_miles", "trip_time",
    "base_passenger_fare",
]
ZONE_REQUIRED_COLUMNS = ["LocationID", "Borough", "Zone", "service_zone"]


class RetrievalError(RuntimeError):
    """A source could not be retrieved or proven complete. Stops the run."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _request_key(url: str) -> str:
    """The part of a URL that identifies *what* was requested, ignoring host:port.

    Sample mode serves the same endpoints from a local server on an ephemeral port, so
    the host changes between runs while the request does not.
    """
    s = urlsplit(url)
    return f"{s.path}?{s.query}"


def _read_manifest(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _write_manifest(path: Path, manifest: dict) -> None:
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def _http_kwargs(cfg, logger, stats, fault=None):
    return dict(
        logger=logger, timeout=cfg.http_timeout_seconds, max_retries=cfg.max_retries,
        base_seconds=cfg.retry_base_seconds, max_wait_seconds=cfg.retry_max_wait_seconds,
        fault=fault, stats=stats,
    )


def _download_file(url: str, dest: Path, *, source: str, cfg, logger, stats) -> dict:
    """Stream `url` to `dest` via a .part file; prove byte count == Content-Length."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_suffix(dest.suffix + ".part")

    def handle(resp):
        expected = resp.headers.get("Content-Length")
        received = 0
        with open(part, "wb") as f:
            while True:
                chunk = resp.read(4 * 1024 * 1024)
                if not chunk:
                    break
                f.write(chunk)
                received += len(chunk)
        expected = int(expected) if expected is not None else None
        if expected is not None and received != expected:
            # Treated as a network failure so the bounded retry loop re-downloads.
            raise ConnectionError(f"truncated download: received={received} expected={expected}")
        return expected, received

    logger.info("Downloading | source=%s url=%s", source, url)
    expected, received = request_with_retries(url, handle, source=source, **_http_kwargs(cfg, logger, stats))
    part.replace(dest)  # only a complete file ever gets the final name
    manifest = {
        "source": source, "url": url, "retrieved_at_utc": _now(),
        "content_length_header": expected, "bytes_received": received,
        "sha256": sha256_file(dest), "file": dest.name,
    }
    logger.info("Downloaded | source=%s bytes=%s expected=%s", source, received, expected)
    return manifest


def _ensure_preserved(url: str, dest: Path, manifest_path: Path, *, source: str, cfg, logger, stats) -> tuple[dict, bool]:
    """Reuse a preserved raw file if its checksum still matches its manifest.

    Returns (manifest, reused). A raw file whose checksum no longer matches is
    NOT silently re-used: it is re-downloaded and the mismatch is logged.
    """
    manifest = _read_manifest(manifest_path)
    if dest.exists() and manifest:
        actual = sha256_file(dest)
        if actual == manifest.get("sha256"):
            logger.info("Raw input already preserved; checksum verified | source=%s file=%s", source, dest)
            return manifest, True
        logger.warning("Raw file checksum mismatch; re-downloading | source=%s expected=%s actual=%s",
                       source, manifest.get("sha256"), actual)
    elif dest.exists() and not manifest:
        logger.warning("Raw file has no retrieval manifest; re-downloading to establish provenance | source=%s", source)
    manifest = _download_file(url, dest, source=source, cfg=cfg, logger=logger, stats=stats)
    _write_manifest(manifest_path, manifest)
    return manifest, False


# --------------------------------------------------------------------------
# Source 1: HVFHV monthly Parquet (file retrieval)
# --------------------------------------------------------------------------
def retrieve_hvfhv(cfg, logger, stats) -> tuple[Path, dict, list[CheckResult]]:
    dest = cfg.raw_hvfhv_dir / f"fhvhv_tripdata_{cfg.month}.parquet"
    manifest, reused = _ensure_preserved(
        cfg.hvfhv_url, dest, cfg.raw_hvfhv_dir / "_retrieval_manifest.json",
        source="hvfhv", cfg=cfg, logger=logger, stats=stats,
    )
    checks = []
    exp, got = manifest.get("content_length_header"), manifest.get("bytes_received")
    checks.append(CheckResult(
        "hvfhv.download_complete",
        PASS if exp is None or exp == got else FAIL,
        f"bytes_received={got} content_length={exp} sha256={manifest['sha256'][:16]}... reused_preserved_copy={reused}",
        category="completeness", evidence={"manifest": manifest},
    ))
    try:
        pf = pq.ParquetFile(dest)
    except Exception as exc:  # corrupt/partial file
        raise RetrievalError(f"hvfhv: preserved file is not readable Parquet: {exc}") from exc
    md = pf.metadata
    checks.append(CheckResult(
        "hvfhv.file_readable", PASS if md.num_rows > 0 else FAIL,
        f"parquet footer readable; rows={md.num_rows:,} row_groups={md.num_row_groups} columns={md.num_columns}",
        category="completeness", evidence={"rows": md.num_rows, "row_groups": md.num_row_groups},
    ))
    manifest["parquet_rows"] = md.num_rows
    manifest["parquet_columns"] = pf.schema_arrow.names
    return dest, manifest, checks


def check_required_columns(columns: list[str], required: list[str], name: str, scope: str = CRITICAL) -> CheckResult:
    missing = [c for c in required if c not in columns]
    return CheckResult(
        f"{name}.required_columns", FAIL if missing else PASS,
        f"missing required columns: {missing}" if missing else f"{len(required)} required columns present",
        scope=scope, category="technical", evidence={"missing": missing},
    )


# --------------------------------------------------------------------------
# Source 2: Taxi Zone Lookup CSV (file retrieval)
# --------------------------------------------------------------------------
def retrieve_zones(cfg, logger, stats) -> tuple[dict, dict, list[CheckResult]]:
    dest = cfg.reference_dir / "taxi_zone_lookup.csv"
    manifest, reused = _ensure_preserved(
        cfg.zone_lookup_url, dest, cfg.reference_dir / "_retrieval_manifest.json",
        source="zone_lookup", cfg=cfg, logger=logger, stats=stats,
    )
    text = dest.read_text(encoding="utf-8-sig")
    rows = list(csv.DictReader(io.StringIO(text)))
    header = list(rows[0].keys()) if rows else []
    checks = [check_required_columns(header, ZONE_REQUIRED_COLUMNS, "zones")]
    if checks[0].status == FAIL:
        return {}, manifest, checks

    ids = [int(r["LocationID"]) for r in rows]
    dupes = len(ids) - len(set(ids))
    checks.append(CheckResult(
        "zones.location_id_unique", PASS if dupes == 0 else FAIL,
        f"rows={len(rows)} duplicate_location_ids={dupes}", category="uniqueness",
    ))
    zones = {int(r["LocationID"]): {"zone": r["Zone"], "borough": r["Borough"], "service_zone": r["service_zone"]} for r in rows}
    # 264/265 are placeholder codes in the TLC lookup, not geographic zones.
    placeholders = sorted(i for i, z in zones.items() if z["borough"] in ("Unknown", "N/A") or z["zone"] in ("N/A", "Outside of NYC"))
    checks.append(CheckResult(
        "zones.placeholder_codes", WARN if placeholders else PASS,
        f"lookup contains non-geographic placeholder IDs {placeholders} (Unknown / Outside of NYC); "
        "rides with these IDs cannot be attributed to an NYC zone",
        scope="info", category="semantic", evidence={"placeholder_ids": placeholders},
    ))
    manifest["rows"] = len(rows)
    return {"zones": zones, "placeholder_ids": placeholders}, manifest, checks


# --------------------------------------------------------------------------
# Source 3: Open-Meteo historical hourly weather (REST API / JSON)
# --------------------------------------------------------------------------
def month_bounds(month: str) -> tuple[date, date]:
    first = date.fromisoformat(month + "-01")
    nxt = date(first.year + (first.month == 12), first.month % 12 + 1, 1)
    return first, nxt - timedelta(days=1)


def weather_request_url(cfg) -> tuple[str, date, date]:
    first, last = month_bounds(cfg.month)
    # One day of lead-in: a ride picked up just after midnight on day 1 was
    # usually requested on the last day of the previous month.
    start = first - timedelta(days=1)
    params = {
        "latitude": cfg.weather_latitude, "longitude": cfg.weather_longitude,
        "start_date": start.isoformat(), "end_date": last.isoformat(),
        "hourly": ",".join(cfg.weather_variables), "timezone": cfg.weather_timezone,
    }
    return f"{cfg.weather_api_url}?{urlencode(params)}", start, last


def retrieve_weather(cfg, logger, stats, *, refresh: bool = False, fault=None) -> tuple[dict | None, dict, list[CheckResult]]:
    """Fetch (or reuse) the raw weather JSON. Failures here are scoped to the
    weather KPI: the trip KPIs remain valid without weather context."""
    url, start, end = weather_request_url(cfg)
    manifest_path = cfg.raw_weather_dir / "_retrieval_manifest.json"
    manifest = _read_manifest(manifest_path)
    raw_path = cfg.raw_weather_dir / manifest["file"] if manifest and manifest.get("file") else None

    reuse = (not refresh and raw_path is not None and raw_path.exists()
             and _request_key(manifest.get("url", "")) == _request_key(url)
             and sha256_file(raw_path) == manifest.get("sha256"))
    if reuse:
        logger.info("Raw weather response already preserved; checksum verified | file=%s", raw_path)
        body = raw_path.read_bytes()
    else:
        cfg.raw_weather_dir.mkdir(parents=True, exist_ok=True)
        try:
            body = request_with_retries(url, lambda r: r.read(), source="open_meteo",
                                        **_http_kwargs(cfg, logger, stats, fault))
        except Exception as exc:
            logger.error("Weather retrieval failed | error=%s", exc)
            return None, {"url": url, "error": str(exc)}, [CheckResult(
                "weather.retrieval", FAIL, f"weather API retrieval failed: {exc}",
                scope=WEATHER, category="completeness")]
        # Preserve the response exactly as received, before parsing it. Raw is
        # append-only: each fetch gets its own file; earlier responses are never overwritten.
        raw_path = cfg.raw_weather_dir / f"open_meteo_hourly_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
        raw_path.write_bytes(body)
        manifest = {"source": "open_meteo", "url": url, "retrieved_at_utc": _now(),
                    "bytes_received": len(body), "sha256": hashlib.sha256(body).hexdigest(),
                    "file": raw_path.name}
        _write_manifest(manifest_path, manifest)
        logger.info("Weather API response preserved | bytes=%s file=%s", len(body), raw_path)

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        return None, manifest, [CheckResult("weather.structure", FAIL, f"response is not JSON: {exc}",
                                            scope=WEATHER, category="technical")]
    return payload, manifest, [CheckResult("weather.retrieval", PASS,
                                           f"response preserved; bytes={manifest['bytes_received']} "
                                           f"requested {start}..{end} (1 lead-in day)",
                                           scope=WEATHER, category="completeness")]


def validate_weather_payload(payload: dict, cfg) -> tuple[dict | None, list[CheckResult]]:
    """Prove the weather response covers every requested hour, once."""
    checks = []
    hourly = payload.get("hourly") if isinstance(payload, dict) else None
    missing_keys = [] if hourly is None else [v for v in ("time", *cfg.weather_variables) if v not in hourly]
    if hourly is None or missing_keys:
        checks.append(CheckResult("weather.structure", FAIL,
                                  f"hourly block missing or lacks variables {missing_keys}; api error={payload.get('reason') if isinstance(payload, dict) else None}",
                                  scope=WEATHER, category="technical"))
        return None, checks
    checks.append(CheckResult("weather.structure", PASS,
                              f"hourly variables present: {list(cfg.weather_variables)}; units={payload.get('hourly_units')}",
                              scope=WEATHER, category="technical"))

    _, start, end = weather_request_url(cfg)
    expected = [datetime(start.year, start.month, start.day) + timedelta(hours=h)
                for h in range(((end - start).days + 1) * 24)]
    times = [datetime.fromisoformat(t) for t in hourly["time"]]
    seen, dupes = set(), 0
    for t in times:
        dupes += t in seen
        seen.add(t)
    missing = [t for t in expected if t not in seen]
    coverage = 1 - len(missing) / len(expected)
    checks.append(CheckResult(
        "weather.hour_coverage",
        PASS if not missing and not dupes else (FAIL if coverage < cfg.min_weather_hour_coverage else WARN),
        f"expected_hours={len(expected)} received={len(times)} missing={len(missing)} duplicate={dupes} "
        f"coverage={coverage:.2%} (threshold {cfg.min_weather_hour_coverage:.0%})",
        scope=WEATHER, category="completeness",
        evidence={"missing_hours_sample": [m.isoformat() for m in missing[:10]]},
    ))
    nulls = {v: sum(x is None for x in hourly[v]) for v in cfg.weather_variables}
    checks.append(CheckResult(
        "weather.variable_nulls", PASS if not any(nulls.values()) else WARN,
        f"null hourly values per variable: {nulls}", scope=WEATHER, category="completeness"))

    # Geographic representation: Open-Meteo snaps to a model grid cell.
    lat, lon = payload.get("latitude"), payload.get("longitude")
    km = _haversine_km(cfg.weather_latitude, cfg.weather_longitude, lat, lon) if lat is not None else None
    checks.append(CheckResult(
        "weather.grid_point_offset", WARN,
        f"requested ({cfg.weather_latitude}, {cfg.weather_longitude}) Central Park; API grid cell ({lat}, {lon}) "
        f"is {km:.1f} km away. One point stands in for all of NYC - an approximation, not per-ride weather.",
        scope="info", category="semantic", evidence={"grid_lat": lat, "grid_lon": lon, "offset_km": km},
    ))
    if payload.get("timezone") != cfg.weather_timezone:
        checks.append(CheckResult("weather.timezone", FAIL,
                                  f"expected timezone {cfg.weather_timezone}, got {payload.get('timezone')}",
                                  scope=WEATHER, category="semantic"))
    series = {"time": times, **{v: hourly[v] for v in cfg.weather_variables},
              "grid_lat": lat, "grid_lon": lon}
    return series, checks


def _haversine_km(lat1, lon1, lat2, lon2) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


# --------------------------------------------------------------------------
# Source 2b: Taxi Zone shapefile (file retrieval) - geometry for 311 mapping
# --------------------------------------------------------------------------
def retrieve_zone_shapes(cfg, logger, stats, zones: dict) -> tuple[list | None, dict, list[CheckResult]]:
    """Same owner and IDs as the lookup CSV; needed only to place 311 complaints in zones.
    Failures are scoped to the 311 friction KPI."""
    from .geo import load_zone_shapes
    dest = cfg.reference_dir / "taxi_zones.zip"
    try:
        manifest, _ = _ensure_preserved(cfg.zone_shapes_url, dest, cfg.reference_dir / "_retrieval_manifest_shapes.json",
                                        source="zone_shapes", cfg=cfg, logger=logger, stats=stats)
        shapes = load_zone_shapes(dest)
    except Exception as exc:
        logger.error("Zone shapefile unavailable | %s", exc)
        return None, {"error": str(exc)}, [CheckResult("zone_shapes.retrieval", FAIL, f"zone shapefile unavailable: {exc}",
                                                        scope="friction", category="completeness")]
    ids = {s.location_id for s in shapes}
    lookup_geo = {i for i in zones["zones"] if i not in zones["placeholder_ids"]}
    missing = sorted(lookup_geo - ids)
    extra = sorted(ids - set(zones["zones"]))
    status = PASS if not missing and not extra else WARN
    return shapes, manifest, [CheckResult(
        "zone_shapes.id_consistency", status,
        f"{len(shapes)} polygons; lookup zones without geometry: {missing or 'none'}; geometry IDs not in lookup: {extra or 'none'}; "
        f"bytes={manifest.get('bytes_received')} sha256={manifest['sha256'][:16]}...",
        scope="friction", category="completeness")]
