"""NYC 311 'For Hire Vehicle Complaint' records: customer-friction CONTEXT.

311 complaints carry no ride ID and cannot be linked to individual rides. They are
used only at zone-month / borough-month grain:

    311 complaint --(state-plane point-in-polygon)--> taxi zone --> borough
    complaints per zone/borough  /  HVFHV pickups in that zone/borough

Retrieval (Socrata SODA API, paginated):
  1. ask the server how many records match   ($select=count(*))
  2. page through them in a stable order      ($order=unique_key, $limit, $offset)
  3. preserve every page byte-for-byte before parsing
  4. prove received == expected and unique_key is unique
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlencode

import numpy as np

from .checks import CheckResult, PASS, WARN, FAIL, UNKNOWN, FRICTION, INFO
from .http_client import request_with_retries
from .ingest import month_bounds, _read_manifest, _write_manifest, sha256_file, _http_kwargs

COMPLAINT_TYPE = "For Hire Vehicle Complaint"
REQUIRED_FIELDS = ["unique_key", "created_date", "complaint_type", "descriptor", "borough"]

# Descriptor classification. Decided from the observed July 2026 values, not assumed:
# "Non Passenger" complaints come from pedestrians / other road users (mostly unsafe
# driving) - street safety, not rider experience. Only passenger and company
# complaints are counted as customer friction.
PASSENGER_DESCRIPTORS = {"Driver Complaint - Passenger", "Car Service Company Complaint"}
NON_PASSENGER_DESCRIPTORS = {"Driver Complaint - Non Passenger", "Driver Report - Non Passenger"}
# Sub-reasons that are about getting (or not getting) a pickup at all.
PICKUP_RELIABILITY_REASONS = {"Refused Pick-Up", "Denied Request", "Service Delay/No Show"}


def _where(month: str) -> str:
    first, last = month_bounds(month)
    return (f"complaint_type='{COMPLAINT_TYPE}' AND created_date between "
            f"'{first.isoformat()}T00:00:00' and '{last.isoformat()}T23:59:59'")


def retrieve_311(cfg, logger, stats, *, refresh: bool = False, fault=None) -> tuple[list | None, dict, list[CheckResult]]:
    """Fetch (or reuse a preserved snapshot of) one month of FHV complaints.

    311 is a live dataset: statuses change and late records can appear. A run therefore
    works from one preserved snapshot; `--refresh-311` takes a new snapshot (append-only).
    """
    base = cfg.nyc311_dir
    manifest_path = base / "_retrieval_manifest.json"
    manifest = _read_manifest(manifest_path)
    where = _where(cfg.month)

    if not refresh and manifest and manifest.get("where") == where and _snapshot_intact(base, manifest):
        logger.info("Raw 311 snapshot already preserved; checksums verified | %s", manifest["snapshot_dir"])
        pages = [json.loads((base / manifest["snapshot_dir"] / p["file"]).read_bytes()) for p in manifest["pages"]]
        rows = [r for page in pages for r in page]
        return rows, manifest, _completeness_checks(rows, manifest)

    snap = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snap_dir = base / snap
    snap_dir.mkdir(parents=True, exist_ok=True)
    kw = _http_kwargs(cfg, logger, stats, fault)
    try:
        count_url = f"{cfg.nyc311_api_url}?{urlencode({'$select': 'count(*)', '$where': where})}"
        count_body = request_with_retries(count_url, lambda r: r.read(), source="nyc311", **kw)
        (snap_dir / "count.json").write_bytes(count_body)
        expected = int(json.loads(count_body)[0]["count"])
        logger.info("311 server-side count | expected=%s", expected)

        rows, pages, offset = [], [], 0
        while True:
            params = {"$where": where, "$order": "unique_key", "$limit": cfg.nyc311_page_size, "$offset": offset}
            url = f"{cfg.nyc311_api_url}?{urlencode(params)}"
            body = request_with_retries(url, lambda r: r.read(), source="nyc311", **kw)
            name = f"page_{len(pages):03d}.json"
            (snap_dir / name).write_bytes(body)  # preserve before parsing
            page = json.loads(body)
            pages.append({"file": name, "offset": offset, "rows": len(page),
                          "sha256": hashlib.sha256(body).hexdigest()})
            rows.extend(page)
            logger.info("Fetched 311 page | page=%s offset=%s rows=%s cumulative=%s", len(pages) - 1, offset, len(page), len(rows))
            if len(page) < cfg.nyc311_page_size:
                break
            offset += cfg.nyc311_page_size
    except Exception as exc:
        logger.error("311 retrieval failed | error=%s", exc)
        return None, {"error": str(exc)}, [CheckResult("nyc311.retrieval", FAIL, f"311 API retrieval failed: {exc}",
                                                        scope=FRICTION, category="completeness")]

    manifest = {"source": "nyc311", "endpoint": cfg.nyc311_api_url, "where": where, "order": "unique_key",
                "page_size": cfg.nyc311_page_size, "retrieved_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "snapshot_dir": snap, "expected_count": expected, "received": len(rows), "pages": pages,
                "count_sha256": sha256_file(snap_dir / "count.json")}
    _write_manifest(manifest_path, manifest)
    return rows, manifest, _completeness_checks(rows, manifest)


def _snapshot_intact(base: Path, manifest: dict) -> bool:
    d = base / manifest.get("snapshot_dir", "")
    try:
        return all(sha256_file(d / p["file"]) == p["sha256"] for p in manifest["pages"])
    except (FileNotFoundError, KeyError):
        return False


def _completeness_checks(rows: list, manifest: dict) -> list[CheckResult]:
    exp, got = manifest["expected_count"], len(rows)
    keys = [r.get("unique_key") for r in rows]
    dupes = len(keys) - len(set(keys))
    return [
        CheckResult("nyc311.retrieval_complete", PASS if exp == got else FAIL,
                    f"server count={exp} received={got} across {len(manifest['pages'])} pages "
                    f"(page_size={manifest['page_size']}, stable order by unique_key); snapshot {manifest['snapshot_dir']}",
                    scope=FRICTION, category="completeness"),
        CheckResult("nyc311.unique_key", PASS if dupes == 0 else FAIL,
                    f"duplicate unique_key across pages: {dupes} (duplicates would mean pagination overlap)",
                    scope=FRICTION, category="uniqueness"),
    ]


def validate_311(rows: list, cfg) -> list[CheckResult]:
    n = len(rows)
    out = []
    missing = {f: sum(1 for r in rows if not r.get(f)) for f in REQUIRED_FIELDS}
    out.append(CheckResult("nyc311.required_fields", FAIL if n and any(v == n for v in missing.values()) else
                           (WARN if any(missing.values()) else PASS),
                           f"rows={n}; missing values per required field: {missing}", scope=FRICTION, category="technical"))
    wrong_type = sum(1 for r in rows if r.get("complaint_type") != COMPLAINT_TYPE)
    agencies = sorted({r.get("agency") for r in rows})
    out.append(CheckResult("nyc311.filter_semantics", FAIL if wrong_type else PASS,
                           f"rows outside complaint_type filter: {wrong_type}; agencies: {agencies}",
                           scope=FRICTION, category="semantic"))

    first, last = month_bounds(cfg.month)
    created = [_ts(r.get("created_date")) for r in rows]
    outside = sum(1 for c in created if c is None or not (first <= c.date() <= last))
    days = {c.date() for c in created if c}
    n_days = (last - first).days + 1
    out.append(CheckResult("nyc311.date_coverage", FAIL if outside else (WARN if len(days) < n_days else PASS),
                           f"created_date outside {cfg.month} or unparseable: {outside}; days with >=1 complaint: {len(days)}/{n_days}",
                           scope=FRICTION, category="completeness"))
    closed_before = sum(1 for r, c in zip(rows, created)
                        if r.get("closed_date") and c and _ts(r["closed_date"]) and _ts(r["closed_date"]) < c)
    open_share = sum(1 for r in rows if r.get("status") != "Closed") / n if n else 0
    out.append(CheckResult("nyc311.chronology_and_status", WARN if closed_before else PASS,
                           f"closed_date before created_date: {closed_before}; not yet closed: {open_share:.0%}. "
                           "Complaint outcomes are not used (most are still in progress when the month is analysed).",
                           scope=INFO, category="chronology"))
    desc = {}
    for r in rows:
        desc[r.get("descriptor")] = desc.get(r.get("descriptor"), 0) + 1
    unknown = {d: c for d, c in desc.items() if d not in PASSENGER_DESCRIPTORS | NON_PASSENGER_DESCRIPTORS}
    out.append(CheckResult("nyc311.descriptor_classification", WARN if unknown else PASS,
                           f"descriptor counts: {desc}. Passenger/company complaints = customer friction; "
                           f"non-passenger = street safety (excluded from KPI 5). Unclassified: {unknown or 'none'}",
                           scope=INFO, category="semantic"))
    return out


def _ts(v):
    try:
        return datetime.fromisoformat(v) if v else None
    except ValueError:
        return None


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return np.nan


def complaints_table(rows: list, shapes, zones: dict, min_coverage: float) -> tuple[list[dict], dict, list[CheckResult]]:
    """One row per complaint with its mapped zone. No ride linkage is made."""
    from .geo import map_points
    x = np.array([_num(r.get("x_coordinate_state_plane")) for r in rows])
    y = np.array([_num(r.get("y_coordinate_state_plane")) for r in rows])
    zone_id, ambiguous = map_points(x, y, shapes) if len(rows) else (np.array([], int), np.array([], bool))
    lookup = zones["zones"]
    table = []
    for i, (r, zid, amb) in enumerate(zip(rows, zone_id, ambiguous)):
        d = r.get("descriptor")
        table.append({
            "unique_key": r.get("unique_key"), "created_date": r.get("created_date"),
            "descriptor": d, "descriptor_2": r.get("descriptor_2"),
            "category": "passenger" if d in PASSENGER_DESCRIPTORS else "non_passenger" if d in NON_PASSENGER_DESCRIPTORS else "other",
            "pickup_reliability_reason": r.get("descriptor_2") in PICKUP_RELIABILITY_REASONS,
            "reported_borough": r.get("borough"),
            "zone_id": int(zid) if zid > 0 else None,
            "zone": lookup.get(int(zid), {}).get("zone") if zid > 0 else None,
            "zone_borough": lookup.get(int(zid), {}).get("borough") if zid > 0 else None,
            "mapping": "no_coordinates" if np.isnan(x[i]) or np.isnan(y[i]) else ("ambiguous_lowest_id" if amb else
                                                                          "mapped" if zid > 0 else "outside_all_zones"),
        })
    n = len(table)
    no_xy = sum(t["mapping"] == "no_coordinates" for t in table)
    outside = sum(t["mapping"] == "outside_all_zones" for t in table)
    amb = sum(t["mapping"] == "ambiguous_lowest_id" for t in table)
    mapped = n - no_xy - outside
    # KPI 5's numerator is the mapped PASSENGER complaints only, so the share of
    # passenger complaints that failed to map is the KPI's understatement, not the
    # overall mapping rate.
    passenger_total = sum(t["category"] == "passenger" for t in table)
    passenger_unmapped = sum(t["category"] == "passenger" and t["zone_id"] is None for t in table)
    passenger_mapped = passenger_total - passenger_unmapped
    comparable = [t for t in table if t["zone_borough"] and t["reported_borough"] not in (None, "Unspecified")]
    agree = sum(t["zone_borough"].upper() == t["reported_borough"].upper() for t in comparable)
    coverage = mapped / n if n else 0
    checks = [
        CheckResult("nyc311.zone_mapping_coverage",
                    PASS if coverage >= 0.95 else (WARN if coverage >= min_coverage else FAIL),
                    f"mapped to a taxi zone: {mapped}/{n} ({coverage:.1%}; FAIL below {min_coverage:.0%}); no coordinates: {no_xy}; "
                    f"outside all zones (water/bridges/outside NYC): {outside}; ambiguous (lowest ID used): {amb}. "
                    f"Passenger/company complaints (the KPI 5 numerator): {passenger_mapped}/{passenger_total} mapped, "
                    f"{passenger_unmapped} unmapped, so KPI 5 understates the true rate by "
                    f"{passenger_unmapped / max(passenger_total, 1):.1%} if unmapped complaints resemble mapped ones. "
                    "Unmapped complaints are kept and counted, never assigned by guess.",
                    scope=FRICTION, category="location"),
        CheckResult("nyc311.zone_mapping_borough_agreement",
                    PASS if comparable and agree / len(comparable) >= 0.98 else WARN,
                    f"311-reported borough equals mapped zone's borough for {agree}/{len(comparable)} "
                    f"({agree / max(len(comparable), 1):.2%}) - independent check that 311 state-plane "
                    "coordinates and the TLC shapefile share a coordinate system (EPSG:2263)",
                    scope=FRICTION, category="location"),
    ]
    return table, {"mapped": mapped, "no_coordinates": no_xy, "outside_all_zones": outside, "ambiguous": amb,
                   "passenger_total": passenger_total, "passenger_mapped": passenger_mapped,
                   "passenger_unmapped": passenger_unmapped}, checks


def aggregate_friction(table: list[dict], pickups_by_zone: dict, zones: dict) -> tuple[dict, dict]:
    """One-to-many -> zone grain and borough grain. Every zone with pickups appears,
    including zones with zero complaints (zero is a value, not a missing row)."""
    per_zone = {zid: {"complaints_all": 0, "complaints_passenger": 0, "complaints_non_passenger": 0,
                      "complaints_pickup_reliability": 0} for zid in pickups_by_zone}
    for t in table:
        zid = t["zone_id"]
        if zid is None:
            continue
        z = per_zone.setdefault(zid, {"complaints_all": 0, "complaints_passenger": 0,
                                      "complaints_non_passenger": 0, "complaints_pickup_reliability": 0})
        z["complaints_all"] += 1
        z["complaints_passenger"] += t["category"] == "passenger"
        z["complaints_non_passenger"] += t["category"] == "non_passenger"
        z["complaints_pickup_reliability"] += bool(t["pickup_reliability_reason"])
    per_borough: dict = {}
    for zid, c in per_zone.items():
        b = zones["zones"].get(zid, {}).get("borough")
        agg = per_borough.setdefault(b, {"hvfhv_pickups": 0, "complaints_all": 0, "complaints_passenger": 0,
                                         "complaints_non_passenger": 0, "complaints_pickup_reliability": 0})
        agg["hvfhv_pickups"] += pickups_by_zone.get(zid, 0)
        for k in ("complaints_all", "complaints_passenger", "complaints_non_passenger", "complaints_pickup_reliability"):
            agg[k] += c[k]
    return per_zone, per_borough


def rate_per_100k(count: int, rides: int):
    return round(count / rides * 100_000, 2) if rides else None
