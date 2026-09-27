"""PROFILE + VALIDATE the trip data against the claims the KPIs will make.

The question is not "are the dtypes right?" but "can we defend the metric?".
Each check detects, quantifies, classifies and decides; none of them edits data.
"""
from __future__ import annotations

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from .checks import CheckResult, PASS, WARN, FAIL, UNKNOWN, CRITICAL, INFO
from .ingest import month_bounds
from .lifecycle import (Lifecycle, ON_SCENE_CODES, VALIDATION_CODES, OS_OBSERVED, OS_SAME_AS_PICKUP,
                        OS_BEFORE_REQUEST, OS_AFTER_PICKUP, OS_MISSING, VS_PICKUP_BEFORE_REQUEST,
                        VS_DROPOFF_NOT_AFTER_PICKUP, VS_MISSING_TS)

NATURAL_KEY = ["hvfhs_license_num", "request_datetime", "on_scene_datetime", "pickup_datetime",
               "dropoff_datetime", "PULocationID", "DOLocationID", "trip_miles", "base_passenger_fare"]


def _as_uint64(col) -> np.ndarray:
    if pa.types.is_string(col.type) or pa.types.is_large_string(col.type):
        return col.combine_chunks().dictionary_encode().indices.to_numpy(zero_copy_only=False).astype(np.uint64)
    arr = col.to_numpy()
    if arr.dtype.kind == "M":
        arr = arr.astype("datetime64[us]").astype(np.int64)
    return np.ascontiguousarray(arr.astype(np.float64) if arr.dtype.kind == "f" else arr.astype(np.int64)).view(np.uint64)


def count_natural_key_duplicates(table: pa.Table) -> int:
    """Exact duplicate count on NATURAL_KEY.

    Step 1: vectorised 64-bit hash per row (fast). Step 2: rows whose hash is not
    unique are only *candidates*; an exact group_by on those rows alone decides.
    """
    n = table.num_rows
    h = np.full(n, 0x9E3779B97F4A7C15, dtype=np.uint64)
    with np.errstate(over="ignore"):
        for name in NATURAL_KEY:
            h ^= _as_uint64(table[name])
            h *= np.uint64(0xFF51AFD7ED558CCD)
            h ^= h >> np.uint64(33)
    order = np.argsort(h, kind="stable")
    hs = h[order]
    same = hs[1:] == hs[:-1]
    if not same.any():
        return 0
    cand = np.zeros(n, dtype=bool)
    cand[order[1:][same]] = True
    cand[order[:-1][same]] = True
    sub = table.filter(pa.array(cand)).select(NATURAL_KEY)
    return sub.num_rows - sub.group_by(NATURAL_KEY).aggregate([]).num_rows


def _pct(k: int, n: int) -> str:
    return f"{k:,} ({k / n:.3%})" if n else "0"


def profile(table: pa.Table, lc: Lifecycle) -> dict:
    """Facts about the source, recorded before any metric is computed."""
    n = table.num_rows
    prof = {"rows": n, "columns": {}, "by_license": {}}
    for name in table.column_names:
        col = table[name]
        entry = {"type": str(col.type), "null_pct": round(col.null_count / n * 100, 4) if n else None}
        if pa.types.is_timestamp(col.type) or pa.types.is_integer(col.type) or pa.types.is_floating(col.type):
            mm = pc.min_max(col)
            entry["min"], entry["max"] = str(mm["min"].as_py()), str(mm["max"].as_py())
        prof["columns"][name] = entry
    lic = table["hvfhs_license_num"].combine_chunks().dictionary_encode()
    lic_idx = lic.indices.to_numpy(zero_copy_only=False)
    for i, code in enumerate(lic.dictionary.to_pylist()):
        m = lic_idx == i
        k = int(m.sum())
        os_counts = {c: int(((lc.on_scene_status == i) & m).sum()) for i, c in enumerate(ON_SCENE_CODES)}
        prof["by_license"][code] = {
            "rides": k,
            "on_scene_status": os_counts,
            "on_scene_observed_pct": round(os_counts[OS_OBSERVED] / k * 100, 3),
            "trip_time_mismatch_pct": round(int((lc.trip_time_mismatch & m).sum()) / k * 100, 3),
        }
    return prof


def validate_trips(table: pa.Table, lc: Lifecycle, zones: dict, cfg) -> tuple[list[CheckResult], dict]:
    n = table.num_rows
    results: list[CheckResult] = []
    decisions: dict = {}

    # ---- completeness: row count & month coverage ---------------------------
    first, last = month_bounds(cfg.month)
    pickup_day = lc.pickup.astype("datetime64[D]")
    in_month = (pickup_day >= np.datetime64(first)) & (pickup_day <= np.datetime64(last))
    outside = int((~in_month & ~np.isnat(lc.pickup)).sum())
    days = np.arange(np.datetime64(first), np.datetime64(last) + 1)
    idx = (pickup_day[in_month] - np.datetime64(first)).astype(int)
    per_day = np.bincount(idx, minlength=len(days))
    empty_days = [str(days[i]) for i in np.where(per_day == 0)[0]]
    results.append(CheckResult(
        "hvfhv.month_coverage", FAIL if empty_days else (WARN if outside else PASS),
        f"rows={n:,}; pickups outside {cfg.month}: {outside:,}; days with zero pickups: {empty_days or 'none'}; "
        f"daily rides min={per_day.min():,} max={per_day.max():,}",
        category="completeness",
        evidence={"daily_rides": {str(d): int(c) for d, c in zip(days, per_day)}},
    ))

    # ---- timestamp coverage --------------------------------------------------
    cov = {name: 1 - table[col].null_count / n for name, col in
           [("request", "request_datetime"), ("on_scene", "on_scene_datetime"),
            ("pickup", "pickup_datetime"), ("dropoff", "dropoff_datetime")]}
    missing_core = int((lc.validation_status == VALIDATION_CODES.index(VS_MISSING_TS)).sum())
    results.append(CheckResult(
        "hvfhv.timestamp_coverage",
        FAIL if missing_core / n > cfg.max_excluded_share else (WARN if missing_core else PASS),
        "populated: " + ", ".join(f"{k}={v:.3%}" for k, v in cov.items())
        + f"; rides missing request/pickup/dropoff: {_pct(missing_core, n)}",
        category="completeness", evidence={"populated_share": cov},
    ))

    # ---- uniqueness ----------------------------------------------------------
    # The source has no trip identifier. ride_id is assigned as the row ordinal in
    # the checksummed raw file; to make sure that is not hiding double-counted
    # trips, test uniqueness of the full natural key.
    dupes = count_natural_key_duplicates(table)
    results.append(CheckResult(
        "hvfhv.trip_uniqueness",
        PASS if dupes == 0 else (FAIL if dupes / n > cfg.max_duplicate_share else WARN),
        f"no trip ID in source; natural key ({len(NATURAL_KEY)} fields) duplicates: {_pct(dupes, n)} "
        f"(FAIL above {cfg.max_duplicate_share:.1%}). ride_id = row ordinal in the checksummed raw file.",
        category="uniqueness", evidence={"duplicate_rows": dupes},
    ))

    # ---- chronology ----------------------------------------------------------
    pbr = int((lc.validation_status == VALIDATION_CODES.index(VS_PICKUP_BEFORE_REQUEST)).sum())
    dnp = int((lc.validation_status == VALIDATION_CODES.index(VS_DROPOFF_NOT_AFTER_PICKUP)).sum())
    excluded = int((~lc.in_kpi_population).sum())
    pbr_mask = lc.validation_status == VALIDATION_CODES.index(VS_PICKUP_BEFORE_REQUEST)
    pbr_whole = int((pbr_mask & lc.request_on_whole_minute).sum())
    results.append(CheckResult(
        "hvfhv.chronology_request_pickup_dropoff",
        FAIL if excluded / n > cfg.max_excluded_share else (WARN if excluded else PASS),
        f"pickup before request: {_pct(pbr, n)}; dropoff not after pickup: {_pct(dnp, n)}. "
        f"These rides are flagged (validation_status) and excluded from the KPI population, not deleted. "
        f"Total excluded {_pct(excluded, n)} vs limit {cfg.max_excluded_share:.0%}. "
        f"{pbr_whole / max(pbr, 1):.0%} of pickup-before-request rows have a request stamped on an exact minute "
        f"(chance rate 1.7%), consistent with pre-scheduled rides storing the booked time.",
        category="chronology",
        evidence={"pickup_before_request": pbr, "dropoff_not_after_pickup": dnp,
                  "pickup_before_request_whole_minute_share": round(pbr_whole / max(pbr, 1), 4)},
    ))

    # ---- the on-scene decision ----------------------------------------------
    counts = {c: int((lc.on_scene_status == i).sum()) for i, c in enumerate(ON_SCENE_CODES)}
    populated = 1 - counts[OS_MISSING] / n
    observed_share = counts[OS_OBSERVED] / n
    use_stages = observed_share >= cfg.min_on_scene_observed_share
    decisions["on_scene"] = {
        "populated_share": round(populated, 5),
        "independently_observed_share": round(observed_share, 5),
        "status_counts": counts,
        "used_for_stage_decomposition": bool(use_stages),
        "used_for_headline_kpi": False,
        "rule": (f"on_scene is used only to split request->pickup into two stages, only on rides where "
                 f"request <= on_scene < pickup, and only if that holds for >= {cfg.min_on_scene_observed_share:.0%} of rides."),
    }
    results.append(CheckResult(
        "hvfhv.on_scene_semantics",
        WARN if use_stages else FAIL,
        f"on_scene populated for {populated:.3%} of rides, but populated != observed: "
        f"equal to pickup to the second {_pct(counts[OS_SAME_AS_PICKUP], n)}, before request {_pct(counts[OS_BEFORE_REQUEST], n)}, "
        f"after pickup {_pct(counts[OS_AFTER_PICKUP], n)}. Independently observed & ordered: {observed_share:.2%}. "
        f"Decision: {'USE for stage decomposition only (headline KPI stays request->pickup)' if use_stages else 'DO NOT use; stage KPI withheld'}.",
        scope=INFO, category="semantic", evidence=decisions["on_scene"],
    ))

    # ---- location integrity --------------------------------------------------
    known = np.array(sorted(zones["zones"].keys()))
    placeholders = np.array(zones["placeholder_ids"])
    for side, col in (("pickup", "PULocationID"), ("dropoff", "DOLocationID")):
        ids = table[col].to_numpy(zero_copy_only=False)
        nulls = table[col].null_count
        unmatched = int((~np.isin(ids, known)).sum()) - nulls
        placeholder = int(np.isin(ids, placeholders).sum())
        bad = unmatched + nulls
        results.append(CheckResult(
            f"hvfhv.{side}_location_integrity",
            FAIL if bad / n > cfg.max_excluded_share else (WARN if bad or placeholder else PASS),
            f"{col}: null {_pct(nulls, n)}; not in lookup {_pct(unmatched, n)}; "
            f"placeholder IDs {placeholders.tolist()} (Unknown/Outside NYC) {_pct(placeholder, n)}. "
            + ("Placeholder pickups are excluded from the zone KPI only." if side == "pickup" else "Dropoff is descriptive only."),
            category="location", evidence={"null": nulls, "unmatched": unmatched, "placeholder": placeholder},
        ))

    # ---- numeric plausibility (flag + count; never delete) --------------------
    miles = table["trip_miles"].to_numpy(zero_copy_only=False)
    fare = table["base_passenger_fare"].to_numpy(zero_copy_only=False)
    r2p = lc.request_to_pickup_min
    pop = lc.in_kpi_population
    long_wait = int((pop & (r2p > cfg.max_plausible_request_to_pickup_min)).sum())
    results.append(CheckResult(
        "hvfhv.numeric_plausibility", WARN,
        f"trip_miles <= 0: {_pct(int((miles <= 0).sum()), n)}; trip_miles > {cfg.max_plausible_trip_miles:g}: "
        f"{_pct(int((miles > cfg.max_plausible_trip_miles).sum()), n)}; base_passenger_fare < 0: {_pct(int((fare < 0).sum()), n)}; "
        f"request->pickup > {cfg.max_plausible_request_to_pickup_min:g} min: {_pct(long_wait, n)}. "
        "Counted, not removed: none of these fields define the KPI population, and median/P90 are robust to a small tail.",
        scope=INFO, category="plausibility",
    ))
    mism = int(lc.trip_time_mismatch.sum())
    results.append(CheckResult(
        "hvfhv.trip_time_consistency", WARN if mism else PASS,
        f"source trip_time differs from (dropoff - pickup) by > 60 s for {_pct(mism, n)} "
        f"(see profile.by_license). Decision: durations are derived from timestamps; trip_time is not used.",
        scope=INFO, category="semantic",
    ))

    # ---- what we cannot establish -------------------------------------------
    whole = lc.request_on_whole_minute & pop
    results.append(CheckResult(
        "hvfhv.request_semantics_scheduled_rides", UNKNOWN,
        f"{_pct(int(whole.sum()), int(pop.sum()))} of KPI-population rides have a request on an exact minute vs a 1.7% chance rate. "
        "The source has no scheduled-ride flag, so request_datetime may be a booked slot, not a live request, "
        "for some rides. Impact is quantified as a sensitivity in metrics.json; it cannot be resolved from this data.",
        scope=INFO, category="semantic",
    ))
    return results, decisions
