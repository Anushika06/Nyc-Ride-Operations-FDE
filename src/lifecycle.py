"""Derive the ride lifecycle from the raw timestamps, without altering them.

Lifecycle (authoritative):   Request ──► Pickup ──► Dropoff
On-scene (conditional):      Request ──► On-scene ──► Pickup
                              (driver arrival)   (curb wait)

Every ride gets flags explaining *why* it is or is not usable for each metric.
Nothing is dropped here: invalid records stay in ride_journey with a reason.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pyarrow as pa

# on_scene_status codes
OS_OBSERVED = "observed"            # request <= on_scene < pickup: a distinct, ordered arrival event
OS_SAME_AS_PICKUP = "same_as_pickup"  # on_scene == pickup to the second: arrival not separately recorded
OS_BEFORE_REQUEST = "before_request"  # on_scene < request: out of order
OS_AFTER_PICKUP = "after_pickup"      # on_scene > pickup: out of order
OS_MISSING = "missing"
ON_SCENE_CODES = [OS_OBSERVED, OS_SAME_AS_PICKUP, OS_BEFORE_REQUEST, OS_AFTER_PICKUP, OS_MISSING]

# validation_status codes (first matching reason wins)
VS_VALID = "valid"
VS_MISSING_TS = "missing_timestamp"
VS_PICKUP_BEFORE_REQUEST = "pickup_before_request"
VS_DROPOFF_NOT_AFTER_PICKUP = "dropoff_not_after_pickup"
VALIDATION_CODES = [VS_VALID, VS_MISSING_TS, VS_PICKUP_BEFORE_REQUEST, VS_DROPOFF_NOT_AFTER_PICKUP]


def _ts(table: pa.Table, col: str) -> np.ndarray:
    return table[col].to_numpy().astype("datetime64[us]")


def _minutes(later: np.ndarray, earlier: np.ndarray) -> np.ndarray:
    delta = (later - earlier).astype("timedelta64[us]").astype(np.float64) / 60e6
    delta[np.isnat(later) | np.isnat(earlier)] = np.nan
    return delta


@dataclass
class Lifecycle:
    n: int
    request: np.ndarray
    on_scene: np.ndarray
    pickup: np.ndarray
    dropoff: np.ndarray
    request_to_pickup_min: np.ndarray
    request_to_on_scene_min: np.ndarray
    on_scene_to_pickup_min: np.ndarray
    trip_duration_min: np.ndarray
    on_scene_status: np.ndarray       # int8 index into ON_SCENE_CODES
    validation_status: np.ndarray     # int8 index into VALIDATION_CODES
    request_on_whole_minute: np.ndarray
    trip_time_mismatch: np.ndarray    # |trip_time - (dropoff - pickup)| > 60 s

    @property
    def in_kpi_population(self) -> np.ndarray:
        return self.validation_status == VALIDATION_CODES.index(VS_VALID)

    @property
    def stage_population(self) -> np.ndarray:
        return self.in_kpi_population & (self.on_scene_status == ON_SCENE_CODES.index(OS_OBSERVED))


def compute_lifecycle(table: pa.Table) -> Lifecycle:
    r, o = _ts(table, "request_datetime"), _ts(table, "on_scene_datetime")
    p, d = _ts(table, "pickup_datetime"), _ts(table, "dropoff_datetime")
    n = len(r)

    r2p = _minutes(p, r)
    r2o = _minutes(o, r)
    o2p = _minutes(p, o)
    dur = _minutes(d, p)

    # validation_status: precedence order is deliberate and documented.
    vs = np.zeros(n, dtype=np.int8)
    missing = np.isnat(r) | np.isnat(p) | np.isnat(d)
    vs[(vs == 0) & missing] = VALIDATION_CODES.index(VS_MISSING_TS)
    vs[(vs == 0) & (r2p < 0)] = VALIDATION_CODES.index(VS_PICKUP_BEFORE_REQUEST)
    vs[(vs == 0) & (dur <= 0)] = VALIDATION_CODES.index(VS_DROPOFF_NOT_AFTER_PICKUP)

    os_ = np.full(n, ON_SCENE_CODES.index(OS_OBSERVED), dtype=np.int8)
    os_[o2p == 0] = ON_SCENE_CODES.index(OS_SAME_AS_PICKUP)
    os_[r2o < 0] = ON_SCENE_CODES.index(OS_BEFORE_REQUEST)
    os_[o2p < 0] = ON_SCENE_CODES.index(OS_AFTER_PICKUP)
    os_[np.isnat(o) | np.isnat(r) | np.isnat(p)] = ON_SCENE_CODES.index(OS_MISSING)

    # Pre-scheduled ride signal: request timestamps landing exactly on a whole
    # minute. Chance rate is 1/60; scheduled trips appear to store the booked
    # slot. Used only for sensitivity analysis, never to exclude rides.
    r_us = r.astype(np.int64)
    whole_min = (~np.isnat(r)) & (r_us % 60_000_000 == 0)

    trip_time = table["trip_time"].to_numpy(zero_copy_only=False).astype(np.float64)
    mismatch = np.abs(trip_time - dur * 60) > 60

    return Lifecycle(
        n=n, request=r, on_scene=o, pickup=p, dropoff=d,
        request_to_pickup_min=r2p, request_to_on_scene_min=r2o, on_scene_to_pickup_min=o2p,
        trip_duration_min=dur, on_scene_status=os_, validation_status=vs,
        request_on_whole_minute=whole_min, trip_time_mismatch=mismatch,
    )


def codes_to_dictionary(codes: np.ndarray, labels: list[str]) -> pa.DictionaryArray:
    return pa.DictionaryArray.from_arrays(pa.array(codes, type=pa.int8()), pa.array(labels))
