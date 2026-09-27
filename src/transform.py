"""TRANSFORM: build `ride_journey`, one row per ride, joined to zone and weather context.

Joins are many-to-one only (ride -> zone, ride -> weather hour), so the row count
cannot change; that invariant is asserted.
"""
from __future__ import annotations

import numpy as np
import pyarrow as pa

from .lifecycle import Lifecycle, ON_SCENE_CODES, VALIDATION_CODES, codes_to_dictionary

TIME_PERIODS = ["overnight_00_05", "am_peak_06_09", "midday_10_15", "pm_peak_16_19", "evening_20_23"]
_HOUR_TO_PERIOD = np.array([0] * 6 + [1] * 4 + [2] * 6 + [3] * 4 + [4] * 4, dtype=np.int8)

PRECIP_BANDS = ["dry", "light_rain", "moderate_or_heavy_rain", "weather_unavailable"]


def _nullable(values: np.ndarray) -> pa.Array:
    """NaN / NaT become real nulls: a missing value stays missing, never 0."""
    return pa.array(values, from_pandas=True)


def _zone_columns(ids: np.ndarray, zones: dict, prefix: str) -> dict:
    """Map location IDs to dictionary-encoded zone/borough labels (IDs kept as-is)."""
    lookup = zones["zones"]
    max_id = max(max(lookup), int(ids.max()) if len(ids) else 0)
    zone_idx = np.full(max_id + 1, -1, dtype=np.int32)
    zone_labels, borough_labels = [], []
    borough_pos = {}
    b_idx = np.full(max_id + 1, -1, dtype=np.int32)
    for i, (lid, z) in enumerate(sorted(lookup.items())):
        zone_idx[lid] = i
        zone_labels.append(z["zone"])
        if z["borough"] not in borough_pos:
            borough_pos[z["borough"]] = len(borough_labels)
            borough_labels.append(z["borough"])
        b_idx[lid] = borough_pos[z["borough"]]
    zi, bi = zone_idx[ids], b_idx[ids]
    return {
        f"{prefix}_zone": pa.DictionaryArray.from_arrays(pa.array(zi, mask=zi < 0), pa.array(zone_labels)),
        f"{prefix}_borough": pa.DictionaryArray.from_arrays(pa.array(bi, mask=bi < 0), pa.array(borough_labels)),
    }


def join_weather(request: np.ndarray, weather: dict | None, cfg) -> dict:
    """Deterministic rule: request_datetime (NYC local) floored to the hour
    == Open-Meteo hourly timestamp (timezone=America/New_York)."""
    n = len(request)
    hour = request.astype("datetime64[h]")
    out = {
        "weather_hour": hour.astype("datetime64[us]"),
        "temperature_c": np.full(n, np.nan, dtype=np.float32),
        "precipitation_mm": np.full(n, np.nan, dtype=np.float32),
        "wind_speed_kmh": np.full(n, np.nan, dtype=np.float32),
        "precip_band": np.full(n, PRECIP_BANDS.index("weather_unavailable"), dtype=np.int8),
        "weather_matched": np.zeros(n, dtype=bool),
    }
    if weather is None:
        return out
    wt = np.array(weather["time"], dtype="datetime64[h]")
    order = np.argsort(wt, kind="stable")
    wt = wt[order]
    pos = np.clip(np.searchsorted(wt, hour), 0, len(wt) - 1)
    matched = (wt[pos] == hour) & ~np.isnat(hour)

    def series(name):
        s = np.array([np.nan if v is None else v for v in weather[name]], dtype=np.float64)[order]
        v = s[pos].astype(np.float32)
        v[~matched] = np.nan
        return v

    out["temperature_c"] = series("temperature_2m")
    out["precipitation_mm"] = series("precipitation")
    out["wind_speed_kmh"] = series("wind_speed_10m")
    precip = out["precipitation_mm"]
    has = matched & ~np.isnan(precip)
    band = out["precip_band"]
    band[has & (precip == 0)] = PRECIP_BANDS.index("dry")
    band[has & (precip > 0) & (precip < cfg.light_rain_max_mm)] = PRECIP_BANDS.index("light_rain")
    band[has & (precip >= cfg.light_rain_max_mm)] = PRECIP_BANDS.index("moderate_or_heavy_rain")
    out["weather_matched"] = has
    return out


def build_ride_journey(table: pa.Table, lc: Lifecycle, zones: dict, weather: dict | None, cfg) -> tuple[pa.Table, dict]:
    n = table.num_rows
    pu = table["PULocationID"].to_numpy(zero_copy_only=False)
    do = table["DOLocationID"].to_numpy(zero_copy_only=False)
    req_hour = (lc.request.astype("datetime64[h]").astype(np.int64) % 24).astype(np.int8)
    # 1970-01-01 was a Thursday -> weekday index with Monday = 0
    req_dow = ((lc.request.astype("datetime64[D]").astype(np.int64) + 3) % 7).astype(np.int8)
    w = join_weather(lc.request, weather, cfg)
    lic = table["hvfhs_license_num"].combine_chunks().dictionary_encode()
    # Pickups that cannot be attributed to an NYC zone: placeholder IDs or IDs absent from the lookup.
    placeholder_pu = np.isin(pu, np.array(zones["placeholder_ids"])) | ~np.isin(pu, np.array(list(zones["zones"])))

    cols = {
        "ride_id": pa.array(np.arange(n, dtype=np.int64)),
        "hvfhs_license_num": lic,
        "request_datetime": _nullable(lc.request),
        "on_scene_datetime": _nullable(lc.on_scene),
        "pickup_datetime": _nullable(lc.pickup),
        "dropoff_datetime": _nullable(lc.dropoff),
        "pickup_zone_id": pa.array(pu),
        **_zone_columns(pu, zones, "pickup"),
        "dropoff_zone_id": pa.array(do),
        **_zone_columns(do, zones, "dropoff"),
        "trip_miles": table["trip_miles"].combine_chunks(),
        "trip_duration_min": _nullable(lc.trip_duration_min.astype(np.float32)),
        "request_to_pickup_min": _nullable(lc.request_to_pickup_min.astype(np.float32)),
        "request_to_on_scene_min": _nullable(lc.request_to_on_scene_min.astype(np.float32)),
        "on_scene_to_pickup_min": _nullable(lc.on_scene_to_pickup_min.astype(np.float32)),
        "on_scene_status": codes_to_dictionary(lc.on_scene_status, ON_SCENE_CODES),
        "validation_status": codes_to_dictionary(lc.validation_status, VALIDATION_CODES),
        "in_kpi_population": pa.array(lc.in_kpi_population),
        "in_stage_population": pa.array(lc.stage_population),
        "in_zone_population": pa.array(lc.in_kpi_population & ~placeholder_pu),
        "request_on_whole_minute": pa.array(lc.request_on_whole_minute),
        "trip_time_mismatch": pa.array(lc.trip_time_mismatch),
        "request_hour": pa.array(req_hour),
        "request_weekday": pa.array(req_dow),
        "is_weekend": pa.array(req_dow >= 5),
        "time_period": codes_to_dictionary(_HOUR_TO_PERIOD[req_hour], TIME_PERIODS),
        "weather_hour": _nullable(w["weather_hour"]),
        "temperature_c": _nullable(w["temperature_c"]),
        "precipitation_mm": _nullable(w["precipitation_mm"]),
        "wind_speed_kmh": _nullable(w["wind_speed_kmh"]),
        "precip_band": codes_to_dictionary(w["precip_band"], PRECIP_BANDS),
    }
    journey = pa.table(cols)
    assert journey.num_rows == n, "ride_journey must keep exactly one row per source ride"
    stats = {
        "rows": n,
        "kpi_population": int(lc.in_kpi_population.sum()),
        "stage_population": int(lc.stage_population.sum()),
        "zone_population": int((lc.in_kpi_population & ~placeholder_pu).sum()),
        "zone_attributable_rides": int((~placeholder_pu).sum()),
        "weather_matched_rides": int(w["weather_matched"].sum()),
        "weather_matched_kpi_rides": int((w["weather_matched"] & lc.in_kpi_population).sum()),
    }
    return journey, {"stats": stats, "weather": w, "pickup_ids": pu, "placeholder_pu": placeholder_pu,
                     "request_hour": req_hour,
                     "license_codes": lic.indices.to_numpy(zero_copy_only=False),
                     "license_labels": lic.dictionary.to_pylist()}
