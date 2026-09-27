"""Unit tests for the rules the KPIs depend on. Run: python -m unittest -v"""
from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from src.checks import FAIL, PASS, WARN, WEATHER, FRICTION, gate
from src.config import Config
from src.geo import ZoneShape, load_zone_shapes, map_points
from src.http_client import FaultInjector, NonRetryableHTTPError, RetryableHTTPError, request_with_retries
from src.ingest import retrieve_hvfhv, validate_weather_payload
from src.lifecycle import ON_SCENE_CODES, VALIDATION_CODES, compute_lifecycle
from src.metrics import compute_kpis, grouped_quantiles, validate_kpis
from src.nyc311 import aggregate_friction, complaints_table, rate_per_100k, retrieve_311
from src.publish import swap_in_partition
from src.transform import build_ride_journey
from src.validate import count_natural_key_duplicates, validate_trips
from tests.helpers import SAMPLE, FakeSources, complaints_311, month_of_trips, table, trip, weather_payload

LOG = logging.getLogger("test")
LOG.addHandler(logging.NullHandler())
LOG.propagate = False
T0 = datetime(2026, 2, 3, 10, 0, 0)
M = timedelta(minutes=1)
ZONES = {"zones": {68: {"zone": "East Chelsea", "borough": "Manhattan", "service_zone": ""},
                   161: {"zone": "Midtown Center", "borough": "Manhattan", "service_zone": ""},
                   237: {"zone": "Upper East Side South", "borough": "Manhattan", "service_zone": ""},
                   264: {"zone": "N/A", "borough": "Unknown", "service_zone": ""}},
         "placeholder_ids": [264]}


def cfg_for(tmp: Path | None = None, **kw) -> Config:
    base = Config.for_month("2026-02")
    if tmp is not None:
        kw.setdefault("data_dir", tmp / "data")
    return Config(**{**base.__dict__, **kw})


# ---------------------------------------------------------------- lifecycle / grain
class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.t = table([
            trip(T0, T0 + 3 * M, T0 + 4 * M, T0 + 20 * M),               # 0 valid, on-scene observed
            trip(T0, T0 + 4 * M, T0 + 4 * M, T0 + 20 * M),               # 1 on-scene == pickup
            trip(T0, T0 - 2 * M, T0 + 4 * M, T0 + 20 * M),               # 2 on-scene before request
            trip(T0 + 10 * M, T0, T0 + 1 * M, T0 + 20 * M),              # 3 pickup before request
            trip(T0, T0 + 1 * M, T0 + 4 * M, T0 + 4 * M),                # 4 dropoff == pickup
            trip(T0, None, T0 + 4 * M, T0 + 20 * M),                     # 5 on-scene missing (still valid)
            trip(T0, T0 + 1 * M, None, T0 + 20 * M),                     # 6 pickup missing
            trip(T0.replace(second=0), T0 + 1 * M, T0 + 2 * M, T0 + 9 * M, trip_time=9999),  # 7 whole-minute + trip_time mismatch
        ])
        self.lc = compute_lifecycle(self.t)

    def test_validation_status_precedence(self):
        self.assertEqual([VALIDATION_CODES[v] for v in self.lc.validation_status],
                         ["valid", "valid", "valid", "pickup_before_request", "dropoff_not_after_pickup",
                          "valid", "missing_timestamp", "valid"])

    def test_on_scene_is_not_assumed_observed(self):
        got = [ON_SCENE_CODES[v] for v in self.lc.on_scene_status]
        self.assertEqual(got[:3], ["observed", "same_as_pickup", "before_request"])
        self.assertEqual(got[5:7], ["missing", "missing"])

    def test_populations(self):
        self.assertEqual(self.lc.in_kpi_population.tolist(), [True, True, True, False, False, True, False, True])
        self.assertEqual(self.lc.stage_population.tolist(), [True, False, False, False, False, False, False, True])

    def test_missing_values_stay_missing(self):
        self.assertTrue(np.isnan(self.lc.request_to_pickup_min[6]))
        self.assertTrue(np.isnan(self.lc.request_to_on_scene_min[5]))

    def test_flags(self):
        self.assertTrue(self.lc.request_on_whole_minute[7])
        self.assertTrue(self.lc.trip_time_mismatch[7])
        self.assertFalse(self.lc.trip_time_mismatch[0])

    def test_ride_journey_keeps_one_row_per_ride_including_invalid(self):
        t = table([trip(T0, T0 + M, T0 + 2 * M, T0 + 9 * M, pu=68), trip(T0 + 5 * M, T0, T0 + M, T0 + 9 * M, pu=264)])
        lc = compute_lifecycle(t)
        journey, ctx = build_ride_journey(t, lc, ZONES, None, cfg_for())
        self.assertEqual(journey.num_rows, 2)                      # invalid ride kept
        self.assertEqual(journey["ride_id"].to_pylist(), [0, 1])
        self.assertEqual(journey["validation_status"].to_pylist(), ["valid", "pickup_before_request"])
        self.assertEqual(journey["pickup_zone"].to_pylist(), ["East Chelsea", "N/A"])
        self.assertEqual(ctx["stats"]["zone_attributable_rides"], 1)  # placeholder 264 not attributable


# ---------------------------------------------------------------- validation
class ValidationTests(unittest.TestCase):
    def by_name(self, results):
        return {r.check: r for r in results}

    def test_duplicate_count_is_exact(self):
        t = month_of_trips(per_day=5)
        self.assertEqual(count_natural_key_duplicates(t), 0)
        self.assertEqual(count_natural_key_duplicates(pa.concat_tables([t, t.slice(0, 7)])), 7)

    def test_clean_month_passes_critical_checks(self):
        t = month_of_trips(per_day=5)
        res, decisions = validate_trips(t, compute_lifecycle(t), ZONES, cfg_for())
        self.assertTrue(gate(res)["publish"], gate(res))
        self.assertTrue(decisions["on_scene"]["used_for_stage_decomposition"])

    def test_missing_day_fails_month_coverage(self):
        t = month_of_trips(per_day=5)
        t = t.filter(pc.not_equal(pc.day(t["pickup_datetime"]), 14))
        res, _ = validate_trips(t, compute_lifecycle(t), ZONES, cfg_for())
        self.assertEqual(self.by_name(res)["hvfhv.month_coverage"].status, FAIL)
        self.assertFalse(gate(res)["publish"])

    def test_too_many_chronology_violations_blocks(self):
        t = pa.concat_tables([month_of_trips(per_day=5), table([trip(T0 + 10 * M, T0, T0 + M, T0 + 5 * M, pu=68)] * 20)])
        t = t.set_column(t.schema.get_field_index("trip_miles"), "trip_miles", pa.array(np.arange(t.num_rows, dtype=float)))
        res, _ = validate_trips(t, compute_lifecycle(t), ZONES, cfg_for())
        self.assertEqual(self.by_name(res)["hvfhv.chronology_request_pickup_dropoff"].status, FAIL)

    def test_on_scene_withheld_when_not_observed(self):
        rows = [trip(T0 + d * timedelta(days=1), T0 + d * timedelta(days=1) + 4 * M, T0 + d * timedelta(days=1) + 4 * M,
                     T0 + d * timedelta(days=1) + 9 * M, pu=68) for d in range(-2, 26)]
        t = table(rows)
        _, decisions = validate_trips(t, compute_lifecycle(t), ZONES, cfg_for())
        self.assertFalse(decisions["on_scene"]["used_for_stage_decomposition"])

    def test_unknown_location_ids_counted_not_dropped(self):
        t = month_of_trips(per_day=5)
        t = t.set_column(t.schema.get_field_index("PULocationID"), "PULocationID",
                         pa.array([999] + [68] * (t.num_rows - 1), pa.int32()))
        r = self.by_name(validate_trips(t, compute_lifecycle(t), ZONES, cfg_for())[0])["hvfhv.pickup_location_integrity"]
        self.assertEqual((r.status, r.evidence["unmatched"]), (WARN, 1))


class WeatherTests(unittest.TestCase):
    def test_full_coverage_passes(self):
        series, res = validate_weather_payload(weather_payload(), cfg_for())
        self.assertEqual({r.check: r for r in res}["weather.hour_coverage"].status, PASS)
        self.assertIsNotNone(series)

    def test_gap_fails_weather_scope_only(self):
        _, res = validate_weather_payload(weather_payload(drop_hours=48), cfg_for())
        cov = {r.check: r for r in res}["weather.hour_coverage"]
        self.assertEqual((cov.status, cov.scope), (FAIL, WEATHER))
        g = gate(res)
        self.assertTrue(g["publish"])
        self.assertFalse(g["publish_weather_analysis"])

    def test_missing_variable_is_structural_failure(self):
        p = weather_payload()
        del p["hourly"]["precipitation"]
        series, res = validate_weather_payload(p, cfg_for())
        self.assertIsNone(series)
        self.assertEqual(res[0].status, FAIL)


# ---------------------------------------------------------------- retrieval
class RetrievalTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.src = FakeSources(month_of_trips(per_day=2), weather_payload())

    def tearDown(self):
        self.src.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def call(self, path, fault=None, stats=None):
        return request_with_retries(self.src.base + path, lambda r: r.read(), logger=LOG, source="t", timeout=5,
                                    max_retries=3, base_seconds=0, max_wait_seconds=0, fault=fault,
                                    sleep=lambda s: None, stats=stats)

    def cfg(self, **kw):
        env = self.src.env()
        return cfg_for(self.tmp, hvfhv_url_template=env["HVFHV_URL_TEMPLATE"], nyc311_api_url=env["NYC311_API_URL"],
                       retry_base_seconds=0, **kw)

    def test_transient_errors_are_retried(self):
        stats = {}
        self.assertIn(b"LocationID", self.call("/zones.csv", fault=FaultInjector([500, 429]), stats=stats))
        self.assertEqual(stats["t"], 2)

    def test_retries_are_bounded(self):
        with self.assertRaises(RetryableHTTPError):
            self.call("/always500")
        self.assertEqual(self.src.hits["/always500"], 3)

    def test_non_retryable_fails_fast(self):
        with self.assertRaises(NonRetryableHTTPError):
            self.call("/missing")
        self.assertEqual(self.src.hits["/missing"], 1)

    def test_truncated_body_is_retried_not_crashed(self):
        with self.assertRaises(RetryableHTTPError):
            self.call("/truncated")
        self.assertEqual(self.src.hits["/truncated"], 3)

    def test_hvfhv_download_preserved_with_manifest_and_reused(self):
        cfg = self.cfg()
        path, manifest, checks = retrieve_hvfhv(cfg, LOG, {})
        self.assertEqual({c.check: c.status for c in checks}, {"hvfhv.download_complete": PASS, "hvfhv.file_readable": PASS})
        self.assertEqual(manifest["bytes_received"], path.stat().st_size)
        retrieve_hvfhv(cfg, LOG, {})
        self.assertEqual(self.src.hits["/trips.parquet"], 1)          # rerun reuses the verified raw copy

    def test_tampered_raw_file_is_redownloaded_not_trusted(self):
        cfg = self.cfg()
        path, _, _ = retrieve_hvfhv(cfg, LOG, {})
        path.write_bytes(path.read_bytes()[:-10] + b"x" * 10)
        retrieve_hvfhv(cfg, LOG, {})
        self.assertEqual(self.src.hits["/trips.parquet"], 2)

    def test_311_paginates_preserves_pages_and_proves_completeness(self):
        cfg = self.cfg(nyc311_page_size=15)                           # 40 rows -> pages of 15, 15, 10
        rows, manifest, checks = retrieve_311(cfg, LOG, {})
        self.assertEqual(len(rows), 40)
        self.assertEqual([p["rows"] for p in manifest["pages"]], [15, 15, 10])
        self.assertEqual({c.check: c.status for c in checks}, {"nyc311.retrieval_complete": PASS, "nyc311.unique_key": PASS})
        snap = cfg.nyc311_dir / manifest["snapshot_dir"]
        self.assertEqual(json.loads((snap / "page_001.json").read_text()), rows[15:30])  # raw page == what was parsed
        hits = self.src.hits["/311"]
        retrieve_311(cfg, LOG, {})
        self.assertEqual(self.src.hits["/311"], hits)                 # snapshot reused, no new requests

    def test_311_count_mismatch_fails_friction_scope(self):
        self.src.count_override = 41                                  # server says 41, pages deliver 40
        _, _, checks = retrieve_311(self.cfg(nyc311_page_size=15), LOG, {})
        c = {c.check: c for c in checks}["nyc311.retrieval_complete"]
        self.assertEqual((c.status, c.scope), (FAIL, FRICTION))


# ---------------------------------------------------------------- geography + 311 aggregation
class GeoAndFrictionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.shapes = load_zone_shapes(SAMPLE / "taxi_zones.zip")

    def test_real_shapefile_parses(self):
        self.assertEqual(len(self.shapes), 263)
        self.assertEqual(len({s.location_id for s in self.shapes}), 263)

    def test_known_points_map_to_expected_zones(self):
        # 111 8th Ave (state plane from a real 311 record) -> East Chelsea (68); far outside NYC -> -1; NaN -> -1
        z, amb = map_points(np.array([983739.0, 500000.0, np.nan]), np.array([209135.0, 500000.0, np.nan]), self.shapes)
        self.assertEqual(z.tolist(), [68, -1, -1])
        self.assertFalse(amb.any())

    def test_holes_and_overlaps_are_deterministic(self):
        square = lambda a, b: np.array([[a, a], [b, a], [b, b], [a, b], [a, a]], dtype=float)
        donut = ZoneShape(5, "donut", "X", (0, 0, 10, 10), [square(0, 10), square(4, 6)])
        overlap = ZoneShape(9, "overlap", "X", (0, 0, 3, 3), [square(0, 3)])
        z, amb = map_points(np.array([1.0, 5.0, 8.0]), np.array([1.0, 5.0, 8.0]), [overlap, donut])
        self.assertEqual(z.tolist(), [5, -1, 5])                      # hole excluded; overlap -> lowest id
        self.assertEqual(amb.tolist(), [True, False, False])

    def test_complaints_are_mapped_not_linked_to_rides(self):
        zones = {"zones": {s.location_id: {"zone": s.zone, "borough": s.borough} for s in self.shapes}, "placeholder_ids": []}
        rows = complaints_311(n=40) + [{"unique_key": "x1", "descriptor": "Driver Complaint - Passenger", "borough": "QUEENS"}]
        tbl, mapping, checks = complaints_table(rows, self.shapes, zones, 0.9)
        self.assertEqual(len(tbl), 41)
        self.assertEqual(tbl[-1]["mapping"], "no_coordinates")        # kept and labelled, never guessed
        self.assertNotIn("ride_id", tbl[0])
        self.assertEqual(mapping["mapped"] + mapping["no_coordinates"] + mapping["outside_all_zones"], 41)
        self.assertEqual({c.check: c.status for c in checks}["nyc311.zone_mapping_borough_agreement"], PASS)

    def test_unmapped_passenger_complaints_are_quantified_not_hidden(self):
        """KPI 5's numerator is mapped PASSENGER complaints, so the undercount that matters
        is the unmapped share of those, not the overall mapping rate."""
        zones = {"zones": {s.location_id: {"zone": s.zone, "borough": s.borough} for s in self.shapes}, "placeholder_ids": []}
        rows = complaints_311(n=40) + [
            {"unique_key": "x1", "descriptor": "Driver Complaint - Passenger", "borough": "QUEENS"},
            {"unique_key": "x2", "descriptor": "Driver Complaint - Non Passenger", "borough": "QUEENS"},
        ]
        tbl, mapping, checks = complaints_table(rows, self.shapes, zones, 0.9)
        passengers = [t for t in tbl if t["category"] == "passenger"]
        self.assertEqual(mapping["passenger_total"], len(passengers))
        self.assertEqual(mapping["passenger_unmapped"], sum(t["zone_id"] is None for t in passengers))
        self.assertEqual(mapping["passenger_mapped"] + mapping["passenger_unmapped"], mapping["passenger_total"])
        self.assertGreaterEqual(mapping["passenger_unmapped"], 1)      # the x1 row has no coordinates
        detail = {c.check: c.detail for c in checks}["nyc311.zone_mapping_coverage"]
        self.assertIn("KPI 5 understates", detail)

    def test_one_to_many_aggregation_zero_fills_and_reconciles(self):
        zones = {"zones": {1: {"borough": "A"}, 2: {"borough": "A"}, 3: {"borough": "B"}}}
        tbl = [{"zone_id": 1, "category": "passenger", "pickup_reliability_reason": True},
               {"zone_id": 1, "category": "non_passenger", "pickup_reliability_reason": False},
               {"zone_id": 3, "category": "passenger", "pickup_reliability_reason": False},
               {"zone_id": None, "category": "passenger", "pickup_reliability_reason": False}]
        per_zone, per_b = aggregate_friction(tbl, {1: 1000, 2: 500, 3: 200}, zones)
        self.assertEqual(per_zone[2]["complaints_all"], 0)             # zone with rides, no complaints -> explicit 0
        self.assertEqual(per_zone[1]["complaints_passenger"], 1)
        self.assertEqual(per_b["A"], {"hvfhv_pickups": 1500, "complaints_all": 2, "complaints_passenger": 1,
                                      "complaints_non_passenger": 1, "complaints_pickup_reliability": 1})
        self.assertEqual(sum(v["complaints_all"] for v in per_b.values()), 3)  # unmapped complaint not invented into a zone
        self.assertEqual(rate_per_100k(1, 1500), 66.67)


# ---------------------------------------------------------------- KPIs + publish
class MetricAndPublishTests(unittest.TestCase):
    def test_grouped_quantiles_exact(self):
        rng = np.random.default_rng(0)
        keys, vals = rng.integers(0, 5, 2000), rng.exponential(5, 2000)
        got = grouped_quantiles(keys, vals, [0.5, 0.9])
        for k in range(5):
            exp = np.quantile(vals[keys == k], [0.5, 0.9])
            self.assertEqual(got[k][1], [round(exp[0], 2), round(exp[1], 2)])

    def test_kpi_values_on_known_rides(self):
        # waits 4, 6, 10 min; on-scene after 3, 3, 9 min -> arrival share = (3+3+9)/(4+6+10) = 75%
        t = table([trip(T0, T0 + 3 * M, T0 + 4 * M, T0 + 20 * M, pu=68),
                   trip(T0, T0 + 3 * M, T0 + 6 * M, T0 + 20 * M, pu=68),
                   trip(T0, T0 + 9 * M, T0 + 10 * M, T0 + 20 * M, pu=161)])
        lc = compute_lifecycle(t)
        cfg = cfg_for(min_rides_per_zone=1)
        _, ctx = build_ride_journey(t, lc, ZONES, None, cfg)
        g = {"publish_friction_kpi": True}
        friction = {"per_borough": {"Manhattan": {"hvfhv_pickups": 3, "complaints_passenger": 1}}}
        kpis, zone_rows, _ = compute_kpis(ctx, lc, ZONES, {"on_scene": {"used_for_stage_decomposition": True}}, g, cfg, friction)
        self.assertEqual(kpis["kpi_1_median_request_to_pickup_min"]["value"], 6.0)
        self.assertEqual(kpis["kpi_2_p90_request_to_pickup_min"]["value"], 9.2)
        self.assertEqual(kpis["kpi_3_driver_arrival_share_of_wait_pct"]["value"], 75.0)
        self.assertEqual(kpis["kpi_4_p90_request_to_pickup_by_zone"]["value"]["worst_zone_p90_min"], 10.0)
        self.assertEqual(kpis["kpi_5_passenger_311_complaints_per_100k_rides"]["value"]["citywide_rate_per_100k"], 33333.33)

    def test_inconsistent_kpis_fail_output_validation(self):
        kpis = {"kpi_1_median_request_to_pickup_min": {"value": 12.0}, "kpi_2_p90_request_to_pickup_min": {"value": 5.0},
                "kpi_3_driver_arrival_share_of_wait_pct": {"value": 140.0}}
        res = validate_kpis(kpis, [{"rides_in_kpi_population": 10}],
                            {"kpi_population": 10, "rows": 10, "zone_population": 10}, None)
        st = {r.check: r.status for r in res}
        self.assertEqual((st["output.headline_consistency"], st["output.stage_share_bounds"]), (FAIL, FAIL))

    def test_partition_swap_replaces_not_appends(self):
        root = Path(tempfile.mkdtemp())
        try:
            final = root / "2026-02"
            for content in ("first", "second"):
                staging = root / f".staging_{content}"
                staging.mkdir()
                (staging / "metrics.json").write_text(content)
                swap_in_partition(staging, final, LOG)
            self.assertEqual((final / "metrics.json").read_text(), "second")
            self.assertEqual(sorted(p.name for p in root.iterdir()), ["2026-02"])
        finally:
            shutil.rmtree(root)


if __name__ == "__main__":
    unittest.main()
