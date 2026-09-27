"""End-to-end: run_pipeline.run() against local fakes of all five sources, and the real sample mode.

Proves the dependability properties: publish on healthy data, idempotent rerun, safe
failure with no publication, context failures withhold only their own output, and the
offline sample mode runs the same stages.
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pyarrow.parquet as pq

import run_pipeline
from tests.helpers import FakeSources, month_of_trips, weather_payload

MONTH = "2026-02"
EXPECTED_FILES = ("ride_journey.parquet", "metrics.json", "validation_report.json", "run_summary.json",
                  "evidence_table.md", "zone_breakdown.csv", "profile.json", "nyc311_complaints_mapped.csv")


def _close_logs():
    for h in logging.getLogger("ride_ops_pipeline").handlers:
        h.close()


class EndToEnd(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.src = FakeSources(month_of_trips(per_day=40), weather_payload())
        self.env = mock.patch.dict(os.environ, {
            **self.src.env(), "DATA_DIR": str(self.tmp / "data"), "OUTPUT_DIR": str(self.tmp / "output"),
            "LOG_DIR": str(self.tmp / "logs"), "MIN_RIDES_PER_ZONE": "10", "RETRY_BASE_SECONDS": "0",
            "NYC311_PAGE_SIZE": "15", "LOG_LEVEL": "WARNING"})
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.src.close()
        _close_logs()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def partition(self, root="output"):
        return self.tmp / root / MONTH

    def metrics(self, root="output"):
        return json.loads((self.partition(root) / "metrics.json").read_text())

    def test_publish_then_idempotent_rerun(self):
        self.assertEqual(run_pipeline.run(MONTH), 0)
        part = self.partition()
        for name in EXPECTED_FILES:
            self.assertTrue((part / name).exists(), name)
        rows = pq.ParquetFile(part / "ride_journey.parquet").metadata.num_rows
        self.assertEqual(rows, 28 * 40)                                  # one row per ride
        m = self.metrics()
        self.assertEqual(sum(not v.get("withheld") for v in m["kpis"].values()), 5)
        for v in m["kpis"].values():
            self.assertIn("definition", v)
            self.assertIn(v["validation_status"], {"PASS", "WARN", "UNKNOWN"})
        self.assertEqual(m["mode"], "full")

        hits_before = dict(self.src.hits)
        self.assertEqual(run_pipeline.run(MONTH), 0)
        self.assertEqual(self.src.hits, hits_before)                     # every raw input reused, zero requests
        self.assertEqual(pq.ParquetFile(part / "ride_journey.parquet").metadata.num_rows, rows)
        self.assertEqual(sorted(p.name for p in (self.tmp / "output").iterdir()), [MONTH])

    def test_schema_break_blocks_and_publishes_nothing(self):
        self.assertEqual(run_pipeline.run(MONTH, "missing_column"), 2)
        self.assertFalse(self.partition("output/_chaos_missing_column").exists())
        failed = list((self.tmp / "logs" / "failed_runs").glob("*.json"))
        self.assertEqual(json.loads(failed[0].read_text())["decision"], "BLOCKED_BY_VALIDATION")

    def test_failed_run_leaves_previous_partition_untouched(self):
        self.assertEqual(run_pipeline.run(MONTH), 0)
        before = (self.partition() / "metrics.json").read_bytes()
        with mock.patch.dict(os.environ, {"MAX_EXCLUDED_SHARE": "-1"}):
            self.assertEqual(run_pipeline.run(MONTH), 2)
        self.assertEqual((self.partition() / "metrics.json").read_bytes(), before)

    def test_311_incomplete_withholds_only_friction_kpi(self):
        self.src.count_override = 999                                    # server count != rows delivered
        self.assertEqual(run_pipeline.run(MONTH), 0)
        k = self.metrics()["kpis"]
        self.assertTrue(k["kpi_5_passenger_311_complaints_per_100k_rides"]["withheld"])
        self.assertIsNotNone(k["kpi_1_median_request_to_pickup_min"]["value"])

    def test_weather_gap_withholds_only_weather_analysis(self):
        self.assertEqual(run_pipeline.run(MONTH, "weather_gap"), 0)
        m = self.metrics("output/_chaos_weather_gap")
        self.assertTrue(m["supporting_analyses"]["precipitation_association"]["withheld"])
        self.assertEqual(sum(not v.get("withheld") for v in m["kpis"].values()), 5)

    def test_flaky_api_recovers(self):
        self.assertEqual(run_pipeline.run(MONTH, "api_flaky"), 0)
        summary = json.loads((self.partition("output/_chaos_api_flaky") / "run_summary.json").read_text())
        self.assertEqual(summary["http_retries"].get("open_meteo"), 2)

    def test_unpublished_month_fails_fast(self):
        self.src.routes["/trips.parquet"] = (403, b"AccessDenied", "text/plain")
        self.assertEqual(run_pipeline.run(MONTH), 1)
        self.assertEqual(self.src.hits.get("/trips.parquet"), 1)          # 403 is not retried
        self.assertFalse(self.partition().exists())


class SampleMode(unittest.TestCase):
    """The committed offline sample, through the real SampleServer and the same stages."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.env = mock.patch.dict(os.environ, {"DATA_DIR": str(self.tmp / "data"), "OUTPUT_DIR": str(self.tmp / "output"),
                                                "LOG_DIR": str(self.tmp / "logs"), "LOG_LEVEL": "WARNING"})
        self.env.start()

    def tearDown(self):
        self.env.stop()
        _close_logs()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_sample_runs_offline_is_labelled_and_idempotent(self):
        self.assertEqual(run_pipeline.run(None, sample=True), 0)
        part = self.tmp / "output" / "sample" / "2026-07"
        m = json.loads((part / "metrics.json").read_text())
        self.assertEqual(m["mode"], "sample")
        self.assertIn("NOT the complete dataset", m["data_scope"])
        self.assertIn("SAMPLE MODE", (part / "evidence_table.md").read_text(encoding="utf-8"))
        self.assertEqual(pq.ParquetFile(part / "ride_journey.parquet").metadata.num_rows, 10_000)
        v = json.loads((part / "validation_report.json").read_text())
        self.assertEqual([c["status"] for c in v["checks"] if c["check"] == "nyc311.retrieval_complete"], ["PASS"])
        first = m["kpis"]["kpi_2_p90_request_to_pickup_min"]["value"]

        self.assertEqual(run_pipeline.run(None, sample=True), 0)
        m2 = json.loads((part / "metrics.json").read_text())
        self.assertEqual(m2["kpis"]["kpi_2_p90_request_to_pickup_min"]["value"], first)   # deterministic
        self.assertEqual(pq.ParquetFile(part / "ride_journey.parquet").metadata.num_rows, 10_000)


if __name__ == "__main__":
    unittest.main()
