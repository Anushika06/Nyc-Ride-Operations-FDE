"""NYC Ride Operations Reliability Pipeline - one command, one month.

    python run_pipeline.py --month 2026-07     # full data (downloads ~511 MB once)
    python run_pipeline.py --sample            # offline, deterministic 10,000-ride sample

Stages
  1  EXTRACT            HVFHV Parquet, zone CSV + shapefile, Open-Meteo JSON, NYC 311 (paginated)
  2  PRESERVE RAW       byte-for-byte copies + retrieval manifests (sha256)
  3  COMPLETENESS       bytes vs Content-Length, readable footer, days, hours, 311 count vs received
  4  PROFILE/VALIDATE   business-oriented checks -> PASS / WARN / FAIL / UNKNOWN
  5  FLAG               invalid rides keep their row and get a validation_status
  6  TRANSFORM          lifecycle intervals
  7  JOIN               zones (reference), weather hour (context); 311 -> zone (point-in-polygon)
  8  MODEL              ride_journey (one row per ride) + zone/borough friction aggregates
  9  KPIs               5 metrics with definitions + supporting analyses
  10 VALIDATE OUTPUT    reconciliation of numbers against populations
  11 PUBLISH            atomic replace of the month partition (only if the gate allows)
  12 LOG                logs/pipeline_<month>.log + run_summary.json

Exit codes: 0 published, 2 blocked by the validation gate (nothing published),
            1 retrieval / unexpected failure (nothing published).
"""
from __future__ import annotations

import argparse
import csv
import dataclasses
import json
import re
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from src import checks as ck
from src.config import PROJECT_ROOT, Config
from src.http_client import FaultInjector, NonRetryableHTTPError, RetryableHTTPError
from src.ingest import (HVFHV_REQUIRED_COLUMNS, RetrievalError, check_required_columns, retrieve_hvfhv,
                        retrieve_weather, retrieve_zone_shapes, retrieve_zones, validate_weather_payload)
from src.lifecycle import compute_lifecycle
from src.log import build_logger
from src.metrics import (attach_validation_status, compute_kpis, pickups_by_zone, supporting_analyses,
                         validate_kpis)
from src.nyc311 import aggregate_friction, complaints_table, rate_per_100k, retrieve_311, validate_311
from src.publish import clean_stale_staging, new_staging_dir, swap_in_partition, write_json
from src.report import evidence_markdown
from src.transform import build_ride_journey
from src.validate import profile, validate_trips

CHAOS = ["none", "missing_column", "duplicate_rows", "weather_gap", "api_flaky"]
SAMPLE_DIR = PROJECT_ROOT / "data" / "sample"
SAMPLE_MIN_RIDES_PER_ZONE = 20  # the 10k-ride sample cannot meet the 1,000-ride production threshold


class GateBlocked(RuntimeError):
    pass


def apply_chaos_to_trips(table: pa.Table, chaos: str, logger) -> pa.Table:
    if chaos == "missing_column":
        logger.warning("CHAOS: dropping pickup_datetime to simulate an upstream schema break")
        return table.drop_columns(["pickup_datetime"])
    if chaos == "duplicate_rows":
        k = min(50_000, max(1, table.num_rows // 20))
        logger.warning("CHAOS: appending %s duplicated rides to simulate a double-delivered file segment", k)
        return pa.concat_tables([table, table.slice(0, k)])
    return table


def apply_chaos_to_weather(series: dict | None, chaos: str, logger) -> dict | None:
    if chaos == "weather_gap" and series:
        logger.warning("CHAOS: removing 48 hourly weather records (in memory only; raw file untouched)")
        keep = [i for i in range(len(series["time"])) if not (100 <= i < 148)]
        return {k: ([v[i] for i in keep] if isinstance(v, list) else v) for k, v in series.items()}
    return series


def build_config(month: str, chaos: str, sample_env: dict | None) -> Config:
    cfg = Config.for_month(month)
    if sample_env is not None:
        cfg = dataclasses.replace(
            cfg,
            hvfhv_url_template=sample_env["HVFHV_URL_TEMPLATE"], zone_lookup_url=sample_env["ZONE_LOOKUP_URL"],
            zone_shapes_url=sample_env["ZONE_SHAPES_URL"], weather_api_url=sample_env["WEATHER_API_URL"],
            nyc311_api_url=sample_env["NYC311_API_URL"], nyc311_page_size=500,
            data_dir=cfg.data_dir / "sample_run", output_dir=cfg.output_dir / "sample",
            min_rides_per_zone=SAMPLE_MIN_RIDES_PER_ZONE,
        )
    if chaos != "none":
        # Chaos runs never overwrite the real partition.
        cfg = dataclasses.replace(cfg, output_dir=cfg.output_dir / f"_chaos_{chaos}")
    return cfg


def run(month: str | None, chaos: str = "none", refresh_weather: bool = False, refresh_311: bool = False,
        sample: bool = False) -> int:
    if sample:
        from src.sample_server import SampleServer
        with SampleServer(SAMPLE_DIR) as server:
            month = server.manifest["month"]
            return _run(month, chaos, refresh_weather, refresh_311, build_config(month, chaos, server.env()), "sample")
    return _run(month, chaos, refresh_weather, refresh_311, build_config(month, chaos, None), "full")


def _run(month: str, chaos: str, refresh_weather: bool, refresh_311: bool, cfg: Config, mode: str) -> int:
    run_id = datetime.now().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
    log_name = month if mode == "full" else f"sample_{month}"
    log_path = cfg.log_dir / f"pipeline_{log_name}.log"
    logger = build_logger(cfg.log_dir, log_name, run_id, cfg.log_level)
    started = time.time()
    timings: dict = {}
    retries: dict = {}
    summary = {"run_id": run_id, "month": month, "mode": mode, "chaos": chaos,
               "started_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
               "thresholds": cfg.thresholds()}
    results: list[ck.CheckResult] = []

    def stage(name):
        timings[name] = round(time.time() - stage.t0, 2)
        stage.t0 = time.time()
        logger.info("Stage complete | %s | %.1fs", name, timings[name])
    stage.t0 = time.time()

    logger.info("Pipeline started | mode=%s month=%s chaos=%s output=%s", mode.upper(), month, chaos, cfg.partition_dir)
    if mode == "sample":
        logger.warning("SAMPLE MODE: deterministic 10,000-ride sample served locally - NOT the complete dataset")
    try:
        clean_stale_staging(cfg.output_dir, month, logger)

        # 1-3 EXTRACT + PRESERVE RAW + COMPLETENESS ---------------------------------
        hv_path, hv_manifest, c = retrieve_hvfhv(cfg, logger, retries); results += c
        zones, zone_manifest, c = retrieve_zones(cfg, logger, retries); results += c
        if any(r.status == ck.FAIL for r in c):
            raise GateBlocked("zone lookup failed validation")
        shapes, shapes_manifest, c = retrieve_zone_shapes(cfg, logger, retries, zones); results += c
        fault = FaultInjector([500, 429]) if chaos == "api_flaky" else None
        payload, w_manifest, c = retrieve_weather(cfg, logger, retries, refresh=refresh_weather or chaos == "api_flaky", fault=fault)
        results += c
        weather = None
        if payload is not None:
            weather, c = validate_weather_payload(payload, cfg)
            weather = apply_chaos_to_weather(weather, chaos, logger)
            if chaos == "weather_gap":
                weather, c = _recheck_weather_coverage(weather, payload, cfg)
            results += c
        rows311, m311, c = retrieve_311(cfg, logger, retries, refresh=refresh_311); results += c
        if rows311 is not None:
            results += validate_311(rows311, cfg)
        stage("extract_preserve_completeness")

        # Load only the columns the model needs; the raw file stays untouched.
        schema_names = pq.ParquetFile(hv_path).schema_arrow.names
        table = pq.read_table(hv_path, columns=[c for c in HVFHV_REQUIRED_COLUMNS if c in schema_names])
        table = apply_chaos_to_trips(table, chaos, logger)
        logger.info("Loaded trips | rows=%s columns=%s", f"{table.num_rows:,}", table.num_columns)
        col_check = check_required_columns(table.column_names, HVFHV_REQUIRED_COLUMNS, "hvfhv")
        results.append(col_check)
        if col_check.status == ck.FAIL:
            raise GateBlocked(col_check.detail)

        # 4-5 PROFILE / VALIDATE / FLAG ------------------------------------------------
        lc = compute_lifecycle(table)
        prof = profile(table, lc)
        trip_checks, decisions = validate_trips(table, lc, zones, cfg)
        results += trip_checks
        stage("profile_validate")
        g = ck.gate(results)
        if not g["publish"]:
            raise GateBlocked(f"critical validation failures: {g['blocking_failures']}")

        # 6-8 TRANSFORM / JOIN / MODEL ---------------------------------------------------
        journey, ctx = build_ride_journey(table, lc, zones, weather if g["publish_weather_analysis"] else None, cfg)
        del table
        logger.info("Built ride_journey | %s", ctx["stats"])
        complaints, friction = None, None
        if rows311 is not None and shapes is not None and g["publish_friction_kpi"]:
            complaints, mapping, c = complaints_table(rows311, shapes, zones, cfg.min_311_mapping_coverage)
            results += c
            pbz = pickups_by_zone(ctx)
            per_zone, per_borough = aggregate_friction(complaints, pbz, zones)
            friction = {"mapping": mapping, "per_zone": per_zone, "per_borough": per_borough, "pickups": pbz}
            logger.info("311 friction aggregated | mapped=%s zones_with_complaints=%s", mapping["mapped"],
                        sum(1 for v in per_zone.values() if v["complaints_all"]))
        stage("transform_model")

        # 9-10 KPIs + OUTPUT VALIDATION ---------------------------------------------------
        g = ck.gate(results)
        kpis, zone_rows, borough_p90 = compute_kpis(ctx, lc, zones, decisions, g, cfg, friction)
        support = supporting_analyses(ctx, lc, complaints, g, cfg, borough_p90)
        results += validate_kpis(kpis, zone_rows, ctx["stats"], friction)
        g = ck.gate(results)
        ck.log_results(results, logger)
        if not g["publish"]:
            raise GateBlocked(f"output validation failures: {g['blocking_failures']}")
        k5 = "kpi_5_passenger_311_complaints_per_100k_rides"
        if not g["publish_friction_kpi"] and kpis[k5].get("value"):
            kpis[k5] = {"value": None, "withheld": True, "reason": f"blocked by {g['friction_kpi_blockers']}",
                        "definition": kpis[k5]["definition"]}
        attach_validation_status(kpis, results)
        stage("kpis")

        # 11 PUBLISH -------------------------------------------------------------------------
        staging = new_staging_dir(cfg.output_dir, month, run_id)
        weather_meta = {"grid_lat": (weather or {}).get("grid_lat"), "grid_lon": (weather or {}).get("grid_lon"),
                        "join_rule": "floor(request_datetime, hour) == open_meteo.hourly.time (America/New_York)"}
        banner = ("SAMPLE MODE - deterministic 10,000-ride sample; NOT the complete dataset. Values are not representative."
                  if mode == "sample" else "FULL DATA - complete monthly HVFHV file")
        metrics_payload = {
            "month": month, "mode": mode, "data_scope": banner, "run_id": run_id,
            "published_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "kpis": kpis, "supporting_analyses": support, "weather": weather_meta,
            "sources": {"hvfhv_sha256": hv_manifest["sha256"], "zones_sha256": zone_manifest["sha256"],
                        "zone_shapes_sha256": (shapes_manifest or {}).get("sha256"),
                        "weather_sha256": w_manifest.get("sha256"),
                        "nyc311_snapshot": (m311 or {}).get("snapshot_dir")},
        }
        validation_payload = {"month": month, "mode": mode, "run_id": run_id, "gate": g, "thresholds": cfg.thresholds(),
                              "decisions": decisions, "checks": [r.to_dict() for r in results]}
        if chaos == "none":
            pq.write_table(journey, staging / "ride_journey.parquet", compression="zstd")
        pacsv.write_csv(journey.slice(0, 1000).cast(_plain_schema(journey.schema)), staging / "ride_journey_sample.csv")
        _write_zone_csv(zone_rows, friction, staging / "zone_breakdown.csv")
        if complaints is not None:
            _write_csv(complaints, staging / "nyc311_complaints_mapped.csv")
        write_json(staging / "metrics.json", metrics_payload)
        write_json(staging / "validation_report.json", validation_payload)
        write_json(staging / "profile.json", prof)
        (staging / "evidence_table.md").write_text(
            evidence_markdown(month, mode, kpis, support, [r.to_dict() for r in results], decisions, ctx["stats"], g,
                              weather_meta, friction["mapping"] if friction else None), encoding="utf-8")
        stage("write_outputs")
        summary.update(_summary_tail(started, timings, retries, ctx["stats"], g, "PUBLISHED",
                                     {"hvfhv": hv_manifest, "zones": zone_manifest, "zone_shapes": shapes_manifest,
                                      "weather": w_manifest, "nyc311": m311}))
        write_json(staging / "run_summary.json", summary)
        swap_in_partition(staging, cfg.partition_dir, logger)
        logger.info("Pipeline completed | decision=PUBLISHED mode=%s friction_kpi=%s weather_analysis=%s | %s", mode,
                    "published" if g["publish_friction_kpi"] else "withheld",
                    "published" if g["publish_weather_analysis"] else "withheld", cfg.partition_dir)
        print(f"\nPIPELINE SUCCESS ({banner}) - published {cfg.partition_dir}")
        for key, k in kpis.items():
            v = k.get("value")
            short = v if not isinstance(v, dict) else {kk: vv for kk, vv in v.items() if not isinstance(vv, (list, dict))}
            print(f"  {key}: {short}  [{k['validation_status']}]")
        return 0

    except GateBlocked as exc:
        ck.log_results(results, logger)
        logger.error("Pipeline stopped at validation gate | %s | NO OUTPUT PUBLISHED; previous partition untouched", exc)
        _write_failure(cfg, summary, started, timings, retries, results, "BLOCKED_BY_VALIDATION", str(exc))
        print(f"\nPIPELINE BLOCKED: {exc}\nNothing was published. See {log_path}")
        return 2
    except (RetrievalError, RetryableHTTPError, NonRetryableHTTPError) as exc:
        hint = ""
        if isinstance(exc, NonRetryableHTTPError) and exc.status in (403, 404):
            hint = " (TLC publishes monthly files with a lag of ~2 months; this month may not be published yet)"
        logger.error("Retrieval failed | %s%s | NO OUTPUT PUBLISHED", exc, hint)
        _write_failure(cfg, summary, started, timings, retries, results, "RETRIEVAL_FAILED", str(exc) + hint)
        print(f"\nPIPELINE FAILED (retrieval): {exc}{hint}")
        return 1
    except Exception as exc:
        logger.exception("Pipeline failed unexpectedly | %s | NO OUTPUT PUBLISHED", exc)
        _write_failure(cfg, summary, started, timings, retries, results, "FAILED", repr(exc))
        print(f"\nPIPELINE FAILED: {exc}")
        return 1


def _recheck_weather_coverage(weather, payload, cfg):
    """After chaos removes hours, re-run the coverage check on what remains."""
    fake = dict(payload)
    fake["hourly"] = {"time": [t.strftime("%Y-%m-%dT%H:%M") for t in weather["time"]],
                      **{v: weather[v] for v in cfg.weather_variables}}
    return validate_weather_payload(fake, cfg)


def _plain_schema(schema: pa.Schema) -> pa.Schema:
    return pa.schema([pa.field(f.name, f.type.value_type if pa.types.is_dictionary(f.type) else f.type) for f in schema])


def _write_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def _write_zone_csv(zone_rows, friction, path):
    """Zone grain: pickup reliability plus 311 counts (zero-filled). Zone-level 311 rates are
    supporting detail only - counts are too small to rank zones monthly."""
    rows = []
    for r in zone_rows:
        f = (friction or {}).get("per_zone", {}).get(r["pickup_zone_id"], {})
        rides = (friction or {}).get("pickups", {}).get(r["pickup_zone_id"])
        rows.append({**r,
                     "hvfhv_pickups_all": rides,
                     "nyc311_complaints_all": f.get("complaints_all") if friction else None,
                     "nyc311_complaints_passenger": f.get("complaints_passenger") if friction else None,
                     "nyc311_complaints_pickup_reliability": f.get("complaints_pickup_reliability") if friction else None,
                     "nyc311_passenger_rate_per_100k": rate_per_100k(f.get("complaints_passenger", 0), rides) if friction and rides else None,
                     "nyc311_low_count_flag": (f.get("complaints_passenger", 0) < 5) if friction else None})
    _write_csv(rows, path)


def _summary_tail(started, timings, retries, stats, g, decision, manifests):
    return {"finished_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "duration_seconds": round(time.time() - started, 1), "stage_seconds": timings,
            "http_retries": retries, "row_counts": stats, "gate": g, "decision": decision,
            "source_manifests": manifests}


def _write_failure(cfg, summary, started, timings, retries, results, decision, reason):
    """Failed runs are recorded under logs/, never under output/."""
    summary.update(_summary_tail(started, timings, retries, None, ck.gate(results), decision, None))
    summary["reason"] = reason
    summary["checks"] = [r.to_dict() for r in results]
    path = cfg.log_dir / "failed_runs" / f"{summary['mode']}_{cfg.month}_{summary['run_id']}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json(path, summary)


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="NYC Ride Operations Reliability Pipeline")
    p.add_argument("--month", help="Logical run month, YYYY-MM (e.g. 2026-07). Required unless --sample.")
    p.add_argument("--sample", action="store_true", help="Run offline on the deterministic sample in data/sample/")
    p.add_argument("--chaos", default="none", choices=CHAOS, help="Inject a controlled failure (demo/testing)")
    p.add_argument("--refresh-weather", action="store_true", help="Take a new weather snapshot even if one is preserved")
    p.add_argument("--refresh-311", action="store_true", help="Take a new 311 snapshot even if one is preserved")
    a = p.parse_args(argv)
    if not a.sample and not a.month:
        p.error("--month YYYY-MM is required (or use --sample)")
    if a.month and not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", a.month):
        p.error("--month must be YYYY-MM")
    return a


if __name__ == "__main__":
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except AttributeError:
            pass
    args = parse_args()
    sys.exit(run(args.month, args.chaos, args.refresh_weather, args.refresh_311, args.sample))
