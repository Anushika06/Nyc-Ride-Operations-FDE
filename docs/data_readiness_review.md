# Data readiness review (2026-07)

Evidence: `output/2026-07/validation_report.json`, `run_summary.json`, `logs/pipeline_2026-07.log`,
the chaos runs in the README, and `python -m unittest` (41 tests).

## Pipeline run

- [x] One command: `python run_pipeline.py --month 2026-07` (full) or `python run_pipeline.py --sample` (offline)
- [x] Safe to repeat: the partition is replaced atomically; 20,921,249 rows before and after a rerun; raw inputs are reused only after checksum verification
- [x] Raw inputs preserved byte-for-byte with sha256 manifests; weather and 311 snapshots are append-only
- [x] Output written only after validation: a critical FAIL exits with code 2 and leaves the previous partition untouched

## Validation (37 checks: 27 PASS · 9 WARN · 0 FAIL · 1 UNKNOWN)

| Area | Status | Evidence |
|---|---|---|
| Trip retrieval completeness | PASS | 511,176,663 bytes == Content-Length; footer readable; 31/31 days |
| Trip uniqueness | PASS | no ID in source; 0 duplicates on the 9-field natural key (exact) |
| Chronology | WARN | 1.18% pickup-before-request, flagged and excluded (limit 5%) |
| On-scene semantics | WARN | populated 100%, observed 93.4%, so it is used for KPI 3 only — and KPI 3 is therefore a lower bound (the excluded rides have no curb wait) |
| Scheduled-ride semantics | UNKNOWN | no flag in source; sensitivity: median 4.45 → 4.47 min |
| 311 retrieval | PASS | server count 1,373 == received 1,373 over 2 pages; 0 duplicate keys |
| 311 date coverage | PASS | 31/31 days, 0 records outside the month |
| 311 → zone mapping | PASS | 97.7% mapped; 99.93% borough agreement; unmapped kept and labelled. 237 of 248 passenger complaints reach the KPI 5 numerator, so that KPI is a lower bound by ~4.4% |
| Weather coverage | PASS | 768/768 hours |
| Output reconciliation | PASS | zone rides, borough complaints and borough ride denominators all reconcile |

## Reliability

| Capability | Status | Evidence |
|---|---|---|
| Bounded retries | PASS | `--chaos api_flaky`: 500 then 429 (Retry-After honoured), then success; max 4 attempts; tested |
| Truncated downloads | PASS | a connection dropped mid-body is retried, never published (tested) |
| Fail fast on permanent errors | PASS | HTTP 403/404 is not retried; the unpublished month 2026-08 gives a clear message |
| Scoped failure | PASS | 311 or weather failures withhold only their own output; trip failures block everything |
| Logging | PASS | stage timings, row counts, every check, retries, publish decision; failed runs → `logs/failed_runs/` |
| Idempotent rerun | PASS | full data, sample, and end-to-end tests. Every evidence file is rewritten byte-identically apart from `run_id`/timestamps; `ride_journey.parquet` is value-identical but not byte-identical (zstd is not bit-reproducible) |
| Offline reproducibility | PASS | `--sample` runs every stage against a local server with the committed 10k-ride sample |

## Gate decision

**READY** for monthly operational reporting of pickup reliability (KPIs 1–4) and the borough-level
customer-friction context (KPI 5), with the stated limitations.

**NOT READY** for prediction models, causal claims about zones or weather, or zone-level complaint rankings.
Those need unserved-request data, a scheduled-ride flag, an intervention log and more months of 311 data.

---

## Demo script (3–5 min): the judgement call

1. **The tempting claim:** "on-scene is 100% populated, so pickup wait = pickup − on-scene."
2. **Profile it** (notebook section 3): for Uber, 6.6% of on-scene times equal pickup *to the second*, and 1.8% come
   *before* the request. Populated is not the same as observed.
3. **And pickup − on-scene is not the rider's wait.** It is the driver at the curb.
4. **Decision:** the headline is **request-to-pickup time**, named exactly that. On-scene is used only for the stage
   split, on the 93.4% of rides where it is ordered. Nothing is invented.
5. **Result:** 80.5% of the wait happens *before the driver arrives*. The lever is supply positioning and dispatch.
   KPI 4 shows where: outer zones (Breezy Point P90 20.7 min vs Upper East Side 6.7).
6. **Second call, on 311:** 81% of FHV complaints come from non-passengers, and none has a ride ID. So passenger
   complaints are used only as a borough-level signal (Queens 1.59 per 100k rides, highest), never attached to rides.
7. **Dependability:** run `--chaos missing_column` (blocked, nothing published), then `--sample` (offline, same stages).
