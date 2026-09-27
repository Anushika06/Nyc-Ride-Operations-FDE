"""The five published KPIs, their definitions, supporting analyses, and output checks.

A KPI definition is a business decision, not merely an expression, so each
definition (formula, numerator/denominator, grain, population, source, exclusions,
interpretation, limitation, and the validation checks it depends on) lives next to
the computation and is written into metrics.json.

Selection (after profiling, see notebooks/01_profile_hvfhv.ipynb and README):
  kept     median + P90 request->pickup (outcome), driver-arrival share (where the
           delay accumulates), P90 by zone (where to act), 311 passenger-complaint
           rate (customer friction).
  demoted  precipitation (hour-matched difference ~0.1 min: no operational signal)
           and company split -> supporting analyses, not KPIs.

Percentiles use linear interpolation (numpy default) over exact values.
"""
from __future__ import annotations

import numpy as np

from .checks import CheckResult, PASS, WARN, FAIL, UNKNOWN, CRITICAL, WEATHER, FRICTION
from .nyc311 import rate_per_100k
from .transform import PRECIP_BANDS

HEADLINE_CHECKS = ["hvfhv.download_complete", "hvfhv.month_coverage", "hvfhv.timestamp_coverage",
                   "hvfhv.trip_uniqueness", "hvfhv.chronology_request_pickup_dropoff",
                   "hvfhv.request_semantics_scheduled_rides", "output.headline_consistency"]

DEFINITIONS = {
    "kpi_1_median_request_to_pickup_min": {
        "name": "Median request-to-pickup time",
        "formula": "median over rides of (pickup_datetime - request_datetime)",
        "numerator": "n/a (percentile)", "denominator": "n/a (percentile)",
        "grain": "one completed HVFHV ride; published per month",
        "population": "in_kpi_population: request, pickup, dropoff present; pickup >= request; dropoff > pickup",
        "source": "HVFHV trip records: request_datetime, pickup_datetime, dropoff_datetime",
        "exclusions": "rides flagged pickup_before_request, dropoff_not_after_pickup or missing_timestamp (flagged, not deleted)",
        "unit": "minutes",
        "interpretation": "Typical time a rider waits between asking for a ride and being picked up.",
        "limitation": "Request-to-pickup time, NOT driver wait time. For unflagged pre-scheduled rides request_datetime "
                      "may be the booked slot (UNKNOWN; bounded by the sensitivity analysis). Completed trips only.",
        "validation_checks": HEADLINE_CHECKS,
    },
    "kpi_2_p90_request_to_pickup_min": {
        "name": "P90 request-to-pickup time",
        "formula": "90th percentile over rides of (pickup_datetime - request_datetime)",
        "numerator": "n/a (percentile)", "denominator": "n/a (percentile)",
        "grain": "one completed HVFHV ride; published per month",
        "population": "same as KPI 1",
        "source": "HVFHV trip records",
        "exclusions": "same as KPI 1",
        "unit": "minutes",
        "interpretation": "Reliability: 1 in 10 riders waits at least this long. The tail a median hides.",
        "limitation": "Same as KPI 1; the tail is the part most exposed to unflagged scheduled rides.",
        "validation_checks": HEADLINE_CHECKS,
    },
    "kpi_3_driver_arrival_share_of_wait_pct": {
        "name": "Driver-arrival share of request-to-pickup time",
        "formula": "100 * sum(on_scene - request) / sum(pickup - request)",
        "numerator": "total minutes from request until the driver is on scene",
        "denominator": "total minutes from request until pickup",
        "grain": "all qualifying rides in the month (ratio of sums)",
        "population": "in_stage_population: KPI population AND request <= on_scene < pickup (on-scene independently observed)",
        "source": "HVFHV trip records: request_datetime, on_scene_datetime, pickup_datetime",
        "exclusions": "rides whose on_scene equals pickup to the second, precedes the request, or follows pickup",
        "unit": "percent",
        "interpretation": "How much of the rider's wait is spent waiting for the driver to arrive (dispatch + travel) versus "
                          "the driver waiting at the curb. Tells operations which stage to act on.",
        "limitation": "Computed on the subset of rides with an observed on-scene event (the subset is published). "
                      "That subset excludes rides whose on_scene equals pickup to the second - exactly the rides with "
                      "no curb wait, whose arrival share would be 100% - so the published share is a LOWER BOUND on the "
                      "driver-arrival share of the wait. Withheld automatically if the subset falls below the configured share.",
        "validation_checks": ["hvfhv.on_scene_semantics", "hvfhv.chronology_request_pickup_dropoff"],
    },
    "kpi_4_p90_request_to_pickup_by_zone": {
        "name": "P90 request-to-pickup time by pickup zone",
        "formula": "90th percentile of (pickup - request) per PULocationID, labelled via the taxi zone lookup",
        "numerator": "n/a (percentile)", "denominator": "n/a (percentile)",
        "grain": "one pickup zone per month",
        "population": "in_zone_population (KPI population with an attributable NYC pickup zone); zones with fewer than "
                      "min_rides_per_zone rides are listed but not ranked",
        "source": "HVFHV PULocationID + request/pickup timestamps; Taxi Zone Lookup",
        "exclusions": "placeholder zone IDs 264/265 (Unknown / Outside of NYC); IDs absent from the lookup",
        "unit": "minutes",
        "interpretation": "Where the poor-tail pickup experience is concentrated, so supply positioning can be targeted.",
        "limitation": "Zone differences mix supply, demand, geography and trip mix: descriptive, not causal.",
        "validation_checks": ["hvfhv.pickup_location_integrity", "output.zone_reconciliation"] + HEADLINE_CHECKS[:5],
    },
    "kpi_5_passenger_311_complaints_per_100k_rides": {
        "name": "Passenger FHV complaints to 311 per 100,000 HVFHV rides",
        "formula": "100000 * passenger_complaints / hvfhv_pickups, citywide and per borough",
        "numerator": "311 'For Hire Vehicle Complaint' records with descriptor 'Driver Complaint - Passenger' or "
                     "'Car Service Company Complaint', created in the month, mapped to a taxi zone",
        "denominator": "HVFHV rides picked up in an attributable NYC zone in the same month (same geography). NOTE: this "
                       "denominator is all zone-attributable pickups, including rides flagged by the lifecycle rules; the "
                       "p90_request_to_pickup_min shown next to it in the borough table is computed on the KPI population "
                       "(in_zone_population), a slightly smaller set. The two columns answer different questions on purpose.",
        "grain": "borough per month (headline) - zone-level counts are published as supporting detail only",
        "population": "all mapped passenger complaints; all zone-attributable HVFHV pickups",
        "source": "NYC 311 Service Requests (Socrata erm2-nwe9), TLC taxi zone shapefile, HVFHV trips",
        "exclusions": "non-passenger complaints (street safety, 81% of FHV complaints); complaints without coordinates "
                      "or outside all zones (counted in validation, not guessed). Unmapped PASSENGER complaints are "
                      "therefore missing from the numerator: the count and the resulting understatement are published as "
                      "passenger_complaints_unmapped / rate_understatement_pct on this KPI",
        "unit": "complaints per 100,000 rides",
        "interpretation": "A contextual customer-friction signal: where riders escalate problems to the city. Read next to "
                          "KPI 4 to see whether slow-pickup areas also generate more rider complaints.",
        "limitation": "NOT a ride-level interaction: 311 has no ride ID and is never linked to a ride. Covers all FHV "
                      "types (not only HVFHV), reflects the propensity to call 311, and is sparse - monthly zone-level "
                      "rates are too noisy to rank, hence borough grain.",
        "validation_checks": ["nyc311.retrieval_complete", "nyc311.unique_key", "nyc311.date_coverage",
                              "nyc311.zone_mapping_coverage", "nyc311.zone_mapping_borough_agreement",
                              "output.friction_reconciliation"],
    },
}

_SEVERITY = {PASS: 0, WARN: 1, UNKNOWN: 2, FAIL: 3}


def _q(values: np.ndarray, qs) -> list:
    if len(values) == 0:
        return [None for _ in qs]
    return [round(float(v), 2) for v in np.quantile(values, qs)]


def grouped_quantiles(keys: np.ndarray, values: np.ndarray, qs) -> dict:
    """Exact per-group quantiles via one stable sort."""
    order = np.argsort(keys, kind="stable")
    k, v = keys[order], values[order]
    uniq, starts, counts = np.unique(k, return_index=True, return_counts=True)
    return {int(g): (int(c), _q(v[s:s + c], qs)) for g, s, c in zip(uniq, starts, counts)}


def pickups_by_zone(ctx: dict) -> dict:
    """Denominator for KPI 5: every HVFHV ride with an attributable NYC pickup zone."""
    ids = ctx["pickup_ids"][~ctx["placeholder_pu"]]
    counts = np.bincount(ids)
    return {int(z): int(c) for z, c in enumerate(counts) if c}


def compute_kpis(ctx: dict, lc, zones: dict, decisions: dict, gate: dict, cfg, friction: dict | None):
    r2p = lc.request_to_pickup_min
    pop = lc.in_kpi_population
    med, p90 = _q(r2p[pop], [0.5, 0.9])
    kpis = {
        "kpi_1_median_request_to_pickup_min": {"value": med, "population_rides": int(pop.sum())},
        "kpi_2_p90_request_to_pickup_min": {"value": p90, "population_rides": int(pop.sum())},
    }

    # KPI 3 --------------------------------------------------------------------
    if decisions["on_scene"]["used_for_stage_decomposition"]:
        sp = lc.stage_population
        r2o, o2p = lc.request_to_on_scene_min[sp], lc.on_scene_to_pickup_min[sp]
        kpis["kpi_3_driver_arrival_share_of_wait_pct"] = {
            "value": round(float(r2o.sum() / r2p[sp].sum() * 100), 1),
            "population_rides": int(sp.sum()),
            "population_share_of_kpi_population_pct": round(float(sp.sum() / pop.sum() * 100), 2),
            "supporting": {"median_request_to_on_scene_min": _q(r2o, [0.5])[0],
                           "median_on_scene_to_pickup_min": _q(o2p, [0.5])[0],
                           "p90_request_to_on_scene_min": _q(r2o, [0.9])[0],
                           "p90_on_scene_to_pickup_min": _q(o2p, [0.9])[0]},
        }
    else:
        kpis["kpi_3_driver_arrival_share_of_wait_pct"] = {
            "value": None, "withheld": True,
            "reason": "on-scene is not independently observed for enough rides (see hvfhv.on_scene_semantics)"}

    # KPI 4 --------------------------------------------------------------------
    zp = pop & ~ctx["placeholder_pu"]
    groups = grouped_quantiles(ctx["pickup_ids"][zp], r2p[zp], [0.5, 0.9])
    zone_rows = []
    for zid, (count, (zmed, zp90)) in groups.items():
        z = zones["zones"].get(zid, {"zone": None, "borough": None})
        zone_rows.append({"pickup_zone_id": zid, "pickup_zone": z["zone"], "pickup_borough": z["borough"],
                          "rides_in_kpi_population": count, "median_request_to_pickup_min": zmed,
                          "p90_request_to_pickup_min": zp90, "ranked": count >= cfg.min_rides_per_zone})
    ranked = sorted((r for r in zone_rows if r["ranked"]), key=lambda r: r["p90_request_to_pickup_min"])
    slim = lambda r: {k: r[k] for k in ("pickup_zone", "pickup_borough", "rides_in_kpi_population",
                                        "median_request_to_pickup_min", "p90_request_to_pickup_min")}
    kpis["kpi_4_p90_request_to_pickup_by_zone"] = {
        "value": {
            "zones_ranked": len(ranked),
            "zones_below_min_rides": sum(not r["ranked"] for r in zone_rows),
            "best_zone_p90_min": ranked[0]["p90_request_to_pickup_min"] if ranked else None,
            "worst_zone_p90_min": ranked[-1]["p90_request_to_pickup_min"] if ranked else None,
            "worst_5": [slim(r) for r in ranked[-5:][::-1]],
            "best_5": [slim(r) for r in ranked[:5]],
            "zones_with_p90_over_1_5x_citywide": sum(r["p90_request_to_pickup_min"] > 1.5 * p90 for r in ranked) if p90 else None,
        },
        "population_rides": int(zp.sum()),
        "detail_file": "zone_breakdown.csv",
    }

    # KPI 5 --------------------------------------------------------------------
    borough_p90 = _borough_quantiles(ctx, r2p, zp, zones, cfg)
    if friction and gate["publish_friction_kpi"]:
        per_b = friction["per_borough"]
        city_c = sum(v["complaints_passenger"] for v in per_b.values())
        city_r = sum(v["hvfhv_pickups"] for v in per_b.values())
        by_b = {}
        for b, v in sorted(per_b.items(), key=lambda kv: str(kv[0])):
            if v["hvfhv_pickups"] < cfg.min_rides_per_zone:
                continue
            by_b[b] = {"passenger_complaints": v["complaints_passenger"], "hvfhv_pickups": v["hvfhv_pickups"],
                       "rate_per_100k": rate_per_100k(v["complaints_passenger"], v["hvfhv_pickups"]),
                       "p90_request_to_pickup_min": borough_p90.get(b, {}).get("p90")}
        mapping = friction.get("mapping", {})
        p_total, p_unmapped = mapping.get("passenger_total", city_c), mapping.get("passenger_unmapped", 0)
        kpis["kpi_5_passenger_311_complaints_per_100k_rides"] = {
            "value": {"citywide_rate_per_100k": rate_per_100k(city_c, city_r),
                      "citywide_passenger_complaints": city_c, "citywide_hvfhv_pickups": city_r,
                      # The numerator counts only complaints that mapped to a zone. The rest are
                      # never guessed into a borough, so the rate is understated by this much.
                      "passenger_complaints_unmapped": p_unmapped,
                      "rate_understatement_pct": round(p_unmapped / p_total * 100, 1) if p_total else None,
                      "denominator_note": "all zone-attributable pickups (includes rides flagged by the lifecycle rules); "
                                          "the P90 column in the borough table uses the KPI population instead",
                      "by_borough": by_b},
            "population_rides": city_r,
        }
    else:
        kpis["kpi_5_passenger_311_complaints_per_100k_rides"] = {
            "value": None, "withheld": True,
            "reason": "311 retrieval, validation or zone mapping failed (see validation_report.json)"}

    for key, d in DEFINITIONS.items():
        kpis[key]["definition"] = d
    return kpis, zone_rows, borough_p90


def _borough_quantiles(ctx, r2p, zp, zones, cfg) -> dict:
    blabels = sorted({z["borough"] for z in zones["zones"].values()})
    id_to_b = np.full(max(zones["zones"]) + 1, -1, dtype=np.int16)
    for zid, z in zones["zones"].items():
        id_to_b[zid] = blabels.index(z["borough"])
    q = grouped_quantiles(id_to_b[ctx["pickup_ids"][zp]], r2p[zp], [0.5, 0.9])
    return {blabels[b]: {"rides": c, "median": m[0], "p90": m[1]} for b, (c, m) in q.items() if c >= cfg.min_rides_per_zone}


def supporting_analyses(ctx: dict, lc, table, gate: dict, cfg, borough_p90: dict) -> dict:
    """Context that informed the KPI choice but is not itself a KPI."""
    r2p, pop = lc.request_to_pickup_min, lc.in_kpi_population
    out = {}
    lic = ctx["license_codes"]
    out["by_company"] = {}
    for i, code in enumerate(ctx["license_labels"]):
        m = pop & (lic == i)
        med, p90 = _q(r2p[m], [0.5, 0.9])
        out["by_company"][code] = {"company": {"HV0003": "Uber", "HV0005": "Lyft"}.get(code, "unknown"),
                                   "rides": int(m.sum()), "median": med, "p90": p90}
    out["by_borough"] = borough_p90

    w = ctx["weather"]
    wp = pop & w["weather_matched"]
    if gate["publish_weather_analysis"] and wp.any():
        band, hours = w["precip_band"], ctx["request_hour"]
        by_band = {}
        for b, label in enumerate(PRECIP_BANDS[:3]):
            m = wp & (band == b)
            bm, bp = _q(r2p[m], [0.5, 0.9])
            by_band[label] = {"rides": int(m.sum()), "median": bm, "p90": bp}
        dry = wp & (band == 0)
        dry_h = grouped_quantiles(hours[dry], r2p[dry], [0.5])
        matched = {}
        for b, label in ((1, "light_rain"), (2, "moderate_or_heavy_rain")):
            m = wp & (band == b)
            num = den = 0.0
            for h, (c, (wmed,)) in grouped_quantiles(hours[m], r2p[m], [0.5]).items():
                if h in dry_h and c >= cfg.min_rides_per_zone:
                    num += c * (wmed - dry_h[h][1][0])
                    den += c
            matched[label] = round(num / den, 2) if den else None
        out["precipitation_association"] = {
            "by_band": by_band, "hour_matched_median_diff_vs_dry_min": matched,
            "conclusion": "Demoted from KPI: hour-matched differences are ~0.1 min, no operational signal this month. "
                          "Association only; one grid point represents all of NYC.",
        }
    else:
        out["precipitation_association"] = {"withheld": True, "reason": "weather validation failed or not joined"}

    alt = pop & ~lc.request_on_whole_minute
    base, excl = _q(r2p[pop], [0.5, 0.9]), _q(r2p[alt], [0.5, 0.9])
    out["sensitivity_scheduled_rides"] = {
        "question": "Are unflagged pre-scheduled rides distorting request-to-pickup time?",
        "method": "recompute KPI 1/2 excluding rides whose request timestamp is on an exact minute",
        "kpi_population": {"rides": int(pop.sum()), "median": base[0], "p90": base[1]},
        "excluding_whole_minute_requests": {"rides": int(alt.sum()), "median": excl[0], "p90": excl[1]},
        "note": "Over-excludes (1 in 60 ordinary requests land on :00 by chance); it bounds the effect.",
    }
    if table is not None:
        cats = {}
        for t in table:
            cats[t["category"]] = cats.get(t["category"], 0) + 1
        out["nyc311_composition"] = {
            "complaints_by_category": cats,
            "pickup_reliability_reasons": sum(bool(t["pickup_reliability_reason"]) for t in table),
            "note": "Non-passenger complaints (street safety) are excluded from KPI 5. Pickup-reliability reasons "
                    "(refused pick-up, denied request, no-show) are too few to publish as their own metric.",
        }
    return out


def attach_validation_status(kpis: dict, results: list[CheckResult]) -> None:
    by_name = {r.check: r.status for r in results}
    for key, k in kpis.items():
        statuses = {c: by_name[c] for c in k["definition"]["validation_checks"] if c in by_name}
        worst = max(statuses.values(), key=lambda s: _SEVERITY[s]) if statuses else UNKNOWN
        k["validation_status"] = "WITHHELD" if k.get("withheld") else worst
        k["validation_checks_status"] = statuses


def validate_kpis(kpis: dict, zone_rows: list[dict], stats: dict, friction: dict | None) -> list[CheckResult]:
    """VALIDATE OUTPUTS: the numbers must reconcile with their populations before publishing."""
    out = []
    k1, k2 = kpis["kpi_1_median_request_to_pickup_min"]["value"], kpis["kpi_2_p90_request_to_pickup_min"]["value"]
    ok = k1 is not None and k2 is not None and 0 <= k1 <= k2
    out.append(CheckResult("output.headline_consistency", PASS if ok else FAIL,
                           f"median={k1} p90={k2}; require 0 <= median <= p90", category="output"))
    pop = stats["kpi_population"]
    out.append(CheckResult("output.population_bounds", PASS if 0 < pop <= stats["rows"] else FAIL,
                           f"kpi_population={pop:,} of rows={stats['rows']:,}", category="output"))
    k3 = kpis["kpi_3_driver_arrival_share_of_wait_pct"]
    if k3.get("value") is not None:
        out.append(CheckResult("output.stage_share_bounds", PASS if 0 <= k3["value"] <= 100 else FAIL,
                               f"driver-arrival share={k3['value']}% must lie in [0, 100]", category="output"))
    zsum = sum(r["rides_in_kpi_population"] for r in zone_rows)
    out.append(CheckResult("output.zone_reconciliation", PASS if zsum == stats["zone_population"] else FAIL,
                           f"sum of zone rides={zsum:,} vs zone population={stats['zone_population']:,}", category="output"))
    if friction is not None:
        mapped = friction["mapping"]["mapped"]
        csum = sum(v["complaints_all"] for v in friction["per_borough"].values())
        rsum = sum(v["hvfhv_pickups"] for v in friction["per_borough"].values())
        ok = csum == mapped and rsum == stats["zone_attributable_rides"]
        out.append(CheckResult("output.friction_reconciliation", PASS if ok else FAIL,
                               f"borough complaint total={csum} vs mapped complaints={mapped}; borough ride denominator="
                               f"{rsum:,} vs zone-attributable rides={stats['zone_attributable_rides']:,}",
                               scope=FRICTION, category="output"))
    published = [k for k, v in kpis.items() if not v.get("withheld")]
    out.append(CheckResult("output.metric_count", PASS if 3 <= len(published) <= 5 else FAIL,
                           f"{len(published)} KPIs published (expected 3-5): {published}", category="output"))
    return out
