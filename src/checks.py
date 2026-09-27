"""Validation result type and the publish gate.

Status semantics (same vocabulary as the FlashEats Class 6 gate):
  PASS    - expectation satisfied
  WARN    - known issue, quantified, does not invalidate the intended KPI
  FAIL    - output affected by this check must not be published
  UNKNOWN - the data cannot establish whether the expectation holds

Scope decides what a FAIL blocks:
  critical - the whole publication (trip data / zone reference problems)
  weather  - only the weather supporting analysis (weather is context, not a KPI)
  friction - only the 311 customer-friction KPI (311 is context, not the KPI itself)
  info     - never blocks; recorded for the reader
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict

PASS, WARN, FAIL, UNKNOWN = "PASS", "WARN", "FAIL", "UNKNOWN"
CRITICAL, WEATHER, FRICTION, INFO = "critical", "weather", "friction", "info"


@dataclass
class CheckResult:
    check: str
    status: str
    detail: str
    scope: str = CRITICAL
    category: str = "technical"  # technical | completeness | uniqueness | chronology | location | plausibility | semantic | output
    evidence: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


def gate(results: list[CheckResult]) -> dict:
    critical_fail = [r.check for r in results if r.status == FAIL and r.scope == CRITICAL]
    weather_fail = [r.check for r in results if r.status == FAIL and r.scope == WEATHER]
    friction_fail = [r.check for r in results if r.status == FAIL and r.scope == FRICTION]
    counts = {s: sum(r.status == s for r in results) for s in (PASS, WARN, FAIL, UNKNOWN)}
    return {
        "publish": not critical_fail,
        "publish_weather_analysis": not critical_fail and not weather_fail,
        "publish_friction_kpi": not critical_fail and not friction_fail,
        "blocking_failures": critical_fail,
        "weather_analysis_blockers": weather_fail,
        "friction_kpi_blockers": friction_fail,
        "status_counts": counts,
    }


def log_results(results: list[CheckResult], logger) -> None:
    for r in results:
        level = logger.warning if r.status in (WARN, UNKNOWN) else logger.error if r.status == FAIL else logger.info
        level("Validation | check=%s status=%s scope=%s | %s", r.check, r.status, r.scope, r.detail)
