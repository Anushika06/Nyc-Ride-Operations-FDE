"""Minimal HTTP client on the standard library (urllib) with bounded retries.

Retry transient failures (HTTP 429, 5xx, network errors) with exponential backoff,
honouring Retry-After when the server sends it. Fail fast on everything else
(404/403/400 ...): retrying cannot fix a missing file or a bad request.
"""
from __future__ import annotations

import http.client
import time
import urllib.error
import urllib.request
from typing import Callable, Optional

RETRYABLE_STATUS = {429, 500, 502, 503, 504}
USER_AGENT = "nyc-ride-ops-pipeline/1.0 (FDE assignment)"


class RetryableHTTPError(RuntimeError):
    """Transient failure that persisted after all retries."""


class NonRetryableHTTPError(RuntimeError):
    """Failure that retrying will not fix (e.g. 403/404, bad parameters)."""

    def __init__(self, message: str, status: Optional[int] = None, body: str = ""):
        super().__init__(message)
        self.status = status
        self.body = body


class FaultInjector:
    """Chaos hook: raise scripted HTTP errors on the first N attempts.

    Used by `--chaos api_flaky` to demonstrate retry behaviour on demand,
    the same way the FlashEats mock API fails deterministically.
    """

    def __init__(self, statuses: list[int]):
        self.remaining = list(statuses)

    def maybe_fail(self, url: str) -> None:
        if self.remaining:
            status = self.remaining.pop(0)
            headers = {"Retry-After": "1"} if status == 429 else {}
            raise urllib.error.HTTPError(url, status, f"injected {status}", headers, None)


def _retry_wait(attempt: int, base: float, cap: float, retry_after: Optional[str]) -> float:
    if retry_after:
        try:
            return min(float(retry_after), cap)
        except ValueError:
            pass
    return min(base * (2 ** (attempt - 1)), cap)


def request_with_retries(
    url: str,
    handle_response: Callable,
    *,
    logger,
    source: str,
    timeout: float,
    max_retries: int,
    base_seconds: float,
    max_wait_seconds: float,
    fault: Optional[FaultInjector] = None,
    sleep: Callable[[float], None] = time.sleep,
    stats: Optional[dict] = None,
):
    """Open `url` and pass the live response to `handle_response`.

    `handle_response` does the reading (so large files can be streamed). If it
    raises a network error mid-stream, the whole request is retried.
    """
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            if fault is not None:
                fault.maybe_fail(url)
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return handle_response(resp)
        except urllib.error.HTTPError as exc:
            body = ""
            try:
                body = exc.read().decode("utf-8", "replace")[:500] if exc.fp else ""
            except Exception:
                pass
            finally:
                exc.close()
            if exc.code not in RETRYABLE_STATUS:
                raise NonRetryableHTTPError(
                    f"{source}: HTTP {exc.code} is not retryable (url={url})",
                    status=exc.code,
                    body=body,
                ) from exc
            wait = _retry_wait(attempt, base_seconds, max_wait_seconds, exc.headers.get("Retry-After") if exc.headers else None)
            last_error = f"HTTP {exc.code}"
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError, http.client.HTTPException) as exc:
            # HTTPException covers IncompleteRead: a connection that dies mid-body is transient.
            wait = _retry_wait(attempt, base_seconds, max_wait_seconds, None)
            last_error = f"{type(exc).__name__}: {exc}"

        if stats is not None:
            stats[source] = stats.get(source, 0) + 1
        logger.warning(
            "Retryable failure | source=%s error=%s attempt=%s/%s wait=%.1fs",
            source, last_error, attempt, max_retries, wait,
        )
        if attempt < max_retries:
            sleep(wait)

    raise RetryableHTTPError(
        f"{source}: still failing after {max_retries} attempts (last error: {last_error})"
    )
