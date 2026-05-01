from __future__ import annotations

import json
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

from seo_audit.http_utils import http_get
from seo_audit.models import CruxRecord, PerformanceRecord


PSI_ENDPOINT = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
CRUX_ENDPOINT = "https://chromeuxreport.googleapis.com/v1/records:queryRecord"
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
PROVIDER_KEY_NAMES = {"GOOGLE_API_KEY", "PSI_API_KEY", "CRUX_API_KEY"}

_LOCAL_KEY_ENV_LOADED = False


@dataclass(slots=True)
class ProviderRetryConfig:
    max_retries: int = 2
    base_backoff_seconds: float = 0.5
    max_backoff_seconds: float = 6.0
    respect_retry_after: bool = True
    max_total_wait_seconds: float = 20.0


class ProviderRateLimiter(Protocol):
    def acquire(self) -> None: ...


class TokenBucketRateLimiter:
    """Thread-safe token-bucket limiter for smoothing provider bursts."""

    def __init__(self, rate_per_second: float, capacity: int) -> None:
        self.rate_per_second = max(0.001, float(rate_per_second))
        self.capacity = max(1, int(capacity))
        self._tokens = float(self.capacity)
        self._updated_at = time.monotonic()
        self._condition = threading.Condition()

    def _refill(self, now: float) -> None:
        elapsed = now - self._updated_at
        if elapsed <= 0:
            return
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate_per_second)
        self._updated_at = now

    def acquire(self) -> None:
        with self._condition:
            while True:
                now = time.monotonic()
                self._refill(now)
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait_seconds = (1.0 - self._tokens) / self.rate_per_second
                self._condition.wait(timeout=max(0.001, wait_seconds))


class ProviderRequestError(RuntimeError):
    def __init__(self, message: str, retries_used: int, total_wait: float) -> None:
        super().__init__(message)
        self.retries_used = retries_used
        self.total_wait = total_wait


def resolve_google_keys() -> tuple[str | None, str | None]:
    _load_local_key_env_once()
    shared = os.getenv("GOOGLE_API_KEY")
    psi_key = os.getenv("PSI_API_KEY") or shared
    crux_key = os.getenv("CRUX_API_KEY") or shared
    return psi_key, crux_key


def _candidate_key_env_files() -> list[Path]:
    explicit = (os.getenv("SEO_AUDIT_ENV_FILE") or "").strip()
    if explicit:
        return [Path(explicit).expanduser()]

    repo_root = Path(__file__).resolve().parents[1]
    cwd = Path.cwd()
    candidates: list[Path] = []
    for base in (cwd, repo_root):
        for name in (".seo_audit.env", ".env.local", ".env"):
            path = (base / name).resolve()
            if path not in candidates:
                candidates.append(path)
    return candidates


def _parse_env_line(line: str) -> tuple[str, str] | None:
    text = line.strip()
    if not text or text.startswith("#"):
        return None
    if text.startswith("export "):
        text = text[len("export ") :].strip()
    if "=" not in text:
        return None
    key, value = text.split("=", 1)
    key = key.strip()
    value = value.strip()
    if not key:
        return None
    if value and len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1]
    return key, value


def _load_local_key_env_once() -> None:
    global _LOCAL_KEY_ENV_LOADED
    if _LOCAL_KEY_ENV_LOADED:
        return

    for path in _candidate_key_env_files():
        if not path.exists() or not path.is_file():
            continue
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except Exception:
            continue
        for raw_line in lines:
            parsed = _parse_env_line(raw_line)
            if not parsed:
                continue
            key, value = parsed
            if key in PROVIDER_KEY_NAMES and value and key not in os.environ:
                os.environ[key] = value

    _LOCAL_KEY_ENV_LOADED = True


def _safe_payload_json(payload: dict, store_payloads: bool) -> str:
    if not store_payloads:
        return "{}"
    return json.dumps(payload, sort_keys=True)


def _error_message(payload: dict, default: str) -> str:
    err = payload.get("error")
    if isinstance(err, dict):
        message = err.get("message")
        if isinstance(message, str) and message.strip():
            return message.strip()
    if isinstance(err, str) and err.strip():
        return err.strip()
    return default


def _redact(text: str, secrets: list[str]) -> str:
    redacted = text
    for secret in secrets:
        if secret:
            redacted = redacted.replace(secret, "[redacted]")
    return redacted


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None

    try:
        seconds = float(raw)
        return seconds if seconds >= 0 else None
    except ValueError:
        pass

    try:
        dt = parsedate_to_datetime(raw)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = (dt - datetime.now(timezone.utc)).total_seconds()
    return delta if delta > 0 else 0.0


def _compute_wait_seconds(
    retry_index: int,
    retry_after_header: str | None,
    retry_config: ProviderRetryConfig,
    total_wait: float,
) -> float | None:
    wait_seconds: float
    retry_after_seconds = _parse_retry_after(retry_after_header) if retry_config.respect_retry_after else None
    if retry_after_seconds is not None:
        wait_seconds = retry_after_seconds
    else:
        backoff = min(
            retry_config.max_backoff_seconds,
            retry_config.base_backoff_seconds * (2**retry_index),
        )
        jitter = random.uniform(0.0, min(0.25, backoff * 0.25))
        wait_seconds = min(retry_config.max_backoff_seconds, backoff + jitter)

    remaining = retry_config.max_total_wait_seconds - total_wait
    if remaining <= 0:
        return None
    wait_seconds = min(wait_seconds, remaining)
    if wait_seconds <= 0:
        return None
    return wait_seconds


def _request_json_with_retry(
    request_func,
    retry_config: ProviderRetryConfig,
    secrets: list[str] | None = None,
    rate_limiter: ProviderRateLimiter | None = None,
) -> tuple[int, dict, dict[str, str], int, float]:
    retries_used = 0
    total_wait = 0.0
    secrets = secrets or []

    while True:
        try:
            if rate_limiter is not None:
                rate_limiter.acquire()
            status_code, payload, headers = request_func()
        except Exception as exc:
            if retries_used >= retry_config.max_retries:
                raise ProviderRequestError(_redact(str(exc), secrets), retries_used, total_wait) from exc

            wait_seconds = _compute_wait_seconds(retries_used, None, retry_config, total_wait)
            if wait_seconds is None:
                raise ProviderRequestError(_redact(str(exc), secrets), retries_used, total_wait) from exc

            time.sleep(wait_seconds)
            retries_used += 1
            total_wait += wait_seconds
            continue

        if status_code not in RETRYABLE_STATUS_CODES or retries_used >= retry_config.max_retries:
            return status_code, payload, headers, retries_used, total_wait

        wait_seconds = _compute_wait_seconds(retries_used, headers.get("retry-after"), retry_config, total_wait)
        if wait_seconds is None:
            return status_code, payload, headers, retries_used, total_wait

        time.sleep(wait_seconds)
        retries_used += 1
        total_wait += wait_seconds


def http_get_json(url: str, timeout: float, params: dict | None = None) -> tuple[int, dict, dict[str, str]]:
    resp = http_get(url, timeout=timeout, params=params)
    raw_text = resp.text.strip()
    payload: dict
    if raw_text:
        try:
            payload = json.loads(raw_text)
        except Exception:
            payload = {"error": {"message": "invalid JSON response"}}
    else:
        payload = {}
    return resp.status_code, payload, resp.headers


def _get_json(url: str, timeout: float, params: dict | None = None) -> tuple[int, dict, dict[str, str]]:
    return http_get_json(url, timeout=timeout, params=params)


def _extract_metric(payload: dict, key: str) -> float | None:
    val = payload.get("lighthouseResult", {}).get("audits", {}).get(key, {}).get("numericValue")
    return float(val) if val is not None else None


def _ensure_provider_telemetry(telemetry: dict[str, float | int] | None) -> dict[str, float | int]:
    if telemetry is None:
        return {}
    telemetry.setdefault("attempts", 0)
    telemetry.setdefault("http_attempts", 0)
    telemetry.setdefault("success", 0)
    telemetry.setdefault("no_data", 0)
    telemetry.setdefault("failed_http", 0)
    telemetry.setdefault("skipped_missing_key", 0)
    telemetry.setdefault("retries", 0)
    telemetry.setdefault("wait_seconds", 0.0)
    telemetry.setdefault("timeouts", 0)
    return telemetry


def _track_provider_outcome(
    telemetry: dict[str, float | int] | None,
    *,
    status: str,
    retries_used: int,
    total_wait: float,
    error_message: str = "",
) -> None:
    if telemetry is None:
        return
    t = _ensure_provider_telemetry(telemetry)
    t["attempts"] = int(t["attempts"]) + 1
    t["retries"] = int(t["retries"]) + retries_used
    t["wait_seconds"] = float(t["wait_seconds"]) + total_wait
    if status != "skipped_missing_key":
        t["http_attempts"] = int(t["http_attempts"]) + 1 + retries_used
    if status in {"success", "no_data", "failed_http", "skipped_missing_key"}:
        t[status] = int(t[status]) + 1
    if "timed out" in (error_message or "").lower():
        t["timeouts"] = int(t["timeouts"]) + 1


def _fetch_psi_internal(
    run_id: str,
    url: str,
    strategy: str,
    timeout: float = 20.0,
    api_key: str | None = None,
    store_payloads: bool = False,
    retry_config: ProviderRetryConfig | None = None,
    rate_limiter: ProviderRateLimiter | None = None,
) -> tuple[PerformanceRecord | None, str | None, int, float]:
    retry_config = retry_config or ProviderRetryConfig()
    params = {
        "url": url,
        "strategy": strategy,
        "category": ["performance", "accessibility", "best-practices", "seo"],
    }
    if api_key is None:
        api_key, _ = resolve_google_keys()
    if not api_key:
        return None, f"skipped_missing_key: {url} [{strategy}] missing PSI_API_KEY/GOOGLE_API_KEY", 0, 0.0

    params["key"] = api_key

    try:
        status_code, payload, _headers, retries_used, total_wait = _request_json_with_retry(
            lambda: _get_json(PSI_ENDPOINT, timeout=timeout, params=params),
            retry_config=retry_config,
            secrets=[api_key],
            rate_limiter=rate_limiter,
        )
    except ProviderRequestError as exc:
        return (
            None,
            f"failed_http: {url} [{strategy}] request failed: {exc} (retries={exc.retries_used}, waited={exc.total_wait:.2f}s)",
            exc.retries_used,
            exc.total_wait,
        )
    except Exception as exc:
        return None, f"failed_http: {url} [{strategy}] request failed: {exc}", 0, 0.0

    if status_code >= 400:
        message = _error_message(payload, f"HTTP {status_code}")
        return (
            None,
            f"failed_http: {url} [{strategy}] {message} (retries={retries_used}, waited={total_wait:.2f}s)",
            retries_used,
            total_wait,
        )

    categories = payload.get("lighthouseResult", {}).get("categories", {})

    def cat(name: str) -> int | None:
        score = categories.get(name, {}).get("score")
        return int(score * 100) if isinstance(score, (int, float)) else None

    if not categories:
        return (
            None,
            f"no_data: {url} [{strategy}] missing lighthouse categories (retries={retries_used}, waited={total_wait:.2f}s)",
            retries_used,
            total_wait,
        )

    performance_score = cat("performance")
    if performance_score is None:
        runtime_error = payload.get("lighthouseResult", {}).get("runtimeError")
        reason = "missing performance category score"
        if isinstance(runtime_error, dict):
            message = str(runtime_error.get("message") or "").strip()
            code = str(runtime_error.get("code") or "").strip()
            if message and code and code not in message:
                reason = f"{message} ({code})"
            elif message:
                reason = message
            elif code:
                reason = code

        return (
            None,
            f"no_data: {url} [{strategy}] {reason} (retries={retries_used}, waited={total_wait:.2f}s)",
            retries_used,
            total_wait,
        )

    rec = PerformanceRecord(
        run_id=run_id,
        url=url,
        strategy=strategy,
        source="psi",
        performance_score=performance_score,
        accessibility_score=cat("accessibility"),
        best_practices_score=cat("best-practices"),
        seo_score=cat("seo"),
        lcp=_extract_metric(payload, "largest-contentful-paint"),
        cls=_extract_metric(payload, "cumulative-layout-shift"),
        inp=_extract_metric(payload, "interaction-to-next-paint"),
        ttfb=_extract_metric(payload, "server-response-time"),
        field_data_available=int(bool(payload.get("loadingExperience") or payload.get("originLoadingExperience"))),
        payload_json=_safe_payload_json(payload, store_payloads=store_payloads),
    )
    return rec, None, retries_used, total_wait


def fetch_psi(
    run_id: str,
    url: str,
    strategy: str,
    timeout: float = 20.0,
    api_key: str | None = None,
    store_payloads: bool = False,
    retry_config: ProviderRetryConfig | None = None,
    rate_limiter: ProviderRateLimiter | None = None,
) -> tuple[PerformanceRecord | None, str | None]:
    row, error, _retries_used, _total_wait = _fetch_psi_internal(
        run_id=run_id,
        url=url,
        strategy=strategy,
        timeout=timeout,
        api_key=api_key,
        store_payloads=store_payloads,
        retry_config=retry_config,
        rate_limiter=rate_limiter,
    )
    return row, error


def fetch_pagespeed(run_id: str, url: str, strategy: str, timeout: float = 20.0) -> tuple[PerformanceRecord | None, str | None]:
    return fetch_psi(run_id, url, strategy, timeout=timeout)


def collect_psi(
    run_id: str,
    urls: list[str],
    timeout: float = 20.0,
    store_payloads: bool = False,
    retry_config: ProviderRetryConfig | None = None,
    telemetry: dict[str, float | int] | None = None,
    workers: int = 4,
    rate_limiter: ProviderRateLimiter | None = None,
) -> tuple[list[PerformanceRecord], list[str]]:
    psi_key, _ = resolve_google_keys()
    retry_config = retry_config or ProviderRetryConfig()
    out: list[PerformanceRecord] = []
    errors: list[str] = []
    telemetry = _ensure_provider_telemetry(telemetry)
    tasks: list[tuple[int, str, str]] = []
    task_index = 0
    for u in urls:
        for strategy in ("mobile", "desktop"):
            tasks.append((task_index, u, strategy))
            task_index += 1

    if not tasks:
        return out, errors

    max_workers = max(1, min(int(workers), len(tasks)))
    results: dict[int, tuple[str, str, PerformanceRecord | None, str | None, int, float]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _fetch_psi_internal,
                run_id,
                u,
                strategy,
                timeout,
                psi_key,
                store_payloads,
                retry_config,
                rate_limiter,
            ): (idx, u, strategy)
            for idx, u, strategy in tasks
        }

        for future in as_completed(futures):
            idx, u, strategy = futures[future]
            try:
                row, error, retries_used, total_wait = future.result()
            except Exception as exc:
                row = None
                error = f"failed_http: {u} [{strategy}] worker failure: {exc}"
                retries_used = 0
                total_wait = 0.0
            results[idx] = (u, strategy, row, error, retries_used, total_wait)

    for idx, _u, _strategy in tasks:
        u, strategy, row, error, retries_used, total_wait = results[idx]
        if row:
            out.append(row)
            _track_provider_outcome(
                telemetry,
                status="success",
                retries_used=retries_used,
                total_wait=total_wait,
            )
            if retries_used > 0:
                errors.append(f"retry_info: {u} [{strategy}] retries={retries_used} waited={total_wait:.2f}s")
        if error:
            if error.startswith("skipped_missing_key:"):
                _track_provider_outcome(
                    telemetry,
                    status="skipped_missing_key",
                    retries_used=retries_used,
                    total_wait=total_wait,
                    error_message=error,
                )
            elif error.startswith("no_data:"):
                _track_provider_outcome(
                    telemetry,
                    status="no_data",
                    retries_used=retries_used,
                    total_wait=total_wait,
                    error_message=error,
                )
            elif error.startswith("failed_http:"):
                _track_provider_outcome(
                    telemetry,
                    status="failed_http",
                    retries_used=retries_used,
                    total_wait=total_wait,
                    error_message=error,
                )
            errors.append(error)
    return out, errors


def collect_performance(
    run_id: str,
    urls: list[str],
    timeout: float = 20.0,
    store_payloads: bool = False,
    retry_config: ProviderRetryConfig | None = None,
    telemetry: dict[str, float | int] | None = None,
    workers: int = 4,
    rate_limiter: ProviderRateLimiter | None = None,
) -> tuple[list[PerformanceRecord], list[str]]:
    return collect_psi(
        run_id,
        urls,
        timeout=timeout,
        store_payloads=store_payloads,
        retry_config=retry_config,
        telemetry=telemetry,
        workers=workers,
        rate_limiter=rate_limiter,
    )


def _crux_percentile(payload: dict, metric: str) -> float | None:
    val = payload.get("record", {}).get("metrics", {}).get(metric, {}).get("percentiles", {}).get("p75")
    return float(val) if isinstance(val, (int, float)) else None


def _crux_origin(url: str) -> str | None:
    parsed = urlsplit(url)
    if not parsed.scheme or not parsed.hostname:
        return None
    return f"{parsed.scheme}://{parsed.hostname}"


def _post_json(url: str, payload: dict, timeout: float, params: dict | None = None) -> tuple[int, dict, dict[str, str]]:
    if params:
        query = urlencode(params, doseq=True)
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}{query}"

    body = json.dumps(payload).encode("utf-8")
    request = Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urlopen(request, timeout=timeout) as resp:  # nosec B310
            raw = resp.read().decode("utf-8", errors="replace")
            return (
                getattr(resp, "status", 200),
                json.loads(raw) if raw.strip() else {},
                {k.lower(): v for k, v in resp.headers.items()},
            )
    except HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
        headers = {k.lower(): v for k, v in exc.headers.items()} if exc.headers else {}
        if raw.strip():
            try:
                return exc.code, json.loads(raw), headers
            except Exception:
                return exc.code, {"error": {"message": raw}}, headers
        return exc.code, {}, headers
    except URLError as exc:
        raise RuntimeError(str(exc)) from exc


def _collect_crux_scope(
    run_id: str,
    url: str,
    scope: str,
    timeout: float,
    api_key: str | None,
    store_payloads: bool = False,
    retry_config: ProviderRetryConfig | None = None,
    origin_fallback_used: int = 0,
    rate_limiter: ProviderRateLimiter | None = None,
) -> tuple[CruxRecord, int, float]:
    retry_config = retry_config or ProviderRetryConfig()
    if not api_key:
        return (
            CruxRecord(
                run_id=run_id,
                url=url,
                query_scope=scope,
                status="skipped_missing_key",
                origin_fallback_used=origin_fallback_used,
                error_message="missing CRUX_API_KEY/GOOGLE_API_KEY",
            ),
            0,
            0.0,
        )

    if scope == "url":
        query_body = {"url": url}
    else:
        origin = _crux_origin(url)
        if not origin:
            return (
                CruxRecord(
                    run_id=run_id,
                    url=url,
                    query_scope=scope,
                    status="failed_http",
                    origin_fallback_used=origin_fallback_used,
                    error_message="could not derive origin from URL",
                ),
                0,
                0.0,
            )
        query_body = {"origin": origin}

    try:
        status_code, payload, _headers, retries_used, total_wait = _request_json_with_retry(
            lambda: _post_json(CRUX_ENDPOINT, query_body, timeout=timeout, params={"key": api_key}),
            retry_config=retry_config,
            secrets=[api_key],
            rate_limiter=rate_limiter,
        )
    except ProviderRequestError as exc:
        return (
            CruxRecord(
                run_id=run_id,
                url=url,
                query_scope=scope,
                status="failed_http",
                origin_fallback_used=origin_fallback_used,
                error_message=(
                    f"request failed: {exc} "
                    f"(retries={exc.retries_used}, waited={exc.total_wait:.2f}s)"
                ),
            ),
            exc.retries_used,
            exc.total_wait,
        )
    except Exception as exc:
        return (
            CruxRecord(
                run_id=run_id,
                url=url,
                query_scope=scope,
                status="failed_http",
                origin_fallback_used=origin_fallback_used,
                error_message=f"request failed: {exc}",
            ),
            0,
            0.0,
        )

    if status_code == 200 and payload.get("record"):
        return (
            CruxRecord(
                run_id=run_id,
                url=url,
                query_scope=scope,
                status="success",
                origin_fallback_used=origin_fallback_used,
                lcp_p75=_crux_percentile(payload, "largest_contentful_paint"),
                cls_p75=_crux_percentile(payload, "cumulative_layout_shift"),
                inp_p75=_crux_percentile(payload, "interaction_to_next_paint"),
                fcp_p75=_crux_percentile(payload, "first_contentful_paint"),
                ttfb_p75=_crux_percentile(payload, "experimental_time_to_first_byte"),
                payload_json=_safe_payload_json(payload, store_payloads=store_payloads),
                error_message=(f"retries={retries_used} waited={total_wait:.2f}s" if retries_used > 0 else ""),
            ),
            retries_used,
            total_wait,
        )

    if status_code == 404 or (status_code == 200 and not payload.get("record")):
        return (
            CruxRecord(
                run_id=run_id,
                url=url,
                query_scope=scope,
                status="no_data",
                origin_fallback_used=origin_fallback_used,
                payload_json=_safe_payload_json(payload, store_payloads=store_payloads),
                error_message=f"no CrUX record (retries={retries_used}, waited={total_wait:.2f}s)",
            ),
            retries_used,
            total_wait,
        )

    message = _error_message(payload, f"HTTP {status_code}")
    return (
        CruxRecord(
            run_id=run_id,
            url=url,
            query_scope=scope,
            status="failed_http",
            origin_fallback_used=origin_fallback_used,
            payload_json=_safe_payload_json(payload, store_payloads=store_payloads),
            error_message=f"{message} (retries={retries_used}, waited={total_wait:.2f}s)",
        ),
        retries_used,
        total_wait,
    )


def collect_crux(
    run_id: str,
    urls: list[str],
    timeout: float = 20.0,
    origin_fallback: bool = True,
    store_payloads: bool = False,
    retry_config: ProviderRetryConfig | None = None,
    telemetry: dict[str, float | int] | None = None,
    rate_limiter: ProviderRateLimiter | None = None,
) -> tuple[list[CruxRecord], list[str]]:
    _, crux_key = resolve_google_keys()
    retry_config = retry_config or ProviderRetryConfig()
    telemetry = _ensure_provider_telemetry(telemetry)
    rows: list[CruxRecord] = []
    errors: list[str] = []
    for url in urls:
        url_row, retries_used, total_wait = _collect_crux_scope(
            run_id,
            url,
            "url",
            timeout,
            crux_key,
            store_payloads=store_payloads,
            retry_config=retry_config,
            rate_limiter=rate_limiter,
        )
        if retries_used > 0:
            errors.append(f"retry_info: {url} [url] retries={retries_used} waited={total_wait:.2f}s")
        _track_provider_outcome(
            telemetry,
            status=url_row.status,
            retries_used=retries_used,
            total_wait=total_wait,
            error_message=url_row.error_message,
        )
        if url_row.status == "no_data" and origin_fallback:
            origin_row, origin_retries, origin_wait = _collect_crux_scope(
                run_id,
                url,
                "origin",
                timeout,
                crux_key,
                store_payloads=store_payloads,
                retry_config=retry_config,
                origin_fallback_used=1,
                rate_limiter=rate_limiter,
            )
            rows.append(origin_row)
            if origin_retries > 0:
                errors.append(f"retry_info: {url} [origin] retries={origin_retries} waited={origin_wait:.2f}s")
            _track_provider_outcome(
                telemetry,
                status=origin_row.status,
                retries_used=origin_retries,
                total_wait=origin_wait,
                error_message=origin_row.error_message,
            )
            if origin_row.status == "failed_http":
                errors.append(f"{url} [origin] {origin_row.error_message}")
            continue

        rows.append(url_row)
        if url_row.status == "failed_http":
            errors.append(f"{url} [url] {url_row.error_message}")
    return rows, errors
