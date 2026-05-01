import argparse
import csv
import json
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from seo_audit.cli import run_audit
from seo_audit.models import LighthouseRecord, PerformanceRecord
from seo_audit.performance import (
    ProviderRetryConfig,
    collect_crux,
    collect_performance,
    fetch_pagespeed,
    fetch_psi,
)


def test_pagespeed_failure_reason(monkeypatch) -> None:
    monkeypatch.setattr(
        "seo_audit.performance.http_get_json",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("network down")),
    )
    row, error = fetch_pagespeed("r1", "https://example.com", "mobile")
    assert row is None
    assert error is not None
    assert "request failed" in error


def _psi_success_payload() -> dict:
    return {
        "lighthouseResult": {
            "categories": {
                "performance": {"score": 0.5},
                "accessibility": {"score": 0.8},
                "best-practices": {"score": 0.9},
                "seo": {"score": 0.7},
            },
            "audits": {
                "largest-contentful-paint": {"numericValue": 1200},
                "cumulative-layout-shift": {"numericValue": 0.03},
                "interaction-to-next-paint": {"numericValue": 250},
                "server-response-time": {"numericValue": 180},
            },
        }
    }


def test_pagespeed_skipped_without_key(monkeypatch) -> None:
    monkeypatch.delenv("PSI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.setattr("seo_audit.performance._get_json", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("network call not expected")))

    row, error = fetch_pagespeed("r1", "https://example.com", "mobile")
    assert row is None
    assert error is not None
    assert error.startswith("skipped_missing_key:")


def test_crux_skipped_without_key_no_network(monkeypatch) -> None:
    monkeypatch.delenv("CRUX_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)

    called = {"count": 0}

    def fake_post(*args, **kwargs):
        called["count"] += 1
        return 200, {"record": {}}, {}

    monkeypatch.setattr("seo_audit.performance._post_json", fake_post)
    rows, errors = collect_crux("r1", ["https://example.com"])

    assert errors == []
    assert len(rows) == 1
    assert rows[0].status == "skipped_missing_key"
    assert called["count"] == 0


def test_pagespeed_retry_after_respected(monkeypatch) -> None:
    responses = [
        (429, {"error": {"message": "rate limit"}}, {"retry-after": "1"}),
        (200, _psi_success_payload(), {}),
    ]

    def fake_get(*args, **kwargs):
        return responses.pop(0)

    sleeps: list[float] = []
    monkeypatch.setattr("seo_audit.performance._get_json", fake_get)
    monkeypatch.setattr("seo_audit.performance.time.sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr("seo_audit.performance.random.uniform", lambda *_args, **_kwargs: 0.0)

    row, error = fetch_psi(
        "r1",
        "https://example.com",
        "mobile",
        api_key="dummy-key",
        retry_config=ProviderRetryConfig(max_retries=2, base_backoff_seconds=0.5, max_backoff_seconds=2.0, respect_retry_after=True, max_total_wait_seconds=5.0),
    )

    assert error is None
    assert row is not None
    assert sleeps
    assert sleeps[0] == 1.0


def test_pagespeed_retry_exhaustion_failed_http(monkeypatch) -> None:
    monkeypatch.setattr("seo_audit.performance._get_json", lambda *args, **kwargs: (503, {"error": {"message": "unavailable"}}, {}))
    monkeypatch.setattr("seo_audit.performance.time.sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("seo_audit.performance.random.uniform", lambda *_args, **_kwargs: 0.0)

    row, error = fetch_psi(
        "r1",
        "https://example.com",
        "mobile",
        api_key="dummy-key",
        retry_config=ProviderRetryConfig(max_retries=1, base_backoff_seconds=0.1, max_backoff_seconds=0.1, respect_retry_after=True, max_total_wait_seconds=1.0),
    )

    assert row is None
    assert error is not None
    assert error.startswith("failed_http:")
    assert "retries=1" in error


def test_pagespeed_error_redacts_secret(monkeypatch) -> None:
    secret = "my-super-secret-key"
    monkeypatch.setattr("seo_audit.performance._get_json", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError(f"key={secret}")))
    monkeypatch.setattr("seo_audit.performance.time.sleep", lambda *_args, **_kwargs: None)

    row, error = fetch_psi(
        "r1",
        "https://example.com",
        "mobile",
        api_key=secret,
        retry_config=ProviderRetryConfig(max_retries=0),
    )

    assert row is None
    assert error is not None
    assert secret not in error
    assert "[redacted]" in error


def test_pagespeed_default_does_not_store_payload(monkeypatch) -> None:
    monkeypatch.setattr("seo_audit.performance._get_json", lambda *args, **kwargs: (200, _psi_success_payload(), {}))

    row, error = fetch_psi("r1", "https://example.com", "mobile", api_key="dummy-key")
    assert error is None
    assert row is not None
    assert row.payload_json == "{}"


def test_pagespeed_opt_in_stores_valid_payload_json(monkeypatch) -> None:
    payload = _psi_success_payload()
    monkeypatch.setattr("seo_audit.performance._get_json", lambda *args, **kwargs: (200, payload, {}))

    row, error = fetch_psi("r1", "https://example.com", "mobile", api_key="dummy-key", store_payloads=True)
    assert error is None
    assert row is not None
    assert json.loads(row.payload_json)["lighthouseResult"]["categories"]["performance"]["score"] == 0.5


def test_pagespeed_success_rows(monkeypatch) -> None:
    payload = _psi_success_payload()
    monkeypatch.setenv("PSI_API_KEY", "dummy-key")
    monkeypatch.setattr("seo_audit.performance._get_json", lambda *args, **kwargs: (200, payload, {}))
    rows, errors = collect_performance("r1", ["https://example.com"], store_payloads=False)
    assert len(rows) == 2
    assert errors == []
    assert rows[0].performance_score == 50


def test_collect_performance_uses_bounded_workers(monkeypatch) -> None:
    active = 0
    max_active = 0
    lock = threading.Lock()

    def fake_fetch_internal(
        run_id: str,
        url: str,
        strategy: str,
        timeout: float = 20.0,
        api_key: str | None = None,
        store_payloads: bool = False,
        retry_config: ProviderRetryConfig | None = None,
        rate_limiter=None,
    ):
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.03)
        with lock:
            active -= 1
        return (
            PerformanceRecord(
                run_id=run_id,
                url=url,
                strategy=strategy,
                source="psi",
                performance_score=80,
                payload_json="{}",
            ),
            None,
            0,
            0.0,
        )

    monkeypatch.setattr("seo_audit.performance._fetch_psi_internal", fake_fetch_internal)

    urls = [f"https://example.com/p{i}" for i in range(6)]
    rows, errors = collect_performance("r1", urls, workers=4)

    assert errors == []
    assert len(rows) == len(urls) * 2
    assert max_active <= 4
    assert max_active >= 2


def test_fetch_psi_rate_limiter_acquires_per_attempt(monkeypatch) -> None:
    responses = [
        (503, {"error": {"message": "retry"}}, {}),
        (200, _psi_success_payload(), {}),
    ]

    def fake_get(*args, **kwargs):
        return responses.pop(0)

    class StubLimiter:
        def __init__(self) -> None:
            self.calls = 0

        def acquire(self) -> None:
            self.calls += 1

    limiter = StubLimiter()

    monkeypatch.setattr("seo_audit.performance._get_json", fake_get)
    monkeypatch.setattr("seo_audit.performance.time.sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("seo_audit.performance.random.uniform", lambda *_args, **_kwargs: 0.0)

    row, error = fetch_psi(
        "r1",
        "https://example.com",
        "mobile",
        api_key="dummy-key",
        retry_config=ProviderRetryConfig(
            max_retries=1,
            base_backoff_seconds=0.1,
            max_backoff_seconds=0.2,
            respect_retry_after=True,
            max_total_wait_seconds=1.0,
        ),
        rate_limiter=limiter,
    )

    assert error is None
    assert row is not None
    assert limiter.calls == 2


def test_pagespeed_no_data_when_performance_score_missing(monkeypatch) -> None:
    payload = _psi_success_payload()
    payload["lighthouseResult"]["categories"]["performance"]["score"] = None
    monkeypatch.setattr("seo_audit.performance._get_json", lambda *args, **kwargs: (200, payload, {}))

    row, error = fetch_psi("r1", "https://example.com", "mobile", api_key="dummy-key")
    assert row is None
    assert error is not None
    assert error.startswith("no_data:")
    assert "missing performance category score" in error


class _PerfHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        server_address = self.server.server_address
        host = str(server_address[0]) if isinstance(server_address, tuple) and len(server_address) >= 1 else "127.0.0.1"
        port = int(server_address[1]) if isinstance(server_address, tuple) and len(server_address) >= 2 else 80
        base = f"http://{host}:{port}"
        if self.path == "/robots.txt":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(f"User-agent: *\nSitemap: {base}/sitemap.xml\n".encode())
            return
        if self.path == "/sitemap.xml":
            self.send_response(200)
            self.send_header("Content-Type", "application/xml")
            self.end_headers()
            self.wfile.write(
                (
                    "<urlset xmlns='http://www.sitemaps.org/schemas/sitemap/0.9'>"
                    f"<url><loc>{base}/</loc></url></urlset>"
                ).encode()
            )
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(b"<html><head><title>Home</title></head><body><h1>Home</h1></body></html>")

    def log_message(self, format, *args):  # noqa: A003
        return


def test_cli_persists_performance_rows_when_provider_returns_data(tmp_path: Path, monkeypatch) -> None:
    server = HTTPServer(("127.0.0.1", 0), _PerfHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    domain = f"http://127.0.0.1:{server.server_port}"
    out = tmp_path / "perf"

    monkeypatch.setattr(
        "seo_audit.cli.collect_performance",
        lambda run_id, urls, timeout=20, **_kwargs: (
            [
                PerformanceRecord(
                    run_id=run_id,
                    url=urls[0],
                    strategy="mobile",
                    performance_score=90,
                    accessibility_score=90,
                    best_practices_score=90,
                    seo_score=90,
                    payload_json="{}",
                )
            ],
            [],
        ),
    )

    args = argparse.Namespace(
        domain=domain,
        output=str(out),
        max_pages=5,
        max_render_pages=2,
        render_mode="none",
        timeout=2.0,
        user_agent="TestAgent",
        ignore_robots=False,
        save_html=False,
        verbose=False,
    )
    run_audit(args)

    con = sqlite3.connect(out / "audit.sqlite")
    run_id = con.execute("SELECT run_id FROM runs ORDER BY rowid DESC LIMIT 1").fetchone()[0]
    assert con.execute("SELECT COUNT(*) FROM performance_metrics WHERE run_id = ?", (run_id,)).fetchone()[0] == 1

    with (out / "performance.csv").open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
        assert len(rows) == 1

    con.close()
    server.shutdown()
    thread.join(timeout=2)


def test_cli_persists_lighthouse_rows_and_budget_issue(tmp_path: Path, monkeypatch) -> None:
    server = HTTPServer(("127.0.0.1", 0), _PerfHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    domain = f"http://127.0.0.1:{server.server_port}"
    out = tmp_path / "lh"

    monkeypatch.setattr("seo_audit.cli.collect_performance", lambda *args, **kwargs: ([], []))
    monkeypatch.setattr("seo_audit.cli.collect_crux", lambda *args, **kwargs: ([], []))
    monkeypatch.setattr(
        "seo_audit.cli.collect_lighthouse",
        lambda run_id, urls, **kwargs: (
            [
                LighthouseRecord(
                    run_id=run_id,
                    url=urls[0],
                    form_factor="desktop",
                    status="success",
                    performance_score=58,
                    seo_score=62,
                    budget_pass=0,
                    budget_failures_json='["performance<70 (58)"]',
                )
            ],
            [],
            {"attempts": 1, "success": 1, "failed": 0, "skipped_missing_dependency": 0, "budget_failed": 1},
        ),
    )

    args = argparse.Namespace(
        domain=domain,
        output=str(out),
        max_pages=5,
        max_render_pages=2,
        render_mode="none",
        timeout=2.0,
        user_agent="TestAgent",
        ignore_robots=False,
        save_html=False,
        verbose=False,
        lighthouse_enabled=True,
        lighthouse_targets=1,
        lighthouse_timeout_seconds=30.0,
        lighthouse_form_factor="desktop",
        lighthouse_config_path="",
        lighthouse_budget_performance_min=70,
        lighthouse_budget_seo_min=70,
    )
    run_audit(args)

    con = sqlite3.connect(out / "audit.sqlite")
    run_id = con.execute("SELECT run_id FROM runs ORDER BY rowid DESC LIMIT 1").fetchone()[0]
    assert con.execute("SELECT COUNT(*) FROM lighthouse_metrics WHERE run_id = ?", (run_id,)).fetchone()[0] == 1
    assert con.execute(
        "SELECT COUNT(*) FROM issues WHERE run_id = ? AND issue_code = 'LIGHTHOUSE_BUDGET_FAIL'",
        (run_id,),
    ).fetchone()[0] == 1

    con.close()
    server.shutdown()
    thread.join(timeout=2)
