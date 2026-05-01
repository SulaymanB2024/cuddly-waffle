import argparse
import csv
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from seo_audit.cli import run_audit, select_performance_targets
from seo_audit.models import CruxRecord, PerformanceRecord
from seo_audit.performance import collect_crux, resolve_google_keys
from seo_audit.reporting import build_markdown_report
from seo_audit.storage import Storage


def _server_base_url(server: object) -> str:
    address = getattr(server, "server_address", None)
    host = str(address[0]) if isinstance(address, tuple) and len(address) >= 1 else "127.0.0.1"
    port = int(address[1]) if isinstance(address, tuple) and len(address) >= 2 else 80
    return f"http://{host}:{port}"


class _ProviderHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        base = _server_base_url(self.server)
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
            self.wfile.write(f"<urlset xmlns='http://www.sitemaps.org/schemas/sitemap/0.9'><url><loc>{base}/</loc></url></urlset>".encode())
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(b"<html><head><title>Home</title></head><body><h1>Home</h1><a href='/a'>A</a></body></html>")

    def log_message(self, format, *args):  # noqa: A003
        return


def test_resolve_google_keys_precedence(monkeypatch) -> None:
    monkeypatch.setenv("GOOGLE_API_KEY", "shared")
    monkeypatch.setenv("PSI_API_KEY", "psi")
    monkeypatch.setenv("CRUX_API_KEY", "crux")
    psi_key, crux_key = resolve_google_keys()
    assert psi_key == "psi"
    assert crux_key == "crux"

    monkeypatch.delenv("PSI_API_KEY")
    monkeypatch.delenv("CRUX_API_KEY")
    psi_key, crux_key = resolve_google_keys()
    assert psi_key == "shared"
    assert crux_key == "shared"


def test_resolve_google_keys_loads_local_env_file(monkeypatch, tmp_path: Path) -> None:
    env_file = tmp_path / ".seo_audit.env"
    env_file.write_text(
        "GOOGLE_API_KEY=from-file-shared\n"
        "PSI_API_KEY=from-file-psi\n"
        "CRUX_API_KEY=from-file-crux\n",
        encoding="utf-8",
    )

    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.delenv("PSI_API_KEY", raising=False)
    monkeypatch.delenv("CRUX_API_KEY", raising=False)
    monkeypatch.setenv("SEO_AUDIT_ENV_FILE", str(env_file))
    monkeypatch.setattr("seo_audit.performance._LOCAL_KEY_ENV_LOADED", False)

    psi_key, crux_key = resolve_google_keys()
    assert psi_key == "from-file-psi"
    assert crux_key == "from-file-crux"


def test_resolve_google_keys_env_overrides_local_env_file(monkeypatch, tmp_path: Path) -> None:
    env_file = tmp_path / ".seo_audit.env"
    env_file.write_text(
        "GOOGLE_API_KEY=file-shared\n"
        "PSI_API_KEY=file-psi\n"
        "CRUX_API_KEY=file-crux\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("SEO_AUDIT_ENV_FILE", str(env_file))
    monkeypatch.setenv("GOOGLE_API_KEY", "env-shared")
    monkeypatch.setenv("PSI_API_KEY", "env-psi")
    monkeypatch.setenv("CRUX_API_KEY", "env-crux")
    monkeypatch.setattr("seo_audit.performance._LOCAL_KEY_ENV_LOADED", False)

    psi_key, crux_key = resolve_google_keys()
    assert psi_key == "env-psi"
    assert crux_key == "env-crux"


def test_collect_crux_url_success(monkeypatch) -> None:
    payload = {
        "record": {
            "metrics": {
                "largest_contentful_paint": {"percentiles": {"p75": 1234}},
                "cumulative_layout_shift": {"percentiles": {"p75": 2}},
                "interaction_to_next_paint": {"percentiles": {"p75": 180}},
                "first_contentful_paint": {"percentiles": {"p75": 1000}},
                "experimental_time_to_first_byte": {"percentiles": {"p75": 220}},
            }
        }
    }
    monkeypatch.setenv("CRUX_API_KEY", "dummy")
    monkeypatch.setattr("seo_audit.performance._post_json", lambda *args, **kwargs: (200, payload, {}))

    rows, errors = collect_crux("r1", ["https://example.com/page"])
    assert errors == []
    assert len(rows) == 1
    assert rows[0].status == "success"
    assert rows[0].query_scope == "url"
    assert rows[0].lcp_p75 == 1234.0


def test_collect_crux_origin_fallback(monkeypatch) -> None:
    calls: list[dict] = []

    def fake_post(_url, payload, timeout, params=None):
        calls.append(payload)
        if "url" in payload:
            return 404, {"error": {"message": "Not found"}}, {}
        return 200, {
            "record": {
                "metrics": {
                    "largest_contentful_paint": {"percentiles": {"p75": 800}},
                    "cumulative_layout_shift": {"percentiles": {"p75": 1}},
                    "interaction_to_next_paint": {"percentiles": {"p75": 120}},
                }
            }
        }, {}

    monkeypatch.setenv("CRUX_API_KEY", "dummy")
    monkeypatch.setattr("seo_audit.performance._post_json", fake_post)

    rows, errors = collect_crux("r1", ["https://example.com/path"])
    assert errors == []
    assert len(rows) == 1
    assert rows[0].status == "success"
    assert rows[0].query_scope == "origin"
    assert rows[0].origin_fallback_used == 1
    assert calls[0]["url"] == "https://example.com/path"
    assert calls[1]["origin"] == "https://example.com"


def test_collect_crux_skipped_without_key(monkeypatch) -> None:
    monkeypatch.delenv("CRUX_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)

    rows, errors = collect_crux("r1", ["https://example.com"])
    assert errors == []
    assert len(rows) == 1
    assert rows[0].status == "skipped_missing_key"


def test_select_performance_targets_filters_system_and_non_html() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/sitemap.xml",
            "status_code": 200,
            "content_type": "text/xml",
            "fetch_error": "",
            "page_type": "other",
            "crawl_depth": 0,
            "internal_links_out": 0,
            "word_count": 0,
        },
        {
            "normalized_url": "https://example.com/",
            "status_code": 200,
            "content_type": "text/html",
            "fetch_error": "",
            "page_type": "homepage",
            "crawl_depth": 0,
            "internal_links_out": 10,
            "word_count": 500,
        },
        {
            "normalized_url": "https://example.com/service",
            "status_code": 200,
            "content_type": "text/html",
            "fetch_error": "",
            "page_type": "service",
            "crawl_depth": 1,
            "internal_links_out": 5,
            "word_count": 300,
        },
        {
            "normalized_url": "https://example.com/image.jpg",
            "status_code": 200,
            "content_type": "image/jpeg",
            "fetch_error": "",
            "page_type": "other",
            "crawl_depth": 1,
            "internal_links_out": 0,
            "word_count": 0,
        },
        {
            "normalized_url": "https://example.com/missing",
            "status_code": 404,
            "content_type": "text/html",
            "fetch_error": "",
            "page_type": "other",
            "crawl_depth": 1,
            "internal_links_out": 0,
            "word_count": 10,
        },
    ]

    targets = select_performance_targets(pages, limit=3)
    assert targets == ["https://example.com/", "https://example.com/service"]


def test_select_performance_targets_prefers_stable_candidates_after_homepage() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/",
            "status_code": 200,
            "content_type": "text/html",
            "fetch_error": "",
            "page_type": "homepage",
            "crawl_depth": 0,
            "internal_links_out": 25,
            "word_count": 450,
            "likely_js_shell": 1,
            "render_gap_score": 95,
        },
        {
            "normalized_url": "https://example.com/insights",
            "status_code": 200,
            "content_type": "text/html",
            "fetch_error": "",
            "page_type": "article",
            "crawl_depth": 1,
            "internal_links_out": 12,
            "word_count": 600,
            "likely_js_shell": 1,
            "render_gap_score": 95,
        },
        {
            "normalized_url": "https://example.com/services",
            "status_code": 200,
            "content_type": "text/html",
            "fetch_error": "",
            "page_type": "other",
            "crawl_depth": 1,
            "internal_links_out": 10,
            "word_count": 320,
            "likely_js_shell": 0,
            "render_gap_score": 20,
        },
        {
            "normalized_url": "https://example.com/contact",
            "status_code": 200,
            "content_type": "text/html",
            "fetch_error": "",
            "page_type": "other",
            "crawl_depth": 1,
            "internal_links_out": 8,
            "word_count": 180,
            "likely_js_shell": 0,
            "render_gap_score": 10,
        },
    ]

    targets = select_performance_targets(pages, limit=3)
    assert targets == [
        "https://example.com/",
        "https://example.com/services",
        "https://example.com/contact",
    ]


def test_cli_provider_targets_exclude_system_urls(tmp_path: Path, monkeypatch) -> None:
    server = HTTPServer(("127.0.0.1", 0), _ProviderHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    captured: dict[str, list[str]] = {"psi": [], "crux": []}

    try:
        monkeypatch.setenv("GOOGLE_API_KEY", "dummy")

        def fake_collect_performance(run_id, urls, timeout=20, **kwargs):
            captured["psi"] = list(urls)
            return [], []

        def fake_collect_crux(run_id, urls, timeout=20, origin_fallback=True, **kwargs):
            captured["crux"] = list(urls)
            return [], []

        monkeypatch.setattr("seo_audit.cli.collect_performance", fake_collect_performance)
        monkeypatch.setattr("seo_audit.cli.collect_crux", fake_collect_crux)

        out = tmp_path / "provider_targets"
        args = argparse.Namespace(
            domain=f"http://127.0.0.1:{server.server_port}",
            output=str(out),
            max_pages=6,
            max_render_pages=2,
            render_mode="none",
            timeout=2.0,
            user_agent="TestAgent",
            ignore_robots=False,
            i_understand_robots_bypass=False,
            save_html=False,
            verbose=False,
            performance_targets=3,
            psi_enabled=True,
            crux_enabled=True,
            crux_origin_fallback=True,
            store_provider_payloads=False,
            payload_retention_days=30,
            provider_max_retries=0,
            provider_base_backoff_seconds=0.1,
            provider_max_backoff_seconds=0.5,
            provider_respect_retry_after=True,
            provider_max_total_wait_seconds=1.0,
        )
        run_audit(args)

        assert captured["psi"]
        assert captured["crux"]
        assert all(not url.endswith(".xml") for url in captured["psi"])
        assert all(not url.endswith(".xml") for url in captured["crux"])
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_cli_overlaps_psi_and_crux_collection(tmp_path: Path, monkeypatch) -> None:
    server = HTTPServer(("127.0.0.1", 0), _ProviderHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    psi_started = threading.Event()
    crux_started = threading.Event()
    captured: dict[str, object] = {}

    try:
        monkeypatch.setenv("GOOGLE_API_KEY", "dummy")

        def fake_collect_performance(run_id, urls, timeout=20, **kwargs):
            captured["psi_workers"] = kwargs.get("workers")
            captured["psi_limiter_type"] = type(kwargs.get("rate_limiter")).__name__
            psi_started.set()
            assert crux_started.wait(timeout=1.0)
            return [], []

        def fake_collect_crux(run_id, urls, timeout=20, origin_fallback=True, **kwargs):
            captured["crux_limiter_type"] = type(kwargs.get("rate_limiter")).__name__
            crux_started.set()
            assert psi_started.wait(timeout=1.0)
            return [], []

        monkeypatch.setattr("seo_audit.cli.collect_performance", fake_collect_performance)
        monkeypatch.setattr("seo_audit.cli.collect_crux", fake_collect_crux)

        out = tmp_path / "provider_overlap"
        args = argparse.Namespace(
            domain=f"http://127.0.0.1:{server.server_port}",
            output=str(out),
            max_pages=5,
            max_render_pages=2,
            render_mode="none",
            timeout=2.0,
            user_agent="TestAgent",
            ignore_robots=False,
            i_understand_robots_bypass=False,
            save_html=False,
            verbose=False,
            performance_targets=1,
            psi_enabled=True,
            crux_enabled=True,
            crux_origin_fallback=True,
            store_provider_payloads=False,
            payload_retention_days=30,
            provider_max_retries=0,
            provider_base_backoff_seconds=0.1,
            provider_max_backoff_seconds=0.5,
            provider_respect_retry_after=True,
            provider_max_total_wait_seconds=1.0,
            psi_workers=4,
            provider_rate_limit_rps=4.0,
            provider_rate_limit_capacity=4,
        )
        run_audit(args)

        assert captured["psi_workers"] == 4
        assert captured["psi_limiter_type"] == "TokenBucketRateLimiter"
        assert captured["crux_limiter_type"] == "TokenBucketRateLimiter"
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_cli_persists_crux_and_report_sections(tmp_path: Path, monkeypatch) -> None:
    server = HTTPServer(("127.0.0.1", 0), _ProviderHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        monkeypatch.setenv("GOOGLE_API_KEY", "dummy")
        monkeypatch.setattr(
            "seo_audit.cli.collect_performance",
            lambda run_id, urls, timeout=20, **kwargs: (
                [
                    PerformanceRecord(
                        run_id=run_id,
                        url=urls[0],
                        strategy="mobile",
                        source="psi",
                        performance_score=88,
                        accessibility_score=80,
                        best_practices_score=90,
                        seo_score=85,
                        payload_json="{}",
                    )
                ],
                [],
            ),
        )
        monkeypatch.setattr(
            "seo_audit.cli.collect_crux",
            lambda run_id, urls, timeout=20, origin_fallback=True, **kwargs: (
                [
                    CruxRecord(
                        run_id=run_id,
                        url=urls[0],
                        query_scope="url",
                        status="success",
                        lcp_p75=1100,
                        payload_json="{}",
                    )
                ],
                [],
            ),
        )

        out = tmp_path / "provider"
        args = argparse.Namespace(
            domain=f"http://127.0.0.1:{server.server_port}",
            output=str(out),
            max_pages=5,
            max_render_pages=2,
            render_mode="none",
            timeout=2.0,
            user_agent="TestAgent",
            ignore_robots=False,
            i_understand_robots_bypass=False,
            save_html=False,
            verbose=False,
            performance_targets=1,
            psi_enabled=True,
            crux_enabled=True,
            crux_origin_fallback=True,
            store_provider_payloads=False,
            payload_retention_days=30,
            provider_max_retries=0,
            provider_base_backoff_seconds=0.1,
            provider_max_backoff_seconds=0.5,
            provider_respect_retry_after=True,
            provider_max_total_wait_seconds=1.0,
        )
        run_audit(args)

        con = sqlite3.connect(out / "audit.sqlite")
        run_id = con.execute("SELECT run_id FROM runs ORDER BY rowid DESC LIMIT 1").fetchone()[0]
        assert con.execute("SELECT COUNT(*) FROM performance_metrics WHERE run_id = ?", (run_id,)).fetchone()[0] == 1
        assert con.execute("SELECT COUNT(*) FROM crux_metrics WHERE run_id = ?", (run_id,)).fetchone()[0] == 1
        con.close()

        with (out / "crux.csv").open(newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
            assert len(rows) == 1
            assert rows[0]["status"] == "success"

        report = (out / "report.md").read_text(encoding="utf-8")
        assert "## Performance findings" in report
        assert "## CrUX findings" in report
        assert "- success: 1" in report
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_report_safe_when_run_row_missing(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "missing.sqlite")
    storage.init_db()
    out_path = tmp_path / "report.md"

    build_markdown_report(storage, "missing-run", out_path)
    text = out_path.read_text(encoding="utf-8")
    assert "- Status: `missing`" in text

    storage.close()
