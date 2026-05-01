import argparse
import csv
import json
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from seo_audit.cli import run_audit


def _server_base_url(server: object) -> str:
    address = getattr(server, "server_address", None)
    host = str(address[0]) if isinstance(address, tuple) and len(address) >= 1 else "127.0.0.1"
    port = int(address[1]) if isinstance(address, tuple) and len(address) >= 2 else 80
    return f"http://{host}:{port}"


class SimpleHandler(BaseHTTPRequestHandler):
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
        self.wfile.write(b"<html><head><title>Home</title></head><body><h1>Home</h1></body></html>")

    def log_message(self, format, *args):  # noqa: A003
        return


def _args(domain: str, out: Path) -> argparse.Namespace:
    return argparse.Namespace(
        domain=domain,
        output=str(out),
        max_pages=5,
        max_render_pages=2,
        render_mode="none",
        timeout=2.0,
        user_agent="TestAgent/1.0",
        ignore_robots=False,
        i_understand_robots_bypass=False,
        run_profile="standard",
        save_html=False,
        verbose=False,
        performance_targets=1,
        crawl_heartbeat_every_pages=None,
        psi_enabled=False,
        crux_enabled=False,
        crux_origin_fallback=True,
        store_provider_payloads=False,
        payload_retention_days=30,
        provider_max_retries=0,
        provider_base_backoff_seconds=0.1,
        provider_max_backoff_seconds=0.5,
        provider_respect_retry_after=True,
        provider_max_total_wait_seconds=1.0,
    )


def test_report_shows_completed_status(tmp_path: Path, monkeypatch) -> None:
    server = HTTPServer(("127.0.0.1", 0), SimpleHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    monkeypatch.setattr("seo_audit.cli.collect_performance", lambda *args, **kwargs: ([], []))
    run_audit(_args(f"http://127.0.0.1:{server.server_port}", tmp_path / "ok"))

    report = (tmp_path / "ok" / "report.md").read_text(encoding="utf-8")
    assert "- Status: `completed`" in report
    assert "internal host policy: strict" in report

    server.shutdown()
    thread.join(timeout=2)


def test_report_shows_failed_status(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("seo_audit.cli.crawl_site", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr("seo_audit.cli.fetch_and_parse_sitemaps", lambda *args, **kwargs: [])

    try:
        run_audit(_args("https://example.com", tmp_path / "fail"))
    except RuntimeError:
        pass
    else:
        raise AssertionError("Expected RuntimeError from crawl_site")

    report = (tmp_path / "fail" / "report.md").read_text(encoding="utf-8")
    assert "- Status: `failed`" in report


def test_ignore_robots_requires_acknowledgement(tmp_path: Path) -> None:
    args = _args("https://example.com", tmp_path / "fail-ignore")
    args.ignore_robots = True
    args.i_understand_robots_bypass = False

    try:
        run_audit(args)
    except ValueError:
        pass
    else:
        raise AssertionError("Expected ValueError when robots bypass acknowledgement is missing")


def test_ignore_robots_with_acknowledgement_proceeds(tmp_path: Path, monkeypatch) -> None:
    server = HTTPServer(("127.0.0.1", 0), SimpleHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    monkeypatch.setattr("seo_audit.cli.collect_performance", lambda *args, **kwargs: ([], []))
    monkeypatch.setattr("seo_audit.cli.collect_crux", lambda *args, **kwargs: ([], []))

    args = _args(f"http://127.0.0.1:{server.server_port}", tmp_path / "ok-ignore")
    args.ignore_robots = True
    args.i_understand_robots_bypass = True

    run_audit(args)
    report = (tmp_path / "ok-ignore" / "report.md").read_text(encoding="utf-8")
    assert "- Status: `completed`" in report

    server.shutdown()
    thread.join(timeout=2)


def test_run_profile_defaults_persisted(tmp_path: Path, monkeypatch) -> None:
    server = HTTPServer(("127.0.0.1", 0), SimpleHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    monkeypatch.setattr("seo_audit.cli.collect_performance", lambda *args, **kwargs: ([], []))
    monkeypatch.setattr("seo_audit.cli.collect_crux", lambda *args, **kwargs: ([], []))

    args = _args(f"http://127.0.0.1:{server.server_port}", tmp_path / "profile")
    args.run_profile = "exploratory"
    args.max_pages = None
    args.max_render_pages = None
    args.render_mode = None
    args.performance_targets = None
    args.provider_max_retries = None
    args.provider_base_backoff_seconds = None
    args.provider_max_backoff_seconds = None
    args.provider_max_total_wait_seconds = None
    args.crawl_heartbeat_every_pages = None

    run_audit(args)

    con = sqlite3.connect(tmp_path / "profile" / "audit.sqlite")
    row = con.execute("SELECT config_json FROM runs ORDER BY rowid DESC LIMIT 1").fetchone()
    con.close()
    assert row is not None
    config = json.loads(row[0])
    assert config["run_profile"] == "exploratory"
    assert config["max_pages"] == 50
    assert config["max_render_pages"] == 0
    assert config["render_mode"] == "none"
    assert config["crawl_discovery_mode"] == "raw"
    assert config["scope_mode"] == "host_only"
    assert config["performance_targets"] == 1
    assert config["crawl_heartbeat_every_pages"] == 10

    report = (tmp_path / "profile" / "report.md").read_text(encoding="utf-8")
    assert "- Run profile: `exploratory`" in report

    server.shutdown()
    thread.join(timeout=2)


def test_run_events_export_and_report_sections(tmp_path: Path, monkeypatch) -> None:
    server = HTTPServer(("127.0.0.1", 0), SimpleHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    monkeypatch.setattr("seo_audit.cli.collect_performance", lambda *args, **kwargs: ([], []))
    monkeypatch.setattr("seo_audit.cli.collect_crux", lambda *args, **kwargs: ([], []))

    args = _args(f"http://127.0.0.1:{server.server_port}", tmp_path / "events")
    args.crawl_heartbeat_every_pages = 1
    run_audit(args)

    con = sqlite3.connect(tmp_path / "events" / "audit.sqlite")
    stage_count = con.execute(
        "SELECT COUNT(*) FROM run_events WHERE event_type = 'stage_timing'"
    ).fetchone()[0]
    heartbeat_count = con.execute(
        "SELECT COUNT(*) FROM run_events WHERE event_type = 'crawl_heartbeat'"
    ).fetchone()[0]
    con.close()

    assert stage_count >= 1
    assert heartbeat_count >= 1

    with (tmp_path / "events" / "run_events.csv").open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert rows
    assert any(row["event_type"] == "stage_timing" for row in rows)

    report = (tmp_path / "events" / "report.md").read_text(encoding="utf-8")
    assert "## Stage timing summary" in report
    assert "## Crawl heartbeat summary" in report
    assert "## Discovery blind spots" in report
    assert "## Search Console reconciliation" in report
    assert "## Governance and answer-layer controls" in report
    assert "## Prioritization tracks" in report
    assert "## URL policy coverage" in report
    assert "## Issue gate coverage" in report
    assert "## Issue verification confidence" in report
    assert "## Performance by template group" in report
    assert "## Snippet and citation controls" in report

    server.shutdown()
    thread.join(timeout=2)


def test_fresh_output_dir_creates_unique_run_subdir(tmp_path: Path, monkeypatch) -> None:
    server = HTTPServer(("127.0.0.1", 0), SimpleHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    monkeypatch.setattr("seo_audit.cli.collect_performance", lambda *args, **kwargs: ([], []))
    monkeypatch.setattr("seo_audit.cli.collect_crux", lambda *args, **kwargs: ([], []))

    args = _args(f"http://127.0.0.1:{server.server_port}", tmp_path / "fresh")
    args.fresh_output_dir = True
    run_audit(args)

    fresh_root = tmp_path / "fresh"
    run_dirs = [path for path in fresh_root.iterdir() if path.is_dir() and path.name.startswith("run-")]
    assert run_dirs
    assert any((path / "report.md").exists() for path in run_dirs)

    server.shutdown()
    thread.join(timeout=2)
