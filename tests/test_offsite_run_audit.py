from __future__ import annotations

import argparse
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from seo_audit.cli import run_audit
from seo_audit.offsite_commoncrawl import (
    OffsiteCommonCrawlCompetitorPayload,
    OffsiteCommonCrawlLaunchContext,
    OffsiteCommonCrawlLinkingDomainPayload,
    OffsiteCommonCrawlManifest,
    OffsiteCommonCrawlSummaryPayload,
    OffsiteCommonCrawlWorkerPayload,
)
from seo_audit.storage import Storage


class _TinySiteHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        address = self.server.server_address
        host = str(address[0]) if isinstance(address, tuple) and len(address) >= 1 else "127.0.0.1"
        port = int(address[1]) if isinstance(address, tuple) and len(address) >= 2 else 80
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
                    f"<url><loc>{base}/</loc></url>"
                    "</urlset>"
                ).encode()
            )
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(b"<html><head><title>Home</title></head><body><h1>Home</h1></body></html>")

    def log_message(self, format, *args):  # noqa: A003
        return


def _build_args(domain: str, out_dir: Path, *, schedule: str, join_budget: float) -> argparse.Namespace:
    return argparse.Namespace(
        domain=domain,
        output=str(out_dir),
        max_pages=3,
        max_render_pages=0,
        render_mode="none",
        timeout=2.0,
        user_agent="TestAgent",
        ignore_robots=False,
        i_understand_robots_bypass=False,
        run_profile="exploratory",
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
        offsite_commoncrawl_enabled=True,
        offsite_commoncrawl_mode="ranks",
        offsite_commoncrawl_schedule=schedule,
        offsite_commoncrawl_release="CC-MAIN-2026-10",
        offsite_commoncrawl_cache_dir=str(out_dir / "cc-cache"),
        offsite_commoncrawl_max_linking_domains=25,
        offsite_commoncrawl_join_budget_seconds=join_budget,
        offsite_commoncrawl_time_budget_seconds=120,
        offsite_commoncrawl_allow_cold_edge_download=False,
        offsite_compare_domains=["competitor-a.com", "competitor-b.com"],
    )


def _launch_context(out_dir: Path) -> OffsiteCommonCrawlLaunchContext:
    release = "CC-MAIN-2026-10"
    release_dir = out_dir / "cc-cache" / release
    return OffsiteCommonCrawlLaunchContext(
        release=release,
        cache_dir=out_dir / "cc-cache",
        release_dir=release_dir,
        cache_state="warm_ranks",
        manifest=OffsiteCommonCrawlManifest(
            release=release,
            vertices_ready=True,
            ranks_ready=True,
            edges_ready=False,
        ),
    )


def _success_payload() -> OffsiteCommonCrawlWorkerPayload:
    now = "2026-01-01T00:00:00+00:00"
    return OffsiteCommonCrawlWorkerPayload(
        summary=OffsiteCommonCrawlSummaryPayload(
            target_domain="example.com",
            cc_release="CC-MAIN-2026-10",
            mode="ranks",
            schedule="blocking",
            status="success",
            cache_state="warm_ranks",
            target_found_flag=1,
            harmonic_centrality=10.0,
            pagerank=0.001,
            comparison_domain_count=1,
            query_elapsed_ms=12,
            background_started_at=now,
            background_finished_at=now,
        ),
        linking_domains=[
            OffsiteCommonCrawlLinkingDomainPayload(
                linking_domain="referrer.com",
                source_num_hosts=10,
                source_harmonic_centrality=9.5,
                source_pagerank=0.0009,
                rank_bucket="top_10",
            )
        ],
        comparisons=[
            OffsiteCommonCrawlCompetitorPayload(
                compare_domain="competitor-a.com",
                cc_release="CC-MAIN-2026-10",
                harmonic_centrality=8.0,
                pagerank=0.0008,
                rank_gap_vs_target=-2.0,
                pagerank_gap_vs_target=-0.0002,
            )
        ],
    )


def test_background_best_effort_does_not_block(monkeypatch, tmp_path: Path) -> None:
    server = HTTPServer(("127.0.0.1", 0), _TinySiteHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    monkeypatch.setattr("seo_audit.cli.collect_performance", lambda *args, **kwargs: ([], []))
    monkeypatch.setattr("seo_audit.cli.collect_crux", lambda *args, **kwargs: ([], []))
    monkeypatch.setattr("seo_audit.cli.inspect_commoncrawl_launch", lambda *args, **kwargs: _launch_context(tmp_path / "best"))

    def slow_worker(_request, _control):
        for _ in range(2000):
            if _control.stop_event.is_set():
                return _success_payload()
            time.sleep(0.005)
        return _success_payload()

    monkeypatch.setattr("seo_audit.cli.run_offsite_commoncrawl_worker", slow_worker)

    args = _build_args(
        f"http://127.0.0.1:{server.server_port}",
        tmp_path / "best",
        schedule="background_best_effort",
        join_budget=0.01,
    )

    started = time.perf_counter()
    try:
        run_audit(args)
    finally:
        server.shutdown()
        thread.join(timeout=2)
    elapsed = time.perf_counter() - started

    con = sqlite3.connect(tmp_path / "best" / "audit.sqlite")
    status = con.execute(
        "SELECT status FROM offsite_commoncrawl_summary ORDER BY offsite_summary_id DESC LIMIT 1"
    ).fetchone()[0]
    con.close()

    assert status == "pending_background"
    assert elapsed < 3.0


def test_background_wait_is_bounded(monkeypatch, tmp_path: Path) -> None:
    server = HTTPServer(("127.0.0.1", 0), _TinySiteHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    monkeypatch.setattr("seo_audit.cli.collect_performance", lambda *args, **kwargs: ([], []))
    monkeypatch.setattr("seo_audit.cli.collect_crux", lambda *args, **kwargs: ([], []))
    monkeypatch.setattr("seo_audit.cli.inspect_commoncrawl_launch", lambda *args, **kwargs: _launch_context(tmp_path / "wait"))

    def slow_worker(_request, _control):
        for _ in range(2000):
            if _control.stop_event.is_set():
                return _success_payload()
            time.sleep(0.005)
        return _success_payload()

    monkeypatch.setattr("seo_audit.cli.run_offsite_commoncrawl_worker", slow_worker)

    args = _build_args(
        f"http://127.0.0.1:{server.server_port}",
        tmp_path / "wait",
        schedule="background_wait",
        join_budget=0.3,
    )

    started = time.perf_counter()
    try:
        run_audit(args)
    finally:
        server.shutdown()
        thread.join(timeout=2)
    elapsed = time.perf_counter() - started

    con = sqlite3.connect(tmp_path / "wait" / "audit.sqlite")
    status = con.execute(
        "SELECT status FROM offsite_commoncrawl_summary ORDER BY offsite_summary_id DESC LIMIT 1"
    ).fetchone()[0]
    con.close()

    assert status == "timeout_background"
    assert elapsed >= 0.2
    assert elapsed < 3.0


def test_offsite_sqlite_writes_stay_on_main_thread(monkeypatch, tmp_path: Path) -> None:
    server = HTTPServer(("127.0.0.1", 0), _TinySiteHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    monkeypatch.setattr("seo_audit.cli.collect_performance", lambda *args, **kwargs: ([], []))
    monkeypatch.setattr("seo_audit.cli.collect_crux", lambda *args, **kwargs: ([], []))
    monkeypatch.setattr("seo_audit.cli.inspect_commoncrawl_launch", lambda *args, **kwargs: _launch_context(tmp_path / "main-thread"))

    worker_thread_id: dict[str, int] = {"id": -1}

    def fast_worker(_request, _control):
        worker_thread_id["id"] = threading.get_ident()
        return _success_payload()

    monkeypatch.setattr("seo_audit.cli.run_offsite_commoncrawl_worker", fast_worker)

    main_thread_id = threading.get_ident()
    write_threads: list[int] = []

    original_insert_summary = Storage.insert_offsite_commoncrawl_summary
    original_insert_linking = Storage.insert_offsite_commoncrawl_linking_domains
    original_insert_comparisons = Storage.insert_offsite_commoncrawl_comparisons

    def wrapped_insert_summary(self, rows):  # noqa: ANN001
        write_threads.append(threading.get_ident())
        return original_insert_summary(self, rows)

    def wrapped_insert_linking(self, rows):  # noqa: ANN001
        write_threads.append(threading.get_ident())
        return original_insert_linking(self, rows)

    def wrapped_insert_comparisons(self, rows):  # noqa: ANN001
        write_threads.append(threading.get_ident())
        return original_insert_comparisons(self, rows)

    monkeypatch.setattr(Storage, "insert_offsite_commoncrawl_summary", wrapped_insert_summary)
    monkeypatch.setattr(Storage, "insert_offsite_commoncrawl_linking_domains", wrapped_insert_linking)
    monkeypatch.setattr(Storage, "insert_offsite_commoncrawl_comparisons", wrapped_insert_comparisons)

    args = _build_args(
        f"http://127.0.0.1:{server.server_port}",
        tmp_path / "main-thread",
        schedule="blocking",
        join_budget=0.5,
    )

    try:
        run_audit(args)
    finally:
        server.shutdown()
        thread.join(timeout=2)

    assert write_threads
    assert all(thread_id == main_thread_id for thread_id in write_threads)
    assert worker_thread_id["id"] != -1
    assert worker_thread_id["id"] != main_thread_id
