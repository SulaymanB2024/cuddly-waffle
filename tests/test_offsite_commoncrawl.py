from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
import requests

from seo_audit.cli import _join_offsite_commoncrawl_future, build_parser
from seo_audit.config import AuditConfig
from seo_audit.offsite_commoncrawl import (
    STATUS_DEFERRED_VERIFY,
    STATUS_FAILED_MISSING_DEPENDENCY,
    STATUS_SKIPPED_COLD_EDGE_CACHE,
    OffsiteCommonCrawlManifest,
    OffsiteCommonCrawlSummaryPayload,
    OffsiteCommonCrawlWorkerControl,
    OffsiteCommonCrawlWorkerPayload,
    OffsiteCommonCrawlWorkerRequest,
    inspect_commoncrawl_launch,
    run_offsite_commoncrawl_worker,
)


class _FailSession:
    def get(self, *args, **kwargs):  # noqa: ANN002, ANN003
        raise requests.RequestException("offline")


class _DummySession:
    def __enter__(self) -> "_DummySession":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False


class _FakeConnection:
    def close(self) -> None:
        return None

    def interrupt(self) -> None:
        return None


class _FakeDuckDB:
    def connect(self, _path: str) -> _FakeConnection:
        return _FakeConnection()


def _success_payload(delay_seconds: float) -> OffsiteCommonCrawlWorkerPayload:
    time.sleep(delay_seconds)
    now = "2026-01-01T00:00:00+00:00"
    return OffsiteCommonCrawlWorkerPayload(
        summary=OffsiteCommonCrawlSummaryPayload(
            target_domain="example.com",
            cc_release="CC-MAIN-2026-10",
            mode="ranks",
            schedule="background_wait",
            status="success",
            cache_state="warm_ranks",
            target_found_flag=1,
            query_elapsed_ms=10,
            background_started_at=now,
            background_finished_at=now,
        )
    )


def test_audit_config_offsite_defaults(tmp_path: Path) -> None:
    config = AuditConfig(domain="https://example.com", output_dir=tmp_path)
    assert config.offsite_commoncrawl_enabled is False
    assert config.offsite_commoncrawl_mode == "ranks"
    assert config.offsite_commoncrawl_schedule == "concurrent_best_effort"
    assert config.offsite_commoncrawl_release == "auto"
    assert config.offsite_commoncrawl_cache_dir == "~/.cache/seo_audit/commoncrawl"
    assert config.offsite_commoncrawl_max_linking_domains == 100
    assert config.offsite_commoncrawl_join_budget_seconds == 0.5
    assert config.offsite_commoncrawl_time_budget_seconds == 180
    assert config.offsite_commoncrawl_allow_cold_edge_download is False
    assert config.offsite_compare_domains == ()


def test_cli_parser_offsite_plumbing() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "audit",
            "--domain",
            "https://example.com",
            "--offsite-commoncrawl-enabled",
            "--offsite-commoncrawl-mode",
            "domains",
            "--offsite-commoncrawl-schedule",
            "background_wait",
            "--offsite-commoncrawl-release",
            "CC-MAIN-2026-10",
            "--offsite-commoncrawl-cache-dir",
            "./cache",
            "--offsite-commoncrawl-max-linking-domains",
            "25",
            "--offsite-commoncrawl-join-budget-seconds",
            "2.5",
            "--offsite-commoncrawl-time-budget-seconds",
            "90",
            "--offsite-commoncrawl-allow-cold-edge-download",
            "--offsite-compare-domain",
            "competitor-a.com",
            "--offsite-compare-domain",
            "competitor-b.com",
        ]
    )

    assert args.offsite_commoncrawl_enabled is True
    assert args.offsite_commoncrawl_mode == "domains"
    assert args.offsite_commoncrawl_schedule == "background_wait"
    assert args.offsite_commoncrawl_release == "CC-MAIN-2026-10"
    assert args.offsite_commoncrawl_cache_dir == "./cache"
    assert args.offsite_commoncrawl_max_linking_domains == 25
    assert float(args.offsite_commoncrawl_join_budget_seconds) == 2.5
    assert int(args.offsite_commoncrawl_time_budget_seconds) == 90
    assert args.offsite_commoncrawl_allow_cold_edge_download is True
    assert args.offsite_compare_domains == ["competitor-a.com", "competitor-b.com"]


def test_cli_parser_hides_verify_from_primary_mode_choices() -> None:
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "audit",
                "--domain",
                "https://example.com",
                "--offsite-commoncrawl-mode",
                "verify",
            ]
        )


def test_cli_parser_allows_hidden_experimental_verify_switches() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "audit",
            "--domain",
            "https://example.com",
            "--offsite-commoncrawl-mode-verify",
            "--offsite-commoncrawl-experimental-verify",
        ]
    )
    assert args.offsite_commoncrawl_mode == "verify"
    assert args.offsite_commoncrawl_experimental_verify is True


def test_release_cache_inspection_falls_back_to_cached_release(tmp_path: Path) -> None:
    cache_root = tmp_path / "cc-cache"
    release_dir = cache_root / "CC-MAIN-2026-10"
    release_dir.mkdir(parents=True, exist_ok=True)
    (release_dir / "manifest.json").write_text(
        """
        {
          "release": "CC-MAIN-2026-10",
          "schema_version": 1,
          "vertices_ready": 1,
          "ranks_ready": 1,
          "edges_ready": 0,
          "downloaded_at": "2026-01-01T00:00:00+00:00",
          "last_used_at": "2026-01-01T00:00:00+00:00"
        }
        """.strip(),
        encoding="utf-8",
    )

    launch = inspect_commoncrawl_launch("auto", cache_root, session=_FailSession(), timeout_seconds=0.01)
    assert launch.release == "CC-MAIN-2026-10"
    assert launch.cache_state == "warm_ranks"


def test_domains_mode_skips_on_cold_edge_cache(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("seo_audit.offsite_commoncrawl._import_duckdb", lambda: _FakeDuckDB())
    monkeypatch.setattr("seo_audit.offsite_commoncrawl.requests.Session", lambda: _DummySession())

    def fake_ensure_vertices_and_ranks_ready(**kwargs):  # noqa: ANN003
        manifest: OffsiteCommonCrawlManifest = kwargs["manifest"]
        manifest.vertices_ready = True
        manifest.ranks_ready = True
        manifest.edges_ready = False
        return manifest

    monkeypatch.setattr("seo_audit.offsite_commoncrawl._ensure_vertices_and_ranks_ready", fake_ensure_vertices_and_ranks_ready)
    monkeypatch.setattr(
        "seo_audit.offsite_commoncrawl.lookup_rank_rows",
        lambda _connection, _domains: {
            "com.example": {
                "domain": "com.example",
                "num_hosts": 1,
                "harmonic_centrality": 1.0,
                "pagerank": 0.1,
            }
        },
    )

    payload = run_offsite_commoncrawl_worker(
        OffsiteCommonCrawlWorkerRequest(
            target_domain="example.com",
            mode="domains",
            schedule="background_best_effort",
            release="CC-MAIN-2026-10",
            cache_dir=tmp_path,
            allow_cold_edge_download=False,
        ),
        OffsiteCommonCrawlWorkerControl(),
    )

    assert payload.summary.status == STATUS_SKIPPED_COLD_EDGE_CACHE


def test_domains_mode_orders_by_harmonic_then_pagerank(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("seo_audit.offsite_commoncrawl._import_duckdb", lambda: _FakeDuckDB())
    monkeypatch.setattr("seo_audit.offsite_commoncrawl.requests.Session", lambda: _DummySession())

    def fake_ensure_vertices_and_ranks_ready(**kwargs):  # noqa: ANN003
        manifest: OffsiteCommonCrawlManifest = kwargs["manifest"]
        manifest.vertices_ready = True
        manifest.ranks_ready = True
        manifest.edges_ready = True
        return manifest

    monkeypatch.setattr("seo_audit.offsite_commoncrawl._ensure_vertices_and_ranks_ready", fake_ensure_vertices_and_ranks_ready)
    monkeypatch.setattr("seo_audit.offsite_commoncrawl._ensure_edges_ready", lambda **kwargs: kwargs["manifest"])
    monkeypatch.setattr(
        "seo_audit.offsite_commoncrawl.lookup_rank_rows",
        lambda _connection, _domains: {
            "com.example": {
                "domain": "com.example",
                "num_hosts": 1,
                "harmonic_centrality": 1.0,
                "pagerank": 0.1,
            }
        },
    )
    monkeypatch.setattr(
        "seo_audit.offsite_commoncrawl.discover_linking_domains",
        lambda _connection, **kwargs: [
            {
                "source_domain": "com.alpha",
                "source_num_hosts": 999,
                "source_harmonic_centrality": 9.0,
                "source_pagerank": 0.001,
            },
            {
                "source_domain": "com.beta",
                "source_num_hosts": 100,
                "source_harmonic_centrality": 10.0,
                "source_pagerank": 0.0001,
            },
            {
                "source_domain": "com.gamma",
                "source_num_hosts": 1,
                "source_harmonic_centrality": 10.0,
                "source_pagerank": 0.0002,
            },
        ],
    )

    payload = run_offsite_commoncrawl_worker(
        OffsiteCommonCrawlWorkerRequest(
            target_domain="example.com",
            mode="domains",
            schedule="background_wait",
            release="CC-MAIN-2026-10",
            cache_dir=tmp_path,
            allow_cold_edge_download=False,
        ),
        OffsiteCommonCrawlWorkerControl(),
    )

    assert [row.linking_domain for row in payload.linking_domains] == ["gamma.com", "beta.com", "alpha.com"]


def test_worker_handles_missing_dependency(monkeypatch, tmp_path: Path) -> None:
    def _raise_import_error():
        raise ImportError("duckdb not installed")

    monkeypatch.setattr("seo_audit.offsite_commoncrawl._import_duckdb", _raise_import_error)

    payload = run_offsite_commoncrawl_worker(
        OffsiteCommonCrawlWorkerRequest(
            target_domain="example.com",
            mode="ranks",
            schedule="background_best_effort",
            release="CC-MAIN-2026-10",
            cache_dir=tmp_path,
        ),
        OffsiteCommonCrawlWorkerControl(),
    )

    assert payload.summary.status == STATUS_FAILED_MISSING_DEPENDENCY


def test_verify_mode_is_deferred(tmp_path: Path) -> None:
    payload = run_offsite_commoncrawl_worker(
        OffsiteCommonCrawlWorkerRequest(
            target_domain="example.com",
            mode="verify",
            schedule="background_best_effort",
            release="CC-MAIN-2026-10",
            cache_dir=tmp_path,
        ),
        OffsiteCommonCrawlWorkerControl(),
    )
    assert payload.summary.status == STATUS_DEFERRED_VERIFY


def test_background_best_effort_join_does_not_block() -> None:
    control = OffsiteCommonCrawlWorkerControl()
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_success_payload, 0.2)
        started = time.perf_counter()
        payload, state = _join_offsite_commoncrawl_future(
            future=future,
            schedule="background_best_effort",
            join_budget_seconds=0.01,
            control=control,
        )
        elapsed = time.perf_counter() - started

    assert payload is None
    assert state == "deferred"
    assert elapsed < 0.15
    assert control.stop_event.is_set()


def test_background_wait_respects_join_budget() -> None:
    control = OffsiteCommonCrawlWorkerControl()
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_success_payload, 0.25)
        started = time.perf_counter()
        payload, state = _join_offsite_commoncrawl_future(
            future=future,
            schedule="background_wait",
            join_budget_seconds=0.05,
            control=control,
        )
        elapsed = time.perf_counter() - started

    assert payload is None
    assert state == "timeout"
    assert elapsed >= 0.04
    assert elapsed < 0.2
    assert control.stop_event.is_set()
