from __future__ import annotations

from pathlib import Path

from seo_audit.dashboard import DashboardStore
from seo_audit.models import (
    OffsiteCommonCrawlComparisonRecord,
    OffsiteCommonCrawlLinkingDomainRecord,
    OffsiteCommonCrawlSummaryRecord,
)
from seo_audit.reporting import build_markdown_report
from seo_audit.storage import Storage


def _seed_run(storage: Storage, run_id: str, domain: str) -> None:
    storage.insert_run(run_id, "2026-01-01T00:00:00+00:00", domain, {"run_profile": "standard"}, "running")
    storage.update_run_completion(run_id, "2026-01-01T00:01:00+00:00", "completed", notes="ok")


def test_report_renders_offsite_success_partial_and_deferred(tmp_path: Path) -> None:
    db = tmp_path / "audit.sqlite"
    storage = Storage(db)
    storage.init_db()

    _seed_run(storage, "r-success", "https://example.com")
    storage.insert_offsite_commoncrawl_summary(
        [
            OffsiteCommonCrawlSummaryRecord(
                run_id="r-success",
                target_domain="example.com",
                cc_release="CC-MAIN-2026-10",
                mode="domains",
                schedule="background_wait",
                status="success",
                cache_state="warm_edges",
                target_found_flag=1,
                harmonic_centrality=12.34,
                pagerank=0.00056,
                referring_domain_count=2,
                weighted_referring_domain_score=1200.0,
                avg_referrer_harmonic=6.2,
                avg_referrer_pagerank=0.00033,
                top_referrer_concentration=0.7,
                comparison_domain_count=1,
                query_elapsed_ms=123,
                background_started_at="2026-01-01T00:00:00+00:00",
                background_finished_at="2026-01-01T00:00:01+00:00",
            )
        ]
    )
    storage.insert_offsite_commoncrawl_linking_domains(
        [
            OffsiteCommonCrawlLinkingDomainRecord(
                run_id="r-success",
                target_domain="example.com",
                linking_domain="referrer-a.com",
                source_num_hosts=20,
                source_harmonic_centrality=8.9,
                source_pagerank=0.0004,
                rank_bucket="top_10",
            )
        ]
    )
    storage.insert_offsite_commoncrawl_comparisons(
        [
            OffsiteCommonCrawlComparisonRecord(
                run_id="r-success",
                target_domain="example.com",
                compare_domain="competitor.com",
                cc_release="CC-MAIN-2026-10",
                harmonic_centrality=9.1,
                pagerank=0.0003,
                rank_gap_vs_target=-3.2,
                pagerank_gap_vs_target=-0.00026,
            )
        ]
    )

    _seed_run(storage, "r-deferred", "https://example.org")
    storage.insert_offsite_commoncrawl_summary(
        [
            OffsiteCommonCrawlSummaryRecord(
                run_id="r-deferred",
                target_domain="example.org",
                cc_release="CC-MAIN-2026-10",
                mode="ranks",
                schedule="concurrent_best_effort",
                status="pending_background",
                cache_state="warm_ranks",
                target_found_flag=0,
                comparison_domain_count=0,
                query_elapsed_ms=0,
                background_started_at="2026-01-01T00:00:00+00:00",
                background_finished_at="2026-01-01T00:00:00+00:00",
            )
        ]
    )

    _seed_run(storage, "r-partial", "https://partial.example")
    storage.insert_offsite_commoncrawl_summary(
        [
            OffsiteCommonCrawlSummaryRecord(
                run_id="r-partial",
                target_domain="partial.example",
                cc_release="CC-MAIN-2026-10",
                mode="ranks",
                schedule="background_wait",
                status="success_partial",
                cache_state="warm_ranks",
                target_found_flag=1,
                comparison_domain_count=0,
                query_elapsed_ms=30,
            )
        ]
    )

    success_report = tmp_path / "report-success.md"
    deferred_report = tmp_path / "report-deferred.md"
    partial_report = tmp_path / "report-partial.md"
    build_markdown_report(storage, "r-success", success_report)
    build_markdown_report(storage, "r-deferred", deferred_report)
    build_markdown_report(storage, "r-partial", partial_report)

    success_text = success_report.read_text(encoding="utf-8")
    deferred_text = deferred_report.read_text(encoding="utf-8")
    partial_text = partial_report.read_text(encoding="utf-8")

    assert "## Offsite visibility (Common Crawl)" in success_text
    assert "- Status: success" in success_text
    assert "- Top linking domains (ordered by harmonic centrality then pagerank):" in success_text
    assert "not exact page-level backlink proof" in success_text

    assert "## Offsite visibility (Common Crawl)" in deferred_text
    assert "- Status: pending_background" in deferred_text
    assert "deferred or partial data" in deferred_text

    assert "## Offsite visibility (Common Crawl)" in partial_text
    assert "- Status: success_partial" in partial_text
    assert "deferred or partial data" in partial_text

    storage.close()


def test_dashboard_summary_includes_offsite_payload(tmp_path: Path) -> None:
    db = tmp_path / "audit.sqlite"
    storage = Storage(db)
    storage.init_db()

    _seed_run(storage, "r-success", "https://example.com")
    storage.insert_offsite_commoncrawl_summary(
        [
            OffsiteCommonCrawlSummaryRecord(
                run_id="r-success",
                target_domain="example.com",
                cc_release="CC-MAIN-2026-10",
                mode="domains",
                schedule="background_wait",
                status="success",
                cache_state="warm_edges",
                target_found_flag=1,
                comparison_domain_count=1,
                query_elapsed_ms=50,
            )
        ]
    )
    storage.insert_offsite_commoncrawl_comparisons(
        [
            OffsiteCommonCrawlComparisonRecord(
                run_id="r-success",
                target_domain="example.com",
                compare_domain="competitor.com",
                cc_release="CC-MAIN-2026-10",
                harmonic_centrality=9.0,
                pagerank=0.0003,
            )
        ]
    )
    storage.insert_offsite_commoncrawl_linking_domains(
        [
            OffsiteCommonCrawlLinkingDomainRecord(
                run_id="r-success",
                target_domain="example.com",
                linking_domain="referrer-a.com",
                source_num_hosts=40,
                source_harmonic_centrality=8.7,
                source_pagerank=0.0002,
                rank_bucket="top_10",
            )
        ]
    )

    _seed_run(storage, "r-deferred", "https://example.org")
    storage.insert_offsite_commoncrawl_summary(
        [
            OffsiteCommonCrawlSummaryRecord(
                run_id="r-deferred",
                target_domain="example.org",
                cc_release="CC-MAIN-2026-10",
                mode="ranks",
                schedule="concurrent_best_effort",
                status="pending_background",
                cache_state="warm_ranks",
                target_found_flag=0,
                comparison_domain_count=0,
                query_elapsed_ms=0,
            )
        ]
    )

    storage.close()

    store = DashboardStore(db)
    success_summary = store.summary("r-success")
    deferred_summary = store.summary("r-deferred")

    assert success_summary["offsite_commoncrawl"]["summary"]["status"] == "success"
    assert len(success_summary["offsite_commoncrawl"]["comparison_domains"]) == 1
    assert len(success_summary["offsite_commoncrawl"]["linking_domains"]) == 1

    assert deferred_summary["offsite_commoncrawl"]["summary"]["status"] == "pending_background"
    assert deferred_summary["offsite_commoncrawl"]["comparison_domains"] == []
    assert deferred_summary["offsite_commoncrawl"]["linking_domains"] == []
