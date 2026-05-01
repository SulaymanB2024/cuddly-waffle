import csv
import json
import sqlite3
from pathlib import Path

from seo_audit.models import (
    AIVisibilityRecord,
    ArtifactCacheRecord,
    OffsiteCommonCrawlComparisonRecord,
    OffsiteCommonCrawlLinkingDomainRecord,
    OffsiteCommonCrawlSummaryRecord,
    PageDiffRecord,
    PageGraphMetricsRecord,
    PageRecord,
    ScoreRecord,
    URLStateRecord,
)
from seo_audit.storage import CURRENT_SCHEMA_VERSION, Storage


def test_storage_init_and_insert(tmp_path: Path) -> None:
    db = tmp_path / "a.sqlite"
    s = Storage(db)
    s.init_db()
    s.insert_run("r1", "2026-01-01T00:00:00Z", "https://example.com", {}, "running")
    s.insert_pages([PageRecord(run_id="r1", discovered_url="https://example.com", normalized_url="https://example.com/")])
    rows = s.query("SELECT * FROM pages WHERE run_id = ?", ("r1",))
    assert len(rows) == 1
    s.close()


def test_export_csvs_scoped_to_run(tmp_path: Path) -> None:
    db = tmp_path / "a.sqlite"
    s = Storage(db)
    s.init_db()
    s.insert_run("r1", "2026-01-01T00:00:00Z", "https://example.com", {}, "running")
    s.insert_run("r2", "2026-01-01T00:00:01Z", "https://example.org", {}, "running")
    s.insert_pages(
        [
            PageRecord(run_id="r1", discovered_url="https://example.com", normalized_url="https://example.com/"),
            PageRecord(run_id="r2", discovered_url="https://example.org", normalized_url="https://example.org/"),
        ]
    )

    out = tmp_path / "out"
    s.export_csvs(out, run_id="r1")
    with (out / "pages.csv").open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))

    assert len(rows) == 1
    assert rows[0]["run_id"] == "r1"
    s.close()


def test_storage_creates_run_scoped_indexes(tmp_path: Path) -> None:
    db = tmp_path / "a.sqlite"
    s = Storage(db)
    s.init_db()

    indexes = {row[1] for row in s.query("PRAGMA index_list('pages')")}
    assert "idx_pages_run_id" in indexes
    assert "idx_pages_run_url" in indexes
    assert "idx_pages_run_effective_content_hash" in indexes
    assert "idx_pages_run_canonical_cluster" in indexes

    link_indexes = {row[1] for row in s.query("PRAGMA index_list('links')")}
    assert "idx_links_run_id" in link_indexes
    assert "idx_links_run_source" in link_indexes
    assert "idx_links_run_target_internal" in link_indexes

    page_columns = {row[1] for row in s.query("PRAGMA table_info('pages')")}
    assert "extract_time_ms" in page_columns
    assert "heading_outline_json" in page_columns
    assert "schema_summary_json" in page_columns
    assert "content_hash" in page_columns
    assert "raw_content_hash" in page_columns
    assert "rendered_content_hash" in page_columns
    assert "effective_content_hash" in page_columns
    assert "effective_field_provenance_json" in page_columns
    assert "measurement_status" in page_columns
    assert "measurement_error_family" in page_columns
    assert "shell_state" in page_columns
    assert "canonical_cluster_key" in page_columns
    assert "canonical_cluster_role" in page_columns
    assert "canonical_signal_summary_json" in page_columns

    snapshot_columns = {row[1] for row in s.query("PRAGMA table_info('page_snapshots')")}
    assert "raw_content_hash" in snapshot_columns
    assert "rendered_content_hash" in snapshot_columns
    assert "effective_content_hash" in snapshot_columns

    link_columns = {row[1] for row in s.query("PRAGMA table_info('links')")}
    assert "dom_region" in link_columns

    graph_columns = {row[1] for row in s.query("PRAGMA table_info('page_graph_metrics')")}
    assert "run_id" in graph_columns
    assert "url" in graph_columns
    assert "internal_pagerank" in graph_columns
    assert "betweenness" in graph_columns
    assert "closeness" in graph_columns
    assert "community_id" in graph_columns
    assert "bridge_flag" in graph_columns

    graph_indexes = {row[1] for row in s.query("PRAGMA index_list('page_graph_metrics')")}
    assert "idx_graph_metrics_run_id" in graph_indexes
    assert "idx_graph_metrics_run_url" in graph_indexes
    s.close()


def test_storage_tracks_schema_version_and_migrations(tmp_path: Path) -> None:
    db = tmp_path / "a.sqlite"
    storage = Storage(db)
    storage.init_db()

    assert storage.schema_version() == CURRENT_SCHEMA_VERSION

    migration_rows = storage.query("SELECT version, name, success FROM schema_migrations ORDER BY version")
    assert migration_rows
    assert int(migration_rows[-1]["version"]) == CURRENT_SCHEMA_VERSION
    assert int(migration_rows[-1]["success"]) == 1

    storage.close()


def test_scores_table_has_explainability_columns(tmp_path: Path) -> None:
    db = tmp_path / "a.sqlite"
    storage = Storage(db)
    storage.init_db()

    score_columns = {row["name"] for row in storage.query("PRAGMA table_info(scores)")}
    assert "score_version" in score_columns
    assert "score_profile" in score_columns
    assert "explanation_json" in score_columns
    assert "scoring_model_version" in score_columns
    assert "scoring_profile" in score_columns
    assert "score_explanation_json" in score_columns

    storage.close()


def test_storage_persists_score_explanation_fields(tmp_path: Path) -> None:
    db = tmp_path / "a.sqlite"
    storage = Storage(db)
    storage.init_db()
    storage.insert_run("r1", "2026-01-01T00:00:00Z", "https://example.com", {}, "running")

    explanation = {
        "scoring_model_version": "2.0.0",
        "scoring_profile": "general",
        "score_version": "2.0.0",
        "score_profile": "general",
        "dimensions": {"scores": {"crawlability_score": 95}},
        "cap": {"score_cap": 100, "reasons": []},
    }
    storage.insert_scores(
        [
            ScoreRecord(
                run_id="r1",
                url="https://example.com/",
                crawlability_score=95,
                onpage_score=90,
                render_risk_score=88,
                internal_linking_score=85,
                local_seo_score=-1,
                performance_score=70,
                overall_score=80,
                quality_score=85,
                risk_score=20,
                coverage_score=100,
                score_cap=100,
                score_version="2.0.0",
                score_profile="general",
                explanation_json=json.dumps(explanation, sort_keys=True),
                scoring_model_version="2.0.0",
                scoring_profile="general",
                score_explanation_json=json.dumps(explanation, sort_keys=True),
            )
        ]
    )

    row = storage.query(
        "SELECT score_version, score_profile, explanation_json, scoring_model_version, scoring_profile, score_explanation_json FROM scores WHERE run_id = ? AND url = ?",
        ("r1", "https://example.com/"),
    )[0]
    assert row["score_version"] == "2.0.0"
    assert row["score_profile"] == "general"
    assert row["scoring_model_version"] == "2.0.0"
    assert row["scoring_profile"] == "general"
    parsed = json.loads(str(row["explanation_json"]))
    assert parsed["dimensions"]["scores"]["crawlability_score"] == 95
    parsed_new = json.loads(str(row["score_explanation_json"]))
    assert parsed_new["scoring_model_version"] == "2.0.0"

    storage.close()


def test_storage_migrates_legacy_scores_table_additively(tmp_path: Path) -> None:
    db = tmp_path / "legacy.sqlite"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE scores (
            score_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            url TEXT NOT NULL,
            crawlability_score INTEGER NOT NULL,
            onpage_score INTEGER NOT NULL,
            render_risk_score INTEGER NOT NULL,
            internal_linking_score INTEGER NOT NULL,
            local_seo_score INTEGER NOT NULL,
            performance_score INTEGER NOT NULL,
            overall_score INTEGER NOT NULL,
            quality_score INTEGER DEFAULT 0,
            risk_score INTEGER DEFAULT 0,
            coverage_score INTEGER DEFAULT 0,
            score_cap INTEGER DEFAULT 100,
            score_version TEXT DEFAULT '1.0.0',
            score_profile TEXT DEFAULT 'default',
            explanation_json TEXT DEFAULT '{}'
        );
        """
    )
    conn.execute(
        """
        INSERT INTO scores (
            run_id, url, crawlability_score, onpage_score, render_risk_score,
            internal_linking_score, local_seo_score, performance_score,
            overall_score, quality_score, risk_score, coverage_score, score_cap,
            score_version, score_profile, explanation_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "legacy-run",
            "https://example.com/",
            90,
            88,
            85,
            80,
            -1,
            70,
            76,
            82,
            20,
            100,
            100,
            "1.1.0",
            "default",
            "{}",
        ),
    )
    conn.commit()
    conn.close()

    storage = Storage(db)
    storage.init_db()
    row = storage.query(
        "SELECT score_version, score_profile, scoring_model_version, scoring_profile, score_explanation_json FROM scores WHERE run_id = ?",
        ("legacy-run",),
    )[0]
    assert row["score_version"] == "1.1.0"
    assert row["score_profile"] == "default"
    assert row["scoring_model_version"] is None
    assert row["scoring_profile"] is None
    assert row["score_explanation_json"] is None

    storage.close()


def test_storage_inserts_page_graph_metrics(tmp_path: Path) -> None:
    db = tmp_path / "a.sqlite"
    storage = Storage(db)
    storage.init_db()
    storage.insert_run("r1", "2026-01-01T00:00:00Z", "https://example.com", {}, "running")

    storage.insert_page_graph_metrics(
        [
            PageGraphMetricsRecord(
                run_id="r1",
                url="https://example.com/",
                internal_pagerank=0.315,
                betweenness=0.25,
                closeness=0.5,
                community_id=1,
                bridge_flag=1,
            ),
            PageGraphMetricsRecord(
                run_id="r1",
                url="https://example.com/service",
                internal_pagerank=0.185,
                betweenness=0.05,
                closeness=0.4,
                community_id=1,
                bridge_flag=0,
            ),
        ]
    )

    rows = storage.query(
        "SELECT run_id, url, internal_pagerank, betweenness, closeness, community_id, bridge_flag FROM page_graph_metrics WHERE run_id = ? ORDER BY url ASC",
        ("r1",),
    )
    assert len(rows) == 2
    assert rows[0]["url"] == "https://example.com/"
    assert float(rows[0]["internal_pagerank"]) > float(rows[1]["internal_pagerank"])
    assert int(rows[0]["bridge_flag"]) == 1

    out = tmp_path / "out"
    storage.export_csvs(out, run_id="r1")
    export_path = out / "page_graph_metrics.csv"
    assert export_path.exists()
    with export_path.open(newline="", encoding="utf-8") as fh:
        exported_rows = list(csv.DictReader(fh))
    assert len(exported_rows) == 2
    assert {row["run_id"] for row in exported_rows} == {"r1"}

    storage.close()


def test_storage_creates_incremental_tables(tmp_path: Path) -> None:
    db = tmp_path / "a.sqlite"
    storage = Storage(db)
    storage.init_db()

    url_state_columns = {row["name"] for row in storage.query("PRAGMA table_info(url_state)")}
    assert "url_key" in url_state_columns
    assert "normalized_url" in url_state_columns
    assert "last_body_sha256" in url_state_columns

    body_blob_columns = {row["name"] for row in storage.query("PRAGMA table_info(body_blobs)")}
    assert "body_sha256" in body_blob_columns
    assert "storage_path" in body_blob_columns

    artifact_columns = {row["name"] for row in storage.query("PRAGMA table_info(artifact_cache)")}
    assert "artifact_sha256" in artifact_columns
    assert "extractor_version" in artifact_columns

    page_diff_columns = {row["name"] for row in storage.query("PRAGMA table_info(page_diffs)")}
    assert "run_id" in page_diff_columns
    assert "diff_family" in page_diff_columns

    storage.close()


def test_storage_blob_url_state_artifact_and_diffs_roundtrip(tmp_path: Path) -> None:
    db = tmp_path / "a.sqlite"
    storage = Storage(db)
    storage.init_db()

    body = b"<html><head><title>x</title></head><body>x</body></html>"
    body_sha = "a" * 64
    blob = storage.ensure_body_blob(body_sha, body, content_encoding="identity")
    assert blob.body_sha256 == body_sha
    restored = storage.read_body_blob(body_sha)
    assert restored == body

    storage.upsert_url_states(
        [
            URLStateRecord(
                url_key="k1",
                normalized_url="https://example.com/",
                last_final_url="https://example.com/",
                etag='"etag-1"',
                last_modified="Wed, 21 Oct 2015 07:28:00 GMT",
                last_status_code=200,
                last_content_type="text/html",
                last_body_sha256=body_sha,
                last_extracted_sha256="b" * 64,
                last_fetched_at="2026-01-01T00:00:00+00:00",
                last_seen_run_id="run-1",
                not_modified_streak=2,
            )
        ]
    )
    state = storage.get_url_state("https://example.com/")
    assert state is not None
    assert state["etag"] == '"etag-1"'
    assert int(state["not_modified_streak"]) == 2

    storage.upsert_artifact_cache(
        [
            ArtifactCacheRecord(
                artifact_sha256="c" * 64,
                body_sha256=body_sha,
                extractor_version="extract:2.0.0|schema:1.0.0|scoring:1.1.0",
                artifact_type="page_extract",
                artifact_json=json.dumps({"extract_data": {"title": "x", "anchors": []}}, sort_keys=True),
            )
        ]
    )
    artifact = storage.get_artifact_cache(
        body_sha,
        "page_extract",
        "extract:2.0.0|schema:1.0.0|scoring:1.1.0",
    )
    assert artifact is not None
    assert artifact["artifact_sha256"] == "c" * 64

    storage.insert_page_diffs(
        [
            PageDiffRecord(
                run_id="run-1",
                url="https://example.com/",
                diff_family="primary_content_hash",
                old_value="old",
                new_value="new",
                severity="medium",
            )
        ]
    )
    diff_row = storage.query("SELECT * FROM page_diffs WHERE run_id = ?", ("run-1",))[0]
    assert diff_row["diff_family"] == "primary_content_hash"

    out = tmp_path / "out"
    storage.export_csvs(out, run_id="run-1")
    assert (out / "url_state.csv").exists()
    assert (out / "body_blobs.csv").exists()
    assert (out / "artifact_cache.csv").exists()
    assert (out / "page_diffs.csv").exists()

    storage.close()


def test_storage_inserts_ai_visibility_events(tmp_path: Path) -> None:
    db = tmp_path / "a.sqlite"
    storage = Storage(db)
    storage.init_db()

    storage.insert_ai_visibility_events(
        [
            AIVisibilityRecord(
                run_id="run-1",
                url="https://example.com/page",
                potential_score=72,
                visibility_json='{"potential":{"score":72}}',
            )
        ]
    )

    rows = storage.query("SELECT * FROM ai_visibility_events WHERE run_id = ?", ("run-1",))
    assert len(rows) == 1
    assert int(rows[0]["potential_score"]) == 72

    out = tmp_path / "out"
    storage.export_csvs(out, run_id="run-1")
    assert (out / "ai_visibility_events.csv").exists()

    storage.close()


def test_storage_inserts_offsite_commoncrawl_rows_and_exports(tmp_path: Path) -> None:
    db = tmp_path / "a.sqlite"
    storage = Storage(db)
    storage.init_db()

    storage.insert_offsite_commoncrawl_summary(
        [
            OffsiteCommonCrawlSummaryRecord(
                run_id="run-1",
                target_domain="example.com",
                cc_release="CC-MAIN-2026-10",
                mode="domains",
                schedule="background_wait",
                status="success",
                cache_state="warm_edges",
                target_found_flag=1,
                harmonic_centrality=12.5,
                pagerank=0.000045,
                referring_domain_count=2,
                weighted_referring_domain_score=12345.0,
                avg_referrer_harmonic=5.5,
                avg_referrer_pagerank=0.000011,
                top_referrer_concentration=0.62,
                comparison_domain_count=1,
                query_elapsed_ms=210,
                background_started_at="2026-01-01T00:00:00+00:00",
                background_finished_at="2026-01-01T00:00:01+00:00",
                notes_json='{"status":"success"}',
            )
        ]
    )
    storage.insert_offsite_commoncrawl_linking_domains(
        [
            OffsiteCommonCrawlLinkingDomainRecord(
                run_id="run-1",
                target_domain="example.com",
                linking_domain="ref-a.com",
                source_num_hosts=10,
                source_harmonic_centrality=8.1,
                source_pagerank=0.00002,
                rank_bucket="top_10",
                evidence_json='{"release":"CC-MAIN-2026-10"}',
            ),
            OffsiteCommonCrawlLinkingDomainRecord(
                run_id="run-1",
                target_domain="example.com",
                linking_domain="ref-b.com",
                source_num_hosts=20,
                source_harmonic_centrality=8.1,
                source_pagerank=0.00001,
                rank_bucket="top_25",
                evidence_json='{"release":"CC-MAIN-2026-10"}',
            ),
        ]
    )
    storage.insert_offsite_commoncrawl_comparisons(
        [
            OffsiteCommonCrawlComparisonRecord(
                run_id="run-1",
                target_domain="example.com",
                compare_domain="competitor.com",
                cc_release="CC-MAIN-2026-10",
                harmonic_centrality=11.0,
                pagerank=0.00004,
                rank_gap_vs_target=-1.5,
                pagerank_gap_vs_target=-0.000005,
            )
        ]
    )

    summary_rows = storage.query("SELECT * FROM offsite_commoncrawl_summary WHERE run_id = ?", ("run-1",))
    linking_rows = storage.query("SELECT * FROM offsite_commoncrawl_linking_domains WHERE run_id = ?", ("run-1",))
    comparison_rows = storage.query("SELECT * FROM offsite_commoncrawl_comparisons WHERE run_id = ?", ("run-1",))

    assert len(summary_rows) == 1
    assert len(linking_rows) == 2
    assert len(comparison_rows) == 1
    assert int(summary_rows[0]["target_found_flag"]) == 1

    out = tmp_path / "out"
    storage.export_csvs(out, run_id="run-1")
    assert (out / "offsite_commoncrawl_summary.csv").exists()
    assert (out / "offsite_commoncrawl_linking_domains.csv").exists()
    assert (out / "offsite_commoncrawl_comparisons.csv").exists()

    with (out / "offsite_commoncrawl_linking_domains.csv").open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert rows[0]["run_id"] == "run-1"

    storage.close()
