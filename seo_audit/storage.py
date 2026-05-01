from __future__ import annotations
from dataclasses import astuple, fields as dataclass_fields

import csv
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from seo_audit.models import (
    AIVisibilityRecord,
    ArtifactCacheRecord,
    BodyBlobRecord,
    CitationEventRecord,
    CruxRecord,
    CrawlFetchRecord,
    IndexStateHistoryRecord,
    IssueRecord,
    LighthouseRecord,
    LinkRecord,
    MediaAssetRecord,
    PageGraphMetricsRecord,
    PageRecord,
    PageSnapshotRecord,
    PerformanceRecord,
    RenderSessionRecord,
    SchemaValidationRecord,
    ScoreRecord,
    SubmissionEventRecord,
    TemplateClusterRecord,
    OffsiteCommonCrawlSummaryRecord,
    OffsiteCommonCrawlLinkingDomainRecord,
    OffsiteCommonCrawlComparisonRecord,
    OffsiteCommonCrawlCompetitorRecord,
    URLStateRecord,
    PageDiffRecord,
)


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    domain TEXT NOT NULL,
    config_json TEXT NOT NULL,
    status TEXT NOT NULL,
    notes TEXT
);
CREATE TABLE IF NOT EXISTS schema_meta (
    meta_key TEXT PRIMARY KEY,
    meta_value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS schema_migrations (
    migration_id INTEGER PRIMARY KEY AUTOINCREMENT,
    version INTEGER NOT NULL UNIQUE,
    name TEXT NOT NULL,
    applied_at TEXT NOT NULL,
    success INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS pages (
    page_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    discovered_url TEXT NOT NULL,
    discovered_via TEXT,
    discovered_from_url TEXT,
    normalized_url TEXT NOT NULL,
    final_url TEXT,
    status_code INTEGER,
    content_type TEXT,
    fetch_error TEXT,
    fetch_time_ms INTEGER,
    extract_time_ms INTEGER,
    html_bytes INTEGER,
    redirect_chain_json TEXT,
    canonical_url TEXT,
    canonical_count INTEGER,
    canonical_urls_json TEXT,
    raw_canonical_urls_json TEXT,
    title TEXT,
    raw_title TEXT,
    meta_description TEXT,
    raw_meta_description TEXT,
    meta_robots TEXT,
    x_robots_tag TEXT,
    effective_robots_json TEXT,
    h1 TEXT,
    h2_json TEXT,
    heading_outline_json TEXT,
    word_count INTEGER,
    language TEXT,
    og_title TEXT,
    og_description TEXT,
    og_url TEXT,
    twitter_title TEXT,
    twitter_description TEXT,
    schema_types_json TEXT,
    schema_summary_json TEXT,
    schema_summary_types_json TEXT,
    schema_parse_error_count INTEGER,
    hreflang_links_json TEXT,
    raw_hreflang_links_json TEXT,
    hreflang_count INTEGER,
    rel_next_url TEXT,
    rel_prev_url TEXT,
    data_nosnippet_count INTEGER,
    has_nosnippet_directive INTEGER,
    max_snippet_directive TEXT,
    max_image_preview_directive TEXT,
    max_video_preview_directive TEXT,
    image_count INTEGER,
    image_alt_coverage REAL,
    internal_links_out INTEGER,
    external_links_out INTEGER,
    last_modified TEXT,
    robots_blocked_flag INTEGER,
    in_sitemap_flag INTEGER,
    is_indexable INTEGER,
    is_noindex INTEGER,
    is_nofollow INTEGER,
    page_type TEXT,
    thin_content_flag INTEGER,
    content_hash TEXT,
    raw_content_hash TEXT,
    duplicate_title_flag INTEGER,
    duplicate_description_flag INTEGER,
    render_checked INTEGER,
    raw_word_count INTEGER,
    rendered_word_count INTEGER,
    render_gap_score INTEGER,
    render_gap_reason TEXT,
        crawl_policy_class TEXT,
        crawl_policy_reason TEXT,
    crawl_depth INTEGER,
    nav_linked_flag INTEGER,
    orphan_risk_flag INTEGER,
    raw_canonical TEXT,
    raw_h1_count INTEGER,
    raw_text_len INTEGER,
    raw_links_json TEXT,
    rendered_title TEXT,
        rendered_meta_description TEXT,
    rendered_canonical TEXT,
        rendered_canonical_urls_json TEXT,
        rendered_canonical_count INTEGER,
    rendered_h1_count INTEGER,
    rendered_h1s_json TEXT,
    rendered_text_len INTEGER,
    rendered_links_json TEXT,
        rendered_hreflang_links_json TEXT,
        rendered_content_hash TEXT,
    rendered_effective_robots_json TEXT,
    rendered_network_requests_json TEXT,
    rendered_api_endpoints_json TEXT,
    rendered_wait_profile TEXT,
    rendered_interaction_count INTEGER,
    rendered_action_recipe TEXT,
    rendered_discovery_links_out INTEGER,
    effective_title TEXT,
    effective_meta_description TEXT,
    effective_canonical TEXT,
    effective_hreflang_links_json TEXT,
    effective_content_hash TEXT,
    effective_field_provenance_json TEXT,
    canonical_cluster_key TEXT,
    canonical_cluster_role TEXT,
    canonical_signal_summary_json TEXT,
    effective_h1_count INTEGER,
    effective_text_len INTEGER,
    effective_links_json TEXT,
    effective_internal_links_out INTEGER,
    used_render INTEGER,
    render_reason TEXT,
    render_error TEXT,
    framework_guess TEXT,
    shell_score INTEGER,
    likely_js_shell INTEGER,
    shell_state TEXT,
    shell_signals_json TEXT,
    measurement_status TEXT,
    measurement_error_family TEXT,
    platform_family TEXT,
    platform_confidence INTEGER,
    platform_signals_json TEXT,
    platform_template_hint TEXT,
    governance_googlebot_allowed INTEGER,
    governance_bingbot_allowed INTEGER,
    governance_openai_allowed INTEGER,
    governance_google_extended_allowed INTEGER,
    governance_gptbot_allowed INTEGER,
    governance_oai_adsbot_allowed INTEGER,
    governance_chatgpt_user_allowed INTEGER,
    governance_matrix_json TEXT,
    ai_discoverability_potential_score INTEGER,
    ai_visibility_json TEXT,
    citation_eligibility_score INTEGER,
    citation_evidence_json TEXT,
    image_details_json TEXT,
    image_discoverability_score INTEGER,
    video_details_json TEXT,
    video_discoverability_score INTEGER,
    schema_graph_json TEXT,
    schema_validation_json TEXT,
    schema_validation_score INTEGER,
    render_failure_family TEXT,
    rendered_console_errors_json TEXT,
    rendered_console_warnings_json TEXT,
    rendered_js_endpoints_json TEXT,
    frontier_priority_score REAL,
    frontier_cluster_key TEXT,
    frontier_cluster_rank INTEGER,
    changed_since_last_run INTEGER
);
CREATE TABLE IF NOT EXISTS links (
    link_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    source_url TEXT NOT NULL,
    target_url TEXT NOT NULL,
    normalized_target_url TEXT NOT NULL,
    is_internal INTEGER NOT NULL,
    anchor_text TEXT,
    nofollow_flag INTEGER,
    source_context TEXT,
    dom_region TEXT
);
CREATE TABLE IF NOT EXISTS issues (
    issue_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    url TEXT NOT NULL,
    severity TEXT NOT NULL,
    issue_code TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    evidence_json TEXT,
    issue_provenance TEXT DEFAULT 'both',
    technical_seo_gate TEXT DEFAULT 'indexability',
    verification_status TEXT DEFAULT 'automated',
    confidence_score INTEGER DEFAULT 100,
    certainty_state TEXT DEFAULT 'Verified',
    priority_score INTEGER DEFAULT 0,
    page_importance REAL DEFAULT 1.0,
    reach TEXT DEFAULT 'single',
    urgency REAL DEFAULT 1.0,
    affected_count INTEGER DEFAULT 1,
    affected_ratio REAL DEFAULT 0.0,
    template_cluster TEXT DEFAULT '',
    affected_page_types TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS scores (
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
    explanation_json TEXT DEFAULT '{}',
    scoring_model_version TEXT,
    scoring_profile TEXT,
    score_explanation_json TEXT
);
CREATE TABLE IF NOT EXISTS page_graph_metrics (
    graph_metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    url TEXT NOT NULL,
    internal_pagerank REAL NOT NULL,
    betweenness REAL NOT NULL,
    closeness REAL NOT NULL,
    community_id INTEGER NOT NULL,
    bridge_flag INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS performance_metrics (
    metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    url TEXT NOT NULL,
    strategy TEXT NOT NULL,
    source TEXT NOT NULL,
    performance_score INTEGER,
    accessibility_score INTEGER,
    best_practices_score INTEGER,
    seo_score INTEGER,
    lcp REAL,
    cls REAL,
    inp REAL,
    ttfb REAL,
    field_data_available INTEGER,
    payload_json TEXT
);
CREATE TABLE IF NOT EXISTS lighthouse_metrics (
    lighthouse_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    url TEXT NOT NULL,
    form_factor TEXT NOT NULL,
    status TEXT NOT NULL,
    performance_score INTEGER,
    accessibility_score INTEGER,
    best_practices_score INTEGER,
    seo_score INTEGER,
    lcp REAL,
    cls REAL,
    inp REAL,
    ttfb REAL,
    total_blocking_time REAL,
    speed_index REAL,
    budget_pass INTEGER NOT NULL,
    budget_failures_json TEXT,
    payload_json TEXT,
    error_message TEXT
);
CREATE TABLE IF NOT EXISTS crux_metrics (
    crux_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    url TEXT NOT NULL,
    query_scope TEXT NOT NULL,
    status TEXT NOT NULL,
    source TEXT NOT NULL,
    origin_fallback_used INTEGER NOT NULL,
    lcp_p75 REAL,
    cls_p75 REAL,
    inp_p75 REAL,
    fcp_p75 REAL,
    ttfb_p75 REAL,
    payload_json TEXT,
    error_message TEXT
);
CREATE TABLE IF NOT EXISTS sitemap_entries (
    entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    sitemap_url TEXT NOT NULL,
    url TEXT NOT NULL,
    entry_kind TEXT,
    lastmod TEXT,
    sitemap_lastmod TEXT,
    changefreq TEXT,
    priority TEXT,
    extensions_json TEXT,
    hreflang_links_json TEXT,
    namespace_decls_json TEXT
);
CREATE TABLE IF NOT EXISTS robots_rules (
    rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    robots_url TEXT NOT NULL,
    user_agent TEXT NOT NULL,
    directive TEXT NOT NULL,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS run_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    event_time TEXT NOT NULL,
    event_type TEXT NOT NULL,
    stage TEXT NOT NULL,
    message TEXT NOT NULL,
    elapsed_ms INTEGER NOT NULL,
    detail_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS crawl_fetches (
    fetch_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    url TEXT NOT NULL,
    status_code INTEGER NOT NULL,
    fetch_time_ms INTEGER NOT NULL,
    content_type TEXT NOT NULL,
    response_bytes INTEGER NOT NULL,
    fetched_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS page_snapshots (
    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    url TEXT NOT NULL,
    content_hash TEXT,
    raw_content_hash TEXT,
    rendered_content_hash TEXT,
    effective_content_hash TEXT,
    last_modified TEXT,
    status_code INTEGER NOT NULL,
    changed_flag INTEGER NOT NULL,
    observed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS url_state (
    url_key TEXT PRIMARY KEY,
    normalized_url TEXT NOT NULL UNIQUE,
    last_final_url TEXT,
    etag TEXT,
    last_modified TEXT,
    last_status_code INTEGER,
    last_content_type TEXT,
    last_body_sha256 TEXT,
    last_extracted_sha256 TEXT,
    last_fetched_at TEXT,
    last_seen_run_id TEXT,
    not_modified_streak INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS body_blobs (
    body_sha256 TEXT PRIMARY KEY,
    storage_path TEXT NOT NULL,
    byte_count INTEGER NOT NULL,
    content_encoding TEXT
);
CREATE TABLE IF NOT EXISTS artifact_cache (
    artifact_sha256 TEXT PRIMARY KEY,
    body_sha256 TEXT NOT NULL,
    extractor_version TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    artifact_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS page_diffs (
    page_diff_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    url TEXT NOT NULL,
    diff_family TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    severity TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS render_sessions (
    session_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    url TEXT NOT NULL,
    used_render INTEGER NOT NULL,
    wait_profile TEXT,
    interaction_count INTEGER,
    action_recipe TEXT,
    failure_family TEXT,
    console_errors_json TEXT,
    console_warnings_json TEXT,
    network_endpoints_json TEXT
);
CREATE TABLE IF NOT EXISTS schema_validations (
    schema_validation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    url TEXT NOT NULL,
    validation_score INTEGER NOT NULL,
    findings_json TEXT NOT NULL,
    raw_render_diff_json TEXT
);
CREATE TABLE IF NOT EXISTS media_assets (
    media_asset_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    url TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    asset_url TEXT NOT NULL,
    discoverability_score INTEGER NOT NULL,
    metadata_json TEXT
);
CREATE TABLE IF NOT EXISTS index_state_history (
    index_state_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    url TEXT NOT NULL,
    source TEXT NOT NULL,
    status TEXT NOT NULL,
    state_payload_json TEXT
);
CREATE TABLE IF NOT EXISTS citation_events (
    citation_event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    url TEXT NOT NULL,
    eligibility_score INTEGER NOT NULL,
    evidence_json TEXT
);
CREATE TABLE IF NOT EXISTS ai_visibility_events (
    ai_visibility_event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    url TEXT NOT NULL,
    potential_score INTEGER NOT NULL,
    visibility_json TEXT
);
CREATE TABLE IF NOT EXISTS submission_events (
    submission_event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    url TEXT NOT NULL,
    engine TEXT NOT NULL,
    action TEXT NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT
);
CREATE TABLE IF NOT EXISTS template_clusters (
    template_cluster_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    template_cluster TEXT NOT NULL,
    page_type TEXT NOT NULL,
    url_count INTEGER NOT NULL,
    avg_score REAL NOT NULL,
    issue_count INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS offsite_commoncrawl_summary (
    offsite_summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    target_domain TEXT NOT NULL,
    cc_release TEXT NOT NULL,
    mode TEXT NOT NULL,
    schedule TEXT NOT NULL,
    status TEXT NOT NULL,
    cache_state TEXT NOT NULL,
    target_found_flag INTEGER NOT NULL,
    harmonic_centrality REAL,
    pagerank REAL,
    referring_domain_count INTEGER NOT NULL,
    weighted_referring_domain_score REAL,
    avg_referrer_harmonic REAL,
    avg_referrer_pagerank REAL,
    top_referrer_concentration REAL,
    comparison_domain_count INTEGER NOT NULL,
    competitor_count INTEGER NOT NULL,
    query_elapsed_ms INTEGER NOT NULL,
    background_started_at TEXT,
    background_finished_at TEXT,
    notes_json TEXT
);
CREATE TABLE IF NOT EXISTS offsite_commoncrawl_linking_domains (
    offsite_linking_domain_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    target_domain TEXT NOT NULL,
    linking_domain TEXT NOT NULL,
    source_num_hosts INTEGER NOT NULL,
    source_harmonic_centrality REAL,
    source_pagerank REAL,
    rank_bucket TEXT,
    evidence_json TEXT
);
CREATE TABLE IF NOT EXISTS offsite_commoncrawl_competitors (
    offsite_competitor_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    target_domain TEXT NOT NULL,
    competitor_domain TEXT NOT NULL,
    cc_release TEXT NOT NULL,
    harmonic_centrality REAL,
    pagerank REAL,
    rank_gap_vs_target REAL,
    pagerank_gap_vs_target REAL
);
CREATE TABLE IF NOT EXISTS offsite_commoncrawl_comparisons (
    offsite_comparison_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    target_domain TEXT NOT NULL,
    compare_domain TEXT NOT NULL,
    cc_release TEXT NOT NULL,
    harmonic_centrality REAL,
    pagerank REAL,
    rank_gap_vs_target REAL,
    pagerank_gap_vs_target REAL
);

CREATE INDEX IF NOT EXISTS idx_pages_run_id ON pages(run_id);
CREATE INDEX IF NOT EXISTS idx_pages_run_url ON pages(run_id, normalized_url);
CREATE INDEX IF NOT EXISTS idx_links_run_id ON links(run_id);
CREATE INDEX IF NOT EXISTS idx_links_run_source ON links(run_id, source_url);
CREATE INDEX IF NOT EXISTS idx_links_run_target_internal ON links(run_id, normalized_target_url, is_internal);
CREATE INDEX IF NOT EXISTS idx_issues_run_id ON issues(run_id);
CREATE INDEX IF NOT EXISTS idx_scores_run_id ON scores(run_id);
CREATE INDEX IF NOT EXISTS idx_graph_metrics_run_id ON page_graph_metrics(run_id);
CREATE INDEX IF NOT EXISTS idx_graph_metrics_run_url ON page_graph_metrics(run_id, url);
CREATE INDEX IF NOT EXISTS idx_perf_run_url ON performance_metrics(run_id, url);
CREATE INDEX IF NOT EXISTS idx_lighthouse_run_url ON lighthouse_metrics(run_id, url);
CREATE INDEX IF NOT EXISTS idx_crux_run_url ON crux_metrics(run_id, url);
CREATE INDEX IF NOT EXISTS idx_sitemaps_run_id ON sitemap_entries(run_id);
CREATE INDEX IF NOT EXISTS idx_robots_run_id ON robots_rules(run_id);
CREATE INDEX IF NOT EXISTS idx_run_events_run_id ON run_events(run_id);
CREATE INDEX IF NOT EXISTS idx_crawl_fetches_run_url ON crawl_fetches(run_id, url);
CREATE INDEX IF NOT EXISTS idx_page_snapshots_run_url ON page_snapshots(run_id, url);
CREATE INDEX IF NOT EXISTS idx_url_state_normalized_url ON url_state(normalized_url);
CREATE INDEX IF NOT EXISTS idx_url_state_last_seen_run ON url_state(last_seen_run_id);
CREATE INDEX IF NOT EXISTS idx_artifact_cache_body_type ON artifact_cache(body_sha256, artifact_type, extractor_version);
CREATE INDEX IF NOT EXISTS idx_page_diffs_run_url ON page_diffs(run_id, url);
CREATE INDEX IF NOT EXISTS idx_page_diffs_run_family ON page_diffs(run_id, diff_family);
CREATE INDEX IF NOT EXISTS idx_render_sessions_run_url ON render_sessions(run_id, url);
CREATE INDEX IF NOT EXISTS idx_schema_validations_run_url ON schema_validations(run_id, url);
CREATE INDEX IF NOT EXISTS idx_media_assets_run_url ON media_assets(run_id, url);
CREATE INDEX IF NOT EXISTS idx_index_state_history_run_url ON index_state_history(run_id, url);
CREATE INDEX IF NOT EXISTS idx_citation_events_run_url ON citation_events(run_id, url);
CREATE INDEX IF NOT EXISTS idx_ai_visibility_events_run_url ON ai_visibility_events(run_id, url);
CREATE INDEX IF NOT EXISTS idx_submission_events_run_url ON submission_events(run_id, url);
CREATE INDEX IF NOT EXISTS idx_template_clusters_run_cluster ON template_clusters(run_id, template_cluster);
CREATE INDEX IF NOT EXISTS idx_offsite_commoncrawl_summary_run ON offsite_commoncrawl_summary(run_id);
CREATE INDEX IF NOT EXISTS idx_offsite_commoncrawl_summary_status ON offsite_commoncrawl_summary(run_id, status);
CREATE INDEX IF NOT EXISTS idx_offsite_commoncrawl_linking_domains_run ON offsite_commoncrawl_linking_domains(run_id, target_domain);
CREATE INDEX IF NOT EXISTS idx_offsite_commoncrawl_competitors_run ON offsite_commoncrawl_competitors(run_id, target_domain);
CREATE INDEX IF NOT EXISTS idx_offsite_commoncrawl_comparisons_run ON offsite_commoncrawl_comparisons(run_id, target_domain);
"""

SCHEMA_MIGRATIONS: tuple[tuple[int, str], ...] = (
    (1, "baseline_schema"),
    (2, "canonical_cluster_metadata"),
    (3, "evidence_quality_model"),
)
CURRENT_SCHEMA_VERSION = SCHEMA_MIGRATIONS[-1][0]


class Storage:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        try:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
        except sqlite3.DatabaseError:
            # Keep startup resilient for read-only or constrained SQLite environments.
            pass

    def init_db(self) -> None:
        self.conn.executescript(SCHEMA_SQL)
        self._ensure_schema_version_tables()
        self._migrate_additive_columns()
        self._record_schema_versions()
        self.conn.commit()

    def _ensure_schema_version_tables(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_meta (
                meta_key TEXT PRIMARY KEY,
                meta_value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                migration_id INTEGER PRIMARY KEY AUTOINCREMENT,
                version INTEGER NOT NULL UNIQUE,
                name TEXT NOT NULL,
                applied_at TEXT NOT NULL,
                success INTEGER NOT NULL DEFAULT 1
            )
            """
        )

    def _record_schema_versions(self) -> None:
        now = datetime.now(timezone.utc).isoformat()
        existing_versions = {
            int(row["version"])
            for row in self.query("SELECT version FROM schema_migrations")
            if row["version"] is not None
        }

        for version, name in SCHEMA_MIGRATIONS:
            if version in existing_versions:
                continue
            self.conn.execute(
                "INSERT INTO schema_migrations (version, name, applied_at, success) VALUES (?, ?, ?, 1)",
                (int(version), str(name), now),
            )

        self.conn.execute(
            """
            INSERT INTO schema_meta (meta_key, meta_value, updated_at)
            VALUES ('schema_version', ?, ?)
            ON CONFLICT(meta_key)
            DO UPDATE SET
                meta_value = excluded.meta_value,
                updated_at = excluded.updated_at
            """,
            (str(CURRENT_SCHEMA_VERSION), now),
        )

    def _migrate_additive_columns(self) -> None:
        page_columns = {row["name"] for row in self.query("PRAGMA table_info(pages)")}
        page_additions: list[tuple[str, str]] = [
            ("discovered_via", "TEXT"),
            ("discovered_from_url", "TEXT"),
            ("extract_time_ms", "INTEGER"),
            ("canonical_count", "INTEGER"),
            ("canonical_urls_json", "TEXT"),
            ("raw_canonical_urls_json", "TEXT"),
            ("heading_outline_json", "TEXT"),
            ("effective_robots_json", "TEXT"),
            ("schema_summary_json", "TEXT"),
            ("schema_summary_types_json", "TEXT"),
            ("schema_parse_error_count", "INTEGER"),
            ("hreflang_links_json", "TEXT"),
            ("raw_hreflang_links_json", "TEXT"),
            ("hreflang_count", "INTEGER"),
            ("rel_next_url", "TEXT"),
            ("rel_prev_url", "TEXT"),
            ("data_nosnippet_count", "INTEGER"),
            ("has_nosnippet_directive", "INTEGER"),
            ("max_snippet_directive", "TEXT"),
            ("max_image_preview_directive", "TEXT"),
            ("max_video_preview_directive", "TEXT"),
            ("robots_blocked_flag", "INTEGER"),
            ("in_sitemap_flag", "INTEGER"),
            ("content_hash", "TEXT"),
            ("raw_content_hash", "TEXT"),
            ("raw_title", "TEXT"),
            ("raw_meta_description", "TEXT"),
            ("raw_canonical", "TEXT"),
            ("raw_h1_count", "INTEGER"),
            ("raw_text_len", "INTEGER"),
            ("raw_links_json", "TEXT"),
            ("rendered_title", "TEXT"),
            ("rendered_meta_description", "TEXT"),
            ("rendered_canonical", "TEXT"),
            ("rendered_canonical_urls_json", "TEXT"),
            ("rendered_canonical_count", "INTEGER"),
            ("rendered_h1_count", "INTEGER"),
            ("rendered_h1s_json", "TEXT"),
            ("rendered_text_len", "INTEGER"),
            ("rendered_links_json", "TEXT"),
            ("rendered_hreflang_links_json", "TEXT"),
            ("rendered_content_hash", "TEXT"),
            ("rendered_effective_robots_json", "TEXT"),
            ("rendered_network_requests_json", "TEXT"),
            ("rendered_api_endpoints_json", "TEXT"),
            ("rendered_wait_profile", "TEXT"),
            ("rendered_interaction_count", "INTEGER"),
            ("rendered_action_recipe", "TEXT"),
            ("rendered_discovery_links_out", "INTEGER"),
            ("effective_title", "TEXT"),
            ("effective_meta_description", "TEXT"),
            ("effective_canonical", "TEXT"),
            ("effective_hreflang_links_json", "TEXT"),
            ("effective_content_hash", "TEXT"),
            ("effective_field_provenance_json", "TEXT"),
            ("canonical_cluster_key", "TEXT"),
            ("canonical_cluster_role", "TEXT"),
            ("canonical_signal_summary_json", "TEXT"),
            ("effective_h1_count", "INTEGER"),
            ("effective_text_len", "INTEGER"),
            ("effective_links_json", "TEXT"),
            ("effective_internal_links_out", "INTEGER"),
            ("used_render", "INTEGER"),
            ("render_reason", "TEXT"),
            ("render_error", "TEXT"),
            ("framework_guess", "TEXT"),
            ("shell_score", "INTEGER"),
            ("likely_js_shell", "INTEGER"),
            ("shell_state", "TEXT"),
            ("shell_signals_json", "TEXT"),
            ("measurement_status", "TEXT"),
            ("measurement_error_family", "TEXT"),
            ("crawl_policy_class", "TEXT"),
            ("crawl_policy_reason", "TEXT"),
            ("platform_family", "TEXT"),
            ("platform_confidence", "INTEGER"),
            ("platform_signals_json", "TEXT"),
            ("platform_template_hint", "TEXT"),
            ("governance_googlebot_allowed", "INTEGER"),
            ("governance_bingbot_allowed", "INTEGER"),
            ("governance_openai_allowed", "INTEGER"),
            ("governance_google_extended_allowed", "INTEGER"),
            ("governance_gptbot_allowed", "INTEGER"),
            ("governance_oai_adsbot_allowed", "INTEGER"),
            ("governance_chatgpt_user_allowed", "INTEGER"),
            ("governance_matrix_json", "TEXT"),
            ("ai_discoverability_potential_score", "INTEGER"),
            ("ai_visibility_json", "TEXT"),
            ("citation_eligibility_score", "INTEGER"),
            ("citation_evidence_json", "TEXT"),
            ("image_details_json", "TEXT"),
            ("image_discoverability_score", "INTEGER"),
            ("video_details_json", "TEXT"),
            ("video_discoverability_score", "INTEGER"),
            ("schema_graph_json", "TEXT"),
            ("schema_validation_json", "TEXT"),
            ("schema_validation_score", "INTEGER"),
            ("render_failure_family", "TEXT"),
            ("rendered_console_errors_json", "TEXT"),
            ("rendered_console_warnings_json", "TEXT"),
            ("rendered_js_endpoints_json", "TEXT"),
            ("frontier_priority_score", "REAL"),
            ("frontier_cluster_key", "TEXT"),
            ("frontier_cluster_rank", "INTEGER"),
            ("changed_since_last_run", "INTEGER"),
        ]
        for column, column_type in page_additions:
            if column not in page_columns:
                self.conn.execute(f"ALTER TABLE pages ADD COLUMN {column} {column_type}")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_pages_run_content_hash ON pages(run_id, content_hash)")
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pages_run_effective_content_hash ON pages(run_id, effective_content_hash)"
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_pages_run_canonical_cluster ON pages(run_id, canonical_cluster_key)")

        snapshot_columns = {row["name"] for row in self.query("PRAGMA table_info(page_snapshots)")}
        snapshot_additions: list[tuple[str, str]] = [
            ("raw_content_hash", "TEXT"),
            ("rendered_content_hash", "TEXT"),
            ("effective_content_hash", "TEXT"),
        ]
        for column, column_type in snapshot_additions:
            if column not in snapshot_columns:
                self.conn.execute(f"ALTER TABLE page_snapshots ADD COLUMN {column} {column_type}")

        link_columns = {row["name"] for row in self.query("PRAGMA table_info(links)")}
        if "dom_region" not in link_columns:
            self.conn.execute("ALTER TABLE links ADD COLUMN dom_region TEXT")

        issue_columns = {row["name"] for row in self.query("PRAGMA table_info(issues)")}
        if "issue_provenance" not in issue_columns:
            self.conn.execute("ALTER TABLE issues ADD COLUMN issue_provenance TEXT DEFAULT 'both'")
        if "technical_seo_gate" not in issue_columns:
            self.conn.execute("ALTER TABLE issues ADD COLUMN technical_seo_gate TEXT DEFAULT 'indexability'")
        if "verification_status" not in issue_columns:
            self.conn.execute("ALTER TABLE issues ADD COLUMN verification_status TEXT DEFAULT 'automated'")
        if "confidence_score" not in issue_columns:
            self.conn.execute("ALTER TABLE issues ADD COLUMN confidence_score INTEGER DEFAULT 100")
        if "certainty_state" not in issue_columns:
            self.conn.execute("ALTER TABLE issues ADD COLUMN certainty_state TEXT DEFAULT 'Verified'")
        if "priority_score" not in issue_columns:
            self.conn.execute("ALTER TABLE issues ADD COLUMN priority_score INTEGER DEFAULT 0")
        if "page_importance" not in issue_columns:
            self.conn.execute("ALTER TABLE issues ADD COLUMN page_importance REAL DEFAULT 1.0")
        if "reach" not in issue_columns:
            self.conn.execute("ALTER TABLE issues ADD COLUMN reach TEXT DEFAULT 'single'")
        if "urgency" not in issue_columns:
            self.conn.execute("ALTER TABLE issues ADD COLUMN urgency REAL DEFAULT 1.0")
        if "affected_count" not in issue_columns:
            self.conn.execute("ALTER TABLE issues ADD COLUMN affected_count INTEGER DEFAULT 1")
        if "affected_ratio" not in issue_columns:
            self.conn.execute("ALTER TABLE issues ADD COLUMN affected_ratio REAL DEFAULT 0.0")
        if "template_cluster" not in issue_columns:
            self.conn.execute("ALTER TABLE issues ADD COLUMN template_cluster TEXT DEFAULT ''")
        if "affected_page_types" not in issue_columns:
            self.conn.execute("ALTER TABLE issues ADD COLUMN affected_page_types TEXT DEFAULT ''")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_issues_run_priority ON issues(run_id, priority_score DESC)")

        score_columns = {row["name"] for row in self.query("PRAGMA table_info(scores)")}
        if "quality_score" not in score_columns:
            self.conn.execute("ALTER TABLE scores ADD COLUMN quality_score INTEGER DEFAULT 0")
        if "risk_score" not in score_columns:
            self.conn.execute("ALTER TABLE scores ADD COLUMN risk_score INTEGER DEFAULT 0")
        if "coverage_score" not in score_columns:
            self.conn.execute("ALTER TABLE scores ADD COLUMN coverage_score INTEGER DEFAULT 0")
        if "score_cap" not in score_columns:
            self.conn.execute("ALTER TABLE scores ADD COLUMN score_cap INTEGER DEFAULT 100")
        if "score_version" not in score_columns:
            self.conn.execute("ALTER TABLE scores ADD COLUMN score_version TEXT DEFAULT '1.0.0'")
        if "score_profile" not in score_columns:
            self.conn.execute("ALTER TABLE scores ADD COLUMN score_profile TEXT DEFAULT 'default'")
        if "explanation_json" not in score_columns:
            self.conn.execute("ALTER TABLE scores ADD COLUMN explanation_json TEXT DEFAULT '{}'")
        if "scoring_model_version" not in score_columns:
            self.conn.execute("ALTER TABLE scores ADD COLUMN scoring_model_version TEXT")
        if "scoring_profile" not in score_columns:
            self.conn.execute("ALTER TABLE scores ADD COLUMN scoring_profile TEXT")
        if "score_explanation_json" not in score_columns:
            self.conn.execute("ALTER TABLE scores ADD COLUMN score_explanation_json TEXT")

        sitemap_columns = {row["name"] for row in self.query("PRAGMA table_info(sitemap_entries)")}
        sitemap_additions: list[tuple[str, str]] = [
            ("entry_kind", "TEXT"),
            ("sitemap_lastmod", "TEXT"),
            ("extensions_json", "TEXT"),
            ("hreflang_links_json", "TEXT"),
            ("namespace_decls_json", "TEXT"),
        ]
        for column, column_type in sitemap_additions:
            if column not in sitemap_columns:
                self.conn.execute(f"ALTER TABLE sitemap_entries ADD COLUMN {column} {column_type}")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_sitemaps_run_kind ON sitemap_entries(run_id, entry_kind)")

        offsite_summary_columns = {row["name"] for row in self.query("PRAGMA table_info(offsite_commoncrawl_summary)")}
        if "comparison_domain_count" not in offsite_summary_columns:
            self.conn.execute(
                "ALTER TABLE offsite_commoncrawl_summary ADD COLUMN comparison_domain_count INTEGER NOT NULL DEFAULT 0"
            )
            self.conn.execute(
                "UPDATE offsite_commoncrawl_summary SET comparison_domain_count = COALESCE(competitor_count, 0)"
            )
        if "competitor_count" not in offsite_summary_columns:
            self.conn.execute(
                "ALTER TABLE offsite_commoncrawl_summary ADD COLUMN competitor_count INTEGER NOT NULL DEFAULT 0"
            )
            self.conn.execute(
                "UPDATE offsite_commoncrawl_summary SET competitor_count = COALESCE(comparison_domain_count, 0)"
            )

    def _blob_root_dir(self) -> Path:
        return self.db_path.parent / "cache" / "blobs"

    def blob_path_for_sha(self, body_sha256: str) -> Path:
        normalized = str(body_sha256 or "").strip().lower()
        if len(normalized) < 8:
            raise ValueError("body_sha256 must be a sha256 hex string")
        return self._blob_root_dir() / normalized[:2] / normalized[2:4] / f"{normalized}.bin"

    def _resolve_blob_storage_path(self, storage_path: str) -> Path:
        path = Path(storage_path)
        if path.is_absolute():
            return path
        return self.db_path.parent / path

    def get_url_state(self, normalized_url: str) -> dict | None:
        url = str(normalized_url or "").strip()
        if not url:
            return None
        row = self.conn.execute(
            "SELECT * FROM url_state WHERE normalized_url = ? LIMIT 1",
            (url,),
        ).fetchone()
        return dict(row) if row else None

    def get_url_states(self, normalized_urls: Iterable[str]) -> dict[str, dict]:
        values = [str(url).strip() for url in normalized_urls if str(url).strip()]
        if not values:
            return {}
        placeholders = ",".join("?" for _ in values)
        rows = self.query(
            f"SELECT * FROM url_state WHERE normalized_url IN ({placeholders})",
            tuple(values),
        )
        return {str(row["normalized_url"]): dict(row) for row in rows}

    def upsert_url_states(self, rows: Iterable[URLStateRecord]) -> None:
        payload = list(rows)
        if not payload:
            return
        self.conn.executemany(
            """
            INSERT INTO url_state (
                url_key,
                normalized_url,
                last_final_url,
                etag,
                last_modified,
                last_status_code,
                last_content_type,
                last_body_sha256,
                last_extracted_sha256,
                last_fetched_at,
                last_seen_run_id,
                not_modified_streak
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(url_key) DO UPDATE SET
                normalized_url = excluded.normalized_url,
                last_final_url = excluded.last_final_url,
                etag = excluded.etag,
                last_modified = excluded.last_modified,
                last_status_code = excluded.last_status_code,
                last_content_type = excluded.last_content_type,
                last_body_sha256 = excluded.last_body_sha256,
                last_extracted_sha256 = excluded.last_extracted_sha256,
                last_fetched_at = excluded.last_fetched_at,
                last_seen_run_id = excluded.last_seen_run_id,
                not_modified_streak = excluded.not_modified_streak
            """,
            [astuple(row) for row in payload],
        )
        self.conn.commit()

    def ensure_body_blob(self, body_sha256: str, body: bytes, *, content_encoding: str = "") -> BodyBlobRecord:
        normalized = str(body_sha256 or "").strip().lower()
        path = self.blob_path_for_sha(normalized)
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_bytes(body)
        relative_path = path.relative_to(self.db_path.parent)
        record = BodyBlobRecord(
            body_sha256=normalized,
            storage_path=relative_path.as_posix(),
            byte_count=len(body),
            content_encoding=str(content_encoding or ""),
        )
        self.conn.execute(
            """
            INSERT INTO body_blobs (body_sha256, storage_path, byte_count, content_encoding)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(body_sha256) DO UPDATE SET
                storage_path = excluded.storage_path,
                byte_count = excluded.byte_count,
                content_encoding = excluded.content_encoding
            """,
            astuple(record),
        )
        self.conn.commit()
        return record

    def read_body_blob(self, body_sha256: str) -> bytes | None:
        normalized = str(body_sha256 or "").strip().lower()
        if not normalized:
            return None
        row = self.conn.execute(
            "SELECT storage_path FROM body_blobs WHERE body_sha256 = ? LIMIT 1",
            (normalized,),
        ).fetchone()
        if row is None:
            return None
        file_path = self._resolve_blob_storage_path(str(row["storage_path"] or ""))
        if not file_path.exists():
            return None
        return file_path.read_bytes()

    def upsert_artifact_cache(self, rows: Iterable[ArtifactCacheRecord]) -> None:
        payload = list(rows)
        if not payload:
            return
        self.conn.executemany(
            """
            INSERT INTO artifact_cache (artifact_sha256, body_sha256, extractor_version, artifact_type, artifact_json)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(artifact_sha256) DO UPDATE SET
                body_sha256 = excluded.body_sha256,
                extractor_version = excluded.extractor_version,
                artifact_type = excluded.artifact_type,
                artifact_json = excluded.artifact_json
            """,
            [astuple(row) for row in payload],
        )
        self.conn.commit()

    def get_artifact_cache(self, body_sha256: str, artifact_type: str, extractor_version: str) -> dict | None:
        row = self.conn.execute(
            """
            SELECT *
            FROM artifact_cache
            WHERE body_sha256 = ?
              AND artifact_type = ?
              AND extractor_version = ?
            ORDER BY artifact_sha256 DESC
            LIMIT 1
            """,
            (
                str(body_sha256 or "").strip().lower(),
                str(artifact_type or "").strip(),
                str(extractor_version or "").strip(),
            ),
        ).fetchone()
        return dict(row) if row else None

    def insert_page_diffs(self, rows: Iterable[PageDiffRecord]) -> None:
        payload = list(rows)
        if not payload:
            return
        self.conn.executemany(
            "INSERT INTO page_diffs (run_id, url, diff_family, old_value, new_value, severity) VALUES (?, ?, ?, ?, ?, ?)",
            [astuple(row) for row in payload],
        )
        self.conn.commit()

    def list_known_sitemap_urls(self, *, limit: int = 5000) -> list[str]:
        rows = self.query(
            """
            SELECT url
            FROM sitemap_entries
            WHERE COALESCE(entry_kind, 'url') = 'url'
            ORDER BY entry_id DESC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        )
        urls: list[str] = []
        seen: set[str] = set()
        for row in rows:
            url = str(row["url"] or "").strip()
            if not url or url in seen:
                continue
            seen.add(url)
            urls.append(url)
        return urls

    def list_recent_changed_urls(self, *, limit: int = 5000) -> list[str]:
        rows = self.query(
            "SELECT url FROM page_snapshots WHERE changed_flag = 1 ORDER BY snapshot_id DESC LIMIT ?",
            (max(1, int(limit)),),
        )
        urls: list[str] = []
        seen: set[str] = set()
        for row in rows:
            url = str(row["url"] or "").strip()
            if not url or url in seen:
                continue
            seen.add(url)
            urls.append(url)
        return urls

    def latest_run_config(self, *, exclude_run_id: str = "") -> dict:
        if exclude_run_id:
            row = self.conn.execute(
                "SELECT config_json FROM runs WHERE run_id != ? ORDER BY rowid DESC LIMIT 1",
                (exclude_run_id,),
            ).fetchone()
        else:
            row = self.conn.execute(
                "SELECT config_json FROM runs ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
        if row is None:
            return {}
        raw = str(row["config_json"] or "{}").strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def schema_version(self) -> int:
        row = self.conn.execute(
            "SELECT meta_value FROM schema_meta WHERE meta_key = 'schema_version'"
        ).fetchone()
        if row is None:
            return 0
        try:
            return int(str(row["meta_value"] or "0"))
        except (TypeError, ValueError):
            return 0

    def close(self) -> None:
        self.conn.close()

    def insert_run(self, run_id: str, started_at: str, domain: str, config: dict, status: str) -> None:
        self.conn.execute(
            "INSERT INTO runs (run_id, started_at, domain, config_json, status, notes) VALUES (?, ?, ?, ?, ?, '')",
            (run_id, started_at, domain, json.dumps(config), status),
        )
        self.conn.commit()

    def update_run_completion(self, run_id: str, completed_at: str, status: str, notes: str = "") -> None:
        self.conn.execute(
            "UPDATE runs SET completed_at = ?, status = ?, notes = ? WHERE run_id = ?",
            (completed_at, status, notes, run_id),
        )
        self.conn.commit()

    def insert_pages(self, pages: Iterable[PageRecord]) -> None:
        rows = list(pages)
        if not rows:
            return
        page_columns = [f.name for f in dataclass_fields(PageRecord)]
        placeholders = ",".join("?" for _ in page_columns)
        sql = f"INSERT INTO pages ({', '.join(page_columns)}) VALUES ({placeholders})"
        self.conn.executemany(sql, [astuple(page) for page in rows])
        self.conn.commit()

    def insert_links(self, links: Iterable[LinkRecord]) -> None:
        self.conn.executemany(
            "INSERT INTO links (run_id, source_url, target_url, normalized_target_url, is_internal, anchor_text, nofollow_flag, source_context, dom_region) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [astuple(link) for link in links],
        )
        self.conn.commit()

    def insert_issues(self, issues: Iterable[IssueRecord]) -> None:
        self.conn.executemany(
            "INSERT INTO issues (run_id, url, severity, issue_code, title, description, evidence_json, issue_provenance, technical_seo_gate, verification_status, confidence_score, certainty_state, priority_score, page_importance, reach, urgency, affected_count, affected_ratio, template_cluster, affected_page_types) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [astuple(issue) for issue in issues],
        )
        self.conn.commit()

    def insert_scores(self, scores: Iterable[ScoreRecord]) -> None:
        self.conn.executemany(
            "INSERT INTO scores (run_id, url, crawlability_score, onpage_score, render_risk_score, internal_linking_score, local_seo_score, performance_score, overall_score, quality_score, risk_score, coverage_score, score_cap, score_version, score_profile, explanation_json, scoring_model_version, scoring_profile, score_explanation_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [astuple(score) for score in scores],
        )
        self.conn.commit()

    def insert_page_graph_metrics(self, rows: Iterable[PageGraphMetricsRecord]) -> None:
        payload = list(rows)
        if not payload:
            return
        self.conn.executemany(
            "INSERT INTO page_graph_metrics (run_id, url, internal_pagerank, betweenness, closeness, community_id, bridge_flag) VALUES (?, ?, ?, ?, ?, ?, ?)",
            [astuple(row) for row in payload],
        )
        self.conn.commit()

    def insert_performance(self, rows: Iterable[PerformanceRecord]) -> None:
        self.conn.executemany(
            "INSERT INTO performance_metrics (run_id, url, strategy, source, performance_score, accessibility_score, best_practices_score, seo_score, lcp, cls, inp, ttfb, field_data_available, payload_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    row.run_id,
                    row.url,
                    row.strategy,
                    row.source,
                    row.performance_score,
                    row.accessibility_score,
                    row.best_practices_score,
                    row.seo_score,
                    row.lcp,
                    row.cls,
                    row.inp,
                    row.ttfb,
                    row.field_data_available,
                    row.payload_json,
                )
                for row in rows
            ],
        )
        self.conn.commit()

    def insert_lighthouse(self, rows: Iterable[LighthouseRecord]) -> None:
        payload = list(rows)
        if not payload:
            return
        self.conn.executemany(
            """
            INSERT INTO lighthouse_metrics (
                run_id,
                url,
                form_factor,
                status,
                performance_score,
                accessibility_score,
                best_practices_score,
                seo_score,
                lcp,
                cls,
                inp,
                ttfb,
                total_blocking_time,
                speed_index,
                budget_pass,
                budget_failures_json,
                payload_json,
                error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [astuple(row) for row in payload],
        )
        self.conn.commit()

    def insert_crux(self, rows: Iterable[CruxRecord]) -> None:
        self.conn.executemany(
            "INSERT INTO crux_metrics (run_id, url, query_scope, status, source, origin_fallback_used, lcp_p75, cls_p75, inp_p75, fcp_p75, ttfb_p75, payload_json, error_message) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    row.run_id,
                    row.url,
                    row.query_scope,
                    row.status,
                    row.source,
                    row.origin_fallback_used,
                    row.lcp_p75,
                    row.cls_p75,
                    row.inp_p75,
                    row.fcp_p75,
                    row.ttfb_p75,
                    row.payload_json,
                    row.error_message,
                )
                for row in rows
            ],
        )
        self.conn.commit()

    def insert_sitemap_entries(self, run_id: str, entries: list[dict]) -> None:
        self.conn.executemany(
            """
            INSERT INTO sitemap_entries (
                run_id,
                sitemap_url,
                url,
                entry_kind,
                lastmod,
                sitemap_lastmod,
                changefreq,
                priority,
                extensions_json,
                hreflang_links_json,
                namespace_decls_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    run_id,
                    e["sitemap_url"],
                    e["url"],
                    e.get("entry_kind", "url"),
                    e.get("lastmod", ""),
                    e.get("sitemap_lastmod", ""),
                    e.get("changefreq", ""),
                    e.get("priority", ""),
                    e.get("extensions_json", "{}"),
                    e.get("hreflang_links_json", "[]"),
                    e.get("namespace_decls_json", "{}"),
                )
                for e in entries
            ],
        )
        self.conn.commit()

    def insert_robots_rules(self, run_id: str, robots_url: str, rules: list[dict]) -> None:
        self.conn.executemany(
            "INSERT INTO robots_rules (run_id, robots_url, user_agent, directive, value) VALUES (?, ?, ?, ?, ?)",
            [(run_id, robots_url, r["user_agent"], r["directive"], r["value"]) for r in rules],
        )
        self.conn.commit()

    def insert_run_events(self, run_id: str, events: list[dict]) -> None:
        if not events:
            return
        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for event in events:
            rows.append(
                (
                    run_id,
                    str(event.get("event_time") or now),
                    str(event.get("event_type") or "event"),
                    str(event.get("stage") or ""),
                    str(event.get("message") or ""),
                    int(event.get("elapsed_ms") or 0),
                    str(event.get("detail_json") or "{}"),
                )
            )
        self.conn.executemany(
            "INSERT INTO run_events (run_id, event_time, event_type, stage, message, elapsed_ms, detail_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        self.conn.commit()

    def insert_crawl_fetches(self, rows: Iterable[CrawlFetchRecord]) -> None:
        payload = list(rows)
        if not payload:
            return
        self.conn.executemany(
            "INSERT INTO crawl_fetches (run_id, url, status_code, fetch_time_ms, content_type, response_bytes, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            [astuple(row) for row in payload],
        )
        self.conn.commit()

    def insert_page_snapshots(self, rows: Iterable[PageSnapshotRecord]) -> None:
        payload = list(rows)
        if not payload:
            return
        self.conn.executemany(
            "INSERT INTO page_snapshots (run_id, url, content_hash, last_modified, status_code, changed_flag, observed_at, raw_content_hash, rendered_content_hash, effective_content_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [astuple(row) for row in payload],
        )
        self.conn.commit()

    def insert_render_sessions(self, rows: Iterable[RenderSessionRecord]) -> None:
        payload = list(rows)
        if not payload:
            return
        self.conn.executemany(
            "INSERT INTO render_sessions (run_id, url, used_render, wait_profile, interaction_count, action_recipe, failure_family, console_errors_json, console_warnings_json, network_endpoints_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [astuple(row) for row in payload],
        )
        self.conn.commit()

    def insert_schema_validations(self, rows: Iterable[SchemaValidationRecord]) -> None:
        payload = list(rows)
        if not payload:
            return
        self.conn.executemany(
            "INSERT INTO schema_validations (run_id, url, validation_score, findings_json, raw_render_diff_json) VALUES (?, ?, ?, ?, ?)",
            [astuple(row) for row in payload],
        )
        self.conn.commit()

    def insert_media_assets(self, rows: Iterable[MediaAssetRecord]) -> None:
        payload = list(rows)
        if not payload:
            return
        self.conn.executemany(
            "INSERT INTO media_assets (run_id, url, asset_type, asset_url, discoverability_score, metadata_json) VALUES (?, ?, ?, ?, ?, ?)",
            [astuple(row) for row in payload],
        )
        self.conn.commit()

    def insert_index_state_history(self, rows: Iterable[IndexStateHistoryRecord]) -> None:
        payload = list(rows)
        if not payload:
            return
        self.conn.executemany(
            "INSERT INTO index_state_history (run_id, url, source, status, state_payload_json) VALUES (?, ?, ?, ?, ?)",
            [astuple(row) for row in payload],
        )
        self.conn.commit()

    def insert_citation_events(self, rows: Iterable[CitationEventRecord]) -> None:
        payload = list(rows)
        if not payload:
            return
        self.conn.executemany(
            "INSERT INTO citation_events (run_id, url, eligibility_score, evidence_json) VALUES (?, ?, ?, ?)",
            [astuple(row) for row in payload],
        )
        self.conn.commit()

    def insert_ai_visibility_events(self, rows: Iterable[AIVisibilityRecord]) -> None:
        payload = list(rows)
        if not payload:
            return
        self.conn.executemany(
            "INSERT INTO ai_visibility_events (run_id, url, potential_score, visibility_json) VALUES (?, ?, ?, ?)",
            [astuple(row) for row in payload],
        )
        self.conn.commit()

    def insert_submission_events(self, rows: Iterable[SubmissionEventRecord]) -> None:
        payload = list(rows)
        if not payload:
            return
        self.conn.executemany(
            "INSERT INTO submission_events (run_id, url, engine, action, status, payload_json) VALUES (?, ?, ?, ?, ?, ?)",
            [astuple(row) for row in payload],
        )
        self.conn.commit()

    def insert_template_clusters(self, rows: Iterable[TemplateClusterRecord]) -> None:
        payload = list(rows)
        if not payload:
            return
        self.conn.executemany(
            "INSERT INTO template_clusters (run_id, template_cluster, page_type, url_count, avg_score, issue_count) VALUES (?, ?, ?, ?, ?, ?)",
            [astuple(row) for row in payload],
        )
        self.conn.commit()

    def insert_offsite_commoncrawl_summary(self, rows: Iterable[OffsiteCommonCrawlSummaryRecord]) -> None:
        payload = list(rows)
        if not payload:
            return
        self.conn.executemany(
            """
            INSERT INTO offsite_commoncrawl_summary (
                run_id,
                target_domain,
                cc_release,
                mode,
                schedule,
                status,
                cache_state,
                target_found_flag,
                harmonic_centrality,
                pagerank,
                referring_domain_count,
                weighted_referring_domain_score,
                avg_referrer_harmonic,
                avg_referrer_pagerank,
                top_referrer_concentration,
                comparison_domain_count,
                competitor_count,
                query_elapsed_ms,
                background_started_at,
                background_finished_at,
                notes_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.run_id,
                    row.target_domain,
                    row.cc_release,
                    row.mode,
                    row.schedule,
                    row.status,
                    row.cache_state,
                    row.target_found_flag,
                    row.harmonic_centrality,
                    row.pagerank,
                    row.referring_domain_count,
                    row.weighted_referring_domain_score,
                    row.avg_referrer_harmonic,
                    row.avg_referrer_pagerank,
                    row.top_referrer_concentration,
                    row.comparison_domain_count,
                    row.comparison_domain_count,
                    row.query_elapsed_ms,
                    row.background_started_at,
                    row.background_finished_at,
                    row.notes_json,
                )
                for row in payload
            ],
        )
        self.conn.commit()

    def insert_offsite_commoncrawl_linking_domains(
        self,
        rows: Iterable[OffsiteCommonCrawlLinkingDomainRecord],
    ) -> None:
        payload = list(rows)
        if not payload:
            return
        self.conn.executemany(
            """
            INSERT INTO offsite_commoncrawl_linking_domains (
                run_id,
                target_domain,
                linking_domain,
                source_num_hosts,
                source_harmonic_centrality,
                source_pagerank,
                rank_bucket,
                evidence_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [astuple(row) for row in payload],
        )
        self.conn.commit()

    def insert_offsite_commoncrawl_comparisons(
        self,
        rows: Iterable[OffsiteCommonCrawlComparisonRecord],
    ) -> None:
        payload = list(rows)
        if not payload:
            return
        self.conn.executemany(
            """
            INSERT INTO offsite_commoncrawl_comparisons (
                run_id,
                target_domain,
                compare_domain,
                cc_release,
                harmonic_centrality,
                pagerank,
                rank_gap_vs_target,
                pagerank_gap_vs_target
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [astuple(row) for row in payload],
        )
        # Maintain legacy compatibility for existing competitor-table readers.
        self.conn.executemany(
            """
            INSERT INTO offsite_commoncrawl_competitors (
                run_id,
                target_domain,
                competitor_domain,
                cc_release,
                harmonic_centrality,
                pagerank,
                rank_gap_vs_target,
                pagerank_gap_vs_target
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.run_id,
                    row.target_domain,
                    row.compare_domain,
                    row.cc_release,
                    row.harmonic_centrality,
                    row.pagerank,
                    row.rank_gap_vs_target,
                    row.pagerank_gap_vs_target,
                )
                for row in payload
            ],
        )
        self.conn.commit()

    def insert_offsite_commoncrawl_competitors(
        self,
        rows: Iterable[OffsiteCommonCrawlCompetitorRecord],
    ) -> None:
        payload = list(rows)
        if not payload:
            return
        normalized_rows = [
            OffsiteCommonCrawlComparisonRecord(
                run_id=str(getattr(row, "run_id", "")),
                target_domain=str(getattr(row, "target_domain", "")),
                compare_domain=str(
                    getattr(row, "compare_domain", "")
                    or getattr(row, "competitor_domain", "")
                ),
                cc_release=str(getattr(row, "cc_release", "")),
                harmonic_centrality=getattr(row, "harmonic_centrality", None),
                pagerank=getattr(row, "pagerank", None),
                rank_gap_vs_target=getattr(row, "rank_gap_vs_target", None),
                pagerank_gap_vs_target=getattr(row, "pagerank_gap_vs_target", None),
            )
            for row in payload
        ]
        self.insert_offsite_commoncrawl_comparisons(normalized_rows)

    def query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        return self.conn.execute(sql, params).fetchall()

    def _table_columns(self, table: str) -> list[str]:
        rows = self.query(f"PRAGMA table_info({table})")
        return [str(row["name"]) for row in rows]

    def purge_provider_payloads_older_than(self, retention_days: int) -> tuple[int, int]:
        if retention_days <= 0:
            return 0, 0

        cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).isoformat()
        perf_cur = self.conn.execute(
            """
            UPDATE performance_metrics
            SET payload_json = '{}'
            WHERE run_id IN (
                SELECT run_id FROM runs WHERE started_at < ?
            )
            AND payload_json IS NOT NULL
            AND TRIM(payload_json) != '{}'
            """,
            (cutoff,),
        )
        crux_cur = self.conn.execute(
            """
            UPDATE crux_metrics
            SET payload_json = '{}'
            WHERE run_id IN (
                SELECT run_id FROM runs WHERE started_at < ?
            )
            AND payload_json IS NOT NULL
            AND TRIM(payload_json) != '{}'
            """,
            (cutoff,),
        )
        self.conn.commit()
        return perf_cur.rowcount if perf_cur.rowcount >= 0 else 0, crux_cur.rowcount if crux_cur.rowcount >= 0 else 0

    def export_csvs(self, out_dir: Path, run_id: str | None = None) -> None:
        out_dir.mkdir(parents=True, exist_ok=True)
        mapping: dict[str, tuple[str, bool]] = {
            "pages.csv": ("pages", True),
            "links.csv": ("links", True),
            "issues.csv": ("issues", True),
            "scores.csv": ("scores", True),
            "page_graph_metrics.csv": ("page_graph_metrics", True),
            "performance.csv": ("performance_metrics", True),
            "lighthouse.csv": ("lighthouse_metrics", True),
            "crux.csv": ("crux_metrics", True),
            "sitemaps.csv": ("sitemap_entries", True),
            "robots_rules.csv": ("robots_rules", True),
            "run_events.csv": ("run_events", True),
            "crawl_fetches.csv": ("crawl_fetches", True),
            "page_snapshots.csv": ("page_snapshots", True),
            "page_diffs.csv": ("page_diffs", True),
            "url_state.csv": ("url_state", False),
            "body_blobs.csv": ("body_blobs", False),
            "artifact_cache.csv": ("artifact_cache", False),
            "render_sessions.csv": ("render_sessions", True),
            "schema_validations.csv": ("schema_validations", True),
            "media_assets.csv": ("media_assets", True),
            "index_state_history.csv": ("index_state_history", True),
            "citation_events.csv": ("citation_events", True),
            "ai_visibility_events.csv": ("ai_visibility_events", True),
            "submission_events.csv": ("submission_events", True),
            "template_clusters.csv": ("template_clusters", True),
            "offsite_commoncrawl_summary.csv": ("offsite_commoncrawl_summary", True),
            "offsite_commoncrawl_linking_domains.csv": ("offsite_commoncrawl_linking_domains", True),
            "offsite_commoncrawl_comparisons.csv": ("offsite_commoncrawl_comparisons", True),
            "offsite_commoncrawl_competitors.csv": ("offsite_commoncrawl_competitors", True),
        }
        for fname, mapping_row in mapping.items():
            table, run_scoped = mapping_row
            if run_id is None or not run_scoped:
                rows = self.query(f"SELECT * FROM {table}")
            else:
                rows = self.query(f"SELECT * FROM {table} WHERE run_id = ?", (run_id,))
            with (out_dir / fname).open("w", newline="", encoding="utf-8") as fh:
                writer = csv.writer(fh)
                columns = list(rows[0].keys()) if rows else self._table_columns(table)
                if columns:
                    writer.writerow(columns)
                for row in rows:
                    writer.writerow(list(row))

    def export_run_events_csv(self, out_dir: Path, run_id: str) -> None:
        out_dir.mkdir(parents=True, exist_ok=True)
        rows = self.query("SELECT * FROM run_events WHERE run_id = ?", (run_id,))
        with (out_dir / "run_events.csv").open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            columns = list(rows[0].keys()) if rows else self._table_columns("run_events")
            if columns:
                writer.writerow(columns)
            for row in rows:
                writer.writerow(list(row))
