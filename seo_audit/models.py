from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class PageRecord:
    run_id: str
    discovered_url: str
    normalized_url: str
    discovered_via: str = "seed"
    discovered_from_url: str = ""
    final_url: str = ""
    status_code: int | None = None
    content_type: str = ""
    fetch_error: str = ""
    fetch_time_ms: int | None = None
    extract_time_ms: int | None = None
    html_bytes: int | None = None
    redirect_chain_json: str = "[]"
    canonical_url: str = ""
    canonical_count: int = 0
    canonical_urls_json: str = "[]"
    raw_canonical_urls_json: str = "[]"
    title: str = ""
    raw_title: str = ""
    meta_description: str = ""
    raw_meta_description: str = ""
    meta_robots: str = ""
    x_robots_tag: str = ""
    effective_robots_json: str = "{}"
    h1: str = ""
    h2_json: str = "[]"
    heading_outline_json: str = "[]"
    word_count: int = 0
    language: str = ""
    og_title: str = ""
    og_description: str = ""
    og_url: str = ""
    twitter_title: str = ""
    twitter_description: str = ""
    schema_types_json: str = "[]"
    schema_summary_json: str = "[]"
    schema_summary_types_json: str = "[]"
    schema_parse_error_count: int = 0
    hreflang_links_json: str = "[]"
    raw_hreflang_links_json: str = "[]"
    hreflang_count: int = 0
    rel_next_url: str = ""
    rel_prev_url: str = ""
    data_nosnippet_count: int = 0
    has_nosnippet_directive: int = 0
    max_snippet_directive: str = ""
    max_image_preview_directive: str = ""
    max_video_preview_directive: str = ""
    image_count: int = 0
    image_alt_coverage: float = 0.0
    internal_links_out: int = 0
    external_links_out: int = 0
    last_modified: str = ""
    robots_blocked_flag: int = 0
    in_sitemap_flag: int = 0
    is_indexable: int = 1
    is_noindex: int = 0
    is_nofollow: int = 0
    page_type: str = "other"
    thin_content_flag: int = 0
    content_hash: str = ""
    raw_content_hash: str = ""
    duplicate_title_flag: int = 0
    duplicate_description_flag: int = 0
    render_checked: int = 0
    raw_word_count: int = 0
    rendered_word_count: int = 0
    render_gap_score: int = 0
    render_gap_reason: str = ""
    crawl_policy_class: str = "crawl_normally"
    crawl_policy_reason: str = "default"
    crawl_depth: int = 0
    nav_linked_flag: int = 0
    orphan_risk_flag: int = 0
    raw_canonical: str = ""
    raw_h1_count: int = 0
    raw_text_len: int = 0
    raw_links_json: str = "[]"
    rendered_title: str = ""
    rendered_meta_description: str = ""
    rendered_canonical: str = ""
    rendered_canonical_urls_json: str = "[]"
    rendered_canonical_count: int = 0
    rendered_h1_count: int = 0
    rendered_h1s_json: str = "[]"
    rendered_text_len: int = 0
    rendered_links_json: str = "[]"
    rendered_hreflang_links_json: str = "[]"
    rendered_content_hash: str = ""
    rendered_effective_robots_json: str = "{}"
    rendered_network_requests_json: str = "[]"
    rendered_api_endpoints_json: str = "[]"
    rendered_wait_profile: str = ""
    rendered_interaction_count: int = 0
    rendered_action_recipe: str = ""
    rendered_discovery_links_out: int = 0
    effective_title: str = ""
    effective_meta_description: str = ""
    effective_canonical: str = ""
    effective_hreflang_links_json: str = "[]"
    effective_content_hash: str = ""
    effective_field_provenance_json: str = "{}"
    canonical_cluster_key: str = ""
    canonical_cluster_role: str = ""
    canonical_signal_summary_json: str = "{}"
    effective_h1_count: int = 0
    effective_text_len: int = 0
    effective_links_json: str = "[]"
    effective_internal_links_out: int = 0
    used_render: int = 0
    render_reason: str = ""
    render_error: str = ""
    framework_guess: str = ""
    shell_score: int = 0
    likely_js_shell: int = 0
    shell_state: str = "raw_shell_unlikely"
    shell_signals_json: str = "{}"
    measurement_status: str = "measurement_not_attempted_by_policy"
    measurement_error_family: str = "not_attempted_policy"
    platform_family: str = ""
    platform_confidence: int = 0
    platform_signals_json: str = "{}"
    platform_template_hint: str = ""
    governance_googlebot_allowed: int = 1
    governance_bingbot_allowed: int = 1
    governance_openai_allowed: int = 1
    governance_google_extended_allowed: int = 1
    governance_gptbot_allowed: int = 1
    governance_oai_adsbot_allowed: int = 1
    governance_chatgpt_user_allowed: int = 1
    governance_matrix_json: str = "{}"
    ai_discoverability_potential_score: int = 0
    ai_visibility_json: str = "{}"
    citation_eligibility_score: int = 0
    citation_evidence_json: str = "{}"
    image_details_json: str = "[]"
    image_discoverability_score: int = 0
    video_details_json: str = "[]"
    video_discoverability_score: int = 0
    schema_graph_json: str = "[]"
    schema_validation_json: str = "{}"
    schema_validation_score: int = 0
    render_failure_family: str = ""
    rendered_console_errors_json: str = "[]"
    rendered_console_warnings_json: str = "[]"
    rendered_js_endpoints_json: str = "[]"
    frontier_priority_score: float = 0.0
    frontier_cluster_key: str = ""
    frontier_cluster_rank: int = 0
    changed_since_last_run: int = 0


@dataclass(slots=True)
class LinkRecord:
    run_id: str
    source_url: str
    target_url: str
    normalized_target_url: str
    is_internal: int
    anchor_text: str = ""
    nofollow_flag: int = 0
    source_context: str = "body"
    dom_region: str = "unknown"


@dataclass(slots=True)
class IssueRecord:
    run_id: str
    url: str
    severity: str
    issue_code: str
    title: str
    description: str
    evidence_json: str = "{}"
    issue_provenance: str = "both"
    technical_seo_gate: str = "indexability"
    verification_status: str = "automated"
    confidence_score: int = 100
    certainty_state: str = "Verified"
    priority_score: int = 0
    page_importance: float = 1.0
    reach: str = "single_page"
    urgency: float = 1.0
    affected_count: int = 1
    affected_ratio: float = 0.0
    template_cluster: str = ""
    affected_page_types: str = ""


@dataclass(slots=True)
class ScoreRecord:
    run_id: str
    url: str
    crawlability_score: int
    onpage_score: int
    render_risk_score: int
    internal_linking_score: int
    local_seo_score: int
    performance_score: int
    overall_score: int
    quality_score: int = 0
    risk_score: int = 0
    coverage_score: int = 0
    score_cap: int = 100
    score_version: str = "1.0.0"
    score_profile: str = "default"
    explanation_json: str = "{}"
    scoring_model_version: str | None = None
    scoring_profile: str | None = None
    score_explanation_json: str | None = None


@dataclass(slots=True)
class PageGraphMetricsRecord:
    run_id: str
    url: str
    internal_pagerank: float
    betweenness: float
    closeness: float
    community_id: int
    bridge_flag: int = 0


@dataclass(slots=True)
class PerformanceRecord:
    run_id: str
    url: str
    strategy: str
    source: str = "pagespeed"
    performance_score: int | None = None
    accessibility_score: int | None = None
    best_practices_score: int | None = None
    seo_score: int | None = None
    lcp: float | None = None
    cls: float | None = None
    inp: float | None = None
    ttfb: float | None = None
    field_data_available: int = 0
    payload_json: str = "{}"


@dataclass(slots=True)
class LighthouseRecord:
    run_id: str
    url: str
    form_factor: str
    status: str
    performance_score: int | None = None
    accessibility_score: int | None = None
    best_practices_score: int | None = None
    seo_score: int | None = None
    lcp: float | None = None
    cls: float | None = None
    inp: float | None = None
    ttfb: float | None = None
    total_blocking_time: float | None = None
    speed_index: float | None = None
    budget_pass: int = 1
    budget_failures_json: str = "[]"
    payload_json: str = "{}"
    error_message: str = ""


@dataclass(slots=True)
class CruxRecord:
    run_id: str
    url: str
    query_scope: str
    status: str
    source: str = "crux"
    origin_fallback_used: int = 0
    lcp_p75: float | None = None
    cls_p75: float | None = None
    inp_p75: float | None = None
    fcp_p75: float | None = None
    ttfb_p75: float | None = None
    payload_json: str = "{}"
    error_message: str = ""


@dataclass(slots=True)
class CrawlFetchRecord:
    run_id: str
    url: str
    status_code: int
    fetch_time_ms: int
    content_type: str
    response_bytes: int
    fetched_at: str


@dataclass(slots=True)
class PageSnapshotRecord:
    run_id: str
    url: str
    content_hash: str
    last_modified: str
    status_code: int
    changed_flag: int
    observed_at: str
    raw_content_hash: str = ""
    rendered_content_hash: str = ""
    effective_content_hash: str = ""


@dataclass(slots=True)
class URLStateRecord:
    url_key: str
    normalized_url: str
    last_final_url: str = ""
    etag: str = ""
    last_modified: str = ""
    last_status_code: int = 0
    last_content_type: str = ""
    last_body_sha256: str = ""
    last_extracted_sha256: str = ""
    last_fetched_at: str = ""
    last_seen_run_id: str = ""
    not_modified_streak: int = 0


@dataclass(slots=True)
class BodyBlobRecord:
    body_sha256: str
    storage_path: str
    byte_count: int
    content_encoding: str = ""


@dataclass(slots=True)
class ArtifactCacheRecord:
    artifact_sha256: str
    body_sha256: str
    extractor_version: str
    artifact_type: str
    artifact_json: str


@dataclass(slots=True)
class PageDiffRecord:
    run_id: str
    url: str
    diff_family: str
    old_value: str
    new_value: str
    severity: str = "low"


@dataclass(slots=True)
class RenderSessionRecord:
    run_id: str
    url: str
    used_render: int
    wait_profile: str
    interaction_count: int
    action_recipe: str
    failure_family: str
    console_errors_json: str = "[]"
    console_warnings_json: str = "[]"
    network_endpoints_json: str = "[]"


@dataclass(slots=True)
class SchemaValidationRecord:
    run_id: str
    url: str
    validation_score: int
    findings_json: str
    raw_render_diff_json: str = "{}"


@dataclass(slots=True)
class MediaAssetRecord:
    run_id: str
    url: str
    asset_type: str
    asset_url: str
    discoverability_score: int
    metadata_json: str = "{}"


@dataclass(slots=True)
class IndexStateHistoryRecord:
    run_id: str
    url: str
    source: str
    status: str
    state_payload_json: str = "{}"


@dataclass(slots=True)
class CitationEventRecord:
    run_id: str
    url: str
    eligibility_score: int
    evidence_json: str = "{}"


@dataclass(slots=True)
class AIVisibilityRecord:
    run_id: str
    url: str
    potential_score: int
    visibility_json: str = "{}"


@dataclass(slots=True)
class SubmissionEventRecord:
    run_id: str
    url: str
    engine: str
    action: str
    status: str
    payload_json: str = "{}"


@dataclass(slots=True)
class TemplateClusterRecord:
    run_id: str
    template_cluster: str
    page_type: str
    url_count: int
    avg_score: float
    issue_count: int


@dataclass(slots=True)
class OffsiteCommonCrawlSummaryRecord:
    run_id: str
    target_domain: str
    cc_release: str
    mode: str
    schedule: str
    status: str
    cache_state: str
    target_found_flag: int
    harmonic_centrality: float | None = None
    pagerank: float | None = None
    referring_domain_count: int = 0
    weighted_referring_domain_score: float | None = None
    avg_referrer_harmonic: float | None = None
    avg_referrer_pagerank: float | None = None
    top_referrer_concentration: float | None = None
    comparison_domain_count: int = 0
    query_elapsed_ms: int = 0
    background_started_at: str = ""
    background_finished_at: str = ""
    notes_json: str = "{}"


@dataclass(slots=True)
class OffsiteCommonCrawlLinkingDomainRecord:
    run_id: str
    target_domain: str
    linking_domain: str
    source_num_hosts: int = 0
    source_harmonic_centrality: float | None = None
    source_pagerank: float | None = None
    rank_bucket: str = ""
    evidence_json: str = "{}"


@dataclass(slots=True)
class OffsiteCommonCrawlComparisonRecord:
    run_id: str
    target_domain: str
    compare_domain: str
    cc_release: str
    harmonic_centrality: float | None = None
    pagerank: float | None = None
    rank_gap_vs_target: float | None = None
    pagerank_gap_vs_target: float | None = None


# Backward-compatible alias for historical naming.
OffsiteCommonCrawlCompetitorRecord = OffsiteCommonCrawlComparisonRecord


@dataclass(slots=True)
class CrawlResult:
    pages: list[PageRecord] = field(default_factory=list)
    links: list[LinkRecord] = field(default_factory=list)
    fetches: list[CrawlFetchRecord] = field(default_factory=list)
    snapshots: list[PageSnapshotRecord] = field(default_factory=list)
    discovered_urls: set[str] = field(default_factory=set)
    crawl_depth: dict[str, int | None] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    discovery_stats: dict[str, int] = field(default_factory=dict)
    incremental_stats: dict[str, int] = field(default_factory=dict)
    planner_stats: dict[str, int] = field(default_factory=dict)


JSONDict = dict[str, Any]
