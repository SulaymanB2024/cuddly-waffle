from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass(slots=True)
class AuditConfig:
    domain: str
    output_dir: Path
    run_profile: str = "standard"
    site_type: str = "general"  # general|local
    crawl_discovery_mode: str = "raw"  # raw|hybrid|browser_first
    scope_mode: str = "apex_www"  # host_only|apex_www|all_subdomains|custom_allowlist
    scope_allowlist: tuple[str, ...] = ()
    max_pages: int = 200
    crawl_frontier_enabled: bool = True
    crawl_frontier_cluster_budget: int = 3
    crawl_workers: int = 1
    crawl_queue_high_weight: int = 3
    crawl_queue_normal_weight: int = 2
    per_host_rate_limit_rps: float = 4.0
    per_host_burst_capacity: int = 4
    incremental_crawl_enabled: bool = False
    max_render_pages: int = 20
    render_mode: str = "sample"  # none|sample|all
    render_frontier_enabled: bool = True
    render_interaction_budget: int = 0
    render_wait_ladder_ms: tuple[int, ...] = (500, 1200, 2500)
    render_mobile_first: bool = True
    render_mobile_viewport: str = "390x844x2,mobile,touch"
    render_desktop_viewport: str = "1440x900x1"
    timeout: float = 10.0
    crawl_persona: str = "googlebot_smartphone"
    user_agent: str = ""
    user_agent_override: str = ""
    robots_user_agent_token: str = "Googlebot"
    meta_robot_scope: str = "googlebot"
    robots_persona_mode: str = "google_exact"  # google_exact|generic
    google_exact_apply_crawl_delay: bool = False
    respect_robots: bool = True
    save_html: bool = False
    verbose: bool = False
    request_delay: float = 0.25
    retries: int = 1
    crawl_base_backoff_seconds: float = 0.25
    crawl_max_backoff_seconds: float = 4.0
    crawl_max_total_wait_seconds: float = 12.0
    crawl_respect_retry_after: bool = True
    max_response_bytes: int = 2_000_000
    max_non_html_bytes: int = 262_144
    psi_enabled: bool = True
    crux_enabled: bool = True
    performance_targets: int = 6
    crux_origin_fallback: bool = True
    store_provider_payloads: bool = False
    payload_retention_days: int = 30
    provider_max_retries: int = 2
    provider_base_backoff_seconds: float = 0.5
    provider_max_backoff_seconds: float = 6.0
    provider_respect_retry_after: bool = True
    provider_max_total_wait_seconds: float = 20.0
    psi_workers: int = 4
    provider_rate_limit_rps: float = 4.0
    provider_rate_limit_capacity: int = 4
    lighthouse_enabled: bool = False
    lighthouse_targets: int = 3
    lighthouse_timeout_seconds: float = 90.0
    lighthouse_form_factor: str = "desktop"
    lighthouse_config_path: str = ""
    lighthouse_budget_performance_min: int = 70
    lighthouse_budget_seo_min: int = 70
    crawl_heartbeat_every_pages: int = 25
    gsc_enabled: bool = False
    gsc_property: str = ""
    gsc_credentials_json: str = ""
    gsc_url_limit: int = 200
    gsc_analytics_enabled: bool = False
    gsc_analytics_days: int = 28
    gsc_analytics_row_limit: int = 5000
    gsc_analytics_dimensions: tuple[str, ...] = ("page", "query", "device", "country", "date")
    offsite_commoncrawl_enabled: bool = False
    offsite_commoncrawl_mode: str = "ranks"  # ranks|domains (verify is experimental)
    offsite_commoncrawl_schedule: str = "concurrent_best_effort"  # concurrent_best_effort|background_wait|blocking
    offsite_commoncrawl_release: str = "auto"
    offsite_commoncrawl_cache_dir: str = "~/.cache/seo_audit/commoncrawl"
    offsite_commoncrawl_max_linking_domains: int = 100
    offsite_commoncrawl_join_budget_seconds: float = 0.5
    offsite_commoncrawl_time_budget_seconds: int = 180
    offsite_commoncrawl_allow_cold_edge_download: bool = False
    offsite_compare_domains: tuple[str, ...] = ()
    platform_detection_enabled: bool = True
    citation_measurement_enabled: bool = True
    url_policy_enabled: bool = True
    faceted_query_param_threshold: int = 2
    faceted_sample_rate: float = 1.0
    faceted_param_keys: tuple[str, ...] = (
        "facet",
        "filter",
        "filters",
        "sort",
        "brand",
        "color",
        "size",
        "price",
        "min",
        "max",
        "category",
        "categories",
        "q",
        "query",
    )
    action_param_keys: tuple[str, ...] = (
        "add-to-cart",
        "add_to_cart",
        "replytocom",
        "session",
        "sessionid",
        "token",
        "auth",
        "nonce",
    )
    diagnostic_param_keys: tuple[str, ...] = (
        "preview",
        "amp",
        "variant",
    )
    headers_only_param_keys: tuple[str, ...] = (
        "download",
        "export",
        "format",
    )
    canonical_candidate_param_keys: tuple[str, ...] = (
        "sort",
        "order",
        "view",
        "variant",
    )
    scoring_profile: str = ""
    extractor_version: str = "2.0.0"
    schema_rule_version: str = "1.0.0"
    scoring_version: str = "1.1.0"

    def to_json_dict(self) -> dict:
        data = asdict(self)
        data["output_dir"] = str(self.output_dir)
        return data
