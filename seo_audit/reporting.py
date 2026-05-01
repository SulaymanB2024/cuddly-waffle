from __future__ import annotations

from collections import Counter, defaultdict
import json
import sqlite3
from pathlib import Path
from urllib.parse import urlsplit

from seo_audit.preview_controls import preview_restriction_score
from seo_audit.policies import LOW_INTERNAL_LINKS_THRESHOLD, LOW_LOCAL_SEO_SCORE_THRESHOLD, LOW_PSI_PERFORMANCE_THRESHOLD
from seo_audit.storage import Storage


def build_markdown_report(storage: Storage, run_id: str, out_path: Path) -> None:
    pages = [dict(row) for row in storage.query("SELECT * FROM pages WHERE run_id = ?", (run_id,))]
    issues = [dict(row) for row in storage.query("SELECT * FROM issues WHERE run_id = ?", (run_id,))]
    scores = [dict(row) for row in storage.query("SELECT * FROM scores WHERE run_id = ?", (run_id,))]
    perf = [dict(row) for row in storage.query("SELECT * FROM performance_metrics WHERE run_id = ?", (run_id,))]
    lighthouse = [dict(row) for row in storage.query("SELECT * FROM lighthouse_metrics WHERE run_id = ?", (run_id,))]
    crux = [dict(row) for row in storage.query("SELECT * FROM crux_metrics WHERE run_id = ?", (run_id,))]
    offsite_summary_rows = [
        dict(row)
        for row in storage.query(
            "SELECT * FROM offsite_commoncrawl_summary WHERE run_id = ? ORDER BY offsite_summary_id DESC",
            (run_id,),
        )
    ]
    offsite_linking_rows = [
        dict(row)
        for row in storage.query(
            """
            SELECT *
            FROM offsite_commoncrawl_linking_domains
            WHERE run_id = ?
            ORDER BY
                COALESCE(source_harmonic_centrality, 0.0) DESC,
                COALESCE(source_pagerank, 0.0) DESC,
                COALESCE(source_num_hosts, 0) DESC,
                linking_domain ASC
            """,
            (run_id,),
        )
    ]
    try:
        offsite_comparison_rows = [
            dict(row)
            for row in storage.query(
                """
                SELECT *
                FROM offsite_commoncrawl_comparisons
                WHERE run_id = ?
                ORDER BY
                    COALESCE(harmonic_centrality, 0.0) DESC,
                    COALESCE(pagerank, 0.0) DESC,
                    compare_domain ASC
                """,
                (run_id,),
            )
        ]
    except sqlite3.OperationalError:
        offsite_comparison_rows = [
            dict(row)
            for row in storage.query(
                """
                SELECT
                    run_id,
                    target_domain,
                    competitor_domain AS compare_domain,
                    cc_release,
                    harmonic_centrality,
                    pagerank,
                    rank_gap_vs_target,
                    pagerank_gap_vs_target
                FROM offsite_commoncrawl_competitors
                WHERE run_id = ?
                ORDER BY
                    COALESCE(harmonic_centrality, 0.0) DESC,
                    COALESCE(pagerank, 0.0) DESC,
                    competitor_domain ASC
                """,
                (run_id,),
            )
        ]
    page_diffs = [dict(row) for row in storage.query("SELECT * FROM page_diffs WHERE run_id = ?", (run_id,))]
    run_events = [dict(row) for row in storage.query("SELECT * FROM run_events WHERE run_id = ? ORDER BY event_id", (run_id,))]
    run_rows = [dict(row) for row in storage.query("SELECT * FROM runs WHERE run_id = ?", (run_id,))]
    run = run_rows[0] if run_rows else {
        "domain": "unknown",
        "status": "missing",
        "started_at": "n/a",
        "completed_at": "n/a",
        "notes": "run row missing",
        "config_json": "{}",
    }

    config_json = (run["config_json"] if not isinstance(run, dict) else run.get("config_json", "{}")) or "{}"
    try:
        run_config = json.loads(config_json)
    except (TypeError, json.JSONDecodeError):
        run_config = {}
    run_profile = str(run_config.get("run_profile") or "standard")
    site_type = str(run_config.get("site_type") or "general")
    crawl_persona = str(run_config.get("crawl_persona") or "googlebot_smartphone")

    def parse_detail(detail_json: str) -> dict:
        if not detail_json:
            return {}
        try:
            parsed = json.loads(detail_json)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def parse_object(raw_json: str) -> dict:
        if not raw_json:
            return {}
        try:
            parsed = json.loads(raw_json)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def is_system_url(url: str) -> bool:
        path = (urlsplit(url).path or "").lower()
        basename = path.rsplit("/", 1)[-1]
        if basename in {"robots.txt", "sitemap.xml", "sitemap.xml.gz", "sitemap_index.xml"}:
            return True
        return basename.endswith(".xml")

    def is_actionable_page(row: dict) -> bool:
        status_code = int(row["status_code"] or 0)
        is_html_page = "html" in (row["content_type"] or "").lower() or bool(row["title"] or row["h1"] or row["word_count"])
        return is_html_page and 200 <= status_code < 400 and not is_system_url(row["normalized_url"])

    utility_segments = {
        "tag",
        "category",
        "gallery",
        "team",
        "gva_header",
        "footer",
        "author",
        "search",
    }
    money_segments = {
        "fasteners",
        "service",
        "services",
        "product",
        "products",
        "portfolio",
        "contact",
        "quote",
    }

    def first_path_segment(url: str) -> str:
        path = (urlsplit(url).path or "").strip("/").lower()
        return path.split("/")[0] if path else ""

    def page_track(row: dict) -> str:
        page_type = str(row.get("page_type") or "").lower()
        segment = first_path_segment(str(row.get("normalized_url") or ""))
        if page_type == "utility" or segment in utility_segments:
            return "utility"
        if page_type in {"homepage", "service", "contact", "location"} or segment in money_segments:
            return "money"
        return "other"

    def template_group(row: dict) -> str:
        page_type = str(row.get("page_type") or "").lower()
        segment = first_path_segment(str(row.get("normalized_url") or ""))
        if page_type == "homepage" or not segment:
            return "home"
        if segment in {"service", "services"} or page_type == "service":
            return "service"
        if segment in {"fasteners", "product", "products", "portfolio"}:
            return "product"
        if segment in {"news", "blog"} or page_type == "article":
            return "blog"
        if page_type == "utility" or segment in utility_segments:
            return "taxonomy"
        return "other"

    def severity_text(counter: Counter) -> str:
        return (
            f"critical={counter.get('critical', 0)} "
            f"high={counter.get('high', 0)} "
            f"medium={counter.get('medium', 0)} "
            f"low={counter.get('low', 0)}"
        )

    def as_int(value: object, default: int = 0) -> int:
        try:
            return int(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return default

    def shell_state(row: dict) -> str:
        state = str(row.get("shell_state") or "").strip()
        if state in {"raw_shell_unlikely", "raw_shell_possible", "raw_shell_confirmed_after_render"}:
            return state
        return "raw_shell_possible" if as_int(row.get("likely_js_shell"), 0) == 1 else "raw_shell_unlikely"

    def effective_provenance(row: dict) -> dict[str, str]:
        payload = parse_object(str(row.get("effective_field_provenance_json") or "{}"))
        return {str(key): str(value) for key, value in payload.items()}

    def canonical_representative_key(row: dict) -> str:
        return str(row.get("canonical_cluster_key") or row.get("final_url") or row.get("normalized_url") or "").strip()

    def representative_actionable_pages(rows: list[dict]) -> list[dict]:
        representatives: dict[str, dict] = {}
        for row in rows:
            key = canonical_representative_key(row)
            if not key:
                continue
            role = str(row.get("canonical_cluster_role") or "").strip().lower()
            current = representatives.get(key)
            if current is None:
                representatives[key] = row
                continue
            current_role = str(current.get("canonical_cluster_role") or "").strip().lower()
            if current_role == "alias" and role != "alias":
                representatives[key] = row
        return list(representatives.values())

    def include_effective_duplicate_signal(row: dict, field: str) -> bool:
        if as_int(row.get("is_noindex"), 0) == 1:
            return False
        provenance = effective_provenance(row)
        source = str(provenance.get(field) or "")
        state = shell_state(row)
        used_render = as_int(row.get("used_render"), 0) == 1
        if (
            (state == "raw_shell_confirmed_after_render" or (state == "raw_shell_possible" and used_render))
            and source in {"", "raw", "raw_fallback"}
        ):
            return False
        return True

    duplicate_excluded_page_types = {
        "utility",
        "search",
        "legal",
        "privacy",
        "terms",
        "tag",
        "category",
        "archive",
    }

    def duplicate_metadata_candidate(row: dict, field: str, value: str) -> bool:
        text = str(value or "").strip()
        if not text:
            return False
        page_type = str(row.get("page_type") or "").strip().lower()
        if page_type in duplicate_excluded_page_types:
            return False
        token_count = len([token for token in text.split() if token])
        if field == "title":
            return len(text) >= 8 and token_count >= 2
        if field == "meta_description":
            return len(text) >= 8 and token_count >= 2
        return True

    def sample_issue_urls(issue_code: str, limit: int = 6) -> list[str]:
        sample: list[str] = []
        for row in issues:
            if str(row.get("issue_code") or "") != issue_code:
                continue
            url = str(row.get("url") or "").strip()
            if not url or url in sample:
                continue
            sample.append(url)
            if len(sample) >= limit:
                break
        return sample

    def confidence_band(score: int) -> str:
        if score >= 85:
            return "high"
        if score >= 70:
            return "medium"
        return "low"

    page_types = Counter(row["page_type"] for row in pages)
    crawl_policy_counts = Counter(str(row.get("crawl_policy_class") or "crawl_normally") for row in pages)
    statuses = Counter(str(row["status_code"] or "error") for row in pages)
    severities = Counter(row["severity"] for row in issues)
    issue_gate_counts = Counter(str(row.get("technical_seo_gate") or "indexability") for row in issues)
    issue_verification_counts = Counter(str(row.get("verification_status") or "automated") for row in issues)
    issue_certainty_counts = Counter(str(row.get("certainty_state") or "Probable") for row in issues)
    issue_confidence_bands = Counter(confidence_band(as_int(row.get("confidence_score"), 100)) for row in issues)
    issue_reach_counts = Counter(str(row.get("reach") or "single_page") for row in issues)
    avg_priority_score = round(
        sum(float(as_int(row.get("priority_score"), 0)) for row in issues) / max(1, len(issues)),
        2,
    ) if issues else 0.0
    top_priority_issues = sorted(
        issues,
        key=lambda row: as_int(row.get("priority_score"), 0),
        reverse=True,
    )[:10]

    avg_quality_score = round(
        sum(float(as_int(row.get("quality_score"), 0)) for row in scores) / max(1, len(scores)),
        2,
    ) if scores else 0.0
    avg_risk_score = round(
        sum(float(as_int(row.get("risk_score"), 0)) for row in scores) / max(1, len(scores)),
        2,
    ) if scores else 0.0
    avg_coverage_score = round(
        sum(float(as_int(row.get("coverage_score"), 0)) for row in scores) / max(1, len(scores)),
        2,
    ) if scores else 0.0
    unknown_performance_pages = sum(1 for row in scores if as_int(row.get("performance_score"), -1) < 0)
    low_confidence_issues = sorted(
        issues,
        key=lambda row: as_int(row.get("confidence_score"), 100),
    )[:10]
    actionable_pages = [row for row in pages if is_actionable_page(row)]
    representative_pages = representative_actionable_pages(actionable_pages)

    content_hash_counts = Counter(
        str(row.get("effective_content_hash") or row.get("content_hash") or "").strip()
        for row in representative_pages
        if str(row.get("effective_content_hash") or row.get("content_hash") or "").strip()
        and as_int(row.get("is_noindex"), 0) == 0
        and include_effective_duplicate_signal(row, "content_hash")
        and as_int(
            row.get("effective_text_len", row.get("word_count", row.get("raw_word_count", 0))),
            0,
        )
        >= 120
    )
    exact_duplicate_diagnostic_clusters = sum(1 for count in content_hash_counts.values() if count > 1)
    exact_duplicate_diagnostic_pages = sum(count for count in content_hash_counts.values() if count > 1)

    exact_duplicate_issue_rows = [
        row for row in issues if str(row.get("issue_code") or "") == "EXACT_CONTENT_DUPLICATE"
    ]
    exact_duplicate_issue_pages = len(
        {
            str(row.get("url") or "").strip()
            for row in exact_duplicate_issue_rows
            if str(row.get("url") or "").strip()
        }
    )
    exact_duplicate_issue_hashes: set[str] = set()
    for row in exact_duplicate_issue_rows:
        payload = parse_object(str(row.get("evidence_json") or "{}"))
        content_hash = str(payload.get("content_hash") or "").strip()
        if content_hash:
            exact_duplicate_issue_hashes.add(content_hash)
    exact_duplicate_issue_clusters = len(exact_duplicate_issue_hashes)

    effective_title_counts = Counter(
        str(row.get("effective_title") or row.get("title") or "").strip()
        for row in representative_pages
        if str(row.get("effective_title") or row.get("title") or "").strip()
        and duplicate_metadata_candidate(row, "title", str(row.get("effective_title") or row.get("title") or ""))
        and include_effective_duplicate_signal(row, "title")
    )
    effective_description_counts = Counter(
        str(row.get("effective_meta_description") or row.get("meta_description") or "").strip()
        for row in representative_pages
        if str(row.get("effective_meta_description") or row.get("meta_description") or "").strip()
        and duplicate_metadata_candidate(
            row,
            "meta_description",
            str(row.get("effective_meta_description") or row.get("meta_description") or ""),
        )
        and include_effective_duplicate_signal(row, "meta_description")
    )
    effective_duplicate_title_clusters = sum(1 for count in effective_title_counts.values() if count > 1)
    effective_duplicate_title_pages = sum(count for count in effective_title_counts.values() if count > 1)
    effective_duplicate_description_clusters = sum(1 for count in effective_description_counts.values() if count > 1)
    effective_duplicate_description_pages = sum(count for count in effective_description_counts.values() if count > 1)

    raw_title_counts = Counter(
        str(row.get("raw_title") or row.get("title") or "").strip()
        for row in representative_pages
        if str(row.get("raw_title") or row.get("title") or "").strip()
    )
    raw_description_counts = Counter(
        str(row.get("raw_meta_description") or row.get("meta_description") or "").strip()
        for row in representative_pages
        if str(row.get("raw_meta_description") or row.get("meta_description") or "").strip()
    )
    raw_duplicate_title_clusters = sum(1 for count in raw_title_counts.values() if count > 1)
    raw_duplicate_description_clusters = sum(1 for count in raw_description_counts.values() if count > 1)
    canonical_cluster_counts = Counter(
        str(row.get("canonical_cluster_key") or row.get("normalized_url") or "")
        for row in actionable_pages
        if str(row.get("canonical_cluster_key") or row.get("normalized_url") or "").strip()
    )
    canonical_cluster_multi_member = sum(1 for count in canonical_cluster_counts.values() if count > 1)
    canonical_alias_pages = sum(
        1
        for row in actionable_pages
        if str(row.get("canonical_cluster_role") or "").strip().lower() == "alias"
    )
    high_render = sorted(actionable_pages, key=lambda r: int(r["render_gap_score"] or 0), reverse=True)[:10]
    stage_events = [row for row in run_events if row["event_type"] == "stage_timing"]
    planner_events = [row for row in run_events if row["event_type"] == "crawl_plan_summary"]
    incremental_events = [row for row in run_events if row["event_type"] == "crawl_incremental_summary"]
    heartbeat_events = [row for row in run_events if row["event_type"] == "crawl_heartbeat"]
    discovery_events = [row for row in run_events if row["event_type"] == "crawl_discovery_summary"]
    sitemap_analysis_events = [row for row in run_events if row["event_type"] == "sitemap_analysis_summary"]
    provider_events = [row for row in run_events if row["event_type"] == "provider_summary"]
    governance_events = [row for row in run_events if row["event_type"] == "governance_summary"]
    provider_by_stage = {str(row.get("stage") or ""): parse_detail(str(row.get("detail_json") or "{}")) for row in provider_events}
    pages_by_url = {row["normalized_url"]: row for row in pages}
    issue_code_counts = Counter(str(row.get("issue_code") or "") for row in issues)
    diff_family_counts = Counter(str(row.get("diff_family") or "") for row in page_diffs)

    def measurement_status_value(row: dict) -> str:
        value = str(row.get("measurement_status") or "").strip()
        return value or "measurement_not_attempted_by_policy"

    def measurement_error_family_value(row: dict) -> str:
        value = str(row.get("measurement_error_family") or "").strip()
        if value:
            return value
        status = measurement_status_value(row)
        if status == "ok":
            return "none"
        if status == "measurement_not_attempted_by_policy":
            return "not_attempted_policy"
        return "unknown"

    measurement_status_counts = Counter(measurement_status_value(row) for row in pages)
    measurement_error_family_counts = Counter(measurement_error_family_value(row) for row in pages)
    provider_status_counts = Counter(
        status
        for status in (
            measurement_status_value(row)
            for row in pages
        )
        if status in {"provider_error", "provider_runtime_error", "provider_unavailable_data"}
    )
    changed_pages_count = sum(1 for row in pages if as_int(row.get("changed_since_last_run"), 0) == 1)
    unchanged_pages_count = max(0, len(pages) - changed_pages_count)

    schema_audit_counts = {
        "syntax_valid": 0,
        "eligible_feature_pages": 0,
        "recognized_supporting_pages": 0,
        "constrained_deprecated_pages": 0,
        "visible_mismatch_pages": 0,
    }
    for row in pages:
        payload = parse_object(str(row.get("schema_validation_json") or "{}"))
        if bool(payload.get("syntax_valid", True)):
            schema_audit_counts["syntax_valid"] += 1
        rendered_validation = payload.get("rendered_validation") if isinstance(payload.get("rendered_validation"), dict) else {}

        eligible = payload.get("eligible_features")
        rendered_eligible = rendered_validation.get("eligible_features") if isinstance(rendered_validation, dict) else []
        if (isinstance(eligible, list) and eligible) or (isinstance(rendered_eligible, list) and rendered_eligible):
            schema_audit_counts["eligible_feature_pages"] += 1

        recognized_types = payload.get("recognized_types")
        rendered_recognized = rendered_validation.get("recognized_types") if isinstance(rendered_validation, dict) else []
        if (isinstance(recognized_types, list) and recognized_types) or (isinstance(rendered_recognized, list) and rendered_recognized):
            schema_audit_counts["recognized_supporting_pages"] += 1

        deprecated = payload.get("deprecated_features")
        rendered_deprecated = rendered_validation.get("deprecated_features") if isinstance(rendered_validation, dict) else []
        missing_required = payload.get("missing_required_by_feature")
        rendered_missing_required = rendered_validation.get("missing_required_by_feature") if isinstance(rendered_validation, dict) else {}
        has_constraints = (
            (isinstance(deprecated, list) and bool(deprecated))
            or (isinstance(rendered_deprecated, list) and bool(rendered_deprecated))
            or (isinstance(missing_required, dict) and any(bool(value) for value in missing_required.values()))
            or (
                isinstance(rendered_missing_required, dict)
                and any(bool(value) for value in rendered_missing_required.values())
            )
        )
        if has_constraints:
            schema_audit_counts["constrained_deprecated_pages"] += 1

        mismatches = payload.get("visible_content_mismatches")
        if isinstance(mismatches, list) and mismatches:
            schema_audit_counts["visible_mismatch_pages"] += 1

    ai_potential_scores: list[int] = []
    ai_observed_signal_pages = 0
    ai_adapter_counts: Counter[str] = Counter()
    for row in pages:
        payload = parse_object(str(row.get("ai_visibility_json") or "{}"))
        potential_payload = payload.get("potential") if isinstance(payload.get("potential"), dict) else {}
        score = as_int(
            row.get("ai_discoverability_potential_score"),
            as_int(potential_payload.get("score"), 0),
        )
        ai_potential_scores.append(max(0, min(100, score)))

        observed_payload = payload.get("observed_evidence") if isinstance(payload.get("observed_evidence"), dict) else {}
        impressions = as_int(observed_payload.get("gsc_impressions"), 0)
        clicks = as_int(observed_payload.get("gsc_clicks"), 0)
        chatgpt_referrals = as_int(observed_payload.get("chatgpt_referrals"), 0)
        if impressions > 0 or clicks > 0 or chatgpt_referrals > 0:
            ai_observed_signal_pages += 1

        observed_sources = observed_payload.get("observed_sources")
        if isinstance(observed_sources, list):
            for source in observed_sources:
                source_name = str(source).strip()
                if source_name:
                    ai_adapter_counts[source_name] += 1

        adapters_applied = payload.get("adapters_applied")
        if isinstance(adapters_applied, list):
            for source in adapters_applied:
                source_name = str(source).strip()
                if source_name:
                    ai_adapter_counts[source_name] += 1
    discovered_via_counts: Counter = Counter()
    for row in pages:
        raw = str(row.get("discovered_via") or "").strip()
        if not raw:
            discovered_via_counts["unknown"] += 1
            continue
        tokens = [token.strip() for token in raw.split(",") if token.strip()]
        if not tokens:
            discovered_via_counts["unknown"] += 1
            continue
        for token in tokens:
            discovered_via_counts[token] += 1

    discovery_access_codes = (
        "ROBOTS_BLOCKED_URL",
        "SITEMAP_URL_BLOCKED_BY_ROBOTS",
        "ROBOTS_NOINDEX_CONFLICT",
        "SITEMAP_URL_NOT_CRAWLED",
        "CRAWLED_URL_NOT_IN_SITEMAP",
        "DISCOVERY_BLIND_SPOT",
        "REDIRECT_CHAIN_LONG",
        "REDIRECT_TO_ERROR",
        "ACCESS_AUTH_BLOCKED",
    )
    canonical_indexability_codes = (
        "DUPLICATE_CANONICAL_TAGS",
        "MULTIPLE_CANONICAL_TAGS",
        "CANONICAL_CONFLICT_RAW_VS_RENDERED",
        "CANONICAL_SELF_MISMATCH",
        "STATIC_SHELL_CANONICAL_REUSED_ACROSS_ROUTES",
        "STATIC_SHELL_HREFLANG_REUSED_ACROSS_ROUTES",
        "CLUSTER_CANONICAL_COLLISION",
        "HOST_DUPLICATION_CLUSTER",
        "HREFLANG_RECIPROCITY_MISSING",
        "PAGINATION_SIGNAL_MISSING",
        "FACETED_NAVIGATION_RISK",
        "CANONICAL_MISMATCH",
        "MISSING_CANONICAL",
        "NOINDEX",
        "STRUCTURED_DATA_PARSE_FAILED",
        "EXACT_CONTENT_DUPLICATE",
    )

    nosnippet_directive_pages = sum(1 for row in pages if as_int(row.get("has_nosnippet_directive"), 0) == 1)
    data_nosnippet_elements = sum(as_int(row.get("data_nosnippet_count"), 0) for row in pages)
    max_snippet_pages = sum(1 for row in pages if str(row.get("max_snippet_directive") or "").strip())
    max_image_preview_pages = sum(1 for row in pages if str(row.get("max_image_preview_directive") or "").strip())
    max_video_preview_pages = sum(1 for row in pages if str(row.get("max_video_preview_directive") or "").strip())
    restrictive_snippet_issue_codes = {
        "BING_PREVIEW_CONTROLS_RESTRICTIVE",
        "OVER_RESTRICTIVE_SNIPPET_CONTROLS",
    }
    restrictive_snippet_issue_urls = sorted(
        {
            str(row.get("url") or "").strip()
            for row in issues
            if str(row.get("issue_code") or "") in restrictive_snippet_issue_codes
            and str(row.get("url") or "").strip()
        }
    )
    restrictive_snippet_pages = [pages_by_url[url] for url in restrictive_snippet_issue_urls if url in pages_by_url]
    restrictive_snippet_diagnostic_pages = [
        row
        for row in pages
        if str(row.get("normalized_url") or "").strip() not in restrictive_snippet_issue_urls
        and as_int(row.get("is_noindex"), 0) == 0
        and preview_restriction_score(
            {
                "has_nosnippet_directive": as_int(row.get("has_nosnippet_directive"), 0),
                "max_snippet_directive": str(row.get("max_snippet_directive") or ""),
                "max_image_preview_directive": str(row.get("max_image_preview_directive") or ""),
                "max_video_preview_directive": str(row.get("max_video_preview_directive") or ""),
                "data_nosnippet_count": as_int(row.get("data_nosnippet_count"), 0),
            }
        )[0]
        >= 4
    ]
    heavy_data_nosnippet_pages = sorted(
        [row for row in pages if as_int(row.get("data_nosnippet_count"), 0) >= 4],
        key=lambda row: as_int(row.get("data_nosnippet_count"), 0),
        reverse=True,
    )
    governance_issue_codes = (
        "OPENAI_SEARCHBOT_BLOCKED",
        "GOOGLE_EXTENDED_BLOCKED",
        "GPTBOT_BLOCKED",
        "OAI_ADSBOT_BLOCKED",
        "BING_PREVIEW_CONTROLS_RESTRICTIVE",
        "OVER_RESTRICTIVE_SNIPPET_CONTROLS",
        "RAW_RENDER_NOINDEX_MISMATCH",
        "RAW_RENDER_PREVIEW_CONTROL_MISMATCH",
    )

    track_page_counts: Counter = Counter()
    for row in actionable_pages:
        track_page_counts[page_track(row)] += 1

    track_issue_counts: Counter = Counter()
    track_issue_severity: dict[str, Counter] = {
        "money": Counter(),
        "utility": Counter(),
        "other": Counter(),
        "global": Counter(),
    }
    track_issue_codes: dict[str, Counter] = {
        "money": Counter(),
        "utility": Counter(),
        "other": Counter(),
        "global": Counter(),
    }
    for issue in issues:
        issue_url = str(issue["url"] or "")
        page = pages_by_url.get(issue_url)
        track = page_track(page) if page else "global"
        track_issue_counts[track] += 1
        track_issue_severity[track][issue["severity"]] += 1
        track_issue_codes[track][issue["issue_code"]] += 1

    perf_rows_by_group: dict[str, int] = defaultdict(int)
    perf_scores_by_group: dict[str, list[int]] = defaultdict(list)
    for row in perf:
        page = pages_by_url.get(str(row.get("url") or ""))
        if page is None:
            group = "other"
        else:
            group = template_group(page)
        perf_rows_by_group[group] += 1
        if row.get("performance_score") is not None:
            perf_scores_by_group[group].append(int(row["performance_score"]))

    lines = [
        f"# SEO Audit Report: {run['domain']}",
        "",
        "## Audit overview",
        f"- Run ID: `{run_id}`",
        f"- Status: `{run['status']}`",
        f"- Started: `{run['started_at']}`",
        f"- Completed: `{run['completed_at']}`",
        f"- Run profile: `{run_profile}`",
        f"- Site type: `{site_type}`",
        f"- Crawl persona: `{crawl_persona}`",
        "",
        "## Crawl stats",
        f"- Pages stored: **{len(pages)}**",
        f"- Issues: **{len(issues)}**",
        f"- Scores: **{len(scores)}**",
        f"- Performance rows: **{len(perf)}**",
        f"- Lighthouse rows: **{len(lighthouse)}**",
        f"- CrUX rows: **{len(crux)}**",
        "",
    ]

    lines.extend([
        "## Crawl planning",
    ])
    if planner_events:
        planner_detail = parse_detail(planner_events[-1]["detail_json"])
        lines.append(f"- Planned crawl URLs: {planner_detail.get('planner_discovered', 0)}")
        lines.append(f"- Seed URLs considered: {planner_detail.get('planner_seed_urls', 0)}")
        lines.append(f"- Known sitemap URLs considered: {planner_detail.get('planner_known_sitemap_urls', 0)}")
        lines.append(f"- Prioritized previously changed URLs: {planner_detail.get('planner_prioritized_changed_urls', 0)}")
    else:
        lines.append("- No crawl planning telemetry recorded.")

    lines.extend([
        "",
        "## Incremental crawl counters",
    ])
    if incremental_events:
        incremental_detail = parse_detail(incremental_events[-1]["detail_json"])
        lines.append(f"- Discovered: {incremental_detail.get('discovered', 0)}")
        lines.append(f"- Fetched: {incremental_detail.get('fetched', 0)}")
        lines.append(f"- Crawl retries used: {incremental_detail.get('fetch_retries_total', 0)}")
        lines.append(f"- Crawl retry wait (ms): {incremental_detail.get('fetch_retry_wait_ms_total', 0)}")
        lines.append(f"- Crawl retries using Retry-After: {incremental_detail.get('fetch_retry_after_used', 0)}")
        lines.append(f"- Reused from cache: {incremental_detail.get('reused_from_cache', 0)}")
        lines.append(f"- Not modified (304): {incremental_detail.get('not_modified', 0)}")
        lines.append(f"- Reparsed (unchanged but invalidated): {incremental_detail.get('reparsed', 0)}")
        lines.append(f"- Rerendered: {incremental_detail.get('rerendered', 0)}")
    else:
        lines.append("- No incremental crawl telemetry recorded.")

    lines.extend([
        "",
        "## Changed vs unchanged pages",
    ])
    lines.append(f"- Changed pages: {changed_pages_count}")
    lines.append(f"- Unchanged pages: {unchanged_pages_count}")
    lines.append(f"- Total page diff rows: {len(page_diffs)}")
    if diff_family_counts:
        top_diff_families = ", ".join(
            f"{family}={count}"
            for family, count in diff_family_counts.most_common(10)
            if family
        )
        lines.append(f"- Top diff families: {top_diff_families}")
    else:
        lines.append("- No persisted page diffs for this run.")

    lines.extend([
        "",
        "## Structured data audit dimensions",
    ])
    lines.append(f"- Pages with syntactically valid structured data: {schema_audit_counts['syntax_valid']}")
    lines.append(f"- Pages with eligible Google features (raw or rendered): {schema_audit_counts['eligible_feature_pages']}")
    lines.append(f"- Pages with recognized/supporting schema: {schema_audit_counts['recognized_supporting_pages']}")
    lines.append(f"- Pages with constrained/deprecated schema: {schema_audit_counts['constrained_deprecated_pages']}")
    lines.append(f"- Pages with visible-content mismatches: {schema_audit_counts['visible_mismatch_pages']}")

    lines.extend([
        "",
        "## Stage timing summary",
    ])

    if stage_events:
        for row in stage_events:
            elapsed_s = (row["elapsed_ms"] or 0) / 1000.0
            lines.append(f"- {row['stage']}: {elapsed_s:.2f}s")
    else:
        lines.append("- No stage timing telemetry recorded.")

    lines.extend([
        "",
        "## Crawl heartbeat summary",
    ])
    if heartbeat_events:
        lines.append(f"- Heartbeat events recorded: {len(heartbeat_events)}")
        last_hb = heartbeat_events[-1]
        hb_detail = parse_detail(last_hb["detail_json"])
        if hb_detail:
            lines.append(
                "- Last heartbeat: "
                f"pages={hb_detail.get('pages_stored', 0)} "
                f"queue={hb_detail.get('queue_size', 0)} "
                f"errors={hb_detail.get('error_count', 0)} "
                f"elapsed_s={(float(hb_detail.get('crawl_elapsed_ms', 0)) / 1000.0):.2f}"
            )
    else:
        lines.append("- No crawl heartbeat telemetry recorded.")

    lines.extend([
        "",
        "## Discovery blind spots",
    ])
    if discovery_events:
        latest_discovery = parse_detail(discovery_events[-1]["detail_json"])
        lines.append(f"- Enqueued URLs total: {latest_discovery.get('enqueued_total', 0)}")
        lines.append(f"- Enqueued via raw links: {latest_discovery.get('enqueued_via_raw_link', 0)}")
        lines.append(f"- Enqueued via rendered links: {latest_discovery.get('enqueued_via_render_link', 0)}")
        lines.append(f"- Queue dedupe skips: {latest_discovery.get('dedupe_skipped', 0)}")
        lines.append(f"- Scope-filtered drops: {latest_discovery.get('scope_skipped', 0)}")
        lines.append(f"- Render frontier checks: {latest_discovery.get('render_frontier_checks', 0)}")
        lines.append(f"- Render frontier successes: {latest_discovery.get('render_frontier_successes', 0)}")
        lines.append(f"- Render frontier failures: {latest_discovery.get('render_frontier_failures', 0)}")
    else:
        lines.append("- No crawl discovery telemetry recorded.")

    if discovered_via_counts:
        via_summary = ", ".join(f"{key}={value}" for key, value in sorted(discovered_via_counts.items()))
        lines.append(f"- Discovery provenance mix: {via_summary}")
    lines.append(f"- Discovery blind-spot issues: {issue_code_counts.get('DISCOVERY_BLIND_SPOT', 0)}")

    lines.extend([
        "",
        "## Sitemap intelligence",
    ])
    if sitemap_analysis_events:
        sitemap_summary = parse_detail(sitemap_analysis_events[-1]["detail_json"])
        lines.append(f"- Sitemap URLs: {sitemap_summary.get('sitemap_url_count', 0)}")
        lines.append(f"- Discovered pages: {sitemap_summary.get('discovered_page_count', 0)}")
        lines.append(f"- Sitemap URLs not crawled: {sitemap_summary.get('urls_in_sitemap_not_crawled', 0)}")
        lines.append(f"- Crawled URLs not in sitemap: {sitemap_summary.get('crawled_urls_not_in_sitemap', 0)}")
        lines.append(f"- Sitemap hreflang mismatches: {sitemap_summary.get('sitemap_hreflang_mismatches', 0)}")
        lines.append(f"- Stale lastmod URLs: {sitemap_summary.get('stale_lastmod_urls', 0)}")
        lines.append(f"- Missing lastmod URLs: {sitemap_summary.get('missing_lastmod_urls', 0)}")
        lines.append(f"- Sitemap scope violations: {sitemap_summary.get('sitemap_scope_violations', 0)}")
    else:
        lines.append("- No sitemap intelligence telemetry recorded.")

    lines.extend([
        "",
        "## Provider telemetry",
    ])
    if provider_events:
        for row in provider_events:
            detail = parse_detail(row["detail_json"])
            lines.append(
                "- "
                f"{row['stage']}: "
                f"attempts={detail.get('attempts', 0)} "
                f"http_attempts={detail.get('http_attempts', 0)} "
                f"retries={detail.get('retries', 0)} "
                f"wait_s={float(detail.get('wait_seconds', 0.0)):.2f} "
                f"timeouts={detail.get('timeouts', 0)} "
                f"success={detail.get('success', 0)} "
                f"no_data={detail.get('no_data', 0)} "
                f"failed_http={detail.get('failed_http', 0)} "
                f"skipped_missing_key={detail.get('skipped_missing_key', 0)}"
            )
    else:
        lines.append("- No provider telemetry recorded.")

    lines.extend([
        "",
        "## Search Console reconciliation",
    ])
    gsc_detail = provider_by_stage.get("gsc", {})
    if gsc_detail:
        lines.append(f"- Status: {gsc_detail.get('status', 'unknown')}")
        lines.append(f"- Property: {gsc_detail.get('property_uri', 'n/a')}")
        lines.append(f"- Crawled URLs in reconciliation set: {gsc_detail.get('crawled_total', 0)}")
        lines.append(f"- Indexed: {gsc_detail.get('indexed', 0)}")
        lines.append(f"- Not indexed: {gsc_detail.get('not_indexed', 0)}")
        lines.append(f"- Unknown: {gsc_detail.get('unknown', 0)}")
        lines.append("- URL Inspection states are sampled snapshots for the inspected URLs in this run, not full-site guarantees.")
        if gsc_detail.get("message"):
            lines.append(f"- Message: {gsc_detail.get('message')}")
    else:
        lines.append("- No Search Console telemetry recorded.")

    lines.extend([
        "",
        "## Offsite visibility (Common Crawl)",
    ])
    if offsite_summary_rows:
        offsite_summary = offsite_summary_rows[0]
        offsite_status = str(offsite_summary.get("status") or "unknown")
        lines.append(f"- Release: {offsite_summary.get('cc_release', 'n/a')}")
        lines.append(f"- Mode: {offsite_summary.get('mode', 'n/a')}")
        lines.append(f"- Schedule: {offsite_summary.get('schedule', 'n/a')}")
        if str(offsite_summary.get("schedule") or "") == "concurrent_best_effort":
            lines.append("- Schedule semantics: concurrent while this audit process runs; it does not continue after run completion.")
        lines.append(f"- Cache state: {offsite_summary.get('cache_state', 'n/a')}")
        lines.append(f"- Status: {offsite_status}")
        lines.append(f"- Target in graph: {int(offsite_summary.get('target_found_flag') or 0)}")
        if offsite_summary.get("harmonic_centrality") is not None:
            lines.append(f"- Target harmonic centrality: {float(offsite_summary.get('harmonic_centrality') or 0.0):.6f}")
        if offsite_summary.get("pagerank") is not None:
            lines.append(f"- Target pagerank: {float(offsite_summary.get('pagerank') or 0.0):.8f}")
        if int(offsite_summary.get("referring_domain_count") or 0) > 0:
            lines.append(f"- Referring domains (domain graph): {int(offsite_summary.get('referring_domain_count') or 0)}")
        if offsite_summary.get("weighted_referring_domain_score") is not None:
            lines.append(
                f"- Weighted referring-domain score: {float(offsite_summary.get('weighted_referring_domain_score') or 0.0):.3f}"
            )
        if offsite_summary.get("avg_referrer_harmonic") is not None:
            lines.append(
                f"- Avg referrer harmonic: {float(offsite_summary.get('avg_referrer_harmonic') or 0.0):.6f}"
            )
        if offsite_summary.get("avg_referrer_pagerank") is not None:
            lines.append(
                f"- Avg referrer pagerank: {float(offsite_summary.get('avg_referrer_pagerank') or 0.0):.8f}"
            )
        if offsite_summary.get("top_referrer_concentration") is not None:
            lines.append(
                f"- Top referrer concentration: {float(offsite_summary.get('top_referrer_concentration') or 0.0):.4f}"
            )
        lines.append(
            f"- Comparison domains evaluated: {int(offsite_summary.get('comparison_domain_count') or offsite_summary.get('competitor_count') or 0)}"
        )
        if offsite_comparison_rows:
            lines.append("- Comparison domains:")
            for row in offsite_comparison_rows[:10]:
                lines.append(
                    "  - "
                    f"{row.get('compare_domain', '')}: "
                    f"harmonic={row.get('harmonic_centrality', 'n/a')} "
                    f"pagerank={row.get('pagerank', 'n/a')} "
                    f"gap_vs_target={row.get('rank_gap_vs_target', 'n/a')}"
                )
        else:
            lines.append("- Comparison domains: none captured.")

        if offsite_linking_rows:
            lines.append("- Top linking domains (ordered by harmonic centrality then pagerank):")
            for row in offsite_linking_rows[:10]:
                lines.append(
                    "  - "
                    f"{row.get('linking_domain', '')}: "
                    f"harmonic={row.get('source_harmonic_centrality', 'n/a')} "
                    f"pagerank={row.get('source_pagerank', 'n/a')} "
                    f"num_hosts={row.get('source_num_hosts', 0)}"
                )
        else:
            lines.append("- Top linking domains: none captured.")

        lines.append(
            "- Note: linking domains are Common Crawl domain-graph evidence and are not exact page-level backlink proof."
        )
        if offsite_status in {"pending_background", "timeout_background", "deferred_verify_not_implemented", "success_partial"}:
            lines.append("- Result state: deferred or partial data within this run; no post-run continuation is performed.")
    else:
        lines.append("- Offsite Common Crawl lane not captured for this run.")

    lines.extend([
        "",
        "## Governance and answer-layer controls",
    ])
    if governance_events:
        governance_detail = parse_detail(governance_events[-1]["detail_json"])
        lines.append(f"- Actionable pages governance-audited: {governance_detail.get('actionable_pages', 0)}")
        lines.append(f"- Googlebot blocked pages: {governance_detail.get('googlebot_blocked_pages', 0)}")
        lines.append(f"- Bingbot blocked pages: {governance_detail.get('bingbot_blocked_pages', 0)}")
        lines.append(f"- OAI-SearchBot blocked pages: {governance_detail.get('oai_searchbot_blocked_pages', 0)}")
        lines.append(f"- Google-Extended blocked pages: {governance_detail.get('google_extended_blocked_pages', 0)}")
        lines.append(f"- GPTBot blocked pages: {governance_detail.get('gptbot_blocked_pages', 0)}")
        lines.append(f"- OAI-AdsBot blocked pages: {governance_detail.get('oai_adsbot_blocked_pages', 0)}")
        lines.append(f"- ChatGPT-User blocked pages (informational): {governance_detail.get('chatgpt_user_blocked_pages', 0)}")
    else:
        lines.append("- Governance telemetry unavailable for this run.")
    for issue_code in governance_issue_codes:
        sample_urls = sample_issue_urls(issue_code)
        rendered_sample = ", ".join(sample_urls[:4]) if sample_urls else "none"
        lines.append(f"- {issue_code}: {issue_code_counts.get(issue_code, 0)} (sample: {rendered_sample})")

    lines.extend([
        "",
        "## Page type counts",
    ])
    lines.extend([f"- {k}: {v}" for k, v in sorted(page_types.items())])
    lines.append("\n## Status-code summary")
    lines.extend([f"- {k}: {v}" for k, v in sorted(statuses.items())])

    lines.append("\n## URL policy coverage")
    lines.extend([f"- {k}: {v}" for k, v in sorted(crawl_policy_counts.items())])

    lines.append("\n## Top issues by severity (all tracks)")
    lines.extend([f"- {k}: {v}" for k, v in severities.items()])

    lines.append("\n## Issue gate coverage")
    if issue_gate_counts:
        lines.extend([f"- {k}: {v}" for k, v in sorted(issue_gate_counts.items())])
    else:
        lines.append("- No issues recorded.")

    lines.append("\n## Issue verification confidence")
    if issues:
        verification_summary = ", ".join(
            f"{k}={v}" for k, v in sorted(issue_verification_counts.items())
        )
        certainty_summary = ", ".join(
            f"{k}={v}" for k, v in sorted(issue_certainty_counts.items())
        )
        reach_summary = ", ".join(
            f"{k}={v}" for k, v in sorted(issue_reach_counts.items())
        )
        band_summary = ", ".join(f"{k}={v}" for k, v in sorted(issue_confidence_bands.items()))
        lines.append(f"- Verification status counts: {verification_summary}")
        lines.append(f"- Certainty state counts: {certainty_summary}")
        lines.append(f"- Confidence bands: {band_summary}")
        lines.append(f"- Reach distribution: {reach_summary}")
        lines.append(f"- Average priority score: {avg_priority_score:.2f}")
        lines.append("- Lowest-confidence issues queue:")
        for row in low_confidence_issues:
            lines.append(
                "  - "
                f"{row['issue_code']} @ {row['url']} "
                f"(confidence={as_int(row.get('confidence_score'), 100)}, "
                f"status={row.get('verification_status') or 'automated'})"
            )
        lines.append("- Highest-priority issues queue:")
        for row in top_priority_issues:
            lines.append(
                "  - "
                f"{row['issue_code']} @ {row['url']} "
                f"(priority={as_int(row.get('priority_score'), 0)}, "
                f"certainty={row.get('certainty_state') or 'Probable'}, "
                f"reach={row.get('reach') or 'single_page'})"
            )
    else:
        lines.append("- No issues recorded.")

    lines.append("\n## Score model diagnostics")
    lines.append(f"- Average quality score: {avg_quality_score:.2f}")
    lines.append(f"- Average risk score: {avg_risk_score:.2f}")
    lines.append(f"- Average measurement coverage: {avg_coverage_score:.2f}")
    lines.append(f"- Performance unknown pages (score=-1): {unknown_performance_pages}")

    lines.append("\n## Measurement coverage taxonomy")
    status_order = {
        "ok": 0,
        "provider_error": 1,
        "provider_runtime_error": 2,
        "provider_unavailable_data": 3,
        "runtime_error": 4,
        "no_field_data": 5,
        "measurement_not_attempted_by_policy": 6,
        "skipped": 7,
    }
    ordered_status_mix = sorted(
        measurement_status_counts.items(),
        key=lambda item: (status_order.get(item[0], 99), item[0]),
    )
    lines.append(
        "- Status mix: "
        + ", ".join(f"{status}={count}" for status, count in ordered_status_mix)
    )
    lines.append(
        "- Error-family mix: "
        + ", ".join(f"{family}={count}" for family, count in sorted(measurement_error_family_counts.items()))
    )
    if provider_status_counts:
        lines.append(
            "- Provider status split: "
            + ", ".join(
                f"{status}={count}"
                for status, count in sorted(provider_status_counts.items())
            )
        )

    lines.append("\n## Discovery and access checks")
    for code in discovery_access_codes:
        lines.append(f"- {code}: {issue_code_counts.get(code, 0)}")

    lines.append("\n## Canonicalization and indexability checks")
    for code in canonical_indexability_codes:
        lines.append(f"- {code}: {issue_code_counts.get(code, 0)}")
    lines.append(f"- Canonical clusters (actionable pages): {len(canonical_cluster_counts)}")
    lines.append(f"- Canonical clusters with multiple members: {canonical_cluster_multi_member}")
    lines.append(f"- Pages declaring canonical aliases: {canonical_alias_pages}")
    lines.append("- Note: noindex is page-level and may remain unknown when robots.txt blocks crawling before directives are observed.")

    lines.append("\n## Prioritization tracks")
    lines.append(
        "- Money pages (priority queue): "
        f"pages={track_page_counts.get('money', 0)} "
        f"issues={track_issue_counts.get('money', 0)} "
        f"({severity_text(track_issue_severity['money'])})"
    )
    lines.append(
        "- Utility/taxonomy/template pages (hygiene backlog): "
        f"pages={track_page_counts.get('utility', 0)} "
        f"issues={track_issue_counts.get('utility', 0)} "
        f"({severity_text(track_issue_severity['utility'])})"
    )
    lines.append(
        "- Other content pages: "
        f"pages={track_page_counts.get('other', 0)} "
        f"issues={track_issue_counts.get('other', 0)} "
        f"({severity_text(track_issue_severity['other'])})"
    )
    lines.append(
        "- Run-level/global issues: "
        f"issues={track_issue_counts.get('global', 0)} "
        f"({severity_text(track_issue_severity['global'])})"
    )

    lines.append("\n## Top issue codes by track")
    for key, label in (
        ("money", "money"),
        ("utility", "utility/taxonomy/template"),
        ("other", "other content"),
        ("global", "run-level/global"),
    ):
        top_codes = track_issue_codes[key].most_common(5)
        if not top_codes:
            lines.append(f"- {label}: none")
            continue
        rendered = ", ".join(f"{code}={count}" for code, count in top_codes)
        lines.append(f"- {label}: {rendered}")

    lines.append("\n## Top pages by render risk")
    if high_render:
        for row in high_render:
            lines.append(f"- {row['normalized_url']} (gap={row['render_gap_score']}, reason={row['render_gap_reason'] or 'n/a'})")
    else:
        lines.append("- No actionable pages with render diagnostics.")

    lines.append("\n## Duplicate title/description findings")
    lines.append(f"- Duplicate title clusters (effective, quality-filtered representatives): {effective_duplicate_title_clusters}")
    lines.append(f"- Representative pages in duplicate title clusters: {effective_duplicate_title_pages}")
    lines.append(f"- Duplicate description clusters (effective, quality-filtered representatives): {effective_duplicate_description_clusters}")
    lines.append(f"- Representative pages in duplicate description clusters: {effective_duplicate_description_pages}")
    lines.append(f"- Exact duplicate content clusters (issue-level): {exact_duplicate_issue_clusters}")
    lines.append(f"- URLs in exact duplicate clusters (issue-level): {exact_duplicate_issue_pages}")
    lines.append(f"- Exact duplicate content clusters (diagnostic candidates): {exact_duplicate_diagnostic_clusters}")
    lines.append(f"- Representative pages in exact duplicate clusters (diagnostic candidates): {exact_duplicate_diagnostic_pages}")
    lines.append(f"- Raw duplicate title clusters (diagnostic): {raw_duplicate_title_clusters}")
    lines.append(f"- Raw duplicate description clusters (diagnostic): {raw_duplicate_description_clusters}")

    lines.append("\n## Internal linking findings")
    lines.append(f"- Orphan risk pages: {sum(1 for row in pages if row['orphan_risk_flag'])}")
    lines.append(
        f"- Low internal outlink pages: {sum(1 for row in pages if ((row['effective_internal_links_out'] if ('effective_internal_links_out' in row.keys() and row['effective_internal_links_out'] is not None) else row['internal_links_out']) or 0) < LOW_INTERNAL_LINKS_THRESHOLD)}"
    )

    lines.append("\n## Render/shell telemetry")
    lines.append(f"- Likely JS-shell pages: {sum(1 for row in pages if (row['likely_js_shell'] or 0))}")
    lines.append(f"- Pages rendered: {sum(1 for row in pages if (row['used_render'] or 0))}")
    lines.append(f"- Render failures: {sum(1 for row in pages if bool(row['render_error']))}")

    lines.append("\n## Snippet and citation controls")
    lines.append(f"- Pages with nosnippet directives (meta or X-Robots-Tag): {nosnippet_directive_pages}")
    lines.append(f"- Elements marked data-nosnippet: {data_nosnippet_elements}")
    lines.append(f"- Pages with max-snippet directive: {max_snippet_pages}")
    lines.append(f"- Pages with max-image-preview directive: {max_image_preview_pages}")
    lines.append(f"- Pages with max-video-preview directive: {max_video_preview_pages}")
    lines.append(f"- Pages with restrictive snippet controls (issue-level): {len(restrictive_snippet_pages)}")
    if restrictive_snippet_pages:
        lines.append(
            "- Restrictive-control sample pages: "
            + ", ".join(str(row.get("normalized_url") or "") for row in restrictive_snippet_pages[:5])
        )
    lines.append(f"- Pages with restrictive snippet directives (diagnostic-only, non-issue): {len(restrictive_snippet_diagnostic_pages)}")
    lines.append(f"- Pages with heavy data-nosnippet usage (>=4 elements): {len(heavy_data_nosnippet_pages)}")
    if heavy_data_nosnippet_pages:
        lines.append(
            "- Heavy data-nosnippet sample: "
            + ", ".join(
                f"{str(row.get('normalized_url') or '')} ({as_int(row.get('data_nosnippet_count'), 0)})"
                for row in heavy_data_nosnippet_pages[:5]
            )
        )

    lines.append("\n## AI discoverability potential vs evidence")
    if ai_potential_scores:
        avg_ai_potential = sum(ai_potential_scores) / len(ai_potential_scores)
        lines.append(f"- Average AI discoverability potential score: {avg_ai_potential:.1f}")
        lines.append(
            f"- Pages with observed visibility signals (GSC/chatgpt referrals): {ai_observed_signal_pages}"
        )
    else:
        lines.append("- No AI discoverability potential scores were captured.")

    if ai_adapter_counts:
        adapter_summary = ", ".join(
            f"{name}={count}"
            for name, count in ai_adapter_counts.most_common(8)
        )
        lines.append(f"- Evidence adapters observed: {adapter_summary}")
    else:
        lines.append("- Evidence adapters observed: none")

    lines.append("\n## Local SEO findings")
    lines.append(
        f"- Pages with low local SEO completeness (<{LOW_LOCAL_SEO_SCORE_THRESHOLD}): "
        f"{sum(1 for row in scores if (row['local_seo_score'] is not None and int(row['local_seo_score']) >= 0 and int(row['local_seo_score']) < LOW_LOCAL_SEO_SCORE_THRESHOLD))}"
    )

    lines.append("\n## Performance findings")
    lines.append("- Field performance should be read from CrUX rows first; PSI is primarily lab data and may have limited field snapshots.")
    psi_rows = [row for row in perf if (row["source"] or "") == "psi"]
    lines.append(
        f"- PSI rows with performance score < {LOW_PSI_PERFORMANCE_THRESHOLD}: "
        f"{sum(1 for row in psi_rows if (row['performance_score'] is not None and int(row['performance_score']) < LOW_PSI_PERFORMANCE_THRESHOLD))}"
    )
    lines.append(
        f"- Lighthouse rows with budget failures: "
        f"{sum(1 for row in lighthouse if int(row.get('budget_pass') or 0) == 0)}"
    )
    lines.append(
        f"- Lighthouse failed/skipped rows: "
        f"{sum(1 for row in lighthouse if str(row.get('status') or '').lower() != 'success')}"
    )

    lines.append("\n## Performance by template group")
    for group in ("home", "product", "service", "blog", "taxonomy", "other"):
        rows_count = perf_rows_by_group.get(group, 0)
        scores_for_group = perf_scores_by_group.get(group, [])
        if not rows_count:
            lines.append(f"- {group}: rows=0")
            continue
        avg_score = sum(scores_for_group) / len(scores_for_group) if scores_for_group else 0.0
        lines.append(
            f"- {group}: rows={rows_count} scored_rows={len(scores_for_group)} avg_perf_score={avg_score:.1f}"
        )

    lines.append("\n## CrUX findings")
    crux_status = Counter(row["status"] for row in crux)
    if crux_status:
        lines.append("- CrUX rows represent origin/URL field aggregates from Chrome UX Report availability.")
        lines.extend([f"- {k}: {v}" for k, v in sorted(crux_status.items())])
    else:
        lines.append("- No CrUX rows collected for this run.")

    notes = (run.get("notes", "") if isinstance(run, dict) else run["notes"]) or ""
    lines.append("\n## Run notes")
    lines.append(f"- {notes if notes else 'none'}")

    lines.append("\n## Top-priority actions")
    lines.append("- Fix money-page critical/high issues first (fetch failures, noindex, missing titles).")
    lines.append("- Treat utility/taxonomy/template findings as a separate hygiene backlog.")
    lines.append("- Improve internal linking to reduce orphan risk on priority pages.")
    lines.append("- Review render gap pages for client-side SEO risk.")

    lines.append("\n## Limitations")
    lines.append("- Public-data-only analysis; Search Console reconciliation depends on configured property access and sampled URL inspection targets.")
    lines.append("- Render and performance collection may be partial when dependencies/APIs are unavailable.")

    out_path.write_text("\n".join(lines), encoding="utf-8")
