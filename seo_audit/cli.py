from __future__ import annotations

import argparse
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

from seo_audit.ai_visibility import (
    build_ai_visibility_payload,
    legacy_citation_evidence_from_payload,
    merge_ai_visibility_payload,
    parse_ai_visibility_payload,
)
from seo_audit.classify import classify_page_result, has_local_business_schema
from seo_audit.citation import build_citation_evidence, compute_citation_eligibility
from seo_audit.config import AuditConfig
from seo_audit.crawler import CURRENT_EXTRACTOR_VERSION, CURRENT_SCHEMA_RULE_VERSION, crawl_site
from seo_audit.discovery import seed_urls
from seo_audit.diffing import generate_page_diffs
from seo_audit.governance_matrix import build_governance_matrix, summarize_governance_matrices
from seo_audit.gsc_analytics import collect_search_analytics, default_date_window, summarize_search_analytics
from seo_audit.integrations import AdapterContext, GSCAnalyticsVisibilityAdapter, apply_visibility_adapters
from seo_audit.issues import build_issues, enrich_issues
from seo_audit.lighthouse import LighthouseBudgetConfig, collect_lighthouse
from seo_audit.linkgraph import compute_graph_metrics, compute_link_metrics
from seo_audit.logging_utils import configure_logging
from seo_audit.models import (
    AIVisibilityRecord,
    CitationEventRecord,
    IndexStateHistoryRecord,
    IssueRecord,
    MediaAssetRecord,
    PageDiffRecord,
    PageGraphMetricsRecord,
    PageSnapshotRecord,
    RenderSessionRecord,
    SchemaValidationRecord,
    ScoreRecord,
    SubmissionEventRecord,
    TemplateClusterRecord,
    OffsiteCommonCrawlComparisonRecord,
    OffsiteCommonCrawlSummaryRecord,
    OffsiteCommonCrawlLinkingDomainRecord,
)
from seo_audit.offsite_commoncrawl import (
    OFFSITE_COMMONCRAWL_DEFAULT_SCHEDULE,
    OFFSITE_COMMONCRAWL_MODES,
    OFFSITE_COMMONCRAWL_SCHEDULES,
    STATUS_FAILED_QUERY,
    STATUS_PENDING_BACKGROUND,
    STATUS_SKIPPED_DISABLED,
    STATUS_TIMEOUT_BACKGROUND,
    OffsiteCommonCrawlWorkerControl,
    OffsiteCommonCrawlWorkerPayload,
    OffsiteCommonCrawlSummaryPayload,
    OffsiteCommonCrawlWorkerRequest,
    canonicalize_offsite_schedule,
    inspect_commoncrawl_launch,
    normalize_domain_for_commoncrawl,
    run_offsite_commoncrawl_worker,
)
from seo_audit.performance import ProviderRetryConfig, TokenBucketRateLimiter, collect_crux, collect_performance
from seo_audit.personas import PERSONAS, resolve_crawl_persona
from seo_audit.render import PlaywrightRenderer, choose_render_sample, compute_render_gap, score_render_escalation
from seo_audit.reporting import build_markdown_report
from seo_audit.resolution import crawl_persona_prefers_rendered, parse_effective_field_provenance, resolve_effective_page_facts
from seo_audit.robots import RobotsData, fetch_robots_with_persona, is_allowed, resolve_crawl_delay, robots_fetch_summary
from seo_audit.scoring import score_page
from seo_audit.scoring_policy import CURRENT_SCORE_VERSION
from seo_audit.schema_render_diff import compare_schema_sets
from seo_audit.search_console import collect_index_states, property_candidates, reconcile_index_states, resolve_property
from seo_audit.sitemap_analysis import analyze_sitemap_intelligence, collect_optional_gsc_sitemap_status
from seo_audit.sitemaps import default_sitemap_candidates, fetch_and_parse_sitemaps
from seo_audit.storage import Storage
from seo_audit.url_utils import internal_hosts_for_site, is_internal_url, normalize_url
from seo_audit.job_queue import AdmissionPolicy, QueueStore
from seo_audit.queue_worker import run_queue_worker


RUN_PROFILE_DEFAULTS: dict[str, dict[str, int | float | str]] = {
    "exploratory": {
        "max_pages": 50,
        "max_render_pages": 0,
        "render_mode": "none",
        "crawl_discovery_mode": "raw",
        "scope_mode": "host_only",
        "render_interaction_budget": 0,
        "faceted_sample_rate": 0.0,
        "performance_targets": 1,
        "provider_max_retries": 1,
        "provider_base_backoff_seconds": 0.25,
        "provider_max_backoff_seconds": 2.0,
        "provider_max_total_wait_seconds": 8.0,
        "crawl_heartbeat_every_pages": 10,
    },
    "standard": {
        "max_pages": 200,
        "max_render_pages": 20,
        "render_mode": "sample",
        "crawl_discovery_mode": "hybrid",
        "scope_mode": "apex_www",
        "render_interaction_budget": 0,
        "faceted_sample_rate": 1.0,
        "performance_targets": 3,
        "provider_max_retries": 2,
        "provider_base_backoff_seconds": 0.5,
        "provider_max_backoff_seconds": 6.0,
        "provider_max_total_wait_seconds": 20.0,
        "crawl_heartbeat_every_pages": 25,
    },
    "deep": {
        "max_pages": 500,
        "max_render_pages": 120,
        "render_mode": "sample",
        "crawl_discovery_mode": "browser_first",
        "scope_mode": "all_subdomains",
        "render_interaction_budget": 1,
        "faceted_sample_rate": 1.0,
        "performance_targets": 20,
        "provider_max_retries": 3,
        "provider_base_backoff_seconds": 0.5,
        "provider_max_backoff_seconds": 8.0,
        "provider_max_total_wait_seconds": 35.0,
        "crawl_heartbeat_every_pages": 50,
    },
}

DEFAULT_FACETED_PARAM_KEYS: tuple[str, ...] = (
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

DEFAULT_ACTION_PARAM_KEYS: tuple[str, ...] = (
    "add-to-cart",
    "add_to_cart",
    "replytocom",
    "session",
    "sessionid",
    "token",
    "auth",
    "nonce",
)

DEFAULT_DIAGNOSTIC_PARAM_KEYS: tuple[str, ...] = (
    "preview",
    "amp",
    "variant",
)

DEFAULT_HEADERS_ONLY_PARAM_KEYS: tuple[str, ...] = (
    "download",
    "export",
    "format",
)

DEFAULT_CANONICAL_CANDIDATE_PARAM_KEYS: tuple[str, ...] = (
    "sort",
    "order",
    "view",
    "variant",
)


def _parse_scope_allowlist(raw_value: object) -> tuple[str, ...]:
    if raw_value is None:
        return ()
    if isinstance(raw_value, (list, tuple, set)):
        tokens = [str(item).strip() for item in raw_value]
    else:
        tokens = [part.strip() for part in str(raw_value).split(",")]
    normalized = [token for token in tokens if token]
    return tuple(dict.fromkeys(normalized))


def _parse_wait_ladder_ms(raw_value: object) -> tuple[int, ...]:
    default = (500, 1200, 2500)
    if raw_value is None:
        return default
    if isinstance(raw_value, (list, tuple)):
        numbers: list[int] = []
        for value in raw_value:
            try:
                numbers.append(max(0, int(value)))
            except (TypeError, ValueError):
                continue
        return tuple(numbers) if numbers else default
    text = str(raw_value).strip()
    if not text:
        return default
    numbers = []
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            numbers.append(max(0, int(part)))
        except ValueError:
            continue
    return tuple(numbers) if numbers else default


def _parse_csv_tokens(raw_value: object, *, default: tuple[str, ...] = ()) -> tuple[str, ...]:
    if raw_value is None:
        return default
    if isinstance(raw_value, (list, tuple, set)):
        tokens = [str(item).strip() for item in raw_value]
    else:
        tokens = [part.strip() for part in str(raw_value).split(",")]
    normalized = tuple(token for token in tokens if token)
    return normalized or default


def _normalize_offsite_mode(raw_value: object, *, allow_experimental_verify: bool = False) -> str:
    mode = str(raw_value or "ranks").strip().lower()
    if mode == "verify" and not allow_experimental_verify:
        return "ranks"
    if mode not in OFFSITE_COMMONCRAWL_MODES:
        return "ranks"
    return mode


def _normalize_offsite_schedule(raw_value: object) -> str:
    schedule = canonicalize_offsite_schedule(raw_value)
    if schedule not in OFFSITE_COMMONCRAWL_SCHEDULES:
        return OFFSITE_COMMONCRAWL_DEFAULT_SCHEDULE
    return schedule


def _parse_offsite_compare_domains(raw_value: object) -> tuple[str, ...]:
    if raw_value is None:
        return ()
    if isinstance(raw_value, (list, tuple, set)):
        values = [str(item).strip() for item in raw_value]
    else:
        values = [part.strip() for part in str(raw_value).split(",")]

    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        domain = normalize_domain_for_commoncrawl(value)
        if not domain or domain in seen:
            continue
        seen.add(domain)
        normalized.append(domain)
    return tuple(normalized)


def _build_pending_offsite_payload(
    *,
    target_domain: str,
    release: str,
    mode: str,
    schedule: str,
    cache_state: str,
    status: str,
    reason: str,
    started_at: str,
    compare_domains: tuple[str, ...],
) -> OffsiteCommonCrawlWorkerPayload:
    finished_at = datetime.now(timezone.utc).isoformat()
    notes_json = json.dumps(
        {
            "release": release,
            "mode": mode,
            "schedule": schedule,
            "status": status,
            "reason": reason,
            "compare_domains": list(compare_domains),
            "execution_scope": "concurrent_within_current_audit_process_only",
            "linking_domain_semantics": "Common Crawl domain graph linking domains; not exact page-level backlinks.",
        },
        sort_keys=True,
    )
    return OffsiteCommonCrawlWorkerPayload(
        summary=OffsiteCommonCrawlSummaryPayload(
            target_domain=target_domain,
            cc_release=release,
            mode=mode,
            schedule=schedule,
            status=status,
            cache_state=cache_state,
            target_found_flag=0,
            harmonic_centrality=None,
            pagerank=None,
            referring_domain_count=0,
            weighted_referring_domain_score=None,
            avg_referrer_harmonic=None,
            avg_referrer_pagerank=None,
            top_referrer_concentration=None,
            comparison_domain_count=len(compare_domains),
            query_elapsed_ms=0,
            background_started_at=started_at,
            background_finished_at=finished_at,
            notes_json=notes_json,
        ),
        linking_domains=[],
        comparisons=[],
    )


def _join_offsite_commoncrawl_future(
    *,
    future: Future[OffsiteCommonCrawlWorkerPayload] | None,
    schedule: str,
    join_budget_seconds: float,
    control: OffsiteCommonCrawlWorkerControl | None,
) -> tuple[OffsiteCommonCrawlWorkerPayload | None, str]:
    if future is None:
        return None, "disabled"

    if future.done():
        return future.result(), "ready"

    normalized_schedule = _normalize_offsite_schedule(schedule)
    budget = max(0.0, float(join_budget_seconds))

    if normalized_schedule == "concurrent_best_effort":
        tiny_budget = min(budget, 0.5)
        if tiny_budget <= 0.0:
            if control is not None:
                control.request_interrupt()
            return None, "deferred"
        try:
            return future.result(timeout=tiny_budget), "ready"
        except FuturesTimeoutError:
            if control is not None:
                control.request_interrupt()
            return None, "deferred"

    if normalized_schedule == "background_wait":
        if budget <= 0.0:
            if control is not None:
                control.request_interrupt()
            return None, "timeout"
        try:
            return future.result(timeout=budget), "ready"
        except FuturesTimeoutError:
            if control is not None:
                control.request_interrupt()
            return None, "timeout"

    return future.result(), "ready"


def _persist_offsite_commoncrawl_payload(
    storage: Storage,
    run_id: str,
    payload: OffsiteCommonCrawlWorkerPayload,
) -> None:
    summary = payload.summary
    storage.insert_offsite_commoncrawl_summary(
        [
            OffsiteCommonCrawlSummaryRecord(
                run_id=run_id,
                target_domain=str(summary.target_domain or ""),
                cc_release=str(summary.cc_release or ""),
                mode=str(summary.mode or ""),
                schedule=str(summary.schedule or ""),
                status=str(summary.status or ""),
                cache_state=str(summary.cache_state or ""),
                target_found_flag=int(summary.target_found_flag or 0),
                harmonic_centrality=summary.harmonic_centrality,
                pagerank=summary.pagerank,
                referring_domain_count=int(summary.referring_domain_count or 0),
                weighted_referring_domain_score=summary.weighted_referring_domain_score,
                avg_referrer_harmonic=summary.avg_referrer_harmonic,
                avg_referrer_pagerank=summary.avg_referrer_pagerank,
                top_referrer_concentration=summary.top_referrer_concentration,
                comparison_domain_count=int(summary.comparison_domain_count or 0),
                query_elapsed_ms=int(summary.query_elapsed_ms or 0),
                background_started_at=str(summary.background_started_at or ""),
                background_finished_at=str(summary.background_finished_at or ""),
                notes_json=str(summary.notes_json or "{}"),
            )
        ]
    )

    if payload.linking_domains:
        storage.insert_offsite_commoncrawl_linking_domains(
            [
                OffsiteCommonCrawlLinkingDomainRecord(
                    run_id=run_id,
                    target_domain=str(summary.target_domain or ""),
                    linking_domain=str(row.linking_domain or ""),
                    source_num_hosts=int(row.source_num_hosts or 0),
                    source_harmonic_centrality=row.source_harmonic_centrality,
                    source_pagerank=row.source_pagerank,
                    rank_bucket=str(row.rank_bucket or ""),
                    evidence_json=str(row.evidence_json or "{}"),
                )
                for row in payload.linking_domains
            ]
        )

    if payload.comparisons:
        storage.insert_offsite_commoncrawl_comparisons(
            [
                OffsiteCommonCrawlComparisonRecord(
                    run_id=run_id,
                    target_domain=str(summary.target_domain or ""),
                    compare_domain=str(row.compare_domain or ""),
                    cc_release=str(row.cc_release or summary.cc_release or ""),
                    harmonic_centrality=row.harmonic_centrality,
                    pagerank=row.pagerank,
                    rank_gap_vs_target=row.rank_gap_vs_target,
                    pagerank_gap_vs_target=row.pagerank_gap_vs_target,
                )
                for row in payload.comparisons
            ]
        )


def _is_html_like_page(page: dict) -> bool:
    content_type = (page.get("content_type") or "").lower()
    if "html" in content_type:
        return True
    return bool(page.get("title") or page.get("h1") or page.get("word_count"))


def _is_system_url(url: str) -> bool:
    path = (urlsplit(url).path or "").lower()
    basename = path.rsplit("/", 1)[-1]
    if basename in {"robots.txt", "sitemap.xml", "sitemap.xml.gz", "sitemap_index.xml"}:
        return True
    return basename.endswith(".xml")


def _is_actionable_html_page(page: dict) -> bool:
    url = page.get("normalized_url") or ""
    if not url or page.get("fetch_error"):
        return False
    status_code = int(page.get("status_code") or 0)
    if status_code < 200 or status_code >= 400:
        return False
    if _is_system_url(url):
        return False
    if not _is_html_like_page(page):
        return False
    return True


def select_performance_targets(pages: list[dict], limit: int) -> list[str]:
    if limit <= 0:
        return []

    priority = {
        "homepage": 0,
        "service": 1,
        "contact": 2,
        "about": 3,
        "location": 4,
        "article": 5,
        "industry": 6,
        "other": 7,
    }

    def stability_bucket(page: dict) -> int:
        # Prefer pages that look less JS-shell-like, because they are less likely
        # to trigger repeated provider runtime errors (for example, NO_FCP).
        render_gap = int(page.get("render_gap_score") or 0)
        if render_gap >= 90:
            return 1
        return 0

    eligible: list[dict] = []
    for page in pages:
        if _is_actionable_html_page(page):
            eligible.append(page)

    ranked = sorted(
        eligible,
        key=lambda page: (
            0 if (page.get("page_type") or "other") == "homepage" else 1,
            stability_bucket(page),
            priority.get(page.get("page_type") or "other", 99),
            int(page.get("crawl_depth") or 99),
            -int(page.get("internal_links_out") or 0),
            -(int(page.get("word_count") or 0)),
            page.get("normalized_url") or "",
        ),
    )

    selected: list[str] = []
    seen: set[str] = set()
    for page in ranked:
        url = page.get("normalized_url") or ""
        if not url or url in seen:
            continue
        seen.add(url)
        selected.append(url)
        if len(selected) >= limit:
            break
    return selected


def select_render_targets(
    pages: list[dict],
    render_mode: str,
    max_render_pages: int,
    *,
    adaptive_escalation: bool = True,
) -> list[dict]:
    eligible = [page for page in pages if _is_actionable_html_page(page)]

    if not adaptive_escalation:
        likely_shell_pages = [page for page in eligible if int(page.get("likely_js_shell") or 0) == 1]
        if render_mode == "all":
            return eligible
        if render_mode == "sample":
            if likely_shell_pages:
                return choose_render_sample(likely_shell_pages, max_render_pages)
            return choose_render_sample(eligible, max_render_pages)
        return []

    scored: list[tuple[dict, int, list[str]]] = []
    for page in eligible:
        escalation_score, escalation_reasons = score_render_escalation(page)
        scored.append((page, escalation_score, escalation_reasons))

    ranked = sorted(
        scored,
        key=lambda row: (
            -int(row[1] or 0),
            0 if (row[0].get("page_type") or "other") == "homepage" else 1,
            int(row[0].get("crawl_depth") or 99),
            -(int(row[0].get("internal_links_out") or 0)),
            row[0].get("normalized_url") or "",
        ),
    )

    if render_mode == "all":
        return [row[0] for row in ranked]

    if render_mode == "sample":
        selected: list[dict] = []
        seen: set[str] = set()
        for page, escalation_score, _ in ranked:
            if int(escalation_score or 0) < 35:
                continue
            url = str(page.get("normalized_url") or "")
            if not url or url in seen:
                continue
            selected.append(page)
            seen.add(url)
            if len(selected) >= max_render_pages:
                return selected

        fallback = choose_render_sample(eligible, max_render_pages)
        for page in fallback:
            url = str(page.get("normalized_url") or "")
            if not url or url in seen:
                continue
            selected.append(page)
            seen.add(url)
            if len(selected) >= max_render_pages:
                break
        return selected

    return []


def _shell_reasons_from_signals_json(shell_signals_json: str) -> list[str]:
    if not shell_signals_json:
        return []
    try:
        parsed = json.loads(shell_signals_json)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, dict):
        return []

    reasons = parsed.get("reasons", [])
    if isinstance(reasons, list):
        return [str(reason).strip() for reason in reasons if str(reason).strip()]
    if isinstance(reasons, str):
        stripped = reasons.strip()
        return [stripped] if stripped else []
    return []


def _format_render_reasons(reasons: list[str]) -> str:
    return "; ".join(reasons)


def _count_internal_rendered_links(
    anchors: list[dict],
    root_domain: str,
    base_url: str,
    *,
    scope_mode: str = "apex_www",
    custom_allowlist: tuple[str, ...] | list[str] | set[str] | None = None,
) -> int:
    count = 0
    for anchor in anchors:
        href = str((anchor or {}).get("href") or "").strip()
        if not href:
            continue
        if is_internal_url(
            href,
            root_domain,
            base_url=base_url,
            scope_mode=scope_mode,
            custom_allowlist=custom_allowlist,
        ):
            count += 1
    return count


def _json_object(raw: object) -> dict[str, object]:
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return dict(parsed)
    return {}


def _json_dict_list(raw: object) -> list[dict]:
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return [item for item in parsed if isinstance(item, dict)]
    return []


def _json_list(raw: object) -> list[object]:
    if isinstance(raw, list):
        return list(raw)
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return list(parsed)
    return []


def _render_failure_family(render_error: str) -> str:
    message = str(render_error or "").strip().lower()
    if not message:
        return ""
    if "timeout" in message:
        return "timeout"
    if "playwright import failed" in message or "browser unavailable" in message:
        return "environment"
    if "navigation" in message:
        return "navigation"
    if "render failure" in message:
        return "render_failure"
    return "runtime"


def _governance_summary_for_pages(pages: list[dict]) -> dict[str, object]:
    actionable = [page for page in pages if _is_actionable_html_page(page)]
    summary: dict[str, object] = {
        "actionable_pages": len(actionable),
    }
    bot_keys = (
        ("googlebot", "governance_googlebot_allowed"),
        ("bingbot", "governance_bingbot_allowed"),
        ("oai_searchbot", "governance_openai_allowed"),
        ("google_extended", "governance_google_extended_allowed"),
        ("gptbot", "governance_gptbot_allowed"),
        ("oai_adsbot", "governance_oai_adsbot_allowed"),
        ("chatgpt_user", "governance_chatgpt_user_allowed"),
    )
    for label, key in bot_keys:
        blocked = [
            str(page.get("normalized_url") or "")
            for page in actionable
            if int(page.get(key, 1) or 0) == 0
        ]
        summary[f"{label}_blocked_pages"] = len(blocked)
        summary[f"{label}_blocked_sample_urls"] = blocked[:10]
    return summary


def _apply_canonical_clusters(pages: list[dict]) -> dict[str, int]:
    cluster_members: dict[str, list[dict]] = {}
    contexts: list[tuple[dict, str, str, str, str]] = []

    for page in pages:
        normalized_url = normalize_url(
            str(page.get("normalized_url") or ""),
            base_url=str(page.get("final_url") or ""),
        )
        final_url = normalize_url(
            str(page.get("final_url") or normalized_url),
            base_url=normalized_url,
        )
        canonical_raw = str(page.get("effective_canonical") or page.get("canonical_url") or "").strip()
        canonical_target = normalize_url(canonical_raw, base_url=final_url or normalized_url) if canonical_raw else ""

        if canonical_target:
            cluster_key = canonical_target
            cluster_role = "self" if canonical_target == final_url else "alias"
        else:
            cluster_key = final_url or normalized_url
            cluster_role = "standalone"

        cluster_members.setdefault(cluster_key, []).append(page)
        contexts.append((page, cluster_key, cluster_role, final_url, canonical_target))

    alias_pages = 0
    multi_member_clusters = 0
    for members in cluster_members.values():
        if len(members) > 1:
            multi_member_clusters += 1

    for page, cluster_key, cluster_role, final_url, canonical_target in contexts:
        member_count = len(cluster_members.get(cluster_key, []))
        indexable_member_count = sum(int(int(item.get("is_indexable", 1) or 0) == 1) for item in cluster_members.get(cluster_key, []))

        if cluster_role == "alias":
            alias_pages += 1

        page["canonical_cluster_key"] = cluster_key
        page["canonical_cluster_role"] = cluster_role
        page["canonical_signal_summary_json"] = json.dumps(
            {
                "normalized_url": str(page.get("normalized_url") or ""),
                "final_url": final_url,
                "canonical_target": canonical_target,
                "canonical_matches_final": int(bool(canonical_target) and canonical_target == final_url),
                "cluster_member_count": member_count,
                "cluster_indexable_member_count": indexable_member_count,
                "is_indexable": int(page.get("is_indexable", 1) or 0),
                "is_noindex": int(page.get("is_noindex", 0) or 0),
                "robots_blocked_flag": int(page.get("robots_blocked_flag", 0) or 0),
            },
            sort_keys=True,
        )

    return {
        "canonical_clusters_total": len(cluster_members),
        "canonical_clusters_multi_member": multi_member_clusters,
        "canonical_alias_pages": alias_pages,
    }


def _build_sitemap_delta_issues(run_id: str, domain: str, pages: list[dict], sitemap_entries: list[dict]) -> list[IssueRecord]:
    if not sitemap_entries:
        return []

    sitemap_urls = {
        normalize_url(str(entry.get("url") or ""), base_url=domain)
        for entry in sitemap_entries
        if str(entry.get("url") or "").strip()
    }
    pages_by_url = {str(page.get("normalized_url") or ""): page for page in pages}
    crawled_urls = {url for url in pages_by_url if url}

    uncrawled_urls = sorted(url for url in sitemap_urls if url not in crawled_urls)
    actionable_crawled = [url for url, page in pages_by_url.items() if _is_actionable_html_page(page)]
    crawled_not_in_sitemap = sorted(url for url in actionable_crawled if url not in sitemap_urls)

    issues: list[IssueRecord] = []
    if uncrawled_urls:
        issues.append(
            IssueRecord(
                run_id,
                domain,
                "medium",
                "SITEMAP_URL_NOT_CRAWLED",
                "Sitemap URLs were not crawled",
                "URLs listed in sitemap were not reached during crawl.",
                evidence_json=json.dumps({"count": len(uncrawled_urls), "sample": uncrawled_urls[:20]}),
                technical_seo_gate="discovery",
                verification_status="automated",
                confidence_score=90,
            )
        )
    if crawled_not_in_sitemap:
        issues.append(
            IssueRecord(
                run_id,
                domain,
                "low",
                "CRAWLED_URL_NOT_IN_SITEMAP",
                "Crawled URLs missing from sitemap",
                "Actionable crawled pages were not present in sitemap entries.",
                evidence_json=json.dumps({"count": len(crawled_not_in_sitemap), "sample": crawled_not_in_sitemap[:20]}),
                technical_seo_gate="discovery",
                verification_status="automated",
                confidence_score=85,
            )
        )
    return issues


def _validate_robots_bypass_flags(args: argparse.Namespace) -> None:
    ignore_robots = bool(getattr(args, "ignore_robots", False))
    bypass_ack = bool(getattr(args, "i_understand_robots_bypass", False))
    if ignore_robots and not bypass_ack:
        raise ValueError("--ignore-robots requires --i-understand-robots-bypass")


def _resolve_run_profile_values(args: argparse.Namespace) -> tuple[str, dict[str, int | float | str]]:
    requested = str(getattr(args, "run_profile", "standard") or "standard").lower()
    profile = requested if requested in RUN_PROFILE_DEFAULTS else "standard"
    defaults = RUN_PROFILE_DEFAULTS[profile]

    def pick(name: str, fallback: int | float | str) -> int | float | str:
        value = getattr(args, name, None)
        if value is None:
            return defaults.get(name, fallback)
        return value

    resolved = {
        "max_pages": pick("max_pages", 200),
        "max_render_pages": pick("max_render_pages", 20),
        "render_mode": pick("render_mode", "sample"),
        "crawl_discovery_mode": pick("crawl_discovery_mode", "raw"),
        "scope_mode": pick("scope_mode", "apex_www"),
        "render_interaction_budget": pick("render_interaction_budget", 0),
        "faceted_sample_rate": pick("faceted_sample_rate", 1.0),
        "performance_targets": pick("performance_targets", 3),
        "provider_max_retries": pick("provider_max_retries", 2),
        "provider_base_backoff_seconds": pick("provider_base_backoff_seconds", 0.5),
        "provider_max_backoff_seconds": pick("provider_max_backoff_seconds", 6.0),
        "provider_max_total_wait_seconds": pick("provider_max_total_wait_seconds", 20.0),
        "crawl_heartbeat_every_pages": pick("crawl_heartbeat_every_pages", 25),
    }
    return profile, resolved


def _fresh_output_dir(base_output: Path) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    candidate = base_output / f"run-{stamp}"
    suffix = 1
    while candidate.exists():
        candidate = base_output / f"run-{stamp}-{suffix}"
        suffix += 1
    return candidate


def _resolve_output_dir(args: argparse.Namespace) -> Path:
    base_output = Path(args.output)
    if bool(getattr(args, "fresh_output_dir", False)):
        return _fresh_output_dir(base_output)
    return base_output


def _version_invalidation_flags(config: AuditConfig, previous_config: dict) -> dict[str, int]:
    prev_extractor = str(previous_config.get("extractor_version") or "").strip()
    prev_schema = str(previous_config.get("schema_rule_version") or "").strip()
    prev_scoring = str(previous_config.get("scoring_version") or "").strip()
    return {
        "extractor_version_changed": int(bool(prev_extractor) and prev_extractor != str(config.extractor_version)),
        "schema_rule_version_changed": int(bool(prev_schema) and prev_schema != str(config.schema_rule_version)),
        "scoring_version_changed": int(bool(prev_scoring) and prev_scoring != str(config.scoring_version)),
    }


def _plan_crawl_start_urls(
    storage: Storage,
    config: AuditConfig,
    base_start_urls: list[str],
    *,
    known_sitemap_urls: list[str],
    recently_changed_urls: list[str],
) -> tuple[list[str], dict[str, int]]:
    candidates: list[str] = []
    seen: set[str] = set()
    for raw_url in [*base_start_urls, *known_sitemap_urls]:
        normalized = normalize_url(str(raw_url or ""), base_url=config.domain)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        candidates.append(normalized)

    if not config.incremental_crawl_enabled:
        return candidates, {
            "planner_discovered": len(candidates),
            "planner_seed_urls": len(base_start_urls),
            "planner_known_sitemap_urls": len(known_sitemap_urls),
            "planner_prioritized_changed_urls": 0,
            "planner_incremental_enabled": 0,
        }

    changed_set = {
        normalize_url(str(url or ""), base_url=config.domain)
        for url in recently_changed_urls
        if str(url or "").strip()
    }
    changed_set.discard("")

    url_state_rows = storage.get_url_states(candidates)

    def sort_key(url: str) -> tuple[int, int, str]:
        state_row = url_state_rows.get(url) or {}
        not_modified_streak = int(state_row.get("not_modified_streak") or 0)
        return (
            0 if url in changed_set else 1,
            not_modified_streak,
            url,
        )

    planned_urls = sorted(candidates, key=sort_key)
    return planned_urls, {
        "planner_discovered": len(planned_urls),
        "planner_seed_urls": len(base_start_urls),
        "planner_known_sitemap_urls": len(known_sitemap_urls),
        "planner_prioritized_changed_urls": sum(1 for url in planned_urls if url in changed_set),
        "planner_incremental_enabled": 1,
    }


PAGE_UPDATE_COLUMNS: tuple[str, ...] = (
    "crawl_depth",
    "nav_linked_flag",
    "orphan_risk_flag",
    "page_type",
    "render_checked",
    "rendered_word_count",
    "render_gap_score",
    "render_gap_reason",
    "raw_title",
    "raw_meta_description",
    "raw_canonical",
    "raw_canonical_urls_json",
    "raw_hreflang_links_json",
    "raw_h1_count",
    "raw_text_len",
    "raw_links_json",
    "raw_content_hash",
    "rendered_title",
    "rendered_meta_description",
    "rendered_canonical",
    "rendered_canonical_urls_json",
    "rendered_canonical_count",
    "rendered_h1_count",
    "rendered_h1s_json",
    "rendered_text_len",
    "rendered_links_json",
    "rendered_hreflang_links_json",
    "rendered_content_hash",
    "rendered_effective_robots_json",
    "rendered_network_requests_json",
    "rendered_api_endpoints_json",
    "rendered_wait_profile",
    "rendered_interaction_count",
    "rendered_action_recipe",
    "rendered_discovery_links_out",
    "effective_title",
    "effective_meta_description",
    "effective_canonical",
    "effective_hreflang_links_json",
    "effective_content_hash",
    "effective_field_provenance_json",
    "canonical_cluster_key",
    "canonical_cluster_role",
    "canonical_signal_summary_json",
    "effective_h1_count",
    "effective_text_len",
    "effective_links_json",
    "effective_internal_links_out",
    "used_render",
    "render_reason",
    "render_error",
    "framework_guess",
    "shell_score",
    "likely_js_shell",
    "shell_state",
    "shell_signals_json",
    "measurement_status",
    "measurement_error_family",
    "platform_family",
    "platform_confidence",
    "platform_signals_json",
    "platform_template_hint",
    "governance_googlebot_allowed",
    "governance_bingbot_allowed",
    "governance_openai_allowed",
    "governance_google_extended_allowed",
    "governance_gptbot_allowed",
    "governance_oai_adsbot_allowed",
    "governance_chatgpt_user_allowed",
    "governance_matrix_json",
    "ai_discoverability_potential_score",
    "ai_visibility_json",
    "citation_eligibility_score",
    "citation_evidence_json",
    "image_details_json",
    "image_discoverability_score",
    "video_details_json",
    "video_discoverability_score",
    "schema_graph_json",
    "schema_validation_json",
    "schema_validation_score",
    "render_failure_family",
    "rendered_console_errors_json",
    "rendered_console_warnings_json",
    "rendered_js_endpoints_json",
    "frontier_priority_score",
    "frontier_cluster_key",
    "frontier_cluster_rank",
    "changed_since_last_run",
    "duplicate_title_flag",
    "duplicate_description_flag",
    "in_sitemap_flag",
)

PROVENANCE_BASELINE_FIELDS: tuple[str, ...] = (
    "title",
    "meta_description",
    "canonical",
    "hreflang",
    "content_hash",
)

DUPLICATE_EXCLUDED_PAGE_TYPES: set[str] = {
    "utility",
    "search",
    "legal",
    "privacy",
    "terms",
    "tag",
    "category",
    "archive",
}

MEASUREMENT_STATUS_RANK: dict[str, int] = {
    "measurement_not_attempted_by_policy": 1,
    "skipped": 1,
    "provider_unavailable_data": 2,
    "no_field_data": 2,
    "runtime_error": 3,
    "provider_runtime_error": 4,
    "provider_error": 5,
    "ok": 6,
}


def _duplicate_metadata_candidate(page: dict, field: str, value: str) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    page_type = str(page.get("page_type") or "").strip().lower()
    if page_type in DUPLICATE_EXCLUDED_PAGE_TYPES:
        return False
    token_count = len([token for token in text.split() if token])
    if field == "title":
        return len(text) >= 8 and token_count >= 2
    if field == "meta_description":
        return len(text) >= 8 and token_count >= 2
    return True


def _representative_pages_by_cluster(pages: list[dict]) -> dict[str, dict]:
    representatives: dict[str, dict] = {}
    for page in pages:
        cluster_key = str(
            page.get("canonical_cluster_key")
            or page.get("final_url")
            or page.get("normalized_url")
            or ""
        ).strip()
        if not cluster_key:
            continue
        current = representatives.get(cluster_key)
        if current is None:
            representatives[cluster_key] = page
            continue
        current_role = str(current.get("canonical_cluster_role") or "").strip().lower()
        role = str(page.get("canonical_cluster_role") or "").strip().lower()
        if current_role == "alias" and role != "alias":
            representatives[cluster_key] = page
    return representatives


def _load_previous_pages_by_url(storage: Storage, run_id: str, urls: list[str]) -> dict[str, dict]:
    normalized_urls = sorted({str(url or "").strip() for url in urls if str(url or "").strip()})
    if not normalized_urls:
        return {}

    result: dict[str, dict] = {}
    chunk_size = 900
    for start in range(0, len(normalized_urls), chunk_size):
        chunk = normalized_urls[start : start + chunk_size]
        placeholders = ", ".join("?" for _ in chunk)
        rows = storage.query(
            f"""
            SELECT *
            FROM pages
            WHERE run_id != ?
              AND normalized_url IN ({placeholders})
            ORDER BY normalized_url ASC, page_id DESC
            """,
            (run_id, *chunk),
        )
        for row in rows:
            row_dict = dict(row)
            normalized_url = str(row_dict.get("normalized_url") or "")
            if normalized_url and normalized_url not in result:
                result[normalized_url] = row_dict
    return result


def _prepare_page_updates(
    *,
    run_id: str,
    pages: list[dict],
    storage: Storage,
    incremental_crawl_enabled: bool,
    raw_title_counts: dict[str, int],
    raw_desc_counts: dict[str, int],
) -> tuple[list[tuple[object, ...]], list[PageSnapshotRecord], list[PageDiffRecord], int, int, int]:
    page_snapshots: list[PageSnapshotRecord] = []
    page_diffs: list[PageDiffRecord] = []
    page_update_rows: list[tuple[object, ...]] = []
    changed_pages = 0

    representatives = _representative_pages_by_cluster(pages)
    representative_urls = {
        str(page.get("normalized_url") or "")
        for page in representatives.values()
        if str(page.get("normalized_url") or "")
    }

    effective_title_counts: dict[str, int] = {}
    effective_desc_counts: dict[str, int] = {}
    for page in representatives.values():
        if int(page.get("is_noindex") or 0) == 1:
            continue
        provenance = parse_effective_field_provenance(page)
        shell_state = str(page.get("shell_state") or "raw_shell_unlikely")
        effective_title = str(page.get("effective_title") or page.get("title") or "").strip()
        effective_desc = str(page.get("effective_meta_description") or page.get("meta_description") or "").strip()
        title_source = str(provenance.get("title") or "")
        desc_source = str(provenance.get("meta_description") or "")
        if (
            _duplicate_metadata_candidate(page, "title", effective_title)
            and not (
                (
                    shell_state == "raw_shell_confirmed_after_render"
                    or (shell_state == "raw_shell_possible" and int(page.get("used_render") or 0) == 1)
                )
                and title_source in {"", "raw", "raw_fallback"}
            )
        ):
            effective_title_counts[effective_title] = effective_title_counts.get(effective_title, 0) + 1
        if (
            _duplicate_metadata_candidate(page, "meta_description", effective_desc)
            and not (
                (
                    shell_state == "raw_shell_confirmed_after_render"
                    or (shell_state == "raw_shell_possible" and int(page.get("used_render") or 0) == 1)
                )
                and desc_source in {"", "raw", "raw_fallback"}
            )
        ):
            effective_desc_counts[effective_desc] = effective_desc_counts.get(effective_desc, 0) + 1

    previous_pages_by_url = _load_previous_pages_by_url(
        storage,
        run_id,
        [str(page.get("normalized_url") or "") for page in pages],
    )

    for page in pages:
        page.setdefault("rendered_effective_robots_json", str(page.get("effective_robots_json") or "{}"))
        page.setdefault("governance_googlebot_allowed", 1)
        page.setdefault("governance_bingbot_allowed", 1)
        page.setdefault("governance_openai_allowed", 1)
        page.setdefault("governance_google_extended_allowed", 1)
        page.setdefault("governance_gptbot_allowed", 1)
        page.setdefault("governance_oai_adsbot_allowed", 1)
        page.setdefault("governance_chatgpt_user_allowed", 1)
        page.setdefault("ai_discoverability_potential_score", int(page.get("citation_eligibility_score") or 0))
        page.setdefault("ai_visibility_json", "{}")
        page.setdefault("citation_eligibility_score", int(page.get("ai_discoverability_potential_score") or 0))
        page.setdefault("citation_evidence_json", "{}")
        page.setdefault("raw_meta_description", str(page.get("meta_description") or ""))
        page.setdefault("raw_canonical_urls_json", str(page.get("canonical_urls_json") or "[]"))
        page.setdefault("raw_hreflang_links_json", str(page.get("hreflang_links_json") or "[]"))
        page.setdefault("raw_content_hash", str(page.get("content_hash") or ""))
        page.setdefault("rendered_meta_description", "")
        page.setdefault("rendered_canonical_urls_json", "[]")
        page.setdefault("rendered_canonical_count", 0)
        page.setdefault("rendered_hreflang_links_json", "[]")
        page.setdefault("rendered_content_hash", "")
        page.setdefault("effective_meta_description", str(page.get("meta_description") or ""))
        page.setdefault("effective_hreflang_links_json", str(page.get("hreflang_links_json") or "[]"))
        page.setdefault("effective_content_hash", str(page.get("content_hash") or ""))
        page.setdefault("effective_field_provenance_json", "{}")
        page.setdefault("shell_state", "raw_shell_unlikely")
        page.setdefault("measurement_status", "measurement_not_attempted_by_policy")
        page.setdefault("measurement_error_family", "not_attempted_policy")

        provenance = parse_effective_field_provenance(page)
        for field in PROVENANCE_BASELINE_FIELDS:
            provenance.setdefault(field, "raw")
        page["effective_field_provenance_json"] = json.dumps(provenance, sort_keys=True)

        shell_state = str(page.get("shell_state") or "raw_shell_unlikely")
        effective_title_value = str(page.get("effective_title") or page.get("title") or "").strip()
        effective_desc_value = str(page.get("effective_meta_description") or page.get("meta_description") or "").strip()
        is_indexable = int(page.get("is_noindex") or 0) == 0
        is_representative = str(page.get("normalized_url") or "") in representative_urls
        title_source = str(provenance.get("title") or "")
        desc_source = str(provenance.get("meta_description") or "")

        page["duplicate_title_flag"] = int(
            is_indexable
            and is_representative
            and _duplicate_metadata_candidate(page, "title", effective_title_value)
            and not (
                (
                    shell_state == "raw_shell_confirmed_after_render"
                    or (shell_state == "raw_shell_possible" and int(page.get("used_render") or 0) == 1)
                )
                and title_source in {"", "raw", "raw_fallback"}
            )
            and effective_title_counts.get(effective_title_value, 0) > 1
        )
        page["duplicate_description_flag"] = int(
            is_indexable
            and is_representative
            and _duplicate_metadata_candidate(page, "meta_description", effective_desc_value)
            and not (
                (
                    shell_state == "raw_shell_confirmed_after_render"
                    or (shell_state == "raw_shell_possible" and int(page.get("used_render") or 0) == 1)
                )
                and desc_source in {"", "raw", "raw_fallback"}
            )
            and effective_desc_counts.get(effective_desc_value, 0) > 1
        )

        normalized_url = str(page.get("normalized_url") or "")
        previous_page = previous_pages_by_url.get(normalized_url)
        current_hash = str(page.get("effective_content_hash") or page.get("content_hash") or "")
        previous_hash = str(
            (previous_page or {}).get("effective_content_hash")
            or (previous_page or {}).get("content_hash")
            or ""
        )

        if incremental_crawl_enabled:
            if current_hash and previous_hash:
                page["changed_since_last_run"] = int(previous_hash != current_hash)
            else:
                page["changed_since_last_run"] = int(page.get("changed_since_last_run") or 0)
        else:
            if current_hash:
                page["changed_since_last_run"] = int((not previous_hash) or previous_hash != current_hash)
            else:
                page["changed_since_last_run"] = 0
        changed_pages += int(page["changed_since_last_run"])

        page_diffs.extend(
            generate_page_diffs(
                run_id,
                normalized_url,
                page,
                previous_page,
            )
        )

        page_snapshots.append(
            PageSnapshotRecord(
                run_id=run_id,
                url=normalized_url,
                content_hash=current_hash,
                last_modified=str(page.get("last_modified") or ""),
                status_code=int(page.get("status_code") or 0),
                changed_flag=int(page["changed_since_last_run"]),
                observed_at=datetime.now(timezone.utc).isoformat(),
                raw_content_hash=str(page.get("raw_content_hash") or page.get("content_hash") or ""),
                rendered_content_hash=str(page.get("rendered_content_hash") or ""),
                effective_content_hash=str(page.get("effective_content_hash") or page.get("content_hash") or ""),
            )
        )

        page_update_rows.append(
            tuple(page.get(column, 0) for column in PAGE_UPDATE_COLUMNS)
            + (int(page["page_id"]),)
        )

    raw_duplicate_titles = sum(1 for value in raw_title_counts.values() if value > 1)
    raw_duplicate_descriptions = sum(1 for value in raw_desc_counts.values() if value > 1)
    return (
        page_update_rows,
        page_snapshots,
        page_diffs,
        changed_pages,
        raw_duplicate_titles,
        raw_duplicate_descriptions,
    )


def _provider_message_url(message: str) -> str:
    text = str(message or "")
    for prefix in ("failed_http:", "no_data:", "skipped_missing_key:", "retry_info:"):
        if text.startswith(prefix):
            payload = text[len(prefix) :].strip()
            return payload.split(" ", 1)[0].strip()
    if text.startswith("http://") or text.startswith("https://"):
        return text.split(" ", 1)[0].strip()
    marker = text.find("https://")
    if marker < 0:
        marker = text.find("http://")
    if marker >= 0:
        return text[marker:].split(" ", 1)[0].strip()
    return ""


def _provider_error_family(message: str) -> str:
    text = str(message or "").lower()
    if "runtimeerror" in text or "worker failure" in text or "exception" in text:
        return "runtime"
    if "timeout" in text or "timed out" in text:
        return "timeout"
    if "429" in text or "quota" in text or "rate limit" in text:
        return "quota"
    if "no_data" in text or "missing performance category score" in text or "no crux record" in text:
        return "no_fcp"
    if "missing key" in text:
        return "not_attempted_policy"
    return "http"


def _derive_measurement_status_by_url(
    pages: list[dict],
    psi_rows: list[Any],
    psi_messages: list[str],
    crux_rows: list[Any],
    crux_errors: list[str],
) -> dict[str, tuple[str, str]]:
    measurement_by_url: dict[str, tuple[str, str]] = {
        str(page.get("normalized_url") or ""): ("measurement_not_attempted_by_policy", "not_attempted_policy")
        for page in pages
        if str(page.get("normalized_url") or "")
    }

    def set_measurement(url: str, status: str, family: str) -> None:
        key = str(url or "").strip()
        if not key:
            return
        current_status, _current_family = measurement_by_url.get(
            key,
            ("measurement_not_attempted_by_policy", "not_attempted_policy"),
        )
        if MEASUREMENT_STATUS_RANK.get(status, 0) >= MEASUREMENT_STATUS_RANK.get(current_status, 0):
            measurement_by_url[key] = (status, family)

    for row in psi_rows:
        set_measurement(str(getattr(row, "url", "") or ""), "ok", "none")

    for row in crux_rows:
        row_status = str(getattr(row, "status", "") or "").strip().lower()
        row_url = str(getattr(row, "url", "") or "")
        if row_status == "success":
            set_measurement(row_url, "ok", "none")
        elif row_status == "no_data":
            set_measurement(row_url, "provider_unavailable_data", "no_fcp")
        elif row_status == "failed_http":
            family = _provider_error_family(str(getattr(row, "error_message", "") or ""))
            status = "provider_runtime_error" if family == "runtime" else "provider_error"
            set_measurement(row_url, status, family)
        elif row_status == "skipped_missing_key":
            set_measurement(row_url, "measurement_not_attempted_by_policy", "not_attempted_policy")

    for message in psi_messages:
        msg = str(message or "")
        url = _provider_message_url(msg)
        if msg.startswith("no_data:"):
            family = _provider_error_family(msg)
            status = "provider_runtime_error" if family == "runtime" else "provider_unavailable_data"
            family = "runtime" if status == "provider_runtime_error" else "no_fcp"
            set_measurement(url, status, family)
        elif msg.startswith("failed_http:"):
            family = _provider_error_family(msg)
            status = "provider_runtime_error" if family == "runtime" else "provider_error"
            set_measurement(url, status, family)
        elif msg.startswith("skipped_missing_key:"):
            set_measurement(url, "measurement_not_attempted_by_policy", "not_attempted_policy")

    for message in crux_errors:
        msg = str(message or "")
        if msg.startswith("retry_info:"):
            continue
        family = _provider_error_family(msg)
        status = "provider_runtime_error" if family == "runtime" else "provider_error"
        set_measurement(_provider_message_url(msg), status, family)

    return measurement_by_url


def _prepare_measurement_records(
    *,
    run_id: str,
    pages: list[dict],
    measurement_by_url: dict[str, tuple[str, str]],
) -> tuple[list[tuple[str, str, int]], list[SchemaValidationRecord], list[MediaAssetRecord]]:
    measurement_update_rows: list[tuple[str, str, int]] = []
    schema_validation_rows: list[SchemaValidationRecord] = []
    media_asset_rows: list[MediaAssetRecord] = []

    for page in pages:
        normalized_url = str(page.get("normalized_url") or "")
        if not normalized_url:
            continue
        status_value, family_value = measurement_by_url.get(
            normalized_url,
            ("measurement_not_attempted_by_policy", "not_attempted_policy"),
        )
        page["measurement_status"] = status_value
        page["measurement_error_family"] = family_value
        measurement_update_rows.append((status_value, family_value, int(page.get("page_id") or 0)))

        schema_validation_rows.append(
            SchemaValidationRecord(
                run_id=run_id,
                url=normalized_url,
                validation_score=int(page.get("schema_validation_score") or 0),
                findings_json=str(page.get("schema_validation_json") or "{}"),
                raw_render_diff_json=json.dumps(
                    _json_object(page.get("schema_validation_json") or "{}").get("render_diff", {}),
                    sort_keys=True,
                ),
            )
        )

        image_score = int(page.get("image_discoverability_score") or 0)
        for asset in _json_dict_list(page.get("image_details_json") or "[]")[:200]:
            asset_url = str(asset.get("normalized_src") or asset.get("src") or "").strip()
            if not asset_url:
                continue
            media_asset_rows.append(
                MediaAssetRecord(
                    run_id=run_id,
                    url=normalized_url,
                    asset_type="image",
                    asset_url=asset_url,
                    discoverability_score=image_score,
                    metadata_json=json.dumps(asset, sort_keys=True),
                )
            )

        video_score = int(page.get("video_discoverability_score") or 0)
        for asset in _json_dict_list(page.get("video_details_json") or "[]")[:200]:
            asset_url = str(
                asset.get("src")
                or asset.get("embed_url")
                or asset.get("content_url")
                or asset.get("thumbnail_url")
                or ""
            ).strip()
            if not asset_url:
                continue
            media_asset_rows.append(
                MediaAssetRecord(
                    run_id=run_id,
                    url=normalized_url,
                    asset_type="video",
                    asset_url=asset_url,
                    discoverability_score=video_score,
                    metadata_json=json.dumps(asset, sort_keys=True),
                )
            )

    return measurement_update_rows, schema_validation_rows, media_asset_rows


def run_audit(args: argparse.Namespace) -> None:
    _validate_robots_bypass_flags(args)
    configure_logging(args.verbose)
    run_profile, profile_values = _resolve_run_profile_values(args)
    output_dir = _resolve_output_dir(args)
    site_type = str(getattr(args, "site_type", "general") or "general").strip().lower()
    if site_type not in {"general", "local"}:
        site_type = "general"
    scoring_profile = str(getattr(args, "scoring_profile", "") or "").strip().lower()
    if not scoring_profile:
        scoring_profile = site_type
    crawl_discovery_mode = str(profile_values["crawl_discovery_mode"] or "raw").strip().lower()
    if crawl_discovery_mode not in {"raw", "hybrid", "browser_first"}:
        crawl_discovery_mode = "raw"
    scope_mode = str(profile_values["scope_mode"] or "apex_www").strip().lower()
    if scope_mode not in {"host_only", "apex_www", "all_subdomains", "custom_allowlist"}:
        scope_mode = "apex_www"
    persona_override = str(getattr(args, "user_agent", "") or "").strip()
    requested_persona = str(getattr(args, "crawl_persona", "googlebot_smartphone") or "googlebot_smartphone").strip().lower()
    resolved_persona = resolve_crawl_persona(requested_persona, user_agent_override=persona_override)
    requested_offsite_mode = str(getattr(args, "offsite_commoncrawl_mode", "ranks") or "ranks").strip().lower()
    allow_experimental_verify = bool(getattr(args, "offsite_commoncrawl_experimental_verify", False))
    offsite_mode = _normalize_offsite_mode(
        requested_offsite_mode,
        allow_experimental_verify=allow_experimental_verify,
    )
    verify_mode_forced_to_ranks = requested_offsite_mode == "verify" and offsite_mode != "verify"
    offsite_schedule = _normalize_offsite_schedule(
        getattr(args, "offsite_commoncrawl_schedule", OFFSITE_COMMONCRAWL_DEFAULT_SCHEDULE)
    )

    config = AuditConfig(
        domain=normalize_url(args.domain),
        output_dir=output_dir,
        run_profile=run_profile,
        site_type=site_type,
        scoring_profile=scoring_profile,
        crawl_persona=resolved_persona.id,
        crawl_discovery_mode=crawl_discovery_mode,
        scope_mode=scope_mode,
        scope_allowlist=_parse_scope_allowlist(getattr(args, "scope_allowlist", "")),
        max_pages=max(1, int(profile_values["max_pages"])),
        crawl_frontier_enabled=bool(getattr(args, "crawl_frontier_enabled", True)),
        crawl_frontier_cluster_budget=max(1, int(getattr(args, "crawl_frontier_cluster_budget", 3))),
        crawl_workers=max(1, int(getattr(args, "crawl_workers", 1))),
        crawl_queue_high_weight=max(1, int(getattr(args, "crawl_queue_high_weight", 3))),
        crawl_queue_normal_weight=max(1, int(getattr(args, "crawl_queue_normal_weight", 2))),
        per_host_rate_limit_rps=max(0.1, float(getattr(args, "per_host_rate_limit_rps", 4.0))),
        per_host_burst_capacity=max(1, int(getattr(args, "per_host_burst_capacity", 4))),
        incremental_crawl_enabled=bool(getattr(args, "incremental_crawl_enabled", False)),
        max_render_pages=max(0, int(profile_values["max_render_pages"])),
        render_mode=str(profile_values["render_mode"]),
        render_frontier_enabled=bool(getattr(args, "render_frontier_enabled", True)),
        render_interaction_budget=max(0, int(profile_values["render_interaction_budget"])),
        render_wait_ladder_ms=_parse_wait_ladder_ms(getattr(args, "render_wait_ladder_ms", None)),
        render_mobile_first=bool(getattr(args, "render_mobile_first", True)),
        render_mobile_viewport=str(getattr(args, "render_mobile_viewport", "390x844x2,mobile,touch") or "390x844x2,mobile,touch"),
        render_desktop_viewport=str(getattr(args, "render_desktop_viewport", "1440x900x1") or "1440x900x1"),
        faceted_sample_rate=max(0.0, min(1.0, float(profile_values["faceted_sample_rate"]))),
        timeout=args.timeout,
        user_agent=resolved_persona.request_user_agent,
        user_agent_override=persona_override,
        robots_user_agent_token=resolved_persona.robots_token,
        meta_robot_scope=resolved_persona.meta_robot_scope,
        robots_persona_mode=str(getattr(args, "robots_persona_mode", resolved_persona.robots_mode) or resolved_persona.robots_mode).strip().lower(),
        google_exact_apply_crawl_delay=bool(getattr(args, "google_exact_apply_crawl_delay", False)),
        respect_robots=not args.ignore_robots,
        save_html=args.save_html,
        verbose=args.verbose,
        retries=max(0, int(getattr(args, "crawl_retries", 1))),
        crawl_base_backoff_seconds=max(0.0, float(getattr(args, "crawl_base_backoff_seconds", 0.25))),
        crawl_max_backoff_seconds=max(0.0, float(getattr(args, "crawl_max_backoff_seconds", 4.0))),
        crawl_max_total_wait_seconds=max(0.0, float(getattr(args, "crawl_max_total_wait_seconds", 12.0))),
        crawl_respect_retry_after=bool(getattr(args, "crawl_respect_retry_after", True)),
        max_response_bytes=max(1024, int(getattr(args, "max_response_bytes", 2_000_000))),
        max_non_html_bytes=max(1024, int(getattr(args, "max_non_html_bytes", 262_144))),
        psi_enabled=bool(getattr(args, "psi_enabled", True)),
        crux_enabled=bool(getattr(args, "crux_enabled", True)),
        performance_targets=max(1, int(profile_values["performance_targets"])),
        crux_origin_fallback=bool(getattr(args, "crux_origin_fallback", True)),
        store_provider_payloads=bool(getattr(args, "store_provider_payloads", False)),
        payload_retention_days=max(0, int(getattr(args, "payload_retention_days", 30))),
        provider_max_retries=max(0, int(profile_values["provider_max_retries"])),
        provider_base_backoff_seconds=max(0.0, float(profile_values["provider_base_backoff_seconds"])),
        provider_max_backoff_seconds=max(0.0, float(profile_values["provider_max_backoff_seconds"])),
        provider_respect_retry_after=bool(getattr(args, "provider_respect_retry_after", True)),
        provider_max_total_wait_seconds=max(0.0, float(profile_values["provider_max_total_wait_seconds"])),
        psi_workers=max(1, int(getattr(args, "psi_workers", 4))),
        provider_rate_limit_rps=max(0.1, float(getattr(args, "provider_rate_limit_rps", 4.0))),
        provider_rate_limit_capacity=max(1, int(getattr(args, "provider_rate_limit_capacity", 4))),
        lighthouse_enabled=bool(getattr(args, "lighthouse_enabled", False)),
        lighthouse_targets=max(1, int(getattr(args, "lighthouse_targets", 3))),
        lighthouse_timeout_seconds=max(10.0, float(getattr(args, "lighthouse_timeout_seconds", 90.0))),
        lighthouse_form_factor=str(getattr(args, "lighthouse_form_factor", "desktop") or "desktop").strip().lower(),
        lighthouse_config_path=str(getattr(args, "lighthouse_config_path", "") or "").strip(),
        lighthouse_budget_performance_min=max(0, min(100, int(getattr(args, "lighthouse_budget_performance_min", 70)))),
        lighthouse_budget_seo_min=max(0, min(100, int(getattr(args, "lighthouse_budget_seo_min", 70)))),
        crawl_heartbeat_every_pages=max(0, int(profile_values["crawl_heartbeat_every_pages"])),
        gsc_enabled=bool(getattr(args, "gsc_enabled", False)),
        gsc_property=str(getattr(args, "gsc_property", "") or "").strip(),
        gsc_credentials_json=str(getattr(args, "gsc_credentials_json", "") or "").strip(),
        gsc_url_limit=max(1, int(getattr(args, "gsc_url_limit", 200))),
        gsc_analytics_enabled=bool(getattr(args, "gsc_analytics_enabled", False)),
        gsc_analytics_days=max(1, int(getattr(args, "gsc_analytics_days", 28))),
        gsc_analytics_row_limit=max(1, int(getattr(args, "gsc_analytics_row_limit", 5000))),
        gsc_analytics_dimensions=_parse_csv_tokens(
            getattr(args, "gsc_analytics_dimensions", ""),
            default=("page", "query", "device", "country", "date"),
        ),
        offsite_commoncrawl_enabled=bool(getattr(args, "offsite_commoncrawl_enabled", False)),
        offsite_commoncrawl_mode=offsite_mode,
        offsite_commoncrawl_schedule=offsite_schedule,
        offsite_commoncrawl_release=str(getattr(args, "offsite_commoncrawl_release", "auto") or "auto").strip(),
        offsite_commoncrawl_cache_dir=str(
            getattr(args, "offsite_commoncrawl_cache_dir", "~/.cache/seo_audit/commoncrawl")
            or "~/.cache/seo_audit/commoncrawl"
        ).strip(),
        offsite_commoncrawl_max_linking_domains=max(
            1,
            int(getattr(args, "offsite_commoncrawl_max_linking_domains", 100)),
        ),
        offsite_commoncrawl_join_budget_seconds=max(
            0.0,
            float(getattr(args, "offsite_commoncrawl_join_budget_seconds", 0.5)),
        ),
        offsite_commoncrawl_time_budget_seconds=max(
            1,
            int(getattr(args, "offsite_commoncrawl_time_budget_seconds", 180)),
        ),
        offsite_commoncrawl_allow_cold_edge_download=bool(
            getattr(args, "offsite_commoncrawl_allow_cold_edge_download", False)
        ),
        offsite_compare_domains=_parse_offsite_compare_domains(
            getattr(args, "offsite_compare_domains", ())
        ),
        platform_detection_enabled=bool(getattr(args, "platform_detection_enabled", True)),
        citation_measurement_enabled=bool(getattr(args, "citation_measurement_enabled", True)),
        url_policy_enabled=bool(getattr(args, "url_policy_enabled", True)),
        faceted_query_param_threshold=max(1, int(getattr(args, "faceted_query_param_threshold", 2))),
        faceted_param_keys=_parse_csv_tokens(
            getattr(args, "faceted_param_keys", ""),
            default=DEFAULT_FACETED_PARAM_KEYS,
        ),
        action_param_keys=_parse_csv_tokens(
            getattr(args, "action_param_keys", ""),
            default=DEFAULT_ACTION_PARAM_KEYS,
        ),
        diagnostic_param_keys=_parse_csv_tokens(
            getattr(args, "diagnostic_param_keys", ""),
            default=DEFAULT_DIAGNOSTIC_PARAM_KEYS,
        ),
        headers_only_param_keys=_parse_csv_tokens(
            getattr(args, "headers_only_param_keys", ""),
            default=DEFAULT_HEADERS_ONLY_PARAM_KEYS,
        ),
        canonical_candidate_param_keys=_parse_csv_tokens(
            getattr(args, "canonical_candidate_param_keys", ""),
            default=DEFAULT_CANONICAL_CANDIDATE_PARAM_KEYS,
        ),
        extractor_version=CURRENT_EXTRACTOR_VERSION,
        schema_rule_version=CURRENT_SCHEMA_RULE_VERSION,
        scoring_version=CURRENT_SCORE_VERSION,
    )
    config.output_dir.mkdir(parents=True, exist_ok=True)
    storage = Storage(config.output_dir / "audit.sqlite")
    storage.init_db()
    run_id = str(uuid4())
    storage.insert_run(run_id, datetime.now(timezone.utc).isoformat(), config.domain, config.to_json_dict(), "running")

    notes: list[str] = []
    stage_timings: list[tuple[str, int]] = []
    run_events: list[dict] = []
    events_flushed_index = 0
    status = "completed"
    provider_retry = ProviderRetryConfig(
        max_retries=config.provider_max_retries,
        base_backoff_seconds=config.provider_base_backoff_seconds,
        max_backoff_seconds=config.provider_max_backoff_seconds,
        respect_retry_after=config.provider_respect_retry_after,
        max_total_wait_seconds=config.provider_max_total_wait_seconds,
    )
    offsite_executor: ThreadPoolExecutor | None = None
    offsite_future: Future[OffsiteCommonCrawlWorkerPayload] | None = None
    offsite_control: OffsiteCommonCrawlWorkerControl | None = None
    offsite_launch_release = str(config.offsite_commoncrawl_release or "auto")
    offsite_cache_state = "cold"
    offsite_lane_started_at = ""
    offsite_target_domain = normalize_domain_for_commoncrawl(config.domain)
    offsite_start_error = ""
    try:
        def add_event(
            event_type: str,
            stage: str,
            message: str = "",
            elapsed_ms: int = 0,
            detail: dict | None = None,
        ) -> None:
            run_events.append(
                {
                    "event_time": datetime.now(timezone.utc).isoformat(),
                    "event_type": event_type,
                    "stage": stage,
                    "message": message,
                    "elapsed_ms": elapsed_ms,
                    "detail_json": json.dumps(detail or {}, sort_keys=True),
                }
            )

        def close_stage(stage: str, started_at: float, detail: dict | None = None) -> int:
            elapsed_ms = int((time.perf_counter() - started_at) * 1000)
            stage_timings.append((stage, elapsed_ms))
            add_event("stage_timing", stage, elapsed_ms=elapsed_ms, detail=detail)
            flush_events()
            return elapsed_ms

        def flush_events() -> None:
            nonlocal events_flushed_index
            if events_flushed_index >= len(run_events):
                return
            pending = run_events[events_flushed_index:]
            storage.insert_run_events(run_id, pending)
            events_flushed_index = len(run_events)

        if config.offsite_commoncrawl_enabled:
            if verify_mode_forced_to_ranks:
                notes.append(
                    "offsite commoncrawl: verify mode requested but experimental verify flag was not set; using mode=ranks"
                )
            offsite_lane_started_at = datetime.now(timezone.utc).isoformat()
            try:
                launch = inspect_commoncrawl_launch(
                    config.offsite_commoncrawl_release,
                    config.offsite_commoncrawl_cache_dir,
                    timeout_seconds=5.0,
                )
                offsite_launch_release = launch.release
                offsite_cache_state = launch.cache_state
                add_event(
                    "offsite_commoncrawl_cache",
                    "offsite_commoncrawl",
                    detail={
                        "release": launch.release,
                        "cache_state": launch.cache_state,
                        "vertices_ready": int(launch.manifest.vertices_ready),
                        "ranks_ready": int(launch.manifest.ranks_ready),
                        "edges_ready": int(launch.manifest.edges_ready),
                    },
                )

                offsite_control = OffsiteCommonCrawlWorkerControl()
                offsite_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="offsite-commoncrawl")
                offsite_future = offsite_executor.submit(
                    run_offsite_commoncrawl_worker,
                    OffsiteCommonCrawlWorkerRequest(
                        target_domain=offsite_target_domain,
                        mode=config.offsite_commoncrawl_mode,
                        schedule=config.offsite_commoncrawl_schedule,
                        release=launch.release,
                        cache_dir=launch.cache_dir,
                        max_linking_domains=config.offsite_commoncrawl_max_linking_domains,
                        time_budget_seconds=config.offsite_commoncrawl_time_budget_seconds,
                        allow_cold_edge_download=config.offsite_commoncrawl_allow_cold_edge_download,
                        compare_domains=config.offsite_compare_domains,
                    ),
                    offsite_control,
                )
                add_event(
                    "offsite_commoncrawl_status",
                    "offsite_commoncrawl",
                    detail={
                        "status": "started",
                        "mode": config.offsite_commoncrawl_mode,
                        "schedule": config.offsite_commoncrawl_schedule,
                        "release": launch.release,
                    },
                )
                notes.append(
                    "offsite commoncrawl: "
                    f"started mode={config.offsite_commoncrawl_mode} "
                    f"schedule={config.offsite_commoncrawl_schedule} "
                    f"release={launch.release} "
                    f"cache_state={launch.cache_state}"
                )
            except Exception as exc:
                offsite_start_error = str(exc)
                add_event(
                    "offsite_commoncrawl_status",
                    "offsite_commoncrawl",
                    detail={
                        "status": STATUS_FAILED_QUERY,
                        "mode": config.offsite_commoncrawl_mode,
                        "schedule": config.offsite_commoncrawl_schedule,
                        "release": offsite_launch_release,
                        "error": offsite_start_error,
                    },
                )
                notes.append(f"offsite commoncrawl: startup failed: {offsite_start_error}")
        else:
            add_event(
                "offsite_commoncrawl_status",
                "offsite_commoncrawl",
                detail={
                    "status": STATUS_SKIPPED_DISABLED,
                    "mode": config.offsite_commoncrawl_mode,
                    "schedule": config.offsite_commoncrawl_schedule,
                },
            )
            notes.append("offsite commoncrawl: disabled")

        notes.append(f"run profile: {run_profile}")
        notes.append(f"site type: {config.site_type}")
        notes.append(f"scoring profile: {config.scoring_profile}")
        notes.append(
            "crawl persona: "
            f"id={config.crawl_persona} "
            f"robots_token={config.robots_user_agent_token} "
            f"meta_scope={config.meta_robot_scope} "
            f"robots_mode={config.robots_persona_mode}"
        )
        notes.append(
            "crawl retries: "
            f"retries={config.retries} "
            f"base_backoff={config.crawl_base_backoff_seconds:.2f}s "
            f"max_backoff={config.crawl_max_backoff_seconds:.2f}s "
            f"max_total_wait={config.crawl_max_total_wait_seconds:.2f}s "
            f"respect_retry_after={int(config.crawl_respect_retry_after)}"
        )
        if config.user_agent_override:
            notes.append("crawl user-agent override: custom header in use")
        elif config.user_agent:
            notes.append("crawl user-agent: persona default")
        scoped_hosts = internal_hosts_for_site(
            config.domain,
            scope_mode=config.scope_mode,
            custom_allowlist=config.scope_allowlist,
        )
        notes.append(
            "internal host policy: strict "
            f"mode={config.scope_mode} hosts="
            + ", ".join(sorted(scoped_hosts))
        )
        if config.scope_allowlist:
            notes.append("internal host allowlist: " + ", ".join(config.scope_allowlist))
        notes.append(
            "crawl discovery policy: "
            f"mode={config.crawl_discovery_mode} render_frontier={int(config.render_frontier_enabled)}"
        )
        notes.append(
            "crawl workers: "
            f"workers={config.crawl_workers} "
            f"queue_weights=high:{config.crawl_queue_high_weight},normal:{config.crawl_queue_normal_weight},low:1"
        )
        if config.output_dir != Path(args.output):
            notes.append(f"fresh output dir: {config.output_dir}")
        purged_perf, purged_crux = storage.purge_provider_payloads_older_than(config.payload_retention_days)
        if purged_perf or purged_crux:
            notes.append(
                f"provider payload retention purge: performance={purged_perf} crux={purged_crux} cutoff_days={config.payload_retention_days}"
            )
        if not config.respect_robots:
            notes.append("robots bypass acknowledged: --ignore-robots with explicit acknowledgement")

        previous_run_config = storage.latest_run_config(exclude_run_id=run_id)
        invalidation_flags = _version_invalidation_flags(config, previous_run_config)
        notes.append(
            "runtime versions: "
            f"extractor={config.extractor_version} "
            f"schema_rules={config.schema_rule_version} "
            f"scoring={config.scoring_version}"
        )
        notes.append(
            "cache invalidation: "
            f"extractor_version_changed={invalidation_flags['extractor_version_changed']} "
            f"schema_rule_version_changed={invalidation_flags['schema_rule_version_changed']} "
            f"scoring_version_changed={invalidation_flags['scoring_version_changed']}"
        )

        stage_started = time.perf_counter()
        print(f"[1/10] robots: {config.domain}")
        robots_data: RobotsData | None = None
        try:
            robots_data = fetch_robots_with_persona(
                config.domain,
                config.timeout,
                config.user_agent,
                persona_mode=config.robots_persona_mode,
            )
            robots_summary = robots_fetch_summary(robots_data)
            notes.append(
                "robots fetch: "
                f"state={robots_summary.get('state', 'unknown')} "
                f"bucket={robots_summary.get('status_bucket', 'unknown')} "
                f"status={int(robots_summary.get('http_status', 0) or 0)} "
                f"hops={int(robots_summary.get('redirect_hops', 0) or 0)}"
            )
            if robots_data and robots_data.rules:
                storage.insert_robots_rules(run_id, robots_data.robots_url, robots_data.rules)
            if config.respect_robots:
                crawl_delay = resolve_crawl_delay(
                    robots_data,
                    config.robots_user_agent_token,
                    persona_mode=config.robots_persona_mode,
                    apply_for_google_exact=config.google_exact_apply_crawl_delay,
                )
                if crawl_delay is not None:
                    effective_delay = max(config.request_delay, crawl_delay)
                    if crawl_delay > config.request_delay:
                        notes.append(
                            f"robots crawl-delay applied: robots={crawl_delay:.2f}s effective={effective_delay:.2f}s"
                        )
                    else:
                        notes.append(
                            f"robots crawl-delay observed: robots={crawl_delay:.2f}s effective remains base={effective_delay:.2f}s"
                        )
            else:
                notes.append("robots rules fetched in diagnostic mode while bypassing enforcement")
        except Exception as exc:
            notes.append(f"robots error: {exc}")
        close_stage(
            "robots",
            stage_started,
            {
                "robots_respected": int(config.respect_robots),
                "robots_loaded": int(robots_data is not None),
                "robots_rules": len((robots_data.rules if robots_data else []) or []),
                "robots_state": str((robots_data.fetch_state if robots_data else "missing") or "missing"),
            },
        )

        stage_started = time.perf_counter()
        print("[2/10] sitemaps")
        if robots_data and robots_data.sitemaps:
            sitemap_urls = robots_data.sitemaps
        else:
            sitemap_urls = default_sitemap_candidates(config.domain)
        sitemap_warnings: list[str] = []
        sitemap_entries = fetch_and_parse_sitemaps(
            sitemap_urls,
            config.timeout,
            config.user_agent,
            errors=sitemap_warnings,
        )
        if sitemap_entries:
            storage.insert_sitemap_entries(run_id, sitemap_entries)
        if sitemap_warnings:
            notes.append("sitemap warnings: " + " | ".join(sitemap_warnings[:5]))
        close_stage(
            "sitemaps",
            stage_started,
            {
                "sitemap_entries": len(sitemap_entries),
                "sitemap_sources": len(sitemap_urls),
                "sitemap_warnings": len(sitemap_warnings),
            },
        )

        stage_started = time.perf_counter()
        print("[3/10] sitemap status")
        sitemap_status_detail = {
            "status": "skipped_disabled",
            "property_uri": "",
            "rows": [],
        }
        if config.gsc_enabled:
            gsc_property_for_sitemaps = resolve_property(config.domain, config.gsc_property)
            sitemap_status_detail = collect_optional_gsc_sitemap_status(
                property_uri=gsc_property_for_sitemaps,
                credentials_json=config.gsc_credentials_json,
                known_sitemaps=[str(entry.get("sitemap_url") or "") for entry in sitemap_entries],
            )
            sitemap_status_detail["known_sitemaps"] = len({
                str(entry.get("sitemap_url") or "").strip()
                for entry in sitemap_entries
                if str(entry.get("sitemap_url") or "").strip()
            })
        add_event("provider_summary", "sitemap_status", detail=sitemap_status_detail)
        notes.append(
            "sitemap status: "
            f"status={sitemap_status_detail.get('status', 'unknown')} "
            f"rows={len(sitemap_status_detail.get('rows') or [])}"
        )
        close_stage("sitemap_status", stage_started, sitemap_status_detail)

        stage_started = time.perf_counter()
        print("[4/10] crawl planning")
        base_start_urls = seed_urls(
            config.domain,
            robots_data,
            sitemap_entries,
            scope_mode=config.scope_mode,
            custom_allowlist=config.scope_allowlist,
        )
        known_sitemap_urls = storage.list_known_sitemap_urls(limit=max(200, int(config.max_pages) * 6))
        recently_changed_urls = storage.list_recent_changed_urls(limit=max(200, int(config.max_pages) * 6))
        start_urls, planner_detail = _plan_crawl_start_urls(
            storage,
            config,
            base_start_urls,
            known_sitemap_urls=known_sitemap_urls,
            recently_changed_urls=recently_changed_urls,
        )
        planner_detail["extractor_version_changed"] = int(invalidation_flags["extractor_version_changed"])
        planner_detail["schema_rule_version_changed"] = int(invalidation_flags["schema_rule_version_changed"])
        planner_detail["scoring_version_changed"] = int(invalidation_flags["scoring_version_changed"])
        add_event("crawl_plan_summary", "plan_crawl", detail=planner_detail)
        notes.append(
            "crawl planner: "
            f"discovered={planner_detail.get('planner_discovered', 0)} "
            f"seed_urls={planner_detail.get('planner_seed_urls', 0)} "
            f"known_sitemap_urls={planner_detail.get('planner_known_sitemap_urls', 0)} "
            f"prioritized_changed_urls={planner_detail.get('planner_prioritized_changed_urls', 0)}"
        )
        close_stage("plan_crawl", stage_started, planner_detail)

        stage_started = time.perf_counter()
        print("[5/10] crawl")

        heartbeat_rows: list[dict] = []

        def on_crawl_heartbeat(payload: dict) -> None:
            heartbeat_rows.append(payload)

        crawled = crawl_site(
            config,
            run_id,
            robots_data,
            start_urls,
            on_heartbeat=on_crawl_heartbeat,
            heartbeat_every_pages=config.crawl_heartbeat_every_pages,
            storage=storage,
            extractor_version=config.extractor_version,
            schema_rule_version=config.schema_rule_version,
            scoring_version=config.scoring_version,
        )
        storage.insert_pages(crawled.pages)
        storage.insert_links(crawled.links)
        storage.insert_crawl_fetches(crawled.fetches)
        crawl_policy_counts: dict[str, int] = {}
        for page in crawled.pages:
            key = str(page.crawl_policy_class or "crawl_normally")
            crawl_policy_counts[key] = crawl_policy_counts.get(key, 0) + 1
        if crawl_policy_counts:
            ordered = ", ".join(f"{k}={v}" for k, v in sorted(crawl_policy_counts.items()))
            notes.append(f"crawl policy coverage: {ordered}")
        if crawled.discovery_stats:
            add_event(
                "crawl_discovery_summary",
                "crawl",
                detail={k: int(v) for k, v in sorted(crawled.discovery_stats.items())},
            )
            notes.append(
                "crawl discovery telemetry: "
                + ", ".join(
                    f"{k}={v}"
                    for k, v in sorted(crawled.discovery_stats.items())
                    if k
                    in {
                        "enqueued_total",
                        "enqueued_via_raw_link",
                        "enqueued_via_render_link",
                        "dedupe_skipped",
                        "scope_skipped",
                        "render_frontier_checks",
                        "render_frontier_successes",
                        "render_frontier_failures",
                    }
                )
            )
        for hb in heartbeat_rows:
            add_event(
                "crawl_heartbeat",
                "crawl",
                message=(
                    f"pages={hb.get('pages_stored', 0)} "
                    f"queue={hb.get('queue_size', 0)} "
                    f"errors={hb.get('error_count', 0)}"
                ),
                elapsed_ms=int(hb.get("crawl_elapsed_ms", 0) or 0),
                detail=hb,
            )
        crawl_incremental = {
            "discovered": int(crawled.incremental_stats.get("discovered", len(crawled.discovered_urls))),
            "fetched": int(crawled.incremental_stats.get("fetched", 0)),
            "reused_from_cache": int(crawled.incremental_stats.get("reused_from_cache", 0)),
            "not_modified": int(crawled.incremental_stats.get("not_modified", 0)),
            "reparsed": int(crawled.incremental_stats.get("reparsed", 0)),
            "rerendered": int(crawled.incremental_stats.get("rerendered", 0)),
        }
        add_event("crawl_incremental_summary", "crawl", detail=crawl_incremental)
        notes.append(
            "crawl incremental: "
            f"discovered={crawl_incremental['discovered']} "
            f"fetched={crawl_incremental['fetched']} "
            f"reused_from_cache={crawl_incremental['reused_from_cache']} "
            f"not_modified={crawl_incremental['not_modified']} "
            f"reparsed={crawl_incremental['reparsed']} "
            f"rerendered={crawl_incremental['rerendered']}"
        )
        close_stage(
            "crawl",
            stage_started,
            {
                "start_urls": len(start_urls),
                "pages": len(crawled.pages),
                "links": len(crawled.links),
                "fetches": len(crawled.fetches),
                "heartbeat_events": len(heartbeat_rows),
                "crawl_policy_counts": crawl_policy_counts,
                "discovery_stats": crawled.discovery_stats,
                "incremental_stats": crawl_incremental,
            },
        )

        pages = [dict(r) for r in storage.query("SELECT * FROM pages WHERE run_id = ?", (run_id,))]
        links = [dict(r) for r in storage.query("SELECT * FROM links WHERE run_id = ?", (run_id,))]
        sitemap_url_set = {
            normalize_url(str(entry.get("url") or ""), base_url=config.domain)
            for entry in sitemap_entries
            if str(entry.get("url") or "").strip()
        }

        stage_started = time.perf_counter()
        print("[6/10] classify")
        raw_title_counts: dict[str, int] = {}
        raw_desc_counts: dict[str, int] = {}
        governance_matrices: list[dict[str, object]] = []
        for p in pages:
            classification = classify_page_result(
                p["normalized_url"],
                p.get("title", ""),
                p.get("h1", ""),
                schema_types=p.get("schema_types_json") or "[]",
            )
            p["page_type"] = classification.page_type
            p["classification_confidence"] = int(classification.confidence)
            p["classification_evidence_json"] = json.dumps(classification.evidence)
            normalized_url = normalize_url(str(p.get("normalized_url") or ""), base_url=config.domain)
            final_url = normalize_url(str(p.get("final_url") or p.get("normalized_url") or ""), base_url=config.domain)
            p["in_sitemap_flag"] = int(normalized_url in sitemap_url_set or final_url in sitemap_url_set)
            text_blob = f"{p.get('title', '')} {p.get('h1', '')}".lower()
            p["has_contact_signal"] = "contact" in text_blob or p["page_type"] == "contact"
            p["has_local_schema"] = has_local_business_schema(p.get("schema_types_json") or "[]")
            p["has_location_signal"] = (
                any(x in text_blob for x in ["city", "state", "near", "location", "address"]) 
                or p["page_type"] == "location"
                or bool(p["has_local_schema"])
            )
            p["has_map"] = "map" in text_blob

            governance_url = str(p.get("final_url") or p.get("normalized_url") or "")
            if robots_data and robots_data.parser and governance_url:
                p["governance_googlebot_allowed"] = int(is_allowed(robots_data, "Googlebot", governance_url))
                p["governance_bingbot_allowed"] = int(is_allowed(robots_data, "Bingbot", governance_url))
                p["governance_openai_allowed"] = int(is_allowed(robots_data, "OAI-SearchBot", governance_url))
                p["governance_google_extended_allowed"] = int(is_allowed(robots_data, "Google-Extended", governance_url))
                p["governance_gptbot_allowed"] = int(is_allowed(robots_data, "GPTBot", governance_url))
                p["governance_oai_adsbot_allowed"] = int(is_allowed(robots_data, "OAI-AdsBot", governance_url))
                p["governance_chatgpt_user_allowed"] = int(is_allowed(robots_data, "ChatGPT-User", governance_url))
            else:
                p["governance_googlebot_allowed"] = 1
                p["governance_bingbot_allowed"] = 1
                p["governance_openai_allowed"] = 1
                p["governance_google_extended_allowed"] = 1
                p["governance_gptbot_allowed"] = 1
                p["governance_oai_adsbot_allowed"] = 1
                p["governance_chatgpt_user_allowed"] = 1

            governance_matrix = build_governance_matrix(p)
            governance_matrices.append(governance_matrix)
            p["governance_matrix_json"] = json.dumps(governance_matrix, sort_keys=True)

            if bool(config.citation_measurement_enabled):
                citation_assessment = compute_citation_eligibility(p, governance_matrix)
                citation_evidence = build_citation_evidence(p)
                citation_evidence["eligibility_reasons"] = citation_assessment.reasons
                citation_evidence["governance_blocked_bots"] = [
                    bot
                    for bot in ("googlebot", "bingbot", "oai_searchbot")
                    if not bool((governance_matrix.get(bot) or {}).get("crawl_allowed", True))
                ]
                visibility_payload = build_ai_visibility_payload(
                    potential_score=int(citation_assessment.eligibility_score),
                    potential_reasons=list(citation_assessment.reasons),
                    observed_evidence=citation_evidence,
                )
                p["ai_discoverability_potential_score"] = int(citation_assessment.eligibility_score)
                p["ai_visibility_json"] = json.dumps(visibility_payload, sort_keys=True)
                p["citation_eligibility_score"] = int(citation_assessment.eligibility_score)
                p["citation_evidence_json"] = json.dumps(
                    legacy_citation_evidence_from_payload(visibility_payload),
                    sort_keys=True,
                )
            else:
                p["ai_discoverability_potential_score"] = 0
                p["ai_visibility_json"] = "{}"
                p["citation_eligibility_score"] = 0
                p["citation_evidence_json"] = "{}"

            raw_title = str(p.get("raw_title") or p.get("title") or "").strip()
            raw_meta_description = str(p.get("raw_meta_description") or p.get("meta_description") or "").strip()
            if raw_title:
                raw_title_counts[raw_title] = raw_title_counts.get(raw_title, 0) + 1
            if raw_meta_description:
                raw_desc_counts[raw_meta_description] = raw_desc_counts.get(raw_meta_description, 0) + 1
        governance_matrix_summary = summarize_governance_matrices(governance_matrices)
        governance_summary = _governance_summary_for_pages(pages)
        governance_summary["matrix_summary"] = governance_matrix_summary
        add_event("governance_summary", "classify", detail=governance_summary)
        notes.append(
            "governance summary: "
            f"googlebot_blocked={int(governance_summary.get('googlebot_blocked_pages', 0))} "
            f"bingbot_blocked={int(governance_summary.get('bingbot_blocked_pages', 0))} "
            f"oai_searchbot_blocked={int(governance_summary.get('oai_searchbot_blocked_pages', 0))} "
            f"google_extended_blocked={int(governance_summary.get('google_extended_blocked_pages', 0))} "
            f"gptbot_blocked={int(governance_summary.get('gptbot_blocked_pages', 0))} "
            f"oai_adsbot_blocked={int(governance_summary.get('oai_adsbot_blocked_pages', 0))}"
        )
        close_stage(
            "classify",
            stage_started,
            {
                "pages": len(pages),
                "links": len(links),
                "governance_summary": governance_summary,
            },
        )

        stage_started = time.perf_counter()
        print("[7/10] render classification+diff")
        render_candidates = [page for page in pages if _is_actionable_html_page(page)]
        shell_count = 0
        render_successes = 0
        render_failures = 0
        raw_text_total = 0
        rendered_text_total = 0
        provenance_counts = {"raw_only": 0, "rendered_only": 0, "both": 0}
        render_sessions: list[RenderSessionRecord] = []
        prefer_rendered_facts = crawl_persona_prefers_rendered(config.crawl_persona)
        for p in render_candidates:
            p["raw_title"] = p.get("raw_title") or p.get("title") or ""
            p["raw_meta_description"] = p.get("raw_meta_description") or p.get("meta_description") or ""
            p["raw_canonical"] = p.get("raw_canonical") or p.get("canonical_url") or ""
            p["raw_canonical_urls_json"] = p.get("raw_canonical_urls_json") or p.get("canonical_urls_json") or "[]"
            p["raw_hreflang_links_json"] = p.get("raw_hreflang_links_json") or p.get("hreflang_links_json") or "[]"
            p["raw_h1_count"] = int(p.get("raw_h1_count") or int(bool(p.get("h1"))))
            p["raw_text_len"] = int(p.get("raw_text_len") or p.get("word_count") or 0)
            p["raw_content_hash"] = p.get("raw_content_hash") or p.get("content_hash") or ""
            raw_text_total += p["raw_text_len"]
            p["shell_state"] = p.get("shell_state") or ("raw_shell_possible" if int(p.get("likely_js_shell") or 0) == 1 else "raw_shell_unlikely")
            if p["likely_js_shell"]:
                shell_count += 1
        heavy_rule_changed = any(int(invalidation_flags.get(key, 0)) == 1 for key in (
             "extractor_version_changed",
            "schema_rule_version_changed",
            "scoring_version_changed",
        ))
        render_source_pages = pages
        if config.incremental_crawl_enabled and not heavy_rule_changed:
            render_source_pages = [
                page
                for page in pages
                if int(page.get("changed_since_last_run") or 0) == 1
            ]
        render_targets = select_render_targets(render_source_pages, config.render_mode, config.max_render_pages)
        if config.incremental_crawl_enabled and not heavy_rule_changed:
            notes.append(
                "render planner: "
                f"changed_pages={len(render_source_pages)} "
                f"rerender_targets={len(render_targets)}"
            )

        render_failure_reasons: set[str] = set()
        with PlaywrightRenderer(
            timeout=config.timeout + 5,
            user_agent=config.user_agent,
            crawler_token=(config.meta_robot_scope if config.meta_robot_scope not in {"generic", "robots", "*"} else ""),
            wait_ladder_ms=config.render_wait_ladder_ms,
            interaction_budget=config.render_interaction_budget,
        ) as renderer:
            for p in render_targets:
                rr, render_error = renderer.render(p["normalized_url"])
                p["render_failure_family"] = ""
                p["rendered_console_errors_json"] = p.get("rendered_console_errors_json") or "[]"
                p["rendered_console_warnings_json"] = p.get("rendered_console_warnings_json") or "[]"
                if render_error:
                    render_failure_reasons.add(render_error)
                    render_failures += 1
                    p["render_error"] = render_error
                    p["render_failure_family"] = _render_failure_family(render_error)
                gap, reason = compute_render_gap(p, rr)
                p["render_checked"] = 1
                p["render_gap_score"] = gap
                p["render_gap_reason"] = reason
                p["render_reason"] = _format_render_reasons(
                    _shell_reasons_from_signals_json(str(p.get("shell_signals_json") or ""))
                )
                if rr:
                    render_successes += 1
                    p["used_render"] = 1
                    p["render_failure_family"] = ""
                    p["rendered_word_count"] = rr.word_count
                    p["rendered_text_len"] = rr.word_count
                    rendered_text_total += rr.word_count
                    p["rendered_title"] = rr.title
                    p["rendered_meta_description"] = rr.meta_description
                    p["rendered_canonical"] = rr.canonical
                    p["rendered_canonical_urls_json"] = json.dumps(rr.canonical_urls)
                    p["rendered_canonical_count"] = int(rr.canonical_count)
                    p["rendered_h1_count"] = rr.h1_count
                    p["rendered_h1s_json"] = json.dumps(rr.h1s)
                    p["rendered_links_json"] = json.dumps(rr.links)
                    p["rendered_hreflang_links_json"] = json.dumps(rr.hreflang_links)
                    p["rendered_content_hash"] = rr.content_hash
                    p["rendered_meta_robots"] = rr.meta_robots
                    p["rendered_effective_robots_json"] = rr.effective_robots_json
                    p["rendered_is_noindex"] = int(rr.is_noindex)
                    p["rendered_has_nosnippet_directive"] = int(rr.has_nosnippet_directive)
                    p["rendered_max_snippet_directive"] = rr.max_snippet_directive
                    p["rendered_max_image_preview_directive"] = rr.max_image_preview_directive
                    p["rendered_max_video_preview_directive"] = rr.max_video_preview_directive
                    p["rendered_data_nosnippet_count"] = int(rr.data_nosnippet_count)
                    p["rendered_network_requests_json"] = json.dumps(rr.network_request_urls)
                    p["rendered_api_endpoints_json"] = json.dumps(rr.api_endpoint_urls)
                    p["rendered_js_endpoints_json"] = json.dumps(rr.api_endpoint_urls)
                    p["rendered_wait_profile"] = rr.wait_profile
                    p["rendered_interaction_count"] = rr.interaction_count
                    p["rendered_action_recipe"] = rr.action_recipe
                    render_base = str(rr.final_url or p.get("final_url") or p.get("normalized_url") or "")
                    rendered_internal_links_out = _count_internal_rendered_links(
                        rr.links,
                        config.domain,
                        render_base,
                        scope_mode=config.scope_mode,
                        custom_allowlist=config.scope_allowlist,
                    )
                    p["rendered_discovery_links_out"] = rendered_internal_links_out

                    if prefer_rendered_facts:
                        p["effective_h1_count"] = rr.h1_count
                        p["effective_text_len"] = rr.word_count
                        p["effective_links_json"] = json.dumps(rr.links)
                        p["effective_internal_links_out"] = rendered_internal_links_out
                    else:
                        p["effective_h1_count"] = int(p.get("raw_h1_count") or int(bool(p.get("h1"))))
                        p["effective_text_len"] = int(p.get("raw_text_len") or p.get("word_count") or 0)
                        p["effective_links_json"] = p.get("raw_links_json") or "[]"
                        p["effective_internal_links_out"] = int(p.get("internal_links_out") or 0)

                    raw_schema_nodes = _json_dict_list(p.get("schema_graph_json") or "[]")
                    rendered_schema_nodes = _json_dict_list(rr.schema_graph_json)
                    schema_diff = compare_schema_sets(raw_schema_nodes, rendered_schema_nodes)
                    validation_payload = _json_object(p.get("schema_validation_json") or "{}")
                    validation_payload["render_diff"] = schema_diff
                    rendered_validation = _json_object(rr.schema_validation_json)
                    if rendered_validation:
                        validation_payload["rendered_validation"] = rendered_validation
                    validation_payload["rendered_validation_score"] = int(rr.schema_validation_score or 0)
                    p["schema_validation_json"] = json.dumps(validation_payload, sort_keys=True)

                    base_schema_score = int(p.get("schema_validation_score") or 0)
                    combined_schema_score = int(round((base_schema_score + int(rr.schema_validation_score or 0)) / 2.0))
                    severity = str(schema_diff.get("severity") or "none")
                    if severity == "high":
                        combined_schema_score = max(0, combined_schema_score - 15)
                    elif severity == "medium":
                        combined_schema_score = max(0, combined_schema_score - 8)
                    p["schema_validation_score"] = int(max(0, min(100, combined_schema_score)))
                else:
                    p["rendered_meta_description"] = ""
                    p["rendered_canonical_urls_json"] = "[]"
                    p["rendered_canonical_count"] = 0
                    p["rendered_hreflang_links_json"] = "[]"
                    p["rendered_content_hash"] = ""
                    p["effective_h1_count"] = int(p.get("raw_h1_count") or 0)
                    p["effective_text_len"] = int(p.get("raw_text_len") or 0)
                    p["effective_links_json"] = p.get("raw_links_json") or "[]"
                    p["effective_internal_links_out"] = int(p.get("internal_links_out") or 0)
                    p["rendered_meta_robots"] = ""
                    p["rendered_effective_robots_json"] = str(p.get("effective_robots_json") or "{}")
                    p["rendered_is_noindex"] = int(p.get("is_noindex") or 0)
                    p["rendered_has_nosnippet_directive"] = int(p.get("has_nosnippet_directive") or 0)
                    p["rendered_max_snippet_directive"] = str(p.get("max_snippet_directive") or "")
                    p["rendered_max_image_preview_directive"] = str(p.get("max_image_preview_directive") or "")
                    p["rendered_max_video_preview_directive"] = str(p.get("max_video_preview_directive") or "")
                    p["rendered_data_nosnippet_count"] = int(p.get("data_nosnippet_count") or 0)
                    p["rendered_network_requests_json"] = p.get("rendered_network_requests_json") or "[]"
                    p["rendered_api_endpoints_json"] = p.get("rendered_api_endpoints_json") or "[]"
                    p["rendered_js_endpoints_json"] = p.get("rendered_js_endpoints_json") or p["rendered_api_endpoints_json"]
                    p["rendered_wait_profile"] = p.get("rendered_wait_profile") or ""
                    p["rendered_interaction_count"] = int(p.get("rendered_interaction_count") or 0)
                    p["rendered_action_recipe"] = p.get("rendered_action_recipe") or ""
                    p["rendered_discovery_links_out"] = int(p.get("rendered_discovery_links_out") or 0)

                p.update(resolve_effective_page_facts(p, rr, crawl_persona=config.crawl_persona))

                render_sessions.append(
                    RenderSessionRecord(
                        run_id=run_id,
                        url=str(p.get("normalized_url") or ""),
                        used_render=int(p.get("used_render") or 0),
                        wait_profile=str(p.get("rendered_wait_profile") or ""),
                        interaction_count=int(p.get("rendered_interaction_count") or 0),
                        action_recipe=str(p.get("rendered_action_recipe") or ""),
                        failure_family=str(p.get("render_failure_family") or ""),
                        console_errors_json=str(p.get("rendered_console_errors_json") or "[]"),
                        console_warnings_json=str(p.get("rendered_console_warnings_json") or "[]"),
                        network_endpoints_json=str(
                            p.get("rendered_js_endpoints_json")
                            or p.get("rendered_api_endpoints_json")
                            or "[]"
                        ),
                    )
                )
        if render_targets and render_failure_reasons:
            notes.append("render unavailable: " + " | ".join(sorted(render_failure_reasons)))
        if render_sessions:
            storage.insert_render_sessions(render_sessions)
        # compute effective graph with rendered links where available
        effective_links: list[dict] = []
        for p in pages:
            links_json = p.get("effective_links_json") or p.get("raw_links_json") or "[]"
            try:
                anchors = json.loads(links_json)
            except json.JSONDecodeError:
                anchors = []
            for anchor in anchors:
                href = str((anchor or {}).get("href") or "")
                target = normalize_url(href, base_url=p.get("final_url") or p.get("normalized_url"))
                effective_links.append(
                    {
                        "source_url": p["normalized_url"],
                        "normalized_target_url": target,
                        "is_internal": int(
                            is_internal_url(
                                target,
                                config.domain,
                                base_url=p.get("final_url") or p.get("normalized_url"),
                                scope_mode=config.scope_mode,
                                custom_allowlist=config.scope_allowlist,
                            )
                        ),
                    }
                )
        metrics = compute_link_metrics(config.domain, pages, effective_links)
        for p in pages:
            p.update(metrics.get(p["normalized_url"], {}))

        graph_metrics = compute_graph_metrics(config.domain, pages, effective_links)
        graph_rows: list[PageGraphMetricsRecord] = []
        for page in pages:
            row = graph_metrics.get(page["normalized_url"], {})
            page.update(row)
            graph_rows.append(
                PageGraphMetricsRecord(
                    run_id=run_id,
                    url=str(page["normalized_url"]),
                    internal_pagerank=float(row.get("internal_pagerank") or 0.0),
                    betweenness=float(row.get("betweenness") or 0.0),
                    closeness=float(row.get("closeness") or 0.0),
                    community_id=int(row.get("community_id") or 0),
                    bridge_flag=int(row.get("bridge_flag") or 0),
                )
            )
        storage.insert_page_graph_metrics(graph_rows)

        community_count = len({row.community_id for row in graph_rows if row.community_id > 0})
        bridge_count = sum(row.bridge_flag for row in graph_rows)
        close_stage(
            "render_diff",
            stage_started,
            {
                "render_mode": config.render_mode,
                "render_candidates": len(render_candidates),
                "render_targets": len(render_targets),
                "render_failures": len(render_failure_reasons),
                "classified_shell_pages": shell_count,
                "render_successes": render_successes,
                "render_failures_count": render_failures,
                "avg_raw_text_len": int(raw_text_total / max(1, len(render_candidates))),
                "avg_rendered_text_len": int(rendered_text_total / max(1, render_successes)),
                "render_sessions": len(render_sessions),
                "graph_metric_rows": len(graph_rows),
                "graph_metric_communities": community_count,
                "graph_metric_bridge_pages": bridge_count,
            },
        )
        canonical_cluster_summary = _apply_canonical_clusters(pages)
        add_event("canonical_cluster_summary", "render_diff", detail=canonical_cluster_summary)
        notes.append(
            "canonical clusters: "
            f"clusters={int(canonical_cluster_summary.get('canonical_clusters_total', 0))} "
            f"multi_member={int(canonical_cluster_summary.get('canonical_clusters_multi_member', 0))} "
            f"alias_pages={int(canonical_cluster_summary.get('canonical_alias_pages', 0))}"
        )
        add_event(
            "crawl_incremental_summary",
            "render_diff",
            detail={
                "discovered": int(crawled.incremental_stats.get("discovered", len(crawled.discovered_urls))),
                "fetched": int(crawled.incremental_stats.get("fetched", 0)),
                "reused_from_cache": int(crawled.incremental_stats.get("reused_from_cache", 0)),
                "not_modified": int(crawled.incremental_stats.get("not_modified", 0)),
                "reparsed": int(crawled.incremental_stats.get("reparsed", 0)),
                "rerendered": len(render_targets),
            },
        )

        stage_started = time.perf_counter()
        print("[8/10] update pages")
        page_update_sql = "UPDATE pages SET " + ", ".join(f"{column} = ?" for column in PAGE_UPDATE_COLUMNS) + " WHERE page_id = ?"
        (
            page_update_rows,
            page_snapshots,
            page_diffs,
            changed_pages,
            raw_duplicate_titles,
            raw_duplicate_descriptions,
        ) = _prepare_page_updates(
            run_id=run_id,
            pages=pages,
            storage=storage,
            incremental_crawl_enabled=config.incremental_crawl_enabled,
            raw_title_counts=raw_title_counts,
            raw_desc_counts=raw_desc_counts,
        )
        if raw_duplicate_titles or raw_duplicate_descriptions:
            notes.append(
                "raw duplicate diagnostics: "
                f"title_clusters={raw_duplicate_titles} "
                f"description_clusters={raw_duplicate_descriptions}"
            )
        if page_update_rows:
            storage.conn.executemany(page_update_sql, page_update_rows)
        storage.conn.commit()
        if page_snapshots:
            storage.insert_page_snapshots(page_snapshots)
        if page_diffs:
            storage.insert_page_diffs(page_diffs)
        close_stage(
            "update_pages",
            stage_started,
            {
                "pages": len(pages),
                "changed_pages": changed_pages,
                "snapshots": len(page_snapshots),
                "page_diffs": len(page_diffs),
            },
        )

        stage_started = time.perf_counter()
        print("[9/10] issues+scores+performance")
        sitemap_intelligence = analyze_sitemap_intelligence(config.domain, pages, sitemap_entries)
        add_event("sitemap_analysis_summary", "issues_scores_performance", detail=sitemap_intelligence)
        notes.append(
            "sitemap intelligence: "
            f"sitemap_urls={sitemap_intelligence.get('sitemap_url_count', 0)} "
            f"discovered_pages={sitemap_intelligence.get('discovered_page_count', 0)} "
            f"not_crawled={sitemap_intelligence.get('urls_in_sitemap_not_crawled', 0)} "
            f"scope_violations={sitemap_intelligence.get('sitemap_scope_violations', 0)}"
        )
        issues = build_issues(run_id, pages)
        issues.extend(_build_sitemap_delta_issues(run_id, config.domain, pages, sitemap_entries))
        render_frontier_checks = int(crawled.discovery_stats.get("render_frontier_checks", 0))
        render_frontier_successes = int(crawled.discovery_stats.get("render_frontier_successes", 0))
        render_frontier_failures = int(crawled.discovery_stats.get("render_frontier_failures", 0))
        render_frontier_enqueued = int(crawled.discovery_stats.get("enqueued_via_render_link", 0))
        schema_validation_rows: list[SchemaValidationRecord] = []
        media_asset_rows: list[MediaAssetRecord] = []
        index_state_rows: list[IndexStateHistoryRecord] = []
        submission_events: list[SubmissionEventRecord] = []
        if (
            config.crawl_discovery_mode in {"hybrid", "browser_first"}
            and shell_count > 0
            and (render_frontier_checks == 0 or (render_frontier_successes == 0 and render_frontier_enqueued == 0))
        ):
            issues.append(
                IssueRecord(
                    run_id,
                    config.domain,
                    "medium",
                    "DISCOVERY_BLIND_SPOT",
                    "Rendered frontier did not expand discovery",
                    "Shell-like pages were detected but rendered discovery produced little or no additional internal URL expansion.",
                    evidence_json=json.dumps(
                        {
                            "shell_like_pages": shell_count,
                            "render_frontier_checks": render_frontier_checks,
                            "render_frontier_successes": render_frontier_successes,
                            "render_frontier_failures": render_frontier_failures,
                            "render_frontier_enqueued": render_frontier_enqueued,
                            "crawl_discovery_mode": config.crawl_discovery_mode,
                        }
                    ),
                    technical_seo_gate="discovery",
                    verification_status="needs_rendered_verification",
                    confidence_score=70,
                )
            )
        for issue in issues:
            provenance_counts[issue.issue_provenance] = provenance_counts.get(issue.issue_provenance, 0) + 1
        if render_targets and render_failure_reasons:
            issues.append(
                IssueRecord(
                    run_id,
                    config.domain,
                    "medium",
                    "RENDER_UNAVAILABLE",
                    "Render unavailable",
                    "Playwright render checks could not execute.",
                    evidence_json=str(sorted(render_failure_reasons)),
                    technical_seo_gate="rendering",
                    verification_status="needs_rendered_verification",
                    confidence_score=70,
                )
            )

        perf_source_pages = pages
        if config.incremental_crawl_enabled and not heavy_rule_changed:
            perf_source_pages = [
                page
                for page in pages
                if int(page.get("changed_since_last_run") or 0) == 1
            ]
        perf_targets = select_performance_targets(perf_source_pages, config.performance_targets)
        if pages and len(perf_targets) < min(config.performance_targets, len(pages)):
            notes.append(
                f"provider targets filtered: selected {len(perf_targets)} eligible page(s) from {len(pages)} crawled"
            )
        if config.incremental_crawl_enabled and not heavy_rule_changed:
            notes.append(
                "performance planner: "
                f"changed_pages={len(perf_source_pages)} "
                f"targets={len(perf_targets)}"
            )
        if perf_targets:
            notes.append("provider targets: " + " | ".join(perf_targets[:5]))

        lighthouse_targets: list[str] = []
        if bool(config.lighthouse_enabled):
            lighthouse_targets = select_performance_targets(perf_source_pages, config.lighthouse_targets)
            if lighthouse_targets:
                notes.append("lighthouse targets: " + " | ".join(lighthouse_targets[:5]))

        psi_rows = []
        psi_messages: list[str] = []
        crux_rows = []
        crux_errors: list[str] = []
        psi_telemetry: dict[str, float | int] = {}
        crux_telemetry: dict[str, float | int] = {}
        lighthouse_rows = []
        lighthouse_messages: list[str] = []
        lighthouse_telemetry: dict[str, int | float] = {}
        if not perf_targets:
            notes.append("psi skipped: no targets")
            notes.append("crux skipped: no targets")
        else:
            notes.append(
                "provider scheduler: "
                f"psi_workers={config.psi_workers} "
                f"rate_limit={config.provider_rate_limit_rps:.2f}/s "
                f"burst={config.provider_rate_limit_capacity}"
            )
            provider_rate_limiter = TokenBucketRateLimiter(
                rate_per_second=config.provider_rate_limit_rps,
                capacity=config.provider_rate_limit_capacity,
            )
            with ThreadPoolExecutor(max_workers=2) as provider_executor:
                psi_future = None
                crux_future = None
                if config.psi_enabled:
                    psi_future = provider_executor.submit(
                        collect_performance,
                        run_id,
                        perf_targets,
                        timeout=20,
                        store_payloads=config.store_provider_payloads,
                        retry_config=provider_retry,
                        telemetry=psi_telemetry,
                        workers=config.psi_workers,
                        rate_limiter=provider_rate_limiter,
                    )
                if config.crux_enabled:
                    crux_future = provider_executor.submit(
                        collect_crux,
                        run_id,
                        perf_targets,
                        timeout=20,
                        origin_fallback=config.crux_origin_fallback,
                        store_payloads=config.store_provider_payloads,
                        retry_config=provider_retry,
                        telemetry=crux_telemetry,
                        rate_limiter=provider_rate_limiter,
                    )
                if psi_future is not None:
                    psi_rows, psi_messages = psi_future.result()
                if crux_future is not None:
                    crux_rows, crux_errors = crux_future.result()

        if not perf_targets:
            pass
        elif config.psi_enabled:
            psi_retry_info = [m for m in psi_messages if m.startswith("retry_info:")]
            psi_failures = [m for m in psi_messages if m.startswith("failed_http:")]
            psi_skipped = [m for m in psi_messages if m.startswith("skipped_missing_key:")]
            psi_no_data = [m for m in psi_messages if m.startswith("no_data:")]

            if psi_rows:
                storage.insert_performance(psi_rows)

            notes.append(
                "psi status: "
                f"success={len(psi_rows)} "
                f"no_data={len(psi_no_data)} "
                f"failed_http={len(psi_failures)} "
                f"skipped_missing_key={len(psi_skipped)}"
            )

            if psi_retry_info:
                notes.append("psi retries: " + " | ".join(psi_retry_info[:5]))
            if psi_skipped:
                notes.append("psi skipped: " + " | ".join(psi_skipped[:5]))
            if psi_no_data:
                notes.append("psi no_data: " + " | ".join(psi_no_data[:5]))
            if psi_failures:
                notes.append("psi errors: " + " | ".join(psi_failures[:5]))
                issues.append(
                    IssueRecord(
                        run_id,
                        config.domain,
                        "low",
                        "PERFORMANCE_PROVIDER_ERROR",
                        "Performance provider errors",
                        "PSI requests failed for one or more targets.",
                        evidence_json=" | ".join(psi_failures[:5]),
                        technical_seo_gate="serving",
                        verification_status="automated",
                        confidence_score=75,
                    )
                )
        else:
            notes.append("psi skipped: disabled")

        if not perf_targets:
            pass
        elif config.crux_enabled:
            if crux_rows:
                storage.insert_crux(crux_rows)

            crux_retry_info = [m for m in crux_errors if m.startswith("retry_info:")]
            crux_failure_errors = [m for m in crux_errors if not m.startswith("retry_info:")]
            crux_status_counts = {
                "success": sum(1 for row in crux_rows if row.status == "success"),
                "no_data": sum(1 for row in crux_rows if row.status == "no_data"),
                "failed_http": sum(1 for row in crux_rows if row.status == "failed_http"),
                "skipped_missing_key": sum(1 for row in crux_rows if row.status == "skipped_missing_key"),
            }
            if crux_rows:
                notes.append(
                    "crux status: "
                    f"success={crux_status_counts['success']} "
                    f"no_data={crux_status_counts['no_data']} "
                    f"failed_http={crux_status_counts['failed_http']} "
                    f"skipped_missing_key={crux_status_counts['skipped_missing_key']}"
                )
            if crux_retry_info:
                notes.append("crux retries: " + " | ".join(crux_retry_info[:5]))
            if crux_failure_errors:
                notes.append("crux errors: " + " | ".join(crux_failure_errors[:5]))
                issues.append(
                    IssueRecord(
                        run_id,
                        config.domain,
                        "low",
                        "CRUX_PROVIDER_ERROR",
                        "CrUX provider errors",
                        "CrUX requests failed for one or more targets.",
                        evidence_json=" | ".join(crux_failure_errors[:5]),
                        technical_seo_gate="serving",
                        verification_status="automated",
                        confidence_score=75,
                    )
                )
            elif not crux_rows:
                notes.append("crux no_data: no rows returned")
        else:
            notes.append("crux skipped: disabled")

        if bool(config.lighthouse_enabled):
            if not lighthouse_targets:
                notes.append("lighthouse skipped: no targets")
            else:
                lighthouse_rows, lighthouse_messages, lighthouse_telemetry = collect_lighthouse(
                    run_id,
                    lighthouse_targets,
                    output_dir=config.output_dir,
                    form_factor=config.lighthouse_form_factor,
                    timeout_seconds=config.lighthouse_timeout_seconds,
                    config_path=config.lighthouse_config_path,
                    store_payloads=config.store_provider_payloads,
                    budgets=LighthouseBudgetConfig(
                        performance_min=config.lighthouse_budget_performance_min,
                        seo_min=config.lighthouse_budget_seo_min,
                    ),
                )

                if lighthouse_rows:
                    storage.insert_lighthouse(lighthouse_rows)

                success_rows = [row for row in lighthouse_rows if str(row.status) == "success"]
                failed_rows = [row for row in lighthouse_rows if str(row.status) == "failed"]
                skipped_rows = [row for row in lighthouse_rows if str(row.status).startswith("skipped")]
                budget_failed_rows = [
                    row
                    for row in success_rows
                    if int(row.budget_pass or 0) == 0
                ]

                notes.append(
                    "lighthouse status: "
                    f"success={len(success_rows)} "
                    f"failed={len(failed_rows)} "
                    f"skipped={len(skipped_rows)} "
                    f"budget_failed={len(budget_failed_rows)}"
                )

                if lighthouse_messages:
                    notes.append("lighthouse messages: " + " | ".join(lighthouse_messages[:5]))

                if budget_failed_rows:
                    budget_failure_sample = [
                        {
                            "url": row.url,
                            "performance_score": row.performance_score,
                            "seo_score": row.seo_score,
                            "budget_failures": _json_list(row.budget_failures_json),
                        }
                        for row in budget_failed_rows[:8]
                    ]
                    issues.append(
                        IssueRecord(
                            run_id,
                            config.domain,
                            "medium",
                            "LIGHTHOUSE_BUDGET_FAIL",
                            "Lighthouse budget thresholds not met",
                            "One or more Lighthouse lab runs failed configured budget thresholds.",
                            evidence_json=json.dumps(
                                {
                                    "performance_budget_min": int(config.lighthouse_budget_performance_min),
                                    "seo_budget_min": int(config.lighthouse_budget_seo_min),
                                    "affected_urls": len(budget_failed_rows),
                                    "samples": budget_failure_sample,
                                },
                                sort_keys=True,
                            ),
                            technical_seo_gate="serving",
                            verification_status="automated",
                            confidence_score=80,
                        )
                    )
        else:
            notes.append("lighthouse skipped: disabled")

        perf_map: dict[str, list[int]] = {}
        for row in psi_rows:
            perf_map.setdefault(row.url, []).append(row.performance_score or 50)

        measurement_by_url = _derive_measurement_status_by_url(
            pages,
            psi_rows,
            psi_messages,
            crux_rows,
            crux_errors,
        )
        measurement_update_rows, schema_validation_rows, media_asset_rows = _prepare_measurement_records(
            run_id=run_id,
            pages=pages,
            measurement_by_url=measurement_by_url,
        )
        if measurement_update_rows:
            storage.conn.executemany(
                "UPDATE pages SET measurement_status = ?, measurement_error_family = ? WHERE page_id = ?",
                measurement_update_rows,
            )
            storage.conn.commit()
        issues = enrich_issues(issues, pages)
        issues_by_url: dict[str, list[IssueRecord]] = {}
        for issue in issues:
            issues_by_url.setdefault(str(issue.url or ""), []).append(issue)

        score_rows = []
        for p in pages:
            perf_val = None
            if p["normalized_url"] in perf_map:
                vals = perf_map[p["normalized_url"]]
                perf_val = int(sum(vals) / len(vals))
            score_rows.append(
                ScoreRecord(
                    run_id=run_id,
                    url=p["normalized_url"],
                    **score_page(
                        p,
                        perf_val,
                        site_type=config.site_type,
                        score_profile=config.scoring_profile,
                        page_issues=issues_by_url.get(str(p.get("normalized_url") or ""), []),
                    ),
                )
            )
        storage.insert_scores(score_rows)
        if psi_telemetry:
            add_event("provider_summary", "psi", detail={k: psi_telemetry[k] for k in sorted(psi_telemetry.keys())})
            notes.append(
                "psi telemetry: "
                f"attempts={int(psi_telemetry.get('attempts', 0))} "
                f"http_attempts={int(psi_telemetry.get('http_attempts', 0))} "
                f"retries={int(psi_telemetry.get('retries', 0))} "
                f"wait_s={float(psi_telemetry.get('wait_seconds', 0.0)):.2f} "
                f"timeouts={int(psi_telemetry.get('timeouts', 0))}"
            )
        if crux_telemetry:
            add_event("provider_summary", "crux", detail={k: crux_telemetry[k] for k in sorted(crux_telemetry.keys())})
            notes.append(
                "crux telemetry: "
                f"attempts={int(crux_telemetry.get('attempts', 0))} "
                f"http_attempts={int(crux_telemetry.get('http_attempts', 0))} "
                f"retries={int(crux_telemetry.get('retries', 0))} "
                f"wait_s={float(crux_telemetry.get('wait_seconds', 0.0)):.2f} "
                f"timeouts={int(crux_telemetry.get('timeouts', 0))}"
            )
        if lighthouse_telemetry:
            add_event("provider_summary", "lighthouse", detail={k: lighthouse_telemetry[k] for k in sorted(lighthouse_telemetry.keys())})
            notes.append(
                "lighthouse telemetry: "
                f"attempts={int(lighthouse_telemetry.get('attempts', 0))} "
                f"success={int(lighthouse_telemetry.get('success', 0))} "
                f"failed={int(lighthouse_telemetry.get('failed', 0))} "
                f"skipped_missing_dependency={int(lighthouse_telemetry.get('skipped_missing_dependency', 0))} "
                f"budget_failed={int(lighthouse_telemetry.get('budget_failed', 0))}"
            )

        gsc_targets = select_performance_targets(pages, config.gsc_url_limit)
        if config.gsc_enabled:
            gsc_property = resolve_property(config.domain, config.gsc_property)
            gsc_rows, gsc_meta = collect_index_states(
                gsc_property,
                gsc_targets,
                credentials_json=config.gsc_credentials_json,
                timeout=config.timeout,
            )
            gsc_summary = reconcile_index_states(gsc_targets, gsc_rows)
            gsc_detail = {
                **gsc_meta,
                **gsc_summary,
                "property_candidates": property_candidates(config.domain),
            }
            submission_events.append(
                SubmissionEventRecord(
                    run_id=run_id,
                    url=config.domain,
                    engine="google",
                    action="url_inspection",
                    status=str(gsc_meta.get("status") or "unknown"),
                    payload_json=json.dumps(gsc_detail, sort_keys=True),
                )
            )
            add_event("provider_summary", "gsc", detail=gsc_detail)
            notes.append(
                "gsc status: "
                f"{gsc_meta.get('status', 'unknown')} "
                f"property={gsc_detail.get('property_uri', '')} "
                f"indexed={gsc_summary.get('indexed', 0)} "
                f"not_indexed={gsc_summary.get('not_indexed', 0)} "
                f"unknown={gsc_summary.get('unknown', 0)}"
            )
            for row in gsc_rows:
                gsc_url = normalize_url(str(row.get("url") or ""), base_url=config.domain)
                if not gsc_url:
                    continue
                state_payload = dict(row)
                state_payload["property_uri"] = gsc_property
                index_state_rows.append(
                    IndexStateHistoryRecord(
                        run_id=run_id,
                        url=gsc_url,
                        source="gsc_url_inspection",
                        status=str(row.get("status") or "unknown"),
                        state_payload_json=json.dumps(state_payload, sort_keys=True),
                    )
                )
            gsc_status = str(gsc_meta.get("status") or "").strip().lower()
            if gsc_status in {"success", "success_partial", "success_empty"}:
                not_indexed_rows = [
                    row for row in gsc_rows if str(row.get("status") or "").strip().lower() == "not_indexed"
                ]
                for row in not_indexed_rows:
                    gsc_url = normalize_url(str(row.get("url") or ""), base_url=config.domain)
                    if not gsc_url:
                        continue
                    evidence = {
                        "status": row.get("status", "unknown"),
                        "coverage_state": str(row.get("coverage_state") or ""),
                        "indexing_state": str(row.get("indexing_state") or ""),
                        "verdict": str(row.get("verdict") or ""),
                        "last_crawl_time": str(row.get("last_crawl_time") or ""),
                        "robots_txt_state": str(row.get("robots_txt_state") or ""),
                        "page_fetch_state": str(row.get("page_fetch_state") or ""),
                        "referring_urls": row.get("referring_urls") if isinstance(row.get("referring_urls"), list) else [],
                    }
                    issues.append(
                        IssueRecord(
                            run_id,
                            gsc_url,
                            "medium",
                            "GSC_INDEX_STATE_NOT_INDEXED",
                            "Search Console reports URL not indexed",
                            "Google Search Console URL inspection indicates this URL is not indexed.",
                            evidence_json=json.dumps(evidence, sort_keys=True),
                            technical_seo_gate="indexability",
                            verification_status="external_validated",
                            confidence_score=95,
                        )
                    )

            if bool(config.gsc_analytics_enabled):
                analytics_start, analytics_end = default_date_window(int(config.gsc_analytics_days))
                analytics_rows, analytics_meta = collect_search_analytics(
                    gsc_property,
                    credentials_json=config.gsc_credentials_json,
                    start_date=analytics_start,
                    end_date=analytics_end,
                    dimensions=tuple(config.gsc_analytics_dimensions),
                    row_limit=int(config.gsc_analytics_row_limit),
                    timeout=max(20.0, float(config.timeout) * 2.0),
                )
                analytics_summary = summarize_search_analytics(analytics_rows)
                analytics_detail = {
                    **analytics_meta,
                    **analytics_summary,
                }
                add_event("provider_summary", "gsc_analytics", detail=analytics_detail)
                submission_events.append(
                    SubmissionEventRecord(
                        run_id=run_id,
                        url=config.domain,
                        engine="google",
                        action="search_analytics",
                        status=str(analytics_meta.get("status") or "unknown"),
                        payload_json=json.dumps(analytics_detail, sort_keys=True),
                    )
                )
                notes.append(
                    "gsc analytics: "
                    f"status={analytics_meta.get('status', 'unknown')} "
                    f"rows={analytics_summary.get('rows', 0)} "
                    f"clicks={analytics_summary.get('clicks', 0.0)} "
                    f"impressions={analytics_summary.get('impressions', 0.0)}"
                )

                page_metrics: dict[str, dict[str, float]] = defaultdict(lambda: {"clicks": 0.0, "impressions": 0.0})
                for row in analytics_rows:
                    page_url_raw = str(row.get("page") or "").strip()
                    if not page_url_raw:
                        continue
                    page_url = normalize_url(page_url_raw, base_url=config.domain)
                    page_metrics[page_url]["clicks"] += float(row.get("clicks") or 0.0)
                    page_metrics[page_url]["impressions"] += float(row.get("impressions") or 0.0)

                gsc_adapter = GSCAnalyticsVisibilityAdapter()
                for page in pages:
                    page_url = str(page.get("normalized_url") or "")
                    metrics = page_metrics.get(page_url)
                    if metrics is None:
                        continue

                    existing_visibility = parse_ai_visibility_payload(page.get("ai_visibility_json") or "{}")
                    existing_evidence = dict(existing_visibility.get("observed_evidence") or {})
                    if not existing_evidence:
                        existing_evidence = _json_object(page.get("citation_evidence_json") or "{}")

                    refreshed = build_citation_evidence(
                        page,
                        gsc_impressions=int(round(metrics.get("impressions", 0.0))),
                        gsc_clicks=int(round(metrics.get("clicks", 0.0))),
                        chatgpt_referrals=int(existing_evidence.get("chatgpt_referrals") or 0),
                    )
                    if "eligibility_reasons" in existing_evidence:
                        refreshed["eligibility_reasons"] = existing_evidence["eligibility_reasons"]
                    if "governance_blocked_bots" in existing_evidence:
                        refreshed["governance_blocked_bots"] = existing_evidence["governance_blocked_bots"]

                    enriched_evidence, adapters_applied, adapter_errors = apply_visibility_adapters(
                        refreshed,
                        context=AdapterContext(
                            run_id=run_id,
                            page=page,
                            gsc_metrics=metrics,
                        ),
                        adapters=(gsc_adapter,),
                    )
                    merged_visibility = merge_ai_visibility_payload(
                        page.get("ai_visibility_json") or "{}",
                        observed_evidence=enriched_evidence,
                        adapters_applied=adapters_applied,
                        adapter_errors=adapter_errors,
                        potential_score=int(page.get("ai_discoverability_potential_score") or page.get("citation_eligibility_score") or 0),
                    )
                    page["ai_visibility_json"] = json.dumps(merged_visibility, sort_keys=True)
                    page["citation_evidence_json"] = json.dumps(
                        legacy_citation_evidence_from_payload(merged_visibility),
                        sort_keys=True,
                    )
                    if int(page.get("citation_eligibility_score") or 0) <= 0:
                        page["citation_eligibility_score"] = int(page.get("ai_discoverability_potential_score") or 0)
        else:
            add_event(
                "provider_summary",
                "gsc",
                detail={
                    "status": "skipped_disabled",
                    "property_uri": "",
                    "urls_requested": 0,
                    "rows_returned": 0,
                    "crawled_total": len(gsc_targets),
                    "indexed": 0,
                    "not_indexed": 0,
                    "unknown": len(gsc_targets),
                    "property_candidates": property_candidates(config.domain),
                },
            )
            notes.append("gsc skipped: disabled")
            submission_events.append(
                SubmissionEventRecord(
                    run_id=run_id,
                    url=config.domain,
                    engine="google",
                    action="url_inspection",
                    status="skipped_disabled",
                    payload_json=json.dumps({"property_candidates": property_candidates(config.domain)}, sort_keys=True),
                )
            )

        citation_event_rows: list[CitationEventRecord] = []
        ai_visibility_event_rows: list[AIVisibilityRecord] = []
        for page in pages:
            page_url = str(page.get("normalized_url") or "")
            if not page_url:
                continue

            visibility_payload = parse_ai_visibility_payload(page.get("ai_visibility_json") or "{}")
            if not visibility_payload.get("observed_evidence") and str(page.get("citation_evidence_json") or "{}").strip():
                visibility_payload = merge_ai_visibility_payload(
                    visibility_payload,
                    observed_evidence=_json_object(page.get("citation_evidence_json") or "{}"),
                    potential_score=int(page.get("ai_discoverability_potential_score") or page.get("citation_eligibility_score") or 0),
                )

            page["ai_discoverability_potential_score"] = int(
                page.get("ai_discoverability_potential_score")
                or page.get("citation_eligibility_score")
                or visibility_payload.get("potential", {}).get("score")
                or 0
            )
            page["citation_eligibility_score"] = int(page.get("citation_eligibility_score") or page.get("ai_discoverability_potential_score") or 0)
            page["ai_visibility_json"] = json.dumps(visibility_payload, sort_keys=True)
            page["citation_evidence_json"] = json.dumps(
                legacy_citation_evidence_from_payload(visibility_payload),
                sort_keys=True,
            )

            citation_event_rows.append(
                CitationEventRecord(
                    run_id=run_id,
                    url=page_url,
                    eligibility_score=int(page.get("citation_eligibility_score") or 0),
                    evidence_json=str(page.get("citation_evidence_json") or "{}"),
                )
            )
            ai_visibility_event_rows.append(
                AIVisibilityRecord(
                    run_id=run_id,
                    url=page_url,
                    potential_score=int(page.get("ai_discoverability_potential_score") or 0),
                    visibility_json=str(page.get("ai_visibility_json") or "{}"),
                )
            )
            storage.conn.execute(
                """
                UPDATE pages
                SET
                    ai_visibility_json = ?,
                    ai_discoverability_potential_score = ?,
                    citation_evidence_json = ?,
                    citation_eligibility_score = ?
                WHERE page_id = ?
                """,
                (
                    str(page.get("ai_visibility_json") or "{}"),
                    int(page.get("ai_discoverability_potential_score") or 0),
                    str(page.get("citation_evidence_json") or "{}"),
                    int(page.get("citation_eligibility_score") or 0),
                    int(page.get("page_id") or 0),
                ),
            )
        storage.conn.commit()

        score_lookup = {row.url: row.overall_score for row in score_rows}
        template_cluster_rollup: dict[tuple[str, str], dict[str, float | int]] = {}
        for page in pages:
            page_url = str(page.get("normalized_url") or "")
            if not page_url:
                continue
            cluster_key = str(
                page.get("frontier_cluster_key")
                or page.get("platform_template_hint")
                or page.get("page_type")
                or "unclustered"
            )
            page_type = str(page.get("page_type") or "other")
            key = (cluster_key, page_type)
            aggregate = template_cluster_rollup.setdefault(
                key,
                {"url_count": 0, "score_total": 0.0, "issue_count": 0},
            )
            aggregate["url_count"] = int(aggregate["url_count"]) + 1
            aggregate["score_total"] = float(aggregate["score_total"]) + float(score_lookup.get(page_url, 0))
            aggregate["issue_count"] = int(aggregate["issue_count"]) + len(issues_by_url.get(page_url, []))

        template_cluster_rows: list[TemplateClusterRecord] = []
        for (cluster_key, page_type), aggregate in sorted(template_cluster_rollup.items()):
            url_count = max(1, int(aggregate["url_count"]))
            template_cluster_rows.append(
                TemplateClusterRecord(
                    run_id=run_id,
                    template_cluster=cluster_key,
                    page_type=page_type,
                    url_count=url_count,
                    avg_score=float(aggregate["score_total"]) / url_count,
                    issue_count=int(aggregate["issue_count"]),
                )
            )

        if schema_validation_rows:
            storage.insert_schema_validations(schema_validation_rows)
        if media_asset_rows:
            storage.insert_media_assets(media_asset_rows)
        if index_state_rows:
            storage.insert_index_state_history(index_state_rows)
        if citation_event_rows:
            storage.insert_citation_events(citation_event_rows)
        if ai_visibility_event_rows:
            storage.insert_ai_visibility_events(ai_visibility_event_rows)
        if submission_events:
            storage.insert_submission_events(submission_events)
        if template_cluster_rows:
            storage.insert_template_clusters(template_cluster_rows)

        storage.insert_issues(issues)
        close_stage(
            "issues_scores_performance",
            stage_started,
            {
                "issues": len(issues),
                "scores": len(score_rows),
                "performance_rows": len(psi_rows),
                "lighthouse_rows": len(lighthouse_rows),
                "performance_targets": len(perf_targets),
                "schema_validations": len(schema_validation_rows),
                "media_assets": len(media_asset_rows),
                "index_state_rows": len(index_state_rows),
                "citation_events": len(citation_event_rows),
                "ai_visibility_events": len(ai_visibility_event_rows),
                "submission_events": len(submission_events),
                "template_clusters": len(template_cluster_rows),
                "issue_provenance_raw_only": provenance_counts.get("raw_only", 0),
                "issue_provenance_rendered_only": provenance_counts.get("rendered_only", 0),
                "issue_provenance_both": provenance_counts.get("both", 0),
                "render_frontier_checks": int(crawled.discovery_stats.get("render_frontier_checks", 0)),
                "render_frontier_successes": int(crawled.discovery_stats.get("render_frontier_successes", 0)),
                "render_frontier_failures": int(crawled.discovery_stats.get("render_frontier_failures", 0)),
                "render_frontier_enqueued": int(crawled.discovery_stats.get("enqueued_via_render_link", 0)),
            },
        )

        if config.offsite_commoncrawl_enabled:
            offsite_join_state = "not_started"
            offsite_payload: OffsiteCommonCrawlWorkerPayload | None = None
            if offsite_future is not None:
                offsite_payload, offsite_join_state = _join_offsite_commoncrawl_future(
                    future=offsite_future,
                    schedule=config.offsite_commoncrawl_schedule,
                    join_budget_seconds=config.offsite_commoncrawl_join_budget_seconds,
                    control=offsite_control,
                )

            if offsite_payload is None:
                deferred_status = STATUS_PENDING_BACKGROUND
                if offsite_join_state == "timeout":
                    deferred_status = STATUS_TIMEOUT_BACKGROUND
                if offsite_start_error and offsite_future is None:
                    deferred_status = STATUS_FAILED_QUERY
                offsite_payload = _build_pending_offsite_payload(
                    target_domain=offsite_target_domain,
                    release=offsite_launch_release,
                    mode=config.offsite_commoncrawl_mode,
                    schedule=config.offsite_commoncrawl_schedule,
                    cache_state=offsite_cache_state,
                    status=deferred_status,
                    reason=(
                        offsite_start_error
                        or f"concurrent_join_state={offsite_join_state}"
                    ),
                    started_at=(offsite_lane_started_at or datetime.now(timezone.utc).isoformat()),
                    compare_domains=config.offsite_compare_domains,
                )

            _persist_offsite_commoncrawl_payload(storage, run_id, offsite_payload)
            offsite_summary = offsite_payload.summary
            add_event(
                "offsite_commoncrawl_status",
                "offsite_commoncrawl",
                detail={
                    "status": offsite_summary.status,
                    "mode": offsite_summary.mode,
                    "schedule": offsite_summary.schedule,
                    "release": offsite_summary.cc_release,
                    "cache_state": offsite_summary.cache_state,
                    "target_found_flag": int(offsite_summary.target_found_flag or 0),
                    "comparison_domain_count": int(offsite_summary.comparison_domain_count or 0),
                    "linking_domains": len(offsite_payload.linking_domains),
                    "join_state": offsite_join_state,
                },
            )
            add_event(
                "offsite_commoncrawl_timing",
                "offsite_commoncrawl",
                elapsed_ms=int(offsite_summary.query_elapsed_ms or 0),
                detail={
                    "query_elapsed_ms": int(offsite_summary.query_elapsed_ms or 0),
                    "background_started_at": offsite_summary.background_started_at,
                    "background_finished_at": offsite_summary.background_finished_at,
                    "join_state": offsite_join_state,
                },
            )
            notes.append(
                "offsite commoncrawl result: "
                f"status={offsite_summary.status} "
                f"mode={offsite_summary.mode} "
                f"schedule={offsite_summary.schedule} "
                f"release={offsite_summary.cc_release} "
                f"cache_state={offsite_summary.cache_state} "
                f"target_found={int(offsite_summary.target_found_flag or 0)}"
            )

        stage_started = time.perf_counter()
        print("[10/10] export")
        storage.export_csvs(config.output_dir, run_id=run_id)
        close_stage("export_csv", stage_started, {"output_dir": str(config.output_dir)})

        if stage_timings:
            timing_summary = ", ".join(f"{stage}={elapsed_ms / 1000.0:.2f}s" for stage, elapsed_ms in stage_timings)
            notes.append("stage timing: " + timing_summary)

        completed_at = datetime.now(timezone.utc).isoformat()
        flush_events()
        storage.export_run_events_csv(config.output_dir, run_id)
        storage.update_run_completion(run_id, completed_at, status, notes="; ".join(notes))

        build_markdown_report(storage, run_id, config.output_dir / "report.md")
        print(f"[10/10] done {run_id}")
    except Exception as exc:
        status = "failed"
        notes.append(f"fatal: {exc}")
        run_events.append(
            {
                "event_time": datetime.now(timezone.utc).isoformat(),
                "event_type": "error",
                "stage": "run",
                "message": str(exc),
                "elapsed_ms": 0,
                "detail_json": "{}",
            }
        )
        try:
            if stage_timings:
                timing_summary = ", ".join(f"{stage}={elapsed_ms / 1000.0:.2f}s" for stage, elapsed_ms in stage_timings)
                notes.append("stage timing: " + timing_summary)
            flush_events()
        except Exception:
            pass
        logging.exception("audit failed")
        completed_at = datetime.now(timezone.utc).isoformat()
        storage.update_run_completion(run_id, completed_at, status, notes="; ".join(notes))
        try:
            build_markdown_report(storage, run_id, config.output_dir / "report.md")
        except Exception:
            pass
        raise
    finally:
        if offsite_control is not None:
            offsite_control.request_interrupt()
        if offsite_executor is not None:
            offsite_executor.shutdown(wait=False, cancel_futures=True)
        storage.close()


def _queue_db_path(raw_path: str) -> Path:
    candidate = Path(str(raw_path or "./out/queue.sqlite")).expanduser()
    if candidate.suffix:
        return candidate.resolve()
    return (candidate / "queue.sqlite").resolve()


def _resolve_enqueue_config(args: argparse.Namespace) -> tuple[str, dict[str, Any]]:
    run_profile = str(args.run_profile or "standard").strip().lower()
    if run_profile not in RUN_PROFILE_DEFAULTS:
        run_profile = "standard"
    profile_values = RUN_PROFILE_DEFAULTS[run_profile]

    domain = normalize_url(str(args.domain or "").strip())
    max_pages = int(args.max_pages if args.max_pages is not None else profile_values["max_pages"])
    render_mode = str(args.render_mode or profile_values["render_mode"]).strip().lower()
    max_render_pages = int(
        args.max_render_pages if args.max_render_pages is not None else profile_values["max_render_pages"]
    )
    performance_targets = int(
        args.performance_targets
        if args.performance_targets is not None
        else profile_values["performance_targets"]
    )

    payload = {
        "domain": domain,
        "run_profile": run_profile,
        "max_pages": max(1, max_pages),
        "render_mode": render_mode,
        "max_render_pages": max(0, max_render_pages),
        "performance_targets": max(1, performance_targets),
        "screenshot_count": max(0, int(getattr(args, "screenshot_count", 0) or 0)),
        "offsite_commoncrawl_enabled": bool(getattr(args, "offsite_commoncrawl_enabled", False)),
        "lighthouse_enabled": bool(getattr(args, "lighthouse_enabled", False)),
    }
    return domain, payload


def run_enqueue(args: argparse.Namespace) -> None:
    queue_db = _queue_db_path(str(getattr(args, "queue_db", "./out/queue.sqlite") or "./out/queue.sqlite"))
    output_dir = Path(str(args.output or "./out")).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    domain, payload = _resolve_enqueue_config(args)
    store = QueueStore(queue_db)
    store.init_db()
    try:
        job = store.enqueue_job(
            domain=domain,
            output_dir=str(output_dir),
            config=payload,
            priority=int(getattr(args, "priority", 0) or 0),
            max_attempts=max(1, int(getattr(args, "max_attempts", 2) or 2)),
            dedupe_key=str(getattr(args, "dedupe_key", "") or ""),
        )
    finally:
        store.close()
    print(json.dumps(job, sort_keys=True))


def run_jobs_command(args: argparse.Namespace) -> None:
    queue_db = _queue_db_path(str(getattr(args, "queue_db", "./out/queue.sqlite") or "./out/queue.sqlite"))
    store = QueueStore(queue_db)
    store.init_db()
    try:
        rows = store.list_jobs(
            state=str(getattr(args, "state", "") or "").strip().lower(),
            limit=max(1, int(getattr(args, "limit", 100) or 100)),
        )
    finally:
        store.close()
    print(json.dumps({"jobs": rows}, sort_keys=True))


def run_cancel_command(args: argparse.Namespace) -> None:
    queue_db = _queue_db_path(str(getattr(args, "queue_db", "./out/queue.sqlite") or "./out/queue.sqlite"))
    store = QueueStore(queue_db)
    store.init_db()
    try:
        payload = store.request_cancel(str(args.job_id))
    finally:
        store.close()
    print(json.dumps(payload, sort_keys=True))


def run_worker_command(args: argparse.Namespace) -> None:
    queue_db = _queue_db_path(str(getattr(args, "queue_db", "./out/queue.sqlite") or "./out/queue.sqlite"))
    worker_id = str(getattr(args, "worker_id", "") or "").strip() or None
    max_jobs = int(getattr(args, "max_jobs", 0) or 0)
    if bool(getattr(args, "once", False)):
        max_jobs = 1
    if max_jobs <= 0:
        max_jobs = None

    policy = AdmissionPolicy(
        total_token_budget=max(1, int(getattr(args, "total_token_budget", 6) or 6)),
        max_render_heavy_jobs=max(0, int(getattr(args, "max_render_heavy_jobs", 1) or 1)),
        max_provider_heavy_jobs=max(0, int(getattr(args, "max_provider_heavy_jobs", 1) or 1)),
        max_offsite_heavy_jobs=max(0, int(getattr(args, "max_offsite_heavy_jobs", 1) or 1)),
        enforce_one_active_job_per_domain=not bool(getattr(args, "allow_concurrent_same_domain", False)),
    )

    processed = run_queue_worker(
        queue_db=queue_db,
        worker_id=worker_id,
        poll_seconds=max(0.1, float(getattr(args, "poll_seconds", 1.0) or 1.0)),
        jitter_seconds=max(0.0, float(getattr(args, "jitter_seconds", 0.35) or 0.35)),
        lease_seconds=max(5.0, float(getattr(args, "lease_seconds", 30.0) or 30.0)),
        heartbeat_seconds=max(1.0, float(getattr(args, "heartbeat_seconds", 5.0) or 5.0)),
        cancel_grace_seconds=max(1.0, float(getattr(args, "cancel_grace_seconds", 8.0) or 8.0)),
        recovery_interval_seconds=max(3.0, float(getattr(args, "recovery_interval_seconds", 15.0) or 15.0)),
        admission_policy=policy,
        max_jobs=max_jobs,
    )
    print(json.dumps({"processed": processed, "queue_db": str(queue_db)}, sort_keys=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="seo_audit")
    sub = parser.add_subparsers(dest="command", required=True)

    audit = sub.add_parser("audit", help="Run audit")
    audit.add_argument("--domain", required=True)
    audit.add_argument("--output", default="./out")
    audit.add_argument("--fresh-output-dir", dest="fresh_output_dir", action="store_true", default=False)
    audit.add_argument("--reuse-output-dir", dest="fresh_output_dir", action="store_false")
    audit.add_argument("--run-profile", choices=["exploratory", "standard", "deep"], default="standard")
    audit.add_argument("--site-type", choices=["general", "local"], default="general")
    audit.add_argument("--scoring-profile", default="")
    audit.add_argument("--max-pages", type=int, default=None)
    audit.add_argument("--max-render-pages", type=int, default=None)
    audit.add_argument("--render-mode", choices=["none", "sample", "all"], default=None)
    audit.add_argument("--crawl-discovery-mode", choices=["raw", "hybrid", "browser_first"], default=None)
    audit.add_argument("--scope-mode", choices=["host_only", "apex_www", "all_subdomains", "custom_allowlist"], default=None)
    audit.add_argument("--scope-allowlist", default="")
    audit.add_argument("--crawl-frontier-enabled", dest="crawl_frontier_enabled", action="store_true", default=True)
    audit.add_argument("--crawl-frontier-disabled", dest="crawl_frontier_enabled", action="store_false")
    audit.add_argument("--crawl-frontier-cluster-budget", type=int, default=3)
    audit.add_argument("--crawl-workers", type=int, default=1)
    audit.add_argument("--crawl-queue-high-weight", type=int, default=3)
    audit.add_argument("--crawl-queue-normal-weight", type=int, default=2)
    audit.add_argument("--per-host-rate-limit-rps", type=float, default=4.0)
    audit.add_argument("--per-host-burst-capacity", type=int, default=4)
    audit.add_argument("--incremental-crawl-enabled", dest="incremental_crawl_enabled", action="store_true", default=False)
    audit.add_argument("--incremental-crawl-disabled", dest="incremental_crawl_enabled", action="store_false")
    audit.add_argument("--render-frontier-enabled", dest="render_frontier_enabled", action="store_true", default=True)
    audit.add_argument("--render-frontier-disabled", dest="render_frontier_enabled", action="store_false")
    audit.add_argument("--render-interaction-budget", type=int, default=None)
    audit.add_argument("--render-wait-ladder-ms", default="")
    audit.add_argument("--render-mobile-first", dest="render_mobile_first", action="store_true", default=True)
    audit.add_argument("--render-desktop-first", dest="render_mobile_first", action="store_false")
    audit.add_argument("--render-mobile-viewport", default="390x844x2,mobile,touch")
    audit.add_argument("--render-desktop-viewport", default="1440x900x1")
    audit.add_argument("--faceted-sample-rate", type=float, default=None)
    audit.add_argument("--faceted-query-param-threshold", type=int, default=2)
    audit.add_argument("--faceted-param-keys", default="")
    audit.add_argument("--action-param-keys", default="")
    audit.add_argument("--diagnostic-param-keys", default="")
    audit.add_argument("--headers-only-param-keys", default="")
    audit.add_argument("--canonical-candidate-param-keys", default="")
    audit.add_argument("--url-policy-enabled", dest="url_policy_enabled", action="store_true", default=True)
    audit.add_argument("--url-policy-disabled", dest="url_policy_enabled", action="store_false")
    audit.add_argument("--platform-detection-enabled", dest="platform_detection_enabled", action="store_true", default=True)
    audit.add_argument("--platform-detection-disabled", dest="platform_detection_enabled", action="store_false")
    audit.add_argument("--citation-measurement-enabled", dest="citation_measurement_enabled", action="store_true", default=True)
    audit.add_argument("--citation-measurement-disabled", dest="citation_measurement_enabled", action="store_false")
    audit.add_argument("--timeout", type=float, default=10.0)
    audit.add_argument("--crawl-persona", choices=sorted(PERSONAS.keys()), default="googlebot_smartphone")
    audit.add_argument("--robots-persona-mode", choices=["google_exact", "generic"], default=None)
    audit.add_argument(
        "--google-exact-apply-crawl-delay",
        dest="google_exact_apply_crawl_delay",
        action="store_true",
        default=False,
    )
    audit.add_argument(
        "--google-exact-ignore-crawl-delay",
        dest="google_exact_apply_crawl_delay",
        action="store_false",
    )
    audit.add_argument("--user-agent", default="")
    audit.add_argument("--crawl-retries", type=int, default=1)
    audit.add_argument("--crawl-base-backoff-seconds", type=float, default=0.25)
    audit.add_argument("--crawl-max-backoff-seconds", type=float, default=4.0)
    audit.add_argument("--crawl-max-total-wait-seconds", type=float, default=12.0)
    audit.add_argument("--crawl-respect-retry-after", dest="crawl_respect_retry_after", action="store_true", default=True)
    audit.add_argument("--crawl-ignore-retry-after", dest="crawl_respect_retry_after", action="store_false")
    audit.add_argument("--max-response-bytes", type=int, default=2_000_000)
    audit.add_argument("--max-non-html-bytes", type=int, default=262_144)
    audit.add_argument("--ignore-robots", action="store_true")
    audit.add_argument("--i-understand-robots-bypass", action="store_true")
    audit.add_argument("--save-html", action="store_true")
    audit.add_argument("--performance-targets", type=int, default=None)
    audit.add_argument("--crawl-heartbeat-every-pages", type=int, default=None)
    audit.add_argument("--store-provider-payloads", dest="store_provider_payloads", action="store_true", default=False)
    audit.add_argument("--no-store-provider-payloads", dest="store_provider_payloads", action="store_false")
    audit.add_argument("--payload-retention-days", type=int, default=30)
    audit.add_argument("--provider-max-retries", type=int, default=None)
    audit.add_argument("--provider-base-backoff-seconds", type=float, default=None)
    audit.add_argument("--provider-max-backoff-seconds", type=float, default=None)
    audit.add_argument("--provider-respect-retry-after", dest="provider_respect_retry_after", action="store_true", default=True)
    audit.add_argument("--provider-ignore-retry-after", dest="provider_respect_retry_after", action="store_false")
    audit.add_argument("--provider-max-total-wait-seconds", type=float, default=None)
    audit.add_argument("--psi-workers", type=int, default=4)
    audit.add_argument("--provider-rate-limit-rps", type=float, default=4.0)
    audit.add_argument("--provider-rate-limit-capacity", type=int, default=4)
    audit.add_argument("--lighthouse-enabled", dest="lighthouse_enabled", action="store_true", default=False)
    audit.add_argument("--lighthouse-disabled", dest="lighthouse_enabled", action="store_false")
    audit.add_argument("--lighthouse-targets", type=int, default=3)
    audit.add_argument("--lighthouse-timeout-seconds", type=float, default=90.0)
    audit.add_argument("--lighthouse-form-factor", choices=["desktop", "mobile"], default="desktop")
    audit.add_argument("--lighthouse-config-path", default="")
    audit.add_argument("--lighthouse-budget-performance-min", type=int, default=70)
    audit.add_argument("--lighthouse-budget-seo-min", type=int, default=70)
    audit.add_argument("--psi-enabled", dest="psi_enabled", action="store_true", default=True)
    audit.add_argument("--psi-disabled", dest="psi_enabled", action="store_false")
    audit.add_argument("--crux-enabled", dest="crux_enabled", action="store_true", default=True)
    audit.add_argument("--crux-disabled", dest="crux_enabled", action="store_false")
    audit.add_argument("--crux-origin-fallback", dest="crux_origin_fallback", action="store_true", default=True)
    audit.add_argument("--no-crux-origin-fallback", dest="crux_origin_fallback", action="store_false")
    audit.add_argument("--gsc-enabled", dest="gsc_enabled", action="store_true", default=False)
    audit.add_argument("--gsc-disabled", dest="gsc_enabled", action="store_false")
    audit.add_argument("--gsc-property", default="")
    audit.add_argument("--gsc-credentials-json", default="")
    audit.add_argument("--gsc-url-limit", type=int, default=200)
    audit.add_argument("--gsc-analytics-enabled", dest="gsc_analytics_enabled", action="store_true", default=False)
    audit.add_argument("--gsc-analytics-disabled", dest="gsc_analytics_enabled", action="store_false")
    audit.add_argument("--gsc-analytics-days", type=int, default=28)
    audit.add_argument("--gsc-analytics-row-limit", type=int, default=5000)
    audit.add_argument("--gsc-analytics-dimensions", default="page,query,device,country,date")
    audit.add_argument("--offsite-commoncrawl-enabled", dest="offsite_commoncrawl_enabled", action="store_true", default=False)
    audit.add_argument("--offsite-commoncrawl-disabled", dest="offsite_commoncrawl_enabled", action="store_false")
    audit.add_argument("--offsite-commoncrawl-mode", choices=["ranks", "domains"], default="ranks")
    audit.add_argument(
        "--offsite-commoncrawl-mode-verify",
        dest="offsite_commoncrawl_mode",
        action="store_const",
        const="verify",
        help=argparse.SUPPRESS,
    )
    audit.add_argument(
        "--offsite-commoncrawl-experimental-verify",
        dest="offsite_commoncrawl_experimental_verify",
        action="store_true",
        default=False,
        help=argparse.SUPPRESS,
    )
    audit.add_argument(
        "--offsite-commoncrawl-schedule",
        default=OFFSITE_COMMONCRAWL_DEFAULT_SCHEDULE,
        help=(
            "Offsite concurrency schedule: concurrent_best_effort|background_wait|blocking "
            "(legacy alias background_best_effort is accepted)."
        ),
    )
    audit.add_argument("--offsite-commoncrawl-release", default="auto")
    audit.add_argument("--offsite-commoncrawl-cache-dir", default="~/.cache/seo_audit/commoncrawl")
    audit.add_argument("--offsite-commoncrawl-max-linking-domains", type=int, default=100)
    audit.add_argument("--offsite-commoncrawl-join-budget-seconds", type=float, default=0.5)
    audit.add_argument("--offsite-commoncrawl-time-budget-seconds", type=int, default=180)
    audit.add_argument(
        "--offsite-commoncrawl-allow-cold-edge-download",
        dest="offsite_commoncrawl_allow_cold_edge_download",
        action="store_true",
        default=False,
    )
    audit.add_argument(
        "--offsite-compare-domain",
        dest="offsite_compare_domains",
        action="append",
        default=[],
    )
    audit.add_argument("--verbose", action="store_true")

    dashboard = sub.add_parser("dashboard", help="Launch interactive dashboard")
    dashboard.add_argument("--db", default="./out/audit.sqlite")
    dashboard.add_argument("--queue-db", default="")
    dashboard.add_argument("--dashboard-worker-enabled", dest="dashboard_worker_enabled", action="store_true", default=True)
    dashboard.add_argument("--dashboard-worker-disabled", dest="dashboard_worker_enabled", action="store_false")
    dashboard.add_argument("--host", default="127.0.0.1")
    dashboard.add_argument("--port", type=int, default=8765)

    enqueue = sub.add_parser("enqueue", help="Queue an audit job")
    enqueue.add_argument("--domain", required=True)
    enqueue.add_argument("--output", default="./out")
    enqueue.add_argument("--run-profile", choices=["exploratory", "standard", "deep"], default="standard")
    enqueue.add_argument("--max-pages", type=int, default=None)
    enqueue.add_argument("--render-mode", choices=["none", "sample", "all"], default=None)
    enqueue.add_argument("--max-render-pages", type=int, default=None)
    enqueue.add_argument("--performance-targets", type=int, default=None)
    enqueue.add_argument("--screenshot-count", type=int, default=0)
    enqueue.add_argument("--offsite-commoncrawl-enabled", dest="offsite_commoncrawl_enabled", action="store_true", default=False)
    enqueue.add_argument("--lighthouse-enabled", dest="lighthouse_enabled", action="store_true", default=False)
    enqueue.add_argument("--queue-db", default="./out/queue.sqlite")
    enqueue.add_argument("--priority", type=int, default=0)
    enqueue.add_argument("--max-attempts", type=int, default=2)
    enqueue.add_argument("--dedupe-key", default="")

    jobs_cmd = sub.add_parser("jobs", help="List queued jobs")
    jobs_cmd.add_argument("--queue-db", default="./out/queue.sqlite")
    jobs_cmd.add_argument("--state", default="")
    jobs_cmd.add_argument("--limit", type=int, default=100)

    cancel_cmd = sub.add_parser("cancel", help="Request queue job cancellation")
    cancel_cmd.add_argument("job_id")
    cancel_cmd.add_argument("--queue-db", default="./out/queue.sqlite")

    worker_cmd = sub.add_parser("worker", help="Process queued audit jobs")
    worker_cmd.add_argument("--queue-db", default="./out/queue.sqlite")
    worker_cmd.add_argument("--worker-id", default="")
    worker_cmd.add_argument("--poll-seconds", type=float, default=1.0)
    worker_cmd.add_argument("--jitter-seconds", type=float, default=0.35)
    worker_cmd.add_argument("--lease-seconds", type=float, default=30.0)
    worker_cmd.add_argument("--heartbeat-seconds", type=float, default=5.0)
    worker_cmd.add_argument("--cancel-grace-seconds", type=float, default=8.0)
    worker_cmd.add_argument("--recovery-interval-seconds", type=float, default=15.0)
    worker_cmd.add_argument("--total-token-budget", type=int, default=6)
    worker_cmd.add_argument("--max-render-heavy-jobs", type=int, default=1)
    worker_cmd.add_argument("--max-provider-heavy-jobs", type=int, default=1)
    worker_cmd.add_argument("--max-offsite-heavy-jobs", type=int, default=1)
    worker_cmd.add_argument("--allow-concurrent-same-domain", action="store_true", default=False)
    worker_cmd.add_argument("--max-jobs", type=int, default=0)
    worker_cmd.add_argument("--once", action="store_true", default=False)

    sub.add_parser("version", help="Print version")

    return parser


def main() -> None:
    parser = build_parser()

    args = parser.parse_args()
    if args.command == "audit":
        try:
            _validate_robots_bypass_flags(args)
        except ValueError as exc:
            parser.error(str(exc))
        run_audit(args)
    elif args.command == "enqueue":
        run_enqueue(args)
    elif args.command == "jobs":
        run_jobs_command(args)
    elif args.command == "cancel":
        run_cancel_command(args)
    elif args.command == "worker":
        run_worker_command(args)
    elif args.command == "dashboard":
        from seo_audit.dashboard import run_dashboard

        queue_db = None
        if str(args.queue_db or "").strip():
            queue_db = _queue_db_path(str(args.queue_db))
        run_dashboard(
            Path(args.db),
            host=args.host,
            port=args.port,
            queue_db_path=queue_db,
            start_worker=bool(args.dashboard_worker_enabled),
        )
    else:
        print("seo-audit 0.1.0")
