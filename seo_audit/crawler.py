from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import hashlib
import json
import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from contextlib import nullcontext
from collections import deque
from collections.abc import Callable

from seo_audit.config import AuditConfig
from seo_audit.crawl_queue import ThreeQueueFrontier
from seo_audit.extract import extract_page_data
from seo_audit.frontier import FrontierItem, PriorityFrontier, cluster_key_for_param_url, compute_frontier_priority, signals_for_url
from seo_audit.http_utils import build_conditional_headers, http_get, http_head
from seo_audit.models import ArtifactCacheRecord, CrawlFetchRecord, CrawlResult, LinkRecord, PageRecord, PageSnapshotRecord, URLStateRecord
from seo_audit.platforms import detect_platform_stack
from seo_audit.render import PlaywrightRenderer, score_render_escalation
from seo_audit.robots import RobotsData, is_allowed, resolve_crawl_delay
from seo_audit.scheduler import HostTokenScheduler
from seo_audit.shell_detection import classify_raw_html_sufficiency
from seo_audit.scoring_policy import CURRENT_SCORE_VERSION
from seo_audit.storage import Storage
from seo_audit.url_policy import URLPolicyDecision, classify_url_policy
from seo_audit.url_utils import is_internal_url, normalize_url, should_skip_asset


CURRENT_EXTRACTOR_VERSION = "2.0.0"
CURRENT_SCHEMA_RULE_VERSION = "1.0.0"
PAGE_EXTRACT_ARTIFACT_TYPE = "page_extract"
CRAWL_RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}


def _url_key(url: str) -> str:
    return hashlib.sha256(str(url).strip().encode("utf-8")).hexdigest()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _artifact_sha(body_sha256: str, artifact_type: str, extractor_version: str) -> str:
    token = f"{body_sha256}|{artifact_type}|{extractor_version}"
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _json_object(raw: str) -> dict:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _apply_extracted_payload(page: PageRecord, data: dict) -> list[dict]:
    for key, value in data.items():
        if hasattr(page, key):
            setattr(page, key, value)
    page.raw_title = str(data.get("raw_title") or page.title or "")
    page.raw_meta_description = str(data.get("raw_meta_description") or page.meta_description or "")
    page.raw_canonical = str(data.get("raw_canonical") or page.canonical_url or "")
    page.raw_canonical_urls_json = str(data.get("raw_canonical_urls_json") or page.canonical_urls_json or "[]")
    page.raw_hreflang_links_json = str(data.get("raw_hreflang_links_json") or page.hreflang_links_json or "[]")
    page.raw_h1_count = int(data.get("h1_count", int(bool(page.h1))))
    page.raw_text_len = int(page.word_count or 0)
    anchors = list(data.get("anchors", [])) if isinstance(data.get("anchors", []), list) else []
    page.raw_links_json = json.dumps(anchors)
    page.raw_content_hash = str(data.get("raw_content_hash") or data.get("content_hash") or page.content_hash or "")
    page.content_hash = page.raw_content_hash
    page.effective_title = page.raw_title
    page.effective_meta_description = page.raw_meta_description
    page.effective_canonical = page.raw_canonical
    page.effective_hreflang_links_json = page.raw_hreflang_links_json
    page.effective_content_hash = page.raw_content_hash
    page.effective_field_provenance_json = json.dumps(
        {
            "canonical": "raw",
            "content_hash": "raw",
            "hreflang": "raw",
            "meta_description": "raw",
            "title": "raw",
        },
        sort_keys=True,
    )
    page.effective_h1_count = page.raw_h1_count
    page.effective_text_len = page.raw_text_len
    page.effective_links_json = page.raw_links_json
    return anchors


def _is_html_response(content_type: str, body: bytes) -> bool:
    if "html" in content_type.lower():
        return True
    probe = body[:512].decode("utf-8", errors="ignore").lower()
    return "<html" in probe or "<!doctype html" in probe


def _parse_retry_after(value: str | None) -> float | None:
    raw = str(value or "").strip()
    if not raw:
        return None

    try:
        seconds = float(raw)
        return seconds if seconds >= 0 else None
    except ValueError:
        pass

    try:
        dt = parsedate_to_datetime(raw)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    wait_seconds = (dt - datetime.now(timezone.utc)).total_seconds()
    return wait_seconds if wait_seconds > 0 else 0.0


def _compute_retry_wait_seconds(
    *,
    retry_index: int,
    retry_after_header: str | None,
    config: AuditConfig,
    total_wait_seconds: float,
) -> tuple[float | None, bool]:
    retry_after_seconds = _parse_retry_after(retry_after_header) if config.crawl_respect_retry_after else None
    used_retry_after = retry_after_seconds is not None

    if used_retry_after:
        wait_seconds = float(retry_after_seconds)
    else:
        backoff = min(
            float(config.crawl_max_backoff_seconds),
            float(config.crawl_base_backoff_seconds) * (2**retry_index),
        )
        jitter = random.uniform(0.0, min(0.25, backoff * 0.25))
        wait_seconds = min(float(config.crawl_max_backoff_seconds), backoff + jitter)

    remaining_budget = float(config.crawl_max_total_wait_seconds) - float(total_wait_seconds)
    if remaining_budget <= 0:
        return None, used_retry_after

    wait_seconds = min(wait_seconds, remaining_budget)
    if wait_seconds <= 0:
        return None, used_retry_after
    return wait_seconds, used_retry_after


def _effective_request_delay(
    config: AuditConfig,
    robots_data: RobotsData | None,
    robots_user_agent_token: str,
) -> float:
    if not config.respect_robots:
        return config.request_delay
    robots_persona_mode = str(
        (getattr(robots_data, "persona_mode", "") if robots_data is not None else "")
        or config.robots_persona_mode
        or "generic"
    ).strip().lower()
    crawl_delay = resolve_crawl_delay(
        robots_data,
        robots_user_agent_token,
        persona_mode=robots_persona_mode,
        apply_for_google_exact=bool(config.google_exact_apply_crawl_delay),
    )
    if crawl_delay is None:
        return config.request_delay
    return max(config.request_delay, crawl_delay)


def crawl_site(
    config: AuditConfig,
    run_id: str,
    robots_data: RobotsData | None,
    start_urls: list[str],
    on_heartbeat: Callable[[dict], None] | None = None,
    heartbeat_every_pages: int = 25,
    *,
    storage: Storage | None = None,
    extractor_version: str = CURRENT_EXTRACTOR_VERSION,
    schema_rule_version: str = CURRENT_SCHEMA_RULE_VERSION,
    scoring_version: str = CURRENT_SCORE_VERSION,
) -> CrawlResult:
    root = normalize_url(config.domain)
    request_user_agent = str(config.user_agent or "").strip()
    request_headers = {"User-Agent": request_user_agent} if request_user_agent else {}
    robots_user_agent_token = str(config.robots_user_agent_token or request_user_agent or "*").strip()
    robots_persona_mode = str(
        (getattr(robots_data, "persona_mode", "") if robots_data is not None else "")
        or config.robots_persona_mode
        or "generic"
    ).strip().lower() or "generic"
    meta_robot_scope = str(config.meta_robot_scope or "generic").strip().lower()
    crawler_controls_token = meta_robot_scope if meta_robot_scope not in {"generic", "robots", "*"} else ""
    q: deque[tuple[str, int, URLPolicyDecision, str, str, float, str, int]] = deque()
    frontier: ThreeQueueFrontier | PriorityFrontier | None = None
    seen: set[str] = set()
    enqueued: set[str] = set()
    discovery_sources: dict[str, set[str]] = {}
    discovered_from: dict[str, str] = {}
    cluster_rank_counts: dict[str, int] = {}
    result = CrawlResult()
    request_delay = _effective_request_delay(config, robots_data, robots_user_agent_token)
    host_scheduler = HostTokenScheduler(
        default_rate_per_second=max(0.1, float(config.per_host_rate_limit_rps)),
        default_capacity=max(1, int(config.per_host_burst_capacity)),
        min_request_delay_seconds=request_delay,
    )
    crawl_started = time.perf_counter()
    scope_mode = str(config.scope_mode or "apex_www")
    scope_allowlist = tuple(config.scope_allowlist or ())
    risky_query_keys = {
        str(key).strip().lower()
        for key in (
            *tuple(config.faceted_param_keys or ()),
            *tuple(config.action_param_keys or ()),
            *tuple(config.diagnostic_param_keys or ()),
            *tuple(config.headers_only_param_keys or ()),
            *tuple(config.canonical_candidate_param_keys or ()),
        )
        if str(key).strip()
    }
    if bool(config.crawl_frontier_enabled):
        frontier = ThreeQueueFrontier(
            max_size=max(100, int(config.max_pages) * 3),
            cluster_budget=max(1, int(config.crawl_frontier_cluster_budget)),
            high_weight=max(1, int(getattr(config, "crawl_queue_high_weight", 3) or 3)),
            normal_weight=max(1, int(getattr(config, "crawl_queue_normal_weight", 2) or 2)),
        )
    discovery_mode = str(config.crawl_discovery_mode or "raw").lower()
    render_frontier_enabled = (
        bool(config.render_frontier_enabled)
        and discovery_mode in {"hybrid", "browser_first"}
        and int(config.max_render_pages) > 0
    )
    render_escalation_threshold = 35
    render_frontier_checks = 0
    cache_extractor_version = (
        f"extract:{extractor_version}|schema:{schema_rule_version}|scoring:{scoring_version}"
    )
    incremental_enabled = bool(config.incremental_crawl_enabled and storage is not None)
    url_state_updates: list[URLStateRecord] = []
    artifact_updates: list[ArtifactCacheRecord] = []

    stats: dict[str, int] = {
        "enqueue_attempts": 0,
        "enqueued_total": 0,
        "enqueued_band_high": 0,
        "enqueued_band_normal": 0,
        "enqueued_band_low": 0,
        "scope_skipped": 0,
        "policy_skipped": 0,
        "dedupe_skipped": 0,
        "seen_skipped": 0,
        "asset_skipped": 0,
        "queue_cap_skipped": 0,
        "render_frontier_checks": 0,
        "render_frontier_successes": 0,
        "render_frontier_failures": 0,
        "fetched": 0,
        "fetch_retries_total": 0,
        "fetch_retry_wait_ms_total": 0,
        "fetch_retry_after_used": 0,
        "fetch_retryable_status_total": 0,
        "fetch_network_retries_total": 0,
        "fetch_retry_budget_exhausted": 0,
        "reused_from_cache": 0,
        "not_modified": 0,
        "reparsed": 0,
        "rerendered": 0,
        "crawl_workers_used": 1,
    }

    def stat_inc(key: str, delta: int = 1) -> None:
        stats[key] = stats.get(key, 0) + delta

    def via_key(discovered_via: str) -> str:
        token = "".join(ch if ch.isalnum() else "_" for ch in discovered_via.strip().lower())
        return token or "unknown"

    def queue_size() -> int:
        if frontier is not None:
            return len(frontier)
        return len(q)

    def pop_next() -> tuple[str, int, URLPolicyDecision, str, str, float, str, int] | None:
        if frontier is not None:
            item = frontier.pop()
            if item is None:
                return None
            payload = item.payload
            decision = payload.get("policy_decision")
            if not isinstance(decision, URLPolicyDecision):
                decision = classify_url_policy(item.url, config)
            return (
                item.url,
                item.depth,
                decision,
                item.discovered_via,
                item.source_url,
                float(item.priority),
                item.cluster_key,
                int(payload.get("cluster_rank") or 0),
            )
        if not q:
            return None
        return q.popleft()

    def queue_band_for_candidate(policy_decision: URLPolicyDecision, *, discovered_via: str) -> str:
        policy_class = str(policy_decision.policy_class or "").strip().lower()
        discovered = str(discovered_via or "").strip().lower()

        if policy_decision.fetch_headers_only or policy_class in {"crawl_once_diagnostic"}:
            return "low"
        if policy_class in {"crawl_sampled", "canonical_candidate_duplicate"}:
            return "normal"
        if discovered in {"seed", "raw_link"}:
            return "high"
        return "normal"

    def enqueue_candidate(target_url: str, next_depth: int, *, discovered_via: str, source_url: str = "") -> None:
        stat_inc("enqueue_attempts")
        normalized_target = normalize_url(target_url, base_url=root)
        discovery_sources.setdefault(normalized_target, set()).add(discovered_via)
        if source_url and normalized_target not in discovered_from:
            discovered_from[normalized_target] = source_url

        if not is_internal_url(
            normalized_target,
            root,
            base_url=source_url or root,
            scope_mode=scope_mode,
            custom_allowlist=scope_allowlist,
        ):
            stat_inc("scope_skipped")
            return
        if should_skip_asset(normalized_target):
            stat_inc("asset_skipped")
            return
        if normalized_target in seen:
            stat_inc("seen_skipped")
            return
        if normalized_target in enqueued:
            stat_inc("dedupe_skipped")
            return
        decision = classify_url_policy(normalized_target, config)
        if not decision.enqueue:
            stat_inc("policy_skipped")
            return
        if len(result.pages) + queue_size() >= config.max_pages * 3:
            stat_inc("queue_cap_skipped")
            return

        frontier_signals = signals_for_url(
            url=normalized_target,
            depth=next_depth,
            discovered_via=discovered_via,
            policy_class=decision.policy_class,
            risky_query_keys=risky_query_keys,
        )
        frontier_priority = compute_frontier_priority(frontier_signals)
        frontier_cluster_key = cluster_key_for_param_url(normalized_target, risky_query_keys)
        cluster_rank = cluster_rank_counts.get(frontier_cluster_key, 0) + 1
        cluster_rank_counts[frontier_cluster_key] = cluster_rank
        queue_band = queue_band_for_candidate(decision, discovered_via=discovered_via)

        inserted = False
        if isinstance(frontier, ThreeQueueFrontier):
            inserted = frontier.push(
                FrontierItem(
                    url=normalized_target,
                    depth=next_depth,
                    priority=frontier_priority,
                    discovered_via=discovered_via,
                    source_url=source_url,
                    cluster_key=frontier_cluster_key,
                    payload={
                        "policy_decision": decision,
                        "cluster_rank": cluster_rank,
                    },
                ),
                band=queue_band,
            )
        elif frontier is not None:
            inserted = frontier.push(
                FrontierItem(
                    url=normalized_target,
                    depth=next_depth,
                    priority=frontier_priority,
                    discovered_via=discovered_via,
                    source_url=source_url,
                    cluster_key=frontier_cluster_key,
                    payload={
                        "policy_decision": decision,
                        "cluster_rank": cluster_rank,
                    },
                )
            )
        else:
            q.append(
                (
                    normalized_target,
                    next_depth,
                    decision,
                    discovered_via,
                    source_url,
                    frontier_priority,
                    frontier_cluster_key,
                    cluster_rank,
                )
            )
            inserted = True

        if not inserted:
            stat_inc("queue_cap_skipped")
            return

        enqueued.add(normalized_target)
        stat_inc("enqueued_total")
        stat_inc(f"enqueued_band_{queue_band}")
        stat_inc(f"enqueued_via_{via_key(discovered_via)}")

    for start_url in start_urls:
        enqueue_candidate(start_url, 0, discovered_via="seed")

    def maybe_emit_heartbeat(current_url: str, depth: int) -> None:
        if on_heartbeat is None or heartbeat_every_pages <= 0:
            return
        pages_stored = len(result.pages)
        if pages_stored == 0 or pages_stored % heartbeat_every_pages != 0:
            return
        on_heartbeat(
            {
                "pages_stored": pages_stored,
                "queue_size": queue_size(),
                "seen_count": len(seen),
                "error_count": len(result.errors),
                "crawl_elapsed_ms": int((time.perf_counter() - crawl_started) * 1000),
                "last_url": current_url,
                "last_depth": depth,
                "request_delay_s": request_delay,
                "enqueued_total": stats.get("enqueued_total", 0),
                "dedupe_skipped": stats.get("dedupe_skipped", 0),
                "fetched": stats.get("fetched", 0),
                "reused_from_cache": stats.get("reused_from_cache", 0),
                "not_modified": stats.get("not_modified", 0),
                "fetch_retries_total": stats.get("fetch_retries_total", 0),
                "fetch_retry_wait_ms_total": stats.get("fetch_retry_wait_ms_total", 0),
            }
        )

    def apply_cached_artifact(page: PageRecord, artifact_json: str) -> list[dict]:
        payload = _json_object(artifact_json)
        extract_data = payload.get("extract_data")
        anchors: list[dict] = []
        if isinstance(extract_data, dict):
            anchors = _apply_extracted_payload(page, extract_data)

        shell_payload = payload.get("shell")
        if isinstance(shell_payload, dict):
            page.shell_score = int(shell_payload.get("shell_score") or 0)
            page.likely_js_shell = int(shell_payload.get("likely_js_shell") or 0)
            page.shell_state = str(shell_payload.get("shell_state") or ("raw_shell_possible" if page.likely_js_shell else "raw_shell_unlikely"))
            page.framework_guess = str(shell_payload.get("framework_guess") or "")
            page.shell_signals_json = str(shell_payload.get("shell_signals_json") or "{}")
            page.render_reason = str(shell_payload.get("render_reason") or "")

        platform_payload = payload.get("platform")
        if isinstance(platform_payload, dict):
            page.platform_family = str(platform_payload.get("platform_family") or "")
            page.platform_confidence = int(platform_payload.get("platform_confidence") or 0)
            page.platform_signals_json = str(platform_payload.get("platform_signals_json") or "{}")
            page.platform_template_hint = str(platform_payload.get("platform_template_hint") or "")

        return anchors

    renderer_context = (
        PlaywrightRenderer(
            timeout=config.timeout + 5,
            user_agent=request_user_agent,
            crawler_token=crawler_controls_token,
            wait_ladder_ms=config.render_wait_ladder_ms,
            interaction_budget=config.render_interaction_budget,
        )
        if render_frontier_enabled
        else nullcontext(None)
    )

    def fetch_candidate_response(
        normalized: str,
        policy_decision: URLPolicyDecision,
        request_headers_for_url: dict[str, str],
    ) -> tuple[object | None, Exception | None, int, dict[str, object]]:
        last_exc: Exception | None = None
        resp = None
        retries_used = 0
        total_retry_wait_seconds = 0.0
        retry_after_uses = 0
        retryable_status_retries = 0
        network_retries = 0
        retry_budget_exhausted = 0
        attempts = 0
        max_attempts = max(1, int(config.retries) + 1)
        last_status_code = 0

        started = time.perf_counter()
        while attempts < max_attempts:
            attempts += 1
            host_scheduler.acquire(normalized)
            try:
                if policy_decision.fetch_headers_only:
                    resp = http_head(normalized, timeout=config.timeout, headers=request_headers_for_url)
                else:
                    resp = http_get(
                        normalized,
                        timeout=config.timeout,
                        headers=request_headers_for_url,
                        max_bytes=config.max_response_bytes,
                        max_non_html_bytes=config.max_non_html_bytes,
                    )
                status_code = int(getattr(resp, "status_code", 0) or 0)
                last_status_code = status_code

                if status_code in CRAWL_RETRYABLE_STATUS_CODES and attempts < max_attempts:
                    wait_seconds, used_retry_after = _compute_retry_wait_seconds(
                        retry_index=retries_used,
                        retry_after_header=str(getattr(resp, "headers", {}).get("retry-after", "") or ""),
                        config=config,
                        total_wait_seconds=total_retry_wait_seconds,
                    )
                    if wait_seconds is None:
                        retry_budget_exhausted = 1
                        break
                    time.sleep(wait_seconds)
                    retries_used += 1
                    total_retry_wait_seconds += wait_seconds
                    retryable_status_retries += 1
                    if used_retry_after:
                        retry_after_uses += 1
                    continue

                last_exc = None
                break
            except Exception as exc:
                last_exc = exc
                if attempts >= max_attempts:
                    break

                wait_seconds, _ = _compute_retry_wait_seconds(
                    retry_index=retries_used,
                    retry_after_header=None,
                    config=config,
                    total_wait_seconds=total_retry_wait_seconds,
                )
                if wait_seconds is None:
                    retry_budget_exhausted = 1
                    break

                time.sleep(wait_seconds)
                retries_used += 1
                total_retry_wait_seconds += wait_seconds
                network_retries += 1

        elapsed_ms = int((time.perf_counter() - started) * 1000)
        retry_meta: dict[str, object] = {
            "attempts": attempts,
            "retries_used": retries_used,
            "retry_wait_ms": int(total_retry_wait_seconds * 1000),
            "retry_after_uses": retry_after_uses,
            "retryable_status_retries": retryable_status_retries,
            "network_retries": network_retries,
            "retry_budget_exhausted": retry_budget_exhausted,
            "last_status_code": last_status_code,
        }
        return resp, last_exc, elapsed_ms, retry_meta

    with renderer_context as renderer:
        def prepare_candidate(
            popped: tuple[str, int, URLPolicyDecision, str, str, float, str, int],
        ) -> dict[str, object] | None:
            (
                url,
                depth,
                policy_decision,
                discovered_via,
                source_url,
                frontier_priority,
                frontier_cluster_key,
                frontier_cluster_rank,
            ) = popped
            normalized = normalize_url(url, base_url=root)
            enqueued.discard(normalized)

            if should_skip_asset(normalized):
                stat_inc("asset_skipped")
                return None
            if normalized in seen:
                stat_inc("seen_skipped")
                return None

            seen.add(normalized)
            combined_sources = sorted(discovery_sources.get(normalized, {discovered_via}))
            discovery_source = ",".join(combined_sources)
            source_parent = discovered_from.get(normalized, source_url)

            blocked_by_robots = bool(
                robots_data
                and not is_allowed(
                    robots_data,
                    robots_user_agent_token,
                    normalized,
                    persona_mode=robots_persona_mode,
                )
            )
            if config.respect_robots and blocked_by_robots:
                blocked_page = PageRecord(
                    run_id=run_id,
                    discovered_url=url,
                    normalized_url=normalized,
                    discovered_via=discovery_source,
                    discovered_from_url=source_parent,
                    robots_blocked_flag=1,
                    is_indexable=0,
                    crawl_depth=depth,
                    crawl_policy_class=policy_decision.policy_class,
                    crawl_policy_reason=policy_decision.reason,
                    frontier_priority_score=frontier_priority,
                    frontier_cluster_key=frontier_cluster_key,
                    frontier_cluster_rank=frontier_cluster_rank,
                )
                result.pages.append(blocked_page)
                result.discovered_urls.add(blocked_page.normalized_url)
                result.crawl_depth[blocked_page.normalized_url] = depth
                result.errors.append(f"Blocked by robots (not fetched): {normalized}")
                maybe_emit_heartbeat(normalized, depth)
                return None

            url_state = storage.get_url_state(normalized) if incremental_enabled and storage is not None else None
            previous_body_sha = str((url_state or {}).get("last_body_sha256") or "").strip().lower()
            previous_not_modified_streak = int((url_state or {}).get("not_modified_streak") or 0)
            request_headers_for_url = dict(request_headers)
            if incremental_enabled and not policy_decision.fetch_headers_only and url_state is not None:
                request_headers_for_url = build_conditional_headers(
                    request_headers_for_url,
                    etag=str(url_state.get("etag") or ""),
                    last_modified=str(url_state.get("last_modified") or ""),
                )

            return {
                "url": url,
                "depth": depth,
                "policy_decision": policy_decision,
                "source_url": source_url,
                "frontier_priority": frontier_priority,
                "frontier_cluster_key": frontier_cluster_key,
                "frontier_cluster_rank": frontier_cluster_rank,
                "normalized": normalized,
                "discovery_source": discovery_source,
                "source_parent": source_parent,
                "blocked_by_robots": blocked_by_robots,
                "url_state": url_state,
                "previous_body_sha": previous_body_sha,
                "previous_not_modified_streak": previous_not_modified_streak,
                "request_headers_for_url": request_headers_for_url,
            }

        def process_fetched_candidate(
            context: dict[str, object],
            *,
            resp: object | None,
            last_exc: Exception | None,
            elapsed_ms: int,
            retry_meta: dict[str, object],
        ) -> None:
            nonlocal render_frontier_checks

            url = str(context.get("url") or "")
            depth = int(context.get("depth") or 0)
            policy_decision = context.get("policy_decision")
            if not isinstance(policy_decision, URLPolicyDecision):
                policy_decision = classify_url_policy(str(context.get("normalized") or ""), config)
            frontier_priority = float(context.get("frontier_priority") or 0.0)
            frontier_cluster_key = str(context.get("frontier_cluster_key") or "")
            frontier_cluster_rank = int(context.get("frontier_cluster_rank") or 0)
            normalized = str(context.get("normalized") or "")
            discovery_source = str(context.get("discovery_source") or "")
            source_parent = str(context.get("source_parent") or "")
            blocked_by_robots = bool(context.get("blocked_by_robots"))
            url_state = context.get("url_state") if isinstance(context.get("url_state"), dict) else None
            previous_body_sha = str(context.get("previous_body_sha") or "").strip().lower()
            previous_not_modified_streak = int(context.get("previous_not_modified_streak") or 0)
            retries_used = int(retry_meta.get("retries_used") or 0)
            retry_wait_ms = int(retry_meta.get("retry_wait_ms") or 0)
            retry_after_uses = int(retry_meta.get("retry_after_uses") or 0)
            retryable_status_retries = int(retry_meta.get("retryable_status_retries") or 0)
            network_retries = int(retry_meta.get("network_retries") or 0)
            retry_budget_exhausted = int(retry_meta.get("retry_budget_exhausted") or 0)

            if retries_used > 0:
                stat_inc("fetch_retries_total", retries_used)
            if retry_wait_ms > 0:
                stat_inc("fetch_retry_wait_ms_total", retry_wait_ms)
            if retry_after_uses > 0:
                stat_inc("fetch_retry_after_used", retry_after_uses)
            if retryable_status_retries > 0:
                stat_inc("fetch_retryable_status_total", retryable_status_retries)
            if network_retries > 0:
                stat_inc("fetch_network_retries_total", network_retries)
            if retry_budget_exhausted > 0:
                stat_inc("fetch_retry_budget_exhausted", retry_budget_exhausted)

            if last_exc or resp is None:
                result.pages.append(
                    PageRecord(
                        run_id=run_id,
                        discovered_url=url,
                        normalized_url=normalized,
                        discovered_via=discovery_source,
                        discovered_from_url=source_parent,
                        fetch_error=str(last_exc),
                        crawl_depth=depth,
                        fetch_time_ms=elapsed_ms,
                        crawl_policy_class=policy_decision.policy_class,
                        crawl_policy_reason=policy_decision.reason,
                        frontier_priority_score=frontier_priority,
                        frontier_cluster_key=frontier_cluster_key,
                        frontier_cluster_rank=frontier_cluster_rank,
                    )
                )
                result.errors.append(f"Fetch error for {normalized}: {last_exc}")
                maybe_emit_heartbeat(normalized, depth)
                return

            if not hasattr(resp, "headers"):
                result.errors.append(f"Invalid fetch response for {normalized}")
                return

            if bool(getattr(resp, "not_modified", False)):
                stat_inc("not_modified")
            else:
                stat_inc("fetched")

            ct = str(resp.headers.get("content-type", ""))
            fetched_at = datetime.now(timezone.utc).isoformat()
            effective_status = int(getattr(resp, "status_code", 0) or 0)
            final_url = normalize_url(str(getattr(resp, "url", normalized) or normalized), base_url=normalized)
            body_bytes = getattr(resp, "content", b"")
            body_sha256 = ""
            if bool(getattr(resp, "not_modified", False)) and url_state is not None:
                final_url = normalize_url(str(url_state.get("last_final_url") or final_url), base_url=normalized)
                if not ct:
                    ct = str(url_state.get("last_content_type") or "")
                previous_status = int(url_state.get("last_status_code") or 0)
                if previous_status > 0:
                    effective_status = previous_status
                body_sha256 = previous_body_sha
                if storage is not None and body_sha256:
                    cached_body = storage.read_body_blob(body_sha256)
                    if cached_body is not None:
                        body_bytes = cached_body
            elif body_bytes:
                body_sha256 = _sha256_bytes(body_bytes)

            result.fetches.append(
                CrawlFetchRecord(
                    run_id=run_id,
                    url=normalized,
                    status_code=int(getattr(resp, "status_code", 0) or 0),
                    fetch_time_ms=elapsed_ms,
                    content_type=ct,
                    response_bytes=len(body_bytes),
                    fetched_at=fetched_at,
                )
            )
            page = PageRecord(
                run_id=run_id,
                discovered_url=url,
                normalized_url=normalized,
                discovered_via=discovery_source,
                discovered_from_url=source_parent,
                final_url=final_url,
                status_code=effective_status,
                content_type=ct,
                fetch_time_ms=elapsed_ms,
                html_bytes=len(body_bytes),
                redirect_chain_json=json.dumps(getattr(resp, "redirect_chain", [])),
                last_modified=str(resp.headers.get("last-modified", "") or str((url_state or {}).get("last_modified") or "")),
                robots_blocked_flag=int(blocked_by_robots),
                crawl_policy_class=policy_decision.policy_class,
                crawl_policy_reason=policy_decision.reason,
                crawl_depth=depth,
                frontier_priority_score=frontier_priority,
                frontier_cluster_key=frontier_cluster_key,
                frontier_cluster_rank=frontier_cluster_rank,
            )

            if incremental_enabled:
                if bool(getattr(resp, "not_modified", False)):
                    page.changed_since_last_run = 0
                elif body_sha256 and previous_body_sha and body_sha256 == previous_body_sha:
                    page.changed_since_last_run = 0
                elif body_sha256:
                    page.changed_since_last_run = 1
                else:
                    page.changed_since_last_run = 0

            if storage is not None and body_sha256 and body_bytes:
                storage.ensure_body_blob(
                    body_sha256,
                    body_bytes,
                    content_encoding=str(resp.headers.get("content-encoding", "") or ""),
                )

            anchors_for_linking: list[dict] = []

            if not policy_decision.fetch_headers_only and _is_html_response(ct, body_bytes) and effective_status < 500:
                artifact_row = None
                if storage is not None and body_sha256:
                    artifact_row = storage.get_artifact_cache(
                        body_sha256,
                        PAGE_EXTRACT_ARTIFACT_TYPE,
                        cache_extractor_version,
                    )

                if incremental_enabled and page.changed_since_last_run == 0 and artifact_row is not None:
                    anchors_for_linking = apply_cached_artifact(page, str(artifact_row.get("artifact_json") or "{}"))
                    stat_inc("reused_from_cache")
                else:
                    if incremental_enabled and page.changed_since_last_run == 0:
                        stat_inc("reparsed")
                    extract_started = time.perf_counter()
                    body_text = body_bytes.decode("utf-8", errors="replace")
                    data = extract_page_data(
                        body_text,
                        page.final_url,
                        page.status_code or 0,
                        ct,
                        resp.headers,
                        header_lists=getattr(resp, "header_lists", {}),
                        crawler_token=crawler_controls_token,
                        site_root_url=root,
                        scope_mode=scope_mode,
                        custom_allowlist=scope_allowlist,
                    )
                    page.extract_time_ms = int((time.perf_counter() - extract_started) * 1000)
                    anchors_for_linking = _apply_extracted_payload(page, data)

                    shell = classify_raw_html_sufficiency(body_text, page.final_url, page.status_code or 0, ct, resp.headers)
                    page.shell_score = shell.shell_score
                    page.likely_js_shell = int(shell.likely_js_shell)
                    page.shell_state = str(shell.shell_state)
                    page.framework_guess = str(shell.signals.get("framework_guess", ""))
                    page.shell_signals_json = json.dumps(shell.signals, sort_keys=True)
                    page.render_reason = "; ".join(shell.reasons)

                    if bool(config.platform_detection_enabled):
                        detection = detect_platform_stack(body_text, resp.headers, page.final_url)
                        if detection is not None:
                            page.platform_family = detection.platform
                            page.platform_confidence = int(detection.confidence)
                            page.platform_signals_json = json.dumps(detection.signals, sort_keys=True)
                            page.platform_template_hint = detection.template_hint

                    if storage is not None and body_sha256:
                        artifact_payload = {
                            "extract_data": data,
                            "shell": {
                                "shell_score": int(page.shell_score or 0),
                                "likely_js_shell": int(page.likely_js_shell or 0),
                                "shell_state": str(page.shell_state or "raw_shell_unlikely"),
                                "framework_guess": str(page.framework_guess or ""),
                                "shell_signals_json": str(page.shell_signals_json or "{}"),
                                "render_reason": str(page.render_reason or ""),
                            },
                            "platform": {
                                "platform_family": str(page.platform_family or ""),
                                "platform_confidence": int(page.platform_confidence or 0),
                                "platform_signals_json": str(page.platform_signals_json or "{}"),
                                "platform_template_hint": str(page.platform_template_hint or ""),
                            },
                        }
                        artifact_updates.append(
                            ArtifactCacheRecord(
                                artifact_sha256=_artifact_sha(
                                    body_sha256,
                                    PAGE_EXTRACT_ARTIFACT_TYPE,
                                    cache_extractor_version,
                                ),
                                body_sha256=body_sha256,
                                extractor_version=cache_extractor_version,
                                artifact_type=PAGE_EXTRACT_ARTIFACT_TYPE,
                                artifact_json=json.dumps(artifact_payload, sort_keys=True),
                            )
                        )
            elif incremental_enabled and bool(getattr(resp, "not_modified", False)) and not body_bytes:
                page.fetch_error = "not_modified_missing_cached_body"
                result.errors.append(f"304 without reusable cache body for {normalized}")

            for anchor in anchors_for_linking:
                target_raw = str(anchor.get("href") or "")
                target = normalize_url(target_raw, base_url=page.final_url)
                is_internal = int(
                    is_internal_url(
                        target,
                        root,
                        base_url=page.final_url,
                        scope_mode=scope_mode,
                        custom_allowlist=scope_allowlist,
                    )
                )
                result.links.append(
                    LinkRecord(
                        run_id,
                        page.normalized_url,
                        target_raw,
                        target,
                        is_internal,
                        str(anchor.get("anchor_text") or ""),
                        int(anchor.get("nofollow", False)),
                        "raw_dom",
                        str(anchor.get("dom_region") or "unknown"),
                    )
                )
                if is_internal and policy_decision.follow_links:
                    enqueue_candidate(target, depth + 1, discovered_via="raw_link", source_url=page.normalized_url)

            if (
                renderer is not None
                and policy_decision.follow_links
                and render_frontier_checks < max(0, int(config.max_render_pages))
                and (not incremental_enabled or page.changed_since_last_run == 1)
            ):
                escalation_score, escalation_reasons = score_render_escalation(
                    {
                        "status_code": page.status_code,
                        "content_type": page.content_type,
                        "title": page.title,
                        "h1": page.h1,
                        "h1_count": page.raw_h1_count,
                        "canonical_url": page.canonical_url,
                        "word_count": page.raw_text_len,
                        "internal_links_out": page.internal_links_out,
                        "shell_score": page.shell_score,
                        "likely_js_shell": page.likely_js_shell,
                        "framework_guess": page.framework_guess,
                        "page_type": page.page_type,
                    }
                )
                should_render_for_frontier = False
                if discovery_mode == "browser_first":
                    should_render_for_frontier = True
                elif discovery_mode == "hybrid" and escalation_score >= render_escalation_threshold:
                    should_render_for_frontier = True

                if should_render_for_frontier:
                    render_frontier_checks += 1
                    stat_inc("render_frontier_checks")
                    rr, render_error = renderer.render(page.final_url)
                    if render_error:
                        stat_inc("render_frontier_failures")
                    elif rr is not None:
                        stat_inc("render_frontier_successes")
                        stat_inc("rerendered")
                        page.rendered_network_requests_json = json.dumps(rr.network_request_urls[:120])
                        page.rendered_api_endpoints_json = json.dumps(rr.api_endpoint_urls[:40])
                        page.rendered_wait_profile = rr.wait_profile
                        page.rendered_interaction_count = rr.interaction_count
                        page.rendered_action_recipe = rr.action_recipe
                        page.rendered_discovery_links_out = 0
                        page.render_reason = "; ".join(escalation_reasons) if escalation_reasons else page.render_reason

                        for anchor in rr.links:
                            target_raw = str((anchor or {}).get("href") or "")
                            if not target_raw:
                                continue
                            target = normalize_url(target_raw, base_url=rr.final_url or page.final_url)
                            is_internal = int(
                                is_internal_url(
                                    target,
                                    root,
                                    base_url=rr.final_url or page.final_url,
                                    scope_mode=scope_mode,
                                    custom_allowlist=scope_allowlist,
                                )
                            )
                            result.links.append(
                                LinkRecord(
                                    run_id,
                                    page.normalized_url,
                                    target_raw,
                                    target,
                                    is_internal,
                                    str((anchor or {}).get("anchor_text") or ""),
                                    int(bool((anchor or {}).get("nofollow", False))),
                                    "render_dom",
                                    str((anchor or {}).get("dom_region") or "unknown"),
                                )
                            )
                            if is_internal:
                                page.rendered_discovery_links_out += 1
                                enqueue_candidate(
                                    target,
                                    depth + 1,
                                    discovered_via="render_link",
                                    source_url=page.normalized_url,
                                )

            if storage is not None:
                if not body_sha256:
                    body_sha256 = previous_body_sha
                next_not_modified_streak = (
                    previous_not_modified_streak + 1
                    if (bool(getattr(resp, "not_modified", False)) or (incremental_enabled and page.changed_since_last_run == 0))
                    else 0
                )
                url_state_updates.append(
                    URLStateRecord(
                        url_key=_url_key(page.normalized_url),
                        normalized_url=page.normalized_url,
                        last_final_url=page.final_url,
                        etag=str(resp.headers.get("etag") or str((url_state or {}).get("etag") or "")),
                        last_modified=str(page.last_modified or ""),
                        last_status_code=int(page.status_code or 0),
                        last_content_type=str(page.content_type or ""),
                        last_body_sha256=str(body_sha256 or ""),
                        last_extracted_sha256=str(page.raw_content_hash or page.content_hash or ""),
                        last_fetched_at=fetched_at,
                        last_seen_run_id=run_id,
                        not_modified_streak=max(0, int(next_not_modified_streak)),
                    )
                )

            result.snapshots.append(
                PageSnapshotRecord(
                    run_id=run_id,
                    url=page.normalized_url,
                    content_hash=str(page.effective_content_hash or page.content_hash or ""),
                    last_modified=str(page.last_modified or ""),
                    status_code=int(page.status_code or 0),
                    changed_flag=0,
                    observed_at=fetched_at,
                    raw_content_hash=str(page.raw_content_hash or page.content_hash or ""),
                    rendered_content_hash=str(page.rendered_content_hash or ""),
                    effective_content_hash=str(page.effective_content_hash or page.content_hash or ""),
                )
            )
            result.pages.append(page)
            result.discovered_urls.add(page.normalized_url)
            result.crawl_depth[page.normalized_url] = depth
            maybe_emit_heartbeat(page.normalized_url, depth)

        crawl_workers = max(1, int(getattr(config, "crawl_workers", 1) or 1))
        stats["crawl_workers_used"] = crawl_workers

        if crawl_workers <= 1:
            while queue_size() > 0 and len(result.pages) < config.max_pages:
                popped = pop_next()
                if popped is None:
                    break
                context = prepare_candidate(popped)
                if context is None:
                    continue
                resp, last_exc, elapsed_ms, retry_meta = fetch_candidate_response(
                    str(context.get("normalized") or ""),
                    context.get("policy_decision") if isinstance(context.get("policy_decision"), URLPolicyDecision) else classify_url_policy(str(context.get("normalized") or ""), config),
                    dict(context.get("request_headers_for_url") or {}),
                )
                process_fetched_candidate(
                    context,
                    resp=resp,
                    last_exc=last_exc,
                    elapsed_ms=elapsed_ms,
                    retry_meta=retry_meta,
                )
        else:
            pending: dict[object, dict[str, object]] = {}
            with ThreadPoolExecutor(max_workers=crawl_workers) as crawl_executor:
                while (queue_size() > 0 or pending) and len(result.pages) < config.max_pages:
                    while (
                        queue_size() > 0
                        and len(pending) < crawl_workers
                        and (len(result.pages) + len(pending)) < config.max_pages
                    ):
                        popped = pop_next()
                        if popped is None:
                            break
                        context = prepare_candidate(popped)
                        if context is None:
                            continue

                        policy_decision = context.get("policy_decision")
                        if not isinstance(policy_decision, URLPolicyDecision):
                            policy_decision = classify_url_policy(str(context.get("normalized") or ""), config)

                        future = crawl_executor.submit(
                            fetch_candidate_response,
                            str(context.get("normalized") or ""),
                            policy_decision,
                            dict(context.get("request_headers_for_url") or {}),
                        )
                        pending[future] = context

                    if not pending:
                        continue

                    done, _ = wait(set(pending.keys()), return_when=FIRST_COMPLETED)
                    for future in done:
                        context = pending.pop(future)
                        try:
                            resp, last_exc, elapsed_ms, retry_meta = future.result()
                        except Exception as exc:  # pragma: no cover - unexpected future failure.
                            resp = None
                            last_exc = exc
                            elapsed_ms = 0
                            retry_meta = {}
                        process_fetched_candidate(
                            context,
                            resp=resp,
                            last_exc=last_exc,
                            elapsed_ms=elapsed_ms,
                            retry_meta=retry_meta,
                        )
                        if len(result.pages) >= config.max_pages:
                            break

    if storage is not None and url_state_updates:
        storage.upsert_url_states(url_state_updates)
    if storage is not None and artifact_updates:
        storage.upsert_artifact_cache(artifact_updates)

    result.discovery_stats = dict(sorted(stats.items()))
    result.incremental_stats = {
        "discovered": len(result.discovered_urls),
        "fetched": stats.get("fetched", 0),
        "fetch_retries_total": stats.get("fetch_retries_total", 0),
        "fetch_retry_wait_ms_total": stats.get("fetch_retry_wait_ms_total", 0),
        "fetch_retry_after_used": stats.get("fetch_retry_after_used", 0),
        "fetch_retryable_status_total": stats.get("fetch_retryable_status_total", 0),
        "fetch_network_retries_total": stats.get("fetch_network_retries_total", 0),
        "fetch_retry_budget_exhausted": stats.get("fetch_retry_budget_exhausted", 0),
        "reused_from_cache": stats.get("reused_from_cache", 0),
        "not_modified": stats.get("not_modified", 0),
        "reparsed": stats.get("reparsed", 0),
        "rerendered": stats.get("rerendered", 0),
    }
    return result
