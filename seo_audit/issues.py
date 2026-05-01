from __future__ import annotations

from collections import Counter, defaultdict
import json
from urllib.parse import parse_qsl, urlsplit

from seo_audit.models import IssueRecord
from seo_audit.policies import FACET_QUERY_PARAM_THRESHOLD, HIGH_RENDER_GAP_THRESHOLD, LONG_REDIRECT_CHAIN_THRESHOLD
from seo_audit.preview_controls import preview_restriction_score
from seo_audit.scoring_policy import (
    first_path_segment,
    internal_link_band_for_page,
    page_importance_for_page,
    resolve_page_type,
    thin_content_threshold_for_page,
)
from seo_audit.url_utils import is_internal_url, normalize_url


_ISSUE_GATE_MAP: dict[str, str] = {
    "FETCH_FAILED": "access",
    "NOINDEX": "indexability",
    "MISSING_TITLE": "indexability",
    "MISSING_META_DESCRIPTION": "indexability",
    "MISSING_H1": "indexability",
    "MISSING_CANONICAL": "canonicalization",
    "CANONICAL_MISMATCH": "canonicalization",
    "THIN_CONTENT": "indexability",
    "RENDER_GAP_HIGH": "rendering",
    "ORPHAN_RISK": "discovery",
    "LOW_INTERNAL_LINKS": "discovery",
    "IMPORTANT_PAGE_WEAK_SUPPORT": "discovery",
    "INTERNAL_FLOW_HUB_OVERLOAD": "discovery",
    "INTERNAL_CLUSTER_DISCONNECTED": "discovery",
    "RAW_ONLY_MISSING_H1": "rendering",
    "RAW_ONLY_CANONICAL_MISMATCH": "rendering",
    "RAW_ONLY_THIN_CONTENT": "rendering",
    "RAW_ONLY_LOW_INTERNAL_LINKS": "rendering",
    "RENDER_UNAVAILABLE": "rendering",
    "PERFORMANCE_PROVIDER_ERROR": "serving",
    "CRUX_PROVIDER_ERROR": "serving",
    "LIGHTHOUSE_BUDGET_FAIL": "serving",
    "ROBOTS_BLOCKED_URL": "access",
    "SITEMAP_URL_BLOCKED_BY_ROBOTS": "discovery",
    "ROBOTS_NOINDEX_CONFLICT": "access",
    "SITEMAP_URL_NOT_CRAWLED": "discovery",
    "CRAWLED_URL_NOT_IN_SITEMAP": "discovery",
    "DISCOVERY_BLIND_SPOT": "discovery",
    "REDIRECT_CHAIN_LONG": "access",
    "REDIRECT_TO_ERROR": "access",
    "ACCESS_AUTH_BLOCKED": "access",
    "DUPLICATE_CANONICAL_TAGS": "canonicalization",
    "MULTIPLE_CANONICAL_TAGS": "canonicalization",
    "CANONICAL_CONFLICT_RAW_VS_RENDERED": "canonicalization",
    "CANONICAL_SELF_MISMATCH": "canonicalization",
    "STATIC_SHELL_CANONICAL_REUSED_ACROSS_ROUTES": "canonicalization",
    "STATIC_SHELL_HREFLANG_REUSED_ACROSS_ROUTES": "canonicalization",
    "CLUSTER_CANONICAL_COLLISION": "canonicalization",
    "HOST_DUPLICATION_CLUSTER": "canonicalization",
    "HREFLANG_RECIPROCITY_MISSING": "canonicalization",
    "PAGINATION_SIGNAL_MISSING": "canonicalization",
    "FACETED_NAVIGATION_RISK": "discovery",
    "GSC_INDEX_STATE_NOT_INDEXED": "indexability",
    "STRUCTURED_DATA_PARSE_FAILED": "indexability",
    "SCHEMA_FEATURE_MISSING_REQUIRED": "indexability",
    "SCHEMA_FEATURE_RECOMMENDED_GAPS": "indexability",
    "SCHEMA_DEPRECATED_MARKUP": "indexability",
    "SCHEMA_VISIBLE_CONTENT_MISMATCH": "indexability",
    "EXACT_CONTENT_DUPLICATE": "indexability",
    "OPENAI_SEARCHBOT_BLOCKED": "discovery",
    "GOOGLE_EXTENDED_BLOCKED": "discovery",
    "GPTBOT_BLOCKED": "discovery",
    "OAI_ADSBOT_BLOCKED": "discovery",
    "BING_PREVIEW_CONTROLS_RESTRICTIVE": "indexability",
    "OVER_RESTRICTIVE_SNIPPET_CONTROLS": "indexability",
    "RAW_RENDER_NOINDEX_MISMATCH": "rendering",
    "RAW_RENDER_PREVIEW_CONTROL_MISMATCH": "rendering",
}

_CONTENT_SENSITIVE_CODES = {
    "MISSING_TITLE",
    "MISSING_META_DESCRIPTION",
    "MISSING_H1",
    "MISSING_CANONICAL",
    "CANONICAL_MISMATCH",
    "THIN_CONTENT",
    "LOW_INTERNAL_LINKS",
    "RAW_RENDER_NOINDEX_MISMATCH",
    "RAW_RENDER_PREVIEW_CONTROL_MISMATCH",
}

_PROVENANCE_CONFIDENCE = {
    "both": 95,
    "rendered_only": 90,
    "raw_only": 85,
}

_SEVERITY_RANK = {
    "info": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
    "critical": 4,
}

_RANK_SEVERITY = {rank: severity for severity, rank in _SEVERITY_RANK.items()}

_SEVERITY_WEIGHT = {
    "critical": 1.00,
    "high": 0.75,
    "medium": 0.45,
    "low": 0.20,
    "info": 0.10,
}

_REACH_WEIGHT = {
    "single_page": 1.00,
    "template_cluster": 1.20,
    "sitewide": 1.35,
}

_GATE_URGENCY = {
    "access": 1.35,
    "indexability": 1.35,
    "canonicalization": 1.20,
    "rendering": 1.15,
    "discovery": 1.00,
    "serving": 0.90,
}

_BLOCKED_CERTAINTY_CODES = {
    "FETCH_FAILED",
    "ACCESS_AUTH_BLOCKED",
    "RENDER_UNAVAILABLE",
}

_UNVERIFIED_CERTAINTY_CODES = {
    "PERFORMANCE_PROVIDER_ERROR",
    "CRUX_PROVIDER_ERROR",
    "LIGHTHOUSE_BUDGET_FAIL",
    "SITEMAP_URL_NOT_CRAWLED",
    "CRAWLED_URL_NOT_IN_SITEMAP",
    "DISCOVERY_BLIND_SPOT",
}


def _is_system_url(url: str) -> bool:
    path = (urlsplit(url).path or "").lower()
    basename = path.rsplit("/", 1)[-1]
    if basename in {"robots.txt", "sitemap.xml", "sitemap.xml.gz", "sitemap_index.xml"}:
        return True
    return basename.endswith(".xml")


def _count_internal_links_from_json(links_json: str, page: dict) -> int:
    try:
        links = json.loads(links_json or "[]")
    except json.JSONDecodeError:
        links = []
    if not isinstance(links, list):
        return 0

    count = 0
    for link in links:
        href = str((link or {}).get("href") or "")
        if href and is_internal_url(href, page.get("normalized_url", ""), base_url=page.get("final_url") or page.get("normalized_url")):
            count += 1
    return count


def _resolve_effective_internal_outlinks(page: dict) -> int:
    for key in ("effective_internal_links_out", "outlinks", "internal_links_out"):
        value = page.get(key)
        if value is not None:
            return int(value or 0)
    if page.get("effective_links_json"):
        return _count_internal_links_from_json(str(page.get("effective_links_json") or "[]"), page)
    return 0


def _resolve_raw_internal_outlinks(page: dict) -> int:
    if page.get("raw_links_json"):
        return _count_internal_links_from_json(str(page.get("raw_links_json") or "[]"), page)
    return int(page.get("internal_links_out", 0) or 0)


def _safe_json_list(value: str) -> list:
    try:
        parsed = json.loads(value or "[]")
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def _json_object(value: str) -> dict[str, object]:
    try:
        parsed = json.loads(value or "{}")
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _effective_field_provenance(page: dict) -> dict[str, str]:
    payload = _json_object(str(page.get("effective_field_provenance_json") or "{}"))
    return {str(key): str(value) for key, value in payload.items()}


def _shell_state(page: dict) -> str:
    state = str(page.get("shell_state") or "").strip()
    if state in {"raw_shell_unlikely", "raw_shell_possible", "raw_shell_confirmed_after_render"}:
        return state
    return "raw_shell_possible" if int(page.get("likely_js_shell") or 0) == 1 else "raw_shell_unlikely"


def _effective_canonical_count(page: dict) -> int:
    provenance = _effective_field_provenance(page)
    canonical_source = str(provenance.get("canonical") or "")
    if canonical_source.startswith("resolver:rendered") or canonical_source == "rendered":
        return int(page.get("rendered_canonical_count") or 0)
    return int(page.get("canonical_count") or 0)


def _redirect_chain(page: dict) -> list[str]:
    chain = _safe_json_list(str(page.get("redirect_chain_json") or "[]"))
    cleaned = [str(url).strip() for url in chain if str(url).strip()]
    return cleaned


def _query_params(url: str) -> list[tuple[str, str]]:
    return parse_qsl(urlsplit(url).query, keep_blank_values=True)


def _as_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    q = min(1.0, max(0.0, float(quantile)))
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])

    index = (len(ordered) - 1) * q
    lower = int(index)
    upper = min(len(ordered) - 1, lower + 1)
    fraction = index - lower
    return float((ordered[lower] * (1.0 - fraction)) + (ordered[upper] * fraction))


def _parse_preview_controls(
    raw_directives: str,
    *,
    data_nosnippet_count: int = 0,
    effective_robots_json: str = "",
) -> dict[str, object]:
    effective_payload = _json_object(str(effective_robots_json or ""))
    if effective_payload:
        return {
            "has_noindex": int(bool(effective_payload.get("is_noindex", 0))),
            "has_nosnippet": int(bool(effective_payload.get("has_nosnippet_directive", 0))),
            "max_snippet": str(effective_payload.get("max_snippet_directive") or ""),
            "max_image_preview": str(effective_payload.get("max_image_preview_directive") or ""),
            "max_video_preview": str(effective_payload.get("max_video_preview_directive") or ""),
            "data_nosnippet_count": max(0, int(data_nosnippet_count or 0)),
        }

    has_nosnippet = 0
    has_noindex = 0
    max_snippet = ""
    max_image_preview = ""
    max_video_preview = ""
    for token in str(raw_directives or "").split(","):
        normalized = token.strip().lower()
        if not normalized:
            continue
        if normalized == "nosnippet":
            has_nosnippet = 1
            continue
        if normalized == "noindex":
            has_noindex = 1
            continue
        if ":" not in normalized:
            continue
        key, value = [part.strip() for part in normalized.split(":", 1)]
        if not value:
            continue
        if key == "max-snippet":
            max_snippet = value
        elif key == "max-image-preview":
            max_image_preview = value
        elif key == "max-video-preview":
            max_video_preview = value
        elif key.endswith("bot") and "noindex" in value:
            # Agent-specific noindex directives still affect indexation semantics.
            has_noindex = 1
    return {
        "has_noindex": has_noindex,
        "has_nosnippet": has_nosnippet,
        "max_snippet": max_snippet,
        "max_image_preview": max_image_preview,
        "max_video_preview": max_video_preview,
        "data_nosnippet_count": max(0, int(data_nosnippet_count or 0)),
    }


def _preview_control_signature(controls: dict[str, object]) -> tuple[object, ...]:
    return (
        int(controls.get("has_nosnippet") or 0),
        str(controls.get("max_snippet") or ""),
        str(controls.get("max_image_preview") or ""),
        str(controls.get("max_video_preview") or ""),
        int(controls.get("data_nosnippet_count") or 0),
    )


def _preview_restriction_score(page: dict) -> tuple[int, list[str]]:
    return preview_restriction_score(page)


def _likely_intentional_preview_policy(page: dict) -> bool:
    page_type = _page_type(page)
    if int(page.get("is_noindex") or 0) == 1:
        return True
    return page_type in {"utility", "search", "legal", "privacy", "terms"}


def _likely_intentional_governance_block(page: dict) -> bool:
    page_type = _page_type(page)
    page_importance = _page_importance(page)
    return page_type in {"utility", "search", "legal", "privacy", "terms"} and page_importance < 1.10


def _load_hreflang_targets(page: dict) -> dict[str, str]:
    targets: dict[str, str] = {}
    links = _safe_json_list(str(page.get("hreflang_links_json") or "[]"))
    for link in links:
        if not isinstance(link, dict):
            continue
        href = str(link.get("href") or "").strip()
        lang = str(link.get("lang") or "").strip().lower()
        if not href:
            continue
        normalized = normalize_url(href, base_url=page.get("final_url") or page.get("normalized_url"))
        targets[normalized] = lang
    return targets


def _issue_gate(issue_code: str) -> str:
    return _ISSUE_GATE_MAP.get(issue_code, "indexability")


def _normalize_severity(severity: str) -> str:
    normalized = str(severity or "").strip().lower()
    if normalized in _SEVERITY_RANK:
        return normalized
    return "info"


def _severity_rank(severity: str) -> int:
    return _SEVERITY_RANK.get(_normalize_severity(severity), 0)


def _severity_from_rank(rank: int) -> str:
    clamped = max(0, min(4, int(rank)))
    return _RANK_SEVERITY.get(clamped, "info")


def _first_path_segment(url: str) -> str:
    return first_path_segment(url)


def _page_type(page: dict | None) -> str:
    return resolve_page_type(page, infer_from_url=True, fallback="other", none_fallback="global")


def _page_importance(page: dict | None) -> float:
    return page_importance_for_page(
        page,
        infer_from_url=True,
        fallback="other",
        none_fallback="global",
        boost_other_type_for_segments=True,
    )


def _template_cluster_for_page(page: dict | None) -> str:
    if not page:
        return "global-template"

    page_type = _page_type(page)
    segment = _first_path_segment(str(page.get("normalized_url") or ""))

    if page_type == "homepage" or not segment:
        return "home-template"
    if page_type == "service" or segment in {"service", "services"}:
        return "service-template"
    if page_type in {"product", "location"} or segment in {"product", "products", "location", "locations", "portfolio", "fasteners"}:
        return "commercial-template"
    if page_type in {"article", "blog"} or segment in {"blog", "news", "article", "articles"}:
        return "content-template"
    if page_type in {"tag", "category", "archive", "utility", "search"}:
        return "utility-template"
    return "other-template"


def _thin_content_threshold(page: dict) -> int:
    return thin_content_threshold_for_page(page, infer_from_url=True, fallback="other", none_fallback="global")


def _internal_link_band(page: dict, outlinks: int) -> str:
    return internal_link_band_for_page(page, outlinks, infer_from_url=True, fallback="other", none_fallback="global")


def _is_actionable_scope_page(page: dict) -> bool:
    url = str(page.get("normalized_url") or "")
    if not url or _is_system_url(url):
        return False
    if page.get("fetch_error"):
        return False
    status_code = int(page.get("status_code") or 0)
    if status_code < 200 or status_code >= 400:
        return False
    content_type = (page.get("content_type") or "").lower()
    is_html = "html" in content_type or bool(page.get("title") or page.get("h1") or page.get("word_count"))
    return is_html


def _certainty_state(
    issue_code: str,
    issue_provenance: str,
    verification_status: str,
    confidence_score: int,
) -> str:
    if issue_code in _BLOCKED_CERTAINTY_CODES or verification_status == "blocked_could_not_test":
        return "Blocked / Could not test"
    if issue_code in _UNVERIFIED_CERTAINTY_CODES or verification_status == "partial_evidence":
        return "Unverified"
    if verification_status == "needs_rendered_verification" or issue_provenance == "raw_only":
        return "Probable"
    if issue_provenance in {"both", "rendered_only"} and confidence_score >= 80:
        return "Verified"
    if confidence_score < 70:
        return "Unverified"
    return "Probable"


def _reach_label(*, affected_count: int, affected_ratio: float, has_template_cluster: bool) -> str:
    if affected_ratio >= 0.40 or affected_count >= 10:
        return "sitewide"
    if has_template_cluster or affected_ratio >= 0.10 or affected_count >= 3:
        return "template_cluster"
    return "single_page"


def _dynamic_severity(
    *,
    issue_code: str,
    base_severity: str,
    page: dict | None,
    page_importance: float,
    affected_ratio: float,
    reach: str,
) -> str:
    page_type = _page_type(page)
    rank = _severity_rank(base_severity)

    blocker_codes = {
        "FETCH_FAILED",
        "ACCESS_AUTH_BLOCKED",
        "REDIRECT_TO_ERROR",
        "ROBOTS_NOINDEX_CONFLICT",
    }

    if issue_code in {"FETCH_FAILED", "ACCESS_AUTH_BLOCKED", "REDIRECT_TO_ERROR", "ROBOTS_NOINDEX_CONFLICT"}:
        rank = max(rank, 4 if page_importance >= 1.20 else 3)
    elif issue_code == "NOINDEX":
        if page_type in {"tag", "category", "archive", "search", "utility"}:
            rank = min(rank, 1)
        if page_importance >= 1.20:
            rank = max(rank, 4)
        else:
            rank = max(rank, 2)
    elif issue_code == "MISSING_TITLE":
        if page_importance >= 1.20:
            rank = max(rank, 3)
        elif page_type in {"tag", "category", "archive", "utility", "search"}:
            rank = min(rank, 1)
        elif page_type in {"article", "blog"}:
            rank = max(2, min(rank, 2))
    elif issue_code == "LOW_INTERNAL_LINKS":
        if (reach == "sitewide" or affected_ratio >= 0.40) and page_importance >= 1.20:
            rank = max(rank, 2)
        elif reach == "template_cluster" or affected_ratio >= 0.10:
            rank = max(rank, 1)
        elif page_importance < 0.90:
            rank = min(rank, 1)
    elif issue_code == "RENDER_GAP_HIGH":
        rank = max(rank, 3 if page_importance >= 1.20 else 2)
    elif issue_code == "THIN_CONTENT":
        if page_type in {"contact", "legal", "privacy", "terms", "utility"}:
            rank = min(rank, 1)
        elif page_importance >= 1.20 and reach != "single_page":
            rank = max(rank, 2)
        else:
            rank = max(rank, 2 if page_importance >= 1.0 else 1)
    elif issue_code in {"CANONICAL_MISMATCH", "HREFLANG_RECIPROCITY_MISSING", "DUPLICATE_CANONICAL_TAGS"}:
        if page_importance >= 1.20 and affected_ratio >= 0.25:
            rank = max(rank, 3)
        else:
            rank = max(rank, 2)
            rank = min(rank, 3)
    elif issue_code in {
        "OPENAI_SEARCHBOT_BLOCKED",
        "GOOGLE_EXTENDED_BLOCKED",
        "GPTBOT_BLOCKED",
        "OAI_ADSBOT_BLOCKED",
    }:
        rank = max(rank, 3 if page_importance >= 1.20 else 2)
    elif issue_code in {"BING_PREVIEW_CONTROLS_RESTRICTIVE", "OVER_RESTRICTIVE_SNIPPET_CONTROLS"}:
        rank = max(rank, 2 if page_importance >= 1.00 else 1)
    elif issue_code in {"RAW_RENDER_NOINDEX_MISMATCH", "RAW_RENDER_PREVIEW_CONTROL_MISMATCH"}:
        rank = max(rank, 3 if page_importance >= 1.20 else 2)
    elif issue_code == "IMPORTANT_PAGE_WEAK_SUPPORT":
        rank = max(rank, 3 if page_importance >= 1.20 else 2)
    elif issue_code == "INTERNAL_FLOW_HUB_OVERLOAD":
        rank = max(rank, 3 if page_importance >= 1.20 else 2)
    elif issue_code == "INTERNAL_CLUSTER_DISCONNECTED":
        rank = max(rank, 2)
        if page_importance >= 1.20:
            rank = max(rank, 3)

    if issue_code.startswith("RAW_ONLY_"):
        # Raw-only findings are diagnostic drift signals and are shell-state gated.
        shell_state = _shell_state(page or {})
        if shell_state == "raw_shell_confirmed_after_render":
            rank = 0
        elif shell_state == "raw_shell_possible":
            rank = min(rank, 1)
        else:
            rank = min(rank, 2)
        if shell_state != "raw_shell_confirmed_after_render" and page_importance < 1.20:
            rank = min(rank, 1)

    if page_importance < 0.85 and reach == "single_page" and rank > 1:
        rank -= 1

    if issue_code not in blocker_codes:
        rank = min(rank, 3)

    return _severity_from_rank(rank)


def _priority_score(
    *,
    severity: str,
    confidence_score: int,
    certainty_state: str,
    page_importance: float,
    reach: str,
    urgency: float,
) -> int:
    severity_weight = _SEVERITY_WEIGHT.get(_normalize_severity(severity), 0.10)
    confidence = max(0.40, min(1.00, float(confidence_score) / 100.0))

    if certainty_state == "Blocked / Could not test":
        confidence = min(confidence, 0.55)
    elif certainty_state == "Unverified":
        confidence = min(confidence, 0.70)
    elif certainty_state == "Probable":
        confidence = min(confidence, 0.85)

    reach_weight = _REACH_WEIGHT.get(reach, 1.00)
    raw = severity_weight * confidence * page_importance * reach_weight * urgency

    max_raw = 1.00 * 1.00 * 1.40 * 1.35 * 1.35
    normalized = (raw / max_raw) * 100.0
    return max(0, min(100, int(round(normalized))))


def _enrich_issue_context(issues: list[IssueRecord], pages: list[dict]) -> None:
    if not issues:
        return

    pages_by_url = {
        str(page.get("normalized_url") or ""): page
        for page in pages
        if str(page.get("normalized_url") or "")
    }
    actionable_total = sum(1 for page in pages if _is_actionable_scope_page(page)) or max(1, len(pages))

    code_urls: dict[str, set[str]] = defaultdict(set)
    code_templates: dict[str, Counter[str]] = defaultdict(Counter)
    code_page_types: dict[str, set[str]] = defaultdict(set)

    for issue in issues:
        code = str(issue.issue_code or "").upper()
        url = str(issue.url or "")
        page = pages_by_url.get(url)
        if url:
            code_urls[code].add(url)
        code_templates[code][_template_cluster_for_page(page)] += 1
        code_page_types[code].add(_page_type(page))

    for issue in issues:
        code = str(issue.issue_code or "").upper()
        url = str(issue.url or "")
        page = pages_by_url.get(url)

        affected_count = max(1, len(code_urls.get(code, set())))
        affected_ratio = min(1.0, max(0.0, affected_count / float(max(1, actionable_total))))

        template_cluster = ""
        template_counter = code_templates.get(code, Counter())
        if template_counter:
            top_template, top_count = template_counter.most_common(1)[0]
            if top_template != "global-template" and top_count >= 3 and (top_count / float(max(1, affected_count))) >= 0.50:
                template_cluster = top_template

        reach = _reach_label(
            affected_count=affected_count,
            affected_ratio=affected_ratio,
            has_template_cluster=bool(template_cluster),
        )
        urgency = float(_GATE_URGENCY.get(str(issue.technical_seo_gate or ""), 1.00))
        page_importance = _page_importance(page)

        issue.severity = _dynamic_severity(
            issue_code=code,
            base_severity=issue.severity,
            page=page,
            page_importance=page_importance,
            affected_ratio=affected_ratio,
            reach=reach,
        )

        issue.certainty_state = _certainty_state(
            code,
            str(issue.issue_provenance or ""),
            str(issue.verification_status or ""),
            int(issue.confidence_score or 0),
        )
        issue.page_importance = round(page_importance, 2)
        issue.reach = reach
        issue.urgency = round(urgency, 2)
        issue.affected_count = affected_count
        issue.affected_ratio = round(affected_ratio, 4)
        issue.template_cluster = template_cluster
        issue.affected_page_types = ",".join(sorted(code_page_types.get(code, {"other"})))
        issue.priority_score = _priority_score(
            severity=issue.severity,
            confidence_score=int(issue.confidence_score or 0),
            certainty_state=issue.certainty_state,
            page_importance=page_importance,
            reach=reach,
            urgency=urgency,
        )


def enrich_issues(issues: list[IssueRecord], pages: list[dict]) -> list[IssueRecord]:
    _enrich_issue_context(issues, pages)
    return issues


def _issue_confidence(issue_code: str, issue_provenance: str, page: dict) -> tuple[str, int]:
    verification_status = "automated"
    confidence_score = int(_PROVENANCE_CONFIDENCE.get(issue_provenance, 80))

    shell_state = _shell_state(page)
    shell_like = shell_state in {"raw_shell_possible", "raw_shell_confirmed_after_render"}
    used_render = bool(int(page.get("used_render") or 0))
    render_checked = bool(int(page.get("render_checked") or 0))

    if issue_code in _CONTENT_SENSITIVE_CODES and shell_like and not used_render:
        verification_status = "needs_rendered_verification"
        confidence_score = min(confidence_score, 60)
    elif issue_code in _CONTENT_SENSITIVE_CODES and shell_like and not render_checked:
        verification_status = "needs_rendered_verification"
        confidence_score = min(confidence_score, 65)

    if issue_provenance == "raw_only" and shell_state == "raw_shell_confirmed_after_render":
        confidence_score = min(confidence_score, 55)

    if issue_code in _BLOCKED_CERTAINTY_CODES:
        verification_status = "blocked_could_not_test"
        confidence_score = min(confidence_score, 60)
    elif issue_code in _UNVERIFIED_CERTAINTY_CODES:
        verification_status = "partial_evidence"
        confidence_score = min(confidence_score, 70)

    return verification_status, max(0, min(100, confidence_score))


def _issue(
    run_id: str,
    url: str,
    severity: str,
    issue_code: str,
    title: str,
    description: str,
    *,
    page: dict,
    evidence_json: str = "{}",
    issue_provenance: str = "both",
) -> IssueRecord:
    verification_status, confidence_score = _issue_confidence(issue_code, issue_provenance, page)
    return IssueRecord(
        run_id=run_id,
        url=url,
        severity=_normalize_severity(severity),
        issue_code=issue_code,
        title=title,
        description=description,
        evidence_json=evidence_json,
        issue_provenance=issue_provenance,
        technical_seo_gate=_issue_gate(issue_code),
        verification_status=verification_status,
        confidence_score=confidence_score,
    )


def build_issues(run_id: str, pages: list[dict]) -> list[IssueRecord]:
    issues: list[IssueRecord] = []
    hreflang_targets_by_page: dict[str, dict[str, str]] = {}
    for page in pages:
        source = str(page.get("normalized_url") or "")
        if source:
            hreflang_targets_by_page[source] = _load_hreflang_targets(page)

    crawled_urls = set(hreflang_targets_by_page.keys())
    content_hash_clusters: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for page in pages:
        url = str(page.get("normalized_url") or "")
        content_hash = str(page.get("effective_content_hash") or page.get("content_hash") or "").strip()
        if not url or not content_hash:
            continue
        if page.get("fetch_error"):
            continue
        status_code = int(page.get("status_code") or 0)
        is_html_page = "html" in (page.get("content_type") or "").lower() or bool(
            page.get("title") or page.get("h1") or page.get("word_count")
        )
        if not is_html_page or status_code < 200 or status_code >= 400 or _is_system_url(url):
            continue
        if int(page.get("is_noindex") or 0) == 1:
            continue
        if int(page.get("effective_text_len") or page.get("word_count") or page.get("raw_word_count") or 0) < 120:
            continue
        if _shell_state(page) == "raw_shell_confirmed_after_render" and int(page.get("used_render") or 0) == 1:
            content_hash_source = str(_effective_field_provenance(page).get("content_hash") or "")
            if content_hash_source in {"raw", "raw_fallback"}:
                continue
        representative_url = str(
            page.get("canonical_cluster_key")
            or page.get("final_url")
            or page.get("normalized_url")
            or ""
        )
        content_hash_clusters[content_hash].append((url, representative_url or url))

    duplicate_content_by_url: dict[str, list[str]] = {}
    for entries in content_hash_clusters.values():
        if not entries:
            continue
        reps = sorted({rep for _, rep in entries if rep})
        if len(reps) <= 1:
            continue
        representative_samples: list[str] = []
        for rep in reps:
            sample_url = next((entry_url for entry_url, entry_rep in entries if entry_rep == rep), rep)
            representative_samples.append(sample_url)
        for entry_url, _entry_rep in entries:
            duplicate_content_by_url[entry_url] = sorted(set(representative_samples))

    actionable_graph_pages = [page for page in pages if _is_actionable_scope_page(page)]
    pagerank_values = [
        _as_float(page.get("internal_pagerank"))
        for page in actionable_graph_pages
        if _as_float(page.get("internal_pagerank")) > 0.0
    ]
    betweenness_values = [
        _as_float(page.get("betweenness"))
        for page in actionable_graph_pages
        if _as_float(page.get("betweenness")) > 0.0
    ]
    weak_pagerank_cutoff = _percentile(pagerank_values, 0.25) if len(pagerank_values) >= 4 else 0.0
    hub_betweenness_cutoff = max(0.05, _percentile(betweenness_values, 0.90)) if betweenness_values else 0.0

    community_counts: Counter[int] = Counter()
    community_representative: dict[int, tuple[float, str]] = {}
    for page in actionable_graph_pages:
        community_id = int(page.get("community_id") or 0)
        url = str(page.get("normalized_url") or "")
        if community_id <= 0 or not url:
            continue
        community_counts[community_id] += 1
        importance = _page_importance(page)
        current = community_representative.get(community_id)
        if current is None or importance > current[0]:
            community_representative[community_id] = (importance, url)

    primary_community_id = 0
    if community_counts:
        primary_community_id = max(
            community_counts.items(),
            key=lambda item: (item[1], item[0]),
        )[0]
    emitted_disconnected_communities: set[int] = set()

    pagination_keys = {"page", "p", "pg", "paged"}
    faceted_keys = {
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
    }

    actionable_pages = [page for page in pages if _is_actionable_scope_page(page)]
    shell_canonical_groups: dict[str, list[dict]] = defaultdict(list)
    shell_hreflang_groups: dict[str, list[dict]] = defaultdict(list)
    for page in actionable_pages:
        if _shell_state(page) != "raw_shell_confirmed_after_render" or int(page.get("used_render") or 0) != 1:
            continue
        final_url = str(page.get("final_url") or page.get("normalized_url") or "")
        raw_canonical = normalize_url(str(page.get("raw_canonical") or ""), base_url=final_url)
        if raw_canonical:
            shell_canonical_groups[raw_canonical].append(page)
        raw_hreflang_signature = str(page.get("raw_hreflang_links_json") or "[]").strip()
        if raw_hreflang_signature and raw_hreflang_signature != "[]":
            shell_hreflang_groups[raw_hreflang_signature].append(page)

    shell_canonical_root_cause_urls: set[str] = set()
    for members in shell_canonical_groups.values():
        unique_urls = {
            str(member.get("normalized_url") or "")
            for member in members
            if str(member.get("normalized_url") or "")
        }
        if len(unique_urls) < 3:
            continue
        shell_canonical_root_cause_urls.update(unique_urls)

    shell_hreflang_root_cause_urls: set[str] = set()
    for members in shell_hreflang_groups.values():
        unique_urls = {
            str(member.get("normalized_url") or "")
            for member in members
            if str(member.get("normalized_url") or "")
        }
        if len(unique_urls) < 3:
            continue
        shell_hreflang_root_cause_urls.update(unique_urls)

    canonical_cluster_members: dict[str, list[dict]] = defaultdict(list)
    for page in actionable_pages:
        cluster_key = str(page.get("canonical_cluster_key") or page.get("final_url") or page.get("normalized_url") or "").strip()
        if cluster_key:
            canonical_cluster_members[cluster_key].append(page)

    cluster_alias_root_cause_urls: set[str] = set()
    cluster_root_cause_member_urls: set[str] = set()
    for members in canonical_cluster_members.values():
        if len(members) < 3:
            continue
        unique_finals = {
            normalize_url(str(member.get("final_url") or member.get("normalized_url") or ""))
            for member in members
            if str(member.get("final_url") or member.get("normalized_url") or "")
        }
        if len(unique_finals) <= 1:
            continue
        for member in members:
            member_url = str(member.get("normalized_url") or "")
            if member_url:
                cluster_root_cause_member_urls.add(member_url)
        for member in members:
            if str(member.get("canonical_cluster_role") or "").strip().lower() != "alias":
                continue
            member_url = str(member.get("normalized_url") or "")
            if member_url:
                cluster_alias_root_cause_urls.add(member_url)

    for p in pages:
        url = p.get("normalized_url", "")
        robots_blocked_unfetched = int(p.get("robots_blocked_flag") or 0) == 1 and p.get("status_code") in {None, 0, ""}
        if robots_blocked_unfetched:
            issues.append(
                _issue(
                    run_id,
                    url,
                    "medium",
                    "ROBOTS_BLOCKED_URL",
                    "URL blocked by robots",
                    "URL was discovered but not fetched because robots policy disallowed crawling.",
                    page=p,
                    evidence_json=json.dumps({"discovered_url": p.get("discovered_url", "")}),
                )
            )
            if int(p.get("in_sitemap_flag") or 0) == 1:
                issues.append(
                    _issue(
                        run_id,
                        url,
                        "medium",
                        "SITEMAP_URL_BLOCKED_BY_ROBOTS",
                        "Sitemap URL blocked by robots",
                        "URL is present in sitemap but blocked by robots, limiting crawl verification for listed content.",
                        page=p,
                    )
                )
            continue

        if p.get("fetch_error"):
            issues.append(
                _issue(
                    run_id,
                    url,
                    "critical",
                    "FETCH_FAILED",
                    "Fetch failed",
                    p["fetch_error"],
                    page=p,
                )
            )
            continue

        status_code = int(p.get("status_code") or 0)
        is_html_page = "html" in (p.get("content_type") or "").lower() or bool(p.get("title") or p.get("h1") or p.get("word_count"))
        is_actionable_page = is_html_page and 200 <= status_code < 400 and not _is_system_url(url)
        page_importance = _page_importance(p)
        page_type = _page_type(p)
        shell_confirmed_after_render = _shell_state(p) == "raw_shell_confirmed_after_render" and int(p.get("used_render") or 0) == 1
        cluster_root_cause_alias = str(url) in cluster_alias_root_cause_urls
        suppress_raw_only_diagnostics = shell_confirmed_after_render
        suppress_canonical_page_symptoms = (
            (shell_confirmed_after_render and str(url) in shell_canonical_root_cause_urls)
            or cluster_root_cause_alias
            or str(url) in cluster_root_cause_member_urls
        )
        suppress_hreflang_page_symptoms = (
            (shell_confirmed_after_render and str(url) in shell_hreflang_root_cause_urls)
            or cluster_root_cause_alias
            or str(url) in cluster_root_cause_member_urls
        )
        internal_pagerank = _as_float(p.get("internal_pagerank"))
        betweenness = _as_float(p.get("betweenness"))
        closeness = _as_float(p.get("closeness"))
        community_id = int(p.get("community_id") or 0)
        bridge_flag = int(p.get("bridge_flag") or 0) == 1

        if status_code in {401, 403}:
            issues.append(_issue(run_id, url, "high", "ACCESS_AUTH_BLOCKED", "Access blocked by authentication", "Page responded with 401/403 and cannot be indexed without access.", page=p))

        redirect_chain = _redirect_chain(p)
        redirect_hops = max(0, len(redirect_chain) - 1)
        if redirect_hops >= LONG_REDIRECT_CHAIN_THRESHOLD:
            issues.append(
                _issue(
                    run_id,
                    url,
                    "medium",
                    "REDIRECT_CHAIN_LONG",
                    "Redirect chain too long",
                    "Long redirect chains can waste crawl budget and delay canonical consolidation.",
                    page=p,
                    evidence_json=json.dumps({"hops": redirect_hops, "chain": redirect_chain[:8]}),
                )
            )
        if redirect_hops > 0 and status_code >= 400:
            issues.append(
                _issue(
                    run_id,
                    url,
                    "high",
                    "REDIRECT_TO_ERROR",
                    "Redirect ends in error",
                    "A redirect path resolves to an error status, which breaks access to the intended URL.",
                    page=p,
                    evidence_json=json.dumps({"hops": redirect_hops, "status_code": status_code, "chain": redirect_chain[:8]}),
                )
            )

        if is_actionable_page and p.get("is_noindex"):
            issues.append(_issue(run_id, url, "high", "NOINDEX", "Noindex detected", "Page has noindex signal.", page=p))
        if is_actionable_page and p.get("is_noindex") and int(p.get("robots_blocked_flag") or 0) == 1:
            issues.append(
                _issue(
                    run_id,
                    url,
                    "high",
                    "ROBOTS_NOINDEX_CONFLICT",
                    "Robots and noindex conflict",
                    "Page is both noindex and blocked by robots, which can prevent crawlers from seeing the noindex directive.",
                    page=p,
                )
            )

        raw_preview_controls = _parse_preview_controls(
            str(p.get("meta_robots") or ""),
            data_nosnippet_count=int(p.get("data_nosnippet_count") or 0),
            effective_robots_json=str(p.get("effective_robots_json") or ""),
        )
        rendered_preview_controls = _parse_preview_controls(
            str(p.get("rendered_meta_robots") or ""),
            data_nosnippet_count=int(p.get("rendered_data_nosnippet_count") or 0),
            effective_robots_json=str(p.get("rendered_effective_robots_json") or ""),
        )

        if is_actionable_page and bool(int(p.get("used_render") or 0)):
            if int(raw_preview_controls.get("has_noindex") or 0) != int(rendered_preview_controls.get("has_noindex") or 0):
                issues.append(
                    _issue(
                        run_id,
                        url,
                        "high" if page_importance >= 1.20 else "medium",
                        "RAW_RENDER_NOINDEX_MISMATCH",
                        "Raw/render noindex mismatch",
                        "Raw HTML and rendered DOM expose different noindex directives, which can hide indexation controls.",
                        page=p,
                        issue_provenance="raw_only",
                        evidence_json=json.dumps(
                            {
                                "classification": "conflict",
                                "raw_meta_robots": str(p.get("meta_robots") or ""),
                                "rendered_meta_robots": str(p.get("rendered_meta_robots") or ""),
                                "raw_has_noindex": int(raw_preview_controls.get("has_noindex") or 0),
                                "rendered_has_noindex": int(rendered_preview_controls.get("has_noindex") or 0),
                            }
                        ),
                    )
                )

            if _preview_control_signature(raw_preview_controls) != _preview_control_signature(rendered_preview_controls):
                issues.append(
                    _issue(
                        run_id,
                        url,
                        "medium",
                        "RAW_RENDER_PREVIEW_CONTROL_MISMATCH",
                        "Raw/render preview control mismatch",
                        "Preview/snippet directives differ between raw HTML and rendered DOM, increasing governance ambiguity.",
                        page=p,
                        issue_provenance="raw_only",
                        evidence_json=json.dumps(
                            {
                                "classification": "conflict",
                                "raw_controls": raw_preview_controls,
                                "rendered_controls": rendered_preview_controls,
                            }
                        ),
                    )
                )

        is_indexable_page = is_actionable_page and not p.get("is_noindex")
        governance_intentional = _likely_intentional_governance_block(p)
        if is_indexable_page and int(p.get("governance_openai_allowed", 1) or 0) == 0 and not governance_intentional:
            issues.append(
                _issue(
                    run_id,
                    url,
                    "high" if page_importance >= 1.20 else "medium",
                    "OPENAI_SEARCHBOT_BLOCKED",
                    "OAI-SearchBot blocked by robots policy",
                    "OAI-SearchBot is blocked for this URL, which suppresses OpenAI search/discovery retrieval.",
                    page=p,
                    evidence_json=json.dumps(
                        {
                            "classification": "likely_accidental_suppression",
                            "oai_searchbot_allowed": 0,
                            "googlebot_allowed": int(p.get("governance_googlebot_allowed", 1) or 0),
                            "bingbot_allowed": int(p.get("governance_bingbot_allowed", 1) or 0),
                        }
                    ),
                )
            )
        if is_indexable_page and int(p.get("governance_google_extended_allowed", 1) or 0) == 0 and not governance_intentional:
            issues.append(
                _issue(
                    run_id,
                    url,
                    "medium" if page_importance >= 1.20 else "low",
                    "GOOGLE_EXTENDED_BLOCKED",
                    "Google-Extended blocked by robots policy",
                    "Google-Extended is blocked for this URL, limiting generative-use eligibility where this control is consulted.",
                    page=p,
                    evidence_json=json.dumps(
                        {
                            "classification": "likely_accidental_suppression",
                            "google_extended_allowed": 0,
                            "googlebot_allowed": int(p.get("governance_googlebot_allowed", 1) or 0),
                        }
                    ),
                )
            )
        if is_indexable_page and int(p.get("governance_gptbot_allowed", 1) or 0) == 0 and not governance_intentional:
            issues.append(
                _issue(
                    run_id,
                    url,
                    "medium" if page_importance >= 1.20 else "low",
                    "GPTBOT_BLOCKED",
                    "GPTBot blocked by robots policy",
                    "GPTBot is blocked for this URL, which limits generative-model training and retrieval access where this control is consulted.",
                    page=p,
                    evidence_json=json.dumps(
                        {
                            "classification": "likely_accidental_suppression",
                            "gptbot_allowed": 0,
                            "googlebot_allowed": int(p.get("governance_googlebot_allowed", 1) or 0),
                        }
                    ),
                )
            )
        if is_indexable_page and int(p.get("governance_oai_adsbot_allowed", 1) or 0) == 0 and not governance_intentional:
            issues.append(
                _issue(
                    run_id,
                    url,
                    "medium" if page_importance >= 1.20 else "low",
                    "OAI_ADSBOT_BLOCKED",
                    "OAI-AdsBot blocked by robots policy",
                    "OAI-AdsBot is blocked for this URL, reducing OpenAI crawler coverage for ad-oriented retrieval paths.",
                    page=p,
                    evidence_json=json.dumps(
                        {
                            "classification": "likely_accidental_suppression",
                            "oai_adsbot_allowed": 0,
                            "oai_searchbot_allowed": int(p.get("governance_openai_allowed", 1) or 0),
                            "googlebot_allowed": int(p.get("governance_googlebot_allowed", 1) or 0),
                        }
                    ),
                )
            )

        preview_restriction_score, preview_reasons = _preview_restriction_score(p)
        preview_intentional = _likely_intentional_preview_policy(p)
        if is_indexable_page and preview_restriction_score >= 5 and not preview_intentional:
            issues.append(
                _issue(
                    run_id,
                    url,
                    "medium" if page_importance >= 1.00 else "low",
                    "OVER_RESTRICTIVE_SNIPPET_CONTROLS",
                    "Over-restrictive snippet controls",
                    "Snippet/preview directives are highly restrictive for an indexable page and may suppress discoverability.",
                    page=p,
                    evidence_json=json.dumps(
                        {
                            "classification": "likely_accidental_suppression",
                            "restriction_score": preview_restriction_score,
                            "reasons": preview_reasons,
                            "has_nosnippet_directive": int(p.get("has_nosnippet_directive") or 0),
                            "max_snippet_directive": str(p.get("max_snippet_directive") or ""),
                            "max_image_preview_directive": str(p.get("max_image_preview_directive") or ""),
                            "max_video_preview_directive": str(p.get("max_video_preview_directive") or ""),
                            "data_nosnippet_count": int(p.get("data_nosnippet_count") or 0),
                        }
                    ),
                )
            )
        if (
            is_indexable_page
            and preview_restriction_score >= 4
            and not preview_intentional
            and (page_importance >= 1.00 or page_type in {"homepage", "service", "location", "product"})
        ):
            issues.append(
                _issue(
                    run_id,
                    url,
                    "high" if page_importance >= 1.20 and preview_restriction_score >= 6 else "medium",
                    "BING_PREVIEW_CONTROLS_RESTRICTIVE",
                    "Bing-facing preview controls are restrictive",
                    "Current preview directives are restrictive enough to reduce snippet/answer-surface visibility for Bing consumption.",
                    page=p,
                    evidence_json=json.dumps(
                        {
                            "classification": "likely_accidental_suppression",
                            "restriction_score": preview_restriction_score,
                            "reasons": preview_reasons,
                            "page_importance": round(page_importance, 2),
                        }
                    ),
                )
            )

        raw_h1_value = p.get("raw_h1_count")
        eff_h1_value = p.get("effective_h1_count")
        raw_h1_missing = int(raw_h1_value if raw_h1_value is not None else int(bool(p.get("h1")))) == 0
        eff_h1_missing = int(eff_h1_value if eff_h1_value is not None else int(bool(p.get("h1")))) == 0
        thin_threshold = _thin_content_threshold(p)
        raw_text_len = int(p.get("raw_text_len", p.get("word_count", 0)) or 0)
        eff_text_len = int(p.get("effective_text_len", p.get("word_count", 0)) or 0)
        raw_thin = raw_text_len < thin_threshold
        eff_thin = eff_text_len < thin_threshold

        raw_internal_outlinks = _resolve_raw_internal_outlinks(p)
        eff_internal_outlinks = _resolve_effective_internal_outlinks(p)
        raw_link_band = _internal_link_band(p, raw_internal_outlinks)
        eff_link_band = _internal_link_band(p, eff_internal_outlinks)
        raw_low_links = raw_link_band in {"severe", "weak"}
        eff_low_links = eff_link_band in {"severe", "weak"}

        rendered_effective_healthy = (
            int(p.get("used_render") or 0) == 1
            and not str(p.get("render_error") or "").strip()
            and bool(p.get("effective_title") or p.get("title"))
            and bool(p.get("effective_meta_description") or p.get("meta_description"))
            and not eff_h1_missing
            and not eff_thin
            and not eff_low_links
        )
        if _shell_state(p) == "raw_shell_possible" and rendered_effective_healthy:
            suppress_raw_only_diagnostics = True

        if is_indexable_page and not p.get("effective_title") and not p.get("title"):
            issues.append(_issue(run_id, url, "high", "MISSING_TITLE", "Missing title", "No <title> found.", page=p, issue_provenance="both"))
        if is_indexable_page and not (p.get("effective_meta_description") or p.get("meta_description")):
            issues.append(_issue(run_id, url, "medium", "MISSING_META_DESCRIPTION", "Missing meta description", "No meta description found.", page=p, issue_provenance="both"))
        if is_indexable_page and eff_h1_missing:
            prov = "both" if raw_h1_missing else "rendered_only"
            issues.append(_issue(run_id, url, "medium", "MISSING_H1", "Missing H1", "No H1 found in effective page facts.", page=p, issue_provenance=prov))
        elif is_indexable_page and raw_h1_missing and p.get("used_render") and not suppress_raw_only_diagnostics:
            issues.append(_issue(run_id, url, "low", "RAW_ONLY_MISSING_H1", "Raw HTML missing H1", "Initial HTML had no H1 but rendered DOM included one.", page=p, issue_provenance="raw_only"))
        if is_indexable_page and not (p.get("effective_canonical") or p.get("canonical_url")):
            issues.append(_issue(run_id, url, "low", "MISSING_CANONICAL", "Missing canonical", "No canonical link found.", page=p, issue_provenance="both"))

        canonical = p.get("effective_canonical") or p.get("canonical_url") or ""
        canonical_provenance = str(_effective_field_provenance(p).get("canonical") or "raw")
        if canonical and is_indexable_page and not suppress_canonical_page_symptoms:
            canonical_norm = normalize_url(canonical, base_url=p.get("final_url") or p.get("normalized_url"))
            final_candidate = str(p.get("final_url") or p.get("normalized_url") or "")
            final_norm = normalize_url(final_candidate)
            if canonical_norm != final_norm:
                if canonical_provenance in {"raw", "raw_fallback", "unresolved"}:
                    issues.append(
                        _issue(
                            run_id,
                            url,
                            "medium",
                            "CANONICAL_MISMATCH",
                            "Canonical mismatch",
                            "Canonical URL differs from final fetched URL.",
                            page=p,
                            evidence_json=json.dumps({"canonical": canonical_norm, "final": final_norm}),
                            issue_provenance="both",
                        )
                    )
                issues.append(
                    _issue(
                        run_id,
                        url,
                        "medium",
                        "CANONICAL_SELF_MISMATCH",
                        "Canonical self mismatch",
                        "Effective canonical does not self-match the final fetched URL.",
                        page=p,
                        evidence_json=json.dumps({"canonical": canonical_norm, "final": final_norm}),
                        issue_provenance="both",
                    )
                )
            elif (
                p.get("used_render")
                and (p.get("raw_canonical") or "")
                and normalize_url(str(p.get("raw_canonical")), base_url=final_candidate) != final_norm
                and not suppress_raw_only_diagnostics
            ):
                issues.append(_issue(run_id, url, "low", "RAW_ONLY_CANONICAL_MISMATCH", "Raw HTML canonical mismatch", "Raw shell canonical differed from final URL but rendered canonical matched.", page=p, issue_provenance="raw_only"))

        if is_indexable_page and not suppress_canonical_page_symptoms:
            raw_canonical = normalize_url(
                str(p.get("raw_canonical") or ""),
                base_url=str(p.get("final_url") or p.get("normalized_url") or ""),
            )
            rendered_canonical = normalize_url(
                str(p.get("rendered_canonical") or ""),
                base_url=str(p.get("final_url") or p.get("normalized_url") or ""),
            )
            if int(p.get("used_render") or 0) == 1 and raw_canonical and rendered_canonical and raw_canonical != rendered_canonical:
                issues.append(
                    _issue(
                        run_id,
                        url,
                        "medium",
                        "CANONICAL_CONFLICT_RAW_VS_RENDERED",
                        "Canonical conflict between raw and rendered state",
                        "Raw HTML and rendered DOM expose conflicting canonical targets.",
                        page=p,
                        evidence_json=json.dumps(
                            {
                                "raw_canonical": raw_canonical,
                                "rendered_canonical": rendered_canonical,
                                "effective_canonical": canonical,
                            }
                        ),
                        issue_provenance="raw_only",
                    )
                )

            if int(p.get("canonical_unresolved") or 0) == 1:
                issues.append(
                    _issue(
                        run_id,
                        url,
                        "medium",
                        "CANONICAL_CONFLICT_RAW_VS_RENDERED",
                        "Canonical conflict unresolved",
                        "Canonical resolution remained unresolved after comparing raw and rendered canonical inventories.",
                        page=p,
                        evidence_json=json.dumps(
                            {
                                "raw_canonical_urls": _safe_json_list(str(p.get("raw_canonical_urls_json") or "[]"))[:8],
                                "rendered_canonical_urls": _safe_json_list(str(p.get("rendered_canonical_urls_json") or "[]"))[:8],
                            }
                        ),
                        issue_provenance="raw_only",
                    )
                )

            effective_canonical_count = _effective_canonical_count(p)
            if effective_canonical_count > 1:
                canonical_urls_key = "rendered_canonical_urls_json" if canonical_provenance.startswith("resolver:rendered") else "canonical_urls_json"
                evidence_payload = json.dumps(
                    {
                        "canonical_count": effective_canonical_count,
                        "canonical_urls": _safe_json_list(str(p.get(canonical_urls_key) or "[]"))[:6],
                        "canonical_source": canonical_provenance or "raw",
                    }
                )
                for code, title, description in (
                    (
                        "MULTIPLE_CANONICAL_TAGS",
                        "Multiple canonical tags",
                        "Multiple canonical tags were found in the active evidence layer. Keep exactly one canonical per page.",
                    ),
                    (
                        "DUPLICATE_CANONICAL_TAGS",
                        "Duplicate canonical tags",
                        "Multiple canonical tags were found on the page. Keep exactly one canonical per page.",
                    ),
                ):
                    issues.append(
                        _issue(
                            run_id,
                            url,
                            "medium",
                            code,
                            title,
                            description,
                            page=p,
                            evidence_json=evidence_payload,
                        )
                    )

        if is_actionable_page and int(p.get("schema_parse_error_count") or 0) > 0:
            issues.append(
                _issue(
                    run_id,
                    url,
                    "low",
                    "STRUCTURED_DATA_PARSE_FAILED",
                    "Structured data parse failures",
                    "One or more JSON-LD blocks could not be parsed; schema absence should be interpreted cautiously.",
                    page=p,
                    evidence_json=json.dumps({"schema_parse_error_count": int(p.get("schema_parse_error_count") or 0)}),
                )
            )

        schema_payload = _json_object(str(p.get("schema_validation_json") or "{}"))
        missing_required = schema_payload.get("missing_required_by_feature")
        if is_indexable_page and isinstance(missing_required, dict):
            for feature_key, fields in sorted(missing_required.items()):
                fields_list = [str(field).strip() for field in (fields or []) if str(field).strip()]
                if not fields_list:
                    continue
                issues.append(
                    _issue(
                        run_id,
                        url,
                        "high" if page_importance >= 1.10 else "medium",
                        "SCHEMA_FEATURE_MISSING_REQUIRED",
                        "Schema feature missing required fields",
                        "Structured-data feature is present but missing required fields for eligibility.",
                        page=p,
                        evidence_json=json.dumps(
                            {
                                "feature": str(feature_key),
                                "missing_required_fields": fields_list,
                            },
                            sort_keys=True,
                        ),
                    )
                )

        missing_recommended = schema_payload.get("missing_recommended_by_feature")
        if is_indexable_page and isinstance(missing_recommended, dict):
            for feature_key, fields in sorted(missing_recommended.items()):
                fields_list = [str(field).strip() for field in (fields or []) if str(field).strip()]
                if not fields_list:
                    continue
                issues.append(
                    _issue(
                        run_id,
                        url,
                        "low",
                        "SCHEMA_FEATURE_RECOMMENDED_GAPS",
                        "Schema feature missing recommended fields",
                        "Structured-data feature has recommended field gaps that may reduce enhancement quality.",
                        page=p,
                        evidence_json=json.dumps(
                            {
                                "feature": str(feature_key),
                                "missing_recommended_fields": fields_list,
                            },
                            sort_keys=True,
                        ),
                    )
                )

        deprecated_features = schema_payload.get("deprecated_features")
        if is_actionable_page and isinstance(deprecated_features, list) and deprecated_features:
            issues.append(
                _issue(
                    run_id,
                    url,
                    "medium",
                    "SCHEMA_DEPRECATED_MARKUP",
                    "Deprecated structured-data markup detected",
                    "Deprecated structured-data types were found and should be migrated.",
                    page=p,
                    evidence_json=json.dumps(
                        {
                            "deprecated_features": [
                                row for row in deprecated_features if isinstance(row, dict)
                            ][:12],
                        },
                        sort_keys=True,
                    ),
                )
            )

        visible_mismatches = schema_payload.get("visible_content_mismatches")
        if is_indexable_page and isinstance(visible_mismatches, list) and visible_mismatches:
            issues.append(
                _issue(
                    run_id,
                    url,
                    "low",
                    "SCHEMA_VISIBLE_CONTENT_MISMATCH",
                    "Structured-data values not visible in page content",
                    "One or more schema values did not match visible page text and may require verification.",
                    page=p,
                    evidence_json=json.dumps(
                        {
                            "visible_content_mismatches": [
                                row for row in visible_mismatches if isinstance(row, dict)
                            ][:12],
                        },
                        sort_keys=True,
                    ),
                )
            )

        if is_indexable_page:
            duplicate_urls = duplicate_content_by_url.get(str(url), [])
            if duplicate_urls:
                issues.append(
                    _issue(
                        run_id,
                        url,
                        "medium",
                        "EXACT_CONTENT_DUPLICATE",
                        "Exact duplicate content",
                        "Visible page text exactly matches other indexable pages after normalization.",
                        page=p,
                        evidence_json=json.dumps(
                            {
                                "content_hash": str(p.get("effective_content_hash") or p.get("content_hash") or ""),
                                "duplicate_count": len(duplicate_urls),
                                "sample_urls": duplicate_urls[:8],
                            }
                        ),
                    )
                )

        hreflang_targets = hreflang_targets_by_page.get(str(url), {})
        if is_indexable_page and hreflang_targets and not suppress_hreflang_page_symptoms:
            reciprocity_missing: list[str] = []
            for target in hreflang_targets:
                if target not in crawled_urls:
                    continue
                target_links = hreflang_targets_by_page.get(target, {})
                if str(url) not in target_links:
                    reciprocity_missing.append(target)
            if reciprocity_missing:
                issues.append(
                    _issue(
                        run_id,
                        url,
                        "medium",
                        "HREFLANG_RECIPROCITY_MISSING",
                        "Hreflang reciprocity missing",
                        "One or more hreflang target pages do not point back to this URL.",
                        page=p,
                        evidence_json=json.dumps({"missing_reciprocal_targets": reciprocity_missing[:8]}),
                    )
                )

        if is_actionable_page:
            params = _query_params(str(url))
            query_keys = {key.strip().lower() for key, _ in params if key.strip()}
            if len(params) >= FACET_QUERY_PARAM_THRESHOLD and query_keys.intersection(faceted_keys):
                issues.append(
                    _issue(
                        run_id,
                        url,
                        "low",
                        "FACETED_NAVIGATION_RISK",
                        "Faceted URL crawl risk",
                        "Parameter-heavy faceted URLs can create crawl bloat and dilute index focus.",
                        page=p,
                        evidence_json=json.dumps({"query_params": sorted(query_keys)}),
                    )
                )
            if query_keys.intersection(pagination_keys) and not (p.get("rel_next_url") or p.get("rel_prev_url")):
                issues.append(
                    _issue(
                        run_id,
                        url,
                        "low",
                        "PAGINATION_SIGNAL_MISSING",
                        "Pagination hints missing",
                        "Pagination parameters were detected but rel=next/prev hints were not found.",
                        page=p,
                        evidence_json=json.dumps({"query_params": sorted(query_keys)}),
                    )
                )

        if is_indexable_page and eff_thin:
            prov = "both" if raw_thin else "rendered_only"
            issues.append(
                _issue(
                    run_id,
                    url,
                    "medium",
                    "THIN_CONTENT",
                    "Thin content",
                    "Body content appears too thin for this page type.",
                    page=p,
                    issue_provenance=prov,
                    evidence_json=json.dumps(
                        {
                            "page_type": _page_type(p),
                            "threshold": thin_threshold,
                            "effective_text_len": eff_text_len,
                            "raw_text_len": raw_text_len,
                        }
                    ),
                )
            )
        elif is_indexable_page and raw_thin and p.get("used_render") and not suppress_raw_only_diagnostics:
            issues.append(_issue(run_id, url, "low", "RAW_ONLY_THIN_CONTENT", "Raw HTML thin content", "Raw HTML appeared thin but rendered DOM had sufficient content.", page=p, issue_provenance="raw_only"))
        if is_indexable_page and int(p.get("render_gap_score") or 0) >= HIGH_RENDER_GAP_THRESHOLD:
            issues.append(_issue(run_id, url, "high", "RENDER_GAP_HIGH", "High render gap", p.get("render_gap_reason", ""), page=p))
        if is_actionable_page and p.get("orphan_risk_flag"):
            issues.append(_issue(run_id, url, "medium", "ORPHAN_RISK", "Orphan risk", "No meaningful inlinks detected.", page=p, evidence_json=json.dumps({"inlinks": p.get("inlinks", 0)}), issue_provenance="both"))
        if is_actionable_page and eff_low_links and not p.get("fetch_error"):
            prov = "both" if raw_low_links else "rendered_only"
            issues.append(
                _issue(
                    run_id,
                    url,
                    "low",
                    "LOW_INTERNAL_LINKS",
                    "Low internal links",
                    "Page has weak internal outgoing link coverage for its page type.",
                    page=p,
                    issue_provenance=prov,
                    evidence_json=json.dumps(
                        {
                            "page_type": _page_type(p),
                            "effective_internal_links_out": eff_internal_outlinks,
                            "effective_band": eff_link_band,
                            "raw_internal_links_out": raw_internal_outlinks,
                            "raw_band": raw_link_band,
                        }
                    ),
                )
            )
        elif is_actionable_page and raw_low_links and p.get("used_render") and not suppress_raw_only_diagnostics:
            issues.append(_issue(run_id, url, "low", "RAW_ONLY_LOW_INTERNAL_LINKS", "Raw HTML low internal links", "Raw HTML had very few internal links but rendered DOM improved coverage.", page=p, issue_provenance="raw_only"))

        if (
            is_indexable_page
            and page_importance >= 1.20
            and weak_pagerank_cutoff > 0.0
            and internal_pagerank > 0.0
            and internal_pagerank <= weak_pagerank_cutoff
            and (int(p.get("inlinks") or 0) <= 1 or eff_internal_outlinks <= 2)
        ):
            issues.append(
                _issue(
                    run_id,
                    url,
                    "high" if page_importance >= 1.30 else "medium",
                    "IMPORTANT_PAGE_WEAK_SUPPORT",
                    "Important page has weak internal support",
                    "A high-importance page has low internal authority flow compared with the rest of the site graph.",
                    page=p,
                    evidence_json=json.dumps(
                        {
                            "page_type": page_type,
                            "page_importance": round(page_importance, 2),
                            "internal_pagerank": round(internal_pagerank, 6),
                            "weak_pagerank_cutoff": round(weak_pagerank_cutoff, 6),
                            "inlinks": int(p.get("inlinks") or 0),
                            "effective_internal_links_out": eff_internal_outlinks,
                            "betweenness": round(betweenness, 6),
                            "closeness": round(closeness, 6),
                            "community_id": community_id,
                        }
                    ),
                )
            )

        if (
            is_actionable_page
            and bridge_flag
            and hub_betweenness_cutoff > 0.0
            and betweenness >= hub_betweenness_cutoff
        ):
            issues.append(
                _issue(
                    run_id,
                    url,
                    "high" if page_importance >= 1.20 else "medium",
                    "INTERNAL_FLOW_HUB_OVERLOAD",
                    "Single hub page carries too much internal flow",
                    "This page acts as a bridge hub with high betweenness centrality and may be a single point of architecture risk.",
                    page=p,
                    evidence_json=json.dumps(
                        {
                            "page_type": page_type,
                            "page_importance": round(page_importance, 2),
                            "bridge_flag": int(bridge_flag),
                            "betweenness": round(betweenness, 6),
                            "hub_betweenness_cutoff": round(hub_betweenness_cutoff, 6),
                            "internal_pagerank": round(internal_pagerank, 6),
                            "inlinks": int(p.get("inlinks") or 0),
                            "effective_internal_links_out": eff_internal_outlinks,
                            "community_id": community_id,
                        }
                    ),
                )
            )

        representative = community_representative.get(community_id)
        if (
            is_actionable_page
            and community_id > 0
            and primary_community_id > 0
            and community_id != primary_community_id
            and community_counts.get(community_id, 0) >= 2
            and representative is not None
            and representative[1] == str(url)
            and community_id not in emitted_disconnected_communities
        ):
            emitted_disconnected_communities.add(community_id)
            issues.append(
                _issue(
                    run_id,
                    url,
                    "medium",
                    "INTERNAL_CLUSTER_DISCONNECTED",
                    "Cluster appears disconnected from primary architecture",
                    "A page cluster outside the primary internal-link community may be under-connected to core service pages.",
                    page=p,
                    evidence_json=json.dumps(
                        {
                            "community_id": community_id,
                            "community_size": int(community_counts.get(community_id, 0)),
                            "primary_community_id": primary_community_id,
                            "primary_community_size": int(community_counts.get(primary_community_id, 0)),
                            "representative_url": str(url),
                            "page_importance": round(page_importance, 2),
                        }
                    ),
                )
            )

    for canonical_value, members in shell_canonical_groups.items():
        unique_urls = sorted({str(member.get("normalized_url") or "") for member in members if str(member.get("normalized_url") or "")})
        if len(unique_urls) < 3:
            continue
        representative = members[0]
        issues.append(
            _issue(
                run_id,
                str(representative.get("normalized_url") or ""),
                "medium",
                "STATIC_SHELL_CANONICAL_REUSED_ACROSS_ROUTES",
                "Static shell canonical reused across routes",
                "Shell-confirmed routes reused the same raw canonical signal across multiple distinct URLs.",
                page=representative,
                evidence_json=json.dumps(
                    {
                        "raw_canonical": canonical_value,
                        "affected_routes": len(unique_urls),
                        "sample_urls": unique_urls[:12],
                    }
                ),
                issue_provenance="raw_only",
            )
        )

    for hreflang_signature, members in shell_hreflang_groups.items():
        unique_urls = sorted({str(member.get("normalized_url") or "") for member in members if str(member.get("normalized_url") or "")})
        if len(unique_urls) < 3:
            continue
        representative = members[0]
        issues.append(
            _issue(
                run_id,
                str(representative.get("normalized_url") or ""),
                "medium",
                "STATIC_SHELL_HREFLANG_REUSED_ACROSS_ROUTES",
                "Static shell hreflang reused across routes",
                "Shell-confirmed routes reused identical raw hreflang payloads across multiple distinct URLs.",
                page=representative,
                evidence_json=json.dumps(
                    {
                        "raw_hreflang_signature": hreflang_signature,
                        "affected_routes": len(unique_urls),
                        "sample_urls": unique_urls[:12],
                    }
                ),
                issue_provenance="raw_only",
            )
        )

    canonical_clusters: dict[str, list[dict]] = defaultdict(list)
    for page in actionable_pages:
        cluster_key = str(page.get("canonical_cluster_key") or page.get("normalized_url") or "").strip()
        if not cluster_key:
            continue
        canonical_clusters[cluster_key].append(page)

    for cluster_key, members in canonical_clusters.items():
        if len(members) <= 1:
            continue
        final_urls = sorted(
            {
                normalize_url(str(member.get("final_url") or member.get("normalized_url") or ""))
                for member in members
                if str(member.get("final_url") or member.get("normalized_url") or "").strip()
            }
        )
        if len(final_urls) <= 1:
            continue
        representative = members[0]
        issues.append(
            _issue(
                run_id,
                str(representative.get("normalized_url") or ""),
                "medium",
                "CLUSTER_CANONICAL_COLLISION",
                "Canonical cluster collision",
                "Multiple final URLs map into the same canonical cluster key and may indicate cluster-level canonical collisions.",
                page=representative,
                evidence_json=json.dumps(
                    {
                        "canonical_cluster_key": cluster_key,
                        "member_count": len(members),
                        "unique_final_urls": len(final_urls),
                        "sample_final_urls": final_urls[:12],
                    }
                ),
            )
        )

    host_hash_clusters: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for page in actionable_pages:
        content_hash = str(page.get("effective_content_hash") or page.get("content_hash") or "").strip()
        if not content_hash:
            continue
        final_url = str(page.get("final_url") or page.get("normalized_url") or "")
        host = str(urlsplit(final_url).hostname or "").strip().lower()
        normalized_url = str(page.get("normalized_url") or "").strip()
        if not host or not normalized_url:
            continue
        host_hash_clusters[content_hash][host].add(normalized_url)

    for content_hash, hosts in host_hash_clusters.items():
        if len(hosts) <= 1:
            continue
        representative_host = sorted(hosts.keys())[0]
        representative_url = sorted(hosts[representative_host])[0]
        representative_page = next(
            (
                page
                for page in actionable_pages
                if str(page.get("normalized_url") or "") == representative_url
            ),
            None,
        )
        if representative_page is None:
            continue
        issues.append(
            _issue(
                run_id,
                representative_url,
                "medium",
                "HOST_DUPLICATION_CLUSTER",
                "Cross-host duplication cluster",
                "Equivalent effective content hashes were observed across multiple hosts.",
                page=representative_page,
                evidence_json=json.dumps(
                    {
                        "content_hash": content_hash,
                        "host_count": len(hosts),
                        "hosts": {host: sorted(urls)[:6] for host, urls in sorted(hosts.items())},
                    }
                ),
            )
        )

    _enrich_issue_context(issues, pages)
    return issues
