from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import urlsplit

DEFAULT_SCORE_PROFILE = "default"
LEGACY_SCORE_VERSION = "1.0.0"
CURRENT_SCORE_VERSION = "1.1.0"
SCORE_FORMULA_ID = "quality-risk-cap-with-coverage"

PROFILE_FALLBACK_BY_SITE_TYPE: dict[str, str] = {
    "general": "general",
    "local": "local",
}

OTHER_TYPE_IMPORTANCE_BOOST_SEGMENTS = {
    "service",
    "services",
    "product",
    "products",
    "location",
    "locations",
}

PAGE_IMPORTANCE_BY_TYPE: dict[str, float] = {
    "homepage": 1.40,
    "service": 1.30,
    "location": 1.30,
    "product": 1.30,
    "contact": 1.25,
    "about": 1.10,
    "industry": 1.05,
    "article": 0.85,
    "blog": 0.85,
    "category": 0.80,
    "tag": 0.75,
    "archive": 0.75,
    "search": 0.70,
    "utility": 0.70,
    "other": 1.00,
    "global": 1.00,
}

THIN_CONTENT_THRESHOLD_BY_TYPE: dict[str, int] = {
    "homepage": 220,
    "service": 220,
    "location": 220,
    "product": 220,
    "article": 170,
    "blog": 170,
    "industry": 170,
    "contact": 80,
    "about": 80,
    "tag": 70,
    "category": 70,
    "archive": 70,
    "utility": 70,
    "search": 70,
}
DEFAULT_THIN_CONTENT_THRESHOLD = 140

INTERNAL_LINK_BAND_THRESHOLDS: dict[str, tuple[int, int, int]] = {
    "homepage": (1, 3, 6),
    "service": (1, 3, 5),
    "location": (1, 3, 5),
    "product": (1, 3, 5),
    "article": (0, 2, 4),
    "blog": (0, 2, 4),
    "tag": (0, 2, 3),
    "category": (0, 2, 3),
    "archive": (0, 2, 3),
    "utility": (0, 2, 3),
    "search": (0, 2, 3),
    "contact": (0, 2, 3),
    "about": (0, 2, 3),
}
DEFAULT_INTERNAL_LINK_BAND = (1, 2, 4)

UTILITY_LIKE_PAGE_TYPES = {
    "tag",
    "category",
    "archive",
    "utility",
    "search",
    "contact",
    "about",
    "legal",
    "privacy",
    "terms",
}

MONEY_PAGE_TYPES = {
    "homepage",
    "service",
    "location",
    "product",
}

INTERNAL_ARCHITECTURE_BASE_SCORE_BY_BAND: dict[str, int] = {
    "severe": 40,
    "weak": 65,
    "okay": 82,
    "strong": 100,
}

RISK_FAMILY_DECAY_FACTOR = 0.50
RISK_FAMILY_BREADTH_BONUS_PER_FAMILY = 2.8
RISK_FAMILY_BREADTH_BONUS_MAX = 16.0
RISK_SERIOUS_FAMILY_COMPONENT_THRESHOLD = 30.0
RISK_SERIOUS_FAMILY_BONUS_PER_FAMILY = 2.5
RISK_SERIOUS_FAMILY_BONUS_MAX = 8.0

ISSUE_RISK_FAMILY_BY_CODE: dict[str, str] = {
    "FETCH_FAILED": "access",
    "ACCESS_AUTH_BLOCKED": "access",
    "REDIRECT_TO_ERROR": "access",
    "REDIRECT_CHAIN_LONG": "access",
    "ROBOTS_BLOCKED_URL": "access",
    "ROBOTS_NOINDEX_CONFLICT": "access",
    "NOINDEX": "indexability",
    "MISSING_TITLE": "content_quality",
    "MISSING_META_DESCRIPTION": "content_quality",
    "MISSING_H1": "content_quality",
    "THIN_CONTENT": "content_quality",
    "STRUCTURED_DATA_PARSE_FAILED": "content_quality",
    "LOW_INTERNAL_LINKS": "internal_linking",
    "ORPHAN_RISK": "internal_linking",
    "IMPORTANT_PAGE_WEAK_SUPPORT": "internal_linking",
    "INTERNAL_FLOW_HUB_OVERLOAD": "internal_linking",
    "INTERNAL_CLUSTER_DISCONNECTED": "discovery",
    "MISSING_CANONICAL": "canonicalization",
    "CANONICAL_MISMATCH": "canonicalization",
    "DUPLICATE_CANONICAL_TAGS": "canonicalization",
    "HREFLANG_RECIPROCITY_MISSING": "canonicalization",
    "PAGINATION_SIGNAL_MISSING": "canonicalization",
    "RENDER_GAP_HIGH": "rendering",
    "RENDER_UNAVAILABLE": "rendering",
    "SITEMAP_URL_NOT_CRAWLED": "discovery",
    "CRAWLED_URL_NOT_IN_SITEMAP": "discovery",
    "SITEMAP_URL_BLOCKED_BY_ROBOTS": "discovery",
    "DISCOVERY_BLIND_SPOT": "discovery",
    "FACETED_NAVIGATION_RISK": "discovery",
    "PERFORMANCE_PROVIDER_ERROR": "serving",
    "CRUX_PROVIDER_ERROR": "serving",
    "LIGHTHOUSE_BUDGET_FAIL": "serving",
    "GSC_INDEX_STATE_NOT_INDEXED": "indexability",
    "OPENAI_SEARCHBOT_BLOCKED": "discovery",
    "GOOGLE_EXTENDED_BLOCKED": "discovery",
    "BING_PREVIEW_CONTROLS_RESTRICTIVE": "indexability",
    "OVER_RESTRICTIVE_SNIPPET_CONTROLS": "indexability",
    "RAW_RENDER_NOINDEX_MISMATCH": "rendering",
    "RAW_RENDER_PREVIEW_CONTROL_MISMATCH": "rendering",
}

# Pagination hint findings stay diagnostic and have intentionally low score pressure.
RISK_DAMPENING_BY_ISSUE_CODE: dict[str, float] = {
    "PAGINATION_SIGNAL_MISSING": 0.15,
}

HIGH_IMPORTANCE_HARD_BLOCKER_GATES = {"access", "indexability"}
HIGH_IMPORTANCE_CAP_55_ISSUE_CODES = {
    "NOINDEX",
    "CANONICAL_MISMATCH",
    "ROBOTS_NOINDEX_CONFLICT",
    "REDIRECT_TO_ERROR",
    "OPENAI_SEARCHBOT_BLOCKED",
    "RAW_RENDER_NOINDEX_MISMATCH",
}
BASELINE_CAP_75_ISSUE_CODES = {
    "MISSING_TITLE",
    "MISSING_H1",
    "THIN_CONTENT",
    "MISSING_META_DESCRIPTION",
    "LOW_INTERNAL_LINKS",
    "BING_PREVIEW_CONTROLS_RESTRICTIVE",
    "OVER_RESTRICTIVE_SNIPPET_CONTROLS",
}


def normalize_page_type(value: object, *, fallback: str = "other") -> str:
    normalized = str(value or "").strip().lower()
    if normalized:
        return normalized
    fallback_normalized = str(fallback or "").strip().lower()
    return fallback_normalized or "other"


def normalize_score_profile(profile: object | None, *, site_type: object | None = None) -> str:
    normalized = str(profile or "").strip().lower()
    if normalized:
        return normalized

    site = str(site_type or "").strip().lower()
    if site in PROFILE_FALLBACK_BY_SITE_TYPE:
        return PROFILE_FALLBACK_BY_SITE_TYPE[site]
    return DEFAULT_SCORE_PROFILE


def first_path_segment(url: str) -> str:
    path = (urlsplit(str(url or "")).path or "").strip("/").lower()
    if not path:
        return ""
    return path.split("/", 1)[0]


def infer_page_type_from_url(url: str, *, fallback: str = "other") -> str:
    segment = first_path_segment(url)
    if not segment:
        return "homepage"
    if segment in {"service", "services"}:
        return "service"
    if segment in {"product", "products", "portfolio", "fasteners"}:
        return "product"
    if segment in {"location", "locations"}:
        return "location"
    if segment in {"blog", "news", "article", "articles"}:
        return "article"
    if segment in {"tag", "category", "archive", "search", "utility"}:
        return segment
    return normalize_page_type(fallback)


def resolve_page_type(
    page: Mapping[str, object] | None,
    *,
    infer_from_url: bool,
    fallback: str = "other",
    none_fallback: str = "other",
) -> str:
    if not page:
        return normalize_page_type(none_fallback)

    explicit = str(page.get("page_type") or "").strip().lower()
    if explicit:
        return explicit

    if infer_from_url:
        url = str(page.get("normalized_url") or page.get("final_url") or "")
        return infer_page_type_from_url(url, fallback=fallback)

    return normalize_page_type(fallback)


def page_importance_for_type(page_type: str) -> float:
    value = float(PAGE_IMPORTANCE_BY_TYPE.get(normalize_page_type(page_type), PAGE_IMPORTANCE_BY_TYPE["other"]))
    return max(0.5, min(1.5, value))


def page_importance_for_page(
    page: Mapping[str, object] | None,
    *,
    infer_from_url: bool,
    fallback: str = "other",
    none_fallback: str = "other",
    boost_other_type_for_segments: bool = False,
) -> float:
    page_type = resolve_page_type(
        page,
        infer_from_url=infer_from_url,
        fallback=fallback,
        none_fallback=none_fallback,
    )
    base = page_importance_for_type(page_type)

    if boost_other_type_for_segments and page and page_type == "other":
        url = str(page.get("normalized_url") or page.get("final_url") or "")
        segment = first_path_segment(url)
        if segment in OTHER_TYPE_IMPORTANCE_BOOST_SEGMENTS:
            base = max(base, 1.25)

    return max(0.5, min(1.5, float(base)))


def thin_content_threshold_for_page_type(page_type: str) -> int:
    return int(THIN_CONTENT_THRESHOLD_BY_TYPE.get(normalize_page_type(page_type), DEFAULT_THIN_CONTENT_THRESHOLD))


def thin_content_threshold_for_page(
    page: Mapping[str, object] | None,
    *,
    infer_from_url: bool,
    fallback: str = "other",
    none_fallback: str = "other",
) -> int:
    page_type = resolve_page_type(
        page,
        infer_from_url=infer_from_url,
        fallback=fallback,
        none_fallback=none_fallback,
    )
    return thin_content_threshold_for_page_type(page_type)


def internal_link_band_for_page_type(page_type: str, outlinks: int) -> str:
    count = max(0, int(outlinks or 0))
    severe_max, weak_max, okay_max = INTERNAL_LINK_BAND_THRESHOLDS.get(
        normalize_page_type(page_type),
        DEFAULT_INTERNAL_LINK_BAND,
    )
    if count <= severe_max:
        return "severe"
    if count <= weak_max:
        return "weak"
    if count <= okay_max:
        return "okay"
    return "strong"


def internal_link_band_for_page(
    page: Mapping[str, object] | None,
    outlinks: int,
    *,
    infer_from_url: bool,
    fallback: str = "other",
    none_fallback: str = "other",
) -> str:
    page_type = resolve_page_type(
        page,
        infer_from_url=infer_from_url,
        fallback=fallback,
        none_fallback=none_fallback,
    )
    return internal_link_band_for_page_type(page_type, outlinks)


def issue_risk_family(issue_code: str) -> str:
    normalized = str(issue_code or "").strip().upper()
    if normalized.startswith("RAW_ONLY_"):
        normalized = normalized.removeprefix("RAW_ONLY_")
    return ISSUE_RISK_FAMILY_BY_CODE.get(normalized, "general")


def thin_content_penalty_for_page(
    page: Mapping[str, object] | None,
    effective_text_len: int,
    *,
    infer_from_url: bool,
    fallback: str = "other",
    none_fallback: str = "other",
) -> tuple[int, dict[str, object]]:
    page_type = resolve_page_type(
        page,
        infer_from_url=infer_from_url,
        fallback=fallback,
        none_fallback=none_fallback,
    )
    threshold = thin_content_threshold_for_page_type(page_type)
    text_len = max(0, int(effective_text_len or 0))

    if page_type in MONEY_PAGE_TYPES:
        max_penalty = 32
        near_empty_penalty = 40
    elif page_type in UTILITY_LIKE_PAGE_TYPES:
        max_penalty = 18
        near_empty_penalty = 28
    else:
        max_penalty = 26
        near_empty_penalty = 34

    near_empty_cutoff = max(20, int(round(threshold * 0.12)))
    if text_len <= near_empty_cutoff:
        return near_empty_penalty, {
            "page_type": page_type,
            "threshold": threshold,
            "effective_text_len": text_len,
            "ratio": round(text_len / float(max(1, threshold)), 4),
            "mode": "near_empty_floor",
            "near_empty_cutoff": near_empty_cutoff,
            "max_penalty": max_penalty,
            "penalty": near_empty_penalty,
        }

    ratio = text_len / float(max(1, threshold))
    if ratio >= 1.0:
        return 0, {
            "page_type": page_type,
            "threshold": threshold,
            "effective_text_len": text_len,
            "ratio": round(ratio, 4),
            "mode": "above_threshold",
            "near_empty_cutoff": near_empty_cutoff,
            "max_penalty": max_penalty,
            "penalty": 0,
        }

    normalized_gap = max(0.0, 1.0 - ratio)
    penalty = int(round((normalized_gap ** 0.70) * max_penalty))
    penalty = max(0, min(near_empty_penalty, penalty))
    return penalty, {
        "page_type": page_type,
        "threshold": threshold,
        "effective_text_len": text_len,
        "ratio": round(ratio, 4),
        "mode": "continuous_curve",
        "curve": "power_0_70",
        "near_empty_cutoff": near_empty_cutoff,
        "max_penalty": max_penalty,
        "penalty": penalty,
    }


def internal_architecture_score_for_page(
    page: Mapping[str, object] | None,
    outlinks: int,
    *,
    infer_from_url: bool,
    fallback: str = "other",
    none_fallback: str = "other",
) -> tuple[int, dict[str, object]]:
    def _as_float(value: object) -> float:
        try:
            return float(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return 0.0

    page_type = resolve_page_type(
        page,
        infer_from_url=infer_from_url,
        fallback=fallback,
        none_fallback=none_fallback,
    )
    band = internal_link_band_for_page_type(page_type, outlinks)
    base = int(INTERNAL_ARCHITECTURE_BASE_SCORE_BY_BAND.get(band, 82))
    score = base

    inlinks = int((page or {}).get("inlinks") or 0)
    depth_value = (page or {}).get("crawl_depth")
    nav_linked = int((page or {}).get("nav_linked_flag") or 0) == 1
    orphan_risk = int((page or {}).get("orphan_risk_flag") or 0) == 1
    internal_pagerank = _as_float((page or {}).get("internal_pagerank"))
    betweenness = _as_float((page or {}).get("betweenness"))
    closeness = _as_float((page or {}).get("closeness"))
    bridge_flag = int((page or {}).get("bridge_flag") or 0) == 1
    page_importance = page_importance_for_page(
        page,
        infer_from_url=infer_from_url,
        fallback=fallback,
        none_fallback=none_fallback,
    )
    is_homepage = page_type == "homepage"

    adjustments: list[dict[str, object]] = []

    def add_adjustment(reason: str, delta: int, **metadata: object) -> None:
        nonlocal score
        if delta == 0:
            return
        score += delta
        entry: dict[str, object] = {"reason": reason, "delta": delta}
        entry.update(metadata)
        adjustments.append(entry)

    if orphan_risk and not is_homepage:
        add_adjustment("orphan_risk", -24)

    if not is_homepage:
        if inlinks == 0:
            add_adjustment("no_inlinks", -14)
        elif inlinks == 1:
            add_adjustment("single_inlink", -8)
        elif inlinks >= 5:
            add_adjustment("strong_inlink_support", 4)

    if depth_value is not None and depth_value != "":
        try:
            depth = int(depth_value)
        except (TypeError, ValueError):
            depth = None
        if depth is not None:
            if depth >= 6:
                add_adjustment("deep_page", -14, crawl_depth=depth)
            elif depth >= 4:
                add_adjustment("moderately_deep_page", -8, crawl_depth=depth)
            elif depth <= 1 and not is_homepage:
                add_adjustment("near_navigation_root", 3, crawl_depth=depth)
    elif not is_homepage:
        add_adjustment("unknown_depth", -6)

    if not nav_linked and page_importance >= 1.20:
        add_adjustment("high_importance_not_nav_linked", -8)
    elif nav_linked and page_importance >= 1.20:
        add_adjustment("high_importance_nav_linked", 3)

    if outlinks >= 8:
        add_adjustment("strong_internal_outlinking", 4)
    elif outlinks == 0 and not is_homepage:
        add_adjustment("no_internal_outlinks", -8)

    graph_metrics_available = any(
        [
            internal_pagerank > 0.0,
            betweenness > 0.0,
            closeness > 0.0,
            bridge_flag,
        ]
    )

    if graph_metrics_available:
        if page_importance >= 1.20 and internal_pagerank > 0.0:
            if internal_pagerank < 0.012:
                add_adjustment(
                    "important_page_low_pagerank_support",
                    -8,
                    internal_pagerank=round(internal_pagerank, 6),
                )
            elif internal_pagerank >= 0.060:
                add_adjustment(
                    "important_page_strong_pagerank_support",
                    4,
                    internal_pagerank=round(internal_pagerank, 6),
                )

        if page_importance >= 1.20 and closeness > 0.0 and closeness < 0.18:
            add_adjustment(
                "important_page_low_closeness",
                -4,
                closeness=round(closeness, 6),
            )

        if bridge_flag and betweenness >= 0.12:
            add_adjustment(
                "hub_overload_bridge_high_betweenness",
                -8,
                betweenness=round(betweenness, 6),
            )
        elif bridge_flag and betweenness >= 0.06:
            add_adjustment(
                "hub_overload_bridge_moderate_betweenness",
                -4,
                betweenness=round(betweenness, 6),
            )

        if page_importance >= 1.20 and betweenness > 0.0 and betweenness <= 0.01 and inlinks <= 1:
            add_adjustment(
                "important_page_low_flow_position",
                -3,
                betweenness=round(betweenness, 6),
                inlinks=inlinks,
            )

    score = max(0, min(100, int(score)))
    return score, {
        "page_type": page_type,
        "page_importance": round(page_importance, 2),
        "internal_link_band": band,
        "base_score": base,
        "signal_inputs": {
            "effective_internal_links_out": int(max(0, int(outlinks or 0))),
            "inlinks": inlinks,
            "crawl_depth": depth_value,
            "nav_linked_flag": int(nav_linked),
            "orphan_risk_flag": int(orphan_risk),
            "internal_pagerank": round(internal_pagerank, 6),
            "betweenness": round(betweenness, 6),
            "closeness": round(closeness, 6),
            "bridge_flag": int(bridge_flag),
        },
        "adjustments": adjustments,
        "score": score,
    }
