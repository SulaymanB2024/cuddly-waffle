from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

from seo_audit.preview_controls import preview_restriction_score, snippet_eligible
from seo_audit.scoring_policy import page_importance_for_page, resolve_page_type


@dataclass(slots=True)
class GovernanceBotState:
    crawl_allowed: bool
    snippet_eligible: bool
    noindex_present: bool


_PREVIEW_INTENTIONAL_PAGE_TYPES = {"utility", "search", "legal", "privacy", "terms"}
_PREVIEW_IMPORTANT_PAGE_TYPES = {"homepage", "service", "location", "product"}


def _likely_intentional_preview_policy(page_type: str, page_importance: float, noindex_present: bool) -> bool:
    if noindex_present:
        return True
    return page_type in _PREVIEW_INTENTIONAL_PAGE_TYPES and page_importance < 1.10


def build_governance_matrix(page: dict) -> dict[str, object]:
    is_snippet_eligible = snippet_eligible(page)
    noindex_present = int(page.get("is_noindex") or 0) == 1
    page_type = resolve_page_type(page, infer_from_url=True, fallback="other", none_fallback="other")
    page_importance = page_importance_for_page(
        page,
        infer_from_url=True,
        fallback="other",
        none_fallback="other",
        boost_other_type_for_segments=True,
    )

    matrix = {
        "googlebot": {
            "crawl_allowed": int(page.get("governance_googlebot_allowed", 1) or 0) == 1,
            "snippet_eligible": is_snippet_eligible,
            "noindex_present": noindex_present,
        },
        "bingbot": {
            "crawl_allowed": int(page.get("governance_bingbot_allowed", 1) or 0) == 1,
            "snippet_eligible": is_snippet_eligible,
            "noindex_present": noindex_present,
        },
        "oai_searchbot": {
            "crawl_allowed": int(page.get("governance_openai_allowed", 1) or 0) == 1,
            "snippet_eligible": is_snippet_eligible,
            "noindex_present": noindex_present,
        },
        "google_extended": {
            "crawl_allowed": int(page.get("governance_google_extended_allowed", 1) or 0) == 1,
            "snippet_eligible": is_snippet_eligible,
            "noindex_present": noindex_present,
        },
        "gptbot": {
            "crawl_allowed": int(page.get("governance_gptbot_allowed", 1) or 0) == 1,
            "snippet_eligible": is_snippet_eligible,
            "noindex_present": noindex_present,
        },
        "oai_adsbot": {
            "crawl_allowed": int(page.get("governance_oai_adsbot_allowed", 1) or 0) == 1,
            "snippet_eligible": is_snippet_eligible,
            "noindex_present": noindex_present,
        },
        "chatgpt_user": {
            "crawl_allowed": int(page.get("governance_chatgpt_user_allowed", 1) or 0) == 1,
            "snippet_eligible": is_snippet_eligible,
            "noindex_present": noindex_present,
            "informational": True,
        },
        "preview_controls": {
            "has_nosnippet": int(page.get("has_nosnippet_directive") or 0),
            "max_snippet": str(page.get("max_snippet_directive") or ""),
            "max_image_preview": str(page.get("max_image_preview_directive") or ""),
            "max_video_preview": str(page.get("max_video_preview_directive") or ""),
            "data_nosnippet_count": int(page.get("data_nosnippet_count") or 0),
            "page_type": page_type,
            "page_importance": round(float(page_importance), 2),
            "snippet_eligible": is_snippet_eligible,
            "noindex_present": noindex_present,
        },
        "raw_render_visibility_mismatch": {
            "noindex": int(page.get("is_noindex") or 0) != int(page.get("rendered_is_noindex", page.get("is_noindex", 0)) or 0),
            "preview_controls": str(page.get("meta_robots") or "") != str(page.get("rendered_meta_robots") or ""),
        },
    }
    return matrix


def summarize_governance_matrices(matrices: list[dict[str, object]]) -> dict[str, object]:
    counts = Counter()
    preview_restricted = 0
    preview_restricted_diagnostic = 0
    over_restrictive_like = 0
    bing_restrictive_like = 0
    mismatch_count = 0

    for matrix in matrices:
        for bot in (
            "googlebot",
            "bingbot",
            "oai_searchbot",
            "google_extended",
            "gptbot",
            "oai_adsbot",
            "chatgpt_user",
        ):
            state = matrix.get(bot, {})
            allowed = bool((state or {}).get("crawl_allowed", True))
            if not allowed:
                counts[f"{bot}_blocked"] += 1

        preview = matrix.get("preview_controls", {})
        preview_score, _reasons = preview_restriction_score(
            {
                "has_nosnippet_directive": int((preview or {}).get("has_nosnippet", 0) or 0),
                "max_snippet_directive": str((preview or {}).get("max_snippet") or ""),
                "max_image_preview_directive": str((preview or {}).get("max_image_preview") or ""),
                "max_video_preview_directive": str((preview or {}).get("max_video_preview") or ""),
                "data_nosnippet_count": int((preview or {}).get("data_nosnippet_count", 0) or 0),
            }
        )
        page_type = str((preview or {}).get("page_type") or "").strip().lower()
        page_importance = float((preview or {}).get("page_importance") or 0.0)
        snippet_is_eligible = bool((preview or {}).get("snippet_eligible", False))
        noindex_present = bool((preview or {}).get("noindex_present", False))
        preview_intentional = _likely_intentional_preview_policy(page_type, page_importance, noindex_present)

        if preview_score >= 4:
            preview_restricted_diagnostic += 1

        if snippet_is_eligible and not preview_intentional:
            if preview_score >= 5:
                over_restrictive_like += 1
            if preview_score >= 4 and (
                page_importance >= 1.00
                or page_type in _PREVIEW_IMPORTANT_PAGE_TYPES
            ):
                bing_restrictive_like += 1

        if (snippet_is_eligible and not preview_intentional) and (
            preview_score >= 5
            or (
                preview_score >= 4
                and (
                    page_importance >= 1.00
                    or page_type in _PREVIEW_IMPORTANT_PAGE_TYPES
                )
            )
        ):
            preview_restricted += 1

        mismatch = matrix.get("raw_render_visibility_mismatch", {})
        if bool((mismatch or {}).get("noindex")) or bool((mismatch or {}).get("preview_controls")):
            mismatch_count += 1

    return {
        "total": len(matrices),
        "blocked_counts": dict(counts),
        "preview_restricted_pages": preview_restricted,
        "preview_restricted_diagnostic_pages": preview_restricted_diagnostic,
        "preview_over_restrictive_issue_like_pages": over_restrictive_like,
        "preview_bing_restrictive_issue_like_pages": bing_restrictive_like,
        "raw_render_mismatch_pages": mismatch_count,
    }
