from __future__ import annotations

from dataclasses import dataclass, field
from urllib.parse import parse_qsl, urlsplit


@dataclass(slots=True)
class CitationAssessment:
    eligibility_score: int
    reasons: list[str] = field(default_factory=list)


def _as_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _answer_like_signal(page: dict) -> float:
    title = str(page.get("effective_title") or page.get("title") or "").strip()
    h1_count = _as_int(page.get("effective_h1_count", page.get("h1_count", 0)))
    text_len = _as_int(page.get("effective_text_len", page.get("word_count", 0)))

    score = 0.0
    if title:
        score += 0.35
    if h1_count > 0:
        score += 0.25
    if text_len >= 220:
        score += 0.40
    elif text_len >= 120:
        score += 0.20
    return min(1.0, score)


def compute_citation_eligibility(page: dict, governance_matrix: dict[str, object]) -> CitationAssessment:
    score = 100
    reasons: list[str] = []

    if _as_int(page.get("is_noindex"), 0) == 1:
        score -= 70
        reasons.append("noindex")

    bots = {
        "googlebot": governance_matrix.get("googlebot", {}),
        "bingbot": governance_matrix.get("bingbot", {}),
        "oai_searchbot": governance_matrix.get("oai_searchbot", {}),
    }
    for bot_name, state in bots.items():
        if not bool((state or {}).get("crawl_allowed", True)):
            score -= 24
            reasons.append(f"{bot_name}_blocked")

    preview_controls = governance_matrix.get("preview_controls", {})
    if int((preview_controls or {}).get("has_nosnippet", 0) or 0) == 1:
        score -= 30
        reasons.append("nosnippet")

    max_snippet = str((preview_controls or {}).get("max_snippet") or "").strip().lower()
    if max_snippet in {"0", "-1"}:
        score -= 16
        reasons.append("max_snippet_non_positive")

    if _as_int(page.get("effective_internal_links_out"), _as_int(page.get("internal_links_out"), 0)) < 2:
        score -= 12
        reasons.append("weak_internal_prominence")

    if _as_int(page.get("schema_validation_score"), 0) > 0:
        if _as_int(page.get("schema_validation_score"), 0) < 70:
            score -= 10
            reasons.append("schema_validation_weak")
        else:
            score += 5

    answer_signal = _answer_like_signal(page)
    score += int(round(answer_signal * 10))
    if answer_signal >= 0.7:
        reasons.append("answer_like_visible_content")

    score = max(0, min(100, score))
    return CitationAssessment(eligibility_score=score, reasons=reasons)


def has_chatgpt_referral_signal(url: str) -> bool:
    query_pairs = parse_qsl(urlsplit(url).query, keep_blank_values=True)
    for key, value in query_pairs:
        if key.strip().lower() == "utm_source" and value.strip().lower() == "chatgpt.com":
            return True
    return False


def build_citation_evidence(
    page: dict,
    *,
    gsc_impressions: int = 0,
    gsc_clicks: int = 0,
    chatgpt_referrals: int = 0,
) -> dict[str, object]:
    return {
        "url": str(page.get("normalized_url") or ""),
        "gsc_impressions": max(0, int(gsc_impressions)),
        "gsc_clicks": max(0, int(gsc_clicks)),
        "chatgpt_referrals": max(0, int(chatgpt_referrals)),
        "has_chatgpt_utm_hint": has_chatgpt_referral_signal(str(page.get("normalized_url") or "")),
    }
