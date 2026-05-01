from __future__ import annotations

from collections.abc import Mapping


def directive_int(value: object) -> int | None:
    raw = str(value or "").strip().lower()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def preview_restriction_score(page: Mapping[str, object]) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []

    if int(page.get("has_nosnippet_directive") or 0) == 1:
        score += 4
        reasons.append("nosnippet")

    max_snippet_num = directive_int(page.get("max_snippet_directive"))
    if max_snippet_num is not None:
        if max_snippet_num == 0:
            score += 3
            reasons.append("max_snippet_zero")
        elif 0 < max_snippet_num <= 50:
            score += 2
            reasons.append("max_snippet_very_low")

    max_image_preview = str(page.get("max_image_preview_directive") or "").strip().lower()
    if max_image_preview == "none":
        score += 3
        reasons.append("max_image_preview_none")
    elif max_image_preview == "standard":
        score += 1
        reasons.append("max_image_preview_standard")

    max_video_num = directive_int(page.get("max_video_preview_directive"))
    if max_video_num is not None:
        if max_video_num == 0:
            score += 1
            reasons.append("max_video_preview_zero")
        elif 0 < max_video_num <= 5:
            score += 1
            reasons.append("max_video_preview_very_low")

    data_nosnippet_count = int(page.get("data_nosnippet_count") or 0)
    if data_nosnippet_count >= 10:
        score += 2
        reasons.append("heavy_data_nosnippet")
    elif data_nosnippet_count >= 4:
        score += 1
        reasons.append("moderate_data_nosnippet")

    return score, reasons


def preview_controls_restrictive(page: Mapping[str, object], *, threshold: int = 4) -> bool:
    score, _reasons = preview_restriction_score(page)
    return score >= max(0, int(threshold))


def snippet_eligible(page: Mapping[str, object]) -> bool:
    if int(page.get("is_noindex") or 0) == 1:
        return False
    if int(page.get("has_nosnippet_directive") or 0) == 1:
        return False

    max_snippet = directive_int(page.get("max_snippet_directive"))
    if max_snippet is not None and max_snippet == 0:
        return False

    max_image_preview = str(page.get("max_image_preview_directive") or "").strip().lower()
    if max_image_preview == "none":
        return False

    max_video_preview = directive_int(page.get("max_video_preview_directive"))
    if max_video_preview is not None and max_video_preview == 0:
        return False

    return True
