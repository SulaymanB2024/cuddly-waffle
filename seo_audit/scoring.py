from __future__ import annotations

import json
from typing import Any, Sequence

from seo_audit.policies import HIGH_RENDER_GAP_THRESHOLD
from seo_audit.scoring_policy import (
    BASELINE_CAP_75_ISSUE_CODES,
    CURRENT_SCORE_VERSION,
    DEFAULT_SCORE_PROFILE,
    HIGH_IMPORTANCE_CAP_55_ISSUE_CODES,
    HIGH_IMPORTANCE_HARD_BLOCKER_GATES,
    RISK_DAMPENING_BY_ISSUE_CODE,
    RISK_FAMILY_BREADTH_BONUS_MAX,
    RISK_FAMILY_BREADTH_BONUS_PER_FAMILY,
    RISK_FAMILY_DECAY_FACTOR,
    RISK_SERIOUS_FAMILY_BONUS_MAX,
    RISK_SERIOUS_FAMILY_BONUS_PER_FAMILY,
    RISK_SERIOUS_FAMILY_COMPONENT_THRESHOLD,
    SCORE_FORMULA_ID,
    internal_architecture_score_for_page,
    internal_link_band_for_page,
    issue_risk_family,
    normalize_score_profile,
    page_importance_for_page,
    resolve_page_type,
    thin_content_penalty_for_page,
    thin_content_threshold_for_page,
)


def clamp(score: int) -> int:
    return max(0, min(100, score))


def _is_html_page(page: dict) -> bool:
    content_type = (page.get("content_type") or "").lower()
    if "html" in content_type:
        return True
    return bool(page.get("title") or page.get("h1") or page.get("word_count"))


def _resolve_internal_outlinks(page: dict) -> int:
    for key in ("effective_internal_links_out", "outlinks", "internal_links_out"):
        value = page.get(key)
        if value is not None:
            return int(value or 0)
    return 0


def _page_type(page: dict) -> str:
    return resolve_page_type(page, infer_from_url=False, fallback="other", none_fallback="other")


def _page_importance(page: dict) -> float:
    return page_importance_for_page(page, infer_from_url=False, fallback="other", none_fallback="other")


def _thin_content_threshold(page: dict) -> int:
    return thin_content_threshold_for_page(page, infer_from_url=False, fallback="other", none_fallback="other")


def _internal_link_band(page: dict, outlinks: int) -> str:
    return internal_link_band_for_page(page, outlinks, infer_from_url=False, fallback="other", none_fallback="other")


def _is_missing_title(page: dict) -> bool:
    return not (page.get("effective_title") or page.get("title"))


def _is_missing_h1(page: dict) -> bool:
    fallback_h1 = int(not bool(page.get("h1")))
    return int(page.get("effective_h1_count", fallback_h1) or 0) == 0


def _is_thin(page: dict) -> bool:
    effective_len = int(page.get("effective_text_len", page.get("word_count", 0)) or 0)
    return effective_len < _thin_content_threshold(page)


def _effective_text_len(page: dict) -> int:
    return int(page.get("effective_text_len", page.get("word_count", 0)) or 0)


def _directive_int(value: object) -> int | None:
    raw = str(value or "").strip().lower()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _issue_codes(page_issues: Sequence[object]) -> set[str]:
    output: set[str] = set()
    for issue in page_issues:
        code = str(_issue_value(issue, "issue_code", "") or "").strip().upper()
        if code:
            output.add(code)
    return output


def _score_preview_controls(
    page: dict,
    *,
    is_html_page: bool,
    page_issues: Sequence[object],
) -> tuple[int | None, list[dict[str, object]]]:
    if not is_html_page or bool(page.get("is_noindex")):
        return None, []

    score = 100
    deductions: list[dict[str, object]] = []

    if int(page.get("has_nosnippet_directive") or 0) == 1:
        score -= 45
        deductions.append({"reason": "nosnippet_directive", "delta": 45})

    max_snippet_num = _directive_int(page.get("max_snippet_directive"))
    if max_snippet_num is not None:
        if max_snippet_num == 0:
            score -= 30
            deductions.append({"reason": "max_snippet_zero", "delta": 30, "value": max_snippet_num})
        elif 0 < max_snippet_num <= 50:
            score -= 15
            deductions.append({"reason": "max_snippet_very_low", "delta": 15, "value": max_snippet_num})

    max_image_preview = str(page.get("max_image_preview_directive") or "").strip().lower()
    if max_image_preview == "none":
        score -= 20
        deductions.append({"reason": "max_image_preview_none", "delta": 20})
    elif max_image_preview == "standard":
        score -= 8
        deductions.append({"reason": "max_image_preview_standard", "delta": 8})

    max_video_num = _directive_int(page.get("max_video_preview_directive"))
    if max_video_num is not None:
        if max_video_num == 0:
            score -= 8
            deductions.append({"reason": "max_video_preview_zero", "delta": 8, "value": max_video_num})
        elif 0 < max_video_num <= 5:
            score -= 4
            deductions.append({"reason": "max_video_preview_very_low", "delta": 4, "value": max_video_num})

    data_nosnippet_count = int(page.get("data_nosnippet_count") or 0)
    if data_nosnippet_count >= 10:
        score -= 12
        deductions.append({"reason": "heavy_data_nosnippet", "delta": 12, "count": data_nosnippet_count})
    elif data_nosnippet_count >= 4:
        score -= 6
        deductions.append({"reason": "moderate_data_nosnippet", "delta": 6, "count": data_nosnippet_count})

    codes = _issue_codes(page_issues)
    if "RAW_RENDER_PREVIEW_CONTROL_MISMATCH" in codes:
        score -= 15
        deductions.append({"reason": "raw_render_preview_control_mismatch", "delta": 15})
    if "RAW_RENDER_NOINDEX_MISMATCH" in codes:
        score -= 20
        deductions.append({"reason": "raw_render_noindex_mismatch", "delta": 20})
    if "OVER_RESTRICTIVE_SNIPPET_CONTROLS" in codes:
        score -= 15
        deductions.append({"reason": "over_restrictive_snippet_controls_issue", "delta": 15})
    if "BING_PREVIEW_CONTROLS_RESTRICTIVE" in codes:
        score -= 12
        deductions.append({"reason": "bing_preview_controls_restrictive_issue", "delta": 12})

    return clamp(score), deductions


def _score_structured_data_validity(
    page: dict,
    *,
    is_html_page: bool,
) -> tuple[int | None, list[dict[str, object]]]:
    if not is_html_page:
        return None, []

    score = 100
    deductions: list[dict[str, object]] = []

    parse_errors = int(page.get("schema_parse_error_count") or 0)
    if parse_errors > 0:
        delta = min(40, 15 + (parse_errors * 5))
        score -= delta
        deductions.append(
            {
                "reason": "structured_data_parse_errors",
                "delta": delta,
                "schema_parse_error_count": parse_errors,
            }
        )

    raw_schema_types = str(page.get("schema_types_json") or "").strip()
    has_schema_types = bool(raw_schema_types and raw_schema_types not in {"[]", "{}"})
    validation_payload: dict[str, Any] = {}
    try:
        parsed_validation = json.loads(str(page.get("schema_validation_json") or "{}"))
        if isinstance(parsed_validation, dict):
            validation_payload = parsed_validation
    except json.JSONDecodeError:
        validation_payload = {}

    rendered_validation = validation_payload.get("rendered_validation")
    if isinstance(rendered_validation, dict):
        recognized_types = rendered_validation.get("recognized_types")
        if isinstance(recognized_types, list) and any(str(token).strip() for token in recognized_types):
            has_schema_types = True

    if not has_schema_types and _page_type(page) in {"homepage", "service", "location", "product", "article"}:
        score -= 10
        deductions.append(
            {
                "reason": "feature_page_without_structured_data",
                "delta": 10,
                "page_type": _page_type(page),
            }
        )

    return clamp(score), deductions


def _score_crawlability(page: dict, status_code: int) -> tuple[int, list[dict[str, object]]]:
    score = 100
    deductions: list[dict[str, object]] = []

    if page.get("fetch_error"):
        score -= 60
        deductions.append({"reason": "fetch_error", "delta": 60})
    if status_code >= 500:
        score -= 65
        deductions.append({"reason": "status_code_5xx", "delta": 65, "status_code": status_code})
    elif status_code >= 400:
        score -= 45
        deductions.append({"reason": "status_code_4xx", "delta": 45, "status_code": status_code})
    if page.get("is_noindex"):
        score -= 50
        deductions.append({"reason": "noindex", "delta": 50})
    if int(page.get("robots_blocked_flag") or 0) == 1 and status_code in {0, None}:
        score -= 35
        deductions.append({"reason": "robots_blocked_unfetched", "delta": 35})

    return clamp(score), deductions


def _score_onpage(page: dict, *, is_html_page: bool) -> tuple[int, list[dict[str, object]]]:
    deductions: list[dict[str, object]] = []
    if page.get("fetch_error"):
        deductions.append({"reason": "fetch_error_floor", "score_floor": 25})
        return 25, deductions

    score = 100
    if is_html_page:
        if _is_missing_title(page):
            score -= 25
            deductions.append({"reason": "missing_title", "delta": 25})
        if not page.get("meta_description"):
            score -= 15
            deductions.append({"reason": "missing_meta_description", "delta": 15})
        if _is_missing_h1(page):
            score -= 15
            deductions.append({"reason": "missing_h1", "delta": 15})

        thin_penalty, thin_detail = thin_content_penalty_for_page(
            page,
            _effective_text_len(page),
            infer_from_url=False,
            fallback="other",
            none_fallback="other",
        )
        if thin_penalty > 0:
            score -= thin_penalty
            deductions.append(
                {
                    "reason": "thin_content_continuous",
                    "delta": thin_penalty,
                    **thin_detail,
                }
            )

    return clamp(score), deductions


def _score_canonical_rendering(page: dict, *, is_html_page: bool) -> tuple[int, list[dict[str, object]]]:
    score = 100
    deductions: list[dict[str, object]] = []

    if page.get("fetch_error"):
        score -= 45
        deductions.append({"reason": "fetch_error", "delta": 45})
    if is_html_page and not (page.get("effective_canonical") or page.get("canonical_url")):
        score -= 20
        deductions.append({"reason": "missing_canonical", "delta": 20})
    if _canonical_mismatch(page):
        score -= 30
        deductions.append({"reason": "canonical_mismatch", "delta": 30})
    effective_canonical_count = _effective_canonical_count(page)
    if effective_canonical_count > 1:
        score -= 15
        deductions.append(
            {
                "reason": "duplicate_canonical_tags",
                "delta": 15,
                "effective_canonical_count": effective_canonical_count,
            }
        )

    render_gap = max(0, min(100, int(page.get("render_gap_score") or 0)))
    if render_gap >= 90:
        score -= 40
        deductions.append({"reason": "render_gap_very_high", "delta": 40, "render_gap_score": render_gap})
    elif render_gap >= HIGH_RENDER_GAP_THRESHOLD:
        score -= 28
        deductions.append({"reason": "render_gap_high", "delta": 28, "render_gap_score": render_gap})
    elif render_gap >= 30:
        score -= 12
        deductions.append({"reason": "render_gap_moderate", "delta": 12, "render_gap_score": render_gap})

    if int(page.get("likely_js_shell") or 0) == 1 and int(page.get("render_checked") or 0) == 0:
        score -= 10
        deductions.append({"reason": "likely_js_shell_not_rendered", "delta": 10})

    return clamp(score), deductions


def _score_internal_architecture(page: dict) -> tuple[int, dict[str, object], list[dict[str, object]]]:
    outlinks = _resolve_internal_outlinks(page)
    score, detail = internal_architecture_score_for_page(
        page,
        outlinks,
        infer_from_url=False,
        fallback="other",
        none_fallback="other",
    )
    deductions = [
        adjustment
        for adjustment in detail.get("adjustments", [])
        if isinstance(adjustment, dict) and int(adjustment.get("delta") or 0) < 0
    ]
    return score, detail, deductions


def _canonical_mismatch(page: dict) -> bool:
    canonical = str(page.get("effective_canonical") or page.get("canonical_url") or "").strip()
    if not canonical:
        return False
    final_url = str(page.get("final_url") or page.get("normalized_url") or "").strip()
    return bool(final_url) and canonical.rstrip("/") != final_url.rstrip("/")


def _effective_field_provenance(page: dict) -> dict[str, str]:
    try:
        payload = json.loads(str(page.get("effective_field_provenance_json") or "{}"))
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    return {str(key): str(value) for key, value in payload.items()}


def _effective_canonical_count(page: dict) -> int:
    provenance = _effective_field_provenance(page)
    canonical_source = str(provenance.get("canonical") or "")
    if canonical_source.startswith("resolver:rendered") or canonical_source == "rendered":
        return int(page.get("rendered_canonical_count") or 0)
    return int(page.get("canonical_count") or 0)


def _issue_value(issue: object, key: str, default: object = None) -> object:
    if isinstance(issue, dict):
        return issue.get(key, default)
    return getattr(issue, key, default)


def _as_int(value: object, default: int = 0) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _risk_from_issues(page_issues: Sequence[object]) -> tuple[int, dict[str, object]]:
    if not page_issues:
        return 0, {
            "mode": "issue_blend_family_diminishing_returns",
            "peak_component": 0.0,
            "avg_component": 0.0,
            "breadth_bonus": 0.0,
            "serious_family_bonus": 0.0,
            "components": [],
            "notable_contributors": [],
            "family_totals": {},
            "top_risk_families": [],
            "family_breakdown": [],
            "neutralized_codes": [],
        }

    severity_weight = {
        "critical": 55,
        "high": 35,
        "medium": 18,
        "low": 8,
        "info": 3,
    }
    gate_weight = {
        "access": 1.25,
        "indexability": 1.20,
        "canonicalization": 1.05,
        "rendering": 1.05,
        "discovery": 0.90,
        "serving": 0.75,
    }
    certainty_weight = {
        "Verified": 1.00,
        "Probable": 0.85,
        "Unverified": 0.65,
        "Blocked / Could not test": 0.70,
    }
    reach_weight = {
        "single_page": 1.00,
        "template_cluster": 1.20,
        "sitewide": 1.35,
    }

    components: list[float] = []
    component_details: list[dict[str, object]] = []
    family_components: dict[str, list[float]] = {}
    neutralized_codes: set[str] = set()

    for issue in page_issues:
        code = str(_issue_value(issue, "issue_code", "") or "").upper()
        severity = str(_issue_value(issue, "severity", "info") or "info").lower()
        gate = str(_issue_value(issue, "technical_seo_gate", "indexability") or "indexability")
        certainty = str(_issue_value(issue, "certainty_state", "Probable") or "Probable")
        reach = str(_issue_value(issue, "reach", "single_page") or "single_page")
        confidence = _as_int(_issue_value(issue, "confidence_score", 80), 0)
        confidence_factor = max(0.40, min(1.00, confidence / 100.0))
        family = issue_risk_family(code)
        dampening_factor = float(RISK_DAMPENING_BY_ISSUE_CODE.get(code, 1.0))
        if dampening_factor < 1.0:
            neutralized_codes.add(code)

        base_component = (
            severity_weight.get(severity, 3)
            * gate_weight.get(gate, 1.0)
            * certainty_weight.get(certainty, 0.75)
            * reach_weight.get(reach, 1.0)
            * confidence_factor
        )
        component = base_component * dampening_factor

        components.append(component)
        family_components.setdefault(family, []).append(component)
        component_details.append(
            {
                "issue_code": code,
                "severity": severity,
                "technical_seo_gate": gate,
                "risk_family": family,
                "certainty_state": certainty,
                "reach": reach,
                "confidence_score": confidence,
                "base_component": round(base_component, 2),
                "dampening_factor": round(dampening_factor, 4),
                "component_score": round(component, 2),
            }
        )

    if not components:
        return 0, {
            "mode": "issue_blend_family_diminishing_returns",
            "peak_component": 0.0,
            "avg_component": 0.0,
            "breadth_bonus": 0.0,
            "serious_family_bonus": 0.0,
            "components": [],
            "notable_contributors": [],
            "family_totals": {},
            "top_risk_families": [],
            "family_breakdown": [],
            "neutralized_codes": sorted(neutralized_codes),
        }

    family_totals: dict[str, float] = {}
    family_breakdown: list[dict[str, object]] = []
    for family, raw_values in family_components.items():
        sorted_values = sorted(raw_values, reverse=True)
        diminished_total = 0.0
        diminished_components: list[dict[str, object]] = []
        for index, raw_component in enumerate(sorted_values):
            taper_factor = RISK_FAMILY_DECAY_FACTOR**index
            tapered_component = raw_component * taper_factor
            diminished_total += tapered_component
            diminished_components.append(
                {
                    "rank": index + 1,
                    "raw_component": round(raw_component, 2),
                    "taper_factor": round(taper_factor, 4),
                    "tapered_component": round(tapered_component, 2),
                }
            )

        family_totals[family] = diminished_total
        family_breakdown.append(
            {
                "risk_family": family,
                "issue_count": len(sorted_values),
                "raw_total": round(sum(sorted_values), 2),
                "diminished_total": round(diminished_total, 2),
                "peak_component": round(sorted_values[0], 2),
                "components": diminished_components[:8],
            }
        )

    diminished_values = sorted(family_totals.values(), reverse=True)
    peak = diminished_values[0] if diminished_values else 0.0
    avg = (sum(diminished_values) / len(diminished_values)) if diminished_values else 0.0

    breadth_bonus = min(
        RISK_FAMILY_BREADTH_BONUS_MAX,
        max(0.0, (len(family_totals) - 1) * RISK_FAMILY_BREADTH_BONUS_PER_FAMILY),
    )
    serious_family_count = sum(
        1 for value in diminished_values if value >= RISK_SERIOUS_FAMILY_COMPONENT_THRESHOLD
    )
    serious_family_bonus = min(
        RISK_SERIOUS_FAMILY_BONUS_MAX,
        max(0.0, (serious_family_count - 1) * RISK_SERIOUS_FAMILY_BONUS_PER_FAMILY),
    )

    blended = (peak * 0.50) + (avg * 0.50) + breadth_bonus + serious_family_bonus
    risk_score = clamp(int(round(blended)))

    sorted_components = sorted(
        component_details,
        key=lambda item: float(item.get("component_score", 0.0)),
        reverse=True,
    )
    sorted_families = sorted(
        family_totals.items(),
        key=lambda item: float(item[1]),
        reverse=True,
    )
    risk_details: dict[str, object] = {
        "mode": "issue_blend_family_diminishing_returns",
        "peak_component": round(peak, 2),
        "avg_component": round(avg, 2),
        "breadth_bonus": round(breadth_bonus, 2),
        "serious_family_bonus": round(serious_family_bonus, 2),
        "components": sorted_components,
        "notable_contributors": sorted_components[:5],
        "family_totals": {key: round(value, 2) for key, value in sorted_families},
        "top_risk_families": [
            {"risk_family": family, "diminished_total": round(total, 2)}
            for family, total in sorted_families[:5]
        ],
        "family_breakdown": sorted(
            family_breakdown,
            key=lambda item: float(item.get("diminished_total", 0.0)),
            reverse=True,
        ),
        "neutralized_codes": sorted(neutralized_codes),
    }
    return risk_score, risk_details


def _score_cap(
    page: dict,
    *,
    page_importance: float,
    page_issues: Sequence[object],
) -> tuple[int, list[dict[str, object]]]:
    cap = 100
    status_code = int(page.get("status_code") or 0)
    cap_reasons: list[dict[str, object]] = []
    seen_reasons: set[tuple[object, ...]] = set()

    def apply_cap(reason: str, max_score: int, **metadata: object) -> None:
        nonlocal cap
        reason_key = (
            reason,
            max_score,
            metadata.get("issue_code"),
            metadata.get("severity"),
            metadata.get("gate"),
        )
        if reason_key in seen_reasons:
            return
        seen_reasons.add(reason_key)
        cap = min(cap, max_score)
        entry = {"reason": reason, "cap": max_score}
        entry.update(metadata)
        cap_reasons.append(entry)

    if page_importance >= 1.20 and (page.get("fetch_error") or status_code >= 500 or page.get("is_noindex")):
        apply_cap(
            "high_importance_access_or_indexability_blocker",
            35,
            status_code=status_code,
            fetch_error=bool(page.get("fetch_error")),
            is_noindex=bool(page.get("is_noindex")),
        )

    if page_issues:
        for issue in page_issues:
            severity = str(_issue_value(issue, "severity", "info") or "info").lower()
            gate = str(_issue_value(issue, "technical_seo_gate", "indexability") or "indexability")
            code = str(_issue_value(issue, "issue_code", "") or "").upper()

            if page_importance >= 1.20 and severity == "critical" and gate in HIGH_IMPORTANCE_HARD_BLOCKER_GATES:
                apply_cap(
                    "critical_high_importance_blocker_issue",
                    35,
                    issue_code=code,
                    severity=severity,
                    gate=gate,
                )
            elif (
                page_importance >= 1.20
                and severity in {"critical", "high"}
                and (
                    gate in HIGH_IMPORTANCE_HARD_BLOCKER_GATES
                    or code in HIGH_IMPORTANCE_CAP_55_ISSUE_CODES
                )
            ):
                apply_cap(
                    "high_importance_major_issue",
                    55,
                    issue_code=code,
                    severity=severity,
                    gate=gate,
                )
            elif (
                page_importance >= 1.0
                and severity in {"critical", "high", "medium"}
                and code in BASELINE_CAP_75_ISSUE_CODES
            ):
                apply_cap(
                    "quality_signal_degradation",
                    75,
                    issue_code=code,
                    severity=severity,
                    gate=gate,
                )
    else:
        if _canonical_mismatch(page) or int(page.get("render_gap_score") or 0) >= HIGH_RENDER_GAP_THRESHOLD:
            apply_cap(
                "fallback_canonical_or_render_gap_blocker",
                55,
                render_gap_score=int(page.get("render_gap_score") or 0),
                canonical_mismatch=_canonical_mismatch(page),
            )
        elif _is_missing_title(page) or _is_missing_h1(page) or _is_thin(page):
            apply_cap(
                "fallback_content_quality_degradation",
                75,
                missing_title=_is_missing_title(page),
                missing_h1=_is_missing_h1(page),
                thin_content=_is_thin(page),
            )

    return cap, cap_reasons


def _quality_weights(site_type: str) -> dict[str, float]:
    normalized = str(site_type or "general").strip().lower()
    if normalized == "local":
        return {
            "access": 0.23,
            "content": 0.15,
            "canonical_rendering": 0.14,
            "internal_architecture": 0.14,
            "local_completeness": 0.18,
            "performance": 0.08,
            "preview_controls": 0.05,
            "structured_data_validity": 0.03,
        }
    return {
        "access": 0.28,
        "content": 0.19,
        "canonical_rendering": 0.15,
        "internal_architecture": 0.14,
        "performance": 0.10,
        "preview_controls": 0.08,
        "structured_data_validity": 0.06,
    }


def _compose_quality_score(
    dimension_scores: dict[str, int | None],
    weights: dict[str, float],
) -> tuple[int, int, float, float, float, list[dict[str, str]]]:
    applicable_weight = sum(weights.values())
    measured_weight = sum(weight for key, weight in weights.items() if dimension_scores.get(key) is not None)

    weighted_sum = 0.0
    if measured_weight > 0:
        weighted_sum = sum(
            int(dimension_scores[key] or 0) * weight
            for key, weight in weights.items()
            if dimension_scores.get(key) is not None
        )
        quality_score = clamp(int(round(weighted_sum / measured_weight)))
    else:
        quality_score = 0

    coverage_score = clamp(int(round((measured_weight / applicable_weight) * 100))) if applicable_weight > 0 else 0

    skipped_notes: list[dict[str, str]] = []
    for key in weights:
        if dimension_scores.get(key) is None:
            skipped_notes.append({"dimension": key, "reason": "not_applicable_or_missing"})

    return quality_score, coverage_score, weighted_sum, measured_weight, applicable_weight, skipped_notes


def _serialize_score_output(
    *,
    crawlability_score: int,
    onpage_score: int,
    render_risk_score: int,
    internal_linking_score: int,
    local_seo_score: int | None,
    performance_score: int | None,
    overall_score: int,
    quality_score: int,
    risk_score: int,
    coverage_score: int,
    score_cap: int,
    scoring_model_version: str,
    scoring_profile: str,
    explanation: dict[str, Any],
) -> dict[str, object]:
    explanation_json = json.dumps(explanation, sort_keys=True)
    return {
        "crawlability_score": crawlability_score,
        "onpage_score": onpage_score,
        "render_risk_score": render_risk_score,
        "internal_linking_score": internal_linking_score,
        "local_seo_score": int(local_seo_score) if local_seo_score is not None else -1,
        "performance_score": performance_score if performance_score is not None else -1,
        "overall_score": overall_score,
        "quality_score": quality_score,
        "risk_score": risk_score,
        "coverage_score": coverage_score,
        "score_cap": score_cap,
        # Legacy score explainability fields retained for backward compatibility.
        "score_version": scoring_model_version,
        "score_profile": scoring_profile,
        "explanation_json": explanation_json,
        # New additive score explainability fields.
        "scoring_model_version": scoring_model_version,
        "scoring_profile": scoring_profile,
        "score_explanation_json": explanation_json,
    }


def score_page(
    page: dict,
    performance_score: int | None = None,
    site_type: str = "general",
    page_issues: Sequence[object] | None = None,
    score_profile: str = DEFAULT_SCORE_PROFILE,
) -> dict:
    normalized_profile = normalize_score_profile(score_profile, site_type=site_type)
    scoring_model_version = CURRENT_SCORE_VERSION

    is_html_page = _is_html_page(page)
    page_type = _page_type(page)
    page_importance = _page_importance(page)
    status_code = int(page.get("status_code") or 0)
    perf: int | None = clamp(int(performance_score)) if performance_score is not None else None

    robots_blocked_unfetched = int(page.get("robots_blocked_flag") or 0) == 1 and status_code == 0
    if robots_blocked_unfetched:
        local_score: int | None = None
        weights = _quality_weights(site_type)
        blocked_dimensions: dict[str, int | None] = {
            "access": 40,
            "content": 35,
            "canonical_rendering": 50,
            "internal_architecture": 50,
            "local_completeness": local_score,
            "performance": perf,
            "preview_controls": None,
            "structured_data_validity": None,
        }
        quality_score, coverage_score, weighted_sum, measured_weight, applicable_weight, skipped_notes = _compose_quality_score(
            blocked_dimensions,
            weights,
        )

        risk_score = 70
        score_cap = 35

        explanation = {
            "scoring_model_version": scoring_model_version,
            "scoring_profile": normalized_profile,
            "score_version": scoring_model_version,
            "score_profile": normalized_profile,
            "formula": {
                "id": SCORE_FORMULA_ID,
                "quality_minus_risk_factor": 0.40,
                "coverage_confidence_floor": 0.55,
                "coverage_confidence_span": 0.45,
                "overall_expression": "min(score_cap, (quality_score - (risk_score * 0.40)) * confidence_factor)",
            },
            "inputs": {
                "status_code": status_code,
                "is_html_page": is_html_page,
                "page_type": page_type,
                "page_importance": round(page_importance, 2),
                "resolved_internal_links_out": _resolve_internal_outlinks(page),
                "performance_score_input": perf,
                "issue_count": len(page_issues or []),
                "robots_blocked_unfetched": True,
            },
            "dimensions": {
                "scores": {
                    "crawlability_score": 40,
                    "onpage_score": 35,
                    "render_risk_score": 50,
                    "internal_linking_score": 50,
                    "local_seo_score": None,
                    "performance_score": perf,
                    "preview_controls_score": None,
                    "structured_data_validity_score": None,
                    "structured_snippets_score": None,
                },
                "deductions": {
                    "crawlability_score": [{"reason": "robots_blocked_unfetched_floor", "delta": 60}],
                    "onpage_score": [{"reason": "robots_blocked_unfetched_floor", "delta": 65}],
                    "render_risk_score": [{"reason": "render_unavailable_due_to_robots_block", "delta": 50}],
                    "internal_linking_score": [{"reason": "internal_architecture_unknown_due_to_robots_block", "delta": 50}],
                    "local_seo_score": [],
                    "performance_score": [],
                    "preview_controls_score": [],
                    "structured_data_validity_score": [],
                    "structured_snippets_score": [],
                },
                "notes": {"not_applicable_or_missing": skipped_notes},
                "weights": weights,
                "applicable_weight": round(applicable_weight, 4),
                "measured_weight": round(measured_weight, 4),
                "weighted_sum": round(weighted_sum, 4),
                "quality_score": quality_score,
                "coverage_score": coverage_score,
            },
            "risk": {
                "mode": "robots_blocked_unfetched_baseline",
                "score": risk_score,
                "components": [
                    {
                        "issue_code": "ROBOTS_BLOCKED_URL",
                        "risk_family": issue_risk_family("ROBOTS_BLOCKED_URL"),
                        "component_score": risk_score,
                    }
                ],
                "notable_contributors": [
                    {
                        "issue_code": "ROBOTS_BLOCKED_URL",
                        "risk_family": issue_risk_family("ROBOTS_BLOCKED_URL"),
                        "component_score": risk_score,
                    }
                ],
                "family_totals": {
                    issue_risk_family("ROBOTS_BLOCKED_URL"): float(risk_score)
                },
                "top_risk_families": [
                    {
                        "risk_family": issue_risk_family("ROBOTS_BLOCKED_URL"),
                        "diminished_total": float(risk_score),
                    }
                ],
                "neutralized_codes": [],
            },
            "cap": {
                "score_cap": score_cap,
                "reasons": [
                    {
                        "reason": "robots_blocked_unfetched_conservative_cap",
                        "cap": score_cap,
                    }
                ],
            },
            "overall": {
                "confidence_factor": 1.0,
                "adjusted_quality": float(score_cap),
                "overall_score": score_cap,
            },
        }

        return _serialize_score_output(
            crawlability_score=40,
            onpage_score=35,
            render_risk_score=50,
            internal_linking_score=50,
            local_seo_score=local_score,
            performance_score=perf,
            overall_score=score_cap,
            quality_score=quality_score,
            risk_score=risk_score,
            coverage_score=coverage_score,
            score_cap=score_cap,
            scoring_model_version=scoring_model_version,
            scoring_profile=normalized_profile,
            explanation=explanation,
        )

    crawlability, crawl_deductions = _score_crawlability(page, status_code)
    onpage, onpage_deductions = _score_onpage(page, is_html_page=is_html_page)
    canonical_rendering, canonical_deductions = _score_canonical_rendering(page, is_html_page=is_html_page)
    internal, internal_detail, internal_deductions = _score_internal_architecture(page)

    local_raw = 0
    local_raw += 30 if page.get("has_contact_signal") else 0
    local_raw += 30 if page.get("has_location_signal") else 0
    local_raw += 20 if page.get("has_local_schema") else 0
    local_raw += 20 if page.get("has_map") else 0
    local_score: int | None = local_raw if str(site_type or "general").strip().lower() == "local" else None

    issues_for_page = list(page_issues or [])

    preview_controls_score, preview_deductions = _score_preview_controls(
        page,
        is_html_page=is_html_page,
        page_issues=issues_for_page,
    )
    structured_validity_score, structured_validity_deductions = _score_structured_data_validity(
        page,
        is_html_page=is_html_page,
    )

    legacy_structured_snippets_score: int | None
    if preview_controls_score is None and structured_validity_score is None:
        legacy_structured_snippets_score = None
    elif preview_controls_score is None:
        legacy_structured_snippets_score = structured_validity_score
    elif structured_validity_score is None:
        legacy_structured_snippets_score = preview_controls_score
    else:
        legacy_structured_snippets_score = clamp(int(round((preview_controls_score + structured_validity_score) / 2)))

    dimension_scores: dict[str, int | None] = {
        "access": crawlability,
        "content": onpage,
        "canonical_rendering": canonical_rendering,
        "internal_architecture": internal,
        "local_completeness": local_score,
        "performance": perf,
        "preview_controls": preview_controls_score,
        "structured_data_validity": structured_validity_score,
    }

    weights = _quality_weights(site_type)
    quality_score, coverage_score, weighted_sum, measured_weight, applicable_weight, skipped_notes = _compose_quality_score(
        dimension_scores,
        weights,
    )

    if issues_for_page:
        risk_score, risk_details = _risk_from_issues(issues_for_page)
    else:
        # Fallback risk heuristics when issue rows are not provided.
        risk_score = 0
        fallback_components: list[dict[str, object]] = []
        fallback_family_totals: dict[str, float] = {}

        def add_fallback_component(signal: str, delta: int, issue_code: str) -> None:
            nonlocal risk_score
            risk_score += delta
            family = issue_risk_family(issue_code)
            fallback_family_totals[family] = fallback_family_totals.get(family, 0.0) + float(delta)
            fallback_components.append(
                {
                    "signal": signal,
                    "issue_code": issue_code,
                    "risk_family": family,
                    "component_score": float(delta),
                }
            )

        if page.get("fetch_error"):
            add_fallback_component("fetch_error", 60, "FETCH_FAILED")
        if status_code >= 500:
            add_fallback_component("status_code_5xx", 55, "FETCH_FAILED")
        elif status_code >= 400:
            add_fallback_component("status_code_4xx", 30, "REDIRECT_TO_ERROR")
        if page.get("is_noindex"):
            add_fallback_component(
                "noindex_detected",
                45 if page_importance >= 1.20 else 20,
                "NOINDEX",
            )
        if int(page.get("render_gap_score") or 0) >= HIGH_RENDER_GAP_THRESHOLD:
            add_fallback_component("high_render_gap", 25, "RENDER_GAP_HIGH")
        if _is_missing_title(page):
            add_fallback_component("missing_title", 20, "MISSING_TITLE")
        risk_score = clamp(risk_score)
        sorted_family_totals = sorted(fallback_family_totals.items(), key=lambda item: float(item[1]), reverse=True)
        risk_details = {
            "mode": "fallback_heuristics",
            "components": fallback_components,
            "notable_contributors": sorted(
                fallback_components,
                key=lambda item: float(item.get("component_score", 0.0)),
                reverse=True,
            )[:5],
            "family_totals": {key: round(value, 2) for key, value in sorted_family_totals},
            "top_risk_families": [
                {"risk_family": family, "diminished_total": round(total, 2)}
                for family, total in sorted_family_totals[:5]
            ],
            "neutralized_codes": [],
            "peak_component": round(
                max((float(item.get("component_score", 0.0)) for item in fallback_components), default=0.0),
                2,
            ),
            "avg_component": round(
                (
                    sum(float(item.get("component_score", 0.0)) for item in fallback_components)
                    / len(fallback_components)
                )
                if fallback_components
                else 0.0,
                2,
            ),
            "breadth_bonus": 0.0,
            "serious_family_bonus": 0.0,
        }

    score_cap, cap_reasons = _score_cap(page, page_importance=page_importance, page_issues=issues_for_page)

    confidence_factor = 0.55 + (0.45 * (coverage_score / 100.0))
    adjusted_quality = (quality_score - (risk_score * 0.40)) * confidence_factor
    overall = clamp(min(score_cap, int(round(adjusted_quality))))

    explanation: dict[str, Any] = {
        "scoring_model_version": scoring_model_version,
        "scoring_profile": normalized_profile,
        "score_version": scoring_model_version,
        "score_profile": normalized_profile,
        "formula": {
            "id": SCORE_FORMULA_ID,
            "quality_minus_risk_factor": 0.40,
            "coverage_confidence_floor": 0.55,
            "coverage_confidence_span": 0.45,
            "overall_expression": "min(score_cap, (quality_score - (risk_score * 0.40)) * confidence_factor)",
        },
        "inputs": {
            "status_code": status_code,
            "is_html_page": is_html_page,
            "page_type": page_type,
            "page_importance": round(page_importance, 2),
            "resolved_internal_links_out": _resolve_internal_outlinks(page),
            "internal_link_band": str(internal_detail.get("internal_link_band") or _internal_link_band(page, _resolve_internal_outlinks(page))),
            "internal_architecture_signals": internal_detail.get("signal_inputs", {}),
            "performance_score_input": perf,
            "issue_count": len(issues_for_page),
            "robots_blocked_unfetched": False,
        },
        "dimensions": {
            "scores": {
                "crawlability_score": crawlability,
                "onpage_score": onpage,
                "render_risk_score": canonical_rendering,
                "internal_linking_score": internal,
                "local_seo_score": local_score,
                "performance_score": perf,
                "preview_controls_score": preview_controls_score,
                "structured_data_validity_score": structured_validity_score,
                "structured_snippets_score": legacy_structured_snippets_score,
            },
            "deductions": {
                "crawlability_score": crawl_deductions,
                "onpage_score": onpage_deductions,
                "render_risk_score": canonical_deductions,
                "internal_linking_score": internal_deductions,
                "local_seo_score": [],
                "performance_score": [],
                "preview_controls_score": preview_deductions,
                "structured_data_validity_score": structured_validity_deductions,
                "structured_snippets_score": [
                    *preview_deductions,
                    *structured_validity_deductions,
                ],
            },
            "notes": {"not_applicable_or_missing": skipped_notes},
            "weights": weights,
            "applicable_weight": round(applicable_weight, 4),
            "measured_weight": round(measured_weight, 4),
            "weighted_sum": round(weighted_sum, 4),
            "quality_score": quality_score,
            "coverage_score": coverage_score,
        },
        "risk": {
            "score": risk_score,
            **risk_details,
        },
        "cap": {
            "score_cap": score_cap,
            "reasons": cap_reasons,
        },
        "overall": {
            "confidence_factor": round(confidence_factor, 4),
            "adjusted_quality": round(adjusted_quality, 4),
            "overall_score": overall,
        },
    }

    return _serialize_score_output(
        crawlability_score=crawlability,
        onpage_score=onpage,
        render_risk_score=canonical_rendering,
        internal_linking_score=internal,
        local_seo_score=local_score,
        performance_score=perf,
        overall_score=overall,
        quality_score=quality_score,
        risk_score=risk_score,
        coverage_score=coverage_score,
        score_cap=score_cap,
        scoring_model_version=scoring_model_version,
        scoring_profile=normalized_profile,
        explanation=explanation,
    )
