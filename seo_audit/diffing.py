from __future__ import annotations

import json

from seo_audit.models import PageDiffRecord


def _json_list(raw: object) -> list:
    if isinstance(raw, list):
        return raw
    text = str(raw or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def _json_object(raw: object) -> dict:
    if isinstance(raw, dict):
        return raw
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _stable_json(value: object) -> str:
    try:
        return json.dumps(value, sort_keys=True)
    except TypeError:
        return str(value or "")


def _norm_text(value: object) -> str:
    return str(value or "").strip()


def _effective_title(page: dict) -> str:
    return _norm_text(page.get("effective_title") or page.get("title"))


def _effective_meta_description(page: dict) -> str:
    return _norm_text(page.get("effective_meta_description") or page.get("meta_description"))


def _effective_canonical(page: dict) -> str:
    return _norm_text(page.get("effective_canonical") or page.get("canonical_url"))


def _robots_signature(page: dict) -> str:
    payload = _json_object(page.get("effective_robots_json"))
    return _stable_json(payload)


def _heading_signature(page: dict) -> str:
    headings = _json_list(page.get("heading_outline_json"))
    h1 = _norm_text(page.get("h1"))
    payload = {
        "h1": h1,
        "h1_count": int(page.get("h1_count") or page.get("effective_h1_count") or 0),
        "outline": headings,
    }
    return _stable_json(payload)


def _schema_types_signature(page: dict) -> str:
    schema_types = sorted({str(value).strip() for value in _json_list(page.get("schema_types_json")) if str(value).strip()})
    return _stable_json(schema_types)


def _internal_links_signature(page: dict) -> str:
    payload = {
        "out": int(page.get("effective_internal_links_out") or page.get("internal_links_out") or 0),
        "links": sorted({
            str((row or {}).get("href") or "").strip()
            for row in _json_list(page.get("effective_links_json") or page.get("raw_links_json"))
            if isinstance(row, dict)
        }),
    }
    return _stable_json(payload)


def _primary_content_hash(page: dict) -> str:
    return _norm_text(page.get("effective_content_hash") or page.get("content_hash"))


def _raw_content_hash(page: dict) -> str:
    return _norm_text(page.get("raw_content_hash") or page.get("content_hash"))


def _rendered_content_hash(page: dict) -> str:
    return _norm_text(page.get("rendered_content_hash"))


def _media_inventory_signature(page: dict) -> str:
    image_assets = _json_list(page.get("image_details_json"))
    video_assets = _json_list(page.get("video_details_json"))
    payload = {
        "images": int(page.get("image_count") or len(image_assets)),
        "videos": len(video_assets),
        "image_keys": sorted({
            str((row or {}).get("normalized_src") or (row or {}).get("src") or "").strip()
            for row in image_assets
            if isinstance(row, dict)
        })[:12],
        "video_keys": sorted({
            str((row or {}).get("src") or (row or {}).get("content_url") or (row or {}).get("embed_url") or "").strip()
            for row in video_assets
            if isinstance(row, dict)
        })[:12],
    }
    return _stable_json(payload)


def _schema_eligibility_signature(page: dict) -> str:
    payload = _json_object(page.get("schema_validation_json"))
    eligible = payload.get("eligible_features")
    deprecated = payload.get("deprecated_features")
    normalized = {
        "eligible_features": eligible if isinstance(eligible, list) else [],
        "deprecated_features": deprecated if isinstance(deprecated, list) else [],
        "syntax_valid": int(bool(payload.get("syntax_valid", True))),
    }
    return _stable_json(normalized)


def _add_diff(
    diffs: list[PageDiffRecord],
    *,
    run_id: str,
    url: str,
    family: str,
    old_value: str,
    new_value: str,
    severity: str,
) -> None:
    if old_value == new_value:
        return
    diffs.append(
        PageDiffRecord(
            run_id=run_id,
            url=url,
            diff_family=family,
            old_value=old_value,
            new_value=new_value,
            severity=severity,
        )
    )


def generate_page_diffs(run_id: str, url: str, current: dict, previous: dict | None) -> list[PageDiffRecord]:
    if previous is None:
        return []

    diffs: list[PageDiffRecord] = []

    _add_diff(
        diffs,
        run_id=run_id,
        url=url,
        family="title",
        old_value=_effective_title(previous),
        new_value=_effective_title(current),
        severity="medium",
    )
    _add_diff(
        diffs,
        run_id=run_id,
        url=url,
        family="meta_description",
        old_value=_effective_meta_description(previous),
        new_value=_effective_meta_description(current),
        severity="low",
    )
    _add_diff(
        diffs,
        run_id=run_id,
        url=url,
        family="canonical",
        old_value=_effective_canonical(previous),
        new_value=_effective_canonical(current),
        severity="medium",
    )
    _add_diff(
        diffs,
        run_id=run_id,
        url=url,
        family="robots_directives",
        old_value=_robots_signature(previous),
        new_value=_robots_signature(current),
        severity="high",
    )
    _add_diff(
        diffs,
        run_id=run_id,
        url=url,
        family="headings",
        old_value=_heading_signature(previous),
        new_value=_heading_signature(current),
        severity="low",
    )
    _add_diff(
        diffs,
        run_id=run_id,
        url=url,
        family="schema_types",
        old_value=_schema_types_signature(previous),
        new_value=_schema_types_signature(current),
        severity="medium",
    )
    _add_diff(
        diffs,
        run_id=run_id,
        url=url,
        family="internal_links",
        old_value=_internal_links_signature(previous),
        new_value=_internal_links_signature(current),
        severity="low",
    )
    _add_diff(
        diffs,
        run_id=run_id,
        url=url,
        family="primary_content_hash",
        old_value=_primary_content_hash(previous),
        new_value=_primary_content_hash(current),
        severity="medium",
    )
    _add_diff(
        diffs,
        run_id=run_id,
        url=url,
        family="raw_content_hash",
        old_value=_raw_content_hash(previous),
        new_value=_raw_content_hash(current),
        severity="low",
    )
    _add_diff(
        diffs,
        run_id=run_id,
        url=url,
        family="rendered_content_hash",
        old_value=_rendered_content_hash(previous),
        new_value=_rendered_content_hash(current),
        severity="low",
    )
    _add_diff(
        diffs,
        run_id=run_id,
        url=url,
        family="media_inventory",
        old_value=_media_inventory_signature(previous),
        new_value=_media_inventory_signature(current),
        severity="low",
    )
    _add_diff(
        diffs,
        run_id=run_id,
        url=url,
        family="structured_data_eligibility_shifts",
        old_value=_schema_eligibility_signature(previous),
        new_value=_schema_eligibility_signature(current),
        severity="medium",
    )

    return diffs
