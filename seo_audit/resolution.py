from __future__ import annotations

import json
from typing import Any

from seo_audit.render import RenderResult
from seo_audit.url_utils import normalize_url


_GOOGLE_LIKE_PERSONAS = {
    "googlebot_smartphone",
    "googlebot_desktop",
    "browser_default",
}


def _json_list(raw: object) -> list[object]:
    if isinstance(raw, list):
        return list(raw)
    text = str(raw or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def _json_object(raw: object) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def crawl_persona_prefers_rendered(crawl_persona: str) -> bool:
    return str(crawl_persona or "").strip().lower() in _GOOGLE_LIKE_PERSONAS


def _normalize_canonical_list(values: list[object], *, base_url: str) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        canonical = normalize_url(str(value or "").strip(), base_url=base_url)
        if not canonical or canonical in seen:
            continue
        seen.add(canonical)
        normalized.append(canonical)
    return normalized


def _normalize_hreflang_links(values: list[object], *, base_url: str) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in values:
        if not isinstance(row, dict):
            continue
        lang = str(row.get("lang") or "").strip().lower()
        href = normalize_url(str(row.get("href") or "").strip(), base_url=base_url)
        if not lang or not href:
            continue
        key = (lang, href)
        if key in seen:
            continue
        seen.add(key)
        normalized.append({"lang": lang, "href": href})
    return normalized


def parse_effective_field_provenance(page: dict) -> dict[str, str]:
    payload = _json_object(page.get("effective_field_provenance_json") or "{}")
    return {str(key): str(value) for key, value in payload.items()}


def resolve_effective_page_facts(
    page: dict,
    rendered: RenderResult | None,
    *,
    crawl_persona: str,
) -> dict[str, object]:
    final_candidate = str(page.get("final_url") or page.get("normalized_url") or "")
    final_url = normalize_url(final_candidate)

    raw_title = _normalize_text(page.get("raw_title") or page.get("title"))
    raw_meta_description = _normalize_text(page.get("raw_meta_description") or page.get("meta_description"))
    raw_content_hash = _normalize_text(page.get("raw_content_hash") or page.get("content_hash"))
    raw_canonical = _normalize_text(page.get("raw_canonical") or page.get("canonical_url"))
    raw_canonical_urls = _normalize_canonical_list(
        _json_list(page.get("raw_canonical_urls_json") or page.get("canonical_urls_json")),
        base_url=final_url,
    )
    raw_hreflang_links = _normalize_hreflang_links(
        _json_list(page.get("raw_hreflang_links_json") or page.get("hreflang_links_json")),
        base_url=final_url,
    )

    rendered_available = rendered is not None
    prefer_rendered = rendered_available and crawl_persona_prefers_rendered(crawl_persona)

    rendered_title = _normalize_text(rendered.title) if rendered_available else ""
    rendered_meta_description = _normalize_text(rendered.meta_description) if rendered_available else ""
    rendered_content_hash = _normalize_text(rendered.content_hash) if rendered_available else ""
    rendered_final_url = normalize_url(str(rendered.final_url or final_url)) if rendered_available else final_url
    rendered_canonical_urls = _normalize_canonical_list(
        list(rendered.canonical_urls if rendered_available else []),
        base_url=rendered_final_url,
    )
    if rendered_available and not rendered_canonical_urls and str(rendered.canonical or "").strip():
        rendered_canonical_urls = _normalize_canonical_list([rendered.canonical], base_url=rendered_final_url)

    rendered_hreflang_links = _normalize_hreflang_links(
        list(rendered.hreflang_links if rendered_available else []),
        base_url=rendered_final_url,
    )

    provenance: dict[str, str] = {}

    if prefer_rendered and rendered_title:
        effective_title = rendered_title
        provenance["title"] = "rendered"
    else:
        effective_title = raw_title
        provenance["title"] = "raw"

    if prefer_rendered and rendered_meta_description:
        effective_meta_description = rendered_meta_description
        provenance["meta_description"] = "rendered"
    else:
        effective_meta_description = raw_meta_description
        provenance["meta_description"] = "raw"

    if prefer_rendered and rendered_hreflang_links:
        effective_hreflang_links = rendered_hreflang_links
        provenance["hreflang"] = "rendered"
    else:
        effective_hreflang_links = raw_hreflang_links
        provenance["hreflang"] = "raw_fallback" if prefer_rendered else "raw"

    if prefer_rendered and rendered_content_hash:
        effective_content_hash = rendered_content_hash
        provenance["content_hash"] = "rendered"
    else:
        effective_content_hash = raw_content_hash
        provenance["content_hash"] = "raw_fallback" if prefer_rendered else "raw"

    raw_canonical_single = normalize_url(raw_canonical, base_url=final_url) if raw_canonical else ""
    if not raw_canonical_single and raw_canonical_urls:
        raw_canonical_single = raw_canonical_urls[0]

    effective_canonical = ""
    canonical_provenance = ""
    canonical_conflict = False
    canonical_unresolved = False

    if prefer_rendered and rendered_canonical_urls:
        if len(rendered_canonical_urls) == 1:
            effective_canonical = rendered_canonical_urls[0]
            canonical_provenance = "resolver:rendered_single"
        else:
            self_matches = [value for value in rendered_canonical_urls if normalize_url(value, base_url=rendered_final_url) == rendered_final_url]
            if len(self_matches) == 1:
                effective_canonical = self_matches[0]
                canonical_provenance = "resolver:rendered_self_match"
            else:
                canonical_unresolved = True
                canonical_provenance = "resolver:rendered_multiple_unresolved"

        if raw_canonical_single:
            raw_norm = normalize_url(raw_canonical_single, base_url=rendered_final_url)
            if effective_canonical:
                canonical_conflict = raw_norm != normalize_url(effective_canonical, base_url=rendered_final_url)
            else:
                canonical_conflict = raw_norm not in rendered_canonical_urls

        if canonical_unresolved and raw_canonical_single and not canonical_conflict:
            effective_canonical = normalize_url(raw_canonical_single, base_url=rendered_final_url)
            canonical_unresolved = False
            canonical_provenance = "resolver:raw_agreement_fallback"

    elif raw_canonical_single:
        effective_canonical = raw_canonical_single
        canonical_provenance = "raw"
    else:
        canonical_provenance = "unresolved"

    provenance["canonical"] = canonical_provenance

    raw_shell_state = str(page.get("shell_state") or "").strip()
    if raw_shell_state not in {"raw_shell_unlikely", "raw_shell_possible", "raw_shell_confirmed_after_render"}:
        raw_shell_state = "raw_shell_possible" if int(page.get("shell_score") or 0) >= 30 else "raw_shell_unlikely"

    shell_state = raw_shell_state
    if rendered_available:
        raw_text_len = int(page.get("raw_text_len") or page.get("word_count") or 0)
        raw_links = int(page.get("internal_links_out") or 0)
        rendered_links = len(rendered.links)
        rendered_word_count = int(rendered.word_count or 0)
        title_changed = bool(raw_title and rendered_title and raw_title != rendered_title)
        description_changed = bool(
            raw_meta_description and rendered_meta_description and raw_meta_description != rendered_meta_description
        )

        if raw_shell_state != "raw_shell_unlikely" and (
            rendered_word_count >= max(180, raw_text_len * 2)
            or rendered_links >= max(4, raw_links + 3)
            or title_changed
            or description_changed
        ):
            shell_state = "raw_shell_confirmed_after_render"
        elif raw_shell_state == "raw_shell_unlikely":
            shell_state = "raw_shell_unlikely"
        else:
            shell_state = "raw_shell_possible"

    return {
        "effective_title": effective_title,
        "effective_meta_description": effective_meta_description,
        "effective_canonical": effective_canonical,
        "effective_hreflang_links_json": json.dumps(effective_hreflang_links, sort_keys=True),
        "effective_content_hash": effective_content_hash,
        "effective_field_provenance_json": json.dumps(provenance, sort_keys=True),
        "canonical_conflict_raw_vs_rendered": int(canonical_conflict),
        "canonical_unresolved": int(canonical_unresolved),
        "shell_state": shell_state,
        "likely_js_shell": int(shell_state != "raw_shell_unlikely"),
    }
