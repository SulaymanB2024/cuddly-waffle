from __future__ import annotations

import hashlib
import json
import re
from typing import Any
from urllib.parse import urlsplit

from lxml import html as lxml_html

from seo_audit.media_images import extract_image_assets
from seo_audit.media_video import extract_video_assets
from seo_audit.page_controls import (
    build_effective_robots_payload,
    resolve_page_controls,
    summarize_directives,
)
from seo_audit.schema_graph import parse_schema_graph_nodes
from seo_audit.schema_validation import validate_schema_nodes
from seo_audit.url_utils import is_internal_url

from seo_audit.url_utils import normalize_url


_WS_RE = re.compile(r"\s+")
_VISIBLE_TEXT_XPATH = (
    "//text()[not(ancestor::head) and not(ancestor::script) and not(ancestor::style) "
    "and not(ancestor::noscript) and not(ancestor::template)]"
)
_HEADING_XPATH = ".//h1|.//h2|.//h3|.//h4|.//h5|.//h6"
_DOM_REGION_TAGS = {"main", "nav", "header", "footer", "aside"}

_SCHEMA_CUSTOM_SUMMARY_TYPES = {
    "Article",
    "BlogPosting",
    "Product",
    "BreadcrumbList",
    "LocalBusiness",
    "Organization",
}
_SCHEMA_CANONICAL_CASE = {token.lower(): token for token in _SCHEMA_CUSTOM_SUMMARY_TYPES}


def _normalize_whitespace(value: str) -> str:
    return _WS_RE.sub(" ", value or "").strip()


def _parse_tree(html: str):
    parser = lxml_html.HTMLParser(recover=True)
    try:
        return lxml_html.fromstring(html, parser=parser)
    except Exception:
        fallback_html = f"<html><body>{html}</body></html>"
        return lxml_html.fromstring(fallback_html, parser=parser)


def _schema_scalar(value: object) -> str:
    if isinstance(value, str):
        return _normalize_whitespace(value)
    if isinstance(value, (int, float)):
        return str(value)
    return ""


def _schema_entity_name(value: object) -> str:
    if isinstance(value, list):
        for item in value:
            name = _schema_entity_name(item)
            if name:
                return name
        return ""
    if isinstance(value, dict):
        for key in ("name", "headline", "@id", "url"):
            name = _schema_entity_name(value.get(key))
            if name:
                return name
        return ""
    return _schema_scalar(value)


def _schema_type_tokens(raw_type: object) -> list[str]:
    if isinstance(raw_type, str):
        token = _normalize_schema_type_token(raw_type)
        return [token] if token else []
    if isinstance(raw_type, list):
        values: list[str] = []
        for item in raw_type:
            values.extend(_schema_type_tokens(item))
        return list(dict.fromkeys(values))
    return []


def _normalize_schema_type_token(raw_type: str) -> str:
    token = str(raw_type or "").strip()
    if not token:
        return ""

    if token.startswith("http://") or token.startswith("https://"):
        token = token.rsplit("/", 1)[-1]
    if "#" in token:
        token = token.rsplit("#", 1)[-1]
    if ":" in token:
        token = token.rsplit(":", 1)[-1]
    token = token.strip()
    return _SCHEMA_CANONICAL_CASE.get(token.lower(), token)


def _meta_value(meta_map: dict[str, list[str]], key: str) -> str:
    values = [str(value or "") for value in meta_map.get(key, [])]
    for value in reversed(values):
        if value.strip():
            return value
    return values[-1] if values else ""


def _collect_x_robots_values(
    headers: dict[str, str],
    header_lists: dict[str, list[str]] | None,
) -> list[str]:
    values: list[str] = []
    if header_lists:
        values.extend([str(value) for value in header_lists.get("x-robots-tag", [])])

    if not values:
        fallback = (
            headers.get("x-robots-tag")
            or headers.get("X-Robots-Tag")
            or headers.get("x-robots-Tag")
            or ""
        )
        if str(fallback).strip():
            values.append(str(fallback))

    return [str(value).strip() for value in values if str(value).strip()]


def _schema_value_from_element(node: Any) -> Any:
    if node is None:
        return ""

    content = _normalize_whitespace(str(node.attrib.get("content", "")))
    if content:
        return content

    for attr in ("href", "src", "datetime", "value", "data"):
        value = _normalize_whitespace(str(node.attrib.get(attr, "")))
        if value:
            return value

    return _normalize_whitespace(node.text_content())


def _add_schema_property(target: dict[str, Any], key: str, value: Any) -> None:
    if not key:
        return
    if isinstance(value, str) and not value:
        return

    if key not in target:
        target[key] = value
        return

    existing = target[key]
    if isinstance(existing, list):
        if value not in existing:
            existing.append(value)
        return

    if existing == value:
        return
    target[key] = [existing, value]


def _nearest_ancestor_with_attr(node: Any, attr_name: str) -> Any:
    current = getattr(node, "getparent", lambda: None)()
    while current is not None:
        if attr_name in getattr(current, "attrib", {}):
            return current
        current = getattr(current, "getparent", lambda: None)()
    return None


def _extract_microdata_scope(scope: Any) -> dict[str, Any] | None:
    type_tokens = [
        _normalize_schema_type_token(token)
        for token in str(scope.attrib.get("itemtype", "")).split()
        if _normalize_schema_type_token(token)
    ]
    if not type_tokens:
        return None

    node: dict[str, Any] = {"@context": "https://schema.org"}
    node["@type"] = type_tokens[0] if len(type_tokens) == 1 else list(dict.fromkeys(type_tokens))

    for prop_node in scope.xpath(".//*[@itemprop]"):
        if _nearest_ancestor_with_attr(prop_node, "itemscope") is not scope:
            continue
        raw_itemprop = _normalize_whitespace(str(prop_node.attrib.get("itemprop", "")))
        if not raw_itemprop:
            continue

        prop_names = [_normalize_schema_type_token(token) for token in raw_itemprop.split()]
        prop_names = [name for name in prop_names if name]
        if not prop_names:
            continue

        if prop_node is not scope and "itemscope" in getattr(prop_node, "attrib", {}):
            value = _extract_microdata_scope(prop_node)
            if value is None:
                value = _schema_value_from_element(prop_node)
        else:
            value = _schema_value_from_element(prop_node)

        for prop_name in prop_names:
            _add_schema_property(node, prop_name, value)

    return node


def _extract_microdata_schema_nodes(tree: Any) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []
    for scope in tree.xpath(".//*[@itemscope]"):
        parsed = _extract_microdata_scope(scope)
        if parsed:
            nodes.append(parsed)
    return nodes


def _extract_rdfa_schema_nodes(tree: Any) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []
    for scope in tree.xpath(".//*[@typeof]"):
        raw_typeof = _normalize_whitespace(str(scope.attrib.get("typeof", "")))
        if not raw_typeof:
            continue

        type_tokens = [_normalize_schema_type_token(token) for token in raw_typeof.split()]
        type_tokens = [token for token in type_tokens if token]
        if not type_tokens:
            continue

        node: dict[str, Any] = {"@context": "https://schema.org"}
        node["@type"] = type_tokens[0] if len(type_tokens) == 1 else list(dict.fromkeys(type_tokens))

        for prop_node in scope.xpath(".//*[@property]"):
            if _nearest_ancestor_with_attr(prop_node, "typeof") is not scope:
                continue

            raw_prop = _normalize_whitespace(str(prop_node.attrib.get("property", "")))
            if not raw_prop:
                continue

            prop_names = [_normalize_schema_type_token(token) for token in raw_prop.split()]
            prop_names = [name for name in prop_names if name]
            if not prop_names:
                continue

            value = _schema_value_from_element(prop_node)
            for prop_name in prop_names:
                _add_schema_property(node, prop_name, value)

        nodes.append(node)

    return nodes


def _dedupe_schema_nodes(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for node in nodes:
        if not isinstance(node, dict):
            continue
        try:
            key = json.dumps(node, sort_keys=True)
        except TypeError:
            continue
        if key in seen:
            continue
        seen.add(key)
        deduped.append(node)
    return deduped


def _schema_summary_for_type(node: dict, schema_type: str) -> dict[str, str | int]:
    summary: dict[str, str | int] = {"type": schema_type}
    name = _schema_entity_name(node.get("name"))
    if name:
        summary["name"] = name
    url = _schema_entity_name(node.get("url") or node.get("@id"))
    if url:
        summary["url"] = url

    if schema_type in {"Article", "BlogPosting"}:
        headline = _schema_entity_name(node.get("headline"))
        author = _schema_entity_name(node.get("author"))
        published = _schema_scalar(node.get("datePublished"))
        if headline:
            summary["headline"] = headline
        if author:
            summary["author"] = author
        if published:
            summary["datePublished"] = published

    if schema_type == "Product":
        sku = _schema_scalar(node.get("sku"))
        brand = _schema_entity_name(node.get("brand"))
        offers = node.get("offers")
        first_offer = offers[0] if isinstance(offers, list) and offers else offers
        if sku:
            summary["sku"] = sku
        if brand:
            summary["brand"] = brand
        if isinstance(first_offer, dict):
            currency = _schema_scalar(first_offer.get("priceCurrency"))
            price = _schema_scalar(first_offer.get("price"))
            availability = _schema_entity_name(first_offer.get("availability"))
            if currency:
                summary["priceCurrency"] = currency
            if price:
                summary["price"] = price
            if availability:
                summary["availability"] = availability

    if schema_type == "BreadcrumbList":
        items = node.get("itemListElement")
        if isinstance(items, list):
            summary["itemCount"] = len(items)

    if schema_type == "LocalBusiness":
        telephone = _schema_scalar(node.get("telephone"))
        address = node.get("address")
        if telephone:
            summary["telephone"] = telephone
        if isinstance(address, dict):
            locality = _schema_scalar(address.get("addressLocality"))
            region = _schema_scalar(address.get("addressRegion"))
            postal = _schema_scalar(address.get("postalCode"))
            country = _schema_scalar(address.get("addressCountry"))
            if locality:
                summary["addressLocality"] = locality
            if region:
                summary["addressRegion"] = region
            if postal:
                summary["postalCode"] = postal
            if country:
                summary["addressCountry"] = country

    if schema_type == "Organization":
        same_as = node.get("sameAs")
        if isinstance(same_as, list):
            summary["sameAsCount"] = len(same_as)

    return summary


def _node_dom_region(node: object) -> str:
    if node is None:
        return "unknown"
    current = node
    while current is not None:
        tag = getattr(current, "tag", "")
        tag_name = str(tag).split("}")[-1].lower() if isinstance(tag, str) else ""
        if tag_name in _DOM_REGION_TAGS:
            return tag_name
        current = getattr(current, "getparent", lambda: None)()
    return "unknown"


def _visible_words(html_tree) -> tuple[list[str], str]:
    text_nodes = html_tree.xpath(_VISIBLE_TEXT_XPATH)
    parts = [_normalize_whitespace(str(node)) for node in text_nodes]
    visible_text = _normalize_whitespace(" ".join(part for part in parts if part))
    words = [word for word in re.split(r"\W+", visible_text) if word]
    return words, visible_text


def _text_hash(visible_text: str) -> str:
    # Case-folding keeps duplicate detection deterministic across casing-only variants.
    normalized = _normalize_whitespace(visible_text).casefold()
    if not normalized:
        return ""
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _iter_json_ld_nodes(value: object):
    if isinstance(value, dict):
        yield value
        for nested in value.values():
            if isinstance(nested, (dict, list)):
                yield from _iter_json_ld_nodes(nested)
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, (dict, list)):
                yield from _iter_json_ld_nodes(item)


def extract_page_data(
    html: str,
    final_url: str,
    status_code: int,
    content_type: str,
    headers: dict[str, str],
    header_lists: dict[str, list[str]] | None = None,
    crawler_token: str = "googlebot",
    site_root_url: str | None = None,
    scope_mode: str = "apex_www",
    custom_allowlist: tuple[str, ...] | list[str] | set[str] | None = None,
) -> dict:
    tree = _parse_tree(html)
    html_lower = html.lower()

    language = _normalize_whitespace(tree.xpath("string((/html|//html)[1]/@lang)"))

    meta_map: dict[str, list[str]] = {}
    for meta_node in tree.xpath(".//meta"):
        content = _normalize_whitespace(str(meta_node.attrib.get("content", "")))
        name = _normalize_whitespace(str(meta_node.attrib.get("name", ""))).lower()
        prop = _normalize_whitespace(str(meta_node.attrib.get("property", ""))).lower()
        if name:
            meta_map.setdefault(name, []).append(content)
        if prop:
            meta_map.setdefault(prop, []).append(content)

    title_inventory: list[str] = []
    for title_node in tree.xpath(".//title"):
        text_value = _normalize_whitespace(title_node.text_content())
        if text_value:
            title_inventory.append(text_value)
    title_inventory = list(dict.fromkeys(title_inventory))

    meta_description_inventory = [
        _normalize_whitespace(str(value or ""))
        for value in meta_map.get("description", [])
        if _normalize_whitespace(str(value or ""))
    ]
    meta_description_inventory = list(dict.fromkeys(meta_description_inventory))

    canonical_urls: list[str] = []
    hreflang_links: list[dict[str, str]] = []
    rel_next = ""
    rel_prev = ""
    for link in tree.xpath(".//link[@href]"):
        href = _normalize_whitespace(str(link.attrib.get("href", "")))
        if not href:
            continue
        rel_tokens = {
            token.strip().lower()
            for token in str(link.attrib.get("rel", "")).split()
            if token.strip()
        }
        if "canonical" in rel_tokens:
            canonical_urls.append(href)
        if "alternate" in rel_tokens:
            hreflang = _normalize_whitespace(str(link.attrib.get("hreflang", "")))
            if hreflang:
                hreflang_links.append({"lang": hreflang, "href": href})
        if "next" in rel_tokens and not rel_next:
            rel_next = href
        if ("prev" in rel_tokens or "previous" in rel_tokens) and not rel_prev:
            rel_prev = href

    headings: list[dict[str, str | int]] = []
    h1_values: list[str] = []
    h2_values: list[str] = []
    for heading in tree.xpath(_HEADING_XPATH):
        tag = str(getattr(heading, "tag", "")).split("}")[-1].lower()
        text_value = _normalize_whitespace(heading.text_content())
        if not text_value:
            continue
        if tag == "h1":
            h1_values.append(text_value)
        if tag == "h2":
            h2_values.append(text_value)
        level = int(tag[1]) if len(tag) == 2 and tag[0] == "h" and tag[1].isdigit() else 0
        if level:
            headings.append({"level": level, "text": text_value})

    links: list[dict] = []
    for anchor in tree.xpath(".//a[@href]"):
        href = _normalize_whitespace(str(anchor.attrib.get("href", "")))
        if not href:
            continue
        rel_tokens = {
            token.strip().lower()
            for token in str(anchor.attrib.get("rel", "")).split()
            if token.strip()
        }
        links.append(
            {
                "href": href,
                "nofollow": "nofollow" in rel_tokens,
                "anchor_text": _normalize_whitespace(anchor.text_content()),
                "dom_region": _node_dom_region(anchor),
            }
        )

    schema_nodes, schema_parse_error_count = parse_schema_graph_nodes(tree)
    schema_nodes = _dedupe_schema_nodes(
        [
            *schema_nodes,
            *_extract_microdata_schema_nodes(tree),
            *_extract_rdfa_schema_nodes(tree),
        ]
    )
    schema_types: set[str] = set()
    schema_summary: list[dict[str, str | int]] = []
    schema_summary_types: set[str] = set()
    seen_schema_summary: set[str] = set()
    for node in schema_nodes:
        node_types = _schema_type_tokens(node.get("@type"))
        for schema_type in node_types:
            schema_types.add(str(schema_type))

        for schema_type in node_types:
            summary = _schema_summary_for_type(node, schema_type)
            if summary == {"type": schema_type}:
                continue
            summary_key = json.dumps(summary, sort_keys=True)
            if summary_key in seen_schema_summary:
                continue
            seen_schema_summary.add(summary_key)
            schema_summary.append(summary)
            if schema_type in _SCHEMA_CUSTOM_SUMMARY_TYPES:
                schema_summary_types.add(schema_type)
            if len(schema_summary) >= 40:
                break
        if len(schema_summary) >= 40:
            break

    og_image_urls = [str(value or "").strip() for value in meta_map.get("og:image", []) if str(value or "").strip()]
    image_assets, image_summary = extract_image_assets(
        tree,
        base_url=final_url,
        schema_nodes=schema_nodes,
        og_image_urls=og_image_urls,
    )
    image_count = int(image_summary.get("count") or 0)
    image_alt_coverage = float(image_summary.get("alt_coverage") or (1.0 if image_count == 0 else 0.0))

    video_assets, video_summary = extract_video_assets(
        tree,
        base_url=final_url,
        schema_nodes=schema_nodes,
        meta_map=meta_map,
    )

    words, visible_text = _visible_words(tree)

    title = _normalize_whitespace(tree.xpath("string(//title[1])"))
    if not title:
        match = re.search(r"<title[^>]*>([^<]+)", html, re.IGNORECASE)
        if match:
            title = match.group(1).strip()

    meta_description = _meta_value(meta_map, "description")
    if not meta_description:
        match = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)', html, re.IGNORECASE)
        if match:
            meta_description = match.group(1).strip()

    h1_value = h1_values[0] if h1_values else ""
    if not h1_value:
        match = re.search(r"<h1[^>]*>([^<]+)", html, re.IGNORECASE)
        if match:
            h1_value = match.group(1).strip()
    if not h1_values and h1_value:
        h1_values = [h1_value]

    internal = 0
    external = 0
    anchors = []
    link_root = site_root_url or final_url
    for link in links:
        href = link["href"].strip()
        if href.startswith("#"):
            continue
        scheme = urlsplit(href).scheme.lower()
        if scheme and scheme not in {"http", "https"}:
            continue
        if is_internal_url(
            href,
            link_root,
            base_url=final_url,
            scope_mode=scope_mode,
            custom_allowlist=custom_allowlist,
        ):
            internal += 1
        else:
            external += 1
        anchors.append(link)

    x_robots_values = _collect_x_robots_values(headers, header_lists)
    meta_robots_values = [str(value) for value in meta_map.get("robots", []) if str(value).strip()]
    meta_robots = ", ".join(meta_robots_values)
    x_robots = ", ".join(x_robots_values)

    robots_decision = resolve_page_controls(
        meta_map=meta_map,
        x_robots_values=x_robots_values,
        crawler_token=crawler_token,
    )
    robots_summary = summarize_directives(robots_decision)
    effective_robots_payload = build_effective_robots_payload(robots_decision)

    noindex = bool(robots_summary["is_noindex"])
    nofollow = bool(robots_summary["is_nofollow"])
    has_nosnippet = int(bool(robots_summary["has_nosnippet_directive"]))
    max_snippet = str(robots_summary["max_snippet_directive"] or "")
    max_image_preview = str(robots_summary["max_image_preview_directive"] or "")
    max_video_preview = str(robots_summary["max_video_preview_directive"] or "")

    if not words:
        scrubbed = re.sub(r"(?is)<(script|style|head)[^>]*>.*?</\\1>", " ", html)
        text_fallback = re.sub(r"<[^>]+>", " ", scrubbed)
        words = [w for w in re.split(r"\W+", text_fallback) if w]
        visible_text = _normalize_whitespace(text_fallback)

    shell_inventory = {
        "empty_mount_root": int(
            bool(
                re.search(
                    r"<div[^>]+id=['\"](root|app|__next|__nuxt)['\"][^>]*>\s*</div>",
                    html,
                    re.IGNORECASE,
                )
            )
        ),
        "noscript_requires_javascript": int(
            bool(re.search(r"<noscript[^>]*>[^<]*(enable|requires?)\s+javascript", html, re.IGNORECASE))
        ),
        "head_script_count": len(re.findall(r"(?is)<head[^>]*>.*?<script\b", html)),
        "script_count": len(re.findall(r"<script\b", html, re.IGNORECASE)),
        "module_script_count": len(re.findall(r"<script[^>]+type=['\"]module['\"]", html, re.IGNORECASE)),
        "framework_hints": [
            hint for hint in ("react", "vite", "next", "nuxt", "webpack", "__next", "__nuxt") if hint in html_lower
        ],
    }

    schema_validation = validate_schema_nodes(schema_nodes, visible_text=visible_text)
    schema_validation_payload = {
        "syntax_valid": bool(schema_validation.syntax_valid and schema_parse_error_count == 0),
        "recognized_types": schema_validation.recognized_types,
        "eligible_features": schema_validation.eligible_features,
        "deprecated_features": schema_validation.deprecated_features,
        "missing_required_by_feature": schema_validation.missing_required_by_feature,
        "missing_recommended_by_feature": schema_validation.missing_recommended_by_feature,
        "visible_content_mismatches": schema_validation.visible_content_mismatches,
        "engine_feature_scores": schema_validation.engine_feature_scores,
        "type_counts": schema_validation.type_counts,
        "findings": schema_validation.findings,
        "legacy_schema_validation_score": int(schema_validation.score),
    }

    normalized_canonicals: list[str] = []
    seen_canonicals: set[str] = set()
    for href in canonical_urls:
        normalized = normalize_url(href, base_url=final_url)
        if normalized not in seen_canonicals:
            seen_canonicals.add(normalized)
            normalized_canonicals.append(normalized)

    normalized_hreflang: list[dict] = []
    seen_hreflang: set[tuple[str, str]] = set()
    for link in hreflang_links:
        lang = str(link.get("lang") or "").strip().lower()
        href = str(link.get("href") or "").strip()
        if not lang or not href:
            continue
        normalized_href = normalize_url(href, base_url=final_url)
        key = (lang, normalized_href)
        if key in seen_hreflang:
            continue
        seen_hreflang.add(key)
        normalized_hreflang.append({"lang": lang, "href": normalized_href})

    raw_content_hash = _text_hash(visible_text)

    return {
        "final_url": final_url,
        "status_code": status_code,
        "content_type": content_type,
        "canonical_url": normalize_url(canonical_urls[0], base_url=final_url) if canonical_urls else "",
        "canonical_count": len(canonical_urls),
        "canonical_urls_json": json.dumps(normalized_canonicals),
        "raw_canonical_urls_json": json.dumps(normalized_canonicals),
        "title": title,
        "raw_title": title,
        "title_inventory_json": json.dumps(title_inventory),
        "meta_description": meta_description,
        "raw_meta_description": meta_description,
        "meta_description_inventory_json": json.dumps(meta_description_inventory),
        "meta_robots": meta_robots,
        "robots_meta_inventory_json": json.dumps(meta_robots_values),
        "x_robots_tag": x_robots,
        "x_robots_inventory_json": json.dumps(x_robots_values),
        "effective_robots_json": json.dumps(effective_robots_payload, sort_keys=True),
        "h1": h1_value,
        "h1_count": len(h1_values) if h1_values else int(bool(h1_value)),
        "h1s": h1_values if h1_values else ([h1_value] if h1_value else []),
        "h2_json": json.dumps(h2_values),
        "heading_outline_json": json.dumps(headings),
        "word_count": len(words),
        "language": language,
        "og_title": _meta_value(meta_map, "og:title"),
        "og_description": _meta_value(meta_map, "og:description"),
        "og_url": _meta_value(meta_map, "og:url"),
        "twitter_title": _meta_value(meta_map, "twitter:title"),
        "twitter_description": _meta_value(meta_map, "twitter:description"),
        "schema_types_json": json.dumps(sorted(schema_types)),
        "schema_summary_json": json.dumps(schema_summary),
        "schema_summary_types_json": json.dumps(sorted(schema_summary_types)),
        "schema_parse_error_count": schema_parse_error_count,
        "hreflang_links_json": json.dumps(normalized_hreflang),
        "raw_hreflang_links_json": json.dumps(normalized_hreflang),
        "hreflang_count": len(normalized_hreflang),
        "rel_next_url": normalize_url(rel_next, base_url=final_url) if rel_next else "",
        "rel_prev_url": normalize_url(rel_prev, base_url=final_url) if rel_prev else "",
        "data_nosnippet_count": len(tree.xpath("//*[@data-nosnippet]")),
        "has_nosnippet_directive": has_nosnippet,
        "max_snippet_directive": max_snippet,
        "max_image_preview_directive": max_image_preview,
        "max_video_preview_directive": max_video_preview,
        "image_count": image_count,
        "image_alt_coverage": image_alt_coverage,
        "image_details_json": json.dumps(image_assets),
        "image_discoverability_score": int(image_summary.get("discoverability_score") or 0),
        "video_details_json": json.dumps(video_assets),
        "video_discoverability_score": int(video_summary.get("discoverability_score") or 0),
        "internal_links_out": internal,
        "external_links_out": external,
        "is_indexable": int(not noindex and status_code < 400),
        "is_noindex": int(noindex),
        "is_nofollow": int(nofollow),
        "thin_content_flag": int(len(words) < 150),
        "raw_word_count": len(words),
        "content_hash": raw_content_hash,
        "raw_content_hash": raw_content_hash,
        "head_inventory_json": json.dumps(
            {
                "canonical_count": len(normalized_canonicals),
                "hreflang_count": len(normalized_hreflang),
                "title_count": len(title_inventory),
                "meta_description_count": len(meta_description_inventory),
                "meta_robots_count": len(meta_robots_values),
                "x_robots_count": len(x_robots_values),
            },
            sort_keys=True,
        ),
        "shell_inventory_json": json.dumps(shell_inventory, sort_keys=True),
        "schema_graph_json": json.dumps(schema_nodes),
        "schema_validation_json": json.dumps(schema_validation_payload, sort_keys=True),
        "schema_validation_score": int(schema_validation.score),
        "anchors": anchors,
    }
