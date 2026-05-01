from __future__ import annotations

import json
import gzip
import re
import xml.etree.ElementTree as ET
from urllib.parse import urljoin

from seo_audit.http_utils import http_get
from seo_audit.url_utils import normalize_url


def default_sitemap_candidates(base_url: str) -> list[str]:
    return [
        urljoin(base_url, "/sitemap.xml"),
        urljoin(base_url, "/sitemap.xml.gz"),
        urljoin(base_url, "/sitemap_index.xml"),
    ]


def _decode_sitemap_body(sitemap_url: str, body: bytes) -> str:
    raw = body
    if sitemap_url.lower().endswith(".gz") or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw.decode("utf-8", errors="replace")


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _extract_namespaces(xml_text: str) -> dict[str, str]:
    namespaces: dict[str, str] = {}
    for prefix, uri in re.findall(r"xmlns(?::([A-Za-z0-9_\-]+))?=\"([^\"]+)\"", xml_text):
        key = str(prefix or "default").strip() or "default"
        value = str(uri or "").strip()
        if value:
            namespaces[key] = value
    return namespaces


def _child_text(node: ET.Element, tag_name: str) -> str:
    for child in list(node):
        if _local_name(child.tag) == tag_name and child.text:
            return child.text.strip()
    return ""


def _collect_children(node: ET.Element, tag_name: str) -> list[ET.Element]:
    return [child for child in list(node) if _local_name(child.tag) == tag_name]


def _parse_image_extension(url_node: ET.Element) -> list[dict[str, str]]:
    images: list[dict[str, str]] = []
    for image_node in _collect_children(url_node, "image"):
        image_payload = {
            "loc": _child_text(image_node, "loc"),
            "title": _child_text(image_node, "title"),
            "caption": _child_text(image_node, "caption"),
            "license": _child_text(image_node, "license"),
        }
        if any(str(value).strip() for value in image_payload.values()):
            images.append(image_payload)
    return images


def _parse_video_extension(url_node: ET.Element) -> list[dict[str, str]]:
    videos: list[dict[str, str]] = []
    for video_node in _collect_children(url_node, "video"):
        video_payload = {
            "thumbnail_loc": _child_text(video_node, "thumbnail_loc"),
            "title": _child_text(video_node, "title"),
            "description": _child_text(video_node, "description"),
            "content_loc": _child_text(video_node, "content_loc"),
            "player_loc": _child_text(video_node, "player_loc"),
            "duration": _child_text(video_node, "duration"),
            "publication_date": _child_text(video_node, "publication_date"),
            "family_friendly": _child_text(video_node, "family_friendly"),
            "live": _child_text(video_node, "live"),
        }
        if any(str(value).strip() for value in video_payload.values()):
            videos.append(video_payload)
    return videos


def _parse_news_extension(url_node: ET.Element) -> dict[str, str]:
    news_nodes = _collect_children(url_node, "news")
    if not news_nodes:
        return {}
    news_node = news_nodes[0]
    publication_nodes = _collect_children(news_node, "publication")
    publication_name = ""
    publication_language = ""
    if publication_nodes:
        publication = publication_nodes[0]
        publication_name = _child_text(publication, "name")
        publication_language = _child_text(publication, "language")
    payload = {
        "publication_name": publication_name,
        "publication_language": publication_language,
        "publication_date": _child_text(news_node, "publication_date"),
        "title": _child_text(news_node, "title"),
        "keywords": _child_text(news_node, "keywords"),
        "stock_tickers": _child_text(news_node, "stock_tickers"),
    }
    if any(str(value).strip() for value in payload.values()):
        return payload
    return {}


def _parse_xhtml_links(url_node: ET.Element, *, sitemap_url: str) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    for link_node in _collect_children(url_node, "link"):
        rel = str(link_node.attrib.get("rel") or "").strip().lower()
        hreflang = str(link_node.attrib.get("hreflang") or "").strip().lower()
        href = str(link_node.attrib.get("href") or "").strip()
        if rel != "alternate" or not hreflang or not href:
            continue
        links.append(
            {
                "lang": hreflang,
                "href": normalize_url(href, base_url=sitemap_url),
            }
        )
    return links


def parse_sitemap_xml(sitemap_url: str, xml_text: str) -> tuple[list[dict], list[str]]:
    entries: list[dict] = []
    nested: list[str] = []

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return entries, nested

    namespace_decls = _extract_namespaces(xml_text)

    root_name = _local_name(root.tag)
    if root_name == "sitemapindex":
        for child in list(root):
            if _local_name(child.tag) != "sitemap":
                continue
            loc = _child_text(child, "loc")
            if loc:
                normalized = normalize_url(loc, base_url=sitemap_url)
                nested.append(normalized)
                entries.append(
                    {
                        "entry_kind": "sitemap_index",
                        "sitemap_url": sitemap_url,
                        "url": normalized,
                        "lastmod": "",
                        "changefreq": "",
                        "priority": "",
                        "sitemap_lastmod": _child_text(child, "lastmod"),
                        "namespace_decls_json": json.dumps(namespace_decls, sort_keys=True),
                        "extensions_json": "{}",
                        "hreflang_links_json": "[]",
                    }
                )
        return entries, nested

    if root_name != "urlset":
        return entries, nested

    for child in list(root):
        if _local_name(child.tag) != "url":
            continue
        loc = _child_text(child, "loc")
        if not loc:
            continue
        image_extension = _parse_image_extension(child)
        video_extension = _parse_video_extension(child)
        news_extension = _parse_news_extension(child)
        hreflang_links = _parse_xhtml_links(child, sitemap_url=sitemap_url)
        entries.append(
            {
                "entry_kind": "url",
                "sitemap_url": sitemap_url,
                "url": normalize_url(loc, base_url=sitemap_url),
                "lastmod": _child_text(child, "lastmod"),
                "changefreq": _child_text(child, "changefreq"),
                "priority": _child_text(child, "priority"),
                "sitemap_lastmod": "",
                "namespace_decls_json": json.dumps(namespace_decls, sort_keys=True),
                "hreflang_links_json": json.dumps(hreflang_links, sort_keys=True),
                "extensions_json": json.dumps(
                    {
                        "image": image_extension,
                        "video": video_extension,
                        "news": news_extension,
                    },
                    sort_keys=True,
                ),
            }
        )
    return entries, nested


def fetch_and_parse_sitemaps(
    start_urls: list[str],
    timeout: float,
    user_agent: str,
    limit: int = 30,
    errors: list[str] | None = None,
) -> list[dict]:
    seen: set[str] = set()
    queue = list(start_urls)
    output: list[dict] = []
    while queue and len(seen) < limit:
        sitemap_url = queue.pop(0)
        if sitemap_url in seen:
            continue
        seen.add(sitemap_url)
        try:
            headers = {"User-Agent": user_agent} if str(user_agent or "").strip() else {}
            resp = http_get(sitemap_url, timeout=timeout, headers=headers)
            if resp.status_code >= 400:
                continue
            xml_text = _decode_sitemap_body(sitemap_url, resp.content)
            entries, nested = parse_sitemap_xml(sitemap_url, xml_text)
            output.extend(entries)
            queue.extend([n for n in nested if n not in seen])
        except Exception as exc:
            if errors is not None:
                errors.append(f"{sitemap_url}: {exc}")
            continue
    return output
