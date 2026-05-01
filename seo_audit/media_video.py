from __future__ import annotations

import json
from urllib.parse import urlsplit

from seo_audit.http_utils import http_head
from seo_audit.url_utils import normalize_url


def _provider_for_embed(src: str) -> str:
    host = (urlsplit(src).hostname or "").lower()
    if "youtube" in host or "youtu.be" in host:
        return "youtube"
    if "vimeo" in host:
        return "vimeo"
    if "wistia" in host:
        return "wistia"
    return "other"


def _schema_type_tokens(raw_type: object) -> list[str]:
    if isinstance(raw_type, str):
        token = raw_type.strip()
        return [token] if token else []
    if isinstance(raw_type, list):
        values: list[str] = []
        for item in raw_type:
            values.extend(_schema_type_tokens(item))
        return values
    return []


def _node_has_type(node: dict, expected: set[str]) -> bool:
    tokens = {token.lower() for token in _schema_type_tokens(node.get("@type"))}
    return bool(tokens.intersection({token.lower() for token in expected}))


def _schema_video_rows(schema_nodes: list[dict]) -> list[dict]:
    rows: list[dict] = []
    for node in schema_nodes:
        if not isinstance(node, dict):
            continue
        if not _node_has_type(node, {"VideoObject", "Clip", "BroadcastEvent", "SeekToAction"}):
            continue
        actions: list[str] = []
        potential_action = node.get("potentialAction")
        if isinstance(potential_action, list):
            for action in potential_action:
                if isinstance(action, dict):
                    actions.extend(_schema_type_tokens(action.get("@type")))
        elif isinstance(potential_action, dict):
            actions.extend(_schema_type_tokens(potential_action.get("@type")))
        rows.append(
            {
                "type": "schema",
                "schema_type": "|".join(_schema_type_tokens(node.get("@type"))),
                "name": str(node.get("name") or node.get("headline") or "")[:180],
                "thumbnail_url": str(node.get("thumbnailUrl") or "")[:240],
                "embed_url": str(node.get("embedUrl") or "")[:240],
                "content_url": str(node.get("contentUrl") or "")[:240],
                "potential_actions": sorted({token for token in actions if token}),
            }
        )
    return rows


def _probe_url(url: str, *, timeout: float) -> dict[str, object]:
    try:
        response = http_head(url, timeout=timeout, headers={"Range": "bytes=0-0"})
        return {
            "url": url,
            "reachable": int(200 <= int(response.status_code) < 400),
            "status_code": int(response.status_code),
            "content_type": str(response.headers.get("content-type") or ""),
        }
    except Exception as exc:  # pragma: no cover
        return {
            "url": url,
            "reachable": 0,
            "status_code": 0,
            "error": str(exc),
        }


def extract_video_assets(
    tree,
    *,
    base_url: str,
    schema_nodes: list[dict] | None = None,
    meta_map: dict[str, list[str]] | None = None,
    sitemap_video_entries: list[dict] | None = None,
    probe_fetch: bool = False,
    probe_timeout: float = 2.0,
) -> tuple[list[dict], dict[str, object]]:
    video_assets: list[dict] = []

    for node in tree.xpath(".//video"):
        src = str(node.attrib.get("src") or "").strip()
        poster = str(node.attrib.get("poster") or "").strip()
        nested_sources = [
            normalize_url(str(source.attrib.get("src") or ""), base_url=base_url)
            for source in node.xpath(".//source[@src]")
            if str(source.attrib.get("src") or "").strip()
        ]
        video_assets.append(
            {
                "type": "video_tag",
                "src": normalize_url(src, base_url=base_url) if src else "",
                "nested_sources": nested_sources,
                "poster": normalize_url(poster, base_url=base_url) if poster else "",
                "has_controls": int("controls" in node.attrib),
                "watch_page_candidate": int(True),
            }
        )

    for node in tree.xpath(".//iframe[@src]"):
        src = str(node.attrib.get("src") or "").strip()
        if not src:
            continue
        provider = _provider_for_embed(src)
        if provider == "other":
            continue
        video_assets.append(
            {
                "type": "iframe_embed",
                "src": normalize_url(src, base_url=base_url),
                "provider": provider,
                "watch_page_candidate": int(True),
            }
        )

    schema_rows = _schema_video_rows(list(schema_nodes or []))
    video_assets.extend(schema_rows[:40])

    metadata = meta_map or {}
    og_video_keys = ("og:video", "og:video:url", "og:video:secure_url", "twitter:player")
    for key in og_video_keys:
        for value in metadata.get(key, []):
            cleaned = str(value or "").strip()
            if not cleaned:
                continue
            video_assets.append(
                {
                    "type": "ogp",
                    "source_key": key,
                    "src": normalize_url(cleaned, base_url=base_url),
                }
            )

    for row in list(sitemap_video_entries or [])[:40]:
        if not isinstance(row, dict):
            continue
        video_assets.append(
            {
                "type": "sitemap_video",
                "metadata_json": json.dumps(row, sort_keys=True),
                "content_url": str(row.get("content_loc") or row.get("content_url") or "").strip(),
                "thumbnail_url": str(row.get("thumbnail_loc") or "").strip(),
                "title": str(row.get("title") or "").strip()[:180],
            }
        )

    if probe_fetch:
        seen_probe_urls: set[str] = set()
        probes: list[dict[str, object]] = []
        for asset in video_assets:
            for key in ("content_url", "src", "thumbnail_url", "poster"):
                candidate = str(asset.get(key) or "").strip()
                if not candidate or candidate in seen_probe_urls:
                    continue
                seen_probe_urls.add(candidate)
                probes.append(_probe_url(candidate, timeout=probe_timeout))
        for asset in video_assets:
            related = [
                probe
                for probe in probes
                if str(probe.get("url") or "")
                in {
                    str(asset.get("content_url") or ""),
                    str(asset.get("src") or ""),
                    str(asset.get("thumbnail_url") or ""),
                    str(asset.get("poster") or ""),
                }
            ]
            if related:
                asset["fetch_probes"] = related

    count = len(video_assets)
    has_schema = int(bool(schema_rows))
    has_embed = int(any(asset.get("type") == "iframe_embed" for asset in video_assets))
    has_tag = int(any(asset.get("type") == "video_tag" for asset in video_assets))
    has_ogp = int(any(asset.get("type") == "ogp" for asset in video_assets))
    has_sitemap_video = int(any(asset.get("type") == "sitemap_video" for asset in video_assets))

    probe_total = 0
    probe_success = 0
    for asset in video_assets:
        for probe in asset.get("fetch_probes", []) if isinstance(asset.get("fetch_probes"), list) else []:
            if not isinstance(probe, dict):
                continue
            probe_total += 1
            probe_success += int(probe.get("reachable") or 0)

    probe_success_ratio = (probe_success / probe_total) if probe_total else 0.0

    score = 0
    if has_schema:
        score += 40
    if has_embed:
        score += 25
    if has_tag:
        score += 25
    if has_ogp:
        score += 5
    if has_sitemap_video:
        score += 5
    if probe_total > 0:
        score += int(round(probe_success_ratio * 10.0))
    if count > 0:
        score += 10
    score = max(0, min(100, score))

    summary = {
        "count": count,
        "has_schema": has_schema,
        "has_embed": has_embed,
        "has_video_tag": has_tag,
        "has_ogp_video": has_ogp,
        "has_sitemap_video": has_sitemap_video,
        "probe_total": probe_total,
        "probe_success_ratio": probe_success_ratio,
        "discoverability_score": score,
    }
    return video_assets, summary
