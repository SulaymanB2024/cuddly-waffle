from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from seo_audit.url_utils import is_internal_url, normalize_url


def _json_object(raw: object) -> dict[str, Any]:
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


def _json_list(raw: object) -> list[Any]:
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


def _first_path_segment(url: str) -> str:
    token = normalize_url(url)
    if "/" not in token:
        return ""
    parts = token.split("/", 3)
    if len(parts) < 4:
        return ""
    path = parts[3].strip("/")
    if not path:
        return "home"
    return path.split("/", 1)[0].lower()


def _template_bucket(url: str) -> str:
    segment = _first_path_segment(url)
    if segment in {"home", ""}:
        return "home"
    if segment in {"service", "services"}:
        return "service"
    if segment in {"blog", "news", "insights", "article", "articles"}:
        return "article"
    if segment in {"product", "products", "location", "locations"}:
        return "commercial"
    return "other"


def _parse_lastmod(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    normalized = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def analyze_sitemap_intelligence(domain: str, pages: list[dict], sitemap_entries: list[dict]) -> dict[str, Any]:
    normalized_domain = normalize_url(domain)
    actionable_pages = [
        page for page in pages if int(page.get("status_code") or 0) >= 200 and int(page.get("status_code") or 0) < 400
    ]
    crawled_urls = {
        normalize_url(str(page.get("normalized_url") or ""), base_url=normalized_domain)
        for page in actionable_pages
        if str(page.get("normalized_url") or "").strip()
    }

    url_entries = [
        entry
        for entry in sitemap_entries
        if str(entry.get("entry_kind") or "url").strip().lower() == "url"
    ]

    sitemap_urls = {
        normalize_url(str(entry.get("url") or ""), base_url=normalized_domain)
        for entry in url_entries
        if str(entry.get("url") or "").strip()
    }

    sitemap_images = 0
    sitemap_videos = 0
    sitemap_hreflang_pairs = set()
    extension_coverage_by_bucket: Counter[str] = Counter()
    stale_lastmod_urls: list[str] = []
    missing_lastmod_urls: list[str] = []
    scope_violations: list[str] = []

    now = datetime.now(timezone.utc)
    for entry in url_entries:
        url = normalize_url(str(entry.get("url") or ""), base_url=normalized_domain)
        if not url:
            continue

        if not is_internal_url(url, normalized_domain, base_url=normalized_domain, scope_mode="apex_www"):
            scope_violations.append(url)

        extensions_payload = _json_object(entry.get("extensions_json"))
        image_rows = _json_list(extensions_payload.get("image"))
        video_rows = _json_list(extensions_payload.get("video"))
        if image_rows:
            extension_coverage_by_bucket[f"{_template_bucket(url)}:image"] += 1
        if video_rows:
            extension_coverage_by_bucket[f"{_template_bucket(url)}:video"] += 1
        if _json_object(extensions_payload.get("news")):
            extension_coverage_by_bucket[f"{_template_bucket(url)}:news"] += 1

        sitemap_images += len([row for row in image_rows if isinstance(row, dict)])
        sitemap_videos += len([row for row in video_rows if isinstance(row, dict)])

        hreflang_rows = _json_list(entry.get("hreflang_links_json"))
        for row in hreflang_rows:
            if not isinstance(row, dict):
                continue
            lang = str(row.get("lang") or "").strip().lower()
            href = normalize_url(str(row.get("href") or ""), base_url=url)
            if lang and href:
                sitemap_hreflang_pairs.add((url, lang, href))

        lastmod = _parse_lastmod(str(entry.get("lastmod") or ""))
        if lastmod is None:
            missing_lastmod_urls.append(url)
        else:
            age_days = (now - lastmod).days
            if age_days >= 365:
                stale_lastmod_urls.append(url)

    on_page_images = sum(int(page.get("image_count") or 0) for page in actionable_pages)
    on_page_videos = 0
    for page in actionable_pages:
        video_rows = _json_list(page.get("video_details_json"))
        on_page_videos += len([row for row in video_rows if isinstance(row, dict)])

    page_hreflang_pairs = set()
    for page in actionable_pages:
        page_url = normalize_url(str(page.get("normalized_url") or ""), base_url=normalized_domain)
        for row in _json_list(page.get("hreflang_links_json")):
            if not isinstance(row, dict):
                continue
            lang = str(row.get("lang") or "").strip().lower()
            href = normalize_url(str(row.get("href") or ""), base_url=page_url)
            if lang and href:
                page_hreflang_pairs.add((page_url, lang, href))

    sitemap_hreflang_mismatches = sorted(sitemap_hreflang_pairs - page_hreflang_pairs)

    return {
        "sitemap_url_count": len(sitemap_urls),
        "discovered_page_count": len(crawled_urls),
        "urls_in_sitemap_not_crawled": len(sitemap_urls - crawled_urls),
        "crawled_urls_not_in_sitemap": len(crawled_urls - sitemap_urls),
        "sitemap_assets": {
            "images": sitemap_images,
            "videos": sitemap_videos,
        },
        "on_page_assets": {
            "images": on_page_images,
            "videos": on_page_videos,
        },
        "sitemap_hreflang_pairs": len(sitemap_hreflang_pairs),
        "page_hreflang_pairs": len(page_hreflang_pairs),
        "sitemap_hreflang_mismatches": len(sitemap_hreflang_mismatches),
        "stale_lastmod_urls": len(stale_lastmod_urls),
        "missing_lastmod_urls": len(missing_lastmod_urls),
        "extension_coverage_by_page_type": dict(sorted(extension_coverage_by_bucket.items())),
        "sitemap_scope_violations": len(scope_violations),
        "scope_violation_samples": scope_violations[:8],
    }


def collect_optional_gsc_sitemap_status(
    *,
    property_uri: str,
    credentials_json: str,
    known_sitemaps: list[str],
) -> dict[str, Any]:
    creds_path = str(credentials_json or "").strip()
    if not creds_path:
        return {
            "status": "skipped_missing_credentials",
            "property_uri": property_uri,
            "rows": [],
        }

    if not Path(creds_path).exists():
        return {
            "status": "failed_invalid_credentials_path",
            "property_uri": property_uri,
            "rows": [],
            "message": f"credentials file not found: {creds_path}",
        }

    try:
        from google.oauth2 import service_account  # type: ignore
        from googleapiclient.discovery import build  # type: ignore
    except Exception as exc:  # pragma: no cover
        return {
            "status": "failed_dependency_missing",
            "property_uri": property_uri,
            "rows": [],
            "message": str(exc),
        }

    try:  # pragma: no cover
        credentials = service_account.Credentials.from_service_account_file(
            creds_path,
            scopes=["https://www.googleapis.com/auth/webmasters.readonly"],
        )
        service = build("searchconsole", "v1", credentials=credentials, cache_discovery=False)
        payload = service.sitemaps().list(siteUrl=property_uri).execute()
        sitemap_rows = payload.get("sitemap") or []
        rows: list[dict[str, Any]] = []
        known = {normalize_url(url, base_url=property_uri) for url in known_sitemaps if str(url or "").strip()}
        for row in sitemap_rows:
            if not isinstance(row, dict):
                continue
            sitemap_path = normalize_url(str(row.get("path") or ""), base_url=property_uri)
            rows.append(
                {
                    "path": sitemap_path,
                    "lastSubmitted": str(row.get("lastSubmitted") or ""),
                    "lastDownloaded": str(row.get("lastDownloaded") or ""),
                    "errors": int(row.get("errors") or 0),
                    "warnings": int(row.get("warnings") or 0),
                    "contents": row.get("contents") if isinstance(row.get("contents"), list) else [],
                    "known_to_crawler": int(sitemap_path in known),
                }
            )
        return {
            "status": "success",
            "property_uri": property_uri,
            "rows": rows,
        }
    except Exception as exc:
        return {
            "status": "failed_api",
            "property_uri": property_uri,
            "rows": [],
            "message": str(exc),
        }
