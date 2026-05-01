from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlsplit

import requests

from seo_audit.url_utils import normalize_url


_GSC_READONLY_SCOPE = "https://www.googleapis.com/auth/webmasters.readonly"
_GSC_INSPECTION_ENDPOINT = "https://searchconsole.googleapis.com/v1/urlInspection/index:inspect"


def property_candidates(domain: str) -> list[str]:
    root = normalize_url(domain)
    host = (urlsplit(root).hostname or "").lower()
    apex = host[4:] if host.startswith("www.") else host
    candidates = [
        f"sc-domain:{apex}",
        f"https://{apex}/",
        f"https://www.{apex}/",
    ]
    seen: set[str] = set()
    ordered: list[str] = []
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        ordered.append(candidate)
    return ordered


def resolve_property(domain: str, explicit_property: str = "") -> str:
    if explicit_property.strip():
        return explicit_property.strip()
    candidates = property_candidates(domain)
    return candidates[0] if candidates else ""


def collect_index_states(
    property_uri: str,
    urls: list[str],
    *,
    credentials_json: str = "",
    timeout: float = 10.0,
) -> tuple[list[dict], dict[str, object]]:
    normalized_urls: list[str] = []
    seen: set[str] = set()
    for raw in urls:
        normalized = normalize_url(raw)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        normalized_urls.append(normalized)

    if not credentials_json:
        return [], {
            "property_uri": property_uri,
            "status": "skipped_missing_credentials",
            "urls_requested": len(normalized_urls),
            "rows_returned": 0,
            "message": "Search Console credentials were not provided.",
        }

    if not normalized_urls:
        return [], {
            "property_uri": property_uri,
            "status": "success_empty",
            "urls_requested": 0,
            "rows_returned": 0,
            "message": "No URLs were provided for Search Console inspection.",
        }

    credentials_path = Path(credentials_json)
    if not credentials_path.exists():
        return [], {
            "property_uri": property_uri,
            "status": "failed_invalid_credentials_path",
            "urls_requested": len(normalized_urls),
            "rows_returned": 0,
            "message": f"Credentials file not found: {credentials_path}",
        }

    try:
        from google.auth.transport.requests import Request as GoogleAuthRequest  # type: ignore[import-not-found]
        from google.oauth2 import service_account  # type: ignore[import-not-found]
    except Exception as exc:
        return [], {
            "property_uri": property_uri,
            "status": "failed_missing_dependency",
            "urls_requested": len(normalized_urls),
            "rows_returned": 0,
            "message": f"google-auth dependency is required for GSC inspection: {exc}",
        }

    try:
        credentials = service_account.Credentials.from_service_account_file(
            str(credentials_path),
            scopes=[_GSC_READONLY_SCOPE],
        )
        credentials.refresh(GoogleAuthRequest())
        access_token = str(credentials.token or "").strip()
        if not access_token:
            raise RuntimeError("empty access token from credentials refresh")
    except Exception as exc:
        return [], {
            "property_uri": property_uri,
            "status": "failed_auth",
            "urls_requested": len(normalized_urls),
            "rows_returned": 0,
            "message": f"Failed to authenticate Search Console credentials: {exc}",
        }

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    rows: list[dict] = []
    error_samples: list[str] = []

    for url in normalized_urls:
        body = {
            "inspectionUrl": url,
            "siteUrl": property_uri,
            "languageCode": "en-US",
        }
        try:
            response = requests.post(_GSC_INSPECTION_ENDPOINT, headers=headers, json=body, timeout=timeout)
        except requests.RequestException as exc:
            if len(error_samples) < 10:
                error_samples.append(f"{url}: network_error={exc}")
            continue

        if response.status_code in {401, 403}:
            message = response.text.strip() or "Search Console API rejected credentials/property access."
            return [], {
                "property_uri": property_uri,
                "status": "failed_auth",
                "urls_requested": len(normalized_urls),
                "rows_returned": 0,
                "message": f"Search Console auth failed ({response.status_code}): {message[:400]}",
            }

        if response.status_code >= 400:
            if len(error_samples) < 10:
                error_samples.append(
                    f"{url}: http_{response.status_code}={response.text.strip()[:240]}"
                )
            continue

        try:
            payload = response.json()
        except ValueError:
            if len(error_samples) < 10:
                error_samples.append(f"{url}: invalid_json_response")
            continue

        rows.append(_inspection_row(url, payload))

    if rows and error_samples:
        status = "success_partial"
        message = "Search Console inspection completed with partial URL-level failures."
    elif rows:
        status = "success"
        message = "Search Console inspection completed."
    elif error_samples:
        status = "failed_api"
        message = "Search Console inspection returned API/network failures for all URLs."
    else:
        status = "success_empty"
        message = "Search Console inspection returned no rows."

    return rows, {
        "property_uri": property_uri,
        "status": status,
        "urls_requested": len(normalized_urls),
        "rows_returned": len(rows),
        "error_count": len(error_samples),
        "error_samples": error_samples,
        "message": message,
    }


def _inspection_row(url: str, payload: dict) -> dict[str, object]:
    inspection_result = payload.get("inspectionResult") if isinstance(payload, dict) else {}
    if not isinstance(inspection_result, dict):
        inspection_result = {}

    index_status = inspection_result.get("indexStatusResult")
    if not isinstance(index_status, dict):
        index_status = {}

    coverage_state = str(index_status.get("coverageState") or "").strip()
    indexing_state = str(index_status.get("indexingState") or "").strip()
    verdict = str(index_status.get("verdict") or "").strip()
    robots_txt_state = str(index_status.get("robotsTxtState") or "").strip()
    page_fetch_state = str(index_status.get("pageFetchState") or "").strip()
    last_crawl_time = str(index_status.get("lastCrawlTime") or "").strip()
    referring_urls = index_status.get("referringUrls")
    if not isinstance(referring_urls, list):
        referring_urls = []

    return {
        "url": normalize_url(url),
        "status": _index_state_status(
            coverage_state=coverage_state,
            indexing_state=indexing_state,
            verdict=verdict,
            page_fetch_state=page_fetch_state,
            robots_txt_state=robots_txt_state,
        ),
        "coverage_state": coverage_state,
        "indexing_state": indexing_state,
        "verdict": verdict,
        "robots_txt_state": robots_txt_state,
        "page_fetch_state": page_fetch_state,
        "last_crawl_time": last_crawl_time,
        "referring_urls": [str(item).strip() for item in referring_urls if str(item).strip()][:10],
    }


def _index_state_status(
    *,
    coverage_state: str,
    indexing_state: str,
    verdict: str,
    page_fetch_state: str,
    robots_txt_state: str,
) -> str:
    coverage = coverage_state.lower()
    if coverage:
        if "not indexed" in coverage or "excluded" in coverage or "blocked" in coverage:
            return "not_indexed"
        if "indexed" in coverage:
            return "indexed"

    indexing = indexing_state.lower()
    verdict_l = verdict.lower()
    fetch_state = page_fetch_state.lower()
    robots_state = robots_txt_state.lower()

    if indexing in {"indexing_not_allowed", "blocked_by_robots_txt"}:
        return "not_indexed"
    if "blocked" in fetch_state or "blocked" in robots_state:
        return "not_indexed"
    if verdict_l == "pass" and indexing == "indexing_allowed":
        return "indexed"
    return "unknown"


def reconcile_index_states(crawled_urls: list[str], index_states: list[dict]) -> dict[str, object]:
    normalized_crawled = [normalize_url(url) for url in crawled_urls]
    state_by_url: dict[str, str] = {}
    for row in index_states:
        if not isinstance(row, dict):
            continue
        url = str(row.get("url") or "").strip()
        if not url:
            continue
        normalized = normalize_url(url)
        status = str(row.get("status") or "unknown").strip().lower()
        if status not in {"indexed", "not_indexed", "unknown"}:
            status = "unknown"
        state_by_url[normalized] = status

    indexed = 0
    not_indexed = 0
    unknown = 0
    unknown_samples: list[str] = []
    for url in normalized_crawled:
        status = state_by_url.get(url, "unknown")
        if status == "indexed":
            indexed += 1
        elif status == "not_indexed":
            not_indexed += 1
        else:
            unknown += 1
            if len(unknown_samples) < 10:
                unknown_samples.append(url)

    return {
        "crawled_total": len(normalized_crawled),
        "gsc_rows": len(state_by_url),
        "indexed": indexed,
        "not_indexed": not_indexed,
        "unknown": unknown,
        "unknown_sample_urls": unknown_samples,
    }


def format_reconciliation_evidence(summary: dict[str, object]) -> str:
    return json.dumps(summary, sort_keys=True)
