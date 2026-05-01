from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from requests import Session
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException


@dataclass(slots=True)
class HTTPResponse:
    url: str
    status_code: int
    headers: dict[str, str]
    content: bytes
    not_modified: bool = False
    header_lists: dict[str, list[str]] = field(default_factory=dict)
    redirect_chain: list[str] = field(default_factory=list)

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")


DEFAULT_POOL_CONNECTIONS = 20
DEFAULT_POOL_MAXSIZE = 20
DEFAULT_MAX_BYTES = 2_000_000
DEFAULT_MAX_NON_HTML_BYTES = 262_144

_SESSION_LOCAL = threading.local()


def _create_session(
    *,
    pool_connections: int = DEFAULT_POOL_CONNECTIONS,
    pool_maxsize: int = DEFAULT_POOL_MAXSIZE,
) -> Session:
    session = Session()
    adapter = HTTPAdapter(pool_connections=pool_connections, pool_maxsize=pool_maxsize)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _get_session(
    *,
    pool_connections: int = DEFAULT_POOL_CONNECTIONS,
    pool_maxsize: int = DEFAULT_POOL_MAXSIZE,
) -> Session:
    cached: Session | None = getattr(_SESSION_LOCAL, "session", None)
    if cached is not None:
        return cached
    created = _create_session(pool_connections=pool_connections, pool_maxsize=pool_maxsize)
    _SESSION_LOCAL.session = created
    return created


def reset_http_session() -> None:
    cached: Session | None = getattr(_SESSION_LOCAL, "session", None)
    if cached is None:
        return
    try:
        cached.close()
    finally:
        _SESSION_LOCAL.session = None


def _response_chain(requested_url: str, response) -> list[str]:
    chain: list[str] = []
    for url in [requested_url, *[r.url for r in response.history], response.url]:
        normalized = str(url)
        if not normalized:
            continue
        if not chain or chain[-1] != normalized:
            chain.append(normalized)
    return chain


def build_conditional_headers(
    headers: dict[str, str] | None = None,
    *,
    etag: str = "",
    last_modified: str = "",
) -> dict[str, str]:
    merged = dict(headers or {})
    lowered = {str(name).strip().lower() for name in merged.keys()}
    cleaned_etag = str(etag or "").strip()
    cleaned_last_modified = str(last_modified or "").strip()
    if cleaned_etag and "if-none-match" not in lowered:
        merged["If-None-Match"] = cleaned_etag
    if cleaned_last_modified and "if-modified-since" not in lowered:
        merged["If-Modified-Since"] = cleaned_last_modified
    return merged


def _response_header_lists(response, extra_headers: dict[str, str]) -> dict[str, list[str]]:
    header_lists: dict[str, list[str]] = {}

    raw_headers = getattr(getattr(response, "raw", None), "headers", None)
    if raw_headers is not None and hasattr(raw_headers, "keys"):
        for raw_name in raw_headers.keys():
            name = str(raw_name).strip().lower()
            if not name or name in header_lists:
                continue

            values: list[str] = []
            if hasattr(raw_headers, "getlist"):
                values = [str(v) for v in raw_headers.getlist(raw_name) if str(v).strip()]
            else:
                raw_value = raw_headers.get(raw_name)
                if raw_value is not None and str(raw_value).strip():
                    values = [str(raw_value)]

            if values:
                header_lists[name] = values

    if not header_lists:
        for name, value in response.headers.items():
            normalized_name = str(name).strip().lower()
            if not normalized_name:
                continue
            cleaned = str(value)
            if cleaned.strip():
                header_lists[normalized_name] = [cleaned]

    for name, value in extra_headers.items():
        normalized_name = str(name).strip().lower()
        if not normalized_name:
            continue
        cleaned = str(value)
        if not cleaned.strip():
            continue
        header_lists.setdefault(normalized_name, []).append(cleaned)

    return header_lists


def _read_bounded_body(
    response,
    *,
    max_bytes: int | None,
    max_non_html_bytes: int | None,
) -> tuple[bytes, dict[str, str]]:
    meta_headers: dict[str, str] = {}
    content_type = str(response.headers.get("content-type", ""))
    lower_content_type = content_type.lower()
    is_html_like = "html" in lower_content_type

    content_length_raw = response.headers.get("content-length")
    try:
        content_length = int(content_length_raw) if content_length_raw is not None else None
    except ValueError:
        content_length = None

    limit = max_bytes if max_bytes and max_bytes > 0 else None
    if max_non_html_bytes and max_non_html_bytes > 0 and not is_html_like:
        if limit is None:
            limit = max_non_html_bytes
        else:
            limit = min(limit, max_non_html_bytes)

        if content_length is not None and content_length > max_non_html_bytes:
            response.close()
            meta_headers["x-seo-audit-body-skipped"] = "1"
            meta_headers["x-seo-audit-body-skip-reason"] = "non_html_content_length_limit"
            meta_headers["x-seo-audit-body-bytes"] = "0"
            return b"", meta_headers

    chunks: list[bytes] = []
    total = 0
    truncated = False
    try:
        for chunk in response.iter_content(chunk_size=32 * 1024):
            if not chunk:
                continue
            if limit is not None and total + len(chunk) > limit:
                remaining = limit - total
                if remaining > 0:
                    chunks.append(chunk[:remaining])
                    total += remaining
                truncated = True
                break
            chunks.append(chunk)
            total += len(chunk)
    finally:
        response.close()

    if truncated:
        meta_headers["x-seo-audit-body-truncated"] = "1"
    meta_headers["x-seo-audit-body-bytes"] = str(total)
    return b"".join(chunks), meta_headers


def _http_request(
    method: str,
    url: str,
    timeout: float = 10.0,
    headers: dict[str, str] | None = None,
    params: dict | None = None,
    *,
    max_bytes: int | None = DEFAULT_MAX_BYTES,
    max_non_html_bytes: int | None = DEFAULT_MAX_NON_HTML_BYTES,
) -> HTTPResponse:
    session = _get_session()
    try:
        response = session.request(
            method=method,
            url=url,
            params=params,
            headers=headers or {},
            timeout=timeout,
            allow_redirects=True,
            stream=(method != "HEAD"),
        )
    except RequestException as exc:
        raise RuntimeError(str(exc)) from exc

    body = b""
    extra_headers: dict[str, str] = {}
    not_modified = int(response.status_code) == 304
    if method != "HEAD" and not not_modified:
        body, extra_headers = _read_bounded_body(response, max_bytes=max_bytes, max_non_html_bytes=max_non_html_bytes)
    else:
        response.close()
        extra_headers["x-seo-audit-body-bytes"] = "0"

    normalized_headers = {k.lower(): v for k, v in response.headers.items()}
    normalized_headers.update(extra_headers)
    header_lists = _response_header_lists(response, extra_headers)
    return HTTPResponse(
        response.url,
        int(response.status_code),
        normalized_headers,
        body,
        not_modified=not_modified,
        header_lists=header_lists,
        redirect_chain=_response_chain(url, response),
    )


def http_get(
    url: str,
    timeout: float = 10.0,
    headers: dict[str, str] | None = None,
    params: dict | None = None,
    *,
    max_bytes: int | None = DEFAULT_MAX_BYTES,
    max_non_html_bytes: int | None = DEFAULT_MAX_NON_HTML_BYTES,
) -> HTTPResponse:
    return _http_request(
        "GET",
        url,
        timeout=timeout,
        headers=headers,
        params=params,
        max_bytes=max_bytes,
        max_non_html_bytes=max_non_html_bytes,
    )


def http_head(url: str, timeout: float = 10.0, headers: dict[str, str] | None = None, params: dict | None = None) -> HTTPResponse:
    return _http_request("HEAD", url, timeout=timeout, headers=headers, params=params, max_bytes=None, max_non_html_bytes=None)


def http_get_json(
    url: str,
    timeout: float = 10.0,
    headers: dict[str, str] | None = None,
    params: dict | None = None,
) -> dict:
    resp = http_get(url, timeout=timeout, headers=headers, params=params, max_bytes=None, max_non_html_bytes=None)
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}")
    return json.loads(resp.text)
