from __future__ import annotations

import re
from urllib.parse import parse_qsl, quote, unquote, urlencode, urljoin, urlsplit, urlunsplit

TRACKING_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "gclid",
    "fbclid",
    "msclkid",
}

BINARY_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".svg", ".pdf", ".zip", ".ico", ".webp"}


def _apex_host(host: str) -> str:
    lowered = host.strip().lower()
    if lowered.startswith("www."):
        return lowered[4:]
    return lowered


def _normalize_allowlist_host(token: str) -> str:
    stripped = token.strip()
    if not stripped:
        return ""
    parsed = urlsplit(stripped if "://" in stripped else f"https://{stripped}")
    host = (parsed.hostname or "").strip().lower()
    return host


def normalize_url(url: str, base_url: str | None = None, prefer_https: bool = True) -> str:
    candidate = urljoin(base_url, url) if base_url else url
    split = urlsplit(candidate)
    if split.scheme:
        scheme = split.scheme.lower()
        if prefer_https and scheme == "http" and base_url and urlsplit(base_url).scheme.lower() == "https":
            scheme = "https"
    elif base_url:
        scheme = urlsplit(base_url).scheme.lower() or ("https" if prefer_https else "http")
    else:
        scheme = "https" if prefer_https else "http"

    host = split.hostname.lower() if split.hostname else ""
    port = split.port
    if (scheme == "https" and port == 443) or (scheme == "http" and port == 80):
        port = None
    netloc = f"{host}:{port}" if port else host

    path = re.sub(r"/{2,}", "/", split.path or "/")
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    path = quote(unquote(path), safe="/:@!$&'()*+,;=-._~")

    q = [(k, v) for k, v in parse_qsl(split.query, keep_blank_values=True) if k.lower() not in TRACKING_PARAMS]
    query = urlencode(sorted(q))
    return urlunsplit((scheme, netloc, path, query, ""))


def internal_hosts_for_site(
    root_url: str,
    homepage_redirect_host: str | None = None,
    *,
    scope_mode: str = "apex_www",
    custom_allowlist: tuple[str, ...] | list[str] | set[str] | None = None,
) -> set[str]:
    """Return hosts considered internal for the crawl.

    Policy (intentionally strict and shared across modules):
    - Internal hosts include only the root host, its apex variant, and its www variant.
    - Relative URLs are internal because they resolve against the current page base URL.
    - Unrelated subdomains (e.g., blog.example.com) are external by default.
    - Optional homepage redirect host can be whitelisted when discovery observes it.
    """

    root = normalize_url(root_url)
    host = (urlsplit(root).hostname or "").lower()
    apex = _apex_host(host)
    mode = scope_mode.strip().lower()

    allowed: set[str]
    if mode == "host_only":
        allowed = {host}
    elif mode == "custom_allowlist":
        allowed = {host, apex, f"www.{apex}"}
        for token in custom_allowlist or ():
            normalized_host = _normalize_allowlist_host(str(token))
            if normalized_host:
                allowed.add(normalized_host)
    else:
        # apex_www and all_subdomains share this explicit baseline host list.
        allowed = {host, apex, f"www.{apex}"}

    if homepage_redirect_host:
        allowed.add(homepage_redirect_host.lower())
    return {h for h in allowed if h}


def _all_subdomains_match(host: str, root_url: str) -> bool:
    root_host = (urlsplit(normalize_url(root_url)).hostname or "").lower()
    apex = _apex_host(root_host)
    if not host or not apex:
        return False
    return host == apex or host.endswith(f".{apex}")


def is_internal_url(
    url: str,
    root_url: str,
    *,
    base_url: str | None = None,
    homepage_redirect_host: str | None = None,
    scope_mode: str = "apex_www",
    custom_allowlist: tuple[str, ...] | list[str] | set[str] | None = None,
) -> bool:
    normalized = normalize_url(url, base_url=base_url)
    host = (urlsplit(normalized).hostname or "").lower()
    if not host:
        return False
    mode = scope_mode.strip().lower()
    if mode == "all_subdomains":
        return _all_subdomains_match(host, root_url)
    allowed = internal_hosts_for_site(
        root_url,
        homepage_redirect_host=homepage_redirect_host,
        scope_mode=scope_mode,
        custom_allowlist=custom_allowlist,
    )
    return host in allowed


def same_registrable_domain(url: str, root_host: str) -> bool:
    host = (urlsplit(url).hostname or "").lower()
    base = root_host.lower()
    if not host or not base:
        return False
    apex = base[4:] if base.startswith("www.") else base
    return host in {base, apex, f"www.{apex}"}


def should_skip_asset(url: str) -> bool:
    path = urlsplit(url).path.lower()
    return any(path.endswith(ext) for ext in BINARY_EXTENSIONS)
