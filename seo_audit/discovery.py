from __future__ import annotations

from urllib.parse import urlsplit

from seo_audit.robots import RobotsData
from seo_audit.sitemaps import default_sitemap_candidates
from seo_audit.url_utils import internal_hosts_for_site, is_internal_url, normalize_url


def site_host_variants(
    domain: str,
    homepage_redirect_host: str | None = None,
    *,
    scope_mode: str = "apex_www",
    custom_allowlist: tuple[str, ...] | list[str] | set[str] | None = None,
) -> set[str]:
    return internal_hosts_for_site(
        domain,
        homepage_redirect_host=homepage_redirect_host,
        scope_mode=scope_mode,
        custom_allowlist=custom_allowlist,
    )


def seed_urls(
    domain: str,
    robots_data: RobotsData | None,
    sitemap_entries: list[dict],
    homepage_redirect_host: str | None = None,
    *,
    scope_mode: str = "apex_www",
    custom_allowlist: tuple[str, ...] | list[str] | set[str] | None = None,
) -> list[str]:
    root = normalize_url(domain)
    allowed_hosts = site_host_variants(
        root,
        homepage_redirect_host=homepage_redirect_host,
        scope_mode=scope_mode,
        custom_allowlist=custom_allowlist,
    )

    seeds = {root}
    for entry in sitemap_entries:
        seeds.add(normalize_url(entry["url"], base_url=root))
    if robots_data:
        for sm in robots_data.sitemaps:
            seeds.add(normalize_url(sm, base_url=root))
    seeds.update(default_sitemap_candidates(root))

    scoped: list[str] = []
    for url in sorted(seeds):
        host = (urlsplit(url).hostname or "").lower()
        if host in allowed_hosts:
            scoped.append(url)
            continue
        if is_internal_url(
            url,
            root,
            scope_mode=scope_mode,
            custom_allowlist=custom_allowlist,
            homepage_redirect_host=homepage_redirect_host,
        ):
            scoped.append(url)
    return scoped
