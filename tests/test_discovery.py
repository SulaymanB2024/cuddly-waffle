from seo_audit.discovery import seed_urls
from seo_audit.robots import parse_robots_text


def test_www_seed_accepts_apex_urls() -> None:
    robots = parse_robots_text("https://www.example.com", "Sitemap: https://example.com/sitemap.xml")
    entries = [{"url": "https://example.com/page", "sitemap_url": "https://example.com/sitemap.xml"}]
    seeds = seed_urls("https://www.example.com", robots, entries)
    assert "https://example.com/page" in seeds


def test_apex_seed_accepts_www_urls() -> None:
    robots = parse_robots_text("https://example.com", "Sitemap: https://www.example.com/sitemap.xml")
    entries = [{"url": "https://www.example.com/page", "sitemap_url": "https://www.example.com/sitemap.xml"}]
    seeds = seed_urls("https://example.com", robots, entries)
    assert "https://www.example.com/page" in seeds


def test_unrelated_subdomain_excluded() -> None:
    robots = parse_robots_text("https://www.example.com", "Sitemap: https://blog.example.com/sitemap.xml")
    entries = [{"url": "https://blog.example.com/page", "sitemap_url": "https://blog.example.com/sitemap.xml"}]
    seeds = seed_urls("https://www.example.com", robots, entries)
    assert all("blog.example.com" not in u for u in seeds)


def test_scope_all_subdomains_includes_subdomain_seeds() -> None:
    robots = parse_robots_text("https://www.example.com", "Sitemap: https://blog.example.com/sitemap.xml")
    entries = [{"url": "https://blog.example.com/page", "sitemap_url": "https://blog.example.com/sitemap.xml"}]
    seeds = seed_urls("https://www.example.com", robots, entries, scope_mode="all_subdomains")
    assert "https://blog.example.com/page" in seeds


def test_scope_custom_allowlist_includes_explicit_external_host() -> None:
    robots = parse_robots_text("https://www.example.com", "Sitemap: https://shop.example.net/sitemap.xml")
    entries = [{"url": "https://shop.example.net/page", "sitemap_url": "https://shop.example.net/sitemap.xml"}]
    seeds = seed_urls(
        "https://www.example.com",
        robots,
        entries,
        scope_mode="custom_allowlist",
        custom_allowlist=("shop.example.net",),
    )
    assert "https://shop.example.net/page" in seeds
