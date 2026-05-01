from seo_audit.http_utils import HTTPResponse
from pathlib import Path

from seo_audit.config import AuditConfig
from seo_audit.crawler import _effective_request_delay
from seo_audit.robots import _strip_inline_comment, fetch_robots, is_allowed, parse_robots_text, resolve_crawl_delay


def test_parse_robots() -> None:
    text = """User-agent: *\nDisallow: /private\nSitemap: https://example.com/sitemap.xml\n"""
    data = parse_robots_text("https://example.com", text)
    assert data.sitemaps == ["https://example.com/sitemap.xml"]
    assert any(r["directive"] == "disallow" for r in data.rules)
    assert not is_allowed(data, "Bot", "https://example.com/private")


def test_fetch_robots_failure_does_not_block(monkeypatch) -> None:
    monkeypatch.setattr(
        "seo_audit.robots.http_get",
        lambda *args, **kwargs: HTTPResponse(
            url="https://example.com/robots.txt",
            status_code=404,
            headers={},
            content=b"",
            redirect_chain=["https://example.com/robots.txt"],
        ),
    )
    robots = fetch_robots("https://example.com", timeout=2.0, user_agent="Bot")
    assert is_allowed(robots, "Bot", "https://example.com/")


def test_resolve_crawl_delay_prefers_specific_user_agent() -> None:
    text = """
User-agent: *
Crawl-delay: 4

User-agent: SEOAuditBot
Crawl-delay: 1.5
"""
    robots = parse_robots_text("https://example.com", text)
    delay = resolve_crawl_delay(robots, "SEOAuditBot/0.1 (+internal)")
    assert delay == 1.5


def test_effective_request_delay_uses_crawl_delay_when_respecting_robots() -> None:
    text = """
User-agent: *
Crawl-delay: 2
"""
    robots = parse_robots_text("https://example.com", text)
    config = AuditConfig(domain="https://example.com", output_dir=Path("."), request_delay=0.25, respect_robots=True)
    assert _effective_request_delay(config, robots, config.robots_user_agent_token) == 2.0


def test_effective_request_delay_ignores_crawl_delay_when_robots_not_respected() -> None:
    text = """
User-agent: *
Crawl-delay: 3
"""
    robots = parse_robots_text("https://example.com", text)
    config = AuditConfig(domain="https://example.com", output_dir=Path("."), request_delay=0.25, respect_robots=False)
    assert _effective_request_delay(config, robots, config.robots_user_agent_token) == 0.25


def test_strip_inline_comment_trims_suffix() -> None:
    assert _strip_inline_comment("Crawl-delay: 10 # note") == "Crawl-delay: 10"


def test_parse_robots_inline_comments_for_crawl_delay_and_disallow() -> None:
    text = """
# header comment
User-agent: *
Crawl-delay: 10 # keep polite
Disallow: /private # hidden
"""
    robots = parse_robots_text("https://example.com", text)

    assert resolve_crawl_delay(robots, "AnyBot/1.0") == 10.0
    assert not is_allowed(robots, "AnyBot", "https://example.com/private")
    assert any(rule["directive"] == "crawl-delay" and rule["value"] == "10" for rule in robots.rules)


def test_parse_robots_comment_only_lines_are_ignored() -> None:
    text = """
# one
# two
User-agent: *
Allow: /
"""
    robots = parse_robots_text("https://example.com", text)
    assert is_allowed(robots, "Bot", "https://example.com/")
    assert all(not str(rule.get("value", "")).startswith("#") for rule in robots.rules)
