from pathlib import Path

from seo_audit.config import AuditConfig
from seo_audit.crawler import crawl_site
from seo_audit.http_utils import HTTPResponse
from seo_audit.robots import parse_robots_text
from seo_audit.url_policy import (
    CANONICAL_CANDIDATE_DUPLICATE,
    CRAWL_ONCE_DIAGNOSTIC,
    FETCH_HEADERS_ONLY,
)


ROOT_HTML = b"""
<html><head><title>Root</title></head><body>
<a href='/product?add-to-cart=123'>Action</a>
<a href='/catalog?color=red&size=l'>Facet</a>
<a href='/diag?preview=1'>Diagnostic</a>
<a href='/export?download=csv'>Headers</a>
<a href='/dup?sort=price'>Canonical Candidate</a>
</body></html>
"""

DIAG_HTML = b"""
<html><head><title>Diag</title></head><body>
<a href='/diag-child'>Child</a>
</body></html>
"""

DUP_HTML = b"""
<html><head><title>Dup</title></head><body>
<a href='/dup-child'>Child</a>
</body></html>
"""


def test_crawler_applies_url_policy_classes(monkeypatch) -> None:
    seen_get: list[str] = []
    seen_head: list[str] = []

    def fake_get(url: str, timeout: float, headers: dict[str, str], **kwargs) -> HTTPResponse:
        del timeout, headers
        del kwargs
        seen_get.append(url)
        payload_by_url = {
            "https://example.com/": ROOT_HTML,
            "https://example.com/diag?preview=1": DIAG_HTML,
            "https://example.com/dup?sort=price": DUP_HTML,
        }
        if url not in payload_by_url:
            raise AssertionError(f"unexpected GET for {url}")
        return HTTPResponse(
            url=url,
            status_code=200,
            headers={"content-type": "text/html"},
            content=payload_by_url[url],
            redirect_chain=[url],
        )

    def fake_head(url: str, timeout: float, headers: dict[str, str], **kwargs) -> HTTPResponse:
        del timeout, headers
        del kwargs
        seen_head.append(url)
        if url != "https://example.com/export?download=csv":
            raise AssertionError(f"unexpected HEAD for {url}")
        return HTTPResponse(
            url=url,
            status_code=200,
            headers={"content-type": "text/csv"},
            content=b"",
            redirect_chain=[url],
        )

    monkeypatch.setattr("seo_audit.crawler.http_get", fake_get)
    monkeypatch.setattr("seo_audit.crawler.http_head", fake_head)

    config = AuditConfig(
        domain="https://example.com",
        output_dir=Path("./out"),
        max_pages=20,
        respect_robots=False,
        faceted_sample_rate=0.0,
    )
    result = crawl_site(config, "run1", robots_data=None, start_urls=["https://example.com/"])
    crawled_urls = {page.normalized_url for page in result.pages}

    assert "https://example.com/product?add-to-cart=123" not in crawled_urls
    assert "https://example.com/catalog?color=red&size=l" not in crawled_urls
    assert "https://example.com/diag-child" not in crawled_urls
    assert "https://example.com/dup-child" not in crawled_urls

    assert "https://example.com/diag?preview=1" in crawled_urls
    assert "https://example.com/dup?sort=price" in crawled_urls
    assert "https://example.com/export?download=csv" in crawled_urls

    pages_by_url = {page.normalized_url: page for page in result.pages}
    assert pages_by_url["https://example.com/diag?preview=1"].crawl_policy_class == CRAWL_ONCE_DIAGNOSTIC
    assert pages_by_url["https://example.com/dup?sort=price"].crawl_policy_class == CANONICAL_CANDIDATE_DUPLICATE

    head_page = pages_by_url["https://example.com/export?download=csv"]
    assert head_page.crawl_policy_class == FETCH_HEADERS_ONLY
    assert head_page.title == ""
    assert head_page.word_count == 0

    assert seen_head == ["https://example.com/export?download=csv"]
    assert "https://example.com/diag-child" not in seen_get
    assert "https://example.com/dup-child" not in seen_get


def test_crawler_persists_discovered_robots_blocked_urls(monkeypatch) -> None:
    html = b"""
    <html><head><title>Home</title></head><body>
    <a href='/private'>Private</a>
    </body></html>
    """

    def fake_get(url: str, timeout: float, headers: dict[str, str], **kwargs) -> HTTPResponse:
        del timeout, headers
        del kwargs
        if url != "https://example.com/":
            raise AssertionError(f"unexpected GET for {url}")
        return HTTPResponse(
            url=url,
            status_code=200,
            headers={"content-type": "text/html"},
            content=html,
            redirect_chain=[url],
        )

    monkeypatch.setattr("seo_audit.crawler.http_get", fake_get)

    robots = parse_robots_text("https://example.com", "User-agent: *\nDisallow: /private\n")
    config = AuditConfig(
        domain="https://example.com",
        output_dir=Path("./out"),
        max_pages=10,
        respect_robots=True,
    )

    result = crawl_site(config, "run1", robots_data=robots, start_urls=["https://example.com/"])
    pages_by_url = {page.normalized_url: page for page in result.pages}

    assert "https://example.com/private" in pages_by_url
    private = pages_by_url["https://example.com/private"]
    assert private.robots_blocked_flag == 1
    assert private.status_code is None
    assert private.fetch_error == ""


def test_crawler_queue_dedupe_telemetry(monkeypatch) -> None:
    home_html = b"""
    <html><head><title>Home</title></head><body>
    <a href='/about'>About one</a>
    <a href='/about'>About two</a>
    </body></html>
    """
    about_html = b"<html><head><title>About</title></head><body><h1>About</h1></body></html>"

    def fake_get(url: str, timeout: float, headers: dict[str, str], **kwargs) -> HTTPResponse:
        del timeout, headers, kwargs
        payload_by_url = {
            "https://example.com/": home_html,
            "https://example.com/about": about_html,
        }
        if url not in payload_by_url:
            raise AssertionError(f"unexpected GET for {url}")
        return HTTPResponse(
            url=url,
            status_code=200,
            headers={"content-type": "text/html"},
            content=payload_by_url[url],
            redirect_chain=[url],
        )

    monkeypatch.setattr("seo_audit.crawler.http_get", fake_get)

    config = AuditConfig(
        domain="https://example.com",
        output_dir=Path("./out"),
        max_pages=3,
        respect_robots=False,
    )
    result = crawl_site(config, "run1", robots_data=None, start_urls=["https://example.com/"])
    assert result.discovery_stats.get("dedupe_skipped", 0) >= 1


def test_crawler_render_frontier_enqueues_rendered_links(monkeypatch) -> None:
    home_html = b"<html><head><title>Home</title></head><body><h1>Home</h1></body></html>"
    rendered_html = b"<html><head><title>Rendered</title></head><body><h1>Rendered</h1></body></html>"

    def fake_get(url: str, timeout: float, headers: dict[str, str], **kwargs) -> HTTPResponse:
        del timeout, headers, kwargs
        payload_by_url = {
            "https://example.com/": home_html,
            "https://example.com/rendered-only": rendered_html,
        }
        if url not in payload_by_url:
            raise AssertionError(f"unexpected GET for {url}")
        return HTTPResponse(
            url=url,
            status_code=200,
            headers={"content-type": "text/html"},
            content=payload_by_url[url],
            redirect_chain=[url],
        )

    class FakeRenderer:
        def __init__(self, *args, **kwargs):
            del args, kwargs

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            return False

        def render(self, url: str):
            del url
            return (
                type("RR", (), {
                    "final_url": "https://example.com/",
                    "links": [{"href": "/rendered-only", "anchor_text": "Rendered"}],
                    "network_request_urls": ["https://example.com/api/nav"],
                    "api_endpoint_urls": ["https://example.com/api/nav"],
                    "wait_profile": "load;networkidle;stability:2",
                    "interaction_count": 0,
                    "action_recipe": "none",
                })(),
                None,
            )

    monkeypatch.setattr("seo_audit.crawler.http_get", fake_get)
    monkeypatch.setattr("seo_audit.crawler.PlaywrightRenderer", FakeRenderer)

    config = AuditConfig(
        domain="https://example.com",
        output_dir=Path("./out"),
        max_pages=3,
        max_render_pages=2,
        crawl_discovery_mode="browser_first",
        render_frontier_enabled=True,
        respect_robots=False,
    )

    result = crawl_site(config, "run1", robots_data=None, start_urls=["https://example.com/"])
    page_urls = {page.normalized_url for page in result.pages}
    assert "https://example.com/rendered-only" in page_urls
    assert result.discovery_stats.get("enqueued_via_render_link", 0) >= 1


def test_crawler_keeps_source_context_and_tracks_dom_region(monkeypatch) -> None:
    html = b"""
    <html><head><title>Home</title></head><body>
      <header><a href='/header-link'>Header link</a></header>
      <main><a href='/main-link'>Main link</a></main>
    </body></html>
    """

    def fake_get(url: str, timeout: float, headers: dict[str, str], **kwargs) -> HTTPResponse:
        del timeout, headers, kwargs
        if url not in {"https://example.com/", "https://example.com/header-link", "https://example.com/main-link"}:
            raise AssertionError(f"unexpected GET for {url}")
        return HTTPResponse(
            url=url,
            status_code=200,
            headers={"content-type": "text/html"},
            content=html,
            redirect_chain=[url],
        )

    monkeypatch.setattr("seo_audit.crawler.http_get", fake_get)

    config = AuditConfig(
        domain="https://example.com",
        output_dir=Path("./out"),
        max_pages=2,
        respect_robots=False,
    )

    result = crawl_site(config, "run1", robots_data=None, start_urls=["https://example.com/"])
    raw_links = [link for link in result.links if link.source_context == "raw_dom"]
    assert raw_links
    assert {link.dom_region for link in raw_links}.issuperset({"header", "main"})
