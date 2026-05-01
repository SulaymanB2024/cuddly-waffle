from pathlib import Path


from seo_audit.config import AuditConfig
from seo_audit.crawler import crawl_site
from seo_audit.extract import extract_page_data
from seo_audit.http_utils import HTTPResponse
from seo_audit.url_utils import internal_hosts_for_site, is_internal_url


HTML = b"""
<html><head><title>Home</title></head><body>
<a href='/relative'>Relative</a>
<a href='https://example.com/apex'>Apex</a>
<a href='https://www.example.com/www'>WWW</a>
<a href='https://blog.example.com/sub'>Subdomain</a>
<a href='https://external.org/x'>External</a>
</body></html>
"""



def test_internal_link_policy_host_variants() -> None:
    allowed = internal_hosts_for_site("https://www.example.com")
    assert allowed == {"example.com", "www.example.com"}
    assert is_internal_url("/a", "https://www.example.com", base_url="https://www.example.com/")
    assert is_internal_url("https://example.com/a", "https://www.example.com")
    assert is_internal_url("https://www.example.com/a", "https://example.com")
    assert not is_internal_url("https://blog.example.com/a", "https://example.com")


def test_extract_uses_shared_internal_link_policy() -> None:
    data = extract_page_data(
        HTML.decode(),
        "https://www.example.com/",
        200,
        "text/html",
        {},
        site_root_url="https://www.example.com",
    )
    assert data["internal_links_out"] == 3
    assert data["external_links_out"] == 2


def test_crawler_uses_shared_internal_link_policy(monkeypatch) -> None:
    def fake_get(url: str, timeout: float, headers: dict[str, str], **kwargs) -> HTTPResponse:
        del kwargs
        return HTTPResponse(
            url="https://www.example.com/",
            status_code=200,
            headers={"content-type": "text/html"},
            content=HTML,
            redirect_chain=["https://www.example.com/"],
        )

    monkeypatch.setattr("seo_audit.crawler.http_get", fake_get)

    config = AuditConfig(domain="https://www.example.com", output_dir=Path("./out"), max_pages=1, respect_robots=False)
    result = crawl_site(config, "run1", robots_data=None, start_urls=["https://www.example.com/"])

    classified = {link.normalized_target_url: link.is_internal for link in result.links}
    assert classified["https://www.example.com/relative"] == 1
    assert classified["https://example.com/apex"] == 1
    assert classified["https://www.example.com/www"] == 1
    assert classified["https://blog.example.com/sub"] == 0
    assert classified["https://external.org/x"] == 0


def test_crawler_scope_all_subdomains_marks_subdomain_internal(monkeypatch) -> None:
    def fake_get(url: str, timeout: float, headers: dict[str, str], **kwargs) -> HTTPResponse:
        del kwargs
        return HTTPResponse(
            url="https://www.example.com/",
            status_code=200,
            headers={"content-type": "text/html"},
            content=HTML,
            redirect_chain=["https://www.example.com/"],
        )

    monkeypatch.setattr("seo_audit.crawler.http_get", fake_get)

    config = AuditConfig(
        domain="https://www.example.com",
        output_dir=Path("./out"),
        max_pages=1,
        respect_robots=False,
        scope_mode="all_subdomains",
    )
    result = crawl_site(config, "run1", robots_data=None, start_urls=["https://www.example.com/"])

    classified = {link.normalized_target_url: link.is_internal for link in result.links}
    assert classified["https://blog.example.com/sub"] == 1
