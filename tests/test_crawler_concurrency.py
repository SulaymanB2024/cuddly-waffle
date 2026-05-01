import threading
import time
from pathlib import Path

from seo_audit.config import AuditConfig
from seo_audit.crawler import crawl_site
from seo_audit.http_utils import HTTPResponse


ROOT_HTML = b"""
<html><head><title>Home</title></head><body>
<a href='/a'>A</a>
<a href='/b'>B</a>
<a href='/c'>C</a>
<a href='/d'>D</a>
</body></html>
"""
CHILD_HTML = b"<html><head><title>Child</title></head><body><h1>Child</h1></body></html>"


def test_crawler_uses_bounded_fetch_workers(monkeypatch) -> None:
    active = 0
    max_active = 0
    lock = threading.Lock()

    def fake_get(url: str, timeout: float, headers: dict[str, str], **kwargs) -> HTTPResponse:
        del timeout, headers, kwargs
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.03)
        with lock:
            active -= 1

        if url == "https://example.com/":
            content = ROOT_HTML
        elif url in {
            "https://example.com/a",
            "https://example.com/b",
            "https://example.com/c",
            "https://example.com/d",
        }:
            content = CHILD_HTML
        else:
            raise AssertionError(f"unexpected url {url}")

        return HTTPResponse(
            url=url,
            status_code=200,
            headers={"content-type": "text/html"},
            content=content,
            redirect_chain=[url],
        )

    monkeypatch.setattr("seo_audit.crawler.http_get", fake_get)

    config = AuditConfig(
        domain="https://example.com",
        output_dir=Path("./out"),
        max_pages=5,
        respect_robots=False,
        crawl_workers=3,
        crawl_frontier_enabled=True,
        crawl_queue_high_weight=3,
        crawl_queue_normal_weight=2,
        request_delay=0.0,
        per_host_rate_limit_rps=100.0,
        per_host_burst_capacity=10,
    )

    result = crawl_site(config, "run-1", robots_data=None, start_urls=["https://example.com/"])

    assert max_active <= 3
    assert max_active >= 2
    assert int(result.discovery_stats.get("crawl_workers_used", 0)) == 3
    assert int(result.discovery_stats.get("enqueued_band_high", 0)) >= 1
