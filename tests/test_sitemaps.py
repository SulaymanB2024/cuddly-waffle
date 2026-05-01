import gzip

from seo_audit.http_utils import HTTPResponse
from seo_audit.sitemaps import default_sitemap_candidates, fetch_and_parse_sitemaps, parse_sitemap_xml


def test_parse_sitemap_urlset_namespaced() -> None:
    xml = """<urlset xmlns='http://www.sitemaps.org/schemas/sitemap/0.9'>
    <url><loc>https://example.com/</loc><lastmod>2025-01-01</lastmod><changefreq>weekly</changefreq><priority>0.8</priority></url>
    </urlset>"""
    entries, nested = parse_sitemap_xml("https://example.com/sitemap.xml", xml)
    assert len(entries) == 1
    assert entries[0]["url"] == "https://example.com/"
    assert entries[0]["lastmod"] == "2025-01-01"
    assert entries[0]["changefreq"] == "weekly"
    assert entries[0]["priority"] == "0.8"
    assert nested == []


def test_parse_sitemap_urlset_non_namespaced() -> None:
    xml = """<urlset>
    <url><loc>/about</loc></url>
    </urlset>"""
    entries, nested = parse_sitemap_xml("https://example.com/sitemap.xml", xml)
    assert [entry["url"] for entry in entries] == ["https://example.com/about"]
    assert nested == []


def test_parse_sitemap_index_with_nested_urls() -> None:
    xml = """<sitemapindex>
    <sitemap><loc>/sitemap-pages.xml</loc></sitemap>
    <sitemap><loc>https://example.com/sitemap-blog.xml</loc></sitemap>
    </sitemapindex>"""
    entries, nested = parse_sitemap_xml("https://example.com/sitemap.xml", xml)
    assert len(entries) == 2
    assert all(entry["entry_kind"] == "sitemap_index" for entry in entries)
    assert nested == [
        "https://example.com/sitemap-pages.xml",
        "https://example.com/sitemap-blog.xml",
    ]


def test_parse_sitemap_malformed_returns_empty() -> None:
    entries, nested = parse_sitemap_xml("https://example.com/sitemap.xml", "<urlset><url><loc>oops")
    assert entries == []
    assert nested == []


def test_parse_sitemap_partial_entries_skip_missing_loc() -> None:
    xml = """<urlset>
    <url><lastmod>2025-01-01</lastmod></url>
    <url><loc>/ok</loc></url>
    </urlset>"""
    entries, _ = parse_sitemap_xml("https://example.com/sitemap.xml", xml)
    assert len(entries) == 1
    assert entries[0]["url"] == "https://example.com/ok"


def test_parse_sitemap_extensions_and_hreflang() -> None:
        xml = """
        <urlset
            xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
            xmlns:image="http://www.google.com/schemas/sitemap-image/1.1"
            xmlns:video="http://www.google.com/schemas/sitemap-video/1.1"
            xmlns:news="http://www.google.com/schemas/sitemap-news/0.9"
            xmlns:xhtml="http://www.w3.org/1999/xhtml"
        >
            <url>
                <loc>https://example.com/article</loc>
                <image:image>
                    <image:loc>https://example.com/image.jpg</image:loc>
                </image:image>
                <video:video>
                    <video:title>Video Title</video:title>
                    <video:thumbnail_loc>https://example.com/thumb.jpg</video:thumbnail_loc>
                </video:video>
                <news:news>
                    <news:title>News Title</news:title>
                </news:news>
                <xhtml:link rel="alternate" hreflang="fr-fr" href="https://example.com/fr/article" />
            </url>
        </urlset>
        """

        entries, nested = parse_sitemap_xml("https://example.com/sitemap.xml", xml)
        assert nested == []
        assert len(entries) == 1
        entry = entries[0]
        assert entry["entry_kind"] == "url"

        import json

        extensions = json.loads(entry["extensions_json"])
        assert len(extensions["image"]) == 1
        assert len(extensions["video"]) == 1
        assert extensions["news"]["title"] == "News Title"

        hreflang = json.loads(entry["hreflang_links_json"])
        assert hreflang == [{"lang": "fr-fr", "href": "https://example.com/fr/article"}]


def test_default_sitemap_candidates_include_gzip() -> None:
    candidates = default_sitemap_candidates("https://example.com")
    assert "https://example.com/sitemap.xml" in candidates
    assert "https://example.com/sitemap.xml.gz" in candidates


def test_fetch_and_parse_supports_gzipped_urlset(monkeypatch) -> None:
    xml = """<urlset><url><loc>https://example.com/gz</loc></url></urlset>"""
    body = gzip.compress(xml.encode("utf-8"))

    def fake_get(url: str, timeout: float, headers: dict[str, str]) -> HTTPResponse:
        del timeout, headers
        if url != "https://example.com/sitemap.xml.gz":
            raise AssertionError(f"unexpected sitemap request: {url}")
        return HTTPResponse(
            url=url,
            status_code=200,
            headers={"content-type": "application/gzip"},
            content=body,
            redirect_chain=[url],
        )

    monkeypatch.setattr("seo_audit.sitemaps.http_get", fake_get)

    entries = fetch_and_parse_sitemaps(["https://example.com/sitemap.xml.gz"], timeout=2.0, user_agent="Bot")
    assert len(entries) == 1
    assert entries[0]["url"] == "https://example.com/gz"


def test_fetch_and_parse_supports_gzipped_index_and_child(monkeypatch) -> None:
    index_xml = """
    <sitemapindex>
      <sitemap><loc>https://example.com/child.xml.gz</loc></sitemap>
    </sitemapindex>
    """
    child_xml = """
    <urlset>
      <url><loc>https://example.com/a</loc></url>
    </urlset>
    """
    index_body = gzip.compress(index_xml.encode("utf-8"))
    child_body = gzip.compress(child_xml.encode("utf-8"))

    def fake_get(url: str, timeout: float, headers: dict[str, str]) -> HTTPResponse:
        del timeout, headers
        payloads = {
            "https://example.com/index.xml.gz": index_body,
            "https://example.com/child.xml.gz": child_body,
        }
        if url not in payloads:
            raise AssertionError(f"unexpected sitemap request: {url}")
        return HTTPResponse(
            url=url,
            status_code=200,
            headers={"content-type": "application/gzip"},
            content=payloads[url],
            redirect_chain=[url],
        )

    monkeypatch.setattr("seo_audit.sitemaps.http_get", fake_get)

    entries = fetch_and_parse_sitemaps(["https://example.com/index.xml.gz"], timeout=2.0, user_agent="Bot")
    assert len(entries) == 2
    urls = {entry["url"] for entry in entries}
    assert "https://example.com/a" in urls
    assert "https://example.com/child.xml.gz" in urls


def test_fetch_and_parse_malformed_gzip_records_error(monkeypatch) -> None:
    def fake_get(url: str, timeout: float, headers: dict[str, str]) -> HTTPResponse:
        del timeout, headers
        return HTTPResponse(
            url=url,
            status_code=200,
            headers={"content-type": "application/gzip"},
            content=b"not-a-gzip-stream",
            redirect_chain=[url],
        )

    monkeypatch.setattr("seo_audit.sitemaps.http_get", fake_get)

    errors: list[str] = []
    entries = fetch_and_parse_sitemaps(
        ["https://example.com/sitemap.xml.gz"],
        timeout=2.0,
        user_agent="Bot",
        errors=errors,
    )
    assert entries == []
    assert errors
