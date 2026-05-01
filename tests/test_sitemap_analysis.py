import json

from seo_audit.sitemap_analysis import analyze_sitemap_intelligence


def test_sitemap_analysis_computes_core_deltas() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/",
            "status_code": 200,
            "image_count": 1,
            "video_details_json": "[]",
            "hreflang_links_json": json.dumps([{"lang": "fr-fr", "href": "https://example.com/fr"}], sort_keys=True),
        },
        {
            "normalized_url": "https://example.com/service",
            "status_code": 200,
            "image_count": 2,
            "video_details_json": json.dumps([{"src": "https://cdn.example.com/v.mp4"}], sort_keys=True),
            "hreflang_links_json": "[]",
        },
    ]

    sitemap_entries = [
        {
            "entry_kind": "url",
            "url": "https://example.com/",
            "lastmod": "2025-01-01",
            "extensions_json": json.dumps({"image": [{"loc": "https://example.com/hero.jpg"}], "video": [], "news": {}}, sort_keys=True),
            "hreflang_links_json": json.dumps([{"lang": "fr-fr", "href": "https://example.com/fr"}], sort_keys=True),
        },
        {
            "entry_kind": "url",
            "url": "https://example.com/blog/post",
            "lastmod": "",
            "extensions_json": json.dumps({"image": [], "video": [{"title": "v"}], "news": {}}, sort_keys=True),
            "hreflang_links_json": "[]",
        },
        {
            "entry_kind": "sitemap_index",
            "url": "https://example.com/sitemap-blog.xml",
            "lastmod": "",
            "extensions_json": "{}",
            "hreflang_links_json": "[]",
        },
    ]

    summary = analyze_sitemap_intelligence("https://example.com", pages, sitemap_entries)
    assert summary["sitemap_url_count"] == 2
    assert summary["discovered_page_count"] == 2
    assert summary["urls_in_sitemap_not_crawled"] == 1
    assert summary["crawled_urls_not_in_sitemap"] == 1
    assert summary["sitemap_assets"]["images"] == 1
    assert summary["sitemap_assets"]["videos"] == 1
    assert summary["on_page_assets"]["images"] == 3
    assert summary["on_page_assets"]["videos"] == 1
    assert summary["missing_lastmod_urls"] >= 1
