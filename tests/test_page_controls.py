import json

from seo_audit.extract import extract_page_data
from seo_audit.page_controls import resolve_page_controls, summarize_directives


def test_duplicate_meta_robots_tags_aggregate() -> None:
    html = """
    <html>
      <head>
        <meta name='robots' content='index,follow' />
        <meta name='robots' content='noindex' />
      </head>
      <body><h1>x</h1></body>
    </html>
    """
    data = extract_page_data(html, "https://example.com/", 200, "text/html", {}, crawler_token="googlebot")

    assert "index" in data["meta_robots"]
    assert "noindex" in data["meta_robots"]
    assert data["is_noindex"] == 1


def test_googlebot_meta_only_applies_to_google_persona() -> None:
    html = """
    <html>
      <head>
        <meta name='robots' content='index,follow' />
        <meta name='googlebot' content='noindex,nofollow' />
      </head>
      <body><h1>x</h1></body>
    </html>
    """

    google = extract_page_data(html, "https://example.com/", 200, "text/html", {}, crawler_token="googlebot")
    bing = extract_page_data(html, "https://example.com/", 200, "text/html", {}, crawler_token="bingbot")

    assert google["is_noindex"] == 1
    assert google["is_nofollow"] == 1
    assert bing["is_noindex"] == 0
    assert bing["is_nofollow"] == 0


def test_repeated_x_robots_headers_are_all_seen() -> None:
    html = "<html><head><title>x</title></head><body>x</body></html>"
    data = extract_page_data(
        html,
        "https://example.com/",
        200,
        "text/html",
        {},
        header_lists={"x-robots-tag": ["noindex", "max-snippet:50"]},
        crawler_token="googlebot",
    )

    assert data["is_noindex"] == 1
    assert data["max_snippet_directive"] == "50"


def test_scoped_x_robots_header_applies_only_to_matching_persona() -> None:
    html = "<html><head><title>x</title></head><body>x</body></html>"
    header_lists = {"x-robots-tag": ["googlebot: nofollow"]}

    google = extract_page_data(
        html,
        "https://example.com/",
        200,
        "text/html",
        {},
        header_lists=header_lists,
        crawler_token="googlebot",
    )
    bing = extract_page_data(
        html,
        "https://example.com/",
        200,
        "text/html",
        {},
        header_lists=header_lists,
        crawler_token="bingbot",
    )

    assert google["is_nofollow"] == 1
    assert bing["is_nofollow"] == 0

    payload = json.loads(google["effective_robots_json"])
    assert "x_robots_scoped" in payload["scoped_sources"]


def test_more_restrictive_snippet_directive_wins_with_nosnippet() -> None:
    decision = resolve_page_controls(
        meta_map={"robots": ["max-snippet:-1", "max-snippet:50", "nosnippet"]},
        x_robots_values=[],
        crawler_token="googlebot",
    )
    summary = summarize_directives(decision)

    assert summary["has_nosnippet_directive"] is True
    assert summary["max_snippet_directive"] == "50"
