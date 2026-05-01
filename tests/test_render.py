from seo_audit.cli import _count_internal_rendered_links, select_render_targets
from seo_audit.render import RenderResult, choose_render_sample, compute_render_gap


def test_choose_render_sample_prioritizes_diverse_pages() -> None:
    pages = [
        {"normalized_url": "https://e.com/", "page_type": "homepage", "internal_links_out": 10, "word_count": 300},
        {"normalized_url": "https://e.com/service", "page_type": "service", "internal_links_out": 8, "word_count": 250},
        {"normalized_url": "https://e.com/blog/x", "page_type": "article", "internal_links_out": 2, "word_count": 200},
        {"normalized_url": "https://e.com/thin", "page_type": "other", "internal_links_out": 1, "word_count": 30},
    ]
    sample = choose_render_sample(pages, 3)
    urls = {p["normalized_url"] for p in sample}
    assert "https://e.com/" in urls
    assert len(sample) == 3


def test_compute_render_gap_reasonable() -> None:
    raw = {"word_count": 20, "title": "A", "canonical_url": "https://e.com/a", "h1": "H", "internal_links_out": 1}
    rendered = RenderResult(
        final_url="https://e.com/a",
        title="B",
        canonical="https://e.com/b",
        h1s=["H2"],
        h1_count=1,
        word_count=500,
        links=[{"href": "/a"} for _ in range(30)],
    )
    score, reason = compute_render_gap(raw, rendered)
    assert score >= 80
    assert "raw thin but rendered rich" in reason


def test_select_render_targets_filters_non_actionable_pages() -> None:
    pages = [
        {
            "normalized_url": "https://e.com/",
            "status_code": 200,
            "content_type": "text/html",
            "fetch_error": "",
            "page_type": "homepage",
            "word_count": 300,
            "internal_links_out": 10,
        },
        {
            "normalized_url": "https://e.com/sitemap.xml",
            "status_code": 200,
            "content_type": "application/xml",
            "fetch_error": "",
            "page_type": "other",
            "word_count": 0,
            "internal_links_out": 0,
        },
        {
            "normalized_url": "https://e.com/missing",
            "status_code": 404,
            "content_type": "text/html",
            "fetch_error": "",
            "page_type": "other",
            "word_count": 40,
            "internal_links_out": 0,
        },
        {
            "normalized_url": "https://e.com/error",
            "status_code": 200,
            "content_type": "text/html",
            "fetch_error": "timeout",
            "page_type": "other",
            "word_count": 100,
            "internal_links_out": 2,
        },
    ]

    targets = select_render_targets(pages, render_mode="all", max_render_pages=10)
    assert [p["normalized_url"] for p in targets] == ["https://e.com/"]


def test_rendered_internal_link_count_includes_relative_and_absolute_internal() -> None:
    anchors = [
        {"href": "/relative"},
        {"href": "https://example.com/apex"},
        {"href": "https://www.example.com/www"},
        {"href": "https://blog.example.com/sub"},
        {"href": "https://external.org/x"},
    ]
    count = _count_internal_rendered_links(anchors, "https://www.example.com", "https://www.example.com/page")
    assert count == 3


def test_rendered_internal_link_count_respects_all_subdomains_scope() -> None:
    anchors = [
        {"href": "https://blog.example.com/sub"},
        {"href": "https://shop.example.com/sub"},
        {"href": "https://external.org/x"},
    ]
    count = _count_internal_rendered_links(
        anchors,
        "https://www.example.com",
        "https://www.example.com/page",
        scope_mode="all_subdomains",
    )
    assert count == 2


def test_select_render_targets_adaptive_prioritizes_shell_like_pages() -> None:
    pages = [
        {
            "normalized_url": "https://e.com/healthy",
            "status_code": 200,
            "content_type": "text/html",
            "fetch_error": "",
            "page_type": "service",
            "word_count": 500,
            "internal_links_out": 8,
            "title": "Healthy",
            "h1": "Healthy",
            "canonical_url": "https://e.com/healthy",
            "h1_count": 1,
            "shell_score": 5,
            "likely_js_shell": 0,
            "framework_guess": "",
        },
        {
            "normalized_url": "https://e.com/shell",
            "status_code": 200,
            "content_type": "text/html",
            "fetch_error": "",
            "page_type": "homepage",
            "word_count": 20,
            "internal_links_out": 0,
            "title": "",
            "h1": "",
            "canonical_url": "",
            "h1_count": 0,
            "shell_score": 60,
            "likely_js_shell": 1,
            "framework_guess": "react",
        },
    ]

    targets = select_render_targets(pages, render_mode="sample", max_render_pages=1)
    assert [p["normalized_url"] for p in targets] == ["https://e.com/shell"]
