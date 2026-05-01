import json

from seo_audit.render import RenderResult
from seo_audit.resolution import crawl_persona_prefers_rendered, resolve_effective_page_facts


def test_persona_prefers_rendered_for_google_like() -> None:
    assert crawl_persona_prefers_rendered("googlebot_smartphone") is True
    assert crawl_persona_prefers_rendered("googlebot_desktop") is True
    assert crawl_persona_prefers_rendered("bingbot") is False


def test_resolver_prefers_rendered_for_google_persona() -> None:
    page = {
        "normalized_url": "https://example.com/page",
        "final_url": "https://example.com/page",
        "raw_title": "Raw Title",
        "raw_meta_description": "Raw Description",
        "raw_canonical": "https://example.com/page",
        "raw_canonical_urls_json": json.dumps(["https://example.com/page"]),
        "raw_hreflang_links_json": json.dumps([{"lang": "en-us", "href": "https://example.com/page"}]),
        "raw_content_hash": "raw-hash",
        "raw_text_len": 20,
        "internal_links_out": 0,
        "shell_state": "raw_shell_possible",
    }
    rendered = RenderResult(
        final_url="https://example.com/page",
        title="Rendered Title",
        canonical="https://example.com/page",
        h1s=["H1"],
        h1_count=1,
        word_count=260,
        links=[{"href": "/about"}, {"href": "/contact"}],
        canonical_urls=["https://example.com/page"],
        canonical_count=1,
        hreflang_links=[{"lang": "en-us", "href": "https://example.com/page"}],
        meta_description="Rendered Description",
        content_hash="rendered-hash",
    )

    updates = resolve_effective_page_facts(page, rendered, crawl_persona="googlebot_smartphone")
    provenance = json.loads(str(updates["effective_field_provenance_json"]))

    assert updates["effective_title"] == "Rendered Title"
    assert updates["effective_meta_description"] == "Rendered Description"
    assert updates["effective_content_hash"] == "rendered-hash"
    assert updates["effective_canonical"] == "https://example.com/page"
    assert provenance["title"] == "rendered"
    assert provenance["canonical"] == "resolver:rendered_single"
    assert updates["shell_state"] == "raw_shell_confirmed_after_render"


def test_resolver_marks_unresolved_canonical_conflict() -> None:
    page = {
        "normalized_url": "https://example.com/a",
        "final_url": "https://example.com/a",
        "raw_canonical": "https://example.com/a",
        "raw_canonical_urls_json": json.dumps(["https://example.com/a"]),
        "raw_content_hash": "raw-hash",
    }
    rendered = RenderResult(
        final_url="https://example.com/a",
        title="A",
        canonical="https://example.com/b",
        h1s=["A"],
        h1_count=1,
        word_count=200,
        links=[{"href": "/a"}],
        canonical_urls=["https://example.com/b", "https://example.com/c"],
        canonical_count=2,
        content_hash="rendered-hash",
    )

    updates = resolve_effective_page_facts(page, rendered, crawl_persona="googlebot_smartphone")
    assert updates["effective_canonical"] == ""
    assert int(updates["canonical_conflict_raw_vs_rendered"]) == 1
    assert int(updates["canonical_unresolved"]) == 1


def test_resolver_falls_back_to_raw_for_non_google_persona() -> None:
    page = {
        "normalized_url": "https://example.com/page",
        "final_url": "https://example.com/page",
        "raw_title": "Raw Title",
        "raw_meta_description": "Raw Description",
        "raw_canonical": "https://example.com/page",
        "raw_hreflang_links_json": "[]",
        "raw_content_hash": "raw-hash",
    }
    rendered = RenderResult(
        final_url="https://example.com/page",
        title="Rendered Title",
        canonical="https://example.com/page",
        h1s=["H1"],
        h1_count=1,
        word_count=260,
        links=[{"href": "/about"}],
        canonical_urls=["https://example.com/page"],
        canonical_count=1,
        content_hash="rendered-hash",
    )

    updates = resolve_effective_page_facts(page, rendered, crawl_persona="bingbot")
    assert updates["effective_title"] == "Raw Title"
    assert updates["effective_meta_description"] == "Raw Description"
    assert updates["effective_content_hash"] == "raw-hash"
