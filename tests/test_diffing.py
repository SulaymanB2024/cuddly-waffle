import json

from seo_audit.diffing import generate_page_diffs


def test_generate_page_diffs_emits_expected_families() -> None:
    previous = {
        "title": "Old Title",
        "meta_description": "Old Description",
        "canonical_url": "https://example.com/old",
        "effective_robots_json": json.dumps({"is_noindex": 0}, sort_keys=True),
        "heading_outline_json": json.dumps([{"level": 1, "text": "Old H1"}], sort_keys=True),
        "h1": "Old H1",
        "h1_count": 1,
        "schema_types_json": json.dumps(["Article"], sort_keys=True),
        "effective_internal_links_out": 2,
        "effective_links_json": json.dumps([{"href": "/about"}], sort_keys=True),
        "content_hash": "hash-old",
        "raw_content_hash": "hash-raw-old",
        "rendered_content_hash": "hash-render-old",
        "effective_content_hash": "hash-effective-old",
        "image_count": 1,
        "video_details_json": json.dumps([], sort_keys=True),
        "image_details_json": json.dumps([{"normalized_src": "https://example.com/a.jpg"}], sort_keys=True),
        "schema_validation_json": json.dumps({"eligible_features": [{"feature_family": "article_rich_results"}]}, sort_keys=True),
    }
    current = {
        "title": "New Title",
        "meta_description": "New Description",
        "canonical_url": "https://example.com/new",
        "effective_robots_json": json.dumps({"is_noindex": 1}, sort_keys=True),
        "heading_outline_json": json.dumps([{"level": 1, "text": "New H1"}], sort_keys=True),
        "h1": "New H1",
        "h1_count": 1,
        "schema_types_json": json.dumps(["Article", "Product"], sort_keys=True),
        "effective_internal_links_out": 4,
        "effective_links_json": json.dumps([{"href": "/pricing"}], sort_keys=True),
        "content_hash": "hash-new",
        "raw_content_hash": "hash-raw-new",
        "rendered_content_hash": "hash-render-new",
        "effective_content_hash": "hash-effective-new",
        "image_count": 2,
        "video_details_json": json.dumps([{"src": "https://example.com/v.mp4"}], sort_keys=True),
        "image_details_json": json.dumps([{"normalized_src": "https://example.com/b.jpg"}], sort_keys=True),
        "schema_validation_json": json.dumps({"eligible_features": [{"feature_family": "product_rich_results"}]}, sort_keys=True),
    }

    diffs = generate_page_diffs("run-1", "https://example.com/page", current, previous)
    families = {row.diff_family for row in diffs}

    assert "title" in families
    assert "meta_description" in families
    assert "canonical" in families
    assert "robots_directives" in families
    assert "headings" in families
    assert "schema_types" in families
    assert "internal_links" in families
    assert "primary_content_hash" in families
    assert "raw_content_hash" in families
    assert "rendered_content_hash" in families
    assert "media_inventory" in families
    assert "structured_data_eligibility_shifts" in families


def test_generate_page_diffs_returns_empty_for_unchanged_page() -> None:
    page = {
        "title": "Same",
        "meta_description": "Same",
        "canonical_url": "https://example.com/page",
        "effective_robots_json": "{}",
        "heading_outline_json": "[]",
        "h1": "Same",
        "h1_count": 1,
        "schema_types_json": "[]",
        "effective_internal_links_out": 1,
        "effective_links_json": "[]",
        "content_hash": "hash-same",
        "raw_content_hash": "hash-raw-same",
        "rendered_content_hash": "hash-render-same",
        "effective_content_hash": "hash-effective-same",
        "image_count": 0,
        "video_details_json": "[]",
        "image_details_json": "[]",
        "schema_validation_json": "{}",
    }

    diffs = generate_page_diffs("run-1", "https://example.com/page", page, dict(page))
    assert diffs == []
