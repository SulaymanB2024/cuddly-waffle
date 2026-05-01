import json

from seo_audit.cli import _format_render_reasons, _shell_reasons_from_signals_json
from seo_audit.issues import build_issues
from seo_audit.linkgraph import compute_link_metrics
from seo_audit.shell_detection import classify_raw_html_sufficiency


def test_shell_classifier_flags_shell_like_html() -> None:
    html = "<html><head><title>App</title><script></script><script></script><script></script><script></script><script></script><script></script></head><body><div id='root'></div></body></html>"
    result = classify_raw_html_sufficiency(html, "https://example.com/app", 200, "text/html", {})
    assert result.likely_js_shell is True
    assert result.shell_score >= 45


def test_issue_provenance_prefers_effective_rendered_facts() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/spa",
            "final_url": "https://example.com/spa",
            "status_code": 200,
            "content_type": "text/html",
            "title": "Shell",
            "meta_description": "desc",
            "h1": "",
            "canonical_url": "https://example.com/wrong",
            "raw_h1_count": 0,
            "raw_text_len": 10,
            "internal_links_out": 0,
            "raw_canonical": "https://example.com/wrong",
            "effective_h1_count": 1,
            "effective_text_len": 300,
            "effective_canonical": "https://example.com/spa",
            "effective_links_json": json.dumps([{"href": "/"}, {"href": "/about"}, {"href": "/contact"}]),
            "used_render": 1,
        }
    ]
    issues = build_issues("run1", pages)
    codes = {i.issue_code for i in issues}
    assert "MISSING_H1" not in codes
    assert "THIN_CONTENT" not in codes
    assert "CANONICAL_MISMATCH" not in codes
    assert "RAW_ONLY_MISSING_H1" in codes
    assert "RAW_ONLY_CANONICAL_MISMATCH" in codes


def test_effective_links_drive_graph_metrics() -> None:
    pages = [{"normalized_url": "https://example.com/"}, {"normalized_url": "https://example.com/spa-page"}]
    links = [
        {"source_url": "https://example.com/", "normalized_target_url": "https://example.com/spa-page", "is_internal": 1},
    ]
    metrics = compute_link_metrics("https://example.com/", pages, links)
    assert metrics["https://example.com/spa-page"]["crawl_depth"] == 1


def test_shell_reason_round_trip_preserves_reason_tokens() -> None:
    html = "<html><head><script></script><script></script><script></script><script></script><script></script><script></script></head><body><div id='root'></div></body></html>"
    classification = classify_raw_html_sufficiency(html, "https://example.com/app", 200, "text/html", {})
    serialized = json.dumps(classification.signals)

    reasons = _shell_reasons_from_signals_json(serialized)
    formatted = _format_render_reasons(reasons)

    assert reasons == classification.reasons
    assert "root_shell_markers" in reasons
    assert "root_shell_markers" in formatted
    assert "r;o;o;t" not in formatted
