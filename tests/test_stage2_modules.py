import json

from lxml import html as lxml_html

from seo_audit.citation import compute_citation_eligibility
from seo_audit.governance_matrix import build_governance_matrix, summarize_governance_matrices
from seo_audit.media_images import extract_image_assets
from seo_audit.media_video import extract_video_assets
from seo_audit.schema_graph import parse_schema_graph_nodes
from seo_audit.schema_render_diff import compare_schema_sets
from seo_audit.schema_validation import validate_schema_nodes


def test_governance_matrix_and_citation_penalize_blocking_signals() -> None:
    page = {
        "is_noindex": 1,
        "has_nosnippet_directive": 1,
        "max_snippet_directive": "0",
        "max_image_preview_directive": "none",
        "governance_googlebot_allowed": 0,
        "governance_bingbot_allowed": 1,
        "governance_openai_allowed": 0,
        "governance_google_extended_allowed": 1,
        "effective_text_len": 40,
        "effective_h1_count": 0,
        "effective_internal_links_out": 0,
        "schema_validation_score": 20,
    }

    matrix = build_governance_matrix(page)
    assessment = compute_citation_eligibility(page, matrix)

    assert matrix["googlebot"]["crawl_allowed"] is False
    assert assessment.eligibility_score < 40
    assert "noindex" in assessment.reasons


def test_governance_summary_treats_minus_one_preview_directives_as_permissive() -> None:
    page = {
        "is_noindex": 0,
        "has_nosnippet_directive": 0,
        "max_snippet_directive": "-1",
        "max_image_preview_directive": "large",
        "max_video_preview_directive": "-1",
    }

    matrix = build_governance_matrix(page)
    summary = summarize_governance_matrices([matrix])

    assert summary["preview_restricted_pages"] == 0


def test_governance_summary_separates_issue_like_and_diagnostic_preview_restrictions() -> None:
    page = {
        "is_noindex": 1,
        "has_nosnippet_directive": 1,
        "max_snippet_directive": "0",
        "max_image_preview_directive": "none",
        "max_video_preview_directive": "0",
    }

    matrix = build_governance_matrix(page)
    summary = summarize_governance_matrices([matrix])

    assert summary["preview_restricted_pages"] == 0
    assert summary["preview_restricted_diagnostic_pages"] == 1


def test_media_schema_helpers_extract_and_compare() -> None:
    html = """
    <html>
      <body>
        <img src="/images/hero-product-photo.jpg" alt="Hero product" width="1400" />
        <video src="/media/demo.mp4" controls></video>
        <iframe src="https://www.youtube.com/embed/abc123"></iframe>
        <script type="application/ld+json">
        {
          "@context": "https://schema.org",
          "@type": "VideoObject",
          "name": "How to tie shoes",
          "thumbnailUrl": "https://example.com/thumb.jpg",
          "uploadDate": "2024-01-01"
        }
        </script>
      </body>
    </html>
    """
    tree = lxml_html.fromstring(html)

    schema_nodes, parse_errors = parse_schema_graph_nodes(tree)
    assert parse_errors == 0
    assert schema_nodes

    validation = validate_schema_nodes(schema_nodes, visible_text="How to tie shoes quickly")
    assert validation.score > 0
    assert validation.type_counts.get("videoobject", 0) >= 1

    images, image_summary = extract_image_assets(tree, base_url="https://example.com/page")
    videos, video_summary = extract_video_assets(tree, base_url="https://example.com/page", schema_nodes=schema_nodes)

    assert len(images) == 1
    assert images[0]["normalized_src"].startswith("https://example.com/")
    assert int(image_summary["discoverability_score"]) > 0

    assert len(videos) >= 2
    assert int(video_summary["discoverability_score"]) > 0

    rendered_schema_nodes = [{"@type": "FAQPage"}]
    diff = compare_schema_sets(schema_nodes, rendered_schema_nodes)
    assert json.dumps(diff)
    assert diff["severity"] in {"medium", "high"}
