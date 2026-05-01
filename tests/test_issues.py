import json

from seo_audit.issues import build_issues


def test_canonical_mismatch_issue_created() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/a",
            "final_url": "https://example.com/a",
            "status_code": 200,
            "content_type": "text/html",
            "canonical_url": "https://example.com/b",
            "title": "A",
            "meta_description": "D",
            "h1": "H",
            "internal_links_out": 4,
        }
    ]
    issues = build_issues("run1", pages)
    codes = {i.issue_code for i in issues}
    assert "CANONICAL_MISMATCH" in codes

    mismatch = next(i for i in issues if i.issue_code == "CANONICAL_MISMATCH")
    assert mismatch.technical_seo_gate == "canonicalization"
    assert mismatch.verification_status == "automated"
    assert mismatch.confidence_score >= 90


def test_system_xml_page_does_not_emit_content_quality_issues() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/sitemap.xml",
            "status_code": 200,
            "content_type": "text/xml",
            "title": "",
            "h1": "",
            "word_count": 0,
            "thin_content_flag": 1,
            "orphan_risk_flag": 1,
            "internal_links_out": 0,
        }
    ]
    issues = build_issues("run1", pages)
    codes = {i.issue_code for i in issues}
    assert "THIN_CONTENT" not in codes
    assert "ORPHAN_RISK" not in codes
    assert "LOW_INTERNAL_LINKS" not in codes


def test_404_html_page_does_not_emit_content_quality_issues() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/not-found",
            "final_url": "https://example.com/not-found",
            "status_code": 404,
            "content_type": "text/html",
            "title": "404 - Not found",
            "meta_description": "",
            "h1": "Not found",
            "word_count": 20,
            "thin_content_flag": 1,
            "orphan_risk_flag": 1,
            "internal_links_out": 0,
        }
    ]
    issues = build_issues("run1", pages)
    codes = {i.issue_code for i in issues}
    assert "THIN_CONTENT" not in codes
    assert "ORPHAN_RISK" not in codes
    assert "LOW_INTERNAL_LINKS" not in codes
    assert "MISSING_META_DESCRIPTION" not in codes


def test_system_xml_page_does_not_emit_render_gap_issue() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/sitemap.xml",
            "status_code": 200,
            "content_type": "application/xml",
            "render_gap_score": 90,
            "render_gap_reason": "xml mismatch",
        }
    ]
    issues = build_issues("run1", pages)
    codes = {i.issue_code for i in issues}
    assert "RENDER_GAP_HIGH" not in codes


def test_low_internal_links_issue_uses_under_three_threshold() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/service",
            "final_url": "https://example.com/service",
            "status_code": 200,
            "content_type": "text/html",
            "title": "Service",
            "meta_description": "desc",
            "h1": "Service",
            "canonical_url": "https://example.com/service",
            "internal_links_out": 2,
            "thin_content_flag": 0,
        }
    ]
    issues = build_issues("run1", pages)
    codes = {i.issue_code for i in issues}
    assert "LOW_INTERNAL_LINKS" in codes


def test_actionable_page_emits_render_gap_issue() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/service",
            "final_url": "https://example.com/service",
            "status_code": 200,
            "content_type": "text/html",
            "title": "Service",
            "meta_description": "Desc",
            "h1": "Service",
            "render_gap_score": 90,
            "render_gap_reason": "raw thin but rendered rich",
            "internal_links_out": 4,
        }
    ]
    issues = build_issues("run1", pages)
    codes = {i.issue_code for i in issues}
    assert "RENDER_GAP_HIGH" in codes


def test_low_internal_links_uses_effective_internal_count() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/service",
            "final_url": "https://example.com/service",
            "status_code": 200,
            "content_type": "text/html",
            "title": "Service",
            "meta_description": "desc",
            "h1": "Service",
            "canonical_url": "https://example.com/service",
            "internal_links_out": 1,
            "effective_internal_links_out": 5,
            "thin_content_flag": 0,
        }
    ]
    issues = build_issues("run1", pages)
    codes = {i.issue_code for i in issues}
    assert "LOW_INTERNAL_LINKS" not in codes


def test_js_shell_without_render_marks_content_issues_for_render_verification() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/spa",
            "final_url": "https://example.com/spa",
            "status_code": 200,
            "content_type": "text/html",
            "title": "SPA page",
            "meta_description": "desc",
            "h1": "",
            "internal_links_out": 4,
            "likely_js_shell": 1,
            "render_checked": 0,
            "used_render": 0,
        }
    ]
    issues = build_issues("run1", pages)
    missing_h1 = next(i for i in issues if i.issue_code == "MISSING_H1")
    assert missing_h1.verification_status == "needs_rendered_verification"
    assert missing_h1.confidence_score <= 60


def test_robots_noindex_conflict_issue_created() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/private",
            "final_url": "https://example.com/private",
            "status_code": 200,
            "content_type": "text/html",
            "title": "Private",
            "h1": "Private",
            "is_noindex": 1,
            "robots_blocked_flag": 1,
            "internal_links_out": 2,
        }
    ]
    issues = build_issues("run1", pages)
    codes = {i.issue_code for i in issues}
    assert "ROBOTS_NOINDEX_CONFLICT" in codes


def test_duplicate_canonical_tags_issue_created() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/page",
            "final_url": "https://example.com/page",
            "status_code": 200,
            "content_type": "text/html",
            "title": "Page",
            "meta_description": "desc",
            "h1": "Page",
            "canonical_url": "https://example.com/page",
            "canonical_count": 2,
            "canonical_urls_json": '["https://example.com/page", "https://example.com/page?dup=1"]',
            "internal_links_out": 4,
        }
    ]
    issues = build_issues("run1", pages)
    duplicate = next(i for i in issues if i.issue_code == "DUPLICATE_CANONICAL_TAGS")
    assert duplicate.technical_seo_gate == "canonicalization"


def test_hreflang_reciprocity_issue_created_when_backlink_missing() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/en/page",
            "final_url": "https://example.com/en/page",
            "status_code": 200,
            "content_type": "text/html",
            "title": "EN",
            "meta_description": "desc",
            "h1": "EN",
            "canonical_url": "https://example.com/en/page",
            "hreflang_links_json": '[{"lang":"fr-fr","href":"https://example.com/fr/page"}]',
            "internal_links_out": 4,
        },
        {
            "normalized_url": "https://example.com/fr/page",
            "final_url": "https://example.com/fr/page",
            "status_code": 200,
            "content_type": "text/html",
            "title": "FR",
            "meta_description": "desc",
            "h1": "FR",
            "canonical_url": "https://example.com/fr/page",
            "hreflang_links_json": '[]',
            "internal_links_out": 4,
        },
    ]
    issues = build_issues("run1", pages)
    codes = {i.issue_code for i in issues}
    assert "HREFLANG_RECIPROCITY_MISSING" in codes


def test_pagination_and_faceted_risks_detected() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/catalog?page=2&sort=price",
            "final_url": "https://example.com/catalog?page=2&sort=price",
            "status_code": 200,
            "content_type": "text/html",
            "title": "Catalog",
            "meta_description": "desc",
            "h1": "Catalog",
            "canonical_url": "https://example.com/catalog",
            "internal_links_out": 4,
            "rel_next_url": "",
            "rel_prev_url": "",
        }
    ]
    issues = build_issues("run1", pages)
    codes = {i.issue_code for i in issues}
    assert "PAGINATION_SIGNAL_MISSING" in codes
    assert "FACETED_NAVIGATION_RISK" in codes


def test_pagination_signal_remains_low_severity_diagnostic() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/services?page=2",
            "final_url": "https://example.com/services?page=2",
            "status_code": 200,
            "content_type": "text/html",
            "title": "Services",
            "meta_description": "desc",
            "h1": "Services",
            "canonical_url": "https://example.com/services",
            "internal_links_out": 4,
            "rel_next_url": "",
            "rel_prev_url": "",
            "page_type": "service",
        }
    ]
    issues = build_issues("run1", pages)
    pagination_issue = next(i for i in issues if i.issue_code == "PAGINATION_SIGNAL_MISSING")

    assert pagination_issue.severity == "low"
    assert pagination_issue.technical_seo_gate == "canonicalization"


def test_redirect_access_truth_checks_detected() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/secure",
            "final_url": "https://example.com/secure",
            "status_code": 403,
            "content_type": "text/html",
            "title": "Forbidden",
            "h1": "Forbidden",
            "redirect_chain_json": '["https://example.com/start", "https://example.com/mid", "https://example.com/secure"]',
        }
    ]
    issues = build_issues("run1", pages)
    codes = {i.issue_code for i in issues}
    assert "ACCESS_AUTH_BLOCKED" in codes
    assert "REDIRECT_TO_ERROR" in codes


def test_long_redirect_chain_issue_detected() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/landing",
            "final_url": "https://example.com/landing",
            "status_code": 200,
            "content_type": "text/html",
            "title": "Landing",
            "h1": "Landing",
            "meta_description": "desc",
            "redirect_chain_json": '["https://example.com/a", "https://example.com/b", "https://example.com/c", "https://example.com/landing"]',
        }
    ]
    issues = build_issues("run1", pages)
    codes = {i.issue_code for i in issues}
    assert "REDIRECT_CHAIN_LONG" in codes


def test_robots_blocked_page_uses_explicit_issue_codes() -> None:
    pages = [
        {
            "discovered_url": "https://example.com/private",
            "normalized_url": "https://example.com/private",
            "status_code": None,
            "content_type": "",
            "fetch_error": "",
            "robots_blocked_flag": 1,
            "in_sitemap_flag": 1,
        }
    ]
    issues = build_issues("run1", pages)
    codes = {i.issue_code for i in issues}
    assert "ROBOTS_BLOCKED_URL" in codes
    assert "SITEMAP_URL_BLOCKED_BY_ROBOTS" in codes
    assert "FETCH_FAILED" not in codes


def test_structured_data_parse_failure_is_reported_separately() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/service",
            "final_url": "https://example.com/service",
            "status_code": 200,
            "content_type": "text/html",
            "title": "Service",
            "meta_description": "desc",
            "h1": "Service",
            "canonical_url": "https://example.com/service",
            "internal_links_out": 4,
            "schema_parse_error_count": 2,
        }
    ]
    issues = build_issues("run1", pages)
    parse_issue = next(i for i in issues if i.issue_code == "STRUCTURED_DATA_PARSE_FAILED")
    assert parse_issue.technical_seo_gate == "indexability"


def test_schema_feature_specific_issues_emitted() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/article",
            "final_url": "https://example.com/article",
            "status_code": 200,
            "content_type": "text/html",
            "title": "Article",
            "meta_description": "desc",
            "h1": "Article",
            "canonical_url": "https://example.com/article",
            "internal_links_out": 4,
            "schema_validation_json": "{\"missing_required_by_feature\": {\"google:article_rich_results\": [\"headline\"]}, \"deprecated_features\": [{\"type\": \"DataVocabulary\", \"status\": \"deprecated\"}], \"visible_content_mismatches\": [{\"field\": \"headline\", \"value\": \"Mismatch\"}]}",
        }
    ]

    issues = build_issues("run1", pages)
    codes = {i.issue_code for i in issues}
    assert "SCHEMA_FEATURE_MISSING_REQUIRED" in codes
    assert "SCHEMA_DEPRECATED_MARKUP" in codes
    assert "SCHEMA_VISIBLE_CONTENT_MISMATCH" in codes


def test_exact_content_duplicate_issue_created() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/a",
            "final_url": "https://example.com/a",
            "status_code": 200,
            "content_type": "text/html",
            "title": "A",
            "meta_description": "desc",
            "h1": "A",
            "word_count": 220,
            "canonical_url": "https://example.com/a",
            "content_hash": "hash-1",
            "internal_links_out": 4,
        },
        {
            "normalized_url": "https://example.com/b",
            "final_url": "https://example.com/b",
            "status_code": 200,
            "content_type": "text/html",
            "title": "B",
            "meta_description": "desc",
            "h1": "B",
            "word_count": 230,
            "canonical_url": "https://example.com/b",
            "content_hash": "hash-1",
            "internal_links_out": 4,
        },
        {
            "normalized_url": "https://example.com/c",
            "final_url": "https://example.com/c",
            "status_code": 200,
            "content_type": "text/html",
            "title": "C",
            "meta_description": "desc",
            "h1": "C",
            "word_count": 230,
            "canonical_url": "https://example.com/c",
            "content_hash": "hash-2",
            "internal_links_out": 4,
        },
    ]

    issues = build_issues("run1", pages)
    duplicate_issues = [issue for issue in issues if issue.issue_code == "EXACT_CONTENT_DUPLICATE"]
    assert len(duplicate_issues) == 2
    assert all(issue.technical_seo_gate == "indexability" for issue in duplicate_issues)


def test_exact_content_duplicate_uses_effective_content_hash() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/a",
            "final_url": "https://example.com/a",
            "status_code": 200,
            "content_type": "text/html",
            "title": "A",
            "meta_description": "desc",
            "h1": "A",
            "word_count": 230,
            "canonical_url": "https://example.com/a",
            "content_hash": "raw-a",
            "effective_content_hash": "effective-shared",
            "internal_links_out": 4,
        },
        {
            "normalized_url": "https://example.com/b",
            "final_url": "https://example.com/b",
            "status_code": 200,
            "content_type": "text/html",
            "title": "B",
            "meta_description": "desc",
            "h1": "B",
            "word_count": 230,
            "canonical_url": "https://example.com/b",
            "content_hash": "raw-b",
            "effective_content_hash": "effective-shared",
            "internal_links_out": 4,
        },
    ]

    issues = build_issues("run1", pages)
    duplicate_issues = [issue for issue in issues if issue.issue_code == "EXACT_CONTENT_DUPLICATE"]
    assert len(duplicate_issues) == 2


def test_shell_confirmed_pages_suppress_raw_duplicate_noise() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/spa/a",
            "final_url": "https://example.com/spa/a",
            "status_code": 200,
            "content_type": "text/html",
            "title": "A",
            "meta_description": "desc",
            "h1": "A",
            "word_count": 240,
            "canonical_url": "https://example.com/spa/a",
            "effective_content_hash": "raw-shell-hash",
            "content_hash": "raw-shell-hash",
            "shell_state": "raw_shell_confirmed_after_render",
            "used_render": 1,
            "effective_field_provenance_json": json.dumps({"content_hash": "raw_fallback"}, sort_keys=True),
            "internal_links_out": 4,
        },
        {
            "normalized_url": "https://example.com/spa/b",
            "final_url": "https://example.com/spa/b",
            "status_code": 200,
            "content_type": "text/html",
            "title": "B",
            "meta_description": "desc",
            "h1": "B",
            "word_count": 240,
            "canonical_url": "https://example.com/spa/b",
            "effective_content_hash": "raw-shell-hash",
            "content_hash": "raw-shell-hash",
            "shell_state": "raw_shell_confirmed_after_render",
            "used_render": 1,
            "effective_field_provenance_json": json.dumps({"content_hash": "raw_fallback"}, sort_keys=True),
            "internal_links_out": 4,
        },
    ]

    issues = build_issues("run1", pages)
    duplicate_issues = [issue for issue in issues if issue.issue_code == "EXACT_CONTENT_DUPLICATE"]
    assert duplicate_issues == []


def test_governance_blocks_emit_openai_google_extended_gptbot_and_oai_adsbot_issues() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/service",
            "final_url": "https://example.com/service",
            "status_code": 200,
            "content_type": "text/html",
            "title": "Service",
            "meta_description": "desc",
            "h1": "Service",
            "canonical_url": "https://example.com/service",
            "internal_links_out": 4,
            "governance_openai_allowed": 0,
            "governance_google_extended_allowed": 0,
            "governance_gptbot_allowed": 0,
            "governance_oai_adsbot_allowed": 0,
            "governance_googlebot_allowed": 1,
            "governance_bingbot_allowed": 1,
        }
    ]

    issues = build_issues("run1", pages)
    codes = {issue.issue_code for issue in issues}
    assert "OPENAI_SEARCHBOT_BLOCKED" in codes
    assert "GOOGLE_EXTENDED_BLOCKED" in codes
    assert "GPTBOT_BLOCKED" in codes
    assert "OAI_ADSBOT_BLOCKED" in codes


def test_preview_controls_restrictive_issues_for_indexable_pages() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/service",
            "final_url": "https://example.com/service",
            "status_code": 200,
            "content_type": "text/html",
            "title": "Service",
            "meta_description": "desc",
            "h1": "Service",
            "canonical_url": "https://example.com/service",
            "internal_links_out": 4,
            "has_nosnippet_directive": 1,
            "max_snippet_directive": "0",
            "max_image_preview_directive": "none",
            "data_nosnippet_count": 6,
        }
    ]
    issues = build_issues("run1", pages)
    codes = {issue.issue_code for issue in issues}
    assert "OVER_RESTRICTIVE_SNIPPET_CONTROLS" in codes
    assert "BING_PREVIEW_CONTROLS_RESTRICTIVE" in codes


def test_raw_render_preview_and_noindex_mismatch_issues() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/service",
            "final_url": "https://example.com/service",
            "status_code": 200,
            "content_type": "text/html",
            "title": "Service",
            "meta_description": "desc",
            "h1": "Service",
            "canonical_url": "https://example.com/service",
            "internal_links_out": 4,
            "meta_robots": "index,follow",
            "rendered_meta_robots": "noindex,nosnippet,max-snippet:0",
            "data_nosnippet_count": 0,
            "rendered_data_nosnippet_count": 5,
            "used_render": 1,
        }
    ]
    issues = build_issues("run1", pages)
    codes = {issue.issue_code for issue in issues}
    assert "RAW_RENDER_NOINDEX_MISMATCH" in codes
    assert "RAW_RENDER_PREVIEW_CONTROL_MISMATCH" in codes


def test_noindex_pages_do_not_emit_over_restrictive_preview_issue() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/private",
            "final_url": "https://example.com/private",
            "status_code": 200,
            "content_type": "text/html",
            "title": "Private",
            "meta_description": "desc",
            "h1": "Private",
            "canonical_url": "https://example.com/private",
            "internal_links_out": 1,
            "is_noindex": 1,
            "has_nosnippet_directive": 1,
            "max_snippet_directive": "0",
            "max_image_preview_directive": "none",
            "data_nosnippet_count": 8,
        }
    ]
    issues = build_issues("run1", pages)
    codes = {issue.issue_code for issue in issues}
    assert "OVER_RESTRICTIVE_SNIPPET_CONTROLS" not in codes


def test_permissive_preview_controls_do_not_emit_restrictive_issues() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/public",
            "final_url": "https://example.com/public",
            "status_code": 200,
            "content_type": "text/html",
            "title": "Public",
            "meta_description": "desc",
            "h1": "Public",
            "canonical_url": "https://example.com/public",
            "internal_links_out": 4,
            "max_snippet_directive": "-1",
            "max_image_preview_directive": "large",
            "max_video_preview_directive": "-1",
        }
    ]
    issues = build_issues("run1", pages)
    codes = {issue.issue_code for issue in issues}
    assert "OVER_RESTRICTIVE_SNIPPET_CONTROLS" not in codes
    assert "BING_PREVIEW_CONTROLS_RESTRICTIVE" not in codes


def test_graph_metrics_emit_architecture_issues() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/",
            "final_url": "https://example.com/",
            "status_code": 200,
            "content_type": "text/html",
            "title": "Home",
            "meta_description": "desc",
            "h1": "Home",
            "canonical_url": "https://example.com/",
            "internal_links_out": 6,
            "effective_internal_links_out": 6,
            "word_count": 320,
            "effective_text_len": 320,
            "page_type": "homepage",
            "inlinks": 12,
            "internal_pagerank": 0.20,
            "betweenness": 0.02,
            "closeness": 0.42,
            "community_id": 1,
            "bridge_flag": 0,
        },
        {
            "normalized_url": "https://example.com/service",
            "final_url": "https://example.com/service",
            "status_code": 200,
            "content_type": "text/html",
            "title": "Service",
            "meta_description": "desc",
            "h1": "Service",
            "canonical_url": "https://example.com/service",
            "internal_links_out": 3,
            "effective_internal_links_out": 3,
            "word_count": 320,
            "effective_text_len": 320,
            "page_type": "service",
            "inlinks": 1,
            "internal_pagerank": 0.01,
            "betweenness": 0.005,
            "closeness": 0.10,
            "community_id": 1,
            "bridge_flag": 0,
        },
        {
            "normalized_url": "https://example.com/services/hub",
            "final_url": "https://example.com/services/hub",
            "status_code": 200,
            "content_type": "text/html",
            "title": "Services Hub",
            "meta_description": "desc",
            "h1": "Services Hub",
            "canonical_url": "https://example.com/services/hub",
            "internal_links_out": 8,
            "effective_internal_links_out": 8,
            "word_count": 320,
            "effective_text_len": 320,
            "page_type": "service",
            "inlinks": 10,
            "internal_pagerank": 0.12,
            "betweenness": 0.20,
            "closeness": 0.48,
            "community_id": 1,
            "bridge_flag": 1,
        },
        {
            "normalized_url": "https://example.com/blog/a",
            "final_url": "https://example.com/blog/a",
            "status_code": 200,
            "content_type": "text/html",
            "title": "Blog A",
            "meta_description": "desc",
            "h1": "Blog A",
            "canonical_url": "https://example.com/blog/a",
            "internal_links_out": 4,
            "effective_internal_links_out": 4,
            "word_count": 320,
            "effective_text_len": 320,
            "page_type": "article",
            "inlinks": 3,
            "internal_pagerank": 0.03,
            "betweenness": 0.01,
            "closeness": 0.22,
            "community_id": 2,
            "bridge_flag": 0,
        },
        {
            "normalized_url": "https://example.com/blog/b",
            "final_url": "https://example.com/blog/b",
            "status_code": 200,
            "content_type": "text/html",
            "title": "Blog B",
            "meta_description": "desc",
            "h1": "Blog B",
            "canonical_url": "https://example.com/blog/b",
            "internal_links_out": 4,
            "effective_internal_links_out": 4,
            "word_count": 320,
            "effective_text_len": 320,
            "page_type": "article",
            "inlinks": 3,
            "internal_pagerank": 0.015,
            "betweenness": 0.008,
            "closeness": 0.20,
            "community_id": 2,
            "bridge_flag": 0,
        },
    ]

    issues = build_issues("run1", pages)
    codes = {issue.issue_code for issue in issues}

    assert "IMPORTANT_PAGE_WEAK_SUPPORT" in codes
    assert "INTERNAL_FLOW_HUB_OVERLOAD" in codes
    assert "INTERNAL_CLUSTER_DISCONNECTED" in codes


def test_shell_confirmed_pages_suppress_raw_only_content_diagnostics() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/spa/route",
            "final_url": "https://example.com/spa/route",
            "status_code": 200,
            "content_type": "text/html",
            "title": "Route",
            "meta_description": "desc",
            "h1": "Rendered H1",
            "raw_h1_count": 0,
            "effective_h1_count": 1,
            "canonical_url": "https://example.com/spa/route",
            "internal_links_out": 1,
            "effective_internal_links_out": 6,
            "raw_text_len": 20,
            "effective_text_len": 260,
            "word_count": 20,
            "used_render": 1,
            "shell_state": "raw_shell_confirmed_after_render",
        }
    ]

    issues = build_issues("run1", pages)
    codes = {issue.issue_code for issue in issues}
    assert "RAW_ONLY_MISSING_H1" not in codes
    assert "RAW_ONLY_THIN_CONTENT" not in codes
    assert "RAW_ONLY_LOW_INTERNAL_LINKS" not in codes


def test_shell_possible_with_healthy_render_suppresses_raw_only_content_diagnostics() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/spa/possible",
            "final_url": "https://example.com/spa/possible",
            "status_code": 200,
            "content_type": "text/html",
            "title": "Route",
            "effective_title": "Route",
            "meta_description": "A descriptive summary that is long enough for quality checks in this test fixture.",
            "effective_meta_description": "A descriptive summary that is long enough for quality checks in this test fixture.",
            "h1": "Rendered H1",
            "raw_h1_count": 0,
            "effective_h1_count": 1,
            "canonical_url": "https://example.com/spa/possible",
            "internal_links_out": 1,
            "effective_internal_links_out": 6,
            "raw_text_len": 30,
            "effective_text_len": 260,
            "word_count": 30,
            "used_render": 1,
            "shell_state": "raw_shell_possible",
            "render_error": "",
        }
    ]

    issues = build_issues("run1", pages)
    codes = {issue.issue_code for issue in issues}
    assert "RAW_ONLY_MISSING_H1" not in codes
    assert "RAW_ONLY_THIN_CONTENT" not in codes
    assert "RAW_ONLY_LOW_INTERNAL_LINKS" not in codes


def test_cluster_root_cause_suppresses_repetitive_canonical_page_symptoms() -> None:
    pages = [
        {
            "normalized_url": "https://example.com/a",
            "final_url": "https://example.com/a",
            "status_code": 200,
            "content_type": "text/html",
            "title": "A",
            "meta_description": "desc",
            "h1": "A",
            "canonical_url": "https://example.com/canonical",
            "effective_canonical": "https://example.com/canonical",
            "canonical_cluster_key": "https://example.com/canonical",
            "canonical_cluster_role": "self",
            "internal_links_out": 4,
        },
        {
            "normalized_url": "https://example.com/b",
            "final_url": "https://example.com/b",
            "status_code": 200,
            "content_type": "text/html",
            "title": "B",
            "meta_description": "desc",
            "h1": "B",
            "canonical_url": "https://example.com/canonical",
            "effective_canonical": "https://example.com/canonical",
            "canonical_cluster_key": "https://example.com/canonical",
            "canonical_cluster_role": "alias",
            "internal_links_out": 4,
        },
        {
            "normalized_url": "https://example.com/c",
            "final_url": "https://example.com/c",
            "status_code": 200,
            "content_type": "text/html",
            "title": "C",
            "meta_description": "desc",
            "h1": "C",
            "canonical_url": "https://example.com/canonical",
            "effective_canonical": "https://example.com/canonical",
            "canonical_cluster_key": "https://example.com/canonical",
            "canonical_cluster_role": "alias",
            "internal_links_out": 4,
        },
    ]

    issues = build_issues("run1", pages)
    codes = {issue.issue_code for issue in issues}

    assert "CLUSTER_CANONICAL_COLLISION" in codes
    assert "CANONICAL_MISMATCH" not in codes
    assert "CANONICAL_SELF_MISMATCH" not in codes


def test_shell_root_cause_suppresses_redundant_canonical_and_hreflang_page_symptoms() -> None:
    shared_hreflang = '[{"lang":"fr-fr","href":"https://example.com/fr/shared"}]'
    pages = [
        {
            "normalized_url": "https://example.com/spa/a",
            "final_url": "https://example.com/spa/a",
            "status_code": 200,
            "content_type": "text/html",
            "title": "A",
            "meta_description": "desc",
            "h1": "A",
            "canonical_url": "https://example.com/spa/a",
            "effective_canonical": "https://example.com/spa/a",
            "raw_canonical": "https://example.com/spa/a",
            "internal_links_out": 4,
            "shell_state": "raw_shell_confirmed_after_render",
            "used_render": 1,
            "raw_hreflang_links_json": shared_hreflang,
            "hreflang_links_json": shared_hreflang,
        },
        {
            "normalized_url": "https://example.com/spa/b",
            "final_url": "https://example.com/spa/b",
            "status_code": 200,
            "content_type": "text/html",
            "title": "B",
            "meta_description": "desc",
            "h1": "B",
            "canonical_url": "https://example.com/spa/a",
            "effective_canonical": "https://example.com/spa/a",
            "raw_canonical": "https://example.com/spa/a",
            "internal_links_out": 4,
            "shell_state": "raw_shell_confirmed_after_render",
            "used_render": 1,
            "raw_hreflang_links_json": shared_hreflang,
            "hreflang_links_json": shared_hreflang,
        },
        {
            "normalized_url": "https://example.com/spa/c",
            "final_url": "https://example.com/spa/c",
            "status_code": 200,
            "content_type": "text/html",
            "title": "C",
            "meta_description": "desc",
            "h1": "C",
            "canonical_url": "https://example.com/spa/a",
            "effective_canonical": "https://example.com/spa/a",
            "raw_canonical": "https://example.com/spa/a",
            "internal_links_out": 4,
            "shell_state": "raw_shell_confirmed_after_render",
            "used_render": 1,
            "raw_hreflang_links_json": shared_hreflang,
            "hreflang_links_json": shared_hreflang,
        },
        {
            "normalized_url": "https://example.com/fr/shared",
            "final_url": "https://example.com/fr/shared",
            "status_code": 200,
            "content_type": "text/html",
            "title": "FR",
            "meta_description": "desc",
            "h1": "FR",
            "canonical_url": "https://example.com/fr/shared",
            "effective_canonical": "https://example.com/fr/shared",
            "internal_links_out": 4,
            "hreflang_links_json": "[]",
        },
    ]

    issues = build_issues("run1", pages)
    codes = {issue.issue_code for issue in issues}

    assert "STATIC_SHELL_CANONICAL_REUSED_ACROSS_ROUTES" in codes
    assert "STATIC_SHELL_HREFLANG_REUSED_ACROSS_ROUTES" in codes
    assert "CANONICAL_MISMATCH" not in codes
    assert "CANONICAL_SELF_MISMATCH" not in codes
    assert "HREFLANG_RECIPROCITY_MISSING" not in codes
