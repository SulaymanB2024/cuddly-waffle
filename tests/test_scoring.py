import json

from seo_audit.scoring import score_page


def test_scoring_deterministic() -> None:
    page = {
        "status_code": 200,
        "title": "T",
        "meta_description": "D",
        "h1": "H",
        "canonical_url": "https://example.com",
        "thin_content_flag": 0,
        "render_gap_score": 10,
        "internal_links_out": 5,
        "has_contact_signal": True,
        "has_location_signal": True,
        "has_local_schema": True,
        "has_map": False,
    }
    s = score_page(page, 80)
    assert s["crawlability_score"] == 100
    assert s["overall_score"] > 70


def test_scoring_clamps_extreme_inputs() -> None:
    page = {
        "status_code": 500,
        "fetch_error": "timeout",
        "render_gap_score": 500,
        "internal_links_out": 0,
    }
    s = score_page(page, 200)
    assert 0 <= s["crawlability_score"] <= 100
    assert 0 <= s["onpage_score"] <= 100
    assert 0 <= s["render_risk_score"] <= 100
    assert 0 <= s["internal_linking_score"] <= 100
    assert 0 <= s["performance_score"] <= 100
    assert 0 <= s["overall_score"] <= 100


def test_scoring_internal_link_threshold() -> None:
    shared = {
        "status_code": 200,
        "title": "T",
        "meta_description": "D",
        "h1": "H",
        "canonical_url": "https://example.com/a",
        "thin_content_flag": 0,
        "render_gap_score": 0,
        "inlinks": 5,
        "crawl_depth": 1,
        "nav_linked_flag": 1,
        "orphan_risk_flag": 0,
    }
    low_links = score_page({**shared, "internal_links_out": 2}, 70)
    healthy_links = score_page({**shared, "internal_links_out": 3}, 70)
    assert low_links["internal_linking_score"] < healthy_links["internal_linking_score"]
    assert healthy_links["internal_linking_score"] >= 82


def test_scoring_prefers_effective_internal_link_count() -> None:
    shared = {
        "status_code": 200,
        "title": "T",
        "meta_description": "D",
        "h1": "H",
        "canonical_url": "https://example.com/a",
        "render_gap_score": 0,
        "internal_links_out": 1,
        "effective_internal_links_out": 5,
        "inlinks": 5,
        "crawl_depth": 1,
        "nav_linked_flag": 1,
        "orphan_risk_flag": 0,
    }
    scored = score_page(shared, 70)
    low = score_page({**shared, "effective_internal_links_out": 1}, 70)
    assert scored["internal_linking_score"] > low["internal_linking_score"]


def test_general_site_uses_neutral_local_baseline() -> None:
    page = {
        "status_code": 200,
        "title": "T",
        "meta_description": "D",
        "h1": "H",
        "canonical_url": "https://example.com",
        "render_gap_score": 0,
        "internal_links_out": 5,
    }
    general = score_page(page, 70, site_type="general")
    local = score_page(page, 70, site_type="local")
    assert general["local_seo_score"] == -1
    assert local["local_seo_score"] == 0


def test_missing_social_cards_do_not_trigger_search_penalty() -> None:
    base = {
        "status_code": 200,
        "title": "Service",
        "meta_description": "Desc",
        "h1": "Service",
        "canonical_url": "https://example.com/service",
        "render_gap_score": 0,
        "internal_links_out": 5,
        "schema_types_json": '["Service"]',
        "schema_parse_error_count": 0,
    }
    without_social = score_page(base, 70)
    with_social = score_page({**base, "og_title": "Service", "twitter_title": "Service"}, 70)

    without_explained = json.loads(without_social["explanation_json"])
    with_explained = json.loads(with_social["explanation_json"])

    assert without_social["quality_score"] == with_social["quality_score"]
    assert (
        without_explained["dimensions"]["scores"]["structured_data_validity_score"]
        == with_explained["dimensions"]["scores"]["structured_data_validity_score"]
    )


def test_preview_controls_dimension_penalizes_restrictive_controls() -> None:
    clean = score_page(
        {
            "status_code": 200,
            "title": "Service",
            "meta_description": "Desc",
            "h1": "Service",
            "canonical_url": "https://example.com/service",
            "render_gap_score": 0,
            "internal_links_out": 5,
        },
        70,
    )
    restrictive = score_page(
        {
            "status_code": 200,
            "title": "Service",
            "meta_description": "Desc",
            "h1": "Service",
            "canonical_url": "https://example.com/service",
            "render_gap_score": 0,
            "internal_links_out": 5,
            "has_nosnippet_directive": 1,
            "max_snippet_directive": "0",
            "max_image_preview_directive": "none",
        },
        70,
    )
    clean_explained = json.loads(clean["explanation_json"])
    restrictive_explained = json.loads(restrictive["explanation_json"])

    assert (
        restrictive_explained["dimensions"]["scores"]["preview_controls_score"]
        < clean_explained["dimensions"]["scores"]["preview_controls_score"]
    )


def test_preview_controls_dimension_treats_negative_one_and_large_as_permissive() -> None:
    clean = score_page(
        {
            "status_code": 200,
            "title": "Service",
            "meta_description": "Desc",
            "h1": "Service",
            "canonical_url": "https://example.com/service",
            "render_gap_score": 0,
            "internal_links_out": 5,
        },
        70,
    )
    permissive = score_page(
        {
            "status_code": 200,
            "title": "Service",
            "meta_description": "Desc",
            "h1": "Service",
            "canonical_url": "https://example.com/service",
            "render_gap_score": 0,
            "internal_links_out": 5,
            "max_snippet_directive": "-1",
            "max_image_preview_directive": "large",
            "max_video_preview_directive": "-1",
        },
        70,
    )
    clean_explained = json.loads(clean["explanation_json"])
    permissive_explained = json.loads(permissive["explanation_json"])

    assert (
        permissive_explained["dimensions"]["scores"]["preview_controls_score"]
        == clean_explained["dimensions"]["scores"]["preview_controls_score"]
    )


def test_scoring_is_conservative_for_robots_blocked_unfetched_pages() -> None:
    page = {
        "normalized_url": "https://example.com/private",
        "status_code": None,
        "robots_blocked_flag": 1,
    }
    scored = score_page(page, 70)
    assert scored["crawlability_score"] == 40
    assert scored["onpage_score"] == 35
    assert scored["overall_score"] == 35
    assert scored["coverage_score"] >= 70


def test_scoring_emits_versioned_explanation_payload() -> None:
    page = {
        "status_code": 200,
        "title": "Service",
        "meta_description": "Desc",
        "h1": "Service",
        "canonical_url": "https://example.com/service",
        "render_gap_score": 0,
        "internal_links_out": 4,
    }
    issues = [
        {
            "issue_code": "MISSING_TITLE",
            "severity": "medium",
            "technical_seo_gate": "indexability",
            "certainty_state": "Verified",
            "reach": "single_page",
            "confidence_score": 90,
        }
    ]

    scored = score_page(page, 70, page_issues=issues)
    parsed = json.loads(scored["explanation_json"])

    assert scored["score_version"] == "1.1.0"
    assert scored["scoring_model_version"] == "1.1.0"
    assert scored["score_profile"] == "default"
    assert scored["scoring_profile"] == "default"
    assert scored["score_explanation_json"] == scored["explanation_json"]
    assert parsed["score_version"] == scored["score_version"]
    assert parsed["scoring_model_version"] == scored["scoring_model_version"]
    assert parsed["score_profile"] == scored["score_profile"]
    assert parsed["scoring_profile"] == scored["scoring_profile"]
    assert parsed["dimensions"]["scores"]["crawlability_score"] == scored["crawlability_score"]
    assert parsed["cap"]["score_cap"] == scored["score_cap"]
    assert parsed["overall"]["overall_score"] == scored["overall_score"]
    assert "not_applicable_or_missing" in parsed["dimensions"]["notes"]


def test_scoring_profile_metadata_explicit_and_site_fallback() -> None:
    page = {
        "status_code": 200,
        "title": "Service",
        "meta_description": "Desc",
        "h1": "Service",
        "canonical_url": "https://example.com/service",
        "render_gap_score": 0,
        "internal_links_out": 4,
    }

    explicit = score_page(page, 70, site_type="local", score_profile="campaign_local")
    fallback = score_page(page, 70, site_type="local", score_profile="")

    assert explicit["scoring_profile"] == "campaign_local"
    assert explicit["score_profile"] == "campaign_local"
    assert fallback["scoring_profile"] == "local"
    assert fallback["score_profile"] == "local"


def test_pagination_signal_has_low_risk_pressure() -> None:
    page = {
        "status_code": 200,
        "title": "Catalog",
        "meta_description": "Desc",
        "h1": "Catalog",
        "canonical_url": "https://example.com/catalog?page=2",
        "render_gap_score": 0,
        "internal_links_out": 5,
        "page_type": "service",
    }
    pagination_issue = {
        "issue_code": "PAGINATION_SIGNAL_MISSING",
        "severity": "low",
        "technical_seo_gate": "canonicalization",
        "certainty_state": "Verified",
        "reach": "sitewide",
        "confidence_score": 95,
    }
    low_links_issue = {
        "issue_code": "LOW_INTERNAL_LINKS",
        "severity": "low",
        "technical_seo_gate": "discovery",
        "certainty_state": "Verified",
        "reach": "sitewide",
        "confidence_score": 95,
    }

    pagination_scored = score_page(page, 70, page_issues=[pagination_issue])
    low_links_scored = score_page(page, 70, page_issues=[low_links_issue])
    pagination_explained = json.loads(pagination_scored["explanation_json"])

    assert pagination_scored["risk_score"] < low_links_scored["risk_score"]
    assert pagination_scored["risk_score"] <= 5
    assert "PAGINATION_SIGNAL_MISSING" in pagination_explained["risk"]["neutralized_codes"]
    assert pagination_explained["risk"]["notable_contributors"][0]["risk_family"] == "canonicalization"


def test_thin_content_penalty_is_continuous_near_threshold() -> None:
    shared = {
        "status_code": 200,
        "title": "Service",
        "meta_description": "Desc",
        "h1": "Service",
        "canonical_url": "https://example.com/service",
        "page_type": "service",
        "render_gap_score": 0,
        "internal_links_out": 5,
        "inlinks": 5,
        "crawl_depth": 1,
        "nav_linked_flag": 1,
        "orphan_risk_flag": 0,
    }
    just_above = score_page({**shared, "effective_text_len": 222}, 70)
    just_below = score_page({**shared, "effective_text_len": 218}, 70)

    assert just_below["onpage_score"] <= just_above["onpage_score"]
    assert (just_above["onpage_score"] - just_below["onpage_score"]) <= 4


def test_near_empty_pages_are_harsh_but_sparse_utility_is_not_treated_as_broken() -> None:
    near_empty_service = score_page(
        {
            "status_code": 200,
            "title": "Service",
            "meta_description": "Desc",
            "h1": "Service",
            "canonical_url": "https://example.com/service",
            "page_type": "service",
            "effective_text_len": 8,
            "render_gap_score": 0,
            "internal_links_out": 5,
            "inlinks": 5,
            "crawl_depth": 1,
            "nav_linked_flag": 1,
        },
        70,
    )
    sparse_utility = score_page(
        {
            "status_code": 200,
            "title": "Contact",
            "meta_description": "Desc",
            "h1": "Contact",
            "canonical_url": "https://example.com/contact",
            "page_type": "contact",
            "effective_text_len": 48,
            "render_gap_score": 0,
            "internal_links_out": 3,
            "inlinks": 2,
            "crawl_depth": 1,
            "nav_linked_flag": 1,
        },
        70,
    )

    assert near_empty_service["onpage_score"] < sparse_utility["onpage_score"]
    assert near_empty_service["quality_score"] < sparse_utility["quality_score"]


def test_risk_diminishing_returns_taper_repeated_family_issues() -> None:
    page = {
        "status_code": 200,
        "title": "Service",
        "meta_description": "Desc",
        "h1": "Service",
        "canonical_url": "https://example.com/service",
        "render_gap_score": 0,
        "internal_links_out": 5,
        "inlinks": 5,
        "crawl_depth": 1,
        "nav_linked_flag": 1,
        "page_type": "service",
    }
    repeated_same_family = [
        {
            "issue_code": "LOW_INTERNAL_LINKS",
            "severity": "low",
            "technical_seo_gate": "discovery",
            "certainty_state": "Verified",
            "reach": "sitewide",
            "confidence_score": 90,
        }
        for _ in range(7)
    ]
    mixed_families = [
        {
            "issue_code": "LOW_INTERNAL_LINKS",
            "severity": "low",
            "technical_seo_gate": "discovery",
            "certainty_state": "Verified",
            "reach": "sitewide",
            "confidence_score": 90,
        },
        {
            "issue_code": "MISSING_TITLE",
            "severity": "low",
            "technical_seo_gate": "indexability",
            "certainty_state": "Verified",
            "reach": "sitewide",
            "confidence_score": 90,
        },
        {
            "issue_code": "CANONICAL_MISMATCH",
            "severity": "low",
            "technical_seo_gate": "canonicalization",
            "certainty_state": "Verified",
            "reach": "sitewide",
            "confidence_score": 90,
        },
        {
            "issue_code": "RENDER_GAP_HIGH",
            "severity": "low",
            "technical_seo_gate": "rendering",
            "certainty_state": "Verified",
            "reach": "sitewide",
            "confidence_score": 90,
        },
    ]

    repeated_score = score_page(page, 70, page_issues=repeated_same_family)
    mixed_score = score_page(page, 70, page_issues=mixed_families)
    repeated_explained = json.loads(repeated_score["explanation_json"])

    assert repeated_score["risk_score"] < mixed_score["risk_score"]
    assert repeated_score["risk_score"] < 55
    assert repeated_explained["risk"]["mode"] == "issue_blend_family_diminishing_returns"
    assert repeated_explained["risk"]["top_risk_families"][0]["risk_family"] == "internal_linking"


def test_internal_architecture_uses_linkgraph_signals() -> None:
    shared = {
        "status_code": 200,
        "title": "Service",
        "meta_description": "Desc",
        "h1": "Service",
        "canonical_url": "https://example.com/service",
        "page_type": "service",
        "render_gap_score": 0,
        "internal_links_out": 3,
    }
    strong_structure = score_page(
        {
            **shared,
            "effective_internal_links_out": 3,
            "inlinks": 8,
            "crawl_depth": 1,
            "nav_linked_flag": 1,
            "orphan_risk_flag": 0,
        },
        70,
    )
    weak_structure = score_page(
        {
            **shared,
            "effective_internal_links_out": 3,
            "inlinks": 0,
            "crawl_depth": 6,
            "nav_linked_flag": 0,
            "orphan_risk_flag": 1,
        },
        70,
    )

    assert strong_structure["internal_linking_score"] > weak_structure["internal_linking_score"]


def test_internal_architecture_uses_second_pass_graph_signals() -> None:
    shared = {
        "status_code": 200,
        "title": "Service",
        "meta_description": "Desc",
        "h1": "Service",
        "canonical_url": "https://example.com/service",
        "page_type": "service",
        "render_gap_score": 0,
        "internal_links_out": 3,
        "effective_internal_links_out": 3,
        "inlinks": 3,
        "crawl_depth": 2,
        "nav_linked_flag": 1,
        "orphan_risk_flag": 0,
    }

    supported = score_page(
        {
            **shared,
            "internal_pagerank": 0.08,
            "betweenness": 0.02,
            "closeness": 0.32,
            "bridge_flag": 0,
        },
        70,
    )
    under_supported = score_page(
        {
            **shared,
            "inlinks": 1,
            "internal_pagerank": 0.008,
            "betweenness": 0.004,
            "closeness": 0.12,
            "bridge_flag": 0,
        },
        70,
    )
    overloaded_hub = score_page(
        {
            **shared,
            "internal_pagerank": 0.08,
            "betweenness": 0.18,
            "closeness": 0.32,
            "bridge_flag": 1,
        },
        70,
    )

    assert supported["internal_linking_score"] > under_supported["internal_linking_score"]
    assert supported["internal_linking_score"] > overloaded_hub["internal_linking_score"]
