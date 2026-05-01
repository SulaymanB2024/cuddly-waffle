from seo_audit.classify import classify_page, has_local_business_schema


def test_classify_rules() -> None:
    assert classify_page("https://example.com/", "Home", "Welcome") == "homepage"
    assert classify_page("https://example.com/contact", "Contact", "Contact us") == "contact"
    assert classify_page("https://example.com/blog/post", "Insight", "Article") == "article"


def test_classify_additional_page_types() -> None:
    assert classify_page("https://example.com/services/web", "What We Do", "Services") == "service"
    assert classify_page("https://example.com/privacy", "Privacy Policy", "Legal") == "legal"
    assert classify_page("https://example.com/locations/austin", "Austin", "Near me") == "location"
    assert classify_page("https://example.com/wp-admin", "Login", "Dashboard") == "utility"


def test_classifier_is_conservative_on_ambiguous_copy() -> None:
    page_type = classify_page(
        "https://example.com/page",
        "Welcome",
        "Learn more",
    )
    assert page_type == "other"


def test_local_business_schema_detector_excludes_organization() -> None:
    assert has_local_business_schema('["Organization"]') is False
    assert has_local_business_schema('["LocalBusiness"]') is True
