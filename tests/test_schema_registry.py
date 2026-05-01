from seo_audit.schema_validation import validate_schema_nodes


def test_schema_validation_registry_supports_feature_dimensions() -> None:
    nodes = [
        {
            "@type": "Article",
            "headline": "Structured Data Headline",
            "author": "Author Name",
            "datePublished": "2025-01-01",
            "image": "https://example.com/image.jpg",
        },
        {
            "@type": "Organization",
            "name": "Void Agency",
            "url": "https://example.com",
        },
    ]

    result = validate_schema_nodes(nodes, visible_text="Structured Data Headline")

    assert result.syntax_valid is True
    assert "Article" in result.recognized_types
    assert "Organization" in result.recognized_types
    assert result.engine_feature_scores.get("google", 0) > 0
    assert result.eligible_features
    assert isinstance(result.missing_required_by_feature, dict)
    assert isinstance(result.missing_recommended_by_feature, dict)


def test_schema_validation_registry_reports_deprecated_markup() -> None:
    nodes = [
        {
            "@type": "DataVocabulary",
        }
    ]

    result = validate_schema_nodes(nodes, visible_text="")

    assert result.deprecated_features
    assert any(item.get("status") == "deprecated" for item in result.deprecated_features)
    assert result.score < 100


def test_schema_validation_marks_visibility_mismatch() -> None:
    nodes = [
        {
            "@type": "Article",
            "headline": "Headline Only In Markup",
        }
    ]

    result = validate_schema_nodes(nodes, visible_text="Completely different visible text")

    assert result.visible_content_mismatches
    assert any(item.get("field") == "headline" for item in result.visible_content_mismatches)
