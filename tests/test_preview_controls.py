from seo_audit.preview_controls import preview_controls_restrictive, preview_restriction_score, snippet_eligible


def test_permissive_preview_controls_are_not_restrictive() -> None:
    page = {
        "has_nosnippet_directive": 0,
        "max_snippet_directive": "-1",
        "max_image_preview_directive": "large",
        "max_video_preview_directive": "-1",
        "data_nosnippet_count": 0,
    }

    score, reasons = preview_restriction_score(page)
    assert score == 0
    assert reasons == []
    assert preview_controls_restrictive(page) is False


def test_restrictive_preview_controls_are_detected() -> None:
    page = {
        "has_nosnippet_directive": 1,
        "max_snippet_directive": "0",
        "max_image_preview_directive": "none",
        "max_video_preview_directive": "0",
        "data_nosnippet_count": 6,
    }

    score, reasons = preview_restriction_score(page)
    assert score >= 4
    assert "nosnippet" in reasons
    assert preview_controls_restrictive(page) is True


def test_snippet_eligibility_treats_minus_one_as_permissive() -> None:
    permissive = {
        "is_noindex": 0,
        "has_nosnippet_directive": 0,
        "max_snippet_directive": "-1",
        "max_image_preview_directive": "large",
        "max_video_preview_directive": "-1",
    }
    restrictive = {
        "is_noindex": 0,
        "has_nosnippet_directive": 0,
        "max_snippet_directive": "0",
        "max_image_preview_directive": "none",
        "max_video_preview_directive": "0",
    }

    assert snippet_eligible(permissive) is True
    assert snippet_eligible(restrictive) is False
