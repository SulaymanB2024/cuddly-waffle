from pathlib import Path

from seo_audit.config import AuditConfig
from seo_audit.url_policy import (
    CANONICAL_CANDIDATE_DUPLICATE,
    CRAWL_NORMALLY,
    CRAWL_ONCE_DIAGNOSTIC,
    CRAWL_SAMPLED,
    FETCH_HEADERS_ONLY,
    NEVER_ENQUEUE,
    classify_url_policy,
)


def _config() -> AuditConfig:
    return AuditConfig(domain="https://example.com", output_dir=Path("./out"))


def test_url_policy_defaults_to_crawl_normally_for_clean_urls() -> None:
    decision = classify_url_policy("https://example.com/service", _config())
    assert decision.policy_class == CRAWL_NORMALLY
    assert decision.enqueue is True
    assert decision.follow_links is True
    assert decision.fetch_headers_only is False


def test_url_policy_marks_action_urls_as_never_enqueue() -> None:
    decision = classify_url_policy("https://example.com/product?add-to-cart=123", _config())
    assert decision.policy_class == NEVER_ENQUEUE
    assert decision.enqueue is False
    assert decision.follow_links is False


def test_url_policy_marks_download_urls_as_headers_only() -> None:
    decision = classify_url_policy("https://example.com/export?download=csv", _config())
    assert decision.policy_class == FETCH_HEADERS_ONLY
    assert decision.enqueue is True
    assert decision.fetch_headers_only is True
    assert decision.follow_links is False


def test_url_policy_marks_preview_urls_as_diagnostic_once() -> None:
    decision = classify_url_policy("https://example.com/page?preview=1", _config())
    assert decision.policy_class == CRAWL_ONCE_DIAGNOSTIC
    assert decision.enqueue is True
    assert decision.follow_links is False


def test_url_policy_marks_sort_urls_as_canonical_candidates() -> None:
    decision = classify_url_policy("https://example.com/catalog?sort=price", _config())
    assert decision.policy_class == CANONICAL_CANDIDATE_DUPLICATE
    assert decision.enqueue is True
    assert decision.follow_links is False


def test_url_policy_can_sample_faceted_urls_out() -> None:
    config = _config()
    config.canonical_candidate_param_keys = ()
    config.faceted_sample_rate = 0.0
    decision = classify_url_policy("https://example.com/catalog?color=red&size=l", config)
    assert decision.policy_class == CRAWL_SAMPLED
    assert decision.enqueue is False
    assert decision.follow_links is False
