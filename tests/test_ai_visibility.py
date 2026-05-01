import json

from seo_audit.ai_visibility import (
    build_ai_visibility_payload,
    legacy_citation_evidence_from_payload,
    merge_ai_visibility_payload,
    parse_ai_visibility_payload,
)
from seo_audit.integrations import AdapterContext, GSCAnalyticsVisibilityAdapter, apply_visibility_adapters


def test_ai_visibility_payload_roundtrip_and_legacy_mapping() -> None:
    payload = build_ai_visibility_payload(
        potential_score=74,
        potential_reasons=["answer_like_visible_content", "schema_validation_weak"],
        observed_evidence={"gsc_impressions": 0, "chatgpt_referrals": 1},
        adapters_applied=["manual_seed"],
    )

    parsed = parse_ai_visibility_payload(json.dumps(payload))
    assert int(parsed["potential"]["score"]) == 74
    assert parsed["potential"]["reasons"] == ["answer_like_visible_content", "schema_validation_weak"]

    legacy = legacy_citation_evidence_from_payload(parsed)
    assert int(legacy["chatgpt_referrals"]) == 1
    assert legacy["eligibility_reasons"] == ["answer_like_visible_content", "schema_validation_weak"]
    assert "manual_seed" in legacy["observed_sources"]


def test_ai_visibility_adapter_enrichment_from_gsc_metrics() -> None:
    base = {
        "gsc_impressions": 0,
        "gsc_clicks": 0,
        "chatgpt_referrals": 0,
    }

    evidence, applied, errors = apply_visibility_adapters(
        base,
        context=AdapterContext(
            run_id="run-1",
            page={"normalized_url": "https://example.com/page"},
            gsc_metrics={"impressions": 42.2, "clicks": 7.0, "ctr": 0.17, "position": 8.3},
        ),
        adapters=(GSCAnalyticsVisibilityAdapter(),),
    )

    assert errors == []
    assert applied == ["gsc_analytics"]
    assert int(evidence["gsc_impressions"]) == 42
    assert int(evidence["gsc_clicks"]) == 7
    assert float(evidence["gsc_ctr"]) > 0.0
    assert "gsc_analytics" in evidence["observed_sources"]


def test_merge_ai_visibility_payload_updates_evidence_without_losing_potential() -> None:
    existing = {
        "potential": {
            "score": 65,
            "reasons": ["weak_internal_prominence"],
        },
        "observed_evidence": {
            "chatgpt_referrals": 0,
        },
    }

    merged = merge_ai_visibility_payload(
        existing,
        observed_evidence={"chatgpt_referrals": 3, "gsc_impressions": 12},
        adapters_applied=["gsc_analytics"],
    )

    assert int(merged["potential"]["score"]) == 65
    assert merged["potential"]["reasons"] == ["weak_internal_prominence"]
    assert int(merged["observed_evidence"]["chatgpt_referrals"]) == 3
    assert int(merged["observed_evidence"]["gsc_impressions"]) == 12
    assert "gsc_analytics" in merged["adapters_applied"]
