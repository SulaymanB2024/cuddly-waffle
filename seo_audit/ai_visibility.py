from __future__ import annotations

import json


def _json_object(raw: object) -> dict[str, object]:
    if isinstance(raw, dict):
        return dict(raw)
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def build_ai_visibility_payload(
    *,
    potential_score: int,
    potential_reasons: list[str],
    observed_evidence: dict[str, object],
    adapters_applied: list[str] | None = None,
    adapter_errors: list[str] | None = None,
) -> dict[str, object]:
    payload = {
        "potential": {
            "score": max(0, min(100, int(potential_score))),
            "reasons": [str(reason).strip() for reason in potential_reasons if str(reason).strip()],
        },
        "observed_evidence": dict(observed_evidence),
        "adapters_applied": [str(name).strip() for name in list(adapters_applied or []) if str(name).strip()],
    }
    normalized_errors = [str(error).strip() for error in list(adapter_errors or []) if str(error).strip()]
    if normalized_errors:
        payload["adapter_errors"] = normalized_errors
    return payload


def parse_ai_visibility_payload(raw: object) -> dict[str, object]:
    payload = _json_object(raw)
    potential = payload.get("potential")
    if not isinstance(potential, dict):
        potential = {}
    observed = payload.get("observed_evidence")
    if not isinstance(observed, dict):
        observed = {}
    adapters_applied = payload.get("adapters_applied")
    if not isinstance(adapters_applied, list):
        adapters_applied = []
    adapter_errors = payload.get("adapter_errors")
    if not isinstance(adapter_errors, list):
        adapter_errors = []

    return {
        "potential": {
            "score": max(0, min(100, int(potential.get("score") or 0))),
            "reasons": [
                str(reason).strip()
                for reason in list(potential.get("reasons") or [])
                if str(reason).strip()
            ],
        },
        "observed_evidence": observed,
        "adapters_applied": [str(name).strip() for name in adapters_applied if str(name).strip()],
        "adapter_errors": [str(error).strip() for error in adapter_errors if str(error).strip()],
    }


def merge_ai_visibility_payload(
    existing_payload: object,
    *,
    observed_evidence: dict[str, object] | None = None,
    adapters_applied: list[str] | None = None,
    adapter_errors: list[str] | None = None,
    potential_score: int | None = None,
    potential_reasons: list[str] | None = None,
) -> dict[str, object]:
    payload = parse_ai_visibility_payload(existing_payload)

    potential = payload.get("potential")
    if not isinstance(potential, dict):
        potential = {"score": 0, "reasons": []}

    score = int(potential.get("score") or 0)
    if potential_score is not None:
        score = max(0, min(100, int(potential_score)))

    reasons = [str(reason).strip() for reason in list(potential.get("reasons") or []) if str(reason).strip()]
    if potential_reasons is not None:
        reasons = [str(reason).strip() for reason in potential_reasons if str(reason).strip()]

    merged = {
        "potential": {
            "score": score,
            "reasons": reasons,
        },
        "observed_evidence": dict(observed_evidence if observed_evidence is not None else payload.get("observed_evidence") or {}),
    }

    applied = list(payload.get("adapters_applied") or [])
    for name in list(adapters_applied or []):
        normalized = str(name).strip()
        if normalized and normalized not in applied:
            applied.append(normalized)
    merged["adapters_applied"] = applied

    errors = list(payload.get("adapter_errors") or [])
    for error in list(adapter_errors or []):
        normalized = str(error).strip()
        if normalized:
            errors.append(normalized)
    if errors:
        merged["adapter_errors"] = errors

    return merged


def legacy_citation_evidence_from_payload(payload: object) -> dict[str, object]:
    parsed = parse_ai_visibility_payload(payload)
    observed = dict(parsed.get("observed_evidence") or {})

    reasons = [str(reason).strip() for reason in list(parsed.get("potential", {}).get("reasons") or []) if str(reason).strip()]
    if reasons and "eligibility_reasons" not in observed:
        observed["eligibility_reasons"] = reasons

    applied = [str(name).strip() for name in list(parsed.get("adapters_applied") or []) if str(name).strip()]
    if applied:
        observed["observed_sources"] = applied

    adapter_errors = [str(error).strip() for error in list(parsed.get("adapter_errors") or []) if str(error).strip()]
    if adapter_errors:
        observed["adapter_errors"] = adapter_errors

    return observed
