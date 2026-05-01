from __future__ import annotations

from dataclasses import dataclass

from seo_audit.schema_registry import (
    SchemaRegistry,
    load_default_schema_registry,
    normalize_schema_type_token,
)


@dataclass(slots=True)
class SchemaValidationResult:
    score: int
    findings: list[dict[str, object]]
    type_counts: dict[str, int]
    syntax_valid: bool
    recognized_types: list[str]
    eligible_features: list[dict[str, object]]
    deprecated_features: list[dict[str, object]]
    missing_required_by_feature: dict[str, list[str]]
    missing_recommended_by_feature: dict[str, list[str]]
    visible_content_mismatches: list[dict[str, object]]
    engine_feature_scores: dict[str, int]


def _type_tokens(node: dict) -> list[str]:
    raw_type = node.get("@type")
    if isinstance(raw_type, str):
        token = normalize_schema_type_token(raw_type)
        return [token] if token else []
    if isinstance(raw_type, list):
        tokens = []
        for item in raw_type:
            token = normalize_schema_type_token(str(item))
            if token:
                tokens.append(token)
        return tokens
    return []


def _has_value(node: dict, key: str) -> bool:
    value = node.get(key)
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, list):
        return len(value) > 0
    if isinstance(value, dict):
        return len(value) > 0
    return True


def _as_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        stripped = value.strip()
        return [stripped] if stripped else []
    return []


def _node_value_text(node: dict, key: str) -> str:
    value = node.get(key)
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        for item in value:
            text = _node_value_text({"_": item}, "_")
            if text:
                return text
        return ""
    if isinstance(value, dict):
        for nested_key in ("name", "headline", "title", "@id", "url"):
            nested = _node_value_text(value, nested_key)
            if nested:
                return nested
        return ""
    return str(value).strip()


def validate_schema_nodes(
    nodes: list[dict],
    *,
    visible_text: str = "",
    page_type: str = "",
    registry: SchemaRegistry | None = None,
    engine: str = "google",
) -> SchemaValidationResult:
    findings: list[dict[str, object]] = []
    type_counts: dict[str, int] = {}

    score = 100
    visible_l = (visible_text or "").lower()
    page_type_token = str(page_type or "").strip().lower()
    resolved_registry = registry or load_default_schema_registry()

    syntax_valid = True
    recognized_types: set[str] = set()
    eligible_features: list[dict[str, object]] = []
    deprecated_features: list[dict[str, object]] = []
    missing_required_by_feature: dict[str, list[str]] = {}
    missing_recommended_by_feature: dict[str, list[str]] = {}
    visible_content_mismatches: list[dict[str, object]] = []

    engine_feature_values: dict[str, list[int]] = {}

    for node in nodes:
        if not isinstance(node, dict):
            syntax_valid = False
            continue

        for type_token in _type_tokens(node):
            type_key = type_token.strip()
            if not type_key:
                continue
            type_counts[type_key.lower()] = type_counts.get(type_key.lower(), 0) + 1
            recognized_types.add(type_key)

            matching_rules = resolved_registry.rules_for_type(type_key, engine=engine)
            if not matching_rules:
                continue

            for rule in matching_rules:
                feature_key = f"{rule.engine}:{rule.feature_family}"
                feature_score = 100

                if rule.status == "deprecated":
                    deprecated_features.append(
                        {
                            "engine": rule.engine,
                            "type": rule.type,
                            "feature_family": rule.feature_family,
                            "status": rule.status,
                            "docs_url": rule.docs_url,
                        }
                    )
                    findings.append(
                        {
                            "type": rule.type,
                            "severity": "medium",
                            "category": "deprecated_markup",
                            "feature_family": rule.feature_family,
                            "docs_url": rule.docs_url,
                        }
                    )
                    score -= 10
                    feature_score = min(feature_score, 35)

                if rule.page_type_allowlist and page_type_token and page_type_token not in {
                    token.lower() for token in rule.page_type_allowlist
                }:
                    feature_score = min(feature_score, 80)

                missing_required = [field for field in rule.required_fields if not _has_value(node, field)]
                missing_recommended = [field for field in rule.recommended_fields if not _has_value(node, field)]

                if missing_required:
                    missing_required_by_feature.setdefault(feature_key, [])
                    for field in missing_required:
                        if field not in missing_required_by_feature[feature_key]:
                            missing_required_by_feature[feature_key].append(field)
                    findings.append(
                        {
                            "type": rule.type,
                            "severity": "high",
                            "category": "missing_required",
                            "feature_family": rule.feature_family,
                            "fields": missing_required,
                        }
                    )
                    score -= min(45, 15 * len(missing_required))
                    feature_score -= 25 * len(missing_required)

                if missing_recommended:
                    missing_recommended_by_feature.setdefault(feature_key, [])
                    for field in missing_recommended:
                        if field not in missing_recommended_by_feature[feature_key]:
                            missing_recommended_by_feature[feature_key].append(field)
                    findings.append(
                        {
                            "type": rule.type,
                            "severity": "medium",
                            "category": "missing_recommended",
                            "feature_family": rule.feature_family,
                            "fields": missing_recommended,
                        }
                    )
                    score -= min(20, 5 * len(missing_recommended))
                    feature_score -= 8 * len(missing_recommended)

                visibility_gaps: list[dict[str, object]] = []
                for field in rule.visibility_requirements:
                    value = _node_value_text(node, field)
                    value_l = value.lower()
                    if value and visible_l and value_l not in visible_l:
                        mismatch = {
                            "engine": rule.engine,
                            "type": rule.type,
                            "feature_family": rule.feature_family,
                            "field": field,
                            "value": value[:160],
                        }
                        visibility_gaps.append(mismatch)
                        visible_content_mismatches.append(mismatch)
                if visibility_gaps:
                    findings.append(
                        {
                            "type": rule.type,
                            "severity": "low",
                            "category": "visible_content_mismatch",
                            "feature_family": rule.feature_family,
                            "mismatches": visibility_gaps,
                        }
                    )
                    score -= min(15, 5 * len(visibility_gaps))
                    feature_score -= 10 * len(visibility_gaps)

                can_be_eligible = (
                    rule.status in {"supported", "limited"}
                    and not missing_required
                    and not visibility_gaps
                )
                if can_be_eligible:
                    eligible_features.append(
                        {
                            "engine": rule.engine,
                            "type": rule.type,
                            "feature_family": rule.feature_family,
                            "status": rule.status,
                            "docs_url": rule.docs_url,
                        }
                    )

                if rule.status == "limited":
                    feature_score = min(feature_score, 85)
                elif rule.status in {"non_search", "other_surface_only"}:
                    feature_score = min(feature_score, 70)

                engine_key = str(rule.engine or "unknown").lower()
                engine_feature_values.setdefault(engine_key, []).append(max(0, min(100, feature_score)))

    score = max(0, min(100, score))

    engine_feature_scores: dict[str, int] = {}
    for engine_key, values in engine_feature_values.items():
        if not values:
            continue
        engine_feature_scores[engine_key] = int(round(sum(values) / len(values)))

    if engine_feature_scores:
        avg_engine_score = int(round(sum(engine_feature_scores.values()) / len(engine_feature_scores)))
        score = int(round((score + avg_engine_score) / 2.0))

    return SchemaValidationResult(
        score=max(0, min(100, score)),
        findings=findings,
        type_counts=type_counts,
        syntax_valid=syntax_valid,
        recognized_types=sorted(recognized_types),
        eligible_features=eligible_features,
        deprecated_features=deprecated_features,
        missing_required_by_feature={k: sorted(set(v)) for k, v in sorted(missing_required_by_feature.items())},
        missing_recommended_by_feature={k: sorted(set(v)) for k, v in sorted(missing_recommended_by_feature.items())},
        visible_content_mismatches=visible_content_mismatches,
        engine_feature_scores=engine_feature_scores,
    )
