from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
from pathlib import Path


def normalize_schema_type_token(raw_type: str) -> str:
    token = str(raw_type or "").strip()
    if not token:
        return ""

    if token.startswith("http://") or token.startswith("https://"):
        token = token.rsplit("/", 1)[-1]
    if "#" in token:
        token = token.rsplit("#", 1)[-1]
    if ":" in token:
        token = token.rsplit(":", 1)[-1]
    return token.strip()


@dataclass(slots=True)
class SchemaRule:
    engine: str
    type: str
    feature_family: str
    status: str
    required_fields: tuple[str, ...]
    recommended_fields: tuple[str, ...]
    visibility_requirements: tuple[str, ...]
    page_type_allowlist: tuple[str, ...]
    docs_url: str
    last_reviewed_at: str

    @property
    def type_key(self) -> str:
        return normalize_schema_type_token(self.type).lower()


@dataclass(slots=True)
class SchemaRegistry:
    version: str
    rules: tuple[SchemaRule, ...]

    def rules_for_type(self, schema_type: str, *, engine: str = "") -> list[SchemaRule]:
        type_key = normalize_schema_type_token(schema_type).lower()
        if not type_key:
            return []

        requested_engine = str(engine or "").strip().lower()
        matched: list[SchemaRule] = []
        for rule in self.rules:
            if rule.type_key != type_key:
                continue
            if requested_engine and str(rule.engine).strip().lower() != requested_engine:
                continue
            matched.append(rule)
        return matched


def _as_tuple_strings(value: object) -> tuple[str, ...]:
    if isinstance(value, (list, tuple, set)):
        return tuple(str(item).strip() for item in value if str(item).strip())
    return ()


@lru_cache(maxsize=1)
def load_default_schema_registry() -> SchemaRegistry:
    registry_path = Path(__file__).with_name("schema_registry.json")
    raw = registry_path.read_text(encoding="utf-8")
    payload = json.loads(raw)

    version = str(payload.get("version") or "")
    raw_rules = payload.get("rules") or []
    rules: list[SchemaRule] = []

    if isinstance(raw_rules, list):
        for row in raw_rules:
            if not isinstance(row, dict):
                continue
            rules.append(
                SchemaRule(
                    engine=str(row.get("engine") or "google"),
                    type=normalize_schema_type_token(str(row.get("type") or "")),
                    feature_family=str(row.get("feature_family") or "unknown"),
                    status=str(row.get("status") or "limited").strip().lower(),
                    required_fields=_as_tuple_strings(row.get("required_fields")),
                    recommended_fields=_as_tuple_strings(row.get("recommended_fields")),
                    visibility_requirements=_as_tuple_strings(row.get("visibility_requirements")),
                    page_type_allowlist=_as_tuple_strings(row.get("page_type_allowlist")),
                    docs_url=str(row.get("docs_url") or ""),
                    last_reviewed_at=str(row.get("last_reviewed_at") or ""),
                )
            )

    return SchemaRegistry(version=version, rules=tuple(rules))
