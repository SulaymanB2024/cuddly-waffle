from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return default


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


@dataclass(slots=True)
class AdapterContext:
    run_id: str
    page: Mapping[str, Any]
    gsc_metrics: Mapping[str, float] | None = None


class VisibilityEvidenceAdapter(Protocol):
    name: str

    def apply(self, evidence: dict[str, object], *, context: AdapterContext) -> dict[str, object]:
        ...


@dataclass(slots=True)
class GSCAnalyticsVisibilityAdapter:
    name: str = "gsc_analytics"

    def apply(self, evidence: dict[str, object], *, context: AdapterContext) -> dict[str, object]:
        metrics = dict(context.gsc_metrics or {})
        if not metrics:
            return dict(evidence)

        merged = dict(evidence)
        merged["gsc_impressions"] = max(0, _safe_int(metrics.get("impressions"), 0))
        merged["gsc_clicks"] = max(0, _safe_int(metrics.get("clicks"), 0))
        merged["gsc_ctr"] = max(0.0, round(_safe_float(metrics.get("ctr"), 0.0), 6))
        merged["gsc_position"] = max(0.0, round(_safe_float(metrics.get("position"), 0.0), 3))

        observed_sources = merged.get("observed_sources")
        normalized_sources: list[str] = []
        if isinstance(observed_sources, list):
            normalized_sources = [
                str(source).strip()
                for source in observed_sources
                if str(source).strip()
            ]
        if self.name not in normalized_sources:
            normalized_sources.append(self.name)
        merged["observed_sources"] = normalized_sources

        return merged


def apply_visibility_adapters(
    base_evidence: dict[str, object],
    *,
    context: AdapterContext,
    adapters: tuple[VisibilityEvidenceAdapter, ...],
) -> tuple[dict[str, object], list[str], list[str]]:
    evidence = dict(base_evidence)
    applied: list[str] = []
    errors: list[str] = []

    for adapter in adapters:
        adapter_name = str(getattr(adapter, "name", adapter.__class__.__name__) or adapter.__class__.__name__)
        try:
            updated = adapter.apply(evidence, context=context)
        except Exception as exc:  # pragma: no cover - adapters are isolated and optional.
            errors.append(f"{adapter_name}:{exc}")
            continue

        if isinstance(updated, dict):
            if updated != evidence:
                applied.append(adapter_name)
            evidence = updated

    return evidence, applied, errors
