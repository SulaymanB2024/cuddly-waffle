from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qsl, urlsplit


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _normalized_query_risk(url: str, risky_keys: set[str]) -> float:
    params = parse_qsl(urlsplit(url).query, keep_blank_values=True)
    if not params:
        return 0.0
    keys = {key.strip().lower() for key, _ in params if key.strip()}
    if not keys:
        return 0.0

    risky = len(keys.intersection(risky_keys))
    density = risky / max(1, len(keys))
    count_factor = min(1.0, len(params) / 10.0)
    return _clamp((density * 0.70) + (count_factor * 0.30), 0.0, 1.0)


def cluster_key_for_param_url(url: str, risky_keys: set[str]) -> str:
    split = urlsplit(url)
    host = (split.hostname or "").lower()
    path = split.path or "/"
    keys = sorted({key.strip().lower() for key, _ in parse_qsl(split.query, keep_blank_values=True) if key.strip()})
    risky = sorted(set(keys).intersection(risky_keys))
    key_blob = ",".join(risky or keys)
    return f"{host}{path}?{key_blob}"


@dataclass(slots=True)
class FrontierSignals:
    depth: int
    in_sitemap: bool = False
    sitemap_lastmod_freshness: float = 0.0
    template_importance: float = 0.5
    internal_prominence: float = 0.0
    canonical_confidence: float = 0.5
    query_param_risk: float = 0.0
    gsc_demand_signal: float = 0.0
    render_risk_signal: float = 0.0
    change_signal: float = 0.0
    platform_priority_signal: float = 0.0


def compute_frontier_priority(signals: FrontierSignals) -> float:
    # Weights intentionally mirror crawl-budget demand/capacity heuristics.
    score = 0.0
    score += _clamp(signals.template_importance, 0.0, 1.0) * 220.0
    score += _clamp(signals.internal_prominence, 0.0, 1.0) * 140.0
    score += _clamp(signals.gsc_demand_signal, 0.0, 1.0) * 120.0
    score += _clamp(signals.change_signal, 0.0, 1.0) * 100.0
    score += _clamp(signals.render_risk_signal, 0.0, 1.0) * 75.0
    score += _clamp(signals.platform_priority_signal, 0.0, 1.0) * 60.0
    score += _clamp(signals.canonical_confidence, 0.0, 1.0) * 45.0

    if signals.in_sitemap:
        score += 35.0
    score += _clamp(signals.sitemap_lastmod_freshness, 0.0, 1.0) * 35.0

    # Query-risk URLs are useful but should spend less budget by default.
    score -= _clamp(signals.query_param_risk, 0.0, 1.0) * 130.0

    # Depth reduces demand as crawl distance increases.
    score -= max(0, int(signals.depth)) * 14.0

    return round(_clamp(score, 0.0, 999.0), 4)


@dataclass(slots=True)
class FrontierItem:
    url: str
    depth: int
    priority: float
    discovered_via: str
    source_url: str
    cluster_key: str
    payload: dict[str, Any] = field(default_factory=dict)


class PriorityFrontier:
    def __init__(self, *, max_size: int, cluster_budget: int = 3) -> None:
        self.max_size = max(1, int(max_size))
        self.cluster_budget = max(1, int(cluster_budget))
        self._heap: list[tuple[float, int, FrontierItem]] = []
        self._seq = 0
        self._url_set: set[str] = set()
        self._cluster_counts: dict[str, int] = {}

    def __len__(self) -> int:
        return len(self._heap)

    def has_url(self, url: str) -> bool:
        return url in self._url_set

    def push(self, item: FrontierItem) -> bool:
        if item.url in self._url_set:
            return False

        if len(self._heap) >= self.max_size:
            return False

        cluster_count = self._cluster_counts.get(item.cluster_key, 0)
        if cluster_count >= self.cluster_budget:
            return False

        self._url_set.add(item.url)
        self._cluster_counts[item.cluster_key] = cluster_count + 1
        self._seq += 1
        heapq.heappush(self._heap, (-float(item.priority), self._seq, item))
        return True

    def pop(self) -> FrontierItem | None:
        if not self._heap:
            return None

        _neg_priority, _seq, item = heapq.heappop(self._heap)
        self._url_set.discard(item.url)
        cluster_count = self._cluster_counts.get(item.cluster_key, 0)
        if cluster_count <= 1:
            self._cluster_counts.pop(item.cluster_key, None)
        else:
            self._cluster_counts[item.cluster_key] = cluster_count - 1
        return item


def signals_for_url(
    *,
    url: str,
    depth: int,
    discovered_via: str,
    policy_class: str,
    risky_query_keys: set[str],
) -> FrontierSignals:
    template_importance = 0.50
    internal_prominence = 0.25
    platform_priority_signal = 0.0

    path = (urlsplit(url).path or "/").lower()
    if path in {"", "/"}:
        template_importance = 1.0
        internal_prominence = 1.0
    elif any(token in path for token in ("/service", "/location", "/product", "/collections", "/article")):
        template_importance = 0.80
    elif any(token in path for token in ("/blog", "/news", "/resources")):
        template_importance = 0.65

    if discovered_via == "seed":
        internal_prominence = max(internal_prominence, 0.95)
    elif discovered_via in {"raw_link", "render_link"}:
        internal_prominence = max(internal_prominence, 0.45)

    query_risk = _normalized_query_risk(url, risky_query_keys)
    if policy_class in {"crawl_sampled", "canonical_candidate_duplicate"}:
        query_risk = max(query_risk, 0.55)
    if policy_class in {"crawl_once_diagnostic", "fetch_headers_only"}:
        query_risk = max(query_risk, 0.35)

    return FrontierSignals(
        depth=max(0, int(depth)),
        template_importance=template_importance,
        internal_prominence=internal_prominence,
        canonical_confidence=0.55,
        query_param_risk=query_risk,
        gsc_demand_signal=0.0,
        render_risk_signal=0.0,
        change_signal=0.0,
        platform_priority_signal=platform_priority_signal,
    )
