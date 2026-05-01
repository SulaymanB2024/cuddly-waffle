from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


_KNOWN_CRAWLER_TOKENS = {
    "*",
    "googlebot",
    "googlebot-news",
    "bingbot",
    "oai-searchbot",
    "gptbot",
    "oai-adsbot",
    "chatgpt-user",
    "google-extended",
}

_IMAGE_PREVIEW_RANK = {
    "none": 0,
    "standard": 1,
    "large": 2,
}


@dataclass(frozen=True)
class RobotsDecision:
    applied_directives: set[str]
    scoped_sources: dict[str, list[str]]

    @property
    def is_noindex(self) -> bool:
        return "noindex" in self.applied_directives

    @property
    def is_nofollow(self) -> bool:
        return "nofollow" in self.applied_directives


def parse_directive_tokens(raw_values: Iterable[str]) -> set[str]:
    tokens: set[str] = set()
    for raw in raw_values:
        for token in str(raw or "").split(","):
            cleaned = token.strip().lower()
            if cleaned:
                tokens.add(cleaned)
    return tokens


def _clean_values(raw_values: Iterable[str]) -> list[str]:
    values: list[str] = []
    for raw in raw_values:
        text = str(raw or "").strip()
        if text:
            values.append(text)
    return values


def _is_scoped_crawler_token(token: str) -> bool:
    cleaned = str(token or "").strip().lower()
    if not cleaned:
        return False
    if cleaned in _KNOWN_CRAWLER_TOKENS:
        return True
    return cleaned.endswith("bot") or cleaned.endswith("bot-news")


def resolve_page_controls(
    *,
    meta_map: dict[str, list[str]],
    x_robots_values: list[str],
    crawler_token: str,
) -> RobotsDecision:
    applied: set[str] = set()
    sources: dict[str, list[str]] = {}

    crawler_scope = str(crawler_token or "").strip().lower()
    generic_meta = _clean_values(meta_map.get("robots", []))
    if generic_meta:
        sources["robots"] = generic_meta
        applied |= parse_directive_tokens(generic_meta)

    if crawler_scope and crawler_scope not in {"generic", "robots", "*"}:
        scoped_meta = _clean_values(meta_map.get(crawler_scope, []))
        if scoped_meta:
            sources[crawler_scope] = scoped_meta
            applied |= parse_directive_tokens(scoped_meta)

    generic_x: list[str] = []
    scoped_x: list[str] = []
    for raw in _clean_values(x_robots_values):
        left, sep, right = raw.partition(":")
        if sep and _is_scoped_crawler_token(left):
            if left.strip().lower() == crawler_scope:
                scoped_value = right.strip()
                if scoped_value:
                    scoped_x.append(scoped_value)
            continue
        generic_x.append(raw)

    if generic_x:
        sources["x_robots_generic"] = generic_x
        applied |= parse_directive_tokens(generic_x)
    if scoped_x:
        sources["x_robots_scoped"] = scoped_x
        applied |= parse_directive_tokens(scoped_x)

    return RobotsDecision(applied_directives=applied, scoped_sources=sources)


def _directive_values(applied_directives: set[str], directive_name: str) -> list[str]:
    prefix = f"{directive_name}:"
    values: list[str] = []
    for directive in applied_directives:
        normalized = str(directive or "").strip().lower()
        if normalized.startswith(prefix):
            values.append(normalized.split(":", 1)[1].strip())
    return values


def _most_restrictive_numeric(values: list[str]) -> str:
    if not values:
        return ""

    parsed: list[int] = []
    for value in values:
        try:
            parsed.append(int(value))
        except ValueError:
            continue

    if parsed:
        non_negative = [value for value in parsed if value >= 0]
        if non_negative:
            return str(min(non_negative))
        return str(max(parsed))

    return values[0]


def _most_restrictive_image_preview(values: list[str]) -> str:
    if not values:
        return ""

    ranked: list[tuple[int, str]] = []
    for value in values:
        normalized = value.strip().lower()
        if normalized in _IMAGE_PREVIEW_RANK:
            ranked.append((_IMAGE_PREVIEW_RANK[normalized], normalized))

    if ranked:
        ranked.sort(key=lambda item: item[0])
        return ranked[0][1]

    return values[0].strip().lower()


def summarize_directives(decision: RobotsDecision) -> dict[str, object]:
    max_snippet = _most_restrictive_numeric(_directive_values(decision.applied_directives, "max-snippet"))
    max_image_preview = _most_restrictive_image_preview(
        _directive_values(decision.applied_directives, "max-image-preview")
    )
    max_video_preview = _most_restrictive_numeric(
        _directive_values(decision.applied_directives, "max-video-preview")
    )

    return {
        "is_noindex": decision.is_noindex,
        "is_nofollow": decision.is_nofollow,
        "has_nosnippet_directive": "nosnippet" in decision.applied_directives,
        "max_snippet_directive": max_snippet,
        "max_image_preview_directive": max_image_preview,
        "max_video_preview_directive": max_video_preview,
    }


def build_effective_robots_payload(decision: RobotsDecision) -> dict[str, object]:
    summary = summarize_directives(decision)
    return {
        "applied_directives": sorted(decision.applied_directives),
        "scoped_sources": decision.scoped_sources,
        **summary,
    }
