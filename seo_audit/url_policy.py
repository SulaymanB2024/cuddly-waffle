from __future__ import annotations

import hashlib
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlsplit

from seo_audit.config import AuditConfig


CRAWL_NORMALLY = "crawl_normally"
CRAWL_SAMPLED = "crawl_sampled"
CRAWL_ONCE_DIAGNOSTIC = "crawl_once_diagnostic"
NEVER_ENQUEUE = "never_enqueue"
FETCH_HEADERS_ONLY = "fetch_headers_only"
CANONICAL_CANDIDATE_DUPLICATE = "canonical_candidate_duplicate"


@dataclass(slots=True)
class URLPolicyDecision:
    policy_class: str = CRAWL_NORMALLY
    reason: str = "default"
    enqueue: bool = True
    follow_links: bool = True
    fetch_headers_only: bool = False


def _query_keys(url: str) -> set[str]:
    query = urlsplit(url).query
    return {key.strip().lower() for key, _ in parse_qsl(query, keep_blank_values=True) if key.strip()}


def _query_param_count(url: str) -> int:
    query = urlsplit(url).query
    return len(parse_qsl(query, keep_blank_values=True))


def _is_sampled_in(url: str, sample_rate: float) -> bool:
    if sample_rate >= 1.0:
        return True
    if sample_rate <= 0.0:
        return False
    digest = hashlib.md5(url.encode("utf-8"), usedforsecurity=False).digest()
    bucket = int.from_bytes(digest[:8], byteorder="big", signed=False) / float(1 << 64)
    return bucket < sample_rate


def classify_url_policy(url: str, config: AuditConfig) -> URLPolicyDecision:
    if not config.url_policy_enabled:
        return URLPolicyDecision()

    keys = _query_keys(url)
    if not keys:
        return URLPolicyDecision()

    action_keys = set(config.action_param_keys)
    if keys.intersection(action_keys):
        return URLPolicyDecision(
            policy_class=NEVER_ENQUEUE,
            reason="action parameters",
            enqueue=False,
            follow_links=False,
        )

    headers_only_keys = set(config.headers_only_param_keys)
    if keys.intersection(headers_only_keys):
        return URLPolicyDecision(
            policy_class=FETCH_HEADERS_ONLY,
            reason="headers-only parameters",
            enqueue=True,
            follow_links=False,
            fetch_headers_only=True,
        )

    diagnostic_keys = set(config.diagnostic_param_keys)
    if keys.intersection(diagnostic_keys):
        return URLPolicyDecision(
            policy_class=CRAWL_ONCE_DIAGNOSTIC,
            reason="diagnostic parameters",
            enqueue=True,
            follow_links=False,
        )

    canonical_candidate_keys = set(config.canonical_candidate_param_keys)
    if keys.intersection(canonical_candidate_keys):
        return URLPolicyDecision(
            policy_class=CANONICAL_CANDIDATE_DUPLICATE,
            reason="canonical-candidate parameters",
            enqueue=True,
            follow_links=False,
        )

    faceted_keys = set(config.faceted_param_keys)
    query_param_count = _query_param_count(url)
    if query_param_count >= config.faceted_query_param_threshold and keys.intersection(faceted_keys):
        sampled_in = _is_sampled_in(url, config.faceted_sample_rate)
        return URLPolicyDecision(
            policy_class=CRAWL_SAMPLED,
            reason="faceted parameters",
            enqueue=sampled_in,
            follow_links=sampled_in,
        )

    return URLPolicyDecision()
