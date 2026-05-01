from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib import robotparser
from urllib.parse import urljoin, urlsplit

from seo_audit.http_utils import http_get


GOOGLE_EXACT_PERSONA = "google_exact"
GENERIC_PERSONA = "generic"

ROBOTS_STATE_SUCCESS = "success"
ROBOTS_STATE_NO_VALID_FILE = "no_valid_file"
ROBOTS_STATE_REDIRECT_LIMIT_EXCEEDED = "redirect_limit_exceeded"
ROBOTS_STATE_REDIRECT_SCOPE_MISMATCH = "redirect_scope_mismatch"
ROBOTS_STATE_RATE_LIMITED = "rate_limited"
ROBOTS_STATE_SERVER_ERROR = "server_error"
ROBOTS_STATE_NETWORK_ERROR = "network_error"
ROBOTS_STATE_UNEXPECTED_STATUS = "unexpected_status"


@dataclass(slots=True)
class RobotsData:
    robots_url: str
    raw_text: str
    rules: list[dict] = field(default_factory=list)
    sitemaps: list[str] = field(default_factory=list)
    parser: robotparser.RobotFileParser | None = None
    fetch_state: str = ROBOTS_STATE_SUCCESS
    status_bucket: str = "2xx"
    http_status: int = 200
    redirect_hops: int = 0
    final_robots_url: str = ""
    applies_to_scope: str = ""
    persona_mode: str = GENERIC_PERSONA
    retry_after_seconds: float | None = None
    error: str = ""


def _normalized_origin_scope(url: str) -> str:
    parsed = urlsplit(str(url or ""))
    scheme = parsed.scheme.lower()
    host = (parsed.hostname or "").lower()
    port = int(parsed.port or (443 if scheme == "https" else 80 if scheme == "http" else 0))
    if not scheme or not host or port <= 0:
        return ""
    return f"{scheme}://{host}:{port}"


def _status_bucket(status_code: int) -> str:
    status = int(status_code or 0)
    if status == 429:
        return "429"
    if 200 <= status < 300:
        return "2xx"
    if 300 <= status < 400:
        return "3xx"
    if 400 <= status < 500:
        return "4xx"
    if 500 <= status < 600:
        return "5xx"
    return "other"


def _parse_retry_after(value: str | None) -> float | None:
    raw = str(value or "").strip()
    if not raw:
        return None

    try:
        seconds = float(raw)
        return seconds if seconds >= 0 else None
    except ValueError:
        pass

    try:
        dt = parsedate_to_datetime(raw)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    wait_seconds = (dt - datetime.now(timezone.utc)).total_seconds()
    return wait_seconds if wait_seconds > 0 else 0.0


def _google_redirect_limit(persona_mode: str) -> int | None:
    if str(persona_mode or "").strip().lower() == GOOGLE_EXACT_PERSONA:
        return 5
    return None


def robots_fetch_summary(robots_data: RobotsData | None) -> dict[str, object]:
    if robots_data is None:
        return {
            "state": "missing",
            "status_bucket": "missing",
            "http_status": 0,
            "redirect_hops": 0,
            "retry_after_seconds": None,
            "rules_loaded": 0,
            "sitemaps_discovered": 0,
        }

    return {
        "state": str(robots_data.fetch_state or ROBOTS_STATE_SUCCESS),
        "status_bucket": str(robots_data.status_bucket or "unknown"),
        "http_status": int(robots_data.http_status or 0),
        "redirect_hops": int(robots_data.redirect_hops or 0),
        "retry_after_seconds": robots_data.retry_after_seconds,
        "rules_loaded": len(robots_data.rules),
        "sitemaps_discovered": len(robots_data.sitemaps),
        "applies_to_scope": str(robots_data.applies_to_scope or ""),
        "final_robots_url": str(robots_data.final_robots_url or robots_data.robots_url or ""),
        "persona_mode": str(robots_data.persona_mode or GENERIC_PERSONA),
    }


def _strip_inline_comment(line: str) -> str:
    return line.split("#", 1)[0].strip()


def parse_robots_text(base_url: str, text: str) -> RobotsData:
    robots_url = urljoin(base_url, "/robots.txt")
    cleaned_lines: list[str] = []
    for raw in text.splitlines():
        cleaned = _strip_inline_comment(raw)
        if cleaned:
            cleaned_lines.append(cleaned)

    parser = robotparser.RobotFileParser()
    parser.parse(cleaned_lines)

    sitemaps: list[str] = []
    rules: list[dict] = []
    ua = "*"
    for line in cleaned_lines:
        if ":" not in line:
            continue
        k, v = [part.strip() for part in line.split(":", 1)]
        lk = k.lower()
        if lk == "user-agent":
            ua = v
            continue
        if lk == "sitemap":
            sitemaps.append(v)
        if lk in {"allow", "disallow", "crawl-delay", "sitemap"}:
            rules.append({"user_agent": ua, "directive": lk, "value": v})
    return RobotsData(
        robots_url=robots_url,
        raw_text=text,
        rules=rules,
        sitemaps=sitemaps,
        parser=parser,
        final_robots_url=robots_url,
        applies_to_scope=_normalized_origin_scope(robots_url),
    )


def _ua_token(user_agent: str) -> str:
    token = (user_agent or "").strip().split(" ", 1)[0]
    if "/" in token:
        token = token.split("/", 1)[0]
    return token.lower()


def _ua_matches(rule_user_agent: str, requested_user_agent: str) -> bool:
    if not rule_user_agent or rule_user_agent == "*":
        return True
    rule = rule_user_agent.lower()
    req = requested_user_agent.lower()
    return req == rule or req.startswith(rule) or rule in req


def resolve_crawl_delay(
    robots_data: RobotsData | None,
    user_agent: str,
    *,
    persona_mode: str = GENERIC_PERSONA,
    apply_for_google_exact: bool = False,
) -> float | None:
    if not robots_data:
        return None

    normalized_mode = str(persona_mode or robots_data.persona_mode or GENERIC_PERSONA).strip().lower()
    if normalized_mode == GOOGLE_EXACT_PERSONA and not apply_for_google_exact:
        return None

    if robots_data.fetch_state != ROBOTS_STATE_SUCCESS:
        return None

    requested_ua = _ua_token(user_agent)
    wildcard_delay: float | None = None
    specific_matches: list[tuple[int, float]] = []

    for rule in robots_data.rules:
        if rule.get("directive") != "crawl-delay":
            continue
        raw_delay = str(rule.get("value", "")).strip()
        try:
            parsed_delay = float(raw_delay)
        except ValueError:
            continue
        if parsed_delay < 0:
            continue

        rule_ua = str(rule.get("user_agent", "*")).strip().lower() or "*"
        if rule_ua == "*":
            if wildcard_delay is None:
                wildcard_delay = parsed_delay
            continue
        if _ua_matches(rule_ua, requested_ua):
            specific_matches.append((len(rule_ua), parsed_delay))

    if specific_matches:
        specific_matches.sort(key=lambda item: item[0], reverse=True)
        return specific_matches[0][1]
    return wildcard_delay


def fetch_robots(base_url: str, timeout: float, user_agent: str) -> RobotsData:
    return fetch_robots_with_persona(base_url, timeout, user_agent, persona_mode=GENERIC_PERSONA)


def fetch_robots_with_persona(
    base_url: str,
    timeout: float,
    user_agent: str,
    *,
    persona_mode: str,
) -> RobotsData:
    robots_url = urljoin(base_url, "/robots.txt")
    scope = _normalized_origin_scope(robots_url)
    headers = {"User-Agent": user_agent} if str(user_agent or "").strip() else {}
    normalized_mode = str(persona_mode or GENERIC_PERSONA).strip().lower()
    redirect_limit = _google_redirect_limit(normalized_mode)
    try:
        response = http_get(robots_url, timeout=timeout, headers=headers)
    except Exception as exc:
        return RobotsData(
            robots_url=robots_url,
            raw_text="",
            parser=None,
            fetch_state=ROBOTS_STATE_NETWORK_ERROR,
            status_bucket="network_error",
            http_status=0,
            redirect_hops=0,
            final_robots_url=robots_url,
            applies_to_scope=scope,
            persona_mode=normalized_mode,
            error=str(exc),
        )

    status = int(response.status_code or 0)
    status_bucket = _status_bucket(status)
    redirect_chain = list(getattr(response, "redirect_chain", []) or [])
    if not redirect_chain:
        redirect_chain = [robots_url, str(getattr(response, "url", robots_url) or robots_url)]
    redirect_hops = max(0, len(redirect_chain) - 1)
    final_robots_url = str(getattr(response, "url", "") or redirect_chain[-1] or robots_url)
    final_scope = _normalized_origin_scope(final_robots_url) or scope

    if redirect_limit is not None and redirect_hops > redirect_limit:
        return RobotsData(
            robots_url=robots_url,
            raw_text="",
            parser=None,
            fetch_state=ROBOTS_STATE_REDIRECT_LIMIT_EXCEEDED,
            status_bucket=status_bucket,
            http_status=status,
            redirect_hops=redirect_hops,
            final_robots_url=final_robots_url,
            applies_to_scope=scope,
            persona_mode=normalized_mode,
        )

    if redirect_limit is not None and final_scope != scope:
        return RobotsData(
            robots_url=robots_url,
            raw_text="",
            parser=None,
            fetch_state=ROBOTS_STATE_REDIRECT_SCOPE_MISMATCH,
            status_bucket=status_bucket,
            http_status=status,
            redirect_hops=redirect_hops,
            final_robots_url=final_robots_url,
            applies_to_scope=scope,
            persona_mode=normalized_mode,
        )

    if 200 <= status < 300:
        parsed = parse_robots_text(base_url, response.text)
        parsed.fetch_state = ROBOTS_STATE_SUCCESS
        parsed.status_bucket = status_bucket
        parsed.http_status = status
        parsed.redirect_hops = redirect_hops
        parsed.final_robots_url = final_robots_url
        parsed.applies_to_scope = final_scope
        parsed.persona_mode = normalized_mode
        return parsed

    if status == 429:
        return RobotsData(
            robots_url=robots_url,
            raw_text="",
            parser=None,
            fetch_state=ROBOTS_STATE_RATE_LIMITED,
            status_bucket=status_bucket,
            http_status=status,
            redirect_hops=redirect_hops,
            final_robots_url=final_robots_url,
            applies_to_scope=scope,
            persona_mode=normalized_mode,
            retry_after_seconds=_parse_retry_after(response.headers.get("retry-after")),
        )

    if 400 <= status < 500:
        return RobotsData(
            robots_url=robots_url,
            raw_text="",
            parser=None,
            fetch_state=ROBOTS_STATE_NO_VALID_FILE,
            status_bucket=status_bucket,
            http_status=status,
            redirect_hops=redirect_hops,
            final_robots_url=final_robots_url,
            applies_to_scope=scope,
            persona_mode=normalized_mode,
        )

    if 500 <= status < 600:
        return RobotsData(
            robots_url=robots_url,
            raw_text="",
            parser=None,
            fetch_state=ROBOTS_STATE_SERVER_ERROR,
            status_bucket=status_bucket,
            http_status=status,
            redirect_hops=redirect_hops,
            final_robots_url=final_robots_url,
            applies_to_scope=scope,
            persona_mode=normalized_mode,
            retry_after_seconds=_parse_retry_after(response.headers.get("retry-after")),
        )

    return RobotsData(
        robots_url=robots_url,
        raw_text="",
        parser=None,
        fetch_state=ROBOTS_STATE_UNEXPECTED_STATUS,
        status_bucket=status_bucket,
        http_status=status,
        redirect_hops=redirect_hops,
        final_robots_url=final_robots_url,
        applies_to_scope=scope,
        persona_mode=normalized_mode,
    )


def is_allowed(
    robots_data: RobotsData | None,
    user_agent: str,
    url: str,
    *,
    persona_mode: str = GENERIC_PERSONA,
) -> bool:
    if not robots_data or not robots_data.parser:
        return True

    normalized_mode = str(persona_mode or robots_data.persona_mode or GENERIC_PERSONA).strip().lower()
    if normalized_mode == GOOGLE_EXACT_PERSONA:
        robots_scope = _normalized_origin_scope(robots_data.applies_to_scope or robots_data.final_robots_url or robots_data.robots_url)
        target_scope = _normalized_origin_scope(url)
        if robots_scope and target_scope and target_scope != robots_scope:
            return True

    return robots_data.parser.can_fetch(user_agent, url)
