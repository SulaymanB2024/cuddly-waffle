from pathlib import Path

from seo_audit.config import AuditConfig
from seo_audit.crawler import _effective_request_delay
from seo_audit.personas import resolve_crawl_persona
from seo_audit.robots import parse_robots_text


def test_resolve_crawl_persona_override_keeps_logical_tokens() -> None:
    persona = resolve_crawl_persona("googlebot_smartphone", user_agent_override="CustomAgent/1.0")
    assert persona.request_user_agent == "CustomAgent/1.0"
    assert persona.robots_token == "Googlebot"
    assert persona.meta_robot_scope == "googlebot"


def test_resolve_crawl_persona_unknown_falls_back_to_googlebot_smartphone() -> None:
    persona = resolve_crawl_persona("not-real")
    assert persona.id == "googlebot_smartphone"


def test_effective_request_delay_uses_robots_token_not_header_ua() -> None:
    robots = parse_robots_text(
        "https://example.com",
        """
User-agent: Googlebot
Crawl-delay: 4

User-agent: CustomAgent
Crawl-delay: 1
""",
    )
    config = AuditConfig(
        domain="https://example.com",
        output_dir=Path("."),
        user_agent="CustomAgent/1.0",
        robots_user_agent_token="Googlebot",
        respect_robots=True,
    )

    assert _effective_request_delay(config, robots, config.robots_user_agent_token) == 4.0
