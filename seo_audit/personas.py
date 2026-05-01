from __future__ import annotations

from dataclasses import dataclass, replace


@dataclass(frozen=True)
class CrawlPersona:
    id: str
    request_user_agent: str
    robots_token: str
    meta_robot_scope: str
    robots_mode: str = "generic"


PERSONAS: dict[str, CrawlPersona] = {
    "googlebot_smartphone": CrawlPersona(
        id="googlebot_smartphone",
        request_user_agent=(
            "Mozilla/5.0 (Linux; Android 12; Pixel 5) AppleWebKit/537.36 "
            "(KHTML, like Gecko; compatible; Googlebot/2.1; +http://www.google.com/bot.html) "
            "Chrome/120.0.0.0 Mobile Safari/537.36"
        ),
        robots_token="Googlebot",
        meta_robot_scope="googlebot",
        robots_mode="google_exact",
    ),
    "googlebot_desktop": CrawlPersona(
        id="googlebot_desktop",
        request_user_agent=(
            "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
        ),
        robots_token="Googlebot",
        meta_robot_scope="googlebot",
        robots_mode="google_exact",
    ),
    "bingbot": CrawlPersona(
        id="bingbot",
        request_user_agent=(
            "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)"
        ),
        robots_token="Bingbot",
        meta_robot_scope="generic",
        robots_mode="generic",
    ),
    "oai_searchbot": CrawlPersona(
        id="oai_searchbot",
        request_user_agent="OAI-SearchBot/1.0; +https://openai.com/searchbot",
        robots_token="OAI-SearchBot",
        meta_robot_scope="generic",
        robots_mode="generic",
    ),
    "browser_default": CrawlPersona(
        id="browser_default",
        request_user_agent="",
        robots_token="*",
        meta_robot_scope="generic",
        robots_mode="generic",
    ),
}


def resolve_crawl_persona(persona_id: str, *, user_agent_override: str = "") -> CrawlPersona:
    selected = PERSONAS.get(str(persona_id or "").strip().lower(), PERSONAS["googlebot_smartphone"])
    override = str(user_agent_override or "").strip()
    if override:
        return replace(selected, request_user_agent=override)
    return selected
