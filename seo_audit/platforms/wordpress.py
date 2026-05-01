from __future__ import annotations

from urllib.parse import urlsplit

from seo_audit.platforms.models import PlatformDetection


def detect_wordpress(html: str, headers: dict[str, str], url: str) -> PlatformDetection | None:
    html_l = (html or "").lower()
    generator = str(headers.get("x-generator") or "").lower()
    host = (urlsplit(url).hostname or "").lower()

    score = 0
    signals: dict[str, str | int | bool] = {}

    if "wp-content" in html_l or "wp-includes" in html_l:
        score += 40
        signals["wp_assets"] = True
    if "wp-json" in html_l:
        score += 25
        signals["wp_json"] = True
    if "wordpress" in html_l or "wordpress" in generator:
        score += 25
        signals["wordpress_marker"] = True
    if host.startswith("wp."):
        score += 5

    if score < 35:
        return None

    path = (urlsplit(url).path or "").lower()
    template_hint = "post" if "/20" in path or "/blog/" in path else "page"

    return PlatformDetection(
        platform="wordpress",
        confidence=min(99, score),
        signals=signals,
        template_hint=template_hint,
    )
