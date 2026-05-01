from __future__ import annotations

from urllib.parse import urlsplit

from seo_audit.platforms.models import PlatformDetection


def detect_wix(html: str, headers: dict[str, str], url: str) -> PlatformDetection | None:
    html_l = (html or "").lower()
    host = (urlsplit(url).hostname or "").lower()

    score = 0
    signals: dict[str, str | int | bool] = {}
    if "wix" in host:
        score += 20
        signals["host_hint"] = host[:80]
    if "static.wixstatic.com" in html_l:
        score += 45
        signals["wix_static"] = True
    if "wix-code" in html_l or "wix-data" in html_l:
        score += 20
        signals["wix_code"] = True
    if "_wixcidx" in html_l or "_wixcidx" in html_l:
        score += 25
        signals["wix_dynamic_markers"] = True

    if score < 35:
        return None

    path = (urlsplit(url).path or "").lower()
    template_hint = "dynamic_item" if "/_" in path or "/item/" in path else "page"

    return PlatformDetection(
        platform="wix",
        confidence=min(99, score),
        signals=signals,
        template_hint=template_hint,
    )
