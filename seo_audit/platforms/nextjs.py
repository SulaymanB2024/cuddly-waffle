from __future__ import annotations

from urllib.parse import urlsplit

from seo_audit.platforms.models import PlatformDetection


def detect_nextjs(html: str, headers: dict[str, str], url: str) -> PlatformDetection | None:
    html_l = (html or "").lower()
    powered = str(headers.get("x-powered-by") or "").lower()
    server = str(headers.get("server") or "").lower()
    host = (urlsplit(url).hostname or "").lower()

    score = 0
    signals: dict[str, str | int | bool] = {}

    if "__next_data__" in html_l:
        score += 45
        signals["next_data"] = True
    if "_next/static" in html_l or "next-route-announcer" in html_l:
        score += 30
        signals["next_static"] = True
    if "next.js" in powered:
        score += 35
        signals["powered_by"] = powered[:80]
    if "vercel" in server or host.endswith(".vercel.app"):
        score += 15
        signals["hosting_hint"] = True

    if score < 35:
        return None

    path = (urlsplit(url).path or "").lower()
    template_hint = "app_route" if path.startswith("/app") else "page_route"

    return PlatformDetection(
        platform="nextjs",
        confidence=min(99, score),
        signals=signals,
        template_hint=template_hint,
    )
