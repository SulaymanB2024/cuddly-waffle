from __future__ import annotations

from seo_audit.platforms.models import PlatformDetection


def detect_cloudflare(html: str, headers: dict[str, str], _url: str) -> PlatformDetection | None:
    server = str(headers.get("server") or "").lower()
    cf_ray = str(headers.get("cf-ray") or "").strip()
    cf_cache = str(headers.get("cf-cache-status") or "").strip()

    score = 0
    signals: dict[str, str | int | bool] = {}
    if "cloudflare" in server:
        score += 55
        signals["server"] = server[:80]
    if cf_ray:
        score += 35
        signals["cf_ray"] = cf_ray[:80]
    if cf_cache:
        score += 10
        signals["cf_cache_status"] = cf_cache[:40]

    if score < 35:
        return None

    return PlatformDetection(
        platform="cloudflare",
        confidence=min(99, score),
        signals=signals,
        template_hint="edge_fronted",
    )
