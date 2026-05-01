from __future__ import annotations

from seo_audit.platforms.cloudflare import detect_cloudflare
from seo_audit.platforms.models import PlatformDetection, choose_stronger
from seo_audit.platforms.nextjs import detect_nextjs
from seo_audit.platforms.shopify import detect_shopify
from seo_audit.platforms.wix import detect_wix
from seo_audit.platforms.wordpress import detect_wordpress


def detect_platform_stack(html: str, headers: dict[str, str], url: str) -> PlatformDetection | None:
    candidate: PlatformDetection | None = None
    for detector in (
        detect_shopify,
        detect_wix,
        detect_wordpress,
        detect_nextjs,
        detect_cloudflare,
    ):
        candidate = choose_stronger(candidate, detector(html, headers, url))
    return candidate


__all__ = ["PlatformDetection", "detect_platform_stack"]
