from __future__ import annotations

from urllib.parse import urlsplit

from seo_audit.platforms.models import PlatformDetection


def detect_shopify(html: str, headers: dict[str, str], url: str) -> PlatformDetection | None:
    html_l = (html or "").lower()
    host = (urlsplit(url).hostname or "").lower()
    server = str(headers.get("server") or "").lower()

    score = 0
    signals: dict[str, str | int | bool] = {}
    if "cdn.shopify.com" in html_l:
        score += 45
        signals["cdn_shopify"] = True
    if "shopify-section" in html_l or "shopify-features" in html_l:
        score += 30
        signals["shopify_sections"] = True
    if "myshopify.com" in host:
        score += 35
        signals["myshopify_host"] = True
    if "shopify" in server:
        score += 15
        signals["server_hint"] = server[:60]

    if score < 35:
        return None

    path = (urlsplit(url).path or "").lower()
    template_hint = ""
    if "/products/" in path:
        template_hint = "product"
    elif "/collections/" in path:
        template_hint = "collection"
    elif "/blogs/" in path:
        template_hint = "blog"
    elif path in {"", "/"}:
        template_hint = "homepage"

    return PlatformDetection(
        platform="shopify",
        confidence=min(99, score),
        signals=signals,
        template_hint=template_hint,
    )
