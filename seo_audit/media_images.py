from __future__ import annotations

from pathlib import PurePosixPath
import re
from urllib.parse import urlsplit

from seo_audit.url_utils import normalize_url


def _filename_quality(src: str) -> str:
    path = PurePosixPath(urlsplit(src).path)
    name = path.name.lower()
    if not name:
        return "missing"
    if any(token in name for token in ("img", "image", "photo", "dsc", "screenshot")):
        return "generic"
    if "-" in name or "_" in name:
        return "descriptive"
    return "basic"


def _safe_int(raw: str) -> int | None:
    value = str(raw or "").strip()
    if not value:
        return None
    try:
        parsed = int(value)
    except ValueError:
        return None
    if parsed <= 0:
        return None
    return parsed


def _parse_srcset(srcset: str, *, base_url: str) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []
    for token in str(srcset or "").split(","):
        stripped = token.strip()
        if not stripped:
            continue
        parts = stripped.split()
        src = parts[0].strip() if parts else ""
        descriptor = parts[1].strip() if len(parts) > 1 else ""
        if not src:
            continue
        candidates.append(
            {
                "src": src,
                "normalized_src": normalize_url(src, base_url=base_url),
                "descriptor": descriptor,
            }
        )
    return candidates


def _alt_quality(alt: str) -> str:
    value = str(alt or "").strip().lower()
    if not value:
        return "missing"
    if len(value) < 4:
        return "weak"
    if re.fullmatch(r"(?:image|photo|picture|img|logo|hero)\s*\d*", value):
        return "generic"
    return "descriptive"


def _format_from_url(url: str) -> str:
    path = PurePosixPath(urlsplit(url).path)
    suffix = path.suffix.lower().lstrip(".")
    return suffix


def _is_supported_format(fmt: str) -> bool:
    return fmt in {"jpg", "jpeg", "png", "webp", "gif", "avif", "svg"}


def _figure_context(node) -> tuple[str, str]:
    parent = node.getparent()
    while parent is not None:
        tag = str(getattr(parent, "tag", "")).split("}")[-1].lower()
        if tag == "figure":
            caption_nodes = parent.xpath(".//figcaption")
            caption_text = ""
            if caption_nodes:
                caption_text = str(caption_nodes[0].text_content() or "").strip()
            title_text = str(parent.attrib.get("title") or "").strip()
            return caption_text[:240], title_text[:240]
        parent = parent.getparent()
    return "", ""


def _nearby_heading(node) -> str:
    current = node
    while current is not None:
        sibling = current.getprevious()
        while sibling is not None:
            tag = str(getattr(sibling, "tag", "")).split("}")[-1].lower()
            if tag in {"h1", "h2", "h3"}:
                heading_text = str(sibling.text_content() or "").strip()
                if heading_text:
                    return heading_text[:180]
            sibling = sibling.getprevious()
        current = current.getparent()
    return ""


def _schema_image_urls(schema_nodes: list[dict] | None, *, base_url: str) -> set[str]:
    urls: set[str] = set()
    for node in list(schema_nodes or []):
        if not isinstance(node, dict):
            continue
        for key in ("image", "thumbnailUrl"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                urls.add(normalize_url(value, base_url=base_url))
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, str) and item.strip():
                        urls.add(normalize_url(item, base_url=base_url))
                    elif isinstance(item, dict):
                        nested = str(item.get("url") or item.get("contentUrl") or "").strip()
                        if nested:
                            urls.add(normalize_url(nested, base_url=base_url))
            elif isinstance(value, dict):
                nested = str(value.get("url") or value.get("contentUrl") or "").strip()
                if nested:
                    urls.add(normalize_url(nested, base_url=base_url))
    return urls


def extract_image_assets(
    tree,
    *,
    base_url: str,
    schema_nodes: list[dict] | None = None,
    og_image_urls: list[str] | None = None,
) -> tuple[list[dict], dict[str, object]]:
    images: list[dict] = []

    schema_image_urls = _schema_image_urls(schema_nodes, base_url=base_url)
    normalized_og_images = {
        normalize_url(str(url or ""), base_url=base_url)
        for url in list(og_image_urls or [])
        if str(url or "").strip()
    }

    picture_sources: dict[object, list[dict[str, str]]] = {}
    for picture in tree.xpath(".//picture"):
        source_candidates: list[dict[str, str]] = []
        for source in picture.xpath(".//source"):
            srcset = str(source.attrib.get("srcset") or "")
            source_candidates.extend(_parse_srcset(srcset, base_url=base_url))
        picture_sources[picture] = source_candidates

    for idx, node in enumerate(tree.xpath(".//img"), start=1):
        src = str(node.attrib.get("src") or "").strip()
        if not src:
            continue
        normalized = normalize_url(src, base_url=base_url)
        parent_picture = node.getparent() if node.getparent() is not None and str(getattr(node.getparent(), "tag", "")).split("}")[-1].lower() == "picture" else None
        candidate_urls = _parse_srcset(str(node.attrib.get("srcset") or ""), base_url=base_url)
        if parent_picture is not None:
            candidate_urls.extend(picture_sources.get(parent_picture, []))

        alt = str(node.attrib.get("alt") or "").strip()
        width = _safe_int(str(node.attrib.get("width") or ""))
        height = _safe_int(str(node.attrib.get("height") or ""))
        loading = str(node.attrib.get("loading") or "").strip().lower()
        decoding = str(node.attrib.get("decoding") or "").strip().lower()

        parent_text = ""
        parent = node.getparent()
        if parent is not None and hasattr(parent, "text_content"):
            parent_text = str(parent.text_content() or "").strip()

        figure_caption, figure_title = _figure_context(node)
        nearby_heading = _nearby_heading(node)
        fmt = _format_from_url(normalized)

        images.append(
            {
                "source_tag": "img",
                "src": src,
                "normalized_src": normalized,
                "candidate_urls": candidate_urls,
                "alt": alt,
                "alt_quality": _alt_quality(alt),
                "width": width,
                "height": height,
                "loading": loading,
                "decoding": decoding,
                "lazy_loaded": int(loading == "lazy"),
                "filename_quality": _filename_quality(normalized),
                "format": fmt,
                "format_supported": int(_is_supported_format(fmt)),
                "nearby_text": parent_text[:240],
                "nearby_heading": nearby_heading,
                "figure_caption": figure_caption,
                "figure_title": figure_title,
                "from_og_image": int(normalized in normalized_og_images),
                "from_schema_image": int(normalized in schema_image_urls),
                "hero_candidate": int(idx == 1),
            }
        )

    existing_urls = {str(row.get("normalized_src") or "") for row in images if str(row.get("normalized_src") or "").strip()}
    for normalized in sorted(normalized_og_images):
        if normalized and normalized not in existing_urls:
            images.append(
                {
                    "source_tag": "og:image",
                    "src": normalized,
                    "normalized_src": normalized,
                    "candidate_urls": [],
                    "alt": "",
                    "alt_quality": "missing",
                    "width": None,
                    "height": None,
                    "loading": "",
                    "decoding": "",
                    "lazy_loaded": 0,
                    "filename_quality": _filename_quality(normalized),
                    "format": _format_from_url(normalized),
                    "format_supported": int(_is_supported_format(_format_from_url(normalized))),
                    "nearby_text": "",
                    "nearby_heading": "",
                    "figure_caption": "",
                    "figure_title": "",
                    "from_og_image": 1,
                    "from_schema_image": int(normalized in schema_image_urls),
                    "hero_candidate": 0,
                }
            )

    for normalized in sorted(schema_image_urls):
        if normalized and normalized not in existing_urls and normalized not in normalized_og_images:
            images.append(
                {
                    "source_tag": "schema",
                    "src": normalized,
                    "normalized_src": normalized,
                    "candidate_urls": [],
                    "alt": "",
                    "alt_quality": "missing",
                    "width": None,
                    "height": None,
                    "loading": "",
                    "decoding": "",
                    "lazy_loaded": 0,
                    "filename_quality": _filename_quality(normalized),
                    "format": _format_from_url(normalized),
                    "format_supported": int(_is_supported_format(_format_from_url(normalized))),
                    "nearby_text": "",
                    "nearby_heading": "",
                    "figure_caption": "",
                    "figure_title": "",
                    "from_og_image": 0,
                    "from_schema_image": 1,
                    "hero_candidate": 0,
                }
            )

    reuse_counts: dict[str, int] = {}
    for image in images:
        normalized = str(image.get("normalized_src") or "").strip()
        if not normalized:
            continue
        reuse_counts[normalized] = reuse_counts.get(normalized, 0) + 1
    for image in images:
        normalized = str(image.get("normalized_src") or "").strip()
        image["reused_asset_count"] = int(reuse_counts.get(normalized, 0))

    count = len(images)
    with_alt = sum(1 for image in images if str(image.get("alt") or "").strip())
    descriptive_alt = sum(1 for image in images if str(image.get("alt_quality") or "") == "descriptive")
    descriptive_file = sum(1 for image in images if image.get("filename_quality") == "descriptive")
    supported_formats = sum(1 for image in images if int(image.get("format_supported") or 0) == 1)
    near_heading = sum(1 for image in images if str(image.get("nearby_heading") or "").strip())
    reused_assets = sum(1 for image in images if int(image.get("reused_asset_count") or 0) > 1)
    large_images = sum(
        1
        for image in images
        if (int(image.get("width") or 0) >= 1200)
    )

    alt_coverage = (with_alt / count) if count else 1.0
    descriptive_alt_ratio = (descriptive_alt / count) if count else 1.0
    filename_coverage = (descriptive_file / count) if count else 1.0
    supported_format_ratio = (supported_formats / count) if count else 1.0
    contextual_ratio = (near_heading / count) if count else 0.0
    reuse_ratio = (reused_assets / count) if count else 0.0
    large_ratio = (large_images / count) if count else 0.0

    score = int(
        round(
            (alt_coverage * 25.0)
            + (descriptive_alt_ratio * 10.0)
            + (filename_coverage * 20.0)
            + (supported_format_ratio * 15.0)
            + (contextual_ratio * 10.0)
            + (large_ratio * 10.0)
            + (reuse_ratio * 10.0)
        )
    )
    score = max(0, min(100, score))

    summary = {
        "count": count,
        "alt_coverage": alt_coverage,
        "descriptive_alt_ratio": descriptive_alt_ratio,
        "descriptive_filename_ratio": filename_coverage,
        "supported_format_ratio": supported_format_ratio,
        "contextual_ratio": contextual_ratio,
        "asset_reuse_ratio": reuse_ratio,
        "large_image_ratio": large_ratio,
        "og_image_count": sum(1 for image in images if int(image.get("from_og_image") or 0) == 1),
        "schema_image_reference_count": sum(1 for image in images if int(image.get("from_schema_image") or 0) == 1),
        "discoverability_score": score,
    }
    return images, summary
