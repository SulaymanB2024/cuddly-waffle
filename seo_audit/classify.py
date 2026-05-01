from __future__ import annotations

from dataclasses import dataclass
import json
import re
from urllib.parse import urlsplit


@dataclass(slots=True)
class ClassificationResult:
    page_type: str
    confidence: int
    evidence: list[str]


_LOCAL_BUSINESS_TYPES = {
    "localbusiness",
    "animalboardingestablishment",
    "automotivebusiness",
    "childcare",
    "dentist",
    "drycleaningorlaundry",
    "emergencyservice",
    "employmentagency",
    "entertainmentbusiness",
    "financialservice",
    "foodestablishment",
    "governmentoffice",
    "healthandbeautybusiness",
    "homeandconstructionbusiness",
    "internetcafe",
    "legalservice",
    "library",
    "lodgingbusiness",
    "medicalbusiness",
    "professionalservice",
    "radio station",
    "realestateagent",
    "selfstorage",
    "shoppingcenter",
    "sportsactivitylocation",
    "store",
    "televisionstation",
    "touristinformationcenter",
    "travelagency",
}


def _normalize_schema_types(schema_types: object) -> list[str]:
    if schema_types is None:
        return []
    if isinstance(schema_types, str):
        raw = schema_types.strip()
        if not raw:
            return []
        if raw.startswith("["):
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                parsed = []
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed if str(item).strip()]
        return [raw]
    if isinstance(schema_types, (list, tuple, set)):
        return [str(item).strip() for item in schema_types if str(item).strip()]
    return []


def has_local_business_schema(schema_types: object) -> bool:
    normalized_types = [
        item.casefold().replace(" ", "")
        for item in _normalize_schema_types(schema_types)
    ]
    for schema_type in normalized_types:
        if schema_type.endswith("localbusiness"):
            return True
        if schema_type in {value.replace(" ", "") for value in _LOCAL_BUSINESS_TYPES}:
            return True
    return False


def classify_page_result(
    url: str,
    title: str,
    h1: str,
    *,
    schema_types: object = None,
) -> ClassificationResult:
    path = (urlsplit(url).path or "/").lower().strip("/")
    first_segment = path.split("/", 1)[0] if path else ""
    text_blob = f"{title} {h1} {path}".lower()

    if not path:
        return ClassificationResult(page_type="homepage", confidence=98, evidence=["root_path"])

    scores: dict[str, int] = {
        "contact": 0,
        "about": 0,
        "legal": 0,
        "article": 0,
        "service": 0,
        "industry": 0,
        "location": 0,
        "utility": 0,
    }
    evidence: dict[str, list[str]] = {key: [] for key in scores}

    def add(page_type: str, points: int, reason: str) -> None:
        scores[page_type] += points
        evidence[page_type].append(reason)

    if first_segment in {"contact", "contact-us", "book", "support"}:
        add("contact", 4, f"path:{first_segment}")
    if first_segment in {"about", "company", "team", "story"}:
        add("about", 4, f"path:{first_segment}")
    if first_segment in {"privacy", "terms", "legal", "cookie", "compliance"}:
        add("legal", 5, f"path:{first_segment}")
    if first_segment in {"blog", "news", "article", "insights", "resources"}:
        add("article", 4, f"path:{first_segment}")
    if first_segment in {"service", "services", "solution", "solutions", "capabilities"}:
        add("service", 4, f"path:{first_segment}")
    if first_segment in {"industry", "industries", "sector", "sectors", "verticals"}:
        add("industry", 4, f"path:{first_segment}")
    if first_segment in {"location", "locations", "near", "area", "city"}:
        add("location", 4, f"path:{first_segment}")
    if first_segment in {
        "wp-admin",
        "login",
        "signin",
        "search",
        "tag",
        "category",
        "feed",
        "cart",
        "checkout",
        "account",
    }:
        add("utility", 5, f"path:{first_segment}")

    text_patterns: list[tuple[str, str, int, str]] = [
        ("contact", r"\b(contact|get in touch|call us|support)\b", 2, "text:contact"),
        ("about", r"\b(about|our story|our team|who we are)\b", 2, "text:about"),
        ("legal", r"\b(privacy|terms|cookie|legal|gdpr)\b", 3, "text:legal"),
        ("article", r"\b(article|insight|news|blog)\b", 2, "text:article"),
        ("service", r"\b(service|services|what we do|solution)\b", 2, "text:service"),
        ("industry", r"\b(industry|industries|sector|vertical)\b", 2, "text:industry"),
        ("location", r"\b(location|near me|city|state|area)\b", 2, "text:location"),
    ]
    for page_type, pattern, points, reason in text_patterns:
        if re.search(pattern, text_blob):
            add(page_type, points, reason)

    if re.search(r"\b20\d{2}\b", path) and "article" in scores:
        add("article", 1, "path:year_like_segment")

    schema_values = _normalize_schema_types(schema_types)
    schema_lower = [value.casefold() for value in schema_values]
    if any(value.endswith("article") or value.endswith("blogposting") for value in schema_lower):
        add("article", 2, "schema:article_like")
    if any(value.endswith("service") for value in schema_lower):
        add("service", 2, "schema:service")
    if has_local_business_schema(schema_values):
        add("location", 2, "schema:local_business")
    if any(value.endswith("contactpage") for value in schema_lower):
        add("contact", 2, "schema:contact_page")
    if any(value.endswith("aboutpage") for value in schema_lower):
        add("about", 2, "schema:about_page")

    ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    top_type, top_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else 0
    margin = top_score - second_score

    if top_score < 3:
        return ClassificationResult(page_type="other", confidence=35, evidence=["insufficient_evidence"])

    if margin <= 1 and top_type not in {"legal", "utility"}:
        return ClassificationResult(
            page_type="other",
            confidence=45,
            evidence=["ambiguous_evidence", f"candidate:{top_type}"],
        )

    confidence = min(95, 45 + (top_score * 8) + (max(0, margin - 1) * 5))
    if confidence < 60 and top_type in {"article", "service", "industry", "location"}:
        return ClassificationResult(
            page_type="other",
            confidence=confidence,
            evidence=["low_confidence_candidate", f"candidate:{top_type}"],
        )

    return ClassificationResult(
        page_type=top_type,
        confidence=max(30, min(99, confidence)),
        evidence=evidence[top_type][:6] or ["lexical_match"],
    )


def classify_page(url: str, title: str, h1: str, *, schema_types: object = None) -> str:
    return classify_page_result(url, title, h1, schema_types=schema_types).page_type
