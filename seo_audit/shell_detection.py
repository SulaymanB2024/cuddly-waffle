from __future__ import annotations

from dataclasses import dataclass
import re

from seo_audit.extract import extract_page_data


ROOT_MARKERS = ("id=\"root\"", "id='root'", "id=\"app\"", "id='app'", "id=\"__next\"", "id=\"__nuxt\"", "data-reactroot")
FRAMEWORK_HINTS = ("react", "vite", "next", "nuxt", "webpack", "__nuxt", "__next")


@dataclass(slots=True)
class ShellClassification:
    signals: dict[str, int | str | bool | list[str]]
    shell_score: int
    likely_js_shell: bool
    shell_state: str
    reasons: list[str]


def classify_raw_html_sufficiency(html: str, final_url: str, status_code: int, content_type: str, headers: dict[str, str]) -> ShellClassification:
    extracted = extract_page_data(html, final_url, status_code, content_type, headers)
    title = (extracted.get("title") or "").strip()
    canonical = (extracted.get("canonical_url") or "").strip()
    text_len = int(extracted.get("word_count") or 0)
    h1_count = int(extracted.get("h1_count") or 0)
    anchor_count = int(extracted.get("internal_links_out") or 0) + int(extracted.get("external_links_out") or 0)
    script_count = len(re.findall(r"<script\b", html, re.IGNORECASE))
    head_script_count = len(re.findall(r"(?is)<head[^>]*>.*?<script\b", html))
    module_script_count = len(re.findall(r"<script[^>]+type=['\"]module['\"]", html, re.IGNORECASE))
    noscript_js_prompt = bool(re.search(r"<noscript[^>]*>[^<]*(enable|requires?)\s+javascript", html, re.IGNORECASE))
    empty_mount_root = bool(re.search(r"<div[^>]+id=['\"](root|app|__next|__nuxt)['\"][^>]*>\s*</div>", html, re.IGNORECASE))
    html_l = html.lower()
    root_markers = sum(1 for marker in ROOT_MARKERS if marker in html_l)
    framework_hits = sorted({hint for hint in FRAMEWORK_HINTS if hint in html_l})

    score = 0
    reasons: list[str] = []
    if text_len < 60:
        score += 30
        reasons.append("very_low_text")
    elif text_len < 120:
        score += 15
        reasons.append("low_text")
    if h1_count == 0:
        score += 15
        reasons.append("no_h1")
    if anchor_count <= 1:
        score += 15
        reasons.append("very_few_links")
    if root_markers:
        score += min(25, root_markers * 10)
        reasons.append("root_shell_markers")
    if script_count >= 6:
        score += 15
        reasons.append("many_scripts")
    if empty_mount_root and script_count >= 3:
        score += 18
        reasons.append("empty_mount_with_bootstrap_scripts")
    if noscript_js_prompt:
        score += 18
        reasons.append("noscript_requires_javascript")
    if module_script_count >= 2:
        score += 8
        reasons.append("module_bootstrap_scripts")
    if not title:
        score += 8
        reasons.append("no_title")
    if not canonical:
        score += 5
        reasons.append("no_canonical")

    likely = score >= 45
    shell_state = "raw_shell_possible" if score >= 30 else "raw_shell_unlikely"
    signals: dict[str, int | str | bool | list[str]] = {
        "title": title,
        "canonical": canonical,
        "h1_count": h1_count,
        "text_len": text_len,
        "anchor_count": anchor_count,
        "script_count": script_count,
        "head_script_count": head_script_count,
        "module_script_count": module_script_count,
        "noscript_js_prompt": noscript_js_prompt,
        "empty_mount_root": empty_mount_root,
        "root_marker_count": root_markers,
        "framework_guess": ",".join(framework_hits),
        "shell_score": min(score, 100),
        "likely_js_shell": likely,
        "shell_state": shell_state,
        "reasons": reasons,
    }
    return ShellClassification(
        signals=signals,
        shell_score=min(score, 100),
        likely_js_shell=likely,
        shell_state=shell_state,
        reasons=reasons,
    )
