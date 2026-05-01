from __future__ import annotations

from dataclasses import dataclass, field
import json
import time
from typing import Any
from urllib.parse import urlsplit

from seo_audit.extract import extract_page_data


@dataclass(slots=True)
class RenderResult:
    final_url: str
    title: str
    canonical: str
    h1s: list[str]
    h1_count: int
    word_count: int
    links: list[dict]
    canonical_urls: list[str] = field(default_factory=list)
    canonical_count: int = 0
    hreflang_links: list[dict] = field(default_factory=list)
    meta_description: str = ""
    title_inventory: list[str] = field(default_factory=list)
    meta_description_inventory: list[str] = field(default_factory=list)
    content_hash: str = ""
    meta_robots: str = ""
    head_inventory_json: str = "{}"
    effective_robots_json: str = "{}"
    is_noindex: int = 0
    is_nofollow: int = 0
    has_nosnippet_directive: int = 0
    max_snippet_directive: str = ""
    max_image_preview_directive: str = ""
    max_video_preview_directive: str = ""
    data_nosnippet_count: int = 0
    network_request_urls: list[str] = field(default_factory=list)
    api_endpoint_urls: list[str] = field(default_factory=list)
    schema_graph_json: str = "[]"
    schema_validation_json: str = "{}"
    schema_validation_score: int = 0
    wait_profile: str = ""
    interaction_count: int = 0
    action_recipe: str = "none"


def _json_list(raw: object) -> list[object]:
    if isinstance(raw, list):
        return list(raw)
    text = str(raw or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def choose_render_sample(pages: list[dict], max_render_pages: int) -> list[dict]:
    if max_render_pages <= 0 or not pages:
        return []

    by_url = {p.get("normalized_url", ""): p for p in pages}
    selected: list[dict] = []

    def add(page: dict | None) -> None:
        if page and page not in selected:
            selected.append(page)

    add(next((p for p in pages if p.get("page_type") == "homepage"), pages[0]))
    add(next((p for p in sorted(pages, key=lambda x: x.get("internal_links_out", 0), reverse=True) if p.get("page_type") in {"service", "industry"}), None))
    add(next((p for p in pages if p.get("page_type") == "article"), None))
    add(next((p for p in sorted(pages, key=lambda x: x.get("word_count", 0)) if p.get("word_count", 0) < 120), None))

    for page in sorted(pages, key=lambda p: (-(p.get("internal_links_out", 0)), p.get("word_count", 0))):
        add(by_url.get(page.get("normalized_url", "")))
        if len(selected) >= max_render_pages:
            break
    return selected[:max_render_pages]


def _looks_like_api_url(url: str) -> bool:
    parsed = urlsplit(url)
    path = (parsed.path or "").lower()
    query = (parsed.query or "").lower()
    if any(marker in path for marker in ("/api/", "/graphql", "/wp-json", ".json")):
        return True
    return any(marker in query for marker in ("format=json", "output=json", "graphql"))


def _extract_api_endpoints(request_urls: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for url in request_urls:
        if not _looks_like_api_url(url):
            continue
        if url in seen:
            continue
        seen.add(url)
        result.append(url)
        if len(result) >= 40:
            break
    return result


def _extract_render_result(
    html: str,
    final_url: str,
    *,
    crawler_token: str = "googlebot",
    network_request_urls: list[str] | None = None,
    wait_profile: str = "",
    interaction_count: int = 0,
    action_recipe: str = "none",
) -> RenderResult:
    data = extract_page_data(
        html,
        final_url,
        200,
        "text/html",
        {},
        header_lists={},
        crawler_token=crawler_token,
    )
    requests = list(network_request_urls or [])
    canonical_urls = [str(value).strip() for value in _json_list(data.get("raw_canonical_urls_json") or data.get("canonical_urls_json")) if str(value).strip()]
    hreflang_links = [
        item
        for item in _json_list(data.get("raw_hreflang_links_json") or data.get("hreflang_links_json"))
        if isinstance(item, dict)
    ]
    canonical = str(data.get("canonical_url", "") or "").strip()
    if not canonical and canonical_urls:
        canonical = canonical_urls[0]
    return RenderResult(
        final_url=final_url,
        title=data.get("title", ""),
        canonical=canonical,
        canonical_urls=canonical_urls,
        canonical_count=int(data.get("canonical_count", len(canonical_urls)) or len(canonical_urls)),
        hreflang_links=hreflang_links,
        h1s=list(data.get("h1s", [])),
        h1_count=int(data.get("h1_count", 0)),
        word_count=data.get("word_count", 0),
        links=list(data.get("anchors", [])),
        meta_description=data.get("meta_description", ""),
        title_inventory=[str(value).strip() for value in _json_list(data.get("title_inventory_json")) if str(value).strip()],
        meta_description_inventory=[
            str(value).strip() for value in _json_list(data.get("meta_description_inventory_json")) if str(value).strip()
        ],
        content_hash=str(data.get("raw_content_hash") or data.get("content_hash") or ""),
        meta_robots=data.get("meta_robots", ""),
        head_inventory_json=str(data.get("head_inventory_json", "{}") or "{}"),
        effective_robots_json=str(data.get("effective_robots_json", "{}") or "{}"),
        is_noindex=int(data.get("is_noindex", 0) or 0),
        is_nofollow=int(data.get("is_nofollow", 0) or 0),
        has_nosnippet_directive=int(data.get("has_nosnippet_directive", 0) or 0),
        max_snippet_directive=str(data.get("max_snippet_directive", "") or ""),
        max_image_preview_directive=str(data.get("max_image_preview_directive", "") or ""),
        max_video_preview_directive=str(data.get("max_video_preview_directive", "") or ""),
        data_nosnippet_count=int(data.get("data_nosnippet_count", 0) or 0),
        network_request_urls=requests,
        api_endpoint_urls=_extract_api_endpoints(requests),
        schema_graph_json=str(data.get("schema_graph_json", "[]") or "[]"),
        schema_validation_json=str(data.get("schema_validation_json", "{}") or "{}"),
        schema_validation_score=int(data.get("schema_validation_score", 0) or 0),
        wait_profile=wait_profile,
        interaction_count=interaction_count,
        action_recipe=action_recipe,
    )


def score_render_escalation(page: dict) -> tuple[int, list[str]]:
    status_code = int(page.get("status_code") or 0)
    content_type = str(page.get("content_type") or "").lower()
    is_html_like = "html" in content_type or bool(page.get("title") or page.get("h1") or page.get("word_count"))
    if status_code < 200 or status_code >= 400 or not is_html_like:
        return 0, ["non_actionable"]

    score = 0
    reasons: list[str] = []
    shell_score = int(page.get("shell_score") or 0)
    likely_shell = int(page.get("likely_js_shell") or 0)
    shell_state = str(page.get("shell_state") or "").strip()
    shell_possible = shell_state in {"raw_shell_possible", "raw_shell_confirmed_after_render"}
    framework_guess = str(page.get("framework_guess") or "").strip()
    raw_word_count = int(page.get("word_count") or 0)
    internal_links = int(page.get("internal_links_out") or 0)
    page_type = str(page.get("page_type") or "").lower()

    if likely_shell or shell_score >= 45 or shell_possible:
        score += 40
        reasons.append("shell_like_raw_html")
    elif shell_score >= 30:
        score += 20
        reasons.append("shell_signals_present")

    if framework_guess:
        score += 20
        reasons.append("framework_detected")
    if raw_word_count < 120:
        score += 15
        reasons.append("low_raw_text")
    if internal_links <= 1:
        score += 12
        reasons.append("sparse_raw_links")
    if not str(page.get("title") or "").strip():
        score += 8
        reasons.append("missing_title")
    raw_h1 = int(page.get("h1_count") or int(bool(page.get("h1"))))
    if raw_h1 == 0:
        score += 8
        reasons.append("missing_h1")
    if not str(page.get("canonical_url") or "").strip():
        score += 6
        reasons.append("missing_canonical")
    if page_type in {"homepage", "service", "contact", "location"}:
        score += 10
        reasons.append("high_value_page")

    return min(100, score), reasons


class PlaywrightRenderer:
    def __init__(
        self,
        timeout: float = 15.0,
        user_agent: str | None = None,
        crawler_token: str = "googlebot",
        *,
        wait_ladder_ms: tuple[int, ...] | list[int] | None = None,
        interaction_budget: int = 0,
        context_reuse_limit: int = 25,
    ) -> None:
        self.timeout = timeout
        self.user_agent = user_agent
        self.crawler_token = crawler_token
        self.wait_ladder_ms = tuple(int(max(0, ms)) for ms in (wait_ladder_ms or (500, 1200, 2500)))
        self.interaction_budget = max(0, int(interaction_budget))
        self.context_reuse_limit = max(1, int(context_reuse_limit))
        self._init_error: str | None = None
        self._playwright_ctx: Any | None = None
        self._playwright: Any | None = None
        self._browser: Any | None = None
        self._context: Any | None = None
        self._context_render_count = 0
        self._playwright_error: type[Exception] = Exception

    def __enter__(self) -> "PlaywrightRenderer":
        try:
            from playwright.sync_api import Error as PlaywrightError, sync_playwright
        except Exception as exc:
            self._init_error = f"playwright import failed: {exc}"
            return self

        self._playwright_error = PlaywrightError
        try:
            self._playwright_ctx = sync_playwright()
            playwright = self._playwright_ctx.__enter__()
            self._playwright = playwright
            self._browser = playwright.chromium.launch(headless=True)
        except Exception as exc:
            self._init_error = f"playwright runtime error: {exc}"
            self._shutdown()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # type: ignore[override]
        self._shutdown()
        return False

    def _shutdown(self) -> None:
        if self._context is not None:
            try:
                self._context.close()
            except Exception:
                pass
            self._context = None

        if self._browser is not None:
            try:
                self._browser.close()
            except Exception:
                pass
            self._browser = None

        if self._playwright_ctx is not None:
            try:
                self._playwright_ctx.__exit__(None, None, None)
            except Exception:
                pass
            self._playwright_ctx = None
            self._playwright = None

    def _ensure_context(self) -> Any:
        if self._browser is None:
            raise RuntimeError("browser unavailable")
        if self._context is None or self._context_render_count >= self.context_reuse_limit:
            if self._context is not None:
                try:
                    self._context.close()
                except Exception:
                    pass
            self._context = self._browser.new_context(user_agent=self.user_agent) if self.user_agent else self._browser.new_context()
            self._context_render_count = 0
        return self._context

    def _wait_ladder(self, page: Any) -> str:
        steps: list[str] = []
        if hasattr(page, "wait_for_load_state"):
            for state, timeout_ms in zip(("domcontentloaded", "load"), self.wait_ladder_ms[:2], strict=False):
                try:
                    page.wait_for_load_state(state, timeout=min(timeout_ms, int(self.timeout * 1000)))
                    steps.append(state)
                except Exception:
                    steps.append(f"{state}:timeout")

        def snapshot_state() -> dict[str, object]:
            if not hasattr(page, "evaluate"):
                return {
                    "ready_state": "unknown",
                    "text_len": 0,
                    "anchor_count": 0,
                    "title_len": 0,
                    "h1_count": 0,
                }

            try:
                snapshot = page.evaluate(
                    """() => {
                        const bodyText = document.body ? document.body.innerText : "";
                        const title = document.title || "";
                        return {
                            ready_state: document.readyState || "unknown",
                            text_len: bodyText.length,
                            anchor_count: document.querySelectorAll("a[href]").length,
                            title_len: title.length,
                            h1_count: document.querySelectorAll("h1").length,
                        };
                    }"""
                )
            except Exception:
                snapshot = None

            if isinstance(snapshot, dict):
                return {
                    "ready_state": str(snapshot.get("ready_state") or "unknown"),
                    "text_len": int(snapshot.get("text_len") or 0),
                    "anchor_count": int(snapshot.get("anchor_count") or 0),
                    "title_len": int(snapshot.get("title_len") or 0),
                    "h1_count": int(snapshot.get("h1_count") or 0),
                }

            # Test doubles may return a scalar from evaluate; treat it as text length.
            fallback_text_len = int(snapshot or 0) if isinstance(snapshot, (int, float)) else 0
            return {
                "ready_state": "unknown",
                "text_len": fallback_text_len,
                "anchor_count": 0,
                "title_len": 0,
                "h1_count": 0,
            }

        stable_rounds = 0
        completeness_rounds = 0
        last_signature: tuple[str, int, int, int, int] | None = None
        stable_deadline = time.time() + min(4.0, self.timeout)
        settle_ms = self.wait_ladder_ms[-1] if self.wait_ladder_ms else 500
        while time.time() < stable_deadline and (stable_rounds < 2 or completeness_rounds < 1):
            snapshot = snapshot_state()
            signature = (
                str(snapshot["ready_state"]),
                int(snapshot["text_len"]),
                int(snapshot["anchor_count"]),
                int(snapshot["title_len"]),
                int(snapshot["h1_count"]),
            )

            if signature == last_signature:
                stable_rounds += 1
            else:
                stable_rounds = 0
            last_signature = signature

            text_len = int(snapshot["text_len"])
            anchor_count = int(snapshot["anchor_count"])
            title_len = int(snapshot["title_len"])
            h1_count = int(snapshot["h1_count"])
            if text_len >= 120 or anchor_count >= 3 or title_len > 0 or h1_count > 0:
                completeness_rounds += 1

            if hasattr(page, "wait_for_timeout"):
                page.wait_for_timeout(max(150, min(350, settle_ms // 6)))
            else:
                time.sleep(0.2)

        steps.append(f"stability:{stable_rounds}")
        steps.append(f"completeness:{'pass' if completeness_rounds > 0 else 'soft'}")
        return ";".join(steps)

    def _run_interaction_recipe(self, page: Any) -> tuple[int, str]:
        if self.interaction_budget <= 0 or not hasattr(page, "query_selector"):
            return 0, "none"

        selectors = (
            "button:has-text('Accept')",
            "button:has-text('I Agree')",
            "button:has-text('Load more')",
            "button:has-text('Show more')",
            "[aria-label*='accept' i]",
            "[data-testid*='accept' i]",
        )

        clicked = 0
        for selector in selectors:
            if clicked >= self.interaction_budget:
                break
            try:
                element = page.query_selector(selector)
                if element is None:
                    continue
                element.click(timeout=1000)
                clicked += 1
                if hasattr(page, "wait_for_timeout"):
                    page.wait_for_timeout(220)
            except Exception:
                continue

        if clicked <= 0:
            return 0, "none"
        return clicked, "bounded_common_ui"

    def render(self, url: str) -> tuple[RenderResult | None, str | None]:
        if self._init_error:
            return None, self._init_error
        if self._browser is None:
            return None, "playwright runtime error: browser unavailable"

        page = None
        network_urls: list[str] = []
        network_seen: set[str] = set()
        wait_profile = ""
        interaction_count = 0
        action_recipe = "none"
        try:
            context = self._ensure_context()
            page = context.new_page()
            self._context_render_count += 1

            if hasattr(page, "on"):
                def remember_request(request: Any) -> None:
                    request_url = str(getattr(request, "url", "") or "").strip()
                    if not request_url or request_url in network_seen:
                        return
                    network_seen.add(request_url)
                    network_urls.append(request_url)
                    if len(network_urls) > 120:
                        network_urls.pop(0)

                try:
                    page.on("request", remember_request)
                except Exception:
                    pass

            if hasattr(page, "route"):
                page.route(
                    "**/*",
                    lambda route: route.abort()
                    if route.request.resource_type in {"image", "font", "media"}
                    or any(t in route.request.url.lower() for t in ("google-analytics", "gtm.js", "doubleclick", "segment.io", "hotjar"))
                    else route.continue_(),
                )
            page.goto(url, timeout=int(self.timeout * 1000), wait_until="domcontentloaded")
            wait_profile = self._wait_ladder(page)

            interaction_count, action_recipe = self._run_interaction_recipe(page)
            if interaction_count > 0:
                wait_profile = ";".join([wait_profile, "post_interaction"]).strip(";")
                _ = self._wait_ladder(page)

            html = page.content()
            final_url = getattr(page, "url", url)
        except self._playwright_error as exc:
            return None, f"playwright runtime error: {exc}"
        except Exception as exc:
            return None, f"render failure: {exc}"
        finally:
            if page is not None:
                try:
                    page.close()
                except Exception:
                    pass

        return (
            _extract_render_result(
                html,
                final_url,
                crawler_token=self.crawler_token,
                network_request_urls=network_urls,
                wait_profile=wait_profile,
                interaction_count=interaction_count,
                action_recipe=action_recipe,
            ),
            None,
        )


def render_url(
    url: str,
    timeout: float = 15.0,
    user_agent: str | None = None,
    crawler_token: str = "googlebot",
) -> tuple[RenderResult | None, str | None]:
    result: tuple[RenderResult | None, str | None] = (None, "render failure: renderer unavailable")
    with PlaywrightRenderer(timeout=timeout, user_agent=user_agent, crawler_token=crawler_token) as renderer:
        result = renderer.render(url)
    return result


def compute_render_gap(raw: dict, rendered: RenderResult | None) -> tuple[int, str]:
    if rendered is None:
        return 0, "render not available"
    gap = 0
    reasons: list[str] = []
    raw_word_count = int(raw.get("raw_text_len") or raw.get("word_count") or 0)
    if raw_word_count < 100 and rendered.word_count > 200:
        gap += 45
        reasons.append("raw thin but rendered rich")
    raw_title = str(raw.get("raw_title") or raw.get("title") or "").strip()
    if raw_title != (rendered.title or "").strip():
        gap += 20
        reasons.append("title mismatch")
    raw_canonical = str(raw.get("raw_canonical") or raw.get("canonical_url") or "").strip()
    if raw_canonical and raw_canonical != (rendered.canonical or "").strip():
        gap += 15
        reasons.append("canonical mismatch")
    if int(raw.get("h1_count", int(bool(raw.get("h1")))) or 0) != int(rendered.h1_count or 0):
        gap += 15
        reasons.append("h1 mismatch")
    if len(rendered.links) > raw.get("internal_links_out", 0) * 2 + 10:
        gap += 15
        reasons.append("rendered links much richer")

    if not reasons:
        reasons.append("raw and rendered signals are aligned")
    return min(100, gap), "; ".join(reasons)
