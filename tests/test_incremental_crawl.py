import hashlib
import json
from pathlib import Path

from seo_audit.config import AuditConfig
from seo_audit.crawler import CURRENT_EXTRACTOR_VERSION, CURRENT_SCHEMA_RULE_VERSION, PAGE_EXTRACT_ARTIFACT_TYPE, crawl_site
from seo_audit.http_utils import HTTPResponse
from seo_audit.models import ArtifactCacheRecord, URLStateRecord
from seo_audit.scoring_policy import CURRENT_SCORE_VERSION
from seo_audit.storage import Storage


def _cache_version() -> str:
    return f"extract:{CURRENT_EXTRACTOR_VERSION}|schema:{CURRENT_SCHEMA_RULE_VERSION}|scoring:{CURRENT_SCORE_VERSION}"


def _artifact_sha(body_sha256: str) -> str:
    token = f"{body_sha256}|{PAGE_EXTRACT_ARTIFACT_TYPE}|{_cache_version()}"
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _seed_cached_page(storage: Storage, url: str, body: bytes) -> str:
    body_sha = hashlib.sha256(body).hexdigest()
    storage.ensure_body_blob(body_sha, body, content_encoding="identity")
    storage.upsert_url_states(
        [
            URLStateRecord(
                url_key=hashlib.sha256(url.encode("utf-8")).hexdigest(),
                normalized_url=url,
                last_final_url=url,
                etag='"etag-1"',
                last_modified="Wed, 21 Oct 2015 07:28:00 GMT",
                last_status_code=200,
                last_content_type="text/html",
                last_body_sha256=body_sha,
                last_extracted_sha256="",
                last_fetched_at="2026-01-01T00:00:00+00:00",
                last_seen_run_id="prev-run",
                not_modified_streak=1,
            )
        ]
    )
    storage.upsert_artifact_cache(
        [
            ArtifactCacheRecord(
                artifact_sha256=_artifact_sha(body_sha),
                body_sha256=body_sha,
                extractor_version=_cache_version(),
                artifact_type=PAGE_EXTRACT_ARTIFACT_TYPE,
                artifact_json=json.dumps(
                    {
                        "extract_data": {
                            "title": "Cached title",
                            "h1": "Cached h1",
                            "word_count": 180,
                            "content_hash": "f" * 64,
                            "anchors": [{"href": "/about", "anchor_text": "About", "nofollow": False, "dom_region": "main"}],
                            "h1_count": 1,
                        },
                        "shell": {
                            "shell_score": 12,
                            "likely_js_shell": 0,
                            "framework_guess": "",
                            "shell_signals_json": "{}",
                            "render_reason": "",
                        },
                        "platform": {
                            "platform_family": "",
                            "platform_confidence": 0,
                            "platform_signals_json": "{}",
                            "platform_template_hint": "",
                        },
                    },
                    sort_keys=True,
                ),
            )
        ]
    )
    return body_sha


def test_incremental_304_reuses_cached_artifacts(monkeypatch, tmp_path: Path) -> None:
    db = tmp_path / "audit.sqlite"
    storage = Storage(db)
    storage.init_db()

    url = "https://example.com/"
    body = b"<html><head><title>Cached title</title></head><body><h1>Cached h1</h1></body></html>"
    _seed_cached_page(storage, url, body)

    observed_headers: dict[str, str] = {}

    def fake_get(target_url: str, timeout: float, headers: dict[str, str], **kwargs) -> HTTPResponse:
        del timeout, kwargs
        observed_headers.update(headers)
        return HTTPResponse(
            url=target_url,
            status_code=304,
            headers={"etag": '"etag-1"', "content-type": "text/html"},
            content=b"",
            not_modified=True,
            redirect_chain=[target_url],
        )

    monkeypatch.setattr("seo_audit.crawler.http_get", fake_get)

    config = AuditConfig(
        domain="https://example.com",
        output_dir=tmp_path,
        max_pages=1,
        respect_robots=False,
        incremental_crawl_enabled=True,
    )
    result = crawl_site(config, "run-1", robots_data=None, start_urls=[url], storage=storage)

    assert observed_headers.get("If-None-Match") == '"etag-1"'
    assert observed_headers.get("If-Modified-Since") == "Wed, 21 Oct 2015 07:28:00 GMT"
    assert result.incremental_stats["not_modified"] == 1
    assert result.incremental_stats["reused_from_cache"] == 1
    assert len(result.pages) == 1
    assert result.pages[0].title == "Cached title"
    assert result.pages[0].changed_since_last_run == 0

    storage.close()


def test_incremental_unchanged_200_skips_reparse_with_matching_artifact(monkeypatch, tmp_path: Path) -> None:
    db = tmp_path / "audit.sqlite"
    storage = Storage(db)
    storage.init_db()

    url = "https://example.com/"
    body = b"<html><head><title>Cached title</title></head><body><h1>Cached h1</h1></body></html>"
    _seed_cached_page(storage, url, body)

    def fake_get(target_url: str, timeout: float, headers: dict[str, str], **kwargs) -> HTTPResponse:
        del timeout, headers, kwargs
        return HTTPResponse(
            url=target_url,
            status_code=200,
            headers={"content-type": "text/html", "etag": '"etag-1"'},
            content=body,
            redirect_chain=[target_url],
        )

    def fail_extract(*args, **kwargs):
        del args, kwargs
        raise AssertionError("extract_page_data should be skipped when unchanged cache artifact is valid")

    monkeypatch.setattr("seo_audit.crawler.http_get", fake_get)
    monkeypatch.setattr("seo_audit.crawler.extract_page_data", fail_extract)

    config = AuditConfig(
        domain="https://example.com",
        output_dir=tmp_path,
        max_pages=1,
        respect_robots=False,
        incremental_crawl_enabled=True,
    )
    result = crawl_site(config, "run-1", robots_data=None, start_urls=[url], storage=storage)

    assert result.incremental_stats["fetched"] == 1
    assert result.incremental_stats["reused_from_cache"] == 1
    assert result.incremental_stats["reparsed"] == 0
    assert result.pages[0].changed_since_last_run == 0

    storage.close()


def test_incremental_version_invalidation_reparses_unchanged_body(monkeypatch, tmp_path: Path) -> None:
    db = tmp_path / "audit.sqlite"
    storage = Storage(db)
    storage.init_db()

    url = "https://example.com/"
    body = b"<html><head><title>Fresh title</title></head><body><h1>Fresh h1</h1></body></html>"
    body_sha = hashlib.sha256(body).hexdigest()
    storage.ensure_body_blob(body_sha, body, content_encoding="identity")
    storage.upsert_url_states(
        [
            URLStateRecord(
                url_key=hashlib.sha256(url.encode("utf-8")).hexdigest(),
                normalized_url=url,
                last_final_url=url,
                etag='"etag-1"',
                last_modified="Wed, 21 Oct 2015 07:28:00 GMT",
                last_status_code=200,
                last_content_type="text/html",
                last_body_sha256=body_sha,
                last_extracted_sha256="",
                last_fetched_at="2026-01-01T00:00:00+00:00",
                last_seen_run_id="prev-run",
                not_modified_streak=4,
            )
        ]
    )

    # Store stale artifact payload with an older version token to force invalidation.
    stale_version = "extract:1.0.0|schema:1.0.0|scoring:1.0.0"
    stale_token = f"{body_sha}|{PAGE_EXTRACT_ARTIFACT_TYPE}|{stale_version}"
    storage.upsert_artifact_cache(
        [
            ArtifactCacheRecord(
                artifact_sha256=hashlib.sha256(stale_token.encode("utf-8")).hexdigest(),
                body_sha256=body_sha,
                extractor_version=stale_version,
                artifact_type=PAGE_EXTRACT_ARTIFACT_TYPE,
                artifact_json=json.dumps({"extract_data": {"title": "stale", "anchors": []}}, sort_keys=True),
            )
        ]
    )

    def fake_get(target_url: str, timeout: float, headers: dict[str, str], **kwargs) -> HTTPResponse:
        del timeout, headers, kwargs
        return HTTPResponse(
            url=target_url,
            status_code=200,
            headers={"content-type": "text/html", "etag": '"etag-1"'},
            content=body,
            redirect_chain=[target_url],
        )

    from seo_audit.extract import extract_page_data as real_extract_page_data

    calls = {"count": 0}

    def counting_extract(*args, **kwargs):
        calls["count"] += 1
        return real_extract_page_data(*args, **kwargs)

    monkeypatch.setattr("seo_audit.crawler.http_get", fake_get)
    monkeypatch.setattr("seo_audit.crawler.extract_page_data", counting_extract)

    config = AuditConfig(
        domain="https://example.com",
        output_dir=tmp_path,
        max_pages=1,
        respect_robots=False,
        incremental_crawl_enabled=True,
    )
    result = crawl_site(config, "run-1", robots_data=None, start_urls=[url], storage=storage)

    assert calls["count"] == 1
    assert result.incremental_stats["reused_from_cache"] == 0
    assert result.incremental_stats["reparsed"] == 1
    assert result.pages[0].changed_since_last_run == 0

    storage.close()
