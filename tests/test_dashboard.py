import json
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

from seo_audit.dashboard import AuditJobManager, DashboardStore, _build_handler, create_dashboard_server
from seo_audit.job_queue import AdmissionPolicy, QueueStore
from seo_audit.models import IssueRecord, LinkRecord, PageGraphMetricsRecord, PageRecord, ScoreRecord
from seo_audit.storage import Storage


def _request_json(
    base_url: str,
    path: str,
    *,
    method: str = "GET",
    payload: dict | None = None,
    expected_status: int = 200,
) -> dict:
    body = None
    headers: dict[str, str] = {}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = Request(base_url + path, data=body, headers=headers, method=method)
    try:
        with urlopen(request, timeout=5) as response:
            assert response.status == expected_status
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        assert exc.code == expected_status
        return json.loads(exc.read().decode("utf-8"))


def _request_text(
    base_url: str,
    path: str,
    *,
    method: str = "GET",
    payload: dict | None = None,
    expected_status: int = 200,
) -> str:
    body = None
    headers: dict[str, str] = {}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = Request(base_url + path, data=body, headers=headers, method=method)
    try:
        with urlopen(request, timeout=5) as response:
            assert response.status == expected_status
            return response.read().decode("utf-8")
    except HTTPError as exc:
        assert exc.code == expected_status
        return exc.read().decode("utf-8")


def _read_json(base_url: str, path: str) -> dict:
    return _request_json(base_url, path)


def _read_text(base_url: str, path: str) -> str:
    with urlopen(base_url + path, timeout=5) as response:
        assert response.status == 200
        return response.read().decode("utf-8")


@contextmanager
def _running_server(db_path: Path):
    server = create_dashboard_server(db_path, host="127.0.0.1", port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    address = server.server_address
    host = str(address[0])
    port = int(address[1])
    base_url = f"http://{host}:{port}"
    try:
        yield base_url
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def _seed_dashboard_db(db_path: Path) -> tuple[str, str]:
    storage = Storage(db_path)
    storage.init_db()

    run_one = "run-001"
    run_two = "run-002"

    storage.insert_run(
        run_one,
        "2026-04-09T00:00:00+00:00",
        "https://example.com",
        {"run_profile": "standard"},
        "running",
    )
    storage.insert_run(
        run_two,
        "2026-04-09T00:30:00+00:00",
        "https://example.com",
        {"run_profile": "deep"},
        "running",
    )

    pages_run_one = [
        PageRecord(
            run_id=run_one,
            discovered_url="https://example.com/",
            normalized_url="https://example.com/",
            final_url="https://example.com/",
            status_code=200,
            content_type="text/html",
            title="Home",
            h1="Home",
            word_count=300,
            page_type="homepage",
            internal_links_out=8,
            external_links_out=1,
            crawl_depth=0,
            nav_linked_flag=1,
        ),
        PageRecord(
            run_id=run_one,
            discovered_url="https://example.com/service",
            normalized_url="https://example.com/service",
            final_url="https://example.com/service",
            status_code=200,
            content_type="text/html",
            title="Service",
            h1="Service",
            word_count=180,
            page_type="service",
            internal_links_out=1,
            external_links_out=0,
            is_noindex=1,
            orphan_risk_flag=1,
            crawl_depth=1,
        ),
    ]

    pages_run_two = [
        PageRecord(
            run_id=run_two,
            discovered_url="https://example.com/",
            normalized_url="https://example.com/",
            final_url="https://example.com/",
            status_code=200,
            content_type="text/html",
            title="Home",
            h1="Home",
            word_count=360,
            page_type="homepage",
            internal_links_out=10,
            external_links_out=2,
            crawl_depth=0,
            nav_linked_flag=1,
        ),
        PageRecord(
            run_id=run_two,
            discovered_url="https://example.com/service",
            normalized_url="https://example.com/service",
            final_url="https://example.com/service",
            status_code=200,
            content_type="text/html",
            title="Service",
            h1="Service",
            word_count=260,
            page_type="service",
            internal_links_out=4,
            external_links_out=0,
            is_noindex=0,
            orphan_risk_flag=0,
            crawl_depth=1,
        ),
    ]

    storage.insert_pages(pages_run_one)
    storage.insert_pages(pages_run_two)

    storage.insert_links(
        [
            LinkRecord(run_one, "https://example.com/", "https://example.com/service", "https://example.com/service", 1),
            LinkRecord(run_one, "https://example.com/service", "https://example.com/", "https://example.com/", 1),
            LinkRecord(run_two, "https://example.com/", "https://example.com/service", "https://example.com/service", 1),
        ]
    )

    storage.insert_issues(
        [
            IssueRecord(
                run_one,
                "https://example.com/service",
                "high",
                "NOINDEX",
                "Noindex detected",
                "Page has noindex signal.",
            ),
            IssueRecord(
                run_one,
                "https://example.com/service",
                "medium",
                "ORPHAN_RISK",
                "Orphan risk",
                "No meaningful inlinks detected.",
                evidence_json='{"inlinks": 0}',
            ),
            IssueRecord(
                run_two,
                "https://example.com/service",
                "low",
                "LOW_INTERNAL_LINKS",
                "Low internal links",
                "Few internal outlinks.",
            ),
        ]
    )

    storage.insert_scores(
        [
            ScoreRecord(run_one, "https://example.com/", 95, 92, 100, 90, 65, 50, 82),
            ScoreRecord(
                run_id=run_one,
                url="https://example.com/service",
                crawlability_score=60,
                onpage_score=68,
                render_risk_score=100,
                internal_linking_score=70,
                local_seo_score=55,
                performance_score=50,
                overall_score=67,
                score_version="1.1.0",
                score_profile="default",
                explanation_json=json.dumps(
                    {
                        "score_version": "1.1.0",
                        "score_profile": "default",
                        "dimensions": {"scores": {"crawlability_score": 60}},
                    },
                    sort_keys=True,
                ),
                scoring_model_version="2.0.0",
                scoring_profile="general",
                score_explanation_json=json.dumps(
                    {
                        "scoring_model_version": "2.0.0",
                        "scoring_profile": "general",
                        "dimensions": {"scores": {"crawlability_score": 60}},
                        "risk": {"score": 30, "top_risk_families": []},
                        "cap": {"score_cap": 80, "reasons": []},
                        "overall": {"overall_score": 67},
                    },
                    sort_keys=True,
                ),
            ),
            ScoreRecord(run_two, "https://example.com/", 98, 95, 100, 94, 70, 60, 86),
            ScoreRecord(run_two, "https://example.com/service", 85, 88, 100, 88, 72, 60, 82),
        ]
    )

    storage.insert_page_graph_metrics(
        [
            PageGraphMetricsRecord(
                run_id=run_one,
                url="https://example.com/",
                internal_pagerank=0.21,
                betweenness=0.02,
                closeness=0.41,
                community_id=1,
                bridge_flag=0,
            ),
            PageGraphMetricsRecord(
                run_id=run_one,
                url="https://example.com/service",
                internal_pagerank=0.01,
                betweenness=0.22,
                closeness=0.14,
                community_id=2,
                bridge_flag=1,
            ),
            PageGraphMetricsRecord(
                run_id=run_two,
                url="https://example.com/",
                internal_pagerank=0.24,
                betweenness=0.03,
                closeness=0.45,
                community_id=1,
                bridge_flag=0,
            ),
            PageGraphMetricsRecord(
                run_id=run_two,
                url="https://example.com/service",
                internal_pagerank=0.16,
                betweenness=0.05,
                closeness=0.30,
                community_id=1,
                bridge_flag=0,
            ),
        ]
    )

    storage.insert_run_events(
        run_one,
        [
            {
                "event_time": "2026-04-09T00:00:05+00:00",
                "event_type": "stage_timing",
                "stage": "crawl",
                "message": "",
                "elapsed_ms": 1200,
                "detail_json": "{}",
            },
            {
                "event_time": "2026-04-09T00:00:20+00:00",
                "event_type": "provider_summary",
                "stage": "psi",
                "message": "",
                "elapsed_ms": 0,
                "detail_json": json.dumps(
                    {
                        "attempts": 2,
                        "http_attempts": 0,
                        "retries": 0,
                        "success": 0,
                        "no_data": 0,
                        "failed_http": 0,
                        "skipped_missing_key": 2,
                        "timeouts": 0,
                        "wait_seconds": 0.0,
                    },
                    sort_keys=True,
                ),
            },
        ],
    )

    storage.insert_run_events(
        run_two,
        [
            {
                "event_time": "2026-04-09T00:30:04+00:00",
                "event_type": "stage_timing",
                "stage": "crawl",
                "message": "",
                "elapsed_ms": 1000,
                "detail_json": "{}",
            }
        ],
    )

    storage.update_run_completion(
        run_one,
        "2026-04-09T00:03:00+00:00",
        "completed",
        notes="run profile: standard",
    )
    storage.update_run_completion(
        run_two,
        "2026-04-09T00:33:00+00:00",
        "completed",
        notes="run profile: deep",
    )
    storage.close()

    return run_one, run_two


def test_dashboard_api_core_paths(tmp_path: Path) -> None:
    db_path = tmp_path / "audit.sqlite"
    run_one, run_two = _seed_dashboard_db(db_path)

    with _running_server(db_path) as base_url:
        runs_payload = _read_json(base_url, "/api/runs")
        assert runs_payload["default_run_id"] == run_two
        assert len(runs_payload["runs"]) == 2

        summary = _read_json(base_url, f"/api/summary?run_id={run_one}")
        assert summary["counts"]["pages"] == 2
        assert summary["counts"]["issues"] == 2
        assert summary["severity_counts"]["high"] == 1

        issues_payload = _read_json(base_url, f"/api/issues?run_id={run_one}&severity=high")
        assert issues_payload["total"] == 1
        assert issues_payload["rows"][0]["issue_code"] == "NOINDEX"

        pages_payload = _read_json(base_url, f"/api/pages?run_id={run_one}&page_type=service")
        assert pages_payload["total"] == 1
        assert pages_payload["rows"][0]["normalized_url"].endswith("/service")

        encoded_url = quote("https://example.com/service", safe="")
        detail_payload = _read_json(base_url, f"/api/url_detail?run_id={run_one}&url={encoded_url}")
        assert detail_payload["page"]["normalized_url"].endswith("/service")
        assert len(detail_payload["issues"]) == 2
        assert "score_explanation" in detail_payload
        assert detail_payload["score_explanation"]["scoring_model_version"] == "2.0.0"
        assert detail_payload["score_explanation"]["scoring_profile"] == "general"
        assert "dimensions" in detail_payload["score_explanation"]

        home_url = quote("https://example.com/", safe="")
        home_detail = _read_json(base_url, f"/api/url_detail?run_id={run_one}&url={home_url}")
        assert home_detail["score_explanation"]["scoring_model_version"] in {"1.0.0", "1.1.0"}
        assert "dimensions" in home_detail["score_explanation"]


def test_dashboard_compare_and_export(tmp_path: Path) -> None:
    db_path = tmp_path / "audit.sqlite"
    run_one, run_two = _seed_dashboard_db(db_path)

    with _running_server(db_path) as base_url:
        compare_payload = _read_json(
            base_url,
            f"/api/compare?left_run_id={run_one}&right_run_id={run_two}",
        )
        assert compare_payload["score_delta"]["shared_urls"] == 2
        assert compare_payload["score_delta"]["avg_overall_delta"] > 0
        assert compare_payload["issue_code_deltas"]

        invalid_compare = _request_json(
            base_url,
            f"/api/compare?left_run_id={run_two}&right_run_id={run_two}",
            expected_status=400,
        )
        assert "must differ" in invalid_compare["error"]

        export_text = _read_text(base_url, f"/api/export?dataset=issues&run_id={run_one}&severity=high")
        assert "issue_code" in export_text
        assert "NOINDEX" in export_text

        page_export = _read_text(base_url, f"/api/export?dataset=pages&run_id={run_one}&page_type=service")
        assert "normalized_url" in page_export
        assert "https://example.com/service" in page_export

        html = _read_text(base_url, "/")
        assert "SEO Audit Interactive Dashboard" in html


def test_dashboard_architecture_endpoint(tmp_path: Path) -> None:
    db_path = tmp_path / "audit.sqlite"
    run_one, _run_two = _seed_dashboard_db(db_path)

    with _running_server(db_path) as base_url:
        payload = _read_json(base_url, f"/api/architecture?run_id={run_one}")
        assert payload["summary"]["nodes"] == 2
        assert payload["summary"]["community_count"] >= 1
        assert "important_pages_weak_support" in payload
        assert "overloaded_hubs" in payload
        assert payload["overloaded_hubs"]
        assert payload["important_pages_weak_support"]


def test_dashboard_html_style_contract(tmp_path: Path) -> None:
    db_path = tmp_path / "audit.sqlite"
    _seed_dashboard_db(db_path)

    with _running_server(db_path) as base_url:
        html = _read_text(base_url, "/")

        # Core VOID token system anchors.
        assert "--color-black: #050505;" in html
        assert "--color-accent: #8b9b87;" in html
        assert "--font-serif: 'Cormorant Garamond', Georgia, serif;" in html
        assert "--text-base: 15px;" in html
        assert "--radius-pill: 50px;" in html

        # Primary CTA and navigation styling hooks.
        assert ".btn-primary {" in html
        assert "background: var(--color-black);" in html
        assert "color: var(--color-white);" in html
        assert ".view-tab::after {" in html
        assert ".view-tab::before {" in html

        # Runtime selector contracts used by dashboard JS wiring.
        assert "function setActiveView(viewName)" in html
        assert 'id="runSelect"' in html
        assert 'id="runModalBackdrop"' in html
        assert 'data-view="overview"' in html
        assert 'id="architecturePanel"' in html
        assert 'id="architectureSignalBars"' in html
        assert "async function refreshArchitecture()" in html
        assert "api('/api/architecture'" in html


def test_dashboard_query_api_and_export(tmp_path: Path) -> None:
    db_path = tmp_path / "audit.sqlite"
    run_one, _run_two = _seed_dashboard_db(db_path)

    with _running_server(db_path) as base_url:
        count_before = _request_json(
            base_url,
            "/api/query",
            method="POST",
            payload={
                "run_id": run_one,
                "query": "SELECT COUNT(*) AS total FROM issues WHERE run_id = :run_id",
            },
        )

        query_payload = _request_json(
            base_url,
            "/api/query",
            method="POST",
            payload={
                "run_id": run_one,
                "limit": 50,
                "query": (
                    "SELECT issue_code, COUNT(*) AS total "
                    "FROM issues "
                    "WHERE run_id = :run_id "
                    "GROUP BY issue_code "
                    "ORDER BY total DESC"
                ),
            },
        )
        assert query_payload["row_count"] == 2
        assert "issue_code" in query_payload["columns"]
        assert "total" in query_payload["columns"]

        query_export = _request_text(
            base_url,
            "/api/query_export",
            method="POST",
            payload={
                "run_id": run_one,
                "limit": 10,
                "query": (
                    "SELECT page_type, COUNT(*) AS pages "
                    "FROM pages "
                    "WHERE run_id = :run_id "
                    "GROUP BY page_type "
                    "ORDER BY pages DESC"
                ),
            },
        )
        assert "page_type,pages" in query_export
        assert "homepage,1" in query_export

        bad_query = _request_json(
            base_url,
            "/api/query",
            method="POST",
            payload={"query": "DELETE FROM issues"},
            expected_status=400,
        )
        assert "SELECT or WITH" in bad_query["error"] or "read-only" in bad_query["error"]

        count_after = _request_json(
            base_url,
            "/api/query",
            method="POST",
            payload={
                "run_id": run_one,
                "query": "SELECT COUNT(*) AS total FROM issues WHERE run_id = :run_id",
            },
        )
        assert count_before["rows"][0]["total"] == count_after["rows"][0]["total"]


def test_dashboard_jobs_screenshots_and_run_audit_validation(tmp_path: Path) -> None:
    db_path = tmp_path / "audit.sqlite"
    run_one, _run_two = _seed_dashboard_db(db_path)

    with _running_server(db_path) as base_url:
        jobs_payload = _read_json(base_url, "/api/jobs")
        assert jobs_payload == {"jobs": []}

        screenshots_payload = _read_json(base_url, f"/api/screenshots?run_id={run_one}")
        assert screenshots_payload["run_id"] == run_one
        assert screenshots_payload["items"] == []

        bad_post = _request_json(
            base_url,
            "/api/run_audit",
            method="POST",
            payload={},
            expected_status=400,
        )
        assert bad_post["error"] == "domain is required"

        queued = _request_json(
            base_url,
            "/api/run_audit",
            method="POST",
            payload={"domain": "https://example.com", "run_profile": "standard"},
            expected_status=202,
        )
        assert queued["status"] == "queued"
        assert queued["queue_state"] == "pending"
        assert queued["job_id"]

        listed = _read_json(base_url, "/api/jobs")
        assert any(str(row.get("job_id") or "") == queued["job_id"] for row in listed.get("jobs", []))


def test_dashboard_cancel_job_endpoint(tmp_path: Path) -> None:
    db_path = tmp_path / "audit.sqlite"
    _seed_dashboard_db(db_path)

    with _running_server(db_path) as base_url:
        queued = _request_json(
            base_url,
            "/api/run_audit",
            method="POST",
            payload={"domain": "https://example.com", "run_profile": "standard"},
            expected_status=202,
        )

        canceled = _request_json(
            base_url,
            "/api/cancel_job",
            method="POST",
            payload={"job_id": queued["job_id"]},
            expected_status=202,
        )
        assert canceled["job_id"] == queued["job_id"]
        assert canceled["queue_state"] == "canceled"
        assert canceled["status"] == "failed"

        status = _read_json(base_url, f"/api/job_status?job_id={queued['job_id']}")
        assert status["queue_state"] == "canceled"
        assert status["status"] == "failed"
        assert "canceled" in str(status["error"] or "").lower()


def test_dashboard_job_status_merges_queue_and_run_events(tmp_path: Path) -> None:
    db_path = tmp_path / "audit.sqlite"
    run_one, _run_two = _seed_dashboard_db(db_path)

    queue_db = tmp_path / "queue.sqlite"
    store = QueueStore(queue_db)
    store.init_db()
    job = store.enqueue_job(
        domain="https://example.com",
        output_dir=str(tmp_path),
        config={
            "domain": "https://example.com",
            "run_profile": "standard",
            "max_pages": 5,
            "render_mode": "none",
            "max_render_pages": 0,
            "performance_targets": 1,
        },
    )
    claimed = store.claim_next_job(worker_id="worker-a", lease_seconds=30.0, policy=AdmissionPolicy())
    assert claimed is not None
    assert claimed["job_id"] == job["job_id"]

    store.mark_starting(job_id=job["job_id"], worker_id="worker-a", pid=4321, lease_seconds=30.0)
    store.mark_running(job_id=job["job_id"], worker_id="worker-a", lease_seconds=30.0)
    store.attach_run_id(job_id=job["job_id"], run_id=run_one)

    attempt_log = tmp_path / "attempt.log"
    attempt_log.write_text("[1/10] crawl: https://example.com\n", encoding="utf-8")
    store.update_attempt_log_paths(job_id=job["job_id"], stdout_log_path=str(attempt_log), stderr_log_path="")
    store.close()

    with _running_server(db_path) as base_url:
        payload = _read_json(base_url, f"/api/job_status?job_id={job['job_id']}")
        assert payload["status"] == "running"
        assert payload["queue_state"] == "running"
        assert payload["run_id"] == run_one
        assert int(payload["progress_percent"] or 0) >= 10
        assert str(payload["current_stage"] or "").strip()


def test_dashboard_startup_recovery_moves_expired_running_job_to_retry_wait(tmp_path: Path) -> None:
    db_path = tmp_path / "audit.sqlite"
    _seed_dashboard_db(db_path)

    queue_db = tmp_path / "queue.sqlite"
    store = QueueStore(queue_db)
    store.init_db()
    job = store.enqueue_job(
        domain="https://retry.example",
        output_dir=str(tmp_path),
        config={"domain": "https://retry.example", "run_profile": "standard"},
    )
    claimed = store.claim_next_job(worker_id="worker-a", lease_seconds=20.0, policy=AdmissionPolicy())
    assert claimed is not None
    store.conn.execute(
        """
        UPDATE jobs
        SET state = 'running', lease_expires_at = ?, pid = ?, run_db_path = ?
        WHERE job_id = ?
        """,
        ("2000-01-01T00:00:00+00:00", 99999999, str(db_path), job["job_id"]),
    )
    store.conn.commit()
    store.close()

    server = create_dashboard_server(db_path, host="127.0.0.1", port=0, start_worker=False)
    server.server_close()

    verify = QueueStore(queue_db)
    try:
        refreshed = verify.get_job(job["job_id"])
    finally:
        verify.close()
    assert refreshed is not None
    assert refreshed["state"] == "retry_wait"


def test_dashboard_startup_recovery_marks_completed_when_run_finished(tmp_path: Path) -> None:
    db_path = tmp_path / "audit.sqlite"
    run_one, _run_two = _seed_dashboard_db(db_path)

    queue_db = tmp_path / "queue.sqlite"
    store = QueueStore(queue_db)
    store.init_db()
    job = store.enqueue_job(
        domain="https://completed.example",
        output_dir=str(tmp_path),
        config={"domain": "https://completed.example", "run_profile": "standard"},
    )
    claimed = store.claim_next_job(worker_id="worker-a", lease_seconds=20.0, policy=AdmissionPolicy())
    assert claimed is not None
    store.attach_run_id(job_id=job["job_id"], run_id=run_one)
    store.conn.execute(
        """
        UPDATE jobs
        SET state = 'running', lease_expires_at = ?, pid = ?, run_db_path = ?
        WHERE job_id = ?
        """,
        ("2000-01-01T00:00:00+00:00", 0, str(db_path), job["job_id"]),
    )
    store.conn.commit()
    store.close()

    server = create_dashboard_server(db_path, host="127.0.0.1", port=0, start_worker=False)
    server.server_close()

    verify = QueueStore(queue_db)
    try:
        refreshed = verify.get_job(job["job_id"])
    finally:
        verify.close()
    assert refreshed is not None
    assert refreshed["state"] == "completed"


def test_dashboard_handler_ignores_client_disconnect_writes(tmp_path: Path) -> None:
    db_path = tmp_path / "audit.sqlite"
    _seed_dashboard_db(db_path)

    store = DashboardStore(db_path)
    manager = AuditJobManager(db_path=db_path, project_root=tmp_path, embedded_worker=False)
    handler_cls = _build_handler(store, manager, "<html></html>")
    handler: Any = handler_cls.__new__(handler_cls)

    handler.send_response = lambda code, message=None: None
    handler.send_header = lambda keyword, value: None
    handler.end_headers = lambda: None

    class _BrokenWriter:
        def write(self, _payload: bytes) -> int:
            raise BrokenPipeError(32, "Broken pipe")

    handler.wfile = _BrokenWriter()

    artifact = tmp_path / "artifact.txt"
    artifact.write_text("ok", encoding="utf-8")

    handler._send_json({"ok": True})
    handler._send_html("<p>ok</p>")
    handler._send_csv("a,b\n1,2\n", "sample.csv")
    handler._send_file(artifact)
