from __future__ import annotations

from pathlib import Path

from seo_audit.cli import build_parser
from seo_audit.job_queue import (
    AdmissionPolicy,
    JOB_STATE_CANCELED,
    JOB_STATE_RETRY_WAIT,
    QueueStore,
)


def _config(domain: str) -> dict[str, object]:
    return {
        "domain": domain,
        "run_profile": "standard",
        "max_pages": 25,
        "render_mode": "none",
        "max_render_pages": 0,
        "performance_targets": 2,
    }


def test_enqueue_and_claim_respects_priority(tmp_path: Path) -> None:
    store = QueueStore(tmp_path / "queue.sqlite")
    store.init_db()

    low = store.enqueue_job(domain="https://low.example", output_dir=str(tmp_path), config=_config("https://low.example"), priority=1)
    high = store.enqueue_job(domain="https://high.example", output_dir=str(tmp_path), config=_config("https://high.example"), priority=5)

    claimed = store.claim_next_job(worker_id="worker-a", lease_seconds=30.0, policy=AdmissionPolicy())
    assert claimed is not None
    assert claimed["job_id"] == high["job_id"]

    next_claim = store.claim_next_job(worker_id="worker-a", lease_seconds=30.0, policy=AdmissionPolicy())
    assert next_claim is not None
    assert next_claim["job_id"] == low["job_id"]

    store.close()


def test_domain_exclusivity_can_be_relaxed(tmp_path: Path) -> None:
    store = QueueStore(tmp_path / "queue.sqlite")
    store.init_db()

    store.enqueue_job(
        domain="https://www.example.com",
        output_dir=str(tmp_path),
        config=_config("https://www.example.com"),
        priority=2,
    )
    store.enqueue_job(
        domain="https://example.com",
        output_dir=str(tmp_path),
        config=_config("https://example.com"),
        priority=1,
    )

    strict_policy = AdmissionPolicy(enforce_one_active_job_per_domain=True)
    permissive_policy = AdmissionPolicy(enforce_one_active_job_per_domain=False)

    first = store.claim_next_job(worker_id="worker-a", lease_seconds=20.0, policy=strict_policy)
    assert first is not None

    blocked = store.claim_next_job(worker_id="worker-b", lease_seconds=20.0, policy=strict_policy)
    assert blocked is None

    allowed = store.claim_next_job(worker_id="worker-b", lease_seconds=20.0, policy=permissive_policy)
    assert allowed is not None

    store.close()


def test_retry_and_cancel_state_transitions(tmp_path: Path) -> None:
    store = QueueStore(tmp_path / "queue.sqlite")
    store.init_db()

    retry_job = store.enqueue_job(
        domain="https://retry.example",
        output_dir=str(tmp_path),
        config=_config("https://retry.example"),
        max_attempts=3,
    )
    claimed = store.claim_next_job(worker_id="worker-a", lease_seconds=15.0, policy=AdmissionPolicy())
    assert claimed is not None
    assert claimed["job_id"] == retry_job["job_id"]

    store.mark_failed(
        job_id=retry_job["job_id"],
        worker_id="worker-a",
        error_summary="temporary timeout",
        exit_code=1,
        sig=None,
        retryable=True,
        backoff_seconds=2.0,
        run_id="",
    )
    refreshed = store.get_job(retry_job["job_id"])
    assert refreshed is not None
    assert refreshed["state"] == JOB_STATE_RETRY_WAIT

    pending_job = store.enqueue_job(
        domain="https://cancel.example",
        output_dir=str(tmp_path),
        config=_config("https://cancel.example"),
    )
    canceled = store.request_cancel(pending_job["job_id"])
    assert canceled["state"] == JOB_STATE_CANCELED

    store.close()


def test_parser_accepts_queue_commands() -> None:
    parser = build_parser()

    enqueue = parser.parse_args(["enqueue", "--domain", "https://example.com"])
    assert enqueue.command == "enqueue"

    jobs_cmd = parser.parse_args(["jobs", "--queue-db", "./out/queue.sqlite"])
    assert jobs_cmd.command == "jobs"

    cancel_cmd = parser.parse_args(["cancel", "job-123"])
    assert cancel_cmd.command == "cancel"

    worker_cmd = parser.parse_args(["worker", "--once"])
    assert worker_cmd.command == "worker"
