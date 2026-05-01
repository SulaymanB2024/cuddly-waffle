from __future__ import annotations

import time
from pathlib import Path

from seo_audit.job_queue import QueueStore
from seo_audit.queue_worker import run_queue_worker


class _FakeProcess:
    def __init__(self, lines: list[str], *, return_code: int = 0) -> None:
        self.pid = 9876
        self._lines = list(lines)
        self._return_code = int(return_code)
        self._finished = False
        self._terminated = False
        self._killed = False
        self.stdout = self._iter_stdout()

    def _iter_stdout(self):
        for line in self._lines:
            time.sleep(0.01)
            yield line
        self._finished = True

    def poll(self) -> int | None:
        if self._finished or self._terminated or self._killed:
            return self._return_code
        return None

    def wait(self) -> int:
        self._finished = True
        return self._return_code

    def terminate(self) -> None:
        self._terminated = True
        self._return_code = -15

    def kill(self) -> None:
        self._killed = True
        self._return_code = -9


def _enqueue(store: QueueStore, out_dir: Path) -> dict:
    return store.enqueue_job(
        domain="https://example.com",
        output_dir=str(out_dir),
        config={
            "domain": "https://example.com",
            "run_profile": "standard",
            "max_pages": 3,
            "render_mode": "none",
            "max_render_pages": 0,
            "performance_targets": 1,
        },
    )


def test_worker_processes_one_job_and_records_run_id(monkeypatch, tmp_path: Path) -> None:
    queue_db = tmp_path / "queue.sqlite"
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    store = QueueStore(queue_db)
    store.init_db()
    job = _enqueue(store, out_dir)
    store.close()

    fake = _FakeProcess([
        "[1/10] robots: https://example.com\n",
        "[10/10] done run-abc123\n",
    ])

    def fake_popen(*args, **kwargs):  # noqa: ANN001
        return fake

    monkeypatch.setattr("seo_audit.queue_worker.subprocess.Popen", fake_popen)

    processed = run_queue_worker(queue_db=queue_db, worker_id="worker-test", max_jobs=1)
    assert processed == 1

    verify = QueueStore(queue_db)
    try:
        payload = verify.get_job(job["job_id"])
        attempt = verify.get_latest_attempt(job["job_id"])
    finally:
        verify.close()

    assert payload is not None
    assert payload["state"] == "completed"
    assert payload["run_id"] == "run-abc123"
    assert attempt is not None
    assert attempt["stdout_log_path"]
    assert Path(str(attempt["stdout_log_path"])).exists()
    assert attempt["stderr_log_path"]
    assert Path(str(attempt["stderr_log_path"])).exists()
    assert int(attempt["line_count"] or 0) >= 2
    assert int(attempt["duration_ms"] or 0) >= 0
    assert str(attempt["last_stage"] or "").lower().startswith("done")
    assert isinstance(attempt.get("summary"), dict)
    assert int(attempt["summary"].get("line_count") or 0) >= 2
    assert str(attempt["summary"].get("stream_mode") or "") == "combined"
