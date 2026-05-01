from __future__ import annotations

import json
import os
import queue
import random
import re
import sqlite3
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from seo_audit.job_queue import (
    AdmissionPolicy,
    JOB_STATE_CANCEL_REQUESTED,
    JOB_STATE_CANCELED,
    JOB_STATE_COMPLETED,
    QueueStore,
    compute_queue_retry_backoff,
    extract_signal_from_return_code,
    pid_is_alive,
    resolve_run_status,
)

STAGE_PROGRESS_PATTERN = re.compile(r"^\[(\d+)/(\d+)\]\s*(.*)$")
STAGE_DONE_PATTERN = re.compile(r"^\[(\d+)/(\d+)\]\s*done\s+(\S+)\s*$", flags=re.IGNORECASE)

RETRYABLE_ERROR_PATTERNS = (
    "temporary",
    "timed out",
    "connection reset",
    "connection aborted",
    "name resolution",
    "database is locked",
    "sqlite_busy",
    "429",
    "503",
    "504",
)

NON_RETRYABLE_ERROR_PATTERNS = (
    "invalid choice",
    "error: argument",
    "requires --",
    "domain is required",
    "unrecognized arguments",
)


@dataclass(slots=True)
class QueueWorkerConfig:
    queue_db: Path
    worker_id: str
    poll_seconds: float = 1.0
    jitter_seconds: float = 0.35
    lease_seconds: float = 30.0
    heartbeat_seconds: float = 5.0
    cancel_grace_seconds: float = 8.0
    recovery_interval_seconds: float = 15.0


class QueueWorker:
    def __init__(
        self,
        *,
        store: QueueStore,
        config: QueueWorkerConfig,
        admission_policy: AdmissionPolicy,
    ) -> None:
        self.store = store
        self.config = config
        self.admission_policy = admission_policy
        self._last_recovery = 0.0

    def run_forever(self, *, stop_event: threading.Event | None = None, max_jobs: int | None = None) -> int:
        processed = 0
        while True:
            if stop_event is not None and stop_event.is_set():
                break
            if max_jobs is not None and processed >= int(max_jobs):
                break

            now = time.monotonic()
            if now - self._last_recovery >= max(3.0, float(self.config.recovery_interval_seconds)):
                self.store.recover_expired_leases(is_pid_alive=pid_is_alive, run_status_lookup=resolve_run_status)
                self._last_recovery = now

            job = self.store.claim_next_job(
                worker_id=self.config.worker_id,
                lease_seconds=self.config.lease_seconds,
                policy=self.admission_policy,
            )
            if job is None:
                sleep_for = max(0.1, float(self.config.poll_seconds)) + random.uniform(
                    0.0,
                    max(0.0, float(self.config.jitter_seconds)),
                )
                time.sleep(sleep_for)
                continue

            self._run_job(job)
            processed += 1

        return processed

    def _run_job(self, job: dict[str, Any]) -> None:
        job_id = str(job.get("job_id") or "")
        if not job_id:
            return

        config = dict(job.get("config") or {})
        output_dir = Path(str(job.get("output_dir") or "./out")).resolve()
        output_dir.mkdir(parents=True, exist_ok=True)

        attempt_no = int(job.get("attempt_count") or 0)
        log_dir = output_dir / "queue_logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        stdout_log_path = log_dir / f"{job_id}-attempt-{attempt_no:02d}.log"
        stderr_log_path = stdout_log_path

        cmd = _build_audit_command(config=config, output_dir=output_dir)
        env = os.environ.copy()
        env.setdefault("PYTHONUNBUFFERED", "1")

        try:
            process = subprocess.Popen(
                cmd,
                cwd=str(Path(__file__).resolve().parents[1]),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
            )
        except Exception as exc:
            self.store.mark_failed(
                job_id=job_id,
                worker_id=self.config.worker_id,
                error_summary=str(exc),
                exit_code=None,
                sig=None,
                retryable=_is_retryable_error(str(exc)),
                backoff_seconds=compute_queue_retry_backoff(attempt_no + 1),
                run_id="",
                attempt_summary={
                    "result": "spawn_error",
                    "error": str(exc),
                    "retryable": int(_is_retryable_error(str(exc))),
                },
            )
            return

        self.store.mark_starting(
            job_id=job_id,
            worker_id=self.config.worker_id,
            pid=int(process.pid),
            lease_seconds=self.config.lease_seconds,
        )
        self.store.mark_running(
            job_id=job_id,
            worker_id=self.config.worker_id,
            lease_seconds=self.config.lease_seconds,
        )
        self.store.update_attempt_log_paths(
            job_id=job_id,
            stdout_log_path=str(stdout_log_path),
            stderr_log_path=str(stderr_log_path),
        )

        line_queue: queue.Queue[str | None] = queue.Queue(maxsize=2048)
        stream_done = threading.Event()

        def _reader() -> None:
            try:
                if process.stdout is None:
                    return
                for raw in process.stdout:
                    try:
                        line_queue.put(raw.rstrip("\n"), timeout=1.0)
                    except queue.Full:
                        continue
            finally:
                stream_done.set()
                try:
                    line_queue.put(None, timeout=0.5)
                except queue.Full:
                    pass

        reader = threading.Thread(target=_reader, daemon=True)
        reader.start()

        run_id = str(job.get("run_id") or "")
        screenshot_target_count = max(0, int(config.get("screenshot_count") or 0))
        lines_tail: list[str] = []
        line_count = 0
        last_stage = ""
        cancel_sent = False
        terminate_sent_at = 0.0
        next_heartbeat = time.monotonic() + max(1.0, float(self.config.heartbeat_seconds))
        started_at = time.monotonic()

        with stdout_log_path.open("a", encoding="utf-8") as log_file:
            while True:
                try:
                    line = line_queue.get(timeout=0.4)
                except queue.Empty:
                    line = ""

                if line is None:
                    if stream_done.is_set() and process.poll() is not None:
                        break
                elif line:
                    log_file.write(line + "\n")
                    log_file.flush()
                    line_count += 1
                    lines_tail.append(line)
                    if len(lines_tail) > 60:
                        lines_tail = lines_tail[-60:]

                    stage_match = STAGE_PROGRESS_PATTERN.match(line)
                    if stage_match:
                        stage_text = str(stage_match.group(3) or "").strip()
                        if stage_text:
                            last_stage = stage_text

                    done_match = STAGE_DONE_PATTERN.match(line)
                    if done_match:
                        parsed_run_id = str(done_match.group(3) or "").strip()
                        if parsed_run_id and parsed_run_id != run_id:
                            run_id = parsed_run_id
                            self.store.attach_run_id(job_id=job_id, run_id=run_id)

                now = time.monotonic()
                if now >= next_heartbeat:
                    self.store.extend_lease(
                        job_id=job_id,
                        worker_id=self.config.worker_id,
                        lease_seconds=self.config.lease_seconds,
                    )
                    fresh = self.store.get_job(job_id)
                    if fresh is not None:
                        cancel_requested = int(fresh.get("cancel_requested") or 0) == 1
                        state = str(fresh.get("state") or "")
                        if cancel_requested or state == JOB_STATE_CANCEL_REQUESTED:
                            if not cancel_sent and process.poll() is None:
                                process.terminate()
                                cancel_sent = True
                                terminate_sent_at = now

                    next_heartbeat = now + max(1.0, float(self.config.heartbeat_seconds))

                if cancel_sent and process.poll() is None:
                    if (now - terminate_sent_at) >= max(1.0, float(self.config.cancel_grace_seconds)):
                        process.kill()

                if stream_done.is_set() and process.poll() is not None and line_queue.empty():
                    break

        return_code = process.wait()
        reader.join(timeout=1.0)
        duration_ms = max(0, int((time.monotonic() - started_at) * 1000.0))

        sig = extract_signal_from_return_code(return_code)
        fresh = self.store.get_job(job_id) or {}
        if not run_id:
            run_id = str(fresh.get("run_id") or "")

        run_status = resolve_run_status(str(fresh.get("run_db_path") or ""), run_id)
        cancel_requested = int(fresh.get("cancel_requested") or 0) == 1

        def _capture_screenshot_artifacts(active_run_id: str) -> tuple[int, str, int]:
            if screenshot_target_count <= 0 or not active_run_id:
                return 0, "", 0

            run_db_path = str(fresh.get("run_db_path") or job.get("run_db_path") or "")
            try:
                screenshots = _capture_screenshots_for_run(
                    run_db_path=run_db_path,
                    output_dir=output_dir,
                    run_id=active_run_id,
                    limit=screenshot_target_count,
                )
                line = f"screenshots: captured {len(screenshots)}"
                added = _append_log_line(stdout_log_path, line)
                return len(screenshots), "", added
            except Exception as exc:
                error_text = str(exc)
                line = f"screenshots unavailable: {error_text}"
                added = _append_log_line(stdout_log_path, line)
                return 0, error_text, added

        if run_status == JOB_STATE_COMPLETED:
            screenshot_captured_count, screenshot_error, added_lines = _capture_screenshot_artifacts(run_id)
            line_count += int(added_lines)
            self.store.mark_completed(
                job_id=job_id,
                worker_id=self.config.worker_id,
                run_id=run_id,
                exit_code=int(return_code),
                line_count=line_count,
                duration_ms=duration_ms,
                last_stage=last_stage,
                stdout_log_path=str(stdout_log_path),
                stderr_log_path=str(stderr_log_path),
                attempt_summary={
                    "result": "completed_from_run_db",
                    "return_code": int(return_code),
                    "line_count": line_count,
                    "duration_ms": duration_ms,
                    "last_stage": last_stage,
                    "stream_mode": "combined",
                    "screenshot_target_count": screenshot_target_count,
                    "screenshot_captured_count": screenshot_captured_count,
                    "screenshot_error": screenshot_error,
                },
            )
            return

        if cancel_requested or run_status == JOB_STATE_CANCELED:
            self.store.mark_canceled(
                job_id=job_id,
                worker_id=self.config.worker_id,
                run_id=run_id,
                exit_code=return_code,
                sig=sig,
                error_summary=_error_summary(lines_tail),
                line_count=line_count,
                duration_ms=duration_ms,
                last_stage=last_stage,
                stdout_log_path=str(stdout_log_path),
                stderr_log_path=str(stderr_log_path),
                attempt_summary={
                    "result": "canceled",
                    "return_code": return_code,
                    "signal": sig,
                    "line_count": line_count,
                    "duration_ms": duration_ms,
                    "last_stage": last_stage,
                    "stream_mode": "combined",
                },
            )
            return

        if int(return_code) == 0:
            screenshot_captured_count, screenshot_error, added_lines = _capture_screenshot_artifacts(run_id)
            line_count += int(added_lines)
            self.store.mark_completed(
                job_id=job_id,
                worker_id=self.config.worker_id,
                run_id=run_id,
                exit_code=int(return_code),
                line_count=line_count,
                duration_ms=duration_ms,
                last_stage=last_stage,
                stdout_log_path=str(stdout_log_path),
                stderr_log_path=str(stderr_log_path),
                attempt_summary={
                    "result": "completed",
                    "return_code": int(return_code),
                    "line_count": line_count,
                    "duration_ms": duration_ms,
                    "last_stage": last_stage,
                    "stream_mode": "combined",
                    "screenshot_target_count": screenshot_target_count,
                    "screenshot_captured_count": screenshot_captured_count,
                    "screenshot_error": screenshot_error,
                },
            )
            return

        summary = _error_summary(lines_tail)
        retryable = _is_retryable_error(summary)
        self.store.mark_failed(
            job_id=job_id,
            worker_id=self.config.worker_id,
            error_summary=summary,
            exit_code=return_code,
            sig=sig,
            retryable=retryable,
            backoff_seconds=compute_queue_retry_backoff(int(fresh.get("attempt_count") or 1)),
            run_id=run_id,
            line_count=line_count,
            duration_ms=duration_ms,
            last_stage=last_stage,
            stdout_log_path=str(stdout_log_path),
            stderr_log_path=str(stderr_log_path),
            attempt_summary={
                "result": "failed",
                "return_code": return_code,
                "signal": sig,
                "retryable": int(retryable),
                "line_count": line_count,
                "duration_ms": duration_ms,
                "last_stage": last_stage,
                "error_summary": summary,
                "stream_mode": "combined",
            },
        )


def _error_summary(lines: list[str]) -> str:
    if not lines:
        return "subprocess exited without output"
    tail = [line.strip() for line in lines[-8:] if str(line).strip()]
    if not tail:
        return "subprocess exited without output"
    return " | ".join(tail)


def _is_retryable_error(summary: str) -> bool:
    text = str(summary or "").strip().lower()
    if not text:
        return False
    if any(token in text for token in NON_RETRYABLE_ERROR_PATTERNS):
        return False
    return any(token in text for token in RETRYABLE_ERROR_PATTERNS)


def _build_audit_command(*, config: dict[str, Any], output_dir: Path) -> list[str]:
    domain = str(config.get("domain") or "").strip()
    if not domain:
        raise ValueError("enqueue config is missing domain")

    run_profile = str(config.get("run_profile") or "standard").strip().lower()
    max_pages = int(config.get("max_pages") or 20)
    render_mode = str(config.get("render_mode") or "none").strip().lower()
    max_render_pages = int(config.get("max_render_pages") or 0)
    performance_targets = int(config.get("performance_targets") or 2)

    cmd = [
        sys.executable,
        "-u",
        "-m",
        "seo_audit",
        "audit",
        "--domain",
        domain,
        "--output",
        str(output_dir),
        "--run-profile",
        run_profile,
        "--max-pages",
        str(max_pages),
        "--render-mode",
        render_mode,
        "--max-render-pages",
        str(max_render_pages),
        "--performance-targets",
        str(performance_targets),
    ]

    if bool(config.get("offsite_commoncrawl_enabled", False)):
        cmd.append("--offsite-commoncrawl-enabled")
    if bool(config.get("lighthouse_enabled", False)):
        cmd.append("--lighthouse-enabled")

    return cmd


def _append_log_line(log_path: Path, line: str) -> int:
    text = str(line or "").strip()
    if not text:
        return 0
    try:
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(text + "\n")
        return 1
    except Exception:
        return 0


def _capture_screenshots_for_run(
    *,
    run_db_path: str,
    output_dir: Path,
    run_id: str,
    limit: int,
) -> list[dict[str, str]]:
    if not run_id or int(limit) <= 0:
        return []

    db_candidate = Path(str(run_db_path or "").strip()) if str(run_db_path or "").strip() else (output_dir / "audit.sqlite")
    if not db_candidate.exists():
        return []

    urls: list[str] = []
    with sqlite3.connect(f"file:{db_candidate.resolve().as_posix()}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only = 1")

        run_row = conn.execute("SELECT domain FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if run_row is not None and run_row["domain"]:
            urls.append(str(run_row["domain"]))

        page_rows = conn.execute(
            """
            SELECT normalized_url
            FROM pages
            WHERE run_id = ?
            AND status_code >= 200
            AND status_code < 400
            AND COALESCE(fetch_error, '') = ''
            ORDER BY COALESCE(crawl_depth, 999) ASC, normalized_url ASC
            LIMIT ?
            """,
            (run_id, max(int(limit) * 4, 12)),
        ).fetchall()
        for row in page_rows:
            urls.append(str(row["normalized_url"]))

    unique_urls: list[str] = []
    seen: set[str] = set()
    for url in urls:
        key = str(url or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        unique_urls.append(key)
        if len(unique_urls) >= int(limit):
            break

    if not unique_urls:
        return []

    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:  # pragma: no cover - depends on environment runtime
        raise RuntimeError(f"playwright not available: {exc}") from exc

    screenshots_dir = output_dir / "screenshots" / run_id
    screenshots_dir.mkdir(parents=True, exist_ok=True)

    captured: list[dict[str, str]] = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 2200})
        for idx, url in enumerate(unique_urls, start=1):
            file_name = f"{idx:02d}.png"
            file_path = screenshots_dir / file_name
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=90000)
                page.wait_for_timeout(1200)
                page.screenshot(path=str(file_path), full_page=True)
                rel = file_path.relative_to(output_dir).as_posix()
                captured.append(
                    {
                        "url": url,
                        "file_name": file_name,
                        "relative_path": rel,
                        "web_path": f"/artifacts/{rel}",
                    }
                )
            except Exception:
                continue
        browser.close()

    index_path = screenshots_dir / "index.json"
    index_path.write_text(json.dumps(captured, indent=2, sort_keys=True), encoding="utf-8")
    return captured


def run_queue_worker(
    *,
    queue_db: Path,
    worker_id: str | None = None,
    poll_seconds: float = 1.0,
    jitter_seconds: float = 0.35,
    lease_seconds: float = 30.0,
    heartbeat_seconds: float = 5.0,
    cancel_grace_seconds: float = 8.0,
    recovery_interval_seconds: float = 15.0,
    admission_policy: AdmissionPolicy | None = None,
    max_jobs: int | None = None,
) -> int:
    worker_identifier = str(worker_id or f"worker-{uuid4()}")
    store = QueueStore(queue_db)
    store.init_db()

    cfg = QueueWorkerConfig(
        queue_db=queue_db,
        worker_id=worker_identifier,
        poll_seconds=poll_seconds,
        jitter_seconds=jitter_seconds,
        lease_seconds=lease_seconds,
        heartbeat_seconds=heartbeat_seconds,
        cancel_grace_seconds=cancel_grace_seconds,
        recovery_interval_seconds=recovery_interval_seconds,
    )

    policy = admission_policy or AdmissionPolicy()
    worker = QueueWorker(store=store, config=cfg, admission_policy=policy)
    try:
        return worker.run_forever(max_jobs=max_jobs)
    finally:
        store.close()
