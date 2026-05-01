from __future__ import annotations

import json
import os
import random
import signal
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit
from uuid import uuid4

from seo_audit.url_utils import normalize_url


JOB_STATE_PENDING = "pending"
JOB_STATE_LEASED = "leased"
JOB_STATE_STARTING = "starting"
JOB_STATE_RUNNING = "running"
JOB_STATE_COMPLETING = "completing"
JOB_STATE_COMPLETED = "completed"
JOB_STATE_RETRY_WAIT = "retry_wait"
JOB_STATE_FAILED = "failed"
JOB_STATE_CANCEL_REQUESTED = "cancel_requested"
JOB_STATE_CANCELED = "canceled"
JOB_STATE_ORPHANED = "orphaned"

ACTIVE_JOB_STATES: tuple[str, ...] = (
    JOB_STATE_LEASED,
    JOB_STATE_STARTING,
    JOB_STATE_RUNNING,
    JOB_STATE_COMPLETING,
    JOB_STATE_CANCEL_REQUESTED,
)

ELIGIBLE_JOB_STATES: tuple[str, ...] = (
    JOB_STATE_PENDING,
    JOB_STATE_RETRY_WAIT,
)

TERMINAL_JOB_STATES: tuple[str, ...] = (
    JOB_STATE_COMPLETED,
    JOB_STATE_FAILED,
    JOB_STATE_CANCELED,
)

QUEUE_MIGRATIONS: tuple[tuple[int, str], ...] = (
    (1, "baseline_queue"),
    (2, "job_attempt_runtime_summaries"),
)
CURRENT_QUEUE_SCHEMA_VERSION = QUEUE_MIGRATIONS[-1][0]


QUEUE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS queue_meta (
    meta_key TEXT PRIMARY KEY,
    meta_value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS queue_migrations (
    migration_id INTEGER PRIMARY KEY AUTOINCREMENT,
    version INTEGER NOT NULL UNIQUE,
    name TEXT NOT NULL,
    applied_at TEXT NOT NULL,
    success INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    submitted_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    next_eligible_at TEXT NOT NULL,
    lease_expires_at TEXT,
    claimed_by TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 2,
    domain TEXT NOT NULL,
    domain_key TEXT NOT NULL,
    dedupe_key TEXT,
    config_json TEXT NOT NULL,
    output_dir TEXT NOT NULL,
    run_db_path TEXT NOT NULL,
    run_id TEXT,
    pid INTEGER,
    resource_class TEXT NOT NULL DEFAULT 'baseline',
    required_tokens INTEGER NOT NULL DEFAULT 1,
    render_heavy INTEGER NOT NULL DEFAULT 0,
    provider_heavy INTEGER NOT NULL DEFAULT 0,
    offsite_heavy INTEGER NOT NULL DEFAULT 0,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    completed_at TEXT,
    last_heartbeat_at TEXT,
    last_error TEXT,
    last_exit_code INTEGER,
    last_signal INTEGER
);

CREATE TABLE IF NOT EXISTS job_attempts (
    attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    attempt_no INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    exit_code INTEGER,
    signal INTEGER,
    worker_id TEXT,
    run_id TEXT,
    pid INTEGER,
    stdout_log_path TEXT,
    stderr_log_path TEXT,
    error_summary TEXT,
    line_count INTEGER NOT NULL DEFAULT 0,
    duration_ms INTEGER NOT NULL DEFAULT 0,
    last_stage TEXT NOT NULL DEFAULT '',
    summary_json TEXT NOT NULL DEFAULT '{}',
    FOREIGN KEY(job_id) REFERENCES jobs(job_id)
);

CREATE TABLE IF NOT EXISTS job_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    event_time TEXT NOT NULL,
    event_type TEXT NOT NULL,
    message TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    FOREIGN KEY(job_id) REFERENCES jobs(job_id)
);

CREATE INDEX IF NOT EXISTS idx_jobs_state_priority_submitted
ON jobs(state, priority DESC, submitted_at ASC);

CREATE INDEX IF NOT EXISTS idx_jobs_next_eligible
ON jobs(next_eligible_at);

CREATE INDEX IF NOT EXISTS idx_jobs_lease_expires
ON jobs(lease_expires_at);

CREATE INDEX IF NOT EXISTS idx_jobs_domain_state
ON jobs(domain_key, state);

CREATE INDEX IF NOT EXISTS idx_job_attempts_job_attempt
ON job_attempts(job_id, attempt_no);

CREATE INDEX IF NOT EXISTS idx_job_events_job_time
ON job_events(job_id, event_time);
"""


@dataclass(slots=True)
class AdmissionPolicy:
    total_token_budget: int = 6
    max_render_heavy_jobs: int = 1
    max_provider_heavy_jobs: int = 1
    max_offsite_heavy_jobs: int = 1
    enforce_one_active_job_per_domain: bool = True


@dataclass(slots=True)
class ResourceRequirements:
    resource_class: str
    required_tokens: int
    render_heavy: int
    provider_heavy: int
    offsite_heavy: int


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso(dt: datetime | None = None) -> str:
    value = dt or _utc_now()
    return value.isoformat()


def domain_key_for_domain(domain: str) -> str:
    normalized = normalize_url(str(domain or "").strip() or "https://invalid.local")
    host = (urlsplit(normalized).hostname or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return host or "unknown"


def derive_resource_requirements(config: dict[str, Any]) -> ResourceRequirements:
    run_profile = str(config.get("run_profile") or "standard").strip().lower()
    render_mode = str(config.get("render_mode") or "none").strip().lower()
    max_render_pages = int(config.get("max_render_pages") or 0)
    performance_targets = int(config.get("performance_targets") or 0)
    lighthouse_enabled = bool(config.get("lighthouse_enabled", False))
    offsite_enabled = bool(config.get("offsite_commoncrawl_enabled", False))

    render_heavy = int(render_mode == "all" or max_render_pages >= 40)
    provider_heavy = int(performance_targets >= 8 or lighthouse_enabled)
    offsite_heavy = int(offsite_enabled)

    tokens = 1
    if render_heavy:
        tokens += 2
    if provider_heavy:
        tokens += 1
    if offsite_heavy:
        tokens += 2

    if offsite_heavy:
        resource_class = "offsite_heavy"
    elif render_heavy:
        resource_class = "render_heavy"
    elif provider_heavy:
        resource_class = "provider_heavy"
    elif run_profile == "exploratory":
        resource_class = "light"
    elif run_profile == "deep":
        resource_class = "deep"
    else:
        resource_class = "baseline"

    return ResourceRequirements(
        resource_class=resource_class,
        required_tokens=max(1, int(tokens)),
        render_heavy=render_heavy,
        provider_heavy=provider_heavy,
        offsite_heavy=offsite_heavy,
    )


def compute_queue_retry_backoff(attempt_count: int) -> float:
    base = min(120.0, 2.0 * (2 ** max(0, int(attempt_count) - 1)))
    jitter = random.uniform(0.0, min(1.0, base * 0.3))
    return float(base + jitter)


class QueueStore:
    def __init__(self, db_path: Path, *, timeout_seconds: float = 5.0) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, timeout=timeout_seconds)
        self.conn.row_factory = sqlite3.Row
        self._configure_connection()

    def _configure_connection(self) -> None:
        try:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.execute("PRAGMA busy_timeout=5000")
        except sqlite3.DatabaseError:
            pass

    def init_db(self) -> None:
        self.conn.executescript(QUEUE_SCHEMA_SQL)
        self._migrate_additive_columns()
        self._record_schema_versions()
        self.conn.commit()

    def _migrate_additive_columns(self) -> None:
        column_rows = self.conn.execute("PRAGMA table_info(job_attempts)").fetchall()
        existing = {str(row["name"]) for row in column_rows if row["name"] is not None}

        if "line_count" not in existing:
            self.conn.execute("ALTER TABLE job_attempts ADD COLUMN line_count INTEGER NOT NULL DEFAULT 0")
        if "duration_ms" not in existing:
            self.conn.execute("ALTER TABLE job_attempts ADD COLUMN duration_ms INTEGER NOT NULL DEFAULT 0")
        if "last_stage" not in existing:
            self.conn.execute("ALTER TABLE job_attempts ADD COLUMN last_stage TEXT NOT NULL DEFAULT ''")
        if "summary_json" not in existing:
            self.conn.execute("ALTER TABLE job_attempts ADD COLUMN summary_json TEXT NOT NULL DEFAULT '{}'" )

    def _record_schema_versions(self) -> None:
        now = _utc_iso()
        existing_versions = {
            int(row["version"])
            for row in self.conn.execute("SELECT version FROM queue_migrations").fetchall()
            if row["version"] is not None
        }
        for version, name in QUEUE_MIGRATIONS:
            if version in existing_versions:
                continue
            self.conn.execute(
                "INSERT INTO queue_migrations (version, name, applied_at, success) VALUES (?, ?, ?, 1)",
                (int(version), str(name), now),
            )

        self.conn.execute(
            """
            INSERT INTO queue_meta (meta_key, meta_value, updated_at)
            VALUES ('queue_schema_version', ?, ?)
            ON CONFLICT(meta_key)
            DO UPDATE SET
                meta_value = excluded.meta_value,
                updated_at = excluded.updated_at
            """,
            (str(CURRENT_QUEUE_SCHEMA_VERSION), now),
        )

    def close(self) -> None:
        self.conn.close()

    def _begin_immediate(self) -> None:
        self.conn.execute("BEGIN IMMEDIATE")

    def _row_to_job(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        payload = dict(row)
        try:
            payload["config"] = json.loads(str(payload.get("config_json") or "{}"))
        except json.JSONDecodeError:
            payload["config"] = {}
        return payload

    def _append_event_tx(
        self,
        job_id: str,
        event_type: str,
        *,
        message: str = "",
        detail: dict[str, Any] | None = None,
    ) -> None:
        self.conn.execute(
            "INSERT INTO job_events (job_id, event_time, event_type, message, detail_json) VALUES (?, ?, ?, ?, ?)",
            (
                job_id,
                _utc_iso(),
                str(event_type),
                str(message or ""),
                json.dumps(detail or {}, sort_keys=True),
            ),
        )

    def _active_job_rows(self) -> list[sqlite3.Row]:
        placeholders = ",".join("?" for _ in ACTIVE_JOB_STATES)
        return self.conn.execute(
            f"SELECT * FROM jobs WHERE state IN ({placeholders})",
            ACTIVE_JOB_STATES,
        ).fetchall()

    def _admission_allowed(
        self,
        candidate: sqlite3.Row,
        active_rows: list[sqlite3.Row],
        policy: AdmissionPolicy,
    ) -> bool:
        candidate_tokens = int(candidate["required_tokens"] or 1)
        active_tokens = sum(int(row["required_tokens"] or 1) for row in active_rows)
        if active_tokens + candidate_tokens > max(1, int(policy.total_token_budget)):
            return False

        if policy.enforce_one_active_job_per_domain:
            candidate_domain = str(candidate["domain_key"] or "")
            if candidate_domain and any(str(row["domain_key"] or "") == candidate_domain for row in active_rows):
                return False

        active_render = sum(int(row["render_heavy"] or 0) for row in active_rows)
        active_provider = sum(int(row["provider_heavy"] or 0) for row in active_rows)
        active_offsite = sum(int(row["offsite_heavy"] or 0) for row in active_rows)

        if int(candidate["render_heavy"] or 0) == 1 and active_render >= max(0, int(policy.max_render_heavy_jobs)):
            return False
        if int(candidate["provider_heavy"] or 0) == 1 and active_provider >= max(
            0, int(policy.max_provider_heavy_jobs)
        ):
            return False
        if int(candidate["offsite_heavy"] or 0) == 1 and active_offsite >= max(0, int(policy.max_offsite_heavy_jobs)):
            return False
        return True

    def enqueue_job(
        self,
        *,
        domain: str,
        output_dir: str,
        config: dict[str, Any],
        priority: int = 0,
        max_attempts: int = 2,
        dedupe_key: str = "",
    ) -> dict[str, Any]:
        now = _utc_now()
        now_iso = _utc_iso(now)
        run_db_path = str((Path(output_dir).resolve() / "audit.sqlite"))
        requirements = derive_resource_requirements(config)
        job_id = str(uuid4())
        payload_json = json.dumps(config, sort_keys=True)

        self._begin_immediate()
        try:
            if dedupe_key:
                existing = self.conn.execute(
                    """
                    SELECT *
                    FROM jobs
                    WHERE dedupe_key = ?
                    AND state IN (?, ?, ?, ?, ?, ?)
                    ORDER BY submitted_at DESC
                    LIMIT 1
                    """,
                    (
                        dedupe_key,
                        JOB_STATE_PENDING,
                        JOB_STATE_RETRY_WAIT,
                        JOB_STATE_LEASED,
                        JOB_STATE_STARTING,
                        JOB_STATE_RUNNING,
                        JOB_STATE_CANCEL_REQUESTED,
                    ),
                ).fetchone()
                if existing is not None:
                    self.conn.commit()
                    payload = self._row_to_job(existing)
                    assert payload is not None
                    return payload

            self.conn.execute(
                """
                INSERT INTO jobs (
                    job_id, state, priority, submitted_at, updated_at, next_eligible_at,
                    attempt_count, max_attempts, domain, domain_key, dedupe_key,
                    config_json, output_dir, run_db_path,
                    resource_class, required_tokens, render_heavy, provider_heavy, offsite_heavy,
                    cancel_requested
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    job_id,
                    JOB_STATE_PENDING,
                    int(priority),
                    now_iso,
                    now_iso,
                    now_iso,
                    0,
                    max(1, int(max_attempts)),
                    str(domain),
                    domain_key_for_domain(domain),
                    str(dedupe_key or ""),
                    payload_json,
                    str(output_dir),
                    run_db_path,
                    requirements.resource_class,
                    requirements.required_tokens,
                    requirements.render_heavy,
                    requirements.provider_heavy,
                    requirements.offsite_heavy,
                ),
            )
            self._append_event_tx(job_id, "job_enqueued", detail={"priority": int(priority)})
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

        payload = self.get_job(job_id)
        if payload is None:
            raise KeyError(f"job not found after enqueue: {job_id}")
        return payload

    def list_jobs(self, *, state: str = "", limit: int = 100) -> list[dict[str, Any]]:
        rows: list[sqlite3.Row]
        if state:
            rows = self.conn.execute(
                "SELECT * FROM jobs WHERE state = ? ORDER BY submitted_at DESC LIMIT ?",
                (state, max(1, int(limit))),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM jobs ORDER BY submitted_at DESC LIMIT ?",
                (max(1, int(limit)),),
            ).fetchall()
        return [self._row_to_job(row) for row in rows if row is not None]

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        row = self.conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        return self._row_to_job(row)

    def get_latest_attempt(self, job_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT *
            FROM job_attempts
            WHERE job_id = ?
            ORDER BY attempt_no DESC, attempt_id DESC
            LIMIT 1
            """,
            (job_id,),
        ).fetchone()
        if row is None:
            return None
        payload = dict(row)
        try:
            payload["summary"] = json.loads(str(payload.get("summary_json") or "{}"))
        except json.JSONDecodeError:
            payload["summary"] = {}
        return payload

    def update_attempt_log_paths(self, *, job_id: str, stdout_log_path: str = "", stderr_log_path: str = "") -> None:
        self._begin_immediate()
        try:
            self.conn.execute(
                """
                UPDATE job_attempts
                SET stdout_log_path = ?, stderr_log_path = ?
                WHERE job_id = ?
                AND attempt_no = (
                    SELECT attempt_count FROM jobs WHERE job_id = ?
                )
                """,
                (
                    str(stdout_log_path or ""),
                    str(stderr_log_path or ""),
                    job_id,
                    job_id,
                ),
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def request_cancel(self, job_id: str) -> dict[str, Any]:
        now_iso = _utc_iso()
        self._begin_immediate()
        try:
            row = self.conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            if row is None:
                raise KeyError("job not found")

            current_state = str(row["state"] or "")
            if current_state in TERMINAL_JOB_STATES:
                self.conn.commit()
                payload = self._row_to_job(row)
                assert payload is not None
                return payload

            next_state = JOB_STATE_CANCEL_REQUESTED
            completed_at: str | None = None
            if current_state in {JOB_STATE_PENDING, JOB_STATE_RETRY_WAIT}:
                next_state = JOB_STATE_CANCELED
                completed_at = now_iso

            self.conn.execute(
                """
                UPDATE jobs
                SET state = ?, cancel_requested = 1, completed_at = COALESCE(?, completed_at), updated_at = ?
                WHERE job_id = ?
                """,
                (next_state, completed_at, now_iso, job_id),
            )
            self._append_event_tx(job_id, "cancel_requested", detail={"previous_state": current_state})
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

        payload = self.get_job(job_id)
        if payload is None:
            raise KeyError("job not found")
        return payload

    def claim_next_job(
        self,
        *,
        worker_id: str,
        lease_seconds: float,
        policy: AdmissionPolicy,
    ) -> dict[str, Any] | None:
        now = _utc_now()
        now_iso = _utc_iso(now)
        lease_expires_iso = _utc_iso(now + timedelta(seconds=max(5.0, float(lease_seconds))))

        self._begin_immediate()
        try:
            self.conn.execute(
                """
                UPDATE jobs
                SET state = ?, updated_at = ?, completed_at = ?, cancel_requested = 1
                WHERE state IN (?, ?) AND cancel_requested = 1
                """,
                (
                    JOB_STATE_CANCELED,
                    now_iso,
                    now_iso,
                    JOB_STATE_PENDING,
                    JOB_STATE_RETRY_WAIT,
                ),
            )

            placeholders = ",".join("?" for _ in ELIGIBLE_JOB_STATES)
            candidates = self.conn.execute(
                f"""
                SELECT *
                FROM jobs
                WHERE state IN ({placeholders})
                AND cancel_requested = 0
                AND next_eligible_at <= ?
                ORDER BY priority DESC, submitted_at ASC
                LIMIT 100
                """,
                (*ELIGIBLE_JOB_STATES, now_iso),
            ).fetchall()
            active_rows = self._active_job_rows()

            for candidate in candidates:
                if not self._admission_allowed(candidate, active_rows, policy):
                    continue

                next_attempt_no = int(candidate["attempt_count"] or 0) + 1
                updated = self.conn.execute(
                    """
                    UPDATE jobs
                    SET state = ?,
                        claimed_by = ?,
                        lease_expires_at = ?,
                        last_heartbeat_at = ?,
                        updated_at = ?,
                        attempt_count = ?
                    WHERE job_id = ?
                    AND state IN (?, ?)
                    AND cancel_requested = 0
                    """,
                    (
                        JOB_STATE_LEASED,
                        worker_id,
                        lease_expires_iso,
                        now_iso,
                        now_iso,
                        next_attempt_no,
                        str(candidate["job_id"]),
                        JOB_STATE_PENDING,
                        JOB_STATE_RETRY_WAIT,
                    ),
                )
                if updated.rowcount != 1:
                    continue

                self.conn.execute(
                    """
                    INSERT INTO job_attempts (job_id, attempt_no, started_at, worker_id)
                    VALUES (?, ?, ?, ?)
                    """,
                    (str(candidate["job_id"]), next_attempt_no, now_iso, worker_id),
                )
                self._append_event_tx(
                    str(candidate["job_id"]),
                    "job_leased",
                    detail={"worker_id": worker_id, "attempt_no": next_attempt_no},
                )
                self.conn.commit()
                return self.get_job(str(candidate["job_id"]))

            self.conn.commit()
            return None
        except Exception:
            self.conn.rollback()
            raise

    def mark_starting(self, *, job_id: str, worker_id: str, pid: int, lease_seconds: float) -> None:
        now = _utc_now()
        now_iso = _utc_iso(now)
        lease_iso = _utc_iso(now + timedelta(seconds=max(5.0, float(lease_seconds))))

        self._begin_immediate()
        try:
            self.conn.execute(
                """
                UPDATE jobs
                SET state = ?,
                    claimed_by = ?,
                    pid = ?,
                    lease_expires_at = ?,
                    last_heartbeat_at = ?,
                    updated_at = ?
                WHERE job_id = ?
                """,
                (
                    JOB_STATE_STARTING,
                    worker_id,
                    int(pid),
                    lease_iso,
                    now_iso,
                    now_iso,
                    job_id,
                ),
            )
            self.conn.execute(
                """
                UPDATE job_attempts
                SET pid = ?, worker_id = ?
                WHERE job_id = ? AND attempt_no = (
                    SELECT attempt_count FROM jobs WHERE job_id = ?
                )
                """,
                (int(pid), worker_id, job_id, job_id),
            )
            self._append_event_tx(job_id, "job_starting", detail={"pid": int(pid)})
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def mark_running(self, *, job_id: str, worker_id: str, lease_seconds: float) -> None:
        now = _utc_now()
        now_iso = _utc_iso(now)
        lease_iso = _utc_iso(now + timedelta(seconds=max(5.0, float(lease_seconds))))
        self._begin_immediate()
        try:
            self.conn.execute(
                """
                UPDATE jobs
                SET state = ?,
                    claimed_by = ?,
                    lease_expires_at = ?,
                    last_heartbeat_at = ?,
                    updated_at = ?
                WHERE job_id = ?
                """,
                (JOB_STATE_RUNNING, worker_id, lease_iso, now_iso, now_iso, job_id),
            )
            self._append_event_tx(job_id, "job_running")
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def extend_lease(self, *, job_id: str, worker_id: str, lease_seconds: float) -> None:
        now = _utc_now()
        now_iso = _utc_iso(now)
        lease_iso = _utc_iso(now + timedelta(seconds=max(5.0, float(lease_seconds))))

        self._begin_immediate()
        try:
            self.conn.execute(
                """
                UPDATE jobs
                SET lease_expires_at = ?,
                    last_heartbeat_at = ?,
                    updated_at = ?,
                    claimed_by = ?
                WHERE job_id = ?
                """,
                (lease_iso, now_iso, now_iso, worker_id, job_id),
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def attach_run_id(self, *, job_id: str, run_id: str) -> None:
        if not run_id:
            return
        self._begin_immediate()
        try:
            self.conn.execute(
                "UPDATE jobs SET run_id = ?, updated_at = ? WHERE job_id = ?",
                (str(run_id), _utc_iso(), job_id),
            )
            self.conn.execute(
                """
                UPDATE job_attempts
                SET run_id = ?
                WHERE job_id = ? AND attempt_no = (
                    SELECT attempt_count FROM jobs WHERE job_id = ?
                )
                """,
                (str(run_id), job_id, job_id),
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def _finish_attempt_tx(
        self,
        *,
        job_id: str,
        run_id: str,
        exit_code: int | None,
        sig: int | None,
        error_summary: str,
        line_count: int = 0,
        duration_ms: int = 0,
        last_stage: str = "",
        attempt_summary: dict[str, Any] | None = None,
        stdout_log_path: str = "",
        stderr_log_path: str = "",
    ) -> None:
        summary_payload = dict(attempt_summary or {})
        self.conn.execute(
            """
            UPDATE job_attempts
            SET finished_at = ?,
                exit_code = ?,
                signal = ?,
                error_summary = ?,
                run_id = COALESCE(?, run_id),
                line_count = ?,
                duration_ms = ?,
                last_stage = ?,
                summary_json = ?,
                stdout_log_path = COALESCE(NULLIF(?, ''), stdout_log_path),
                stderr_log_path = COALESCE(NULLIF(?, ''), stderr_log_path)
            WHERE job_id = ?
            AND attempt_no = (
                SELECT attempt_count FROM jobs WHERE job_id = ?
            )
            """,
            (
                _utc_iso(),
                exit_code,
                sig,
                str(error_summary or ""),
                str(run_id or "") or None,
                max(0, int(line_count)),
                max(0, int(duration_ms)),
                str(last_stage or ""),
                json.dumps(summary_payload, sort_keys=True),
                str(stdout_log_path or ""),
                str(stderr_log_path or ""),
                job_id,
                job_id,
            ),
        )

    def mark_completed(
        self,
        *,
        job_id: str,
        worker_id: str,
        run_id: str = "",
        exit_code: int = 0,
        line_count: int = 0,
        duration_ms: int = 0,
        last_stage: str = "",
        attempt_summary: dict[str, Any] | None = None,
        stdout_log_path: str = "",
        stderr_log_path: str = "",
    ) -> None:
        now_iso = _utc_iso()
        self._begin_immediate()
        try:
            self.conn.execute(
                """
                UPDATE jobs
                SET state = ?,
                    updated_at = ?,
                    completed_at = ?,
                    claimed_by = NULL,
                    lease_expires_at = NULL,
                    pid = NULL,
                    run_id = COALESCE(?, run_id),
                    last_exit_code = ?,
                    last_error = ''
                WHERE job_id = ?
                """,
                (
                    JOB_STATE_COMPLETED,
                    now_iso,
                    now_iso,
                    str(run_id or "") or None,
                    int(exit_code),
                    job_id,
                ),
            )
            self._finish_attempt_tx(
                job_id=job_id,
                run_id=run_id,
                exit_code=exit_code,
                sig=None,
                error_summary="",
                line_count=line_count,
                duration_ms=duration_ms,
                last_stage=last_stage,
                attempt_summary=attempt_summary,
                stdout_log_path=stdout_log_path,
                stderr_log_path=stderr_log_path,
            )
            self._append_event_tx(job_id, "job_completed", detail={"worker_id": worker_id, "run_id": run_id})
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def mark_canceled(
        self,
        *,
        job_id: str,
        worker_id: str,
        run_id: str = "",
        exit_code: int | None = None,
        sig: int | None = None,
        error_summary: str = "",
        line_count: int = 0,
        duration_ms: int = 0,
        last_stage: str = "",
        attempt_summary: dict[str, Any] | None = None,
        stdout_log_path: str = "",
        stderr_log_path: str = "",
    ) -> None:
        now_iso = _utc_iso()
        self._begin_immediate()
        try:
            self.conn.execute(
                """
                UPDATE jobs
                SET state = ?,
                    updated_at = ?,
                    completed_at = ?,
                    claimed_by = NULL,
                    lease_expires_at = NULL,
                    pid = NULL,
                    run_id = COALESCE(?, run_id),
                    last_exit_code = ?,
                    last_signal = ?,
                    last_error = ?
                WHERE job_id = ?
                """,
                (
                    JOB_STATE_CANCELED,
                    now_iso,
                    now_iso,
                    str(run_id or "") or None,
                    exit_code,
                    sig,
                    str(error_summary or ""),
                    job_id,
                ),
            )
            self._finish_attempt_tx(
                job_id=job_id,
                run_id=run_id,
                exit_code=exit_code,
                sig=sig,
                error_summary=str(error_summary or ""),
                line_count=line_count,
                duration_ms=duration_ms,
                last_stage=last_stage,
                attempt_summary=attempt_summary,
                stdout_log_path=stdout_log_path,
                stderr_log_path=stderr_log_path,
            )
            self._append_event_tx(job_id, "job_canceled", detail={"worker_id": worker_id})
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def mark_failed(
        self,
        *,
        job_id: str,
        worker_id: str,
        error_summary: str,
        exit_code: int | None,
        sig: int | None,
        retryable: bool,
        backoff_seconds: float,
        run_id: str = "",
        line_count: int = 0,
        duration_ms: int = 0,
        last_stage: str = "",
        attempt_summary: dict[str, Any] | None = None,
        stdout_log_path: str = "",
        stderr_log_path: str = "",
    ) -> None:
        now = _utc_now()
        now_iso = _utc_iso(now)

        self._begin_immediate()
        try:
            row = self.conn.execute("SELECT attempt_count, max_attempts, cancel_requested FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            if row is None:
                raise KeyError("job not found")

            attempts = int(row["attempt_count"] or 0)
            max_attempts = int(row["max_attempts"] or 1)
            cancel_requested = int(row["cancel_requested"] or 0) == 1

            should_retry = bool(retryable and not cancel_requested and attempts < max_attempts)
            if should_retry:
                retry_at = _utc_iso(now + timedelta(seconds=max(1.0, float(backoff_seconds))))
                self.conn.execute(
                    """
                    UPDATE jobs
                    SET state = ?,
                        updated_at = ?,
                        next_eligible_at = ?,
                        claimed_by = NULL,
                        lease_expires_at = NULL,
                        pid = NULL,
                        run_id = COALESCE(?, run_id),
                        last_error = ?,
                        last_exit_code = ?,
                        last_signal = ?
                    WHERE job_id = ?
                    """,
                    (
                        JOB_STATE_RETRY_WAIT,
                        now_iso,
                        retry_at,
                        str(run_id or "") or None,
                        str(error_summary or ""),
                        exit_code,
                        sig,
                        job_id,
                    ),
                )
                event_type = "job_retry_wait"
            else:
                self.conn.execute(
                    """
                    UPDATE jobs
                    SET state = ?,
                        updated_at = ?,
                        completed_at = ?,
                        claimed_by = NULL,
                        lease_expires_at = NULL,
                        pid = NULL,
                        run_id = COALESCE(?, run_id),
                        last_error = ?,
                        last_exit_code = ?,
                        last_signal = ?
                    WHERE job_id = ?
                    """,
                    (
                        JOB_STATE_FAILED,
                        now_iso,
                        now_iso,
                        str(run_id or "") or None,
                        str(error_summary or ""),
                        exit_code,
                        sig,
                        job_id,
                    ),
                )
                event_type = "job_failed"

            self._finish_attempt_tx(
                job_id=job_id,
                run_id=run_id,
                exit_code=exit_code,
                sig=sig,
                error_summary=str(error_summary or ""),
                line_count=line_count,
                duration_ms=duration_ms,
                last_stage=last_stage,
                attempt_summary=attempt_summary,
                stdout_log_path=stdout_log_path,
                stderr_log_path=stderr_log_path,
            )
            self._append_event_tx(
                job_id,
                event_type,
                detail={
                    "worker_id": worker_id,
                    "retryable": int(retryable),
                    "attempt_count": attempts,
                    "max_attempts": max_attempts,
                },
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def recover_expired_leases(
        self,
        *,
        is_pid_alive: Callable[[int], bool],
        run_status_lookup: Callable[[str, str], str | None],
    ) -> list[dict[str, Any]]:
        now = _utc_now()
        now_iso = _utc_iso(now)
        self._begin_immediate()
        recovered_ids: list[str] = []
        try:
            placeholders = ",".join("?" for _ in ACTIVE_JOB_STATES)
            rows = self.conn.execute(
                f"""
                SELECT *
                FROM jobs
                WHERE state IN ({placeholders})
                AND lease_expires_at IS NOT NULL
                AND lease_expires_at <= ?
                """,
                (*ACTIVE_JOB_STATES, now_iso),
            ).fetchall()

            for row in rows:
                job_id = str(row["job_id"])
                pid = int(row["pid"] or 0)
                run_db_path = str(row["run_db_path"] or "")
                run_id = str(row["run_id"] or "")
                attempts = int(row["attempt_count"] or 0)
                max_attempts = int(row["max_attempts"] or 1)

                if pid > 0 and is_pid_alive(pid):
                    self.conn.execute(
                        """
                        UPDATE jobs
                        SET state = ?, updated_at = ?, last_error = ?
                        WHERE job_id = ?
                        """,
                        (JOB_STATE_ORPHANED, now_iso, "lease expired while process still alive", job_id),
                    )
                    self._append_event_tx(job_id, "job_orphaned", detail={"pid": pid})
                    recovered_ids.append(job_id)
                    continue

                reconciled_state = None
                if run_db_path and run_id:
                    status = run_status_lookup(run_db_path, run_id)
                    if status == JOB_STATE_COMPLETED:
                        reconciled_state = JOB_STATE_COMPLETED
                    elif status == JOB_STATE_CANCELED:
                        reconciled_state = JOB_STATE_CANCELED

                if reconciled_state == JOB_STATE_COMPLETED:
                    self.conn.execute(
                        """
                        UPDATE jobs
                        SET state = ?, updated_at = ?, completed_at = ?, claimed_by = NULL,
                            lease_expires_at = NULL, pid = NULL
                        WHERE job_id = ?
                        """,
                        (JOB_STATE_COMPLETED, now_iso, now_iso, job_id),
                    )
                    self._append_event_tx(job_id, "job_recovered_completed")
                elif reconciled_state == JOB_STATE_CANCELED:
                    self.conn.execute(
                        """
                        UPDATE jobs
                        SET state = ?, updated_at = ?, completed_at = ?, claimed_by = NULL,
                            lease_expires_at = NULL, pid = NULL
                        WHERE job_id = ?
                        """,
                        (JOB_STATE_CANCELED, now_iso, now_iso, job_id),
                    )
                    self._append_event_tx(job_id, "job_recovered_canceled")
                elif attempts < max_attempts:
                    retry_at = _utc_iso(now + timedelta(seconds=compute_queue_retry_backoff(attempts + 1)))
                    self.conn.execute(
                        """
                        UPDATE jobs
                        SET state = ?, updated_at = ?, next_eligible_at = ?,
                            claimed_by = NULL, lease_expires_at = NULL, pid = NULL,
                            last_error = ?
                        WHERE job_id = ?
                        """,
                        (JOB_STATE_RETRY_WAIT, now_iso, retry_at, "lease expired; retry scheduled", job_id),
                    )
                    self._append_event_tx(job_id, "job_recovered_retry_wait")
                else:
                    self.conn.execute(
                        """
                        UPDATE jobs
                        SET state = ?, updated_at = ?, completed_at = ?, claimed_by = NULL,
                            lease_expires_at = NULL, pid = NULL,
                            last_error = ?
                        WHERE job_id = ?
                        """,
                        (JOB_STATE_FAILED, now_iso, now_iso, "lease expired; attempts exhausted", job_id),
                    )
                    self._append_event_tx(job_id, "job_recovered_failed")

                recovered_ids.append(job_id)

            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

        recovered: list[dict[str, Any]] = []
        for job_id in recovered_ids:
            payload = self.get_job(job_id)
            if payload is not None:
                recovered.append(payload)
        return recovered

    def list_events(self, job_id: str, *, limit: int = 100) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT event_id, job_id, event_time, event_type, message, detail_json
            FROM job_events
            WHERE job_id = ?
            ORDER BY event_id DESC
            LIMIT ?
            """,
            (job_id, max(1, int(limit))),
        ).fetchall()
        events: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            try:
                payload["detail"] = json.loads(str(payload.get("detail_json") or "{}"))
            except json.JSONDecodeError:
                payload["detail"] = {}
            events.append(payload)
        return events


def pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def resolve_run_status(run_db_path: str, run_id: str) -> str | None:
    db_path = str(run_db_path or "").strip()
    if not db_path or not run_id:
        return None
    path = Path(db_path)
    if not path.exists():
        return None

    try:
        conn = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only = 1")
        row = conn.execute("SELECT status FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        conn.close()
    except sqlite3.DatabaseError:
        return None

    if row is None:
        return None

    raw = str(row["status"] or "").strip().lower()
    if not raw:
        return None
    if raw == "completed":
        return JOB_STATE_COMPLETED
    if raw in {"failed", "error"}:
        return JOB_STATE_FAILED
    if raw in {"canceled", "cancelled"}:
        return JOB_STATE_CANCELED
    return raw


def extract_signal_from_return_code(return_code: int | None) -> int | None:
    if return_code is None:
        return None
    if return_code >= 0:
        return None
    sig = -int(return_code)
    if sig <= 0:
        return None
    return sig if sig in {int(sig_item) for sig_item in signal.Signals} else sig
