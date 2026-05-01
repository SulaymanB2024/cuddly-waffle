from __future__ import annotations

import csv
import io
import json
import mimetypes
import os
import re
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from statistics import median
from typing import Any, Callable
from urllib.parse import parse_qs, urlsplit

from seo_audit.job_queue import (
  JOB_STATE_CANCELED,
  JOB_STATE_CANCEL_REQUESTED,
  JOB_STATE_COMPLETED,
  JOB_STATE_FAILED,
  JOB_STATE_LEASED,
  JOB_STATE_ORPHANED,
  JOB_STATE_PENDING,
  JOB_STATE_RETRY_WAIT,
  JOB_STATE_RUNNING,
  JOB_STATE_STARTING,
  QueueStore,
  pid_is_alive,
  resolve_run_status,
)
from seo_audit.queue_worker import run_queue_worker

DEFAULT_PAGE_SIZE = 25
MAX_PAGE_SIZE = 250
MAX_EXPORT_ROWS = 10000
MAX_QUERY_ROWS = 1000
MAX_QUERY_LENGTH = 6000

MONEY_PAGE_TYPES = {"homepage", "service", "location", "product", "contact"}

QUERY_PREFIX_PATTERN = re.compile(r"^\s*(select|with)\b", flags=re.IGNORECASE)
QUERY_MUTATION_PATTERN = re.compile(
  r"\b(insert|update|delete|drop|alter|create|replace|attach|detach|vacuum|reindex|analyze|pragma|begin|commit|rollback)\b",
  flags=re.IGNORECASE,
)
STAGE_PROGRESS_PATTERN = re.compile(r"^\[(\d+)/(\d+)\]\s*(.*)$")
STAGE_DONE_PATTERN = re.compile(r"^\[(\d+)/(\d+)\]\s*done\s+(\S+)\s*$", flags=re.IGNORECASE)


def _coerce_int(
    value: str | None,
    *,
    name: str,
    default: int,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    if value is None or value == "":
        parsed = default
    else:
        try:
            parsed = int(value)
        except ValueError as exc:
            raise ValueError(f"{name} must be an integer") from exc
    if minimum is not None and parsed < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    if maximum is not None and parsed > maximum:
        raise ValueError(f"{name} must be <= {maximum}")
    return parsed


def _coerce_optional_int(value: str | None, *, name: str) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


def _coerce_sort_direction(value: str | None, *, default: str = "asc") -> str:
    normalized = (value or default).strip().lower()
    if normalized not in {"asc", "desc"}:
        raise ValueError("sort_dir must be asc or desc")
    return normalized


def _normalize_read_only_query(query_text: str) -> str:
    query = str(query_text or "").strip()
    if not query:
        raise ValueError("query is required")
    if len(query) > MAX_QUERY_LENGTH:
        raise ValueError(f"query exceeds {MAX_QUERY_LENGTH} characters")

    if ";" in query:
        stripped = query.rstrip()
        if stripped.endswith(";"):
            stripped = stripped[:-1].rstrip()
        if ";" in stripped:
            raise ValueError("query must contain a single statement")
        query = stripped

    if not QUERY_PREFIX_PATTERN.match(query):
        raise ValueError("query must begin with SELECT or WITH")
    if QUERY_MUTATION_PATTERN.search(query):
        raise ValueError("query must be read-only")
    return query


def _parse_json_object(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return parsed


def _serialize_payload(payload: Any) -> bytes:
    return json.dumps(payload, sort_keys=True).encode("utf-8")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _as_float(value: Any, default: float = 0.0) -> float:
  try:
    return float(value)
  except (TypeError, ValueError):
    return default


def _percentile(values: list[float], quantile: float) -> float:
  if not values:
    return 0.0
  q = min(1.0, max(0.0, float(quantile)))
  ordered = sorted(values)
  if len(ordered) == 1:
    return float(ordered[0])

  index = (len(ordered) - 1) * q
  lower = int(index)
  upper = min(len(ordered) - 1, lower + 1)
  fraction = index - lower
  return float((ordered[lower] * (1.0 - fraction)) + (ordered[upper] * fraction))


def _sqlite_read_only_uri(db_path: Path) -> str:
  return f"file:{db_path.resolve().as_posix()}?mode=ro"


def _score_dimension_snapshot(page: dict[str, Any]) -> dict[str, Any]:
  local_score = page.get("local_seo_score")
  perf_score = page.get("performance_score")
  return {
    "crawlability_score": page.get("crawlability_score"),
    "onpage_score": page.get("onpage_score"),
    "render_risk_score": page.get("render_risk_score"),
    "internal_linking_score": page.get("internal_linking_score"),
    "local_seo_score": None if local_score in {-1, None, ""} else local_score,
    "performance_score": None if perf_score in {-1, None, ""} else perf_score,
  }


def _fallback_score_explanation(page: dict[str, Any]) -> dict[str, Any]:
  score_cap = int(page.get("score_cap") or 100)
  overall_score = int(page.get("overall_score") or 0)
  scoring_model_version = str(page.get("scoring_model_version") or page.get("score_version") or "1.0.0")
  scoring_profile = str(page.get("scoring_profile") or page.get("score_profile") or "default")
  return {
    "scoring_model_version": scoring_model_version,
    "scoring_profile": scoring_profile,
    "score_version": scoring_model_version,
    "score_profile": scoring_profile,
    "dimensions": {
      "scores": _score_dimension_snapshot(page),
      "quality_score": int(page.get("quality_score") or 0),
      "coverage_score": int(page.get("coverage_score") or 0),
    },
    "risk": {
      "score": int(page.get("risk_score") or 0),
      "notable_contributors": [],
      "family_totals": {},
      "top_risk_families": [],
      "neutralized_codes": [],
    },
    "cap": {
      "score_cap": score_cap,
      "reasons": [],
    },
    "overall": {
      "overall_score": overall_score,
      "adjusted_quality": float(overall_score),
    },
  }


def _coerce_score_explanation(page: dict[str, Any]) -> dict[str, Any]:
  parsed = _parse_json_object(str(page.get("score_explanation_json") or page.get("explanation_json") or "{}"))
  if not parsed:
    return _fallback_score_explanation(page)

  scoring_model_version = str(page.get("scoring_model_version") or page.get("score_version") or "1.0.0")
  scoring_profile = str(page.get("scoring_profile") or page.get("score_profile") or "default")

  parsed.setdefault("scoring_model_version", scoring_model_version)
  parsed.setdefault("scoring_profile", scoring_profile)
  parsed.setdefault("score_version", scoring_model_version)
  parsed.setdefault("score_profile", scoring_profile)

  dimensions = parsed.setdefault("dimensions", {})
  if isinstance(dimensions, dict):
    dimensions.setdefault("scores", _score_dimension_snapshot(page))
    dimensions.setdefault("quality_score", int(page.get("quality_score") or 0))
    dimensions.setdefault("coverage_score", int(page.get("coverage_score") or 0))

  risk = parsed.setdefault("risk", {})
  if isinstance(risk, dict):
    risk.setdefault("score", int(page.get("risk_score") or 0))
    risk.setdefault("notable_contributors", [])
    risk.setdefault("family_totals", {})
    risk.setdefault("top_risk_families", [])
    risk.setdefault("neutralized_codes", [])

  cap = parsed.setdefault("cap", {})
  if isinstance(cap, dict):
    cap.setdefault("score_cap", int(page.get("score_cap") or 100))
    cap.setdefault("reasons", [])

  overall = parsed.setdefault("overall", {})
  if isinstance(overall, dict):
    overall.setdefault("overall_score", int(page.get("overall_score") or 0))
    overall.setdefault("adjusted_quality", float(page.get("overall_score") or 0))

  return parsed


@dataclass(slots=True)
class AuditJob:
  job_id: str
  domain: str
  output_dir: str
  run_profile: str
  max_pages: int
  render_mode: str
  max_render_pages: int
  performance_targets: int
  screenshot_count: int
  started_at: str = field(default_factory=_utc_now_iso)
  completed_at: str = ""
  status: str = "queued"
  progress_percent: int = 0
  current_stage: str = ""
  run_id: str = ""
  lines: list[str] = field(default_factory=list)
  screenshots: list[dict[str, str]] = field(default_factory=list)
  error: str = ""

  def to_dict(self) -> dict[str, Any]:
    return {
      "job_id": self.job_id,
      "domain": self.domain,
      "output_dir": self.output_dir,
      "run_profile": self.run_profile,
      "max_pages": self.max_pages,
      "render_mode": self.render_mode,
      "max_render_pages": self.max_render_pages,
      "performance_targets": self.performance_targets,
      "screenshot_count": self.screenshot_count,
      "started_at": self.started_at,
      "completed_at": self.completed_at,
      "status": self.status,
      "progress_percent": self.progress_percent,
      "current_stage": self.current_stage,
      "run_id": self.run_id,
      "lines": list(self.lines),
      "screenshots": list(self.screenshots),
      "error": self.error,
    }


class AuditJobManager:
  QUEUED_STATES = {JOB_STATE_PENDING, JOB_STATE_RETRY_WAIT}
  RUNNING_STATES = {
    JOB_STATE_LEASED,
    JOB_STATE_STARTING,
    JOB_STATE_RUNNING,
    "completing",
    JOB_STATE_CANCEL_REQUESTED,
    JOB_STATE_ORPHANED,
  }

  def __init__(
    self,
    db_path: Path,
    project_root: Path,
    *,
    queue_db_path: Path | None = None,
    embedded_worker: bool | None = None,
  ) -> None:
    self.db_path = db_path.resolve()
    self.output_dir = self.db_path.parent.resolve()
    self.project_root = project_root.resolve()
    self.queue_db_path = (queue_db_path or (self.output_dir / "queue.sqlite")).resolve()
    self._store_lock = threading.Lock()
    self._embedded_worker_thread: threading.Thread | None = None

    self._store_call(lambda store: store.init_db())
    self._recover_startup_orphans()

    should_embed = self._resolve_embedded_worker(embedded_worker)
    if should_embed:
      self._start_embedded_worker()

  def _resolve_embedded_worker(self, embedded_worker: bool | None) -> bool:
    if embedded_worker is not None:
      return bool(embedded_worker)
    raw = str(os.environ.get("SEO_AUDIT_DASHBOARD_EMBEDDED_WORKER", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}

  def _store_call(self, callback: Callable[[QueueStore], Any]) -> Any:
    with self._store_lock:
      store = QueueStore(self.queue_db_path)
      try:
        store.init_db()
        return callback(store)
      finally:
        store.close()

  def _recover_startup_orphans(self) -> None:
    def _recover(store: QueueStore) -> None:
      store.recover_expired_leases(is_pid_alive=pid_is_alive, run_status_lookup=resolve_run_status)

    self._store_call(_recover)

  def _start_embedded_worker(self) -> None:
    if self._embedded_worker_thread is not None and self._embedded_worker_thread.is_alive():
      return

    worker_id = f"dashboard-{uuid.uuid4()}"

    def _worker_loop() -> None:
      try:
        run_queue_worker(queue_db=self.queue_db_path, worker_id=worker_id)
      except Exception:
        return

    self._embedded_worker_thread = threading.Thread(target=_worker_loop, daemon=True)
    self._embedded_worker_thread.start()

  def _status_from_queue_state(self, queue_state: str) -> str:
    state = str(queue_state or "").strip().lower()
    if state in self.QUEUED_STATES:
      return "queued"
    if state in self.RUNNING_STATES:
      return "running"
    if state == JOB_STATE_COMPLETED:
      return "completed"
    if state in {JOB_STATE_FAILED, JOB_STATE_CANCELED}:
      return "failed"
    return "queued"

  def _default_stage_for_state(self, queue_state: str) -> str:
    state = str(queue_state or "").strip().lower()
    if state == JOB_STATE_RETRY_WAIT:
      return "Waiting to retry"
    if state == JOB_STATE_CANCEL_REQUESTED:
      return "Cancellation requested"
    if state == JOB_STATE_CANCELED:
      return "Audit canceled"
    if state == JOB_STATE_COMPLETED:
      return "Audit finished"
    if state == JOB_STATE_FAILED:
      return "Audit failed"
    if state in self.RUNNING_STATES:
      return "Audit in progress"
    return "Queued"

  def _tail_lines(self, path_text: str, *, limit: int) -> list[str]:
    if limit <= 0:
      return []
    path_raw = str(path_text or "").strip()
    if not path_raw:
      return []
    path = Path(path_raw)
    if not path.exists():
      return []
    try:
      lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
      return []
    return lines[-limit:]

  def _progress_from_lines(self, lines: list[str]) -> tuple[int, str, str]:
    progress = 0
    current_stage = ""
    parsed_run_id = ""
    for line in lines:
      text = str(line or "").strip()
      if not text:
        continue
      stage_match = STAGE_PROGRESS_PATTERN.match(text)
      if stage_match:
        stage_num = int(stage_match.group(1) or 0)
        stage_total = int(stage_match.group(2) or 0)
        if stage_total > 0:
          progress = max(progress, min(99, int((stage_num / float(stage_total)) * 100)))
        stage_text = str(stage_match.group(3) or "").strip()
        if stage_text:
          current_stage = stage_text

      done_match = STAGE_DONE_PATTERN.match(text)
      if done_match:
        candidate = str(done_match.group(3) or "").strip()
        if candidate:
          parsed_run_id = candidate

    if not current_stage and lines:
      current_stage = str(lines[-1]).strip()
    return progress, current_stage, parsed_run_id

  def _run_event_progress(self, run_id: str) -> tuple[int | None, str]:
    if not run_id:
      return None, ""

    try:
      with sqlite3.connect(_sqlite_read_only_uri(self.db_path), uri=True) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only = 1")
        rows = conn.execute(
          """
          SELECT stage
          FROM run_events
          WHERE run_id = ? AND event_type = 'stage_timing'
          ORDER BY event_id ASC
          """,
          (run_id,),
        ).fetchall()
    except sqlite3.DatabaseError:
      return None, ""

    if not rows:
      return None, ""

    done_count = len(rows)
    latest_stage = str(rows[-1]["stage"] or "").strip()
    progress = min(99, int((done_count / 10.0) * 100.0))
    stage_text = f"completed {latest_stage}" if latest_stage else "processing"
    return progress, stage_text

  def _shape_job(
    self,
    job: dict[str, Any],
    *,
    attempt: dict[str, Any] | None,
    include_lines: bool,
  ) -> dict[str, Any]:
    queue_state = str(job.get("state") or "").strip().lower()
    status = self._status_from_queue_state(queue_state)

    line_limit = 250 if include_lines else 0
    stdout_log_path = str((attempt or {}).get("stdout_log_path") or "")
    lines = self._tail_lines(stdout_log_path, limit=line_limit)
    line_progress, line_stage, parsed_run_id = self._progress_from_lines(lines)

    run_id = str(job.get("run_id") or (attempt or {}).get("run_id") or parsed_run_id or "")
    progress = max(0, line_progress)
    current_stage = line_stage or self._default_stage_for_state(queue_state)

    event_progress, event_stage = self._run_event_progress(run_id)
    if event_progress is not None:
      progress = max(progress, int(event_progress))
      if event_stage:
        current_stage = event_stage

    error_text = str(job.get("last_error") or (attempt or {}).get("error_summary") or "")

    if queue_state == JOB_STATE_COMPLETED:
      progress = 100
      current_stage = "Audit finished"
      error_text = ""
    elif queue_state == JOB_STATE_CANCELED:
      progress = min(99, max(progress, 1))
      current_stage = "Audit canceled"
      if not error_text:
        error_text = "Audit canceled"
    elif queue_state == JOB_STATE_FAILED:
      progress = min(99, max(progress, 1))
      current_stage = "Audit failed"
    elif queue_state in self.QUEUED_STATES:
      progress = max(progress, 0)
      current_stage = current_stage or "Queued"
    elif queue_state in self.RUNNING_STATES:
      progress = min(99, max(progress, 1))
      current_stage = current_stage or "Audit in progress"

    attempt_payload: dict[str, Any] | None = None
    if attempt is not None:
      attempt_payload = {
        "attempt_no": int(attempt.get("attempt_no") or 0),
        "started_at": str(attempt.get("started_at") or ""),
        "finished_at": str(attempt.get("finished_at") or ""),
        "exit_code": attempt.get("exit_code"),
        "signal": attempt.get("signal"),
        "worker_id": str(attempt.get("worker_id") or ""),
        "pid": int(attempt.get("pid") or 0),
        "stdout_log_path": str(attempt.get("stdout_log_path") or ""),
        "stderr_log_path": str(attempt.get("stderr_log_path") or ""),
        "line_count": int(attempt.get("line_count") or 0),
        "duration_ms": int(attempt.get("duration_ms") or 0),
        "last_stage": str(attempt.get("last_stage") or ""),
        "error_summary": str(attempt.get("error_summary") or ""),
        "summary": dict(attempt.get("summary") or {}),
      }

    screenshots = self.screenshots_for_run(run_id) if include_lines and run_id else []

    payload = {
      "job_id": str(job.get("job_id") or ""),
      "domain": str(job.get("domain") or ""),
      "output_dir": str(job.get("output_dir") or str(self.output_dir)),
      "run_profile": str((job.get("config") or {}).get("run_profile") or "standard"),
      "max_pages": int((job.get("config") or {}).get("max_pages") or 0),
      "render_mode": str((job.get("config") or {}).get("render_mode") or "none"),
      "max_render_pages": int((job.get("config") or {}).get("max_render_pages") or 0),
      "performance_targets": int((job.get("config") or {}).get("performance_targets") or 0),
      "screenshot_count": int((job.get("config") or {}).get("screenshot_count") or 0),
      "started_at": str(job.get("submitted_at") or ""),
      "completed_at": str(job.get("completed_at") or ""),
      "status": status,
      "queue_state": queue_state,
      "progress_percent": int(progress),
      "current_stage": current_stage,
      "run_id": run_id,
      "lines": lines,
      "screenshots": screenshots,
      "error": error_text,
      "last_error": str(job.get("last_error") or ""),
      "last_exit_code": job.get("last_exit_code"),
      "last_signal": job.get("last_signal"),
      "attempt_count": int(job.get("attempt_count") or 0),
      "max_attempts": int(job.get("max_attempts") or 0),
      "next_eligible_at": str(job.get("next_eligible_at") or ""),
      "cancel_requested": bool(int(job.get("cancel_requested") or 0)),
      "attempt": attempt_payload,
    }
    return payload

  def jobs(self) -> dict[str, Any]:
    def _load(store: QueueStore) -> list[tuple[dict[str, Any], dict[str, Any] | None]]:
      rows = store.list_jobs(limit=100)
      return [(row, store.get_latest_attempt(str(row.get("job_id") or ""))) for row in rows]

    rows = self._store_call(_load)
    payload = [
      self._shape_job(job, attempt=attempt, include_lines=False)
      for job, attempt in rows
    ]
    return {"jobs": payload}

  def job_status(self, job_id: str) -> dict[str, Any]:
    def _load(store: QueueStore) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
      job = store.get_job(job_id)
      attempt = store.get_latest_attempt(job_id) if job is not None else None
      return job, attempt

    job, attempt = self._store_call(_load)
    if job is None:
      raise KeyError("job not found")
    return self._shape_job(job, attempt=attempt, include_lines=True)

  def start_job(
    self,
    *,
    domain: str,
    run_profile: str,
    max_pages: int,
    render_mode: str,
    max_render_pages: int,
    performance_targets: int,
    screenshot_count: int,
  ) -> dict[str, Any]:
    config = {
      "domain": domain,
      "run_profile": run_profile,
      "max_pages": int(max_pages),
      "render_mode": render_mode,
      "max_render_pages": int(max_render_pages),
      "performance_targets": int(performance_targets),
      "screenshot_count": int(screenshot_count),
    }

    def _enqueue(store: QueueStore) -> tuple[dict[str, Any], dict[str, Any] | None]:
      job = store.enqueue_job(
        domain=domain,
        output_dir=str(self.output_dir),
        config=config,
      )
      attempt = store.get_latest_attempt(str(job.get("job_id") or ""))
      return job, attempt

    job, attempt = self._store_call(_enqueue)
    return self._shape_job(job, attempt=attempt, include_lines=True)

  def cancel_job(self, job_id: str) -> dict[str, Any]:
    def _cancel(store: QueueStore) -> tuple[dict[str, Any], dict[str, Any] | None]:
      job = store.request_cancel(job_id)
      attempt = store.get_latest_attempt(job_id)
      return job, attempt

    job, attempt = self._store_call(_cancel)
    return self._shape_job(job, attempt=attempt, include_lines=True)

  def screenshots_for_run(self, run_id: str) -> list[dict[str, str]]:
    index_path = self.output_dir / "screenshots" / run_id / "index.json"
    if not index_path.exists():
      return []
    try:
      parsed = json.loads(index_path.read_text(encoding="utf-8"))
    except Exception:
      return []
    if not isinstance(parsed, list):
      return []
    return [item for item in parsed if isinstance(item, dict)]



class DashboardStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(_sqlite_read_only_uri(self.db_path), uri=True)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only = 1")
        return conn

    def _fetchall(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def _fetchone(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(sql, params).fetchone()
        return dict(row) if row is not None else None

    def _count(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        row = self._fetchone(sql, params)
        if not row:
            return 0
        return int(row.get("count") or 0)

    def _run_row(self, run_id: str) -> dict[str, Any]:
        run = self._fetchone("SELECT * FROM runs WHERE run_id = ?", (run_id,))
        if run is None:
            raise KeyError("run not found")
        config = _parse_json_object(str(run.get("config_json") or "{}"))
        run["run_profile"] = str(config.get("run_profile") or "standard")
        run["crawl_persona"] = str(config.get("crawl_persona") or "googlebot_smartphone")
        return run

    def list_runs(self, limit: int = 50) -> dict[str, Any]:
        rows = self._fetchall(
            """
            SELECT run_id, started_at, completed_at, domain, status, notes, config_json
            FROM runs
            ORDER BY started_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        for row in rows:
            config = _parse_json_object(str(row.get("config_json") or "{}"))
            row["run_profile"] = str(config.get("run_profile") or "standard")
            row["crawl_persona"] = str(config.get("crawl_persona") or "googlebot_smartphone")
            row.pop("config_json", None)
        default_run_id = rows[0]["run_id"] if rows else ""
        return {"runs": rows, "default_run_id": default_run_id}

    def summary(self, run_id: str) -> dict[str, Any]:
        run = self._run_row(run_id)
        counts = {
            "pages": self._count("SELECT COUNT(*) AS count FROM pages WHERE run_id = ?", (run_id,)),
            "issues": self._count("SELECT COUNT(*) AS count FROM issues WHERE run_id = ?", (run_id,)),
            "scores": self._count("SELECT COUNT(*) AS count FROM scores WHERE run_id = ?", (run_id,)),
            "performance": self._count(
                "SELECT COUNT(*) AS count FROM performance_metrics WHERE run_id = ?", (run_id,)
            ),
            "crux": self._count("SELECT COUNT(*) AS count FROM crux_metrics WHERE run_id = ?", (run_id,)),
        }

        severity_rows = self._fetchall(
            "SELECT severity, COUNT(*) AS count FROM issues WHERE run_id = ? GROUP BY severity",
            (run_id,),
        )
        severity_counts = {str(row["severity"]): int(row["count"]) for row in severity_rows}

        issue_code_rows = self._fetchall(
            """
            SELECT issue_code, COUNT(*) AS count
            FROM issues
            WHERE run_id = ?
            GROUP BY issue_code
            ORDER BY count DESC, issue_code ASC
            LIMIT 15
            """,
            (run_id,),
        )
        top_issue_codes = [
            {"issue_code": str(row["issue_code"]), "count": int(row["count"])} for row in issue_code_rows
        ]

        score_rows = self._fetchall(
            "SELECT overall_score FROM scores WHERE run_id = ? AND overall_score IS NOT NULL", (run_id,)
        )
        values = [int(row["overall_score"]) for row in score_rows]
        score_summary = {
            "avg": round(sum(values) / len(values), 2) if values else None,
            "median": int(median(values)) if values else None,
            "min": min(values) if values else None,
            "max": max(values) if values else None,
        }

        stage_rows = self._fetchall(
            """
            SELECT stage, elapsed_ms
            FROM run_events
            WHERE run_id = ? AND event_type = 'stage_timing'
            ORDER BY event_id ASC
            """,
            (run_id,),
        )
        stage_timings = [
            {"stage": str(row["stage"]), "elapsed_ms": int(row["elapsed_ms"] or 0)} for row in stage_rows
        ]

        provider_rows = self._fetchall(
            """
            SELECT stage, detail_json
            FROM run_events
            WHERE run_id = ? AND event_type = 'provider_summary'
            ORDER BY event_id ASC
            """,
            (run_id,),
        )
        providers: list[dict[str, Any]] = []
        for row in provider_rows:
            detail = _parse_json_object(str(row.get("detail_json") or "{}"))
            providers.append(
                {
                    "provider": str(row["stage"]),
                    "attempts": int(detail.get("attempts") or 0),
                    "http_attempts": int(detail.get("http_attempts") or 0),
                    "retries": int(detail.get("retries") or 0),
                    "timeouts": int(detail.get("timeouts") or 0),
                    "wait_seconds": float(detail.get("wait_seconds") or 0.0),
                    "success": int(detail.get("success") or 0),
                    "no_data": int(detail.get("no_data") or 0),
                    "failed_http": int(detail.get("failed_http") or 0),
                    "skipped_missing_key": int(detail.get("skipped_missing_key") or 0),
                }
            )

        schema_rows = self._fetchall(
            "SELECT schema_validation_json FROM pages WHERE run_id = ?",
            (run_id,),
        )
        schema_audit = {
            "syntax_valid_pages": 0,
            "eligible_feature_pages": 0,
            "deprecated_markup_pages": 0,
            "visible_mismatch_pages": 0,
        }
        for row in schema_rows:
            payload = _parse_json_object(str(row.get("schema_validation_json") or "{}"))
            if bool(payload.get("syntax_valid", True)):
                schema_audit["syntax_valid_pages"] += 1
            eligible = payload.get("eligible_features")
            if isinstance(eligible, list) and eligible:
                schema_audit["eligible_feature_pages"] += 1
            deprecated = payload.get("deprecated_features")
            if isinstance(deprecated, list) and deprecated:
                schema_audit["deprecated_markup_pages"] += 1
            mismatches = payload.get("visible_content_mismatches")
            if isinstance(mismatches, list) and mismatches:
                schema_audit["visible_mismatch_pages"] += 1

        ai_rows = self._fetchall(
            "SELECT ai_discoverability_potential_score, ai_visibility_json FROM pages WHERE run_id = ?",
            (run_id,),
        )
        ai_potential_scores: list[int] = []
        ai_observed_signal_pages = 0
        for row in ai_rows:
            ai_potential_scores.append(int(row.get("ai_discoverability_potential_score") or 0))
            payload = _parse_json_object(str(row.get("ai_visibility_json") or "{}"))
            observed = payload.get("observed_evidence")
            if isinstance(observed, dict):
                impressions = int(observed.get("gsc_impressions") or 0)
                clicks = int(observed.get("gsc_clicks") or 0)
                referrals = int(observed.get("chatgpt_referrals") or 0)
                if impressions > 0 or clicks > 0 or referrals > 0:
                    ai_observed_signal_pages += 1

        ai_audit = {
            "avg_potential_score": (
                round(sum(ai_potential_scores) / len(ai_potential_scores), 2)
                if ai_potential_scores
                else None
            ),
            "observed_signal_pages": ai_observed_signal_pages,
            "pages_with_ai_payload": len(ai_rows),
        }

        offsite_summary = self._fetchone(
          """
          SELECT *
          FROM offsite_commoncrawl_summary
          WHERE run_id = ?
          ORDER BY offsite_summary_id DESC
          LIMIT 1
          """,
          (run_id,),
        )
        offsite_payload: dict[str, Any] = {
          "summary": offsite_summary or {},
          "comparison_domains": [],
          "competitors": [],
          "linking_domains": [],
        }
        if offsite_summary:
          try:
            offsite_payload["comparison_domains"] = self._fetchall(
              """
              SELECT compare_domain, cc_release, harmonic_centrality, pagerank, rank_gap_vs_target, pagerank_gap_vs_target
              FROM offsite_commoncrawl_comparisons
              WHERE run_id = ?
              ORDER BY
                COALESCE(harmonic_centrality, 0.0) DESC,
                COALESCE(pagerank, 0.0) DESC,
                compare_domain ASC
              LIMIT 15
              """,
              (run_id,),
            )
          except sqlite3.OperationalError:
            offsite_payload["comparison_domains"] = self._fetchall(
              """
              SELECT competitor_domain AS compare_domain, cc_release, harmonic_centrality, pagerank, rank_gap_vs_target, pagerank_gap_vs_target
              FROM offsite_commoncrawl_competitors
              WHERE run_id = ?
              ORDER BY
                COALESCE(harmonic_centrality, 0.0) DESC,
                COALESCE(pagerank, 0.0) DESC,
                competitor_domain ASC
              LIMIT 15
              """,
              (run_id,),
            )
          offsite_payload["linking_domains"] = self._fetchall(
            """
            SELECT linking_domain, source_num_hosts, source_harmonic_centrality, source_pagerank, rank_bucket
            FROM offsite_commoncrawl_linking_domains
            WHERE run_id = ?
            ORDER BY
              COALESCE(source_harmonic_centrality, 0.0) DESC,
              COALESCE(source_pagerank, 0.0) DESC,
              COALESCE(source_num_hosts, 0) DESC,
              linking_domain ASC
            LIMIT 20
            """,
            (run_id,),
          )
          offsite_payload["competitors"] = list(offsite_payload["comparison_domains"])

        return {
            "run": {
                "run_id": run["run_id"],
                "domain": run["domain"],
                "status": run["status"],
                "started_at": run["started_at"],
                "completed_at": run.get("completed_at"),
                "notes": run.get("notes", ""),
                "run_profile": run["run_profile"],
                "crawl_persona": run["crawl_persona"],
            },
            "counts": counts,
            "severity_counts": severity_counts,
            "top_issue_codes": top_issue_codes,
            "score_summary": score_summary,
            "stage_timings": stage_timings,
            "provider_telemetry": providers,
            "schema_audit": schema_audit,
            "ai_audit": ai_audit,
            "offsite_commoncrawl": offsite_payload,
        }

    def _build_pages_where(
        self,
        run_id: str,
        filters: dict[str, Any],
    ) -> tuple[str, list[Any]]:
        where = ["p.run_id = ?"]
        params: list[Any] = [run_id]

        page_type = str(filters.get("page_type") or "").strip()
        if page_type:
            where.append("p.page_type = ?")
            params.append(page_type)

        status_code = filters.get("status_code")
        if status_code is not None:
            where.append("p.status_code = ?")
            params.append(int(status_code))

        max_depth = filters.get("max_depth")
        if max_depth is not None:
            where.append("COALESCE(p.crawl_depth, 999) <= ?")
            params.append(int(max_depth))

        min_score = filters.get("min_score")
        if min_score is not None:
            where.append("COALESCE(s.overall_score, 0) >= ?")
            params.append(int(min_score))

        severity = str(filters.get("severity") or "").strip().lower()
        if severity:
            where.append(
                """
                EXISTS (
                    SELECT 1
                    FROM issues i
                    WHERE i.run_id = p.run_id
                    AND i.url = p.normalized_url
                    AND i.severity = ?
                )
                """
            )
            params.append(severity)

        query = str(filters.get("query") or "").strip().lower()
        if query:
            pattern = f"%{query}%"
            where.append(
                "(" 
                "LOWER(p.normalized_url) LIKE ? "
                "OR LOWER(COALESCE(p.title, '')) LIKE ? "
                "OR LOWER(COALESCE(p.h1, '')) LIKE ?"
                ")"
            )
            params.extend([pattern, pattern, pattern])

        return " AND ".join(where), params

    def _build_issues_where(
        self,
        run_id: str,
        filters: dict[str, Any],
    ) -> tuple[str, list[Any]]:
        where = ["i.run_id = ?"]
        params: list[Any] = [run_id]

        severity = str(filters.get("severity") or "").strip().lower()
        if severity:
            where.append("i.severity = ?")
            params.append(severity)

        issue_code = str(filters.get("issue_code") or "").strip().upper()
        if issue_code:
            where.append("i.issue_code = ?")
            params.append(issue_code)

        page_type = str(filters.get("page_type") or "").strip()
        if page_type:
            where.append("COALESCE(p.page_type, 'other') = ?")
            params.append(page_type)

        status_code = filters.get("status_code")
        if status_code is not None:
            where.append("COALESCE(p.status_code, 0) = ?")
            params.append(int(status_code))

        certainty_state = str(filters.get("certainty_state") or "").strip()
        if certainty_state:
            where.append("COALESCE(i.certainty_state, 'Verified') = ?")
            params.append(certainty_state)

        min_priority = filters.get("min_priority")
        if min_priority is not None:
            where.append("COALESCE(i.priority_score, 0) >= ?")
            params.append(int(min_priority))

        query = str(filters.get("query") or "").strip().lower()
        if query:
            pattern = f"%{query}%"
            where.append(
                "(" 
                "LOWER(i.url) LIKE ? "
                "OR LOWER(i.title) LIKE ? "
                "OR LOWER(i.description) LIKE ?"
                ")"
            )
            params.extend([pattern, pattern, pattern])

        return " AND ".join(where), params

    def list_pages(
        self,
        run_id: str,
        filters: dict[str, Any],
        page: int,
        page_size: int,
        sort_by: str,
        sort_dir: str,
        *,
        limit_override: int | None = None,
    ) -> dict[str, Any]:
        self._run_row(run_id)
        sort_map = {
            "url": "p.normalized_url",
            "score": "COALESCE(s.overall_score, 0)",
            "depth": "COALESCE(p.crawl_depth, 999)",
            "status": "COALESCE(p.status_code, 0)",
            "issues": (
                "(SELECT COUNT(*) FROM issues i "
                "WHERE i.run_id = p.run_id AND i.url = p.normalized_url)"
            ),
        }
        sort_sql = sort_map.get(sort_by, sort_map["score"])
        direction = "DESC" if sort_dir == "desc" else "ASC"

        where_sql, params = self._build_pages_where(run_id, filters)
        total = self._count(
            """
            SELECT COUNT(*) AS count
            FROM pages p
            LEFT JOIN scores s
            ON s.run_id = p.run_id AND s.url = p.normalized_url
            WHERE
            """
            + where_sql,
            tuple(params),
        )

        if limit_override is None:
            limit = page_size
            offset = (page - 1) * page_size
        else:
            limit = min(limit_override, MAX_EXPORT_ROWS)
            offset = 0

        rows = self._fetchall(
            """
            SELECT
                p.normalized_url,
                p.final_url,
                p.status_code,
                p.page_type,
                p.crawl_depth,
                p.orphan_risk_flag,
                p.nav_linked_flag,
                p.internal_links_out,
                p.external_links_out,
                p.render_gap_score,
                p.render_gap_reason,
                p.is_noindex,
                COALESCE(s.overall_score, 0) AS overall_score,
                (
                    SELECT COUNT(*)
                    FROM issues i
                    WHERE i.run_id = p.run_id AND i.url = p.normalized_url
                ) AS issue_count
            FROM pages p
            LEFT JOIN scores s
            ON s.run_id = p.run_id AND s.url = p.normalized_url
            WHERE
            """
            + where_sql
            + f" ORDER BY {sort_sql} {direction}, p.normalized_url ASC LIMIT ? OFFSET ?",
            tuple(params + [limit, offset]),
        )

        return {
            "rows": rows,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size if page_size else 0,
        }

    def list_issues(
        self,
        run_id: str,
        filters: dict[str, Any],
        page: int,
        page_size: int,
        sort_by: str,
        sort_dir: str,
        *,
        limit_override: int | None = None,
    ) -> dict[str, Any]:
        self._run_row(run_id)
        severity_rank_sql = (
            "CASE i.severity "
            "WHEN 'critical' THEN 0 "
            "WHEN 'high' THEN 1 "
            "WHEN 'medium' THEN 2 "
            "WHEN 'low' THEN 3 "
            "ELSE 4 END"
        )
        reach_rank_sql = (
            "CASE COALESCE(i.reach, 'single_page') "
            "WHEN 'sitewide' THEN 3 "
            "WHEN 'template_cluster' THEN 2 "
            "ELSE 1 END"
        )
        sort_map = {
            "priority": "COALESCE(i.priority_score, 0)",
            "confidence": "COALESCE(i.confidence_score, 0)",
            "importance": "COALESCE(i.page_importance, 1.0)",
            "reach": reach_rank_sql,
            "severity": severity_rank_sql,
            "code": "i.issue_code",
            "url": "i.url",
            "score": "COALESCE(s.overall_score, 0)",
        }
        sort_sql = sort_map.get(sort_by, sort_map["priority"])
        direction = "DESC" if sort_dir == "desc" else "ASC"
        priority_chain_sql = (
            "COALESCE(i.confidence_score, 0) DESC, "
            f"{reach_rank_sql} DESC, "
            "COALESCE(i.page_importance, 1.0) DESC, "
            f"{severity_rank_sql} ASC, "
            "i.issue_id ASC"
        )
        order_by_sql = f"{sort_sql} {direction}, {priority_chain_sql}"

        where_sql, params = self._build_issues_where(run_id, filters)
        total = self._count(
            """
            SELECT COUNT(*) AS count
            FROM issues i
            LEFT JOIN pages p
            ON p.run_id = i.run_id AND p.normalized_url = i.url
            LEFT JOIN scores s
            ON s.run_id = i.run_id AND s.url = i.url
            WHERE
            """
            + where_sql,
            tuple(params),
        )

        if limit_override is None:
            limit = page_size
            offset = (page - 1) * page_size
        else:
            limit = min(limit_override, MAX_EXPORT_ROWS)
            offset = 0

        rows = self._fetchall(
            """
            SELECT
                i.issue_id,
                i.url,
                i.severity,
                i.issue_code,
                i.title,
                i.description,
                i.evidence_json,
                i.issue_provenance,
                i.technical_seo_gate,
                i.verification_status,
                i.confidence_score,
                COALESCE(i.certainty_state, 'Verified') AS certainty_state,
                COALESCE(i.priority_score, 0) AS priority_score,
                COALESCE(i.page_importance, 1.0) AS page_importance,
                COALESCE(i.reach, 'single_page') AS reach,
                COALESCE(i.urgency, 1.0) AS urgency,
                COALESCE(i.affected_count, 1) AS affected_count,
                COALESCE(i.affected_ratio, 0.0) AS affected_ratio,
                COALESCE(i.template_cluster, '') AS template_cluster,
                COALESCE(i.affected_page_types, '') AS affected_page_types,
                p.page_type,
                p.status_code,
                COALESCE(s.overall_score, 0) AS overall_score
            FROM issues i
            LEFT JOIN pages p
            ON p.run_id = i.run_id AND p.normalized_url = i.url
            LEFT JOIN scores s
            ON s.run_id = i.run_id AND s.url = i.url
            WHERE
            """
            + where_sql
            + f" ORDER BY {order_by_sql} LIMIT ? OFFSET ?",
            tuple(params + [limit, offset]),
        )

        return {
            "rows": rows,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size if page_size else 0,
        }

    def url_detail(self, run_id: str, url: str) -> dict[str, Any]:
        self._run_row(run_id)
        page = self._fetchone(
            """
            SELECT p.*, s.crawlability_score, s.onpage_score, s.render_risk_score,
            s.internal_linking_score, s.local_seo_score, s.performance_score, s.overall_score,
          s.quality_score, s.risk_score, s.coverage_score, s.score_cap,
          COALESCE(s.scoring_model_version, s.score_version, '1.0.0') AS scoring_model_version,
          COALESCE(s.scoring_profile, s.score_profile, 'default') AS scoring_profile,
          COALESCE(NULLIF(s.score_explanation_json, ''), s.explanation_json, '{}') AS score_explanation_json,
          COALESCE(s.score_version, '1.0.0') AS score_version,
          COALESCE(s.score_profile, 'default') AS score_profile,
          COALESCE(s.explanation_json, '{}') AS explanation_json
            FROM pages p
            LEFT JOIN scores s
            ON s.run_id = p.run_id AND s.url = p.normalized_url
            WHERE p.run_id = ? AND p.normalized_url = ?
            """,
            (run_id, url),
        )
        if page is None:
            raise KeyError("url not found in run")

        score_explanation = _coerce_score_explanation(page)

        issues = self._fetchall(
            """
          SELECT issue_id, severity, issue_code, title, description, evidence_json,
            technical_seo_gate, verification_status, confidence_score,
            certainty_state, priority_score, page_importance, reach,
            urgency, affected_count, affected_ratio, template_cluster, affected_page_types
            FROM issues
            WHERE run_id = ? AND url = ?
          ORDER BY COALESCE(priority_score, 0) DESC, COALESCE(confidence_score, 0) DESC, issue_id ASC
            """,
            (run_id, url),
        )

        outgoing_links = self._fetchall(
            """
            SELECT target_url, normalized_target_url, is_internal, anchor_text, nofollow_flag, source_context
            FROM links
            WHERE run_id = ? AND source_url = ?
            ORDER BY is_internal DESC, normalized_target_url ASC
            LIMIT 250
            """,
            (run_id, url),
        )

        incoming_links = self._fetchall(
            """
            SELECT source_url, anchor_text, source_context
            FROM links
            WHERE run_id = ? AND normalized_target_url = ? AND is_internal = 1
            ORDER BY source_url ASC
            LIMIT 250
            """,
            (run_id, url),
        )

        performance_rows = self._fetchall(
            """
            SELECT strategy, source, performance_score, accessibility_score, best_practices_score,
                seo_score, lcp, cls, inp, ttfb, field_data_available
            FROM performance_metrics
            WHERE run_id = ? AND url = ?
            ORDER BY strategy ASC
            """,
            (run_id, url),
        )

        crux_rows = self._fetchall(
            """
            SELECT query_scope, status, source, origin_fallback_used,
                lcp_p75, cls_p75, inp_p75, fcp_p75, ttfb_p75, error_message
            FROM crux_metrics
            WHERE run_id = ? AND url = ?
            ORDER BY query_scope ASC
            """,
            (run_id, url),
        )

        return {
            "page": page,
          "score_explanation": score_explanation,
            "issues": issues,
            "outgoing_links": outgoing_links,
            "incoming_links": incoming_links,
            "performance": performance_rows,
            "crux": crux_rows,
        }

    def compare_runs(self, left_run_id: str, right_run_id: str) -> dict[str, Any]:
      if left_run_id == right_run_id:
        raise ValueError("left_run_id and right_run_id must differ")

      left_summary = self.summary(left_run_id)
      right_summary = self.summary(right_run_id)

      left_codes = {
        row["issue_code"]: int(row["count"])
        for row in self._fetchall(
          "SELECT issue_code, COUNT(*) AS count FROM issues WHERE run_id = ? GROUP BY issue_code",
          (left_run_id,),
        )
      }
      right_codes = {
        row["issue_code"]: int(row["count"])
        for row in self._fetchall(
          "SELECT issue_code, COUNT(*) AS count FROM issues WHERE run_id = ? GROUP BY issue_code",
          (right_run_id,),
        )
      }
      all_codes = sorted(set(left_codes.keys()) | set(right_codes.keys()))
      issue_code_deltas = [
        {
          "issue_code": code,
          "left_count": left_codes.get(code, 0),
          "right_count": right_codes.get(code, 0),
          "delta": right_codes.get(code, 0) - left_codes.get(code, 0),
        }
        for code in all_codes
      ]

      score_deltas = self._fetchall(
        """
        SELECT
          l.url AS url,
          l.overall_score AS left_score,
          r.overall_score AS right_score,
          (r.overall_score - l.overall_score) AS delta
        FROM scores l
        JOIN scores r
        ON l.url = r.url
        WHERE l.run_id = ? AND r.run_id = ?
        ORDER BY ABS(delta) DESC, l.url ASC
        LIMIT 100
        """,
        (left_run_id, right_run_id),
      )

      shared = len(score_deltas)
      avg_delta = round(
        sum(float(row["delta"] or 0.0) for row in score_deltas) / shared,
        2,
      ) if shared else 0.0

      return {
        "left_run": {
          "run_id": left_summary["run"]["run_id"],
          "domain": left_summary["run"]["domain"],
          "status": left_summary["run"]["status"],
          "counts": left_summary["counts"],
          "avg_overall_score": left_summary["score_summary"]["avg"],
        },
        "right_run": {
          "run_id": right_summary["run"]["run_id"],
          "domain": right_summary["run"]["domain"],
          "status": right_summary["run"]["status"],
          "counts": right_summary["counts"],
          "avg_overall_score": right_summary["score_summary"]["avg"],
        },
        "issue_code_deltas": issue_code_deltas,
        "score_deltas": score_deltas,
        "score_delta": {
          "shared_urls": shared,
          "avg_overall_delta": avg_delta,
        },
      }

    def architecture_insights(self, run_id: str) -> dict[str, Any]:
      self._run_row(run_id)
      rows = self._fetchall(
        """
        SELECT
          gm.url,
          gm.internal_pagerank,
          gm.betweenness,
          gm.closeness,
          gm.community_id,
          gm.bridge_flag,
          COALESCE(p.page_type, 'other') AS page_type,
          COALESCE(p.effective_internal_links_out, p.internal_links_out, 0) AS internal_links_out,
          COALESCE(s.overall_score, 0) AS overall_score,
          (
            SELECT COUNT(*)
            FROM links l
            WHERE l.run_id = gm.run_id
            AND l.normalized_target_url = gm.url
            AND l.is_internal = 1
          ) AS inlinks
        FROM page_graph_metrics gm
        LEFT JOIN pages p
        ON p.run_id = gm.run_id AND p.normalized_url = gm.url
        LEFT JOIN scores s
        ON s.run_id = gm.run_id AND s.url = gm.url
        WHERE gm.run_id = ?
        """,
        (run_id,),
      )

      if not rows:
        return {
          "summary": {
            "nodes": 0,
            "community_count": 0,
            "disconnected_community_count": 0,
            "weak_support_count": 0,
            "overloaded_hub_count": 0,
          },
          "cutoffs": {"weak_pagerank": 0.0, "hub_betweenness": 0.0},
          "important_pages_weak_support": [],
          "overloaded_hubs": [],
          "disconnected_clusters": [],
        }

      pagerank_values = [_as_float(row.get("internal_pagerank")) for row in rows if _as_float(row.get("internal_pagerank")) > 0.0]
      betweenness_values = [_as_float(row.get("betweenness")) for row in rows if _as_float(row.get("betweenness")) > 0.0]
      weak_pagerank_cutoff = _percentile(pagerank_values, 0.25) if pagerank_values else 0.0
      hub_betweenness_cutoff = max(0.05, _percentile(betweenness_values, 0.90)) if betweenness_values else 0.0

      community_counts: dict[int, int] = {}
      community_type_counts: dict[int, dict[str, int]] = {}
      community_rows: dict[int, list[dict[str, Any]]] = {}
      for row in rows:
        community_id = int(row.get("community_id") or 0)
        if community_id <= 0:
          continue
        community_counts[community_id] = community_counts.get(community_id, 0) + 1
        page_type = str(row.get("page_type") or "other")
        type_counts = community_type_counts.setdefault(community_id, {})
        type_counts[page_type] = type_counts.get(page_type, 0) + 1
        community_rows.setdefault(community_id, []).append(row)

      primary_community_id = 0
      if community_counts:
        primary_community_id = max(community_counts.items(), key=lambda item: (item[1], item[0]))[0]

      def dominant_type(community_id: int) -> str:
        type_counts = community_type_counts.get(community_id, {})
        if not type_counts:
          return "other"
        return max(type_counts.items(), key=lambda item: (item[1], item[0]))[0]

      primary_type = dominant_type(primary_community_id)

      weak_support = [
        {
          "url": str(row.get("url") or ""),
          "page_type": str(row.get("page_type") or "other"),
          "overall_score": int(row.get("overall_score") or 0),
          "internal_pagerank": _as_float(row.get("internal_pagerank")),
          "closeness": _as_float(row.get("closeness")),
          "inlinks": int(row.get("inlinks") or 0),
          "internal_links_out": int(row.get("internal_links_out") or 0),
          "community_id": int(row.get("community_id") or 0),
        }
        for row in rows
        if str(row.get("page_type") or "other") in MONEY_PAGE_TYPES
        and _as_float(row.get("internal_pagerank")) > 0.0
        and _as_float(row.get("internal_pagerank")) <= weak_pagerank_cutoff
        and (int(row.get("inlinks") or 0) <= 1 or _as_float(row.get("closeness")) < 0.18)
      ]
      weak_support.sort(key=lambda row: (row["internal_pagerank"], row["overall_score"]))

      overloaded_hubs = [
        {
          "url": str(row.get("url") or ""),
          "page_type": str(row.get("page_type") or "other"),
          "overall_score": int(row.get("overall_score") or 0),
          "betweenness": _as_float(row.get("betweenness")),
          "internal_pagerank": _as_float(row.get("internal_pagerank")),
          "bridge_flag": int(row.get("bridge_flag") or 0),
          "community_id": int(row.get("community_id") or 0),
        }
        for row in rows
        if int(row.get("bridge_flag") or 0) == 1
        and hub_betweenness_cutoff > 0.0
        and _as_float(row.get("betweenness")) >= hub_betweenness_cutoff
      ]
      overloaded_hubs.sort(key=lambda row: row["betweenness"], reverse=True)

      disconnected_clusters: list[dict[str, Any]] = []
      for community_id, count in sorted(community_counts.items(), key=lambda item: (-item[1], item[0])):
        if community_id == primary_community_id or count < 2:
          continue
        cluster_rows = sorted(
          community_rows.get(community_id, []),
          key=lambda row: int(row.get("overall_score") or 0),
          reverse=True,
        )
        disconnected_clusters.append(
          {
            "community_id": community_id,
            "size": count,
            "dominant_page_type": dominant_type(community_id),
            "primary_dominant_page_type": primary_type,
            "sample_urls": [str(row.get("url") or "") for row in cluster_rows[:3]],
          }
        )

      return {
        "summary": {
          "nodes": len(rows),
          "community_count": len(community_counts),
          "disconnected_community_count": len(disconnected_clusters),
          "weak_support_count": len(weak_support),
          "overloaded_hub_count": len(overloaded_hubs),
        },
        "cutoffs": {
          "weak_pagerank": round(weak_pagerank_cutoff, 8),
          "hub_betweenness": round(hub_betweenness_cutoff, 8),
        },
        "important_pages_weak_support": weak_support[:10],
        "overloaded_hubs": overloaded_hubs[:10],
        "disconnected_clusters": disconnected_clusters,
      }

    def run_query(
        self,
        *,
        query: str,
        run_id: str = "",
        limit: int = 250,
    ) -> dict[str, Any]:
        sql = _normalize_read_only_query(query)
        bounded_limit = max(1, min(int(limit), MAX_QUERY_ROWS))

        params: dict[str, Any] = {"_limit": bounded_limit}
        normalized_run_id = str(run_id or "").strip()
        if ":run_id" in sql:
            if not normalized_run_id:
                raise ValueError("run_id is required when query uses :run_id")
            self._run_row(normalized_run_id)
            params["run_id"] = normalized_run_id

        started = time.perf_counter()
        with self._connect() as conn:
            cursor = conn.execute(
                f"SELECT * FROM ({sql}) AS dashboard_query LIMIT :_limit",
                params,
            )
            fetched_rows = cursor.fetchall()
            columns = [entry[0] for entry in (cursor.description or [])]
        elapsed_ms = int((time.perf_counter() - started) * 1000)

        rows = [dict(row) for row in fetched_rows]
        return {
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "limit": bounded_limit,
            "elapsed_ms": elapsed_ms,
        }

    def export_query(self, *, query: str, run_id: str = "", limit: int = 250) -> tuple[str, str]:
        payload = self.run_query(query=query, run_id=run_id, limit=limit)
        columns = list(payload.get("columns") or [])
        rows = list(payload.get("rows") or [])
        if not columns and rows:
            columns = list(rows[0].keys())

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        if columns:
            writer.writerow(columns)
            for row in rows:
                writer.writerow([row.get(column, "") for column in columns])
        else:
            writer.writerow(["result"])

        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return buffer.getvalue(), f"query-{stamp}.csv"

    def export_dataset(self, run_id: str, dataset: str, filters: dict[str, Any]) -> tuple[str, str]:
        dataset_key = dataset.strip().lower()
        if dataset_key == "issues":
            response = self.list_issues(
                run_id,
                filters,
                page=1,
                page_size=MAX_EXPORT_ROWS,
                sort_by="severity",
                sort_dir="asc",
                limit_override=MAX_EXPORT_ROWS,
            )
            rows = response["rows"]
            columns = [
                "issue_id",
                "url",
                "severity",
                "issue_code",
                "title",
                "description",
                "page_type",
                "status_code",
                "overall_score",
            ]
            filename = f"issues-{run_id}.csv"
        elif dataset_key == "pages":
            response = self.list_pages(
                run_id,
                filters,
                page=1,
                page_size=MAX_EXPORT_ROWS,
                sort_by="score",
                sort_dir="desc",
                limit_override=MAX_EXPORT_ROWS,
            )
            rows = response["rows"]
            columns = [
                "normalized_url",
                "final_url",
                "status_code",
                "page_type",
                "crawl_depth",
                "orphan_risk_flag",
                "internal_links_out",
                "external_links_out",
                "is_noindex",
                "render_gap_score",
                "render_gap_reason",
                "overall_score",
                "issue_count",
            ]
            filename = f"pages-{run_id}.csv"
        else:
            raise ValueError("dataset must be issues or pages")

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columns)
        for row in rows:
            writer.writerow([row.get(column, "") for column in columns])
        return buffer.getvalue(), filename


def _build_html() -> str:
    template_path = Path(__file__).with_name("dashboard_ui.html")
    if template_path.exists():
        return template_path.read_text(encoding="utf-8")

    # The UI is intentionally self-contained so the dashboard can run with zero frontend build tooling.
    return """<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>SEO Audit Dashboard</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap');

    :root {
      --bg: #f6f1e8;
      --ink: #1c2533;
      --muted: #5b6675;
      --card: #fffaf3;
      --line: #e8dccb;
      --brand: #005f73;
      --accent: #f28500;
      --accent-soft: #f8e7cc;
      --ok: #2f855a;
      --warn: #b45309;
      --bad: #b42318;
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      color: var(--ink);
      background: radial-gradient(circle at 10% -10%, #fee7c8 0%, transparent 45%),
                  radial-gradient(circle at 100% 0%, #d6f0ea 0%, transparent 40%),
                  var(--bg);
      font-family: 'Space Grotesk', sans-serif;
      min-height: 100vh;
    }

    header {
      position: sticky;
      top: 0;
      z-index: 20;
      backdrop-filter: blur(6px);
      background: rgba(246, 241, 232, 0.9);
      border-bottom: 1px solid var(--line);
      padding: 1rem 1.2rem;
    }

    .header-grid {
      max-width: 1400px;
      margin: 0 auto;
      display: grid;
      grid-template-columns: 1fr auto auto;
      gap: 0.8rem;
      align-items: end;
    }

    h1 {
      margin: 0;
      font-size: 1.4rem;
      letter-spacing: 0.01em;
    }

    .subtitle {
      margin: 0.25rem 0 0;
      color: var(--muted);
      font-size: 0.92rem;
    }

    label {
      display: block;
      color: var(--muted);
      font-size: 0.8rem;
      margin-bottom: 0.25rem;
    }

    select,
    input,
    button {
      width: 100%;
      border: 1px solid var(--line);
      background: white;
      border-radius: 0.7rem;
      color: var(--ink);
      padding: 0.55rem 0.65rem;
      font-family: 'IBM Plex Mono', monospace;
      font-size: 0.8rem;
    }

    button {
      cursor: pointer;
      border: 1px solid transparent;
      background: linear-gradient(135deg, var(--brand), #0a9396);
      color: white;
      font-family: 'Space Grotesk', sans-serif;
      font-weight: 600;
      letter-spacing: 0.01em;
      transition: transform 120ms ease, filter 120ms ease;
    }

    button:hover {
      transform: translateY(-1px);
      filter: brightness(1.05);
    }

    main {
      max-width: 1400px;
      margin: 1rem auto 2rem;
      padding: 0 1rem;
      display: grid;
      gap: 1rem;
    }

    .card-row {
      display: grid;
      grid-template-columns: repeat(4, minmax(140px, 1fr));
      gap: 0.8rem;
    }

    .card,
    .panel {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 1rem;
      box-shadow: 0 14px 28px rgba(24, 39, 66, 0.06);
      overflow: hidden;
    }

    .card {
      padding: 0.9rem;
    }

    .card h2 {
      margin: 0;
      font-size: 0.82rem;
      color: var(--muted);
      font-weight: 500;
      letter-spacing: 0.02em;
      text-transform: uppercase;
    }

    .card .value {
      margin-top: 0.45rem;
      font-size: 1.55rem;
      font-weight: 700;
    }

    .panel header {
      position: static;
      background: transparent;
      border: 0;
      border-bottom: 1px solid var(--line);
      padding: 0.75rem 0.9rem;
      margin: 0;
    }

    .panel-title {
      margin: 0;
      font-size: 1.03rem;
    }

    .panel-subtitle {
      margin: 0.2rem 0 0;
      color: var(--muted);
      font-size: 0.8rem;
    }

    .panel-body {
      padding: 0.85rem;
      display: grid;
      gap: 0.8rem;
    }

    .filters {
      display: grid;
      grid-template-columns: repeat(6, minmax(100px, 1fr));
      gap: 0.55rem;
      align-items: end;
    }

    .table-wrap {
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 0.7rem;
      background: white;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 760px;
      font-family: 'IBM Plex Mono', monospace;
      font-size: 0.73rem;
    }

    thead {
      background: #fff2dd;
    }

    th,
    td {
      text-align: left;
      padding: 0.46rem 0.52rem;
      border-bottom: 1px solid #f0e6d8;
      vertical-align: top;
    }

    tbody tr:hover {
      background: #fcf6ed;
    }

    .mono {
      font-family: 'IBM Plex Mono', monospace;
    }

    .split {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 1rem;
    }

    .badge {
      display: inline-block;
      border-radius: 999px;
      padding: 0.14rem 0.48rem;
      font-size: 0.72rem;
      font-weight: 600;
      border: 1px solid transparent;
      text-transform: uppercase;
      letter-spacing: 0.03em;
    }

    .sev-critical { background: #fde2e1; color: var(--bad); border-color: #efc0bc; }
    .sev-high { background: #ffe9df; color: #b54708; border-color: #f7cfbd; }
    .sev-medium { background: #fff4dc; color: var(--warn); border-color: #f5dfb2; }
    .sev-low { background: #e8f6ee; color: var(--ok); border-color: #c8e9d5; }

    .link-btn {
      border: 0;
      background: transparent;
      color: var(--brand);
      text-align: left;
      padding: 0;
      width: auto;
      font: inherit;
      text-decoration: underline;
      cursor: pointer;
    }

    .inline-actions {
      display: flex;
      gap: 0.4rem;
      flex-wrap: wrap;
    }

    .ghost {
      background: white;
      color: var(--brand);
      border-color: var(--brand);
    }

    .status-line {
      min-height: 1.15rem;
      color: var(--muted);
      font-size: 0.78rem;
    }

    .error {
      color: var(--bad);
      font-weight: 600;
    }

    .runner-grid {
      display: grid;
      grid-template-columns: repeat(8, minmax(110px, 1fr));
      gap: 0.55rem;
      align-items: end;
    }

    .progress-track {
      width: 100%;
      height: 0.9rem;
      border-radius: 999px;
      background: #efe3d1;
      overflow: hidden;
      border: 1px solid var(--line);
    }

    .progress-fill {
      width: 0%;
      height: 100%;
      background: linear-gradient(90deg, #0a9396, #f28500);
      transition: width 220ms ease;
    }

    .log-box {
      width: 100%;
      min-height: 120px;
      max-height: 220px;
      resize: vertical;
      border: 1px solid var(--line);
      border-radius: 0.7rem;
      background: white;
      padding: 0.5rem;
      font-family: 'IBM Plex Mono', monospace;
      font-size: 0.73rem;
      line-height: 1.35;
      white-space: pre;
      overflow: auto;
    }

    .viz-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 0.8rem;
    }

    .bar-list {
      display: grid;
      gap: 0.4rem;
    }

    .bar-item {
      display: grid;
      grid-template-columns: 120px 1fr 56px;
      gap: 0.5rem;
      align-items: center;
      font-family: 'IBM Plex Mono', monospace;
      font-size: 0.72rem;
    }

    .bar-track {
      height: 0.75rem;
      border-radius: 999px;
      background: #efe6d7;
      overflow: hidden;
    }

    .bar-fill {
      height: 100%;
      background: linear-gradient(90deg, #0a9396, #005f73);
    }

    .gallery-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(180px, 1fr));
      gap: 0.8rem;
    }

    .shot-card {
      border: 1px solid var(--line);
      border-radius: 0.8rem;
      background: white;
      overflow: hidden;
      display: grid;
      gap: 0.35rem;
      padding-bottom: 0.45rem;
    }

    .shot-card img {
      width: 100%;
      height: 160px;
      object-fit: cover;
      border-bottom: 1px solid var(--line);
      background: #f2ede4;
    }

    .shot-card .meta {
      padding: 0 0.55rem;
      font-family: 'IBM Plex Mono', monospace;
      font-size: 0.68rem;
      word-break: break-word;
      color: var(--muted);
    }

    .empty-state {
      padding: 1rem;
      border: 1px dashed var(--line);
      border-radius: 0.7rem;
      color: var(--muted);
      font-size: 0.8rem;
      background: #fffdf8;
    }

    .meta-line {
      color: var(--muted);
      font-size: 0.75rem;
      font-family: 'IBM Plex Mono', monospace;
    }

    .runner-status {
      color: var(--muted);
      font-size: 0.78rem;
      font-family: 'IBM Plex Mono', monospace;
    }

    .summary-box {
      border: 1px solid var(--line);
      border-radius: 0.7rem;
      background: #fffdf8;
      padding: 0.6rem;
      font-size: 0.74rem;
      line-height: 1.35;
      color: var(--muted);
      font-family: 'IBM Plex Mono', monospace;
      white-space: pre-wrap;
    }

    @media (max-width: 1100px) {
      .header-grid {
        grid-template-columns: 1fr;
      }
      .card-row {
        grid-template-columns: repeat(2, minmax(120px, 1fr));
      }
      .filters {
        grid-template-columns: repeat(2, minmax(120px, 1fr));
      }
      .split {
        grid-template-columns: 1fr;
      }
      .viz-grid {
        grid-template-columns: 1fr;
      }
      .runner-grid {
        grid-template-columns: repeat(2, minmax(120px, 1fr));
      }
      .gallery-grid {
        grid-template-columns: repeat(2, minmax(150px, 1fr));
      }
    }

    @media (max-width: 640px) {
      .card-row {
        grid-template-columns: 1fr;
      }
      .filters {
        grid-template-columns: 1fr;
      }
      .runner-grid {
        grid-template-columns: 1fr;
      }
      .gallery-grid {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <header>
    <div class=\"header-grid\">
      <div>
        <h1>SEO Audit Interactive Dashboard</h1>
        <p class=\"subtitle\">Run-scoped diagnostics, issue triage, URL drill-down, and run comparison.</p>
      </div>
      <div>
        <label for=\"runSelect\">Primary run</label>
        <select id=\"runSelect\"></select>
      </div>
      <div class=\"inline-actions\">
        <button id=\"refreshBtn\">Refresh views</button>
      </div>
    </div>
  </header>

  <main>
    <div id=\"statusLine\" class=\"status-line\"></div>

    <section class=\"card-row\">
      <article class=\"card\">
        <h2>Pages</h2>
        <div id=\"pagesCount\" class=\"value\">-</div>
      </article>
      <article class=\"card\">
        <h2>Issues</h2>
        <div id=\"issuesCount\" class=\"value\">-</div>
      </article>
      <article class=\"card\">
        <h2>High/Critical</h2>
        <div id=\"highCriticalCount\" class=\"value\">-</div>
      </article>
      <article class=\"card\">
        <h2>Avg Score</h2>
        <div id=\"avgScore\" class=\"value\">-</div>
      </article>
    </section>

    <section class=\"split\">
      <section class=\"panel\">
        <header>
          <h2 class=\"panel-title\">Run observability</h2>
          <p class=\"panel-subtitle\">Stage timings, provider telemetry, and top issue concentration for the selected run.</p>
        </header>
        <div class=\"panel-body\">
          <div class=\"viz-grid\">
            <div>
              <div class=\"meta-line\">Stage timings (ms)</div>
              <div id=\"stageTimingBars\" class=\"bar-list\"></div>
            </div>
            <div>
              <div class=\"meta-line\">Top issue codes</div>
              <div id=\"issueCodeBars\" class=\"bar-list\"></div>
            </div>
          </div>
          <div>
            <div class=\"meta-line\">Provider telemetry</div>
            <div id=\"providerTelemetry\" class=\"summary-box\">No provider telemetry for this run.</div>
          </div>
          <div>
            <div class=\"meta-line\">Run notes</div>
            <div id=\"runNotes\" class=\"summary-box\">No notes available.</div>
          </div>
        </div>
      </section>

      <section class=\"panel\">
        <header>
          <h2 class=\"panel-title\">Audit runner</h2>
          <p class=\"panel-subtitle\">Start an audit from the dashboard and stream live stage/log progress.</p>
        </header>
        <div class=\"panel-body\">
          <div class=\"runner-grid\">
            <div>
              <label for=\"runDomain\">Domain</label>
              <input id=\"runDomain\" placeholder=\"https://example.com\">
            </div>
            <div>
              <label for=\"runProfile\">Run profile</label>
              <select id=\"runProfile\">
                <option value=\"standard\">standard</option>
                <option value=\"exploratory\">exploratory</option>
                <option value=\"deep\">deep</option>
              </select>
            </div>
            <div>
              <label for=\"runMaxPages\">Max pages</label>
              <input id=\"runMaxPages\" type=\"number\" min=\"1\" value=\"50\">
            </div>
            <div>
              <label for=\"runRenderMode\">Render mode</label>
              <select id=\"runRenderMode\">
                <option value=\"none\">none</option>
                <option value=\"sample\">sample</option>
                <option value=\"all\">all</option>
              </select>
            </div>
            <div>
              <label for=\"runMaxRenderPages\">Max render pages</label>
              <input id=\"runMaxRenderPages\" type=\"number\" min=\"0\" value=\"8\">
            </div>
            <div>
              <label for=\"runPerfTargets\">Perf targets</label>
              <input id=\"runPerfTargets\" type=\"number\" min=\"1\" value=\"3\">
            </div>
            <div>
              <label for=\"runShotCount\">Screenshot count</label>
              <input id=\"runShotCount\" type=\"number\" min=\"0\" value=\"4\">
            </div>
            <div class=\"inline-actions\">
              <button id=\"startAuditBtn\">Start audit</button>
            </div>
          </div>
          <div id=\"runnerStatus\" class=\"runner-status\">No active dashboard job.</div>
          <div class=\"progress-track\"><div id=\"jobProgressFill\" class=\"progress-fill\"></div></div>
          <div id=\"jobStage\" class=\"meta-line\">Awaiting job...</div>
          <div id=\"jobLog\" class=\"log-box\">No logs yet.</div>
        </div>
      </section>
    </section>

    <section class=\"panel\">
      <header>
        <h2 class=\"panel-title\">Technical triage</h2>
        <p class=\"panel-subtitle\">Filter issues by severity, code, content type, and text query.</p>
      </header>
      <div class=\"panel-body\">
        <div class=\"filters\">
          <div>
            <label for=\"issueSeverity\">Severity</label>
            <select id=\"issueSeverity\">
              <option value=\"\">All</option>
              <option value=\"critical\">critical</option>
              <option value=\"high\">high</option>
              <option value=\"medium\">medium</option>
              <option value=\"low\">low</option>
            </select>
          </div>
          <div>
            <label for=\"issueCode\">Issue code</label>
            <input id=\"issueCode\" placeholder=\"NOINDEX\">
          </div>
          <div>
            <label for=\"issuePageType\">Page type</label>
            <input id=\"issuePageType\" placeholder=\"service\">
          </div>
          <div>
            <label for=\"issueQuery\">Text query</label>
            <input id=\"issueQuery\" placeholder=\"url/title/description\">
          </div>
          <div>
            <label for=\"issuePage\">Page</label>
            <input id=\"issuePage\" type=\"number\" min=\"1\" value=\"1\">
          </div>
          <div class=\"inline-actions\">
            <button id=\"applyIssueFilters\">Apply</button>
            <button id=\"exportIssues\" class=\"ghost\">Export CSV</button>
          </div>
        </div>
        <div class=\"table-wrap\">
          <table>
            <thead>
              <tr>
                <th>Severity</th>
                <th>Code</th>
                <th>URL</th>
                <th>Title</th>
                <th>Page type</th>
                <th>Score</th>
              </tr>
            </thead>
            <tbody id=\"issuesBody\"></tbody>
          </table>
        </div>
        <div id=\"issuesMeta\" class=\"meta-line\"></div>
      </div>
    </section>

    <section class=\"panel\">
      <header>
        <h2 class=\"panel-title\">Page explorer</h2>
        <p class=\"panel-subtitle\">Sort and filter crawl inventory by score, status, depth, and issue pressure.</p>
      </header>
      <div class=\"panel-body\">
        <div class=\"filters\">
          <div>
            <label for=\"pageType\">Page type</label>
            <input id=\"pageType\" placeholder=\"homepage\">
          </div>
          <div>
            <label for=\"statusCode\">Status code</label>
            <input id=\"statusCode\" type=\"number\" placeholder=\"200\">
          </div>
          <div>
            <label for=\"maxDepth\">Max depth</label>
            <input id=\"maxDepth\" type=\"number\" min=\"0\" placeholder=\"3\">
          </div>
          <div>
            <label for=\"minScore\">Min score</label>
            <input id=\"minScore\" type=\"number\" min=\"0\" max=\"100\" placeholder=\"50\">
          </div>
          <div>
            <label for=\"pageSeverity\">Has issue severity</label>
            <select id=\"pageSeverity\">
              <option value=\"\">Any</option>
              <option value=\"critical\">critical</option>
              <option value=\"high\">high</option>
              <option value=\"medium\">medium</option>
              <option value=\"low\">low</option>
            </select>
          </div>
          <div>
            <label for=\"pageQuery\">Text query</label>
            <input id=\"pageQuery\" placeholder=\"url/title/h1\">
          </div>
          <div>
            <label for=\"pageSort\">Sort by</label>
            <select id=\"pageSort\">
              <option value=\"score\">score</option>
              <option value=\"url\">url</option>
              <option value=\"depth\">depth</option>
              <option value=\"status\">status</option>
              <option value=\"issues\">issues</option>
            </select>
          </div>
          <div>
            <label for=\"pageSortDir\">Direction</label>
            <select id=\"pageSortDir\">
              <option value=\"desc\">desc</option>
              <option value=\"asc\">asc</option>
            </select>
          </div>
          <div>
            <label for=\"pagePage\">Page</label>
            <input id=\"pagePage\" type=\"number\" min=\"1\" value=\"1\">
          </div>
          <div class=\"inline-actions\">
            <button id=\"applyPageFilters\">Apply</button>
            <button id=\"exportPages\" class=\"ghost\">Export CSV</button>
          </div>
        </div>
        <div class=\"table-wrap\">
          <table>
            <thead>
              <tr>
                <th>URL</th>
                <th>Status</th>
                <th>Type</th>
                <th>Depth</th>
                <th>Score</th>
                <th>Issues</th>
                <th>Noindex</th>
                <th>Render gap</th>
              </tr>
            </thead>
            <tbody id=\"pagesBody\"></tbody>
          </table>
        </div>
        <div id=\"pagesMeta\" class=\"meta-line\"></div>
      </div>
    </section>

    <section class=\"split\">
      <section class=\"panel\">
        <header>
          <h2 class=\"panel-title\">URL inspector</h2>
          <p class=\"panel-subtitle\">Click any URL from tables above to inspect full details.</p>
        </header>
        <div class=\"panel-body\">
          <div id=\"urlDetail\" class=\"mono\">Select a URL to inspect page facts, score, issues, and links.</div>
        </div>
      </section>

      <section class=\"panel\">
        <header>
          <h2 class=\"panel-title\">Run comparison</h2>
          <p class=\"panel-subtitle\">Compare aggregate deltas and URL-level score shifts across runs.</p>
        </header>
        <div class=\"panel-body\">
          <div class=\"filters\" style=\"grid-template-columns: 1fr auto;\">
            <div>
              <label for=\"compareRunSelect\">Compare against run</label>
              <select id=\"compareRunSelect\"></select>
            </div>
            <div class=\"inline-actions\" style=\"align-self: end;\">
              <button id=\"runCompare\">Compare</button>
            </div>
          </div>
          <div id=\"compareSummary\" class=\"mono\">Choose a run to compare.</div>
          <div class=\"table-wrap\">
            <table>
              <thead>
                <tr>
                  <th>URL</th>
                  <th>Left score</th>
                  <th>Right score</th>
                  <th>Delta</th>
                </tr>
              </thead>
              <tbody id=\"compareBody\"></tbody>
            </table>
          </div>
        </div>
      </section>
    </section>

    <section class=\"panel\">
      <header>
        <h2 class=\"panel-title\">Screenshot gallery</h2>
        <p class=\"panel-subtitle\">Captured screenshots for the selected run (from dashboard-run jobs).</p>
      </header>
      <div class=\"panel-body\">
        <div class=\"inline-actions\">
          <button id=\"refreshShotsBtn\" class=\"ghost\">Refresh screenshots</button>
        </div>
        <div id=\"screenshotGallery\" class=\"gallery-grid\"></div>
      </div>
    </section>
  </main>

  <script>
    const state = {
      runId: '',
      compareRunId: '',
      issuePage: 1,
      pagePage: 1,
      activeJobId: '',
      jobPollTimer: null,
      lastSummary: null,
    };

    const statusLine = document.getElementById('statusLine');

    function escapeHtml(value) {
      return String(value || '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }

    function setStatus(message, isError = false) {
      statusLine.textContent = message;
      statusLine.className = isError ? 'status-line error' : 'status-line';
    }

    function setRunnerStatus(message) {
      document.getElementById('runnerStatus').textContent = message;
    }

    async function api(path, params = {}) {
      const url = new URL(path, window.location.origin);
      for (const [key, value] of Object.entries(params)) {
        if (value !== undefined && value !== null && value !== '') {
          url.searchParams.set(key, value);
        }
      }
      const response = await fetch(url.toString());
      const contentType = response.headers.get('content-type') || '';
      if (!response.ok) {
        let detail = 'request failed';
        if (contentType.includes('application/json')) {
          const payload = await response.json();
          detail = payload.error || detail;
        }
        throw new Error(detail);
      }
      if (contentType.includes('application/json')) {
        return response.json();
      }
      return response.text();
    }

    async function apiPost(path, payload) {
      const response = await fetch(path, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload || {}),
      });

      const contentType = response.headers.get('content-type') || '';
      if (!response.ok) {
        let detail = 'request failed';
        if (contentType.includes('application/json')) {
          const errorPayload = await response.json();
          detail = errorPayload.error || detail;
        }
        throw new Error(detail);
      }
      if (contentType.includes('application/json')) {
        return response.json();
      }
      return {};
    }

    function severityBadge(severity) {
      const normalized = String(severity || '').toLowerCase();
      const safe = escapeHtml(normalized || 'unknown');
      return `<span class=\"badge sev-${safe}\">${safe}</span>`;
    }

    function issueFilters() {
      return {
        run_id: state.runId,
        severity: document.getElementById('issueSeverity').value.trim(),
        issue_code: document.getElementById('issueCode').value.trim(),
        page_type: document.getElementById('issuePageType').value.trim(),
        query: document.getElementById('issueQuery').value.trim(),
        page: document.getElementById('issuePage').value,
        page_size: 25,
        sort_by: 'severity',
        sort_dir: 'asc',
      };
    }

    function pageFilters() {
      return {
        run_id: state.runId,
        page_type: document.getElementById('pageType').value.trim(),
        status_code: document.getElementById('statusCode').value.trim(),
        max_depth: document.getElementById('maxDepth').value.trim(),
        min_score: document.getElementById('minScore').value.trim(),
        severity: document.getElementById('pageSeverity').value.trim(),
        query: document.getElementById('pageQuery').value.trim(),
        page: document.getElementById('pagePage').value,
        page_size: 25,
        sort_by: document.getElementById('pageSort').value,
        sort_dir: document.getElementById('pageSortDir').value,
      };
    }

    function renderCards(summary) {
      document.getElementById('pagesCount').textContent = summary.counts.pages;
      document.getElementById('issuesCount').textContent = summary.counts.issues;
      const high = Number(summary.severity_counts.high || 0);
      const critical = Number(summary.severity_counts.critical || 0);
      document.getElementById('highCriticalCount').textContent = high + critical;
      const avg = summary.score_summary.avg;
      document.getElementById('avgScore').textContent = avg === null ? 'n/a' : avg;
    }

    function renderBarList(items, containerId, labelFn, valueFn, valueFormatter) {
      const container = document.getElementById(containerId);
      if (!items.length) {
        container.innerHTML = '<div class=\"empty-state\">No data for this run.</div>';
        return;
      }

      const values = items.map((item) => Number(valueFn(item) || 0));
      const maxValue = Math.max(...values, 0);
      container.innerHTML = items.map((item) => {
        const label = escapeHtml(labelFn(item));
        const rawValue = Number(valueFn(item) || 0);
        const width = maxValue > 0 ? Math.max(5, Math.round((rawValue / maxValue) * 100)) : 0;
        const displayValue = escapeHtml(valueFormatter(rawValue));
        return `
          <div class=\"bar-item\">
            <div>${label}</div>
            <div class=\"bar-track\"><div class=\"bar-fill\" style=\"width:${width}%\"></div></div>
            <div>${displayValue}</div>
          </div>
        `;
      }).join('');
    }

    function renderObservability(summary) {
      renderBarList(
        summary.stage_timings || [],
        'stageTimingBars',
        (row) => row.stage || 'stage',
        (row) => row.elapsed_ms || 0,
        (value) => `${value}ms`
      );

      renderBarList(
        summary.top_issue_codes || [],
        'issueCodeBars',
        (row) => row.issue_code || 'issue',
        (row) => row.count || 0,
        (value) => String(value)
      );

      const providerTelemetry = summary.provider_telemetry || [];
      document.getElementById('providerTelemetry').textContent = providerTelemetry.length
        ? providerTelemetry
            .map((row) => (
              `${row.provider}: attempts=${row.attempts} http=${row.http_attempts} retries=${row.retries} ` +
              `timeouts=${row.timeouts} wait_s=${row.wait_seconds} success=${row.success} ` +
              `no_data=${row.no_data} failed=${row.failed_http} skipped=${row.skipped_missing_key}`
            ))
            .join('\n')
        : 'No provider telemetry for this run.';

      document.getElementById('runNotes').textContent =
        (summary.run && summary.run.notes ? String(summary.run.notes) : 'No notes available.');
    }

    function renderIssues(payload) {
      const body = document.getElementById('issuesBody');
      const meta = document.getElementById('issuesMeta');
      meta.textContent = `page ${payload.page}/${payload.total_pages || 1} | rows ${payload.rows.length} of ${payload.total}`;

      if (!payload.rows.length) {
        body.innerHTML = '<tr><td colspan=\"6\">No matching issues.</td></tr>';
        return;
      }
      body.innerHTML = payload.rows.map((row) => `
        <tr>
          <td>${severityBadge(row.severity)}</td>
          <td>${escapeHtml(row.issue_code)}</td>
          <td><button class=\"link-btn\" data-url=\"${escapeHtml(row.url)}\">${escapeHtml(row.url)}</button></td>
          <td>${escapeHtml(row.title)}</td>
          <td>${escapeHtml(row.page_type || 'other')}</td>
          <td>${escapeHtml(row.overall_score)}</td>
        </tr>
      `).join('');
    }

    function renderPages(payload) {
      const body = document.getElementById('pagesBody');
      const meta = document.getElementById('pagesMeta');
      meta.textContent = `page ${payload.page}/${payload.total_pages || 1} | rows ${payload.rows.length} of ${payload.total}`;

      if (!payload.rows.length) {
        body.innerHTML = '<tr><td colspan=\"8\">No matching pages.</td></tr>';
        return;
      }
      body.innerHTML = payload.rows.map((row) => `
        <tr>
          <td><button class=\"link-btn\" data-url=\"${escapeHtml(row.normalized_url)}\">${escapeHtml(row.normalized_url)}</button></td>
          <td>${escapeHtml(row.status_code)}</td>
          <td>${escapeHtml(row.page_type)}</td>
          <td>${escapeHtml(row.crawl_depth)}</td>
          <td>${escapeHtml(row.overall_score)}</td>
          <td>${escapeHtml(row.issue_count)}</td>
          <td>${Number(row.is_noindex || 0) ? 'yes' : 'no'}</td>
          <td>${escapeHtml(row.render_gap_score || 0)}</td>
        </tr>
      `).join('');
    }

    function renderCompare(payload) {
      const summary = document.getElementById('compareSummary');
      const body = document.getElementById('compareBody');
      const left = payload.left_run;
      const right = payload.right_run;
      summary.textContent =
        `left pages=${left.counts.pages}, right pages=${right.counts.pages}, ` +
        `left issues=${left.counts.issues}, right issues=${right.counts.issues}, ` +
        `shared scored urls=${payload.score_delta.shared_urls}, avg delta=${payload.score_delta.avg_overall_delta}`;

      if (!payload.score_deltas.length) {
        body.innerHTML = '<tr><td colspan=\"4\">No shared scored URLs across runs.</td></tr>';
        return;
      }

      body.innerHTML = payload.score_deltas.map((row) => `
        <tr>
          <td>${escapeHtml(row.url)}</td>
          <td>${escapeHtml(row.left_score)}</td>
          <td>${escapeHtml(row.right_score)}</td>
          <td>${escapeHtml(row.delta)}</td>
        </tr>
      `).join('');
    }

    function renderUrlDetail(payload) {
      const page = payload.page;
      const issueLines = payload.issues.map((issue) =>
        `- [${issue.severity}] ${issue.issue_code}: ${issue.title}`
      );
      const incoming = payload.incoming_links.length;
      const outgoing = payload.outgoing_links.length;
      const perf = payload.performance.length;
      const crux = payload.crux.length;

      const details = [
        `URL: ${page.normalized_url}`,
        `status: ${page.status_code} | page_type: ${page.page_type} | depth: ${page.crawl_depth}`,
        `score: ${page.overall_score ?? 'n/a'} | noindex: ${Number(page.is_noindex || 0) ? 'yes' : 'no'}`,
        `render_gap_score: ${page.render_gap_score || 0} (${page.render_gap_reason || 'n/a'})`,
        `incoming links: ${incoming} | outgoing links: ${outgoing}`,
        `performance rows: ${perf} | crux rows: ${crux}`,
        issueLines.length ? 'issues:\n' + issueLines.join('\n') : 'issues: none',
      ];

      document.getElementById('urlDetail').textContent = details.join('\n');
    }

    function renderScreenshots(items) {
      const gallery = document.getElementById('screenshotGallery');
      if (!items.length) {
        gallery.innerHTML = '<div class=\"empty-state\">No screenshots captured for this run.</div>';
        return;
      }

      gallery.innerHTML = items.map((item) => `
        <article class=\"shot-card\">
          <a href=\"${escapeHtml(item.web_path || '')}\" target=\"_blank\" rel=\"noopener\">
            <img src=\"${escapeHtml(item.web_path || '')}\" alt=\"${escapeHtml(item.url || 'screenshot')}\">
          </a>
          <div class=\"meta\">${escapeHtml(item.url || '')}</div>
        </article>
      `).join('');
    }

    function updateJobPanel(job) {
      const progress = Math.max(0, Math.min(100, Number(job.progress_percent || 0)));
      const runLabel = job.run_id ? ` | run=${job.run_id}` : '';
      setRunnerStatus(`job=${job.job_id} | status=${job.status}${runLabel}`);
      document.getElementById('jobProgressFill').style.width = `${progress}%`;
      document.getElementById('jobStage').textContent = job.current_stage || 'Awaiting stage...';
      const logText = (job.lines || []).join('\n');
      document.getElementById('jobLog').textContent = logText || 'No logs yet.';
      if (Array.isArray(job.screenshots) && job.screenshots.length) {
        renderScreenshots(job.screenshots);
      }
    }

    function stopJobPolling() {
      if (state.jobPollTimer !== null) {
        clearInterval(state.jobPollTimer);
        state.jobPollTimer = null;
      }
    }

    async function pollJobStatus() {
      if (!state.activeJobId) {
        return;
      }
      try {
        const job = await api('/api/job_status', { job_id: state.activeJobId });
        updateJobPanel(job);

        if (job.status === 'completed' || job.status === 'failed') {
          stopJobPolling();
          if (job.run_id) {
            await loadRuns();
            const runSelect = document.getElementById('runSelect');
            const exists = Array.from(runSelect.options).some((option) => option.value === job.run_id);
            if (exists) {
              state.runId = job.run_id;
              runSelect.value = job.run_id;
              await refreshAll();
            }
            await loadScreenshots(job.run_id);
          }

          if (job.status === 'completed') {
            setStatus('Audit job completed.');
          } else {
            setStatus(job.error || 'Audit job failed.', true);
          }
        }
      } catch (error) {
        stopJobPolling();
        setStatus(error.message, true);
      }
    }

    function startJobPolling(jobId) {
      state.activeJobId = jobId;
      stopJobPolling();
      state.jobPollTimer = window.setInterval(pollJobStatus, 1800);
      void pollJobStatus();
    }

    async function refreshSummary() {
      const payload = await api('/api/summary', { run_id: state.runId });
      state.lastSummary = payload;
      renderCards(payload);
      renderObservability(payload);
    }

    async function refreshIssues() {
      const payload = await api('/api/issues', issueFilters());
      renderIssues(payload);
    }

    async function refreshPages() {
      const payload = await api('/api/pages', pageFilters());
      renderPages(payload);
    }

    async function loadScreenshots(runId = state.runId) {
      if (!runId) {
        renderScreenshots([]);
        return;
      }
      const payload = await api('/api/screenshots', { run_id: runId });
      renderScreenshots(payload.items || []);
    }

    async function refreshAll() {
      if (!state.runId) {
        return;
      }
      setStatus('Loading run data...');
      try {
        await Promise.all([refreshSummary(), refreshIssues(), refreshPages(), loadScreenshots(state.runId)]);
        setStatus('Ready.');
      } catch (error) {
        setStatus(error.message, true);
      }
    }

    async function loadRuns() {
      setStatus('Loading runs...');
      try {
        const payload = await api('/api/runs');
        const runSelect = document.getElementById('runSelect');
        const compareSelect = document.getElementById('compareRunSelect');
        runSelect.innerHTML = '';
        compareSelect.innerHTML = '';

        for (const run of payload.runs) {
          const label = `${run.started_at} | ${run.domain} | ${run.status} | ${run.run_profile}`;
          const option = document.createElement('option');
          option.value = run.run_id;
          option.textContent = label;
          runSelect.appendChild(option);

          const compareOption = document.createElement('option');
          compareOption.value = run.run_id;
          compareOption.textContent = label;
          compareSelect.appendChild(compareOption);
        }

        state.runId = payload.default_run_id || '';
        runSelect.value = state.runId;

        const alt = payload.runs.find((run) => run.run_id !== state.runId);
        state.compareRunId = alt ? alt.run_id : state.runId;
        compareSelect.value = state.compareRunId;

        if (!state.runId) {
          renderScreenshots([]);
          setStatus('No runs found. Start an audit job.', true);
          return;
        }

        await refreshAll();
      } catch (error) {
        setStatus(error.message, true);
      }
    }

    async function runCompare() {
      if (!state.runId) {
        return;
      }
      const compareRunId = document.getElementById('compareRunSelect').value;
      if (!compareRunId) {
        setStatus('Select a comparison run first.', true);
        return;
      }
      setStatus('Computing run comparison...');
      try {
        const payload = await api('/api/compare', {
          left_run_id: state.runId,
          right_run_id: compareRunId,
        });
        renderCompare(payload);
        setStatus('Comparison ready.');
      } catch (error) {
        setStatus(error.message, true);
      }
    }

    async function loadUrlDetail(url) {
      setStatus('Loading URL detail...');
      try {
        const payload = await api('/api/url_detail', {
          run_id: state.runId,
          url,
        });
        renderUrlDetail(payload);
        setStatus('URL detail ready.');
      } catch (error) {
        setStatus(error.message, true);
      }
    }

    function exportDataset(dataset, filters) {
      if (!state.runId) {
        return;
      }
      const url = new URL('/api/export', window.location.origin);
      url.searchParams.set('dataset', dataset);
      for (const [key, value] of Object.entries(filters)) {
        if (value !== undefined && value !== null && value !== '') {
          url.searchParams.set(key, value);
        }
      }
      window.open(url.toString(), '_blank', 'noopener');
    }

    async function launchAudit() {
      const domain = document.getElementById('runDomain').value.trim();
      if (!domain) {
        setStatus('domain is required', true);
        return;
      }

      const payload = {
        domain,
        run_profile: document.getElementById('runProfile').value,
        max_pages: Number(document.getElementById('runMaxPages').value || 50),
        render_mode: document.getElementById('runRenderMode').value,
        max_render_pages: Number(document.getElementById('runMaxRenderPages').value || 0),
        performance_targets: Number(document.getElementById('runPerfTargets').value || 2),
        screenshot_count: Number(document.getElementById('runShotCount').value || 4),
      };

      setStatus('Submitting audit job...');
      try {
        const job = await apiPost('/api/run_audit', payload);
        updateJobPanel(job);
        startJobPolling(job.job_id);
        setStatus('Audit job accepted.');
      } catch (error) {
        setStatus(error.message, true);
      }
    }

    async function restoreLatestJob() {
      try {
        const payload = await api('/api/jobs');
        const jobs = payload.jobs || [];
        if (!jobs.length) {
          return;
        }

        const candidate = jobs.find((job) => job.status === 'running' || job.status === 'queued') || jobs[0];
        updateJobPanel(candidate);
        if (candidate.status === 'running' || candidate.status === 'queued') {
          startJobPolling(candidate.job_id);
        }
      } catch (_error) {
        // Non-fatal on page load; core read-only views continue to work.
      }
    }

    function wireEvents() {
      document.getElementById('runSelect').addEventListener('change', async (event) => {
        state.runId = event.target.value;
        document.getElementById('issuePage').value = '1';
        document.getElementById('pagePage').value = '1';
        await refreshAll();
      });

      document.getElementById('refreshBtn').addEventListener('click', refreshAll);
      document.getElementById('applyIssueFilters').addEventListener('click', refreshIssues);
      document.getElementById('applyPageFilters').addEventListener('click', refreshPages);
      document.getElementById('runCompare').addEventListener('click', runCompare);
      document.getElementById('startAuditBtn').addEventListener('click', launchAudit);
      document.getElementById('refreshShotsBtn').addEventListener('click', () => loadScreenshots(state.runId));

      document.getElementById('exportIssues').addEventListener('click', () => {
        exportDataset('issues', issueFilters());
      });
      document.getElementById('exportPages').addEventListener('click', () => {
        exportDataset('pages', pageFilters());
      });

      document.getElementById('issuesBody').addEventListener('click', (event) => {
        const trigger = event.target.closest('button[data-url]');
        if (!trigger) {
          return;
        }
        event.preventDefault();
        loadUrlDetail(trigger.dataset.url);
      });

      document.getElementById('pagesBody').addEventListener('click', (event) => {
        const trigger = event.target.closest('button[data-url]');
        if (!trigger) {
          return;
        }
        event.preventDefault();
        loadUrlDetail(trigger.dataset.url);
      });
    }

    async function boot() {
      wireEvents();
      await loadRuns();
      await restoreLatestJob();
    }

    boot();
  </script>
</body>
</html>
"""


def _build_handler(
  store: DashboardStore,
  job_manager: AuditJobManager,
  html_supplier: Callable[[], str],
) -> type[BaseHTTPRequestHandler]:
    class DashboardHandler(BaseHTTPRequestHandler):
        @staticmethod
        def _is_client_disconnect(exc: BaseException) -> bool:
            if isinstance(exc, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, TimeoutError)):
                return True
            if isinstance(exc, OSError):
                # EPIPE=32, ECONNRESET=104, ENOTCONN=107
                return exc.errno in {32, 104, 107}
            return False

        def _send_bytes(
            self,
            payload: bytes,
            *,
            status: HTTPStatus = HTTPStatus.OK,
            content_type: str = "application/octet-stream",
            extra_headers: dict[str, str] | None = None,
        ) -> None:
            try:
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                if extra_headers:
                    for name, value in extra_headers.items():
                        self.send_header(name, value)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            except BaseException as exc:
                if self._is_client_disconnect(exc):
                    return
                raise

        def _send_json(
            self,
            payload: Any,
            *,
            status: HTTPStatus = HTTPStatus.OK,
            extra_headers: dict[str, str] | None = None,
        ) -> None:
            content = _serialize_payload(payload)
            self._send_bytes(
                content,
                status=status,
                content_type="application/json; charset=utf-8",
                extra_headers=extra_headers,
            )

        def _send_html(self, content: str) -> None:
            payload = content.encode("utf-8")
            self._send_bytes(payload, status=HTTPStatus.OK, content_type="text/html; charset=utf-8")

        def _send_csv(
            self,
            content: str,
            filename: str,
            *,
            extra_headers: dict[str, str] | None = None,
        ) -> None:
            payload = content.encode("utf-8")
            headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
            if extra_headers:
                headers.update(extra_headers)
            self._send_bytes(
                payload,
                status=HTTPStatus.OK,
                content_type="text/csv; charset=utf-8",
                extra_headers=headers,
            )

        def _send_file(self, file_path: Path) -> None:
            payload = file_path.read_bytes()
            content_type, _encoding = mimetypes.guess_type(str(file_path))
            self._send_bytes(payload, status=HTTPStatus.OK, content_type=content_type or "application/octet-stream")

        def _read_json_body(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length", "0") or 0)
            if length <= 0:
                return {}
            raw = self.rfile.read(length)
            if not raw:
                return {}
            try:
                parsed = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError as exc:
                raise ValueError("request body must be valid JSON") from exc
            if not isinstance(parsed, dict):
                raise ValueError("request body must be a JSON object")
            return parsed

        def _resolve_artifact_path(self, relative_path: str) -> Path:
            rel = relative_path.lstrip("/")
            if not rel:
                raise KeyError("artifact not found")
            target = (job_manager.output_dir / rel).resolve()
            root = job_manager.output_dir.resolve()
            if root not in target.parents and target != root:
                raise ValueError("invalid artifact path")
            if not target.exists() or not target.is_file():
                raise KeyError("artifact not found")
            return target

        def _error_text(self, exc: Exception) -> str:
            if isinstance(exc, KeyError) and exc.args:
                return str(exc.args[0])
            return str(exc)

        def _param(self, query: dict[str, list[str]], name: str, default: str = "") -> str:
            values = query.get(name)
            if not values:
                return default
            return values[0]

        def _require(self, query: dict[str, list[str]], name: str) -> str:
            value = self._param(query, name).strip()
            if not value:
                raise ValueError(f"{name} is required")
            return value

        def _page_filters(self, query: dict[str, list[str]]) -> dict[str, Any]:
            return {
                "page_type": self._param(query, "page_type", "").strip(),
                "status_code": _coerce_optional_int(self._param(query, "status_code", ""), name="status_code"),
                "max_depth": _coerce_optional_int(self._param(query, "max_depth", ""), name="max_depth"),
                "min_score": _coerce_optional_int(self._param(query, "min_score", ""), name="min_score"),
                "severity": self._param(query, "severity", "").strip().lower(),
                "query": self._param(query, "query", "").strip(),
            }

        def _issue_filters(self, query: dict[str, list[str]]) -> dict[str, Any]:
            return {
                "severity": self._param(query, "severity", "").strip().lower(),
                "issue_code": self._param(query, "issue_code", "").strip().upper(),
                "page_type": self._param(query, "page_type", "").strip(),
            "status_code": _coerce_optional_int(self._param(query, "status_code", ""), name="status_code"),
            "certainty_state": self._param(query, "certainty_state", "").strip(),
            "min_priority": _coerce_optional_int(self._param(query, "min_priority", ""), name="min_priority"),
                "query": self._param(query, "query", "").strip(),
            }

        def _get_healthz(self, _query: dict[str, list[str]]) -> None:
          self._send_json({"ok": True})

        def _get_jobs(self, _query: dict[str, list[str]]) -> None:
          self._send_json(job_manager.jobs())

        def _get_job_status(self, query: dict[str, list[str]]) -> None:
          job_id = self._require(query, "job_id")
          self._send_json(job_manager.job_status(job_id))

        def _get_screenshots(self, query: dict[str, list[str]]) -> None:
          run_id = self._require(query, "run_id")
          self._send_json({"run_id": run_id, "items": job_manager.screenshots_for_run(run_id)})

        def _get_runs(self, query: dict[str, list[str]]) -> None:
          limit = _coerce_int(
            self._param(query, "limit", "50"),
            name="limit",
            default=50,
            minimum=1,
            maximum=500,
          )
          self._send_json(store.list_runs(limit=limit))

        def _get_summary(self, query: dict[str, list[str]]) -> None:
          run_id = self._require(query, "run_id")
          self._send_json(store.summary(run_id))

        def _get_pages(self, query: dict[str, list[str]]) -> None:
          run_id = self._require(query, "run_id")
          page = _coerce_int(
            self._param(query, "page", "1"),
            name="page",
            default=1,
            minimum=1,
            maximum=10000,
          )
          page_size = _coerce_int(
            self._param(query, "page_size", str(DEFAULT_PAGE_SIZE)),
            name="page_size",
            default=DEFAULT_PAGE_SIZE,
            minimum=1,
            maximum=MAX_PAGE_SIZE,
          )
          sort_by = self._param(query, "sort_by", "score").strip().lower()
          sort_dir = _coerce_sort_direction(
            self._param(query, "sort_dir", "desc"),
            default="desc",
          )
          payload = store.list_pages(
            run_id,
            self._page_filters(query),
            page,
            page_size,
            sort_by,
            sort_dir,
          )
          self._send_json(payload)

        def _get_issues(self, query: dict[str, list[str]]) -> None:
          run_id = self._require(query, "run_id")
          page = _coerce_int(
            self._param(query, "page", "1"),
            name="page",
            default=1,
            minimum=1,
            maximum=10000,
          )
          page_size = _coerce_int(
            self._param(query, "page_size", str(DEFAULT_PAGE_SIZE)),
            name="page_size",
            default=DEFAULT_PAGE_SIZE,
            minimum=1,
            maximum=MAX_PAGE_SIZE,
          )
          sort_by = self._param(query, "sort_by", "priority").strip().lower()
          sort_dir = _coerce_sort_direction(
            self._param(query, "sort_dir", "desc"),
            default="desc",
          )
          payload = store.list_issues(
            run_id,
            self._issue_filters(query),
            page,
            page_size,
            sort_by,
            sort_dir,
          )
          self._send_json(payload)

        def _get_url_detail(self, query: dict[str, list[str]]) -> None:
          run_id = self._require(query, "run_id")
          url = self._require(query, "url")
          self._send_json(store.url_detail(run_id, url))

        def _get_compare(self, query: dict[str, list[str]]) -> None:
          left_run_id = self._require(query, "left_run_id")
          right_run_id = self._require(query, "right_run_id")
          self._send_json(store.compare_runs(left_run_id, right_run_id))

        def _get_architecture(self, query: dict[str, list[str]]) -> None:
          run_id = self._require(query, "run_id")
          self._send_json(store.architecture_insights(run_id))

        def _get_export(self, query: dict[str, list[str]]) -> None:
          run_id = self._require(query, "run_id")
          dataset = self._require(query, "dataset")
          filters = self._issue_filters(query) if dataset == "issues" else self._page_filters(query)
          content, filename = store.export_dataset(run_id, dataset, filters)
          self._send_csv(content, filename)

        def _post_query(self) -> None:
          payload = self._read_json_body()
          run_id = str(payload.get("run_id") or "").strip()
          query = str(payload.get("query") or "")
          limit = _coerce_int(
            str(payload.get("limit") or "250"),
            name="limit",
            default=250,
            minimum=1,
            maximum=MAX_QUERY_ROWS,
          )
          self._send_json(
            store.run_query(query=query, run_id=run_id, limit=limit),
            extra_headers={"X-Database-Access": "read-only"},
          )

        def _post_query_export(self) -> None:
          payload = self._read_json_body()
          run_id = str(payload.get("run_id") or "").strip()
          query = str(payload.get("query") or "")
          limit = _coerce_int(
            str(payload.get("limit") or "250"),
            name="limit",
            default=250,
            minimum=1,
            maximum=MAX_QUERY_ROWS,
          )
          content, filename = store.export_query(query=query, run_id=run_id, limit=limit)
          self._send_csv(
            content,
            filename,
            extra_headers={"X-Database-Access": "read-only"},
          )

        def _post_run_audit(self) -> None:
          payload = self._read_json_body()
          domain = str(payload.get("domain") or "").strip()
          if not domain:
            raise ValueError("domain is required")

          run_profile = str(payload.get("run_profile") or "standard").strip().lower()
          if run_profile not in {"exploratory", "standard", "deep"}:
            raise ValueError("run_profile must be exploratory, standard, or deep")

          render_mode = str(payload.get("render_mode") or "none").strip().lower()
          if render_mode not in {"none", "sample", "all"}:
            raise ValueError("render_mode must be none, sample, or all")

          max_pages = _coerce_int(
            str(payload.get("max_pages") or "20"),
            name="max_pages",
            default=20,
            minimum=1,
            maximum=5000,
          )
          max_render_pages = _coerce_int(
            str(payload.get("max_render_pages") or "0"),
            name="max_render_pages",
            default=0,
            minimum=0,
            maximum=1000,
          )
          performance_targets = _coerce_int(
            str(payload.get("performance_targets") or "2"),
            name="performance_targets",
            default=2,
            minimum=1,
            maximum=100,
          )
          screenshot_count = _coerce_int(
            str(payload.get("screenshot_count") or "4"),
            name="screenshot_count",
            default=4,
            minimum=0,
            maximum=20,
          )

          job = job_manager.start_job(
            domain=domain,
            run_profile=run_profile,
            max_pages=max_pages,
            render_mode=render_mode,
            max_render_pages=max_render_pages,
            performance_targets=performance_targets,
            screenshot_count=screenshot_count,
          )
          self._send_json(job, status=HTTPStatus.ACCEPTED)

        def _post_cancel_job(self) -> None:
          payload = self._read_json_body()
          job_id = str(payload.get("job_id") or "").strip()
          if not job_id:
            raise ValueError("job_id is required")

          job = job_manager.cancel_job(job_id)
          self._send_json(job, status=HTTPStatus.ACCEPTED)

        def _dispatch_get_api(self, path: str, query: dict[str, list[str]]) -> bool:
          routes: dict[str, Callable[[dict[str, list[str]]], None]] = {
            "/api/healthz": self._get_healthz,
            "/api/jobs": self._get_jobs,
            "/api/job_status": self._get_job_status,
            "/api/screenshots": self._get_screenshots,
            "/api/runs": self._get_runs,
            "/api/summary": self._get_summary,
            "/api/pages": self._get_pages,
            "/api/issues": self._get_issues,
            "/api/url_detail": self._get_url_detail,
            "/api/compare": self._get_compare,
            "/api/architecture": self._get_architecture,
            "/api/export": self._get_export,
          }
          handler = routes.get(path)
          if handler is None:
            return False
          handler(query)
          return True

        def _dispatch_post_api(self, path: str) -> bool:
          routes: dict[str, Callable[[], None]] = {
            "/api/query": self._post_query,
            "/api/query_export": self._post_query_export,
            "/api/run_audit": self._post_run_audit,
            "/api/cancel_job": self._post_cancel_job,
          }
          handler = routes.get(path)
          if handler is None:
            return False
          handler()
          return True

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlsplit(self.path)
            path = parsed.path
            query = parse_qs(parsed.query)

            try:
                if path.startswith("/artifacts/"):
                    target = self._resolve_artifact_path(path[len("/artifacts/") :])
                    self._send_file(target)
                    return

                if path in {"/", "/index.html"}:
                    self._send_html(html_supplier())
                    return

                if self._dispatch_get_api(path, query):
                  return

                self._send_json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)
            except KeyError as exc:
                self._send_json({"error": self._error_text(exc)}, status=HTTPStatus.NOT_FOUND)
            except ValueError as exc:
                self._send_json({"error": self._error_text(exc)}, status=HTTPStatus.BAD_REQUEST)
            except Exception:
                self._send_json({"error": "internal_server_error"}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlsplit(self.path)
            path = parsed.path

            try:
                if self._dispatch_post_api(path):
                    return

                self._send_json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)
            except KeyError as exc:
                self._send_json({"error": self._error_text(exc)}, status=HTTPStatus.NOT_FOUND)
            except ValueError as exc:
                self._send_json({"error": self._error_text(exc)}, status=HTTPStatus.BAD_REQUEST)
            except Exception:
                self._send_json({"error": "internal_server_error"}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            return

    return DashboardHandler


def create_dashboard_server(
  db_path: Path,
  host: str = "127.0.0.1",
  port: int = 8765,
  *,
  queue_db_path: Path | None = None,
  start_worker: bool = False,
) -> ThreadingHTTPServer:
  if not db_path.exists():
    raise FileNotFoundError(f"database file not found: {db_path}")
  store = DashboardStore(db_path)
  project_root = Path(__file__).resolve().parents[1]
  job_manager = AuditJobManager(
    db_path=db_path,
    project_root=project_root,
    queue_db_path=queue_db_path,
    embedded_worker=start_worker,
  )
  handler = _build_handler(store, job_manager, _build_html)
  return ThreadingHTTPServer((host, port), handler)


def run_dashboard(
  db_path: Path,
  host: str = "127.0.0.1",
  port: int = 8765,
  *,
  queue_db_path: Path | None = None,
  start_worker: bool = True,
) -> None:
  server = create_dashboard_server(
    db_path=db_path,
    host=host,
    port=port,
    queue_db_path=queue_db_path,
    start_worker=start_worker,
  )
  address = server.server_address
  bind_host = str(address[0])
  bind_port = int(address[1])
  print(f"dashboard listening on http://{bind_host}:{bind_port}")
  try:
    server.serve_forever()
  except KeyboardInterrupt:
    print("\ndashboard stopped")
  finally:
    server.server_close()
