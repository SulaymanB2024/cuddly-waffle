#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PORT="${SEO_AUDIT_DASHBOARD_PORT:-9080}"
FALLBACK_PORT="${SEO_AUDIT_DASHBOARD_FALLBACK_PORT:-8765}"
HOST="${SEO_AUDIT_DASHBOARD_HOST:-0.0.0.0}"
DB_PATH="${SEO_AUDIT_DASHBOARD_DB:-./out/audit.sqlite}"

resolve_python_bin() {
  local -a candidates=()
  local candidate

  if [[ -n "${SEO_AUDIT_PYTHON_BIN:-}" ]]; then
    candidates+=("${SEO_AUDIT_PYTHON_BIN}")
  fi

  if [[ -n "${VIRTUAL_ENV:-}" ]]; then
    candidates+=("${VIRTUAL_ENV}/bin/python")
  fi

  candidates+=("${ROOT_DIR}/.venv/bin/python")

  for candidate in "${HOME}/.cache/pypoetry/virtualenvs/seo-audit-"*/bin/python; do
    candidates+=("$candidate")
  done

  if command -v python >/dev/null 2>&1; then
    candidates+=("$(command -v python)")
  fi

  if command -v python3 >/dev/null 2>&1; then
    candidates+=("$(command -v python3)")
  fi

  for candidate in "${candidates[@]}"; do
    if [[ -x "$candidate" ]] && "$candidate" -c "import lxml" >/dev/null 2>&1; then
      echo "$candidate"
      return 0
    fi
  done

  for candidate in "${candidates[@]}"; do
    if [[ -x "$candidate" ]]; then
      echo "$candidate"
      return 0
    fi
  done

  return 1
}

if ! PYTHON_BIN="$(resolve_python_bin)"; then
  echo "could not resolve a Python interpreter for dashboard startup"
  exit 1
fi

is_dashboard_health_ok() {
  local candidate_port="$1"
  local payload
  if ! payload="$(curl -sS --max-time 2 "http://127.0.0.1:${candidate_port}/api/healthz" 2>/dev/null)"; then
    return 1
  fi
  [[ "$payload" == *'"ok"'* ]]
}

is_any_listener() {
  local candidate_port="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -iTCP:"$candidate_port" -sTCP:LISTEN -t >/dev/null 2>&1
    return $?
  fi
  return 1
}

listener_pid() {
  local candidate_port="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -iTCP:"$candidate_port" -sTCP:LISTEN -t 2>/dev/null | head -n 1
    return 0
  fi
  return 1
}

maybe_reclaim_port() {
  local candidate_port="$1"
  local pid
  pid="$(listener_pid "$candidate_port")"
  if [[ -z "$pid" ]]; then
    return 1
  fi

  local cmd
  cmd="$(ps -p "$pid" -o args= 2>/dev/null || true)"
  if [[ "$cmd" == *"http.server"* ]]; then
    echo "reclaiming port $candidate_port from: $cmd"
    kill "$pid" >/dev/null 2>&1 || true
    if is_any_listener "$candidate_port"; then
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
    if ! is_any_listener "$candidate_port"; then
      return 0
    fi
  fi
  return 1
}

if is_dashboard_health_ok "$PORT"; then
  echo "dashboard already running on port $PORT"
  exit 0
fi

if is_any_listener "$PORT"; then
  if maybe_reclaim_port "$PORT"; then
    echo "port $PORT reclaimed for dashboard"
  elif [[ "$PORT" != "$FALLBACK_PORT" ]]; then
    echo "port $PORT is occupied by a non-dashboard service; falling back to $FALLBACK_PORT"
    PORT="$FALLBACK_PORT"
  fi
fi

if is_dashboard_health_ok "$PORT"; then
  echo "dashboard already running on port $PORT"
  exit 0
fi

if is_any_listener "$PORT"; then
  echo "port $PORT is occupied and is not serving the dashboard API"
  exit 1
fi

if [[ ! -f "$DB_PATH" ]]; then
  mkdir -p "$(dirname "$DB_PATH")"
  SEO_AUDIT_DASHBOARD_DB_INIT="$DB_PATH" "$PYTHON_BIN" - <<'PY'
import os
from pathlib import Path
from seo_audit.storage import Storage

db_path = Path(os.environ["SEO_AUDIT_DASHBOARD_DB_INIT"])
storage = Storage(db_path)
storage.init_db()
storage.close()
print(f"initialized dashboard database at {db_path}")
PY
fi

echo "starting dashboard on http://${HOST}:${PORT}"
echo "using python interpreter: $PYTHON_BIN"
exec "$PYTHON_BIN" -m seo_audit dashboard --db "$DB_PATH" --host "$HOST" --port "$PORT"
