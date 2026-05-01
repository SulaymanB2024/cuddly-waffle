from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import requests


OFFSITE_COMMONCRAWL_MODES: set[str] = {"ranks", "domains", "verify"}
OFFSITE_COMMONCRAWL_SCHEDULES: set[str] = {
    "concurrent_best_effort",
    "background_best_effort",
    "background_wait",
    "blocking",
}
OFFSITE_COMMONCRAWL_DEFAULT_SCHEDULE = "concurrent_best_effort"
OFFSITE_COMMONCRAWL_SCHEDULE_ALIASES: dict[str, str] = {
    "background_best_effort": OFFSITE_COMMONCRAWL_DEFAULT_SCHEDULE,
}

STATUS_SKIPPED_DISABLED = "skipped_disabled"
STATUS_PENDING_BACKGROUND = "pending_background"
STATUS_SUCCESS = "success"
STATUS_SUCCESS_PARTIAL = "success_partial"
STATUS_SUCCESS_EMPTY = "success_empty"
STATUS_SKIPPED_COLD_EDGE_CACHE = "skipped_cold_edge_cache"
STATUS_TIMEOUT_BACKGROUND = "timeout_background"
STATUS_FAILED_HTTP = "failed_http"
STATUS_FAILED_QUERY = "failed_query"
STATUS_FAILED_MISSING_DEPENDENCY = "failed_missing_dependency"
STATUS_DEFERRED_VERIFY = "deferred_verify_not_implemented"

_MANIFEST_FILE_NAME = "manifest.json"
_MANIFEST_SCHEMA_VERSION = 2
_MATERIALIZATION_SCHEMA_VERSION = 1
_COLLINFO_URL = "https://index.commoncrawl.org/collinfo.json"
_DEFAULT_HTTP_TIMEOUT_SECONDS = 30.0


class OffsiteCommonCrawlCancelledError(RuntimeError):
    """Raised when the background worker is cancelled cooperatively."""


@dataclass(slots=True)
class OffsiteCommonCrawlManifest:
    release: str
    schema_version: int = _MANIFEST_SCHEMA_VERSION
    materialization_version: int = _MATERIALIZATION_SCHEMA_VERSION
    duckdb_version: str = ""
    vertices_ready: bool = False
    ranks_ready: bool = False
    edges_ready: bool = False
    vertices_source_url: str = ""
    ranks_source_url: str = ""
    edges_source_url: str = ""
    vertices_sha256: str = ""
    ranks_sha256: str = ""
    edges_sha256: str = ""
    vertices_etag: str = ""
    ranks_etag: str = ""
    edges_etag: str = ""
    vertices_bytes: int = 0
    ranks_bytes: int = 0
    edges_bytes: int = 0
    column_mapping_json: str = "{}"
    downloaded_at: str = ""
    last_used_at: str = ""


@dataclass(slots=True)
class OffsiteCommonCrawlLaunchContext:
    release: str
    cache_dir: Path
    release_dir: Path
    cache_state: str
    manifest: OffsiteCommonCrawlManifest


@dataclass(slots=True)
class OffsiteCommonCrawlWorkerRequest:
    target_domain: str
    mode: str
    schedule: str
    release: str
    cache_dir: Path
    max_linking_domains: int = 100
    time_budget_seconds: int = 180
    allow_cold_edge_download: bool = False
    compare_domains: tuple[str, ...] = ()


@dataclass(slots=True)
class OffsiteCommonCrawlAssetResult:
    path: Path
    source_url: str = ""
    bytes_downloaded: int = 0
    sha256: str = ""
    etag: str = ""


@dataclass(slots=True)
class OffsiteCommonCrawlWorkerControl:
    stop_event: threading.Event = field(default_factory=threading.Event)
    _connection: Any | None = field(default=None, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def request_stop(self) -> None:
        self.stop_event.set()

    def request_interrupt(self) -> None:
        self.stop_event.set()
        with self._lock:
            connection = self._connection
        if connection is None:
            return
        try:
            connection.interrupt()
        except Exception:
            # DuckDB interrupt support is best-effort.
            pass

    def attach_connection(self, connection: Any) -> None:
        with self._lock:
            self._connection = connection

    def detach_connection(self) -> None:
        with self._lock:
            self._connection = None


@dataclass(slots=True)
class OffsiteCommonCrawlSummaryPayload:
    target_domain: str
    cc_release: str
    mode: str
    schedule: str
    status: str
    cache_state: str
    target_found_flag: int
    harmonic_centrality: float | None = None
    pagerank: float | None = None
    referring_domain_count: int = 0
    weighted_referring_domain_score: float | None = None
    avg_referrer_harmonic: float | None = None
    avg_referrer_pagerank: float | None = None
    top_referrer_concentration: float | None = None
    comparison_domain_count: int = 0
    query_elapsed_ms: int = 0
    background_started_at: str = ""
    background_finished_at: str = ""
    notes_json: str = "{}"


@dataclass(slots=True)
class OffsiteCommonCrawlLinkingDomainPayload:
    linking_domain: str
    source_num_hosts: int
    source_harmonic_centrality: float | None
    source_pagerank: float | None
    rank_bucket: str
    evidence_json: str = "{}"


@dataclass(slots=True)
class OffsiteCommonCrawlComparisonPayload:
    compare_domain: str
    cc_release: str
    harmonic_centrality: float | None
    pagerank: float | None
    rank_gap_vs_target: float | None
    pagerank_gap_vs_target: float | None


@dataclass(slots=True)
class OffsiteCommonCrawlWorkerPayload:
    summary: OffsiteCommonCrawlSummaryPayload
    linking_domains: list[OffsiteCommonCrawlLinkingDomainPayload] = field(default_factory=list)
    comparisons: list[OffsiteCommonCrawlComparisonPayload] = field(default_factory=list)


# Backward-compatible alias for external imports.
OffsiteCommonCrawlCompetitorPayload = OffsiteCommonCrawlComparisonPayload


def canonicalize_offsite_schedule(raw_value: object) -> str:
    schedule = str(raw_value or OFFSITE_COMMONCRAWL_DEFAULT_SCHEDULE).strip().lower()
    schedule = OFFSITE_COMMONCRAWL_SCHEDULE_ALIASES.get(schedule, schedule)
    if schedule not in OFFSITE_COMMONCRAWL_SCHEDULES:
        return OFFSITE_COMMONCRAWL_DEFAULT_SCHEDULE
    return schedule


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_domain_for_commoncrawl(raw_domain: str) -> str:
    raw = str(raw_domain or "").strip().lower()
    if not raw:
        return ""
    parsed = urlsplit(raw if "://" in raw else f"https://{raw}")
    hostname = (parsed.hostname or raw).strip().lower().rstrip(".")
    if hostname.startswith("www."):
        hostname = hostname[4:]
    return hostname


def to_reverse_domain(domain: str) -> str:
    normalized = normalize_domain_for_commoncrawl(domain)
    if not normalized:
        return ""
    labels = [label for label in normalized.split(".") if label]
    return ".".join(reversed(labels))


def from_reverse_domain(reverse_domain: str) -> str:
    value = str(reverse_domain or "").strip().lower()
    if not value:
        return ""
    labels = [label for label in value.split(".") if label]
    return ".".join(reversed(labels))


def expand_commoncrawl_cache_dir(cache_dir: str | Path) -> Path:
    return Path(str(cache_dir)).expanduser().resolve()


def _manifest_path(release_dir: Path) -> Path:
    return release_dir / _MANIFEST_FILE_NAME


def _load_manifest(release_dir: Path, release: str) -> OffsiteCommonCrawlManifest:
    path = _manifest_path(release_dir)
    if not path.exists():
        return OffsiteCommonCrawlManifest(release=release)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return OffsiteCommonCrawlManifest(release=release)
    return OffsiteCommonCrawlManifest(
        release=str(payload.get("release") or release),
        schema_version=int(payload.get("schema_version") or _MANIFEST_SCHEMA_VERSION),
        materialization_version=int(payload.get("materialization_version") or _MATERIALIZATION_SCHEMA_VERSION),
        duckdb_version=str(payload.get("duckdb_version") or ""),
        vertices_ready=bool(payload.get("vertices_ready", False)),
        ranks_ready=bool(payload.get("ranks_ready", False)),
        edges_ready=bool(payload.get("edges_ready", False)),
        vertices_source_url=str(payload.get("vertices_source_url") or ""),
        ranks_source_url=str(payload.get("ranks_source_url") or ""),
        edges_source_url=str(payload.get("edges_source_url") or ""),
        vertices_sha256=str(payload.get("vertices_sha256") or ""),
        ranks_sha256=str(payload.get("ranks_sha256") or ""),
        edges_sha256=str(payload.get("edges_sha256") or ""),
        vertices_etag=str(payload.get("vertices_etag") or ""),
        ranks_etag=str(payload.get("ranks_etag") or ""),
        edges_etag=str(payload.get("edges_etag") or ""),
        vertices_bytes=int(payload.get("vertices_bytes") or 0),
        ranks_bytes=int(payload.get("ranks_bytes") or 0),
        edges_bytes=int(payload.get("edges_bytes") or 0),
        column_mapping_json=str(payload.get("column_mapping_json") or "{}"),
        downloaded_at=str(payload.get("downloaded_at") or ""),
        last_used_at=str(payload.get("last_used_at") or ""),
    )


def _save_manifest(release_dir: Path, manifest: OffsiteCommonCrawlManifest) -> None:
    release_dir.mkdir(parents=True, exist_ok=True)
    manifest.schema_version = _MANIFEST_SCHEMA_VERSION
    manifest.last_used_at = utc_now_iso()
    path = _manifest_path(release_dir)
    tmp_path = path.with_name(f".{path.name}.tmp")
    payload = {
        "release": manifest.release,
        "schema_version": int(manifest.schema_version),
        "materialization_version": int(manifest.materialization_version),
        "duckdb_version": manifest.duckdb_version,
        "vertices_ready": int(bool(manifest.vertices_ready)),
        "ranks_ready": int(bool(manifest.ranks_ready)),
        "edges_ready": int(bool(manifest.edges_ready)),
        "vertices_source_url": manifest.vertices_source_url,
        "ranks_source_url": manifest.ranks_source_url,
        "edges_source_url": manifest.edges_source_url,
        "vertices_sha256": manifest.vertices_sha256,
        "ranks_sha256": manifest.ranks_sha256,
        "edges_sha256": manifest.edges_sha256,
        "vertices_etag": manifest.vertices_etag,
        "ranks_etag": manifest.ranks_etag,
        "edges_etag": manifest.edges_etag,
        "vertices_bytes": int(manifest.vertices_bytes),
        "ranks_bytes": int(manifest.ranks_bytes),
        "edges_bytes": int(manifest.edges_bytes),
        "column_mapping_json": manifest.column_mapping_json,
        "downloaded_at": manifest.downloaded_at,
        "last_used_at": manifest.last_used_at,
    }
    tmp_path.write_text(json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8")
    os.replace(tmp_path, path)


def derive_cache_state(manifest: OffsiteCommonCrawlManifest) -> str:
    if manifest.vertices_ready and manifest.ranks_ready and manifest.edges_ready:
        return "warm_edges"
    if manifest.vertices_ready and manifest.ranks_ready:
        return "warm_ranks"
    if manifest.vertices_ready or manifest.ranks_ready or manifest.edges_ready:
        return "partial"
    return "cold"


def _latest_cached_release(cache_dir: Path) -> str:
    if not cache_dir.exists():
        return ""
    candidates = sorted(
        [path.name for path in cache_dir.iterdir() if path.is_dir() and path.name.startswith("CC-MAIN-")],
        reverse=True,
    )
    return candidates[0] if candidates else ""


def resolve_commoncrawl_release(
    requested_release: str,
    *,
    cache_dir: Path,
    session: requests.Session | None = None,
    timeout_seconds: float = 5.0,
) -> str:
    requested = str(requested_release or "auto").strip()
    if requested and requested.lower() != "auto":
        return requested

    owns_session = session is None
    client = session or requests.Session()
    try:
        response = client.get(_COLLINFO_URL, timeout=max(1.0, float(timeout_seconds)))
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            for row in payload:
                if not isinstance(row, dict):
                    continue
                candidate = str(row.get("id") or row.get("name") or "").strip()
                if candidate.startswith("CC-MAIN-"):
                    return candidate
    except Exception:
        cached_release = _latest_cached_release(cache_dir)
        if cached_release:
            return cached_release
    finally:
        if owns_session:
            client.close()

    return "CC-MAIN-UNKNOWN"


def inspect_commoncrawl_launch(
    requested_release: str,
    cache_dir: str | Path,
    *,
    session: requests.Session | None = None,
    timeout_seconds: float = 5.0,
) -> OffsiteCommonCrawlLaunchContext:
    expanded_cache_dir = expand_commoncrawl_cache_dir(cache_dir)
    release = resolve_commoncrawl_release(
        requested_release,
        cache_dir=expanded_cache_dir,
        session=session,
        timeout_seconds=timeout_seconds,
    )
    release_dir = expanded_cache_dir / release
    manifest = _load_manifest(release_dir, release)
    cache_state = derive_cache_state(manifest)
    return OffsiteCommonCrawlLaunchContext(
        release=release,
        cache_dir=expanded_cache_dir,
        release_dir=release_dir,
        cache_state=cache_state,
        manifest=manifest,
    )


def _import_duckdb() -> Any:
    import duckdb

    return duckdb


def _quote_identifier(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _sql_string_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _table_exists(connection: Any, table_name: str) -> bool:
    rows = connection.execute("PRAGMA show_tables").fetchall()
    names = {str(row[0]).lower() for row in rows}
    return str(table_name).lower() in names


def _table_columns(connection: Any, table_name: str) -> list[str]:
    rows = connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    return [str(row[1]) for row in rows]


def _pick_required(columns: list[str], candidates: tuple[str, ...], *, table_name: str) -> str:
    lowered = {column.lower(): column for column in columns}
    for candidate in candidates:
        resolved = lowered.get(candidate.lower())
        if resolved:
            return resolved
    raise ValueError(f"missing required column in {table_name}: {', '.join(candidates)}")


def _pick_optional(columns: list[str], candidates: tuple[str, ...]) -> str | None:
    lowered = {column.lower(): column for column in columns}
    for candidate in candidates:
        resolved = lowered.get(candidate.lower())
        if resolved:
            return resolved
    return None


def _check_cancelled(control: OffsiteCommonCrawlWorkerControl) -> None:
    if control.stop_event.is_set():
        raise OffsiteCommonCrawlCancelledError("offsite commoncrawl worker cancelled")


def _check_time_budget(
    started_monotonic: float,
    time_budget_seconds: int,
    control: OffsiteCommonCrawlWorkerControl,
) -> None:
    if time_budget_seconds <= 0:
        return
    if (time.perf_counter() - started_monotonic) > float(time_budget_seconds):
        control.request_stop()
        raise TimeoutError("offsite commoncrawl worker time budget exceeded")


def _duckdb_runtime_version(connection: Any) -> str:
    try:
        row = connection.execute("SELECT version()").fetchone()
        return str(row[0] or "") if row else ""
    except Exception:
        return ""


def _manifest_column_mapping(manifest: OffsiteCommonCrawlManifest) -> dict[str, str]:
    try:
        payload = json.loads(manifest.column_mapping_json or "{}")
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    return {str(key): str(value) for key, value in payload.items()}


def _set_manifest_column_mapping(manifest: OffsiteCommonCrawlManifest, mapping: dict[str, str]) -> None:
    manifest.column_mapping_json = json.dumps(mapping, sort_keys=True)


def _drop_materialized_tables(connection: Any) -> None:
    for table_name in (
        "cc_domain_edges",
        "cc_domain_edges_raw",
        "cc_domain_ranks",
        "cc_domain_ranks_raw",
        "cc_domain_vertices",
        "cc_domain_vertices_raw",
    ):
        connection.execute(f"DROP TABLE IF EXISTS {table_name}")


def _manifest_requires_rebuild(manifest: OffsiteCommonCrawlManifest, runtime_duckdb_version: str) -> bool:
    if int(manifest.materialization_version or 0) != _MATERIALIZATION_SCHEMA_VERSION:
        return True
    if runtime_duckdb_version and manifest.duckdb_version and manifest.duckdb_version != runtime_duckdb_version:
        return True
    return False


def _download_asset_if_needed(
    *,
    session: requests.Session,
    release: str,
    release_dir: Path,
    asset_name: str,
    control: OffsiteCommonCrawlWorkerControl,
    timeout_seconds: float,
) -> OffsiteCommonCrawlAssetResult:
    _check_cancelled(control)
    raw_dir = release_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    target_path = raw_dir / f"{asset_name}.csv.gz"
    if target_path.exists() and target_path.stat().st_size > 0:
        return OffsiteCommonCrawlAssetResult(
            path=target_path,
            bytes_downloaded=int(target_path.stat().st_size),
        )

    release_lower = release.lower()
    base_urls = (
        f"https://data.commoncrawl.org/projects/hyperlinkgraph/{release_lower}/",
        f"https://data.commoncrawl.org/projects/hyperlinkgraph/{release}/",
    )
    name_map: dict[str, tuple[str, ...]] = {
        "vertices": (
            f"cc-main-{release_lower}-domains-vertices.csv.gz",
            f"cc-main-{release_lower}-domains-vertices.txt.gz",
            f"cc-main-{release_lower}-domain-vertices.csv.gz",
        ),
        "ranks": (
            f"cc-main-{release_lower}-domains-ranks.csv.gz",
            f"cc-main-{release_lower}-domains-ranks.txt.gz",
            f"cc-main-{release_lower}-domain-ranks.csv.gz",
        ),
        "edges": (
            f"cc-main-{release_lower}-domains-graph.csv.gz",
            f"cc-main-{release_lower}-domains-graph.txt.gz",
            f"cc-main-{release_lower}-domains-edges.csv.gz",
            f"cc-main-{release_lower}-domain-graph.csv.gz",
        ),
    }

    if asset_name not in name_map:
        raise ValueError(f"unsupported asset_name: {asset_name}")

    last_error: Exception | None = None
    for base_url in base_urls:
        for file_name in name_map[asset_name]:
            _check_cancelled(control)
            url = f"{base_url}{file_name}"
            temp_path = target_path.with_name(f".{target_path.name}.{int(time.time() * 1000)}.tmp")
            try:
                with session.get(url, stream=True, timeout=max(5.0, timeout_seconds)) as response:
                    if response.status_code >= 400:
                        continue
                    digest = hashlib.sha256()
                    byte_count = 0
                    with temp_path.open("wb") as handle:
                        for chunk in response.iter_content(chunk_size=1024 * 1024):
                            _check_cancelled(control)
                            if not chunk:
                                continue
                            handle.write(chunk)
                            digest.update(chunk)
                            byte_count += len(chunk)
                if temp_path.exists() and temp_path.stat().st_size > 0:
                    os.replace(temp_path, target_path)
                    return OffsiteCommonCrawlAssetResult(
                        path=target_path,
                        source_url=url,
                        bytes_downloaded=int(byte_count),
                        sha256=digest.hexdigest(),
                        etag=str(response.headers.get("ETag") or "").strip(),
                    )
            except OffsiteCommonCrawlCancelledError:
                if temp_path.exists():
                    temp_path.unlink(missing_ok=True)
                raise
            except Exception as exc:
                last_error = exc
                if temp_path.exists():
                    temp_path.unlink(missing_ok=True)
                continue

    if last_error is not None:
        raise last_error
    raise requests.HTTPError(f"unable to download Common Crawl {asset_name} asset for release {release}")


def _ensure_vertices_table(connection: Any, vertices_path: Path) -> dict[str, str]:
    if not _table_exists(connection, "cc_domain_vertices_raw"):
        path_literal = _sql_string_literal(str(vertices_path))
        connection.execute(
            f"CREATE TABLE cc_domain_vertices_raw AS SELECT * FROM read_csv_auto({path_literal}, union_by_name=true, ignore_errors=true)"
        )

    columns = _table_columns(connection, "cc_domain_vertices_raw")
    id_column = _pick_required(columns, ("id", "vertex_id", "node_id"), table_name="cc_domain_vertices_raw")
    domain_column = _pick_required(columns, ("domain", "name", "host"), table_name="cc_domain_vertices_raw")
    num_hosts_column = _pick_optional(columns, ("num_hosts", "host_count", "hosts", "degree"))

    num_hosts_expr = "0"
    if num_hosts_column:
        num_hosts_expr = f"COALESCE(CAST({_quote_identifier(num_hosts_column)} AS BIGINT), 0)"

    connection.execute(
        f"""
        CREATE OR REPLACE TABLE cc_domain_vertices AS
        SELECT
            CAST({_quote_identifier(id_column)} AS BIGINT) AS id,
            LOWER(TRIM(CAST({_quote_identifier(domain_column)} AS VARCHAR))) AS domain,
            {num_hosts_expr} AS num_hosts
        FROM cc_domain_vertices_raw
        """
    )

    return {
        "vertices_id": id_column,
        "vertices_domain": domain_column,
        "vertices_num_hosts": num_hosts_column or "",
    }


def _ensure_ranks_table(connection: Any, ranks_path: Path) -> dict[str, str]:
    if not _table_exists(connection, "cc_domain_ranks_raw"):
        path_literal = _sql_string_literal(str(ranks_path))
        connection.execute(
            f"CREATE TABLE cc_domain_ranks_raw AS SELECT * FROM read_csv_auto({path_literal}, union_by_name=true, ignore_errors=true)"
        )

    columns = _table_columns(connection, "cc_domain_ranks_raw")
    id_column = _pick_required(columns, ("id", "vertex_id", "node_id"), table_name="cc_domain_ranks_raw")
    harmonic_column = _pick_optional(columns, ("harmonic_centrality", "harmonic", "harmonic_rank"))
    pagerank_column = _pick_optional(columns, ("pagerank", "page_rank", "pr"))

    harmonic_expr = "NULL"
    if harmonic_column:
        harmonic_expr = f"CAST({_quote_identifier(harmonic_column)} AS DOUBLE)"

    pagerank_expr = "NULL"
    if pagerank_column:
        pagerank_expr = f"CAST({_quote_identifier(pagerank_column)} AS DOUBLE)"

    connection.execute(
        f"""
        CREATE OR REPLACE TABLE cc_domain_ranks AS
        SELECT
            CAST({_quote_identifier(id_column)} AS BIGINT) AS id,
            {harmonic_expr} AS harmonic_centrality,
            {pagerank_expr} AS pagerank
        FROM cc_domain_ranks_raw
        """
    )

    return {
        "ranks_id": id_column,
        "ranks_harmonic": harmonic_column or "",
        "ranks_pagerank": pagerank_column or "",
    }


def _ensure_edges_table(connection: Any, edges_path: Path) -> dict[str, str]:
    if not _table_exists(connection, "cc_domain_edges_raw"):
        path_literal = _sql_string_literal(str(edges_path))
        connection.execute(
            f"CREATE TABLE cc_domain_edges_raw AS SELECT * FROM read_csv_auto({path_literal}, union_by_name=true, ignore_errors=true)"
        )

    columns = _table_columns(connection, "cc_domain_edges_raw")
    source_column = _pick_required(
        columns,
        ("source_id", "src_id", "from_id", "source", "src"),
        table_name="cc_domain_edges_raw",
    )
    target_column = _pick_required(
        columns,
        ("target_id", "dst_id", "to_id", "target", "dst"),
        table_name="cc_domain_edges_raw",
    )

    connection.execute(
        f"""
        CREATE OR REPLACE TABLE cc_domain_edges AS
        SELECT
            CAST({_quote_identifier(source_column)} AS BIGINT) AS source_id,
            CAST({_quote_identifier(target_column)} AS BIGINT) AS target_id
        FROM cc_domain_edges_raw
        """
    )

    return {
        "edges_source_id": source_column,
        "edges_target_id": target_column,
    }


def _ensure_vertices_and_ranks_ready(
    *,
    session: requests.Session,
    connection: Any,
    release: str,
    release_dir: Path,
    manifest: OffsiteCommonCrawlManifest,
    control: OffsiteCommonCrawlWorkerControl,
    started_monotonic: float,
    time_budget_seconds: int,
    duckdb_version: str,
) -> OffsiteCommonCrawlManifest:
    _check_time_budget(started_monotonic, time_budget_seconds, control)
    vertices_asset = _download_asset_if_needed(
        session=session,
        release=release,
        release_dir=release_dir,
        asset_name="vertices",
        control=control,
        timeout_seconds=_DEFAULT_HTTP_TIMEOUT_SECONDS,
    )
    _check_time_budget(started_monotonic, time_budget_seconds, control)
    ranks_asset = _download_asset_if_needed(
        session=session,
        release=release,
        release_dir=release_dir,
        asset_name="ranks",
        control=control,
        timeout_seconds=_DEFAULT_HTTP_TIMEOUT_SECONDS,
    )

    _check_time_budget(started_monotonic, time_budget_seconds, control)
    mapping = _manifest_column_mapping(manifest)
    mapping.update(_ensure_vertices_table(connection, vertices_asset.path))
    mapping.update(_ensure_ranks_table(connection, ranks_asset.path))
    _set_manifest_column_mapping(manifest, mapping)

    manifest.vertices_ready = True
    manifest.ranks_ready = True
    manifest.materialization_version = _MATERIALIZATION_SCHEMA_VERSION
    manifest.duckdb_version = duckdb_version
    manifest.vertices_source_url = vertices_asset.source_url or manifest.vertices_source_url
    manifest.ranks_source_url = ranks_asset.source_url or manifest.ranks_source_url
    manifest.vertices_bytes = int(vertices_asset.bytes_downloaded or manifest.vertices_bytes)
    manifest.ranks_bytes = int(ranks_asset.bytes_downloaded or manifest.ranks_bytes)
    if vertices_asset.sha256:
        manifest.vertices_sha256 = vertices_asset.sha256
    if ranks_asset.sha256:
        manifest.ranks_sha256 = ranks_asset.sha256
    if vertices_asset.etag:
        manifest.vertices_etag = vertices_asset.etag
    if ranks_asset.etag:
        manifest.ranks_etag = ranks_asset.etag
    if not manifest.downloaded_at:
        manifest.downloaded_at = utc_now_iso()
    _save_manifest(release_dir, manifest)
    return manifest


def _ensure_edges_ready(
    *,
    session: requests.Session,
    connection: Any,
    release: str,
    release_dir: Path,
    manifest: OffsiteCommonCrawlManifest,
    control: OffsiteCommonCrawlWorkerControl,
    started_monotonic: float,
    time_budget_seconds: int,
    duckdb_version: str,
) -> OffsiteCommonCrawlManifest:
    _check_time_budget(started_monotonic, time_budget_seconds, control)
    edges_asset = _download_asset_if_needed(
        session=session,
        release=release,
        release_dir=release_dir,
        asset_name="edges",
        control=control,
        timeout_seconds=_DEFAULT_HTTP_TIMEOUT_SECONDS,
    )

    _check_time_budget(started_monotonic, time_budget_seconds, control)
    mapping = _manifest_column_mapping(manifest)
    mapping.update(_ensure_edges_table(connection, edges_asset.path))
    _set_manifest_column_mapping(manifest, mapping)

    manifest.edges_ready = True
    manifest.materialization_version = _MATERIALIZATION_SCHEMA_VERSION
    manifest.duckdb_version = duckdb_version
    manifest.edges_source_url = edges_asset.source_url or manifest.edges_source_url
    manifest.edges_bytes = int(edges_asset.bytes_downloaded or manifest.edges_bytes)
    if edges_asset.sha256:
        manifest.edges_sha256 = edges_asset.sha256
    if edges_asset.etag:
        manifest.edges_etag = edges_asset.etag
    if not manifest.downloaded_at:
        manifest.downloaded_at = utc_now_iso()
    _save_manifest(release_dir, manifest)
    return manifest


def lookup_rank_rows(connection: Any, reverse_domains: tuple[str, ...]) -> dict[str, dict[str, float | int | str | None]]:
    values = tuple(sorted({value for value in reverse_domains if value}))
    if not values:
        return {}

    placeholders = ",".join("?" for _ in values)
    rows = connection.execute(
        f"""
        SELECT
            v.domain,
            v.num_hosts,
            r.harmonic_centrality,
            r.pagerank
        FROM cc_domain_vertices v
        LEFT JOIN cc_domain_ranks r
        ON r.id = v.id
        WHERE v.domain IN ({placeholders})
        """,
        list(values),
    ).fetchall()

    payload: dict[str, dict[str, float | int | str | None]] = {}
    for row in rows:
        payload[str(row[0] or "")] = {
            "domain": str(row[0] or ""),
            "num_hosts": int(row[1] or 0),
            "harmonic_centrality": float(row[2]) if row[2] is not None else None,
            "pagerank": float(row[3]) if row[3] is not None else None,
        }
    return payload


def discover_linking_domains(
    connection: Any,
    *,
    target_reverse_domain: str,
    limit: int,
) -> list[dict[str, float | int | str | None]]:
    if not target_reverse_domain:
        return []

    rows = connection.execute(
        """
        SELECT
            src.domain AS source_domain,
            src.num_hosts AS source_num_hosts,
            src_rank.harmonic_centrality AS source_harmonic_centrality,
            src_rank.pagerank AS source_pagerank
        FROM cc_domain_edges e
        INNER JOIN cc_domain_vertices dst
        ON dst.id = e.target_id
        INNER JOIN cc_domain_vertices src
        ON src.id = e.source_id
        LEFT JOIN cc_domain_ranks src_rank
        ON src_rank.id = src.id
        WHERE dst.domain = ?
        GROUP BY 1, 2, 3, 4
        ORDER BY
            COALESCE(src_rank.harmonic_centrality, 0.0) DESC,
            COALESCE(src_rank.pagerank, 0.0) DESC,
            COALESCE(src.num_hosts, 0) DESC,
            src.domain ASC
        LIMIT ?
        """,
        [target_reverse_domain, max(1, int(limit))],
    ).fetchall()

    payload: list[dict[str, float | int | str | None]] = []
    for row in rows:
        payload.append(
            {
                "source_domain": str(row[0] or ""),
                "source_num_hosts": int(row[1] or 0),
                "source_harmonic_centrality": float(row[2]) if row[2] is not None else None,
                "source_pagerank": float(row[3]) if row[3] is not None else None,
            }
        )
    return payload


def _rank_bucket_for_index(index: int) -> str:
    if index < 10:
        return "top_10"
    if index < 25:
        return "top_25"
    if index < 50:
        return "top_50"
    return "top_100"


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_referrer_aggregates(
    linking_rows: list[OffsiteCommonCrawlLinkingDomainPayload],
) -> tuple[int, float | None, float | None, float | None, float | None]:
    if not linking_rows:
        return 0, None, None, None, None

    harmonic_values = [row.source_harmonic_centrality for row in linking_rows if row.source_harmonic_centrality is not None]
    pagerank_values = [row.source_pagerank for row in linking_rows if row.source_pagerank is not None]

    weighted_scores: list[float] = []
    for row in linking_rows:
        harmonic = float(row.source_harmonic_centrality or 0.0)
        pagerank = float(row.source_pagerank or 0.0)
        weighted_scores.append((harmonic * 1000.0) + (pagerank * 1_000_000.0))

    weighted_total = sum(weighted_scores)
    top_concentration = None
    if weighted_total > 0 and weighted_scores:
        top_concentration = max(weighted_scores) / weighted_total

    avg_harmonic = (sum(harmonic_values) / len(harmonic_values)) if harmonic_values else None
    avg_pagerank = (sum(pagerank_values) / len(pagerank_values)) if pagerank_values else None
    weighted_score = weighted_total if weighted_total > 0 else None

    return len(linking_rows), weighted_score, avg_harmonic, avg_pagerank, top_concentration


def _base_notes(
    *,
    release: str,
    mode: str,
    cache_state: str,
    status: str,
    extra: dict[str, Any] | None = None,
) -> str:
    payload = {
        "release": release,
        "mode": mode,
        "cache_state": cache_state,
        "status": status,
        "execution_scope": "concurrent_within_current_audit_process_only",
        "linking_domain_semantics": "Common Crawl domain graph linking domains; not exact page-level backlinks.",
    }
    if extra:
        payload.update(extra)
    return json.dumps(payload, sort_keys=True)


def run_offsite_commoncrawl_worker(
    request: OffsiteCommonCrawlWorkerRequest,
    control: OffsiteCommonCrawlWorkerControl,
) -> OffsiteCommonCrawlWorkerPayload:
    started_at = utc_now_iso()
    started_monotonic = time.perf_counter()
    normalized_schedule = canonicalize_offsite_schedule(request.schedule)

    target_domain = normalize_domain_for_commoncrawl(request.target_domain)
    compare_domains: tuple[str, ...] = tuple(
        domain
        for domain in dict.fromkeys(normalize_domain_for_commoncrawl(value) for value in request.compare_domains)
        if domain and domain != target_domain
    )

    release = str(request.release or "").strip() or "CC-MAIN-UNKNOWN"
    release_dir = expand_commoncrawl_cache_dir(request.cache_dir) / release
    manifest = _load_manifest(release_dir, release)
    cache_state = derive_cache_state(manifest)

    if request.mode == "verify":
        finished_at = utc_now_iso()
        elapsed_ms = int((time.perf_counter() - started_monotonic) * 1000)
        return OffsiteCommonCrawlWorkerPayload(
            summary=OffsiteCommonCrawlSummaryPayload(
                target_domain=target_domain,
                cc_release=release,
                mode=request.mode,
                schedule=normalized_schedule,
                status=STATUS_DEFERRED_VERIFY,
                cache_state=cache_state,
                target_found_flag=0,
                comparison_domain_count=len(compare_domains),
                query_elapsed_ms=elapsed_ms,
                background_started_at=started_at,
                background_finished_at=finished_at,
                notes_json=_base_notes(
                    release=release,
                    mode=request.mode,
                    cache_state=cache_state,
                    status=STATUS_DEFERRED_VERIFY,
                    extra={
                        "message": "Page-level verification is deferred for a later bounded Common Crawl query/index integration.",
                    },
                ),
            )
        )

    if request.mode not in OFFSITE_COMMONCRAWL_MODES:
        finished_at = utc_now_iso()
        elapsed_ms = int((time.perf_counter() - started_monotonic) * 1000)
        return OffsiteCommonCrawlWorkerPayload(
            summary=OffsiteCommonCrawlSummaryPayload(
                target_domain=target_domain,
                cc_release=release,
                mode=request.mode,
                schedule=normalized_schedule,
                status=STATUS_FAILED_QUERY,
                cache_state=cache_state,
                target_found_flag=0,
                comparison_domain_count=len(compare_domains),
                query_elapsed_ms=elapsed_ms,
                background_started_at=started_at,
                background_finished_at=finished_at,
                notes_json=_base_notes(
                    release=release,
                    mode=request.mode,
                    cache_state=cache_state,
                    status=STATUS_FAILED_QUERY,
                    extra={"error": f"unsupported mode: {request.mode}"},
                ),
            )
        )

    if not target_domain:
        finished_at = utc_now_iso()
        elapsed_ms = int((time.perf_counter() - started_monotonic) * 1000)
        return OffsiteCommonCrawlWorkerPayload(
            summary=OffsiteCommonCrawlSummaryPayload(
                target_domain="",
                cc_release=release,
                mode=request.mode,
                schedule=normalized_schedule,
                status=STATUS_FAILED_QUERY,
                cache_state=cache_state,
                target_found_flag=0,
                comparison_domain_count=len(compare_domains),
                query_elapsed_ms=elapsed_ms,
                background_started_at=started_at,
                background_finished_at=finished_at,
                notes_json=_base_notes(
                    release=release,
                    mode=request.mode,
                    cache_state=cache_state,
                    status=STATUS_FAILED_QUERY,
                    extra={"error": "target domain could not be normalized"},
                ),
            )
        )

    connection: Any | None = None
    status = STATUS_SUCCESS
    linking_payload: list[OffsiteCommonCrawlLinkingDomainPayload] = []
    comparison_payload: list[OffsiteCommonCrawlComparisonPayload] = []
    target_harmonic: float | None = None
    target_pagerank: float | None = None
    target_found_flag = 0

    try:
        _check_time_budget(started_monotonic, request.time_budget_seconds, control)
        duckdb = _import_duckdb()

        with requests.Session() as session:
            _check_cancelled(control)
            release_dir.mkdir(parents=True, exist_ok=True)

            connection = duckdb.connect(str(release_dir / "commoncrawl.duckdb"))
            control.attach_connection(connection)
            runtime_duckdb_version = _duckdb_runtime_version(connection)
            if _manifest_requires_rebuild(manifest, runtime_duckdb_version):
                _drop_materialized_tables(connection)
                manifest.vertices_ready = False
                manifest.ranks_ready = False
                manifest.edges_ready = False
                manifest.column_mapping_json = "{}"
            manifest.materialization_version = _MATERIALIZATION_SCHEMA_VERSION
            manifest.duckdb_version = runtime_duckdb_version

            manifest = _ensure_vertices_and_ranks_ready(
                session=session,
                connection=connection,
                release=release,
                release_dir=release_dir,
                manifest=manifest,
                control=control,
                started_monotonic=started_monotonic,
                time_budget_seconds=request.time_budget_seconds,
                duckdb_version=runtime_duckdb_version,
            )
            cache_state = derive_cache_state(manifest)

            reverse_domains = tuple(
                dict.fromkeys(
                    [to_reverse_domain(target_domain)]
                    + [to_reverse_domain(domain) for domain in compare_domains]
                )
            )
            ranks = lookup_rank_rows(connection, reverse_domains)
            target_rank = ranks.get(to_reverse_domain(target_domain))

            if target_rank:
                target_found_flag = 1
                target_harmonic = _float_or_none(target_rank.get("harmonic_centrality"))
                target_pagerank = _float_or_none(target_rank.get("pagerank"))

            for compare_domain in compare_domains:
                reverse_key = to_reverse_domain(compare_domain)
                row = ranks.get(reverse_key)
                if row is None:
                    continue
                harmonic = _float_or_none(row.get("harmonic_centrality"))
                pagerank = _float_or_none(row.get("pagerank"))
                rank_gap = None
                pagerank_gap = None
                if target_harmonic is not None and harmonic is not None:
                    rank_gap = harmonic - target_harmonic
                if target_pagerank is not None and pagerank is not None:
                    pagerank_gap = pagerank - target_pagerank
                comparison_payload.append(
                    OffsiteCommonCrawlComparisonPayload(
                        compare_domain=compare_domain,
                        cc_release=release,
                        harmonic_centrality=harmonic,
                        pagerank=pagerank,
                        rank_gap_vs_target=rank_gap,
                        pagerank_gap_vs_target=pagerank_gap,
                    )
                )

            if request.mode == "domains":
                if not manifest.edges_ready and not request.allow_cold_edge_download:
                    status = STATUS_SKIPPED_COLD_EDGE_CACHE
                else:
                    manifest = _ensure_edges_ready(
                        session=session,
                        connection=connection,
                        release=release,
                        release_dir=release_dir,
                        manifest=manifest,
                        control=control,
                        started_monotonic=started_monotonic,
                        time_budget_seconds=request.time_budget_seconds,
                        duckdb_version=runtime_duckdb_version,
                    )
                    cache_state = derive_cache_state(manifest)
                    _check_time_budget(started_monotonic, request.time_budget_seconds, control)
                    rows = discover_linking_domains(
                        connection,
                        target_reverse_domain=to_reverse_domain(target_domain),
                        limit=max(1, int(request.max_linking_domains)),
                    )
                    rows = sorted(
                        rows,
                        key=lambda row: (
                            -float(row.get("source_harmonic_centrality") or 0.0),
                            -float(row.get("source_pagerank") or 0.0),
                            -int(row.get("source_num_hosts") or 0),
                            str(row.get("source_domain") or ""),
                        ),
                    )
                    for index, row in enumerate(rows):
                        linking_payload.append(
                            OffsiteCommonCrawlLinkingDomainPayload(
                                linking_domain=from_reverse_domain(str(row.get("source_domain") or "")),
                                source_num_hosts=int(row.get("source_num_hosts") or 0),
                                source_harmonic_centrality=_float_or_none(row.get("source_harmonic_centrality")),
                                source_pagerank=_float_or_none(row.get("source_pagerank")),
                                rank_bucket=_rank_bucket_for_index(index),
                                evidence_json=json.dumps(
                                    {
                                        "release": release,
                                        "mode": request.mode,
                                        "source_domain_reverse": str(row.get("source_domain") or ""),
                                    },
                                    sort_keys=True,
                                ),
                            )
                        )

            if status == STATUS_SUCCESS:
                if request.mode == "domains" and not linking_payload:
                    status = STATUS_SUCCESS_PARTIAL if target_found_flag else STATUS_SUCCESS_EMPTY
                elif request.mode == "ranks":
                    if target_found_flag and len(compare_domains) == len(comparison_payload):
                        status = STATUS_SUCCESS
                    elif target_found_flag:
                        status = STATUS_SUCCESS_PARTIAL
                    else:
                        status = STATUS_SUCCESS_EMPTY

            manifest.last_used_at = utc_now_iso()
            _save_manifest(release_dir, manifest)
            cache_state = derive_cache_state(manifest)
    except ImportError:
        status = STATUS_FAILED_MISSING_DEPENDENCY
    except OffsiteCommonCrawlCancelledError:
        status = STATUS_TIMEOUT_BACKGROUND
    except TimeoutError:
        status = STATUS_TIMEOUT_BACKGROUND
    except requests.RequestException:
        status = STATUS_FAILED_HTTP
    except Exception as exc:
        if control.stop_event.is_set() and "interrupt" in str(exc).lower():
            status = STATUS_TIMEOUT_BACKGROUND
        else:
            status = STATUS_FAILED_QUERY
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
        control.detach_connection()

    elapsed_ms = int((time.perf_counter() - started_monotonic) * 1000)
    finished_at = utc_now_iso()

    ref_domain_count, weighted_score, avg_harmonic, avg_pagerank, concentration = _build_referrer_aggregates(
        linking_payload
    )

    summary = OffsiteCommonCrawlSummaryPayload(
        target_domain=target_domain,
        cc_release=release,
        mode=request.mode,
        schedule=normalized_schedule,
        status=status,
        cache_state=cache_state,
        target_found_flag=int(target_found_flag),
        harmonic_centrality=target_harmonic,
        pagerank=target_pagerank,
        referring_domain_count=ref_domain_count,
        weighted_referring_domain_score=weighted_score,
        avg_referrer_harmonic=avg_harmonic,
        avg_referrer_pagerank=avg_pagerank,
        top_referrer_concentration=concentration,
        comparison_domain_count=len(comparison_payload),
        query_elapsed_ms=elapsed_ms,
        background_started_at=started_at,
        background_finished_at=finished_at,
        notes_json=_base_notes(
            release=release,
            mode=request.mode,
            cache_state=cache_state,
            status=status,
            extra={
                "target_reverse_domain": to_reverse_domain(target_domain),
                "compare_domains": list(compare_domains),
            },
        ),
    )

    return OffsiteCommonCrawlWorkerPayload(
        summary=summary,
        linking_domains=linking_payload,
        comparisons=comparison_payload,
    )
