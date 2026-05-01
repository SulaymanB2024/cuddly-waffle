# Status Ledger

## Durable queue orchestration and dashboard integration (completed)

### Scope
- Replace in-memory dashboard run orchestration with a durable SQLite queue control plane.
- Expose queue lifecycle through CLI and dashboard APIs, including cancellation.
- Persist richer attempt metadata (log paths, duration/line counters, stage summaries) and recover expired leases on startup.
- Preserve dashboard screenshot behavior for queue-backed successful runs.

### Implemented
- Added `seo_audit/job_queue.py`:
  - durable queue schema and migrations for `jobs`, `job_attempts`, `job_events`,
  - lease-based claim/admission policy enforcement,
  - retry/cancel/failure/completion transitions,
  - startup recovery for expired/orphaned leases,
  - additive attempt metadata (`line_count`, `duration_ms`, `last_stage`, `summary_json`, log paths).
- Added `seo_audit/queue_worker.py`:
  - subprocess worker loop around `python -u -m seo_audit audit ...`,
  - heartbeat lease extension and terminate/kill cancellation handling,
  - run-id extraction from stage logs,
  - attempt-level summaries and combined stdout/stderr log path persistence,
  - restored post-run screenshot capture for successful jobs when `screenshot_count > 0`.
- Updated `seo_audit/cli.py`:
  - added queue commands: `enqueue`, `jobs`, `cancel`, `worker`,
  - added dashboard queue flags (`--queue-db`, `--dashboard-worker-enabled|--dashboard-worker-disabled`),
  - switched run-event persistence to incremental flushes during execution.
- Updated `seo_audit/dashboard.py`:
  - migrated `AuditJobManager` to queue-backed operations,
  - added startup orphan reconciliation hook,
  - added embedded worker startup option,
  - wired `/api/run_audit`, `/api/jobs`, `/api/job_status`, and `/api/cancel_job` to queue lifecycle + run-event progress.
- Added/updated tests:
  - `tests/test_job_queue.py`,
  - `tests/test_queue_worker.py`,
  - `tests/test_dashboard.py` queue integration/cancel/recovery coverage.

### Validation
- Focused regressions (pass):
  - `pytest -q tests/test_job_queue.py tests/test_queue_worker.py tests/test_dashboard.py`
- Full suite (pass):
  - `pytest -q`
- Lint on modified queue/dashboard surfaces (pass):
  - `ruff check seo_audit/dashboard.py seo_audit/queue_worker.py seo_audit/job_queue.py seo_audit/cli.py tests/test_dashboard.py tests/test_queue_worker.py`
- Required live smoke (`docs/USAGE.md` / `AGENTS.md`) (pass):
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m seo_audit audit --domain https://void-agency.com --output ./out`
  - run id: `65abd51d-067a-467e-9b89-79f2378f4645`

## Architecture hardening waves: orchestration extraction, dashboard route registry, and scheduler reliability (completed)

### Scope
- Complete the remaining wave objectives from the architecture hardening plan:
  - extract high-risk `run_audit` stage logic into explicit helpers,
  - replace dashboard API route `if` chains with registry-based dispatch,
  - harden scheduler behavior for deterministic testing and add contention/fairness tests.

### Implemented
- Updated `seo_audit/cli.py`:
  - added explicit Stage 8 helper contracts (`_prepare_page_updates`, `_load_previous_pages_by_url`) and moved duplicate/provenance/change-diff state transitions out of inline orchestration,
  - removed per-page previous-run lookup query pattern and replaced it with batched previous-page preload,
  - added Stage 9 measurement helper contracts (`_derive_measurement_status_by_url`, `_prepare_measurement_records`) and removed duplicated inline provider-message parsing code,
  - introduced canonical helper constants (`PAGE_UPDATE_COLUMNS`, measurement status ranking/provenance defaults) to keep stage contracts deterministic.
- Updated `seo_audit/dashboard.py`:
  - replaced large `do_GET` and `do_POST` API `if` chains with endpoint-specific handler methods,
  - added route-dispatch registries (`_dispatch_get_api`, `_dispatch_post_api`) for maintainable endpoint mapping,
  - preserved existing API semantics and response contracts while reducing control-flow complexity.
- Updated `seo_audit/scheduler.py`:
  - added deterministic clock/sleep injection hooks (`time_fn`, `sleep_fn`) and configurable `min_sleep_seconds`,
  - added `acquire_with_wait(...)` for wait-time observability while preserving existing `acquire(...)` call sites.
- Added `tests/test_scheduler.py`:
  - deterministic min-delay enforcement coverage,
  - per-host independence/fairness coverage,
  - contention/no-deadlock stress regression.

### Validation
- Focused regressions (pass):
  - `pytest -q tests/test_dashboard.py tests/test_scheduler.py tests/test_integration_pipeline.py tests/test_offsite_run_audit.py tests/test_provider_reconcile.py tests/test_report_status.py`
- Full suite (pass):
  - `pytest -q`
- Required live smoke (`docs/USAGE.md` / `AGENTS.md`) (pass):
  - `python -m seo_audit audit --domain https://void-agency.com --output ./out`
  - run id: `cb050f3d-e48d-4e61-95f1-11cfaa2dcdcd`

## Reporting consistency hardening: snippet controls, duplicates, canonical root-cause suppression, shell-aware diagnostics, and measurement taxonomy defaults (completed)

### Scope
- Remove report-layer contradictions where summary rollups diverged from issue emission.
- Reduce repetitive page-level canonical/hreflang symptom spam when a cluster/root-cause issue already explains the defect.
- Strengthen shell-aware suppression for raw-only diagnostics when rendered/effective evidence is healthy.
- Keep measurement failures explicitly separated into policy-not-attempted, provider error classes, and error families.

### Implemented
- Updated `seo_audit/governance_matrix.py`:
  - governance preview summaries now separate issue-like restrictive pages from diagnostic-only restrictive pages,
  - summary carries additive counts for over-restrictive-like and Bing-restrictive-like populations,
  - intentional/noindex policy pages no longer inflate issue-like restrictive counts.
- Updated `seo_audit/cli.py` and `seo_audit/reporting.py`:
  - duplicate title/description rollups now use quality filters (utility/template exclusions + minimal lexical checks) while preserving compatibility flags,
  - duplicate rollups apply shell-source suppression only when shell state is confirmed (or shell-possible with actual render),
  - exact duplicate reporting is now explicitly split into issue-level vs diagnostic-candidate counts,
  - snippet diagnostic count excludes URLs already in issue-level restrictive snippet findings and excludes noindex rows,
  - measurement taxonomy defaults now map missing values to explicit classes (`measurement_not_attempted_by_policy`, `not_attempted_policy`) instead of implicit `skipped`/`none` fallbacks.
- Updated `seo_audit/issues.py`:
  - canonical/hreflang page symptoms are suppressed across full multi-member canonical-collision clusters (not alias-only),
  - raw-only content diagnostics are additionally suppressed for `raw_shell_possible` pages when render succeeded and effective facts are healthy.

### Tests added/updated
- Updated `tests/test_issues.py`:
  - added shell-possible + healthy-render suppression regression,
  - added cluster-level canonical symptom suppression regression.
- Updated `tests/test_stage2_modules.py`:
  - added governance summary split regression (issue-like vs diagnostic restrictive preview counts).

### Validation
- Focused regressions (pass):
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pytest -q tests/test_issues.py tests/test_stage2_modules.py tests/test_preview_controls.py tests/test_scoring.py tests/test_integration_pipeline.py::test_pipeline_integration_local_site`
- Full suite (pass):
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pytest -q`
- Required live smoke (`docs/USAGE.md` / `AGENTS.md`) (pass):
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m seo_audit audit --domain https://void-agency.com --output ./out`
  - run id: `160bf9f0-6420-468f-9d3b-99159a57bf9c`

## Measurement-first evidence model refactor: raw/rendered/effective split, provenance, and coverage taxonomy (completed)

### Scope
- Recenter decisions on measurement quality by separating observed evidence (raw + rendered) from effective truth used by scoring/reporting.
- Keep legacy fields additive for compatibility while moving duplicate detection, canonical logic, and diff/change decisions onto effective facts.
- Reduce JS-shell noise with shell-state-aware duplicate suppression and add explicit measurement coverage taxonomy.

### Implemented
- Updated `seo_audit/models.py` and `seo_audit/storage.py`:
  - added additive page evidence fields for raw/rendered/effective signal layers,
  - added `effective_field_provenance_json`, `measurement_status`, `measurement_error_family`, and `shell_state`,
  - added additive snapshot hash columns (`raw_content_hash`, `rendered_content_hash`, `effective_content_hash`),
  - bumped schema migration tracking to version `3` and added `idx_pages_run_effective_content_hash`.
- Updated `seo_audit/extract.py` and `seo_audit/render.py`:
  - extraction now emits richer inventory/evidence payloads (canonical/hreflang inventories, title/meta inventories, head/shell inventory payloads, raw content hash),
  - render result now preserves canonical list/count, hreflang list, rendered content hash, and inventory payloads.
- Added `seo_audit/resolution.py`:
  - central arbitration layer for effective facts with persona-aware rendered preference,
  - canonical arbitration (`rendered_single`, `rendered_self_match`, unresolved/conflict states),
  - explicit field-level provenance payload generation,
  - shell-state confirmation (`raw_shell_confirmed_after_render`) based on post-render evidence.
- Updated `seo_audit/cli.py`:
  - wired resolver into render/effective fact assignment,
  - switched duplicate flags to effective title/meta fields,
  - switched changed-since-last-run + snapshots to effective content hash,
  - persisted new raw/rendered/effective evidence fields,
  - added page-level measurement taxonomy derivation from PSI/CrUX outcomes and persisted it.
- Updated `seo_audit/diffing.py`:
  - switched `primary_content_hash` family to `effective_content_hash`,
  - added secondary drift families for `raw_content_hash` and `rendered_content_hash`.
- Updated `seo_audit/issues.py`:
  - duplicate clustering now uses effective content hash + representative URL collapsing,
  - added shell-confirmed suppression for raw-fallback duplicate hashes,
  - moved canonical checks to effective facts with compatibility fallback,
  - added canonical/root-cause families (`MULTIPLE_CANONICAL_TAGS`, `CANONICAL_CONFLICT_RAW_VS_RENDERED`, `CANONICAL_SELF_MISMATCH`, `STATIC_SHELL_*`, `CLUSTER_CANONICAL_COLLISION`, `HOST_DUPLICATION_CLUSTER`),
  - fixed preview-control permissive handling (`-1`, `large` no longer misclassified),
  - made raw-only severity/confidence shell-state aware.
- Updated `seo_audit/scoring.py`:
  - aligned preview-control penalties with permissive directive semantics,
  - consumed rendered structured-data validation to avoid false schema-absence penalties,
  - canonical duplicate-tag penalties now use effective provenance source counts.
- Updated `seo_audit/reporting.py`:
  - duplicate clusters now keyed by effective content hash,
  - added raw duplicate diagnostics section,
  - structured-data summary now includes rendered validation-derived eligibility/recognized/constrained dimensions,
  - added explicit measurement coverage taxonomy section (`measurement_status`, `measurement_error_family`).

### Tests added/updated
- Added:
  - `tests/test_resolution.py` (persona-aware effective resolution, canonical conflict, shell confirmation).
- Updated:
  - `tests/test_extract.py` (raw-layer inventory/hash assertions),
  - `tests/test_diffing.py` (effective + raw/rendered hash diff families),
  - `tests/test_issues.py` (effective-hash duplicate behavior, shell suppression, permissive preview controls),
  - `tests/test_scoring.py` (permissive preview controls are not penalized),
  - `tests/test_storage.py` (new schema/index/measurement/snapshot column coverage).

### Validation
- Focused regressions (pass):
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pytest -q tests/test_resolution.py tests/test_extract.py tests/test_render.py tests/test_diffing.py tests/test_issues.py tests/test_scoring.py tests/test_storage.py tests/test_incremental_crawl.py`
- Full suite (pass):
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pytest -q`
- Required live smoke (`docs/USAGE.md` / `AGENTS.md`) (pass):
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m seo_audit audit --domain https://void-agency.com --output ./out`
  - run id: `612f7bd4-ff68-4987-8564-f8c8bd6c8424`

## Standards hardening pass: robots, retries, render readiness, canonical clustering, and schema version tracking (completed)

### Scope
- Preserve existing architecture while improving standards alignment for robots persona semantics, crawl retries/revalidation telemetry, render readiness checks, canonical clustering visibility, reporting honesty, and migration/version discipline.
- Keep changes additive and backward-compatible, including existing run/report/dashboard workflows.

### Implemented
- Updated `seo_audit/robots.py`, `seo_audit/personas.py`, `seo_audit/config.py`, and `seo_audit/cli.py`:
  - added explicit robots persona semantics (`google_exact` vs `generic`),
  - added persona-aware robots fetch telemetry summaries,
  - added crawl retry/backoff CLI controls,
  - added explicit robots fetch/crawl-delay notes in run telemetry.
- Updated `seo_audit/crawler.py`:
  - replaced fixed retry sleep with status-aware retries for `408/429/5xx`,
  - added optional `Retry-After` honoring with bounded exponential backoff + jitter,
  - added retry telemetry counters (`fetch_retries_total`, wait budget, retry-after usage, status/network retry counts),
  - applied persona-aware robots allow checks and crawl-delay behavior.
- Updated `seo_audit/render.py`:
  - removed `networkidle` dependency from readiness logic,
  - added DOM/content stabilization + completeness checks in wait profile.
- Updated `seo_audit/models.py`, `seo_audit/storage.py`, and `seo_audit/cli.py`:
  - added additive canonical clustering fields (`canonical_cluster_key`, `canonical_cluster_role`, `canonical_signal_summary_json`),
  - computed/persisted cluster metadata without mutating discovered/fetched URL identity,
  - added schema migration tracking tables (`schema_meta`, `schema_migrations`) and recorded schema version state.
- Updated `seo_audit/reporting.py` and `seo_audit/dashboard_ui.html`:
  - clarified CrUX vs PSI semantics,
  - clarified URL Inspection snapshot semantics,
  - strengthened canonical/indexability caveats and Common Crawl caveat language.

### Tests added/updated
- Updated `tests/test_storage.py`:
  - verifies canonical cluster columns/index,
  - verifies schema version + migration tracking rows.

### Validation
- Targeted regressions (pass):
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pytest -q tests/test_robots.py tests/test_personas.py tests/test_crawler_policy.py tests/test_incremental_crawl.py tests/test_render.py tests/test_render_diagnostics.py tests/test_storage.py tests/test_report_status.py tests/test_dashboard.py`
- Required live smoke (`docs/USAGE.md` / `AGENTS.md`) (pass):
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m seo_audit audit --domain https://void-agency.com --output ./out`
  - run id: `90a2b0fc-5d82-4de0-81ca-3c0a229f4c22`

## Common Crawl offsite background lane (completed)

### Scope
- Add additive Common Crawl offsite intelligence without changing same-site crawl core behavior.
- Run offsite work as a concurrent background lane with schedule controls to avoid default runtime inflation.
- Keep worker writes out of audit SQLite and persist all offsite rows on main thread only.

### Implemented
- Added `seo_audit/offsite_commoncrawl.py`:
  - release resolution + cache inspection,
  - release-scoped cache + manifest handling,
  - streamed local asset materialization,
  - per-worker DuckDB connection usage,
  - `ranks`, `domains`, and deferred experimental `verify` mode handling,
  - cooperative cancellation + query interrupt support,
  - pure worker payload dataclasses.
- Updated `seo_audit/config.py` and `seo_audit/cli.py`:
  - added offsite config fields and CLI flags,
  - started offsite lane early in `run_audit`,
  - added schedule-aware end-join behavior (`concurrent_best_effort`, `background_wait`, `blocking`),
  - added dedicated telemetry events: `offsite_commoncrawl_status`, `offsite_commoncrawl_cache`, `offsite_commoncrawl_timing`.
- Updated `seo_audit/models.py` and `seo_audit/storage.py`:
  - added additive records/tables for summary, linking domains, and comparison domains,
  - added insert APIs and CSV exports:
    - `offsite_commoncrawl_summary.csv`
    - `offsite_commoncrawl_linking_domains.csv`
    - `offsite_commoncrawl_comparisons.csv`
    - `offsite_commoncrawl_competitors.csv`
- Updated `seo_audit/reporting.py`:
  - added `## Offsite visibility (Common Crawl)` report section with status/cache/mode/release, rank metrics, comparison domains, linking-domain caveat, and deferred/partial note.
- Updated `seo_audit/dashboard.py` and `seo_audit/dashboard_ui.html`:
  - exposed `offsite_commoncrawl` summary payload in `/api/summary`,
  - added compact overview panel for offsite status, comparison domains, and linking domains.
- Updated `pyproject.toml`:
  - added `duckdb>=1.0` dependency for offsite graph queries.

### Tests added/updated
- Added:
  - `tests/test_offsite_commoncrawl.py`
  - `tests/test_offsite_run_audit.py`
  - `tests/test_offsite_reporting_dashboard.py`
- Updated:
  - `tests/test_storage.py` (offsite schema/insert/export coverage)

### Validation
- Targeted suites (pass):
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pytest -q tests/test_offsite_commoncrawl.py tests/test_offsite_run_audit.py tests/test_offsite_reporting_dashboard.py tests/test_storage.py tests/test_dashboard.py tests/test_report_status.py`
- Full suite (pass):
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pytest -q`
- Required live smoke (pass):
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m seo_audit audit --domain https://void-agency.com --output ./out`
  - run id: `3798dcd0-61de-4dd9-ba8f-a3aa9bb23576`

## PR1 incremental crawl foundation (completed)

### Scope
- Establish durable crawl-state and cache foundations before schema/media/AI waves.
- Keep implementation additive and backward-compatible across existing storage, reporting, and run paths.
- Add deterministic regression coverage for conditional requests, 304 reuse, unchanged-body skip, and version invalidation behavior.

### Implemented
- Updated [seo_audit/models.py](seo_audit/models.py):
  - added additive records: URLStateRecord, BodyBlobRecord, ArtifactCacheRecord, PageDiffRecord,
  - extended CrawlResult with incremental_stats and planner_stats.
- Updated [seo_audit/storage.py](seo_audit/storage.py):
  - added additive tables and indexes: url_state, body_blobs, artifact_cache, page_diffs,
  - added content-addressed blob storage helpers using cache/blobs/ab/cd/sha.bin pathing,
  - added URL-state/artifact upsert and lookup APIs,
  - added run-config lookup and crawl-planning helpers,
  - extended CSV export with page_diffs.csv, url_state.csv, body_blobs.csv, artifact_cache.csv.
- Updated [seo_audit/http_utils.py](seo_audit/http_utils.py):
  - added conditional header builder (If-None-Match, If-Modified-Since),
  - added explicit HTTPResponse.not_modified semantics for 304 responses.
- Updated [seo_audit/crawler.py](seo_audit/crawler.py):
  - added pre-fetch URL-state lookup and conditional request usage,
  - added 304 body reuse via blob cache and artifact reuse path,
  - added unchanged-body short-circuit behavior for heavy extraction paths,
  - added extractor/schema/scoring version-token cache invalidation path,
  - persisted incremental telemetry and URL-state/artifact updates per crawl.
- Updated [seo_audit/cli.py](seo_audit/cli.py):
  - added explicit crawl planning stage before crawl,
  - added planner telemetry and notes (seed/sitemap/prioritized-changed counts),
  - passed version tokens and storage into crawl flow,
  - added incremental counters (discovered/fetched/reused/not_modified/reparsed/rerendered),
  - added primary-content-hash diffs persisted to page_diffs.
- Updated [seo_audit/reporting.py](seo_audit/reporting.py):
  - added Crawl planning and Incremental crawl counters report sections from run-event telemetry.

### Tests added or updated
- Updated [tests/test_http_utils.py](tests/test_http_utils.py):
  - conditional header construction,
  - 304 not-modified response semantics.
- Updated [tests/test_storage.py](tests/test_storage.py):
  - incremental table creation checks,
  - blob/url-state/artifact/page-diff round-trip and export checks.
- Added [tests/test_incremental_crawl.py](tests/test_incremental_crawl.py):
  - conditional request header usage,
  - 304 cache reuse behavior,
  - unchanged-200 artifact reuse skip behavior,
  - version-token invalidation forcing reparsing.

### Validation
- Focused regressions (pass):
  - /home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pytest -q tests/test_incremental_crawl.py tests/test_http_utils.py tests/test_storage.py tests/test_crawler_policy.py
- Full suite (pass):
  - /home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pytest -q
- Required live smoke from usage/docs (pass):
  - /home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m seo_audit audit --domain https://void-agency.com --output ./out
  - run id: c15aab16-0c35-4060-880a-b392e8ff4b45

## Crawler-control fidelity pass (completed)

### Scope
- Improve robots and crawler-control accuracy for page-level directives, robots.txt parsing, sitemap compression handling, and crawler persona semantics.
- Expand governance crawler coverage while keeping ChatGPT-User informational.
- Add additive schema extraction breadth (Microdata/RDFa) and split discovered schema types from custom summary coverage.

### Implemented
- Added `seo_audit/page_controls.py`:
  - preserves and resolves combined robots directives across duplicate meta tags and repeated/scoped `X-Robots-Tag` headers,
  - applies crawler-scope logic (`robots`, crawler-specific meta, generic/scoped `X-Robots-Tag`),
  - emits normalized effective control payloads and restrictive directive summaries.
- Updated `seo_audit/http_utils.py`:
  - kept legacy `headers: dict[str, str]`,
  - added additive `header_lists: dict[str, list[str>]` in `HTTPResponse` so repeated headers survive.
- Updated `seo_audit/extract.py`:
  - collects meta tags as `dict[str, list[str]]`,
  - resolves page controls via `page_controls`,
  - adds additive page outputs: `effective_robots_json`, robust `is_noindex`/`is_nofollow`, and snippet control fields from normalized directives,
  - adds Microdata and RDFa schema extraction,
  - adds additive `schema_summary_types_json` to separate custom summary coverage from discovered schema type set.
- Added `seo_audit/personas.py` and wired persona semantics through `seo_audit/config.py`, `seo_audit/cli.py`, `seo_audit/crawler.py`, and `seo_audit/render.py`:
  - persona id, request user-agent, robots token, and meta scope are now distinct runtime concepts,
  - `--crawl-persona` added,
  - `--user-agent` retained as an override.
- Updated `seo_audit/robots.py`:
  - fixed `parse_robots_text(...)` inline comment handling with `_strip_inline_comment(...)`,
  - parser and rule extraction now operate on cleaned lines.
- Updated `seo_audit/sitemaps.py`:
  - default candidates now include `/sitemap.xml.gz`,
  - supports gzip detection by URL suffix and magic bytes,
  - supports recursive parsing through gzipped sitemap indexes and children,
  - malformed gzip surfaces as captured errors (no crash).
- Expanded governance fields and usage in `seo_audit/models.py`, `seo_audit/storage.py`, `seo_audit/cli.py`, `seo_audit/governance_matrix.py`, `seo_audit/issues.py`, and `seo_audit/reporting.py`:
  - added additive page fields for `governance_gptbot_allowed`, `governance_oai_adsbot_allowed`, and `governance_chatgpt_user_allowed`,
  - added actionable issues for GPTBot/OAI-AdsBot blocks,
  - kept ChatGPT-User informational.
- Updated dashboard run summaries in `seo_audit/dashboard.py` to surface `crawl_persona`.

### Tests added/updated
- Added:
  - `tests/test_page_controls.py`
  - `tests/test_personas.py`
- Updated:
  - `tests/test_robots.py`
  - `tests/test_sitemaps.py`
  - `tests/test_http_utils.py`
  - `tests/test_extract.py`
  - `tests/test_issues.py`

### Validation
- Lint (touched files):
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m ruff check seo_audit/cli.py seo_audit/config.py seo_audit/crawler.py seo_audit/dashboard.py seo_audit/extract.py seo_audit/governance_matrix.py seo_audit/http_utils.py seo_audit/issues.py seo_audit/models.py seo_audit/page_controls.py seo_audit/personas.py seo_audit/render.py seo_audit/reporting.py seo_audit/robots.py seo_audit/sitemaps.py seo_audit/storage.py tests/test_extract.py tests/test_http_utils.py tests/test_issues.py tests/test_page_controls.py tests/test_personas.py tests/test_robots.py tests/test_sitemaps.py` (pass)
- Focused suites:
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pytest -q tests/test_page_controls.py tests/test_personas.py tests/test_robots.py tests/test_sitemaps.py tests/test_http_utils.py tests/test_extract.py tests/test_issues.py tests/test_stage2_modules.py` (pass)
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pytest -q tests/test_crawler_policy.py tests/test_internal_link_policy.py tests/test_render.py tests/test_report_status.py tests/test_integration_pipeline.py` (pass)
- Full suite:
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pytest -q` (pass)
- Required live smoke (`docs/USAGE.md` / `AGENTS.md`):
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m seo_audit audit --domain https://void-agency.com --output ./out` (pass, run id `ac105cff-f1a1-4e55-af2a-c1808f8b2815`)

## Stage-2 foundation integration tranche (completed)

### Scope
- Implement the first end-to-end integration pass for Stage-2 architecture pillars across crawl scheduling/frontier behavior, platform/governance/citation intelligence, media/schema analysis, GSC split modules, and scale-posture persistence.
- Keep all changes additive and backward-compatible with existing run/report/dashboard behavior.

### Implemented
- Added new Stage-2 modules:
  - `seo_audit/frontier.py`, `seo_audit/scheduler.py` for priority frontier + per-host pacing.
  - `seo_audit/platforms/*` for stack detection (Shopify/Wix/WordPress/Next.js/Cloudflare).
  - `seo_audit/governance_matrix.py`, `seo_audit/citation.py` for bot-governance and citation eligibility/evidence.
  - `seo_audit/media_images.py`, `seo_audit/media_video.py` for media discoverability extraction.
  - `seo_audit/schema_graph.py`, `seo_audit/schema_validation.py`, `seo_audit/schema_render_diff.py` for schema parsing/validation/render-diff analysis.
  - `seo_audit/gsc_inspection.py`, `seo_audit/gsc_analytics.py` and compatibility facade in `seo_audit/search_console.py`.
- Updated `seo_audit/crawler.py`:
  - wired priority frontier queueing and per-host scheduler pacing,
  - persisted frontier score/cluster metadata on pages,
  - added platform detection in crawl extraction pass,
  - emitted crawl fetch + snapshot telemetry records.
- Updated `seo_audit/extract.py`:
  - integrated schema graph/validation outputs,
  - integrated image/video detail extraction with discoverability scores,
  - populated additive page fields for Stage-2 schema/media payloads.
- Updated `seo_audit/render.py` and `seo_audit/cli.py`:
  - captured render session records,
  - attached schema raw-vs-render diffs,
  - persisted governance matrix + citation scores/evidence,
  - persisted schema/media/index/citation/submission/template-cluster datasets,
  - added optional GSC Search Analytics pull and citation enrichment wiring,
  - added additive CLI/config switches for Stage-2 controls.
- Updated `seo_audit/models.py` and `seo_audit/storage.py`:
  - added additive dataclasses and write APIs for new persistence tables,
  - expanded CSV exports for new scale/governance/schema/media/GSC artifacts.
- Added test coverage:
  - `tests/test_frontier.py` for frontier scoring/budget behavior.
  - `tests/test_stage2_modules.py` for governance/citation/media/schema helpers.

### Validation
- New-module focused:
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pytest -q tests/test_frontier.py tests/test_stage2_modules.py` (pass)
- Full suite:
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pytest -q` (pass)

## Dashboard architecture insights UI wiring (completed)

### Scope
- Wire frontend consumption for existing `/api/architecture` backend payload.
- Surface graph architecture risks directly in Overview analysis panels.
- Preserve existing dashboard interaction patterns and mode toggles.

### Implemented
- Updated `seo_audit/dashboard_ui.html`:
  - added `Internal link architecture` analysis panel in Overview,
  - added architecture signal and cutoff bar visual containers,
  - added weak-support, overloaded-hub, and disconnected-cluster tables,
  - added `state.lastArchitecture` cache,
  - added `refreshArchitecture()` API fetch path,
  - added `renderArchitectureInsights(...)` renderer,
  - integrated architecture refresh into `refreshAll()` orchestration,
  - added architecture re-render on console mode switch.
- Updated `tests/test_dashboard.py`:
  - extended HTML style contract assertions for architecture panel IDs and refresh wiring.

### Validation
- Lint (touched Python tests):
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m ruff check tests/test_dashboard.py` (pass)
- Focused suite:
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pytest -q tests/test_dashboard.py` (pass)
- Required live smoke (`docs/USAGE.md` / `AGENTS.md`):
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m seo_audit audit --domain https://void-agency.com --output ./out` (pass, run id `a95c36d0-2954-4007-bf1e-380f4b4006bc`)

## Graph analytics second-pass + architecture insights API (completed)

### Scope
- Add second-pass internal graph analytics without replacing existing link metrics.
- Persist per-page graph metrics in SQLite and export them alongside existing run artifacts.
- Feed graph metrics into internal architecture scoring and actionable issue generation.
- Expose architecture analysis payloads through dashboard API for chart integration.

### Implemented
- Updated `seo_audit/models.py`:
  - added additive `PageGraphMetricsRecord` dataclass.
- Updated `seo_audit/storage.py`:
  - added additive `page_graph_metrics` table and indexes,
  - added `insert_page_graph_metrics(...)` helper,
  - added `page_graph_metrics.csv` to run-scoped CSV export mapping.
- Updated `seo_audit/linkgraph.py`:
  - retained `compute_link_metrics(...)` baseline behavior,
  - added second-pass `compute_graph_metrics(...)` with deterministic PageRank, betweenness, closeness, community IDs, and bridge flags.
- Updated `seo_audit/cli.py`:
  - computes second-pass graph metrics after effective-link assembly,
  - attaches graph metrics to in-memory page dicts for same-run scoring/issues,
  - persists graph rows via storage insert path,
  - adds graph telemetry counts to stage detail payload.
- Updated `seo_audit/scoring_policy.py` and `seo_audit/issues.py`:
  - internal architecture scoring now consumes second-pass graph signals,
  - added architecture issue families: `IMPORTANT_PAGE_WEAK_SUPPORT`, `INTERNAL_FLOW_HUB_OVERLOAD`, `INTERNAL_CLUSTER_DISCONNECTED`.
- Updated `seo_audit/dashboard.py`:
  - added backend `architecture_insights(run_id)` aggregation,
  - added `/api/architecture` endpoint for architecture chart payloads.
- Updated tests:
  - `tests/test_linkgraph.py` adds centrality/community/bridge coverage,
  - `tests/test_storage.py` adds graph schema/insert/export coverage,
  - `tests/test_integration_pipeline.py` verifies persisted graph rows and CSV output,
  - `tests/test_issues.py` and `tests/test_scoring.py` verify graph-aware issue and scoring behavior,
  - `tests/test_dashboard.py` verifies `/api/architecture` payload behavior.

### Validation
- Lint (touched files):
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m ruff check seo_audit/linkgraph.py seo_audit/models.py seo_audit/storage.py seo_audit/cli.py seo_audit/issues.py seo_audit/scoring_policy.py seo_audit/dashboard.py tests/test_linkgraph.py tests/test_storage.py tests/test_integration_pipeline.py tests/test_issues.py tests/test_scoring.py tests/test_dashboard.py` (pass)
- Focused suites:
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pytest -q tests/test_linkgraph.py tests/test_storage.py tests/test_issues.py tests/test_scoring.py tests/test_integration_pipeline.py tests/test_dashboard.py` (pass)
- Full suite:
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pytest -q` (pass)
- Required live smoke (`docs/USAGE.md` / `AGENTS.md`):
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m seo_audit audit --domain https://void-agency.com --output ./out` (pass, run id `5d3082af-b833-436c-8fcc-796c5985f46a`)

## Trust hardening wave: governance, scoring, and GSC reconciliation (completed)

### Scope
- Close credibility gaps in governance/preview controls, Search Console reconciliation, and weak scoring proxies.
- Keep changes scoped to existing observed-facts architecture with additive, backward-compatible outputs.

### Implemented
- Updated `seo_audit/search_console.py`:
  - replaced Stage 1 scaffold behavior with URL Inspection API implementation using service-account credentials,
  - added explicit runtime statuses for missing dependencies/credentials, auth failures, API failures, empty and partial success paths,
  - retained existing `collect_index_states(...)` and `reconcile_index_states(...)` contract shape.
- Updated `seo_audit/cli.py`:
  - upgraded classification path to conservative evidence-weighted classification input (`classify_page_result`) and stricter local schema detection,
  - added governance evaluation flags for `Googlebot`, `Bingbot`, `OAI-SearchBot`, and `Google-Extended`,
  - added governance telemetry summary event (`run_events.event_type=governance_summary`),
  - wired rendered preview/noindex facts into issue-generation inputs,
  - upgraded GSC reconciliation issue generation to URL-level `GSC_INDEX_STATE_NOT_INDEXED` evidence rows when inspection succeeds.
- Updated `seo_audit/classify.py`:
  - added conservative scoring-based classifier result contract,
  - added explicit `has_local_business_schema(...)` helper,
  - removed implicit `Organization => LocalBusiness` equivalence.
- Updated `seo_audit/issues.py` and `seo_audit/scoring_policy.py`:
  - added governance/preview issue codes: `OPENAI_SEARCHBOT_BLOCKED`, `GOOGLE_EXTENDED_BLOCKED`, `BING_PREVIEW_CONTROLS_RESTRICTIVE`, `OVER_RESTRICTIVE_SNIPPET_CONTROLS`, `RAW_RENDER_NOINDEX_MISMATCH`, `RAW_RENDER_PREVIEW_CONTROL_MISMATCH`,
  - added intentional-policy safeguards so restrictive controls are not always defects,
  - elevated severity/cap behavior for materially important conflict/suppression cases.
- Updated `seo_audit/scoring.py`:
  - removed social-card proxy penalty from core search-facing score behavior,
  - split scoring into `preview_controls_score` and `structured_data_validity_score`,
  - retained `structured_snippets_score` in explanation payload as compatibility alias.
- Updated `seo_audit/render.py` and `seo_audit/reporting.py`:
  - carried rendered preview/noindex facts for mismatch auditing,
  - expanded report output with governance/answer-layer control section and actionable URL samples.
- Updated docs:
  - `README.md`, `docs/ARCHITECTURE.md`, `docs/USAGE.md` now describe implemented GSC behavior, governance semantics, scoring changes, and limits.

### Tests added/updated
- `tests/test_search_console.py`:
  - success, partial-failure, and auth-failure paths using deterministic fake auth/API responses.
- `tests/test_classify.py`:
  - conservative classification behavior and strict local-schema detection.
- `tests/test_scoring.py`:
  - verifies missing social cards no longer drive search-facing deduction,
  - verifies preview-control scoring behavior.
- `tests/test_issues.py`:
  - governance block, restrictive preview controls, and raw/render mismatch issue coverage.
- `tests/test_report_status.py`:
  - verifies governance section presence in generated report.

### Validation
- Install/update dependencies:
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pip install -e .[dev]` (pass)
- Lint (touched Python files):
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m ruff check seo_audit/search_console.py seo_audit/classify.py seo_audit/cli.py seo_audit/issues.py seo_audit/scoring.py seo_audit/scoring_policy.py seo_audit/reporting.py seo_audit/render.py tests/test_search_console.py tests/test_classify.py tests/test_scoring.py tests/test_issues.py tests/test_report_status.py` (pass)
- Focused suites:
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pytest -q tests/test_search_console.py tests/test_classify.py tests/test_scoring.py tests/test_issues.py tests/test_report_status.py tests/test_dashboard.py` (pass)
- Full suite:
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pytest -q` (pass)
- Required live smoke (`docs/USAGE.md` / `AGENTS.md`):
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m seo_audit audit --domain https://void-agency.com --output ./out` (pass, run id `4a7832be-d6f9-4e9c-96af-ca3ae084ccd0`)

## Dashboard workflow clarity pass (completed)

### Scope
- Improve operator comprehension across Overview, Issues, Pages, Comparison, and Exports without changing the established palette.
- Add explicit workflow orientation so users can quickly understand what to do next in each workspace.

### Implemented
- Updated `seo_audit/dashboard_ui.html`:
  - added a global `Recommended Workflow` guide under primary view tabs,
  - added per-workspace `Main question / Next action / Done when` compass blocks for Overview, Issues, Pages, Comparison, and Exports,
  - added responsive styling for the new orientation components,
  - added workflow-guide interaction wiring and active-step synchronization tied to view changes and run/comparison selection state.

### Validation
- `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pytest -q tests/test_dashboard.py` (pass)
- `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m seo_audit audit --domain https://www.example.com --output ./out/example-none --max-pages 20 --render-mode none` (pass, run id `e05927d0-f910-4e9b-8b03-5e9f8913065f`)

## Scoring profile runtime wiring + compare fix (completed)

### Scope
- Align runtime behavior with existing scoring metadata docs by wiring `--scoring-profile` through CLI/config into score serialization.
- Fix compare endpoint control flow so two-run compare payloads return correctly while retaining same-run validation guard.

### Implemented
- Updated [seo_audit/config.py](seo_audit/config.py):
  - added additive `AuditConfig.scoring_profile` field.
- Updated [seo_audit/cli.py](seo_audit/cli.py):
  - added `--scoring-profile` audit flag,
  - defaulted effective scoring profile to active `site_type` when unset,
  - added run note logging for scoring profile,
  - passed `score_profile=config.scoring_profile` into `score_page`.
- Updated [seo_audit/dashboard.py](seo_audit/dashboard.py):
  - fixed `compare_runs` indentation/control flow so non-identical runs return expected compare payloads.
- Updated [tests/test_scoring.py](tests/test_scoring.py):
  - added explicit scoring-profile metadata fallback coverage.

### Validation
- `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pytest -q tests/test_scoring.py tests/test_storage.py tests/test_dashboard.py tests/test_issues.py` (pass)
- `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pytest -q` (pass)
- `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m seo_audit audit --domain https://www.example.com --output ./out/example-none --max-pages 20 --render-mode none` (pass, run id `15c1ea78-d81e-4727-8000-32a6945bf4e3`)

## Scoring contract hardening + explanation compatibility pass (completed)

### Scope
- Keep the existing quality/risk/coverage/cap model shape while improving explainability and scoring robustness.
- Add additive persisted score metadata fields for model/version/profile and explanation payload evolution.
- Reduce policy drift between issue scoring context and page scoring logic.
- Improve thin-content and clustered-risk behavior without rewriting discrete blocker handling.
- Surface explanation details in dashboard URL drill-down while preserving old-row compatibility.

### Implemented
- Updated `seo_audit/scoring_policy.py`:
  - added profile normalization helpers and centralized page-importance segment boost behavior,
  - added selective thin-content continuous penalty helper,
  - added internal-architecture scoring helper using existing linkgraph signals (`effective_internal_links_out`, `inlinks`, `crawl_depth`, `nav_linked_flag`, `orphan_risk_flag`),
  - added family-risk aggregation coefficients for diminishing returns.
- Updated `seo_audit/issues.py`:
  - removed local page-importance boost duplication by using shared policy helper options.
- Updated `seo_audit/scoring.py`:
  - refactored dimension scoring into helper functions with explicit deductions,
  - kept discrete blocker behavior for access/indexability/canonical/render failures,
  - replaced thin-content cliff deduction with selective continuous penalty,
  - replaced issue risk blend with family-based diminishing-returns aggregation,
  - expanded explanation payload structure (`dimensions.deductions`, skipped/not-applicable notes, top risk families, cap reasons),
  - emits additive metadata/output keys while retaining legacy aliases.
- Updated `seo_audit/models.py` and `seo_audit/storage.py`:
  - added nullable additive score columns/fields: `scoring_model_version`, `scoring_profile`, `score_explanation_json`,
  - kept legacy columns (`score_version`, `score_profile`, `explanation_json`) intact,
  - updated insert path and additive migrations for both old and new fields.
- Updated `seo_audit/config.py` and `seo_audit/cli.py`:
  - added `scoring_profile` runtime path,
  - added `--scoring-profile` CLI flag,
  - defaulted runtime scoring-profile metadata to active `site_type` when flag is unset.
- Updated `seo_audit/dashboard.py` and `seo_audit/dashboard_ui.html`:
  - URL detail API now reads new explainability columns first and falls back to legacy columns,
  - detail renderer now shows model/profile from new or legacy keys and includes a concise main-deductions/causes list.
- Updated tests:
  - `tests/test_scoring.py` covers thin-content smoothing, family-risk damping, internal architecture scoring, and explanation payload contract,
  - `tests/test_storage.py` covers additive explainability columns, persistence of new fields, and additive migration from legacy score schema,
  - `tests/test_dashboard.py` validates URL detail behavior with rows carrying new explainability fields and fallback rows.

### Validation
- Dependency sync:
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pip install -e .[dev]` (pass)
- Focused suites:
  - `pytest -q tests/test_scoring.py tests/test_storage.py tests/test_dashboard.py tests/test_issues.py` (pass)
- Full suite:
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pytest -q` (pass)
- Required live smoke (`docs/USAGE.md`):
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m seo_audit audit --domain https://www.example.com --output ./out/example-none --max-pages 20 --render-mode none` (pass, run id `c2cbca0b-2b5f-431e-b89e-9a9652b59fae`)

## Extraction hardening + duplicate-readiness pass (completed)

### Scope
- Replace the custom extractor parser flow with an lxml tree parser while preserving existing observed-fact behavior.
- Add bounded new observed facts (`heading_outline_json`, compact allowlisted `schema_summary_json`, `content_hash`, link `dom_region`).
- Preserve provenance semantics (`links.source_context` remains raw/render provenance).
- Persist and surface exact duplicate-content readiness without adding approximate duplicate logic.
- Add lightweight extract-time instrumentation and keep schema migration backward-safe for existing DB files.

### Implemented
- Updated `seo_audit/extract.py`:
  - migrated primary parsing to lxml (`HTMLParser(recover=True)`),
  - retained malformed-fragment fallbacks for title/meta/h1/word count,
  - preserved canonical/hreflang/rel-next-prev/snippet-controls/JSON-LD parse error accounting/link extraction behavior,
  - added `heading_outline_json`, `schema_summary_json`, `content_hash`, and anchor `dom_region` extraction.
- Updated `seo_audit/models.py`:
  - added `PageRecord` fields `extract_time_ms`, `heading_outline_json`, `schema_summary_json`, `content_hash`,
  - added `LinkRecord.dom_region` while keeping `source_context` unchanged.
- Updated `seo_audit/storage.py`:
  - added additive `pages` columns for new observed fields,
  - added additive `links.dom_region`,
  - added run-scoped `content_hash` index via migration path,
  - extended links insert contract to persist `dom_region`,
  - fixed existing-DB compatibility by creating the new index in migration (not in base schema script).
- Updated `seo_audit/crawler.py`:
  - captures `extract_time_ms`,
  - persists link `dom_region` from both raw and rendered extraction,
  - keeps `source_context` semantics (`raw_dom`/`render_dom`) intact.
- Updated `seo_audit/issues.py`:
  - added `EXACT_CONTENT_DUPLICATE` issue generation using exact `content_hash` clusters,
  - conservatively gated to actionable/indexable HTML pages with minimum content to avoid short-page false positives.
- Updated `seo_audit/reporting.py`:
  - reports exact duplicate-content cluster/page counts,
  - includes `EXACT_CONTENT_DUPLICATE` in canonical/indexability reporting counts.
- Updated dependency/docs:
  - `pyproject.toml` adds `lxml>=5.2`,
  - `README.md`, `docs/ARCHITECTURE.md`, and `docs/USAGE.md` document new extraction/storage contracts.

### Tests added/updated
- `tests/test_extract.py`
  - dom-region extraction,
  - heading outline extraction,
  - content hash population,
  - allowlisted schema summary extraction.
- `tests/test_extract_malformed.py`
  - malformed HTML still extracts with non-empty content hash.
- `tests/test_crawler_policy.py`
  - verifies `source_context` remains provenance and `dom_region` is stored separately.
- `tests/test_storage.py`
  - verifies new page/link columns and migration-time schema presence.
- `tests/test_integration_pipeline.py`
  - verifies persisted `content_hash` and link `dom_region` values.
- `tests/test_issues.py`
  - verifies exact duplicate-content issue generation.

### Validation
- Install:
  - `python -m pip install -e .[dev]` (pass)
- Focused suites:
  - `pytest -q tests/test_extract.py tests/test_extract_malformed.py` (pass)
  - `pytest -q tests/test_storage.py tests/test_crawler_policy.py` (pass)
  - `pytest -q tests/test_dashboard.py tests/test_integration_pipeline.py` (pass)
  - `pytest -q tests/test_issues.py tests/test_scoring.py` (pass)
- Full suite:
  - `pytest -q` (pass)
- Required live smoke (`docs/USAGE.md` / `AGENTS.md`):
  - `python -m seo_audit audit --domain https://void-agency.com --output ./out` (pass, run id `409ea115-ba9a-4a3c-96ba-395b2ac8139d`)

## Shared scoring policy + score explainability tranche (completed)

### Scope
- Centralize duplicated scoring/issue policy logic in one shared module.
- Persist score version/profile plus a durable score explanation contract.
- Expose score explanation in dashboard URL detail with backward-compatible fallback.
- Keep query-lab/dashboard data access read-only hardened.
- Keep pagination hint issue diagnostic while reducing score pressure.

### Implemented
- Added `seo_audit/scoring_policy.py`:
  - shared page-type helpers,
  - shared page-importance mapping,
  - shared thin-content thresholds,
  - shared internal-link band policy,
  - shared risk-family metadata and cap-trigger metadata,
  - score version/profile constants.
- Updated `seo_audit/scoring.py`:
  - imports shared policy helpers instead of local duplicates,
  - adds `score_version`, `score_profile`, and `explanation_json` to score output,
  - persists deterministic explanation data (inputs, dimensions, quality weighting, risk contributors/families, cap reasons, overall formula metadata),
  - keeps hard blockers and thresholded cap behavior,
  - de-weights `PAGINATION_SIGNAL_MISSING` in risk blending via shared dampening metadata.
- Updated `seo_audit/issues.py`:
  - imports shared page-type/importance/threshold/band helpers,
  - preserves existing issue-generation behavior while removing duplicated policy code.
- Updated `seo_audit/models.py` and `seo_audit/storage.py`:
  - `ScoreRecord` now includes `score_version`, `score_profile`, `explanation_json`,
  - additive `scores` schema/migration columns for those fields,
  - extended score insert contract.
- Updated `seo_audit/dashboard.py`:
  - `DashboardStore` now opens SQLite in read-only mode (`mode=ro`, `PRAGMA query_only=1`),
  - query-lab responses include explicit read-only access header,
  - URL detail API returns `score_explanation` payload with fallback generation for legacy rows.
- Updated `seo_audit/dashboard_ui.html`:
  - added page-detail "Score explanation" section showing overall score, version/profile, dimension breakdown, cap reasons, and notable risk contributors.
- Updated docs:
  - `docs/ARCHITECTURE.md`, `docs/USAGE.md`, `README.md` now document shared scoring policy module, explainability contract, and score version/profile semantics.

### Validation
- Install:
  - `python -m pip install -e .[dev]` (pass)
- Targeted tests:
  - `pytest -q tests/test_scoring.py tests/test_issues.py tests/test_storage.py tests/test_dashboard.py` (pass)
- Full suite:
  - `pytest -q` (pass)
- Live smoke (`docs/USAGE.md` audit command variant):
  - `python -m seo_audit audit --domain https://www.example.com --output ./out/example-none --max-pages 20 --render-mode none` (pass, run id `2b4933b8-72ba-4579-ab23-1d01ca352773`)

## Priority-aware scoring and triage model pass (completed)

### Scope
- Replace static severity-only triage with context-aware issue prioritization.
- Replace flat arithmetic page grading with weighted applicable scoring and explicit coverage/risk outputs.
- Make dashboard issue ranking and exports reflect priority/certainty/reach instead of severity-only ordering.

### Implemented
- Updated `seo_audit/models.py`:
  - expanded `IssueRecord` with `certainty_state`, `priority_score`, `page_importance`, `reach`, `urgency`, `affected_count`, `affected_ratio`, `template_cluster`, and `affected_page_types`,
  - expanded `ScoreRecord` with `quality_score`, `risk_score`, `coverage_score`, and `score_cap`.
- Updated `seo_audit/storage.py`:
  - added additive schema/migrations for new issue and score columns,
  - extended `insert_issues` and `insert_scores` persistence contracts,
  - added run-scoped priority index for issues.
- Updated `seo_audit/issues.py`:
  - separated technical severity from operational priority,
  - introduced certainty states (`Verified`, `Probable`, `Unverified`, `Blocked / Could not test`),
  - added page-type-aware thin-content and internal-link banding logic,
  - added context enrichment pass to compute dynamic severity, scope/reach, affected ratios/counts, template clustering, and `priority_score`.
- Updated `seo_audit/scoring.py`:
  - replaced flat six-way averaging with weighted applicable dimensions,
  - excluded non-applicable/unknown dimensions from weighted denominators,
  - added explicit `coverage_score`, `quality_score`, `risk_score`, and blocking `score_cap` behavior,
  - preserved conservative handling for robots-blocked unfetched URLs,
  - removed legacy duplicate scorer path.
- Updated `seo_audit/cli.py`:
  - enriched all issue rows before persistence,
  - passed per-page issue context into scoring for risk/cap-aware overall scores,
  - consolidated provider error issues into main issue enrichment flow.
- Updated `seo_audit/dashboard.py` and `seo_audit/dashboard_ui.html`:
  - default issue sorting to priority-desc,
  - exposed certainty/reach/priority/scope fields in issue APIs, URL detail, and CSV export,
  - exposed page `quality/risk/coverage/cap` fields in page APIs/export,
  - updated issue workspace grouping and drilldowns to show certainty/reach/priority context.
- Updated `seo_audit/reporting.py`:
  - added certainty/reach distribution and top-priority issue diagnostics,
  - added score-model diagnostics (`quality`, `risk`, `coverage`, unknown performance count).

### Validation
- Targeted suites:
  - `pytest -q tests/test_scoring.py tests/test_report_status.py tests/test_provider_reconcile.py tests/test_performance_diagnostics.py` (pass)
- Full suite:
  - `pytest -q` (pass)

## Dashboard layout + trust-state stabilization pass (completed)

### Scope
- Eliminate real layout breakages (KPI/priority overlap, unusable runner rail at desktop breakpoints).
- Fix state-communication trust gaps (zero-delta wording, filter-state messaging, readiness category consistency, missing-value rendering).
- Strengthen selection affordance and reduce sticky nested-scroll friction in high-traffic panels.

### Implemented
- Updated `seo_audit/dashboard_ui.html`:
  - locked the overview hero strip to a safer three-column grid with explicit min widths and no overlap bleed,
  - adjusted overview desktop composition so `Performance summary` and `Live run control` stack when width cannot safely sustain two columns,
  - removed always-accented idle selector styling for run/comparison controls; accent now stays focus/active-driven,
  - clarified filter placeholders as examples and strengthened placeholder styling,
  - made filter-state copy explicit: no active chips means no active filters in the current result set,
  - upgraded `More filters` to a clear interactive control with chevron/expanded state,
  - reduced nested scrolling by making overview tables page-scroll-first while retaining bounded scrolling where needed,
  - strengthened selected states across issue/page rows and action queue cards (tinted background + left accent + aria selection),
  - widened and clamped Issues `One-line fix` content to improve side-by-side scanability,
  - collapsed heavy overview raw run notes behind disclosure by default,
  - corrected comparison interpretation semantics for near-zero score delta (`No meaningful score change`),
  - added compact comparison summary chips to reduce compressed narrative text,
  - collapsed mover/regression modules to stable-state messaging when deltas are effectively zero,
  - fixed page detail binding for partial/missing values (`Depth / score` now renders as `— / 80` style when needed),
  - rendered explicit missing states for Title/H1/Canonical and other nullable fields,
  - split export readiness language into category states (`Core exports`, `Performance data`, `CrUX`, `Screenshots`, `Query output`) instead of a single ambiguous ready flag,
  - clarified export CTA scope labels to avoid top-level/module-level action ambiguity.

### Validation
- HTML/CSS diagnostics:
  - editor diagnostics for `seo_audit/dashboard_ui.html` (pass)
- JS syntax:
  - extracted inline script and ran `node --check` (pass)
- Focused dashboard suite:
  - `pytest -q tests/test_dashboard.py` (pass)
- Live runtime smoke (existing auto-start task on port `9080`):
  - `GET /` returned `200` and served updated selectors/anchors (`overview-stage-grid`, `compare-summary-strip`, `issue-fix-text`, updated filter placeholders)
  - `GET /api/summary` responded with expected validation error (`run_id is required`), confirming route availability.

## Dashboard visual-story + chart-variety pass (completed)

### Scope
- Deliver one fast explanatory visual story per tab (Overview, Issues, Pages, Comparison, Exports).
- Replace repeated mini-bar grammar with a mixed set (composition strip, heatmap, histogram, scatter, slopegraph, waterfall, readiness board).
- Improve scan speed in tables and detail panels with inline micro-visuals and selected-item fingerprints.

### Implemented
- Updated `seo_audit/dashboard_ui.html`:
  - added and wired Overview narrative visuals:
    - run health composition strip (healthy/low/medium/high/uncategorized),
    - pipeline progress rail (`robots/sitemaps -> crawl -> classify -> render diff -> issue scoring -> exports`),
    - provider result chips for PSI/CrUX/GSC states,
    - issue-pressure-by-page-group matrix summary.
  - added and wired Issues visuals:
    - issue-type-by-template heatmap,
    - filter-impact summary pills,
    - selected issue fingerprint strip,
    - issue severity ladder,
    - affected-page mini map by page type.
  - added and wired Pages visuals:
    - score histogram,
    - depth-vs-score scatterplot (point size by issue count, selected URL highlight),
    - page-type proportional block map,
    - filter-impact summary,
    - selected page fingerprint + detail-side coverage timeline.
  - added and wired Comparison visuals:
    - issue delta waterfall,
    - score delta distribution (centered bins around zero),
    - issue-type slopegraph,
    - URL mover strip with in-row delta bars.
  - added and wired Exports visuals:
    - stacked dataset composition strip,
    - artifact readiness board (`ready/partial/missing`) for issues/pages/screenshots/performance/CrUX/query output.
  - added table-inline scan aids:
    - Issues table severity tick + affected-page spark meter + priority pressure meter,
    - Pages table status dots + in-cell score bar + issue-count dot scale.
  - strengthened visual polish:
    - larger primary KPI card hierarchy,
    - more intentional expand-toggle styling,
    - scaffold-style empty visual states.

### Validation
- JS syntax:
  - `node --check /tmp/dashboard_ui_script.js` (pass)
- Focused dashboard suite:
  - `pytest -q tests/test_dashboard.py` (pass)
- Live smoke (`docs/USAGE.md` dashboard command):
  - `python -m seo_audit dashboard --db ./out/audit.sqlite --host 127.0.0.1 --port 8766` (listening confirmed)
  - `curl http://127.0.0.1:8766/api/healthz` returned `{"ok": true}`
  - `curl /` confirms new visual container IDs are served.

## Dashboard client-disconnect write resilience fix (completed)

### Scope
- Eliminate noisy `BrokenPipeError` / `ConnectionResetError` tracebacks when dashboard clients disconnect mid-response.
- Keep normal request handling unchanged while making disconnect behavior benign.

### Implemented
- Updated `seo_audit/dashboard.py`:
  - added centralized response writer `_send_bytes(...)` used by `_send_json`, `_send_html`, `_send_csv`, and `_send_file`,
  - added `_is_client_disconnect(...)` guard for expected disconnect exceptions/errno values,
  - swallowed disconnect write failures during response send paths to avoid cascading fallback-write crashes.
- Updated `tests/test_dashboard.py`:
  - added `test_dashboard_handler_ignores_client_disconnect_writes` to assert response helpers do not raise on `BrokenPipeError` writes.

### Validation
- Focused dashboard suite:
  - `pytest -q tests/test_dashboard.py` (pass)
- Live smoke (dashboard startup and API):
  - `python -m seo_audit dashboard --db ./out/audit.sqlite --host 127.0.0.1 --port 8766` (listening confirmed)
  - `curl http://127.0.0.1:8766/api/healthz` returned `{"ok": true}`
  - forced raw-socket disconnect bursts no longer produced server traceback output.

## Dashboard workspace flow + interaction architecture pass (completed)

### Scope
- Convert Issues/Pages/Comparison/Exports from static report panes into operator workspaces with faster path-to-evidence.
- Add progressive disclosure for filters and advanced tools while keeping defaults simple.
- Improve run-launch guardrails and keyboard/operator ergonomics.

### Implemented
- Updated `seo_audit/dashboard_ui.html`:
  - restructured Issues and Pages into split master-detail workspaces with sticky local workbars,
  - added progressive filter disclosure rows (`More filters`), active filter chips with one-click removal, and preset actions,
  - added row-level quick actions (inspect/open/copy/export-slice/show-page) with keyboard row selection,
  - added page-row selection persistence and default selection behavior for page detail workflows,
  - rewired Comparison with invalid-state gating, explicit prior-run fallback action, and notable movers-first analytics,
  - added comparison delta breadth metrics including coverage delta and top improved/regression issue movers,
  - separated Exports into stakeholder-first export center and collapsed-by-default operator Query lab,
  - added structured telemetry cards and explicit readiness labels in Exports,
  - restructured launch modal into scoped sections and added live launch preview + cross-field validation guardrails,
  - added keyboard shortcuts for Enter-based filter apply and Ctrl/Cmd+Enter query execution.

### Validation
- JS syntax:
  - `node --check /tmp/dashboard_ui_script.js` (pass)
- Focused dashboard suite:
  - `pytest -q tests/test_dashboard.py` (pass)
- Required live smoke (`docs/USAGE.md`):
  - `python -m seo_audit dashboard --db ./out/audit.sqlite --host 127.0.0.1 --port 8765` (listening confirmed)

## Stage 1.6l correctness + runtime hardening pass (completed)

### Scope
- Confirm and fix high-impact correctness bugs in extraction, render diagnostics, link counting, robots representation, and normalization behavior.
- Improve runtime stability/performance without replacing the architecture.
- Add regression coverage for every fixed defect.

### Implemented
- Structured-data extraction hardening (`seo_audit.extract`):
  - fixed JSON-LD parser ordering bug where `application/ld+json` scripts were previously shadowed by generic script handling,
  - switched to per-script JSON-LD block accumulation,
  - added recursive `@type` extraction across nested JSON-LD structures,
  - added `schema_parse_error_count` so parse failures are distinct from schema absence.
- Snippet/citation control observability (`seo_audit.extract`, models/storage/reporting):
  - extracted `nosnippet`, `max-snippet`, `max-image-preview`, `max-video-preview`, and `data-nosnippet` signals,
  - persisted new fields on `pages`,
  - added report section `Snippet and citation controls`.
- Render reason contract fix (`seo_audit.shell_detection`, `seo_audit.cli`):
  - shell reasons now stay list-typed in `shell_signals_json`,
  - render reason formatting now parses list/string safely and avoids character-level corruption.
- Rendered internal-link counting fix (`seo_audit.cli`):
  - replaced `href.startswith("/")` counting with `is_internal_url(...)` host-policy evaluation,
  - now counts both relative and absolute internal URLs consistently.
- Robots-blocked URL truthfulness (`seo_audit.crawler`, `seo_audit.issues`, `seo_audit.reporting`, `seo_audit.scoring`):
  - crawler now persists discovered-but-robots-blocked URLs as page rows instead of silently skipping,
  - added explicit issues: `ROBOTS_BLOCKED_URL`, `SITEMAP_URL_BLOCKED_BY_ROBOTS`,
  - report discovery/access section now surfaces these codes,
  - scoring for unfetched robots-blocked pages is conservative to avoid false certainty.
- HTTP runtime improvements (`seo_audit.http_utils`, `seo_audit.config`, `seo_audit.crawler`, `pyproject.toml`):
  - moved fetch transport to pooled `requests.Session` (thread-local reuse),
  - added bounded body guards for crawler fetches (`max_response_bytes`, `max_non_html_bytes`),
  - preserved retry/timeout behavior in crawler,
  - added response metadata for skipped/truncated payload diagnostics.
- Render runtime improvements (`seo_audit.render`, `seo_audit.cli`):
  - introduced reusable `PlaywrightRenderer` context manager,
  - audit render stage now reuses one browser across render targets,
  - cleanup/teardown remains explicit and guarded.
- URL normalization policy adjustment (`seo_audit.url_utils`):
  - explicit HTTP inputs are preserved (supports HTTP-only sites),
  - schemeless URLs still default to HTTPS,
  - HTTPS base context remains respected for relative/internal normalization.
- Internal-host policy auditability (`seo_audit.cli`):
  - run notes now explicitly record strict internal host policy hosts.

### Tests added/updated
- `tests/test_extract.py`
  - JSON-LD valid/multiple block extraction,
  - malformed JSON-LD non-crashing behavior + parse error counting,
  - snippet/citation control extraction checks.
- `tests/test_js_render_pipeline.py`
  - render-reason list round-trip + formatting corruption regression.
- `tests/test_render.py`
  - rendered internal-link counting covers relative + absolute internal URLs.
- `tests/test_crawler_policy.py`
  - robots-blocked discovered URL persistence regression.
- `tests/test_issues.py`
  - explicit robots-blocked issue code coverage,
  - structured-data parse-failure issue coverage.
- `tests/test_http_utils.py`
  - session reuse regression,
  - non-HTML payload guard regression.
- `tests/test_render_diagnostics.py`
  - browser reuse regression for multi-URL rendering in one renderer context.
- `tests/test_scoring.py`
  - conservative scoring regression for unfetched robots-blocked pages.
- Updated existing tests for changed contracts/policies:
  - `tests/test_url_utils.py`
  - `tests/test_integration_pipeline.py`
  - `tests/test_report_status.py`
  - `tests/test_internal_link_policy.py`
  - `tests/test_crawler_policy.py`

### Validation
- Full suite:
  - `pytest -q` (pass)
- Required live smoke (`docs/USAGE.md`):
  - `python -m seo_audit audit --domain https://void-agency.com --output ./out`
  - completed with run id `06891f0c-761a-46a7-b25a-88542e07d0e4`.

## Dashboard chrome compression + triage acceleration pass (completed)

### Scope
- Compress the top chrome and reduce pre-decision scroll depth.
- Make the first viewport answer run health, top problem, and next action.
- Increase triage scan speed, tighten null states, and diversify chart grammar.

### Implemented
- Updated `seo_audit/dashboard_ui.html`:
  - replaced the tall top stack with a compact operator toolbar containing run selectors, launch/refresh controls, and status chips,
  - moved the run-completion/status banner into the `Run overview` panel header,
  - kept tabs sticky with a tighter offset and reduced top-shell vertical mass,
  - rebuilt first-screen overview into a three-part triage layout:
    - condensed run metadata (left),
    - larger four-KPI cluster with serif numerals (center),
    - dominant top-priority action card with direct CTA (right),
  - added run-health chip logic and top-priority deep-link behavior to open the issues queue with focused filtering,
  - densified issue queue rows to show severity, human label, code, occurrences, affected pages, priority score, and one-line fix by default,
  - added per-row expand/collapse rationale blocks for root cause and evidence,
  - reduced internal module padding, table row height, and empty-state footprint for instrument-panel density,
  - applied accent discipline for active tab, selected run/comparison selectors, focus ring, and live-stage marker,
  - unified chart grammar:
    - severity profile: segmented stacked bar,
    - score distribution: compact histogram,
    - page-type issue pressure: matrix table,
    - stage timing: bars with percentage-of-total annotation,
  - shifted internal labels and null-state copy toward operational language (`Run overview`, `Live run control`, compact comparison-unavailable callouts).

### Validation
- JS parse check:
  - `node --check /tmp/dashboard_ui_check.js` (pass)
- Focused dashboard suite:
  - `pytest -q tests/test_dashboard.py` (pass)
- Required live smoke (`docs/USAGE.md`):
  - `python -m seo_audit dashboard --db ./out/maasverde/run-20260414T192154Z/audit.sqlite --host 127.0.0.1 --port 8765` (listening confirmed)

## Dashboard VOID style-system alignment pass (completed)

### Scope
- Align the interactive dashboard visual language to the VOID website style guide.
- Keep dashboard runtime behavior and selector contracts unchanged while restyling.
- Preserve utility colors only for severity/status/chart contexts.

### Implemented
- Updated `seo_audit/dashboard_ui.html`:
  - replaced the previous root theme with a full tokenized VOID system (palette, typography, spacing, radii, shadows, motion),
  - remapped existing compatibility aliases so legacy selectors continue to resolve while using new source-of-truth tokens,
  - switched primary CTA treatment to black fill + white text and restyled secondary/ghost controls,
  - restyled nav tabs with lightweight editorial cues (underline wipe + dot indicator) while preserving active-view logic,
  - normalized panel/card/table/input spacing, border, and shadow language to restrained high-contrast editorial surfaces,
  - applied serif emphasis to hero/elevated headings while retaining sans for dense data-panel readability,
  - kept utility severity/status color bands for operational signaling without shifting core UI identity.
- Updated `tests/test_dashboard.py`:
  - added `test_dashboard_html_style_contract` to assert key style token anchors and critical runtime UI/JS contracts in served HTML.

### Validation
- Focused dashboard suite:
  - `pytest -q tests/test_dashboard.py` (pass)
- Required live smoke (`docs/USAGE.md`):
  - `python -m seo_audit dashboard --db ./out/audit.sqlite --host 127.0.0.1 --port 8765` (listening confirmed)
- Live endpoint checks:
  - `GET /` returned dashboard HTML shell,
  - `GET /api/runs` returned run payload from the live server.

## Dashboard tab visibility + stylesheet parse fix (completed)

### Scope
- Resolve the reported mostly-white/stacked dashboard appearance.
- Ensure only the active tab view is rendered and CSS parses cleanly.

### Implemented
- Updated `seo_audit/dashboard_ui.html`:
  - removed `display: grid` from shared `.layout-*` rules so `.view { display: none; }` is not overridden for inactive tabs,
  - restored missing `.btn { ... }` selector wrapper that had been causing CSS parser errors in the button style block.

### Validation
- Focused suite:
  - `pytest -q tests/test_dashboard.py` (pass)
- Required live smoke (`docs/USAGE.md`):
  - `python -m seo_audit dashboard --db ./out/audit.sqlite --host 127.0.0.1 --port 8765` (listening confirmed)
- Live checks:
  - before restart on `9080`, all tab views were visible simultaneously;
  - after restart on `9080`, only `view-overview` is visible and page height dropped substantially;
  - editor diagnostics for `seo_audit/dashboard_ui.html` now show no errors.

## Dashboard live telemetry streaming fix (completed)

### Scope
- Fix dashboard run telemetry staying stuck at `Starting audit process` during active jobs.

### Implemented
- Updated dashboard job runner in `seo_audit/dashboard.py`:
  - launch audit subprocess with Python unbuffered mode (`-u`),
  - force unbuffered child IO via `PYTHONUNBUFFERED=1` fallback,
  - preserves existing stage parsing and progress logic.
- Added regression coverage in `tests/test_dashboard.py`:
  - asserts job subprocess command includes `-u`,
  - asserts child env includes `PYTHONUNBUFFERED=1`,
  - verifies staged lines are consumed and run id is parsed.

### Validation
- Focused dashboard suite:
  - `pytest -q tests/test_dashboard.py` (pass)
- Live dashboard API verification:
  - launched dashboard job via `/api/run_audit`,
  - observed `/api/job_status` stage progression from `Starting audit process` to stage markers and completion,
  - confirmed non-empty streamed lines and parsed run id.
- Required live smoke (`docs/USAGE.md`):
  - `python -m seo_audit audit --domain https://void-agency.com --output ./out`
  - completed with run id `abaa4e59-d221-40f0-9b9c-df8cb603575a`.

## Provider wiring reliability pass (completed)

### Scope
- Address recurring operator concern that PSI/CrUX were not wired correctly by improving provider targeting and clarifying PSI success/no-data semantics.

### Implemented
- Updated `seo_audit.cli.select_performance_targets()`:
  - keeps homepage coverage,
  - prioritizes lower render-gap URLs before high render-gap candidates,
  - reduces repeated PSI runtime/provider noise from unstable JS-heavy targets.
- Added provider target traceability in run notes:
  - run notes now include `provider targets: ...` for quick verification of what PSI/CrUX were asked to evaluate.
- Hardened PSI classification in `seo_audit.performance._fetch_psi_internal()`:
  - responses with missing numeric `performance` category score are now classified as `no_data` (not false `success` rows).

### Tests added/updated
- `tests/test_provider_reconcile.py`
  - added selection regression asserting stable candidates are preferred after homepage.
- `tests/test_performance_diagnostics.py`
  - added regression asserting PSI payloads without `performance` score produce `no_data`.

### Validation
- Focused suites:
  - `pytest -q tests/test_performance_diagnostics.py tests/test_provider_reconcile.py tests/test_dashboard.py` (pass)
- Required live smoke (`docs/USAGE.md`):
  - `python -m seo_audit audit --domain https://void-agency.com --output ./out`
  - completed with run id `5592b907-f200-4c45-bceb-dc6b5ad75a59`.
- Smoke evidence:
  - run notes include explicit `provider targets: ...` list,
  - PSI now reports `no_data` when performance score is missing (instead of storing ambiguous success rows),
  - CrUX remains wired with explicit `no_data` status semantics.

## Dashboard expansion + runtime hardening pass (completed)

### Scope
- Address reported dashboard usability regressions (tabs, screenshots, display ambiguity) and expand analytics depth.

### Implemented
- Extended analytics in `seo_audit/dashboard_ui.html` with additional diagnostics:
  - status-code mix chart,
  - page-type score profile chart,
  - issue pressure by page type chart,
  - issue-severity heatmap table by page type.
- Hardened tab navigation behavior:
  - explicit `type="button"` on view tabs,
  - URL-hash synchronized view state (`#overview`, `#issues`, etc.),
  - hash restoration on boot and hash-change handling.
- Improved screenshot gallery reliability and operator clarity:
  - added screenshot metadata line,
  - explicit empty-state explanation for runs without captured screenshots,
  - automatic fallback display to latest run containing screenshots when selected run has none.
- Added defensive event binding helper and safer boot initialization to reduce null-reference startup failures.

### Validation
- Focused dashboard suite:
  - `pytest -q tests/test_dashboard.py` (pass)
- JS parse check:
  - `node --check` on extracted dashboard script (pass)
- Headless UI smoke (Playwright) on active ports `8765` and `9080`:
  - tab switching across all views (pass)
  - screenshot gallery rendering + fallback messaging (pass)

## Dashboard run-state UI contract pass (completed)

### Scope
- Tighten dashboard presentation around explicit run lifecycle states so live runs read as active execution, not empty analysis.
- Remove duplicated run-control entry points and concentrate operations in the live execution panel.

### Implemented
- Updated `seo_audit/dashboard_ui.html` with lifecycle-aware UI behavior:
  - added explicit frontend lifecycle mapping (`accepted`, `initializing`, `crawling`, `rendering`, `scoring`, `exporting`, `completed`, `failed`),
  - status banner now reflects active execution state and next system action,
  - analysis panels are gated until scoring/issue data exists,
  - pre-analysis states now show explanatory waiting copy instead of inert empty-analysis content.
- Reworked run brief semantics for live runs:
  - replaced misleading hard zeros with pending states (`In progress`, `Pending analysis`, `Calculating`, `Collecting`) where appropriate.
- Consolidated run-control UX:
  - removed duplicate top action-card run control,
  - kept the runner panel as the operational run-control center,
  - converted topbar control to a runner navigation action.
- Upgraded `Launch report` panel into a live execution surface:
  - stage label,
  - stage history,
  - last event time,
  - observed URL count,
- Improved run selector labels to concise human-readable timestamps.

- Focused dashboard suite:
  - `pytest -q tests/test_dashboard.py` (pass)
- Required live smoke (`docs/USAGE.md`):
  - `python -m seo_audit audit --domain https://void-agency.com --output ./out`
  - completed with run id `e443638e-70f5-43f4-99f3-b2583b7fca91`.

## Stage 1.6k issue gate + confidence foundation (completed)

### Scope
- Introduce gate-oriented issue metadata and confidence labeling as the first implementation slice of the technical SEO gate model.
- Keep behavior deterministic and backward-safe for existing runs, exports, and dashboard reads.

### Implemented
- Extended `IssueRecord` with three new fields:
  - `technical_seo_gate`
  - `confidence_score`
- Updated issue persistence SQL to store all metadata fields end-to-end.
- Tagged CLI-synthesized issues (`RENDER_UNAVAILABLE`, provider error issues) with explicit gate/verification/confidence metadata.

### Tests added/updated
- `tests/test_issues.py`
  - validates canonical mismatch gate mapping and confidence defaults.
  - validates JS-shell/no-render pages are marked `needs_rendered_verification` for content-sensitive issues.
- `tests/test_report_status.py`
  - validates presence of new report sections:
    - `## Issue gate coverage`
    - `## Issue verification confidence`

### Validation
- Focused impacted suites:
  - `pytest -q tests/test_issues.py tests/test_report_status.py tests/test_storage.py tests/test_dashboard.py`
- Full suite:
  - `pytest -q`
- Optional lint tool:
  - `ruff check .` (not available in current container: `ruff: command not found`)
- Required live smoke (`docs/USAGE.md`):
  - `python -m seo_audit audit --domain https://void-agency.com --output ./out`
  - completed with run id `62ce8493-5266-4ea5-be9e-838a9daea786`.

## Stage 1.6j JS-shell truthfulness + effective graph (completed)

### Scope
- Fix shell-HTML false positives by wiring rendered facts into issue/scoring/link graph paths without forcing full-site rendering.

### Implemented
- Added raw HTML shell classifier (`seo_audit.shell_detection`) using weighted signals (text/h1/links/scripts/root markers/framework hints) with tunable score and reasons.
- Extended page model + storage schema to persist raw/rendered/effective fact families plus render/shell telemetry fields.
- Updated crawler to persist raw facts and shell classification at fetch time.
- Upgraded render path to include bounded DOM stabilization + asset/tracker blocking, and to extract rendered links/h1/title/canonical.
- Switched issue/scoring logic to effective facts and added issue provenance support (`issue_provenance` in `issues` table/exports).
- Switched link graph depth/orphan calculations to effective links and removed fake crawl depth sentinel (`99` -> `NULL` for unknown).
- Expanded run/report telemetry to include shell/render counters and provenance summaries.

### Validation
- `pytest -q tests/test_render.py tests/test_linkgraph.py tests/test_issues.py tests/test_js_render_pipeline.py`
- `python -m seo_audit audit --domain https://void-agency.com --output ./out`

## Dashboard stabilization and UX pass (completed)

### Scope
- Repair broken dashboard module syntax/runtime pathing.
- Improve dashboard operator UX using existing observability/job APIs.
- Add endpoint test coverage for dashboard job/screenshot and POST validation paths.

### Implemented
- Fixed `seo_audit.dashboard` handler structure and server factory wiring:
  - repaired malformed indentation/scoping in `do_GET`, `do_POST`, and `create_dashboard_server()`,
  - added stricter JSON body parsing with explicit `400` for malformed payloads,
  - normalized API error text handling for cleaner `KeyError` responses.
- Improved API pagination metadata:
  - `list_pages()` and `list_issues()` now include `total_pages`.
- Expanded interactive UI behavior while preserving self-contained Python+inline frontend architecture:
  - added run observability panel (stage timing bars, top issue code bars, provider telemetry, run notes),
  - added dashboard-run audit launcher with live job progress + log polling,
  - added screenshot gallery wiring for run artifacts,
  - added table meta rows for page/issue pagination context.

### Tests added/updated
- `tests/test_dashboard.py`
  - added reusable HTTP request helper supporting expected non-200 responses,
  - added coverage for `/api/jobs`, `/api/screenshots`, and `/api/run_audit` validation failure (`domain is required`).

### Validation
- Focused dashboard suite:
  - `pytest -q tests/test_dashboard.py` (pass)
- Full suite (current workspace state):
  - `pytest -q` (fails in pre-existing non-dashboard modules: `tests/test_extract.py`, `tests/test_issues.py`, `tests/test_performance_diagnostics.py`)
- Required live smoke (`docs/USAGE.md`):
  - `python -m seo_audit audit --domain https://void-agency.com --output ./out`
  - completed with run id `64e6cad7-a7df-4f8c-b934-20dce124f6c0`.

## Stage 1.6h reliability hardening pass (completed)

### Scope
- Fix sitemap namespace fragility, unify internal-link policy, centralize thresholds, add SQLite indexes for hot paths, and add PR CI quality gate.

### Implemented
- Namespace-agnostic sitemap parsing in `seo_audit.sitemaps.parse_sitemap_xml()`:
  - supports namespaced and non-namespaced `urlset` and `sitemapindex`,
  - preserves `loc`, `lastmod`, `changefreq`, `priority`,
  - gracefully returns empty results for malformed XML instead of failing silently.
- Shared internal-link policy utilities in `seo_audit.url_utils`:
  - `internal_hosts_for_site()` and `is_internal_url()` now define one policy used by discovery, extraction, and crawler link classification.
  - Policy: internal includes relative URLs and apex/www host variants of the root; unrelated subdomains remain external by default.
- Thresholds centralized in new `seo_audit/policies.py` and consumed by issues/scoring/reporting:
  - low internal links, high render gap, low local SEO summary, low PSI summary.
  - fixed prior mismatch where `LOW_INTERNAL_LINKS` issue logic and report summary used different cutoffs.
- Storage schema updated with conservative run/url indexes for common read/report/export filters:
  - run-scoped indexes on pages/links/issues/scores/performance/crux/sitemaps/robots tables,
  - url-targeted indexes for pages and links hot-path lookups.
- Minimal CI workflow added for pull requests:
  - installs dev deps, runs `ruff check .`, runs `pytest -q`.
- Adjacent robustness fixes found during this pass:
  - `build_issues()` now guards `render_gap_score` when DB row value is NULL,
  - report render-gap sorting now handles NULL values safely.

### Tests added/updated
- `tests/test_sitemaps.py`
  - namespaced urlset, non-namespaced urlset, sitemap index recursion entries, malformed XML fallback, partial entries with missing `loc`.
- `tests/test_internal_link_policy.py`
  - shared host-policy tests plus extraction and crawler classification consistency checks.
- `tests/test_policy_thresholds.py`
  - parity test ensuring LOW_INTERNAL_LINKS issue generation and markdown report summary remain aligned.
- `tests/test_storage.py`
  - index-regression assertions verifying expected pages/links indexes exist after DB init.
- `tests/test_url_utils.py`
  - added policy tests for internal host variants and URL classification helper.
- `tests/test_performance_diagnostics.py` and `tests/test_integration_pipeline.py`
  - lint-hardening cleanup to satisfy CI lint gate.

### Validation
- `ruff check .`
- `pytest -q tests/test_sitemaps.py tests/test_internal_link_policy.py tests/test_policy_thresholds.py tests/test_storage.py tests/test_url_utils.py`
- `pytest -q`
- Required live smoke (`docs/USAGE.md`):
  - `python -m seo_audit audit --domain https://void-agency.com --output ./out`
  - completed with run id `237da42d-b134-40a3-af7e-7af49532fafd`.

## Stage 1.6g data-collection hygiene pass (completed)

### Scope
- Start implementation of the two-stack optimization plan with collection-first fixes.
- Focused slice only: provider target hygiene + issue-noise suppression for system/error pages.

### Implemented
- Provider target filtering added in orchestration:
  - `seo_audit.cli.select_performance_targets()` now selects only eligible 2xx HTML-like pages,
  - excludes system endpoints (`*.xml`, `sitemap*.xml`, `robots.txt`), fetch-error rows, and non-HTML assets,
  - deterministic priority ordering (homepage/service/contact/about/location/article/industry/other).
- Provider collection now uses filtered targets in `run_audit()` instead of naive first-N pages.
- Content-quality issue gating hardened in `seo_audit.issues`:
  - content issues now apply only to actionable content pages (2xx HTML-like and non-system URLs),
  - suppresses noisy findings on XML/system and 404 pages for
    `NOINDEX`, `MISSING_*`, `THIN_CONTENT`, `ORPHAN_RISK`, and `LOW_INTERNAL_LINKS`.

### Tests added/updated
- `tests/test_provider_reconcile.py`
  - added target-selection unit test for filtering system/non-HTML/404 rows,
  - added CLI orchestration test proving PSI/CrUX target lists exclude `.xml` URLs.
- `tests/test_issues.py`
  - updated canonical mismatch fixture to explicit actionable page context,
  - added tests confirming XML and 404 pages do not emit content-quality noise issues.

### Validation
- Focused: `pytest -q tests/test_provider_reconcile.py tests/test_performance_diagnostics.py`
- Focused: `pytest -q tests/test_issues.py tests/test_provider_reconcile.py`
- Full suite: `pytest -q`
- Required live smoke (`docs/USAGE.md`):
  - `python -m seo_audit audit --domain https://void-agency.com --output ./out`
  - run completed with run id `0aa6cbcf-5419-4dc3-aabc-2f553dcb6445`.

### Smoke-run spot checks
- latest run status: `completed`
- notes: `psi skipped: missing PSI_API_KEY/GOOGLE_API_KEY; crux status: success=0 no_data=0 failed=0 skipped=6`
- `issues` rows for XML URLs in latest run: `0` (system URL issue-noise suppression confirmed)

## Stage 1.6f provider reconciliation (completed)

### Scope lock
- Stage 1.6f only: branch reconciliation and provider restore.
- Explicitly out of scope: Stage 2 features (SERP observation, competitor modules, dashboards, or new product surface).

### Defects addressed
- Runtime/test drift fixed by restoring provider symbols in runtime:
  - `performance.resolve_google_keys`
  - `performance.collect_crux`
- CrUX runtime path restored with URL-level query plus optional origin fallback.
- Provider key resolution restored in runtime:
  - PSI: `PSI_API_KEY` else `GOOGLE_API_KEY`
  - CrUX: `CRUX_API_KEY` else `GOOGLE_API_KEY`
- CLI/provider flow reconciled:
  - PSI and CrUX toggles/target caps added to CLI + config.
  - PSI rows persist to `performance_metrics`.
  - CrUX rows persist to `crux_metrics` with status semantics (`success`, `no_data`, `failed`, `skipped`).
- Reporting/export coherence restored:
  - run-scoped CSV export in CLI run path,
  - `crux.csv` export,
  - report includes CrUX section and run notes,
  - report handles missing run row safely.

### Secondary regression checks (explicit)
- Provider payload JSON validity: preserved via `json.dumps(...)` persistence for PSI and CrUX payloads.
- Fragile provider insertion order: provider inserts now use explicit tuple mapping in storage methods.
- Export scoping by run: fixed (`storage.export_csvs(..., run_id=...)`) and covered by test.
- Anchor text extraction: restored.
- Canonical rel case-insensitivity: restored.
- Visible word count excludes head/script/style/ld+json text: restored.
- Report safety for missing run rows: restored.
- Playwright cleanup on exception: browser close now guarded in `finally`.
- Additional in-scope fix found during bounded validation:
  - `robots.txt` fetch failure no longer behaves like deny-all crawl blocking.

### Tests added/updated
- Added `tests/test_provider_reconcile.py`:
  - key precedence behavior,
  - CrUX URL success parsing,
  - CrUX origin fallback,
  - missing-key skipped semantics,
  - CLI persistence/report sections for PSI+CrUX,
  - report safety for missing run row.
- Updated `tests/test_extract.py` for anchor/canonical/visible-word-count regressions.
- Updated `tests/test_storage.py` for run-scoped export behavior.
- Updated `tests/test_robots.py` for robots fetch-failure crawl behavior.

### Validation commands and outcomes
- `pytest -q` (pass)
- `pytest -q tests/test_provider_reconcile.py tests/test_performance_diagnostics.py tests/test_report_status.py tests/test_integration_pipeline.py` (pass)
- Bounded external audit:
  - `python -m seo_audit audit --domain https://example.com --output ./out/provider_reconcile --max-pages 3 --psi-enabled --crux-enabled --performance-targets 1`
  - completed end-to-end without provider import/runtime breakage.
  - run produced no crawl targets in this environment (`psi skipped: no targets; crux skipped: no targets`).
- Bounded local audit (to prove target-path execution):
  - `python -m seo_audit audit --domain http://127.0.0.1:8765 --output ./out/provider_reconcile_local --max-pages 3 --psi-enabled --crux-enabled --performance-targets 1`
  - completed with crawled targets and truthful provider statuses (`psi skipped: missing key`, `crux skipped` row persisted).
- Required live smoke from usage docs:
  - `python -m seo_audit audit --domain https://void-agency.com --output ./out`
  - completed; latest run persisted 28 pages, 0 PSI rows (missing key), 6 CrUX rows (`skipped`).
- Keyed live bounded provider run (with user-supplied Google API key):
  - `python -m seo_audit audit --domain https://void-agency.com --output ./out/provider_reconcile_live_key --max-pages 5 --render-mode none --psi-enabled --crux-enabled --performance-targets 1`
  - completed with 5 crawled pages, 1 PSI row persisted (`mobile`), and 1 CrUX row persisted (`no_data` after origin fallback).
  - run notes recorded truthful mixed outcome: desktop PSI timed out while CrUX returned `no_data` (not skipped/faked success).

### Stage 1.6f readiness judgment
- **READY for live provider validation**.
- Remaining environment blockers:
  - provider API keys not set in this environment, so PSI/CrUX success-path collection remains unproven here.
  - Playwright not installed, so render success-path remains unavailable in this environment.

## Stage 1.6b defect-fix pass (completed)

### Defect A: www/apex discovery eligibility
- **Defect found**: seed filtering admitted only a narrow host form and could exclude valid apex/www crossover sitemap URLs.
- **Change**:
  - `discovery.py`: added explicit apex/www eligibility rules via `site_host_variants()`.
  - `url_utils.py`: tightened internal host matching to apex+www variants (excluding unrelated subdomains by default).
- **Proof tests**:
  - `tests/test_discovery.py::test_www_seed_accepts_apex_urls`
  - `tests/test_discovery.py::test_apex_seed_accepts_www_urls`
  - `tests/test_discovery.py::test_unrelated_subdomain_excluded`
  - `tests/test_url_utils.py` updated same-site assertions.
- **Live validation**:
  - attempted with `https://www.example.com`; run completed but outbound tunnel block still prevented success-path crawl expansion.

### Defect B: stale run status in report.md
- **Defect found**: report generation occurred before final run completion update.
- **Change**:
  - `cli.py`: persist `completed`/`failed` status + `completed_at` before generating report.
  - failure path now attempts report generation after failed status write.
- **Proof tests**:
  - `tests/test_report_status.py::test_report_shows_completed_status`
  - `tests/test_report_status.py::test_report_shows_failed_status`

### Defect C: render-path diagnostics
- **Defect found**: render fallback was silent (`None`) with no explicit operator signal.
- **Change**:
  - `render.py`: `render_url()` now returns `(result, error)` and includes explicit import/runtime error messages.
  - `cli.py`: aggregates render errors into `runs.notes` and emits `RENDER_UNAVAILABLE` issue when render targets exist but rendering fails.
  - `docs/USAGE.md`: explicit Playwright + browser install steps.
- **Proof tests**:
  - `tests/test_render_diagnostics.py::test_render_import_failure_is_explicit`

### Defect D: performance-provider diagnostics
- **Defect found**: provider failures collapsed to empty rows with weak diagnostics.
- **Change**:
  - `performance.py`: provider APIs return `(rows, errors)` / `(row, error)` with specific failure reasons.
  - `cli.py`: provider errors written to `runs.notes` and surfaced as `PERFORMANCE_PROVIDER_ERROR` issue.
  - successful provider rows still persist to DB/CSV.
- **Proof tests**:
  - `tests/test_performance_diagnostics.py::test_pagespeed_failure_reason`
  - `tests/test_performance_diagnostics.py::test_pagespeed_success_rows`
  - `tests/test_performance_diagnostics.py::test_cli_persists_performance_rows_when_provider_returns_data`

### Stage 1.6b validation commands
- `pytest -q` (pass)
- `python -m seo_audit audit --domain https://www.example.com --output ./out/stage16b_www_example_none --max-pages 20 --render-mode none`
- `python -m seo_audit audit --domain https://example.com --output ./out/stage16b_example_sample --max-pages 20 --render-mode sample --max-render-pages 5`

### Stage 1.6b live validation outcome
- Runs completed with correct final `completed` status in `report.md`.
- Environment remains tunnel-blocked (`403 Forbidden`) for outbound HTTP, so external success-path crawl expansion and PageSpeed live-success rows remain blocked.
- Render diagnostics now explicit in run notes (`playwright import failed: No module named 'playwright'`) and surfaced as issue codes.

### Stage 2 readiness judgment (post-1.6b)
- **NOT READY** (live success-path crawl/render/provider validation still blocked in this environment).

## Stage 1.5 validation/hardening

### Validation classification (current)
- **Implemented**
  - Stage 1 pipeline modules remain complete end-to-end.
  - Stage 1.5 hardening changes added: redirect-chain tracking, crawler retry loop, improved HTML gating, canonical mismatch issue detection, non-HTML issue/scoring gating, explicit fetch-failure score penalties, malformed HTML extraction fallbacks, improved render sample strategy.
- **Unit-tested**
  - Existing unit suite still passes.
  - New unit tests added for malformed extraction, canonical mismatch issue generation, render gap logic, and localhost URL normalization behavior.
- **Integration-tested**
  - Added controlled end-to-end mini-site integration test (`tests/test_integration_pipeline.py`) covering robots, sitemap index recursion, redirect, noindex, thin content, duplicate title/description, canonical mismatch, DB rows, issues/scores, and export/report generation.
- **Live-validated**
  - Bounded live audits were executed for:
    - `https://void-agency.com`
    - `https://example.com`
    - `https://www.python.org`
  - Commands completed pipeline and produced outputs.
- **Blocked / unproven**
  - Outbound HTTP in this environment is tunnel-blocked (`403 Forbidden`), so live runs only validated failure-path behavior, not real crawl/provider success-paths.
  - Live render success-path (Playwright + external pages) remains unproven here.

### Stage 1.5 milestones and checks

#### M1: Plan + baseline docs (completed)
- Added `docs/PLAN_STAGE_1_5.md` and restructured status categories.
- Check run: `pytest -q` (pass).

#### M2: Redirect/canonical/output hardening (completed)
Changes:
- `http_utils.py`: redirect chain capture.
- `crawler.py`: retry-aware fetch loop + redirect-chain persistence + stronger HTML detection.
- `issues.py`: added `CANONICAL_MISMATCH`; improved HTML-only issue gating; added `LOW_INTERNAL_LINKS`.
- `scoring.py`: apply on-page penalties only to HTML-like pages.
Checks:
- `pytest -q` (pass).

#### M3: Parser + render hardening (completed)
Changes:
- `extract.py`: malformed HTML fallbacks for title/meta description/h1/word-count.
- `render.py`: improved sample strategy diversity and clearer zero-gap reason.
Checks:
- `pytest -q` (pass).

#### M4: Integration testing track (completed)
Changes:
- Added integration pipeline test with in-process HTTP server mini-site.
Checks:
- `pytest -q` (pass; integration included).

#### M5: Live validation + output review (completed with blocker)
Commands run:
- `python -m seo_audit audit --domain https://void-agency.com --output ./out/live_void_none --max-pages 20 --render-mode none`
- `python -m seo_audit audit --domain https://example.com --output ./out/live_example_sample --max-pages 20 --render-mode sample --max-render-pages 5`
- `python -m seo_audit audit --domain https://www.python.org --output ./out/live_python_none --max-pages 20 --render-mode none`
Results:
- All commands completed and wrote SQLite/CSV/report artifacts.
- All three runs recorded network tunnel failure (`403 Forbidden`) in run notes and per-page fetch errors.
- Additional post-hardening sample rerun: `https://example.com` with `--render-mode sample` confirmed lower (50-range) scores for fetch-failed rows after scoring penalty fix.
- Provider (PageSpeed) failure path behaved non-fatally.

Output-quality review (controlled run)
- Local controlled run validated sample rows from `pages`, `issues`, `scores`:
  - Coherent `normalized_url` / `final_url`.
  - Redirect chains present for redirect routes.
  - `CANONICAL_MISMATCH`, `NOINDEX`, `THIN_CONTENT` and duplicate flags were emitted in expected scenarios.
  - Score directionality was sensible (noindex/thin lower than strong content pages).

### Stage 2 readiness judgment
- **NOT READY** to declare production-grade live trust yet, because live success-path validation remains blocked by environment HTTP tunneling restrictions.
- **READY** to begin limited Stage 2 design work *if* Stage 2 work does not rely on claims of proven live crawl/provider success in this environment.

## Done (Stage 1)
- **Milestone 0**: Repository bootstrap complete (package/docs/tests skeleton).
- **Milestone 1**: Config/model/storage foundation implemented with SQLite schema for all required Stage 1 tables.
- **Milestone 2**: URL normalization, robots parser, sitemap parser implemented.
- **Milestone 3**: Same-domain BFS crawler and metadata extraction implemented with resilient fetch-error recording.
- **Milestone 4**: Deterministic page classification, issue generation, and heuristic scoring implemented.
- **Milestone 5**: Optional Playwright render module with sample/all/none strategies and render-gap scoring.
- **Milestone 6**: PageSpeed provider layer implemented with graceful failure handling.
- **Milestone 7**: CSV exports and Markdown report generation implemented.
- **Milestone 8**: Live smoke test command executed against `https://void-agency.com`.

## Remaining
- Re-run live validation in an environment with outbound access and Playwright browser availability to prove success-path behavior.

## How to run current build
- Install editable package (if network allows): `python -m pip install -e .[dev]`
- Run tests: `pytest -q`
- Run audit: `python -m seo_audit audit --domain https://void-agency.com --output ./out`

## Known limitations
- Outbound HTTP tunnel restrictions in this environment block full live success-path validation.
- Playwright render success-path could not be proven against live external pages here.

## Dashboard comparability/trust-state hardening pass (completed)

### Scope
- Remove the UI release blocker where raw JavaScript leaked into the page footer.
- Enforce stage-truth behavior for run comparison and top-priority CTA actions.
- Eliminate contradictory launch-modal defaults and strengthen screenshot fallback provenance.

### Implemented
- Updated `seo_audit/dashboard_ui.html`:
  - removed duplicated trailing script/HTML block that rendered raw JavaScript text in the footer,
  - added explicit comparability model (`Comparable`, `Partially comparable`, `Not comparable`) and gated comparison rendering so deltas are deferred until runs are stage-compatible,
  - deferred comparison UI with explicit messaging (`Comparison deferred until scoring completes`) when selected runs are incomplete,
  - hid score-centric comparison visuals when there are no shared scored URLs,
  - updated KPI delta handling so average-score delta does not imply movement when shared scored URLs are absent,
  - corrected top-priority CTA behavior: pre-analysis state now routes to live run control instead of false issue-inspection affordance,
  - normalized launch-modal render defaults (`render_mode=none` now opens with `max_render_pages=0`) and added cross-field normalization on modal open/input changes,
  - strengthened screenshot fallback provenance with explicit fallback banner, source labels, and distinct fallback card styling.

### Validation
- Focused dashboard suite:
  - `pytest -q tests/test_dashboard.py` (pass)
- Required live smoke (`docs/USAGE.md` dashboard command):
  - `python -m seo_audit dashboard --db ./out/audit.sqlite --host 127.0.0.1 --port 8765`
  - startup confirmed with `dashboard listening on http://127.0.0.1:8765`.

## Stage-2 closing wave: PR5-PR8 integration (completed)

### Scope
- Finalize remaining Stage-2 integration waves in order:
  - PR5 media integration wiring,
  - PR6 AI visibility potential/evidence split,
  - PR7 optional Lighthouse lab layer,
  - PR8 bounded crawl concurrency with weighted queueing.
- Keep migration strategy additive/backward-compatible and preserve legacy citation consumers.

### Implemented
- PR5 media integration completion:
  - updated `seo_audit/extract.py` callsites to pass enriched context into media extractors (`schema_nodes`, `og_image_urls`, `meta_map`).
- PR6 AI visibility split:
  - added `seo_audit/ai_visibility.py` payload helpers,
  - added optional observed-evidence adapter layer in `seo_audit/integrations/visibility_adapters.py` (including GSC analytics enrichment path),
  - added additive persistence model/table wiring for AI visibility events,
  - preserved legacy compatibility by continuing to populate `citation_eligibility_score` and `citation_evidence_json` from merged payloads.
- PR7 Lighthouse lab layer:
  - added `seo_audit/lighthouse.py` collection flow with runtime command resolution (`lighthouse` / `npx lighthouse`),
  - added sidecar config/budget handling and persistence via `lighthouse_metrics` + `lighthouse.csv`,
  - added budget-violation issue path (`LIGHTHOUSE_BUDGET_FAIL`) and report/dashboard surfacing.
- PR8 crawl concurrency and queueing:
  - added `seo_audit/crawl_queue.py` weighted multi-band queue,
  - updated `seo_audit/crawler.py` with worker-bounded concurrent fetch orchestration while preserving per-host politeness scheduling,
  - added config/CLI controls for crawl workers and queue weighting.
- Runtime migration hardening follow-up:
  - fixed legacy DB startup ordering in `seo_audit/storage.py` by deferring `idx_sitemaps_run_kind` creation to additive migration after `entry_kind` is guaranteed.

### Tests added/updated
- Added:
  - `tests/test_ai_visibility.py`
  - `tests/test_lighthouse.py`
  - `tests/test_crawl_queue.py`
  - `tests/test_crawler_concurrency.py`
- Updated:
  - `tests/test_storage.py`
  - `tests/test_performance_diagnostics.py`
  - plus wave-specific updates across extraction/reporting/dashboard and integration slices.

### Validation
- Focused wave suites (pass):
  - `pytest -q tests/test_extract.py tests/test_stage2_modules.py tests/test_issues.py`
  - `pytest -q tests/test_ai_visibility.py tests/test_storage.py tests/test_report_status.py tests/test_dashboard.py`
  - `pytest -q tests/test_lighthouse.py tests/test_performance_diagnostics.py tests/test_provider_reconcile.py`
  - `pytest -q tests/test_crawl_queue.py tests/test_crawler_concurrency.py tests/test_crawler_policy.py tests/test_frontier.py tests/test_incremental_crawl.py`
- Full suite (pass):
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m pytest -q`
- Required live smoke (`docs/USAGE.md` / `AGENTS.md`) (pass):
  - `/home/codespace/.cache/pypoetry/virtualenvs/seo-audit-EkeeUdp5-py3.12/bin/python -m seo_audit audit --domain https://void-agency.com --output ./out`
  - run id: `6ebbfee4-f8db-48fd-a156-355548118ce3`
