# Stage 1.6i Observability and Runtime Profiles Plan

## Purpose
Add run-level observability and practical runtime profiles to improve operator visibility and run-speed control, without adding Stage 2 scope.

## In scope
- Stage timing breakdown persisted per run.
- Periodic crawl heartbeat telemetry.
- Provider attempt/retry/wait/timeout telemetry summaries.
- `run_events` telemetry table and run-scoped CSV export.
- Report + run-notes surfacing of stage/provider telemetry.
- Runtime profiles: `exploratory`, `standard`, `deep`.
- Profile-aware defaults with explicit CLI overrides.

## Out of scope
- Stage 2 features (SERP observation, competitor modules, dashboards, product/UI expansion).
- New external data providers.
- Distributed execution architecture.

## Required runtime semantics
- `run_events` rows must be run-scoped and persisted in SQLite.
- Stage timing must include elapsed milliseconds per major orchestration stage.
- Heartbeat cadence must be configurable and disable-able.
- Provider telemetry must include aggregate counters:
  - `attempts`, `http_attempts`, `retries`, `wait_seconds`, `timeouts`
  - outcome counters (`success`, `no_data`, `failed_http`, `skipped_missing_key`)
- Runtime profile defaults must be deterministic and documented.
- Explicit flag values must override profile defaults.

## Milestones
1. Telemetry schema and storage
- Add `run_events` table.
- Add insert/export helpers for run events.

2. Orchestration stage timing
- Instrument major CLI stages.
- Persist stage timing events and summarize in run notes.

3. Crawl heartbeat
- Add crawler heartbeat callback and periodic payload emission.
- Persist heartbeat events from orchestration.

4. Provider telemetry summary
- Accumulate provider attempt/retry/wait/timeout counters.
- Persist provider summary events and append concise run notes.

5. Runtime profiles
- Add profile selector (`exploratory|standard|deep`) and defaults.
- Apply profile values across crawl/render/provider knobs.
- Keep explicit CLI overrides authoritative.

6. Reporting and exports
- Include stage timing, heartbeat summary, provider telemetry, and run profile in report.
- Ensure `run_events.csv` includes final flushed telemetry rows.

7. Tests and validation
- Add tests for profile persistence/defaults, run-events persistence/export, and report sections.
- Run focused tests, full suite, and required live smoke command.

## Acceptance gates
- Gate 1: run events are persisted in DB for stage timing and heartbeat telemetry.
- Gate 2: `run_events.csv` contains flushed telemetry events for the run.
- Gate 3: report includes stage timing, heartbeat summary, provider telemetry, and run profile.
- Gate 4: runtime profile defaults are applied when explicit overrides are absent.
- Gate 5: explicit CLI overrides still win over profile defaults.
- Gate 6: focused and full test suites pass.
- Gate 7: required live smoke command completes and artifacts are generated.
