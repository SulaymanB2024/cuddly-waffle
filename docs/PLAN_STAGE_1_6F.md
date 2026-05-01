# Stage 1.6f Branch Reconciliation and Provider-Restore Plan

## Purpose
Restore a coherent Stage 1.6e-capable provider implementation after branch drift, without adding Stage 2 scope.

## In scope
- Reconcile runtime and tests for provider entry points.
- Restore PSI path and CrUX path in runtime code.
- Restore runtime key-resolution behavior for shared and provider-specific keys.
- Verify storage/export/report coherence for PSI and CrUX rows.
- Re-run bounded validation and record truthful provider outcomes.

## Out of scope
- SERP observation, competitor intelligence, dashboards, and new product surface.

## Required symbols and semantics
- Runtime exposes `resolve_google_keys`.
- Runtime exposes `collect_crux`.
- Runtime supports PSI + CrUX provider flow with explicit statuses: `success`, `no_data`, `failed`, `skipped`.
- Key resolution order:
  - PSI: `PSI_API_KEY` else `GOOGLE_API_KEY`
  - CrUX: `CRUX_API_KEY` else `GOOGLE_API_KEY`

## Milestones
1. Docs baseline
- Add Stage 1.6f section in status ledger before major code edits.

2. Provider runtime reconciliation
- Restore provider entry points and consistent naming.
- Reconcile call sites in CLI/runtime.

3. CrUX restoration
- Implement URL-level CrUX query with optional origin fallback.
- Persist rows into `crux_metrics` and export `crux.csv`.

4. PSI coherence
- Keep official PSI path intact.
- Preserve category/lab parsing and payload JSON persistence.

5. Tests
- Full suite plus focused provider tests.
- Add tests for key resolution, CrUX URL parse, CrUX origin fallback, PSI/CrUX persistence, report safety.

6. Bounded validation
- Run bounded audit with PSI and CrUX enabled.
- Inspect `audit.sqlite`, `report.md`, `performance.csv`, and `crux.csv`.

7. Documentation close-out
- Update status ledger with restored items, checks run, blocked conditions, and readiness judgment.

## Acceptance gates
- Gate 1: full tests pass.
- Gate 2: `resolve_google_keys` exists and passes tests.
- Gate 3: `collect_crux` exists and passes tests.
- Gate 4: PSI runtime/storage/report paths are coherent.
- Gate 5: CrUX runtime/storage/report paths are coherent.
- Gate 6: bounded audit runs end-to-end without provider import/runtime breakage.
- Gate 7: outputs show truthful provider statuses.
- Gate 8: explicit READY or NOT READY judgment recorded.
