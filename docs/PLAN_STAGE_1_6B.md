# Stage 1.6b Defect-Fix & Validation Plan

## Purpose
Address concrete post-validation defects from Stage 1.5 without expanding scope into Stage 2 features.

## Defects in scope
1. **Apex/www discovery eligibility bug** in seed/internal filtering.
2. **Stale run status in report** caused by report generation before final run state write.
3. **Render-path diagnostics gap** when Playwright/browser is unavailable.
4. **Performance-provider diagnostics gap** when PageSpeed requests fail.

## Milestones
### M1: Docs baseline
- Add Stage 1.6b status section before code changes.

### M2: Defect A (highest priority)
- Refine same-site host eligibility to allow apex↔www crossover for the seeded site.
- Keep unrelated subdomains excluded.
- Add tests for www-seeded and apex-seeded acceptance + unrelated subdomain rejection.

### M3: Defect B
- Ensure final run status/completed timestamp are persisted before final report generation.
- Add tests proving report status correctness for completed and failed runs.

### M4: Defects C and D diagnostics
- Render: return explicit diagnostic outcome for import/browser/runtime failures and surface in run notes/issues.
- Performance: surface structured failure reasons and distinguish failure from empty success.
- Add tests proving diagnostics are explicit.

### M5: Validation rerun
- Run full test suite.
- Run bounded external validations if possible.
- Inspect output rows for discovery improvement and diagnostics visibility.

## Validation gates
- Gate 1: `pytest -q` passes.
- Gate 2: www/apex bug fixed and tested.
- Gate 3: `report.md` shows final status correctly.
- Gate 4: render diagnostics explicit.
- Gate 5: performance diagnostics explicit.
- Gate 6: external run demonstrates improved discovery if network permits.
- Gate 7: explicit READY/NOT READY judgment documented.
