# Stage 1.5 Validation & Hardening Plan

## Why Stage 1.5 exists
Stage 1 delivered core implementation and unit tests. Stage 1.5 exists to validate runtime behavior, harden weak points, and establish a trustworthy baseline before Stage 2 expansion.

## Hardening goals
1. Improve crawl correctness under redirects/content-type variance/partial failures.
2. Validate and harden render-mode sample behavior and render-gap logic.
3. Validate provider success/failure handling and metric persistence sanity.
4. Improve output trustworthiness (pages/issues/scores coherence).
5. Add integration-style pipeline coverage beyond isolated unit tests.

## Milestone plan
### M1: Validation scaffolding + docs baseline
- Add Stage 1.5 section in `docs/STATUS.md` with explicit validation categories.
- Add this plan file with gates and exit criteria.

### M2: Redirect/canonical/output hardening
- Improve redirect chain capture and canonical mismatch issue handling.
- Tighten content-type extraction gating and failure recording clarity.
- Improve duplicate title/description handling precision.

### M3: Parser and render hardening
- Evaluate parser robustness with malformed HTML fixtures.
- Improve render sample selection and render-gap reason quality if needed.
- Add tests for render-gap logic and parser edge cases.

### M4: Integration testing track
- Add controlled mini-site integration path (robots, sitemap/index, redirect, noindex, thin page, contact/local signal, canonical mismatch).
- Exercise end-to-end pipeline writes to DB + export/report paths.

### M5: Live validation track
- Attempt bounded live audits (render none + sample).
- If blocked, record exact network blocker and rely on integration substitutes.
- Inspect actual pages/issues/scores rows from produced outputs.

## Validation gates
- Gate 1: `pytest -q` passes.
- Gate 2: Integration-style pipeline test exists and passes.
- Gate 3: Render-mode sample behavior validated live or via strong controlled tests.
- Gate 4: Redirect/canonical/output coherence reviewed and hardened.
- Gate 5: `docs/STATUS.md` explicitly separates implemented / unit-tested / integration-tested / live-validated / blocked-unproven.
- Gate 6: Explicit Stage 2 readiness judgment documented.

## Exit criteria for Stage 2 readiness
- PASS all gates above.
- No known critical data-integrity defects in Stage 1 pipeline.
- Remaining risks are documented with clear scope boundaries and mitigation notes.
