# Stage 1.6h Google API and Crawler Compliance Hardening Plan

## Purpose
Implement a narrow Stage 1.6h hardening pass for Google-facing provider behavior and crawler controls, without adding Stage 2 scope.

## In scope
- PSI key enforcement inside provider functions.
- CrUX key enforcement continuity with explicit status semantics.
- Bounded retry/backoff with Retry-After support for PSI and CrUX endpoints.
- Raw provider payload storage default-off with explicit opt-in.
- Payload retention control for stored provider payloads.
- robots.txt crawl-delay enforcement when robots are respected.
- Explicit acknowledgement requirement for robots bypass.
- Unified configured user-agent across raw and rendered fetch paths.
- Tests and documentation updates for all controls above.

## Out of scope
- Stage 2 features: SERP observation, competitor modules, dashboards, and new product surfaces.
- New external providers beyond existing PSI/CrUX flows.

## Required runtime semantics
- Provider outcomes must be explicit and observable:
  - `success`
  - `no_data`
  - `failed_http`
  - `skipped_missing_key`
- Missing key behavior must short-circuit before network requests.

## Milestones
1. Docs baseline
- Add Stage 1.6h status ledger section before code edits.
- Record scope lock and acceptance gates.

2. Provider runtime hardening
- Add key-required enforcement in PSI and CrUX function-level paths.
- Add shared bounded retry/backoff helper with Retry-After support.
- Ensure retry metadata is surfaced in errors/notes.

3. Payload controls
- Add CLI/config switch for provider payload storage opt-in.
- Default to parsed metrics only (`payload_json` omitted or `{}`).
- Ensure stored provider payloads remain valid JSON.
- Add payload retention control and runtime cleanup hook.

4. Crawl/robots hardening
- Compute effective crawl delay from robots rules for configured UA or wildcard.
- Apply crawl-delay enforcement to crawl pacing when robots are respected.
- Require explicit acknowledgement flag for robots bypass.

5. Render identity hardening
- Pass configured user-agent into Playwright browser context/page creation.
- Preserve explicit non-fatal render diagnostics.

6. Tests
- Add/update tests for provider auth, retry/backoff, payload controls, crawl-delay behavior, robots bypass acknowledgement, and render UA consistency.
- Run full test suite.

7. Validation and close-out
- Run bounded compliance audit command.
- Run required live smoke command from usage docs.
- Update docs (`STATUS`, `ARCHITECTURE`, `USAGE`, `README`) with concrete controls and conservative readiness judgment.

## Acceptance gates
- Gate 1: tests pass.
- Gate 2: PSI direct path cannot issue unauthenticated requests.
- Gate 3: CrUX direct path cannot issue unauthenticated requests.
- Gate 4: PSI/CrUX calls use bounded retry/backoff with Retry-After support.
- Gate 5: raw payload storage is opt-in and default-off.
- Gate 6: stored payload JSON remains valid when enabled.
- Gate 7: crawl-delay is enforced when robots are respected.
- Gate 8: robots bypass requires explicit acknowledgement.
- Gate 9: rendered fetches use configured user-agent.
- Gate 10: docs describe behavior accurately without overstatement.
- Gate 11: explicit readiness call (`READY for controlled Google-facing use` or `NOT READY`).
