# Implementation Runbook

1. Treat `docs/PLAN.md` as the source of truth for scope and milestone order.
2. Keep diffs scoped to the active milestone; avoid unrelated refactors.
3. After each milestone:
   - run relevant tests/checks,
   - repair failures,
   - log progress/decisions in `docs/STATUS.md`.
4. Record assumptions when ambiguity exists; choose the smallest reasonable behavior.
5. Keep architecture modular and deterministic:
   - observed facts in extraction/storage,
   - heuristics in classify/scoring/issues.
6. Before claiming done, execute a live smoke test and verify required outputs.
