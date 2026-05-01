# Plan

## Objective
Build Stage 1 of a public-data-only SEO audit CLI that crawls a domain, extracts SEO signals, computes explainable heuristics, and writes SQLite + CSV + Markdown outputs.

## Scope
- Crawl/discovery, robots/sitemaps, extraction, render comparison, link graph, classification, issues, scores, public performance provider, reporting, tests, docs.

## Non-scope
- SERP/rank tracking, private analytics integrations, backlinks, UI/dashboard, distributed workers, cloud deployment.

## Milestones
1. **M0 Bootstrap**: skeleton, packaging, docs, test harness.
2. **M1 Core**: config, models, storage schema and helpers.
3. **M2 Discovery primitives**: URL normalization, robots, sitemaps.
4. **M3 Crawl+extract**: crawler, metadata extraction, links persistence.
5. **M4 Analysis**: page classification, issues, scoring.
6. **M5 Render**: Playwright module and raw-vs-render diff.
7. **M6 Performance**: PageSpeed provider with graceful failure.
8. **M7 Reporting**: CSV export + Markdown report.
9. **M8 Smoke+polish**: live run against void-agency.com, fix/document.
10. **M9 Fidelity+quality hardening**: render dependency readiness, fresh-output hygiene, scoring reliability alignment, signal-to-noise reporting tracks, and deeper template-group performance interpretation.

## Acceptance criteria by milestone
- **M0**: package installable; test command executes.
- **M1**: DB init and insert/select tests pass.
- **M2**: deterministic tests for URL/robots/sitemaps pass.
- **M3**: crawl and extraction persist page/link data.
- **M4**: deterministic classifier and scoring tests pass.
- **M5**: render module can run or skip safely without breaking audits.
- **M6**: performance fetch failures are non-fatal and logged.
- **M7**: required CSVs and report generated.
- **M8**: live smoke test produces usable artifacts and status update.

## Future direction
- Shift high-volume lab auditing to local Lighthouse/Lighthouse CI pipelines while keeping CrUX collection as a separate field-data track.
