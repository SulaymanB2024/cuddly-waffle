# Model Handoff: Codebase Structure, Design, and Usage

## Purpose
This repository implements a **Stage 1 SEO audit CLI** that performs public-data crawling and analysis for a target domain, then writes SQLite + CSV + Markdown artifacts.

## Runtime Pipeline (end-to-end)
1. Parse CLI args and build `AuditConfig`.
2. Initialize SQLite and create a run row.
3. Fetch/parse robots + sitemap data.
4. Seed URLs and run BFS crawl with retries.
5. Extract on-page SEO facts + links.
6. Compute link graph metrics and classify page type.
7. Optionally run rendered DOM checks (Playwright).
8. Build issues and deterministic scores.
9. Optionally collect PageSpeed metrics.
10. Export CSVs and build markdown report.

## Package layout
- `seo_audit/cli.py`: top-level orchestration and CLI subcommands.
- `seo_audit/config.py`: dataclass runtime config.
- `seo_audit/models.py`: typed dataclass records used throughout the pipeline.
- `seo_audit/storage.py`: SQLite schema and persistence + CSV exports.
- `seo_audit/url_utils.py`: normalization/domain eligibility/asset filtering.
- `seo_audit/http_utils.py`: HTTP wrapper with redirect-chain tracking.
- `seo_audit/robots.py`: robots fetch/parse/allow decisions.
- `seo_audit/sitemaps.py`: sitemap parsing and recursive discovery.
- `seo_audit/discovery.py`: seed URL generation and host eligibility policy.
- `seo_audit/crawler.py`: crawl loop, HTML gating, extraction, link persistence payloads.
- `seo_audit/extract.py`: HTML parsing and on-page fact extraction.
- `seo_audit/linkgraph.py`: inlinks/outlinks/depth/orphan/nav-linked metrics.
- `seo_audit/classify.py`: deterministic page-type classification.
- `seo_audit/render.py`: optional rendered-content capture + render-gap scoring.
- `seo_audit/issues.py`: explainable issue generation.
- `seo_audit/scoring.py`: transparent heuristic scoring model.
- `seo_audit/performance.py`: PageSpeed provider integration.
- `seo_audit/reporting.py`: markdown report writer.

## Design principles reflected in code
- **Observed vs derived separation**: extraction modules collect observable facts; analysis modules derive classification/issues/scores.
- **Deterministic heuristics**: no ML model, scores and issues are rule-based and explainable.
- **Run isolation**: all key tables include `run_id` so historical runs can coexist.
- **Graceful degradation**:
  - render unavailable => note + issue, run still completes
  - performance provider failure => note + low-severity issue, run still completes
  - robots/sitemap failures become diagnostics, not fatal

## Data model summary
- `runs`: execution metadata and notes.
- `pages`: core observed and derived page facts.
- `links`: observed links from source page to normalized target.
- `issues`: generated findings with severity/code/title/description/evidence.
- `scores`: sub-scores and overall deterministic score.
- `performance_metrics`: PageSpeed rows per URL and strategy.
- `sitemap_entries`, `robots_rules`: discovery artifacts.

## Test suite structure
- Unit tests per module for normalization, robots, sitemaps, extraction, classification, issues, scoring, render diagnostics, performance diagnostics.
- Integration tests spin up local `HTTPServer` mini-sites to validate full pipeline behavior and artifact generation.
- Tests avoid live external dependencies by monkeypatching provider/render paths when needed.

## Developer usage
- Install: `python -m pip install -e .[dev]`
- Run tests: `pytest -q`
- Run audit: `python -m seo_audit audit --domain https://void-agency.com --output ./out`

## Current stage context
According to `docs/PLAN.md`, this codebase targets Stage 1 milestones through smoke/polish and intentionally excludes rank tracking, private analytics integrations, backlinks, distributed workers, and UI/dashboard.
