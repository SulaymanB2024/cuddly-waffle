# Architecture

## Module responsibilities
- `cli.py`: orchestration of full audit run and CLI flags.
- `config.py`: runtime config model.
- `storage.py`: SQLite schema, writes, and CSV exports.
- `url_utils.py`: normalization, domain checks, asset filtering.
- `robots.py`: robots fetch/parse and allow checks.
- `sitemaps.py`: sitemap recursion and entry extraction.
- `discovery.py`: URL seed strategy from root + robots + sitemaps.
- `crawler.py`: same-domain BFS crawl, retry loop, content-type/html gating, and persistence payloads.
- `extract.py`: on-page metadata extraction (observed facts) with malformed-HTML fallbacks.
- `render.py`: optional Playwright render capture and render-gap heuristics.
- `linkgraph.py`: internal directed graph metrics plus second-pass page graph analytics.
- `classify.py`: conservative evidence-weighted page type classification.
- `issues.py`: issue generation with severities/codes plus governance/preview mismatch auditing.
- `scoring.py`: transparent 0-100 heuristic scoring with preview-control and structured-data-validity dimensions.
- `scoring_policy.py`: shared score/issue policy metadata (page type helpers, page importance, thin-content/internal-link thresholds, risk families, cap trigger metadata, score version/profile constants).
- `performance.py`: PSI and CrUX provider collection, parsing, and status semantics.
- `offsite_commoncrawl.py`: Common Crawl offsite domain-graph worker lane (release resolution, cache/manifest management, DuckDB materialization/query, pure payload return).
- `search_console.py`: optional Search Console URL Inspection collection and index-state reconciliation.
- `reporting.py`: Markdown summary report.
- `dashboard.py`: local interactive dashboard server and run-scoped read-only API.
- `http_utils.py`: lightweight HTTP client wrapper with redirect-chain tracking.

## Same-site host eligibility (Stage 1.6b)
- Discovery seed filtering now treats only apex + `www` variants of the target site as same-site eligible.
- Example: `https://www.example.com` accepts `https://example.com/...` and `https://www.example.com/...` seeds.
- Unrelated subdomains (e.g., `blog.example.com`) are excluded from seed eligibility unless explicitly added by future policy.
- A `homepage_redirect_host` parameter exists in discovery for future use when homepage host normalization needs to include an observed redirect host.

## Data flow
1. Config + run row initialized.
2. Robots + sitemap discovery persisted.
3. Crawl fetches pages and links.
4. Link graph and page classification enrich page rows.
5. Second-pass graph analytics compute and persist `internal_pagerank`, `betweenness`, `closeness`, `community_id`, and `bridge_flag` in `page_graph_metrics`.
6. Optional render comparison enriches render-gap fields.
7. Issues and scores derived from page facts + heuristics.
8. Optional PSI and CrUX metrics collected.
9. Optional Common Crawl offsite worker starts early as a concurrent lane; main crawl pipeline proceeds without default blocking.
10. Optional Search Console URL-inspection rows are collected and reconciled for sampled targets.
11. Run telemetry events persisted (`run_events`) for stage timing, crawl heartbeats, governance summaries, provider summaries, and offsite lane status/cache/timing events.
12. CSV + report exported from SQLite.

## Offsite Common Crawl lane
- Offsite lane is additive to same-site crawl truth; it never writes SQLite from worker threads.
- Worker execution model:
  - own requests session,
  - own DuckDB connection,
  - returns pure payload only,
  - main thread persists summary/linking-domain/comparison-domain rows.
- Scheduling model:
  - `concurrent_best_effort` (default): no new visible blocking stage; tiny end join budget then deferred/pending within the same run process.
  - `background_wait`: bounded join at end.
  - `blocking`: explicit full wait.
- Cancellation model:
  - cooperative `threading.Event` stop signal,
  - chunk-level cancellation checks during streaming downloads,
  - main-thread interrupt request for active DuckDB query when join budget is exceeded.
- Cache model:
  - release-scoped path: `~/.cache/seo_audit/commoncrawl/<release>/`,
  - includes raw assets, persistent `commoncrawl.duckdb`, and manifest metadata (`vertices_ready`, `ranks_ready`, `edges_ready`, timestamps),
  - local asset materialization is preferred over remote CSV scans.
- Mode model:
  - `ranks`: target + comparison domains from domain ranks.
  - `domains`: linking-domain discovery only when edge cache is warm by default; cold edge scan requires explicit opt-in.
  - `verify`: deferred status only in this milestone (`deferred_verify_not_implemented`) and hidden behind an experimental flag.
- Domain-graph caveat:
  - outputs represent Common Crawl domain graph linking-domain intelligence and do not claim exact page-level backlink proof.

## Governance and preview-control semantics
- Governance checks reuse parsed robots rules and evaluate per-page access for Googlebot, Bingbot, OAI-SearchBot, and Google-Extended.
- Restrictive controls are not always defects by default; issue generation distinguishes likely intentional restrictions from likely accidental suppression/conflict.
- Raw-vs-rendered mismatches for noindex and preview controls are treated as higher-risk evidence-quality conflicts.
- Governance snapshots are recorded in `run_events` (`event_type=governance_summary`) and surfaced in reports.

## Search Console reconciliation behavior
- Search Console collection uses URL Inspection API calls per sampled URL (`--gsc-url-limit`) and preserves existing reconciliation contracts.
- Runtime status is explicit (`success`, `success_partial`, `success_empty`, `skipped_missing_credentials`, `failed_invalid_credentials_path`, `failed_auth`, `failed_api`, `failed_missing_dependency`).
- URL-level non-indexed states are emitted as `GSC_INDEX_STATE_NOT_INDEXED` issues with evidence payloads when inspection succeeds.
- Reconciliation remains additive evidence and does not replace crawler-observed facts.

## Dashboard read path (Stage 1.x extension)
- Local command `python -m seo_audit dashboard --db <path>` starts a read-only HTTP server.
- Dashboard API reads run-scoped data from existing SQLite tables (`runs`, `pages`, `links`, `issues`, `scores`, `page_graph_metrics`, `performance_metrics`, `crux_metrics`, `run_events`).
- Dashboard exposes architecture analysis payloads through `/api/architecture`.
- Query-lab and dashboard read paths open SQLite connections in read-only mode and still enforce read-only SQL validation (`SELECT`/`WITH` only).
- No audit-time schema or write-path changes are required; dashboard does not mutate run artifacts.
- Interactive views support run selection, issue/page filtering, URL detail drill-down, two-run comparison, and filtered CSV export.

## Storage design
SQLite with run isolation by `run_id` across tables:
- `runs`, `pages`, `links`, `issues`, `scores`, `page_graph_metrics`, `performance_metrics`, `crux_metrics`, `sitemap_entries`, `robots_rules`, `run_events`.

Observed facts are persisted directly from network/HTML parsing. Derived fields (page type, orphan risk, duplicate flags, scores, issues) are computed in separate modules and stored explicitly.

## Parser robustness decision (Stage 1 hardening)
- Primary extraction now uses a recovery-capable lxml tree parser for deterministic malformed-HTML handling.
- Existing fallback behavior is retained for malformed fragments (`<title>`, meta description, `<h1>`, and fallback word-count extraction).
- Extraction continues to separate observed page facts from downstream heuristics.
- New observed facts include `heading_outline_json`, compact allowlisted `schema_summary_json`, `content_hash`, and link-level `dom_region`.
- Link provenance semantics remain unchanged: `links.source_context` still captures provenance (`raw_dom` vs `render_dom`), while semantic region is stored separately in `links.dom_region`.

## Redirect and output hardening (Stage 1.5)
- `http_utils` now tracks redirect chains for each fetch.
- `crawler` persists redirect chain JSON from HTTP client response and applies retry behavior.
- `issues` now includes `CANONICAL_MISMATCH` and skips HTML-only issue classes for clearly non-HTML pages.
- `scoring` applies on-page penalties only for HTML-like pages.

## Integration test design (Stage 1.5)
- In-process local HTTP mini-site (`HTTPServer`) exercises end-to-end pipeline behavior:
  - `robots.txt`, sitemap index + nested sitemap,
  - redirects,
  - internal links,
  - noindex page,
  - thin page,
  - canonical mismatch,
  - duplicate-ish title/description.
- Test validates DB writes, issue coverage, score rows, redirect chain presence, and exported artifacts.

## Provider key resolution (Stage 1.6f)
- Runtime resolves keys in this order:
  - PSI: `PSI_API_KEY` else `GOOGLE_API_KEY`
  - CrUX: `CRUX_API_KEY` else `GOOGLE_API_KEY`
- CrUX rows carry explicit status semantics (`success`, `no_data`, `failed_http`, `skipped_missing_key`) and include `query_scope` (`url` or `origin`).
- CrUX origin fallback is optional and controlled by CLI/runtime config.

## Google API and crawler hardening (Stage 1.6h)
- Provider auth enforcement:
  - PSI and CrUX both enforce key checks inside provider functions.
  - Missing-key paths short-circuit before outbound requests with explicit `skipped_missing_key` semantics.
- Retry/backoff policy:
  - Shared bounded retry helper is used for PSI and CrUX HTTP calls.
  - Retryable statuses: `429`, `500`, `502`, `503`, `504`.
  - `Retry-After` is honored by default when present.
  - Otherwise exponential backoff with jitter is applied.
  - Retry count and cumulative wait are surfaced through provider diagnostics and run notes.
- Payload storage policy:
  - Parsed provider fields are stored by default.
  - Raw provider payload storage is opt-in via runtime/CLI flag.
  - Stored payload JSON remains valid JSON and run-scoped in exports.
  - Retention policy can purge older payload JSON to `{}` based on configured retention days.
- Crawl-delay enforcement:
  - Robots `crawl-delay` is resolved from parsed rules and applied when robots are respected.
  - Precedence is explicit: most specific matching user-agent rule first, wildcard fallback second.
  - Effective per-request delay is `max(base_request_delay, resolved_crawl_delay)`.
- Robots bypass control:
  - Bypass requires both `--ignore-robots` and explicit acknowledgement `--i-understand-robots-bypass`.
  - Missing acknowledgement produces a fail-fast CLI error.
- Unified request identity:
  - Playwright rendered fetches use the configured crawler user-agent through browser context setup.

## Observability and runtime profiles (Stage 1.6i)
- Runtime profile controls:
  - CLI/runtime profile selector: `exploratory`, `standard`, `deep`.
  - Profile defaults tune crawl cap, render sampling, provider target count, provider retry budget, and crawl heartbeat cadence.
  - Explicit CLI flags continue to override profile defaults.
- Stage timing telemetry:
  - Orchestration records per-stage elapsed milliseconds in `run_events` (`event_type=stage_timing`).
  - A compact stage timing summary is appended to `runs.notes` for quick post-run inspection.
- Crawl heartbeat telemetry:
  - Crawler emits periodic heartbeat payloads (`event_type=crawl_heartbeat`) with pages stored, queue size, errors, elapsed time, and current URL/depth.
  - Heartbeat cadence is configurable by page count interval.
- Provider telemetry summary:
  - PSI/CrUX collection records aggregate attempt counters (`attempts`, `http_attempts`, `retries`, `wait_seconds`, `timeouts`) and outcome counters.
  - Summaries are written into `run_events` (`event_type=provider_summary`) and mirrored in `runs.notes`.
- Report integration:
  - Report includes stage timing section, crawl heartbeat summary, provider telemetry section, and active run profile.

## Export behavior (Stage 1.6f)
- CSV export is run-scoped in CLI execution (`run_id` filter applied).
- Provider exports include both `performance.csv` (PSI rows) and `crux.csv` (CrUX rows).
- Telemetry export includes `run_events.csv`.

## Scoring philosophy
- Deterministic and explainable weighted penalties/bonuses.
- No ML or opaque ranking model.
- Separate sub-scores: crawlability, on-page, render risk, internal linking, local SEO completeness, performance, preview controls, structured-data validity.
- Thin-content uses a selective continuous penalty curve to avoid threshold cliff effects, while discrete blockers (for example noindex/access/canonical hard failures) remain discrete.
- Internal-linking score is treated as internal architecture using linkgraph inputs (`effective_internal_links_out`, `inlinks`, `crawl_depth`, `nav_linked_flag`, `orphan_risk_flag`) rather than raw outlink count only.
- Quality score is a weighted average of applicable dimensions (non-applicable dimensions are excluded from the denominator).
- Risk score blends issue-level severity/gate/certainty/reach components with family-based diminishing returns so repeated mild findings taper within a family while distinct families still stack.
- Missing social cards are intentionally not treated as core search-discovery penalties.
- Overall score uses capped risk-adjusted quality with coverage confidence:
  - `confidence_factor = 0.55 + 0.45 * (coverage_score / 100)`
  - `overall_score = clamp(min(score_cap, (quality_score - risk_score * 0.40) * confidence_factor))`

## Score explainability contract
- Scores now persist additive explainability metadata fields: `scoring_model_version`, `scoring_profile`, and `score_explanation_json`.
- Legacy fields remain persisted and readable for backward compatibility: `score_version`, `score_profile`, and `explanation_json`.
- `scoring_profile` supports current `general`/`local` behavior and is independent from runtime crawl `run_profile`.
- Explanation payloads persist deterministic inputs, per-dimension scores and deductions, quality weighting details, family-level risk contributors, cap reasons, and overall formula metadata for dashboard drill-down.
- Explainability includes `preview_controls_score` and `structured_data_validity_score`; `structured_snippets_score` remains as a compatibility alias in explanation payloads.
- `PAGINATION_SIGNAL_MISSING` remains a diagnostic issue and is intentionally dampened in risk blending so it does not materially distort scoring/caps.

## Extension points for Stage 2+
- Add provider interfaces in `performance.py` for new public endpoints.
- Add new derived analyzers (SERP/archive/offsite) writing new tables while preserving current run model.
- Upgrade link graph calculations without changing crawl schema.
- Add incremental crawl strategy with additional run metadata.
