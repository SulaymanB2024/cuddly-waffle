# Usage

## Basic run
```bash
python -m pip install -e .[dev]
python -m seo_audit audit --domain https://void-agency.com --output ./out
```

## Validation-focused examples
```bash
# bounded crawl, no rendering
python -m seo_audit audit --domain https://www.example.com --output ./out/example-none --max-pages 20 --render-mode none

# bounded crawl with render sampling
python -m seo_audit audit --domain https://example.com --output ./out/example-sample --max-pages 20 --render-mode sample --max-render-pages 5

# bounded provider validation
python -m seo_audit audit --domain https://example.com --output ./out/provider-reconcile --max-pages 3 --psi-enabled --crux-enabled --performance-targets 1

# Stage 1.6h compliance-hardening validation (keys optional; skip states are explicit)
python -m seo_audit audit --domain https://example.com --output ./out/compliance_hardening --max-pages 3 --psi-enabled --crux-enabled --performance-targets 1 --provider-max-retries 2 --provider-base-backoff-seconds 0.5 --provider-max-backoff-seconds 6.0 --provider-max-total-wait-seconds 20 --provider-respect-retry-after --no-store-provider-payloads

# Stage 1.6i observability/profile validation
python -m seo_audit audit --domain https://example.com --output ./out/observability_profile --run-profile exploratory --crawl-heartbeat-every-pages 5

# Lighthouse + bounded concurrency validation
python -m seo_audit audit --domain https://example.com --output ./out/lab_concurrency --max-pages 20 --crawl-workers 4 --crawl-queue-high-weight 4 --crawl-queue-normal-weight 2 --lighthouse-enabled --lighthouse-targets 3 --lighthouse-form-factor mobile --lighthouse-budget-performance-min 60 --lighthouse-budget-seo-min 70

# Production-grade deep run with fresh per-run output and broader render/perf coverage
python -m seo_audit audit --domain https://example.com --output ./out/production --fresh-output-dir --run-profile deep --max-render-pages 160 --performance-targets 30 --site-type general --psi-workers 4 --provider-rate-limit-rps 4 --provider-rate-limit-capacity 4

# Offsite Common Crawl ranks (default concurrent schedule)
python -m seo_audit audit --domain https://example.com --output ./out --offsite-commoncrawl-enabled --offsite-commoncrawl-mode ranks

# Offsite Common Crawl ranks with comparison domains
python -m seo_audit audit --domain https://example.com --output ./out --offsite-commoncrawl-enabled --offsite-commoncrawl-mode ranks --offsite-compare-domain competitor1.com --offsite-compare-domain competitor2.com

# Offsite Common Crawl domains mode (warm edge cache path)
python -m seo_audit audit --domain https://example.com --output ./out --offsite-commoncrawl-enabled --offsite-commoncrawl-mode domains

# Offsite Common Crawl domains mode with explicit cold-edge opt-in and bounded wait
python -m seo_audit audit --domain https://example.com --output ./out --offsite-commoncrawl-enabled --offsite-commoncrawl-mode domains --offsite-commoncrawl-allow-cold-edge-download --offsite-commoncrawl-schedule background_wait --offsite-commoncrawl-join-budget-seconds 3
```

## Interactive dashboard
```bash
# Launch local interactive dashboard against an existing run database.
python -m seo_audit dashboard --db ./out/audit.sqlite --queue-db ./out/queue.sqlite --host 127.0.0.1 --port 8765

# Use an external worker process (dashboard enqueues only).
python -m seo_audit dashboard --db ./out/audit.sqlite --queue-db ./out/queue.sqlite --dashboard-worker-disabled
```

## Queue workflow
```bash
# Enqueue an audit job.
python -m seo_audit enqueue --domain https://example.com --output ./out --queue-db ./out/queue.sqlite

# Run a queue worker continuously.
python -m seo_audit worker --queue-db ./out/queue.sqlite

# Run a single-job worker tick (useful for smoke checks).
python -m seo_audit worker --queue-db ./out/queue.sqlite --once

# List current jobs.
python -m seo_audit jobs --queue-db ./out/queue.sqlite --limit 50

# Request cancellation for a queued/running job.
python -m seo_audit cancel <job_id> --queue-db ./out/queue.sqlite
```

## Setup notes
- Render setup (required when render comparisons are expected):
  ```bash
  python -m pip install playwright
  python -m playwright install chromium
  ```
- Optional provider API keys (override or shared fallback):
  ```bash
  export GOOGLE_API_KEY=your_key
  export PSI_API_KEY=your_key
  export CRUX_API_KEY=your_key
  ```
- Optional Lighthouse CLI runtime for lab collection (either form works):
  ```bash
  npm install -g lighthouse
  # or rely on npx resolution at runtime:
  npx lighthouse --version
  ```
- Optional Search Console credential usage (passed explicitly via CLI flag):
  ```bash
  python -m seo_audit audit --domain https://example.com --output ./out/gsc --gsc-enabled --gsc-credentials-json /absolute/path/to/service-account.json
  ```
- Runtime auto-load fallback when shell variables are missing:
  - `.seo_audit.env`
  - `.env.local`
  - `.env`
- Optional explicit env-file override:
  ```bash
  export SEO_AUDIT_ENV_FILE=/absolute/path/to/env-file
  ```

## Key flags
- `--domain`: root site URL.
- `--output`: artifact directory.
- `--fresh-output-dir|--reuse-output-dir`: write each run to a timestamped subdirectory (recommended for production).
- `--run-profile`: runtime defaults bundle (`exploratory|standard|deep`).
- `--site-type`: scoring mode (`general|local`) controlling local-SEO weighting behavior.
- `--scoring-profile`: optional score metadata profile label (defaults to the active `site_type`).
- `--max-pages`: crawl cap.
- `--max-render-pages`: cap for `sample` render mode.
- `--render-mode`: `none|sample|all`.
- `--timeout`: request timeout seconds.
- `--crawl-persona`: crawler persona (`googlebot_smartphone|googlebot_desktop|bingbot|oai_searchbot|browser_default`).
- `--robots-persona-mode`: robots-evaluation semantics (`google_exact|generic`), defaults to selected persona behavior.
- `--google-exact-apply-crawl-delay|--google-exact-ignore-crawl-delay`: opt in/out of applying `crawl-delay` when using `google_exact` robots semantics.
- `--user-agent`: optional request-header override for the selected crawl persona.
- `--max-response-bytes`: hard cap for crawler response body bytes per URL.
- `--max-non-html-bytes`: stricter body cap for non-HTML responses to avoid oversized downloads.
- `--crawl-retries`: bounded retries per crawl fetch request.
- `--crawl-base-backoff-seconds`: initial crawl retry backoff for exponential retries.
- `--crawl-max-backoff-seconds`: upper bound for any single crawl retry wait.
- `--crawl-max-total-wait-seconds`: upper bound for cumulative crawl retry waiting.
- `--crawl-respect-retry-after|--crawl-ignore-retry-after`: honor or ignore crawl-side `Retry-After` headers.
- `--crawl-workers`: bounded crawler worker concurrency.
- `--crawl-queue-high-weight`: weighted turns allocated to high-priority queue band.
- `--crawl-queue-normal-weight`: weighted turns allocated to normal-priority queue band.
- `--performance-targets`: max crawled URLs sent to providers.
- `--crawl-heartbeat-every-pages`: emit crawl progress events every N stored pages (`0` disables heartbeat).
- `--psi-enabled|--psi-disabled`: enable/disable PSI collection.
- `--crux-enabled|--crux-disabled`: enable/disable CrUX collection.
- `--crux-origin-fallback|--no-crux-origin-fallback`: use origin fallback when URL-level CrUX has no data.
- `--lighthouse-enabled|--lighthouse-disabled`: enable/disable local Lighthouse lab collection.
- `--lighthouse-targets`: max crawled URLs selected for Lighthouse runs.
- `--lighthouse-timeout-seconds`: timeout budget per Lighthouse run.
- `--lighthouse-form-factor`: Lighthouse strategy (`desktop|mobile`).
- `--lighthouse-config-path`: optional custom Lighthouse config JSON path.
- `--lighthouse-budget-performance-min`: performance budget threshold for `LIGHTHOUSE_BUDGET_FAIL` issue checks.
- `--lighthouse-budget-seo-min`: SEO budget threshold for `LIGHTHOUSE_BUDGET_FAIL` issue checks.
- `--gsc-enabled|--gsc-disabled`: enable/disable Search Console URL inspection reconciliation.
- `--gsc-property`: optional explicit Search Console property (for example `sc-domain:example.com`).
- `--gsc-credentials-json`: absolute path to service-account JSON credentials with Search Console property access.
- `--gsc-url-limit`: cap for inspected URLs passed to Search Console reconciliation.
- `--gsc-analytics-enabled|--gsc-analytics-disabled`: enable/disable Search Console Search Analytics enrichment.
- `--gsc-analytics-days`: lookback window for Search Analytics extraction.
- `--gsc-analytics-row-limit`: row cap for Search Analytics collection.
- `--gsc-analytics-dimensions`: comma-separated dimensions (default `page,query,device,country,date`).
- `--offsite-commoncrawl-enabled|--offsite-commoncrawl-disabled`: enable/disable additive Common Crawl offsite lane.
- `--offsite-commoncrawl-mode`: `ranks|domains` (`verify` is experimental-only).
- `--offsite-commoncrawl-schedule`: `concurrent_best_effort|background_wait|blocking` (`background_best_effort` accepted as legacy alias).
- `--offsite-commoncrawl-release`: release selector (`auto` by default).
- `--offsite-commoncrawl-cache-dir`: release-scoped cache root.
- `--offsite-commoncrawl-max-linking-domains`: top linking-domain cap for domains mode.
- `--offsite-commoncrawl-join-budget-seconds`: bounded join wait budget for background schedules.
- `--offsite-commoncrawl-time-budget-seconds`: worker-side time budget.
- `--offsite-commoncrawl-allow-cold-edge-download`: explicit opt-in for cold edge-cache downloads.
- `--offsite-compare-domain`: repeatable comparison-domain input.
- `--offsite-commoncrawl-experimental-verify`: hidden experimental gate for deferred verify mode.
- `--citation-measurement-enabled|--citation-measurement-disabled`: enable/disable AI citation visibility derivation.
- `--ignore-robots`: request robots bypass (only active when acknowledgement flag is also provided).
- `--i-understand-robots-bypass`: required acknowledgement flag for robots bypass.
- `--provider-max-retries`: bounded retries per PSI/CrUX request.
- `--provider-base-backoff-seconds`: initial backoff seconds used for exponential retry.
- `--provider-max-backoff-seconds`: upper bound for any single retry wait.
- `--provider-max-total-wait-seconds`: upper bound for cumulative wait across retries.
- `--provider-respect-retry-after|--provider-ignore-retry-after`: honor or ignore provider `Retry-After` headers.
- `--psi-workers`: bounded PSI worker pool size (default `4`).
- `--provider-rate-limit-rps`: shared provider token refill rate per second (default `4.0`).
- `--provider-rate-limit-capacity`: shared provider token bucket burst capacity (default `4`).
- `--store-provider-payloads|--no-store-provider-payloads`: opt-in/out for raw PSI/CrUX payload storage (default off).
- `--payload-retention-days`: retention window for stored provider payload JSON; older payloads are purged to `{}`.
- `--save-html`: reserved flag for future HTML snapshots.
- `--verbose`: debug logging.

Dashboard command flags:
- `--db`: path to audit SQLite database.
- `--queue-db`: path to queue SQLite database (defaults to sibling `queue.sqlite` when omitted).
- `--dashboard-worker-enabled|--dashboard-worker-disabled`: run embedded queue worker in dashboard process or rely on external workers.
- `--host`: bind host for dashboard server.
- `--port`: bind port for dashboard server.

Queue command flags:
- `enqueue --queue-db`: target queue database path.
- `enqueue --priority`: job scheduling priority (higher first).
- `enqueue --max-attempts`: maximum attempts before terminal failure.
- `enqueue --dedupe-key`: optional dedupe key to coalesce duplicate in-flight jobs.
- `worker --once`: process one job and exit.
- `worker --max-jobs`: process N jobs then exit.
- `worker --allow-concurrent-same-domain`: disable per-domain exclusivity admission guard.
- `worker --total-token-budget`: total admission token budget across active jobs.
- `jobs --state`: optional state filter for listing.

## Outputs
- `audit.sqlite`
- `pages.csv`, `links.csv`, `issues.csv`, `scores.csv`, `performance.csv`, `lighthouse.csv`, `crux.csv`, `sitemaps.csv`, `robots_rules.csv`, `run_events.csv`
- `page_graph_metrics.csv`
- `media_assets.csv`, `citation_events.csv`, `ai_visibility_events.csv`, `template_clusters.csv`, `submission_events.csv`
- `crawl_fetches.csv`, `page_snapshots.csv`, `page_diffs.csv`, `render_sessions.csv`, `schema_validations.csv`
- `offsite_commoncrawl_summary.csv`, `offsite_commoncrawl_linking_domains.csv`, `offsite_commoncrawl_comparisons.csv`, `offsite_commoncrawl_competitors.csv`
- `report.md`

## Interpreting outputs
- `pages` table stores observed page-level facts plus derived flags, including explicit robots-blocked representation for discovered-but-unfetched URLs.
- Canonical clustering metadata is persisted additively on pages (`canonical_cluster_key`, `canonical_cluster_role`, `canonical_signal_summary_json`) without rewriting fetched URL identity.
- Structured-data fields separate schema absence from parse failures (`schema_types_json` vs `schema_parse_error_count`).
- Snippet/citation control fields are persisted (`has_nosnippet_directive`, `max_*_directive`, `data_nosnippet_count`).
- `issues` table stores explainable findings with severity and issue code.
- Governance/preview-control issue coverage includes `OPENAI_SEARCHBOT_BLOCKED`, `GOOGLE_EXTENDED_BLOCKED`, `BING_PREVIEW_CONTROLS_RESTRICTIVE`, `OVER_RESTRICTIVE_SNIPPET_CONTROLS`, `RAW_RENDER_NOINDEX_MISMATCH`, and `RAW_RENDER_PREVIEW_CONTROL_MISMATCH`.
- AI visibility is split into potential + observed evidence (`ai_discoverability_potential_score`, `ai_visibility_json`) while keeping legacy compatibility aliases (`citation_eligibility_score`, `citation_evidence_json`).
- `ai_visibility_events` stores run-scoped AI visibility payload snapshots per URL.
- `scores` table stores deterministic heuristic subscores and overall score, plus additive explainability fields: `scoring_model_version`, `scoring_profile`, `score_explanation_json`.
- `page_graph_metrics` stores second-pass architecture analytics per URL (`internal_pagerank`, `betweenness`, `closeness`, `community_id`, `bridge_flag`).
- Legacy explainability aliases remain available for older tooling and rows: `score_version`, `score_profile`, `explanation_json`.
- `score_explanation_json` is structured data for dashboard detail rendering (dimension scores/deductions, risk family contributors, cap reasons, and composition metadata), including `preview_controls_score` and `structured_data_validity_score`.
- `score_explanation_json` keeps `structured_snippets_score` as a compatibility alias.
- `performance` rows contain parsed PSI metrics for successful requests.
- `lighthouse` rows include local lab statuses (`success`, `failed`, `skipped_*`) plus optional budget-failure evidence used by `LIGHTHOUSE_BUDGET_FAIL` issues.
- `crux` rows include explicit status semantics: `success`, `no_data`, `failed_http`, `skipped_missing_key`.
- Search Console provider telemetry carries explicit status semantics such as `success`, `success_partial`, `success_empty`, `skipped_missing_credentials`, `failed_invalid_credentials_path`, `failed_auth`, and `failed_api`.
- `runs.notes` includes provider status summaries, retry diagnostics, robots bypass acknowledgement, and crawl-delay application notes.
- `run_events` stores stage timing rows, periodic crawl heartbeats, governance summaries, and provider telemetry summaries for the run.
- Offsite Common Crawl rows are additive and run-scoped:
  - `offsite_commoncrawl_summary` stores release/mode/schedule/status/cache/target-rank metrics and timing.
  - `offsite_commoncrawl_comparisons` stores comparison-domain rank deltas.
  - `offsite_commoncrawl_competitors` is retained as a compatibility mirror for older tooling.
  - `offsite_commoncrawl_linking_domains` stores top linking domains ordered by harmonic centrality then PageRank.
- `report.md` separates issue interpretation into money-page priority vs utility/template backlog and includes performance grouped by template family.

## Search Console behavior and limits
- Search Console inspection is URL-sampled (`--gsc-url-limit`) and uses URL Inspection API responses for those targets only.
- Common Crawl offsite domain-graph intelligence is additive and does not claim exact page-level backlink proof in this milestone.
- `GSC_INDEX_STATE_NOT_INDEXED` issues are emitted per inspected URL when inspection explicitly reports non-indexed state.
- Failed inspection/auth paths are surfaced in run notes and provider telemetry; they are not interpreted as index-state proof.
- When Search Console is disabled or credentials are missing, reconciliation remains an unknown-state baseline rather than inferred deindexing.

## Runtime profiles and observability
- Runtime profiles select practical defaults while still allowing explicit flag overrides:
  - `exploratory`: small crawl, render off, low provider retries, frequent heartbeat.
  - `standard`: balanced crawl/render/provider defaults.
  - `deep`: broader crawl/render/provider depth with higher retry budget.
- Stage timing is persisted in `run_events` and summarized in `report.md`.
- Crawl heartbeat telemetry emits `pages_stored`, queue/error counts, and elapsed time.
- Incremental/discovery telemetry includes crawl retry counters (`fetch_retries_total`, `fetch_retry_wait_ms_total`, `fetch_retry_after_used`).
- Provider telemetry summarizes attempts, HTTP attempts, retries, cumulative wait, and timeout counts.

## Provider behavior and data handling
- PSI and CrUX key checks are enforced inside provider runtime functions; missing keys short-circuit with `skipped_missing_key` semantics and no outbound request.
- Provider request retries are bounded and use exponential backoff with jitter for retryable responses (`429`, selected `5xx`); `Retry-After` is honored by default.
- PSI uses a bounded worker pool and PSI/CrUX collection overlap in the same stage; a shared token-bucket limiter smooths request bursts, including retry bursts.
- Raw provider payload persistence is opt-in (`--store-provider-payloads`). Default behavior stores parsed fields only (`payload_json = {}`).
- Stored provider payload JSON is written as valid JSON and remains run-scoped through existing run-scoped export behavior.
- Retention control is applied at runtime start via `--payload-retention-days`: payload JSON older than the retention window is purged to `{}`.
- Raw payload storage may have usage/redistribution constraints; prefer leaving payload storage disabled outside internal debugging.

## Crawl and render behavior
- When robots are respected, crawl pacing enforces `crawl-delay` when present for the configured user-agent (or wildcard fallback) with precedence:
  - most specific matching user-agent rule
  - wildcard (`*`) rule
- Effective crawl delay is `max(request_delay, crawl-delay)`.
- In `google_exact` robots persona mode, `crawl-delay` is ignored by default and can be enabled explicitly via `--google-exact-apply-crawl-delay`.
- Internal-host policy is intentionally strict by default: root host + apex/www variants only; other subdomains are treated as external unless explicitly whitelisted by redirect discovery.
- URL normalization preserves explicit HTTP roots (important for HTTP-only legacy sites) and defaults schemeless URLs to HTTPS.
- Rendered fetches now use the configured crawler user-agent in Playwright context creation.

## Common failure modes
- Playwright not installed / browser binaries missing: render checks become unavailable and run notes include explicit render error text.
- Provider key missing: run notes include explicit `skipped_missing_key` status for PSI/CrUX.
- PSI unavailable/rate-limited/network-blocked: run notes include explicit `failed_http` diagnostics and retry metadata; `PERFORMANCE_PROVIDER_ERROR` is recorded.
- CrUX unavailable/network-blocked: run notes include explicit `failed_http` diagnostics and retry metadata; `CRUX_PROVIDER_ERROR` is recorded.
- Search Console credentials missing/invalid: provider telemetry reports `skipped_missing_credentials` or `failed_invalid_credentials_path`.
- Search Console auth/property failures: provider telemetry reports `failed_auth` with provider message details.
- Search Console API/network partial failures: provider telemetry may report `success_partial` with URL-level error samples.
- Outbound HTTP blocked by environment: fetch failures are recorded as `FETCH_FAILED` issues and run notes.
- Strict robots: crawl scope may be very small and crawl-delay can materially slow collection.
- Robots-blocked discovered URLs are persisted and reported via explicit issue codes instead of being silently dropped.
- Robots bypass requested without acknowledgement: command fails fast with `--ignore-robots requires --i-understand-robots-bypass`.
