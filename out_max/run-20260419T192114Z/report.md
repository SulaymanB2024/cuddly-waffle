# SEO Audit Report: https://void-agency.com/

## Audit overview
- Run ID: `8946f235-727a-4b28-b3d1-cb0b887325a9`
- Status: `completed`
- Started: `2026-04-19T19:21:14.807593+00:00`
- Completed: `2026-04-19T19:30:33.431001+00:00`
- Run profile: `deep`
- Site type: `general`
- Crawl persona: `googlebot_smartphone`

## Crawl stats
- Pages stored: **58**
- Issues: **355**
- Scores: **58**
- Performance rows: **0**
- Lighthouse rows: **0**
- CrUX rows: **20**

## Crawl planning
- Planned crawl URLs: 28
- Seed URLs considered: 28
- Known sitemap URLs considered: 25
- Prioritized previously changed URLs: 0

## Incremental crawl counters
- Discovered: 58
- Fetched: 58
- Crawl retries used: 0
- Crawl retry wait (ms): 0
- Crawl retries using Retry-After: 0
- Reused from cache: 0
- Not modified (304): 0
- Reparsed (unchanged but invalidated): 0
- Rerendered: 55

## Changed vs unchanged pages
- Changed pages: 55
- Unchanged pages: 3
- Total page diff rows: 0
- No persisted page diffs for this run.

## Structured data audit dimensions
- Pages with syntactically valid structured data: 58
- Pages with eligible Google feature markup: 0
- Pages with deprecated markup present: 0
- Pages with visible-content mismatches: 0

## Stage timing summary
- robots: 0.27s
- sitemaps: 0.25s
- sitemap_status: 0.00s
- plan_crawl: 0.00s
- crawl: 141.16s
- classify: 0.01s
- render_diff: 115.49s
- update_pages: 0.08s
- issues_scores_performance: 301.26s
- export_csv: 0.07s

## Crawl heartbeat summary
- Heartbeat events recorded: 1
- Last heartbeat: pages=50 queue=7 errors=0 elapsed_s=126.69

## Discovery blind spots
- Enqueued URLs total: 58
- Enqueued via raw links: 0
- Enqueued via rendered links: 30
- Queue dedupe skips: 481
- Scope-filtered drops: 0
- Render frontier checks: 58
- Render frontier successes: 58
- Render frontier failures: 0
- Discovery provenance mix: render_link=30, seed=28
- Discovery blind-spot issues: 0

## Sitemap intelligence
- Sitemap URLs: 25
- Discovered pages: 56
- Sitemap URLs not crawled: 0
- Crawled URLs not in sitemap: 31
- Sitemap hreflang mismatches: 0
- Stale lastmod URLs: 0
- Missing lastmod URLs: 0
- Sitemap scope violations: 0

## Provider telemetry
- sitemap_status: attempts=0 http_attempts=0 retries=0 wait_s=0.00 timeouts=0 success=0 no_data=0 failed_http=0 skipped_missing_key=0
- psi: attempts=40 http_attempts=88 retries=48 wait_s=35.16 timeouts=0 success=0 no_data=4 failed_http=36 skipped_missing_key=0
- crux: attempts=40 http_attempts=40 retries=0 wait_s=0.00 timeouts=0 success=0 no_data=40 failed_http=0 skipped_missing_key=0
- gsc: attempts=0 http_attempts=0 retries=0 wait_s=0.00 timeouts=0 success=0 no_data=0 failed_http=0 skipped_missing_key=0

## Search Console reconciliation
- Status: skipped_disabled
- Property: 
- Crawled URLs in reconciliation set: 55
- Indexed: 0
- Not indexed: 0
- Unknown: 55
- URL Inspection states are sampled snapshots for the inspected URLs in this run, not full-site guarantees.

## Offsite visibility (Common Crawl)
- Offsite Common Crawl lane not captured for this run.

## Governance and answer-layer controls
- Actionable pages governance-audited: 55
- Googlebot blocked pages: 0
- Bingbot blocked pages: 0
- OAI-SearchBot blocked pages: 0
- Google-Extended blocked pages: 0
- GPTBot blocked pages: 0
- OAI-AdsBot blocked pages: 0
- ChatGPT-User blocked pages (informational): 0
- OPENAI_SEARCHBOT_BLOCKED: 0 (sample: none)
- GOOGLE_EXTENDED_BLOCKED: 0 (sample: none)
- GPTBOT_BLOCKED: 0 (sample: none)
- OAI_ADSBOT_BLOCKED: 0 (sample: none)
- BING_PREVIEW_CONTROLS_RESTRICTIVE: 22 (sample: https://void-agency.com/, https://www.void-agency.com/, https://void-agency.com/auto-body, https://void-agency.com/dentists)
- OVER_RESTRICTIVE_SNIPPET_CONTROLS: 0 (sample: none)
- RAW_RENDER_NOINDEX_MISMATCH: 0 (sample: none)
- RAW_RENDER_PREVIEW_CONTROL_MISMATCH: 0 (sample: none)

## Page type counts
- article: 29
- homepage: 2
- legal: 4
- other: 23

## Status-code summary
- 200: 56
- 404: 2

## URL policy coverage
- crawl_normally: 58

## Top issues by severity (all tracks)
- high: 5
- medium: 186
- low: 164

## Issue gate coverage
- access: 2
- canonicalization: 109
- discovery: 1
- indexability: 80
- rendering: 162
- serving: 1

## Issue verification confidence
- Verification status counts: automated=355
- Certainty state counts: Probable=162, Unverified=2, Verified=191
- Confidence bands: high=354, medium=1
- Reach distribution: single_page=4, sitewide=348, template_cluster=3
- Average priority score: 19.24
- Lowest-confidence issues queue:
  - PERFORMANCE_PROVIDER_ERROR @ https://void-agency.com/ (confidence=75, status=automated)
  - RAW_ONLY_MISSING_H1 @ https://void-agency.com/ (confidence=85, status=automated)
  - RAW_ONLY_THIN_CONTENT @ https://void-agency.com/ (confidence=85, status=automated)
  - RAW_ONLY_LOW_INTERNAL_LINKS @ https://void-agency.com/ (confidence=85, status=automated)
  - RAW_ONLY_MISSING_H1 @ https://www.void-agency.com/ (confidence=85, status=automated)
  - RAW_ONLY_THIN_CONTENT @ https://www.void-agency.com/ (confidence=85, status=automated)
  - RAW_ONLY_LOW_INTERNAL_LINKS @ https://www.void-agency.com/ (confidence=85, status=automated)
  - RAW_ONLY_MISSING_H1 @ https://void-agency.com/auto-body (confidence=85, status=automated)
  - RAW_ONLY_THIN_CONTENT @ https://void-agency.com/auto-body (confidence=85, status=automated)
  - RAW_ONLY_LOW_INTERNAL_LINKS @ https://void-agency.com/auto-body (confidence=85, status=automated)
- Highest-priority issues queue:
  - CANONICAL_MISMATCH @ https://void-agency.com/ (priority=63, certainty=Verified, reach=sitewide)
  - CANONICAL_MISMATCH @ https://www.void-agency.com/ (priority=63, certainty=Verified, reach=sitewide)
  - HREFLANG_RECIPROCITY_MISSING @ https://www.void-agency.com/ (priority=63, certainty=Verified, reach=sitewide)
  - BING_PREVIEW_CONTROLS_RESTRICTIVE @ https://void-agency.com/ (priority=43, certainty=Verified, reach=sitewide)
  - EXACT_CONTENT_DUPLICATE @ https://void-agency.com/ (priority=43, certainty=Verified, reach=sitewide)
  - BING_PREVIEW_CONTROLS_RESTRICTIVE @ https://www.void-agency.com/ (priority=43, certainty=Verified, reach=sitewide)
  - EXACT_CONTENT_DUPLICATE @ https://www.void-agency.com/ (priority=43, certainty=Verified, reach=sitewide)
  - REDIRECT_TO_ERROR @ https://void-agency.com/sitemap.xml.gz (priority=38, certainty=Verified, reach=single_page)
  - REDIRECT_TO_ERROR @ https://void-agency.com/sitemap_index.xml (priority=38, certainty=Verified, reach=single_page)
  - BING_PREVIEW_CONTROLS_RESTRICTIVE @ https://void-agency.com/auto-body (priority=31, certainty=Verified, reach=sitewide)

## Score model diagnostics
- Average quality score: 87.84
- Average risk score: 38.98
- Average measurement coverage: 89.28
- Performance unknown pages (score=-1): 58

## Discovery and access checks
- ROBOTS_BLOCKED_URL: 0
- SITEMAP_URL_BLOCKED_BY_ROBOTS: 0
- ROBOTS_NOINDEX_CONFLICT: 0
- SITEMAP_URL_NOT_CRAWLED: 0
- CRAWLED_URL_NOT_IN_SITEMAP: 1
- DISCOVERY_BLIND_SPOT: 0
- REDIRECT_CHAIN_LONG: 0
- REDIRECT_TO_ERROR: 2
- ACCESS_AUTH_BLOCKED: 0

## Canonicalization and indexability checks
- DUPLICATE_CANONICAL_TAGS: 0
- HREFLANG_RECIPROCITY_MISSING: 54
- PAGINATION_SIGNAL_MISSING: 0
- FACETED_NAVIGATION_RISK: 0
- CANONICAL_MISMATCH: 55
- MISSING_CANONICAL: 0
- NOINDEX: 0
- STRUCTURED_DATA_PARSE_FAILED: 0
- EXACT_CONTENT_DUPLICATE: 55
- Canonical clusters (actionable pages): 1
- Canonical clusters with multiple members: 1
- Pages declaring canonical aliases: 55
- Note: noindex is page-level and may remain unknown when robots.txt blocks crawling before directives are observed.

## Prioritization tracks
- Money pages (priority queue): pages=2 issues=15 (critical=0 high=3 medium=4 low=8)
- Utility/taxonomy/template pages (hygiene backlog): pages=0 issues=0 (critical=0 high=0 medium=0 low=0)
- Other content pages: pages=53 issues=340 (critical=0 high=2 medium=182 low=156)
- Run-level/global issues: issues=0 (critical=0 high=0 medium=0 low=0)

## Top issue codes by track
- money: CANONICAL_MISMATCH=2, BING_PREVIEW_CONTROLS_RESTRICTIVE=2, EXACT_CONTENT_DUPLICATE=2, RAW_ONLY_MISSING_H1=2, RAW_ONLY_THIN_CONTENT=2
- utility/taxonomy/template: none
- other content: EXACT_CONTENT_DUPLICATE=53, CANONICAL_MISMATCH=53, HREFLANG_RECIPROCITY_MISSING=53, RAW_ONLY_MISSING_H1=53, RAW_ONLY_LOW_INTERNAL_LINKS=53
- run-level/global: none

## Top pages by render risk
- https://void-agency.com/ (gap=50, reason=title mismatch; h1 mismatch; rendered links much richer)
- https://void-agency.com/auto-body (gap=50, reason=title mismatch; h1 mismatch; rendered links much richer)
- https://void-agency.com/dentists (gap=50, reason=title mismatch; h1 mismatch; rendered links much richer)
- https://www.void-agency.com/smbs (gap=50, reason=title mismatch; h1 mismatch; rendered links much richer)
- https://www.void-agency.com/insights (gap=50, reason=title mismatch; h1 mismatch; rendered links much richer)
- https://void-agency.com/foundation-repair (gap=50, reason=title mismatch; h1 mismatch; rendered links much richer)
- https://void-agency.com/hvac (gap=50, reason=title mismatch; h1 mismatch; rendered links much richer)
- https://void-agency.com/insights (gap=50, reason=title mismatch; h1 mismatch; rendered links much richer)
- https://www.void-agency.com/plumbers (gap=50, reason=title mismatch; h1 mismatch; rendered links much richer)
- https://www.void-agency.com/dentists (gap=50, reason=title mismatch; h1 mismatch; rendered links much richer)

## Duplicate title/description findings
- Duplicate titles: 55
- Duplicate descriptions: 55
- Exact duplicate content clusters: 1
- Pages in exact duplicate clusters: 55

## Internal linking findings
- Orphan risk pages: 3
- Low internal outlink pages: 3

## Render/shell telemetry
- Likely JS-shell pages: 0
- Pages rendered: 55
- Render failures: 0

## Snippet and citation controls
- Pages with nosnippet directives (meta or X-Robots-Tag): 0
- Elements marked data-nosnippet: 0
- Pages with max-snippet directive: 55
- Pages with max-image-preview directive: 55
- Pages with max-video-preview directive: 55
- Pages with restrictive snippet controls: 55
- Restrictive-control sample pages: https://void-agency.com/, https://void-agency.com/auto-body, https://void-agency.com/dentists, https://www.void-agency.com/smbs, https://www.void-agency.com/insights
- Pages with heavy data-nosnippet usage (>=4 elements): 0

## AI discoverability potential vs evidence
- Average AI discoverability potential score: 83.3
- Pages with observed visibility signals (GSC/chatgpt referrals): 0
- Evidence adapters observed: none

## Local SEO findings
- Pages with low local SEO completeness (<40): 0

## Performance findings
- Field performance should be read from CrUX rows first; PSI is primarily lab data and may have limited field snapshots.
- PSI rows with performance score < 50: 0
- Lighthouse rows with budget failures: 0
- Lighthouse failed/skipped rows: 0

## Performance by template group
- home: rows=0
- product: rows=0
- service: rows=0
- blog: rows=0
- taxonomy: rows=0
- other: rows=0

## CrUX findings
- CrUX rows represent origin/URL field aggregates from Chrome UX Report availability.
- no_data: 20

## Run notes
- offsite commoncrawl: disabled; run profile: deep; site type: general; scoring profile: general; crawl persona: id=googlebot_smartphone robots_token=Googlebot meta_scope=googlebot robots_mode=google_exact; crawl retries: retries=1 base_backoff=0.25s max_backoff=4.00s max_total_wait=12.00s respect_retry_after=1; crawl user-agent: persona default; internal host policy: strict mode=all_subdomains hosts=void-agency.com, www.void-agency.com; crawl discovery policy: mode=browser_first render_frontier=1; crawl workers: workers=1 queue_weights=high:3,normal:2,low:1; fresh output dir: out_max/run-20260419T192114Z; runtime versions: extractor=2.0.0 schema_rules=1.0.0 scoring=1.1.0; cache invalidation: extractor_version_changed=0 schema_rule_version_changed=0 scoring_version_changed=0; robots fetch: state=redirect_scope_mismatch bucket=2xx status=200 hops=1; sitemap status: status=skipped_disabled rows=0; crawl planner: discovered=28 seed_urls=28 known_sitemap_urls=25 prioritized_changed_urls=0; crawl policy coverage: crawl_normally=58; crawl discovery telemetry: dedupe_skipped=481, enqueued_total=58, enqueued_via_render_link=30, render_frontier_checks=58, render_frontier_failures=0, render_frontier_successes=58, scope_skipped=0; crawl incremental: discovered=58 fetched=58 reused_from_cache=0 not_modified=0 reparsed=0 rerendered=58; governance summary: googlebot_blocked=0 bingbot_blocked=0 oai_searchbot_blocked=0 google_extended_blocked=0 gptbot_blocked=0 oai_adsbot_blocked=0; canonical clusters: clusters=4 multi_member=1 alias_pages=55; sitemap intelligence: sitemap_urls=25 discovered_pages=56 not_crawled=0 scope_violations=0; provider targets: https://void-agency.com/ | https://www.void-agency.com/ | https://void-agency.com/insights | https://www.void-agency.com/insights | https://www.void-agency.com/insights?topic=SMB; provider scheduler: psi_workers=4 rate_limit=4.00/s burst=4; psi status: success=0 no_data=4 failed_http=36 skipped_missing_key=0; psi no_data: no_data: https://void-agency.com/ [mobile] missing lighthouse categories (retries=1, waited=0.61s) | no_data: https://void-agency.com/ [desktop] missing lighthouse categories (retries=3, waited=3.83s) | no_data: https://www.void-agency.com/ [mobile] missing lighthouse categories (retries=0, waited=0.00s) | no_data: https://www.void-agency.com/ [desktop] missing lighthouse categories (retries=2, waited=1.58s); psi errors: failed_http: https://void-agency.com/insights [mobile] Lighthouse returned error: NO_FCP. The page did not paint any content. Please ensure you keep the browser window in the foreground during the load and try again. (NO_FCP) (retries=1, waited=0.56s) | failed_http: https://void-agency.com/insights [desktop] Lighthouse returned error: NO_FCP. The page did not paint any content. Please ensure you keep the browser window in the foreground during the load and try again. (NO_FCP) (retries=0, waited=0.00s) | failed_http: https://www.void-agency.com/insights [mobile] Lighthouse returned error: NO_FCP. The page did not paint any content. Please ensure you keep the browser window in the foreground during the load and try again. (NO_FCP) (retries=2, waited=1.75s) | failed_http: https://www.void-agency.com/insights [desktop] Lighthouse returned error: NO_FCP. The page did not paint any content. Please ensure you keep the browser window in the foreground during the load and try again. (NO_FCP) (retries=2, waited=1.58s) | failed_http: https://www.void-agency.com/insights?topic=SMB [mobile] Lighthouse returned error: NO_FCP. The page did not paint any content. Please ensure you keep the browser window in the foreground during the load and try again. (NO_FCP) (retries=2, waited=1.56s); crux status: success=0 no_data=20 failed_http=0 skipped_missing_key=0; lighthouse skipped: disabled; psi telemetry: attempts=40 http_attempts=88 retries=48 wait_s=35.16 timeouts=0; crux telemetry: attempts=40 http_attempts=40 retries=0 wait_s=0.00 timeouts=0; gsc skipped: disabled; stage timing: robots=0.27s, sitemaps=0.25s, sitemap_status=0.00s, plan_crawl=0.00s, crawl=141.16s, classify=0.01s, render_diff=115.49s, update_pages=0.08s, issues_scores_performance=301.26s, export_csv=0.07s

## Top-priority actions
- Fix money-page critical/high issues first (fetch failures, noindex, missing titles).
- Treat utility/taxonomy/template findings as a separate hygiene backlog.
- Improve internal linking to reduce orphan risk on priority pages.
- Review render gap pages for client-side SEO risk.

## Limitations
- Public-data-only analysis; Search Console reconciliation depends on configured property access and sampled URL inspection targets.
- Render and performance collection may be partial when dependencies/APIs are unavailable.