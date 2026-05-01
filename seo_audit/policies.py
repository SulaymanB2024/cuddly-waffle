from __future__ import annotations

"""Shared policy constants for issue generation, scoring, and reporting."""

# Internal linking policy threshold: fewer than this many internal outlinks is considered low.
LOW_INTERNAL_LINKS_THRESHOLD = 3

# Render gap threshold for emitting RENDER_GAP_HIGH issue.
HIGH_RENDER_GAP_THRESHOLD = 60

# Redirect chains at or above this number of hops are considered risky.
LONG_REDIRECT_CHAIN_THRESHOLD = 3

# Minimum query parameter count before faceted-url crawl risk is flagged.
FACET_QUERY_PARAM_THRESHOLD = 2

# Report thresholds.
LOW_LOCAL_SEO_SCORE_THRESHOLD = 40
LOW_PSI_PERFORMANCE_THRESHOLD = 50
