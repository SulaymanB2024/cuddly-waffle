from __future__ import annotations

# Backward-compatible facade: legacy callers import from this module,
# while implementation is now split into focused GSC modules.

from seo_audit.gsc_analytics import (  # noqa: F401
    collect_search_analytics,
    default_date_window,
    summarize_search_analytics,
)
from seo_audit.gsc_inspection import (  # noqa: F401
    collect_index_states,
    format_reconciliation_evidence,
    property_candidates,
    reconcile_index_states,
    resolve_property,
)

__all__ = [
    "property_candidates",
    "resolve_property",
    "collect_index_states",
    "reconcile_index_states",
    "format_reconciliation_evidence",
    "collect_search_analytics",
    "default_date_window",
    "summarize_search_analytics",
]
