from seo_audit.frontier import (
    FrontierItem,
    FrontierSignals,
    PriorityFrontier,
    cluster_key_for_param_url,
    compute_frontier_priority,
    signals_for_url,
)


def test_frontier_priority_penalizes_depth_and_query_risk() -> None:
    shallow = compute_frontier_priority(
        FrontierSignals(
            depth=1,
            template_importance=0.9,
            internal_prominence=0.8,
            query_param_risk=0.0,
        )
    )
    deep_and_risky = compute_frontier_priority(
        FrontierSignals(
            depth=5,
            template_importance=0.9,
            internal_prominence=0.8,
            query_param_risk=0.85,
        )
    )

    assert shallow > deep_and_risky


def test_priority_frontier_respects_cluster_budget() -> None:
    risky_keys = {"color", "size"}
    frontier = PriorityFrontier(max_size=10, cluster_budget=1)

    cluster_key = cluster_key_for_param_url("https://example.com/products?color=red", risky_keys)
    first = FrontierItem(
        url="https://example.com/products?color=red",
        depth=1,
        priority=90.0,
        discovered_via="raw_link",
        source_url="https://example.com/",
        cluster_key=cluster_key,
    )
    second = FrontierItem(
        url="https://example.com/products?color=blue",
        depth=1,
        priority=91.0,
        discovered_via="raw_link",
        source_url="https://example.com/",
        cluster_key=cluster_key,
    )

    assert frontier.push(first) is True
    assert frontier.push(second) is False
    assert len(frontier) == 1


def test_signals_for_homepage_seed_url() -> None:
    signals = signals_for_url(
        url="https://example.com/",
        depth=0,
        discovered_via="seed",
        policy_class="crawl_normally",
        risky_query_keys=set(),
    )

    assert signals.template_importance == 1.0
    assert signals.internal_prominence == 1.0
    assert signals.query_param_risk == 0.0
