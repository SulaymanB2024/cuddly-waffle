from seo_audit.linkgraph import compute_graph_metrics, compute_link_metrics


def test_linkgraph_normalizes_apex_www_edges() -> None:
    pages = [
        {"normalized_url": "https://example.com/"},
        {"normalized_url": "https://example.com/about"},
        {"normalized_url": "https://example.com/contact"},
    ]
    links = [
        {
            "source_url": "https://example.com/",
            "normalized_target_url": "https://www.example.com/about",
            "is_internal": 1,
        },
        {
            "source_url": "https://www.example.com/about",
            "normalized_target_url": "https://example.com/contact",
            "is_internal": 1,
        },
    ]

    metrics = compute_link_metrics("https://example.com/", pages, links)

    assert metrics["https://example.com/about"]["crawl_depth"] == 1
    assert metrics["https://example.com/contact"]["crawl_depth"] == 2
    assert metrics["https://example.com/about"]["orphan_risk_flag"] == 0
    assert metrics["https://example.com/contact"]["orphan_risk_flag"] == 0
    assert metrics["https://example.com/about"]["nav_linked_flag"] == 1
    assert metrics["https://example.com/about"]["effective_internal_links_out"] == 1


def test_linkgraph_marks_unreachable_as_orphan() -> None:
    pages = [
        {"normalized_url": "https://example.com/"},
        {"normalized_url": "https://example.com/orphan"},
    ]

    metrics = compute_link_metrics("https://example.com/", pages, links=[])

    assert metrics["https://example.com/"]["crawl_depth"] == 0
    assert metrics["https://example.com/"]["orphan_risk_flag"] == 0
    assert metrics["https://example.com/orphan"]["crawl_depth"] is None
    assert metrics["https://example.com/orphan"]["orphan_risk_flag"] == 1


def test_graph_metrics_identify_bridge_and_communities() -> None:
    pages = [
        {"normalized_url": "https://example.com/"},
        {"normalized_url": "https://example.com/hub"},
        {"normalized_url": "https://example.com/service"},
        {"normalized_url": "https://example.com/blog"},
        {"normalized_url": "https://example.com/blog/article"},
        {"normalized_url": "https://example.com/isolated"},
        {"normalized_url": "https://example.com/isolated/child"},
    ]
    links = [
        {
            "source_url": "https://example.com/",
            "normalized_target_url": "https://example.com/hub",
            "is_internal": 1,
        },
        {
            "source_url": "https://example.com/hub",
            "normalized_target_url": "https://example.com/service",
            "is_internal": 1,
        },
        {
            "source_url": "https://example.com/service",
            "normalized_target_url": "https://example.com/hub",
            "is_internal": 1,
        },
        {
            "source_url": "https://example.com/hub",
            "normalized_target_url": "https://example.com/blog",
            "is_internal": 1,
        },
        {
            "source_url": "https://example.com/blog",
            "normalized_target_url": "https://example.com/blog/article",
            "is_internal": 1,
        },
        {
            "source_url": "https://example.com/blog/article",
            "normalized_target_url": "https://example.com/hub",
            "is_internal": 1,
        },
        {
            "source_url": "https://example.com/isolated",
            "normalized_target_url": "https://example.com/isolated/child",
            "is_internal": 1,
        },
        {
            "source_url": "https://example.com/isolated/child",
            "normalized_target_url": "https://example.com/isolated",
            "is_internal": 1,
        },
    ]

    metrics = compute_graph_metrics("https://example.com/", pages, links)

    hub = metrics["https://example.com/hub"]
    service = metrics["https://example.com/service"]
    blog = metrics["https://example.com/blog"]
    home = metrics["https://example.com/"]
    isolated = metrics["https://example.com/isolated"]

    assert hub["bridge_flag"] == 1
    assert service["bridge_flag"] == 0
    assert hub["internal_pagerank"] > home["internal_pagerank"]
    assert hub["betweenness"] > service["betweenness"]
    assert hub["betweenness"] > blog["betweenness"]
    assert hub["closeness"] > service["closeness"]
    assert hub["community_id"] != isolated["community_id"]


def test_graph_metrics_normalize_apex_www_edges() -> None:
    pages = [
        {"normalized_url": "https://example.com/"},
        {"normalized_url": "https://example.com/pricing"},
    ]
    links = [
        {
            "source_url": "https://www.example.com/",
            "normalized_target_url": "https://example.com/pricing",
            "is_internal": 1,
        },
        {
            "source_url": "https://example.com/pricing",
            "normalized_target_url": "https://www.example.com/",
            "is_internal": 1,
        },
    ]

    metrics = compute_graph_metrics("https://example.com/", pages, links)

    assert metrics["https://example.com/"]["community_id"] > 0
    assert metrics["https://example.com/pricing"]["community_id"] > 0
    assert metrics["https://example.com/"]["community_id"] == metrics["https://example.com/pricing"]["community_id"]
