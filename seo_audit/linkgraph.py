from __future__ import annotations

from collections import defaultdict, deque
from urllib.parse import urlsplit, urlunsplit

from seo_audit.url_utils import normalize_url, same_registrable_domain


MAX_BETWEENNESS_NODES = 450
MAX_CLOSENESS_NODES = 1500


def _canonical_link_url(url: str, home_host: str) -> str:
    normalized = normalize_url(url, prefer_https=False)
    split = urlsplit(normalized)
    host = (split.hostname or "").lower()
    if not host or not same_registrable_domain(normalized, home_host):
        return normalized

    apex = home_host[4:] if home_host.startswith("www.") else home_host
    netloc = f"{apex}:{split.port}" if split.port else apex
    return urlunsplit((split.scheme, netloc, split.path or "/", split.query, ""))


def compute_link_metrics(home_url: str, pages: list[dict], links: list[dict]) -> dict[str, dict]:
    home_normalized = normalize_url(home_url, prefer_https=False)
    home_host = (urlsplit(home_normalized).hostname or "").lower()
    home_key = _canonical_link_url(home_normalized, home_host)

    out_map: dict[str, set[str]] = defaultdict(set)
    in_map: dict[str, set[str]] = defaultdict(set)
    for link in links:
        src = _canonical_link_url(link["source_url"], home_host)
        tgt = _canonical_link_url(link["normalized_target_url"], home_host)
        if not link["is_internal"]:
            continue
        out_map[src].add(tgt)
        in_map[tgt].add(src)

    depth = {home_key: 0}
    q = deque([home_key])
    while q:
        current = q.popleft()
        for nxt in out_map.get(current, set()):
            if nxt not in depth:
                depth[nxt] = depth[current] + 1
                q.append(nxt)

    metrics: dict[str, dict] = {}
    for page in pages:
        url = page["normalized_url"]
        key = _canonical_link_url(url, home_host)
        inlinks = len(in_map.get(key, set()))
        outlinks = len(out_map.get(key, set()))
        nav_linked = int(any(src == home_key for src in in_map.get(key, set())))
        orphan_risk = int(inlinks == 0 and key != home_key)
        metrics[url] = {
            "inlinks": inlinks,
            "outlinks": outlinks,
            "effective_internal_links_out": outlinks,
            "crawl_depth": depth.get(key),
            "nav_linked_flag": nav_linked,
            "orphan_risk_flag": orphan_risk,
        }
    return metrics


def _graph_maps(
    home_url: str,
    pages: list[dict],
    links: list[dict],
) -> tuple[dict[str, str], list[str], dict[str, set[str]], dict[str, set[str]], dict[str, set[str]]]:
    home_normalized = normalize_url(home_url, prefer_https=False)
    home_host = (urlsplit(home_normalized).hostname or "").lower()

    key_by_url: dict[str, str] = {}
    nodes: set[str] = set()
    for page in pages:
        url = str(page.get("normalized_url") or "")
        if not url:
            continue
        key = _canonical_link_url(url, home_host)
        key_by_url[url] = key
        nodes.add(key)

    out_map: dict[str, set[str]] = {node: set() for node in nodes}
    in_map: dict[str, set[str]] = {node: set() for node in nodes}

    for link in links:
        if not link.get("is_internal"):
            continue
        src = _canonical_link_url(str(link.get("source_url") or ""), home_host)
        tgt = _canonical_link_url(str(link.get("normalized_target_url") or ""), home_host)
        if src not in nodes or tgt not in nodes:
            continue
        out_map[src].add(tgt)
        in_map[tgt].add(src)

    undirected: dict[str, set[str]] = {node: set() for node in nodes}
    for src, targets in out_map.items():
        for tgt in targets:
            undirected[src].add(tgt)
            undirected[tgt].add(src)

    return key_by_url, sorted(nodes), out_map, in_map, undirected


def _compute_pagerank(nodes: list[str], out_map: dict[str, set[str]]) -> dict[str, float]:
    if not nodes:
        return {}

    damping = 0.85
    max_iters = 80
    tolerance = 1e-9

    node_count = len(nodes)
    inv_n = 1.0 / node_count
    scores = {node: inv_n for node in nodes}
    out_degree = {node: len(out_map.get(node, set())) for node in nodes}

    incoming: dict[str, set[str]] = {node: set() for node in nodes}
    for src, targets in out_map.items():
        for tgt in targets:
            incoming[tgt].add(src)

    for _ in range(max_iters):
        dangling_total = sum(scores[node] for node in nodes if out_degree[node] == 0)
        base = ((1.0 - damping) * inv_n) + (damping * dangling_total * inv_n)
        updated: dict[str, float] = {}
        delta = 0.0

        for node in nodes:
            inbound_share = 0.0
            for src in incoming[node]:
                degree = out_degree[src]
                if degree > 0:
                    inbound_share += scores[src] / degree
            next_score = base + (damping * inbound_share)
            updated[node] = next_score
            delta += abs(next_score - scores[node])

        scores = updated
        if delta <= tolerance:
            break

    total = sum(scores.values())
    if total <= 0.0:
        return {node: 0.0 for node in nodes}
    return {node: (score / total) for node, score in scores.items()}


def _compute_betweenness(nodes: list[str], out_map: dict[str, set[str]]) -> dict[str, float]:
    node_count = len(nodes)
    if node_count <= 2:
        return {node: 0.0 for node in nodes}
    if node_count > MAX_BETWEENNESS_NODES:
        return {node: 0.0 for node in nodes}

    scores = {node: 0.0 for node in nodes}

    for source in nodes:
        stack: list[str] = []
        predecessors: dict[str, list[str]] = {node: [] for node in nodes}
        sigma = {node: 0.0 for node in nodes}
        sigma[source] = 1.0
        distance = {node: -1 for node in nodes}
        distance[source] = 0

        queue: deque[str] = deque([source])
        while queue:
            node = queue.popleft()
            stack.append(node)
            for neighbor in out_map.get(node, set()):
                if distance[neighbor] < 0:
                    queue.append(neighbor)
                    distance[neighbor] = distance[node] + 1
                if distance[neighbor] == distance[node] + 1:
                    sigma[neighbor] += sigma[node]
                    predecessors[neighbor].append(node)

        dependency = {node: 0.0 for node in nodes}
        while stack:
            node = stack.pop()
            sigma_node = sigma[node]
            if sigma_node > 0.0:
                for predecessor in predecessors[node]:
                    dependency[predecessor] += (sigma[predecessor] / sigma_node) * (1.0 + dependency[node])
            if node != source:
                scores[node] += dependency[node]

    normalization = float((node_count - 1) * (node_count - 2))
    if normalization <= 0.0:
        return scores
    return {node: (value / normalization) for node, value in scores.items()}


def _compute_closeness(nodes: list[str], out_map: dict[str, set[str]]) -> dict[str, float]:
    node_count = len(nodes)
    if node_count <= 1:
        return {node: 0.0 for node in nodes}
    if node_count > MAX_CLOSENESS_NODES:
        return {node: 0.0 for node in nodes}

    scores: dict[str, float] = {}
    for source in nodes:
        distance = {source: 0}
        queue: deque[str] = deque([source])

        while queue:
            node = queue.popleft()
            for neighbor in out_map.get(node, set()):
                if neighbor in distance:
                    continue
                distance[neighbor] = distance[node] + 1
                queue.append(neighbor)

        reachable = len(distance) - 1
        total_distance = sum(distance.values())
        if reachable <= 0 or total_distance <= 0:
            scores[source] = 0.0
            continue
        wf = reachable / float(node_count - 1)
        scores[source] = wf * (reachable / float(total_distance))

    return scores


def _community_ids(nodes: list[str], undirected: dict[str, set[str]]) -> dict[str, int]:
    visited: set[str] = set()
    communities: list[set[str]] = []

    for node in nodes:
        if node in visited:
            continue
        component: set[str] = set()
        queue: deque[str] = deque([node])
        visited.add(node)
        while queue:
            current = queue.popleft()
            component.add(current)
            for neighbor in undirected.get(current, set()):
                if neighbor in visited:
                    continue
                visited.add(neighbor)
                queue.append(neighbor)
        communities.append(component)

    communities.sort(key=lambda component: (-len(component), min(component)))
    community_id_by_node: dict[str, int] = {}
    for idx, component in enumerate(communities, start=1):
        for node in component:
            community_id_by_node[node] = idx
    return community_id_by_node


def _articulation_points(nodes: list[str], undirected: dict[str, set[str]]) -> set[str]:
    if not nodes:
        return set()

    visited: set[str] = set()
    discovered: dict[str, int] = {}
    low_link: dict[str, int] = {}
    parent: dict[str, str | None] = {}
    articulation: set[str] = set()
    time_index = 0

    def dfs(node: str) -> None:
        nonlocal time_index
        visited.add(node)
        discovered[node] = time_index
        low_link[node] = time_index
        time_index += 1
        children = 0

        for neighbor in sorted(undirected.get(node, set())):
            if neighbor not in visited:
                parent[neighbor] = node
                children += 1
                dfs(neighbor)
                low_link[node] = min(low_link[node], low_link[neighbor])

                if parent.get(node) is None and children > 1:
                    articulation.add(node)
                if parent.get(node) is not None and low_link[neighbor] >= discovered[node]:
                    articulation.add(node)
            elif neighbor != parent.get(node):
                low_link[node] = min(low_link[node], discovered[neighbor])

    for node in nodes:
        if node in visited:
            continue
        parent[node] = None
        dfs(node)

    return articulation


def compute_graph_metrics(home_url: str, pages: list[dict], links: list[dict]) -> dict[str, dict]:
    key_by_url, nodes, out_map, _in_map, undirected = _graph_maps(home_url, pages, links)
    if not nodes:
        return {}

    pagerank = _compute_pagerank(nodes, out_map)
    betweenness = _compute_betweenness(nodes, out_map)
    closeness = _compute_closeness(nodes, out_map)
    community_id_by_node = _community_ids(nodes, undirected)
    articulation = _articulation_points(nodes, undirected)

    metrics: dict[str, dict] = {}
    for url, node_key in key_by_url.items():
        metrics[url] = {
            "internal_pagerank": float(pagerank.get(node_key, 0.0)),
            "betweenness": float(betweenness.get(node_key, 0.0)),
            "closeness": float(closeness.get(node_key, 0.0)),
            "community_id": int(community_id_by_node.get(node_key, 0)),
            "bridge_flag": int(node_key in articulation),
        }
    return metrics
