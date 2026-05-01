from __future__ import annotations

import json
from collections.abc import Iterable


_LD_JSON_XPATH = (
    ".//script[translate(normalize-space(@type), "
    "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')='application/ld+json']"
)


def _iter_nodes(value: object) -> Iterable[dict]:
    if isinstance(value, dict):
        yield value
        graph = value.get("@graph")
        if isinstance(graph, list):
            for item in graph:
                if isinstance(item, dict):
                    yield from _iter_nodes(item)
        for nested in value.values():
            if isinstance(nested, dict):
                yield from _iter_nodes(nested)
            elif isinstance(nested, list):
                for item in nested:
                    if isinstance(item, dict):
                        yield from _iter_nodes(item)
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                yield from _iter_nodes(item)


def parse_schema_graph_nodes(tree) -> tuple[list[dict], int]:
    nodes: list[dict] = []
    parse_errors = 0

    seen: set[str] = set()
    for script in tree.xpath(_LD_JSON_XPATH):
        raw = (script.text or "").strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except Exception:
            parse_errors += 1
            continue

        for node in _iter_nodes(parsed):
            if not isinstance(node, dict):
                continue
            key = json.dumps(node, sort_keys=True)
            if key in seen:
                continue
            seen.add(key)
            nodes.append(node)

    return nodes, parse_errors
