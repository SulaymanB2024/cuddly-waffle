from __future__ import annotations


def _type_set(nodes: list[dict]) -> set[str]:
    output: set[str] = set()
    for node in nodes:
        raw_type = node.get("@type")
        if isinstance(raw_type, str):
            token = raw_type.strip().lower()
            if token:
                output.add(token)
        elif isinstance(raw_type, list):
            for item in raw_type:
                token = str(item).strip().lower()
                if token:
                    output.add(token)
    return output


def compare_schema_sets(raw_nodes: list[dict], rendered_nodes: list[dict]) -> dict[str, object]:
    raw_types = _type_set(raw_nodes)
    rendered_types = _type_set(rendered_nodes)

    added = sorted(rendered_types - raw_types)
    removed = sorted(raw_types - rendered_types)
    shared = sorted(raw_types.intersection(rendered_types))

    severity = "none"
    if added or removed:
        severity = "medium"
    if len(added) + len(removed) >= 4:
        severity = "high"

    return {
        "raw_type_count": len(raw_types),
        "rendered_type_count": len(rendered_types),
        "added_types": added,
        "removed_types": removed,
        "shared_types": shared,
        "severity": severity,
    }
