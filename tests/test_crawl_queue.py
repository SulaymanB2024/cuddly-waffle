from seo_audit.crawl_queue import ThreeQueueFrontier
from seo_audit.frontier import FrontierItem


def _item(url: str, priority: float = 50.0) -> FrontierItem:
    return FrontierItem(
        url=url,
        depth=1,
        priority=priority,
        discovered_via="raw_link",
        source_url="https://example.com/",
        cluster_key="cluster",
    )


def test_three_queue_frontier_prefers_high_then_normal_then_low() -> None:
    queue = ThreeQueueFrontier(max_size=10, cluster_budget=3, high_weight=2, normal_weight=1)

    assert queue.push(_item("https://example.com/low-1", 10), band="low")
    assert queue.push(_item("https://example.com/high-1", 50), band="high")
    assert queue.push(_item("https://example.com/high-2", 45), band="high")
    assert queue.push(_item("https://example.com/normal-1", 40), band="normal")

    first = queue.pop()
    second = queue.pop()
    third = queue.pop()
    fourth = queue.pop()

    assert first is not None and "high-1" in first.url
    assert second is not None and "high-2" in second.url
    assert third is not None and "normal-1" in third.url
    assert fourth is not None and "low-1" in fourth.url


def test_three_queue_frontier_respects_global_size_and_dedupe() -> None:
    queue = ThreeQueueFrontier(max_size=2, cluster_budget=3)

    first = _item("https://example.com/a", 20)
    second = _item("https://example.com/b", 10)
    duplicate = _item("https://example.com/a", 99)
    overflow = _item("https://example.com/c", 80)

    assert queue.push(first, band="high") is True
    assert queue.push(second, band="normal") is True
    assert queue.push(duplicate, band="high") is False
    assert queue.push(overflow, band="high") is False
    assert len(queue) == 2
