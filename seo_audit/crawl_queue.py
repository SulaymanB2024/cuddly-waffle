from __future__ import annotations

from seo_audit.frontier import FrontierItem, PriorityFrontier


class ThreeQueueFrontier:
    def __init__(
        self,
        *,
        max_size: int,
        cluster_budget: int,
        high_weight: int = 3,
        normal_weight: int = 2,
    ) -> None:
        self.max_size = max(1, int(max_size))
        self._total = 0
        self._global_urls: set[str] = set()
        self._frontiers = {
            "high": PriorityFrontier(max_size=self.max_size, cluster_budget=cluster_budget),
            "normal": PriorityFrontier(max_size=self.max_size, cluster_budget=cluster_budget),
            "low": PriorityFrontier(max_size=self.max_size, cluster_budget=cluster_budget),
        }
        high = max(1, int(high_weight))
        normal = max(1, int(normal_weight))
        self._rotation = ["high"] * high + ["normal"] * normal + ["low"]
        self._cursor = 0

    def __len__(self) -> int:
        return self._total

    def has_url(self, url: str) -> bool:
        return url in self._global_urls

    def push(self, item: FrontierItem, *, band: str) -> bool:
        normalized_band = str(band or "normal").strip().lower()
        if normalized_band not in self._frontiers:
            normalized_band = "normal"

        if item.url in self._global_urls:
            return False
        if self._total >= self.max_size:
            return False

        frontier = self._frontiers[normalized_band]
        if not frontier.push(item):
            return False

        self._global_urls.add(item.url)
        self._total += 1
        return True

    def pop(self) -> FrontierItem | None:
        if self._total <= 0:
            return None

        for _ in range(max(1, len(self._rotation))):
            band = self._rotation[self._cursor]
            self._cursor = (self._cursor + 1) % len(self._rotation)
            item = self._frontiers[band].pop()
            if item is None:
                continue
            self._global_urls.discard(item.url)
            self._total = max(0, self._total - 1)
            return item

        for band in ("high", "normal", "low"):
            item = self._frontiers[band].pop()
            if item is None:
                continue
            self._global_urls.discard(item.url)
            self._total = max(0, self._total - 1)
            return item
        return None
