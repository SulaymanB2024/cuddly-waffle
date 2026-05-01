from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Callable
from urllib.parse import urlsplit


@dataclass(slots=True)
class TokenBucket:
    capacity: float
    refill_rate: float
    tokens: float
    updated_at: float

    @classmethod
    def create(
        cls,
        capacity: int,
        refill_rate: float,
        *,
        now: float | None = None,
    ) -> "TokenBucket":
        now_value = time.monotonic() if now is None else float(now)
        cap = float(max(1, int(capacity)))
        rate = max(0.001, float(refill_rate))
        return cls(capacity=cap, refill_rate=rate, tokens=cap, updated_at=now_value)

    def refill(self, now: float) -> None:
        elapsed = now - self.updated_at
        if elapsed <= 0:
            return
        self.tokens = min(self.capacity, self.tokens + (elapsed * self.refill_rate))
        self.updated_at = now


class HostTokenScheduler:
    """Sync-friendly per-host scheduler with token buckets and minimum delay."""

    def __init__(
        self,
        *,
        default_rate_per_second: float,
        default_capacity: int,
        min_request_delay_seconds: float = 0.0,
        time_fn: Callable[[], float] | None = None,
        sleep_fn: Callable[[float], None] | None = None,
        min_sleep_seconds: float = 0.001,
    ) -> None:
        self.default_rate_per_second = max(0.001, float(default_rate_per_second))
        self.default_capacity = max(1, int(default_capacity))
        self.min_request_delay_seconds = max(0.0, float(min_request_delay_seconds))
        self.min_sleep_seconds = max(0.0, float(min_sleep_seconds))
        self._time = time_fn or time.monotonic
        self._sleep = sleep_fn or time.sleep
        self._lock = threading.Lock()
        self._buckets: dict[str, TokenBucket] = {}
        self._last_request_at: dict[str, float] = {}

    def _host(self, url: str) -> str:
        host = (urlsplit(url).hostname or "").lower()
        return host or "_unknown"

    def _get_bucket(self, host: str, now: float) -> TokenBucket:
        bucket = self._buckets.get(host)
        if bucket is not None:
            return bucket
        bucket = TokenBucket.create(
            self.default_capacity,
            self.default_rate_per_second,
            now=now,
        )
        self._buckets[host] = bucket
        return bucket

    def acquire(self, url: str) -> None:
        self.acquire_with_wait(url)

    def acquire_with_wait(self, url: str) -> float:
        host = self._host(url)
        total_wait = 0.0

        while True:
            wait_seconds = 0.0
            with self._lock:
                now = self._time()
                bucket = self._get_bucket(host, now)
                bucket.refill(now)

                last = self._last_request_at.get(host)
                if last is not None and self.min_request_delay_seconds > 0:
                    min_delay_remaining = self.min_request_delay_seconds - (now - last)
                    if min_delay_remaining > 0:
                        wait_seconds = max(wait_seconds, min_delay_remaining)

                if bucket.tokens >= 1.0 and wait_seconds <= 0:
                    bucket.tokens -= 1.0
                    self._last_request_at[host] = now
                    return total_wait

                if bucket.tokens < 1.0:
                    refill_wait = (1.0 - bucket.tokens) / bucket.refill_rate
                    wait_seconds = max(wait_seconds, refill_wait)

            sleep_for = max(self.min_sleep_seconds, wait_seconds)
            self._sleep(sleep_for)
            total_wait += sleep_for
