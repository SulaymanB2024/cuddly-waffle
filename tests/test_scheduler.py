from __future__ import annotations

import threading

import pytest

from seo_audit.scheduler import HostTokenScheduler


class _FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += max(0.0, float(seconds))


def test_scheduler_enforces_min_request_delay_per_host() -> None:
    clock = _FakeClock()
    scheduler = HostTokenScheduler(
        default_rate_per_second=1000.0,
        default_capacity=5,
        min_request_delay_seconds=0.5,
        time_fn=clock.monotonic,
        sleep_fn=clock.sleep,
        min_sleep_seconds=0.0,
    )

    wait_first = scheduler.acquire_with_wait("https://example.com/")
    wait_second = scheduler.acquire_with_wait("https://example.com/about")

    assert wait_first == pytest.approx(0.0)
    assert wait_second == pytest.approx(0.5, abs=1e-6)
    assert clock.now == pytest.approx(0.5, abs=1e-6)


def test_scheduler_tracks_hosts_independently() -> None:
    clock = _FakeClock()
    scheduler = HostTokenScheduler(
        default_rate_per_second=0.5,
        default_capacity=1,
        min_request_delay_seconds=0.0,
        time_fn=clock.monotonic,
        sleep_fn=clock.sleep,
        min_sleep_seconds=0.0,
    )

    wait_a1 = scheduler.acquire_with_wait("https://a.example/")
    wait_b1 = scheduler.acquire_with_wait("https://b.example/")
    wait_a2 = scheduler.acquire_with_wait("https://a.example/next")

    assert wait_a1 == pytest.approx(0.0)
    assert wait_b1 == pytest.approx(0.0)
    assert wait_a2 == pytest.approx(2.0, abs=1e-6)


def test_scheduler_contention_completes_without_deadlock() -> None:
    scheduler = HostTokenScheduler(
        default_rate_per_second=1000.0,
        default_capacity=100,
        min_request_delay_seconds=0.0,
    )

    completed: list[int] = []
    lock = threading.Lock()

    def worker(index: int) -> None:
        scheduler.acquire("https://example.com/")
        with lock:
            completed.append(index)

    threads = [threading.Thread(target=worker, args=(idx,)) for idx in range(64)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2.0)

    assert all(not thread.is_alive() for thread in threads)
    assert len(completed) == 64
