"""Regression coverage for memory provider throttle registry is bounded under endpoint churn."""

from __future__ import annotations

import asyncio
import threading
import time
import tracemalloc
from pathlib import Path

import pytest

from schema_sanitizer.core_impl.process_resources import _Governor
from schema_sanitizer.errors import SchemaSanitizerResourceError
from schema_sanitizer.remote_impl.provider_session_pool import RemoteProviderSessionPool
from schema_sanitizer.remote_impl.provider_throttle import ProviderThrottleGovernor

ROOT = Path(__file__).resolve().parents[2]


class _Client:
    """Minimal asynchronously closable provider client."""

    def __init__(self) -> None:
        """Initialize close accounting."""
        self.close_calls = 0

    async def close(self) -> None:
        """Record one operation-final close."""
        self.close_calls += 1


def test_provider_throttle_registry_is_bounded_under_endpoint_churn() -> None:
    """Sequential high-cardinality hosts evict idle state instead of leaking forever."""
    governor = ProviderThrottleGovernor(max_tracked_keys=3)
    for index in range(20):
        lease, delay = governor.try_acquire(f"https://host-{index}.example")
        assert lease is not None, delay
        lease.success()

    snapshot = governor.registry_snapshot()
    assert snapshot.tracked_keys == 3
    assert snapshot.max_tracked_keys == 3
    assert snapshot.evictions == 17
    assert snapshot.saturation_rejections == 0


def test_provider_throttle_unknown_snapshots_do_not_create_registry_entries() -> None:
    """Observability cannot itself amplify attacker-controlled endpoint cardinality."""
    governor = ProviderThrottleGovernor(max_tracked_keys=2)
    for index in range(100):
        snapshot = governor.snapshot(f"unknown-{index}")
        assert snapshot.in_flight == 0
    assert governor.registry_snapshot().tracked_keys == 0


def test_provider_throttle_compacts_oversized_endpoint_keys_incrementally() -> None:
    """A huge endpoint cannot become a huge process-lifetime registry key or copy."""
    governor = ProviderThrottleGovernor(max_tracked_keys=2)
    key = "https://" + ("x" * (2 << 20)) + ".example"
    tracemalloc.start()
    lease, delay = governor.try_acquire(key)
    assert lease is not None, delay
    lease.success()
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert peak < 512 << 10
    assert len(next(iter(governor._states))) < 100
    assert governor.snapshot(key).successes == 1


def test_provider_throttle_never_evicts_live_endpoint_state() -> None:
    """A saturated registry waits until one live endpoint becomes idle."""
    governor = ProviderThrottleGovernor(max_tracked_keys=2)
    first, _delay = governor.try_acquire("first")
    second, _delay = governor.try_acquire("second")
    assert first is not None
    assert second is not None

    blocked, delay = governor.try_acquire("third")
    assert blocked is None
    assert delay > 0
    saturated = governor.registry_snapshot()
    assert saturated.tracked_keys == 2
    assert saturated.active_keys == 2
    assert saturated.saturation_rejections == 1

    first.release()
    admitted, delay = governor.try_acquire("third")
    assert admitted is not None, delay
    after = governor.registry_snapshot()
    assert after.tracked_keys == 2
    assert after.evictions == 1
    second.release()
    admitted.release()


def test_provider_throttle_does_not_evict_open_circuit_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Endpoint churn cannot erase a live circuit-breaker cooldown."""
    from schema_sanitizer.remote_impl import provider_throttle as module

    monkeypatch.setattr(module, "monotonic", lambda: 100.0)
    governor = module.ProviderThrottleGovernor(max_tracked_keys=1)
    lease, delay = governor.try_acquire("penalized")
    assert lease is not None, delay
    throttled = RuntimeError("rate limited")
    throttled.status = 429  # type: ignore[attr-defined]
    throttled.retry_after = 30.0  # type: ignore[attr-defined]
    lease.failure(throttled)

    blocked, delay = governor.try_acquire("new-endpoint")
    assert blocked is None
    assert delay > 0
    registry = governor.registry_snapshot()
    assert registry.tracked_keys == 1
    assert registry.open_circuits == 1
    assert registry.evictions == 0
    assert registry.saturation_rejections == 1
    assert governor.snapshot("penalized").circuit_open_until == 130.0


def test_provider_throttle_registry_stays_bounded_under_threaded_churn() -> None:
    """Concurrent endpoint turnover preserves live leases and the registry ceiling."""
    governor = ProviderThrottleGovernor(max_tracked_keys=8)
    errors: list[BaseException] = []

    def exercise(worker: int) -> None:
        """Acquire many unique endpoint slots with bounded saturation retries."""
        try:
            for index in range(50):
                key = f"worker-{worker}-endpoint-{index}"
                while True:
                    lease, _delay = governor.try_acquire(key)
                    if lease is not None:
                        lease.success()
                        break
                    time.sleep(0.0005)
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=exercise, args=(worker,)) for worker in range(12)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    snapshot = governor.registry_snapshot()
    assert snapshot.tracked_keys <= snapshot.max_tracked_keys == 8
    assert snapshot.active_keys == 0


def test_provider_pool_retires_failed_key_gates_immediately() -> None:
    """Unique failed provider keys cannot accumulate operation-lifetime locks."""

    async def exercise() -> None:
        """Attempt many distinct factories inside one long-lived operation pool."""
        pool = RemoteProviderSessionPool()
        await pool.__aenter__()

        async def fail() -> _Client:
            """Fail before publishing a provider client."""
            raise RuntimeError("factory failed")

        for index in range(200):
            with pytest.raises(RuntimeError, match="factory failed"):
                await pool.borrow_client(("http", index), fail)
            assert len(pool._key_locks) == 0
        await pool.__aexit__(None, None, None)

    asyncio.run(exercise())


def test_provider_pool_keeps_single_flight_while_reclaiming_key_gate() -> None:
    """Gate reclamation preserves one factory execution for concurrent same-key calls."""

    async def exercise() -> tuple[int, int, _Client]:
        """Overlap two borrows and inspect the gate before and after publication."""
        pool = RemoteProviderSessionPool()
        await pool.__aenter__()
        started = asyncio.Event()
        release = asyncio.Event()
        client = _Client()
        calls = 0

        async def factory() -> _Client:
            """Suspend the only allowed factory while a second borrower queues."""
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            return client

        first = asyncio.create_task(pool.borrow_client(("http", "same"), factory))
        await started.wait()
        second = asyncio.create_task(pool.borrow_client(("http", "same"), factory))
        await asyncio.sleep(0)
        during = len(pool._key_locks)
        release.set()
        await asyncio.gather(first, second)
        after = len(pool._key_locks)
        await pool.__aexit__(None, None, None)
        return calls, during, after, client

    calls, during, after, client = asyncio.run(exercise())
    assert calls == 1
    assert during == 1
    assert after == 0
    assert client.close_calls == 1


def test_provider_pool_cancelled_waiter_releases_its_gate_reference() -> None:
    """Cancelling a queued same-key borrow cannot retain a key gate."""

    async def exercise() -> tuple[int, int]:
        """Cancel one waiter while the publishing factory remains active."""
        pool = RemoteProviderSessionPool()
        await pool.__aenter__()
        started = asyncio.Event()
        release = asyncio.Event()
        client = _Client()

        async def factory() -> _Client:
            """Hold the key gate until the cancellation has propagated."""
            started.set()
            await release.wait()
            return client

        creator = asyncio.create_task(pool.borrow_client(("http", "cancel"), factory))
        await started.wait()
        waiter = asyncio.create_task(pool.borrow_client(("http", "cancel"), factory))
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        during = len(pool._key_locks)
        release.set()
        await creator
        after = len(pool._key_locks)
        await pool.__aexit__(None, None, None)
        return during, after

    during, after = asyncio.run(exercise())
    assert during == 1
    assert after == 0


def test_remote_directory_session_does_not_duplicate_file_sequences() -> None:
    """Directory staging retains one provider sample and reuses chunk sequences directly."""
    text = (ROOT / "src/schema_sanitizer/remote_impl/directory_downloads.py").read_text(
        encoding="utf-8"
    )
    assert "self._first_file = files[0] if files else None" in text
    assert "self._files = tuple(files)" not in text
    assert "_download_files_with_context(\n            list(files)," not in text
    assert "files: Sequence[RemoteFile]" in text


def test_operation_diagnostics_include_provider_registry_pressure() -> None:
    """Completed operation records expose endpoint-registry saturation and churn."""
    api_impl = ROOT / "src/schema_sanitizer/api_impl"
    text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (
            api_impl / "operation_context.py",
            api_impl / "operation_resource_diagnostics.py",
        )
    )
    assert '"provider_throttle": asdict(process_provider_throttle_snapshot())' in text


def test_resource_waiter_timeouts_are_removed_without_ticket_tombstones() -> None:
    """Mass timeout behind one holder leaves no deferred per-ticket metadata."""
    governor = _Governor(1, "bounded_test", max_waiters=32)
    holder = governor.acquire()
    timed_out = 0
    timed_out_lock = threading.Lock()

    def wait_and_timeout() -> None:
        """Join the FIFO briefly and then abandon it."""
        nonlocal timed_out
        try:
            governor.acquire(timeout_seconds=0.03)
        except SchemaSanitizerResourceError:
            with timed_out_lock:
                timed_out += 1

    threads = [threading.Thread(target=wait_and_timeout) for _ in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=1)

    assert timed_out == 16
    assert governor.snapshot().waiting == 0
    assert not hasattr(governor, "_abandoned_tickets")
    holder.release()


def test_resource_wait_queue_fails_fast_at_its_memory_bound() -> None:
    """External contention cannot append an unlimited number of FIFO waiters."""
    governor = _Governor(1, "bounded_test", max_waiters=2)
    holder = governor.acquire()
    acquired = 0
    acquired_lock = threading.Lock()

    def wait_for_slot() -> None:
        """Acquire after the holder releases and return capacity immediately."""
        nonlocal acquired
        lease = governor.acquire(timeout_seconds=1)
        with acquired_lock:
            acquired += 1
        lease.release()

    threads = [threading.Thread(target=wait_for_slot) for _ in range(2)]
    for thread in threads:
        thread.start()
    deadline = time.monotonic() + 1
    while governor.snapshot().waiting < 2 and time.monotonic() < deadline:
        time.sleep(0.005)
    assert governor.snapshot().waiting == 2

    with pytest.raises(SchemaSanitizerResourceError, match="wait queue exhausted"):
        governor.acquire(timeout_seconds=1)
    saturated = governor.snapshot()
    assert saturated.waiting == 2
    assert saturated.queue_capacity == 2
    assert saturated.rejected_waiters == 1

    holder.release()
    for thread in threads:
        thread.join(timeout=1)
    assert acquired == 2
    assert governor.snapshot().waiting == 0
