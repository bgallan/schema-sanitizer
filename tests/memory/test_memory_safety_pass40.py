"""Regression coverage for concurrency/memory hardening pass 40."""

from __future__ import annotations

import importlib.util
import sys
import threading
import time
import types
from pathlib import Path

import pytest


def test_unscoped_governor_release_cannot_bypass_exact_ledger() -> None:
    from schema_sanitizer.core_impl.process_resources import _Governor

    governor = _Governor(2, "pass40-ledger")
    first = governor.acquire(1)
    second = governor.acquire(1)
    governor.release(2)
    snapshot = governor.snapshot()
    assert snapshot.in_use == 2
    assert snapshot.active_leases == 2
    assert snapshot.compatibility_release_attempts == 1
    with pytest.raises(AttributeError):
        first._amount = 2
    with pytest.raises(AttributeError):
        first._governor = _Governor(2, "other")
    first.release()
    second.release()
    assert governor.snapshot().in_use == 0


def test_lease_finalizer_authenticates_after_weakref_clear() -> None:
    import gc

    from schema_sanitizer.core_impl import process_resources as module

    governor = module._Governor(1, "pass40-finalizer-ledger")
    lease = governor.try_acquire_up_to(1)
    assert governor.snapshot().in_use == 1
    del lease
    gc.collect()
    assert governor.snapshot().in_use == 0
    assert governor.snapshot().unknown_lease_releases == 0


def test_retry_primitive_keys_are_type_tagged_and_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import schema_sanitizer.core_impl.retry_scheduler as module

    scheduler = module._RetryScheduler()
    monkeypatch.setattr(scheduler, "_ensure_workers", lambda: None)
    assert scheduler.schedule(True, lambda: None, delay_seconds=60)
    assert scheduler.schedule(1, lambda: None, delay_seconds=60)
    assert scheduler.schedule(1.0, lambda: None, delay_seconds=60)
    assert scheduler.snapshot().pending_retries == 3
    scheduler.cancel(True)
    assert scheduler.snapshot().pending_retries == 2
    scheduler.cancel(1)
    assert scheduler.snapshot().pending_retries == 1
    scheduler.cancel(1.0)
    assert scheduler.snapshot().pending_retries == 0
    with pytest.raises(ValueError, match="too many elements"):
        scheduler.schedule(tuple(range(300)), lambda: None, delay_seconds=1)
    with pytest.raises(ValueError, match="metadata budget"):
        scheduler.schedule("x" * (70 * 1024), lambda: None, delay_seconds=1)
    assert scheduler.close(deadline_seconds=1)


class _HostileError(BaseException):
    def __str__(self) -> str:
        raise RuntimeError("hostile str")

    def __repr__(self) -> str:
        raise RuntimeError("hostile repr")


def test_hostile_exception_cannot_kill_cleanup_dispatcher_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import schema_sanitizer.core_impl.cleanup_dispatcher as module

    monkeypatch.setattr(module, "_MAX_CLEANUP_ATTEMPTS", 1)
    dispatcher = module._CleanupDispatcher()

    def cleanup() -> None:
        raise _HostileError()

    assert dispatcher.submit(cleanup, retained_bytes=128)
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        snapshot = dispatcher.snapshot()
        if snapshot.dead_letter_calls == 1:
            break
        time.sleep(0.01)
    snapshot = dispatcher.snapshot()
    assert snapshot.dead_letter_calls == 1
    assert snapshot.active_calls == 0
    assert snapshot.active_workers == 0
    assert not dispatcher.close(deadline_seconds=0)


def test_guardian_is_governed_and_deduplicates_terminal_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import schema_sanitizer.core_impl.retry_scheduler as module

    monkeypatch.setattr(module, "_RELEASE_MAX_ATTEMPTS", 1)
    permit_lock = threading.Lock()
    active_permits = 0

    class WorkerPermit:
        def __init__(self) -> None:
            nonlocal active_permits
            with permit_lock:
                active_permits += 1

        def release(self) -> None:
            nonlocal active_permits
            with permit_lock:
                active_permits -= 1

    monkeypatch.setattr(module, "acquire_release_guardian_thread", WorkerPermit)
    guardian = module._ReleaseGuardian()
    entered = threading.Event()
    resume = threading.Event()

    class Owner:
        def release(self) -> None:
            entered.set()
            assert resume.wait(2)
            raise _HostileError()

        def close(self) -> None:
            raise AssertionError("alternate terminal method must not be admitted")

    owner = Owner()
    assert guardian.adopt(owner, retained_bytes=128)
    assert entered.wait(1)
    with permit_lock:
        assert 1 <= active_permits <= module._MAX_RELEASE_GUARDIAN_WORKERS
    resume.set()
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        snapshot = guardian.snapshot()
        if snapshot.dead_letter_owners == 1:
            break
        time.sleep(0.01)
    assert guardian.snapshot().dead_letter_owners == 1
    assert guardian.adopt(owner, retained_bytes=128)
    assert not guardian.adopt(owner, method="close", retained_bytes=128)
    time.sleep(0.05)
    snapshot = guardian.snapshot()
    assert snapshot.dead_letter_owners == 1
    assert snapshot.active_releases == 0
    assert not guardian.close(deadline_seconds=0)


def test_huge_integer_duration_saturates_without_float_overflow() -> None:
    from schema_sanitizer.core_impl.durations import (
        deadline_ns_from_timeout,
        normalize_duration,
    )

    huge = 10**10000
    assert normalize_duration(huge, name="huge") > 0
    assert deadline_ns_from_timeout(huge, name="huge") == (1 << 63) - 1


def test_runtime_registry_propagates_process_control_exceptions() -> None:
    from schema_sanitizer.core_impl.durations import deadline_ns_from_timeout
    from schema_sanitizer.core_impl.runtime_registry import _RuntimeServiceRegistry

    registry = _RuntimeServiceRegistry()

    class Service:
        def close(self, *, deadline_seconds: float) -> bool:
            raise KeyboardInterrupt()

    service = Service()
    registry.register(service, kind="pass40-control", close_name="close")
    with pytest.raises(KeyboardInterrupt):
        registry.close_all(deadline_ns=deadline_ns_from_timeout(1, name="pass40 registry"))


def test_transactional_thread_publication_never_releases_live_worker_permit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package = types.ModuleType("schema_sanitizer.pipeline")
    package.__path__ = [str(Path("src/schema_sanitizer/pipeline").resolve())]
    monkeypatch.setitem(sys.modules, "schema_sanitizer.pipeline", package)
    module_name = "schema_sanitizer.pipeline.partition_lookahead_worker"
    spec = importlib.util.spec_from_file_location(
        module_name, Path("src/schema_sanitizer/pipeline/partition_lookahead_worker.py")
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, module_name, module)
    spec.loader.exec_module(module)

    class Lease:
        def __init__(self) -> None:
            self.releases = 0

        def release(self) -> None:
            self.releases += 1

    class Registration:
        def activate(self) -> None:
            raise MemoryError("publish")

        def close(self) -> None:
            return None

    class Thread:
        def __init__(self, **_kwargs: object) -> None:
            self.started = False

        def start(self) -> None:
            self.started = True

        def is_alive(self) -> bool:
            return self.started

    lease = Lease()
    monkeypatch.setattr(module, "reserve_runtime_service", lambda *_a, **_k: Registration())
    monkeypatch.setattr(module, "Thread", Thread)
    with pytest.raises(MemoryError, match="publish"):
        module.ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="pass40",
            permit_factory=lambda *_a, **_k: lease,
        )
    assert lease.releases == 0


def test_availability_callbacks_are_one_shot_and_off_releaser_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import process_resources as module

    callback_thread: list[int] = []
    called = threading.Event()

    def dispatch(event: module.AvailabilityEvent) -> None:
        assert event is module.AvailabilityEvent.RETRY_SCHEDULER
        callback_thread.append(threading.get_ident())
        called.set()

    governor = module._Governor(1, "pass40-callback", availability_dispatcher=dispatch)
    lease = governor.acquire(1)
    assert governor.register_availability_event(module.AvailabilityEvent.RETRY_SCHEDULER)
    releasing_thread = threading.get_ident()
    lease.release()
    assert called.wait(2)
    assert callback_thread[0] != releasing_thread
    assert governor.snapshot().availability_callbacks == 0


def test_integral_snapshot_includes_governed_notifier_and_global_epoch() -> None:
    from schema_sanitizer.core_impl.runtime_diagnostics import (
        concurrency_runtime_debug_snapshot,
    )

    snapshot = concurrency_runtime_debug_snapshot()
    assert snapshot["version"] >= 3
    assert "availability_notifier_threads" in snapshot
    assert snapshot["retry_scheduler"]["capture_epoch"].count(":") >= 4


def test_native_reaper_is_joinable_lazy_and_reserves_teardown_capacity() -> None:
    source = Path("cpp/src/internal/runtime/operation_task_arena.cc").read_text()
    assert (
        ".detach()"
        not in source[
            source.index("class ArenaCleanupReaper") : source.index(
                '#include "internal/runtime/operation_task_arena_runtime.cc.inc"'
            )
        ]
    )
    assert "reaper_reserved_bytes" in source
    assert "teardown capacity exhausted" in source
    assert "std::atexit" in source
    assert "SaturatingSubtract" in source
