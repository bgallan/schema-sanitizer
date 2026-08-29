"""Combines concurrent terminal-debt reaping with poisoned debt, no-throw snapshots, cgroup
migration samples, multipart manifests, and native backpressure. It verifies one reaper
per debt, conservative cross-process ownership, bounded retry state, and deadline
fairness without lost wakeups."""

from __future__ import annotations

import threading
from pathlib import Path

import pytest
from _support.synchronization import SCHEDULER_TIMEOUT_SECONDS, join_thread_or_fail

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src" / "schema_sanitizer"
CPP = ROOT / "cpp" / "src"
CPP_TESTS = ROOT / "cpp" / "tests"


def _source(relative: str) -> str:
    """Return the production source text inspected by this module."""
    return (SRC / relative).read_text(encoding="utf-8")


def _cpp_source(relative: str) -> str:
    """Return the production C++ source inspected by the test."""
    return (CPP / relative).read_text(encoding="utf-8")


def test_async_terminal_debt_has_exactly_one_concurrent_reaper() -> None:
    """Verify async terminal debt has exactly one concurrent reaper."""
    from schema_sanitizer.core_impl import async_scheduler as scheduler

    scheduler._reap_async_terminal_debts()
    entered = threading.Event()
    release = threading.Event()

    class DoneTask:
        def done(self) -> bool:
            """Report whether the done task test double has completed."""
            return True

    class BlockingOwner:
        def __init__(self) -> None:
            """Initialize the blocking owner test double."""
            self.calls = 0
            self.lock = threading.Lock()

        def close(self) -> None:
            """Close the resources owned by the blocking owner test double."""
            with self.lock:
                self.calls += 1
            entered.set()
            assert release.wait(SCHEDULER_TIMEOUT_SECONDS)

    owner = BlockingOwner()
    admission = scheduler._AsyncSchedulerAdmission(1, stage_admission=owner)
    assert scheduler._park_async_terminal_debt({DoneTask()}, admission, None)  # type: ignore[arg-type]

    results: list[bool] = []
    errors: list[BaseException] = []

    def reap() -> None:
        """Run one terminal-debt reaping attempt and capture its result or error."""
        try:
            results.append(scheduler._reap_one_async_terminal_debt())
        except BaseException as exc:  # pragma: no cover - assertion below reports it
            errors.append(exc)

    first = threading.Thread(target=reap)
    first.start()
    assert entered.wait(SCHEDULER_TIMEOUT_SECONDS)
    second = threading.Thread(target=reap)
    second.start()
    join_thread_or_fail(second)
    # The published debt is CLAIMED, so a second OS thread cannot run cleanup.
    assert results == [False]
    assert owner.calls == 1

    release.set()
    join_thread_or_fail(first)
    assert not errors
    assert sorted(results) == [False, True]
    assert owner.calls == 1
    assert admission.stage_admission is None


def test_poison_terminal_debt_cannot_block_new_debt_publication() -> None:
    """Verify poison terminal debt cannot block new debt publication."""
    from schema_sanitizer.core_impl import async_scheduler as scheduler

    scheduler._reap_async_terminal_debts()

    class PoisonOwner:
        def __init__(self) -> None:
            """Initialize the poison owner test double."""
            self.fail = True

        def close(self) -> None:
            """Close the resources owned by the poison owner test double."""
            if self.fail:
                raise RuntimeError("poison debt")

    poison = PoisonOwner()
    old = scheduler._AsyncSchedulerAdmission(1, stage_admission=poison)
    assert scheduler._park_async_terminal_debt(set(), old, None)
    with pytest.raises(RuntimeError, match="poison debt"):
        scheduler._reap_one_async_terminal_debt()
    before = scheduler._ASYNC_TERMINAL_DEBT_COUNT

    fresh_owner = PoisonOwner()
    fresh_owner.fail = False
    fresh = scheduler._AsyncSchedulerAdmission(1, stage_admission=fresh_owner)
    # Publication is a commit and must not execute cleanup of the old poison debt.
    assert scheduler._park_async_terminal_debt(set(), fresh, None)
    assert scheduler._ASYNC_TERMINAL_DEBT_COUNT == before + 1
    assert fresh.stage_admission is fresh_owner

    poison.fail = False
    scheduler._reap_async_terminal_debts()
    assert scheduler._ASYNC_TERMINAL_DEBT_COUNT == 0
    assert old.stage_admission is None
    assert fresh.stage_admission is None


def test_async_snapshot_is_no_throw_with_retry_pending_terminal_debt() -> None:
    """Verify async snapshot is no throw with retry pending terminal debt."""
    from schema_sanitizer.core_impl import async_scheduler as scheduler

    scheduler._reap_async_terminal_debts()

    class PoisonOwner:
        fail = True

        def close(self) -> None:
            """Close the resources owned by the poison owner test double."""
            if self.fail:
                raise RuntimeError("snapshot poison")

    owner = PoisonOwner()
    admission = scheduler._AsyncSchedulerAdmission(1, stage_admission=owner)
    assert scheduler._park_async_terminal_debt(set(), admission, None)
    with pytest.raises(RuntimeError, match="snapshot poison"):
        scheduler._reap_one_async_terminal_debt()

    snapshot = scheduler.async_scheduler_snapshot()
    assert snapshot.terminal_debts >= 1
    assert snapshot.terminal_retry_pending >= 1
    assert snapshot.terminal_reap_failures >= 1

    owner.fail = False
    scheduler._reap_async_terminal_debts()


def test_cross_process_shrink_failure_does_not_resurrect_logical_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify cross process shrink failure does not resurrect logical owner."""
    from schema_sanitizer.core_impl.bounded_generation import BoundedGenerationPool
    from schema_sanitizer.core_impl.cross_process_memory import _ProcessCrossMemoryCoordinator

    class FakePhysical:
        def __init__(self) -> None:
            """Initialize the fake physical test double."""
            self.fail_down = False
            self.size = 0

        def resize(self, value: int) -> None:
            """Resize the resource represented by the fake physical test double."""
            if self.fail_down and value < self.size:
                raise OSError("injected shrink failure")
            self.size = value

        def _set_capacity(self, _value: int) -> None:
            """Set the governor capacity for the contention scenario."""
            return None

    coordinator = _ProcessCrossMemoryCoordinator(16 << 20)
    coordinator._generation_pool = BoundedGenerationPool(1)
    physical = FakePhysical()
    coordinator._physical = physical  # type: ignore[assignment]
    coordinator._physical_bytes = 0
    # Keep the test deterministic: pending reconcile is observable but no worker
    # races the injected failing fake in the background.
    monkeypatch.setattr(coordinator, "_schedule_reconcile_locked", lambda *, start_worker: None)

    reservation = coordinator.acquire(1)
    first_token = reservation._token
    assert coordinator._contributions
    physical.fail_down = True
    reservation.close()  # logical close has committed even though shrink fails
    assert reservation.reserved_bytes == 0
    assert not coordinator._contributions
    assert not coordinator._contribution_owners
    assert coordinator._pending_shrink
    assert coordinator._shrink_failures == 1

    # The sole bounded slot must be immediately reusable; the stale reservation
    # must not later authenticate as the owner of this recycled generation.
    second = coordinator.acquire(0)
    assert second._token != first_token
    second.close()
    assert not coordinator._contributions


def test_cgroup_effective_read_retries_entire_sample_after_migration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify cgroup effective read retries entire sample after migration."""
    from schema_sanitizer.core_impl import cgroup_view

    mount_a = tmp_path / "a"
    mount_b = tmp_path / "b"
    mount_a.mkdir()
    mount_b.mkdir()
    (mount_a / "memory.max").write_text("100\n", encoding="ascii")
    (mount_b / "memory.max").write_text("200\n", encoding="ascii")
    views = [
        cgroup_view.CgroupView(2, mount_a, mount_a, resolution_known=True),
        cgroup_view.CgroupView(2, mount_b, mount_b, resolution_known=True),
    ]
    calls = 0

    def current(*, refresh: bool = False):
        """Return the controlled current task used by the reaper test."""
        nonlocal calls
        view = views[min(calls, 1)]
        calls += 1
        return view

    stable = iter((False, True))
    monkeypatch.setattr(cgroup_view, "_sample_membership_before", lambda: ("/x", {}))
    monkeypatch.setattr(cgroup_view, "_membership_sample_stable", lambda _before: next(stable))
    monkeypatch.setattr(cgroup_view, "current_cgroup_view", current)

    sample = cgroup_view.read_effective_cgroup_integer("memory.max", controller="memory")
    assert sample.state is cgroup_view.CgroupValueState.VALUE
    assert sample.value == 200
    assert calls == 2


def test_incomplete_cgroup_subtree_never_claims_unbounded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify incomplete cgroup subtree never claims unbounded."""
    from schema_sanitizer.core_impl import cgroup_view

    root = tmp_path / "subtree"
    root.mkdir()
    (root / "memory.max").write_text("max\n", encoding="ascii")
    view = cgroup_view.CgroupView(2, root, root, resolution_known=True, hierarchy_complete=False)
    monkeypatch.setattr(cgroup_view, "current_cgroup_view", lambda **_kwargs: view)
    monkeypatch.setattr(cgroup_view, "_sample_membership_before", lambda: ("/x", {}))
    monkeypatch.setattr(cgroup_view, "_membership_sample_stable", lambda _before: True)
    sample = cgroup_view.read_effective_cgroup_integer("memory.max", controller="memory")
    assert sample.state is cgroup_view.CgroupValueState.UNKNOWN


def test_async_result_memory_contract_is_explicit_for_externally_governed_results() -> None:
    """Verify async result memory contract is explicit for externally governed results."""
    from schema_sanitizer.core_impl.async_scheduler import (
        AsyncResultMemoryContract,
        AsyncResultOwnershipMode,
        _contract_estimators,
    )

    contract = AsyncResultMemoryContract(
        preflight_bytes=512,
        ownership_mode=AsyncResultOwnershipMode.EXTERNALLY_GOVERNED,
    )
    postflight, preflight = _contract_estimators(contract)
    assert postflight is None
    assert preflight == 512

    discovery = _source("pipeline/source_discovery.py")
    assert "AsyncResultOwnershipMode.EXTERNALLY_GOVERNED" in discovery
    assert "memory_contract=_DISCOVERY_RESULT_MEMORY_CONTRACT" in discovery


def test_s3_multipart_manifest_keeps_memory_ownership_until_commit() -> None:
    """Verify s3 multipart manifest keeps memory ownership until commit."""
    import ast
    import sys
    from typing import Any

    source = _source("remote_impl/upload_policy.py")
    tree = ast.parse(source)
    selected = [
        node
        for node in tree.body
        if (isinstance(node, ast.ClassDef) and node.name == "S3MultipartManifestBudget")
        or (isinstance(node, ast.FunctionDef) and node.name == "acquire_s3_multipart_manifest")
    ]
    leases = []

    class FakeLease:
        def __init__(self, initial: int) -> None:
            """Initialize the fake lease test double."""
            self.sizes = [initial]
            self.closed = False

        def resize(self, value: int) -> None:
            """Resize the resource represented by the fake lease test double."""
            self.sizes.append(value)

        def close(self) -> None:
            """Close the resources owned by the fake lease test double."""
            self.closed = True

    def acquire(amount: int, *, stage: str):
        """Acquire the resource under the controlled scheduling conditions."""
        assert stage == "s3_multipart_manifest"
        lease = FakeLease(amount)
        leases.append(lease)
        return lease

    namespace = {"Any": Any, "sys": sys, "acquire_operation_memory": acquire}
    exec(compile(ast.Module(body=selected, type_ignores=[]), "upload_policy.py", "exec"), namespace)
    manifest = namespace["acquire_s3_multipart_manifest"](10_000)
    parts: list[dict[str, object]] = []
    initial = manifest.reserved_bytes
    manifest.append_part(parts, "etag-" + "x" * 4096, 1)
    assert len(parts) == 1
    assert manifest.reserved_bytes > initial
    assert leases[0].sizes[-1] == manifest.reserved_bytes
    manifest.close()
    assert leases[0].closed
    assert manifest.reserved_bytes == 0

    async_s3 = _source("remote_impl/providers/s3.py")
    sync_s3 = _source("remote_impl/providers/s3_sync.py")
    for provider_source in (async_s3, sync_s3):
        assert "acquire_s3_multipart_manifest" in provider_source
        assert "manifest.append_part" in provider_source


def test_native_backpressure_has_dynamic_deadline_fairness_and_lost_wakeup_guards() -> None:
    """Verify native backpressure has dynamic deadline fairness and lost wakeup guards."""
    source = _cpp_source("internal/runtime/operation_task_arena.cc")
    assert "std::lock_guard retained_lock(retained_wait_mutex)" in source
    assert "retained_ready.notify_all()" in source
    assert "retained_ready.notify_one()" not in source
    assert "backpressure_timeout_ns.load" in source
    assert "backpressure_deadline_ns.load" in source
    assert "backpressure_waiters.fetch_add" in source
    assert "backpressure_timeouts.fetch_add" in source
    assert "logical_backpressure_timeouts.fetch_add" in source
    assert "kMaxLogicalDeadlineMillis" in source

    for relative in (
        "ingest/prepare/prepare.cc",
        "api/python_abi3/arrow_direct/_core_abi3_arrow_direct.cc",
        "api/python_abi3/registry/arrow_source_sinks.cc",
        "api/python_abi3/registry/path_source_sinks.cc",
    ):
        assert "SetBackpressureTimeoutMillis" in _cpp_source(relative)


def test_native_cgroup_sample_is_revalidated_after_hierarchy_read() -> None:
    """Verify native cgroup sample is revalidated after hierarchy read."""
    source = _cpp_source("internal/runtime/cgroup_view.hh")
    assert "membership_after" in source
    assert "hierarchy_complete" in source
    assert "std::strcmp(membership_after, membership) == 0" in source


def test_tsan_probe_covers_dynamic_backpressure_deadline() -> None:
    """Verify TSan probe covers dynamic backpressure deadline."""
    source = (CPP_TESTS / "ordered_executor_tsan.cc").read_text(encoding="utf-8")
    assert "run_arena_backpressure_deadline_round" in source
    assert "arena_backpressure_deadline" in source
