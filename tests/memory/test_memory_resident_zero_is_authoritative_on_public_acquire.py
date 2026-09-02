"""Tests authoritative resident zero and unknown probes across pool identity, registry or
claim caps, physical and logical publication, FIFO shrink, native stack or descriptor
snapshots, and release gates. A real zero replaces stale credit, unknown or unstable
observations fail closed, wrappers deduplicate by declared identity, and claims publish
before native or governor commit."""

from __future__ import annotations

import threading
from pathlib import Path
from time import monotonic
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.usefixtures("isolated_external_runtime_coordinator")
from _support.synchronization import SCHEDULER_TIMEOUT_SECONDS, join_thread_or_fail

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src/schema_sanitizer"
CPP = ROOT / "cpp/src"


def _clear_external_coordinator(module) -> None:
    """Clear the cached external coordinator before the lifecycle check."""
    with module._EXTERNAL_RUNTIME_POOL_COORDINATOR_LOCK:
        module._EXTERNAL_RUNTIME_POOL_COORDINATOR.clear()


class _ExactNative:
    """Current exact-receipt runtime authority plus optional residency accounting."""

    supports_resident_attribution = True

    def __init__(self, events: list[tuple[str, int]]) -> None:
        """Initialize the exact native test double."""
        self.events = events

    def acquire_exact_permit_lease(self, desired: int, minimum: int):
        """Acquire the fake exact-permit lease requested by the resource owner."""
        assert desired >= minimum
        self.events.append(("acquire", desired))
        return SimpleNamespace(amount=desired), desired

    def resize_exact_permit_lease(self, receipt: object, target: int) -> int:
        """Resize the fake exact-permit lease to the requested amount."""
        previous = int(receipt.amount)  # type: ignore[attr-defined]
        receipt.amount = target  # type: ignore[attr-defined]
        if previous != target:
            self.events.append(("release", previous - target))
        return target

    @staticmethod
    def exact_permit_lease_amount(receipt: object) -> int:
        """Return the exact permit amount tracked by the fake lease."""
        return int(receipt.amount)  # type: ignore[attr-defined]

    def external_runtime_resident_threads_add(self, amount: int) -> None:
        """Record an increase in resident runtime threads."""
        self.events.append(("resident-add", amount))

    def external_runtime_resident_threads_release(self, amount: int) -> None:
        """Record a release from resident runtime threads."""
        self.events.append(("resident-release", amount))


def test_resident_zero_is_authoritative_on_public_acquire(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify resident zero is authoritative on public acquire."""
    from schema_sanitizer.core_impl import process_resources as module

    _clear_external_coordinator(module)
    width = [3]
    events: list[tuple[str, int]] = []

    class Runtime:
        @staticmethod
        def schema_sanitizer_resident_thread_count() -> int:
            """Return the controlled resident-thread count."""
            return width[0]

    native = _ExactNative(events)
    monkeypatch.setattr(module, "_native_external_thread_api", lambda: native)

    acquired = module._acquire_shared_external_native_thread_permits(Runtime, 2)
    permit, granted = acquired.owner, acquired.amount
    assert permit is not None and granted == 2
    permit.resize_physical_thread_permits(0)
    assert module.external_runtime_pool_snapshot()["resident_width"] == 3

    width[0] = 0
    acquired2 = module._acquire_shared_external_native_thread_permits(Runtime, 1)
    permit2, granted2 = acquired2.owner, acquired2.amount
    assert permit2 is not None and granted2 == 1
    assert ("resident-release", 3) in events
    permit2.resize_physical_thread_permits(0)
    assert module.external_runtime_pool_snapshot()["coordinator_entries"] == 0


def test_unknown_resident_probe_preserves_stale_identity_credit_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify unknown resident probe preserves stale identity credit fail closed."""
    from schema_sanitizer.core_impl import process_resources as module

    _clear_external_coordinator(module)
    state = [3]
    events: list[tuple[str, int]] = []

    class Runtime:
        @staticmethod
        def schema_sanitizer_resident_thread_count() -> int:
            """Return the resident width or raise when the probe is unavailable."""
            value = state[0]
            if value < 0:
                raise RuntimeError("probe unavailable")
            return value

    native = _ExactNative(events)
    monkeypatch.setattr(module, "_native_external_thread_api", lambda: native)
    acquired = module._acquire_shared_external_native_thread_permits(Runtime, 1)
    permit, granted = acquired.owner, acquired.amount
    assert permit is not None and granted == 1
    permit.resize_physical_thread_permits(0)
    assert module.external_runtime_pool_snapshot()["resident_width"] == 3

    state[0] = -1
    acquired2 = module._acquire_shared_external_native_thread_permits(Runtime, 1)
    permit2, granted2 = acquired2.owner, acquired2.amount
    assert permit2 is not None and granted2 == 1
    # Probe failure is uncertainty, not proof of zero resident workers. Keeping
    # identity prevents reopening CPU capacity while the external pool may live.
    assert ("resident-release", 3) not in events
    permit2.resize_physical_thread_permits(0)
    assert module.external_runtime_pool_snapshot()["resident_width"] == 3

    state[0] = 0
    acquired3 = module._acquire_shared_external_native_thread_permits(Runtime, 1)
    permit3, granted3 = acquired3.owner, acquired3.amount
    assert permit3 is not None and granted3 == 1
    permit3.resize_physical_thread_permits(0)
    assert ("resident-release", 3) in events
    assert module.external_runtime_pool_snapshot()["coordinator_entries"] == 0


def test_declared_pool_identity_deduplicates_wrappers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify declared pool identity deduplicates wrappers."""
    from schema_sanitizer.core_impl import process_resources as module

    _clear_external_coordinator(module)
    events: list[tuple[str, int]] = []

    class Runtime:
        def schema_sanitizer_thread_pool_identity(self) -> str:
            """Return the controlled native thread-pool identity."""
            return "shared-arrow-pool"

        def schema_sanitizer_resident_thread_count(self) -> int:
            """Return the controlled resident-thread count."""
            return 4

    native = _ExactNative(events)
    monkeypatch.setattr(module, "_native_external_thread_api", lambda: native)
    a, b = Runtime(), Runtime()
    acquired_a = module._acquire_shared_external_native_thread_permits(a, 3)
    acquired_b = module._acquire_shared_external_native_thread_permits(b, 2)
    pa, ga = acquired_a.owner, acquired_a.amount
    pb, gb = acquired_b.owner, acquired_b.amount
    assert pa is not None and pb is not None
    assert (ga, gb) == (3, 2)
    snap = module.external_runtime_pool_snapshot()
    assert snap["coordinator_entries"] == 1
    assert snap["resident_width"] == 4
    assert [event for event in events if event[0] == "resident-add"] == [("resident-add", 4)]
    # Overlap shares the established width; only one native generation was acquired.
    assert [event for event in events if event[0] == "acquire"] == [("acquire", 3)]
    pb.resize_physical_thread_permits(0)
    pa.resize_physical_thread_permits(0)
    _clear_external_coordinator(module)


def test_external_pool_registry_has_global_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify external pool registry has global bound."""
    from schema_sanitizer.core_impl import process_resources as module

    _clear_external_coordinator(module)
    monkeypatch.setattr(module, "_MAX_EXTERNAL_RUNTIME_POOL_ENTRIES", 1)
    first = object()
    second = object()
    with module._EXTERNAL_RUNTIME_POOL_COORDINATOR_LOCK:
        entry = module._external_runtime_entry_locked(first, create=True)
        assert entry is not None
        with pytest.raises(Exception, match="coordinator capacity exhausted"):
            module._external_runtime_entry_locked(second, create=True)
        module._EXTERNAL_RUNTIME_POOL_COORDINATOR.clear()


def test_external_pool_total_claim_bound_precedes_native_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify external pool total claim bound precedes native commit."""
    from schema_sanitizer.core_impl import process_resources as module

    _clear_external_coordinator(module)
    monkeypatch.setattr(module, "_MAX_EXTERNAL_RUNTIME_POOL_TOTAL_CLAIMS", 1)
    events: list[tuple[str, int]] = []

    class Runtime:
        def __init__(self, token: str) -> None:
            """Initialize the runtime test double."""
            self.token = token

        def schema_sanitizer_thread_pool_identity(self) -> str:
            """Return the controlled native thread-pool identity."""
            return self.token

        @staticmethod
        def schema_sanitizer_resident_thread_count() -> int:
            """Return the controlled resident-thread count."""
            return 1

    native = _ExactNative(events)
    native.supports_resident_attribution = False
    monkeypatch.setattr(module, "_native_external_thread_api", lambda: native)
    acquired = module._acquire_shared_external_native_thread_permits(Runtime("pool-a"), 1)
    first, width = acquired.owner, acquired.amount
    assert first is not None and width == 1
    blocked_acquisition = module._acquire_shared_external_native_thread_permits(
        Runtime("pool-b"), 1
    )
    blocked, blocked_width = blocked_acquisition.owner, blocked_acquisition.amount
    assert blocked is not None and blocked_width == 0
    assert [event for event in events if event[0] == "acquire"] == [("acquire", 1)]
    assert module.external_runtime_pool_snapshot()["claims"] == 1

    logical_calls = 0

    def acquire_logical(*_args, **_kwargs):
        """Acquire the logical permit used by the residency check."""
        nonlocal logical_calls
        logical_calls += 1
        raise AssertionError("aggregate claim cap must precede logical governor commit")

    monkeypatch.setattr(module, "acquire_project_threads", acquire_logical)
    with pytest.raises(Exception, match="logical claim capacity exhausted"):
        module._acquire_shared_external_logical_thread_lease(Runtime("pool-c"), 1)
    assert logical_calls == 0
    assert module.external_runtime_pool_snapshot()["coordinator_entries"] == 1

    first.resize_physical_thread_permits(0)
    _clear_external_coordinator(module)


def test_physical_claim_slot_is_published_before_native_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify physical claim slot is published before native commit."""
    from schema_sanitizer.core_impl import process_resources as module

    _clear_external_coordinator(module)

    class Runtime:
        @staticmethod
        def schema_sanitizer_resident_thread_count() -> int:
            """Return the controlled resident-thread count."""
            return 1

    runtime = Runtime()
    events: list[tuple[str, int]] = []

    class FailInsert(dict[int, int]):
        def __setitem__(self, key: int, value: int) -> None:
            """Store the requested value in the fail insert test double."""
            if key not in self:
                raise MemoryError("injected claim-slot OOM")
            super().__setitem__(key, value)

    native = _ExactNative(events)
    native.supports_resident_attribution = False
    monkeypatch.setattr(module, "_native_external_thread_api", lambda: native)
    with module._EXTERNAL_RUNTIME_POOL_COORDINATOR_LOCK:
        entry = module._external_runtime_entry_locked(runtime, create=True)
        assert entry is not None
        entry.physical_claims = FailInsert()
    with pytest.raises(MemoryError, match="claim-slot OOM"):
        module._acquire_shared_external_native_thread_permits(runtime, 2)
    assert not events
    assert module.external_runtime_pool_snapshot()["coordinator_entries"] == 0


def test_logical_claim_slot_is_published_before_governor_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify logical claim slot is published before governor commit."""
    from schema_sanitizer.core_impl import process_resources as module

    _clear_external_coordinator(module)
    runtime = object()
    calls = 0

    class FailInsert(dict[int, int]):
        def __setitem__(self, key: int, value: int) -> None:
            """Store the requested value in the fail insert test double."""
            if key not in self:
                raise MemoryError("injected logical-slot OOM")
            super().__setitem__(key, value)

    def acquire(*_args, **_kwargs):
        """Acquire the resource under the controlled scheduling conditions."""
        nonlocal calls
        calls += 1
        raise AssertionError("governor must not be reached")

    monkeypatch.setattr(module, "acquire_project_threads", acquire)
    with module._EXTERNAL_RUNTIME_POOL_COORDINATOR_LOCK:
        entry = module._external_runtime_entry_locked(runtime, create=True)
        assert entry is not None
        entry.logical_claims = FailInsert()
    with pytest.raises(MemoryError, match="logical-slot OOM"):
        module._acquire_shared_external_logical_thread_lease(runtime, 2)
    assert calls == 0
    assert module.external_runtime_pool_snapshot()["coordinator_entries"] == 0


def test_capacity_shrink_ejects_impossible_fifo_head() -> None:
    """Verify capacity shrink ejects impossible fifo head."""
    from schema_sanitizer.core_impl.process_resources import _Governor

    governor = _Governor(2, "resident-zero-is-authoritative-on-public_shrink", teardown_reserve=0)
    held = governor.acquire(1)
    result: list[str] = []
    entered = threading.Event()

    def wait_big() -> None:
        """Wait until the larger competing acquisition can proceed."""
        entered.set()
        try:
            governor.acquire(2, timeout_seconds=None)
        except Exception as exc:  # exact error type is covered by production tests
            result.append(str(exc))

    thread = threading.Thread(target=wait_big)
    thread.start()
    assert entered.wait(SCHEDULER_TIMEOUT_SECONDS)
    # Ensure the waiter has reached the queue before shrinking.
    deadline = monotonic() + SCHEDULER_TIMEOUT_SECONDS
    while not governor.snapshot().waiting and monotonic() < deadline:
        threading.Event().wait(0.001)
    assert governor.snapshot().waiting == 1
    governor.refresh_capacity(1)
    join_thread_or_fail(thread)
    assert result and "no longer fits refreshed capacity" in result[0]
    assert governor.snapshot().waiting == 0
    held.release()


def test_native_stack_snapshot_resident_and_fd_contracts() -> None:
    """Verify native stack snapshot resident and FD contracts."""
    arena = (CPP / "internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    header = (CPP / "internal/runtime/operation_task_arena.hh").read_text(encoding="utf-8")
    probe = (CPP / "api/python_abi3/runtime/ordered_executor_probe.cc").read_text(encoding="utf-8")

    assert "SCHEMA_SANITIZER_THREAD_STACK_RESERVATION_BYTES" in arena
    assert "ReadNumericEnvironmentVariable" in arena
    assert "GetEnvironmentVariableA" in arena
    assert "std::array<char, 64> configured_storage" in arena
    windows_environment_reader = arena[
        arena.index(
            "#if defined(_WIN32)", arena.index("ReadNumericEnvironmentVariable")
        ) : arena.index("#else", arena.index("ReadNumericEnvironmentVariable"))
    ]
    assert "std::getenv(" not in windows_environment_reader
    assert "RLIMIT_STACK" in arena
    assert "ProcessThreadStackReservationCount" in arena
    assert "ProcessThreadStackReservationCount(total_reserved)" in arena
    assert "return std::max(total_reserved, modelled)" in arena
    assert "g_process_external_runtime_stack_debt_threads" in arena
    assert "std::max(external_active, resident_stack_debt)" in arena
    assert "g_process_thread_ledger_mutations_inflight" in arena
    assert "g_process_thread_ledger_mutation_epoch" in arena
    assert "epoch_before == epoch_after" in arena
    assert "ReadThreadPermitLedgerSnapshot" in arena
    snapshot = arena[
        arena.index("ReadThreadPermitLedgerSnapshot()") : arena.index(
            "process_thread_stack_reservation_bytes"
        )
    ]
    assert "fetch_add(\n        0U, std::memory_order_acq_rel)" in snapshot
    assert "std::atomic_thread_fence(" not in snapshot
    assert snapshot.index("out.total =") < snapshot.index("const auto epoch_after")

    arena_runtime = (CPP / "internal/runtime/operation_task_arena_runtime.cc.inc").read_text(
        encoding="utf-8"
    )
    activity_stop = arena_runtime[
        arena_runtime.index("void Stop() noexcept") : arena_runtime.index(
            "private:", arena_runtime.index("void Stop() noexcept")
        )
    ]
    assert activity_stop.index("running.store(false") < activity_stop.index(
        "SaturatingAtomicSubtract(state_->active"
    )
    assert "thread_permit_snapshot_stable" in header
    assert "g_external_runtime_resident_protocol_violations" in arena
    assert (
        "compare_exchange_weak"
        in arena[arena.index("add_process_external_runtime_resident_threads") :]
    )
    assert "g_process_fd_waiters" in arena
    assert "kProcessFdTicketSlots" in arena
    assert "g_process_fd_next_ticket" in arena
    assert "g_process_fd_serving_ticket" in arena
    assert "g_process_fd_cancelled_tickets" in arena
    assert "TryReserveFdTicket" in arena
    assert "RetireFdTicketLocked" in arena
    assert "g_process_fd_wait_mutex" in arena
    assert "g_process_fd_wait_cv" in arena
    assert "queued_waiter" in arena
    assert "PyTuple_New(30)" in probe
    assert "snapshot.external_runtime_stack_debt_threads" in probe
    assert "py_process_external_runtime_thread_permit_lease_acquire" in probe
    assert "destroy_external_runtime_permit_lease_capsule" in probe
    assert "py_process_file_descriptor_permit_lease_acquire_wait" in probe
    assert "destroy_fd_permit_lease_capsule" in probe
    assert "py_process_external_runtime_thread_permit_lease_resize" in probe
    assert "py_process_file_descriptor_permit_lease_resize" in probe

    resources = (SRC / "core_impl/process_resources.py").read_text(encoding="utf-8")
    assert "external_runtime_pool_coordinator" in resources
    assert "_MAX_EXTERNAL_RUNTIME_POOL_ENTRIES" in resources
    assert "_MAX_EXTERNAL_RUNTIME_POOL_TOTAL_CLAIMS" in resources
    assert "_MAX_EXTERNAL_RUNTIME_POOL_IDENTITY_UNITS" in resources
    assert "_EXTERNAL_RUNTIME_POOL_CLAIM_CONTROL_BYTES" in resources


def test_release_gate_rejects_unstable_or_resident_protocol_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify release gate rejects unstable or resident protocol snapshot."""
    from schema_sanitizer.core_impl import concurrency_coverage as coverage
    from schema_sanitizer.core_impl import runtime_diagnostics

    base = {
        "available": True,
        "snapshot_schema_fields": 30,
        "completion_memory_protocol_violations": 0,
        "counter_underflows": 0,
        "native_physical_threads": 2,
        "external_runtime_thread_permits": 1,
        "total_physical_thread_permits": 3,
        "native_physical_thread_capacity": 8,
        "thread_permit_snapshot_stable": 1,
        "external_runtime_resident_protocol_violations": 0,
        "external_runtime_resident_threads": 2,
        "external_runtime_stack_debt_threads": 2,
    }
    monkeypatch.setattr(runtime_diagnostics, "_native_arena_snapshot", lambda: dict(base))
    coverage.validate_native_concurrency_protocol_health()

    unstable = dict(base, thread_permit_snapshot_stable=0)
    monkeypatch.setattr(runtime_diagnostics, "_native_arena_snapshot", lambda: unstable)
    with pytest.raises(RuntimeError, match="not transactionally stable"):
        coverage.validate_native_concurrency_protocol_health()

    bad_resident = dict(base, external_runtime_resident_protocol_violations=1)
    monkeypatch.setattr(runtime_diagnostics, "_native_arena_snapshot", lambda: bad_resident)
    with pytest.raises(RuntimeError, match="resident-thread protocol violations"):
        coverage.validate_native_concurrency_protocol_health()
