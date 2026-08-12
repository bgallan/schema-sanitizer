"""Regression coverage for memory reserved finalizer processed owner cannot stick claimed on recycle failure."""

from __future__ import annotations

import gc
import json
import os
import sys
import tempfile
import threading
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src" / "schema_sanitizer"
CPP = ROOT / "cpp" / "src"


def test_reserved_finalizer_processed_owner_cannot_stick_claimed_on_recycle_failure(
    monkeypatch,
) -> None:
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    escrow: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(2)
    ticket = escrow.reserve_ticket()
    assert ticket is not None
    owner = object()
    assert escrow.publish_reserved(ticket, owner)

    original = ReservedFinalizerEscrow._recycle_one_pending_locked

    def boom(self):
        raise MemoryError("reserved-finalizer-processed-owner-cannot-stick recycle fault")

    monkeypatch.setattr(ReservedFinalizerEscrow, "_recycle_one_pending_locked", boom)
    seen: list[object] = []
    assert escrow.process_one(lambda _ticket, value: seen.append(value)) is True
    assert seen == [owner]
    assert escrow.active_count() == 0
    assert escrow.published_count() == 0
    assert escrow.activity_is_quiescent()

    monkeypatch.setattr(ReservedFinalizerEscrow, "_recycle_one_pending_locked", original)
    # One free slot still exists; consuming it forces scavenging of the pending one.
    first = escrow.reserve_ticket()
    assert first is not None
    second = escrow.reserve_ticket()
    assert second is not None
    assert escrow.release_ticket(first)
    assert escrow.release_ticket(second)


def test_operation_memory_constructor_keeps_owner_when_registration_and_rollback_fail(
    monkeypatch,
) -> None:
    from schema_sanitizer.core_impl import memory_budget as module

    class Ledger:
        def __init__(self) -> None:
            self.reserved = 0
            self.release_calls = 0

        def reserve(self, amount: int, *, stage: str) -> None:
            self.reserved += amount

        def _register_python_lease(self, _owner, _size):
            raise MemoryError("registration fault")

        def release(self, amount: int) -> None:
            self.release_calls += 1
            if self.release_calls == 1:
                raise MemoryError("rollback fault")
            self.reserved -= amount

    ledger = Ledger()
    with pytest.raises(MemoryError, match="registration fault"):
        module.OperationMemoryLease(ledger, 123, "reserved-finalizer-processed-owner-cannot-stick")
    gc.collect()
    module.drain_abandoned_memory_finalizers()
    assert ledger.reserved == 0
    assert ledger.release_calls >= 2


def test_cross_process_growth_repairs_journal_when_local_commit_fails(
    monkeypatch, tmp_path: Path
) -> None:
    from schema_sanitizer.core_impl import cross_process_memory as module

    if module.fcntl is None:
        pytest.skip("cross-process coordination requires POSIX flock")
    monkeypatch.setenv("SCHEMA_SANITIZER_CROSS_PROCESS_MEMORY_RESERVATIONS", "1")
    monkeypatch.setenv("SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    lease = module.CrossProcessMemoryLease(1_000, 10)
    original = module._update_direct_lease_reserved
    injected = {"done": False}

    def fail_once(owner, lease_id, capability, reserved):
        if reserved == 20 and not injected["done"]:
            injected["done"] = True
            raise MemoryError("local commit fault")
        return original(owner, lease_id, capability, reserved)

    monkeypatch.setattr(module, "_update_direct_lease_reserved", fail_once)
    with pytest.raises(MemoryError, match="local commit fault"):
        lease.resize(20)
    assert lease.reserved_bytes == 10
    path = tmp_path / "schema-sanitizer-resident-memory.json"
    state = json.loads(path.read_text())
    assert state["leases"][lease._key]["reserved"] == 10
    monkeypatch.setattr(module, "_update_direct_lease_reserved", original)
    lease.release()
    state = json.loads(path.read_text())
    assert lease._key not in state["leases"]


def test_cross_process_release_retains_journal_cleanup_owner_after_fsync_failure(
    monkeypatch, tmp_path: Path
) -> None:
    from schema_sanitizer.core_impl import cross_process_memory as module

    if module.fcntl is None:
        pytest.skip("cross-process coordination requires POSIX flock")
    monkeypatch.setenv("SCHEMA_SANITIZER_CROSS_PROCESS_MEMORY_RESERVATIONS", "1")
    monkeypatch.setenv("SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    lease = module.CrossProcessMemoryLease(1_000, 12)
    original = module.CrossProcessMemoryLease._write_owner_journal_total
    injected = {"done": False}

    def fail_zero(self, total):
        if total == 0 and not injected["done"]:
            injected["done"] = True
            raise OSError("journal fsync fault")
        return original(self, total)

    monkeypatch.setattr(module.CrossProcessMemoryLease, "_write_owner_journal_total", fail_zero)
    with pytest.raises(OSError, match="journal fsync fault"):
        lease.release()
    assert lease._journal_cleanup_pending is True
    assert lease._lease_id == 0
    assert lease._finalizer_ticket >= 0
    monkeypatch.setattr(module.CrossProcessMemoryLease, "_write_owner_journal_total", original)
    lease.release()
    assert lease._released is True
    assert lease._journal_cleanup_pending is False


def test_external_runtime_failed_post_setter_probe_is_uncertain_and_memory_conservative(
    monkeypatch,
) -> None:
    from schema_sanitizer.core_impl import process_resources as module
    from schema_sanitizer.errors import SchemaSanitizerResourceError

    class Native:
        supports_resident_attribution = True
        supports_stack_debt = True
        supports_atomic_residency_update = False

        def external_runtime_stack_debt_threads_add(self, _amount: int) -> None:
            pass

        def external_runtime_stack_debt_threads_release(self, _amount: int) -> None:
            pass

        def external_runtime_resident_threads_add(self, _amount: int) -> None:
            pass

        def external_runtime_resident_threads_release(self, _amount: int) -> None:
            pass

    monkeypatch.setattr(module, "_native_external_thread_api", lambda: Native())
    runtime = types.ModuleType("pyarrow")
    state = {"width": 8, "fail_verify": True}

    def cpu_count() -> int:
        if state["width"] == 2 and state["fail_verify"]:
            state["fail_verify"] = False
            raise RuntimeError("verify fault")
        return state["width"]

    def set_cpu_count(value: int) -> None:
        state["width"] = value

    runtime.cpu_count = cpu_count  # type: ignore[attr-defined]
    runtime.set_cpu_count = set_cpu_count  # type: ignore[attr-defined]
    prior = sys.modules.get("pyarrow")
    sys.modules["pyarrow"] = runtime
    try:
        with pytest.raises(SchemaSanitizerResourceError):
            module.constrain_external_runtime_worker_pool(runtime, 2)
        key = module._external_runtime_pool_identity_key(runtime)
        with module._EXTERNAL_RUNTIME_POOL_COORDINATOR_LOCK:
            entry = module._EXTERNAL_RUNTIME_POOL_COORDINATOR[key]
            assert entry.config_state == "uncertain"
            assert entry.config_attempted_width == 2
            assert entry.resident_stack_debt >= 2
            assert not entry.config_inflight
        assert module.constrain_external_runtime_worker_pool(runtime, 2) == 2
        with module._EXTERNAL_RUNTIME_POOL_COORDINATOR_LOCK:
            entry = module._EXTERNAL_RUNTIME_POOL_COORDINATOR[key]
            assert entry.config_state == "stable"
        assert module.retire_external_runtime_pool(runtime)
    finally:
        if prior is None:
            sys.modules.pop("pyarrow", None)
        else:
            sys.modules["pyarrow"] = prior


def test_external_runtime_retirement_predicate_blocks_inflight_configuration() -> None:
    from schema_sanitizer.core_impl import process_resources as module

    key = ("declared", "reserved-finalizer-processed-owner-cannot-stick-config-inflight")
    entry = module._ExternalRuntimePoolCoordinatorEntry(runtime=None, runtime_key=key)
    entry.config_inflight = True
    entry.config_state = "inflight"
    with module._EXTERNAL_RUNTIME_POOL_COORDINATOR_LOCK:
        module._EXTERNAL_RUNTIME_POOL_COORDINATOR[key] = entry
        module._retire_external_runtime_entry_locked(key, entry)
        assert module._EXTERNAL_RUNTIME_POOL_COORDINATOR.get(key) is entry
        entry.config_inflight = False
        entry.config_state = "stable"
        module._retire_external_runtime_entry_locked(key, entry)
        assert key not in module._EXTERNAL_RUNTIME_POOL_COORDINATOR


def test_temporary_storage_move_keeps_replacement_rooted_when_both_releases_fail(
    monkeypatch,
) -> None:
    from schema_sanitizer.core_impl.temporary_storage_governor import (
        _ProcessTemporaryStorageGovernor,
    )

    governor = _ProcessTemporaryStorageGovernor()
    old = governor.reserve_capability(
        1, path=tempfile.gettempdir(), label="reserved-finalizer-processed-owner-cannot-stick-old"
    )
    old_device = old.device
    real_filesystem = governor.filesystem
    real_release = governor._release_capability_exact

    def fake_filesystem(path):
        _device, target, free = real_filesystem(path)
        return old_device + 1, target, free

    monkeypatch.setattr(governor, "filesystem", fake_filesystem)
    captured = {"replacement": None}

    def fail_release(capability):
        if capability is old:
            raise RuntimeError("old release fault")
        captured["replacement"] = capability
        raise RuntimeError("replacement rollback fault")

    monkeypatch.setattr(governor, "_release_capability_exact", fail_release)
    with pytest.raises(RuntimeError):
        governor.resize_capability(
            old,
            2,
            path=tempfile.gettempdir(),
            label="reserved-finalizer-processed-owner-cannot-stick-move",
        )
    replacement = old.pending_replacement
    assert replacement is not None
    assert replacement.active

    monkeypatch.setattr(governor, "_release_capability_exact", real_release)
    assert governor.release_capability(old) is True
    assert old.pending_replacement is None
    assert governor.authoritative_snapshot().reserved_bytes == 0


def test_temporary_storage_resize_does_not_hold_pool_condition_across_process_resize(
    monkeypatch,
) -> None:
    from schema_sanitizer.core_impl import temporary_storage as module

    monkeypatch.setattr(
        module,
        "memory_budget",
        lambda _limit: types.SimpleNamespace(replay_spool_bytes=1 << 20),
    )
    pool = module.TemporaryStoragePermitPool(1 << 20)
    first = pool.acquire(
        1, label="reserved-finalizer-processed-owner-cannot-stick-first", path=tempfile.gettempdir()
    )
    entered = threading.Event()
    resume = threading.Event()
    original = module._PROCESS_TEMPORARY_STORAGE.resize_capability

    def blocking_resize(*args, **kwargs):
        entered.set()
        assert resume.wait(5)
        return original(*args, **kwargs)

    monkeypatch.setattr(module._PROCESS_TEMPORARY_STORAGE, "resize_capability", blocking_resize)
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            first.resize(2)
        except BaseException as exc:  # pragma: no cover - diagnostic capture
            errors.append(exc)

    thread = threading.Thread(target=worker)
    thread.start()
    assert entered.wait(5)
    # Another lease must still be able to use the pool while filesystem/journal
    # work for the first lease is blocked outside the condition.
    second = pool.acquire(
        1,
        label="reserved-finalizer-processed-owner-cannot-stick-second",
        path=tempfile.gettempdir(),
    )
    resume.set()
    thread.join(5)
    assert not thread.is_alive()
    assert errors == []
    second.release()
    first.release()
    pool.close()


def test_staged_result_terminal_consume_retires_prepared_finalizer_slot() -> None:
    from schema_sanitizer.core_impl.finalizer_cleanup import prepared_finalizer_capacity_snapshot
    from schema_sanitizer.remote_impl.staged_ownership import StagedResultOwnership

    baseline = prepared_finalizer_capacity_snapshot()[1]
    ownership = StagedResultOwnership()
    payload = object()
    ownership.publish(payload)
    assert prepared_finalizer_capacity_snapshot()[1] == baseline + 1
    assert ownership.consume(payload) is payload
    assert prepared_finalizer_capacity_snapshot()[1] == baseline
    assert ownership._finalizer_capsule is None


def test_native_residency_update_rejects_final_debt_below_identity() -> None:
    arena = (CPP / "internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    body = arena[arena.index("void update_process_external_runtime_residency") :]
    assert "target_debt < target_identity" in body
    assert "g_external_runtime_resident_protocol_violations" in body
    add_identity = arena[arena.index("void add_process_external_runtime_resident_threads") :]
    assert "if (target > debt)" in add_identity
    release_debt = arena[arena.index("void release_process_external_runtime_stack_debt_threads") :]
    assert "debt - amount < identity" in release_debt


def test_inflight_latches_prepare_allocating_counters_before_publish() -> None:
    staged = (SRC / "remote_impl/staged_ownership.py").read_text(encoding="utf-8")
    lookahead = (SRC / "pipeline/partition_lookahead.py").read_text(encoding="utf-8")
    resources = (SRC / "core_impl/process_resources.py").read_text(encoding="utf-8")
    staged_body = staged[
        staged.index("def _claim_cleanup_locked") : staged.index("def _finish_cleanup")
    ]
    assert staged_body.index("next_generation =") < staged_body.index(
        "self._cleanup_inflight = True"
    )
    config_body = resources[resources.index("def constrain_external_runtime_worker_pool") :]
    assert config_body.index("next_generation =") < config_body.index(
        "entry.config_inflight = True"
    )
    take = lookahead[lookahead.index("def take_next") : lookahead.index("def _current_options")]
    assert take.index("next_submissions =") < take.index("self._consumer_inflight = True")


def test_release_ticket_authority_is_not_destroyed_before_retirement_commit() -> None:
    memory = (SRC / "core_impl/memory_budget.py").read_text(encoding="utf-8")
    temp = (SRC / "core_impl/temporary_storage.py").read_text(encoding="utf-8")
    cross = (SRC / "core_impl/cross_process_memory.py").read_text(encoding="utf-8")
    assert (
        "if _MEMORY_LEASE_FINALIZER_ESCROW.release_ticket(ticket):\n                    self._finalizer_ticket = None"
        in memory
    )
    assert (
        "if _TEMP_STORAGE_FINALIZER_ESCROW.release_ticket(ticket):\n                    self._finalizer_ticket = -1"
        in temp
    )
    assert (
        "if _DIRECT_CROSS_MEMORY_FINALIZER_ESCROW.release_ticket(ticket):\n                    self._finalizer_ticket = -1"
        in cross
    )


def test_finalizer_success_tail_has_owner_free_recycle_state() -> None:
    source = (SRC / "core_impl/finalizer_escrow.py").read_text(encoding="utf-8")
    body = source[
        source.index("    def process_one", source.index("class ReservedFinalizerEscrow")) :
    ]
    # Owner-first cleanup publishes a no-replay PROCESSED marker before retirement;
    # only owner-free RECYCLE_PENDING slots are returned to admission capacity.
    assert "self._states[slot] = _PROCESSED" in body
    assert "self._states[slot] = _RECYCLE_PENDING" in body
    assert body.index("self._states[slot] = _PROCESSED") < body.index(
        "self._states[slot] = _RECYCLE_PENDING"
    )
    assert "self._recycle_one_pending_locked()" in body
    assert "PROCESSED is intentionally not rolled back" in body


def test_temporary_storage_post_commit_baseexception_rolls_back_exact_capability(
    monkeypatch,
) -> None:
    from schema_sanitizer.core_impl import temporary_storage_governor as module

    governor = module._ProcessTemporaryStorageGovernor()

    def interrupt(**_kwargs):
        raise KeyboardInterrupt(
            "reserved-finalizer-processed-owner-cannot-stick post-commit interrupt"
        )

    monkeypatch.setattr(module, "record_resource_telemetry", interrupt)
    with pytest.raises(KeyboardInterrupt, match="post-commit interrupt"):
        governor.reserve_capability(
            1,
            path=tempfile.gettempdir(),
            label="reserved-finalizer-processed-owner-cannot-stick-async",
        )
    snapshot = governor.authoritative_snapshot()
    assert snapshot.reserved_bytes == 0
    assert snapshot.reserved_inodes == 0
    assert not governor._capabilities


def test_temporary_storage_failed_post_commit_rollback_remains_globally_rooted(monkeypatch) -> None:
    from schema_sanitizer.core_impl import temporary_storage_governor as module

    governor = module._ProcessTemporaryStorageGovernor()
    telemetry_calls = {"count": 0}

    def interrupt(**_kwargs):
        telemetry_calls["count"] += 1
        if telemetry_calls["count"] == 1:
            raise KeyboardInterrupt(
                "reserved-finalizer-processed-owner-cannot-stick post-commit interrupt"
            )

    monkeypatch.setattr(module, "record_resource_telemetry", interrupt)
    real_release = governor._release_capability_exact
    release_calls = {"count": 0}

    def fail_once(capability):
        release_calls["count"] += 1
        if release_calls["count"] == 1:
            raise MemoryError("reserved-finalizer-processed-owner-cannot-stick rollback fault")
        return real_release(capability)

    monkeypatch.setattr(governor, "_release_capability_exact", fail_once)
    with pytest.raises(KeyboardInterrupt, match="post-commit interrupt"):
        governor.reserve_capability(
            1,
            path=tempfile.gettempdir(),
            label="reserved-finalizer-processed-owner-cannot-stick-orphan",
        )
    assert governor.authoritative_snapshot().reserved_bytes == 1
    rooted = [cap for cap in governor._capabilities.values() if cap.orphaned]
    assert len(rooted) == 1 and rooted[0].active

    # The next admission safe point retries the exact rooted capability before
    # publishing any new aggregate owner.
    monkeypatch.setattr(governor, "_release_capability_exact", real_release)
    zero = governor.reserve_capability(
        0, path=tempfile.gettempdir(), label="reserved-finalizer-processed-owner-cannot-stick-drain"
    )
    assert zero.active is False
    assert governor.authoritative_snapshot().reserved_bytes == 0
    assert not governor._capabilities


def test_temporary_storage_control_ticket_failure_does_not_repeat_physical_release(
    monkeypatch,
) -> None:
    from schema_sanitizer.core_impl import temporary_storage as module

    monkeypatch.setattr(
        module,
        "memory_budget",
        lambda _limit: types.SimpleNamespace(replay_spool_bytes=1 << 20),
    )
    pool = module.TemporaryStoragePermitPool(1 << 20)
    lease = pool.acquire(
        7,
        label="reserved-finalizer-processed-owner-cannot-stick-release-tail",
        path=tempfile.gettempdir(),
    )
    lease_id = lease._lease_id
    original = module.release_control_plane
    calls = {"count": 0}

    def fail_once(ticket):
        calls["count"] += 1
        if calls["count"] == 1:
            raise MemoryError(
                "reserved-finalizer-processed-owner-cannot-stick control release fault"
            )
        return original(ticket)

    monkeypatch.setattr(module, "release_control_plane", fail_once)
    with pytest.raises(MemoryError, match="control release fault"):
        lease.release()
    # Physical and local bytes are already gone, but the exact entry remains as
    # the sole owner of the not-yet-retired control-plane ticket.
    assert module._PROCESS_TEMPORARY_STORAGE.authoritative_snapshot().reserved_bytes == 0
    with pool._condition:
        entry = pool._leases[lease_id]
        assert entry.process_released and entry.local_released
        assert entry.control_ticket is not None

    monkeypatch.setattr(module, "release_control_plane", original)
    lease.release()
    with pool._condition:
        assert lease_id not in pool._leases
    assert module._PROCESS_TEMPORARY_STORAGE.authoritative_snapshot().reserved_bytes == 0
    pool.close()


def test_memory_lease_post_release_observation_fault_cannot_double_debit() -> None:
    from threading import Lock

    from schema_sanitizer.core_impl import memory_budget as module

    class Native:
        def __init__(self) -> None:
            self.releases = 0

        def operation_memory_ledger_release(self, _capsule, amount):
            assert amount == 9
            self.releases += 1

        def operation_memory_ledger_snapshot(self, _capsule):
            raise MemoryError("reserved-finalizer-processed-owner-cannot-stick observation fault")

    ledger = object.__new__(module.OperationMemoryLedger)
    ledger._pid = os.getpid()
    ledger._lock = Lock()
    ledger._cross_process_io_lock = Lock()
    ledger._capsule = object()
    ledger._native = Native()
    ledger._close_started = False
    ledger._post_release_observation_failures = 0
    ledger._python_lease_sequence = 1
    cap = object()
    entry = module._PythonMemoryLeaseEntry(id(ledger), cap, 9, None)
    # Use a tiny owner shell whose identity is authenticated by the entry.
    owner = types.SimpleNamespace(_lease_id=1, _capability=cap)
    entry.owner_id = id(owner)
    ledger._python_leases = {1: entry}
    ledger._unknown_python_lease_releases = 0
    ledger._finalizer_ticket = None

    ledger._release_python_lease(owner)
    assert ledger._native.releases == 1
    assert 1 not in ledger._python_leases
    assert entry.physical_released is True


def test_remote_io_control_tail_failure_retains_exact_owner_without_double_release(
    monkeypatch,
) -> None:
    from schema_sanitizer.remote_impl import io_permits as module

    governor = module.RemoteIoPermitGovernor(2)
    capability = object()
    ticket = object()
    governor._permit_owners[1] = module._CapabilityEntry(
        1,
        capability,
        1,
        control_ticket=ticket,  # type: ignore[arg-type]
    )
    governor._in_use = 1
    calls = {"count": 0}

    def fail_once(value):
        assert value is ticket
        calls["count"] += 1
        if calls["count"] == 1:
            raise MemoryError(
                "reserved-finalizer-processed-owner-cannot-stick remote control fault"
            )
        return True

    monkeypatch.setattr(module, "release_control_plane", fail_once)
    with pytest.raises(MemoryError, match="remote control fault"):
        governor._release_permit_capability(1, capability)
    assert governor._in_use == 0
    assert governor._permit_owners[1].resource_released is True
    assert governor._permit_owners[1].control_ticket is ticket

    governor._release_permit_capability(1, capability)
    assert governor._in_use == 0
    assert not governor._permit_owners
