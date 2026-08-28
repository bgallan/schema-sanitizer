"""Regression coverage for memory reserved finalizer escrow generation blocks stale ticket aba."""

from __future__ import annotations

import gc
import time
import weakref
from pathlib import Path
from threading import Event, Thread
from types import SimpleNamespace


def _compact_noop(_value: str) -> None:
    return


import pytest


def test_reserved_finalizer_escrow_generation_blocks_stale_ticket_aba() -> None:
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    escrow: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(1)
    first = escrow.reserve_ticket()
    assert first is not None
    owner = object()
    assert escrow.publish_reserved(first, owner)
    seen: list[object] = []
    assert escrow.process_one(lambda _ticket, value: seen.append(value))
    assert seen == [owner]
    second = escrow.reserve_ticket()
    assert second is not None and second != first
    escrow.release_ticket(first)  # stale generation must not free second
    assert escrow.reserve_ticket() is None
    escrow.release_ticket(second)
    assert escrow.reserve_ticket() is not None


def test_reserved_finalizer_escrow_keeps_owner_rooted_on_consumer_oom() -> None:
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    escrow: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(1)
    ticket = escrow.reserve_ticket()
    assert ticket is not None
    owner = bytearray(1 << 20)

    # Use a weakref-capable wrapper so liveness is observable.
    class Box:
        pass

    box = Box()
    box.payload = owner
    box_ref = weakref.ref(box)
    assert escrow.publish_reserved(ticket, box)
    del box
    with pytest.raises(MemoryError):
        escrow.process_one(lambda _ticket, _owner: (_ for _ in ()).throw(MemoryError()))
    gc.collect()
    assert box_ref() is not None
    assert escrow.published_count() == 1
    assert escrow.reserved_count() == 1


def test_reserved_finalizer_fork_reset_roots_inherited_owner() -> None:
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    destroyed: list[int] = []

    class Owner:
        def __del__(self) -> None:
            destroyed.append(1)

    escrow: ReservedFinalizerEscrow[Owner] = ReservedFinalizerEscrow(1)
    ticket = escrow.reserve_ticket()
    assert ticket is not None
    owner = Owner()
    assert escrow.publish_reserved(ticket, owner)
    escrow.prepare_for_fork()
    escrow.reset_after_fork()
    del owner
    gc.collect()
    assert destroyed == []
    assert escrow.reserved_count() == 0


def test_runtime_close_all_keeps_logically_closed_live_thread_registered() -> None:
    from schema_sanitizer.core_impl.durations import deadline_ns_from_timeout
    from schema_sanitizer.core_impl.runtime_registry import _RuntimeServiceRegistry

    class Service:
        def __init__(self) -> None:
            self.calls = 0

        def close(self, *, deadline_seconds: float) -> bool:
            self.calls += 1
            return True

    registry = _RuntimeServiceRegistry()
    service = Service()
    registration = registry.reserve(
        service, kind="reserved-finalizer-escrow-generation-blocks-stale", close_name="close"
    )
    exit_event = Event()
    thread = Thread(target=lambda: exit_event.wait(1.0))
    registration.start_thread(thread)
    closed, remaining = registry.close_all(
        deadline_ns=deadline_ns_from_timeout(
            0.06, name="reserved-finalizer-escrow-generation-blocks-stale"
        )
    )
    assert closed == 0 and remaining == 1
    assert registry.snapshot().registered_services == 1
    # 50-ms bounded re-probes, not an unbounded hot loop.
    assert service.calls < 10
    exit_event.set()
    thread.join(1.0)
    registration.close()
    assert registry.snapshot().registered_services == 0


class _FailingSetDict(dict):
    def __setitem__(self, key, value):  # type: ignore[no-untyped-def]
        raise MemoryError("injected ledger publication OOM")


def test_process_governor_capability_oom_does_not_commit_physical_count() -> None:
    from schema_sanitizer.core_impl.process_resources import _Governor

    governor = _Governor(2, "reserved-finalizer-escrow-generation-blocks-stale")
    governor._active_leases = _FailingSetDict()
    with pytest.raises(MemoryError):
        governor.acquire(1)
    snap = governor.snapshot()
    assert snap.in_use == 0
    assert snap.active_leases == 0


def test_provider_throttle_capability_oom_does_not_commit_inflight() -> None:
    from schema_sanitizer.remote_impl.provider_throttle import ProviderThrottleGovernor

    governor = ProviderThrottleGovernor()
    governor._active_leases = _FailingSetDict()
    with pytest.raises(MemoryError):
        governor.try_acquire("endpoint")
    snap = governor.snapshot("endpoint")
    assert snap.in_flight == 0
    assert governor.registry_snapshot().active_leases == 0


def test_remote_submission_capability_oom_does_not_commit_pending() -> None:
    from schema_sanitizer.remote_impl.io_permits import RemoteIoPermitGovernor

    governor = RemoteIoPermitGovernor(capacity=1)
    governor._submission_owners = _FailingSetDict()
    with pytest.raises(MemoryError):
        governor.reserve_submission()
    snap = governor.snapshot()
    assert snap.pending_submissions == 0
    assert snap.active_submission_reservations == 0


def test_remote_io_fork_reset_drops_inherited_capability_ledgers() -> None:
    from schema_sanitizer.remote_impl.io_permits import RemoteIoPermitGovernor

    governor = RemoteIoPermitGovernor(capacity=2)
    submission = governor.reserve_submission()
    registration = governor.register_capacity(2)
    assert governor.snapshot().active_submission_reservations == 1
    assert governor.snapshot().active_capacity_registrations == 1
    governor.reset_after_fork()
    snap = governor.snapshot()
    assert snap.in_use == 0
    assert snap.active_permits == 0
    assert snap.active_submission_reservations == 0
    assert snap.active_capacity_registrations == 0
    # Inherited wrappers are now non-authoritative in the reset runtime.
    submission._pid = -1
    registration._pid = -1


def test_remote_capacity_registration_has_hard_ceiling() -> None:
    from schema_sanitizer.errors import SchemaSanitizerResourceError
    from schema_sanitizer.remote_impl.io_permits import RemoteIoPermitGovernor

    governor = RemoteIoPermitGovernor(capacity=1, max_capacity_registrations=1)
    registration = governor.register_capacity(1)
    with pytest.raises(SchemaSanitizerResourceError):
        governor.register_capacity(1)
    snap = governor.snapshot()
    assert snap.capacity_registration_capacity == 1
    assert snap.rejected_capacity_registrations == 1
    registration.release()


def test_cleanup_dispatcher_oom_rolls_back_owner_charge(monkeypatch: pytest.MonkeyPatch) -> None:
    from schema_sanitizer.core_impl.cleanup_dispatcher import _CleanupDispatcher

    dispatcher = _CleanupDispatcher()
    monkeypatch.setattr(dispatcher, "_ensure_workers", lambda: None)
    monkeypatch.setattr(
        dispatcher,
        "_enqueue_runnable_locked",
        lambda _call: (_ for _ in ()).throw(MemoryError("queue OOM")),
    )
    with pytest.raises(MemoryError):
        dispatcher.submit(_compact_noop, "compact", retained_bytes=1024)
    snap = dispatcher.snapshot()
    assert snap.owned_calls == 0
    assert snap.owned_bytes == 0
    assert snap.pending_calls == 0


def test_compact_callback_rejects_huge_python_int_without_materializing_bytes() -> None:
    from schema_sanitizer.core_impl.compact_callback import _is_compact_value

    huge = 1 << (16 * 1024 * 1024)
    assert not _is_compact_value(huge)
    assert _is_compact_value(123)


def test_temporary_storage_publication_oom_rolls_back_pending_and_process_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from schema_sanitizer.core_impl import temporary_storage as module

    monkeypatch.setattr(
        module, "memory_budget", lambda _limit: SimpleNamespace(replay_spool_bytes=100)
    )
    monkeypatch.setattr(
        module._PROCESS_TEMPORARY_STORAGE, "filesystem", lambda _path: (7, tmp_path, 1 << 30)
    )
    released: list[tuple[int, int]] = []
    capability = SimpleNamespace(device=7)
    monkeypatch.setattr(
        module._PROCESS_TEMPORARY_STORAGE,
        "reserve_capability",
        lambda amount, *, path, label, inode_count: capability,
    )
    monkeypatch.setattr(
        module._PROCESS_TEMPORARY_STORAGE,
        "release_capability",
        lambda owner: released.append((owner.device, 10)) or True,
    )
    pool = module.TemporaryStoragePermitPool(None)
    pool._leases = _FailingSetDict()
    with pytest.raises(MemoryError):
        pool.try_acquire(
            10, label="reserved-finalizer-escrow-generation-blocks-stale", path=tmp_path
        )
    assert pool._pending_reserved_bytes == 0
    assert pool._pending_active_leases == 0
    assert pool.snapshot().reserved_bytes == 0
    assert pool.snapshot().active_leases == 0
    assert released == [(7, 10)]


def test_cross_process_memory_contribution_oom_precedes_physical_growth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import cross_process_memory as module

    monkeypatch.setattr(module, "_enabled", lambda: False)
    coordinator = module._ProcessCrossMemoryCoordinator(1 << 30)
    coordinator._contribution_owners = _FailingSetDict()
    with pytest.raises(MemoryError):
        coordinator.acquire(8 << 20)
    assert coordinator._physical_bytes == 0
    assert coordinator._contributions == {}
    assert coordinator._finalizer_releases.reserved_count() == 0


def test_path_claim_owner_uses_pre_reserved_generation_ticket() -> None:
    from schema_sanitizer.core_impl.path_identity import PathClaimOwner

    owner = PathClaimOwner(None, None, None)
    assert type(owner.finalizer_ticket) is int and owner.finalizer_ticket >= 0
    owner.__del__()
    # The same generation is published; explicit safe-point release remains authoritative.
    from schema_sanitizer.core_impl.path_identity import (
        _drain_path_claim_finalizers,
        path_claim_finalizer_snapshot,
    )

    assert owner.finalizer_ticket == -1
    assert path_claim_finalizer_snapshot()[0] == 1
    assert _drain_path_claim_finalizers(limit=1) == 1
    assert path_claim_finalizer_snapshot()[0] == 0


def test_result_wrapper_exposes_safe_point_close_for_finalizer_cleanup() -> None:
    source = Path("src/schema_sanitizer/api_impl/results.py").read_text()
    result_block = source[source.index("class Result") : source.index("class SinkResult")]
    assert "def close(self)" in result_block
    assert "self._clean_data_cache = self._UNSET" in result_block
    assert "self._table_cache = self._UNSET" in result_block


def test_cpp_retention_traits_sum_source_and_output_and_rollback_private_cursor() -> None:
    source = Path("cpp/src/internal/runtime/ordered_executor.hh").read_text()
    assert "SaturatingRetainedAdd(source_hint, output_hint)" in source
    assert "AdditionalInlineOwnedBytes(value.rows)" in source
    assert "AdditionalInlineOwnedBytes(value.nodes)" in source
    private = source[
        source.index("retained-byte charge exceeds private") : source.index("// Stop accepting")
    ]
    assert private.index("try {") < private.index("ScheduledPacket scheduled")
    assert "completion_ring_.RollbackSubmit();" in private
    assert "TryTransferActiveToCompletion" in source


def test_process_lease_finalizer_publishes_compact_capability_only() -> None:
    from schema_sanitizer.core_impl.finalizer_cleanup import drain_finalizer_cleanup
    from schema_sanitizer.core_impl.process_resources import _Governor

    drain_finalizer_cleanup()
    governor = _Governor(1, "reserved-finalizer-escrow-generation-blocks-stale-finalizer")
    lease = governor.acquire(1)
    del lease
    gc.collect()
    # snapshot() is itself a safe point and drains prepared finalizers, so
    # inspect the authoritative ledger directly before the explicit drain.
    assert governor._in_use == 1
    assert len(governor._active_leases) == 1
    assert drain_finalizer_cleanup() >= 1
    assert governor.snapshot().in_use == 0
    assert governor.snapshot().active_leases == 0


def test_remote_and_provider_finalizers_release_from_capability_ledgers() -> None:
    from schema_sanitizer.core_impl.finalizer_cleanup import drain_finalizer_cleanup
    from schema_sanitizer.remote_impl.io_permits import RemoteIoPermitGovernor
    from schema_sanitizer.remote_impl.provider_throttle import ProviderThrottleGovernor

    drain_finalizer_cleanup()
    remote = RemoteIoPermitGovernor(capacity=1)
    submission = remote.reserve_submission()
    del submission
    provider = ProviderThrottleGovernor()
    lease, _delay = provider.try_acquire("endpoint")
    assert lease is not None
    del lease
    gc.collect()
    assert remote.snapshot().pending_submissions == 1
    assert provider.snapshot("endpoint").in_flight == 1
    assert drain_finalizer_cleanup() >= 2
    assert remote.snapshot().pending_submissions == 0
    assert provider.snapshot("endpoint").in_flight == 0


def test_result_finalizer_detaches_wrapper_but_roots_large_graph_until_safe_point() -> None:
    from schema_sanitizer.api_impl.results import Result
    from schema_sanitizer.core_impl.finalizer_cleanup import drain_finalizer_cleanup

    class Raw:
        def __init__(self) -> None:
            self.closed = 0

        def close(self) -> None:
            self.closed += 1

    class Box:
        pass

    drain_finalizer_cleanup()
    raw = Raw()
    box = Box()
    box.payload = bytearray(8 << 20)
    box_ref = weakref.ref(box)
    result = Result(raw, clean_data=box)
    result_ref = weakref.ref(result)
    del box, result
    gc.collect()
    # The wrapper itself is gone; only its detached cleanup state is retained.
    assert result_ref() is None
    assert box_ref() is not None
    assert raw.closed == 0
    assert drain_finalizer_cleanup() >= 1
    gc.collect()
    assert raw.closed == 1
    assert box_ref() is None


def test_terminal_ownership_rejection_latch_survives_counter_oom() -> None:
    from schema_sanitizer.core_impl.terminal_ownership import TerminalOwnershipLedger

    class ExplodingInt(int):
        def __add__(self, _other: object):
            raise MemoryError("injected diagnostic counter OOM")

    ledger = TerminalOwnershipLedger(capacity=1)
    assert ledger.publish("reserved-finalizer-escrow-generation-blocks-stale", 1)
    ledger._rejected = ExplodingInt(0)
    assert not ledger.publish("reserved-finalizer-escrow-generation-blocks-stale", 2)
    assert ledger.snapshot().rejected >= 1


def test_large_budgeted_payloads_use_prepared_lease_capsules_instead_of_self() -> None:
    source = Path("src/schema_sanitizer/remote_impl/transport.py").read_text()
    block = source[source.index("class _BudgetedBytes") : source.index("class _HttpStatusError")]
    assert "reserve_resource_finalizer_cleanup(lease)" in block
    assert "defer_prepared_finalizer_cleanup" in block
    assert "defer_finalizer_cleanup(self)" not in block


def test_provider_registry_limit_rejects_coercible_non_integer() -> None:
    from schema_sanitizer.remote_impl.provider_throttle import ProviderThrottleGovernor

    with pytest.raises(TypeError):
        ProviderThrottleGovernor(max_tracked_keys=True)


def test_production_finalizers_do_not_publish_rich_self_owners() -> None:
    root = Path("src/schema_sanitizer")
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        text = path.read_text()
        if "defer_finalizer_cleanup(self)" not in text:
            continue
        offenders.append(path.as_posix())
    assert offenders == []


def test_cpp_arena_submission_rolls_back_completion_on_throwing_publication() -> None:
    header = Path("cpp/src/internal/runtime/ordered_executor.hh").read_text()
    high_core = Path("cpp/src/internal/runtime/ordered_executor_submission.cc.inc").read_text()
    execution = Path("cpp/src/internal/runtime/ordered_executor_execution.cc.inc").read_text()
    assert "ExternalLease external_lease(shared, completion_shard);" in header
    arena_block = header[
        header.index("ExternalLease external_lease") : header.index("if (!submit_status.ok())")
    ]
    assert "catch (const std::bad_alloc &)" in arena_block
    assert "completion_ring_.RollbackSubmit();" in arena_block
    assert "Packet &&packet" in execution
    assert "completion_ring_.RollbackSubmit();" in high_core


def test_prepared_capsule_self_publishes_unused_reserved_ticket() -> None:
    from schema_sanitizer.core_impl import finalizer_cleanup as module

    escrow = module._PREPARED_FINALIZER_ESCROW
    capsule = module.reserve_reference_finalizer_cleanup()
    ticket = capsule.ticket
    authority = capsule._authority
    slot = escrow._ticket_slots[ticket]
    assert escrow._tickets[slot] == ticket
    assert escrow._slots[slot] is authority

    # Authenticate this exact pre-reserved generation while its wrapper dies.
    # A process-global cleanup count may also fall when this GC or drain retires
    # unrelated owners, so it cannot prove publication or retirement here.
    with escrow._slot_locks[slot]:
        del capsule
        gc.collect()
        assert authority.is_armed_for(ticket)
        assert escrow._ticket_slots[ticket] == slot
        assert escrow._tickets[slot] == ticket
        assert escrow._slots[slot] is authority

    deadline = time.monotonic() + 1.0
    while (
        ticket in escrow._ticket_slots
        or authority.is_armed_for(ticket)
        or authority.ticket == ticket
    ):
        module.drain_finalizer_cleanup()
        if (
            ticket not in escrow._ticket_slots
            and not authority.is_armed_for(ticket)
            and authority.ticket == 0
        ):
            break
        if time.monotonic() >= deadline:
            pytest.fail("exact unused prepared finalizer generation was not retired")
        time.sleep(0)

    assert ticket not in escrow._ticket_slots
    assert not authority.is_armed_for(ticket)
    assert authority.ticket == 0
    assert escrow._slots[slot] is not authority
