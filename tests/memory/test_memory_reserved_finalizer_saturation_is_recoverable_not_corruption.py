"""Regression coverage for memory reserved finalizer saturation is recoverable not corruption."""

from __future__ import annotations

import re
from pathlib import Path
from threading import Event, Thread

import pytest

ROOT = Path(__file__).resolve().parents[2]

_CPP_IGNORED = re.compile(
    r"//[^\n]*|/\*.*?\*/|(?:u8|u|U|L)?\"(?:\\.|[^\"\\])*\"|"
    r"(?:u8|u|U|L)?'(?:\\.|[^'\\])*'",
    re.DOTALL,
)
_CPP_TOKEN = re.compile(
    r"[A-Za-z_]\w*|\d+(?:'\d+)*(?:[A-Za-z_]\w*)?|::|->|&&|\|\||"
    r"==|!=|<=|>=|\+\+|--|[{}()\[\],;.&*!=<>+\-/]"
)


def _cpp_scope(source: str, signature: str) -> str:
    code = _CPP_IGNORED.sub(" ", source)
    match = re.search(signature, code)
    assert match is not None, signature
    opening = code.find("{", match.end())
    assert opening >= 0, signature
    depth = 0
    for index in range(opening, len(code)):
        if code[index] == "{":
            depth += 1
        elif code[index] == "}":
            depth -= 1
            if depth == 0:
                return code[match.start() : index + 1]
    raise AssertionError(f"unterminated C++ scope: {signature}")


def _cpp_tokens(source: str) -> tuple[str, ...]:
    return tuple(_CPP_TOKEN.findall(_CPP_IGNORED.sub(" ", source)))


def _token_index(tokens: tuple[str, ...], needle: tuple[str, ...], *, start: int = 0) -> int:
    width = len(needle)
    for index in range(start, len(tokens) - width + 1):
        if tokens[index : index + width] == needle:
            return index
    raise AssertionError(f"missing C++ token sequence: {' '.join(needle)}")


def _token_count(tokens: tuple[str, ...], needle: tuple[str, ...]) -> int:
    width = len(needle)
    return sum(tokens[index : index + width] == needle for index in range(len(tokens) - width + 1))


def test_reserved_finalizer_saturation_is_recoverable_not_corruption() -> None:
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    escrow: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(1)
    ticket = escrow.reserve_ticket()
    assert ticket is not None
    assert escrow.reserve_ticket() is None
    saturated = escrow.capacity_snapshot()
    assert saturated.admission_rejections == 1
    assert saturated.publication_failures == 0
    assert saturated.overflowed is False
    escrow.release_ticket(ticket)
    replacement = escrow.reserve_ticket()
    assert replacement is not None and replacement != ticket
    escrow.release_ticket(replacement)


def test_finalizer_snapshot_never_waits_for_reserved_publisher_lock() -> None:
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    escrow: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(1)
    ticket = escrow.reserve_ticket()
    assert ticket is not None
    lock = escrow._slot_locks[0]
    lock.acquire()
    done = Event()
    box: list[object] = []

    def snapshot() -> None:
        box.append(escrow.capacity_snapshot())
        done.set()

    worker = Thread(target=snapshot)
    worker.start()
    assert done.wait(0.5), "observability contended with the finalizer publisher lock"
    lock.release()
    worker.join(timeout=1)
    assert box and box[0].active == 1
    escrow.release_ticket(ticket)


def test_reserved_consumer_does_not_contend_with_unpublished_reserved_slot() -> None:
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    escrow: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(1)
    ticket = escrow.reserve_ticket()
    assert ticket is not None
    lock = escrow._slot_locks[0]
    lock.acquire()
    done = Event()
    result: list[bool] = []

    def consume() -> None:
        result.append(escrow.process_one(lambda _ticket, _value: None))
        done.set()

    worker = Thread(target=consume)
    worker.start()
    assert done.wait(0.5)
    lock.release()
    worker.join(timeout=1)
    assert result == [False]
    assert escrow.publish_reserved(ticket, object())
    assert escrow.process_one(lambda _ticket, _value: None)


def test_control_plane_release_uses_authoritative_amount_not_mutable_ticket() -> None:
    from schema_sanitizer.core_impl.control_plane_budget import _ProcessControlPlaneBudget

    budget = _ProcessControlPlaneBudget()
    ticket = budget.reserve("reserved-finalizer-saturation-is-recoverable-not", 512)
    ticket.amount = 1 << 30
    assert budget.release(ticket)
    snapshot = budget.snapshot()
    assert snapshot.reserved_bytes == 0
    assert snapshot.active_tickets == 0


def test_control_plane_child_reset_replaces_inherited_locked_state() -> None:
    from schema_sanitizer.core_impl.control_plane_budget import _ProcessControlPlaneBudget

    budget = _ProcessControlPlaneBudget()
    budget.prepare_for_fork()
    old_lock = budget._lock
    old_lock.acquire()
    try:
        budget.reset_after_fork()
        ticket = budget.reserve("child-reset", 256)
        assert budget.snapshot().active_tickets == 1
        budget.release(ticket)
    finally:
        old_lock.release()


def test_storage_account_child_reset_replaces_inherited_global_lock() -> None:
    from schema_sanitizer.core_impl import cross_process_storage as storage

    storage._prepare_storage_accounts_for_fork()
    old_lock = storage._ACCOUNT_LOCK
    old_lock.acquire()
    try:
        storage._reset_storage_accounts_after_fork()
        account = storage.open_cross_process_storage_account(123456)
        assert account.token == 1
        storage.close_cross_process_storage_account(account)
    finally:
        old_lock.release()


def test_provider_expiry_index_is_one_node_per_live_key() -> None:
    from schema_sanitizer.remote_impl.provider_throttle import ProviderThrottleGovernor

    governor = ProviderThrottleGovernor(max_tracked_keys=4)
    for index in range(64):
        lease, _delay = governor.try_acquire("same-key")
        assert lease is not None
        lease._release_outcome(outcome="failure", throttled=True, retry_after_seconds=0.0)
        with governor._condition:
            state = governor._states["same-key"]
            state.circuit_open_until = 0.0
            if state.expiry_node is not None:
                governor._expiry_heap.update(state.expiry_node, 0.0)
    assert len(governor._expiry_heap) == 1
    assert governor._expiry_heap.peak_entries == 1
    assert governor.registry_snapshot().stale_expiry_entries == 0


def test_provider_state_admission_failure_does_not_create_finalizer_debt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl.finalizer_cleanup import finalizer_cleanup_snapshot
    from schema_sanitizer.remote_impl import provider_throttle as module

    governor = module.ProviderThrottleGovernor(max_tracked_keys=1)
    before = finalizer_cleanup_snapshot()

    def fail(*_args: object, **_kwargs: object) -> object:
        raise MemoryError("control-plane OOM")

    monkeypatch.setattr(module, "reserve_control_plane", fail)
    with pytest.raises(MemoryError):
        governor.try_acquire("new-key")
    assert finalizer_cleanup_snapshot() == before
    assert governor.registry_snapshot().active_leases == 0


def test_operation_memory_resize_commits_into_mutable_authoritative_entry() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/memory_budget.py").read_text(encoding="utf-8")
    start = source.index("    def _resize_python_lease(")
    end = source.index("\n    def _transfer_python_lease", start)
    body = source[start:end]
    assert "entry.size_bytes = committed_bytes" in body
    assert "self._python_leases[" not in body
    assert "= (" not in body


def test_finalizer_activity_token_changes_across_cardinality_aba() -> None:
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow
    from schema_sanitizer.core_impl.finalizer_registry import (
        finalizer_activity_token,
        register_finalizer_domain,
    )

    escrow: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(1)
    name = f"reserved-finalizer-saturation-is-recoverable-not-aba-{id(escrow)}"
    register_finalizer_domain(
        name,
        drain=lambda: 0,
        snapshot=lambda: (0, 0),
        escrows=((name, escrow),),
    )
    first_ticket = escrow.reserve_ticket()
    assert first_ticket is not None
    before = finalizer_activity_token()
    escrow.release_ticket(first_ticket)
    second_ticket = escrow.reserve_ticket()
    assert second_ticket is not None
    after = finalizer_activity_token()
    assert before != after
    assert escrow.capacity_snapshot().active == 1
    escrow.release_ticket(second_ticket)


def test_active_owner_domains_are_charged_to_control_plane_budget() -> None:
    sources = {
        "process_resource_lease": ROOT / "src/schema_sanitizer/core_impl/process_resources.py",
        "remote_io_waiter": ROOT / "src/schema_sanitizer/remote_impl/io_permits.py",
        "remote_io_capacity_registration": ROOT / "src/schema_sanitizer/remote_impl/io_permits.py",
        "remote_io_submission_reservation": ROOT / "src/schema_sanitizer/remote_impl/io_permits.py",
        "provider_request_lease": ROOT / "src/schema_sanitizer/remote_impl/provider_throttle.py",
        "temporary_storage_lease": ROOT / "src/schema_sanitizer/core_impl/temporary_storage.py",
        "operation_memory_lease": ROOT / "src/schema_sanitizer/core_impl/memory_budget.py",
    }
    for kind, path in sources.items():
        text = path.read_text(encoding="utf-8")
        needle = "process_resource_lease:" if kind == "process_resource_lease" else kind
        assert needle in text and "reserve_control_plane(" in text, kind


def test_composite_parallel_admission_acquires_bytes_before_physical_slots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import memory_budget, process_resources

    order: list[str] = []

    class Execution:
        amount = 2
        released = False

        def release(self) -> None:
            self.released = True

    class MemoryLease:
        def close(self) -> None:
            pass

    class Ledger:
        def acquire(self, _amount: int, *, stage: str):
            order.append("bytes")
            return MemoryLease()

    execution = Execution()
    monkeypatch.setattr(memory_budget, "adaptive_parallel_slots", lambda *_a, **_k: 3)
    monkeypatch.setattr(memory_budget, "current_operation_memory_ledger", lambda: Ledger())
    monkeypatch.setattr(
        process_resources,
        "acquire_project_threads",
        lambda *_a, **_k: order.append("threads") or execution,
    )
    admission = memory_budget.acquire_parallel_admission(
        3,
        per_slot_bytes=1024,
        stage="reserved-finalizer-saturation-is-recoverable-not",
        reserve_bytes=0,
    )
    assert order == ["bytes", "threads"]
    assert admission.slots == 3
    admission.close()
    assert execution.released


def test_56_pair_contracts_are_backed_by_concrete_runtime_implementations() -> None:
    from schema_sanitizer.core_impl.concurrency_coverage import validate_concurrency_pair_contracts

    pair_count, evidence = validate_concurrency_pair_contracts()
    assert pair_count == 49
    assert {item.name for item in evidence} == {
        "transferable_resident_memory_credit",
        "composite_slot_and_byte_admission",
        "process_control_plane_budget",
    }
    assert all(item.implementation_module and item.implementation_name for item in evidence)


def test_native_active_to_completion_uses_single_authoritative_transfer() -> None:
    ordered = (ROOT / "cpp/src/internal/runtime/ordered_executor.hh").read_text(encoding="utf-8")
    arena = (ROOT / "cpp/src/internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    runtime = (ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc").read_text(
        encoding="utf-8"
    )

    publish = _cpp_tokens(_cpp_scope(ordered, r"\bvoid\s+Publish\s*\("))
    transfer_call = (
        "arena",
        "->",
        "TryTransferActiveToCompletion",
        "(",
        "input_retained_bytes",
        ",",
        "retained",
        ",",
        "&",
        "completion_lease",
        ")",
    )
    assert _token_count(publish, transfer_call) == 1
    transfer = _token_index(publish, transfer_call)
    lease_publication = _token_index(
        publish,
        (
            "slot",
            ".",
            "retained_lease",
            "=",
            "std",
            "::",
            "move",
            "(",
            "completion_lease",
            ")",
        ),
    )
    assert transfer < lease_publication
    assert not {"CanRetainCompletionBytesAfterTransfer", "RetainCompletionBytes"}.intersection(
        publish
    )

    arena_transfer = _cpp_tokens(
        _cpp_scope(
            arena,
            r"\bbool\s+OperationTaskArena\s*::\s*TryTransferActiveToCompletion\s*\(",
        )
    )
    authoritative_commit = (
        "scope",
        "->",
        "TryTransfer",
        "(",
        "state",
        ".",
        "get",
        "(",
        ")",
        ",",
        "active_credit",
        ",",
        "completion_bytes",
        ")",
    )
    assert _token_count(arena_transfer, authoritative_commit) == 1
    commit = _token_index(arena_transfer, authoritative_commit)
    lease_construction = _token_index(
        arena_transfer,
        (
            "*",
            "completion_lease",
            "=",
            "CompletionMemoryLease",
            "(",
            "state",
            ",",
            "completion_bytes",
            ")",
        ),
    )
    assert commit < lease_construction
    assert "RetainCompletionBytes" not in arena_transfer

    charge_transfer = _cpp_tokens(_cpp_scope(runtime, r"\bbool\s+TryTransfer\s*\("))
    capacity_guard = _token_index(
        charge_transfer,
        ("without_active", ">", "state_", "->", "queue_byte_capacity", "-", "completion_bytes"),
    )
    cas = _token_index(
        charge_transfer,
        ("state_", "->", "retained_bytes_total", ".", "compare_exchange_weak", "("),
    )
    active_release = _token_index(
        charge_transfer,
        (
            "SaturatingAtomicSubtract",
            "(",
            "state_",
            "->",
            "active_bytes",
            ",",
            "active_credit",
            ")",
        ),
    )
    committed = _token_index(charge_transfer, ("transferred_", "=", "true", ";"))
    assert capacity_guard < cas < active_release < committed

    runtime_tokens = _cpp_tokens(runtime)
    assert _token_count(runtime_tokens, ("class", "ActiveRetainedCharge", ";")) == 1
    assert (
        _token_count(
            runtime_tokens,
            (
                "thread_local",
                "ActiveRetainedCharge",
                "*",
                "g_active_retained_charge",
                "=",
                "nullptr",
                ";",
            ),
        )
        == 1
    )


def test_cross_process_pruning_is_single_pass_and_bounded() -> None:
    storage = (ROOT / "src/schema_sanitizer/core_impl/cross_process_storage.py").read_text(
        encoding="utf-8"
    )
    memory = (ROOT / "src/schema_sanitizer/core_impl/cross_process_memory.py").read_text(
        encoding="utf-8"
    )
    for source in (storage, memory):
        assert "_MAX_PROCESS_RECORDS" in source or "_MAX_PROCESS_LEASE_RECORDS" in source
        assert "stale_keys" in source
        assert "while stale" not in source


def test_shutdown_uses_registered_finalizer_domains_epochs_and_control_budget() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/runtime_shutdown.py").read_text(
        encoding="utf-8"
    )
    assert "write_finalizer_activity_into(" in source
    assert "for domain in finalizer_domains()" in source
    assert "process_control_plane_snapshot()" in source
    assert "control_plane_active" in source
    assert "control_plane_reserved" in source
    assert "publication_failures" in source
    assert "Admission rejection is intentionally *not* terminal corruption" in source


def test_availability_emergency_debt_starts_worker_without_normal_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import process_resources as module

    local_threads = module._Governor(
        1, "reserved-finalizer-saturation-is-recoverable-not-emergency-notifier-thread"
    )
    monkeypatch.setattr(module, "_NOTIFIER_THREAD_GOVERNOR", local_threads)
    notifier = module._AvailabilityNotifier()
    completed = Event()

    class Debt:
        def _retry_dirty_availability(self) -> None:
            completed.set()

    notifier.arm_emergency_republish(Debt())  # type: ignore[arg-type]
    assert completed.wait(1.0)
    assert notifier.close(deadline_seconds=1.0)


def test_availability_dispatcher_is_sealed_per_governor_instance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import process_resources as module

    local_threads = module._Governor(
        1, "reserved-finalizer-saturation-is-recoverable-not-dispatch-isolation-thread"
    )
    monkeypatch.setattr(module, "_NOTIFIER_THREAD_GOVERNOR", local_threads)
    notifier = module._AvailabilityNotifier()
    first = Event()
    stolen = Event()
    governor = module._Governor(
        1,
        "reserved-finalizer-saturation-is-recoverable-not-dispatch-isolation",
        availability_dispatcher=lambda _event: first.set(),
    )
    event = module.AvailabilityEvent.RETRY_SCHEDULER
    assert governor.register_availability_event(event)
    delivery = governor._availability_events[event]
    monkeypatch.setattr(module, "_dispatch_availability_event", lambda _event: stolen.set())
    assert notifier.publish_one(delivery)
    assert first.wait(1.0)
    assert not stolen.is_set()
    assert notifier.close(deadline_seconds=1.0)


def test_availability_dirty_retry_does_not_depend_on_retry_scheduler() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/process_resources.py").read_text(
        encoding="utf-8"
    )
    start = source.index("def _schedule_availability_retry_noexcept")
    end = source.index("\n    def ", start + 8)
    body = source[start:end]
    assert "arm_emergency_republish" in body
    assert "schedule_retry" not in body
    assert "_reserved_thread_lease" in source


def test_remote_delivery_reclaims_batch_tail_before_propagating_baseexception() -> None:
    source = (ROOT / "src/schema_sanitizer/remote_impl/io_permits.py").read_text(encoding="utf-8")
    start = source.index("    def _deliver(")
    end = source.index("\n    def reset_after_fork", start)
    body = source[start:end]
    assert "KeyboardInterrupt, SystemExit" in body
    assert "tail_batch" in body
    assert "_reclaim_granted_waiter_locked" in body
    assert "raise" in body


def test_control_plane_static_baseline_is_part_of_governed_headroom() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/memory_budget.py").read_text(encoding="utf-8")
    assert "exact.reserved_bytes + control.governed_bytes" in source
    assert "snapshot.capacity_bytes - snapshot.reserved_bytes" in source


def test_runtime_shutdown_does_not_import_finalizer_domains_dynamically() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/runtime_shutdown.py").read_text(
        encoding="utf-8"
    )
    phase = source[source.index("# Phase 3:") : source.index("# Phase 4:")]
    assert "__import__" not in phase
    assert "finalizer_domains()" in phase
