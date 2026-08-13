"""Regression coverage for memory remote io cancel after grant delivers successor."""

from __future__ import annotations

import asyncio
import inspect
import os
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Any

import pytest
from _support.synchronization import SCHEDULER_TIMEOUT_SECONDS, ContentionObservedLock


def test_remote_io_cancel_after_grant_delivers_successor() -> None:
    from schema_sanitizer.remote_impl.io_permits import RemoteIoPermitGovernor

    async def scenario() -> None:
        governor = RemoteIoPermitGovernor(capacity=1)
        original_deliver = governor._deliver
        held: list[object] = []

        def hold_first_nonempty(waiters: list[object]) -> None:
            if waiters and not held:
                held.extend(waiters)
                return
            original_deliver(waiters)  # type: ignore[arg-type]

        governor._deliver = hold_first_nonempty  # type: ignore[method-assign]
        first = asyncio.create_task(governor.acquire(operation_id="a"))
        await asyncio.sleep(0)
        second = asyncio.create_task(governor.acquire(operation_id="b"))
        await asyncio.sleep(0)
        assert governor.snapshot().in_use == 1
        assert governor.snapshot().waiting == 1

        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        permit = await asyncio.wait_for(second, timeout=1.0)
        assert governor.snapshot().waiting == 0
        assert governor.snapshot().in_use == 1
        permit.release()
        assert governor.snapshot().in_use == 0

    asyncio.run(scenario())


def test_runtime_close_cannot_hide_concurrently_starting_thread() -> None:
    from schema_sanitizer.core_impl.runtime_registry import _RuntimeServiceRegistry

    class Service:
        def close(self, *, deadline_seconds: float) -> bool:
            return True

    registry = _RuntimeServiceRegistry()
    registration = registry.reserve(
        Service(), kind="remote-io-cancel-after-grant-delivers", close_name="close"
    )
    registration._lock = ContentionObservedLock()
    target_started = Event()
    target_exit = Event()
    start_entered = Event()
    allow_start = Event()

    thread = Thread(
        target=lambda: (
            target_started.set(),
            target_exit.wait(SCHEDULER_TIMEOUT_SECONDS),
        )
    )
    physical_start = thread.start

    def blocked_start() -> None:
        start_entered.set()
        assert allow_start.wait(SCHEDULER_TIMEOUT_SECONDS)
        physical_start()

    thread.start = blocked_start  # type: ignore[method-assign]
    starter = Thread(target=lambda: registration.start_thread(thread))
    starter.start()
    assert start_entered.wait(SCHEDULER_TIMEOUT_SECONDS)

    close_done = Event()

    def close_registration() -> None:
        registration.close()
        close_done.set()

    closer = Thread(target=close_registration)
    closer.start()
    assert registration._lock.contention_entered.wait(SCHEDULER_TIMEOUT_SECONDS)
    assert not close_done.is_set()
    allow_start.set()
    starter.join(SCHEDULER_TIMEOUT_SECONDS)
    assert target_started.wait(SCHEDULER_TIMEOUT_SECONDS)
    closer.join(SCHEDULER_TIMEOUT_SECONDS)
    assert close_done.is_set()
    assert registry.snapshot().registered_services == 1
    assert any(entry.state.name == "RETIRING" for entry in registry._entries.values())

    target_exit.set()
    thread.join(SCHEDULER_TIMEOUT_SECONDS)
    registration.close()
    assert registry.snapshot().registered_services == 0


def test_temporary_storage_release_uses_authoritative_lease_amount(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from types import SimpleNamespace

    from schema_sanitizer.core_impl import temporary_storage as module

    monkeypatch.setattr(
        module, "memory_budget", lambda _limit: SimpleNamespace(replay_spool_bytes=100)
    )
    pool = module.TemporaryStoragePermitPool(None)
    pool.limit_bytes = 100
    first = pool.try_acquire(10, label="first", path=tmp_path)
    second = pool.try_acquire(10, label="second", path=tmp_path)
    assert first is not None and second is not None
    first._reserved_bytes = 20
    first.release()
    assert pool.snapshot().reserved_bytes == 10
    assert second.reserved_bytes == 10
    second.release()
    assert pool.snapshot().reserved_bytes == 0


def test_remote_io_permit_release_ignores_mutated_weight() -> None:
    from schema_sanitizer.remote_impl.io_permits import RemoteIoPermitGovernor

    async def scenario() -> None:
        governor = RemoteIoPermitGovernor(capacity=2)
        first = await governor.acquire(operation_id="a")
        second = await governor.acquire(operation_id="b")
        first._weight = 2
        first.release()
        assert governor.snapshot().in_use == 1
        assert governor.snapshot().active_permits == 1
        second.release()
        assert governor.snapshot().in_use == 0

    asyncio.run(scenario())


def test_remote_submission_and_capacity_require_exact_capability() -> None:
    from schema_sanitizer.remote_impl.io_permits import RemoteIoPermitGovernor

    governor = RemoteIoPermitGovernor(capacity=2, max_pending_submissions=2)
    first = governor.reserve_submission()
    second = governor.reserve_submission()
    first._lease_id = second._lease_id
    first._capability = second._capability
    with pytest.raises(RuntimeError, match="authoritative"):
        first.release()
    assert governor.snapshot().pending_submissions == 2
    first._released = True  # corrupted owner cannot safely retry; keep second exact
    second.release()

    registration = governor.register_capacity(1)
    registration._capability = object()
    with pytest.raises(RuntimeError, match="authoritative"):
        registration.release()
    assert governor.snapshot().active_capacity_registrations == 1
    registration._released = True


def test_provider_throttle_release_uses_authoritative_endpoint() -> None:
    from schema_sanitizer.remote_impl.provider_throttle import ProviderThrottleGovernor

    governor = ProviderThrottleGovernor()
    first, _ = governor.try_acquire("endpoint-a")
    second, _ = governor.try_acquire("endpoint-b")
    assert first is not None and second is not None
    first._key = "endpoint-b"
    first.release()
    assert governor.snapshot("endpoint-a").in_flight == 0
    assert governor.snapshot("endpoint-b").in_flight == 1
    second.release()


def test_operation_memory_release_uses_authoritative_per_lease_amount() -> None:
    from schema_sanitizer.core_impl.memory_budget import OperationMemoryLease, OperationMemoryLedger

    class FakeLedger:
        _register_python_lease = OperationMemoryLedger._register_python_lease
        _python_lease_entry = OperationMemoryLedger._python_lease_entry
        _python_lease_entry_authority = OperationMemoryLedger._python_lease_entry_authority
        _python_lease_size = OperationMemoryLedger._python_lease_size
        _resize_python_lease = OperationMemoryLedger._resize_python_lease
        _release_python_lease = OperationMemoryLedger._release_python_lease
        _release_python_lease_authority = OperationMemoryLedger._release_python_lease_authority
        _maybe_finish_deferred_close = OperationMemoryLedger._maybe_finish_deferred_close

        def __init__(self) -> None:
            self._pid = os.getpid()
            self._lock = Lock()
            self._python_lease_sequence = 0
            self._python_leases: dict[int, tuple[int, object, int]] = {}
            self._unknown_python_lease_releases = 0
            self.total = 0

        def reserve(self, size_bytes: int, *, stage: str) -> None:
            self.total += size_bytes

        def release(self, size_bytes: int, *, _release_entry: Any = None) -> None:
            self.total -= size_bytes
            if _release_entry is not None:
                _release_entry.physical_released = True

    ledger = FakeLedger()
    first = OperationMemoryLease(ledger, 10, "remote-io-cancel-after-grant-delivers")  # type: ignore[arg-type]
    second = OperationMemoryLease(ledger, 10, "remote-io-cancel-after-grant-delivers")  # type: ignore[arg-type]
    first._size_bytes = 20
    first.release()
    assert ledger.total == 10
    assert second.reserved_bytes == 10
    second.release()
    assert ledger.total == 0


def test_direct_cross_process_memory_uses_authoritative_local_amount(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import cross_process_memory as module

    monkeypatch.setattr(module, "_enabled", lambda: False)
    first = module.CrossProcessMemoryLease(100, 0)
    second = module.CrossProcessMemoryLease(100, 0)
    module._update_direct_lease_reserved(first, first._lease_id, first._capability, 10)
    module._update_direct_lease_reserved(second, second._lease_id, second._capability, 10)
    first._reserved = 20
    first.release()
    live, reserved, unknown = module._direct_lease_snapshot()
    assert live >= 1
    assert reserved >= 10
    assert unknown == 0
    second.release()


def test_cleanup_rejects_closure_and_rich_argument_hidden_owners(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl.cleanup_dispatcher import _CleanupDispatcher

    dispatcher = _CleanupDispatcher()
    monkeypatch.setattr(dispatcher, "_ensure_workers", lambda: None)
    owner = bytearray(8 << 20)

    def closure() -> None:
        _ = owner

    assert not dispatcher.submit(closure, retained_bytes=1024)
    assert not dispatcher.submit(print, owner, retained_bytes=1024)
    snapshot = dispatcher.snapshot()
    assert snapshot.rejected_hidden_owner_calls == 2
    assert snapshot.owned_calls == 0


def test_retry_rejects_hidden_owner_and_negative_charge(monkeypatch: pytest.MonkeyPatch) -> None:
    from schema_sanitizer.core_impl.retry_scheduler import _RetryScheduler

    scheduler = _RetryScheduler()
    monkeypatch.setattr(scheduler, "_ensure_workers", lambda: None)
    owner = bytearray(8 << 20)

    def closure() -> None:
        _ = owner

    assert not scheduler.schedule(
        ("remote-io-cancel-after-grant-delivers", 1), closure, delay_seconds=1.0, retained_bytes=256
    )
    with pytest.raises(ValueError, match="non-negative"):
        scheduler.schedule(
            ("remote-io-cancel-after-grant-delivers", 2),
            lambda: None,
            delay_seconds=1.0,
            retained_bytes=-1,
        )


def test_gc_finalizers_do_not_call_blocking_release_methods() -> None:
    """Keep the no-blocking-GC rule mechanically enforceable across Python owners."""
    import ast

    root = Path(__file__).parents[2] / "src" / "schema_sanitizer"
    forbidden = {
        "self.release",
        "self.close",
        "self.close_all",
        "self.abandon",
        "self._retry_stopped_thread_lease",
        "self._release_thread_lease",
        "self._release_finalizer_ticket",
    }
    violations: list[str] = []
    for source_path in root.rglob("*.py"):
        tree = ast.parse(source_path.read_text())
        for node in ast.walk(tree):
            if (
                not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                or node.name != "__del__"
            ):
                continue
            for call in ast.walk(node):
                if not isinstance(call, ast.Call):
                    continue
                try:
                    name = ast.unparse(call.func)
                except Exception:
                    continue
                if name in forbidden:
                    violations.append(f"{source_path}:{node.lineno}:{name}")
    assert violations == []


def test_temporary_storage_finalizer_only_publishes_preallocated_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from types import SimpleNamespace

    from schema_sanitizer.core_impl import temporary_storage as module

    monkeypatch.setattr(
        module, "memory_budget", lambda _limit: SimpleNamespace(replay_spool_bytes=1024)
    )
    pool = module.TemporaryStoragePermitPool(None)
    lease = pool.try_acquire(
        1, label="remote-io-cancel-after-grant-delivers-finalizer", path=tmp_path
    )
    assert lease is not None
    original_release = pool._release_lease
    calls: list[bool] = []

    def forbidden_release(_lease: object) -> None:
        calls.append(True)
        raise AssertionError("GC entered temporary-storage accounting")

    monkeypatch.setattr(pool, "_release_lease", forbidden_release)
    lease.__del__()
    assert calls == []
    monkeypatch.setattr(pool, "_release_lease", original_release)
    assert module.drain_temporary_storage_finalizers() >= 1
    assert pool.snapshot().reserved_bytes == 0


def test_temporary_storage_cross_process_shrink_is_coalesced(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from schema_sanitizer.core_impl import temporary_storage_governor as module

    governor = module._ProcessTemporaryStorageGovernor()
    monkeypatch.setattr(governor, "filesystem", lambda _path: (7, tmp_path, 1 << 30))
    monkeypatch.setattr(governor, "free_inodes", lambda _path: 1 << 20)
    monkeypatch.setattr(module, "cross_process_storage_enabled", lambda: True)
    monkeypatch.setattr(module, "cross_process_storage_directory", lambda: tmp_path)
    reserve_calls: list[tuple[int, int]] = []
    release_calls: list[tuple[int, int]] = []
    monkeypatch.setattr(
        module,
        "_reserve_cross_process_raw",
        lambda _device, amount, _capacity, *, inode_count, **_kwargs: reserve_calls.append(
            (amount, inode_count)
        ),
    )
    monkeypatch.setattr(
        module,
        "_release_cross_process_raw",
        lambda _device, amount, *, inode_count, **_kwargs: release_calls.append(
            (amount, inode_count)
        ),
    )
    one_mib = 1 << 20
    governor.reserve(one_mib, path=tmp_path, label="a", inode_count=1)
    governor.reserve(one_mib, path=tmp_path, label="b", inode_count=1)
    assert reserve_calls == [(one_mib, 1), (one_mib, 1)]
    governor.release(7, one_mib, inode_count=1)
    assert release_calls == []
    governor.release(7, one_mib, inode_count=1)
    assert release_calls == [(2 * one_mib, 2)]
    snapshot = governor.authoritative_snapshot()
    assert snapshot.reserved_bytes == 0
    assert snapshot.cross_reserved_bytes == 0


def test_governed_thread_permit_lives_until_physical_thread_exit() -> None:
    from schema_sanitizer.core_impl.governed_thread import (
        defer_governed_thread_retirement,
        governed_thread_retirement_snapshot,
        reap_governed_thread_retirements,
    )

    reap_governed_thread_retirements()
    ready = Event()
    exit_event = Event()
    releases: list[int] = []
    holder: dict[str, Thread] = {}

    def target() -> None:
        assert defer_governed_thread_retirement(holder["thread"], lambda: releases.append(1))
        ready.set()
        exit_event.wait(SCHEDULER_TIMEOUT_SECONDS)

    thread = Thread(target=target)
    holder["thread"] = thread
    thread.start()
    assert ready.wait(SCHEDULER_TIMEOUT_SECONDS)
    reap_governed_thread_retirements()
    assert releases == []
    assert governed_thread_retirement_snapshot()[0] >= 1
    exit_event.set()
    thread.join(SCHEDULER_TIMEOUT_SECONDS)
    reap_governed_thread_retirements()
    assert releases == [1]


def test_retirement_reaper_claims_debt_before_reentrant_release() -> None:
    from schema_sanitizer.core_impl.governed_thread import (
        defer_governed_thread_retirement,
        reap_governed_thread_retirements,
    )

    reap_governed_thread_retirements()
    done = Event()
    holder: dict[str, Thread] = {}
    calls: list[int] = []

    def release() -> None:
        calls.append(1)
        reap_governed_thread_retirements()

    def target() -> None:
        assert defer_governed_thread_retirement(holder["thread"], release)
        done.set()

    thread = Thread(target=target)
    holder["thread"] = thread
    thread.start()
    assert done.wait(SCHEDULER_TIMEOUT_SECONDS)
    thread.join(SCHEDULER_TIMEOUT_SECONDS)
    reap_governed_thread_retirements()
    assert calls == [1]


def test_runtime_shutdown_checks_authoritative_finalizer_and_physical_ledgers() -> None:
    import ast

    from schema_sanitizer.core_impl import runtime_shutdown

    tree = ast.parse(inspect.getsource(runtime_shutdown._perform_shutdown))
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
    simple_calls = {node.func.id for node in calls if isinstance(node.func, ast.Name)}

    # Quiescence is observed through fixed-size buffers allocated before
    # teardown, rather than the historical allocating activity token.
    assert {
        "finalizer_activity_buffer_size",
        "freeze_finalizer_registry",
        "write_finalizer_activity_into",
    } <= simple_calls
    assert "finalizer_activity_token" not in simple_calls
    activity_buffers = [
        node
        for node in calls
        if isinstance(node.func, ast.Name)
        and node.func.id == "bytearray"
        and len(node.args) == 1
        and isinstance(node.args[0], ast.Name)
        and node.args[0].id == "activity_size"
    ]
    assert len(activity_buffers) == 2

    domain_loops = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.For)
        and isinstance(node.iter, ast.Call)
        and isinstance(node.iter.func, ast.Name)
        and node.iter.func.id == "finalizer_domains"
    ]

    def loop_calls_domain_method(loop: ast.For, method: str) -> bool:
        return any(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "domain"
            and node.func.attr == method
            for node in ast.walk(loop)
        )

    assert any(loop_calls_domain_method(loop, "drain") for loop in domain_loops)
    assert any(loop_calls_domain_method(loop, "snapshot") for loop in domain_loops)

    def mapping_get_keys(mapping: str) -> set[str]:
        return {
            node.args[0].value
            for node in calls
            if isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == mapping
            and node.func.attr == "get"
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        }

    assert "finalizer_cleanup" in mapping_get_keys("registered_finalizer_snapshots")
    assert {
        "cross_process_memory",
        "temporary_storage_authoritative",
    } <= mapping_get_keys("authoritative")

    resources_assignment = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "resources_drained"
            for target in node.targets
        )
    )
    resource_debts = {
        node.id
        for node in ast.walk(resources_assignment.value)
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)
    }
    assert {
        "cross_direct_live_bytes",
        "cross_physical_bytes",
        "retirement_debts",
        "temporary_cross_bytes",
        "temporary_logical_bytes",
    } <= resource_debts


def test_native_ordered_executor_retains_completion_ownership_until_take() -> None:
    import re

    root = Path(__file__).parents[2]

    def cpp_tokens(path: Path) -> list[str]:
        lexemes = re.findall(
            r'//[^\n]*|/\*.*?\*/|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\''
            r"|[A-Za-z_]\w*|::|->|&&|\|\||==|!=|<=|>=|<<|>>|\+\+|--|[^\s]",
            path.read_text(encoding="utf-8"),
            flags=re.DOTALL,
        )
        return [token for token in lexemes if not token.startswith(("//", "/*", '"', "'"))]

    def sequence_index(tokens: list[str], expected: list[str]) -> int:
        width = len(expected)
        for index in range(len(tokens) - width + 1):
            if tokens[index : index + width] == expected:
                return index
        return -1

    def scope_tokens(tokens: list[str], signature: list[str]) -> list[str]:
        signature_index = sequence_index(tokens, signature)
        assert signature_index >= 0
        opening = tokens.index("{", signature_index + len(signature))
        depth = 0
        for index in range(opening, len(tokens)):
            if tokens[index] == "{":
                depth += 1
            elif tokens[index] == "}":
                depth -= 1
                if depth == 0:
                    return tokens[opening + 1 : index]
        raise AssertionError("unterminated C++ scope")

    ordered = cpp_tokens(root / "cpp/src/internal/runtime/ordered_executor.hh")
    completion = cpp_tokens(
        root / "cpp/src/internal/runtime/ordered_executor_arena_completion.cc.inc"
    )
    arena_header = cpp_tokens(root / "cpp/src/internal/runtime/operation_task_arena.hh")
    arena = cpp_tokens(root / "cpp/src/internal/runtime/operation_task_arena.cc")

    slot = scope_tokens(ordered, ["struct", "ArenaOutcomeSlot", "final"])
    publish = scope_tokens(ordered, ["void", "Publish", "("])
    take_arena = scope_tokens(completion, ["take_next_arena", "("])
    take_private = scope_tokens(
        ordered, ["sanitize", "::", "Result", "<", "Outcome", ">", "TakeNext", "("]
    )
    store_private = scope_tokens(ordered, ["bool", "store_outcome_locked", "("])
    completion_lease = scope_tokens(arena_header, ["class", "CompletionMemoryLease", "final"])

    assert sequence_index(slot, ["CompletionMemoryLease", "retained_lease", ";"]) >= 0
    transfer = sequence_index(
        publish,
        [
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
        ],
    )
    retain = sequence_index(
        publish,
        [
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
        ],
    )
    assert 0 <= transfer < retain

    move_outcome = sequence_index(
        take_arena,
        ["outcome", ".", "emplace", "(", "std", "::", "move", "(", "*", "slot"],
    )
    release_lease = sequence_index(
        take_arena, ["slot", ".", "retained_lease", ".", "reset", "(", ")"]
    )
    assert 0 <= move_outcome < release_lease

    assert (
        sequence_index(
            store_private,
            [
                "completed_retained_bytes_",
                "[",
                "completion_slot",
                "]",
                "=",
                "output_retained",
                ";",
            ],
        )
        >= 0
    )
    private_take = sequence_index(
        take_private,
        [
            "std",
            "::",
            "exchange",
            "(",
            "completed_retained_bytes_",
            "[",
            "completion_slot",
            "]",
            ",",
        ],
    )
    private_release = sequence_index(
        take_private,
        ["private_retained_bytes_", "=", "retained", ">=", "private_retained_bytes_"],
    )
    assert 0 <= private_take < private_release

    assert (
        sequence_index(
            completion_lease,
            [
                "CompletionMemoryLease",
                "(",
                "const",
                "CompletionMemoryLease",
                "&",
                ")",
                "=",
                "delete",
            ],
        )
        >= 0
    )
    assert sequence_index(arena, ["CompletionMemoryLease", "::", "reset", "(", ")"]) >= 0
    assert "ReleaseCompletionBytes" not in ordered
    assert "ReleaseCompletionBytes" not in arena_header
    assert "ReleaseCompletionBytes" not in arena
