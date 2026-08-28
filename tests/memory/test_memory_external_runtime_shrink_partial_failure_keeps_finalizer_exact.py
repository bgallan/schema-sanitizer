"""Tests shared and standalone external-runtime pools across partial shrink failure,
post-fork locks, Arrow-to-Polars memory, overlapping claims, operation borrows,
completion ownership, and format evidence. Pool width never re-expands under live
overlap, shared suffixes alone release, and named finalizer state retains exact
move-only ownership."""

from __future__ import annotations

import os
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src" / "schema_sanitizer"
CPP = ROOT / "cpp" / "src"


def _runtime_pool_widths(module: Any, runtime: object) -> tuple[int, int] | None:
    """Read the exact pool entry without counting unrelated cleanup workers."""
    state = _runtime_pool_state(module, runtime)
    if state is None:
        return None
    logical_width, physical_amount, _, _ = state
    return logical_width, physical_amount


def _runtime_pool_state(module: Any, runtime: object) -> tuple[int, int, int, int] | None:
    """Read one runtime's exact logical/physical envelope and claim counts."""
    runtime_key = module._external_runtime_pool_identity_key(runtime)
    with module._EXTERNAL_RUNTIME_POOL_COORDINATOR_LOCK:
        entry = module._EXTERNAL_RUNTIME_POOL_COORDINATOR.get(runtime_key)
        if entry is None:
            return None
        return (
            entry.logical_width,
            entry.physical_amount,
            len(entry.logical_claims),
            len(entry.physical_claims),
        )


class _ExactNative:
    """Minimal current exact-receipt native API used by runtime-pool tests."""

    def __init__(self, calls: list[tuple[str, int]]) -> None:
        """Initialize the exact native test double."""
        self.calls = calls

    def acquire_exact_permit_lease(self, desired: int, minimum: int):
        """Acquire the fake exact-permit lease requested by the resource owner."""
        assert desired >= minimum
        self.calls.append(("acquire", desired))
        return SimpleNamespace(amount=desired), desired

    def resize_exact_permit_lease(self, receipt: object, target: int) -> int:
        """Resize the fake exact-permit lease to the requested amount."""
        previous = int(receipt.amount)  # type: ignore[attr-defined]
        receipt.amount = target  # type: ignore[attr-defined]
        if previous != target:
            self.calls.append(("release", previous - target))
        return target

    @staticmethod
    def exact_permit_lease_amount(receipt: object) -> int:
        """Return the exact permit amount tracked by the fake lease."""
        return int(receipt.amount)  # type: ignore[attr-defined]


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
def test_external_runtime_shrink_fails_before_inherited_lock_after_fork() -> None:
    """Verify external runtime shrink fails before inherited lock after fork."""
    from schema_sanitizer.core_impl import process_resources as module

    runtime = module.ExternalRuntimeConcurrencyLease(None, workers=2, parallel=True)
    lock_held = threading.Event()
    release_lock = threading.Event()

    def hold_lock() -> None:
        """Hold the ownership lock until the competing shrink arrives."""
        with runtime._lock:
            lock_held.set()
            release_lock.wait(5)

    holder = threading.Thread(target=hold_lock)
    holder.start()
    assert lock_held.wait(2)

    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:  # pragma: no cover - child assertion reported through pipe
        try:
            os.close(read_fd)
            try:
                runtime.shrink_to(1)
            except RuntimeError as exc:
                os.write(write_fd, ("ok:" + str(exc)).encode("utf-8", "replace"))
                os._exit(0)
            os.write(write_fd, b"unexpected-success")
            os._exit(2)
        except BaseException as exc:
            try:
                os.write(write_fd, ("child-error:" + repr(exc)).encode("utf-8", "replace"))
            finally:
                os._exit(3)

    os.close(write_fd)
    try:
        _, status = os.waitpid(pid, 0)
        message = os.read(read_fd, 4096).decode("utf-8", "replace")
    finally:
        os.close(read_fd)
        release_lock.set()
        holder.join(2)
        runtime.close()

    assert os.waitstatus_to_exitcode(status) == 0
    assert message.startswith("ok:")
    assert "cannot be reused after fork" in message


def test_arrow_table_to_polars_admits_reported_polars_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify Arrow table to Polars admits reported Polars pool."""
    from schema_sanitizer.api_impl import results as module

    seen_runtime: list[object] = []

    class Polars:
        @staticmethod
        def thread_pool_size() -> int:
            """Return the configured external thread-pool size."""
            return 3

        @staticmethod
        def from_arrow(value: object, *, rechunk: bool) -> tuple[object, bool]:
            """Convert the Arrow value through the fake Polars entry point."""
            return (value, rechunk)

    class Lease:
        workers = 3
        parallel = True

        def close(self) -> None:
            """Close the resources owned by the lease test double."""
            pass

    monkeypatch.setattr(module, "ensure_optional_dependency", lambda *args, **kwargs: Polars)

    def admit(runtime: object | None = None) -> Lease:
        """Admit the resource under the controlled scheduling conditions."""
        seen_runtime.append(runtime)
        return Lease()

    monkeypatch.setattr(module, "_unconfigurable_external_threads", admit)
    value = object()
    converted = module.convert_arrow_table_output(
        value, "polars", feature="external-runtime-shrink-partial-failure-keeps"
    )
    assert converted == (value, False)
    assert seen_runtime == [Polars]


def test_process_global_runtime_pool_shares_logical_and_native_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify process global runtime pool shares logical and native envelope."""
    from schema_sanitizer.core_impl import concurrency_contracts
    from schema_sanitizer.core_impl import process_resources as module

    governor = module._Governor(
        16, "external-runtime-shrink-partial-failure-keeps_shared_runtime_pool"
    )
    calls: list[tuple[str, int]] = []

    class Runtime:
        pass

    runtime_identity = Runtime()
    native = _ExactNative(calls)
    monkeypatch.setattr(module, "_THREAD_GOVERNOR", governor)
    monkeypatch.setattr(module, "_refresh_thread_governor_capacity", lambda: None)
    monkeypatch.setattr(module, "_native_external_thread_api", lambda: native)
    monkeypatch.setattr(concurrency_contracts, "current_runtime_execution_lease", lambda: None)

    first = module.acquire_external_runtime_threads(
        8, allow_parallel=True, runtime=runtime_identity
    )
    second = module.acquire_external_runtime_threads(
        8, allow_parallel=True, runtime=runtime_identity
    )

    assert first.workers == second.workers == 8
    # One process-global runtime pool => one logical governor envelope and one
    # physical native envelope, despite two concurrent operation claims.
    assert _runtime_pool_state(module, runtime_identity) == (8, 8, 2, 2)
    assert calls == [("acquire", 8)]

    first.close()
    assert _runtime_pool_state(module, runtime_identity) == (8, 8, 1, 1)
    assert calls == [("acquire", 8)]
    second.close()
    assert _runtime_pool_state(module, runtime_identity) is None
    assert calls == [("acquire", 8), ("release", 8)]


def test_shared_runtime_pool_releases_only_unshared_suffix(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify shared runtime pool releases only unshared suffix."""
    from schema_sanitizer.core_impl import concurrency_contracts
    from schema_sanitizer.core_impl import process_resources as module

    governor = module._Governor(
        16, "external-runtime-shrink-partial-failure-keeps_shared_runtime_shrink"
    )
    calls: list[tuple[str, int]] = []

    runtime_identity = object()
    native = _ExactNative(calls)
    monkeypatch.setattr(module, "_THREAD_GOVERNOR", governor)
    monkeypatch.setattr(module, "_refresh_thread_governor_capacity", lambda: None)
    monkeypatch.setattr(module, "_native_external_thread_api", lambda: native)
    monkeypatch.setattr(concurrency_contracts, "current_runtime_execution_lease", lambda: None)

    first = module.acquire_external_runtime_threads(
        8, allow_parallel=True, runtime=runtime_identity
    )
    second = module.acquire_external_runtime_threads(
        8, allow_parallel=True, runtime=runtime_identity
    )
    first.shrink_to(2)
    # The second 8-wide claim still owns the global pool: no physical/logical shrink yet.
    assert _runtime_pool_widths(module, runtime_identity) == (8, 8)
    assert calls == [("acquire", 8)]
    second.shrink_to(2)
    # Both claims now need only two workers, so exactly the unshared suffix returns.
    assert _runtime_pool_widths(module, runtime_identity) == (2, 2)
    assert calls == [("acquire", 8), ("release", 6)]
    first.close()
    assert _runtime_pool_widths(module, runtime_identity) == (2, 2)
    second.close()
    assert _runtime_pool_widths(module, runtime_identity) is None
    assert calls[-1] == ("release", 2)


def test_live_global_runtime_pool_never_reexpands_for_overlapping_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify live global runtime pool never reexpands for overlapping claim."""
    from schema_sanitizer.core_impl import concurrency_contracts
    from schema_sanitizer.core_impl import process_resources as module

    governor = module._Governor(
        16, "external-runtime-shrink-partial-failure-keeps_monotonic_runtime_pool"
    )
    calls: list[tuple[str, int]] = []

    runtime_identity = object()
    native = _ExactNative(calls)
    monkeypatch.setattr(module, "_THREAD_GOVERNOR", governor)
    monkeypatch.setattr(module, "_refresh_thread_governor_capacity", lambda: None)
    monkeypatch.setattr(module, "_native_external_thread_api", lambda: native)
    monkeypatch.setattr(concurrency_contracts, "current_runtime_execution_lease", lambda: None)

    first = module.acquire_external_runtime_threads(
        8, allow_parallel=True, runtime=runtime_identity
    )
    first.shrink_to(2)
    assert _runtime_pool_widths(module, runtime_identity) == (2, 2)
    assert calls == [("acquire", 8), ("release", 6)]

    # The runtime is process-global and already constrained to two workers. A
    # later overlapping request for eight shares two: no physical re-expansion
    # and no unnecessary serial degradation.
    second = module.acquire_external_runtime_threads(
        8, allow_parallel=True, runtime=runtime_identity
    )
    assert second.workers == 2
    assert second.parallel is True
    assert _runtime_pool_widths(module, runtime_identity) == (2, 2)
    assert calls == [("acquire", 8), ("release", 6)]

    first.close()
    assert _runtime_pool_widths(module, runtime_identity) == (2, 2)
    second.close()
    assert _runtime_pool_widths(module, runtime_identity) is None
    assert calls[-1] == ("release", 2)


def test_operation_borrow_shares_already_constrained_global_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify operation borrow shares already constrained global pool."""
    from schema_sanitizer.core_impl import concurrency_contracts
    from schema_sanitizer.core_impl import process_resources as module

    governor = module._Governor(
        16, "external-runtime-shrink-partial-failure-keeps_parent_borrow_global_pool"
    )
    operation = governor.try_acquire_up_to(9, minimum=9)
    calls: list[tuple[str, int]] = []

    runtime_identity = object()
    native = _ExactNative(calls)
    monkeypatch.setattr(module, "_THREAD_GOVERNOR", governor)
    monkeypatch.setattr(module, "_native_external_thread_api", lambda: native)
    monkeypatch.setattr(concurrency_contracts, "current_runtime_execution_lease", lambda: operation)

    first = module.acquire_external_runtime_threads(
        8, allow_parallel=True, runtime=runtime_identity
    )
    first.shrink_to(2)
    budget = operation.__dict__["_external_runtime_borrow_budget"]
    assert budget.borrowed == 2

    second = module.acquire_external_runtime_threads(
        8, allow_parallel=True, runtime=runtime_identity
    )
    assert second.workers == 2
    assert second.parallel is True
    assert budget.borrowed == 4
    assert calls == [("acquire", 8), ("release", 6)]

    first.close()
    assert budget.borrowed == 2
    second.close()
    assert budget.borrowed == 0
    assert calls[-1] == ("release", 2)
    operation.release()
    with governor._condition:
        assert operation.lease_id not in governor._active_leases


def test_standalone_claim_aligns_to_pool_preconstrained_by_operation_borrow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify standalone claim aligns to pool preconstrained by operation borrow."""
    from schema_sanitizer.core_impl import concurrency_contracts
    from schema_sanitizer.core_impl import process_resources as module

    governor = module._Governor(
        20, "external-runtime-shrink-partial-failure-keeps_mixed_global_pool"
    )
    operation = governor.try_acquire_up_to(9, minimum=9)
    calls: list[tuple[str, int]] = []
    active_parent: list[object | None] = [operation]

    runtime_identity = object()
    native = _ExactNative(calls)
    monkeypatch.setattr(module, "_THREAD_GOVERNOR", governor)
    monkeypatch.setattr(module, "_refresh_thread_governor_capacity", lambda: None)
    monkeypatch.setattr(module, "_native_external_thread_api", lambda: native)
    monkeypatch.setattr(
        concurrency_contracts, "current_runtime_execution_lease", lambda: active_parent[0]
    )

    borrowed = module.acquire_external_runtime_threads(
        8, allow_parallel=True, runtime=runtime_identity
    )
    borrowed.shrink_to(2)
    active_parent[0] = None

    standalone = module.acquire_external_runtime_threads(
        8, allow_parallel=True, runtime=runtime_identity
    )
    assert standalone.workers == 2
    assert standalone.parallel is True
    assert module.external_runtime_pool_snapshot()["physical_permits"] == 2
    assert calls == [("acquire", 8), ("release", 6)]

    borrowed.close()
    # Standalone claim still owns the two-worker physical envelope.
    assert module.external_runtime_pool_snapshot()["physical_permits"] == 2
    standalone.close()
    assert module.external_runtime_pool_snapshot()["physical_permits"] == 0
    assert calls[-1] == ("release", 2)
    operation.release()
    # A process cleanup worker may start while this test temporarily installs
    # the local governor.  Authenticate retirement of the operation's exact
    # capability instead of mistaking that independent one-thread owner for a
    # leaked external-runtime borrow.
    with governor._condition:
        assert operation.lease_id not in governor._active_leases


def test_completion_memory_ownership_is_move_only_not_quantity_release() -> None:
    """Verify completion memory ownership is move only not quantity release."""
    header = (CPP / "internal/runtime/operation_task_arena.hh").read_text(encoding="utf-8")
    arena = (CPP / "internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    ordered = (CPP / "internal/runtime/ordered_executor.hh").read_text(encoding="utf-8")
    completion = (CPP / "internal/runtime/ordered_executor_arena_completion.cc.inc").read_text(
        encoding="utf-8"
    )

    assert "class CompletionMemoryLease final" in header
    assert "CompletionMemoryLease(const CompletionMemoryLease &) = delete" in header
    assert "void ReleaseCompletionBytes" not in header
    assert "CompletionMemoryLease::reset" in arena
    assert "OperationTaskArena::ReleaseCompletionBytes" not in arena
    assert "CompletionMemoryLease retained_lease" in ordered
    assert "slot.retained_lease = std::move(completion_lease)" in ordered
    assert "slot.retained_lease.reset()" in completion


def test_release_gate_requires_format_specific_stage_evidence() -> None:
    """Verify release gate requires format specific stage evidence."""
    from schema_sanitizer.core_impl import concurrency_coverage as coverage
    from schema_sanitizer.core_impl.concurrency_stage_evidence import (
        INPUT_PRIMARY_RUNTIME_STAGE,
        OUTPUT_PRIMARY_RUNTIME_STAGE,
    )

    assert set(INPUT_PRIMARY_RUNTIME_STAGE) == set(coverage.INPUT_CONCURRENCY_COVERAGE)
    assert set(OUTPUT_PRIMARY_RUNTIME_STAGE) == set(coverage.OUTPUT_CONCURRENCY_COVERAGE)
    for name, stage in INPUT_PRIMARY_RUNTIME_STAGE.items():
        assert stage in coverage.INPUT_CONCURRENCY_COVERAGE[name]
    for name, stage in OUTPUT_PRIMARY_RUNTIME_STAGE.items():
        assert stage in coverage.OUTPUT_CONCURRENCY_COVERAGE[name]

    source = (SRC / "core_impl/concurrency_coverage.py").read_text(encoding="utf-8")
    assert "validate_stage_observed_concurrency_pair_contracts()" in source
    assert "stage_count = validate_stage_observed_concurrency_pair_contracts()" in source


def test_complex_finalizers_use_named_state_for_external_runtime_and_result() -> None:
    """Verify complex finalizers use named state for external runtime and result."""
    process = (SRC / "core_impl/process_resources.py").read_text(encoding="utf-8")
    result = (SRC / "api_impl/results.py").read_text(encoding="utf-8")
    parquet = (SRC / "adapters/parquet/record_batch_factory.py").read_text(encoding="utf-8")
    assert "class _ExternalRuntimeCleanupState" in process
    assert "state.native" in process
    assert "capsule.arg5 = self._lease" not in process
    assert "class _ResultFinalizerState" in result
    assert "state.resource_owner" in result
    assert 'capsule.arg7 = getattr(self, "_keepalive", None)' not in result
    assert "class _DatasetLifetimeCleanupState" in parquet
    assert "state.fd_capability" in parquet
    assert "self._finalizer_capsule.arg2 = staged_artifact" not in parquet
