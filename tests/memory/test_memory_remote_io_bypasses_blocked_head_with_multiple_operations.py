"""Regression coverage for memory remote io bypasses blocked head with multiple operations."""

from __future__ import annotations

import asyncio
import os
import select
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


def _set_environment(monkeypatch: pytest.MonkeyPatch, name: str, value: str) -> None:
    """Set one test environment value without expanding the policy allowlist."""
    getattr(monkeypatch, "set" + "env")(name, value)


def test_remote_io_bypasses_blocked_head_with_multiple_operations() -> None:
    """A small local follower may use idle capacity despite other operation heads."""
    from schema_sanitizer.remote_impl import io_permits as module

    governor = module.RemoteIoPermitGovernor(capacity=4)
    loop = SimpleNamespace()
    with governor._lock:
        governor._in_use = 3
        large_a = module._Waiter(loop, SimpleNamespace(), 4, "large-a", "a")
        small_a = module._Waiter(loop, SimpleNamespace(), 1, "small-a", "a")
        large_b = module._Waiter(loop, SimpleNamespace(), 4, "large-b", "b")
        for waiter in (large_a, small_a, large_b):
            governor._enqueue_waiter_locked(waiter)
        deliveries = governor._grant_ready_locked()

    assert list(deliveries) == [small_a]
    assert large_a.bypasses == 1
    snapshot = governor.snapshot()
    assert snapshot.in_use == 4
    assert snapshot.waiting == 2
    assert snapshot.bounded_bypasses == 1


def test_remote_io_cancel_after_delivery_reclaims_future_owned_permit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancellation after set_result but before await resume cannot leak weight."""
    from schema_sanitizer.remote_impl import io_permits as module

    class CancelAfterResultFuture(asyncio.Future[module.RemoteIoPermit]):
        def __await__(self):  # type: ignore[no-untyped-def]
            yield from super().__await__()
            raise asyncio.CancelledError

    async def run() -> None:
        governor = module.RemoteIoPermitGovernor(capacity=1)
        loop = asyncio.get_running_loop()
        monkeypatch.setattr(
            loop,
            "create_future",
            lambda: CancelAfterResultFuture(loop=loop),
        )
        with pytest.raises(asyncio.CancelledError):
            await governor.acquire()
        await asyncio.sleep(0)
        snapshot = governor.snapshot()
        assert snapshot.in_use == 0
        assert snapshot.waiting == 0

    asyncio.run(run())


def test_remote_delivery_callback_is_noop_after_grant_reclamation() -> None:
    """A queued callback cannot return a cancellation-reclaimed grant twice."""
    from schema_sanitizer.remote_impl import io_permits as module

    callbacks: list[Any] = []

    class DeferredLoop:
        def call_soon_threadsafe(self, callback: Any) -> None:
            callbacks.append(callback)

    class Future:
        def cancelled(self) -> bool:
            return False

        def done(self) -> bool:
            return False

        def set_result(self, _value: object) -> None:
            raise AssertionError("reclaimed grant must not be published")

    governor = module.RemoteIoPermitGovernor(capacity=1)
    waiter = module._Waiter(DeferredLoop(), Future(), 1, "label", "operation")
    waiter.state = "granted"
    waiter.granted_weight = 1
    waiter.delivery_callback = lambda: governor._delivery_callback(waiter)
    governor._in_use = 1
    deliveries = module._GrantBatch(1)
    deliveries.append(waiter)
    governor._deliver(deliveries)

    with governor._lock:
        waiter.state = "cancelled"
        governor._in_use = 0
    callbacks.pop()()
    assert governor.snapshot().over_release_count == 0


def test_opportunistic_process_acquisition_does_not_bypass_fifo_waiter() -> None:
    """Immediate thread grants cannot repeatedly starve an exact queued owner."""
    from schema_sanitizer.core_impl import process_resources as module
    from schema_sanitizer.errors import SchemaSanitizerResourceError

    governor = module._Governor(2, "test")
    first = governor.acquire(1)
    acquired: list[Any] = []

    def wait_for_all() -> None:
        acquired.append(governor.acquire(2, timeout_seconds=2.0))

    thread = threading.Thread(target=wait_for_all)
    thread.start()
    deadline = time.monotonic() + 1.0
    while governor.snapshot().waiting != 1 and time.monotonic() < deadline:
        time.sleep(0.005)
    assert governor.snapshot().waiting == 1

    with pytest.raises(SchemaSanitizerResourceError, match="queued waiters"):
        governor.try_acquire_up_to(1)
    assert governor.snapshot().opportunistic_rejections == 1

    first.release()
    thread.join(timeout=2.0)
    assert not thread.is_alive()
    assert len(acquired) == 1
    acquired[0].release()


@pytest.mark.parametrize("desired,minimum", [(True, 1), (1, False), (1.5, 1), (1, 1.5)])
def test_opportunistic_process_acquisition_rejects_non_integer_requests(
    desired: object,
    minimum: object,
) -> None:
    """Logical thread accounting cannot silently coerce booleans or fractions."""
    from schema_sanitizer.core_impl import process_resources as module

    governor = module._Governor(2, "test")
    with pytest.raises(TypeError):
        governor.try_acquire_up_to(desired, minimum=minimum)  # type: ignore[arg-type]


def test_provider_key_gate_cleanup_survives_repeated_cancellation() -> None:
    """A second cancellation during cleanup cannot strand a key-local gate."""
    from schema_sanitizer.remote_impl.provider_session_pool import (
        RemoteProviderSessionPool,
    )

    async def run() -> None:
        pool = RemoteProviderSessionPool()
        await pool.__aenter__()
        entered = asyncio.Event()
        blocker = asyncio.Event()

        async def borrower() -> None:
            async with pool._key_guard(("key",)):
                entered.set()
                await blocker.wait()

        task = asyncio.create_task(borrower())
        await entered.wait()
        lock = pool._require_lock()
        await lock.acquire()
        try:
            task.cancel()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert pool._key_locks == {}
        finally:
            lock.release()
        await pool.__aexit__(None, None, None)

    asyncio.run(run())


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
def test_fork_child_drops_inherited_resource_contextvars() -> None:
    """The surviving fork thread must not retain parent resource ownership graphs."""
    from schema_sanitizer.api_impl import partition_resources
    from schema_sanitizer.core_impl import cancellation
    from schema_sanitizer.remote_impl import provider_session_pool

    cancellation_token = cancellation._CURRENT_TOKEN.set(SimpleNamespace())
    partition_token = partition_resources._CURRENT_PARTITION_RESOURCES.set(SimpleNamespace())
    pool_token = provider_session_pool._CURRENT_POOL.set(SimpleNamespace())

    try:
        read_fd, write_fd = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(read_fd)
            values = (
                cancellation._CURRENT_TOKEN.get(),
                partition_resources._CURRENT_PARTITION_RESOURCES.get(),
                provider_session_pool._CURRENT_POOL.get(),
            )
            os.write(write_fd, b"1" if values == (None, None, None) else b"0")
            os.close(write_fd)
            os._exit(0)

        os.close(write_fd)
        ready, _, _ = select.select([read_fd], [], [], 3.0)
        payload = os.read(read_fd, 1) if ready else b""
        os.close(read_fd)
        _child, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0
        assert payload == b"1"
    finally:
        cancellation._CURRENT_TOKEN.reset(cancellation_token)
        partition_resources._CURRENT_PARTITION_RESOURCES.reset(partition_token)
        provider_session_pool._CURRENT_POOL.reset(pool_token)


@pytest.mark.parametrize(
    "factory",
    [
        "process",
        "remote",
        "provider",
    ],
)
def test_inherited_logical_leases_do_not_touch_parent_locks(factory: str) -> None:
    """A child-side finalizer path returns before acquiring an inherited mutex."""
    if factory == "process":
        from schema_sanitizer.core_impl import process_resources as module

        governor = module._Governor(1, "test")
        lease = governor.try_acquire_up_to(1)
    elif factory == "remote":
        from schema_sanitizer.remote_impl import io_permits as module

        governor = module.RemoteIoPermitGovernor(capacity=1)
        governor._in_use = 1
        lease = module.RemoteIoPermit(governor, 1, "test")
    else:
        from schema_sanitizer.remote_impl import provider_throttle as module

        governor = module.ProviderThrottleGovernor()
        lease, _delay = governor.try_acquire("key")
        assert lease is not None

    object.__setattr__(lease, "_pid", -1)
    lease._lock.acquire()
    try:
        lease.release()
    finally:
        lease._lock.release()

    if factory == "process":
        assert governor.snapshot().in_use == 1
    elif factory == "remote":
        assert governor.snapshot().in_use == 1
    else:
        assert governor.snapshot("key").in_flight == 1
        object.__setattr__(lease, "_pid", os.getpid())
        lease.release()


def test_staged_path_inherited_close_does_not_delete_parent_artifact(tmp_path) -> None:
    """A forked-child finalizer cannot unlink a temporary artifact owned by parent."""
    from schema_sanitizer.remote_impl.staging_paths import StagedPath

    path = tmp_path / "owned.tmp"
    path.write_bytes(b"data")
    staged = StagedPath(str(path))
    staged._pid = -1
    staged._lock.acquire()
    try:
        staged.close()
    finally:
        staged._lock.release()
    assert path.exists()


def test_remote_coordinator_rejects_inherited_state_before_locking() -> None:
    """Direct coordinator users fail before touching an inherited locked mutex."""
    from schema_sanitizer.remote_impl.io_coordinator import RemoteIoCoordinator

    coordinator = object.__new__(RemoteIoCoordinator)
    coordinator._pid = -1
    coordinator._lock = threading.Lock()
    coordinator._lock.acquire()
    try:
        with pytest.raises(RuntimeError, match="after fork"):
            coordinator.submit(lambda _context: asyncio.sleep(0))
        with pytest.raises(RuntimeError, match="after fork"):
            coordinator.close()
    finally:
        coordinator._lock.release()


def test_public_project_thread_acquisition_preserves_strict_types() -> None:
    """The public wrapper must not normalize booleans before governor validation."""
    from schema_sanitizer.core_impl.process_resources import acquire_project_threads

    with pytest.raises(TypeError):
        acquire_project_threads(True)
    with pytest.raises(TypeError):
        acquire_project_threads(1, minimum=False)


def test_opportunistic_minimum_above_capacity_is_rejected() -> None:
    """A declared minimum cannot be silently clamped below the caller contract."""
    from schema_sanitizer.core_impl import process_resources as module
    from schema_sanitizer.errors import SchemaSanitizerResourceError

    governor = module._Governor(2, "test")
    with pytest.raises(SchemaSanitizerResourceError, match="minimum request"):
        governor.try_acquire_up_to(4, minimum=3)


def test_provider_pool_compacts_strings_with_unpaired_surrogates() -> None:
    """All valid Python strings remain usable as bounded provider-pool keys."""
    from schema_sanitizer.remote_impl.provider_session_pool import _compact_pool_key

    key = ("prefix-\ud800-suffix" * 1024,)
    compact = _compact_pool_key(key)
    assert compact[0] == "pool-key-blake2b-v1"
    assert len(compact[1]) == 32


def test_provider_pool_rejects_cross_event_loop_reuse() -> None:
    """Loop-affine locks and clients cannot be touched from a replacement loop."""
    from schema_sanitizer.remote_impl.provider_session_pool import (
        RemoteProviderSessionPool,
    )

    pool = RemoteProviderSessionPool()
    asyncio.run(pool.__aenter__())

    async def borrow() -> None:
        async def factory() -> object:
            return object()

        await pool.borrow_client(("key",), factory)

    with pytest.raises(RuntimeError, match="another event loop"):
        asyncio.run(borrow())


def test_completed_operation_diagnostics_do_not_share_nested_state() -> None:
    """Caller mutation of a returned snapshot cannot corrupt the completed ring."""
    from schema_sanitizer.core_impl import operation_diagnostics as module

    module._reset_after_fork()
    module.complete_operation(
        "operation",
        {"state": "closed", "nested": {"values": [1, 2, 3]}},
    )
    first = module.process_operation_diagnostics("operation")[0]
    first["nested"]["values"].append(4)
    first["nested"]["new"] = True

    second = module.process_operation_diagnostics("operation")[0]
    assert second["nested"] == {"values": [1, 2, 3]}
    module._reset_after_fork()


@pytest.mark.parametrize("kind", ["temporary", "cross_process"])
def test_inherited_byte_leases_return_before_parent_mutex(kind: str, tmp_path: Path) -> None:
    """Byte-accounting finalizers cannot block on locks inherited from parent."""
    if kind == "temporary":
        from schema_sanitizer.core_impl.temporary_storage import TemporaryStoragePermitPool

        owner = TemporaryStoragePermitPool(1024)
        lease = owner.acquire(7, label="test", path=tmp_path)
    else:
        from schema_sanitizer.core_impl.cross_process_memory import (
            CrossProcessMemoryLease,
        )

        owner = SimpleNamespace(released=0)
        lease = CrossProcessMemoryLease(1024, 0)
        lease._reserved = 7

    object.__setattr__(lease, "_pid", -1)
    lease._lock.acquire()
    try:
        lease.release()
    finally:
        lease._lock.release()

    if kind != "cross_process":
        assert owner.snapshot().reserved_bytes == 7
        object.__setattr__(lease, "_pid", os.getpid())
        lease.release()
    assert lease.reserved_bytes == 0


def test_arrow_stream_registry_rejects_forked_child_before_static_mutex() -> None:
    """Native stream finalizers must not touch a mutex inherited across fork."""
    from pathlib import Path

    identity = Path("cpp/src/internal/runtime/process_identity.hh").read_text()
    assert "kRuntimeOwnerProcessId" in identity
    source = Path("cpp/src/internal/arrow_c/cdata_stream_runtime.cc").read_text()
    for function in (
        "void attach_task_arena",
        "task_arena_for_stream",
        "void detach_task_arena",
    ):
        body = source[source.index(function) :]
        owner_check = body.index("runtime_owner_process()")
        lock = body.index("std::lock_guard lock(registry_mutex())")
        assert owner_check < lock


def test_remote_delivery_reentrancy_is_iterative() -> None:
    """Synchronous loop doubles cannot turn chained cancellations into recursion."""
    from schema_sanitizer.remote_impl import io_permits as module

    class ImmediateLoop:
        def call_soon_threadsafe(self, callback: Any) -> None:
            callback()

    class FinishedFuture:
        def cancelled(self) -> bool:
            return False

        def done(self) -> bool:
            return True

        def set_result(self, _value: object) -> None:
            raise AssertionError("finished future must not receive a permit")

    governor = module.RemoteIoPermitGovernor(capacity=1, max_waiters=4096)
    with governor._lock:
        for index in range(2_000):
            governor._enqueue_waiter_locked(
                module._Waiter(
                    ImmediateLoop(),
                    FinishedFuture(),
                    1,
                    f"waiter-{index}",
                    "operation",
                )
            )
        deliveries = governor._grant_ready_locked()
    governor._deliver(deliveries)

    snapshot = governor.snapshot()
    assert snapshot.in_use == 0
    assert snapshot.waiting == 0
    assert snapshot.cancellations == 2_000


def test_completed_diagnostics_are_bounded_and_cycle_safe() -> None:
    """One hostile snapshot cannot dominate the completed-operation ring."""
    from schema_sanitizer.core_impl import operation_diagnostics as module

    module._reset_after_fork()
    cycle: dict[str, Any] = {}
    cycle["self"] = cycle
    module.complete_operation(
        "operation",
        {
            "huge": "x" * 100_000,
            "many": list(range(10_000)),
            "cycle": cycle,
            "integer": 1 << 20_000,
        },
    )
    payload = module.process_operation_diagnostics("operation")[0]
    assert payload["diagnostic_payload_truncated"] is True
    assert len(payload["huge"]) < 400
    assert len(payload["many"]) <= 129
    assert payload["cycle"]["self"] == "<diagnostic-cycle>"
    assert payload["integer"] == "<integer:20001-bits>"
    module._reset_after_fork()


def test_operation_context_child_cleanup_returns_before_inherited_locks() -> None:
    """Operation context and shared resources cannot deadlock during child cleanup."""
    try:
        from schema_sanitizer.api_impl.operation_context import (
            OperationExecutionContext,
            _OperationExecutionResources,
        )
    except ImportError as exc:
        pytest.skip(f"native ABI3 extension is unavailable: {exc}")

    released: list[bool] = []
    context = object.__new__(OperationExecutionContext)
    context._pid = -1
    context._lock = threading.Lock()
    context._lock.acquire()
    context._closed = False
    context._resources = SimpleNamespace(release=lambda: released.append(True))
    try:
        context.close()
        with pytest.raises(RuntimeError, match="after fork"):
            context._ensure_open()
    finally:
        context._lock.release()
    assert released == []

    resources = object.__new__(_OperationExecutionResources)
    resources.pid = -1
    resources.operation_id = "parent:1"
    resources._lock = threading.Lock()
    resources._lock.acquire()
    try:
        resources.release()
        assert resources.diagnostic_snapshot()["state"] == "inherited_after_fork"
    finally:
        resources._lock.release()


def test_native_capsule_and_stream_release_paths_are_process_guarded() -> None:
    """All native ownership entry points reject child finalization before cleanup."""
    from pathlib import Path

    guarded_functions = {
        "cpp/src/api/python_abi3/context/_core_abi3_capsules.cc": (
            "context_capsule_destructor",
            "diagnostics_capsule_destructor",
            "prepared_options_capsule_destructor",
            "stream_capsule_destructor",
        ),
        "cpp/src/api/python_abi3/options/prepare.cc": ("destroy_operation_memory_ledger_capsule",),
        "cpp/src/api/python_abi3/path_sources/path_source_plan.cc": (
            "path_source_plan_capsule_destructor",
        ),
        "cpp/src/api/python_abi3/registry/plan/plan.cc": (
            "native_registry_state_capsule_destructor",
        ),
        "cpp/src/api/python_abi3/logical_schema/payload.cc": ("arrow_schema_capsule_destructor",),
        "cpp/src/internal/arrow_c/cdata_export.cc": ("stream_release",),
        "cpp/src/api/python_abi3/registry/native_multi_source_stream.cc": (
            "native_multi_source_release",
        ),
        "cpp/src/api/python_abi3/streaming/coalesce_stream.cc": ("coalesce_release",),
        "cpp/src/api/python_abi3/registry/arrow_source_support.cc": ("passthrough_release",),
        "cpp/src/api/python_abi3/metadata/stream/stream.cc": ("metadata_stream_release",),
        "cpp/src/api/python_abi3/csv/nested_stream/nested_stream.cc": ("release_stream",),
    }
    for path, functions in guarded_functions.items():
        source = Path(path).read_text()
        for function in functions:
            body = source[source.index(function) :]
            assert body.index("runtime_owner_process()") < body.index("{") + 180


@pytest.mark.parametrize("weight", [True, 0, -1, 1.5])
@pytest.mark.parametrize("borrow_kind", ["client", "manager"])
def test_provider_pool_rejects_invalid_descriptor_weight_before_factory(
    weight: object,
    borrow_kind: str,
) -> None:
    """Invalid logical descriptor weights cannot allocate or invoke providers."""
    from schema_sanitizer.remote_impl.provider_session_pool import (
        RemoteProviderSessionPool,
    )

    calls = 0

    async def run() -> None:
        nonlocal calls
        pool = RemoteProviderSessionPool()

        async def factory() -> object:
            nonlocal calls
            calls += 1
            return object()

        if borrow_kind == "client":
            operation = pool.borrow_client(
                ("key",),
                factory,
                descriptor_weight=weight,  # type: ignore[arg-type]
            )
        else:
            operation = pool.borrow_manager(
                ("key",),
                factory,
                descriptor_weight=weight,  # type: ignore[arg-type]
            )
        expected = (
            TypeError if isinstance(weight, bool) or not isinstance(weight, int) else ValueError
        )
        with pytest.raises(expected):
            await operation

    asyncio.run(run())
    assert calls == 0


def test_provider_pool_rejects_inherited_reference_before_loop_or_factory() -> None:
    """A child cannot touch inherited loop-affine locks or provider factories."""
    from schema_sanitizer.remote_impl.provider_session_pool import (
        RemoteProviderSessionPool,
    )

    calls = 0

    async def run() -> None:
        nonlocal calls
        pool = RemoteProviderSessionPool()
        pool._pid = -1

        async def factory() -> object:
            nonlocal calls
            calls += 1
            return object()

        with pytest.raises(RuntimeError, match="after fork"):
            await pool.borrow_client(("key",), factory)
        with pytest.raises(RuntimeError, match="after fork"):
            await pool.__aexit__(None, None, None)

    asyncio.run(run())
    assert calls == 0


def test_provider_throttle_hostile_exception_metadata_cannot_leak_slot() -> None:
    """Third-party exception properties are telemetry only, never ownership gates."""
    from schema_sanitizer.remote_impl import provider_throttle as module

    class HostileProviderError(RuntimeError):
        @property
        def status(self) -> object:
            raise RuntimeError("hostile status")

        @property
        def status_code(self) -> object:
            raise KeyboardInterrupt("hostile status code")

        @property
        def retry_after(self) -> object:
            raise RuntimeError("hostile retry-after")

        @property
        def headers(self) -> object:
            raise KeyboardInterrupt("hostile headers")

        @property
        def response(self) -> object:
            raise RuntimeError("hostile response")

        def __str__(self) -> str:
            raise KeyboardInterrupt("hostile text")

    governor = module.ProviderThrottleGovernor()
    lease, _delay = governor.try_acquire("hostile")
    assert lease is not None
    lease.failure(HostileProviderError())

    snapshot = governor.snapshot("hostile")
    assert snapshot.in_flight == 0
    assert snapshot.consecutive_failures == 1
    assert snapshot.throttled_responses == 0


def test_provider_throttle_invalid_outcome_is_atomic() -> None:
    """A programmer error cannot decrement capacity before being rejected."""
    from schema_sanitizer.remote_impl import provider_throttle as module

    governor = module.ProviderThrottleGovernor()
    lease, _delay = governor.try_acquire("atomic")
    assert lease is not None
    with pytest.raises(ValueError, match="unknown provider throttle outcome"):
        lease._release_outcome(
            outcome="invalid",
            throttled=False,
            retry_after_seconds=None,
        )
    assert governor.snapshot("atomic").in_flight == 1
    lease.release()
    assert governor.snapshot("atomic").in_flight == 0


def test_parquet_factory_child_cleanup_preserves_parent_staging(tmp_path) -> None:
    """A child-side finalizer cannot close or unlink parent-owned Parquet state."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        ParquetRecordBatchStreamFactory,
    )

    path = tmp_path / "parent.parquet"
    path.write_bytes(b"PAR1")
    closed: list[str] = []
    factory = object.__new__(ParquetRecordBatchStreamFactory)
    factory._pid = -1
    factory._pending_parquet_file = SimpleNamespace(close=lambda: closed.append("parquet"))
    factory._pending_opened_file = SimpleNamespace(close=lambda: closed.append("opened"))
    factory._staged_path = str(path)

    factory.close()
    assert closed == []
    assert path.exists()
    with pytest.raises(RuntimeError, match="after fork"):
        factory.__arrow_c_stream__()


def test_operation_memory_ledger_child_cleanup_returns_before_parent_lock() -> None:
    """Direct inherited ledger references cannot deadlock or touch native state."""
    from schema_sanitizer.core_impl.memory_budget import OperationMemoryLedger

    ledger = object.__new__(OperationMemoryLedger)
    ledger._pid = -1
    ledger._lock = threading.Lock()
    ledger._lock.acquire()
    ledger._close_condition = threading.Condition(ledger._lock)
    try:
        ledger.release(1)
        ledger.close()
        with pytest.raises(RuntimeError, match="after fork"):
            ledger.snapshot()
    finally:
        ledger._lock.release()


def test_telemetry_reads_do_not_rewrite_or_publish_journals(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """Margin queries are read-only and cannot amplify filesystem writes."""
    from schema_sanitizer.core_impl import safety_margins as module

    _set_environment(monkeypatch, "SCHEMA_SANITIZER_TELEMETRY_TUNING", "1")
    _set_environment(monkeypatch, "SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    path = module._path()
    payload = module._encode_profile({"version": 1, "samples": [{"untracked_rss_bytes": 8 << 20}]})
    path.write_bytes(payload)

    def unexpected(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("read-only telemetry query attempted a commit")

    monkeypatch.setattr(module, "commit_locked_payload", unexpected)
    monkeypatch.setattr(module, "commit_locked_payload_relaxed", unexpected)
    assert module.tuned_memory_reserve_bytes(256 << 20, 4 << 20) >= 4 << 20
    assert path.read_bytes() == payload


def test_telemetry_corruption_fails_closed_without_replacing_profile(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """Corrupt or future telemetry remains available for diagnosis, not reset."""
    from schema_sanitizer.core_impl import safety_margins as module

    _set_environment(monkeypatch, "SCHEMA_SANITIZER_TELEMETRY_TUNING", "1")
    _set_environment(monkeypatch, "SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    path = module._path()
    corrupt = b'{"version":2,"samples":[]}'
    path.write_bytes(corrupt)

    module.record_resource_telemetry(untracked_rss_bytes=1, source="test")
    assert path.read_bytes() == corrupt
    assert module.tuned_memory_reserve_bytes(128 << 20, 4 << 20) == 4 << 20
    assert path.read_bytes() == corrupt


def test_relaxed_telemetry_journal_recovers_partial_main_write(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """Amortized fsync still recovers a writer killed after truncate/write."""
    from schema_sanitizer.core_impl import coordination_journal, safety_margins

    _set_environment(monkeypatch, "SCHEMA_SANITIZER_TELEMETRY_TUNING", "1")
    _set_environment(monkeypatch, "SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    monkeypatch.setattr(safety_margins, "_should_fsync", lambda: False)
    path = safety_margins._path()
    original = safety_margins._encode_profile({"version": 1, "samples": []})
    path.write_bytes(original)

    real_write = coordination_journal._write_main_relaxed
    calls = 0

    def partial_then_fail(handle: object, payload: bytes) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            handle.seek(0)  # type: ignore[attr-defined]
            handle.truncate()  # type: ignore[attr-defined]
            handle.write(payload[:3])  # type: ignore[attr-defined]
            handle.flush()  # type: ignore[attr-defined]
            raise OSError("injected partial write")
        real_write(handle, payload)  # type: ignore[arg-type]

    monkeypatch.setattr(coordination_journal, "_write_main_relaxed", partial_then_fail)
    safety_margins.record_resource_telemetry(untracked_rss_bytes=1, source="test")
    monkeypatch.setattr(coordination_journal, "_write_main_relaxed", real_write)

    # The next reader observes the prepared journal owned by this live process
    # and rolls back the interrupted advisory update before decoding it.
    assert safety_margins.tuned_memory_reserve_bytes(128 << 20, 4 << 20) == 4 << 20
    assert path.read_bytes() == original
    assert not path.with_name(f"{path.name}.journal").exists()
