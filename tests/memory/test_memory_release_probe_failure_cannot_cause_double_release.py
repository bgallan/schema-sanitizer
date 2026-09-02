"""Collects no-double-release lifecycle cases for directory discovery, partition lookahead,
remote coordinator startup or shutdown, queued-waiter exceptions, native diagnostics,
and generated or stream finalizers. Every worker slot and capsule has one terminal
owner; bounded joins and shutdown-aware finalizers avoid replaying native cleanup or
secure wiping."""

from __future__ import annotations

import asyncio
import weakref
from gc import collect
from threading import Event
from time import monotonic, sleep
from typing import Any

import pytest
from _support.synchronization import SCHEDULER_TIMEOUT_SECONDS


def test_directory_uri_matching_rejects_infinite_duplicate_sources() -> None:
    """Duplicate-only URI streams cannot spin forever waiting for another key."""
    from schema_sanitizer.input_impl.directory_inputs import DirectoryDiscoveryBuilder

    builder = DirectoryDiscoveryBuilder.from_uris(("a", "b"))
    observed = 0

    def duplicates() -> Any:
        """Yield one valid URI forever."""
        nonlocal observed
        while True:
            observed += 1
            yield "a"

    with pytest.raises(Exception, match="bounded scan window") as captured:
        builder.add(duplicates(), object())
    assert captured.value.detail["limit_name"] == "directory_uri_match_observations"
    assert observed == 65
    assert builder.files_by_uri == {"a": [], "b": []}


class _ThreadLeaseDouble:
    """Track release of one synthetic process-wide thread slot."""

    amount = 1

    def __init__(self) -> None:
        """Initialize the thread lease double test double."""
        self.released = Event()

    def release(self) -> None:
        """Release the resource held by the thread lease double test double."""
        self.released.set()


def test_partition_lookahead_thread_is_governed_and_drops_idle_task_graph(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The daemon owns a thread slot and retains no completed task arguments."""
    from schema_sanitizer.pipeline import partition_lookahead_worker as module

    lease = _ThreadLeaseDouble()
    executor = module.ThreadPoolExecutor(
        max_workers=1,
        thread_name_prefix="release-probe-failure-cannot-cause-double-lookahead",
        permit_factory=lambda *_a, **_k: lease,
    )

    class Payload:
        """Weak-referenceable task argument."""

    payload = Payload()
    retained = weakref.ref(payload)

    def consume(_value: object) -> None:
        """Consume one argument without retaining it."""

    future = executor.submit(consume, payload)
    assert future.result(timeout=SCHEDULER_TIMEOUT_SECONDS) is None
    del payload
    del future
    deadline = monotonic() + SCHEDULER_TIMEOUT_SECONDS
    while retained() is not None and monotonic() < deadline:
        collect()
        sleep(0.01)
    assert retained() is None

    executor.shutdown(wait=True, cancel_futures=True)
    assert lease.released.wait(SCHEDULER_TIMEOUT_SECONDS)


def test_remote_coordinator_governs_only_unreserved_host_threads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Standalone coordinators charge a slot while operation-owned ones borrow it."""
    from schema_sanitizer.remote_impl import io_coordinator as module

    leases: list[_ThreadLeaseDouble] = []

    def acquire(*_args: object, **_kwargs: object) -> _ThreadLeaseDouble:
        """Return and retain one synthetic thread lease."""
        lease = _ThreadLeaseDouble()
        leases.append(lease)
        return lease

    monkeypatch.setattr(module, "acquire_project_threads", acquire)
    standalone = module.RemoteIoCoordinator(shutdown_timeout_seconds=1.0)
    assert len(leases) == 1
    standalone.close()
    assert leases[0].released.wait(SCHEDULER_TIMEOUT_SECONDS)

    borrowed = module.RemoteIoCoordinator(
        shutdown_timeout_seconds=1.0,
        thread_slot_reserved=True,
    )
    assert len(leases) == 1
    borrowed.close()


def test_remote_waiter_baseexception_removes_queued_state() -> None:
    """Non-cancellation BaseException cannot leave a dead future in the queue."""
    from schema_sanitizer.remote_impl.io_permits import RemoteIoPermitGovernor

    class Abort(BaseException):
        """Synthetic control-flow abort."""

    async def exercise() -> None:
        """Abort one waiter while all weighted capacity is occupied."""
        governor = RemoteIoPermitGovernor(1)
        first = await governor.acquire()
        blocked = asyncio.create_task(governor.acquire())
        await asyncio.sleep(0)
        assert governor.snapshot().waiting == 1
        queue = next(iter(governor._operation_waiters.values()))
        waiter = next(iter(queue.values()))
        waiter.future.set_exception(Abort())
        try:
            await blocked
        except Abort:
            pass
        else:  # pragma: no cover - defensive assertion
            raise AssertionError("synthetic BaseException did not propagate")
        assert governor.snapshot().waiting == 0
        first.release()
        assert governor.snapshot().in_use == 0

    asyncio.run(exercise())


def test_native_diagnostics_close_freezes_json_and_releases_capsule(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicit sink cleanup no longer retains the live diagnostics capsule."""
    from schema_sanitizer.core_impl import native_results as module

    class Native:
        """Return one stable final diagnostics payload."""

        @staticmethod
        def diagnostics_json(_capsule: object) -> str:
            """Return the controlled native diagnostics payload."""
            return '{"batches":3}'

    monkeypatch.setattr(module, "_native", Native())
    capsule = object()
    output = module.SinkOutput(
        sink="stream",
        diagnostics_capsule=capsule,
        diagnostics_json="{}",
    )
    diagnostics = output.diagnostics

    output.close()

    assert diagnostics._diagnostics_capsule is None
    assert diagnostics.to_json() == '{"batches":3}'
    assert diagnostics.batches == 3


def test_native_diagnostics_finalizer_skips_runtime_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Interpreter teardown cannot trigger a native capsule decrement."""
    from schema_sanitizer.core_impl import native_results as module

    capsule = object()
    diagnostics = module.IngestDiagnostics(diagnostics_capsule=capsule)
    monkeypatch.setattr(module, "runtime_is_finalizing", lambda: True)

    diagnostics.__del__()

    assert diagnostics._diagnostics_capsule is capsule


def test_partition_lookahead_degrades_when_thread_window_is_full(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Optional speculative work falls back to synchronous preparation."""
    from types import SimpleNamespace

    from schema_sanitizer.errors import SchemaSanitizerResourceError
    from schema_sanitizer.pipeline import partition_lookahead as module

    monkeypatch.setattr(
        module,
        "execution_policy",
        lambda *_a, **_k: SimpleNamespace(is_single=False, effective_workers=2),
    )

    def exhausted(*_args: object, **_kwargs: object) -> object:
        """Reject the optional lookahead host slot."""
        raise SchemaSanitizerResourceError("thread window exhausted")

    monkeypatch.setattr(module, "acquire_project_threads", exhausted)
    lookahead = module.PartitionSourceLookahead(
        {"multi_threading": True},
        memory_limit_bytes=64 << 20,
    )

    assert lookahead.enabled is False
    assert lookahead._executor is None
    lookahead.close()


def test_remote_coordinator_start_failure_returns_thread_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed host-thread start cannot retain its process-wide permit."""
    from schema_sanitizer.remote_impl import io_coordinator as module

    lease = _ThreadLeaseDouble()
    monkeypatch.setattr(module, "acquire_project_threads", lambda *_a, **_k: lease)

    monkeypatch.setattr(
        module,
        "start_governed_thread",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("thread startup failed")),
    )
    with pytest.raises(RuntimeError, match="thread startup failed"):
        module.RemoteIoCoordinator(shutdown_timeout_seconds=1.0)
    assert lease.released.is_set()


def test_generated_bytes_finalizer_skips_secure_wipe_during_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Interpreter teardown does not spend time overwriting transient buffers."""
    from schema_sanitizer.core_impl import generated_bytes as module

    reader = module.BufferedGeneratedBytesReader(
        "release-probe-failure-cannot-cause-double", default_chunk_bytes=16
    )
    reader._buffer.extend(b"secret")
    monkeypatch.setattr(module, "runtime_is_finalizing", lambda: True)

    reader.__del__()

    assert reader._buffer == bytearray(b"secret")
    assert reader._closed is False


def test_stream_finalizer_skips_native_cleanup_during_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """API stream wrappers leave native cleanup to process teardown."""
    from schema_sanitizer.api_impl import streams as module

    class Raw:
        """Track whether finalizer cleanup reached the backend."""

        def __init__(self) -> None:
            """Initialize the raw test double."""
            self.closed = 0

        def close_main_stream(self) -> None:
            """Close the primary stream while recording lifecycle calls."""
            self.closed += 1

    raw = Raw()
    stream = module.ArrowCStream(raw)
    monkeypatch.setattr(module, "runtime_is_finalizing", lambda: True)

    stream.__del__()

    assert raw.closed == 0
    assert stream.raw is raw


def test_remote_coordinator_startup_error_join_is_bounded() -> None:
    """A broken startup cannot block forever while joining its host thread."""
    from schema_sanitizer.remote_impl.io_coordinator import RemoteIoCoordinator

    class ThreadDouble:
        """Record the requested timeout while remaining alive."""

        def __init__(self) -> None:
            """Initialize the thread double test double."""
            self.timeout: float | None = None

        def join(self, timeout: float | None = None) -> None:
            """Wait for the thread double test double to finish."""
            self.timeout = timeout

        def is_alive(self) -> bool:
            """Report whether the thread double test double is active."""
            return True

    coordinator = object.__new__(RemoteIoCoordinator)
    coordinator._startup_error = RuntimeError("startup failed")
    coordinator._shutdown_timeout_seconds = 0.25
    coordinator._thread = ThreadDouble()

    with pytest.raises(RuntimeError, match="startup failed") as captured:
        coordinator._raise_startup_error()

    assert coordinator._thread.timeout == 0.25
    assert any("bounded join" in note for note in captured.value.__notes__)


def test_remote_shutdown_keeps_manager_until_coroutine_starts() -> None:
    """A rejected loop submission cannot discard the provider manager handle."""
    from schema_sanitizer.remote_impl.io_coordinator import RemoteIoCoordinator

    manager = object()
    context = object()
    coordinator = object.__new__(RemoteIoCoordinator)
    coordinator._context_manager = manager
    coordinator._context = context

    shutdown = coordinator._shutdown(0.0)
    try:
        assert coordinator._context_manager is manager
        assert coordinator._context is context
    finally:
        shutdown.close()
