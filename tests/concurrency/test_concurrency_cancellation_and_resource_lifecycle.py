"""Exercise cancellation and resource ownership across local and remote concurrency services.

The tests connect writer reservations, nested deadlines, governor queues, cross-process memory,
provider throttling, diagnostics, fork safety, staged paths, janitor cleanup, and retry teardown.
They verify that interruption preserves progress and that ownership ends only after real cleanup.
"""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import os
import threading
import uuid
from pathlib import Path

import pytest
from _support.synchronization import SCHEDULER_TIMEOUT_SECONDS

from schema_sanitizer.core_impl.cancellation import (
    OperationCancellationToken,
    activate_operation_cancellation_token,
    operation_cancellation,
)
from schema_sanitizer.core_impl.cross_process_memory import (
    CrossProcessMemoryLease,
    cross_process_memory_reserved_bytes,
)
from schema_sanitizer.core_impl.operation_diagnostics import (
    complete_operation,
    process_operation_diagnostics,
    register_operation,
)
from schema_sanitizer.core_impl.process_resources import _Governor
from schema_sanitizer.errors import (
    SchemaSanitizerCancelledError,
    SchemaSanitizerResourceError,
)
from schema_sanitizer.remote_impl.file_streams import write_sync_reader_to_file
from schema_sanitizer.remote_impl.provider_throttle import ProviderThrottleGovernor

_REQUIRES_POSIX_COORDINATION = pytest.mark.skipif(
    os.name == "nt",
    reason="optional cross-process coordination requires POSIX advisory locks",
)


def _hold_cross_process_memory(
    directory: str,
    connection: multiprocessing.connection.Connection,
) -> None:
    """Hold one crash-recoverable resident-memory reservation in a child."""
    os.environ["SCHEMA_SANITIZER_CROSS_PROCESS_MEMORY_RESERVATIONS"] = "1"
    os.environ["SCHEMA_SANITIZER_COORDINATION_DIR"] = directory
    lease: CrossProcessMemoryLease | None = None
    try:
        lease = CrossProcessMemoryLease(100, 70)
        connection.send(("reserved", lease.reserved_bytes))
        if connection.recv() != "release":  # pragma: no cover - protocol guard
            raise RuntimeError("unexpected cross-process memory test command")
    except BaseException as exc:  # pragma: no cover - returned to parent
        try:
            connection.send(("error", type(exc).__name__, str(exc)))
        except (BrokenPipeError, EOFError, OSError):
            pass
    finally:
        if lease is not None:
            lease.release()
        connection.close()


def _fork_safety_child(result: multiprocessing.queues.Queue) -> None:
    """Report whether inherited runtime state is rejected after fork."""
    from schema_sanitizer.core_impl.fork_safety import ensure_runtime_fork_safe

    try:
        ensure_runtime_fork_safe()
    except RuntimeError as exc:
        result.put(str(exc))
    else:  # pragma: no cover - regression signal
        result.put("accepted")


class _StorageReservationSpy:
    """Record disk-admission calls made by a streamed writer."""

    def __init__(self) -> None:
        """Initialize the storage reservation spy test double."""
        self.events: list[tuple[str, int]] = []

    def reset_after_truncate(self) -> None:
        """Reset the storage reservation after output truncation."""
        self.events.append(("reset", 0))

    def before_write(self, chunk_bytes: int) -> None:
        """Record the byte reservation requested before a streamed write."""
        self.events.append(("reserve", chunk_bytes))

    def finalize(self, actual_size_bytes: int) -> None:
        """Record the final on-disk size reported by the writer."""
        self.events.append(("finalize", actual_size_bytes))


def test_streaming_writer_reserves_disk_before_every_write(tmp_path: Path) -> None:
    """Unknown-length downloads grow the storage lease before bytes reach disk."""
    chunks = iter((b"abc", b"defgh", b""))
    spy = _StorageReservationSpy()
    output = tmp_path / "stream.bin"

    written = write_sync_reader_to_file(
        lambda _size: next(chunks),
        str(output),
        chunk_bytes=2,
        storage_reservation=spy,  # type: ignore[arg-type]
    )

    assert written == 8
    assert output.read_bytes() == b"abcdefgh"
    assert spy.events == [
        ("reset", 0),
        ("reserve", 3),
        ("reserve", 5),
        ("finalize", 8),
    ]


def test_operation_cancellation_deadline_and_manual_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One public token covers deadline and explicit cooperative cancellation."""
    from schema_sanitizer.core_impl import cancellation as cancellation_module

    now = [100.0]
    monkeypatch.setattr(cancellation_module, "monotonic", lambda: now[0])
    with operation_cancellation(timeout_seconds=0.02) as token:
        assert not token.cancelled()
        assert token.deadline == 100.02
        now[0] = 100.02
        with pytest.raises(SchemaSanitizerCancelledError, match="cancelled"):
            token.raise_if_cancelled(stage="deadline_test")

    manual = OperationCancellationToken()
    manual.cancel()
    with activate_operation_cancellation_token(manual):
        with pytest.raises(SchemaSanitizerCancelledError):
            manual.raise_if_cancelled(stage="manual_test")


def test_nested_cancellation_scope_inherits_parent_event() -> None:
    """Nested scopes cannot detach work from a cancelled parent operation."""
    with operation_cancellation() as parent:
        with operation_cancellation(timeout_seconds=1) as child:
            parent.cancel()
            assert child.cancelled()
            with pytest.raises(SchemaSanitizerCancelledError):
                child.raise_if_cancelled(stage="nested")


def test_cancellation_scope_cancels_token_on_exit() -> None:
    """Workers retaining a scope token observe cancellation after scope exit."""
    with operation_cancellation() as token:
        assert not token.cancelled()
    assert token.cancelled()


def test_async_bridge_has_a_finite_default_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The sync-to-async bridge cannot block forever without a public deadline."""
    from schema_sanitizer.remote_impl import async_bridge

    monkeypatch.setattr(async_bridge, "bounded_wait_timeout", lambda _default: 0.01)

    async def exercise() -> None:
        """Invoke the synchronous bridge from an active event loop."""

        async def slow() -> None:
            """Remain suspended until the bounded bridge cancels this task."""
            await asyncio.Event().wait()

        with pytest.raises(TimeoutError, match="bounded wait"):
            async_bridge.run_sync(slow(), threading_mode="multi")

    asyncio.run(exercise())


def test_cancelled_governor_ticket_does_not_block_followers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A timed-out FIFO ticket is skipped so later requests still progress."""
    governor = _Governor(1, "test_slots")
    holder = governor.acquire()
    timed_out = threading.Event()
    follower_acquired = threading.Event()
    first_waiting = threading.Event()
    original_wait = governor._condition.wait  # noqa: SLF001

    def observe_wait(timeout: float | None = None) -> bool:
        """Publish that the first blocked ticket reached the condition wait."""
        first_waiting.set()
        return original_wait(timeout)

    monkeypatch.setattr(governor._condition, "wait", observe_wait)  # noqa: SLF001

    def timeout_waiter() -> None:
        """Attempt the permit acquisition expected to time out."""
        try:
            governor.acquire(timeout_seconds=0.05)
        except SchemaSanitizerResourceError:
            timed_out.set()

    def follower() -> None:
        """Acquire and release the permit after the cancelled waiter."""
        lease = governor.acquire(timeout_seconds=SCHEDULER_TIMEOUT_SECONDS)
        follower_acquired.set()
        lease.release()

    first = threading.Thread(target=timeout_waiter)
    second = threading.Thread(target=follower)
    first.start()
    assert first_waiting.wait(timeout=SCHEDULER_TIMEOUT_SECONDS)
    second.start()
    assert timed_out.wait(timeout=SCHEDULER_TIMEOUT_SECONDS)
    holder.release()
    assert follower_acquired.wait(timeout=SCHEDULER_TIMEOUT_SECONDS)
    first.join(timeout=SCHEDULER_TIMEOUT_SECONDS)
    second.join(timeout=SCHEDULER_TIMEOUT_SECONDS)
    assert governor.snapshot().in_use == 0


@_REQUIRES_POSIX_COORDINATION
def test_cross_process_memory_rejects_combined_overcommit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Independent workers cannot simultaneously reserve the same RAM headroom."""
    monkeypatch.setenv("SCHEMA_SANITIZER_CROSS_PROCESS_MEMORY_RESERVATIONS", "1")
    monkeypatch.setenv("SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    context = multiprocessing.get_context("spawn")
    parent_connection, child_connection = context.Pipe(duplex=True)
    child = context.Process(
        target=_hold_cross_process_memory,
        args=(str(tmp_path), child_connection),
    )
    child.start()
    child_connection.close()
    try:
        assert parent_connection.poll(30), "child reservation handshake timed out"
        assert parent_connection.recv() == ("reserved", 70)
        with pytest.raises(SchemaSanitizerResourceError, match="cross-process"):
            CrossProcessMemoryLease(100, 40)
        assert cross_process_memory_reserved_bytes() == 70
    finally:
        try:
            parent_connection.send("release")
        except (BrokenPipeError, EOFError, OSError):
            pass
        parent_connection.close()
        child.join(timeout=30)
        if child.is_alive():  # pragma: no cover - emergency anti-hang cleanup
            child.terminate()
            child.join(timeout=30)
    assert child.exitcode == 0
    assert cross_process_memory_reserved_bytes() == 0


@_REQUIRES_POSIX_COORDINATION
def test_cross_process_memory_reclaims_dead_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A dead process cannot leave resident-memory admission permanently wedged."""
    monkeypatch.setenv("SCHEMA_SANITIZER_CROSS_PROCESS_MEMORY_RESERVATIONS", "1")
    monkeypatch.setenv("SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    path = tmp_path / "schema-sanitizer-resident-memory.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "leases": {
                    "999999:dead": {
                        "pid": 999999,
                        "start": "dead",
                        "reserved": 90,
                        "updated": 0,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    lease = CrossProcessMemoryLease(100, 80)
    assert lease.reserved_bytes == 80
    lease.release()
    assert cross_process_memory_reserved_bytes() == 0


def test_provider_throttle_neutral_release_does_not_open_circuit() -> None:
    """Abandoned leases release capacity without being counted as provider faults."""
    governor = ProviderThrottleGovernor()
    lease, _delay = governor.try_acquire("neutral")
    assert lease is not None
    lease.release()
    snapshot = governor.snapshot("neutral")
    assert snapshot.in_flight == 0
    assert snapshot.consecutive_failures == 0


def test_provider_throttle_reduces_window_and_opens_bounded_circuit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated throttling applies multiplicative decrease and bounded fail-fast."""
    from schema_sanitizer.remote_impl import provider_throttle

    monkeypatch.setattr(provider_throttle, "monotonic", lambda: 100.0)
    governor = ProviderThrottleGovernor()
    initial = governor.snapshot("api").window
    for _ in range(5):
        lease, delay = governor.try_acquire("api")
        assert lease is not None, delay
        error = RuntimeError("429 too many requests")
        error.status = 429  # type: ignore[attr-defined]
        lease.failure(error)
    snapshot = governor.snapshot("api")
    assert snapshot.window < initial
    assert snapshot.throttled_responses == 5
    assert snapshot.circuit_open_until == 101.0


def test_operation_diagnostics_separate_live_and_completed_operations() -> None:
    """Concurrent callers can query a bounded operation-specific diagnostic record."""
    operation_id = f"test-{uuid.uuid4().hex}"

    class Owner:
        """Expose a bound-method snapshot without global retention."""

        def snapshot(self) -> dict[str, object]:
            """Return a snapshot of the state recorded by the test double."""
            return {"operation_id": operation_id, "state": "running", "workers": 2}

    owner = Owner()
    register_operation(operation_id, owner.snapshot)
    assert process_operation_diagnostics(operation_id) == (
        {"operation_id": operation_id, "state": "running", "workers": 2},
    )
    complete_operation(operation_id, {"state": "completed", "rows": 7})
    completed = process_operation_diagnostics(operation_id)
    assert len(completed) == 1
    assert completed[0]["state"] == "completed"
    assert completed[0]["rows"] == 7


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork is unavailable")
def test_initialized_runtime_fails_fast_after_fork() -> None:
    """A forked child is told to use spawn/exec instead of inheriting runtime locks."""
    from schema_sanitizer.core_impl.fork_safety import ensure_runtime_fork_safe

    ensure_runtime_fork_safe()
    context = multiprocessing.get_context("fork")
    result = context.Queue()
    child = context.Process(target=_fork_safety_child, args=(result,))
    child.start()
    message = result.get(timeout=SCHEDULER_TIMEOUT_SECONDS)
    child.join(timeout=SCHEDULER_TIMEOUT_SECONDS)
    assert child.exitcode == 0
    assert "spawn" in message
    assert "forkserver" in message


def test_provider_throttle_respects_retry_after_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A provider Retry-After response blocks new admission for that endpoint."""
    from schema_sanitizer.remote_impl import provider_throttle

    monkeypatch.setattr(provider_throttle, "monotonic", lambda: 100.0)
    governor = ProviderThrottleGovernor()
    lease, _delay = governor.try_acquire("retry-after")
    assert lease is not None

    class Throttled(RuntimeError):
        """Minimal provider exception carrying HTTP retry metadata."""

        status = 429
        headers = {"Retry-After": "2"}

    lease.failure(Throttled("rate limited"))
    next_lease, delay = governor.try_acquire("retry-after")
    assert next_lease is None
    assert delay == 1.0
    assert governor.snapshot("retry-after").circuit_open_until == 102.0


def test_staged_path_retains_lease_when_cleanup_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A resistant temporary artifact is quarantined without releasing capacity."""
    from schema_sanitizer.remote_impl import staging_paths

    directory = tmp_path / "resistant"
    directory.mkdir()
    (directory / "data.bin").write_bytes(b"payload")

    class Lease:
        """Track whether capacity was returned prematurely."""

        def __init__(self) -> None:
            """Initialize the lease test double."""
            self.releases = 0

        def release(self) -> None:
            """Release the resource held by the lease test double."""
            self.releases += 1

    lease = Lease()
    captured: dict[str, object] = {}

    def fail_delete(_path: Path) -> None:
        """Raise the cleanup failure injected before staged lease release."""
        raise OSError("busy")

    def capture(path: Path, *, is_dir: bool, lease: object, expected_identity: object) -> bool:
        """Capture the staged path retained after cleanup failure."""
        captured.update(
            path=path,
            is_dir=is_dir,
            lease=lease,
            expected_identity=expected_identity,
        )
        return True

    monkeypatch.setattr(staging_paths.shutil, "rmtree", fail_delete)
    monkeypatch.setattr(staging_paths, "quarantine_temporary_artifact", capture)
    staged = staging_paths.StagedPath(
        str(directory),
        is_dir=True,
        storage_lease=lease,  # type: ignore[arg-type]
    )
    staged.close()

    retained_path = Path(captured["path"])
    assert retained_path.exists()
    assert retained_path != directory
    assert lease.releases == 0
    assert captured["is_dir"] is True
    assert captured["lease"] is lease
    assert captured["expected_identity"] is not None


def test_temporary_janitor_releases_only_after_actual_deletion(tmp_path: Path) -> None:
    """The cleanup janitor keeps storage charged until its retry succeeds."""
    from schema_sanitizer.core_impl.temporary_janitor import _TemporaryArtifactJanitor

    path = tmp_path / "quarantined.bin"
    path.write_bytes(b"payload")

    class Lease:
        """Count exact lease releases."""

        def __init__(self) -> None:
            """Initialize the lease test double."""
            self.releases = 0

        def release(self) -> None:
            """Release the resource held by the lease test double."""
            self.releases += 1

    lease = Lease()
    janitor = _TemporaryArtifactJanitor()
    janitor._scanned = True  # avoid unrelated global quarantine leftovers
    janitor._ensure_worker = lambda: None  # type: ignore[method-assign]
    outcomes = iter((False, True))

    def delete_owned(path, _is_dir, identity):
        """Return one failed and then one successful identity-safe deletion."""
        deleted = next(outcomes)
        return deleted, path, identity, False

    janitor._delete_owned = delete_owned  # type: ignore[method-assign]
    janitor.quarantine(path, is_dir=False, lease=lease)  # type: ignore[arg-type]
    janitor.sweep()
    assert lease.releases == 0
    assert janitor.snapshot().pending_artifacts == 1
    janitor.sweep()
    assert lease.releases == 1
    assert janitor.snapshot().pending_artifacts == 0


def test_cancelled_retry_is_never_replayed() -> None:
    """The generic retry scheduler must not reinterpret cancellation as transient I/O."""
    from schema_sanitizer.core_impl.async_scheduler import retry_async

    attempts = 0

    async def operation() -> None:
        """Raise the stable cancellation error on the first call."""
        nonlocal attempts
        attempts += 1
        raise SchemaSanitizerCancelledError("stop")

    with pytest.raises(SchemaSanitizerCancelledError):
        asyncio.run(retry_async(operation, retries=8))
    assert attempts == 1


def test_public_cancellation_scope_reaches_conversion(tmp_path: Path) -> None:
    """The public scope aborts a native-backed conversion before it starts work."""
    import schema_sanitizer as ss

    source = tmp_path / "input.csv"
    output = tmp_path / "output.jsonl"
    source.write_text("value\n1\n", encoding="utf-8")
    with ss.operation_cancellation() as token:
        token.cancel()
        with pytest.raises(SchemaSanitizerCancelledError):
            ss.to_jsonl(source, output, input_format="csv")
    assert not output.exists()


def test_completed_operation_diagnostics_include_cross_resource_snapshots(
    tmp_path: Path,
) -> None:
    """A real conversion records bounded process and operation resource metrics."""
    import schema_sanitizer as ss

    before = {item.get("operation_id") for item in ss.process_operation_diagnostics()}
    source = tmp_path / "diag.csv"
    output = tmp_path / "diag.jsonl"
    source.write_text("value\n1\n", encoding="utf-8")
    ss.to_jsonl(source, output, input_format="csv", multi_threading=True)
    created = [
        item
        for item in ss.process_operation_diagnostics()
        if item.get("operation_id") not in before
    ]
    assert created
    latest = created[-1]
    assert latest["state"] == "closed"
    assert "process_threads" in latest
    assert "process_file_descriptors" in latest
    assert "process_temporary_storage" in latest
    assert "temporary_janitor" in latest
    assert "system_pressure" in latest
