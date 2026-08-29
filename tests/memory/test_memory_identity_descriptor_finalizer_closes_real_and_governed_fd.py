"""Exercises finalizer-driven descriptor closure across staged-path handoff, remote-permit
retry, janitor deletion, quarantine, teardown dispatch, diagnostics, file-limit
exhaustion, deadlines, queue weights, and arena locking. Real and governed descriptors
close under one identity owner, while bounded queues skip nonfitting work and retain
failures without linear scans."""

from __future__ import annotations

import asyncio
import errno
import gc
import os
import threading
import time
from concurrent.futures import Future
from pathlib import Path
from types import SimpleNamespace

import pytest
from _support.synchronization import SCHEDULER_TIMEOUT_SECONDS, join_thread_or_fail

pytestmark = pytest.mark.skipif(
    os.name == "nt", reason="POSIX descriptor-relative filesystem hardening suite"
)


def test_identity_descriptor_finalizer_closes_real_and_governed_fd(
    tmp_path: Path,
) -> None:
    """Verify identity descriptor finalizer closes real and governed FD."""
    from schema_sanitizer.core_impl.path_identity import claim_path_identity

    path = tmp_path / "owned"
    path.write_text("x")
    identity = claim_path_identity(path)
    assert identity is not None
    owner = identity.descriptor_owner
    assert owner is not None
    descriptor = owner.descriptor_snapshot()
    lease = owner.fd_lease
    assert descriptor is not None
    assert lease is not None
    os.fstat(descriptor)
    del identity
    gc.collect()

    # Finalization publishes this owner for bounded safe-point cleanup. Another
    # cleanup worker may already have claimed the slot, so GC completion does
    # not imply that the physical close has committed synchronously.
    deadline = time.monotonic() + SCHEDULER_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if owner.descriptor_snapshot() is None and owner.fd_lease is None and lease._released:
            break
        time.sleep(0.01)

    assert owner.descriptor_snapshot() is None
    assert owner.fd_lease is None
    # Check the resources owned by this identity. Global counts may change
    # concurrently while bounded cleanup workers make progress.
    assert lease._released
    with pytest.raises(OSError) as captured:
        os.fstat(descriptor)
    assert captured.value.errno == errno.EBADF


def test_staged_path_handoff_transfers_original_claim_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify staged path handoff transfers original claim owner."""
    import schema_sanitizer.remote_impl.staging_paths as module
    from schema_sanitizer.core_impl.path_identity import release_path_identity
    from schema_sanitizer.remote_impl.staging_paths import StagedPath

    path = tmp_path / "stage"
    path.write_text("payload")
    lease = SimpleNamespace(release=lambda: None)
    owner = StagedPath(str(path), storage_lease=lease)
    original = owner._identity
    captured: dict[str, object] = {}
    original_unlink = Path.unlink

    def fail_private_unlink(current: Path, *args: object, **kwargs: object) -> None:
        """Inject the private unlink failure at the controlled test point."""
        if current.parent.name == ".schema-sanitizer-delete":
            raise OSError("busy")
        original_unlink(current, *args, **kwargs)

    def accept(
        current: Path,
        *,
        is_dir: bool,
        lease: object,
        expected_identity: object,
    ) -> bool:
        """Accept the controlled replacement submitted by the test."""
        captured["path"] = current
        captured["identity"] = expected_identity
        return True

    monkeypatch.setattr(Path, "unlink", fail_private_unlink)
    monkeypatch.setattr(module, "quarantine_temporary_artifact", accept)
    owner.close()
    assert captured["identity"] is original
    release_path_identity(original)
    os.unlink(captured["path"])


def test_successful_remote_result_retains_failed_permit_for_retry() -> None:
    """Verify successful remote result retains failed permit for retry."""
    from schema_sanitizer.remote_impl.io_coordinator import RemoteIoCoordinator

    class Reservation:
        def release(self) -> None:
            """Release the resource held by the reservation test double."""
            return None

    class Permit:
        def __init__(self) -> None:
            """Initialize the permit test double."""
            self.calls = 0

        def release(self) -> None:
            """Release the resource held by the permit test double."""
            self.calls += 1
            if self.calls == 1:
                raise OSError("release")

    permit = Permit()

    class Governor:
        def reserve_submission(self) -> Reservation:
            """Reserve one controlled asynchronous submission."""
            return Reservation()

        async def acquire(self, *args: object, **kwargs: object) -> Permit:
            """Acquire the resource represented by the governor test double."""
            return permit

    coordinator = RemoteIoCoordinator(
        permit_governor=Governor(),
        permit_capacity=None,
        shutdown_timeout_seconds=2.0,
    )
    future = coordinator.submit(lambda _context: asyncio.sleep(0, result="ok"))
    assert future.result(timeout=SCHEDULER_TIMEOUT_SECONDS) == "ok"
    submission = future._schema_sanitizer_remote_submission
    assert submission.task_error is None
    assert isinstance(submission.permit_cleanup_error, OSError)
    coordinator.close()
    assert permit.calls == 2


def test_janitor_private_delete_location_is_idempotent(tmp_path: Path) -> None:
    """Verify janitor private delete location is idempotent."""
    from schema_sanitizer.core_impl.temporary_janitor import (
        _TemporaryArtifactJanitor,
    )

    private = tmp_path / ".delete" / "delete-1"
    private.parent.mkdir()
    private.write_text("x")
    assert _TemporaryArtifactJanitor._private_delete_path(private) == private


def test_late_quarantine_commit_keeps_post_close_worker_route(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify late quarantine commit keeps post close worker route."""
    from schema_sanitizer.core_impl.path_identity import release_path_identity
    from schema_sanitizer.core_impl.temporary_janitor import (
        _TemporaryArtifactJanitor,
    )

    source = tmp_path / "late"
    source.write_text("x")
    janitor = _TemporaryArtifactJanitor()
    started = threading.Event()
    proceed = threading.Event()
    real_replace = os.replace

    def blocked_replace(left: object, right: object, **kwargs: object) -> None:
        """Pause at the blocked replace synchronization point."""
        started.set()
        assert proceed.wait(SCHEDULER_TIMEOUT_SECONDS)
        real_replace(left, right, **kwargs)

    ensured = threading.Event()
    monkeypatch.setattr(os, "replace", blocked_replace)
    monkeypatch.setattr(janitor, "_ensure_worker", ensured.set)
    lease = SimpleNamespace(release=lambda: None)
    result: list[bool] = []
    thread = threading.Thread(
        target=lambda: result.append(janitor.quarantine(source, is_dir=False, lease=lease))
    )
    thread.start()
    assert started.wait(SCHEDULER_TIMEOUT_SECONDS)
    with janitor._condition:
        janitor._closed = True
    proceed.set()
    join_thread_or_fail(thread)
    assert result == [True]
    assert ensured.is_set()
    assert len(janitor._pending) == 1
    artifact = next(iter(janitor._pending.values()))
    release_path_identity(artifact.identity)
    artifact.path.unlink()


def test_cleanup_dispatcher_uses_teardown_reserve_when_public_envelope_is_full(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Terminal cleanup progresses off-thread behind a saturated operation."""
    import schema_sanitizer.core_impl.cleanup_dispatcher as module

    callback_thread: list[int] = []
    callback_done = threading.Event()
    lease_released = threading.Event()
    teardown_acquired = threading.Event()

    class Lease:
        def release(self) -> None:
            """Release the resource held by the lease test double."""
            lease_released.set()

    def public_full(*_args: object, **_kwargs: object) -> object:
        """Raise the deliberate failure for the public full path."""
        raise RuntimeError("public thread envelope is full")

    def acquire_teardown(*_args: object, **_kwargs: object) -> Lease:
        """Acquire the teardown reserve after external admission closes."""
        teardown_acquired.set()
        return Lease()

    monkeypatch.setattr(module, "acquire_project_threads", public_full)
    monkeypatch.setattr(module, "acquire_teardown_project_threads", acquire_teardown)
    monkeypatch.setattr(module, "start_governed_thread", lambda worker: worker.start())

    def retire(_worker: threading.Thread, release: object) -> bool:
        """Retire the tracked owner at the controlled lifecycle point."""
        assert callable(release)
        release()
        return True

    monkeypatch.setattr(module, "defer_governed_thread_retirement", retire)
    dispatcher = module._CleanupDispatcher()
    caller_thread = threading.get_ident()

    def cleanup() -> None:
        """Record the cleanup thread and signal its completion."""
        callback_thread.append(threading.get_ident())
        callback_done.set()

    assert dispatcher.submit(cleanup, retained_bytes=1)
    assert callback_done.wait(SCHEDULER_TIMEOUT_SECONDS)
    assert teardown_acquired.is_set()
    assert callback_thread != [caller_thread]
    assert lease_released.wait(SCHEDULER_TIMEOUT_SECONDS)


def test_terminal_callback_diagnostics_are_bounded() -> None:
    """Verify terminal callback diagnostics are bounded."""
    from schema_sanitizer.remote_impl.io_coordinator import _RemoteIoSubmission

    owner = _RemoteIoSubmission(SimpleNamespace(release=lambda: None))
    owner.future = Future()
    owner.future.set_result(None)

    def fail(_future: Future[object]) -> None:
        """Raise the deliberate failure injected by the test."""
        raise OSError("cleanup")

    for _index in range(10):
        owner.callbacks_pending += 1
        owner.callback_quiescent.clear()
        owner._complete_callback(fail)
    assert owner.callback_failure_count == 10
    assert len(owner.callback_errors) <= 2


def test_cleanup_dispatcher_enforces_retained_byte_capacity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify cleanup dispatcher enforces retained byte capacity."""
    import schema_sanitizer.core_impl.cleanup_dispatcher as module

    dispatcher = module._CleanupDispatcher()
    monkeypatch.setattr(dispatcher, "_ensure_workers", lambda: None)
    assert dispatcher.submit(lambda: None, retained_bytes=module._MAX_PENDING_BYTES)
    assert not dispatcher.submit(lambda: None, retained_bytes=1)
    snapshot = dispatcher.snapshot()
    assert snapshot.pending_bytes == module._MAX_PENDING_BYTES
    assert snapshot.rejected_bytes == 1


def test_path_identity_fails_closed_on_emfile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify path identity fails closed on emfile."""
    import schema_sanitizer.core_impl.path_identity as module

    path = tmp_path / "fd-pressure"
    path.write_text("x")
    real_open = os.open

    def fail_open(target: object, flags: int, *args: object, **kwargs: object) -> int:
        """Inject the open failure at the controlled test point."""
        if Path(target) == path:
            raise OSError(errno.EMFILE, "too many open files")
        return real_open(target, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", fail_open)
    with pytest.raises(OSError) as captured:
        module.claim_path_identity(path)
    assert captured.value.errno == errno.EMFILE


def test_external_claim_reads_are_bounded(tmp_path: Path) -> None:
    """Verify external claim reads are bounded."""
    import schema_sanitizer.core_impl.path_identity as module

    claim = tmp_path / "claim-large"
    claim.write_bytes(b"x" * (module._MAX_CLAIM_BYTES + 1))
    with pytest.raises(OSError, match="size limit"):
        module._read_claim_bytes(claim)


def test_native_and_local_deadline_contracts() -> None:
    """Verify native and local deadline contracts."""
    root = Path(__file__).resolve().parents[2]
    header = (root / "cpp/src/internal/runtime/operation_task_arena.hh").read_text()
    source = (root / "cpp/src/internal/runtime/operation_task_arena.cc").read_text()
    memory = (root / "src/schema_sanitizer/core_impl/memory_budget.py").read_text()
    storage = (root / "src/schema_sanitizer/core_impl/temporary_storage.py").read_text()
    assert "bool explicit_charge = false" in header
    assert "unknown_charge_submissions" in header
    assert "unknown task charge rejected under " in source
    assert '"pressure"' in source
    assert "slot->abandoned_tasks.clear()" in source
    assert "operation memory ledger close exceeded its deadline" in memory
    assert "temporary-storage admissions exceeded their close deadline" in storage


def test_weight_buckets_skip_nonfitting_operations_without_linear_scan() -> None:
    """Verify weight buckets skip nonfitting operations without linear scan."""
    import asyncio

    from schema_sanitizer.remote_impl.io_permits import RemoteIoPermitGovernor, _Waiter

    loop = asyncio.new_event_loop()
    try:
        governor = RemoteIoPermitGovernor(capacity=4, max_waiters=5000)
        with governor._lock:
            for index in range(4095):
                governor._enqueue_waiter_locked(
                    _Waiter(loop, loop.create_future(), 4, "heavy", f"op-{index}")
                )
            light = _Waiter(loop, loop.create_future(), 1, "light", "light-op")
            governor._enqueue_waiter_locked(light)
            candidate_calls = 0
            bucket_weight_calls = 0
            effective_weight_calls = 0
            original_candidate = governor._operation_candidate_locked
            original_bucket_weight = governor._operation_bucket_weight_locked
            original_effective_weight = governor._effective_weight

            def counted_candidate(*args: object, **kwargs: object) -> object:
                """Record counted candidate for the enclosing assertion."""
                nonlocal candidate_calls
                candidate_calls += 1
                return original_candidate(*args, **kwargs)

            def counted_bucket_weight(*args: object, **kwargs: object) -> int | None:
                """Record counted bucket weight for the enclosing assertion."""
                nonlocal bucket_weight_calls
                bucket_weight_calls += 1
                return original_bucket_weight(*args, **kwargs)

            def counted_effective_weight(*args: object, **kwargs: object) -> int:
                """Record counted effective weight for the enclosing assertion."""
                nonlocal effective_weight_calls
                effective_weight_calls += 1
                return original_effective_weight(*args, **kwargs)

            governor._operation_candidate_locked = counted_candidate  # type: ignore[method-assign]
            governor._operation_bucket_weight_locked = counted_bucket_weight  # type: ignore[method-assign]
            governor._effective_weight = counted_effective_weight  # type: ignore[method-assign]
            selected = governor._take_candidate_locked(1)
        assert selected is light
        assert candidate_calls == 1
        assert bucket_weight_calls == 2
        assert effective_weight_calls == 4
    finally:
        loop.close()


def test_arena_clears_swapped_queue_only_while_slot_mutex_is_held() -> None:
    """Verify arena clears swapped queue only while slot mutex is held."""
    root = Path(__file__).resolve().parents[2]
    source = (root / "cpp/src/internal/runtime/operation_task_arena.cc").read_text()
    swap = source.index("drain.swap(slot->tasks);")
    clear = source.index("slot->tasks.clear();", swap)
    unlock = source.index("\n      }", swap)
    assert swap < clear < unlock
