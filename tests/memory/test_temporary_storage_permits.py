"""Exercises temporary-storage permits for path identity, bounded symlink-safe tree
measurement, pool reuse or oversize rejection, zero-capability growth, staged resize,
remote output, and cancelled or failed prefetch. Bytes and files remain charged through
upload or consumption, then exact permits release on every failure without following
external links."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest


def test_path_identity_fallback_avoids_posix_directory_claims(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Non-POSIX platforms retain a releasable fingerprint without dir FDs."""
    import schema_sanitizer.core_impl.path_identity as identity_module

    path = tmp_path / "owned-directory"
    path.mkdir()
    monkeypatch.setattr(identity_module, "_HAS_POSIX_PATH_AUTHORITY", False)

    identity = identity_module.claim_path_identity(path)

    assert identity is not None
    assert identity.owns_claim
    assert identity.external_claim_path is None
    assert identity.descriptor_owner is None
    assert identity_module.lstat_identity(path) == identity
    identity_module.release_path_identity(identity)


def test_path_based_tree_measurement_is_bounded_and_does_not_follow_symlinks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The Windows-compatible traversal counts regular files without link descent."""
    import schema_sanitizer.remote_impl.staging_paths as staging_module

    root = tmp_path / "staged-directory"
    nested = root / "nested"
    nested.mkdir(parents=True)
    (nested / "payload.bin").write_bytes(b"12345")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "ignored.bin").write_bytes(b"x" * 100)
    try:
        (root / "linked").symlink_to(outside, target_is_directory=True)
    except OSError:
        pass
    monkeypatch.setattr(staging_module, "_HAS_DESCRIPTOR_RELATIVE_TRAVERSAL", False)
    staged = staging_module.StagedPath(str(root), is_dir=True)
    try:
        assert staged._measure_owned_tree() == (5, 4 if (root / "linked").exists() else 3)
    finally:
        staged.close()


def test_temporary_storage_pool_bounds_and_reuses_released_capacity(require_native: None) -> None:
    """Reservations are aggregate, resizable, and returned exactly once."""
    from schema_sanitizer.core_impl.temporary_storage import TemporaryStoragePermitPool

    pool = TemporaryStoragePermitPool(16 << 20)
    assert pool.limit_bytes == 64 << 20
    first = pool.acquire(48 << 20, label="first")
    assert pool.try_acquire(20 << 20, label="blocked") is None

    first.resize(8 << 20)
    second = pool.acquire(20 << 20, label="second")
    snapshot = pool.snapshot()
    assert snapshot.reserved_bytes == 28 << 20
    assert snapshot.peak_reserved_bytes == 48 << 20
    assert snapshot.active_leases == 2

    first.release()
    first.release()
    second.release()
    assert pool.snapshot().reserved_bytes == 0
    assert pool.snapshot().active_leases == 0


def test_temporary_storage_pool_rejects_one_oversized_artifact(require_native: None) -> None:
    """One artifact cannot silently exceed the derived operation spool cap."""
    from schema_sanitizer.core_impl.temporary_storage import TemporaryStoragePermitPool
    from schema_sanitizer.errors import SchemaSanitizerResourceError

    pool = TemporaryStoragePermitPool(16 << 20)
    with pytest.raises(SchemaSanitizerResourceError, match="temporary storage limit") as captured:
        pool.acquire(pool.limit_bytes + 1, label="oversized")

    assert captured.value.detail == {
        "stage": "temporary_storage",
        "limit_name": "temporary_storage_bytes",
        "limit_bytes": pool.limit_bytes,
        "actual_bytes": pool.limit_bytes + 1,
        "artifact": "oversized",
    }


def test_zero_temporary_storage_capability_can_grow_and_release(
    tmp_path: Path, require_native: None
) -> None:
    """A zero-byte, zero-inode lease still owns an exact resizable capability."""
    from schema_sanitizer.core_impl.temporary_storage import TemporaryStoragePermitPool

    pool = TemporaryStoragePermitPool(16 << 20)
    lease = pool.acquire(0, label="zero-capability", path=tmp_path, artifact_count=0)
    lease.resize(4096)
    assert lease.reserved_bytes == 4096
    assert pool.snapshot().reserved_bytes == 4096
    lease.release()
    assert pool.snapshot().reserved_bytes == 0


def test_staged_path_resizes_and_releases_its_permit(tmp_path: Path, require_native: None) -> None:
    """Exact staged bytes replace the estimate and disappear on cleanup."""
    from schema_sanitizer.core_impl.temporary_storage import TemporaryStoragePermitPool
    from schema_sanitizer.remote_impl.staging import StagedPath

    pool = TemporaryStoragePermitPool(16 << 20)
    lease = pool.acquire(1 << 20, label="staged file", path=tmp_path)
    path = tmp_path / "staged.bin"
    path.write_bytes(b"x" * (2 << 20))
    staged = StagedPath(str(path), storage_lease=lease)

    staged.reserve_actual_size(pool, label="staged file")
    assert pool.snapshot().reserved_bytes == 2 << 20
    staged.close()

    assert not path.exists()
    assert pool.snapshot().reserved_bytes == 0


def test_remote_output_is_accounted_until_upload_finishes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """Remote publication holds its exact local spool permit through upload."""
    from schema_sanitizer.api_impl.operation_context import OperationExecutionContext
    from schema_sanitizer.remote_impl import staging

    observed: list[tuple[int, bytes]] = []
    operation = OperationExecutionContext(
        threading_mode="multi",
        memory_limit_bytes=16 << 20,
    )

    async def fake_upload(
        local_path: str,
        _uri: str,
        *,
        memory_limit_bytes: int | None,
        threading_mode: str,
    ) -> None:
        """Capture the active reservation while upload owns the staged file."""
        assert memory_limit_bytes == 16 << 20
        assert threading_mode == "multi"
        observed.append(
            (
                operation.temporary_storage.snapshot().reserved_bytes,
                Path(local_path).read_bytes(),
            )
        )
        await asyncio.sleep(0)

    monkeypatch.setattr(staging, "upload_file", fake_upload)
    target = staging.prepare_output_target(
        "s3://bucket/result.jsonl",
        memory_limit_bytes=16 << 20,
        threading_mode="multi",
        operation_context=operation,
    )
    payload = b'{"id":1}\n' * 1024
    Path(target.local_path).write_bytes(payload)

    try:
        staging.finalize_output_target(target)
        assert observed == [(len(payload), payload)]
        snapshot = operation.temporary_storage.snapshot()
        assert snapshot.reserved_bytes == 0
        assert snapshot.peak_reserved_bytes == len(payload)
    finally:
        operation.close()


def test_remote_prefetch_waits_for_temporary_storage_release(require_native: None) -> None:
    """Prefetch cannot multiply staged packets beyond the operation spool cap."""
    from schema_sanitizer.api_impl.operation_context import OperationExecutionContext
    from schema_sanitizer.api_impl.source_plan.remote import RemoteChunkPrefetchIterator

    operation = OperationExecutionContext(
        threading_mode="multi",
        memory_limit_bytes=64 << 20,
    )
    starts: list[int] = []

    class Session:
        """Minimal async provider-session context."""

        async def __aenter__(self) -> Session:
            """Enter the asynchronous context managed by the session test double."""
            return self

        async def __aexit__(self, *_exc: object) -> None:
            """Exit the asynchronous context managed by the session test double and run cleanup."""
            pass

    class FakeStaged:
        """Own the packet lease until the source consumer closes it."""

        def __init__(self, lease: object) -> None:
            """Initialize the fake staged test double."""
            self._lease = lease

        def close(self) -> None:
            """Close the resources owned by the fake staged test double."""
            self._lease.release()

    class Manifest:
        """Expose packets that individually consume most of the spool pool."""

        files = (object(), object(), object())
        chunk_size = 1
        threading_mode = "multi"
        memory_limit_bytes = 64 << 20
        operation_context = operation

        @staticmethod
        def open_staging_session() -> Session:
            """Open the controlled staging session for the sink."""
            return Session()

        @staticmethod
        def next_chunk_start(start: int) -> int:
            """Return the next byte offset in the staged manifest."""
            return start + 1

        @staticmethod
        def estimated_chunk_bytes(_start: int) -> int:
            """Return the manifest estimate for one staged chunk."""
            return 160 << 20

        @staticmethod
        def try_acquire_storage_lease(start: int) -> object | None:
            """Attempt to acquire the manifest storage lease."""
            return operation.temporary_storage.try_acquire(
                160 << 20,
                label=f"packet-{start}",
            )

        @staticmethod
        async def stage_chunk_async(
            start: int,
            _session: Session,
            *,
            storage_lease: object,
        ) -> FakeStaged:
            """Stage one chunk through the controlled asynchronous session."""
            starts.append(start)
            await asyncio.sleep(0)
            return FakeStaged(storage_lease)

    iterator = RemoteChunkPrefetchIterator(Manifest())
    try:
        first = next(iterator)
        assert starts == [0]
        assert operation.temporary_storage.snapshot().reserved_bytes == 160 << 20
        first.close()

        second = next(iterator)
        assert starts == [0, 1]
        second.close()
    finally:
        iterator.close()
        operation.close()

    assert operation.temporary_storage.snapshot().reserved_bytes == 0


def test_remote_prefetch_failure_releases_reserved_packet(require_native: None) -> None:
    """A failed async stage returns its pre-acquired storage permit."""
    from schema_sanitizer.api_impl.operation_context import OperationExecutionContext
    from schema_sanitizer.api_impl.source_plan.remote import RemoteChunkPrefetchIterator

    operation = OperationExecutionContext(
        threading_mode="multi",
        memory_limit_bytes=16 << 20,
    )

    class Session:
        """Minimal async provider-session context."""

        async def __aenter__(self) -> Session:
            """Enter the asynchronous context managed by the session test double."""
            return self

        async def __aexit__(self, *_exc: object) -> None:
            """Exit the asynchronous context managed by the session test double and run cleanup."""
            pass

    class Manifest:
        """Fail after the iterator reserves one remote packet."""

        files = (object(),)
        chunk_size = 1
        threading_mode = "multi"
        memory_limit_bytes = 16 << 20
        operation_context = operation

        @staticmethod
        def open_staging_session() -> Session:
            """Open the controlled staging session for the sink."""
            return Session()

        @staticmethod
        def next_chunk_start(start: int) -> int:
            """Return the next byte offset in the staged manifest."""
            return start + 1

        @staticmethod
        def estimated_chunk_bytes(_start: int) -> int:
            """Return the manifest estimate for one staged chunk."""
            return 8 << 20

        @staticmethod
        def try_acquire_storage_lease(start: int) -> object | None:
            """Attempt to acquire the manifest storage lease."""
            return operation.temporary_storage.try_acquire(
                8 << 20,
                label=f"failing-packet-{start}",
            )

        @staticmethod
        async def stage_chunk_async(
            _start: int,
            _session: Session,
            *,
            storage_lease: object,
        ) -> None:
            """Stage one chunk through the controlled asynchronous session."""
            try:
                await asyncio.sleep(0)
                raise RuntimeError("forced remote stage failure")
            except BaseException:
                storage_lease.release()
                raise

    iterator = RemoteChunkPrefetchIterator(Manifest())
    try:
        with pytest.raises(RuntimeError, match="forced remote stage failure"):
            next(iterator)
    finally:
        iterator.close()
        operation.close()

    assert operation.temporary_storage.snapshot().reserved_bytes == 0
    assert operation.temporary_storage.snapshot().active_leases == 0


def test_cancelled_prefetch_future_releases_preacquired_permit(require_native: None) -> None:
    """Cancellation before coroutine start cannot orphan a packet reservation."""
    from concurrent.futures import Future

    from schema_sanitizer.api_impl.operation_context import OperationExecutionContext
    from schema_sanitizer.api_impl.source_plan.remote import RemoteChunkPrefetchIterator

    operation = OperationExecutionContext(
        threading_mode="multi",
        memory_limit_bytes=16 << 20,
    )
    pending: Future[object] = Future()

    class Coordinator:
        """Return one future that has not begun executing."""

        @staticmethod
        def submit(_operation: object, **_permit_options: object) -> Future[object]:
            """Submit work through the coordinator test double."""
            return pending

    class Manifest:
        """Supply only the fields required by the iterator constructor."""

        threading_mode = "multi"
        memory_limit_bytes = 16 << 20
        files = (object(),)
        chunk_size = 1

        @staticmethod
        def estimated_chunk_bytes(_start: int) -> int:
            """Return the manifest estimate for one staged chunk."""
            return 8 << 20

        @staticmethod
        async def stage_chunk_async(*_args: object, **_kwargs: object) -> None:
            """Stage one chunk through the controlled asynchronous session."""
            raise AssertionError("staging coroutine should not start")

    lease = operation.temporary_storage.acquire(8 << 20, label="cancelled packet")
    iterator = RemoteChunkPrefetchIterator(Manifest())
    iterator._coordinator = Coordinator()
    submitted = iterator._submit_stage(0, lease)
    assert operation.temporary_storage.snapshot().reserved_bytes == 8 << 20

    assert submitted.cancel() is True
    assert operation.temporary_storage.snapshot().reserved_bytes == 0
    assert operation.temporary_storage.snapshot().active_leases == 0
    operation.close()


def test_failed_remote_output_releases_spool_permit_and_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    require_native: None,
) -> None:
    """A publication failure removes its spool and returns the exact lease."""
    from schema_sanitizer.api_impl.operation_context import OperationExecutionContext
    from schema_sanitizer.remote_impl import staging

    operation = OperationExecutionContext(
        threading_mode="multi",
        memory_limit_bytes=16 << 20,
    )

    async def fail_upload(*_args: object, **_kwargs: object) -> None:
        """Fail after the completed spool has acquired its exact permit."""
        assert operation.temporary_storage.snapshot().reserved_bytes > 0
        raise RuntimeError("forced publication failure")

    monkeypatch.setattr(staging, "upload_file", fail_upload)
    target = staging.prepare_output_target(
        "s3://bucket/failure.jsonl",
        memory_limit_bytes=16 << 20,
        threading_mode="multi",
        operation_context=operation,
    )
    local_path = Path(target.local_path)
    local_path.write_bytes(b"0123456789")

    try:
        with pytest.raises(RuntimeError, match="forced publication failure"):
            staging.finalize_output_target(target)
        assert not local_path.exists()
        snapshot = operation.temporary_storage.snapshot()
        assert snapshot.reserved_bytes == 0
        assert snapshot.active_leases == 0
    finally:
        operation.close()
