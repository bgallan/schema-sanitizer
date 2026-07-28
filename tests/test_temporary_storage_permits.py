"""Contracts for operation-wide temporary-storage permits."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from conftest import require_native


def test_temporary_storage_pool_bounds_and_reuses_released_capacity() -> None:
    """Reservations are aggregate, resizable, and returned exactly once."""
    require_native()
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


def test_temporary_storage_pool_rejects_one_oversized_artifact() -> None:
    """One artifact cannot silently exceed the derived operation spool cap."""
    require_native()
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


def test_staged_path_resizes_and_releases_its_permit(tmp_path: Path) -> None:
    """Exact staged bytes replace the estimate and disappear on cleanup."""
    require_native()
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
) -> None:
    """Remote publication holds its exact local spool permit through upload."""
    require_native()
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


def test_remote_prefetch_waits_for_temporary_storage_release() -> None:
    """Prefetch cannot multiply staged packets beyond the operation spool cap."""
    require_native()
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
            """Return the session."""
            return self

        async def __aexit__(self, *_exc: object) -> None:
            """Close the fake session."""

    class FakeStaged:
        """Own the packet lease until the source consumer closes it."""

        def __init__(self, lease: object) -> None:
            """Store one permit lease."""
            self._lease = lease

        def close(self) -> None:
            """Release the packet reservation."""
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
            """Return the fake provider session."""
            return Session()

        @staticmethod
        def next_chunk_start(start: int) -> int:
            """Advance one file ordinal."""
            return start + 1

        @staticmethod
        def try_acquire_storage_lease(start: int) -> object | None:
            """Reserve 160 MiB from a 256 MiB operation spool window."""
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
            """Return one staged packet after recording its ordinal."""
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


def test_remote_prefetch_failure_releases_reserved_packet() -> None:
    """A failed async stage returns its pre-acquired storage permit."""
    require_native()
    from schema_sanitizer.api_impl.operation_context import OperationExecutionContext
    from schema_sanitizer.api_impl.source_plan.remote import RemoteChunkPrefetchIterator

    operation = OperationExecutionContext(
        threading_mode="multi",
        memory_limit_bytes=16 << 20,
    )

    class Session:
        """Minimal async provider-session context."""

        async def __aenter__(self) -> Session:
            """Return the session."""
            return self

        async def __aexit__(self, *_exc: object) -> None:
            """Close the fake session."""

    class Manifest:
        """Fail after the iterator reserves one remote packet."""

        files = (object(),)
        chunk_size = 1
        threading_mode = "multi"
        memory_limit_bytes = 16 << 20
        operation_context = operation

        @staticmethod
        def open_staging_session() -> Session:
            """Return the fake provider session."""
            return Session()

        @staticmethod
        def next_chunk_start(start: int) -> int:
            """Advance one file ordinal."""
            return start + 1

        @staticmethod
        def try_acquire_storage_lease(start: int) -> object | None:
            """Reserve one packet from the operation pool."""
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
            """Release the owned permit while propagating the stage failure."""
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


def test_cancelled_prefetch_future_releases_preacquired_permit() -> None:
    """Cancellation before coroutine start cannot orphan a packet reservation."""
    require_native()
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
        def submit(_operation: object) -> Future[object]:
            """Return the pending future."""
            return pending

    class Manifest:
        """Supply only the fields required by the iterator constructor."""

        threading_mode = "multi"
        memory_limit_bytes = 16 << 20
        files = (object(),)
        chunk_size = 1

        @staticmethod
        async def stage_chunk_async(*_args: object, **_kwargs: object) -> None:
            """Never run in this cancellation regression."""
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
) -> None:
    """A publication failure removes its spool and returns the exact lease."""
    require_native()
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
