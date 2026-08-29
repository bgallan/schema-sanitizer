"""Validates one atomic cross-language operation-memory ledger for Python reservations,
directory metadata, native reader work, high-level text streams, remote control bodies,
and transfer chunks. Charges precede blocking reads and remain held until the owning
stream or body closes, while peak usage survives ledger closure."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier, Event, Lock

import pytest
from _support.synchronization import SCHEDULER_TIMEOUT_SECONDS, wait_event_or_fail

from schema_sanitizer.api_impl.operation_context import OperationExecutionContext
from schema_sanitizer.core_impl.execution import ExecutionContext as NativeExecutionContext
from schema_sanitizer.core_impl.memory_budget import OperationMemoryLedger
from schema_sanitizer.errors import SchemaSanitizerResourceError
from schema_sanitizer.input_impl.directory_inputs import DirectoryMetadataBudget
from schema_sanitizer.options_impl.call_options import (
    attach_operation_detected_at,
    normalize_call_options,
    unwrap_options,
)
from schema_sanitizer.sources import RemoteFile


def test_python_ledger_reservations_are_atomic_and_preserve_peak_after_close() -> None:
    """Concurrent Python resources cannot reserve beyond one exact ceiling."""
    limit = 512 * 1024
    unit = 64 * 1024
    ledger = OperationMemoryLedger(limit)
    barrier = Barrier(16)
    release = Event()
    all_attempted = Event()
    leases = []
    attempted = 0
    lock = Lock()

    def reserve_one() -> bool:
        """Attempt one synchronized reservation and release it after observation."""
        nonlocal attempted
        barrier.wait()
        lease = None
        try:
            lease = ledger.acquire(unit, stage="concurrent_python_metadata")
        except SchemaSanitizerResourceError:
            pass
        with lock:
            if lease is not None:
                leases.append(lease)
            attempted += 1
            if attempted == 16:
                all_attempted.set()
        if lease is None:
            return False
        wait_event_or_fail(release)
        lease.close()
        return True

    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = [pool.submit(reserve_one) for _ in range(16)]
        assert all_attempted.wait(timeout=SCHEDULER_TIMEOUT_SECONDS)
        snapshot = ledger.snapshot()
        assert snapshot.reserved_bytes == limit
        assert snapshot.peak_reserved_bytes == limit
        release.set()
        results = [future.result(timeout=SCHEDULER_TIMEOUT_SECONDS) for future in futures]

    assert sum(results) == limit // unit
    assert ledger.snapshot().reserved_bytes == 0
    ledger.close()
    assert ledger.snapshot().reserved_bytes == 0
    assert ledger.snapshot().peak_reserved_bytes == limit


def test_directory_metadata_and_native_reader_share_one_atomic_limit() -> None:
    """Python metadata retained before parsing reduces native allocator headroom."""
    limit = 1 << 20
    operation = OperationExecutionContext(
        threading_mode="single",
        memory_limit_bytes=limit,
    )
    metadata = DirectoryMetadataBudget(
        limit,
        operation_memory_ledger=operation.memory_ledger,
    )
    metadata.charge_file(RemoteFile("s3://bucket/" + "x" * 50_000 + ".csv", "x" * 50_000, 1))
    retained = operation.memory_ledger.snapshot().reserved_bytes
    assert retained > 0

    options = attach_operation_detected_at(
        normalize_call_options(memory_limit_bytes=limit),
        operation.detected_at,
        operation.memory_ledger,
    )
    remaining = limit - retained
    lease = operation.memory_ledger.acquire(
        max(0, remaining - 8 * 1024),
        stage="python_staging_window",
    )
    native = NativeExecutionContext()
    payload = "a,b\n" + "1,2\n" * 1000
    try:
        with pytest.raises(RuntimeError, match="out of memory"):
            native.to_sink_from_source(
                "stream",
                "csv",
                "text",
                payload,
                unwrap_options(options),
            )
        snapshot = operation.memory_ledger.snapshot()
        assert snapshot.reserved_bytes <= limit
        assert snapshot.peak_reserved_bytes <= limit
    finally:
        lease.close()
        metadata.close()
        operation.close()

    final = operation.memory_ledger.snapshot()
    assert final.reserved_bytes == 0
    assert final.peak_reserved_bytes <= limit


def test_high_level_text_input_keeps_its_reservation_until_stream_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Materialized text and native stream allocations share one output lifetime."""
    from schema_sanitizer.api_impl import execution_context as module

    created: list[OperationExecutionContext] = []
    real_factory = module.OperationExecutionContext

    def create_context(**kwargs):
        """Capture the operation context created by the public execution path."""
        context = real_factory(**kwargs)
        created.append(context)
        return context

    monkeypatch.setattr(module, "OperationExecutionContext", create_context)
    payload = "a,b\n" + "1,2\n" * 2000
    output = module.ExecutionContext().to_sink(
        payload,
        sink="stream",
        options=normalize_call_options(memory_limit_bytes=2 << 20),
        format="csv",
        source="text",
    )
    assert len(created) == 1
    operation = created[0]
    during = operation.memory_ledger.snapshot()
    assert during.reserved_bytes >= len(payload.encode("utf-8"))
    assert during.peak_reserved_bytes <= during.limit_bytes

    output.close()
    after = operation.memory_ledger.snapshot()
    assert after.reserved_bytes == 0
    assert after.peak_reserved_bytes >= len(payload.encode("utf-8"))


def test_remote_control_body_retains_shared_ledger_charge_until_released(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A returned HTTP control body remains charged after provider work returns."""
    from schema_sanitizer.remote_impl import sync_http

    class Response:
        """Provide the response surface required by the current regression."""

        status = 200

        def read(self, size: int = -1) -> bytes:
            """Read bounded data from the response test double."""
            return b'{"value":1}' if size < 0 else b'{"value":1}'[:size]

        def getheaders(self):
            """Return the controlled HTTP response headers."""
            return []

    class Connection:
        """Expose the closeable connection paired with the bounded response."""

        def close(self) -> None:
            """Close the resources owned by the connection test double."""
            pass

    monkeypatch.setattr(
        sync_http,
        "_request_once",
        lambda *_args, **_kwargs: (Connection(), Response()),
    )
    operation = OperationExecutionContext(
        threading_mode="single",
        memory_limit_bytes=1 << 20,
    )
    result = operation.run_remote_sync(
        lambda: sync_http.request_bytes(
            "GET",
            "https://example.invalid/control",
            timeout=1.0,
            max_response_bytes=4096,
        )
    )
    try:
        snapshot = operation.memory_ledger.snapshot()
        assert snapshot.reserved_bytes == 4097
        assert bytes(result.body) == b'{"value":1}'
    finally:
        close = getattr(result.body, "close", None)
        if callable(close):
            close()
        operation.close()

    assert operation.memory_ledger.snapshot().reserved_bytes == 0


def test_remote_transfer_reserves_chunk_before_blocking_read(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Synchronous staging reserves its chunk window before allocating bytes."""
    from schema_sanitizer.remote_impl import sync_http
    from schema_sanitizer.remote_impl.io_footprint import RemoteIoFootprint

    operation = OperationExecutionContext(
        threading_mode="single",
        memory_limit_bytes=2 << 20,
    )
    observed: list[int] = []

    class Response:
        """Provide the response surface required by the current regression."""

        status = 200
        calls = 0

        def getheader(self, _name: str):
            """Return the requested controlled HTTP response header."""
            return None

        def read(self, _size: int = -1) -> bytes:
            """Read bounded data from the response test double."""
            observed.append(operation.memory_ledger.snapshot().reserved_bytes)
            self.calls += 1
            return b"data" if self.calls == 1 else b""

    class Connection:
        """Expose the closeable connection paired with the staged response."""

        def close(self) -> None:
            """Close the resources owned by the connection test double."""
            pass

    monkeypatch.setattr(
        sync_http,
        "_request_once",
        lambda *_args, **_kwargs: (Connection(), Response()),
    )
    operation.run_remote_sync(
        lambda: sync_http.download_to_file(
            "https://example.invalid/data",
            str(tmp_path / "data.bin"),
            headers=None,
            timeout=1.0,
        ),
        footprint=RemoteIoFootprint(network_fds=0, local_file_fds=1),
    )
    operation.close()

    assert observed
    assert min(observed) >= sync_http.TRANSFER_CHUNK_BYTES
    assert operation.memory_ledger.snapshot().reserved_bytes == 0
