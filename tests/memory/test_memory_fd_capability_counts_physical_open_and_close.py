"""Traces descriptor credit through open and close, scandir duplication, nested path
identity, remote callback failures, Windows mappings, DuckDB streaming, directory
indexing, janitor scans, and fragmented HTTP reads. Physical and reserved states remain
distinct, every adopted descriptor is precharged, and bounded readers avoid full-copy or
list-clone barriers."""

from __future__ import annotations

import asyncio
import io
import os
from pathlib import Path

import pytest
from _support.source_contracts import package_source_text

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src/schema_sanitizer"
CPP = ROOT / "cpp/src"


def test_fd_capability_counts_physical_open_and_close(tmp_path: Path) -> None:
    """Verify FD capability counts physical open and close."""
    from schema_sanitizer.core_impl import process_resources as module

    target = tmp_path / "payload.bin"
    target.write_bytes(b"x")
    before = module._python_governed_fds_opened()
    with module.acquire_file_descriptor_capability(
        1, label="fd-capability-counts-physical-open-and-test"
    ) as capability:
        with capability.open_descriptor(
            lambda: os.open(target, os.O_RDONLY),
            label="fd-capability-counts-physical-open-and-test-open",
        ) as descriptor:
            assert descriptor >= 0
            assert capability.opened == 1
            assert module._python_governed_fds_opened() == before + 1
        assert capability.opened == 0
        assert module._python_governed_fds_opened() == before
    assert module._python_governed_fds_opened() == before


@pytest.mark.skipif(os.name == "nt", reason="descriptor-relative scandir is POSIX-only")
def test_fd_capability_accounts_scandir_descriptor_duplication(tmp_path: Path) -> None:
    """Verify FD capability accounts scandir descriptor duplication."""
    from schema_sanitizer.core_impl import process_resources as module

    (tmp_path / "entry.txt").write_text("x", encoding="utf-8")
    flags = os.O_RDONLY | int(getattr(os, "O_DIRECTORY", 0))
    before = module._python_governed_fds_opened()
    with module.acquire_file_descriptor_capability(
        2, label="fd-capability-counts-physical-open-and-scandir"
    ) as capability:
        with capability.open_descriptor(
            lambda: os.open(tmp_path, flags),
            label="fd-capability-counts-physical-open-and-directory",
        ) as directory_fd:
            assert module._python_governed_fds_opened() == before + 1
            with capability.scandir(
                directory_fd, label="fd-capability-counts-physical-open-and-scandir"
            ) as entries:
                assert {entry.name for entry in entries} == {"entry.txt"}
                assert capability.opened == 2
                assert module._python_governed_fds_opened() == before + 2
            assert capability.opened == 1
            assert module._python_governed_fds_opened() == before + 1
    assert module._python_governed_fds_opened() == before


def test_path_identity_nested_claim_reads_use_preacquired_capability() -> None:
    """Verify path identity nested claim reads use preacquired capability."""
    source = package_source_text("core_impl/path_identity.py")
    assert "acquire_file_descriptor_capability(" in source
    assert "temporary_claim_remove" in source and "temporary_claim_recovery" in source
    assert "capability=capability" in source
    read_claim = source[
        source.index("def _read_claim_at(") : source.index("def _validate_open_directory")
    ]
    assert "local_capability.open_descriptor" in read_claim
    assert "_IdentityDescriptorOwner(descriptor, lease)" not in read_claim


def test_remote_async_publication_delivery_baseexception_reclaims_waiter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify remote async publication delivery baseexception reclaims waiter."""
    from schema_sanitizer.remote_impl.io_permits import RemoteIoPermitGovernor

    async def run() -> None:
        """Inject a delivery interruption and verify waiter and permit reclamation."""
        governor = RemoteIoPermitGovernor(1, max_waiters=8)
        holder = await governor.acquire(label="holder")
        original_deliver = governor._deliver
        armed = True

        def fail_once(deliveries: object) -> None:
            """Inject the once failure at the controlled test point."""
            nonlocal armed
            if armed:
                armed = False
                raise KeyboardInterrupt("injected publication/delivery interruption")
            original_deliver(deliveries)

        monkeypatch.setattr(governor, "_deliver", fail_once)
        with pytest.raises(KeyboardInterrupt, match="publication/delivery"):
            await governor.acquire(label="interrupted")
        snapshot = governor.snapshot()
        assert snapshot.waiting == 0
        assert snapshot.in_use == 1
        assert snapshot.active_permits == 1
        holder.release()
        snapshot = governor.snapshot()
        assert snapshot.waiting == 0
        assert snapshot.in_use == 0
        assert snapshot.active_permits == 0

    asyncio.run(run())


def test_native_fd_lease_has_explicit_physical_close_commit_and_debt() -> None:
    """Verify native FD lease has explicit physical close commit and debt."""
    header = (CPP / "internal/runtime/process_fd_governor.hh").read_text(encoding="utf-8")
    mapped = (CPP / "ingest/chunk_source_file.cc").read_text(encoding="utf-8")
    secure_file = (CPP / "ingest/secure_read_only_file.cc").read_text(encoding="utf-8")
    assert "retain_uncertain_close()" in header
    assert "commit_physical_close(bool proven_closed" in header
    assert "close_stream_and_commit" in header
    assert "ProcessFdStreamCloseGuard" in header
    assert "SecureReadOnlyFile" in mapped
    assert "fd_lease_.commit_physical_close(closed)" in secure_file


def test_windows_read_only_mapping_allows_staged_path_rename_without_write_sharing() -> None:
    """Mapped staged inputs permit cleanup rename without granting write sharing."""
    secure_file = (CPP / "ingest/secure_read_only_file.cc").read_text(encoding="utf-8")
    create_file = secure_file[
        secure_file.index("CreateFileW(native_path.c_str()") : secure_file.index(
            "if (handle == INVALID_HANDLE_VALUE)"
        )
    ]
    compact_create_file = " ".join(create_file.split())
    assert "GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_DELETE" in compact_create_file
    assert "FILE_SHARE_WRITE" not in create_file
    assert "OPEN_EXISTING" in create_file


def test_duckdb_stream_handoff_has_no_full_batch_list_barrier() -> None:
    """Verify duckdb stream handoff has no full batch list barrier."""
    results = package_source_text("api_impl/results.py")
    coverage = package_source_text("core_impl/concurrency_coverage.py")
    duckdb = results[
        results.index('if target == "duckdb":') : results.index(
            "raise AssertionError", results.index('if target == "duckdb":')
        )
    ]
    assert "_duckdb_from_arrow_serial(duckdb, reader)" in duckdb
    assert "list(reader)" not in results
    assert "reader_transferred = True" in duckdb
    assert "record_batch_reader_direct_duckdb_handoff" in coverage


def test_remote_success_reads_are_bounded_even_without_operation_ledger() -> None:
    """Verify remote success reads are bounded even without operation ledger."""
    source = package_source_text("remote_impl/transport.py")
    success = source[
        source.index("async def read_response_bytes") : source.index(
            "async def", source.index("async def read_response_bytes") + 10
        )
    ]
    assert "MAX_CONTROL_RESPONSE_BYTES" in success
    assert "await response.read()" not in success


def test_remote_directory_index_overhead_is_precharged_and_lists_are_not_recloned() -> None:
    """Verify remote directory index overhead is precharged and lists are not recloned."""
    staging = package_source_text("remote_impl/staging.py")
    preparation = package_source_text("api_impl/input/preparation.py")
    assert "selected = list(files)" not in staging
    assert "list(discovered.remote_files)" not in preparation
    for provider in ("azure.py", "azure_sync.py", "gcs.py", "gcs_sync.py", "s3.py", "s3_sync.py"):
        source = package_source_text(f"remote_impl/providers/{provider}")
        assert "charge_file(remote_file, associations=4)" in source


def test_temporary_janitor_uses_atomic_root_bundle_and_governed_scandir() -> None:
    """Verify temporary janitor uses atomic root bundle and governed scandir."""
    source = package_source_text("core_impl/temporary_janitor.py")
    root = source[source.index("def _root_handle") : source.index("def _close_root_handle")]
    scan = source[source.index("def _iter_directory") : source.index("def _stale_scan_candidates")]
    assert "acquire_teardown_file_descriptors(2, timeout_seconds=0)" in root
    assert "descriptor_lease.shrink(1)" in root
    assert "record_physical_file_descriptors_opened(1)" in root
    assert "acquire_teardown_file_descriptors(1, timeout_seconds=0)" not in root
    assert "acquire_file_descriptor_capability(" in scan
    assert "capability.scandir(" in scan
    assert "capability.scandir_path(" in scan


def test_staged_tree_pending_directory_metadata_is_hard_bounded() -> None:
    """Verify staged tree pending directory metadata is hard bounded."""
    source = package_source_text("remote_impl/staging_paths.py")
    assert "_MAX_PENDING_TREE_DIRECTORIES = 4096" in source
    assert source.count("len(pending) >= _MAX_PENDING_TREE_DIRECTORIES") == 2
    assert "pending-directory metadata limit" in source


def test_remote_local_file_adopts_preacquired_descriptor_credit(tmp_path: Path) -> None:
    """Verify remote local file adopts preacquired descriptor credit."""
    from schema_sanitizer.core_impl import process_resources as resources
    from schema_sanitizer.remote_impl.io_footprint import (
        ActiveRemoteIoFootprint,
        RemoteIoFootprint,
        activate_remote_io_footprint,
        open_remote_local_file,
    )

    path = tmp_path / "remote-spool.bin"
    path.write_bytes(b"payload")
    lease = resources.acquire_file_descriptors(1)
    owner = ActiveRemoteIoFootprint(RemoteIoFootprint(network_fds=0, local_file_fds=1), lease)
    before = resources._python_governed_fds_opened()
    try:
        with activate_remote_io_footprint(owner):
            with open_remote_local_file(
                path, "rb", label="fd-capability-counts-physical-open-and-remote-local"
            ) as handle:
                assert isinstance(handle, io.IOBase)
                assert handle.tell() == 0
                assert handle.read() == b"payload"
                assert handle.seek(0) == 0
                assert resources._python_governed_fds_opened() == before + 1
                with pytest.raises(RuntimeError, match="local files remain open"):
                    owner.release_descriptor_lease()
        assert resources._python_governed_fds_opened() == before
        owner.release_descriptor_lease()
    finally:
        # Idempotent after the normal release; defensive if an assertion fails.
        try:
            owner.release_descriptor_lease()
        except RuntimeError:
            pass


def test_bounded_http_reader_handles_fragmented_aiohttp_without_third_full_copy() -> None:
    """Verify bounded HTTP reader handles fragmented aiohttp without third full copy."""
    source = package_source_text("remote_impl/transport.py")
    bounded = source[
        source.index("async def read_bounded_response_bytes") : source.index(
            "async def read_bounded_response_text"
        )
    ]
    assert "if content.at_eof():" in bounded
    assert "while len(payload_buffer) <= limit" in bounded
    assert "remaining = limit + 1 - len(payload_buffer)" in bounded
    assert "retained = _BudgetedBytes(payload_buffer, lease)" in bounded
    assert "payload = bytes(payload_buffer)" not in bounded
