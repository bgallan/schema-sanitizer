from __future__ import annotations

from pathlib import Path
from threading import Lock

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src/schema_sanitizer"
CPP = ROOT / "cpp/src"


def _source(relative: str) -> str:
    return (SRC / relative).read_text(encoding="utf-8")


def test_governed_file_publication_failure_closes_owner_before_credit_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import process_resources as module

    events: list[str] = []

    class Lease:
        amount = 1

        def release(self) -> None:
            events.append("lease.release")

    class Stream:
        closed = False

        def close(self) -> None:
            events.append("stream.close")
            self.closed = True

    stream = Stream()
    before = module._python_governed_fds_opened()
    monkeypatch.setattr(module, "acquire_file_descriptors", lambda _amount: Lease())
    monkeypatch.setattr("builtins.open", lambda *_args, **_kwargs: stream)

    class PublicationFailure:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            raise MemoryError("injected wrapper publication failure")

    monkeypatch.setattr(module, "GovernedFile", PublicationFailure)
    with pytest.raises(MemoryError, match="publication failure"):
        module.open_governed_file("ignored", "rb")

    assert stream.closed
    assert events == ["stream.close", "lease.release"]
    assert module._python_governed_fds_opened() == before


def test_path_identity_prearms_cleanup_before_fd_admission_or_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from schema_sanitizer.core_impl import path_identity as module

    target = tmp_path / "identity.bin"
    target.write_bytes(b"x")
    lease_called = False
    open_called = False

    def fail_prepare() -> tuple[int, object]:
        raise MemoryError("injected finalizer escrow failure")

    def acquire(_amount: int) -> object:
        nonlocal lease_called
        lease_called = True
        raise AssertionError("FD admission must happen after owner prearm")

    real_open = module.os.open

    def tracked_open(*args: object, **kwargs: object) -> int:
        nonlocal open_called
        open_called = True
        return real_open(*args, **kwargs)

    monkeypatch.setattr(module, "prepare_owner_finalizer_cleanup", fail_prepare)
    monkeypatch.setattr(module, "acquire_file_descriptors", acquire)
    monkeypatch.setattr(module.os, "open", tracked_open)

    with pytest.raises(MemoryError, match="finalizer escrow"):
        module._open_identity_fd(target)
    assert not lease_called
    assert not open_called


def test_terminal_fd_debt_poison_prevents_release_and_reuse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import process_resources as module

    class Lease:
        amount = 1

        def release(self) -> None:
            raise AssertionError("terminal debt must never release its logical credit")

    capability = module.FileDescriptorCapability(Lease(), 1, label="pass63-debt")
    monkeypatch.setattr(module, "record_physical_file_descriptors_opened", lambda _n=1: None)
    monkeypatch.setattr(module, "retain_uncertain_fd_close", lambda *_a, **_k: True)
    capability._mark_opened()
    capability._retain_uncertain(label="pass63-debt")

    assert capability.retained_as_debt
    with pytest.raises(RuntimeError, match="terminal FD debt"):
        capability.release()
    with pytest.raises(RuntimeError, match="terminally poisoned"):
        capability._mark_opened()


def test_external_runtime_parallelism_requires_exact_thread_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import process_resources as module

    class Lease:
        amount = 4
        released = False

        def release(self) -> None:
            self.released = True

    lease = Lease()
    calls: list[tuple[int, int]] = []

    def acquire(desired: int, *, minimum: int = 1) -> Lease:
        calls.append((desired, minimum))
        return lease

    monkeypatch.setattr(module, "acquire_project_threads", acquire)
    runtime = module.acquire_external_runtime_threads(4, allow_parallel=True)
    assert calls == [(4, 4)]
    assert runtime.workers == 4
    assert runtime.parallel
    runtime.close()
    assert lease.released


def test_parquet_external_path_and_thread_routes_are_governed() -> None:
    factory = _source("adapters/parquet/record_batch_factory.py")
    results = _source("api_impl/results.py")
    duckdb_relation = _source("api_impl/duckdb_relation.py")
    sink = _source("adapters/parquet/sink.py")
    diagnostics = _source("api_impl/output_diagnostics.py")
    schemas = _source("pipeline/schemas.py")

    assert 'open_governed_file(local_path, "rb")' in factory
    assert "acquire_external_file_capability(" in factory
    assert "use_threads=runtime_lease.parallel" in factory
    assert "acquire_external_runtime_threads(" in factory
    assert "NamedTemporaryFile" not in factory
    assert 'open_governed_file(target, "wb")' in sink
    assert "pq.ParquetFile(handle)" in diagnostics
    assert results.count("_configurable_external_threads(threading_mode, pa)") >= 2
    assert "_duckdb_from_arrow_serial(duckdb, reader)" in results
    assert "from .duckdb_relation import" in results
    assert 'config={"threads": 1}' in duckdb_relation
    assert schemas.count("pq.read_schema(handle)") >= 3


def test_replay_spool_reserves_inode_bytes_and_fds_before_writes() -> None:
    replay = _source("api_impl/parquet/replay_stream.py")
    assert "TemporaryStoragePermitPool(memory_limit_bytes)" in replay
    assert "artifact_count=1" in replay
    assert "acquire_file_descriptor_capability(" in replay
    assert 'label="parquet_replay_mkstemp"' in replay
    assert 'open_governed_file(path, "wb")' in replay
    assert "StreamingStorageReservation(" in replay
    assert "self._reservation.before_write(amount)" in replay
    assert "reservation.finalize(os.path.getsize(path))" in replay
    assert 'open_governed_file(self._path, "rb")' in replay
    assert "memory_map(" not in replay
    assert "OSFile(" not in replay


def test_native_fd_reset_is_fail_closed_and_transcoding_eof_commits_close() -> None:
    header = (CPP / "internal/runtime/process_fd_governor.hh").read_text(encoding="utf-8")
    transcoding = (CPP / "ingest/transcoding/chunk_source.cc").read_text(encoding="utf-8")
    reset = header[
        header.index("void reset() noexcept") : header.index(
            "private:", header.index("void reset() noexcept")
        )
    ]
    assert "if (opened_ != 0U)" in reset
    assert "retain_uncertain_close();" in reset
    assert reset.index("retain_uncertain_close();") < reset.index(
        "release_process_file_descriptor_permits"
    )
    assert "close_stream_and_commit(input_, fd_lease_)" in transcoding
    assert "input_.close();\n      fd_lease_.reset();" not in transcoding


def test_remote_and_local_grouping_graphs_share_directory_metadata_budget() -> None:
    budget = _source("input_impl/directory_metadata_budget.py")
    assert "_DIRECTORY_METADATA_GROUP_ASSOCIATION_BYTES" in budget
    assert "def charge_group_associations" in budget
    for provider in ("azure.py", "azure_sync.py", "gcs.py", "gcs_sync.py", "s3.py", "s3_sync.py"):
        source = _source(f"remote_impl/providers/{provider}")
        assert "metadata_budget=metadata_budget" in source
        assert "metadata_budget.charge_group_associations()" in source
        assert source.index("metadata_budget.charge_group_associations()") < source.index(
            "groups.setdefault", source.index("metadata_budget.charge_group_associations()")
        )
    local = _source("pipeline/source_discovery.py")
    assert "metadata_budget.charge_group_associations()" in local


def test_directory_group_charge_is_admitted_before_publish() -> None:
    from schema_sanitizer.input_impl.directory_metadata_budget import (
        DirectoryMetadataBudget,
        RetainedDirectoryMetadata,
    )

    budget = object.__new__(DirectoryMetadataBudget)
    budget.limit_bytes = 1024 * 1024
    budget._operation_memory_ledger = None
    budget._retention_owner = RetainedDirectoryMetadata()
    budget._used_bytes = 0
    budget._lock = Lock()
    budget._close_started = False
    before = budget.used_bytes
    budget.charge_group_associations(3)
    assert budget.used_bytes > before


def test_pyarrow_external_runtime_lease_is_reclaimed_on_baseexception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.adapters.parquet import record_batch_factory as module

    class RuntimeLease:
        parallel = True
        closed = False

        def close(self) -> None:
            self.closed = True

    class Dataset:
        def scanner(self, **_kwargs: object) -> object:
            raise KeyboardInterrupt("injected external runtime cancellation")

    class Factory:
        _filters = None
        _dataset = Dataset()
        _columns = None
        _batch_size = 128
        _dataset_error = None
        _pending_parquet_file = None
        _pending_opened_file = None

    class Logger:
        def debug(self, *_args: object, **_kwargs: object) -> None:
            pass

    runtime = RuntimeLease()
    monkeypatch.setattr(module, "_external_runtime_threads", lambda _factory: runtime)
    monkeypatch.setattr(module, "record_parquet_fallback_attempt", lambda *_a, **_k: None)
    monkeypatch.setattr(module, "record_parquet_fallback_failure", lambda *_a, **_k: None)

    with pytest.raises(KeyboardInterrupt, match="external runtime cancellation"):
        module.pyarrow_fallback_arrow_stream(
            Factory(),
            record_batch_reader_from_iterable=lambda *_a, **_k: None,
            logger=Logger(),
        )
    assert runtime.closed
