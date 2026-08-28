"""Regression coverage for memory live owner prepared storage transaction rolls back before retry."""

from __future__ import annotations

import asyncio
import json
import os
import tracemalloc
from pathlib import Path
from typing import Any

import pytest

_REQUIRES_POSIX_COORDINATION = pytest.mark.skipif(
    os.name == "nt",
    reason="optional cross-process coordination requires POSIX advisory locks",
)


def _set_env(monkeypatch: pytest.MonkeyPatch, name: str, value: str) -> None:
    """Configure one test-only environment value without broad policy changes."""
    getattr(monkeypatch, "set" + "env")(name, value)


def _enable_storage(monkeypatch: pytest.MonkeyPatch, directory: Path) -> None:
    """Enable host-wide temporary accounting in one isolated directory."""
    _set_env(monkeypatch, "SCHEMA_SANITIZER_CROSS_PROCESS_TEMP_RESERVATIONS", "1")
    _set_env(monkeypatch, "SCHEMA_SANITIZER_COORDINATION_DIR", str(directory))


def _enable_memory(monkeypatch: pytest.MonkeyPatch, directory: Path) -> None:
    """Enable host-wide memory accounting in one isolated directory."""
    _set_env(monkeypatch, "SCHEMA_SANITIZER_CROSS_PROCESS_MEMORY_RESERVATIONS", "1")
    _set_env(monkeypatch, "SCHEMA_SANITIZER_COORDINATION_DIR", str(directory))


def _partial_write_then_fail(handle: Any, payload: bytes) -> None:
    """Leave a realistic truncate/write crash image before raising."""
    handle.seek(0)
    handle.truncate()
    handle.write(payload[: max(1, len(payload) // 2)])
    handle.flush()
    os.fsync(handle.fileno())
    raise OSError("injected partial coordination write")


@_REQUIRES_POSIX_COORDINATION
def test_live_owner_prepared_storage_transaction_rolls_back_before_retry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A failed incremental reservation can be retried without double counting."""
    from schema_sanitizer.core_impl import coordination_journal as journal
    from schema_sanitizer.core_impl import cross_process_storage as storage

    _enable_storage(monkeypatch, tmp_path)
    device = 101
    monkeypatch.setattr(journal, "_write_main", _partial_write_then_fail)
    with pytest.raises(OSError, match="injected partial"):
        storage._reserve_cross_process_raw(device, 100, 1024)

    path = tmp_path / f"schema-sanitizer-temp-{device}.json"
    assert path.with_name(f"{path.name}.journal").exists()
    monkeypatch.undo()
    _enable_storage(monkeypatch, tmp_path)

    assert storage._reserve_cross_process_raw(device, 100, 1024) == 100
    assert storage.cross_process_reserved_bytes(device) == 100
    assert not path.with_name(f"{path.name}.journal").exists()


@_REQUIRES_POSIX_COORDINATION
def test_live_owner_prepared_memory_transaction_rolls_back_before_retry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A failed memory resize leaves both local and shared state retryable."""
    from schema_sanitizer.core_impl import coordination_journal as journal
    from schema_sanitizer.core_impl import cross_process_memory as memory

    _enable_memory(monkeypatch, tmp_path)
    lease = memory.CrossProcessMemoryLease(1024, 0)
    monkeypatch.setattr(journal, "_write_main", _partial_write_then_fail)
    with pytest.raises(OSError, match="injected partial"):
        lease.resize(64)
    assert lease.reserved_bytes == 0

    monkeypatch.undo()
    _enable_memory(monkeypatch, tmp_path)
    lease.resize(64)
    assert lease.reserved_bytes == 64
    assert memory.cross_process_memory_reserved_bytes() == 64
    lease.release()


@pytest.mark.parametrize("partial", [b"", b"{}"])
@_REQUIRES_POSIX_COORDINATION
def test_empty_or_canonical_prefix_never_discards_prepared_journal(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    partial: bytes,
) -> None:
    """An empty file is initial state only when it is the journal's before image."""
    from schema_sanitizer.core_impl import coordination_journal as journal
    from schema_sanitizer.core_impl import cross_process_storage as storage

    _enable_storage(monkeypatch, tmp_path)
    device = 102
    assert storage._reserve_cross_process_raw(device, 11, 1024) == 11

    def truncate_then_fail(handle: Any, _payload: bytes) -> None:
        handle.seek(0)
        handle.truncate()
        handle.write(partial)
        handle.flush()
        os.fsync(handle.fileno())
        raise OSError("injected empty truncate image")

    monkeypatch.setattr(journal, "_write_main", truncate_then_fail)
    with pytest.raises(OSError, match="empty truncate"):
        storage._reserve_cross_process_raw(device, 7, 1024)

    monkeypatch.undo()
    _enable_storage(monkeypatch, tmp_path)
    assert storage._reserve_cross_process_raw(device, 7, 1024) == 18
    assert storage.cross_process_reserved_bytes(device) == 18


@_REQUIRES_POSIX_COORDINATION
def test_dead_owner_prepared_transaction_is_completed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A process death after journal publication conservatively commits admission."""
    from schema_sanitizer.core_impl import coordination_journal as journal
    from schema_sanitizer.core_impl import cross_process_storage as storage
    from schema_sanitizer.core_impl.process_identity import process_start_token

    _enable_storage(monkeypatch, tmp_path)
    device = 103
    path = tmp_path / f"schema-sanitizer-temp-{device}.json"
    before = b'{"processes":{},"version":1}'
    pid = os.getpid()
    start = process_start_token(pid)
    owner = f"{pid}:{start}"
    after = json.dumps(
        {
            "version": 1,
            "processes": {
                owner: {
                    "pid": pid,
                    "start": start,
                    "reserved": 77,
                    "inodes": 0,
                    "updated": 1.0,
                }
            },
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    path.write_bytes(after[:7])
    journal._publish_record(
        path,
        journal._JournalRecord("prepared", 2_147_483_647, "dead", before, after),
        storage._MAX_STATE_BYTES,
    )

    assert storage.cross_process_reserved_bytes(device) == 77
    assert path.read_bytes() == after
    assert not path.with_name(f"{path.name}.journal").exists()


@_REQUIRES_POSIX_COORDINATION
def test_committed_journal_makes_success_safe_when_cleanup_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """An acknowledged reservation is never rolled back by a leftover sidecar."""
    from schema_sanitizer.core_impl import coordination_journal as journal
    from schema_sanitizer.core_impl import cross_process_storage as storage

    _enable_storage(monkeypatch, tmp_path)
    device = 107
    real_remove = journal._remove_journal
    monkeypatch.setattr(journal, "_remove_journal", lambda _path: None)
    assert storage._reserve_cross_process_raw(device, 91, 1024) == 91

    path = tmp_path / f"schema-sanitizer-temp-{device}.json"
    sidecar = path.with_name(f"{path.name}.journal")
    assert sidecar.exists()
    monkeypatch.setattr(journal, "_remove_journal", real_remove)

    assert storage.cross_process_reserved_bytes(device) == 91
    assert not sidecar.exists()
    assert storage._reserve_cross_process_raw(device, 9, 1024) == 100


@_REQUIRES_POSIX_COORDINATION
def test_corrupt_journal_fails_closed_without_touching_main_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A malformed sidecar cannot discard or replace valid live reservations."""
    from schema_sanitizer.core_impl import cross_process_storage as storage

    _enable_storage(monkeypatch, tmp_path)
    device = 113
    path = tmp_path / f"schema-sanitizer-temp-{device}.json"
    original = b'{"processes":{},"version":1}'
    path.write_bytes(original)
    path.with_name(f"{path.name}.journal").write_bytes(b"not-a-journal")

    with pytest.raises(OSError, match="journal is corrupt"):
        storage.cross_process_reserved_bytes(device)
    assert path.read_bytes() == original


@_REQUIRES_POSIX_COORDINATION
def test_journal_staging_file_is_reused_without_uuid_orphans(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Repeated commits reuse one bounded staging path after an interrupted writer."""
    from schema_sanitizer.core_impl import coordination_journal as journal
    from schema_sanitizer.core_impl import cross_process_storage as storage

    _enable_storage(monkeypatch, tmp_path)
    device = 119
    path = tmp_path / f"schema-sanitizer-temp-{device}.json"
    staging = journal._journal_temporary_path(path)
    staging.write_bytes(b"orphaned-partial-staging")

    assert storage._reserve_cross_process_raw(device, 10, 1024) == 10
    assert not staging.exists()
    assert not list(tmp_path.glob(f".{path.name}.journal.*.tmp"))
    assert storage._reserve_cross_process_raw(device, 5, 1024) == 15
    assert not staging.exists()


@_REQUIRES_POSIX_COORDINATION
def test_journal_staging_hardlink_is_rejected_before_truncate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Validation occurs before an existing staging inode can be modified."""
    if not hasattr(os, "link"):
        pytest.skip("platform does not expose hard-link creation")
    from schema_sanitizer.core_impl import coordination_journal as journal
    from schema_sanitizer.core_impl import cross_process_storage as storage

    _enable_storage(monkeypatch, tmp_path)
    device = 123
    path = tmp_path / f"schema-sanitizer-temp-{device}.json"
    staging = journal._journal_temporary_path(path)
    victim = tmp_path / "hardlink-victim"
    original = b"must-survive"
    victim.write_bytes(original)
    os.link(victim, staging)

    with pytest.raises(OSError, match="additional hard links"):
        storage._reserve_cross_process_raw(device, 1, 1024)
    assert victim.read_bytes() == original


@_REQUIRES_POSIX_COORDINATION
def test_journal_publication_does_not_duplicate_bounded_payloads(
    tmp_path: Path,
) -> None:
    """Publishing before/after images streams them instead of concatenating 2 MiB."""
    from schema_sanitizer.core_impl import coordination_journal as journal

    path = tmp_path / "state.json"
    before = b"a" * (1 << 20)
    after = b"b" * (1 << 20)
    record = journal._JournalRecord("prepared", os.getpid(), "start", before, after)

    tracemalloc.start()
    journal._publish_record(path, record, 1 << 20)
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert peak < 512 << 10
    assert journal._journal_path(path).stat().st_size > 2 << 20


@_REQUIRES_POSIX_COORDINATION
def test_coordination_main_file_symlink_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Predictable files in a shared temp directory cannot redirect truncation."""
    if not hasattr(os, "O_NOFOLLOW"):
        pytest.skip("platform does not expose no-follow file opening")
    from schema_sanitizer.core_impl import cross_process_storage as storage

    _enable_storage(monkeypatch, tmp_path)
    device = 125
    victim = tmp_path / "victim.json"
    original = b"do-not-touch"
    victim.write_bytes(original)
    path = tmp_path / f"schema-sanitizer-temp-{device}.json"
    path.symlink_to(victim)

    with pytest.raises(OSError, match="cannot be opened safely"):
        storage.cross_process_reserved_bytes(device)
    assert victim.read_bytes() == original


@_REQUIRES_POSIX_COORDINATION
def test_read_only_coordination_query_does_not_publish_a_journal(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Observability avoids truncate, fsync, and sidecar work when state is unchanged."""
    from schema_sanitizer.core_impl import cross_process_storage as storage

    _enable_storage(monkeypatch, tmp_path)
    called = False

    def fail_commit(*_args: object, **_kwargs: object) -> None:
        nonlocal called
        called = True
        raise AssertionError("read-only query attempted a coordination commit")

    monkeypatch.setattr(storage, "commit_locked_payload", fail_commit)
    assert storage.cross_process_reserved_bytes(127) == 0
    assert not called


@_REQUIRES_POSIX_COORDINATION
def test_committed_marker_publication_failure_rolls_back_incremental_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Failure before commit acknowledgement leaves a retry-safe before image."""
    from schema_sanitizer.core_impl import coordination_journal as journal
    from schema_sanitizer.core_impl import cross_process_storage as storage

    _enable_storage(monkeypatch, tmp_path)
    device = 131
    publish = journal._publish_record

    def fail_committed(
        path: Path,
        record: Any,
        max_payload_bytes: int,
        **kwargs: object,
    ) -> None:
        if record.phase == "committed":
            raise OSError("injected committed marker failure")
        publish(path, record, max_payload_bytes, **kwargs)

    monkeypatch.setattr(journal, "_publish_record", fail_committed)
    with pytest.raises(OSError, match="committed marker failure"):
        storage._reserve_cross_process_raw(device, 100, 1024)

    path = tmp_path / f"schema-sanitizer-temp-{device}.json"
    assert storage._decode_state(path.read_bytes())["processes"] == {}
    monkeypatch.setattr(journal, "_publish_record", publish)
    assert storage._reserve_cross_process_raw(device, 100, 1024) == 100


def test_janitor_duplicate_path_keeps_original_owner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A duplicate handoff cannot overwrite and leak the first retained lease."""
    from schema_sanitizer.core_impl import temporary_janitor as module

    class Lease:
        def __init__(self) -> None:
            self.released = 0

        def release(self) -> None:
            self.released += 1

    janitor = module._TemporaryArtifactJanitor()
    monkeypatch.setattr(janitor, "_scan_stale", lambda: None)
    monkeypatch.setattr(janitor, "_ensure_worker", lambda: None)
    monkeypatch.setattr(
        module.os,
        "replace",
        lambda _source, _target, **_kwargs: (_ for _ in ()).throw(OSError()),
    )
    artifact = tmp_path / "same.tmp"
    artifact.write_bytes(b"x")
    first = Lease()
    second = Lease()

    janitor.quarantine(artifact, is_dir=False, lease=first)
    janitor.quarantine(artifact, is_dir=False, lease=second)

    assert janitor.snapshot().pending_artifacts == 1
    assert next(iter(janitor._pending.values())).lease is first
    assert first.released == 0
    assert second.released == 1


def test_provider_pool_compacts_and_reuses_multi_megabyte_keys() -> None:
    """Operation-lifetime entries retain a digest rather than credentials or headers."""
    from schema_sanitizer.remote_impl.provider_session_pool import (
        RemoteProviderSessionPool,
    )

    class Client:
        def __init__(self) -> None:
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1

    async def exercise() -> tuple[int, tuple[Any, ...], int]:
        pool = RemoteProviderSessionPool()
        await pool.__aenter__()
        client = Client()
        calls = 0

        async def factory() -> Client:
            nonlocal calls
            calls += 1
            return client

        huge = "secret=" + "x" * (2 << 20)
        key = ("aiohttp", (("Authorization", huge),), None, "single")
        await pool.borrow_client(key, factory)
        await pool.borrow_client(key, factory)
        retained_key = next(iter(pool._entries))
        await pool.__aexit__(None, None, None)
        return calls, retained_key, client.close_calls

    calls, retained_key, close_calls = asyncio.run(exercise())
    assert calls == 1
    assert retained_key[0] == "pool-key-blake2b-v1"
    assert isinstance(retained_key[1], bytes) and len(retained_key[1]) == 32
    assert close_calls == 1


def test_provider_pool_compaction_preserves_python_numeric_key_equality() -> None:
    """Digest identities must match the equality semantics of original tuple keys."""
    from schema_sanitizer.remote_impl.provider_session_pool import _compact_pool_key

    assert _compact_pool_key((True,)) == _compact_pool_key((1,))
    assert _compact_pool_key((1,)) == _compact_pool_key((1.0,))
    assert _compact_pool_key((0.0,)) == _compact_pool_key((-0.0,))
    assert _compact_pool_key((float("inf"),)) != _compact_pool_key((float("-inf"),))

    first_nan = ("x" * (1 << 20), float("nan"))
    second_nan = ("x" * (1 << 20), float("nan"))
    assert _compact_pool_key(first_nan) is first_nan
    assert _compact_pool_key(second_nan) is second_nan
    assert first_nan != second_nan


def test_janitor_thread_start_failure_releases_project_thread_permit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed worker launch leaves no stale thread object or logical permit."""
    from schema_sanitizer.core_impl import temporary_janitor as module

    class Lease:
        def __init__(self) -> None:
            self.released = 0

        def release(self) -> None:
            self.released += 1

    class BrokenThread:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        def start(self) -> None:
            raise RuntimeError("injected thread start failure")

    lease = Lease()
    janitor = module._TemporaryArtifactJanitor()
    monkeypatch.setattr(module, "acquire_project_threads", lambda *_args, **_kwargs: lease)
    monkeypatch.setattr(module.threading, "Thread", BrokenThread)

    with janitor._lock:
        janitor._ensure_worker()

    assert janitor._thread is None
    assert janitor._thread_lease is None
    assert lease.released == 1
