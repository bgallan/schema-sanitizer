"""Tests cancellation of an already claimed retry alongside accepted replacement
generations, guardian dead letters, and hard-link rejection by claim readers.
Cancellation invalidates the claimed callback without advancing ownership prematurely,
while permanent cleanup failures remain explicitly parked."""

from __future__ import annotations

import sys
import time

import pytest
from _support.synchronization import SCHEDULER_TIMEOUT_SECONDS


def _move_pending_to_ready(scheduler):
    """Move one retry record from pending to ready state."""
    with scheduler._condition:
        items = tuple(scheduler._current.values())
        for item in items:
            scheduler._current.pop(item.key, None)
            scheduler._drop_pending_charge_locked(item)
            scheduler._enqueue_ready_locked(item)
            scheduler._ready_by_key[item.key] = item
            scheduler._ready_bytes += item.retained_bytes


def test_cancel_invalidates_already_claimed_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify cancel invalidates already claimed retry."""
    import schema_sanitizer.core_impl.retry_scheduler as module

    scheduler = module._RetryScheduler()
    monkeypatch.setattr(scheduler, "_ensure_workers", lambda: None)
    calls: list[str] = []
    key = ("cancel-invalidates-already-claimed-retry-aba", 1)
    assert scheduler.schedule(key, lambda: calls.append("old"), delay_seconds=0)
    _move_pending_to_ready(scheduler)
    with scheduler._condition:
        item = scheduler._take_ready_locked()
        assert item is not None
        scheduler._active_retries += 1
        scheduler._active_bytes += item.retained_bytes
    scheduler.cancel(key)
    with scheduler._condition:
        live = scheduler._key_generations.get(item.key) == item.token
    if live:
        item.callback()
    else:
        scheduler._stale_generation_drops += 1
    assert calls == []
    assert scheduler._stale_generation_drops == 1


def test_replacement_advances_generation_only_after_acceptance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify replacement advances generation only after acceptance."""
    import schema_sanitizer.core_impl.retry_scheduler as module

    scheduler = module._RetryScheduler()
    monkeypatch.setattr(scheduler, "_ensure_workers", lambda: None)
    monkeypatch.setattr(module, "_MAX_EMERGENCY_RETRIES", 0)
    key = ("cancel-invalidates-already-claimed-retry-generation", 1)
    normalized_key = module._normalize_retry_key(key)
    assert scheduler.schedule(key, lambda: None, delay_seconds=1, retained_bytes=64)
    generation = scheduler._key_generations[normalized_key]
    monkeypatch.setattr(module, "_MAX_PENDING_BYTES", 1)
    assert not scheduler.schedule(key, lambda: None, delay_seconds=1, retained_bytes=64)
    assert scheduler._key_generations[normalized_key] == generation


def test_release_guardian_dead_letters_permanent_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify release guardian dead letters permanent failure."""
    import schema_sanitizer.core_impl.retry_scheduler as module

    monkeypatch.setattr(module, "_RELEASE_MAX_ATTEMPTS", 2)
    monkeypatch.setattr(module, "_RELEASE_RETRY_MAX_SECONDS", 0.001)
    monkeypatch.setattr(module, "_IDLE_SECONDS", 0.005)

    class WorkerPermit:
        def release(self) -> None:
            """Release the resource held by the worker permit test double."""
            pass

    monkeypatch.setattr(module, "acquire_release_guardian_thread", WorkerPermit)
    guardian = module._ReleaseGuardian()

    class Broken:
        def release(self) -> None:
            """Release the resource held by the broken test double."""
            raise OSError("permanent")

    assert guardian.adopt(Broken(), retained_bytes=32)
    deadline = time.monotonic() + SCHEDULER_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        snap = guardian.snapshot()
        if snap.dead_letter_owners:
            break
        time.sleep(0.002)
    snap = guardian.snapshot()
    assert snap.pending_owners == 0
    assert snap.dead_letter_owners == 1
    assert snap.dead_letter_bytes == 32


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX hard-link claim reader required")
def test_claim_reader_rejects_hardlinks(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify claim reader rejects hardlinks."""
    import schema_sanitizer.core_impl.path_identity as module

    original = tmp_path / "claim-a"
    alias = tmp_path / "claim-b"
    original.write_bytes(b"{}")
    try:
        alias.hardlink_to(original)
    except OSError:
        pytest.skip("hard links unavailable")
    monkeypatch.setattr(
        module,
        "acquire_file_descriptors",
        lambda _n: type("L", (), {"release": lambda self: None})(),
    )
    fd = __import__("os").open(tmp_path, __import__("os").O_RDONLY)
    try:
        with pytest.raises(OSError, match="hard-link aliases"):
            module._read_claim_at(fd, original.name)
    finally:
        __import__("os").close(fd)
