"""Verify that cross-process resource evidence can tune admission without weakening limits.

Combined reservations must reject overcommit and stale owners must be reclaimed, while opt-in,
bounded percentile telemetry adjusts concurrency reserves without changing static defaults.
"""

from __future__ import annotations

import json
import multiprocessing
import os
from pathlib import Path

import pytest
from _support.synchronization import (
    SCHEDULER_TIMEOUT_SECONDS,
    join_process_or_fail,
    wait_event_or_fail,
)

from schema_sanitizer.core_impl import memory_budget as memory_budget_module
from schema_sanitizer.core_impl.cross_process_storage import (
    _release_cross_process_raw,
    _reserve_cross_process_raw,
    cross_process_reserved_bytes,
)
from schema_sanitizer.core_impl.memory_budget import (
    ProcessResidentMemorySnapshot,
    adaptive_concurrency_target,
)
from schema_sanitizer.core_impl.safety_margins import (
    record_resource_telemetry,
    tuned_memory_reserve_bytes,
    tuned_temporary_free_bytes,
)

_REQUIRES_POSIX_COORDINATION = pytest.mark.skipif(
    os.name == "nt",
    reason="optional cross-process coordination requires POSIX advisory locks",
)


def _reserve_in_child(
    directory: str,
    device: int,
    requested: int,
    capacity: int,
    ready: multiprocessing.synchronize.Event,
    release: multiprocessing.synchronize.Event,
    result: multiprocessing.queues.Queue,
) -> None:
    """Reserve one host-wide share until the parent releases the child."""
    os.environ["SCHEMA_SANITIZER_CROSS_PROCESS_TEMP_RESERVATIONS"] = "1"
    os.environ["SCHEMA_SANITIZER_COORDINATION_DIR"] = directory
    try:
        total = _reserve_cross_process_raw(device, requested, capacity)
        result.put(("reserved", total))
        ready.set()
        try:
            wait_event_or_fail(release)
        finally:
            _release_cross_process_raw(device, requested)
    except BaseException as exc:  # pragma: no cover - returned to parent
        result.put(("error", type(exc).__name__, str(exc)))
        ready.set()
        raise


@_REQUIRES_POSIX_COORDINATION
def test_cross_process_reservations_reject_combined_overcommit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Independent processes cannot each claim the same filesystem headroom."""
    monkeypatch.setenv("SCHEMA_SANITIZER_CROSS_PROCESS_TEMP_RESERVATIONS", "1")
    monkeypatch.setenv("SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    release = context.Event()
    result = context.Queue()
    child = context.Process(
        target=_reserve_in_child,
        args=(str(tmp_path), 99123, 70, 100, ready, release, result),
    )
    child.start()
    assert ready.wait(timeout=SCHEDULER_TIMEOUT_SECONDS)
    assert result.get(timeout=SCHEDULER_TIMEOUT_SECONDS) == ("reserved", 70)
    with pytest.raises(OSError, match="cross-process"):
        _reserve_cross_process_raw(99123, 40, 100)
    assert cross_process_reserved_bytes(99123) == 70
    release.set()
    join_process_or_fail(child)
    assert child.exitcode == 0
    assert cross_process_reserved_bytes(99123) == 0


@_REQUIRES_POSIX_COORDINATION
def test_cross_process_registry_reclaims_dead_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reservations from crashed processes are removed before new admission."""
    monkeypatch.setenv("SCHEMA_SANITIZER_CROSS_PROCESS_TEMP_RESERVATIONS", "1")
    monkeypatch.setenv("SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    state = {
        "version": 1,
        "processes": {
            "999999:dead": {
                "pid": 999999,
                "start": "dead",
                "reserved": 90,
                "updated": 0,
            }
        },
    }
    path = tmp_path / "schema-sanitizer-temp-77.json"
    path.write_text(json.dumps(state), encoding="utf-8")
    assert _reserve_cross_process_raw(77, 80, 100) == 80
    _release_cross_process_raw(77, 80)
    assert cross_process_reserved_bytes(77) == 0


@_REQUIRES_POSIX_COORDINATION
def test_telemetry_tuning_uses_bounded_high_percentiles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Observed overhead raises margins but cannot consume an unsafe fraction."""
    monkeypatch.setenv("SCHEMA_SANITIZER_TELEMETRY_TUNING", "1")
    monkeypatch.setenv("SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    for value in (8 << 20, 16 << 20, 24 << 20, 512 << 20):
        record_resource_telemetry(
            untracked_rss_bytes=value,
            temporary_free_floor_bytes=value,
            source="test",
        )
    memory = tuned_memory_reserve_bytes(256 << 20, 4 << 20)
    disk = tuned_temporary_free_bytes(64 << 20)
    assert memory == 64 << 20  # capped at 25% of resident capacity
    assert disk == 512 << 20


def test_telemetry_is_opt_in_and_preserves_static_defaults(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Persisted samples do not alter runtime policy without explicit opt-in."""
    monkeypatch.setenv("SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    monkeypatch.delenv("SCHEMA_SANITIZER_TELEMETRY_TUNING", raising=False)
    (tmp_path / "schema-sanitizer-resource-telemetry.json").write_text(
        json.dumps(
            {
                "version": 1,
                "samples": [
                    {
                        "untracked_rss_bytes": 1 << 30,
                        "temporary_free_floor_bytes": 1 << 30,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    assert tuned_memory_reserve_bytes(256 << 20, 4 << 20) == 4 << 20
    assert tuned_temporary_free_bytes(64 << 20) == 64 << 20


@_REQUIRES_POSIX_COORDINATION
def test_telemetry_profile_remains_bounded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Long-running workers retain only the newest bounded sample window."""
    monkeypatch.setenv("SCHEMA_SANITIZER_TELEMETRY_TUNING", "1")
    monkeypatch.setenv("SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    for value in range(300):
        record_resource_telemetry(untracked_rss_bytes=value, source="fuzz")
    profile = json.loads((tmp_path / "schema-sanitizer-resource-telemetry.json").read_text())
    assert len(profile["samples"]) == 256
    assert profile["samples"][0]["untracked_rss_bytes"] == 44


@_REQUIRES_POSIX_COORDINATION
def test_adaptive_concurrency_consumes_tuned_memory_reserve(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Persisted p95 overhead directly narrows live worker admission."""
    monkeypatch.setenv("SCHEMA_SANITIZER_TELEMETRY_TUNING", "1")
    monkeypatch.setenv("SCHEMA_SANITIZER_COORDINATION_DIR", str(tmp_path))
    record_resource_telemetry(untracked_rss_bytes=20 << 20, source="production")
    monkeypatch.setattr(
        memory_budget_module,
        "process_resident_memory_snapshot",
        lambda: ProcessResidentMemorySnapshot(100 << 20, 0, 0),
    )
    assert adaptive_concurrency_target(10, per_slot_bytes=10 << 20) == 7
