"""Bounded, opt-in tuning of safety margins from local resource telemetry."""

from __future__ import annotations

import json
import math
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from threading import Lock
from time import monotonic, time
from typing import Iterator

from .coordination_journal import (
    commit_locked_payload,
    commit_locked_payload_relaxed,
    coordination_file_lock,
    open_coordination_file,
    recover_locked_payload,
)
from .process_identity import process_is_alive, process_start_token

try:  # pragma: no cover - POSIX CI path
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]

_ENV_ENABLED = "SCHEMA_SANITIZER_TELEMETRY_TUNING"
_ENV_DIRECTORY = "SCHEMA_SANITIZER_COORDINATION_DIR"
_MAX_SAMPLES = 256
_MAX_FILE_BYTES = 1 << 20
_FSYNC_SECONDS = 10.0
_FSYNC_EVERY = 16
_SYNC_LOCK = Lock()
_LAST_FSYNC = 0.0
_WRITES_SINCE_FSYNC = 0


def telemetry_tuning_enabled() -> bool:
    """Return whether persisted telemetry may influence safety margins."""
    value = os.getenv(_ENV_ENABLED, "").strip().lower()
    return fcntl is not None and value in {"1", "true", "yes", "on"}


def _path() -> Path:
    """Implement the internal _path helper."""
    base = Path(os.getenv(_ENV_DIRECTORY, tempfile.gettempdir()))
    base.mkdir(parents=True, exist_ok=True)
    return base / "schema-sanitizer-resource-telemetry.json"


def _should_fsync() -> bool:
    """Amortize durable flushes while keeping the profile immediately readable."""
    global _LAST_FSYNC, _WRITES_SINCE_FSYNC
    now = monotonic()
    with _SYNC_LOCK:
        _WRITES_SINCE_FSYNC += 1
        if _WRITES_SINCE_FSYNC < _FSYNC_EVERY and now - _LAST_FSYNC < _FSYNC_SECONDS:
            return False
        _WRITES_SINCE_FSYNC = 0
        _LAST_FSYNC = now
        return True


def _decode_profile(raw: bytes) -> dict[str, object]:
    """Decode advisory telemetry without replacing corrupt or future state."""
    if not raw:
        return {"version": 1, "samples": []}
    if len(raw) > _MAX_FILE_BYTES:
        raise OSError("resource telemetry profile exceeds its bounded file size")
    try:
        decoded = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OSError("resource telemetry profile is corrupt") from exc
    if not isinstance(decoded, dict):
        raise OSError("resource telemetry profile root must be an object")
    version = decoded.get("version", 1)
    if version != 1:
        raise OSError(f"unsupported resource telemetry profile version: {version!r}")
    samples = decoded.get("samples", [])
    if not isinstance(samples, list) or any(not isinstance(item, dict) for item in samples):
        raise OSError("resource telemetry samples must be an array of objects")
    return {"version": 1, "samples": samples}


def _encode_profile(profile: object) -> bytes:
    """Return the bounded canonical representation shared by all writers."""
    return json.dumps(profile, sort_keys=True, separators=(",", ":")).encode("utf-8")


@contextmanager
def _locked_profile(
    *,
    writable: bool = False,
    durable: bool = False,
) -> Iterator[tuple[object, dict[str, object]]]:
    """Recover, lock and optionally transactionally update telemetry state."""
    path = _path()
    with open_coordination_file(path) as handle:
        with coordination_file_lock(handle):
            raw = recover_locked_payload(
                path,
                handle,
                max_payload_bytes=_MAX_FILE_BYTES,
                process_alive=process_is_alive,
            )
            profile = _decode_profile(raw)
            baseline = _encode_profile(profile)
            try:
                yield handle, profile
            except BaseException:
                raise
            else:
                if writable:
                    payload = _encode_profile(profile)
                    if len(payload) > _MAX_FILE_BYTES:
                        raise OSError("resource telemetry profile exceeds its bounded file size")
                    if payload != baseline:
                        commit = (
                            commit_locked_payload
                            if durable or _should_fsync()
                            else commit_locked_payload_relaxed
                        )
                        commit(
                            path,
                            handle,
                            before=raw,
                            after=payload,
                            max_payload_bytes=_MAX_FILE_BYTES,
                            process_start=process_start_token(os.getpid()),
                        )


def record_resource_telemetry(
    *,
    untracked_rss_bytes: int | None = None,
    temporary_free_floor_bytes: int | None = None,
    source: str = "runtime",
) -> None:
    """Append one bounded sample without fsyncing every operation close."""
    if not telemetry_tuning_enabled():
        return
    try:
        sample = {
            "timestamp": time(),
            "source": str(source)[:64],
            "untracked_rss_bytes": (
                None if untracked_rss_bytes is None else max(0, int(untracked_rss_bytes))
            ),
            "temporary_free_floor_bytes": (
                None
                if temporary_free_floor_bytes is None
                else max(0, int(temporary_free_floor_bytes))
            ),
        }
        with _locked_profile(writable=True) as (_handle, profile):
            raw = profile.get("samples")
            samples = raw if isinstance(raw, list) else []
            samples.append(sample)
            profile["samples"] = samples[-_MAX_SAMPLES:]
    except BaseException:
        # Telemetry is advisory and often runs after an ownership/resource
        # commit. No exception, including asynchronous BaseException subclasses,
        # may turn observation into a failed resource transaction. Cancellation
        # remains authoritative at explicit operation safe points instead.
        return


def _percentile(values: list[int], quantile: float) -> int | None:
    """Implement the internal _percentile helper."""
    if not values:
        return None
    ordered = sorted(max(0, int(value)) for value in values)
    rank = max(0, min(len(ordered) - 1, math.ceil(quantile * len(ordered)) - 1))
    return ordered[rank]


def _samples() -> list[dict[str, object]]:
    """Implement the internal _samples helper."""
    if not telemetry_tuning_enabled() or not _path().exists():
        return []
    try:
        with _locked_profile() as (_handle, profile):
            raw = profile.get("samples")
            return [item for item in raw if isinstance(item, dict)] if isinstance(raw, list) else []
    except OSError:
        return []


def tuned_memory_reserve_bytes(capacity_bytes: int, fallback_bytes: int) -> int:
    """Implement the internal tuned_memory_reserve_bytes helper."""
    capacity = max(0, int(capacity_bytes))
    fallback = max(0, int(fallback_bytes))
    values = [
        value for item in _samples() if isinstance(value := item.get("untracked_rss_bytes"), int)
    ]
    observed = _percentile(values, 0.95)
    if observed is None:
        return fallback
    upper = max(fallback, min(capacity // 4, 2 << 30))
    return max(fallback, min(upper, observed + observed // 8))


def tuned_temporary_free_bytes(fallback_bytes: int) -> int:
    """Implement the internal tuned_temporary_free_bytes helper."""
    fallback = max(0, int(fallback_bytes))
    values = [
        value
        for item in _samples()
        if isinstance(value := item.get("temporary_free_floor_bytes"), int)
    ]
    observed = _percentile(values, 0.95)
    return fallback if observed is None else max(fallback, min(4 << 30, observed))


def _reset_after_fork() -> None:
    """Implement the internal _reset_after_fork helper."""
    global _SYNC_LOCK, _LAST_FSYNC, _WRITES_SINCE_FSYNC
    _SYNC_LOCK = Lock()
    _LAST_FSYNC = 0.0
    _WRITES_SINCE_FSYNC = 0


from .fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler("safety-margins", mode="quarantine_only")


__all__ = [
    "record_resource_telemetry",
    "telemetry_tuning_enabled",
    "tuned_memory_reserve_bytes",
    "tuned_temporary_free_bytes",
]
