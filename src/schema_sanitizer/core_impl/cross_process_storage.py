"""Optional host-wide temporary-storage accounting across worker processes."""

from __future__ import annotations

import json
import os
import tempfile
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from time import time
from typing import Iterator

from .coordination_journal import (
    commit_locked_payload,
    coordination_file_lock,
    open_coordination_file,
    recover_locked_payload,
)
from .process_identity import process_identity_matches, process_start_token

try:  # pragma: no cover - exercised on POSIX CI
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback
    fcntl = None  # type: ignore[assignment]

_ENV_ENABLED = "SCHEMA_SANITIZER_CROSS_PROCESS_TEMP_RESERVATIONS"
_ENV_DIRECTORY = "SCHEMA_SANITIZER_COORDINATION_DIR"
_MAX_STATE_BYTES = 1 << 20
_COORDINATION_DIRECTORY_OVERRIDE: ContextVar[Path | None] = ContextVar(
    "schema_sanitizer_storage_coordination_directory_override", default=None
)


def _enabled() -> bool:
    """Return whether cross-process coordination is explicitly enabled."""
    value = os.getenv(_ENV_ENABLED, "").strip().lower()
    return fcntl is not None and value in {"1", "true", "yes", "on"}


def cross_process_storage_enabled() -> bool:
    """Return the configuration snapshot used by a new process-local device state."""
    return _enabled()


def cross_process_storage_directory() -> Path:
    """Return the coordination directory captured by a new device state."""
    return _coordination_directory()


def _coordination_directory() -> Path:
    """Return the shared host directory used for reservation state."""
    configured = os.getenv(_ENV_DIRECTORY)
    path = Path(configured) if configured else Path(tempfile.gettempdir())
    path.mkdir(parents=True, exist_ok=True)
    return path


def _process_start_token(pid: int) -> str:
    """Return the shared PID-reuse-safe process start token."""
    return process_start_token(pid)


def _nonnegative_int(value: object) -> int:
    """Return a non-negative JSON integer, rejecting every other type."""
    return max(0, value) if isinstance(value, int) else 0


def _process_alive(pid: int, start_token: str) -> bool:
    """Return whether the recorded process still owns its PID instance."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    current = _process_start_token(pid)
    return process_identity_matches(start_token, current)


def _decode_state(raw: bytes) -> dict[str, object]:
    """Decode storage coordination state without losing reservations."""
    if not raw:
        return {"version": 1, "processes": {}}
    if len(raw) > _MAX_STATE_BYTES:
        raise OSError("cross-process temporary-storage state exceeds its bounded file size")
    try:
        decoded = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OSError("cross-process temporary-storage state is corrupt") from exc
    if not isinstance(decoded, dict):
        raise OSError("cross-process temporary-storage state root must be an object")
    version = decoded.get("version", 1)
    if version != 1:
        raise OSError(f"unsupported cross-process temporary-storage state version: {version!r}")
    processes = decoded.get("processes", {})
    if not isinstance(processes, dict):
        raise OSError("cross-process temporary-storage processes must be an object")
    return {"version": 1, "processes": processes}


def _encode_state(state: object) -> bytes:
    """Return the canonical representation shared with legacy writers."""
    return json.dumps(state, sort_keys=True, separators=(",", ":")).encode()


@contextmanager
def _locked_state(device: int) -> Iterator[tuple[object, dict[str, object]]]:
    """Lock and transactionally update one device reservation document."""
    directory = _COORDINATION_DIRECTORY_OVERRIDE.get() or _coordination_directory()
    path = directory / f"schema-sanitizer-temp-{device}.json"
    with open_coordination_file(path) as handle:
        with coordination_file_lock(handle):
            raw = recover_locked_payload(
                path,
                handle,
                max_payload_bytes=_MAX_STATE_BYTES,
                validate=_decode_state,
                canonicalize=_encode_state,
                process_alive=_process_alive,
            )
            state = _decode_state(raw)
            baseline = _encode_state(state)
            try:
                yield handle, state
            finally:
                payload = _encode_state(state)
                if len(payload) > _MAX_STATE_BYTES:
                    raise OSError(
                        "cross-process temporary-storage state exceeds its bounded file size"
                    )
                if payload != baseline:
                    commit_locked_payload(
                        path,
                        handle,
                        before=raw,
                        after=payload,
                        max_payload_bytes=_MAX_STATE_BYTES,
                        process_start=_process_start_token(os.getpid()),
                    )


@contextmanager
def _locked_state_for(device: int, directory: Path) -> Iterator[tuple[object, dict[str, object]]]:
    """Use one reservation-lifetime directory with the legacy lock hook."""
    token = _COORDINATION_DIRECTORY_OVERRIDE.set(directory)
    try:
        with _locked_state(device) as value:
            yield value
    finally:
        _COORDINATION_DIRECTORY_OVERRIDE.reset(token)


def _clean_processes(state: dict[str, object]) -> dict[str, dict[str, object]]:
    """Remove crashed or PID-reused owners and return the live process map."""
    raw = state.get("processes")
    if not isinstance(raw, dict):
        raise OSError("cross-process temporary-storage processes must be an object")
    processes = raw
    live: dict[str, dict[str, object]] = {}
    for key, value in processes.items():
        if not isinstance(value, dict):
            raise OSError(f"invalid temporary-storage process entry: {key!r}")
        try:
            pid = int(value.get("pid", -1))
            token = str(value.get("start", "unknown"))
            reserved = int(value.get("reserved", 0))
            inodes = int(value.get("inodes", 0))
            updated = float(value.get("updated", 0.0) or 0.0)
        except (TypeError, ValueError) as exc:
            raise OSError(f"invalid temporary-storage process entry: {key!r}") from exc
        if pid <= 0 or reserved < 0 or inodes < 0:
            raise OSError(f"invalid temporary-storage process entry: {key!r}")
        if (reserved or inodes) and _process_alive(pid, token):
            live[str(key)] = {
                "pid": pid,
                "start": token,
                "reserved": reserved,
                "inodes": inodes,
                "updated": updated,
            }
    state["processes"] = live
    return live


def reserve_cross_process(
    device: int,
    size_bytes: int,
    capacity_bytes: int,
    *,
    inode_count: int = 0,
    inode_capacity: int | None = None,
    enabled: bool | None = None,
    coordination_directory: Path | None = None,
) -> int:
    """Atomically reserve host-wide bytes and return the resulting total.

    A disabled or unsupported platform returns zero. When enabled, admission and
    dead-owner cleanup happen under one interprocess lock.
    """
    requested = max(0, int(size_bytes))
    requested_inodes = max(0, int(inode_count))
    coordinated = _enabled() if enabled is None else bool(enabled and fcntl is not None)
    if not coordinated or (requested == 0 and requested_inodes == 0):
        return 0
    pid = os.getpid()
    start = _process_start_token(pid)
    owner = f"{pid}:{start}"
    state_context = (
        _locked_state_for(device, coordination_directory)
        if coordination_directory is not None
        else _locked_state(device)
    )
    with state_context as (_handle, state):
        processes = _clean_processes(state)
        total = sum(_nonnegative_int(item.get("reserved")) for item in processes.values())
        next_total = total + requested
        total_inodes = sum(_nonnegative_int(item.get("inodes")) for item in processes.values())
        next_inodes = total_inodes + requested_inodes
        if next_total > max(0, int(capacity_bytes)):
            raise OSError(
                f"cross-process temporary-storage capacity exhausted: "
                f"{next_total} bytes > {capacity_bytes} bytes"
            )
        if inode_capacity is not None and next_inodes > max(0, int(inode_capacity)):
            raise OSError(
                f"cross-process temporary inode capacity exhausted: "
                f"{next_inodes} inodes > {inode_capacity} inodes"
            )
        current = processes.get(owner, {"pid": pid, "start": start, "reserved": 0, "inodes": 0})
        processes[owner] = {
            "pid": pid,
            "start": start,
            "reserved": _nonnegative_int(current.get("reserved")) + requested,
            "inodes": _nonnegative_int(current.get("inodes")) + requested_inodes,
            "updated": time(),
        }
        return next_total


def release_cross_process(
    device: int,
    size_bytes: int,
    *,
    inode_count: int = 0,
    enabled: bool | None = None,
    coordination_directory: Path | None = None,
) -> int:
    """Release this process host-wide bytes and return the remaining total."""
    amount = max(0, int(size_bytes))
    amount_inodes = max(0, int(inode_count))
    coordinated = _enabled() if enabled is None else bool(enabled and fcntl is not None)
    if not coordinated or (amount == 0 and amount_inodes == 0):
        return 0
    pid = os.getpid()
    start = _process_start_token(pid)
    owner = f"{pid}:{start}"
    state_context = (
        _locked_state_for(device, coordination_directory)
        if coordination_directory is not None
        else _locked_state(device)
    )
    with state_context as (_handle, state):
        processes = _clean_processes(state)
        current = processes.get(owner)
        if current is not None:
            remaining = max(0, _nonnegative_int(current.get("reserved")) - amount)
            remaining_inodes = max(0, _nonnegative_int(current.get("inodes")) - amount_inodes)
            if remaining or remaining_inodes:
                current["reserved"] = remaining
                current["inodes"] = remaining_inodes
                current["updated"] = time()
            else:
                processes.pop(owner, None)
        return sum(_nonnegative_int(item.get("reserved")) for item in processes.values())


def cross_process_reserved_bytes(device: int) -> int:
    """Return live host-wide reservations for one filesystem device."""
    if not _enabled():
        return 0
    with _locked_state(device) as (_handle, state):
        processes = _clean_processes(state)
        return sum(_nonnegative_int(item.get("reserved")) for item in processes.values())


__all__ = [
    "cross_process_reserved_bytes",
    "cross_process_storage_directory",
    "cross_process_storage_enabled",
    "release_cross_process",
    "reserve_cross_process",
]
