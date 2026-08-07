"""Optional cgroup/host-wide resident-memory admission across processes."""

from __future__ import annotations

import json
import os
import tempfile
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from threading import Lock
from time import time
from typing import Iterator

from .coordination_journal import (
    commit_locked_payload,
    coordination_file_lock,
    open_coordination_file,
    recover_locked_payload,
)
from .finalization import runtime_is_finalizing
from .process_identity import process_identity_matches, process_start_token

try:  # pragma: no cover - exercised on POSIX CI
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback
    fcntl = None  # type: ignore[assignment]

_ENV_ENABLED = "SCHEMA_SANITIZER_CROSS_PROCESS_MEMORY_RESERVATIONS"
_ENV_DIRECTORY = "SCHEMA_SANITIZER_COORDINATION_DIR"
_MAX_STATE_BYTES = 1 << 20
_COORDINATION_PATH_OVERRIDE: ContextVar[Path | None] = ContextVar(
    "schema_sanitizer_memory_coordination_path_override", default=None
)


def _enabled() -> bool:
    """Implement the internal _enabled helper."""
    value = os.getenv(_ENV_ENABLED, "").strip().lower()
    return fcntl is not None and value in {"1", "true", "yes", "on"}


def _coordination_path() -> Path:
    """Implement the internal _coordination_path helper."""
    configured = os.getenv(_ENV_DIRECTORY)
    directory = Path(configured) if configured else Path(tempfile.gettempdir())
    directory.mkdir(parents=True, exist_ok=True)
    return directory / "schema-sanitizer-resident-memory.json"


def _process_start_token(pid: int) -> str:
    """Return the shared PID-reuse-safe process start token."""
    return process_start_token(pid)


def _nonnegative_int(value: object) -> int:
    """Return a non-negative JSON integer, rejecting every other type."""
    return max(0, value) if isinstance(value, int) else 0


def _process_alive(pid: int, start_token: str) -> bool:
    """Implement the internal _process_alive helper."""
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
    """Decode coordination state without failing open on corruption."""
    if not raw:
        return {"version": 1, "leases": {}}
    if len(raw) > _MAX_STATE_BYTES:
        raise OSError("cross-process resident-memory state exceeds its bounded file size")
    try:
        decoded = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OSError("cross-process resident-memory state is corrupt") from exc
    if not isinstance(decoded, dict):
        raise OSError("cross-process resident-memory state root must be an object")
    version = decoded.get("version", 1)
    if version != 1:
        raise OSError(f"unsupported cross-process resident-memory state version: {version!r}")
    leases = decoded.get("leases", {})
    if not isinstance(leases, dict):
        raise OSError("cross-process resident-memory leases must be an object")
    return {"version": 1, "leases": leases}


def _encode_state(state: object) -> bytes:
    """Return the canonical representation shared with legacy writers."""
    return json.dumps(state, sort_keys=True, separators=(",", ":")).encode()


@contextmanager
def _locked_state() -> Iterator[dict[str, object]]:
    """Lock and transactionally update resident-memory coordination state."""
    path = _COORDINATION_PATH_OVERRIDE.get() or _coordination_path()
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
                yield state
            finally:
                payload = _encode_state(state)
                if len(payload) > _MAX_STATE_BYTES:
                    raise OSError(
                        "cross-process resident-memory state exceeds its bounded file size"
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
def _locked_state_for(path: Path) -> Iterator[dict[str, object]]:
    """Use one lease-lifetime path while preserving the legacy lock hook."""
    token = _COORDINATION_PATH_OVERRIDE.set(path)
    try:
        with _locked_state() as state:
            yield state
    finally:
        _COORDINATION_PATH_OVERRIDE.reset(token)


def _clean_leases(state: dict[str, object]) -> dict[str, dict[str, object]]:
    """Implement the internal _clean_leases helper."""
    raw = state.get("leases")
    if not isinstance(raw, dict):
        raise OSError("cross-process resident-memory leases must be an object")
    leases = raw
    live: dict[str, dict[str, object]] = {}
    for key, value in leases.items():
        if not isinstance(value, dict):
            raise OSError(f"invalid resident-memory lease entry: {key!r}")
        try:
            pid = int(value.get("pid", -1))
            start = str(value.get("start", "unknown"))
            reserved = int(value.get("reserved", 0))
            updated = float(value.get("updated", 0.0) or 0.0)
        except (TypeError, ValueError) as exc:
            raise OSError(f"invalid resident-memory lease entry: {key!r}") from exc
        if pid <= 0 or reserved < 0:
            raise OSError(f"invalid resident-memory lease entry: {key!r}")
        if reserved and _process_alive(pid, start):
            live[str(key)] = {
                "pid": pid,
                "start": start,
                "reserved": reserved,
                "updated": updated,
            }
    state["leases"] = live
    return live


class CrossProcessMemoryLease:
    """Crash-recoverable host-wide resident-memory admission lease."""

    def __init__(self, capacity_bytes: int, initial_bytes: int) -> None:
        """Initialize this helper."""
        self._capacity = max(1, int(capacity_bytes))
        self._pid = os.getpid()
        self._start = _process_start_token(self._pid)
        # New writers aggregate all leases owned by one PID instance. Legacy
        # per-operation keys remain readable and coexist during rolling deploys.
        self._key = f"{self._pid}:{self._start}"
        self._reserved = 0
        self._lock = Lock()
        self._released = False
        self._coordinated = _enabled()
        self._coordination_path = _coordination_path() if self._coordinated else None
        if self._coordinated and initial_bytes > 0:
            self.resize(initial_bytes)

    @property
    def reserved_bytes(self) -> int:
        """Implement the internal reserved_bytes helper."""
        if os.getpid() != self._pid:
            return 0
        with self._lock:
            return 0 if self._released else self._reserved

    def resize(self, size_bytes: int) -> None:
        """Implement the internal resize helper."""
        if os.getpid() != self._pid:
            raise RuntimeError("cross-process memory lease cannot be reused after fork")
        requested = max(0, int(size_bytes))
        with self._lock:
            if self._released:
                return
            if not self._coordinated:
                self._reserved = 0
                return
            coordination_path = self._coordination_path
            if coordination_path is None:
                raise RuntimeError("coordinated memory lease has no coordination path")
            with _locked_state_for(coordination_path) as state:
                leases = _clean_leases(state)
                owner_reserved = _nonnegative_int(leases.get(self._key, {}).get("reserved"))
                if owner_reserved < self._reserved:
                    raise OSError(
                        "cross-process resident-memory owner total is smaller than this live lease"
                    )
                next_owner_reserved = owner_reserved - self._reserved + requested
                total_reserved = sum(
                    _nonnegative_int(item.get("reserved")) for item in leases.values()
                )
                next_total = total_reserved - owner_reserved + next_owner_reserved
                if requested > self._reserved and next_total > self._capacity:
                    from ..errors import SchemaSanitizerResourceError

                    raise SchemaSanitizerResourceError(
                        "cross-process resident-memory capacity exhausted",
                        detail={
                            "stage": "cross_process_memory",
                            "limit_name": "cross_process_resident_memory_bytes",
                            "limit_bytes": self._capacity,
                            "actual_bytes": next_total,
                        },
                    )
                if next_owner_reserved:
                    leases[self._key] = {
                        "pid": self._pid,
                        "start": self._start,
                        "reserved": next_owner_reserved,
                        "updated": time(),
                    }
                else:
                    leases.pop(self._key, None)
            self._reserved = requested

    def release(self) -> None:
        """Implement the internal release helper."""
        if os.getpid() != self._pid:
            return
        with self._lock:
            if self._released:
                return
            if self._coordinated and os.getpid() == self._pid:
                coordination_path = self._coordination_path
                if coordination_path is None:
                    raise RuntimeError("coordinated memory lease has no coordination path")
                with _locked_state_for(coordination_path) as state:
                    leases = _clean_leases(state)
                    owner_reserved = _nonnegative_int(leases.get(self._key, {}).get("reserved"))
                    if owner_reserved < self._reserved:
                        raise OSError(
                            "cross-process resident-memory owner total is smaller "
                            "than this live lease"
                        )
                    remaining = owner_reserved - self._reserved
                    if remaining:
                        leases[self._key] = {
                            "pid": self._pid,
                            "start": self._start,
                            "reserved": remaining,
                            "updated": time(),
                        }
                    else:
                        leases.pop(self._key, None)
            self._reserved = 0
            self._released = True

    close = release

    def __del__(self) -> None:
        """Release ownership unless interpreter teardown makes I/O unsafe."""
        try:
            if runtime_is_finalizing():
                return
            self.release()
        except BaseException:
            pass


def acquire_cross_process_memory(
    capacity_bytes: int, requested_limit: int
) -> CrossProcessMemoryLease:
    """Admit one operation with a conservative incremental initial reservation."""
    initial = min(256 << 20, max(8 << 20, max(1, int(requested_limit)) // 16))
    return CrossProcessMemoryLease(capacity_bytes, min(initial, max(1, capacity_bytes)))


def cross_process_memory_reserved_bytes() -> int:
    """Return live host-wide reservations, pruning crashed owners first."""
    if not _enabled():
        return 0
    with _locked_state() as state:
        return sum(_nonnegative_int(item.get("reserved")) for item in _clean_leases(state).values())


__all__ = [
    "CrossProcessMemoryLease",
    "acquire_cross_process_memory",
    "cross_process_memory_reserved_bytes",
]
