"""Coordinate optional resident-memory admission across worker processes.

A locked journal and fair coordinator track exact leases, reconcile finalizer releases, remove
stale process state, and rebuild safe synchronization after a fork.
"""

from __future__ import annotations

import json
import math
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from threading import Lock
from time import time
from typing import Iterator, cast

from .bounded_generation import BoundedGenerationPool
from .coordination_journal import (
    commit_locked_payload,
    coordination_file_lock,
    open_coordination_file,
    recover_locked_payload,
)
from .finalization import runtime_is_finalizing
from .finalizer_escrow import ReservedFinalizerEscrow, _reserved_escrow_static_bytes
from .fork_safety import quarantine_inherited_state
from .process_identity import process_is_alive, process_start_token
from .rooted_finalizer import FinalizerReplayCapability, RootedFinalizerAuthority
from .safe_errors import add_bounded_note, clear_exception_traceback
from .terminal_ownership import publish_terminal_owner, retire_terminal_owner

try:  # pragma: no cover - exercised on POSIX CI
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback
    fcntl = None  # type: ignore[assignment]


class _RetryFinalizerDrain(RuntimeError):
    """Keep a claimed escrow owner published when authentication cannot commit."""


_ENV_ENABLED = "SCHEMA_SANITIZER_CROSS_PROCESS_MEMORY_RESERVATIONS"
_ENV_DIRECTORY = "SCHEMA_SANITIZER_COORDINATION_DIR"
_MAX_STATE_BYTES = 1 << 20
_MAX_PROCESS_LEASE_RECORDS = 4096
_MAX_LIVENESS_CHECKS_PER_TRANSACTION = 256
_STALE_KEY_SCRATCH: list[str | None] = [None] * _MAX_PROCESS_LEASE_RECORDS
_STALE_KEY_SCRATCH_LOCK = Lock()
_STALE_KEY_SCRATCH_LOCK_BANK = (Lock(), Lock())
_STALE_KEY_SCRATCH_BANK_INDEX = 0
_STALE_KEY_SCRATCH_FORK_FRESH_LOCK: Lock | None = None
from .static_control_plane import (  # noqa: E402
    register_static_control_plane as _register_static_control_plane,
)

_register_static_control_plane(
    "cross_process_memory_stale_scratch", _MAX_PROCESS_LEASE_RECORDS * 8 + 4096
)
# stale_keys are stored in this fixed scratch buffer; no per-prune list allocation.


def _enabled() -> bool:
    """Return whether cross-process memory coordination is enabled and supported."""
    value = os.getenv(_ENV_ENABLED, "").strip().lower()
    return fcntl is not None and value in {"1", "true", "yes", "on"}


def _coordination_path() -> Path:
    """Return the shared resident-memory coordination document path."""
    configured = os.getenv(_ENV_DIRECTORY)
    directory = Path(configured) if configured else Path(tempfile.gettempdir())
    directory.mkdir(parents=True, exist_ok=True)
    return directory / "schema-sanitizer-resident-memory.json"


def _nonnegative_int(value: object) -> int:
    """Return a non-negative exact JSON integer, rejecting bool/coercions."""
    return max(0, value) if type(value) is int else 0


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
    if set(decoded) != {"version", "leases"}:
        raise OSError("cross-process resident-memory state has unknown or missing fields")
    version = decoded["version"]
    if type(version) is not int or version != 1:
        raise OSError(f"unsupported cross-process resident-memory state version: {version!r}")
    leases = decoded["leases"]
    if not isinstance(leases, dict):
        raise OSError("cross-process resident-memory leases must be an object")
    return {"version": 1, "leases": leases}


def _encode_state(state: object) -> bytes:
    """Return the canonical coordination-state representation."""
    return json.dumps(state, sort_keys=True, separators=(",", ":")).encode()


@contextmanager
def _locked_state(path: Path | None = None) -> Iterator[dict[str, object]]:
    """Lock and transactionally update resident-memory coordination state."""
    path = _coordination_path() if path is None else path
    with open_coordination_file(path) as handle:
        with coordination_file_lock(handle):
            raw = recover_locked_payload(
                path,
                handle,
                max_payload_bytes=_MAX_STATE_BYTES,
                process_alive=process_is_alive,
            )
            state = _decode_state(raw)
            baseline = _encode_state(state)
            committed = False
            try:
                yield state
                committed = True
            finally:
                # Owner mutations are transactional: unexpected exceptions do
                # not persist a partially modified journal. Stale housekeeping
                # is conservatively retried by the next successful transaction.
                if committed:
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
                            process_start=process_start_token(os.getpid()),
                        )


def _clean_leases(state: dict[str, object]) -> dict[str, dict[str, object]]:
    """Prune dead process records in one O(n) scan using preallocated scratch."""
    raw = state.get("leases")
    if not isinstance(raw, dict):
        raise OSError("cross-process resident-memory leases must be an object")
    leases: dict[str, dict[str, object]] = raw
    if len(leases) > _MAX_PROCESS_LEASE_RECORDS:
        raise OSError("cross-process resident-memory process registry exceeds its bound")
    with _STALE_KEY_SCRATCH_LOCK:
        stale_count = 0
        liveness_checks = 0
        for key, value in leases.items():
            if type(key) is not str or not isinstance(value, dict):
                raise OSError(f"invalid resident-memory lease entry: {key!r}")
            pid_value = value.get("pid", -1)
            start_value = value.get("start", "unknown")
            reserved_value = value.get("reserved", 0)
            updated_value = value.get("updated", 0.0)
            if type(pid_value) is not int or type(reserved_value) is not int:
                raise OSError(f"invalid resident-memory lease entry: {key!r}")
            if type(start_value) is not str:
                raise OSError(f"invalid resident-memory lease entry: {key!r}")
            if (
                isinstance(updated_value, bool)
                or not isinstance(updated_value, (int, float))
                or not math.isfinite(float(updated_value))
            ):
                raise OSError(f"invalid resident-memory lease entry: {key!r}")
            if pid_value <= 0 or reserved_value < 0:
                raise OSError(f"invalid resident-memory lease entry: {key!r}")
            if key != f"{pid_value}:{start_value}":
                raise OSError(f"invalid resident-memory lease identity: {key!r}")
            alive = True
            if reserved_value:
                if liveness_checks < _MAX_LIVENESS_CHECKS_PER_TRANSACTION:
                    alive = process_is_alive(pid_value, start_value)
                    liveness_checks += 1
                # Beyond the bounded probe budget, retain the record
                # conservatively; a later transaction continues housekeeping.
            if not reserved_value or not alive:
                _STALE_KEY_SCRATCH[stale_count] = key
                stale_count += 1
        for index in range(stale_count):
            stale_key = _STALE_KEY_SCRATCH[index]
            if stale_key is not None:
                leases.pop(stale_key, None)
                _STALE_KEY_SCRATCH[index] = None
    return leases


_DIRECT_LEASE_LOCK = Lock()
_MAX_DIRECT_LEASES = 4096
_DIRECT_LEASE_FREE = list(range(1, _MAX_DIRECT_LEASES + 1))
_DIRECT_LEASE_FREE_COUNT = _MAX_DIRECT_LEASES
_DIRECT_LEASE_GENERATIONS = [0] * (_MAX_DIRECT_LEASES + 1)


class _DirectLeaseRegistration:
    __slots__ = ("lease_id", "capability")

    def __init__(self, capability: object) -> None:
        """Initialize the direct lease registration and its owned runtime state."""
        self.lease_id = 0
        self.capability = capability

    def __iter__(self):
        """Iterate over the retained values."""
        yield self.lease_id
        yield self.capability


class _DirectLeaseEntry:
    __slots__ = ("owner_id", "capability", "reserved", "generation")

    def __init__(self, owner_id: int, capability: object, generation: int) -> None:
        """Initialize the direct lease entry and its owned runtime state."""
        self.owner_id = owner_id
        self.capability = capability
        self.reserved = 0
        self.generation = generation


_DIRECT_LEASE_LEDGER: dict[int, _DirectLeaseEntry] = {}
_DIRECT_LEASE_UNKNOWN_RELEASES = 0


def _rebuild_direct_lease_free_locked() -> None:
    """Reconstruct direct-lease admission mirrors from exact ledger owners."""
    global _DIRECT_LEASE_FREE_COUNT
    live_slots = bytearray(_MAX_DIRECT_LEASES + 1)
    for lease_id, entry in _DIRECT_LEASE_LEDGER.items():
        slot = lease_id & ((1 << 13) - 1)
        if 0 < slot <= _MAX_DIRECT_LEASES:
            live_slots[slot] = 1
            if entry.generation > _DIRECT_LEASE_GENERATIONS[slot]:
                _DIRECT_LEASE_GENERATIONS[slot] = entry.generation
    free_count = 0
    generation_limit = (1 << 31) - 1
    for slot in range(1, _MAX_DIRECT_LEASES + 1):
        if live_slots[slot] or _DIRECT_LEASE_GENERATIONS[slot] >= generation_limit:
            continue
        _DIRECT_LEASE_FREE[free_count] = slot
        free_count += 1
    for index in range(free_count, _MAX_DIRECT_LEASES):
        _DIRECT_LEASE_FREE[index] = 0
    _DIRECT_LEASE_FREE_COUNT = free_count


def _register_direct_lease(owner: object) -> _DirectLeaseRegistration:
    """Register authoritative ownership for a direct memory lease."""
    global _DIRECT_LEASE_FREE_COUNT
    capability = FinalizerReplayCapability()
    result = _DirectLeaseRegistration(capability)
    with _DIRECT_LEASE_LOCK:
        _rebuild_direct_lease_free_locked()
        if _DIRECT_LEASE_FREE_COUNT <= 0:
            raise OSError("cross-process memory direct-lease registry is full")
        next_free_count = _DIRECT_LEASE_FREE_COUNT - 1
        slot = _DIRECT_LEASE_FREE[next_free_count]
        generation = _DIRECT_LEASE_GENERATIONS[slot] + 1
        if generation >= (1 << 31):
            # Retire the exhausted slot permanently; do not lose a free-list
            # entry by raising after it has already been removed.
            _DIRECT_LEASE_GENERATIONS[slot] = (1 << 31) - 1
            _DIRECT_LEASE_FREE_COUNT = next_free_count
            raise OSError("cross-process memory direct-lease generation exhausted")
        lease_id = (generation << 13) | slot
        entry = _DirectLeaseEntry(id(owner), capability, generation)
        try:
            _DIRECT_LEASE_LEDGER[lease_id] = entry
        except BaseException:
            raise
        _DIRECT_LEASE_GENERATIONS[slot] = generation
        _DIRECT_LEASE_FREE_COUNT = next_free_count
        result.lease_id = lease_id
    return result


def _direct_lease_reserved_authority(owner_id: int, lease_id: int, capability: object) -> int:
    """Return bytes retained by an authenticated direct lease owner."""
    with _DIRECT_LEASE_LOCK:
        entry = _DIRECT_LEASE_LEDGER.get(lease_id)
        if entry is None or entry.owner_id != owner_id or entry.capability is not capability:
            raise OSError("cross-process memory lease capability is not authoritative")
        return entry.reserved


def _direct_lease_reserved(owner: object, lease_id: int, capability: object) -> int:
    """Return bytes retained by an authoritative direct lease."""
    return _direct_lease_reserved_authority(id(owner), lease_id, capability)


def _update_direct_lease_reserved_authority(
    owner_id: int, lease_id: int, capability: object, reserved: int
) -> None:
    """Update bytes retained by an authenticated direct lease owner."""
    global _DIRECT_LEASE_UNKNOWN_RELEASES
    with _DIRECT_LEASE_LOCK:
        entry = _DIRECT_LEASE_LEDGER.get(lease_id)
        if entry is None or entry.owner_id != owner_id or entry.capability is not capability:
            _DIRECT_LEASE_UNKNOWN_RELEASES += 1
            raise OSError("cross-process memory lease capability is not authoritative")
        entry.reserved = reserved


def _update_direct_lease_reserved(
    owner: object, lease_id: int, capability: object, reserved: int
) -> None:
    """Update bytes retained by one authoritative direct memory lease."""
    _update_direct_lease_reserved_authority(id(owner), lease_id, capability, reserved)


def _retire_direct_lease_authority(owner_id: int, lease_id: int, capability: object) -> int:
    """Retire an authenticated direct lease and return its released bytes."""
    global _DIRECT_LEASE_UNKNOWN_RELEASES, _DIRECT_LEASE_FREE_COUNT
    with _DIRECT_LEASE_LOCK:
        entry = _DIRECT_LEASE_LEDGER.get(lease_id)
        if entry is None:
            if isinstance(capability, FinalizerReplayCapability) and capability.released:
                _rebuild_direct_lease_free_locked()
                return 0
            _DIRECT_LEASE_UNKNOWN_RELEASES += 1
            raise OSError("cross-process memory lease capability is not authoritative")
        if entry.owner_id != owner_id or entry.capability is not capability:
            _DIRECT_LEASE_UNKNOWN_RELEASES += 1
            raise OSError("cross-process memory lease capability is not authoritative")
        if isinstance(capability, FinalizerReplayCapability) and capability.released:
            # Prior exact retirement committed before its mapping/free-list
            # bookkeeping. Complete both from authoritative ledger membership.
            _DIRECT_LEASE_LEDGER.pop(lease_id, None)
            _rebuild_direct_lease_free_locked()
            return 0
        reserved = entry.reserved
        if isinstance(capability, FinalizerReplayCapability):
            capability.released = True
        del _DIRECT_LEASE_LEDGER[lease_id]
        _rebuild_direct_lease_free_locked()
        return reserved


def _retire_direct_lease(owner: object, lease_id: int, capability: object) -> int:
    """Retire authoritative ownership for a direct memory lease."""
    return _retire_direct_lease_authority(id(owner), lease_id, capability)


def _direct_lease_snapshot() -> tuple[int, int, int]:
    """Return active lease count, reserved bytes, and unknown releases."""
    with _DIRECT_LEASE_LOCK:
        return (
            len(_DIRECT_LEASE_LEDGER),
            sum(entry.reserved for entry in _DIRECT_LEASE_LEDGER.values()),
            _DIRECT_LEASE_UNKNOWN_RELEASES,
        )


def _direct_lease_total_reserved() -> int:
    """Return the exact current-process total used to reconcile the journal."""
    with _DIRECT_LEASE_LOCK:
        return sum(entry.reserved for entry in _DIRECT_LEASE_LEDGER.values())


def _run_direct_cross_memory_finalizer(authority: RootedFinalizerAuthority) -> None:
    """Release exact direct authority and repair its process journal entry."""
    owner_id = int(cast(int, authority.arg0) or 0)
    lease_id = int(cast(int, authority.arg1) or 0)
    capability = authority.arg2
    coordinated = bool(authority.arg3)
    coordination_path = authority.arg4
    if lease_id > 0 and capability is not None:
        _retire_direct_lease_authority(owner_id, lease_id, capability)
        authority.arg1 = 0
        authority.arg2 = None
    if not coordinated:
        return
    if not isinstance(coordination_path, Path) or not isinstance(authority.arg7, str):
        raise RuntimeError("direct cross-process finalizer lost journal identity")
    owner_total = _direct_lease_total_reserved()
    key = authority.arg7
    with _locked_state(coordination_path) as state:
        leases = _clean_leases(state)
        if owner_total:
            if key not in leases and len(leases) >= _MAX_PROCESS_LEASE_RECORDS:
                raise OSError("cross-process resident-memory process registry is full")
            leases[key] = {
                "pid": int(cast(int, authority.arg5) or 0),
                "start": str(authority.arg6 or ""),
                "reserved": owner_total,
                "updated": time(),
            }
        else:
            leases.pop(key, None)


class CrossProcessMemoryLease:
    """Crash-recoverable host-wide resident-memory admission lease."""

    def __init__(self, capacity_bytes: int, initial_bytes: int) -> None:
        """Initialize the cross process memory lease and its owned runtime state."""
        if type(capacity_bytes) is not int or type(initial_bytes) is not int:
            raise TypeError("cross-process memory sizes must be exact integers")
        if capacity_bytes <= 0 or initial_bytes < 0:
            raise ValueError("cross-process memory sizes are out of range")
        self._capacity = capacity_bytes
        self._pid = os.getpid()
        self._start = process_start_token(self._pid)
        self._key = f"{self._pid}:{self._start}"
        self._reserved = 0
        self._lock = Lock()
        self._released = True
        self._lease_id = 0
        self._capability = None
        self._finalizer_ticket = -1
        self._finalizer_owner: RootedFinalizerAuthority | None = None
        self._journal_reconcile_required = False
        self._journal_cleanup_pending = False
        owner = RootedFinalizerAuthority(_run_direct_cross_memory_finalizer)
        self._finalizer_owner = owner
        ticket: int | None = None
        try:
            ticket = _DIRECT_CROSS_MEMORY_FINALIZER_ESCROW.reserve_rooted(owner)
            if ticket is None:
                raise RuntimeError("direct cross-process memory finalizer escrow exhausted")
            self._finalizer_ticket = ticket
            registration = _register_direct_lease(self)
            self._lease_id = registration.lease_id
            self._capability = registration.capability
            self._coordinated = _enabled()
            self._coordination_path = _coordination_path() if self._coordinated else None
            owner.arg0 = id(self)
            owner.arg1 = self._lease_id
            owner.arg2 = self._capability
            owner.arg3 = self._coordinated
            owner.arg4 = self._coordination_path
            owner.arg5 = self._pid
            owner.arg6 = self._start
            owner.arg7 = self._key
            self._released = False
            if self._coordinated and initial_bytes > 0:
                self.resize(initial_bytes)
        except BaseException as primary:
            cleanup_committed = True
            if self._journal_reconcile_required or self._journal_cleanup_pending:
                try:
                    self._reconcile_journal_to_direct()
                except BaseException as cleanup_error:
                    cleanup_committed = False
                    self._released = False
                    add_bounded_note(
                        primary,
                        "cross-process memory constructor journal reconciliation also failed",
                        cleanup_error,
                    )
            if cleanup_committed and self._lease_id and self._capability is not None:
                try:
                    _retire_direct_lease(self, self._lease_id, self._capability)
                except BaseException as cleanup_error:
                    cleanup_committed = False
                    # Keep the partially-built object live; its prepared finalizer
                    # owns the exact direct capability and can retry deterministically.
                    self._released = False
                    add_bounded_note(
                        primary,
                        "cross-process memory constructor rollback also failed",
                        cleanup_error,
                    )
            if cleanup_committed:
                self._released = True
                self._lease_id = 0
                self._capability = None
                owner.make_ack_only()
                if ticket is not None:
                    try:
                        retired = _DIRECT_CROSS_MEMORY_FINALIZER_ESCROW.release_ticket(ticket)
                    except BaseException:
                        retired = False
                    if retired:
                        self._finalizer_ticket = -1
                        owner.ticket = 0
                        owner.clear()
                    elif _DIRECT_CROSS_MEMORY_FINALIZER_ESCROW.publish_rooted(ticket, owner):
                        self._finalizer_ticket = -1
            raise

    def _write_owner_journal_total(self, owner_total: int) -> None:
        """Project one exact local process total into the persistent journal."""
        if not self._coordinated:
            self._journal_reconcile_required = False
            self._journal_cleanup_pending = False
            return
        coordination_path = self._coordination_path
        if coordination_path is None:
            raise RuntimeError("coordinated memory lease has no coordination path")
        with _locked_state(coordination_path) as state:
            leases = _clean_leases(state)
            if owner_total:
                if self._key not in leases and len(leases) >= _MAX_PROCESS_LEASE_RECORDS:
                    raise OSError("cross-process resident-memory process registry is full")
                leases[self._key] = {
                    "pid": self._pid,
                    "start": self._start,
                    "reserved": owner_total,
                    "updated": time(),
                }
            else:
                leases.pop(self._key, None)
        self._journal_reconcile_required = False
        self._journal_cleanup_pending = False

    def _reconcile_journal_to_direct(self) -> None:
        """Repair journal drift using the exact in-process direct ledger."""
        if not self._coordinated:
            self._journal_reconcile_required = False
            self._journal_cleanup_pending = False
            return
        self._write_owner_journal_total(_direct_lease_total_reserved())

    def _set_capacity(self, capacity_bytes: int) -> None:
        """Set the process lease ceiling after authenticated owner-cap recompute."""
        if type(capacity_bytes) is not int or capacity_bytes <= 0:
            raise ValueError("cross-process memory capacity must be a positive integer")
        with self._lock:
            self._capacity = capacity_bytes

    @property
    def reserved_bytes(self) -> int:
        """Return bytes still reserved by this cross-process memory lease."""
        if os.getpid() != self._pid:
            return 0
        with self._lock:
            if self._released or self._journal_cleanup_pending:
                return 0
            return _direct_lease_reserved(self, self._lease_id, self._capability)

    def resize(self, size_bytes: int) -> None:
        """Resize with direction-aware journal/direct commit ordering.

        Growth publishes to the journal first; shrink publishes to the direct
        ledger first. Every intermediate state is therefore conservative. Any
        failed second commit is repaired from the exact direct-ledger total.
        """
        if os.getpid() != self._pid:
            raise RuntimeError("cross-process memory lease cannot be reused after fork")
        if type(size_bytes) is not int:
            raise TypeError("cross-process memory size must be an exact integer")
        if size_bytes < 0:
            raise ValueError("cross-process memory size must be >= 0")
        requested = size_bytes
        with self._lock:
            if self._released:
                return
            if self._journal_cleanup_pending:
                self._reconcile_journal_to_direct()
                self._released = True
                raise RuntimeError("cross-process memory lease is already released")
            if self._journal_reconcile_required:
                self._reconcile_journal_to_direct()
            current_reserved = _direct_lease_reserved(self, self._lease_id, self._capability)
            if not self._coordinated:
                self._reserved = 0
                _update_direct_lease_reserved(self, self._lease_id, self._capability, 0)
                return
            coordination_path = self._coordination_path
            if coordination_path is None:
                raise RuntimeError("coordinated memory lease has no coordination path")
            direct_total = _direct_lease_total_reserved()
            next_owner_reserved = direct_total - current_reserved + requested

            if requested >= current_reserved:
                # Growth: journal first so another process never sees less than
                # the live local reservations during the commit window.
                with _locked_state(coordination_path) as state:
                    leases = _clean_leases(state)
                    owner_reserved = _nonnegative_int(leases.get(self._key, {}).get("reserved"))
                    # Repair any earlier conservative drift before admission math.
                    if owner_reserved != direct_total:
                        if direct_total:
                            leases[self._key] = {
                                "pid": self._pid,
                                "start": self._start,
                                "reserved": direct_total,
                                "updated": time(),
                            }
                        else:
                            leases.pop(self._key, None)
                        owner_reserved = direct_total
                    total_reserved = sum(
                        _nonnegative_int(item.get("reserved")) for item in leases.values()
                    )
                    next_total = total_reserved - owner_reserved + next_owner_reserved
                    if requested > current_reserved and next_total > self._capacity:
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
                        if self._key not in leases and len(leases) >= _MAX_PROCESS_LEASE_RECORDS:
                            raise OSError("cross-process resident-memory process registry is full")
                        leases[self._key] = {
                            "pid": self._pid,
                            "start": self._start,
                            "reserved": next_owner_reserved,
                            "updated": time(),
                        }
                    else:
                        leases.pop(self._key, None)
                try:
                    _update_direct_lease_reserved(self, self._lease_id, self._capability, requested)
                except BaseException as primary:
                    self._journal_reconcile_required = True
                    try:
                        self._reconcile_journal_to_direct()
                    except BaseException as cleanup_error:
                        add_bounded_note(
                            primary,
                            "cross-process memory journal rollback also failed",
                            cleanup_error,
                        )
                    raise
            else:
                # Shrink: direct ledger first, leaving the journal temporarily
                # high rather than ever exposing optimistic host-wide capacity.
                _update_direct_lease_reserved(self, self._lease_id, self._capability, requested)
                try:
                    self._write_owner_journal_total(next_owner_reserved)
                except BaseException as primary:
                    try:
                        _update_direct_lease_reserved(
                            self, self._lease_id, self._capability, current_reserved
                        )
                    except BaseException as rollback_error:
                        self._journal_reconcile_required = True
                        add_bounded_note(
                            primary,
                            "cross-process memory direct-ledger rollback also failed",
                            rollback_error,
                        )
                    try:
                        self._reconcile_journal_to_direct()
                    except BaseException as cleanup_error:
                        self._journal_reconcile_required = True
                        add_bounded_note(
                            primary,
                            "cross-process memory journal reconciliation also failed",
                            cleanup_error,
                        )
                    raise
            self._reserved = requested

    def release(self) -> None:
        """Retire local authority first; reconcile persistent over-reservation second."""
        if os.getpid() != self._pid:
            return
        with self._lock:
            if self._released:
                ticket = self._finalizer_ticket
                owner = self._finalizer_owner
                if ticket < 0:
                    ticket = owner.ticket if owner is not None else -1
                if ticket >= 0 and isinstance(owner, RootedFinalizerAuthority):
                    if owner.is_armed_for(ticket):
                        self._finalizer_ticket = -1
                        return
                    owner.make_ack_only()
                    try:
                        retired = _DIRECT_CROSS_MEMORY_FINALIZER_ESCROW.release_ticket(ticket)
                    except BaseException:
                        retired = False
                    if retired:
                        self._finalizer_ticket = -1
                        owner.ticket = 0
                        owner.clear()
                    elif _DIRECT_CROSS_MEMORY_FINALIZER_ESCROW.publish_rooted(ticket, owner):
                        raise RuntimeError(
                            "cross-process memory finalizer slot retirement did not commit"
                        )
                    return
                return
            if self._journal_cleanup_pending:
                self._reconcile_journal_to_direct()
                self._journal_cleanup_pending = False
                self._released = True
            else:
                if self._journal_reconcile_required:
                    self._reconcile_journal_to_direct()
                # Local exact authority commits first. The journal can therefore
                # only remain conservatively high if its subsequent fsync fails.
                _retire_direct_lease(self, self._lease_id, self._capability)
                owner = getattr(self, "_finalizer_owner", None)
                if isinstance(owner, RootedFinalizerAuthority):
                    owner.arg1 = 0
                    owner.arg2 = None
                self._reserved = 0
                self._lease_id = 0
                self._capability = None
                if self._coordinated:
                    self._journal_cleanup_pending = True
                    try:
                        self._reconcile_journal_to_direct()
                    except BaseException:
                        # Keep this owner live solely as a journal-cleanup capability.
                        raise
                    self._journal_cleanup_pending = False
                self._released = True
            ticket = self._finalizer_ticket
            owner = getattr(self, "_finalizer_owner", None)
            if ticket >= 0 and isinstance(owner, RootedFinalizerAuthority):
                owner.make_ack_only()
                try:
                    retired = _DIRECT_CROSS_MEMORY_FINALIZER_ESCROW.release_ticket(ticket)
                except BaseException:
                    retired = False
                if retired:
                    self._finalizer_ticket = -1
                    owner.ticket = 0
                    owner.clear()
                elif _DIRECT_CROSS_MEMORY_FINALIZER_ESCROW.publish_rooted(ticket, owner):
                    raise RuntimeError(
                        "cross-process memory finalizer slot retirement did not commit"
                    )
                else:
                    raise RuntimeError(
                        "cross-process memory finalizer slot retirement did not commit"
                    )

    close = release

    def __del__(self) -> None:
        """Arm separate finalizer authority without waiting on escrow locks."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", -1)
            owner = getattr(self, "_finalizer_owner", None)
            if (type(ticket) is not int or ticket < 0) and isinstance(
                owner, RootedFinalizerAuthority
            ):
                ticket = owner.ticket
            if (
                type(ticket) is not int
                or ticket < 0
                or not isinstance(owner, RootedFinalizerAuthority)
            ):
                return
            if bool(getattr(self, "_released", True)):
                owner.make_ack_only()
            if _DIRECT_CROSS_MEMORY_FINALIZER_ESCROW.publish_rooted(ticket, owner):
                self._finalizer_ticket = -1
                return
            try:
                _mark_direct_cross_memory_finalizer_overflow(ticket)
            except BaseException:
                pass
        except BaseException:
            pass


_MAX_ABANDONED_DIRECT_LEASES = 1024
_DIRECT_CROSS_MEMORY_FINALIZER_ESCROW: ReservedFinalizerEscrow[RootedFinalizerAuthority] = (
    ReservedFinalizerEscrow(_MAX_ABANDONED_DIRECT_LEASES, static_kind="cross_process_memory_direct")
)
_DIRECT_CROSS_MEMORY_FINALIZER_OVERFLOWS = 0


def _mark_direct_cross_memory_finalizer_overflow(ticket: int) -> None:
    """Mark direct cross memory finalizer overflow."""
    global _DIRECT_CROSS_MEMORY_FINALIZER_OVERFLOWS
    try:
        _DIRECT_CROSS_MEMORY_FINALIZER_OVERFLOWS += 1
    except MemoryError:
        pass
    publish_terminal_owner("cross_process_memory_finalizer_overflow", ticket, retained_bytes=256)


def drain_direct_cross_process_memory_finalizers() -> int:
    """Release direct leases while their generation remains rooted in escrow."""
    drained = 0

    def process(ticket: int, owner: RootedFinalizerAuthority) -> None:
        """Process one retained work item."""
        nonlocal drained
        owner.run()
        owner.clear()
        owner.ticket = 0
        retire_terminal_owner("cross_process_memory_finalizer", ticket)
        retire_terminal_owner("cross_process_memory_finalizer_overflow", ticket)
        drained += 1

    while True:
        try:
            if not _DIRECT_CROSS_MEMORY_FINALIZER_ESCROW.process_one(process):
                break
        except BaseException:
            # The exact owner remains PUBLISHED in the same generation.
            break

    return drained


def direct_cross_process_memory_finalizer_snapshot() -> tuple[int, int]:
    """Return published direct-finalizer owners and irreversible overflows."""
    return (
        _DIRECT_CROSS_MEMORY_FINALIZER_ESCROW.published_count(),
        max(1, _DIRECT_CROSS_MEMORY_FINALIZER_OVERFLOWS)
        if _DIRECT_CROSS_MEMORY_FINALIZER_ESCROW.overflowed
        else _DIRECT_CROSS_MEMORY_FINALIZER_OVERFLOWS,
    )


_PROCESS_COORDINATOR_LOCK = Lock()
_PROCESS_COORDINATOR: _ProcessCrossMemoryCoordinator | None = None
_COORDINATOR_SLAB_BYTES = 4 << 20
_COORDINATOR_SHRINK_HYSTERESIS_BYTES = 8 << 20
_MAX_FINALIZER_RELEASE_TOKENS = 4096
# Register the lazy coordinator's fixed escrow footprint before any operation can
# observe payload headroom. Construction later re-registers the identical kind
# and amount, so the static baseline cannot shrink after admission begins.
_register_static_control_plane(
    "reserved_finalizer_escrow:cross_process_memory_coordinator",
    _reserved_escrow_static_bytes(_MAX_FINALIZER_RELEASE_TOKENS),
)
_PROCESS_FINALIZER_RELEASE_OVERFLOWS = 0
_PROCESS_FINALIZER_RELEASE_OVERFLOWED = False


def _round_reservation(value: int) -> int:
    """Round a memory reservation to the coordination quantum."""
    if value <= 0:
        return 0
    slab = _COORDINATOR_SLAB_BYTES
    return ((value + slab - 1) // slab) * slab


class _ProcessCrossMemoryFinalizerOwner:
    """Pre-rooted exact cleanup authority separate from the user reservation.

    The escrow must never root ``_ProcessCrossMemoryReservation`` itself: doing
    so would prevent the reservation's destructor from running.  This compact
    record retains only the authentication fields required by the
    safe-point drain, while the reservation remains collectible and merely arms
    this owner during its non-blocking finalizer.
    """

    __slots__ = (
        "_token",
        "_owner_id",
        "_capability",
        "_finalizer_ticket",
        "_escrow_armed_ticket",
        "_primary_released",
    )

    def __init__(self, token: int, owner_id: int, capability: object, ticket: int) -> None:
        """Initialize the process cross memory finalizer owner and its owned runtime state."""
        self._token = token
        self._owner_id = owner_id
        self._capability = capability
        self._finalizer_ticket = ticket
        self._escrow_armed_ticket = 0
        self._primary_released = False

    @property
    def ticket(self) -> int:
        """Return the current ownership ticket."""
        return self._finalizer_ticket

    @ticket.setter
    def ticket(self, value: int) -> None:
        """Replace the ownership ticket mirrored by this finalizer owner."""
        self._finalizer_ticket = int(value)

    def arm_for_ticket(self, ticket: int) -> None:
        """Arm finalizer cleanup for the supplied ownership ticket."""
        exact = int(ticket)
        if exact <= 0:
            raise ValueError("finalizer arm ticket must be positive")
        self._escrow_armed_ticket = exact

    def disarm_ticket(self, ticket: int | None = None) -> None:
        """Disarm cleanup authority for the matching ownership ticket."""
        if ticket is None or self._escrow_armed_ticket == int(ticket):
            self._escrow_armed_ticket = 0

    def is_armed_for(self, ticket: int) -> bool:
        """Return whether cleanup is armed for the supplied ownership ticket."""
        return self._escrow_armed_ticket == int(ticket)


class _ProcessCrossMemoryReservation:
    """One logical contribution to the process-aggregated host reservation."""

    __slots__ = (
        "_coordinator",
        "_token",
        "_finalizer_ticket",
        "_pid",
        "_lock",
        "_reserved",
        "_released",
        "_finalizer_owner",
    )

    def __init__(
        self,
        coordinator: "_ProcessCrossMemoryCoordinator",
        token: int,
        capability: object,
        initial: int,
        finalizer_ticket: int,
    ) -> None:
        """Initialize the process cross memory reservation and its owned runtime state."""
        self._coordinator = coordinator
        self._token = token
        self._finalizer_ticket = finalizer_ticket
        self._pid = os.getpid()
        self._lock = Lock()
        self._reserved = initial
        self._released = False
        self._finalizer_owner = _ProcessCrossMemoryFinalizerOwner(
            token, id(self), capability, finalizer_ticket
        )

    @property
    def _capability(self) -> object:
        # Keep finalizer authentication in the separately rooted authority so
        # stale-capability fault injection remains visible after publication.
        """Return the authoritative capability retained by this lease."""
        return self._finalizer_owner._capability

    @_capability.setter
    def _capability(self, value: object) -> None:
        """Replace the authoritative capability retained by this lease."""
        self._finalizer_owner._capability = value

    def _bind_generation(self, token: int, ticket: int) -> None:
        """Bind this memory lease to its coordinator generation."""
        token = int(token)
        ticket = int(ticket)
        if token <= 0 or ticket <= 0:
            raise RuntimeError("cross-process memory exact owner binding is invalid")
        if self._token not in (0, token) or self._finalizer_owner._token not in (0, token):
            raise RuntimeError("cross-process memory generation binding mismatch")
        self._token = token
        self._finalizer_owner._token = token
        self._finalizer_ticket = ticket
        self._finalizer_owner._finalizer_ticket = ticket

    @property
    def reserved_bytes(self) -> int:
        """Return bytes still reserved by this cross-process memory lease."""
        if os.getpid() != self._pid:
            return 0
        with self._lock:
            return 0 if self._released else self._reserved

    def resize(self, size_bytes: int) -> None:
        """Resize the retained reservation."""
        if os.getpid() != self._pid:
            raise RuntimeError("cross-process memory reservation cannot be reused after fork")
        if type(size_bytes) is not int:
            raise TypeError("cross-process memory size must be an exact integer")
        if size_bytes < 0:
            raise ValueError("cross-process memory size must be >= 0")
        with self._lock:
            if self._released:
                return
            self._coordinator.resize(self._token, id(self), self._capability, size_bytes)
            self._reserved = size_bytes

    def release(self) -> None:
        """Release resources owned by this process cross memory reservation."""
        if os.getpid() != self._pid:
            return
        with self._lock:
            if self._released:
                ticket = self._finalizer_ticket
                if ticket >= 0:
                    if not self._coordinator.release_finalizer_ticket(
                        ticket, owner=self._finalizer_owner
                    ):
                        raise RuntimeError(
                            "cross-process memory finalizer acknowledgement did not commit"
                        )
                    self._finalizer_ticket = -1
                return
            # Explicit close may perform the final coalesced downward commit.
            self._coordinator.release(self._token, id(self), self._capability)
            self._finalizer_owner._primary_released = True
            # Primary authority is irreversibly gone at this point. Publish that
            # fact locally before attempting any secondary escrow retirement.
            self._reserved = 0
            self._released = True
            ticket = self._finalizer_ticket
            if ticket >= 0:
                if not self._coordinator.release_finalizer_ticket(
                    ticket, owner=self._finalizer_owner
                ):
                    raise RuntimeError(
                        "cross-process memory finalizer acknowledgement did not commit"
                    )
                self._finalizer_ticket = -1

    close = release

    def _release_nonblocking(self) -> None:
        """Publish nonblocking release of this cross-process memory lease."""
        if os.getpid() != self._pid:
            return
        # Never wait for a reservation lock from ``__del__``.  If an explicit
        # release somehow owns it concurrently, leaving the contribution
        # charged is safer than publishing into a ticket that may be recycled.
        if not self._lock.acquire(blocking=False):
            self._coordinator.record_finalizer_overflow()
            return
        try:
            if self._released:
                ticket = self._finalizer_ticket
                if ticket >= 0 and self._coordinator.defer_finalizer_ack(
                    ticket, owner=self._finalizer_owner
                ):
                    self._finalizer_ticket = -1
                return
            ticket = self._finalizer_ticket
            if ticket < 0:
                ticket = self._finalizer_owner._finalizer_ticket
            if not self._coordinator.defer_release(self._finalizer_owner, ticket=ticket):
                # Fail closed: contribution and ticket remain owned forever and
                # shutdown observes the irreversible overflow counter.
                return
            self._finalizer_ticket = -1
            self._reserved = 0
            self._released = True
        finally:
            self._lock.release()

    def __del__(self) -> None:
        """Schedule best-effort cleanup during garbage collection."""
        try:
            if runtime_is_finalizing():
                return
            self._release_nonblocking()
        except BaseException:
            pass


class _ProcessCrossMemoryCoordinator:
    """Single process-scoped physical reservation with coalesced shrink I/O."""

    def __init__(self, capacity_bytes: int) -> None:
        """Initialize the process cross memory coordinator and its owned runtime state."""
        self._pid = os.getpid()
        self._capacity = capacity_bytes
        self._lock = Lock()
        self._physical = CrossProcessMemoryLease(capacity_bytes, 0)
        self._coordination_signature = (
            self._physical._coordinated,
            str(self._physical._coordination_path)
            if self._physical._coordination_path is not None
            else "",
        )
        self._physical_bytes = 0
        self._contributions: dict[int, int] = {}
        self._contribution_owners: dict[int, tuple[int, object]] = {}
        # Each live generation retains the safety ceiling observed by the
        # operation that owns it.  Effective process admission is the minimum
        # of live owners, so releasing a transient restrictive owner can safely
        # restore capacity without waiting for unrelated metadata to disappear.
        self._contribution_capacities: dict[int, int] = {}
        self._unknown_releases = 0
        self._generation_pool = BoundedGenerationPool(_MAX_FINALIZER_RELEASE_TOKENS)
        self._pending_shrink = False
        self._reconcile_scheduled = False
        self._shrink_failures = 0
        self._reconcile_failures = 0
        # Finalizers publish only integer tokens here. The bounded queue has
        # no dependency on the coordinator lock or coordination-file I/O; a
        # full queue fails closed and is surfaced by shutdown observability.
        self._finalizer_releases: ReservedFinalizerEscrow[_ProcessCrossMemoryFinalizerOwner] = (
            ReservedFinalizerEscrow(
                _MAX_FINALIZER_RELEASE_TOKENS,
                static_kind="cross_process_memory_coordinator",
            )
        )

    def _environment_compatible(self) -> bool:
        """Return whether the singleton still targets the same process/domain."""
        enabled = _enabled()
        signature = (enabled, str(_coordination_path()) if enabled else "")
        return self._pid == os.getpid() and self._coordination_signature == signature

    def _effective_capacity_locked(self) -> int:
        """Return effective cross-process capacity while holding the coordinator lock."""
        if self._contribution_capacities:
            return min(self._contribution_capacities.values())
        return self._capacity

    def _refresh_effective_capacity_locked(self) -> None:
        """Refresh effective cross-process capacity while holding the coordinator lock."""
        effective = self._effective_capacity_locked()
        self._physical._set_capacity(effective)

    def _logical_total_locked(self) -> int:
        """Return logical reservations while holding the coordinator lock."""
        return sum(self._contributions.values())

    def _drain_finalizer_releases_locked(self) -> None:
        """Drain finalizer releases while holding the governing lock."""
        removed = False

        def process(_ticket: int, owner: _ProcessCrossMemoryFinalizerOwner) -> None:
            """Process one retained work item."""
            nonlocal removed
            if owner._primary_released:
                return
            owner_token = owner._token
            capability = owner._capability
            owner_id = owner._owner_id
            expected = self._contribution_owners.get(owner_token)
            if expected is None or expected[0] != owner_id or expected[1] is not capability:
                self._unknown_releases += 1
                raise _RetryFinalizerDrain("finalizer capability mismatch")
            if owner_token not in self._contributions:
                self._unknown_releases += 1
                raise _RetryFinalizerDrain("finalizer contribution missing")
            if not self._generation_pool.release_for(owner):
                raise _RetryFinalizerDrain("finalizer generation retirement failed")
            self._contributions.pop(owner_token, None)
            self._contribution_owners.pop(owner_token, None)
            self._contribution_capacities.pop(owner_token, None)
            removed = True

        while True:
            try:
                if not self._finalizer_releases.process_one(process):
                    break
            except _RetryFinalizerDrain:
                break
        if removed:
            self._refresh_effective_capacity_locked()
            self._pending_shrink = True

    def record_finalizer_overflow(self) -> None:
        """Record irreversible inability to complete a guaranteed handoff."""
        global _PROCESS_FINALIZER_RELEASE_OVERFLOWS, _PROCESS_FINALIZER_RELEASE_OVERFLOWED
        _PROCESS_FINALIZER_RELEASE_OVERFLOWED = True
        try:
            _PROCESS_FINALIZER_RELEASE_OVERFLOWS += 1
        except MemoryError:
            pass

    def defer_release(
        self,
        owner: _ProcessCrossMemoryFinalizerOwner,
        *,
        ticket: int,
    ) -> bool:
        """Publish a finalizer owner without acquiring the coordinator lock.

        The reservation object is published into its exclusive pre-reserved
        slot and authenticated while draining under the coordinator lock.
        """
        if ticket < 0:
            self.record_finalizer_overflow()
            return False
        try:
            accepted = self._finalizer_releases.publish_rooted(ticket, owner)
        except BaseException:
            accepted = False
        if not accepted:
            self.record_finalizer_overflow()
        return accepted

    def defer_finalizer_ack(self, ticket: int, *, owner: _ProcessCrossMemoryFinalizerOwner) -> bool:
        """Publish an ACK-only owner for an already-released contribution."""
        if ticket < 0:
            return True
        try:
            accepted = bool(self._finalizer_releases.publish_rooted(ticket, owner))
        except BaseException:
            accepted = False
        if not accepted:
            self.record_finalizer_overflow()
        return accepted

    def release_finalizer_ticket(
        self, ticket: int, *, owner: _ProcessCrossMemoryFinalizerOwner
    ) -> bool:
        """Retire or safely transfer an ACK-only reserved generation.

        A failed ``release_ticket`` must never destroy the caller's only proof
        of the generation.  If retirement does not commit, publish a preallocated
        ACK marker into that exact ticket so the coordinator can recycle it at
        the next safe point without touching contribution accounting.
        """
        if ticket < 0:
            return True
        # This API is only valid after primary contribution ownership is gone.
        # Commit ACK semantics before any fallible secondary retirement so a
        # fallback publication can never replay contribution release.
        owner._primary_released = True
        try:
            if self._finalizer_releases.release_ticket(ticket):
                return True
        except BaseException:
            pass
        return self.defer_finalizer_ack(ticket, owner=owner)

    def acquire(
        self, initial_bytes: int, capacity_bytes: int | None = None
    ) -> _ProcessCrossMemoryReservation:
        """Acquire governed capacity through this process cross memory coordinator."""
        owner_capacity = self._capacity if capacity_bytes is None else capacity_bytes
        if type(owner_capacity) is not int or owner_capacity <= 0:
            raise ValueError("cross-process memory owner capacity must be positive")
        with self._lock:
            previous_physical = self._physical_bytes
            saw_ticket_capacity = False
            reservation: _ProcessCrossMemoryReservation | None = None
            ticket: int | None = None
            token: int | None = None

            # Owner-first admission: the reservation/finalizer owner exists
            # before either escrow or generation capacity commits. Losing an
            # integer return can therefore be recovered by exact owner identity.
            for _attempt in range(2):
                self._drain_finalizer_releases_locked()
                capability = object()
                candidate = _ProcessCrossMemoryReservation(self, 0, capability, initial_bytes, 0)
                try:
                    ticket = self._finalizer_releases.reserve_rooted(candidate._finalizer_owner)
                    if ticket is None:
                        continue
                    saw_ticket_capacity = True
                    candidate._finalizer_ticket = ticket
                    candidate._finalizer_owner._finalizer_ticket = ticket
                    token = self._generation_pool.acquire_for(candidate._finalizer_owner)
                    if token is None:
                        self._finalizer_releases.release_rooted_owner(candidate._finalizer_owner)
                        ticket = None
                        continue
                    candidate._bind_generation(token, ticket)
                    reservation = candidate
                    break
                except BaseException:
                    try:
                        self._generation_pool.release_for(candidate._finalizer_owner)
                    except BaseException:
                        pass
                    try:
                        self._finalizer_releases.release_rooted_owner(candidate._finalizer_owner)
                    except BaseException:
                        pass
                    raise

            if reservation is None or token is None or ticket is None:
                if saw_ticket_capacity:
                    raise RuntimeError(
                        "cross-process memory contribution generation capacity exhausted"
                    )
                raise RuntimeError("cross-process memory finalizer escrow capacity exhausted")

            published_contribution = False
            published_owner = False
            try:
                capability = reservation._capability
                self._contributions[token] = initial_bytes
                published_contribution = True
                self._contribution_owners[token] = (id(reservation), capability)
                published_owner = True
                self._contribution_capacities[token] = owner_capacity
                self._refresh_effective_capacity_locked()

                logical = self._logical_total_locked()
                effective_capacity = self._effective_capacity_locked()
                if logical > effective_capacity:
                    from ..errors import SchemaSanitizerResourceError

                    raise SchemaSanitizerResourceError(
                        "cross-process resident-memory live-owner ceiling exhausted",
                        detail={
                            "stage": "cross_process_memory",
                            "limit_name": "cross_process_resident_memory_bytes",
                            "limit_bytes": effective_capacity,
                            "actual_bytes": logical,
                        },
                    )
                target = _round_reservation(logical)
                if target > previous_physical:
                    self._physical.resize(target)
                    self._physical_bytes = target
                return reservation
            except BaseException as primary:
                if published_owner:
                    self._contribution_owners.pop(token, None)
                    self._contribution_capacities.pop(token, None)
                    self._refresh_effective_capacity_locked()
                if published_contribution:
                    self._contributions.pop(token, None)
                if self._physical_bytes != previous_physical:
                    try:
                        self._physical.resize(previous_physical)
                        self._physical_bytes = previous_physical
                    except BaseException as cleanup_error:
                        from .safe_errors import add_bounded_note

                        add_bounded_note(
                            primary,
                            "cross-process memory acquisition rollback also failed",
                            cleanup_error,
                        )
                try:
                    self._generation_pool.release_for(reservation._finalizer_owner)
                except BaseException as cleanup_error:
                    from .safe_errors import add_bounded_note

                    add_bounded_note(
                        primary,
                        "cross-process memory generation rollback did not commit",
                        cleanup_error,
                    )
                reservation._finalizer_owner._primary_released = True
                reservation._reserved = 0
                reservation._released = True
                if self.release_finalizer_ticket(ticket, owner=reservation._finalizer_owner):
                    reservation._finalizer_ticket = -1
                raise

    def resize(self, token: int, owner_id: int, capability: object, requested: int) -> None:
        """Resize the retained reservation."""
        with self._lock:
            self._drain_finalizer_releases_locked()
            expected = self._contribution_owners.get(token)
            if expected is None or expected[0] != owner_id or expected[1] is not capability:
                self._unknown_releases += 1
                raise RuntimeError("cross-process memory reservation is not authoritative")
            current = self._contributions[token]
            logical = self._logical_total_locked() - current + requested
            effective_capacity = self._effective_capacity_locked()
            if logical > effective_capacity:
                from ..errors import SchemaSanitizerResourceError

                raise SchemaSanitizerResourceError(
                    "cross-process resident-memory live-owner ceiling exhausted",
                    detail={
                        "stage": "cross_process_memory",
                        "limit_name": "cross_process_resident_memory_bytes",
                        "limit_bytes": effective_capacity,
                        "actual_bytes": logical,
                    },
                )
            target = _round_reservation(logical)
            if target > self._physical_bytes:
                self._physical.resize(target)
                self._physical_bytes = target
                self._pending_shrink = False
            self._contributions[token] = requested
            if target < self._physical_bytes:
                self._pending_shrink = True
                if self._physical_bytes - target >= _COORDINATOR_SHRINK_HYSTERESIS_BYTES:
                    self._schedule_reconcile_locked(start_worker=True)

    def release(
        self,
        token: int,
        owner_id: int,
        capability: object,
    ) -> None:
        """Release resources owned by this process cross memory coordinator."""
        with self._lock:
            self._drain_finalizer_releases_locked()
            expected = self._contribution_owners.get(token)
            if expected is None or expected[0] != owner_id or expected[1] is not capability:
                self._unknown_releases += 1
                raise RuntimeError("cross-process memory reservation is not authoritative")
            generation_owner = self._generation_pool.owner_for(token)
            if generation_owner is not None:
                generation_released = self._generation_pool.release_for(generation_owner)
            else:
                # Exact generation retirement may have committed before an
                # asynchronous exception prevented contribution-map cleanup.
                # Under this coordinator lock, an authenticated contribution
                # with no live generation owner is therefore a retryable
                # post-commit state, not grounds to resurrect/re-release token.
                generation_released = True
            if not generation_released:
                raise RuntimeError("cross-process memory generation retirement did not commit")
            self._contributions.pop(token, None)
            self._contribution_owners.pop(token, None)
            self._contribution_capacities.pop(token, None)
            self._refresh_effective_capacity_locked()
            self._pending_shrink = True
            target = _round_reservation(self._logical_total_locked())
            try:
                if target != self._physical_bytes:
                    self._physical.resize(target)
                    self._physical_bytes = target
                self._pending_shrink = False
            except BaseException:
                # Logical release already committed above. A failed downward
                # physical resize is conservative over-reservation, not failed
                # ownership release. Never let the reservation object/finalizer
                # retain a stale generation after this commit.
                self._shrink_failures += 1
                self._schedule_reconcile_locked(start_worker=True)
                return

    def _schedule_reconcile_locked(self, *, start_worker: bool) -> None:
        """Schedule reconcile while holding the governing lock."""
        if self._reconcile_scheduled or not self._pending_shrink:
            return
        self._reconcile_scheduled = True
        try:
            from .cleanup_dispatcher import CleanupSubsystem, dispatch_cleanup

            accepted = dispatch_cleanup(
                _reconcile_process_cross_memory,
                retained_bytes=512,
                start_worker=start_worker,
                subsystem=CleanupSubsystem.MEMORY,
            )
        except BaseException:
            accepted = False
        if not accepted:
            self._reconcile_scheduled = False

    def reconcile_pending(self) -> None:
        """Reconcile deferred cross-process memory journal updates."""
        with self._lock:
            self._drain_finalizer_releases_locked()
            self._reconcile_scheduled = False
            if not self._pending_shrink:
                return
            target = _round_reservation(self._logical_total_locked())
            if target == self._physical_bytes:
                self._pending_shrink = False
                return
            # This executes on the cleanup host or an explicit diagnostics path.
            try:
                self._physical.resize(target)
            except BaseException:
                # Keep the conservative physical reservation and the logical
                # pending bit observable. The next release/acquire/shutdown
                # reconciliation may retry; ownership itself is already exact.
                self._reconcile_failures += 1
                raise
            self._physical_bytes = target
            self._pending_shrink = False

    def rebase_empty_capacity(self, capacity_bytes: int) -> bool:
        """Adopt a new logical baseline while retaining conservative physical debt.

        A failed downward reconciliation must not make future operations depend
        on cleanup availability.  With no live logical owners it is safe to
        reuse the existing (possibly over-reserved) physical lease and refresh
        only the admission ceiling for the next generation.
        """
        if type(capacity_bytes) is not int or capacity_bytes <= 0:
            raise ValueError("cross-process memory capacity must be positive")
        with self._lock:
            self._drain_finalizer_releases_locked()
            if self._contributions:
                return False
            self._capacity = capacity_bytes
            self._refresh_effective_capacity_locked()
            if self._physical_bytes:
                self._pending_shrink = True
                self._schedule_reconcile_locked(start_worker=True)
            return True

    def empty(self) -> bool:
        """Return whether the governed state is empty."""
        with self._lock:
            self._drain_finalizer_releases_locked()
            return not self._contributions


def _reconcile_process_cross_memory() -> None:
    """Reconcile the singleton coordinator without retaining it in a callback."""
    coordinator = _PROCESS_COORDINATOR
    if coordinator is not None:
        coordinator.reconcile_pending()


def _get_process_coordinator(capacity_bytes: int) -> _ProcessCrossMemoryCoordinator:
    """Return the process-wide cross-memory coordinator."""
    global _PROCESS_COORDINATOR
    with _PROCESS_COORDINATOR_LOCK:
        coordinator = _PROCESS_COORDINATOR
        if coordinator is None:
            coordinator = _ProcessCrossMemoryCoordinator(capacity_bytes)
            _PROCESS_COORDINATOR = coordinator
            return coordinator
        if coordinator._environment_compatible():
            if not coordinator.empty():
                return coordinator
            # Prefer a clean replacement, but never make new admission depend
            # on a conservative shrink succeeding. If reconciliation fails,
            # keep the exact existing physical owner and rebase only its empty
            # logical ceiling; over-reservation remains visible/retryable.
            try:
                coordinator.reconcile_pending()
            except BaseException as exc:
                clear_exception_traceback(exc)
                if coordinator.rebase_empty_capacity(capacity_bytes):
                    return coordinator
                raise
            coordinator = _ProcessCrossMemoryCoordinator(capacity_bytes)
            _PROCESS_COORDINATOR = coordinator
            return coordinator
        if not coordinator.empty():
            raise RuntimeError(
                "cross-process memory coordination domain changed while reservations are live"
            )
        coordinator.reconcile_pending()
        coordinator = _ProcessCrossMemoryCoordinator(capacity_bytes)
        _PROCESS_COORDINATOR = coordinator
        return coordinator


def acquire_cross_process_memory(
    capacity_bytes: int, requested_limit: int
) -> _ProcessCrossMemoryReservation:
    """Admit one operation through one process-aggregated physical reservation."""
    drain_direct_cross_process_memory_finalizers()
    if type(capacity_bytes) is not int or type(requested_limit) is not int:
        raise TypeError("cross-process memory sizes must be exact integers")
    if capacity_bytes <= 0 or requested_limit <= 0:
        raise ValueError("cross-process memory sizes must be > 0")
    initial = min(256 << 20, max(8 << 20, requested_limit // 16))
    initial = min(initial, capacity_bytes)
    return _get_process_coordinator(capacity_bytes).acquire(initial, capacity_bytes=capacity_bytes)


def cross_process_memory_finalizer_overflow_count() -> int:
    """Return irreversible loss of bounded finalizer-release publications."""
    return (
        max(1, _PROCESS_FINALIZER_RELEASE_OVERFLOWS)
        if _PROCESS_FINALIZER_RELEASE_OVERFLOWED
        else _PROCESS_FINALIZER_RELEASE_OVERFLOWS
    )


def cross_process_memory_reserved_bytes() -> int:
    """Return live host-wide reservations, pruning crashed owners first."""
    drain_direct_cross_process_memory_finalizers()
    if not _enabled():
        return 0
    coordinator = _PROCESS_COORDINATOR
    if coordinator is not None and coordinator._environment_compatible():
        try:
            coordinator.reconcile_pending()
        except BaseException:
            # Diagnostics must remain conservative; read the persisted state.
            pass
    with _locked_state() as state:
        return sum(_nonnegative_int(item.get("reserved")) for item in _clean_leases(state).values())


def process_cross_memory_snapshot() -> dict[str, int]:
    """Return authoritative process-coordinator ownership for shutdown checks."""
    coordinator = _PROCESS_COORDINATOR
    direct_live_leases, direct_live_bytes, direct_unknown = _direct_lease_snapshot()
    if coordinator is None:
        direct, direct_overflows = direct_cross_process_memory_finalizer_snapshot()
        return {
            "logical_contributions": 0,
            "logical_bytes": 0,
            "physical_bytes": 0,
            "deferred_finalizers": 0,
            "direct_finalizers": direct,
            "direct_live_leases": direct_live_leases,
            "direct_live_bytes": direct_live_bytes,
            "unknown_releases": direct_unknown,
            "pending_shrink": 0,
            "shrink_failures": 0,
            "reconcile_failures": 0,
            "finalizer_overflows": (
                max(1, _PROCESS_FINALIZER_RELEASE_OVERFLOWS)
                if _PROCESS_FINALIZER_RELEASE_OVERFLOWED
                else _PROCESS_FINALIZER_RELEASE_OVERFLOWS
            )
            + direct_overflows,
        }
    with coordinator._lock:
        coordinator._drain_finalizer_releases_locked()
        direct, direct_overflows = direct_cross_process_memory_finalizer_snapshot()
        return {
            "logical_contributions": len(coordinator._contributions),
            "logical_bytes": coordinator._logical_total_locked(),
            "physical_bytes": coordinator._physical_bytes,
            "deferred_finalizers": coordinator._finalizer_releases.published_count(),
            "unknown_releases": coordinator._unknown_releases + direct_unknown,
            "pending_shrink": int(coordinator._pending_shrink),
            "shrink_failures": coordinator._shrink_failures,
            "reconcile_failures": coordinator._reconcile_failures,
            "direct_finalizers": direct,
            "direct_live_leases": direct_live_leases,
            "direct_live_bytes": direct_live_bytes,
            "finalizer_overflows": (
                max(1, _PROCESS_FINALIZER_RELEASE_OVERFLOWS)
                if _PROCESS_FINALIZER_RELEASE_OVERFLOWED
                else _PROCESS_FINALIZER_RELEASE_OVERFLOWS
            )
            + direct_overflows,
        }


def _prepare_stale_scratch_for_fork() -> None:
    """Prepare stale scratch for fork."""
    global _STALE_KEY_SCRATCH_FORK_FRESH_LOCK
    _STALE_KEY_SCRATCH_FORK_FRESH_LOCK = _STALE_KEY_SCRATCH_LOCK_BANK[_STALE_KEY_SCRATCH_BANK_INDEX]


def _clear_stale_scratch_fork() -> None:
    """Clear stale scratch fork."""
    global _STALE_KEY_SCRATCH_FORK_FRESH_LOCK
    _STALE_KEY_SCRATCH_FORK_FRESH_LOCK = None


def _reset_stale_scratch_after_fork() -> None:
    """Reset stale scratch after fork."""
    global \
        _STALE_KEY_SCRATCH_LOCK, \
        _STALE_KEY_SCRATCH_FORK_FRESH_LOCK, \
        _STALE_KEY_SCRATCH_BANK_INDEX
    prepared = _STALE_KEY_SCRATCH_FORK_FRESH_LOCK
    if prepared is None:
        return
    quarantine_inherited_state("cross-process-memory-scratch", _STALE_KEY_SCRATCH_LOCK)
    _STALE_KEY_SCRATCH_LOCK = prepared
    _STALE_KEY_SCRATCH_FORK_FRESH_LOCK = None
    _STALE_KEY_SCRATCH_BANK_INDEX = 1 - _STALE_KEY_SCRATCH_BANK_INDEX


def _reset_cross_process_memory_after_fork() -> None:
    """Drop inherited process-scoped coordination without touching parent locks."""
    global _PROCESS_COORDINATOR_LOCK, _PROCESS_COORDINATOR
    global _PROCESS_FINALIZER_RELEASE_OVERFLOWS, _PROCESS_FINALIZER_RELEASE_OVERFLOWED
    global _DIRECT_CROSS_MEMORY_FINALIZER_OVERFLOWS
    global \
        _DIRECT_LEASE_LOCK, \
        _DIRECT_LEASE_LEDGER, \
        _DIRECT_LEASE_FREE, \
        _DIRECT_LEASE_FREE_COUNT, \
        _DIRECT_LEASE_GENERATIONS
    global _DIRECT_LEASE_UNKNOWN_RELEASES
    # Rebind before dropping inherited owners. Their finalizers observe a PID
    # mismatch and therefore cannot mutate the child's new coordination state.
    _PROCESS_COORDINATOR_LOCK = Lock()
    _DIRECT_LEASE_LOCK = Lock()
    _DIRECT_LEASE_LEDGER = {}
    _DIRECT_LEASE_FREE = list(range(1, _MAX_DIRECT_LEASES + 1))
    _DIRECT_LEASE_FREE_COUNT = _MAX_DIRECT_LEASES
    _DIRECT_LEASE_GENERATIONS = [0] * (_MAX_DIRECT_LEASES + 1)
    _DIRECT_LEASE_UNKNOWN_RELEASES = 0
    _PROCESS_COORDINATOR = None
    _DIRECT_CROSS_MEMORY_FINALIZER_ESCROW.reset_after_fork()
    _DIRECT_CROSS_MEMORY_FINALIZER_OVERFLOWS = 0
    _PROCESS_FINALIZER_RELEASE_OVERFLOWS = 0
    _PROCESS_FINALIZER_RELEASE_OVERFLOWED = False


from .fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler(
    "cross-process-memory-scratch",
    before=_prepare_stale_scratch_for_fork,
    after_in_parent=_clear_stale_scratch_fork,
    after_in_child=_reset_stale_scratch_after_fork,
)
_register_fork_handler("cross-process-memory", mode="quarantine_only")


from .finalizer_registry import (  # noqa: E402
    register_finalizer_domain as _register_finalizer_domain,
)

_register_finalizer_domain(
    "direct_cross_process_memory",
    drain=drain_direct_cross_process_memory_finalizers,
    snapshot=direct_cross_process_memory_finalizer_snapshot,
    escrows=(("direct_cross_process_memory", _DIRECT_CROSS_MEMORY_FINALIZER_ESCROW),),
)


from .shutdown_observers import (  # noqa: E402
    register_shutdown_observer as _register_shutdown_observer,
)

_register_shutdown_observer("cross_process_memory", process_cross_memory_snapshot)


__all__ = [
    "CrossProcessMemoryLease",
    "acquire_cross_process_memory",
    "cross_process_memory_finalizer_overflow_count",
    "cross_process_memory_reserved_bytes",
    "direct_cross_process_memory_finalizer_snapshot",
    "drain_direct_cross_process_memory_finalizers",
    "process_cross_memory_snapshot",
]
