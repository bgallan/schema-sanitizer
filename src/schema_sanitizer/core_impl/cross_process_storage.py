"""Optional host-wide temporary-storage accounting across worker processes."""

from __future__ import annotations

import json
import os
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, field
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
from .fork_safety import quarantine_inherited_state
from .process_identity import process_is_alive, process_start_token

try:  # pragma: no cover - exercised on POSIX CI
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback
    fcntl = None  # type: ignore[assignment]

_ENV_ENABLED = "SCHEMA_SANITIZER_CROSS_PROCESS_TEMP_RESERVATIONS"
_ENV_DIRECTORY = "SCHEMA_SANITIZER_COORDINATION_DIR"
_MAX_STATE_BYTES = 1 << 20
_MAX_PROCESS_RECORDS = 4096
_MAX_LIVENESS_CHECKS_PER_TRANSACTION = 256
_STALE_KEY_SCRATCH: list[str | None] = [None] * _MAX_PROCESS_RECORDS
_STALE_KEY_SCRATCH_LOCK = Lock()
_STALE_KEY_SCRATCH_LOCK_BANK = (Lock(), Lock())
_STALE_KEY_SCRATCH_BANK_INDEX = 0
_STALE_KEY_SCRATCH_FORK_FRESH_LOCK: Lock | None = None
from .static_control_plane import (  # noqa: E402
    register_static_control_plane as _register_static_control_plane,
)

_register_static_control_plane(
    "cross_process_storage_stale_scratch", _MAX_PROCESS_RECORDS * 8 + 4096
)
# stale_keys are stored in this fixed scratch buffer; no per-prune list allocation.
_MAX_LOCAL_ACCOUNTS = 4096


@dataclass(slots=True)
class CrossProcessStorageAccount:
    """Exact process-local capability for one device's host-wide contribution."""

    device: int
    capability: object
    pid: int
    reserved_bytes: int = 0
    reserved_inodes: int = 0
    closed: bool = False
    token: int = 0
    reconciliation_pending: bool = False
    reconciliation_failures: int = 0
    lock: Lock = field(default_factory=Lock, repr=False, compare=False)


_ACCOUNT_LOCK = Lock()
_ACCOUNT_SEQUENCE = 0
_ACCOUNTS: dict[int, tuple[CrossProcessStorageAccount, object, int]] = {}
_ACCOUNT_FORK_BANKS: tuple[
    tuple[Lock, dict[int, tuple[CrossProcessStorageAccount, object, int]]], ...
] = ((Lock(), {}), (Lock(), {}))
_ACCOUNT_FORK_BANK_INDEX = 0
_ACCOUNT_FORK_FRESH_LOCK: Lock | None = None
_ACCOUNT_FORK_FRESH_ACCOUNTS: dict[int, tuple[CrossProcessStorageAccount, object, int]] | None = (
    None
)


def open_cross_process_storage_account(device: int) -> CrossProcessStorageAccount:
    """Create one bounded exact capability before host-wide publication."""
    global _ACCOUNT_SEQUENCE
    if type(device) is not int:
        raise TypeError("cross-process storage device must be an exact integer")
    account = CrossProcessStorageAccount(device, object(), os.getpid())
    with _ACCOUNT_LOCK:
        if len(_ACCOUNTS) >= _MAX_LOCAL_ACCOUNTS:
            raise RuntimeError("cross-process storage account registry exhausted")
        # Tokens are intentionally bounded and reusable. Authority is the token
        # plus the unforgeable object capability, so reuse cannot introduce ABA.
        token = _ACCOUNT_SEQUENCE
        for _ in range(_MAX_LOCAL_ACCOUNTS):
            token += 1
            if token > _MAX_LOCAL_ACCOUNTS:
                token = 1
            if token not in _ACCOUNTS:
                break
        else:  # defensive; the length guard above should make this unreachable.
            raise RuntimeError("cross-process storage account registry exhausted")
        _ACCOUNTS[token] = (account, account.capability, device)
        _ACCOUNT_SEQUENCE = token
    account.token = token
    return account


def cross_process_storage_enabled() -> bool:
    """Return whether cross-process coordination is explicitly enabled."""
    value = os.getenv(_ENV_ENABLED, "").strip().lower()
    return fcntl is not None and value in {"1", "true", "yes", "on"}


def cross_process_storage_directory() -> Path:
    """Return the shared host directory used for reservation state."""
    configured = os.getenv(_ENV_DIRECTORY)
    path = Path(configured) if configured else Path(tempfile.gettempdir())
    path.mkdir(parents=True, exist_ok=True)
    return path


def _nonnegative_int(value: object) -> int:
    """Return a non-negative JSON integer, rejecting every other type."""
    return max(0, value) if type(value) is int else 0


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
    if set(decoded) != {"version", "processes"}:
        raise OSError("cross-process temporary-storage state has unknown or missing fields")
    version = decoded["version"]
    if type(version) is not int or version != 1:
        raise OSError(f"unsupported cross-process temporary-storage state version: {version!r}")
    processes = decoded["processes"]
    if not isinstance(processes, dict):
        raise OSError("cross-process temporary-storage processes must be an object")
    return {"version": 1, "processes": processes}


def _encode_state(state: object) -> bytes:
    """Return the canonical coordination-state representation."""
    return json.dumps(state, sort_keys=True, separators=(",", ":")).encode()


@contextmanager
def _locked_state(
    device: int,
    directory: Path | None = None,
) -> Iterator[tuple[object, dict[str, object]]]:
    """Lock and transactionally update one device reservation document."""
    directory = cross_process_storage_directory() if directory is None else directory
    path = directory / f"schema-sanitizer-temp-{device}.json"
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
            commit_owner_delta = False
            try:
                yield handle, state
                commit_owner_delta = True
            finally:
                # Unexpected body failures never publish a partial owner delta.
                # Stale-owner housekeeping is conservative and may wait until
                # the next successful transaction rather than commit alongside
                # a failed reservation/release.
                if commit_owner_delta:
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
                            process_start=process_start_token(os.getpid()),
                        )


def _clean_processes(state: dict[str, object]) -> dict[str, dict[str, object]]:
    """Prune dead owners in one O(n) scan using preallocated scratch."""
    raw = state.get("processes")
    if not isinstance(raw, dict):
        raise OSError("cross-process temporary-storage processes must be an object")
    processes: dict[str, dict[str, object]] = raw
    if len(processes) > _MAX_PROCESS_RECORDS:
        raise OSError("cross-process temporary-storage process registry exceeds its bound")
    with _STALE_KEY_SCRATCH_LOCK:
        stale_count = 0
        liveness_checks = 0
        for key, value in processes.items():
            if type(key) is not str or not isinstance(value, dict):
                raise OSError(f"invalid temporary-storage process entry: {key!r}")
            pid = value.get("pid", -1)
            token = value.get("start", "unknown")
            reserved = value.get("reserved", 0)
            inodes = value.get("inodes", 0)
            updated = value.get("updated", 0.0)
            if (
                type(pid) is not int
                or type(token) is not str
                or type(reserved) is not int
                or type(inodes) is not int
                or isinstance(updated, bool)
                or not isinstance(updated, (int, float))
            ):
                raise OSError(f"invalid temporary-storage process entry: {key!r}")
            if pid <= 0 or reserved < 0 or inodes < 0:
                raise OSError(f"invalid temporary-storage process entry: {key!r}")
            if key != f"{pid}:{token}":
                raise OSError(f"invalid temporary-storage process identity: {key!r}")
            alive = True
            if reserved or inodes:
                if liveness_checks < _MAX_LIVENESS_CHECKS_PER_TRANSACTION:
                    alive = process_is_alive(pid, token)
                    liveness_checks += 1
            if not (reserved or inodes) or not alive:
                _STALE_KEY_SCRATCH[stale_count] = key
                stale_count += 1
        for index in range(stale_count):
            stale_key = _STALE_KEY_SCRATCH[index]
            if stale_key is not None:
                processes.pop(stale_key, None)
                _STALE_KEY_SCRATCH[index] = None
    return processes


def _reserve_cross_process_raw(
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
    if (
        type(device) is not int
        or type(size_bytes) is not int
        or type(capacity_bytes) is not int
        or type(inode_count) is not int
        or (inode_capacity is not None and type(inode_capacity) is not int)
    ):
        raise TypeError("cross-process storage accounting values must be exact integers")
    if (
        size_bytes < 0
        or capacity_bytes < 0
        or inode_count < 0
        or (inode_capacity is not None and inode_capacity < 0)
    ):
        raise ValueError("cross-process storage accounting values must be >= 0")
    requested = size_bytes
    requested_inodes = inode_count
    coordinated = (
        cross_process_storage_enabled() if enabled is None else bool(enabled and fcntl is not None)
    )
    if not coordinated or (requested == 0 and requested_inodes == 0):
        return 0
    pid = os.getpid()
    start = process_start_token(pid)
    owner = f"{pid}:{start}"
    with _locked_state(device, coordination_directory) as (_handle, state):
        processes = _clean_processes(state)
        total = sum(_nonnegative_int(item.get("reserved")) for item in processes.values())
        next_total = total + requested
        total_inodes = sum(_nonnegative_int(item.get("inodes")) for item in processes.values())
        next_inodes = total_inodes + requested_inodes
        if next_total > capacity_bytes:
            raise OSError(
                f"cross-process temporary-storage capacity exhausted: "
                f"{next_total} bytes > {capacity_bytes} bytes"
            )
        if inode_capacity is not None and next_inodes > inode_capacity:
            raise OSError(
                f"cross-process temporary inode capacity exhausted: "
                f"{next_inodes} inodes > {inode_capacity} inodes"
            )
        if owner not in processes and len(processes) >= _MAX_PROCESS_RECORDS:
            raise OSError("cross-process temporary-storage process registry is full")
        current = processes.get(owner, {"pid": pid, "start": start, "reserved": 0, "inodes": 0})
        processes[owner] = {
            "pid": pid,
            "start": start,
            "reserved": _nonnegative_int(current.get("reserved")) + requested,
            "inodes": _nonnegative_int(current.get("inodes")) + requested_inodes,
            "updated": time(),
        }
        return next_total


def _release_cross_process_raw(
    device: int,
    size_bytes: int,
    *,
    inode_count: int = 0,
    enabled: bool | None = None,
    coordination_directory: Path | None = None,
) -> int:
    """Release this process host-wide bytes and return the remaining total."""
    if type(device) is not int or type(size_bytes) is not int or type(inode_count) is not int:
        raise TypeError("cross-process storage release values must be exact integers")
    if size_bytes < 0 or inode_count < 0:
        raise ValueError("cross-process storage release values must be >= 0")
    amount = size_bytes
    amount_inodes = inode_count
    coordinated = (
        cross_process_storage_enabled() if enabled is None else bool(enabled and fcntl is not None)
    )
    if not coordinated or (amount == 0 and amount_inodes == 0):
        return 0
    pid = os.getpid()
    start = process_start_token(pid)
    owner = f"{pid}:{start}"
    with _locked_state(device, coordination_directory) as (_handle, state):
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


def _reconcile_cross_process_account_raw(
    device: int,
    target_bytes: int,
    target_inodes: int,
    *,
    enabled: bool | None = None,
    coordination_directory: Path | None = None,
) -> int:
    """Set this process contribution down to its exact local authority.

    Reconciliation is cleanup-only: it may reduce a stale host record but never
    grow one.  This makes ambiguous interruption windows conservatively
    overcharged instead of manufacturing shared headroom.
    """
    coordinated = (
        cross_process_storage_enabled() if enabled is None else bool(enabled and fcntl is not None)
    )
    if not coordinated:
        return 0
    pid = os.getpid()
    start = process_start_token(pid)
    owner = f"{pid}:{start}"
    with _locked_state(device, coordination_directory) as (_handle, state):
        processes = _clean_processes(state)
        current = processes.get(owner)
        current_bytes = 0 if current is None else _nonnegative_int(current.get("reserved"))
        current_inodes = 0 if current is None else _nonnegative_int(current.get("inodes"))
        if target_bytes > current_bytes or target_inodes > current_inodes:
            raise RuntimeError(
                "cross-process storage reconciliation would grow stale host authority"
            )
        if target_bytes or target_inodes:
            processes[owner] = {
                "pid": pid,
                "start": start,
                "reserved": target_bytes,
                "inodes": target_inodes,
                "updated": time(),
            }
        else:
            processes.pop(owner, None)
        return sum(_nonnegative_int(item.get("reserved")) for item in processes.values())


def _authenticate_account_registry_unlocked(account: CrossProcessStorageAccount) -> None:
    """Authenticate against the bounded registry while ``_ACCOUNT_LOCK`` is held."""
    entry = _ACCOUNTS.get(account.token)
    if (
        entry is None
        or entry[0] is not account
        or entry[1] is not account.capability
        or entry[2] != account.device
    ):
        raise RuntimeError("cross-process storage account is not authoritative")


def _device_authority_unlocked(device: int) -> tuple[int, int]:
    """Return exact local authority for a device while ``_ACCOUNT_LOCK`` is held."""
    total_bytes = 0
    total_inodes = 0
    for registered, capability, registered_device in _ACCOUNTS.values():
        if (
            registered_device != device
            or registered.closed
            or registered.pid != os.getpid()
            or registered.capability is not capability
        ):
            continue
        total_bytes += registered.reserved_bytes
        total_inodes += registered.reserved_inodes
    return total_bytes, total_inodes


def _reconcile_account_locked(
    account: CrossProcessStorageAccount,
    *,
    enabled: bool | None,
    coordination_directory: Path | None,
) -> None:
    if not account.reconciliation_pending:
        return
    # The host journal aggregates by process+device, not by local account. Keep
    # the registry lock across the rare recovery I/O so another same-device
    # account cannot move exact authority between target calculation and commit.
    with _ACCOUNT_LOCK:
        _authenticate_account_registry_unlocked(account)
        target_bytes, target_inodes = _device_authority_unlocked(account.device)
        try:
            _reconcile_cross_process_account_raw(
                account.device,
                target_bytes,
                target_inodes,
                enabled=enabled,
                coordination_directory=coordination_directory,
            )
        except BaseException:
            account.reconciliation_failures += 1
            raise
        account.reconciliation_pending = False


def _authenticate_account_locked(account: CrossProcessStorageAccount) -> None:
    """Authenticate while the caller owns ``account.lock``."""
    if type(account) is not CrossProcessStorageAccount:
        raise TypeError("cross-process storage account must be exact")
    if account.pid != os.getpid() or account.closed or account.token <= 0:
        raise RuntimeError("cross-process storage account is not active")
    with _ACCOUNT_LOCK:
        _authenticate_account_registry_unlocked(account)


def reserve_cross_process_account(
    account: CrossProcessStorageAccount,
    size_bytes: int,
    capacity_bytes: int,
    *,
    inode_count: int = 0,
    inode_capacity: int | None = None,
    enabled: bool | None = None,
    coordination_directory: Path | None = None,
) -> int:
    """Grow an authenticated process contribution; bytes alone are not authority."""
    if type(account) is not CrossProcessStorageAccount:
        raise TypeError("cross-process storage account must be exact")
    if account.pid != os.getpid():
        raise RuntimeError("cross-process storage account cannot be reused after fork")
    if type(size_bytes) is not int or type(inode_count) is not int:
        raise TypeError("cross-process storage reservation values must be exact integers")
    if size_bytes < 0 or inode_count < 0:
        raise ValueError("cross-process storage reservation values must be >= 0")
    with account.lock:
        _authenticate_account_locked(account)
        _reconcile_account_locked(
            account, enabled=enabled, coordination_directory=coordination_directory
        )
        # Growth commits host-first. If interruption lands after host publication
        # but before local authority, mark reconciliation so the next safe point
        # can only shrink the host record back to the exact local total.
        next_bytes = account.reserved_bytes + size_bytes
        next_inodes = account.reserved_inodes + inode_count
        with _ACCOUNT_LOCK:
            _authenticate_account_registry_unlocked(account)
            try:
                total = _reserve_cross_process_raw(
                    account.device,
                    size_bytes,
                    capacity_bytes,
                    inode_count=inode_count,
                    inode_capacity=inode_capacity,
                    enabled=enabled,
                    coordination_directory=coordination_directory,
                )
                account.reserved_bytes = next_bytes
                account.reserved_inodes = next_inodes
                return total
            except BaseException:
                account.reconciliation_pending = True
                raise


def release_cross_process_account(
    account: CrossProcessStorageAccount,
    size_bytes: int,
    *,
    inode_count: int = 0,
    enabled: bool | None = None,
    coordination_directory: Path | None = None,
) -> int:
    """Shrink only bytes owned by the exact authenticated contribution."""
    if type(account) is not CrossProcessStorageAccount:
        raise TypeError("cross-process storage account must be exact")
    if account.pid != os.getpid():
        raise RuntimeError("cross-process storage account cannot be reused after fork")
    if type(size_bytes) is not int or type(inode_count) is not int:
        raise TypeError("cross-process storage release values must be exact integers")
    if size_bytes < 0 or inode_count < 0:
        raise ValueError("cross-process storage release values must be >= 0")
    with account.lock:
        _authenticate_account_locked(account)
        _reconcile_account_locked(
            account, enabled=enabled, coordination_directory=coordination_directory
        )
        if size_bytes > account.reserved_bytes or inode_count > account.reserved_inodes:
            raise RuntimeError("cross-process storage release exceeds authoritative contribution")
        next_bytes = account.reserved_bytes - size_bytes
        next_inodes = account.reserved_inodes - inode_count
        # Local exact authority is the release commit point. Serialize all
        # same-process account transitions because the host journal stores one
        # aggregate process+device record rather than one record per account.
        with _ACCOUNT_LOCK:
            _authenticate_account_registry_unlocked(account)
            account.reserved_bytes = next_bytes
            account.reserved_inodes = next_inodes
            account.reconciliation_pending = True
            try:
                total = _release_cross_process_raw(
                    account.device,
                    size_bytes,
                    inode_count=inode_count,
                    enabled=enabled,
                    coordination_directory=coordination_directory,
                )
            except BaseException:
                account.reconciliation_failures += 1
                # Local exact authority has already committed, so retrying the same
                # amount cannot debit another owner. Preserve cancellation/primary
                # exceptions instead of swallowing them; higher-level exact owners
                # can detect this committed lower-layer state and finish their own
                # commit before propagating the exception.
                raise
            account.reconciliation_pending = False
            return total


def close_cross_process_storage_account(account: CrossProcessStorageAccount) -> None:
    """Retire an empty exact capability, serialized against reserve/release."""
    if type(account) is not CrossProcessStorageAccount:
        raise TypeError("cross-process storage account must be exact")
    if account.pid != os.getpid():
        raise RuntimeError("cross-process storage account cannot be reused after fork")
    with account.lock:
        _authenticate_account_locked(account)
        _reconcile_account_locked(account, enabled=None, coordination_directory=None)
        if account.reserved_bytes or account.reserved_inodes:
            raise RuntimeError(
                "cannot close a cross-process storage account with live contributions"
            )
        with _ACCOUNT_LOCK:
            _authenticate_account_registry_unlocked(account)
            _ACCOUNTS.pop(account.token, None)
        account.closed = True


def cross_process_reserved_bytes(device: int) -> int:
    """Return live host-wide reservations for one filesystem device."""
    if not cross_process_storage_enabled():
        return 0
    with _locked_state(device) as (_handle, state):
        processes = _clean_processes(state)
        return sum(_nonnegative_int(item.get("reserved")) for item in processes.values())


def _prepare_storage_accounts_for_fork() -> None:
    global _ACCOUNT_FORK_FRESH_LOCK, _ACCOUNT_FORK_FRESH_ACCOUNTS
    global _STALE_KEY_SCRATCH_FORK_FRESH_LOCK
    _ACCOUNT_FORK_FRESH_LOCK, _ACCOUNT_FORK_FRESH_ACCOUNTS = _ACCOUNT_FORK_BANKS[
        _ACCOUNT_FORK_BANK_INDEX
    ]
    _STALE_KEY_SCRATCH_FORK_FRESH_LOCK = _STALE_KEY_SCRATCH_LOCK_BANK[_STALE_KEY_SCRATCH_BANK_INDEX]


def _clear_storage_account_fork_preparation() -> None:
    global _ACCOUNT_FORK_FRESH_LOCK, _ACCOUNT_FORK_FRESH_ACCOUNTS
    global _STALE_KEY_SCRATCH_FORK_FRESH_LOCK
    _ACCOUNT_FORK_FRESH_LOCK = None
    _ACCOUNT_FORK_FRESH_ACCOUNTS = None
    _STALE_KEY_SCRATCH_FORK_FRESH_LOCK = None


def _reset_storage_accounts_after_fork() -> None:
    global _ACCOUNT_LOCK, _ACCOUNT_SEQUENCE, _ACCOUNTS
    global _ACCOUNT_FORK_FRESH_LOCK, _ACCOUNT_FORK_FRESH_ACCOUNTS
    global _STALE_KEY_SCRATCH_LOCK, _STALE_KEY_SCRATCH_FORK_FRESH_LOCK
    global _ACCOUNT_FORK_BANK_INDEX, _STALE_KEY_SCRATCH_BANK_INDEX
    if (
        _ACCOUNT_FORK_FRESH_LOCK is None
        or _ACCOUNT_FORK_FRESH_ACCOUNTS is None
        or _STALE_KEY_SCRATCH_FORK_FRESH_LOCK is None
    ):
        return
    quarantine_inherited_state(
        "cross-process-storage", _ACCOUNT_LOCK, _ACCOUNTS, _STALE_KEY_SCRATCH_LOCK
    )
    _ACCOUNT_LOCK = _ACCOUNT_FORK_FRESH_LOCK
    _STALE_KEY_SCRATCH_LOCK = _STALE_KEY_SCRATCH_FORK_FRESH_LOCK
    _ACCOUNTS = _ACCOUNT_FORK_FRESH_ACCOUNTS
    _ACCOUNT_SEQUENCE = 0
    _ACCOUNT_FORK_FRESH_LOCK = None
    _ACCOUNT_FORK_FRESH_ACCOUNTS = None
    _STALE_KEY_SCRATCH_FORK_FRESH_LOCK = None
    _ACCOUNT_FORK_BANK_INDEX = 1 - _ACCOUNT_FORK_BANK_INDEX
    _STALE_KEY_SCRATCH_BANK_INDEX = 1 - _STALE_KEY_SCRATCH_BANK_INDEX


from .fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler(
    "cross-process-storage",
    before=_prepare_storage_accounts_for_fork,
    after_in_parent=_clear_storage_account_fork_preparation,
    after_in_child=_reset_storage_accounts_after_fork,
)


__all__ = [
    "CrossProcessStorageAccount",
    "close_cross_process_storage_account",
    "cross_process_reserved_bytes",
    "cross_process_storage_directory",
    "cross_process_storage_enabled",
    "open_cross_process_storage_account",
    "release_cross_process_account",
    "reserve_cross_process_account",
]
