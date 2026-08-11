"""Process and optional host-wide temporary filesystem admission."""

from __future__ import annotations

import os
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock

from ..errors import SchemaSanitizerResourceError
from .cross_process_storage import (
    CrossProcessStorageAccount,
    _release_cross_process_raw,
    _reserve_cross_process_raw,
    close_cross_process_storage_account,
    cross_process_storage_directory,
    cross_process_storage_enabled,
    open_cross_process_storage_account,
    release_cross_process_account,
    reserve_cross_process_account,
)
from .fork_safety import quarantine_inherited_state
from .safe_errors import add_bounded_note
from .safety_margins import record_resource_telemetry, tuned_temporary_free_bytes

_MINIMUM_FREE_BYTES = 64 * 1024 * 1024
_CROSS_PROCESS_SHRINK_QUANTUM_BYTES = 4 * 1024 * 1024
_CROSS_PROCESS_SHRINK_QUANTUM_INODES = 64
_MAX_PROCESS_TEMPORARY_STORAGE_CAPABILITIES = 16_384
# Registry nodes are charged by the existing 384-byte per-TemporaryStorageLease
# dynamic control-plane ticket. Keeping the same 16K hard cardinality bound
# prevents an unowned auxiliary index from growing beyond that charged domain.


def _minimum_free_bytes() -> int:
    """Return the bounded telemetry-tuned emergency disk reserve."""
    return tuned_temporary_free_bytes(_MINIMUM_FREE_BYTES)


@dataclass(frozen=True, slots=True)
class ProcessTemporaryStorageSnapshot:
    """Aggregate temporary-storage reservations for one filesystem."""

    capacity_bytes: int
    reserved_bytes: int
    peak_reserved_bytes: int
    capacity_inodes: int
    reserved_inodes: int
    peak_reserved_inodes: int


@dataclass(frozen=True, slots=True)
class ProcessTemporaryStorageAuthoritativeSnapshot:
    """Totals across all live filesystem states, including physical host reservations."""

    states: int
    reserved_bytes: int
    reserved_inodes: int
    cross_reserved_bytes: int
    cross_reserved_inodes: int
    protocol_violations: int = 0


@dataclass(frozen=True, slots=True)
class ProcessTemporaryStorageDiagnostics:
    """Cleanup anomalies observed by the process filesystem governor."""

    over_release_count: int
    over_release_bytes: int
    protocol_violations: int = 0


@dataclass(slots=True)
class _FilesystemReservationState:
    """Mutable process-wide reservation state for one filesystem device."""

    capacity_bytes: int
    capacity_inodes: int
    reserved_bytes: int = 0
    peak_reserved_bytes: int = 0
    reserved_inodes: int = 0
    peak_reserved_inodes: int = 0
    # Compatibility amount-based reservations are isolated from exact
    # capability owners. They remain an authority only for legacy callers and
    # can never retire bytes belonging to a capability.
    legacy_reserved_bytes: int | None = None
    legacy_reserved_inodes: int | None = None
    cross_reserved_bytes: int = 0
    cross_reserved_inodes: int = 0
    cross_process_enabled: bool = False
    coordination_directory: Path | None = None
    cross_account: CrossProcessStorageAccount | None = None
    users: int = 0
    corrupted: bool = False
    lock: object = field(default_factory=Lock, repr=False, compare=False)

    def __post_init__(self) -> None:
        # Older amount-based constructors supplied only aggregate reserved
        # fields. Preserve those bytes as explicit legacy authority rather than
        # letting exact capability reconciliation erase them.
        if self.legacy_reserved_bytes is None:
            self.legacy_reserved_bytes = self.reserved_bytes
        if self.legacy_reserved_inodes is None:
            self.legacy_reserved_inodes = self.reserved_inodes


class ProcessTemporaryStorageCapability:
    """Exact process-wide authority for one temporary-storage reservation.

    Production release/resize paths use this identity instead of a bare
    ``device + amount`` pair, so a stale retry cannot release another owner's
    aggregate bytes after its own commit already succeeded.
    """

    __slots__ = (
        "governor",
        "token",
        "device",
        "reserved_bytes",
        "reserved_inodes",
        "active",
        "inflight",
        "pending_replacement",
        "orphaned",
    )

    def __init__(self, governor: "_ProcessTemporaryStorageGovernor") -> None:
        self.governor = governor
        self.token = 0
        self.device = -1
        self.reserved_bytes = 0
        self.reserved_inodes = 0
        self.active = False
        self.inflight = False
        self.pending_replacement: ProcessTemporaryStorageCapability | None = None
        self.orphaned = False


_FORKED_STORAGE_KEEPALIVE: list[object] = []


class _ProcessTemporaryStorageGovernor:
    """Serialize temporary-space admission across independent operations."""

    def __init__(self) -> None:
        """Create an empty device-indexed reservation registry."""
        self._lock = Lock()
        self._states: dict[int, _FilesystemReservationState] = {}
        self._capabilities: dict[int, ProcessTemporaryStorageCapability] = {}
        self._next_capability = 1
        self._over_release_count = 0
        self._over_release_bytes = 0
        self._protocol_violations = 0
        self._orphaned_capability_hint = False
        self._fork_banks: tuple[
            tuple[
                Lock,
                dict[int, _FilesystemReservationState],
                dict[int, ProcessTemporaryStorageCapability],
            ],
            ...,
        ] = ((Lock(), {}, {}), (Lock(), {}, {}))
        self._fork_bank_index = 0
        self._fork_prepared: (
            tuple[
                Lock,
                dict[int, _FilesystemReservationState],
                dict[int, ProcessTemporaryStorageCapability],
            ]
            | None
        ) = None

    def _borrow_state(
        self,
        device: int,
        *,
        create: _FilesystemReservationState | None = None,
    ) -> _FilesystemReservationState | None:
        """Retain one device state without holding the registry across I/O."""
        with self._lock:
            state = self._states.get(device)
            if state is None and create is not None:
                state = create
                self._states[device] = state
            if state is not None:
                state.users += 1
            return state

    def _return_state(self, device: int, state: _FilesystemReservationState) -> None:
        """Drop one borrowed reference and retire an idle device safely."""
        with self._lock:
            if state.users <= 0:
                # Pin the state rather than allowing a duplicate borrower return
                # to retire the device identity while another authority may live.
                self._protocol_violations += 1
                return
            state.users -= 1
            if (
                state.users == 0
                and state.reserved_bytes == 0
                and state.reserved_inodes == 0
                and state.cross_reserved_bytes == 0
                and state.cross_reserved_inodes == 0
                and self._states.get(device) is state
            ):
                account = state.cross_account
                if account is not None and not account.closed:
                    try:
                        close_cross_process_storage_account(account)
                    except BaseException:
                        # Keep the state rooted if capability retirement cannot commit.
                        return
                self._states.pop(device, None)

    @staticmethod
    def target(path: str | Path | None) -> Path:
        """Return the nearest existing directory used for capacity checks."""
        target = Path(path) if path is not None else Path(tempfile.gettempdir())
        if target.exists() and target.is_file():
            target = target.parent
        while not target.exists() and target != target.parent:
            target = target.parent
        if not target.exists():
            raise OSError(f"unable to locate temporary filesystem for {path!r}")
        return target

    @classmethod
    def filesystem(cls, path: str | Path | None) -> tuple[int, Path, int]:
        """Resolve a device key, existing target, and currently free bytes."""
        target = cls.target(path)
        try:
            device = int(os.stat(target).st_dev)
            free_bytes = int(shutil.disk_usage(target).free)
        except OSError as exc:
            raise OSError(f"unable to inspect temporary filesystem at {target}") from exc
        return device, target, free_bytes

    @staticmethod
    def free_inodes(path: Path) -> int:
        """Return available inodes, or a conservative large fallback."""
        try:
            stats = os.statvfs(path)
            available = int(stats.f_favail)
            return available if available > 0 else 1 << 30
        except (AttributeError, OSError):
            return 1 << 30

    def _reconcile_state_authority_locked(
        self, device: int, state: _FilesystemReservationState
    ) -> bool:
        """Repair per-device caches from exact capabilities and quarantine drift."""
        legacy_bytes = state.legacy_reserved_bytes
        legacy_inodes = state.legacy_reserved_inodes
        if (
            type(legacy_bytes) is not int
            or legacy_bytes < 0
            or type(legacy_inodes) is not int
            or legacy_inodes < 0
        ):
            state.corrupted = True
            self._protocol_violations += 1
            legacy_bytes = max(0, legacy_bytes) if type(legacy_bytes) is int else 0
            legacy_inodes = max(0, legacy_inodes) if type(legacy_inodes) is int else 0
        exact_bytes = legacy_bytes
        exact_inodes = legacy_inodes
        with self._lock:
            for capability in self._capabilities.values():
                if capability.active and capability.device == device:
                    if capability.reserved_bytes < 0 or capability.reserved_inodes < 0:
                        state.corrupted = True
                        self._protocol_violations += 1
                        continue
                    exact_bytes += capability.reserved_bytes
                    exact_inodes += capability.reserved_inodes
        mismatch = state.reserved_bytes != exact_bytes or state.reserved_inodes != exact_inodes
        account = state.cross_account
        cross_bytes = state.cross_reserved_bytes
        cross_inodes = state.cross_reserved_inodes
        if state.cross_process_enabled and account is not None:
            with account.lock:
                cross_bytes = account.reserved_bytes
                cross_inodes = account.reserved_inodes
            if (
                state.cross_reserved_bytes != cross_bytes
                or state.cross_reserved_inodes != cross_inodes
            ):
                mismatch = True
        if mismatch:
            state.corrupted = True
            self._protocol_violations += 1
            state.reserved_bytes = exact_bytes
            state.reserved_inodes = exact_inodes
            # Cross-process accounting may intentionally retain slack above local
            # capabilities. Its exact process account, when present, is authority.
            state.cross_reserved_bytes = cross_bytes
            state.cross_reserved_inodes = cross_inodes
        return not state.corrupted

    @staticmethod
    def _raise_quarantined(label: str) -> None:
        raise SchemaSanitizerResourceError(
            "temporary filesystem admission quarantined after accounting corruption",
            detail={
                "stage": "temporary_storage",
                "limit_name": "temporary_storage_corruption_quarantine",
                "limit_bytes": 0,
                "actual_bytes": 1,
                "artifact": label,
            },
        )

    def reserve(
        self,
        size_bytes: int,
        *,
        path: str | Path | None,
        label: str,
        inode_count: int = 0,
        _capability: ProcessTemporaryStorageCapability | None = None,
    ) -> int:
        """Reserve bytes on one device without stalling unrelated devices."""
        if type(size_bytes) is not int or type(inode_count) is not int:
            raise TypeError("temporary storage sizes must be exact integers")
        if size_bytes < 0 or inode_count < 0:
            raise ValueError("temporary storage sizes must be >= 0")
        requested = size_bytes
        requested_inodes = inode_count
        device, target, free_bytes = self.filesystem(path)
        free_inodes = self.free_inodes(target)
        capability_target_bytes = requested
        capability_target_inodes = requested_inodes
        if _capability is not None:
            # Initial publication and in-place growth both commit capability +
            # aggregate counters under the same device lock.
            if _capability.active:
                if _capability.device != device:
                    raise RuntimeError("temporary-storage growth changed device")
                capability_target_bytes = _capability.reserved_bytes + requested
                capability_target_inodes = _capability.reserved_inodes + requested_inodes
            else:
                _capability.device = device
                _capability.reserved_bytes = requested
                _capability.reserved_inodes = requested_inodes
        if requested == 0 and requested_inodes == 0:
            return device
        state = self._borrow_state(device)
        if state is None:
            cross_process_enabled = cross_process_storage_enabled()
            candidate = _FilesystemReservationState(
                capacity_bytes=max(0, free_bytes - _minimum_free_bytes()),
                capacity_inodes=max(0, free_inodes - min(1024, max(32, free_inodes // 100))),
                cross_process_enabled=cross_process_enabled,
                coordination_directory=(
                    cross_process_storage_directory() if cross_process_enabled else None
                ),
                cross_account=(
                    open_cross_process_storage_account(device) if cross_process_enabled else None
                ),
            )
            state = self._borrow_state(device, create=candidate)
            if state is not candidate and candidate.cross_account is not None:
                # Another thread won publication. Retire the unpublished exact
                # capability immediately so a losing constructor cannot leak a
                # host-wide storage-account owner.
                close_cross_process_storage_account(candidate.cross_account)
        assert state is not None
        try:
            with state.lock:  # type: ignore[attr-defined]
                if not self._reconcile_state_authority_locked(device, state):
                    self._raise_quarantined(label)
                current_headroom = max(0, free_bytes - _minimum_free_bytes())
                effective_capacity = min(
                    state.capacity_bytes, state.reserved_bytes + current_headroom
                )
                next_reserved = state.reserved_bytes + requested
                current_inode_headroom = max(
                    0, free_inodes - min(1024, max(32, free_inodes // 100))
                )
                effective_inode_capacity = min(
                    state.capacity_inodes,
                    state.reserved_inodes + current_inode_headroom,
                )
                next_inodes = state.reserved_inodes + requested_inodes
                if next_reserved > effective_capacity:
                    self._raise_exhausted(
                        next_reserved,
                        effective_capacity,
                        label,
                        cross_process=False,
                    )
                if next_inodes > effective_inode_capacity:
                    raise SchemaSanitizerResourceError(
                        "temporary filesystem inode capacity exhausted: "
                        f"{next_inodes} inodes > {effective_inode_capacity} inodes; "
                        f"artifact: {label}",
                        detail={
                            "stage": "temporary_storage",
                            "limit_name": "filesystem_free_inodes",
                            "limit_bytes": effective_inode_capacity,
                            "actual_bytes": next_inodes,
                            "artifact": label,
                        },
                    )
                cross_growth = max(0, next_reserved - state.cross_reserved_bytes)
                cross_inode_growth = max(0, next_inodes - state.cross_reserved_inodes)
                try:
                    if state.cross_process_enabled:
                        account = state.cross_account
                        if account is None:
                            raise RuntimeError("missing cross-process storage capability")
                        reserve_cross_process_account(
                            account,
                            cross_growth,
                            effective_capacity,
                            inode_count=cross_inode_growth,
                            inode_capacity=effective_inode_capacity,
                            enabled=True,
                            coordination_directory=state.coordination_directory,
                            _reserve_impl=_reserve_cross_process_raw,
                        )
                    else:
                        # Preserve the private raw-coordinator seam even when
                        # coordination is disabled. The production helper is a
                        # cheap no-op with enabled=False, while fault injection
                        # can still prove that this per-device transaction never
                        # serializes unrelated filesystem states.
                        _reserve_cross_process_raw(
                            device,
                            cross_growth,
                            effective_capacity,
                            inode_count=cross_inode_growth,
                            inode_capacity=effective_inode_capacity,
                            enabled=False,
                            coordination_directory=state.coordination_directory,
                        )
                except OSError as exc:
                    if "inode" in str(exc).lower():
                        raise SchemaSanitizerResourceError(
                            str(exc),
                            detail={
                                "stage": "temporary_storage",
                                "limit_name": ("cross_process_temporary_storage_inodes"),
                                "limit_bytes": effective_inode_capacity,
                                "actual_bytes": next_inodes,
                                "artifact": label,
                            },
                        ) from exc
                    self._raise_exhausted(
                        next_reserved,
                        effective_capacity,
                        label,
                        cross_process=True,
                        message=str(exc),
                    )
                next_legacy_bytes = state.legacy_reserved_bytes
                next_legacy_inodes = state.legacy_reserved_inodes
                if _capability is None:
                    assert type(next_legacy_bytes) is int and type(next_legacy_inodes) is int
                    next_legacy_bytes += requested
                    next_legacy_inodes += requested_inodes
                state.cross_reserved_bytes += cross_growth
                state.cross_reserved_inodes += cross_inode_growth
                # Commit legacy authority before its aggregate cache. An async
                # interruption can therefore only leave a conservative mismatch
                # that the next admission quarantines/rebuilds.
                state.legacy_reserved_bytes = next_legacy_bytes
                state.legacy_reserved_inodes = next_legacy_inodes
                state.reserved_bytes = next_reserved
                state.peak_reserved_bytes = max(state.peak_reserved_bytes, next_reserved)
                state.reserved_inodes = next_inodes
                state.peak_reserved_inodes = max(state.peak_reserved_inodes, next_inodes)
                if _capability is not None:
                    # Ownership publication is part of the same device-lock tail
                    # as aggregate counters; no concurrent admission can observe
                    # one side of an in-place growth without the other.
                    _capability.reserved_bytes = capability_target_bytes
                    _capability.reserved_inodes = capability_target_inodes
                    _capability.active = True
                    _capability.orphaned = False
        finally:
            self._return_state(device, state)
        record_resource_telemetry(
            temporary_free_floor_bytes=max(_MINIMUM_FREE_BYTES, requested),
            source="temporary_reservation",
        )
        return device

    def _prepublish_capability(self) -> ProcessTemporaryStorageCapability:
        """Allocate and root exact release authority before aggregate commit."""
        capability = ProcessTemporaryStorageCapability(self)
        with self._lock:
            if len(self._capabilities) >= _MAX_PROCESS_TEMPORARY_STORAGE_CAPABILITIES:
                raise RuntimeError("temporary-storage capability registry exhausted")
            token = max(1, int(self._next_capability))
            while token in self._capabilities:
                token += 1
                if token >= (1 << 63):
                    token = 1
            self._capabilities[token] = capability
            self._next_capability = token + 1
            capability.token = token
        return capability

    def _begin_capability_mutation(self, capability: ProcessTemporaryStorageCapability) -> bool:
        """Atomically claim one exact capability for release/resize."""
        with self._lock:
            if (
                capability.governor is not self
                or not capability.active
                or capability.inflight
                or self._capabilities.get(capability.token) is not capability
            ):
                return False
            capability.inflight = True
            return True

    def _finish_capability_mutation(
        self, capability: ProcessTemporaryStorageCapability, *, retire: bool
    ) -> None:
        """Commit/release mutation ownership without amount-based authority."""
        with self._lock:
            if retire:
                if self._capabilities.get(capability.token) is capability:
                    self._capabilities.pop(capability.token, None)
                capability.active = False
                capability.orphaned = False
                capability.token = 0
            capability.inflight = False

    def _mark_orphaned_capability(self, capability: ProcessTemporaryStorageCapability) -> None:
        """Keep a post-commit failure rooted for a later exact safe-point retry."""
        with self._lock:
            if self._capabilities.get(capability.token) is capability and capability.active:
                capability.orphaned = True
                self._orphaned_capability_hint = True

    def _drain_one_orphaned_capability_noexcept(self) -> None:
        """Retry at most one post-commit orphan outside the registry lock."""
        if not self._orphaned_capability_hint:
            return
        candidate = None
        with self._lock:
            for value in self._capabilities.values():
                if value.orphaned and value.active and not value.inflight:
                    candidate = value
                    break
            if candidate is None:
                self._orphaned_capability_hint = False
                return
        try:
            if self.release_capability(candidate):
                candidate.orphaned = False
        except BaseException:
            return
        with self._lock:
            self._orphaned_capability_hint = any(
                value.orphaned and value.active for value in self._capabilities.values()
            )

    def reserve_capability(
        self,
        size_bytes: int,
        *,
        path: str | Path | None,
        label: str,
        inode_count: int = 0,
    ) -> ProcessTemporaryStorageCapability:
        """Reserve one exact capability, allocating its owner before commit."""
        self._drain_one_orphaned_capability_noexcept()
        capability = self._prepublish_capability()
        try:
            self.reserve(
                size_bytes,
                path=path,
                label=label,
                inode_count=inode_count,
                _capability=capability,
            )
        except BaseException as primary:
            if capability.active:
                # The lower aggregate commit completed. Roll it back using the
                # exact prepublished authority; if cleanup itself fails, keep
                # that authority globally rooted for a later safe-point retry.
                try:
                    if not self._release_capability_exact(capability):
                        raise RuntimeError("post-commit temporary-storage rollback lost authority")
                except BaseException as cleanup_error:
                    self._mark_orphaned_capability(capability)
                    add_bounded_note(
                        primary,
                        "post-commit temporary-storage rollback also failed; authority retained",
                        cleanup_error,
                    )
            else:
                with self._lock:
                    if self._capabilities.get(capability.token) is capability:
                        self._capabilities.pop(capability.token, None)
                capability.token = 0
            raise
        if not capability.active:
            with self._lock:
                if self._capabilities.get(capability.token) is capability:
                    self._capabilities.pop(capability.token, None)
            capability.token = 0
        return capability

    @staticmethod
    def _cross_release_committed(
        account: CrossProcessStorageAccount, target_bytes: int, target_inodes: int
    ) -> bool:
        """Return whether a fallible host tail already committed local exact authority."""
        try:
            with account.lock:
                return (
                    account.reserved_bytes == target_bytes
                    and account.reserved_inodes == target_inodes
                    and account.reconciliation_pending
                )
        except BaseException:
            return False

    def _release_capability_exact(self, capability: ProcessTemporaryStorageCapability) -> bool:
        """Release one exact capability without touching move-transaction children."""
        if capability.governor is not self:
            raise RuntimeError("temporary-storage capability belongs to another governor")
        if not self._begin_capability_mutation(capability):
            return False
        device = capability.device
        amount = capability.reserved_bytes
        amount_inodes = capability.reserved_inodes
        try:
            state = self._borrow_state(device)
        except BaseException:
            self._finish_capability_mutation(capability, retire=False)
            raise
        if state is None:
            self._finish_capability_mutation(capability, retire=False)
            raise RuntimeError("temporary-storage capability lost its device state")
        committed = False
        post_commit_error: BaseException | None = None
        try:
            with state.lock:  # type: ignore[attr-defined]
                self._reconcile_state_authority_locked(device, state)
                if not capability.active:
                    return False
                if amount > state.reserved_bytes or amount_inodes > state.reserved_inodes:
                    raise RuntimeError("temporary-storage capability exceeds authoritative state")
                next_reserved = state.reserved_bytes - amount
                next_inodes = state.reserved_inodes - amount_inodes
                byte_slack = max(0, state.cross_reserved_bytes - next_reserved)
                inode_slack = max(0, state.cross_reserved_inodes - next_inodes)
                release_bytes = (
                    byte_slack
                    if next_reserved == 0 or byte_slack >= _CROSS_PROCESS_SHRINK_QUANTUM_BYTES
                    else 0
                )
                release_inodes = (
                    inode_slack
                    if next_inodes == 0 or inode_slack >= _CROSS_PROCESS_SHRINK_QUANTUM_INODES
                    else 0
                )
                next_cross_bytes = max(next_reserved, state.cross_reserved_bytes - release_bytes)
                next_cross_inodes = max(next_inodes, state.cross_reserved_inodes - release_inodes)
                # All Python arithmetic is complete before the host-wide commit.
                if release_bytes or release_inodes or not state.cross_process_enabled:
                    if state.cross_process_enabled:
                        account = state.cross_account
                        if account is None:
                            raise RuntimeError("missing cross-process storage capability")
                        try:
                            release_cross_process_account(
                                account,
                                release_bytes,
                                inode_count=release_inodes,
                                enabled=True,
                                coordination_directory=state.coordination_directory,
                                _release_impl=_release_cross_process_raw,
                            )
                        except BaseException as exc:
                            if not self._cross_release_committed(
                                account, next_cross_bytes, next_cross_inodes
                            ):
                                raise
                            # Lower exact authority committed before its fallible
                            # host tail failed. Finish this capability's commit so
                            # a caller retry cannot debit a different owner, then
                            # propagate the original cancellation/error below.
                            post_commit_error = exc
                    else:
                        _release_cross_process_raw(
                            device,
                            release_bytes,
                            inode_count=release_inodes,
                            enabled=False,
                            coordination_directory=state.coordination_directory,
                        )
                # Noexcept commit tail: install only values prepared above and
                # revoke exact authority in the same critical section.
                state.cross_reserved_bytes = next_cross_bytes
                state.cross_reserved_inodes = next_cross_inodes
                state.reserved_bytes = next_reserved
                state.reserved_inodes = next_inodes
                capability.reserved_bytes = 0
                capability.reserved_inodes = 0
                committed = True
        finally:
            try:
                self._return_state(device, state)
            except BaseException:
                if not committed:
                    self._finish_capability_mutation(capability, retire=False)
                    raise
            if committed:
                self._finish_capability_mutation(capability, retire=True)
            else:
                self._finish_capability_mutation(capability, retire=False)
        if post_commit_error is not None:
            raise post_commit_error
        return committed

    def release_capability(self, capability: ProcessTemporaryStorageCapability) -> bool:
        """Release one exact capability plus any rooted failed-move replacement."""
        if capability.governor is not self:
            raise RuntimeError("temporary-storage capability belongs to another governor")
        pending = capability.pending_replacement
        if pending is not None:
            if not self._release_capability_exact(pending):
                return False
            capability.pending_replacement = None
        return self._release_capability_exact(capability)

    def resize_capability(
        self,
        capability: ProcessTemporaryStorageCapability,
        size_bytes: int,
        *,
        path: str | Path | None,
        label: str,
        inode_count: int | None = None,
    ) -> ProcessTemporaryStorageCapability:
        """Resize exact authority; movement publishes a new capability first."""
        if capability.governor is not self or not capability.active:
            raise RuntimeError("temporary-storage capability is not authoritative")
        if type(size_bytes) is not int or size_bytes < 0:
            raise ValueError("temporary storage size must be a non-negative exact integer")
        target_device, _target, _free = self.filesystem(path)
        target_inodes = capability.reserved_inodes if inode_count is None else inode_count
        if target_device != capability.device:
            replacement = self.reserve_capability(
                size_bytes, path=path, label=label, inode_count=target_inodes
            )
            # The old capability remains the construction owner for the new one
            # until exactly one terminal move state commits. If rollback of the
            # replacement fails, the caller's still-authoritative old capability
            # keeps it reachable for a later release retry.
            capability.pending_replacement = replacement
            try:
                if not self._release_capability_exact(capability):
                    raise RuntimeError("temporary-storage capability changed during move")
            except BaseException as primary:
                try:
                    rolled_back = self._release_capability_exact(replacement)
                except BaseException as cleanup_error:
                    add_bounded_note(
                        primary,
                        "temporary-storage move replacement rollback also failed",
                        cleanup_error,
                    )
                    raise
                if rolled_back:
                    capability.pending_replacement = None
                raise
            capability.pending_replacement = None
            return replacement

        if not self._begin_capability_mutation(capability):
            raise RuntimeError("temporary-storage capability is not authoritative")
        current = capability.reserved_bytes
        delta = size_bytes - current
        if delta == 0:
            self._finish_capability_mutation(capability, retire=False)
            return capability
        if delta > 0:
            try:
                # reserve() performs all capacity and host-wide checks before its
                # aggregate commit. The exact capability is already rooted.
                self.reserve(
                    delta,
                    path=path,
                    label=label,
                    inode_count=0,
                    _capability=capability,
                )
            finally:
                self._finish_capability_mutation(capability, retire=False)
            return capability

        # Partial shrink needs exact ownership but keeps the capability alive.
        release_amount = -delta
        device = capability.device
        try:
            state = self._borrow_state(device)
        except BaseException:
            self._finish_capability_mutation(capability, retire=False)
            raise
        if state is None:
            self._finish_capability_mutation(capability, retire=False)
            raise RuntimeError("temporary-storage capability lost its device state")
        committed = False
        post_commit_error: BaseException | None = None
        try:
            with state.lock:  # type: ignore[attr-defined]
                self._reconcile_state_authority_locked(device, state)
                if not capability.active or capability.reserved_bytes != current:
                    raise RuntimeError("temporary-storage capability changed during resize")
                next_reserved = state.reserved_bytes - release_amount
                next_capability = current - release_amount
                byte_slack = max(0, state.cross_reserved_bytes - next_reserved)
                release_bytes = (
                    byte_slack
                    if next_reserved == 0 or byte_slack >= _CROSS_PROCESS_SHRINK_QUANTUM_BYTES
                    else 0
                )
                next_cross_bytes = max(next_reserved, state.cross_reserved_bytes - release_bytes)
                if release_bytes or not state.cross_process_enabled:
                    if state.cross_process_enabled:
                        account = state.cross_account
                        if account is None:
                            raise RuntimeError("missing cross-process storage capability")
                        try:
                            release_cross_process_account(
                                account,
                                release_bytes,
                                inode_count=0,
                                enabled=True,
                                coordination_directory=state.coordination_directory,
                                _release_impl=_release_cross_process_raw,
                            )
                        except BaseException as exc:
                            if not self._cross_release_committed(
                                account, next_cross_bytes, state.cross_reserved_inodes
                            ):
                                raise
                            post_commit_error = exc
                    else:
                        _release_cross_process_raw(
                            device,
                            release_bytes,
                            inode_count=0,
                            enabled=False,
                            coordination_directory=state.coordination_directory,
                        )
                state.cross_reserved_bytes = next_cross_bytes
                state.reserved_bytes = next_reserved
                capability.reserved_bytes = next_capability
                committed = True
        finally:
            try:
                self._return_state(device, state)
            except BaseException:
                if not committed:
                    self._finish_capability_mutation(capability, retire=False)
                    raise
            self._finish_capability_mutation(capability, retire=False)
        if post_commit_error is not None:
            raise post_commit_error
        return capability

    @staticmethod
    def _raise_exhausted(
        actual: int,
        capacity: int,
        label: str,
        *,
        cross_process: bool,
        message: str | None = None,
    ) -> None:
        """Raise one stable public capacity error."""
        limit_name = (
            "cross_process_temporary_storage_bytes"
            if cross_process
            else "process_temporary_storage_bytes"
        )
        raise SchemaSanitizerResourceError(
            message
            or f"process temporary-storage capacity exhausted: "
            f"{actual} bytes > {capacity} bytes; artifact: {label}",
            detail={
                "stage": "temporary_storage",
                "limit_name": limit_name,
                "limit_bytes": capacity,
                "actual_bytes": actual,
                "artifact": label,
            },
        )

    def release(self, device: int, size_bytes: int, *, inode_count: int = 0) -> None:
        """Release one device without serializing unrelated filesystems."""
        if type(device) is not int or type(size_bytes) is not int or type(inode_count) is not int:
            raise TypeError("temporary storage release values must be exact integers")
        if size_bytes < 0 or inode_count < 0:
            raise ValueError("temporary storage release sizes must be >= 0")
        amount = size_bytes
        amount_inodes = inode_count
        if amount == 0 and amount_inodes == 0:
            return
        state = self._borrow_state(device)
        if state is None:
            with self._lock:
                self._over_release_count += 1
                self._over_release_bytes += amount
            return
        excess = 0
        excess_inodes = False
        post_commit_error: BaseException | None = None
        try:
            with state.lock:  # type: ignore[attr-defined]
                self._reconcile_state_authority_locked(device, state)
                legacy_bytes = state.legacy_reserved_bytes
                legacy_inodes = state.legacy_reserved_inodes
                assert type(legacy_bytes) is int and type(legacy_inodes) is int
                # Bare amount cleanup owns only the legacy subledger. It may not
                # debit exact capability reservations even when the aggregate is
                # large enough to hide a duplicate/stale release.
                released = min(amount, legacy_bytes)
                excess = max(0, amount - legacy_bytes)
                released_inodes = min(amount_inodes, legacy_inodes)
                excess_inodes = amount_inodes > legacy_inodes
                next_legacy_bytes = legacy_bytes - released
                next_legacy_inodes = legacy_inodes - released_inodes
                next_reserved = state.reserved_bytes - released
                next_inodes = state.reserved_inodes - released_inodes
                # Cross-process capacity is aggregated per process/device.  Shrink
                # conservatively in chunks to avoid one file-lock/journal/fsync cycle
                # per local lease release, but always reconcile completely at zero.
                byte_slack = max(0, state.cross_reserved_bytes - next_reserved)
                inode_slack = max(0, state.cross_reserved_inodes - next_inodes)
                release_bytes = (
                    byte_slack
                    if next_reserved == 0 or byte_slack >= _CROSS_PROCESS_SHRINK_QUANTUM_BYTES
                    else 0
                )
                release_inodes = (
                    inode_slack
                    if next_inodes == 0 or inode_slack >= _CROSS_PROCESS_SHRINK_QUANTUM_INODES
                    else 0
                )
                if release_bytes or release_inodes or not state.cross_process_enabled:
                    # Even when host-wide coordination is disabled, keep the
                    # release helper in the transaction. The helper is a cheap
                    # no-op in that mode, while tests/control doubles and future
                    # implementations can still fail before local state commits.
                    if state.cross_process_enabled:
                        account = state.cross_account
                        if account is None:
                            raise RuntimeError("missing cross-process storage capability")
                        target_cross_bytes = max(
                            next_reserved, state.cross_reserved_bytes - release_bytes
                        )
                        target_cross_inodes = max(
                            next_inodes, state.cross_reserved_inodes - release_inodes
                        )
                        try:
                            release_cross_process_account(
                                account,
                                release_bytes,
                                inode_count=release_inodes,
                                enabled=True,
                                coordination_directory=state.coordination_directory,
                                _release_impl=_release_cross_process_raw,
                            )
                        except BaseException as exc:
                            if not self._cross_release_committed(
                                account, target_cross_bytes, target_cross_inodes
                            ):
                                raise
                            post_commit_error = exc
                    else:
                        # Keep the private raw coordinator hook in the commit
                        # sequence even when host-wide coordination is disabled.
                        # The real implementation is a no-op in this mode, but
                        # resolving the module variable at call time preserves
                        # fault injection/instrumentation without exposing the
                        # amount-based API publicly.
                        _release_cross_process_raw(
                            device,
                            release_bytes,
                            inode_count=release_inodes,
                            enabled=False,
                            coordination_directory=state.coordination_directory,
                        )
                    state.cross_reserved_bytes = max(
                        next_reserved, state.cross_reserved_bytes - release_bytes
                    )
                    state.cross_reserved_inodes = max(
                        next_inodes, state.cross_reserved_inodes - release_inodes
                    )
                state.legacy_reserved_bytes = next_legacy_bytes
                state.legacy_reserved_inodes = next_legacy_inodes
                state.reserved_bytes = next_reserved
                state.reserved_inodes = next_inodes
        finally:
            self._return_state(device, state)
        if post_commit_error is not None:
            raise post_commit_error
        if excess or excess_inodes:
            with self._lock:
                if excess:
                    self._over_release_count += 1
                    self._over_release_bytes += excess
                if excess_inodes:
                    self._over_release_count += 1

    def diagnostics(self) -> ProcessTemporaryStorageDiagnostics:
        """Return aggregate process-governor cleanup anomalies."""
        with self._lock:
            return ProcessTemporaryStorageDiagnostics(
                self._over_release_count, self._over_release_bytes, self._protocol_violations
            )

    def snapshot(self, path: str | Path | None) -> ProcessTemporaryStorageSnapshot:
        """Return one device snapshot without blocking other filesystems."""
        device, target, free_bytes = self.filesystem(path)
        state = self._borrow_state(device)
        if state is None:
            free_inodes = self.free_inodes(target)
            return ProcessTemporaryStorageSnapshot(
                capacity_bytes=max(0, free_bytes - _minimum_free_bytes()),
                reserved_bytes=0,
                peak_reserved_bytes=0,
                capacity_inodes=max(0, free_inodes - min(1024, max(32, free_inodes // 100))),
                reserved_inodes=0,
                peak_reserved_inodes=0,
            )
        try:
            with state.lock:  # type: ignore[attr-defined]
                return ProcessTemporaryStorageSnapshot(
                    capacity_bytes=state.capacity_bytes,
                    reserved_bytes=state.reserved_bytes,
                    peak_reserved_bytes=state.peak_reserved_bytes,
                    capacity_inodes=state.capacity_inodes,
                    reserved_inodes=state.reserved_inodes,
                    peak_reserved_inodes=state.peak_reserved_inodes,
                )
        finally:
            self._return_state(device, state)

    def authoritative_snapshot(self) -> ProcessTemporaryStorageAuthoritativeSnapshot:
        """Return exact logical/physical totals without creating filesystem state."""
        with self._lock:
            states = tuple(self._states.values())
        reserved_bytes = 0
        reserved_inodes = 0
        cross_reserved_bytes = 0
        cross_reserved_inodes = 0
        for state in states:
            with state.lock:  # type: ignore[attr-defined]
                reserved_bytes += state.reserved_bytes
                reserved_inodes += state.reserved_inodes
                cross_reserved_bytes += state.cross_reserved_bytes
                cross_reserved_inodes += state.cross_reserved_inodes
        return ProcessTemporaryStorageAuthoritativeSnapshot(
            len(states),
            reserved_bytes,
            reserved_inodes,
            cross_reserved_bytes,
            cross_reserved_inodes,
            self._protocol_violations,
        )

    def prepare_for_fork(self) -> None:
        self._fork_prepared = self._fork_banks[self._fork_bank_index]

    def clear_fork_preparation(self) -> None:
        self._fork_prepared = None

    def reset_after_fork(self) -> None:
        """Quarantine inherited reservations and swap a preallocated empty registry."""
        quarantine_inherited_state("temporary-storage", self._states)
        quarantine_inherited_state("temporary-storage-capabilities", self._capabilities)
        prepared = self._fork_prepared
        if prepared is None:
            return
        self._lock, self._states, self._capabilities = prepared
        self._next_capability = 1
        self._fork_bank_index = 1 - self._fork_bank_index
        self._fork_prepared = None
        self._over_release_count = 0
        self._over_release_bytes = 0


_PROCESS_TEMPORARY_STORAGE = _ProcessTemporaryStorageGovernor()

from .fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler(
    "temporary-storage-governor",
    before=_PROCESS_TEMPORARY_STORAGE.prepare_for_fork,
    after_in_parent=_PROCESS_TEMPORARY_STORAGE.clear_fork_preparation,
    after_in_child=_PROCESS_TEMPORARY_STORAGE.reset_after_fork,
)


def process_temporary_storage_snapshot(
    path: str | Path | None = None,
) -> ProcessTemporaryStorageSnapshot:
    """Return process-wide temporary-space accounting for one filesystem."""
    return _PROCESS_TEMPORARY_STORAGE.snapshot(path)


def process_temporary_storage_authoritative_snapshot() -> (
    ProcessTemporaryStorageAuthoritativeSnapshot
):
    """Return all authoritative temporary-storage ownership domains."""
    return _PROCESS_TEMPORARY_STORAGE.authoritative_snapshot()


def process_temporary_storage_diagnostics() -> ProcessTemporaryStorageDiagnostics:
    """Return process-wide temporary-storage cleanup anomalies."""
    return _PROCESS_TEMPORARY_STORAGE.diagnostics()


from .shutdown_observers import (  # noqa: E402
    register_shutdown_observer as _register_shutdown_observer,
)

_register_shutdown_observer(
    "temporary_storage_authoritative", process_temporary_storage_authoritative_snapshot
)


__all__ = [
    "ProcessTemporaryStorageAuthoritativeSnapshot",
    "ProcessTemporaryStorageCapability",
    "ProcessTemporaryStorageDiagnostics",
    "ProcessTemporaryStorageSnapshot",
    "_PROCESS_TEMPORARY_STORAGE",
    "process_temporary_storage_authoritative_snapshot",
    "process_temporary_storage_diagnostics",
    "process_temporary_storage_snapshot",
]
