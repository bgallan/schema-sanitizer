"""Bound retained directory metadata before high-cardinality discovery allocates."""

from __future__ import annotations

from collections.abc import Iterable
from threading import Condition, Lock
from time import monotonic
from typing import TYPE_CHECKING

from ..core_impl.bounded_text import utf8_size_bounded
from ..core_impl.safe_errors import add_bounded_note
from ..errors import SchemaSanitizerResourceError
from ..sources.models import RemoteFile

if TYPE_CHECKING:
    from ..core_impl.memory_budget import OperationMemoryLease, OperationMemoryLedger
    from .directory_inputs import FolderFile

_DIRECTORY_METADATA_BASE_BYTES = 256
_DIRECTORY_METADATA_REFERENCE_BYTES = 16
# Conservative retained charge for the transient provider grouping graph:
# outer/inner dict slots, list growth and the URI reference.  The charge is
# intentionally retained for the discovery lifetime because those structures
# coexist with DirectoryDiscoveryBuilder until provider scanning completes.
_DIRECTORY_METADATA_GROUP_ASSOCIATION_BYTES = 128
_DIRECTORY_METADATA_FIXED_OVERHEAD_BYTES = 64 * 1024
_DIRECTORY_METADATA_CLOSE_TIMEOUT_SECONDS = 30.0


def _utf8_size_bounded(value: str, maximum_bytes: int) -> int:
    """Compatibility wrapper for the shared allocation-bounded UTF-8 meter."""
    return utf8_size_bounded(value, maximum_bytes)


class RetainedDirectoryMetadata:
    """Lifetime owner for metadata that escapes the discovery call scope."""

    __slots__ = ("_lease", "_lock")

    def __init__(self) -> None:
        self._lease: OperationMemoryLease | None = None
        self._lock = Lock()

    def _adopt(self, lease: OperationMemoryLease | None) -> None:
        if lease is None:
            return
        with self._lock:
            if self._lease is not None:
                raise RuntimeError("directory metadata owner already adopted a lease")
            self._lease = lease

    @property
    def reserved_bytes(self) -> int:
        """Return bytes still governed for escaped discovery metadata."""
        with self._lock:
            lease = self._lease
        return 0 if lease is None else lease.reserved_bytes

    def close(self) -> None:
        """Release retained metadata only after the lease cleanup commits.

        A failed ``OperationMemoryLease.close()`` leaves the exact lease attached
        to this owner, so a later close retries the same generation instead of
        falsely reporting zero governed bytes.
        """
        with self._lock:
            lease = self._lease
            if lease is None:
                return
            lease.close()
            if self._lease is lease:
                self._lease = None

    def live_lease(self) -> OperationMemoryLease | None:
        """Return the authoritative retained lease for capability validation."""
        with self._lock:
            return self._lease


class TransientDirectoryMetadataReservation:
    """Aggregate O(n) scratch metadata and return it when grouping drains."""

    __slots__ = ("_budget", "_bytes", "_lock")

    def __init__(self, budget: "DirectoryMetadataBudget") -> None:
        self._budget: DirectoryMetadataBudget | None = budget
        self._bytes = 0
        self._lock = Lock()

    def charge_before_publish(self, associations: int = 1) -> None:
        count = max(0, int(associations))
        amount = count * _DIRECTORY_METADATA_GROUP_ASSOCIATION_BYTES
        if amount == 0:
            return
        budget = self._budget
        if budget is None:
            raise RuntimeError("transient directory metadata reservation is closed")
        budget._charge(amount, observed=count)
        try:
            with self._lock:
                if self._budget is None:
                    raise RuntimeError("transient directory metadata reservation is closed")
                self._bytes += amount
        except BaseException:
            budget._uncharge(amount)
            raise

    def rollback_publish(self, associations: int = 1) -> None:
        count = max(0, int(associations))
        amount = count * _DIRECTORY_METADATA_GROUP_ASSOCIATION_BYTES
        if amount == 0:
            return
        with self._lock:
            budget = self._budget
            if budget is None or amount > self._bytes:
                raise RuntimeError("transient directory metadata rollback over-release")
            self._bytes -= amount
        budget._uncharge(amount)

    def close(self) -> None:
        with self._lock:
            budget = self._budget
            amount = self._bytes
            self._budget = None
            self._bytes = 0
        if budget is not None and amount:
            budget._uncharge(amount)

    def __enter__(self) -> "TransientDirectoryMetadataReservation":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


class DirectoryMetadataBudget:
    """Conservatively charge retained directory metadata to one operation."""

    def __init__(
        self,
        memory_limit_bytes: int | None,
        *,
        operation_memory_ledger: OperationMemoryLedger | None = None,
    ) -> None:
        """Derive the ceiling and optionally debit the cross-language ledger."""
        from ..core_impl.memory_budget import memory_budget

        self.limit_bytes = max(
            memory_budget(memory_limit_bytes).metadata_bytes,
            _DIRECTORY_METADATA_FIXED_OVERHEAD_BYTES,
        )
        self._operation_memory_ledger = operation_memory_ledger
        self._memory_lease = (
            operation_memory_ledger.acquire(0, stage="directory_metadata")
            if operation_memory_ledger is not None
            else None
        )
        self._retention_owner = RetainedDirectoryMetadata()
        self._used_bytes = 0
        # Serializes lease resizing without holding the state/condition lock.
        # That keeps process-global memory-governor locks out of our lock order
        # while still making reserve-before-publish transactional.
        self._admission_lock = Lock()
        self._lock = Lock()
        self._close_condition = Condition(self._lock)
        self._close_started = False
        self._closing = False
        self._closed = False

    @property
    def used_bytes(self) -> int:
        """Return the currently retained conservative metadata estimate."""
        with self._lock:
            return self._used_bytes

    @property
    def retention_owner(self) -> RetainedDirectoryMetadata:
        """Return the stable owner attached to metadata created in this scope."""
        return self._retention_owner

    def _available(self) -> tuple[int, int]:
        """Return one admission snapshot before bounded materialization begins."""
        with self._lock:
            if self._close_started:
                raise RuntimeError("directory metadata budget is closed")
            return self._used_bytes, max(0, self.limit_bytes - self._used_bytes)

    def _limit_error(self, actual_bytes: int, observed: int) -> SchemaSanitizerResourceError:
        """Build the stable public error used by preflight and final admission."""
        return SchemaSanitizerResourceError(
            "memory_limit_bytes limit exceeded during directory_metadata: "
            f"{actual_bytes} bytes > {self.limit_bytes} bytes",
            detail={
                "stage": "directory_metadata",
                "limit_name": "directory_metadata_bytes",
                "limit_bytes": self.limit_bytes,
                "actual_bytes": actual_bytes,
                "observed_value": observed,
            },
        )

    def charge_uris(self, uris: Iterable[str]) -> tuple[str, ...]:
        """Reserve each URI before retaining it in the materialized result."""
        values: list[str] = []
        charges: list[int] = []
        try:
            for uri in uris:
                _used, available = self._available()
                remaining_for_text = max(
                    0,
                    (available - _DIRECTORY_METADATA_BASE_BYTES) // 2,
                )
                encoded = utf8_size_bounded(uri, remaining_for_text)
                item_charge = _DIRECTORY_METADATA_BASE_BYTES + 2 * encoded
                self._charge(item_charge, observed=len(values) + 1)
                try:
                    values.append(uri)
                except BaseException:
                    self._uncharge(item_charge)
                    raise
                charges.append(item_charge)

            # The temporary tuple reference graph is admitted before allocation.
            # Once built, the list and tuple coexist only inside this bounded span.
            temporary = len(values) * _DIRECTORY_METADATA_REFERENCE_BYTES
            self._charge(temporary, observed=len(values))
            try:
                retained = tuple(values)
            finally:
                self._uncharge(temporary)
            return retained
        except BaseException:
            for amount in reversed(charges):
                self._uncharge(amount)
            raise

    def charge_references(
        self, values: Iterable[object], *, references_per_item: int = 1
    ) -> tuple[object, ...]:
        """Reserve reference credit before every append and tuple conversion."""
        multiplier = max(1, int(references_per_item))
        charge_per_item = multiplier * _DIRECTORY_METADATA_REFERENCE_BYTES
        retained: list[object] = []
        charged = 0
        try:
            for value in values:
                self._charge(charge_per_item, observed=len(retained) + 1)
                charged += charge_per_item
                try:
                    retained.append(value)
                except BaseException:
                    self._uncharge(charge_per_item)
                    charged -= charge_per_item
                    raise
            temporary = len(retained) * _DIRECTORY_METADATA_REFERENCE_BYTES
            self._charge(temporary, observed=len(retained))
            try:
                result = tuple(retained)
            finally:
                self._uncharge(temporary)
            return result
        except BaseException:
            if charged:
                self._uncharge(charged)
            raise

    def charge_file(self, file: FolderFile | RemoteFile, *, associations: int = 1) -> None:
        """Charge one retained file and bind its lifetime to the stable owner."""
        existing_owner = getattr(file, "_metadata_owner", None)
        if existing_owner is not None and existing_owner is not self._retention_owner:
            raise RuntimeError("directory metadata object is already governed by another owner")
        strings: tuple[str, ...]
        if isinstance(file, RemoteFile):
            strings = (
                file.uri,
                file.name,
                file.generation or "",
                file.metageneration or "",
                file.etag or "",
                file.crc32c or "",
                file.updated.isoformat() if file.updated is not None else "",
                file.time_created.isoformat() if file.time_created is not None else "",
            )
        else:
            strings = (file.display_name, file.name, file.native_path or "")
        association_bytes = max(0, associations) * _DIRECTORY_METADATA_REFERENCE_BYTES
        maximum_strings = max(
            0,
            self.limit_bytes - _DIRECTORY_METADATA_BASE_BYTES - association_bytes,
        )
        string_bytes = 0
        for value in strings:
            string_bytes += utf8_size_bounded(value, max(0, maximum_strings - string_bytes))
            if string_bytes > maximum_strings:
                break
        self._charge(
            _DIRECTORY_METADATA_BASE_BYTES + string_bytes + association_bytes,
            observed=1,
        )
        # The file object itself can escape independently from its parent tuple.
        # Bind the stable retention owner only after the charge commit so a
        # failed admission never publishes an unbacked ownership claim.
        if existing_owner is None:
            object.__setattr__(file, "_metadata_owner", self._retention_owner)

    def charge_associations(self, associations: int) -> None:
        """Charge additional retained references for an already-charged file."""
        count = max(0, int(associations))
        self._charge(count * _DIRECTORY_METADATA_REFERENCE_BYTES, observed=count)

    def transient_group_associations(self) -> TransientDirectoryMetadataReservation:
        """Return a scoped aggregate reservation for provider grouping scratch."""
        return TransientDirectoryMetadataReservation(self)

    def charge_group_associations(self, associations: int = 1) -> None:
        """Pre-admit transient provider grouping nodes before publishing them.

        Bulk discovery first groups requested URIs by provider parent/child and
        only later builds the retained discovery result.  That intermediate
        dict/list graph is O(n) and therefore must share the same metadata
        ceiling rather than being treated as free scratch memory.
        """
        count = max(0, int(associations))
        self._charge(
            count * _DIRECTORY_METADATA_GROUP_ASSOCIATION_BYTES,
            observed=count,
        )

    def _charge(self, size: int, *, observed: int) -> None:
        """Pre-admit one conservative estimate before caller-side allocation."""
        amount = max(0, int(size))
        if amount == 0:
            return

        # Compatibility for focused historical doubles constructed with
        # ``object.__new__``. Production instances always own ``_memory_lease``.
        if not hasattr(self, "_memory_lease"):
            owner = self._operation_memory_ledger
            if owner is not None:
                owner.reserve(amount, stage="directory_metadata")
            try:
                with self._lock:
                    if self._close_started:
                        raise RuntimeError("directory metadata budget is closed")
                    next_used = self._used_bytes + amount
                    if next_used > self.limit_bytes:
                        raise self._limit_error(next_used, observed)
                    self._used_bytes = next_used
            except BaseException as primary:
                if owner is not None:
                    try:
                        owner.release(amount)
                    except BaseException as cleanup_error:
                        add_bounded_note(
                            primary,
                            "directory metadata reservation rollback also failed",
                            cleanup_error,
                        )
                raise
            return

        with self._admission_lock:
            with self._lock:
                if self._close_started:
                    raise RuntimeError("directory metadata budget is closed")
                previous = self._used_bytes
                next_used = previous + amount
                if next_used > self.limit_bytes:
                    raise self._limit_error(next_used, observed)
                lease = self._memory_lease

            if lease is not None:
                lease.resize(next_used)
            try:
                with self._lock:
                    # Close may start while the external memory governor is
                    # admitting the resize. Roll back rather than publishing
                    # ownership after terminal intent became visible.
                    if self._close_started:
                        raise RuntimeError("directory metadata budget is closed")
                    if self._used_bytes != previous:
                        raise RuntimeError("directory metadata reservation changed concurrently")
                    self._used_bytes = next_used
            except BaseException as primary:
                if lease is not None:
                    try:
                        lease.resize(previous)
                    except BaseException as cleanup_error:
                        add_bounded_note(
                            primary,
                            "directory metadata reservation rollback also failed",
                            cleanup_error,
                        )
                raise

    def _uncharge(self, size: int) -> None:
        """Rollback bytes retained only by a failed or temporary materialization."""
        amount = max(0, int(size))
        if amount == 0:
            return

        if not hasattr(self, "_memory_lease"):
            with self._lock:
                released = min(amount, self._used_bytes)
            owner = self._operation_memory_ledger
            if released and owner is not None:
                owner.release(released)
            with self._lock:
                self._used_bytes = max(0, self._used_bytes - released)
            return

        with self._admission_lock:
            with self._lock:
                previous = self._used_bytes
                target = max(0, previous - amount)
                lease = self._memory_lease
            if lease is not None:
                lease.resize(target)
            with self._lock:
                if self._used_bytes != previous:
                    raise RuntimeError("directory metadata reservation changed concurrently")
                self._used_bytes = target

    def retain(self) -> RetainedDirectoryMetadata:
        """Transfer the live lease to metadata objects escaping discovery."""
        with self._close_condition:
            self._close_started = True
            deadline = monotonic() + _DIRECTORY_METADATA_CLOSE_TIMEOUT_SECONDS
            while self._closing:
                remaining = deadline - monotonic()
                if remaining <= 0 or not self._close_condition.wait(timeout=remaining):
                    raise RuntimeError("directory metadata retain exceeded its deadline")
            if self._closed:
                return self._retention_owner
            self._closing = True

        try:
            with self._admission_lock:
                lease = self._memory_lease
                successor = lease
                if lease is not None:
                    transferred = lease.transfer_stage("retained_discovery_metadata")
                    if transferred is not None:
                        successor = transferred
                self._retention_owner._adopt(successor)
        except BaseException:
            with self._close_condition:
                self._closing = False
                self._close_condition.notify_all()
            raise

        with self._close_condition:
            self._memory_lease = None
            self._used_bytes = 0
            self._closed = True
            self._closing = False
            self._close_condition.notify_all()
        return self._retention_owner

    def close(self) -> None:
        """Stop admission and release owned metadata within a bounded close wait."""
        with self._close_condition:
            self._close_started = True
            deadline = monotonic() + _DIRECTORY_METADATA_CLOSE_TIMEOUT_SECONDS
            while self._closing:
                remaining = deadline - monotonic()
                if remaining <= 0 or not self._close_condition.wait(timeout=remaining):
                    raise RuntimeError("directory metadata budget close exceeded its deadline")
            if self._closed:
                return
            self._closing = True

        try:
            if hasattr(self, "_memory_lease"):
                with self._admission_lock:
                    lease = self._memory_lease
                    if lease is not None:
                        lease.close()
            else:
                # Historical injected owner: release exactly the retained debit.
                owner = self._operation_memory_ledger
                with self._lock:
                    used = self._used_bytes
                if owner is not None and used:
                    owner.release(used)
        except BaseException:
            with self._close_condition:
                self._closing = False
                self._close_condition.notify_all()
            raise

        with self._close_condition:
            if hasattr(self, "_memory_lease"):
                self._memory_lease = None
            self._used_bytes = 0
            self._closed = True
            self._closing = False
            self._close_condition.notify_all()


__all__ = ["DirectoryMetadataBudget", "RetainedDirectoryMetadata"]
