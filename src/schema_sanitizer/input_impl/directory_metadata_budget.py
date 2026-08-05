"""Bound retained directory metadata before high-cardinality discovery allocates."""

from __future__ import annotations

from collections.abc import Iterable
from threading import Condition, Lock
from typing import TYPE_CHECKING

from schema_sanitizer.core_impl.safe_errors import add_bounded_note

from ..core_impl.bounded_text import utf8_size_bounded
from ..errors import SchemaSanitizerResourceError
from .remote_files import RemoteFile

if TYPE_CHECKING:
    from ..core_impl.memory_budget import OperationMemoryLedger
    from .directory_inputs import FolderFile

_DIRECTORY_METADATA_BASE_BYTES = 256
_DIRECTORY_METADATA_REFERENCE_BYTES = 16
_DIRECTORY_METADATA_FIXED_OVERHEAD_BYTES = 64 * 1024


def _utf8_size_bounded(value: str, maximum_bytes: int) -> int:
    """Compatibility wrapper for the shared allocation-bounded UTF-8 meter."""
    return utf8_size_bounded(value, maximum_bytes)


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
        self._used_bytes = 0
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
        """Bound iteration and string measurement before retaining URI keys."""
        initial_used, available = self._available()
        values: list[str] = []
        charged = 0
        for uri in uris:
            remaining_for_text = max(
                0,
                (available - charged - _DIRECTORY_METADATA_BASE_BYTES) // 2,
            )
            encoded = utf8_size_bounded(uri, remaining_for_text)
            item_charge = _DIRECTORY_METADATA_BASE_BYTES + 2 * encoded
            next_charge = charged + item_charge
            if next_charge > available:
                raise self._limit_error(initial_used + next_charge, len(values) + 1)
            values.append(uri)
            charged = next_charge
        # Convert while the temporary reference graph is still bounded, then
        # publish accounting. A concurrent closer/charger is resolved by _charge.
        retained = tuple(values)
        self._charge(charged, observed=len(retained))
        return retained

    def charge_references(
        self, values: Iterable[object], *, references_per_item: int = 1
    ) -> tuple[object, ...]:
        """Materialize an iterable only while retained references fit the budget."""
        multiplier = max(1, int(references_per_item))
        initial_used, available = self._available()
        charge_per_item = multiplier * _DIRECTORY_METADATA_REFERENCE_BYTES
        retained: list[object] = []
        for value in values:
            next_charge = (len(retained) + 1) * charge_per_item
            if next_charge > available:
                raise self._limit_error(initial_used + next_charge, len(retained) + 1)
            retained.append(value)
        result = tuple(retained)
        self._charge(len(result) * charge_per_item, observed=len(result))
        return result

    def charge_file(self, file: FolderFile | RemoteFile, *, associations: int = 1) -> None:
        """Charge one retained file record plus list/dictionary references."""
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

    def charge_associations(self, associations: int) -> None:
        """Charge additional retained references for an already-charged file."""
        count = max(0, int(associations))
        self._charge(count * _DIRECTORY_METADATA_REFERENCE_BYTES, observed=count)

    def _charge(self, size: int, *, observed: int) -> None:
        """Reserve one conservative estimate or reject before retaining it."""
        amount = max(0, int(size))
        ledger = self._operation_memory_ledger
        if ledger is not None:
            ledger.reserve(amount, stage="directory_metadata")
        try:
            with self._lock:
                if self._close_started:
                    raise RuntimeError("directory metadata budget is closed")
                next_used = self._used_bytes + amount
                if next_used > self.limit_bytes:
                    raise self._limit_error(next_used, observed)
                self._used_bytes = next_used
        except BaseException as exc:
            if ledger is not None:
                try:
                    ledger.release(amount)
                except BaseException as cleanup_error:
                    add_bounded_note(
                        exc,
                        "directory metadata admission rollback also failed",
                        cleanup_error,
                    )
            raise

    def close(self) -> None:
        """Stop admission and keep ledger release retryable after native faults."""
        with self._close_condition:
            self._close_started = True
            while self._closing:
                self._close_condition.wait()
            if self._closed:
                return
            self._closing = True
            used = self._used_bytes
            ledger = self._operation_memory_ledger

        try:
            if ledger is not None:
                ledger.release(used)
        except BaseException:
            with self._close_condition:
                self._closing = False
                self._close_condition.notify_all()
            raise

        with self._close_condition:
            self._used_bytes = 0
            self._closed = True
            self._closing = False
            self._close_condition.notify_all()


__all__ = ["DirectoryMetadataBudget"]
