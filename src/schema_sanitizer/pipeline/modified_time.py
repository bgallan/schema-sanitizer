"""UTC modified-time planning for flat remote prefixes."""

from __future__ import annotations

from bisect import bisect_right
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from time import perf_counter
from typing import TYPE_CHECKING

from ..core_impl.execution_policy import normalize_threading_mode
from ..core_impl.memory_budget import (
    OperationMemoryLedger,
    activate_operation_memory_ledger,
    normalize_memory_limit,
)
from ..core_impl.uris import remote_provider
from ..input_impl.directory_inputs import (
    DirectoryMetadataBudget,
    directory_metadata_budget_scope,
)
from ..input_impl.remote_files import RemoteFile, remote_file_sort_key
from ..input_impl.source_manifest import SourceManifest
from ..remote_impl import routing, sync_backend
from ..remote_impl.transport import run_sync

if TYPE_CHECKING:
    from .types import PartitionRunPlan


def _aware_utc(value: datetime, *, field_name: str) -> datetime:
    """Validate one aware datetime and normalize it to UTC."""
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True, init=False)
class UtcWindow:
    """One validated half-open UTC interval: ``start <= value < end``."""

    start: datetime
    end: datetime

    def __init__(self, start: datetime, end: datetime) -> None:
        """Normalize aware bounds to UTC and reject empty or reversed windows."""
        normalized_start = _aware_utc(start, field_name="start")
        normalized_end = _aware_utc(end, field_name="end")
        if normalized_start >= normalized_end:
            raise ValueError("UTC window start must be earlier than end")
        object.__setattr__(self, "start", normalized_start)
        object.__setattr__(self, "end", normalized_end)

    def contains(self, value: datetime) -> bool:
        """Return whether one aware datetime belongs to this half-open window."""
        normalized = _aware_utc(value, field_name="value")
        return self.start <= normalized < self.end

    @property
    def logical_date(self) -> date:
        """Return the UTC calendar date on which the window begins."""
        return self.start.date()


@dataclass(frozen=True, slots=True)
class ModifiedTimeWindowPlan:
    """One immutable source manifest assigned to one UTC window."""

    source_uri: str
    source_window: UtcWindow
    source_manifest: SourceManifest
    discovery_seconds: float = field(default=0.0, compare=False)

    def __post_init__(self) -> None:
        """Validate source consistency and window membership."""
        if self.source_manifest.source_uri != self.source_uri:
            raise ValueError("source manifest URI does not match the window plan URI")
        for file in self.source_manifest.files:
            if file.updated is None or not self.source_window.contains(file.updated):
                raise ValueError(
                    "source manifest contains an object outside its modified-time window: "
                    f"{file.uri!r}"
                )
        object.__setattr__(self, "discovery_seconds", max(float(self.discovery_seconds), 0.0))

    @property
    def selected_object_count(self) -> int:
        """Return the number of objects assigned to this window."""
        return self.source_manifest.object_count

    @property
    def total_bytes(self) -> int | None:
        """Return the selected input byte total when every size is known."""
        return self.source_manifest.total_bytes

    @property
    def earliest_update(self) -> datetime | None:
        """Return the earliest selected object update."""
        return self.source_manifest.earliest_update

    @property
    def latest_update(self) -> datetime | None:
        """Return the latest selected object update."""
        return self.source_manifest.latest_update

    def to_partition_run_plan(self, output_uri: str) -> PartitionRunPlan:
        """Create a partition run plan carrying this frozen selection."""
        from .types import PartitionRunPlan

        return PartitionRunPlan(
            logical_date=self.source_window.logical_date,
            source_uri=self.source_uri,
            output_uri=output_uri,
            source_window=self.source_window,
            source_manifest=self.source_manifest,
            discovery_seconds=self.discovery_seconds,
        )


def build_utc_daily_windows(start_date: date, end_date: date) -> tuple[UtcWindow, ...]:
    """Build consecutive UTC days from inclusive calendar-date bounds.

    Every object exactly at ``00:00:00Z`` belongs to the day beginning at that
    instant. The final window ends at midnight after ``end_date``.
    """
    if isinstance(start_date, datetime) or not isinstance(start_date, date):
        raise TypeError("start_date must be a date, not a datetime")
    if isinstance(end_date, datetime) or not isinstance(end_date, date):
        raise TypeError("end_date must be a date, not a datetime")
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date")
    windows: list[UtcWindow] = []
    current = start_date
    while current <= end_date:
        start = datetime.combine(current, time.min, tzinfo=UTC)
        windows.append(UtcWindow(start, start + timedelta(days=1)))
        current += timedelta(days=1)
    return tuple(windows)


def select_remote_files_by_modified_time(
    files: Iterable[RemoteFile],
    source_window: UtcWindow,
) -> tuple[RemoteFile, ...]:
    """Select objects in ``[start, end)`` and order them by content identity."""
    selected: list[RemoteFile] = []
    for file in files:
        if not isinstance(file, RemoteFile):
            raise TypeError("modified-time discovery requires RemoteFile values")
        if file.updated is None:
            raise ValueError(f"remote object has no modification time: {file.uri!r}")
        if source_window.contains(file.updated):
            selected.append(file)
    return tuple(sorted(selected, key=remote_file_sort_key))


def _validated_windows(windows: Iterable[UtcWindow]) -> tuple[UtcWindow, ...]:
    """Return canonical non-overlapping windows sorted by UTC start."""
    values = tuple(windows)
    if any(not isinstance(window, UtcWindow) for window in values):
        raise TypeError("windows must contain only UtcWindow values")
    ordered = tuple(sorted(values, key=lambda window: (window.start, window.end)))
    for previous, current in zip(ordered, ordered[1:], strict=False):
        if current.start < previous.end:
            raise ValueError("modified-time planning windows must not overlap")
    return ordered


def plan_modified_time_windows_from_listing(
    source_uri: str,
    files: Iterable[RemoteFile],
    windows: Iterable[UtcWindow],
    *,
    include_empty: bool = False,
    discovery_seconds: float = 0.0,
) -> tuple[ModifiedTimeWindowPlan, ...]:
    """Group one already completed listing into deterministic UTC manifests."""
    if not isinstance(source_uri, str) or not source_uri.strip():
        raise ValueError("source_uri must be a non-empty string")
    ordered_windows = _validated_windows(windows)
    if not ordered_windows:
        return ()
    starts = tuple(window.start for window in ordered_windows)
    buckets: list[list[RemoteFile]] = [[] for _ in ordered_windows]
    identities: set[tuple[str, str | None]] = set()
    for file in files:
        if not isinstance(file, RemoteFile):
            raise TypeError("modified-time planning requires RemoteFile values")
        if file.updated is None:
            raise ValueError(f"remote object has no modification time: {file.uri!r}")
        updated = _aware_utc(file.updated, field_name=f"updated for {file.uri!r}")
        identity = file.content_identity
        if identity in identities:
            raise ValueError(f"listing contains duplicate content identity: {identity!r}")
        identities.add(identity)
        index = bisect_right(starts, updated) - 1
        if index >= 0 and updated < ordered_windows[index].end:
            buckets[index].append(file)

    elapsed = max(float(discovery_seconds), 0.0)
    plans: list[ModifiedTimeWindowPlan] = []
    for source_window, bucket in zip(ordered_windows, buckets, strict=True):
        if not bucket and not include_empty:
            continue
        manifest = SourceManifest(source_uri, bucket)
        plans.append(
            ModifiedTimeWindowPlan(
                source_uri=source_uri,
                source_window=source_window,
                source_manifest=manifest,
                discovery_seconds=elapsed if not plans else 0.0,
            )
        )
    return tuple(plans)


def _validate_gcs_prefix(source_uri: str) -> None:
    """Reject providers that do not expose the versioned metadata contract."""
    if remote_provider(source_uri) != "gcs":
        raise ValueError("modified-time prefix planning currently supports only GCS URIs")


def _plan_listed_gcs_files(
    source_uri: str,
    files: Sequence[RemoteFile],
    *,
    start_date: date,
    end_date: date,
    include_empty: bool,
    discovery_seconds: float,
) -> tuple[ModifiedTimeWindowPlan, ...]:
    """Validate generation identity and group one GCS listing."""
    for file in files:
        if not file.generation:
            raise ValueError(
                f"GCS modified-time planning requires an immutable object generation: {file.uri!r}"
            )
    return plan_modified_time_windows_from_listing(
        source_uri,
        files,
        build_utc_daily_windows(start_date, end_date),
        include_empty=include_empty,
        discovery_seconds=discovery_seconds,
    )


async def plan_gcs_modified_time_windows_async(
    source_uri: str,
    start_date: date,
    end_date: date,
    *,
    suffixes: Sequence[str] = ("csv",),
    include_empty: bool = False,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "multi",
) -> tuple[ModifiedTimeWindowPlan, ...]:
    """List one GCS prefix once and asynchronously build daily UTC plans."""
    _validate_gcs_prefix(source_uri)
    normalized_limit = normalize_memory_limit(memory_limit_bytes)
    mode = normalize_threading_mode(threading_mode)
    ledger = OperationMemoryLedger(normalized_limit)
    metadata_budget = DirectoryMetadataBudget(
        normalized_limit,
        operation_memory_ledger=ledger,
    )
    started = perf_counter()
    try:
        with activate_operation_memory_ledger(ledger):
            with directory_metadata_budget_scope(normalized_limit, budget=metadata_budget):
                files = await routing.list_remote_directory(
                    source_uri,
                    suffixes,
                    memory_limit_bytes=normalized_limit,
                    threading_mode=mode,
                )
        elapsed = max(perf_counter() - started, 0.0)
        return _plan_listed_gcs_files(
            source_uri,
            files,
            start_date=start_date,
            end_date=end_date,
            include_empty=include_empty,
            discovery_seconds=elapsed,
        )
    finally:
        metadata_budget.close()
        ledger.close()


def plan_gcs_modified_time_windows(
    source_uri: str,
    start_date: date,
    end_date: date,
    *,
    suffixes: Sequence[str] = ("csv",),
    include_empty: bool = False,
    memory_limit_bytes: int | None = None,
    threading_mode: str = "single",
) -> tuple[ModifiedTimeWindowPlan, ...]:
    """List one GCS prefix once and synchronously build daily UTC plans."""
    _validate_gcs_prefix(source_uri)
    normalized_limit = normalize_memory_limit(memory_limit_bytes)
    mode = normalize_threading_mode(threading_mode)
    if mode != "single":
        return run_sync(
            plan_gcs_modified_time_windows_async(
                source_uri,
                start_date,
                end_date,
                suffixes=suffixes,
                include_empty=include_empty,
                memory_limit_bytes=normalized_limit,
                threading_mode=mode,
            ),
            threading_mode=mode,
        )

    ledger = OperationMemoryLedger(normalized_limit)
    metadata_budget = DirectoryMetadataBudget(
        normalized_limit,
        operation_memory_ledger=ledger,
    )
    started = perf_counter()
    try:
        with activate_operation_memory_ledger(ledger):
            with directory_metadata_budget_scope(normalized_limit, budget=metadata_budget):
                files = sync_backend.list_remote_directory(
                    source_uri,
                    suffixes,
                    memory_limit_bytes=normalized_limit,
                )
        elapsed = max(perf_counter() - started, 0.0)
        return _plan_listed_gcs_files(
            source_uri,
            files,
            start_date=start_date,
            end_date=end_date,
            include_empty=include_empty,
            discovery_seconds=elapsed,
        )
    finally:
        metadata_budget.close()
        ledger.close()


__all__ = [
    "ModifiedTimeWindowPlan",
    "UtcWindow",
    "build_utc_daily_windows",
    "plan_gcs_modified_time_windows",
    "plan_gcs_modified_time_windows_async",
    "plan_modified_time_windows_from_listing",
    "select_remote_files_by_modified_time",
]
