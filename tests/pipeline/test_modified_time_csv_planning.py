"""UTC modified-time planning and source-manifest contracts.

It covers UTC normalization, half-open daily windows, exact timestamp boundaries,
immutable manifests, empty windows, and generation requirements.
"""

from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError
from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from schema_sanitizer.pipeline.modified_time import (
    ModifiedTimeWindowPlan,
    UtcWindow,
    build_utc_daily_windows,
    plan_gcs_modified_time_windows,
    plan_gcs_modified_time_windows_async,
    plan_modified_time_windows_from_listing,
    select_remote_files_by_modified_time,
)
from schema_sanitizer.sources import RemoteFile, SourceManifest


def _file(
    name: str,
    updated: datetime | None,
    *,
    generation: str,
    size: int = 10,
) -> RemoteFile:
    """Return one versioned fake GCS object."""
    return RemoteFile(
        uri=f"gs://bucket/events/{name}",
        name=name,
        size=size,
        updated=updated,
        generation=generation,
    )


def test_utc_window_normalizes_offsets_and_rejects_ambiguous_bounds() -> None:
    """Window bounds are aware UTC values and cannot be empty or naive."""
    window = UtcWindow(
        datetime(2026, 8, 1, 2, tzinfo=timezone(timedelta(hours=2))),
        datetime(2026, 8, 2, 2, tzinfo=timezone(timedelta(hours=2))),
    )

    assert window.start == datetime(2026, 8, 1, tzinfo=UTC)
    assert window.end == datetime(2026, 8, 2, tzinfo=UTC)
    with pytest.raises(ValueError, match="timezone-aware"):
        UtcWindow(datetime(2026, 8, 1), datetime(2026, 8, 2, tzinfo=UTC))
    with pytest.raises(ValueError, match="earlier"):
        UtcWindow(window.end, window.start)


def test_daily_windows_use_inclusive_dates_and_half_open_midnights() -> None:
    """A midnight object belongs only to the UTC day beginning at midnight."""
    windows = build_utc_daily_windows(date(2026, 8, 1), date(2026, 8, 3))

    assert len(windows) == 3
    assert windows[0].start == datetime(2026, 8, 1, tzinfo=UTC)
    assert windows[-1].end == datetime(2026, 8, 4, tzinfo=UTC)
    midnight = datetime(2026, 8, 2, tzinfo=UTC)
    assert not windows[0].contains(midnight)
    assert windows[1].contains(midnight)
    with pytest.raises(TypeError, match="date, not a datetime"):
        build_utc_daily_windows(midnight, date(2026, 8, 3))  # type: ignore[arg-type]


def test_modified_time_selection_enforces_exact_start_and_end_boundaries() -> None:
    """Start is inclusive, end is exclusive, and unrelated objects are omitted."""
    window = UtcWindow(
        datetime(2026, 8, 1, tzinfo=UTC),
        datetime(2026, 8, 2, tzinfo=UTC),
    )
    files = [
        _file("before.csv", window.start - timedelta(microseconds=1), generation="1"),
        _file("start.csv", window.start, generation="2"),
        _file("inside.csv", window.end - timedelta(microseconds=1), generation="3"),
        _file(
            "offset.csv",
            datetime(2026, 8, 1, 2, tzinfo=timezone(timedelta(hours=2))),
            generation="5",
        ),
        _file("end.csv", window.end, generation="4"),
    ]

    selected = select_remote_files_by_modified_time(reversed(files), window)

    assert [file.name for file in selected] == ["inside.csv", "offset.csv", "start.csv"]


def test_modified_time_selection_rejects_missing_or_naive_object_times() -> None:
    """Objects without an unambiguous instant cannot enter a UTC plan."""
    window = UtcWindow(
        datetime(2026, 8, 1, tzinfo=UTC),
        datetime(2026, 8, 2, tzinfo=UTC),
    )

    with pytest.raises(ValueError, match="no modification time"):
        select_remote_files_by_modified_time(
            [_file("missing.csv", None, generation="1")],
            window,
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        select_remote_files_by_modified_time(
            [_file("naive.csv", datetime(2026, 8, 1), generation="1")],
            window,
        )


def test_source_manifest_is_immutable_ordered_and_version_aware() -> None:
    """A manifest freezes exact generations in canonical identity order."""
    later = datetime(2026, 8, 1, 12, tzinfo=UTC)
    files = [
        _file("b.csv", later, generation="9", size=7),
        _file("a.csv", later, generation="9", size=11),
        _file("a.csv", later, generation="2", size=13),
    ]

    manifest = SourceManifest("gs://bucket/events", files)

    assert manifest.content_identities == (
        ("gs://bucket/events/a.csv", "2"),
        ("gs://bucket/events/a.csv", "9"),
        ("gs://bucket/events/b.csv", "9"),
    )
    assert manifest.object_count == 3
    assert manifest.total_bytes == 31
    assert manifest.earliest_update == later
    assert manifest.latest_update == later
    offset_manifest = SourceManifest(
        manifest.source_uri,
        [
            _file(
                "offset.csv",
                datetime(2026, 8, 1, 14, tzinfo=timezone(timedelta(hours=2))),
                generation="10",
            )
        ],
    )
    offset_earliest = offset_manifest.earliest_update
    assert offset_earliest == later
    assert offset_earliest is not None
    assert offset_earliest.tzinfo is UTC
    with pytest.raises(FrozenInstanceError):
        manifest.source_uri = "gs://other"  # type: ignore[misc]
    with pytest.raises(ValueError, match="duplicate content identity"):
        SourceManifest("gs://bucket/events", [files[0], files[0]])
    with pytest.raises(ValueError, match="timezone-aware"):
        SourceManifest(
            "gs://bucket/events",
            [_file("naive.csv", datetime(2026, 8, 1), generation="11")],
        )


def test_one_listing_builds_distinct_deterministic_daily_manifests() -> None:
    """One immutable listing is distributed locally without duplicate membership."""
    windows = build_utc_daily_windows(date(2026, 8, 1), date(2026, 8, 3))
    files = [
        _file("outside-before.csv", windows[0].start - timedelta(seconds=1), generation="0"),
        _file("day1-z.csv", windows[0].end - timedelta(seconds=1), generation="3", size=30),
        _file("day1-a.csv", windows[0].start, generation="1", size=10),
        _file("day2.csv", windows[1].start, generation="2", size=20),
        _file("outside-after.csv", windows[-1].end, generation="4"),
    ]

    first = plan_modified_time_windows_from_listing(
        "gs://bucket/events",
        files,
        windows,
    )
    second = plan_modified_time_windows_from_listing(
        "gs://bucket/events",
        reversed(files),
        reversed(windows),
    )

    assert first == second
    assert [plan.source_window.logical_date for plan in first] == [
        date(2026, 8, 1),
        date(2026, 8, 2),
    ]
    assert first[0].source_uri == first[1].source_uri
    assert first[0].source_window != first[1].source_window
    assert first[0].source_manifest.content_identities == (
        ("gs://bucket/events/day1-a.csv", "1"),
        ("gs://bucket/events/day1-z.csv", "3"),
    )
    assert first[0].selected_object_count == 2
    assert first[0].total_bytes == 40
    assert first[0].earliest_update == windows[0].start
    assert first[0].latest_update == windows[0].end - timedelta(seconds=1)
    selected = [identity for plan in first for identity in plan.source_manifest.content_identities]
    assert len(selected) == len(set(selected)) == 3


def test_empty_windows_can_be_skipped_or_retained_explicitly() -> None:
    """Empty days are not errors and are omitted by default."""
    windows = build_utc_daily_windows(date(2026, 8, 1), date(2026, 8, 2))
    files = [_file("day1.csv", windows[0].start, generation="1")]

    skipped = plan_modified_time_windows_from_listing("gs://bucket/events", files, windows)
    retained = plan_modified_time_windows_from_listing(
        "gs://bucket/events", files, windows, include_empty=True
    )

    assert len(skipped) == 1
    assert len(retained) == 2
    assert retained[1].source_manifest.files == ()
    assert retained[1].selected_object_count == 0
    assert retained[1].total_bytes == 0


def test_overlapping_windows_are_rejected_before_assignment() -> None:
    """A file can never be assigned to two overlapping execution windows."""
    first = UtcWindow(
        datetime(2026, 8, 1, tzinfo=UTC),
        datetime(2026, 8, 2, tzinfo=UTC),
    )
    overlap = UtcWindow(
        datetime(2026, 8, 1, 12, tzinfo=UTC),
        datetime(2026, 8, 2, 12, tzinfo=UTC),
    )

    with pytest.raises(ValueError, match="must not overlap"):
        plan_modified_time_windows_from_listing("gs://bucket/events", [], [first, overlap])


def test_window_plan_populates_partition_run_telemetry_and_preserves_it() -> None:
    """Run plans carry the exact window, manifest, and source summary values."""
    window = build_utc_daily_windows(date(2026, 8, 1), date(2026, 8, 1))[0]
    manifest = SourceManifest(
        "gs://bucket/events",
        [_file("a.csv", window.start, generation="1", size=17)],
    )
    window_plan = ModifiedTimeWindowPlan(
        source_uri=manifest.source_uri,
        source_window=window,
        source_manifest=manifest,
        discovery_seconds=1.25,
    )

    run_plan = window_plan.to_partition_run_plan("gs://bucket/out/2026-08-01.parquet")
    updated = run_plan.with_discovered_input(object())
    timed = run_plan.with_discovery_timing(None, 2.5)

    assert run_plan.source_window is window
    assert run_plan.source_manifest is manifest
    assert run_plan.source_file_count == 1
    assert run_plan.source_bytes == 17
    assert run_plan.source_earliest_update == window.start
    assert run_plan.source_latest_update == window.start
    assert run_plan.discovery_seconds == pytest.approx(1.25)
    assert updated.source_manifest is manifest
    assert timed.source_window is window
    assert timed.source_file_count == 1
    assert timed.source_bytes == 17

    afternoon = UtcWindow(window.start + timedelta(hours=12), window.end)
    afternoon_manifest = SourceManifest(
        manifest.source_uri,
        [_file("b.csv", afternoon.start, generation="2", size=5)],
    )
    same_identity_fields = ModifiedTimeWindowPlan(
        source_uri=manifest.source_uri,
        source_window=afternoon,
        source_manifest=afternoon_manifest,
    ).to_partition_run_plan(run_plan.output_uri)
    assert same_identity_fields.logical_date == run_plan.logical_date
    assert same_identity_fields.source_uri == run_plan.source_uri
    assert same_identity_fields.output_uri == run_plan.output_uri
    assert same_identity_fields != run_plan


def test_sync_gcs_planner_lists_prefix_once_and_skips_empty_days(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The public blocking planner performs one prefix listing for all days."""
    calls: list[tuple[str, tuple[str, ...], int | None]] = []
    windows = build_utc_daily_windows(date(2026, 8, 1), date(2026, 8, 3))
    files = [
        _file("day1.csv", windows[0].start, generation="1"),
        _file("day3.csv", windows[2].start, generation="2"),
    ]

    def list_once(
        uri: str,
        suffixes: tuple[str, ...],
        *,
        memory_limit_bytes: int | None = None,
    ) -> list[RemoteFile]:
        """Return the fixed listing and record the single sync call."""
        calls.append((uri, tuple(suffixes), memory_limit_bytes))
        return files

    monkeypatch.setattr(
        "schema_sanitizer.pipeline.modified_time.sync_backend.list_remote_directory",
        list_once,
    )

    plans = plan_gcs_modified_time_windows(
        "gs://bucket/events",
        date(2026, 8, 1),
        date(2026, 8, 3),
        suffixes=("csv",),
        memory_limit_bytes=8 * 1024 * 1024,
    )

    assert len(calls) == 1
    assert calls[0][:2] == ("gs://bucket/events", ("csv",))
    assert [plan.source_window.logical_date for plan in plans] == [
        date(2026, 8, 1),
        date(2026, 8, 3),
    ]
    assert plans[0].discovery_seconds >= 0.0
    assert plans[1].discovery_seconds == 0.0


def test_async_gcs_planner_lists_prefix_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """The asynchronous facade preserves the same one-listing contract."""
    calls = 0
    window = build_utc_daily_windows(date(2026, 8, 1), date(2026, 8, 1))[0]

    async def list_once(*_args: object, **_kwargs: object) -> list[RemoteFile]:
        """Return one asynchronous listing and count invocations."""
        nonlocal calls
        calls += 1
        return [_file("a.csv", window.start, generation="1")]

    monkeypatch.setattr(
        "schema_sanitizer.pipeline.modified_time.routing.list_remote_directory",
        list_once,
    )

    plans = asyncio.run(
        plan_gcs_modified_time_windows_async(
            "gs://bucket/events",
            date(2026, 8, 1),
            date(2026, 8, 1),
        )
    )

    assert calls == 1
    assert len(plans) == 1


def test_gcs_planner_requires_generation_and_rejects_other_providers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Window plans never weaken GCS identity to URI-only contents."""
    monkeypatch.setattr(
        "schema_sanitizer.pipeline.modified_time.sync_backend.list_remote_directory",
        lambda *_args, **_kwargs: [
            RemoteFile(
                "gs://bucket/events/a.csv",
                "a.csv",
                1,
                updated=datetime(2026, 8, 1, tzinfo=UTC),
            )
        ],
    )

    with pytest.raises(ValueError, match="immutable object generation"):
        plan_gcs_modified_time_windows("gs://bucket/events", date(2026, 8, 1), date(2026, 8, 1))
    with pytest.raises(ValueError, match="only GCS"):
        plan_gcs_modified_time_windows("s3://bucket/events", date(2026, 8, 1), date(2026, 8, 1))
