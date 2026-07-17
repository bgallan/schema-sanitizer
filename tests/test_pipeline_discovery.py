"""Pipeline source discovery tests."""

# ruff: noqa: F405

from __future__ import annotations

from pipeline_shared import *  # noqa: F403

from schema_sanitizer.input_impl.directory_inputs import (
    DirectoryDiscovery,
    FolderFile,
    RemoteFile,
)


def test_pipeline_hive_range_plan_renders_hourly_prefixes() -> None:
    """Verify reusable Hive planning renders source and output partitions."""
    plans = build_hive_range_plan(
        HiveRangeConfig(
            source_prefix="gs://raw/events/rt",
            output_prefix="gs://silver/events/rt",
            start_date=date(2026, 6, 25),
            end_date=date(2026, 6, 25),
            start_hour=8,
            end_hour=9,
            partition_granularity="hourly",
            input_format="jsonl",
        )
    )

    assert [plan.label for plan in plans] == [
        "2026-06-25/hour=08",
        "2026-06-25/hour=09",
    ]
    assert plans[0].source_uri == (
        "gs://raw/events/rt/year=2026/month=06/date=2026-06-25/hour=08/events_20260625_08.jsonl"
    )


def test_pipeline_source_discovery_filters_local_single_files(tmp_path) -> None:
    """Verify source discovery filters missing local files."""
    existing = tmp_path / "existing.jsonl"
    missing = tmp_path / "missing.jsonl"
    existing.write_text('{"ok": true}\n', encoding="utf-8")
    plans = [
        PartitionRunPlan(date(2026, 1, 1), str(existing), str(tmp_path / "a.parquet")),
        PartitionRunPlan(date(2026, 1, 2), str(missing), str(tmp_path / "b.parquet")),
    ]

    discovery = discover_existing_source_plans(
        plans,
        input_mode="single_file",
        input_format="jsonl",
    )

    assert discovery.existing_plans == [plans[0]]
    assert discovery.skipped_plans == [plans[1]]
    assert discovery.existing_plans[0].source_file_count == 1
    assert discovery.existing_plans[0].source_bytes == existing.stat().st_size


def test_pipeline_source_discovery_records_per_source_latency(monkeypatch, tmp_path) -> None:
    """Selected plans must carry discovery latency into partition accounting."""
    import schema_sanitizer.pipeline.source_discovery as source_discovery_mod

    existing = tmp_path / "existing.jsonl"
    existing.write_text('{"ok": true}\n', encoding="utf-8")
    plan = PartitionRunPlan(
        date(2026, 1, 1),
        str(existing),
        str(tmp_path / "out.parquet"),
    )
    clock = iter((10.0, 12.5))
    monkeypatch.setattr(source_discovery_mod, "perf_counter", lambda: next(clock))

    discovery = discover_existing_source_plans(
        [plan],
        input_mode="single_file",
        input_format="jsonl",
    )

    assert discovery.existing_plans[0].discovery_seconds == pytest.approx(2.5)


def test_pipeline_source_discovery_filters_local_directories(tmp_path) -> None:
    """Verify directory source discovery checks direct child extensions."""
    populated = tmp_path / "populated"
    empty = tmp_path / "empty"
    populated.mkdir()
    empty.mkdir()
    (populated / "row.jsonl").write_text('{"ok": true}\n', encoding="utf-8")
    (populated / "ignored.txt").write_text("ignored", encoding="utf-8")
    plans = [
        PartitionRunPlan(date(2026, 1, 1), str(populated), str(tmp_path / "a.parquet")),
        PartitionRunPlan(date(2026, 1, 2), str(empty), str(tmp_path / "b.parquet")),
    ]

    discovery = discover_existing_source_plans(
        plans,
        input_mode="directory",
        input_format="jsonl",
    )

    assert discovery.existing_plans == [plans[0]]
    assert discovery.skipped_plans == [plans[1]]
    assert discovery.existing_plans[0].source_file_count == 1
    assert discovery.existing_plans[0].source_bytes == (populated / "row.jsonl").stat().st_size


def test_pipeline_runner_reuses_discovered_local_directory_files(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Verify directory discovery files are reused during partition conversion."""
    pq = pytest.importorskip("pyarrow.parquet")
    import schema_sanitizer.api_impl.input.directory_preparation as directory_native

    source = tmp_path / "source"
    source.mkdir()
    child = source / "part.jsonl"
    child.write_text('{"id": 1}\n', encoding="utf-8")
    output = tmp_path / "out.parquet"
    plan = PartitionRunPlan(date(2026, 1, 1), str(source), str(output))

    discovery = discover_existing_source_plans(
        [plan],
        input_mode="directory",
        input_format="jsonl",
    )
    discovered_plan = discovery.existing_plans[0]
    assert discovered_plan.discovered_input is not None

    def fail_folder_files(*_args, **_kwargs):
        """Fail if conversion relists the already-discovered source directory."""
        raise AssertionError("directory should not be listed again during conversion")

    monkeypatch.setattr(directory_native, "folder_files", fail_folder_files)

    run_partitioned_to_parquet_registry_json(
        [discovered_plan],
        initial_schema_registry_json="{}",
        to_parquet_kwargs={
            "input_format": "jsonl",
            "input_mode": "directory",
            "field_name_policy": "lower_alpha",
        },
    )

    rows = pq.read_table(output).to_pylist()
    assert rows[0]["id"] == 1
    assert rows[0]["source_file"] == str(child.resolve())


def test_pipeline_source_discovery_uses_gcs_bulk_directory_checks(monkeypatch) -> None:
    """Verify GCS directory discovery avoids one remote list call per partition."""
    import schema_sanitizer.pipeline.source_discovery as source_discovery_mod

    plans = [
        PartitionRunPlan(
            date(2026, 1, 1),
            f"gs://bucket/events/date=2026-01-01/hour={hour:02d}",
            f"gs://bucket/out/date=2026-01-01/hour={hour:02d}/part.parquet",
            logical_hour=hour,
        )
        for hour in (0, 1, 2)
    ]
    captured: dict[str, object] = {}

    async def fake_bulk(
        uris: list[str],
        extensions: tuple[str, ...],
        *,
        memory_limit_bytes: int | None = None,
    ) -> DirectoryDiscovery[RemoteFile]:
        """Capture bulk discovery inputs and return one missing hour."""
        captured["uris"] = uris
        captured["extensions"] = extensions
        exists_by_uri = {uri: not uri.endswith("hour=01") for uri in uris}
        return DirectoryDiscovery(
            exists_by_uri=exists_by_uri,
            files_by_uri={uri: [] for uri in uris},
        )

    async def fail_individual_directory_list(*_args, **_kwargs):
        """Fail if the per-directory remote listing path is used."""
        raise AssertionError("per-directory remote listing should not run for GCS")

    monkeypatch.setattr(source_discovery_mod.gcs, "directories_containing_files", fake_bulk)
    monkeypatch.setattr(
        source_discovery_mod.routing, "list_remote_directory", fail_individual_directory_list
    )

    discovery = discover_existing_source_plans(
        plans,
        input_mode="directory",
        input_format="json",
    )

    assert captured["uris"] == [plan.source_uri for plan in plans]
    assert captured["extensions"] == ("json",)
    assert [plan.label for plan in discovery.existing_plans] == [
        "2026-01-01/hour=00",
        "2026-01-01/hour=02",
    ]
    assert [plan.label for plan in discovery.skipped_plans] == ["2026-01-01/hour=01"]


def test_pipeline_source_discovery_uses_s3_bulk_directory_checks(monkeypatch) -> None:
    """Verify S3 directory discovery avoids one remote list call per partition."""
    import schema_sanitizer.pipeline.source_discovery as source_discovery_mod

    plans = [
        PartitionRunPlan(
            date(2026, 1, 1),
            f"s3://bucket/events/date=2026-01-01/hour={hour:02d}",
            f"s3://bucket/out/date=2026-01-01/hour={hour:02d}/part.parquet",
            logical_hour=hour,
        )
        for hour in (0, 1, 2)
    ]
    captured: dict[str, object] = {}

    async def fake_bulk(
        uris: list[str],
        extensions: tuple[str, ...],
        *,
        memory_limit_bytes: int | None = None,
    ) -> DirectoryDiscovery[RemoteFile]:
        """Capture bulk discovery inputs and return one missing hour."""
        captured["uris"] = uris
        captured["extensions"] = extensions
        exists_by_uri = {uri: not uri.endswith("hour=01") for uri in uris}
        return DirectoryDiscovery(
            exists_by_uri=exists_by_uri,
            files_by_uri={uri: [] for uri in uris},
        )

    async def fail_individual_directory_list(*_args, **_kwargs):
        """Fail if the per-directory remote listing path is used."""
        raise AssertionError("per-directory remote listing should not run for S3")

    monkeypatch.setattr(source_discovery_mod.s3, "directories_containing_files", fake_bulk)
    monkeypatch.setattr(
        source_discovery_mod.routing, "list_remote_directory", fail_individual_directory_list
    )

    discovery = discover_existing_source_plans(
        plans,
        input_mode="directory",
        input_format="json",
    )

    assert captured["uris"] == [plan.source_uri for plan in plans]
    assert captured["extensions"] == ("json",)
    assert [plan.label for plan in discovery.existing_plans] == [
        "2026-01-01/hour=00",
        "2026-01-01/hour=02",
    ]
    assert [plan.label for plan in discovery.skipped_plans] == ["2026-01-01/hour=01"]


def test_pipeline_source_discovery_uses_azure_bulk_directory_checks(monkeypatch) -> None:
    """Verify Azure directory discovery avoids one remote list call per partition."""
    import schema_sanitizer.pipeline.source_discovery as source_discovery_mod

    plans = [
        PartitionRunPlan(
            date(2026, 1, 1),
            f"azure://account/container/events/date=2026-01-01/hour={hour:02d}",
            f"azure://account/container/out/date=2026-01-01/hour={hour:02d}/part.parquet",
            logical_hour=hour,
        )
        for hour in (0, 1, 2)
    ]
    captured: dict[str, object] = {}

    async def fake_bulk(
        uris: list[str],
        extensions: tuple[str, ...],
        *,
        memory_limit_bytes: int | None = None,
    ) -> DirectoryDiscovery[RemoteFile]:
        """Capture bulk discovery inputs and return one missing hour."""
        captured["uris"] = uris
        captured["extensions"] = extensions
        exists_by_uri = {uri: not uri.endswith("hour=01") for uri in uris}
        return DirectoryDiscovery(
            exists_by_uri=exists_by_uri,
            files_by_uri={uri: [] for uri in uris},
        )

    async def fail_individual_directory_list(*_args, **_kwargs):
        """Fail if the per-directory remote listing path is used."""
        raise AssertionError("per-directory remote listing should not run for Azure")

    monkeypatch.setattr(source_discovery_mod.azure, "directories_containing_files", fake_bulk)
    monkeypatch.setattr(
        source_discovery_mod.routing, "list_remote_directory", fail_individual_directory_list
    )

    discovery = discover_existing_source_plans(
        plans,
        input_mode="directory",
        input_format="json",
    )

    assert captured["uris"] == [plan.source_uri for plan in plans]
    assert captured["extensions"] == ("json",)
    assert [plan.label for plan in discovery.existing_plans] == [
        "2026-01-01/hour=00",
        "2026-01-01/hour=02",
    ]
    assert [plan.label for plan in discovery.skipped_plans] == ["2026-01-01/hour=01"]


def test_pipeline_source_discovery_uses_local_grouped_directory_checks(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Verify local directory discovery uses grouped checks for partition ranges."""
    import schema_sanitizer.pipeline.source_discovery as source_discovery_mod

    plans = [
        PartitionRunPlan(
            date(2026, 1, 1),
            str(tmp_path / f"hour={hour:02d}"),
            str(tmp_path / f"out/hour={hour:02d}/part.parquet"),
            logical_hour=hour,
        )
        for hour in (0, 1, 2)
    ]
    captured: dict[str, object] = {}

    def fake_bulk(
        locations: dict[str, str], extensions: tuple[str, ...]
    ) -> DirectoryDiscovery[FolderFile]:
        """Capture grouped local discovery inputs and return one missing hour."""
        captured["locations"] = locations
        captured["extensions"] = extensions
        exists_by_uri = {uri: not uri.endswith("hour=01") for uri in locations}
        return DirectoryDiscovery(
            exists_by_uri=exists_by_uri,
            files_by_uri={uri: [] for uri in locations},
        )

    monkeypatch.setattr(source_discovery_mod, "_local_directories_containing_files", fake_bulk)

    discovery = discover_existing_source_plans(
        plans,
        input_mode="directory",
        input_format="json",
    )

    assert captured["locations"] == {plan.source_uri: "path" for plan in plans}
    assert captured["extensions"] == ("json",)
    assert [plan.label for plan in discovery.existing_plans] == [
        "2026-01-01/hour=00",
        "2026-01-01/hour=02",
    ]
    assert [plan.label for plan in discovery.skipped_plans] == ["2026-01-01/hour=01"]


def test_pipeline_source_discovery_accepts_windows_drive_paths() -> None:
    """Verify Windows drive-letter paths are treated as local paths, not URI schemes."""
    plan = PartitionRunPlan(
        date(2026, 1, 1),
        r"C:\missing\events.jsonl",
        r"C:\output\events.parquet",
    )

    discovery = discover_existing_source_plans(
        [plan],
        input_mode="single_file",
        input_format="jsonl",
    )

    assert discovery.existing_plans == []
    assert discovery.skipped_plans == [plan]
