"""Pipeline source discovery tests."""

from __future__ import annotations

from contextlib import nullcontext
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

import schema_sanitizer.pipeline.partition_execution as partition_execution
from schema_sanitizer.core_impl import hive_uris
from schema_sanitizer.input_impl.directory_inputs import (
    DirectoryDiscovery,
    FolderFile,
)
from schema_sanitizer.pipeline.hive import HiveRangeConfig, build_hive_range_plan
from schema_sanitizer.pipeline.partition_execution import (
    run_partitioned_to_parquet_registry_json,
)
from schema_sanitizer.pipeline.source_discovery import (
    _unique_source_locations,
    discover_existing_source_plans,
)
from schema_sanitizer.pipeline.types import PartitionRunPlan, SchemaRegistryState
from schema_sanitizer.sources import RemoteFile


def test_pipeline_hive_range_plan_renders_hourly_prefixes() -> None:
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
    import schema_sanitizer.pipeline.source_discovery_sync as source_discovery_mod

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
    import schema_sanitizer.pipeline.source_discovery_sync as source_discovery_mod

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

    def fake_bulk(
        provider: str,
        uris: list[str],
        extensions: tuple[str, ...],
        *,
        memory_limit_bytes: int | None = None,
    ) -> DirectoryDiscovery[RemoteFile]:
        """Capture bulk discovery inputs and return one missing hour."""
        assert provider == "gcs"
        captured["uris"] = uris
        captured["extensions"] = extensions
        exists_by_uri = {uri: not uri.endswith("hour=01") for uri in uris}
        return DirectoryDiscovery(
            exists_by_uri=exists_by_uri,
            files_by_uri={uri: [] for uri in uris},
        )

    def fail_individual_directory_list(*_args, **_kwargs):
        """Fail if the per-directory remote listing path is used."""
        raise AssertionError("per-directory remote listing should not run for GCS")

    monkeypatch.setattr(
        source_discovery_mod.sync_backend, "directories_containing_files", fake_bulk
    )
    monkeypatch.setattr(
        source_discovery_mod.sync_backend, "list_remote_directory", fail_individual_directory_list
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
    import schema_sanitizer.pipeline.source_discovery_sync as source_discovery_mod

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

    def fake_bulk(
        provider: str,
        uris: list[str],
        extensions: tuple[str, ...],
        *,
        memory_limit_bytes: int | None = None,
    ) -> DirectoryDiscovery[RemoteFile]:
        """Capture bulk discovery inputs and return one missing hour."""
        assert provider == "s3"
        captured["uris"] = uris
        captured["extensions"] = extensions
        exists_by_uri = {uri: not uri.endswith("hour=01") for uri in uris}
        return DirectoryDiscovery(
            exists_by_uri=exists_by_uri,
            files_by_uri={uri: [] for uri in uris},
        )

    def fail_individual_directory_list(*_args, **_kwargs):
        """Fail if the per-directory remote listing path is used."""
        raise AssertionError("per-directory remote listing should not run for S3")

    monkeypatch.setattr(
        source_discovery_mod.sync_backend, "directories_containing_files", fake_bulk
    )
    monkeypatch.setattr(
        source_discovery_mod.sync_backend, "list_remote_directory", fail_individual_directory_list
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
    import schema_sanitizer.pipeline.source_discovery_sync as source_discovery_mod

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

    def fake_bulk(
        provider: str,
        uris: list[str],
        extensions: tuple[str, ...],
        *,
        memory_limit_bytes: int | None = None,
    ) -> DirectoryDiscovery[RemoteFile]:
        """Capture bulk discovery inputs and return one missing hour."""
        assert provider == "azure"
        captured["uris"] = uris
        captured["extensions"] = extensions
        exists_by_uri = {uri: not uri.endswith("hour=01") for uri in uris}
        return DirectoryDiscovery(
            exists_by_uri=exists_by_uri,
            files_by_uri={uri: [] for uri in uris},
        )

    def fail_individual_directory_list(*_args, **_kwargs):
        """Fail if the per-directory remote listing path is used."""
        raise AssertionError("per-directory remote listing should not run for Azure")

    monkeypatch.setattr(
        source_discovery_mod.sync_backend, "directories_containing_files", fake_bulk
    )
    monkeypatch.setattr(
        source_discovery_mod.sync_backend, "list_remote_directory", fail_individual_directory_list
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
    import schema_sanitizer.pipeline.source_discovery_sync as source_discovery_mod

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


def test_hive_output_validation_reports_each_duplicate() -> None:
    """Duplicate outputs fail with the complete conflicting set."""
    from schema_sanitizer.pipeline.hive import _validate_unique_outputs

    plans = [PartitionRunPlan(None, f"input-{index}", f"output-{index % 2}") for index in range(4)]

    with pytest.raises(ValueError, match="output-0.*output-1"):
        _validate_unique_outputs(plans)


def test_hive_partition_points_flow_into_the_plan() -> None:
    """Hourly range planning preserves endpoints across multiple days."""
    plans = build_hive_range_plan(
        HiveRangeConfig(
            source_prefix="gs://bucket/raw/events",
            output_prefix="gs://bucket/silver/events",
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 2),
            partition_granularity="hourly",
            start_hour=22,
            end_hour=23,
            input_format="jsonl",
        )
    )

    assert len(plans) == 4
    assert plans[0].source_uri.endswith("events_20260701_22.jsonl")
    assert plans[-1].output_uri.endswith("events_20260702_23.parquet")


def test_hive_template_analysis_is_cached() -> None:
    """Repeated partitions reuse template and URI-prefix analysis."""
    hive_uris._template_fields.cache_clear()
    hive_uris.normalize_uri_prefix.cache_clear()
    template = "gs://bucket/year={year}/month={month}/date={date}/part_{yyyymmdd}.jsonl"

    for day in range(1, 5):
        hive_uris.render_uri_for_partition(template, date(2026, 7, day), None)
        hive_uris.build_partition_directory_uri(
            "gs://bucket/table/",
            date(2026, 7, day),
            logical_hour=None,
        )

    assert hive_uris._template_fields.cache_info().hits >= 3
    assert hive_uris.normalize_uri_prefix.cache_info().hits >= 3


def test_hive_uri_value_expansion_is_shared() -> None:
    """File and directory routes reuse one partition-value expansion."""
    hive_uris._uri_template_values.cache_clear()
    hive_uris.build_partitioned_file_uri(
        "gs://bucket/table",
        date(2026, 7, 12),
        logical_hour=3,
        file_name_prefix="part",
        extension="parquet",
    )
    hive_uris.build_partition_directory_uri(
        "gs://bucket/table",
        date(2026, 7, 12),
        logical_hour=3,
    )

    assert hive_uris._uri_template_values.cache_info().hits >= 1


def test_static_partition_kwargs_remain_live(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A caller can deliberately mutate shared options between partitions."""
    options = {"input_format": "jsonl", "field_name_policy": "preserve"}
    initial_state = object()
    seen_policies: list[str] = []
    monkeypatch.setattr(
        partition_execution,
        "native_registry_state_context",
        lambda _state: nullcontext(),
    )
    monkeypatch.setattr(
        partition_execution,
        "discovered_directory_input_context",
        lambda _uri, _discovered: nullcontext(),
    )

    def fake_to_parquet(*_args: object, **kwargs: object) -> SimpleNamespace:
        seen_policies.append(str(kwargs["field_name_policy"]))
        return SimpleNamespace(
            stats={},
            schema_registry_json=None,
            schema_drifts_json="[]",
            native_registry_state=initial_state,
        )

    def mutate_options(*_args: object) -> None:
        options["field_name_policy"] = "lower_snake"

    monkeypatch.setattr(partition_execution, "to_parquet", fake_to_parquet)
    plans = [
        PartitionRunPlan(None, f"source-{index}", str(tmp_path / f"{index}.parquet"))
        for index in range(2)
    ]
    partition_execution.run_partitioned_to_parquet_registry_json(
        plans,
        initial_schema_registry_json="{}",
        initial_schema_registry_state=SchemaRegistryState(
            schema_registry_json="{}",
            native_registry_state=initial_state,
        ),
        to_parquet_kwargs=options,
        after_partition=mutate_options,
    )

    assert seen_policies == ["preserve", "lower_snake"]


def test_source_plan_deduplication_keeps_first_seen_uris() -> None:
    """Discovery classifies a repeated source URI only once."""
    plans = [
        PartitionRunPlan(date(2026, 1, 1), "gs://bucket/a", "out-a"),
        PartitionRunPlan(date(2026, 1, 2), "gs://bucket/b", "out-b"),
        PartitionRunPlan(date(2026, 1, 3), "gs://bucket/a", "out-c"),
    ]

    assert _unique_source_locations(plans) == {
        "gs://bucket/a": "gcs",
        "gs://bucket/b": "gcs",
    }
    with pytest.raises(ValueError, match="Unsupported source URI scheme: 'hdfs'"):
        _unique_source_locations([PartitionRunPlan(date(2026, 1, 1), "hdfs://cluster/a", "out")])
