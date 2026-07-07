"""Tests for reusable partition pipeline helpers."""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from schema_sanitizer.pipeline import (
    HiveRangeConfig,
    PartitionRunPlan,
    SchemaRegistryState,
    build_hive_range_plan,
    compact_stats_for_log,
    diff_flat_schema_paths,
    discover_existing_source_plans,
    format_duration,
    infer_warm_up_schema_registry,
    infer_warm_up_schema_registry_json,
    infer_warm_up_schema_registry_state,
    parse_final_schema_registry,
    read_parquet_schema,
    run_partitioned_to_parquet,
    run_partitioned_to_parquet_registry_json,
    run_partitioned_to_parquet_registry_state,
)


def _write_warm_up_source(
    tmp_path: Path,
    input_format: str,
    input_mode: str,
    name: str,
    field_name: str,
) -> Path:
    """Write one warm-up source for a public input format/mode pair."""
    folder = tmp_path / f"{input_format}-{input_mode}-{name}"
    if input_mode == "directory":
        folder.mkdir()
        path = folder / f"part.{_input_suffix(input_format)}"
    else:
        path = folder.with_suffix(f".{_input_suffix(input_format)}")

    if input_format == "csv":
        path.write_text(
            f"alpha,beta\n{1 if field_name == 'alpha' else ''},{2 if field_name == 'beta' else ''}\n",
            encoding="utf-8",
        )
    elif input_format == "json":
        path.write_text(f'{{"{field_name}": 1}}', encoding="utf-8")
    elif input_format == "json_array":
        path.write_text(f'[{{"{field_name}": 1}}]', encoding="utf-8")
    elif input_format in {"jsonl", "ndjson"}:
        path.write_text(f'{{"{field_name}": 1}}\n', encoding="utf-8")
    elif input_format == "xml":
        path.write_text(f"<row><{field_name}>1</{field_name}></row>", encoding="utf-8")
    elif input_format == "parquet":
        pa = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")
        table = pa.table({field_name: [1]})
        pq.write_table(table, path)
    else:  # pragma: no cover - exhaustive guard for future parametrization changes
        raise AssertionError(f"Unhandled input_format={input_format!r}")
    return path if input_mode == "single_file" else folder


def _input_suffix(input_format: str) -> str:
    """Return the public suffix for one input format."""
    if input_format == "json_array":
        return "json"
    if input_format == "parquet":
        return "parquet"
    return input_format


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


def test_pipeline_runner_reuses_discovered_local_directory_files(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Verify directory discovery files are reused during partition conversion."""
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import public_input

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

    monkeypatch.setattr(public_input, "folder_files", fail_folder_files)

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
    from schema_sanitizer.pipeline import discovery as discovery_mod

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

    async def fake_bulk(uris: list[str], extensions: tuple[str, ...]) -> dict[str, bool]:
        """Capture bulk discovery inputs and return one missing hour."""
        captured["uris"] = uris
        captured["extensions"] = extensions
        return {uri: not uri.endswith("hour=01") for uri in uris}

    async def fail_individual_directory_list(*_args, **_kwargs):
        """Fail if the per-directory remote listing path is used."""
        raise AssertionError("per-directory remote listing should not run for GCS")

    monkeypatch.setattr(discovery_mod, "_gcs_directories_containing_files", fake_bulk)
    monkeypatch.setattr(discovery_mod, "_list_remote_directory", fail_individual_directory_list)

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
    from schema_sanitizer.pipeline import discovery as discovery_mod

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

    async def fake_bulk(uris: list[str], extensions: tuple[str, ...]) -> dict[str, bool]:
        """Capture bulk discovery inputs and return one missing hour."""
        captured["uris"] = uris
        captured["extensions"] = extensions
        return {uri: not uri.endswith("hour=01") for uri in uris}

    async def fail_individual_directory_list(*_args, **_kwargs):
        """Fail if the per-directory remote listing path is used."""
        raise AssertionError("per-directory remote listing should not run for S3")

    monkeypatch.setattr(discovery_mod, "_s3_directories_containing_files", fake_bulk)
    monkeypatch.setattr(discovery_mod, "_list_remote_directory", fail_individual_directory_list)

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
    from schema_sanitizer.pipeline import discovery as discovery_mod

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

    async def fake_bulk(uris: list[str], extensions: tuple[str, ...]) -> dict[str, bool]:
        """Capture bulk discovery inputs and return one missing hour."""
        captured["uris"] = uris
        captured["extensions"] = extensions
        return {uri: not uri.endswith("hour=01") for uri in uris}

    async def fail_individual_directory_list(*_args, **_kwargs):
        """Fail if the per-directory remote listing path is used."""
        raise AssertionError("per-directory remote listing should not run for Azure")

    monkeypatch.setattr(discovery_mod, "_azure_directories_containing_files", fake_bulk)
    monkeypatch.setattr(discovery_mod, "_list_remote_directory", fail_individual_directory_list)

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
    from schema_sanitizer.pipeline import discovery as discovery_mod

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

    def fake_bulk(uris: list[str], extensions: tuple[str, ...]) -> dict[str, bool]:
        """Capture grouped local discovery inputs and return one missing hour."""
        captured["uris"] = uris
        captured["extensions"] = extensions
        return {uri: not uri.endswith("hour=01") for uri in uris}

    monkeypatch.setattr(discovery_mod, "_local_directories_containing_files", fake_bulk)

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


def test_pipeline_runner_carries_registry_forward(monkeypatch, tmp_path) -> None:
    """Verify the reusable runner passes registry JSON to later partitions."""
    from schema_sanitizer.pipeline import runner

    seen_registries: list[str] = []

    def fake_to_parquet(input_path, output_path, **kwargs):
        """Return a fake result while recording the input registry."""
        seen_registries.append(kwargs["schema_registry"])
        generation = len(seen_registries)
        return SimpleNamespace(
            stats={"input": input_path, "output": output_path},
            schema_registry_json=f'{{"schema_generation":{generation}}}',
            schema_drifts_json="[]",
        )

    monkeypatch.setattr(runner, "to_parquet", fake_to_parquet)
    plans = [
        PartitionRunPlan(date(2026, 1, 1), "raw/a.jsonl", str(tmp_path / "a.parquet")),
        PartitionRunPlan(date(2026, 1, 2), "raw/b.jsonl", str(tmp_path / "b.parquet")),
    ]

    result = runner.run_partitioned_to_parquet(
        plans,
        initial_schema_registry={"schema_generation": 0},
        to_parquet_kwargs={"input_format": "jsonl"},
    )

    assert seen_registries == ['{"schema_generation":0}', '{"schema_generation":1}']
    assert result.final_schema_registry == {"schema_generation": 2}
    assert result.final_schema_registry_json == '{"schema_generation":2}'
    assert [run.plan for run in result.completed_runs] == plans


def test_pipeline_json_registry_runner_carries_registry_json_forward(monkeypatch, tmp_path) -> None:
    """Verify the JSON-native runner passes registry strings between partitions."""
    from schema_sanitizer.pipeline import runner

    seen_registries = []

    def fake_to_parquet(input_path, output_path, **kwargs):
        """Return a fake JSON registry result while recording registry input."""
        seen_registries.append(kwargs["schema_registry"])
        generation = len(seen_registries)
        return SimpleNamespace(
            stats={"input": input_path, "output": output_path},
            schema_registry_json=f'{{"schema_generation":{generation}}}',
            schema_drifts_json="[]",
        )

    monkeypatch.setattr(runner, "to_parquet", fake_to_parquet)
    plans = [
        PartitionRunPlan(date(2026, 1, 1), "raw/a.jsonl", str(tmp_path / "a.parquet")),
        PartitionRunPlan(date(2026, 1, 2), "raw/b.jsonl", str(tmp_path / "b.parquet")),
    ]

    result = run_partitioned_to_parquet_registry_json(
        plans,
        initial_schema_registry_json='{"schema_generation":0}',
        to_parquet_kwargs={"input_format": "jsonl"},
    )

    assert seen_registries == ['{"schema_generation":0}', '{"schema_generation":1}']
    assert result.final_schema_registry_json == '{"schema_generation":2}'
    assert result.final_schema_registry == {"schema_generation": 2}
    assert parse_final_schema_registry(result) == {"schema_generation": 2}
    assert [run.schema_registry_json for run in result.completed_runs] == [
        '{"schema_generation":1}',
        '{"schema_generation":2}',
    ]


def test_pipeline_runner_carries_native_registry_state_forward(monkeypatch, tmp_path) -> None:
    """Verify partition runs pass the previous native registry state when available."""
    from schema_sanitizer.api_impl import file_convert_core
    from schema_sanitizer.pipeline import runner

    states = [object(), object()]
    seen_states = []

    def fake_to_parquet(input_path, output_path, **kwargs):
        """Return a fake result while recording the incoming native state."""
        seen_states.append(file_convert_core._SCHEMA_REGISTRY_NATIVE_STATE.get())
        generation = len(seen_states)
        return SimpleNamespace(
            stats={"input": input_path, "output": output_path},
            schema_registry_json=f'{{"schema_generation":{generation}}}',
            schema_drifts_json="[]",
            native_registry_state=states[generation - 1],
        )

    monkeypatch.setattr(runner, "to_parquet", fake_to_parquet)
    plans = [
        PartitionRunPlan(date(2026, 1, 1), "raw/a.jsonl", str(tmp_path / "a.parquet")),
        PartitionRunPlan(date(2026, 1, 2), "raw/b.jsonl", str(tmp_path / "b.parquet")),
    ]

    result = run_partitioned_to_parquet_registry_json(
        plans,
        initial_schema_registry_json='{"schema_generation":0}',
        to_parquet_kwargs={"input_format": "jsonl"},
    )

    assert seen_states == [None, states[0]]
    assert [run.native_registry_state for run in result.completed_runs] == states


def test_pipeline_runner_accepts_initial_schema_registry_state(monkeypatch, tmp_path) -> None:
    """Verify the state-based runner seeds the first partition with native state."""
    from schema_sanitizer.api_impl import file_convert_core
    from schema_sanitizer.pipeline import runner

    initial_state = object()
    final_state = object()
    seen_states = []

    def fake_to_parquet(input_path, output_path, **kwargs):
        """Record initial native state handoff and return a replacement state."""
        seen_states.append(file_convert_core._SCHEMA_REGISTRY_NATIVE_STATE.get())
        return SimpleNamespace(
            stats={"input": input_path, "output": output_path},
            schema_registry_json='{"schema_generation":2}',
            schema_drifts_json="[]",
            native_registry_state=final_state,
        )

    monkeypatch.setattr(runner, "to_parquet", fake_to_parquet)
    plans = [
        PartitionRunPlan(date(2026, 1, 1), "raw/a.jsonl", str(tmp_path / "a.parquet")),
    ]

    result = run_partitioned_to_parquet_registry_state(
        plans,
        initial_schema_registry_state=SchemaRegistryState(
            schema_registry_json='{"schema_generation":1}',
            native_registry_state=initial_state,
        ),
        to_parquet_kwargs={"input_format": "jsonl"},
    )

    assert seen_states == [initial_state]
    assert result.final_schema_registry_json == '{"schema_generation":2}'
    assert result.final_native_registry_state is final_state
    assert result.final_schema_registry_state.native_registry_state is final_state


def test_pipeline_runner_compiles_initial_registry_json_state(monkeypatch, tmp_path) -> None:
    """Verify JSON-only bootstrap can seed the first partition with native state."""
    from schema_sanitizer.api_impl import file_convert_core
    from schema_sanitizer.pipeline import runner

    compiled_state = object()
    seen_states = []
    compile_calls = []

    def fake_compile(registry_json, **kwargs):
        """Record registry compilation from durable JSON."""
        compile_calls.append((registry_json, kwargs["field_name_policy"]))
        return compiled_state

    def fake_to_parquet(input_path, output_path, **kwargs):
        """Record native state visible to the first partition."""
        seen_states.append(file_convert_core._SCHEMA_REGISTRY_NATIVE_STATE.get())
        return SimpleNamespace(
            stats={"input": input_path, "output": output_path},
            schema_registry_json='{"schema_generation":2}',
            schema_drifts_json="[]",
            native_registry_state=None,
        )

    monkeypatch.setattr(runner, "native_registry_state_from_json", fake_compile)
    monkeypatch.setattr(runner, "to_parquet", fake_to_parquet)
    plans = [
        PartitionRunPlan(date(2026, 1, 1), "raw/a.jsonl", str(tmp_path / "a.parquet")),
    ]

    run_partitioned_to_parquet_registry_json(
        plans,
        initial_schema_registry_json='{"schema_generation":1}',
        to_parquet_kwargs={"input_format": "jsonl", "field_name_policy": "lower_snake"},
    )

    assert compile_calls == [('{"schema_generation":1}', "lower_snake")]
    assert seen_states == [compiled_state]


def test_pipeline_runner_keeps_parquet_writer_options_out_of_registry_compile(
    monkeypatch,
    tmp_path,
) -> None:
    """Verify Parquet writer options do not leak into schema option normalization."""
    from schema_sanitizer.pipeline import runner

    seen_to_parquet_kwargs = []

    def fake_compile(registry_json, **kwargs):
        """Return no native state after option normalization has succeeded."""
        assert registry_json == '{"schema_generation":1}'
        assert kwargs["field_name_policy"] == "lower_snake"
        assert kwargs["options"] is not None
        return None

    def fake_to_parquet(input_path, output_path, **kwargs):
        """Record writer options are still passed to the actual Parquet converter."""
        seen_to_parquet_kwargs.append(kwargs)
        return SimpleNamespace(
            stats={"input": input_path, "output": output_path},
            schema_registry_json='{"schema_generation":2}',
            schema_drifts_json="[]",
            native_registry_state=None,
        )

    monkeypatch.setattr(runner, "native_registry_state_from_json", fake_compile)
    monkeypatch.setattr(runner, "to_parquet", fake_to_parquet)

    run_partitioned_to_parquet_registry_json(
        [PartitionRunPlan(date(2026, 1, 1), "raw/a.jsonl", str(tmp_path / "a.parquet"))],
        initial_schema_registry_json='{"schema_generation":1}',
        to_parquet_kwargs={
            "input_format": "jsonl",
            "field_name_policy": "lower_snake",
            "parquet_compression": "gzip",
            "parquet_gzip_level": 6,
        },
    )

    assert seen_to_parquet_kwargs == [
        {
            "input_format": "jsonl",
            "field_name_policy": "lower_snake",
            "parquet_compression": "gzip",
            "parquet_gzip_level": 6,
            "schema_registry": '{"schema_generation":1}',
        }
    ]


def test_pipeline_runner_clears_stale_native_registry_state(monkeypatch, tmp_path) -> None:
    """Verify JSON updates without a capsule do not reuse an older native state."""
    from schema_sanitizer.api_impl import file_convert_core
    from schema_sanitizer.pipeline import runner

    first_state = object()
    seen_states = []

    def fake_to_parquet(input_path, output_path, **kwargs):
        """Return a native state only for the first partition."""
        seen_states.append(file_convert_core._SCHEMA_REGISTRY_NATIVE_STATE.get())
        generation = len(seen_states)
        return SimpleNamespace(
            stats={"input": input_path, "output": output_path},
            schema_registry_json=f'{{"schema_generation":{generation}}}',
            schema_drifts_json="[]",
            native_registry_state=first_state if generation == 1 else None,
        )

    monkeypatch.setattr(runner, "to_parquet", fake_to_parquet)
    plans = [
        PartitionRunPlan(date(2026, 1, 1), "raw/a.jsonl", str(tmp_path / "a.parquet")),
        PartitionRunPlan(date(2026, 1, 2), "raw/b.jsonl", str(tmp_path / "b.parquet")),
        PartitionRunPlan(date(2026, 1, 3), "raw/c.jsonl", str(tmp_path / "c.parquet")),
    ]

    run_partitioned_to_parquet_registry_json(
        plans,
        initial_schema_registry_json='{"schema_generation":0}',
        to_parquet_kwargs={"input_format": "jsonl"},
    )

    assert seen_states == [None, first_state, None]


def test_pipeline_warm_up_uses_native_path_registry_probe(monkeypatch, tmp_path) -> None:
    """Verify JSONL warm-up batches local sources into one best-effort native probe."""
    source_a = tmp_path / "a.jsonl"
    source_b = tmp_path / "b.jsonl"
    source_a.write_text('{"id": 1}\n', encoding="utf-8")
    source_b.write_text('{"name": "Ada"}\n', encoding="utf-8")
    calls = []

    class FakeRaw:
        """Fake raw native context."""

        def registry_probe_path_sources(self, sources, options, **kwargs):
            """Fail if warm-up goes through the strict path before best effort."""
            raise AssertionError("strict path-source probe should not be called")

        def registry_probe_path_sources_best_effort(self, sources, options, **kwargs):
            """Record path-source probe arguments and return a registry payload."""
            calls.append((sources, options, kwargs))
            return SimpleNamespace(schema_registry_json='{"schema_generation":1}')

    class FakePool:
        """Fake context pool."""

        def get(self):
            """Return a fake high-level context wrapper."""
            return SimpleNamespace(_raw=FakeRaw())

    from schema_sanitizer.pipeline import registry_bootstrap

    monkeypatch.setattr(registry_bootstrap, "default_pool", lambda: FakePool())

    registry = infer_warm_up_schema_registry(
        [
            PartitionRunPlan(date(2026, 1, 1), str(source_a), str(tmp_path / "a.parquet")),
            PartitionRunPlan(date(2026, 1, 2), str(source_b), str(tmp_path / "b.parquet")),
        ],
        input_format="jsonl",
        input_mode="single_file",
        options={},
        schema_registry={},
        field_name_policy="lower_snake",
    )

    assert registry == {"schema_generation": 1}
    assert len(calls) == 1
    sources, _options, kwargs = calls[0]
    assert sources == [
        ("json", str(source_a), str(source_a)),
        ("json", str(source_b), str(source_b)),
    ]
    assert kwargs == {
        "registry_json": "{}",
        "field_name_policy": "lower_snake",
        "schema_mode": "additive",
    }


def test_pipeline_warm_up_prefers_native_auto_registry_stream(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Verify warm-up shares the normal native auto-registry source-plan path."""
    source = tmp_path / "a.jsonl"
    source.write_text('{"id": 1}\n', encoding="utf-8")
    closed: list[str] = []
    calls: list[tuple[list[tuple[str, str, str]], dict[str, object]]] = []

    class FakeRaw:
        """Fake native registry stream result."""

        schema_registry_json = '{"schema_generation":1}'
        schema_drifts_json = "[]"
        conversion_timestamp = "2026-01-01T00:00:00Z"
        field_names = ("id",)

        def close(self) -> None:
            """Record stream close."""
            closed.append("raw")

    class FakeRawContext:
        """Raw context exposing the normal native auto-registry stream."""

        def to_registry_sink_path_sources_auto_registry(
            self,
            sink,
            sources,
            call_options,
            **kwargs,
        ):
            """Capture the auto-registry call."""
            assert sink == "stream"
            assert call_options is None
            calls.append((sources, kwargs))
            return FakeRaw()

        def registry_probe_path_sources_best_effort(self, *_args, **_kwargs):
            """Fail if warm-up falls back to the older probe path."""
            raise AssertionError("warm-up should use native auto-registry stream")

    class FakePool:
        """Fake context pool."""

        def get(self):
            """Return a fake high-level context wrapper."""
            return SimpleNamespace(_raw=FakeRawContext())

    from schema_sanitizer.pipeline import registry_bootstrap

    monkeypatch.setattr(registry_bootstrap, "default_pool", lambda: FakePool())

    registry = infer_warm_up_schema_registry(
        [PartitionRunPlan(date(2026, 1, 1), str(source), str(tmp_path / "out.parquet"))],
        input_format="jsonl",
        input_mode="single_file",
        options={},
        schema_registry={},
        field_name_policy="lower_snake",
    )

    assert registry == {"schema_generation": 1}
    assert closed == ["raw"]
    assert calls == [
        (
            [("json", str(source), str(source))],
            {
                "registry_json": "{}",
                "field_name_policy": "lower_snake",
                "schema_mode": "additive",
                "first_row_columns": {},
                "timestamp_columns": (),
                "skip_invalid_json_sources": True,
            },
        )
    ]


def test_pipeline_warm_up_uses_source_plan_probe_helper() -> None:
    """Verify warm-up does not own low-level source-plan probing."""
    from schema_sanitizer.pipeline import registry_bootstrap

    assert not hasattr(registry_bootstrap, "probe_source_plan_registry")
    assert hasattr(registry_bootstrap, "probe_prepared_source_plan_registry")


def test_pipeline_warm_up_skips_invalid_json_probe_sources(tmp_path: Path) -> None:
    """Verify warm-up can skip invalid JSON sources outside the main run range."""
    bad = tmp_path / "bad.jsonl"
    good = tmp_path / "good.jsonl"
    bad.write_bytes(b'{"broken":"raw \x01 control"}\n')
    good.write_text('{"alpha": 1}\n', encoding="utf-8")

    registry = infer_warm_up_schema_registry(
        [
            PartitionRunPlan(date(2026, 1, 1), str(bad), str(tmp_path / "bad.parquet")),
            PartitionRunPlan(date(2026, 1, 2), str(good), str(tmp_path / "good.parquet")),
        ],
        input_format="jsonl",
        input_mode="single_file",
        options={},
        schema_registry={},
        field_name_policy="lower_snake",
    )

    assert registry["canonical_schema"]["fields"][0]["name"] == "alpha"


def test_pipeline_warm_up_can_return_registry_json(tmp_path: Path) -> None:
    """Verify warm-up can return canonical registry JSON without parsing it."""
    source = tmp_path / "a.jsonl"
    source.write_text('{"alpha": 1}\n', encoding="utf-8")

    registry_json = infer_warm_up_schema_registry_json(
        [PartitionRunPlan(date(2026, 1, 1), str(source), str(tmp_path / "a.parquet"))],
        input_format="jsonl",
        input_mode="single_file",
        options={},
        schema_registry={},
        field_name_policy="lower_snake",
    )

    assert isinstance(registry_json, str)
    assert json.loads(registry_json)["canonical_schema"]["fields"][0]["name"] == "alpha"


def test_pipeline_warm_up_can_return_registry_state(tmp_path: Path) -> None:
    """Verify warm-up returns native registry state for the normal run boundary."""
    source = tmp_path / "a.jsonl"
    source.write_text('{"alpha": 1}\n', encoding="utf-8")

    state = infer_warm_up_schema_registry_state(
        [PartitionRunPlan(date(2026, 1, 1), str(source), str(tmp_path / "a.parquet"))],
        input_format="jsonl",
        input_mode="single_file",
        options={},
        schema_registry={},
        field_name_policy="lower_snake",
    )

    assert state.native_registry_state is not None
    assert state.schema_registry["canonical_schema"]["fields"][0]["name"] == "alpha"


def test_pipeline_warm_up_keeps_parquet_writer_options_out_of_schema_options(
    tmp_path: Path,
) -> None:
    """Verify full to_parquet kwargs do not leak writer options into warm-up inference."""
    source = tmp_path / "a.jsonl"
    source.write_text('{"alpha": 1}\n', encoding="utf-8")

    state = infer_warm_up_schema_registry_state(
        [PartitionRunPlan(date(2026, 1, 1), str(source), str(tmp_path / "a.parquet"))],
        input_format="jsonl",
        input_mode="single_file",
        options={
            "input_format": "jsonl",
            "input_mode": "single_file",
            "field_name_policy": "lower_snake",
            "parquet_compression": "gzip",
            "parquet_gzip_level": 6,
        },
        schema_registry={},
        field_name_policy="lower_snake",
    )

    assert state.schema_registry["canonical_schema"]["fields"][0]["name"] == "alpha"


def test_pipeline_parquet_warm_up_uses_native_arrow_sources(tmp_path: Path) -> None:
    """Verify Parquet warm-up bypasses the Parquet-to-JSONL fallback."""
    pytest.importorskip("pyarrow")
    first = _write_warm_up_source(tmp_path, "parquet", "single_file", "first", "alpha")
    second = _write_warm_up_source(tmp_path, "parquet", "single_file", "second", "beta")

    from schema_sanitizer.pipeline.registry_bootstrap import last_warm_up_route

    state = infer_warm_up_schema_registry_state(
        [
            PartitionRunPlan(date(2026, 1, 1), str(first), str(tmp_path / "first.parquet")),
            PartitionRunPlan(date(2026, 1, 2), str(second), str(tmp_path / "second.parquet")),
        ],
        input_format="parquet",
        input_mode="single_file",
        options={},
        schema_registry={},
        field_name_policy="lower_snake",
    )

    fields = state.schema_registry["canonical_schema"]["fields"]
    assert {field["name"] for field in fields} >= {"alpha", "beta"}
    assert state.native_registry_state is not None
    assert last_warm_up_route() == "native_parquet_arrow_sources"


def test_pipeline_parquet_directory_warm_up_bypasses_jsonl_bridge(tmp_path: Path) -> None:
    """Verify mixed-schema Parquet directory warm-up uses child Arrow sources."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    folder = tmp_path / "parquet"
    folder.mkdir()
    pq.write_table(pa.table({"alpha": [1]}), folder / "a.parquet")
    pq.write_table(pa.table({"beta": [2]}), folder / "b.parquet")

    from schema_sanitizer.pipeline.registry_bootstrap import last_warm_up_route

    state = infer_warm_up_schema_registry_state(
        [PartitionRunPlan(date(2026, 1, 1), str(folder), str(tmp_path / "out.parquet"))],
        input_format="parquet",
        input_mode="directory",
        options={},
        schema_registry={},
        field_name_policy="lower_snake",
    )

    fields = state.schema_registry["canonical_schema"]["fields"]
    assert {field["name"] for field in fields} >= {"alpha", "beta"}
    assert state.native_registry_state is not None
    assert last_warm_up_route() == "native_parquet_arrow_sources"


def test_pipeline_xml_directory_warm_up_bypasses_wrapper(
    tmp_path: Path,
) -> None:
    """Verify XML directory warm-up infers row tags and reads child paths natively."""
    folder = tmp_path / "xml"
    folder.mkdir()
    (folder / "a.xml").write_text(
        '<?xml version="1.0"?><row><alpha>1</alpha></row>', encoding="utf-8"
    )
    (folder / "b.xml").write_text("<row><beta>2</beta></row>", encoding="utf-8")

    from schema_sanitizer.pipeline.registry_bootstrap import last_warm_up_route

    registry = infer_warm_up_schema_registry(
        [PartitionRunPlan(date(2026, 1, 1), str(folder), str(tmp_path / "out.parquet"))],
        input_format="xml",
        input_mode="directory",
        options={},
        schema_registry={},
        field_name_policy="lower_snake",
    )

    fields = registry["canonical_schema"]["fields"]
    assert {field["name"] for field in fields} >= {"alpha", "beta"}
    assert last_warm_up_route() == "native_manifest_paths"


def test_pipeline_xml_warm_up_infers_row_tag_natively(tmp_path: Path) -> None:
    """Verify XML warm-up no longer needs the temp wrapper to infer row tags."""
    first = _write_warm_up_source(tmp_path, "xml", "single_file", "first", "alpha")
    second = _write_warm_up_source(tmp_path, "xml", "single_file", "second", "beta")

    from schema_sanitizer.pipeline.registry_bootstrap import last_warm_up_route

    registry = infer_warm_up_schema_registry(
        [
            PartitionRunPlan(date(2026, 1, 1), str(first), str(tmp_path / "first.parquet")),
            PartitionRunPlan(date(2026, 1, 2), str(second), str(tmp_path / "second.parquet")),
        ],
        input_format="xml",
        input_mode="single_file",
        options={},
        schema_registry={},
        field_name_policy="lower_snake",
    )

    fields = registry["canonical_schema"]["fields"]
    assert {field["name"] for field in fields} >= {"alpha", "beta"}
    assert last_warm_up_route() == "native_manifest_paths"


@pytest.mark.parametrize("input_format", ["csv", "xml"])
def test_pipeline_warm_up_native_manifest_replaces_fallback_routing(
    tmp_path: Path,
    input_format: str,
) -> None:
    """Verify CSV/XML warm-up builds native manifests without fallback routing."""
    source = _write_warm_up_source(tmp_path, input_format, "directory", "native", "alpha")

    from schema_sanitizer.pipeline import registry_bootstrap

    assert not hasattr(registry_bootstrap, "_route_prepared_inputs_for_warm_up")

    prepared = registry_bootstrap.prepare_schema_warm_up_input(
        [PartitionRunPlan(date(2026, 1, 1), str(source), str(tmp_path / "out.parquet"))],
        input_format=input_format,
        input_mode="directory",
        input_text_encoding="utf-8",
        xml_row_tag="row",
        csv_delimiter=",",
        csv_has_header=True,
        batch_memory_limit_bytes=None,
        call_options=None,
    )
    try:
        assert prepared.source == "source_plan"
        assert prepared.data.source_batch.input_format == input_format
    finally:
        prepared.close()


@pytest.mark.parametrize(
    "input_format",
    ["jsonl", "ndjson", "json", "json_array", "csv", "xml"],
)
def test_pipeline_warm_up_and_normal_directory_share_source_descriptors(
    tmp_path: Path,
    input_format: str,
) -> None:
    """Verify warm-up and normal directory ingestion build the same native sources."""
    from schema_sanitizer.api_impl.public_input import prepare_public_input
    from schema_sanitizer.api_impl.source_plan import source_plan_from_data
    from schema_sanitizer.pipeline.registry_bootstrap import prepare_schema_warm_up_input

    source = _write_warm_up_source(tmp_path, input_format, "directory", "shared", "alpha")
    options = {
        "input_format": input_format,
        "input_mode": "directory",
        "input_text_encoding": "utf-8",
        "xml_row_tag": "row",
        "csv_delimiter": ",",
        "csv_has_header": True,
    }
    normal = prepare_public_input(source, memory_limit_bytes=None, **options)
    warm = prepare_schema_warm_up_input(
        [PartitionRunPlan(date(2026, 1, 1), str(source), str(tmp_path / "out.parquet"))],
        batch_memory_limit_bytes=None,
        call_options=None,
        **options,
    )
    try:
        normal_plan = source_plan_from_data(normal.data)
        assert normal_plan is not None
        assert normal_plan.source_batch is not None
        assert warm.source == "source_plan"
        assert normal_plan.source_batch.input_format == warm.data.source_batch.input_format
        assert normal_plan.source_batch.input_mode == warm.data.source_batch.input_mode
        assert normal_plan.payload == warm.data.payload
    finally:
        normal.close()
        warm.close()


@pytest.mark.parametrize(
    "input_format",
    ["jsonl", "ndjson", "json", "json_array", "csv", "xml", "parquet"],
)
@pytest.mark.parametrize("input_mode", ["single_file", "directory"])
def test_pipeline_warm_up_supports_all_public_file_formats_and_modes(
    tmp_path: Path,
    input_format: str,
    input_mode: str,
) -> None:
    """Verify warm-up can infer across every public input format and mode."""
    first = _write_warm_up_source(tmp_path, input_format, input_mode, "first", "alpha")
    second = _write_warm_up_source(tmp_path, input_format, input_mode, "second", "beta")

    registry = infer_warm_up_schema_registry(
        [
            PartitionRunPlan(date(2026, 1, 1), str(first), str(tmp_path / "first.parquet")),
            PartitionRunPlan(date(2026, 1, 2), str(second), str(tmp_path / "second.parquet")),
        ],
        input_format=input_format,
        input_mode=input_mode,
        options={
            "field_name_policy": "lower_snake",
            "csv_has_header": True,
            "csv_delimiter": ",",
            "xml_row_tag": "row",
        },
        schema_registry={},
        field_name_policy="lower_snake",
    )

    assert "canonical_schema" in registry
    fields = registry["canonical_schema"]["fields"]
    assert {field["name"] for field in fields} >= {"alpha", "beta"}


def test_pipeline_warm_up_registry_does_not_inject_rows_into_normal_partitions(
    tmp_path: Path,
) -> None:
    """Verify warm-up data only seeds schema inference, never normal output rows."""
    pq = pytest.importorskip("pyarrow.parquet")
    warm = tmp_path / "warm.jsonl"
    normal = tmp_path / "normal.jsonl"
    out = tmp_path / "normal.parquet"
    warm.write_text('{"id":"warm-row","warm_only":1}\n', encoding="utf-8")
    normal.write_text('{"id":"normal-row","normal_only":2}\n', encoding="utf-8")

    registry = infer_warm_up_schema_registry(
        [PartitionRunPlan(date(2026, 1, 1), str(warm), str(tmp_path / "warm.parquet"))],
        input_format="jsonl",
        input_mode="single_file",
        options={"field_name_policy": "lower_alpha"},
        schema_registry={},
        field_name_policy="lower_alpha",
    )

    run_partitioned_to_parquet(
        [PartitionRunPlan(date(2026, 1, 2), str(normal), str(out))],
        initial_schema_registry=registry,
        to_parquet_kwargs={
            "input_format": "jsonl",
            "input_mode": "single_file",
            "field_name_policy": "lower_alpha",
        },
    )

    rows = pq.read_table(out).to_pylist()
    assert [row["id"] for row in rows] == ["normal-row"]
    assert rows[0]["normalonly"] == 2
    assert rows[0]["warmonly"] is None


def test_pipeline_warm_up_registry_uses_native_registry_stream_normal_partition(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Verify non-overlapping warm-up dates use the native registry stream."""
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import source_plan

    warm = tmp_path / "warm"
    normal = tmp_path / "normal"
    warm.mkdir()
    normal.mkdir()
    (warm / "warm.json").write_text('{"id": 1, "name": "warm"}\n', encoding="utf-8")
    (normal / "normal.json").write_text('{"id": 2, "name": "normal"}\n', encoding="utf-8")

    registry = infer_warm_up_schema_registry(
        [PartitionRunPlan(date(2026, 1, 1), str(warm), str(tmp_path / "warm.parquet"))],
        input_format="json",
        input_mode="directory",
        options={"field_name_policy": "lower_alpha"},
        schema_registry={},
        field_name_policy="lower_alpha",
    )

    registry_stream_calls = 0
    real_registry_stream = source_plan.open_source_plan_registry_stream

    def tracking_registry_stream(*args, **kwargs):
        """Track native registry stream use while preserving behavior."""
        nonlocal registry_stream_calls
        registry_stream_calls += 1
        return real_registry_stream(*args, **kwargs)

    monkeypatch.setattr(
        source_plan,
        "open_source_plan_registry_stream",
        tracking_registry_stream,
    )

    out = tmp_path / "normal.parquet"
    run_partitioned_to_parquet(
        [PartitionRunPlan(date(2026, 2, 20), str(normal), str(out))],
        initial_schema_registry=registry,
        to_parquet_kwargs={
            "input_format": "json",
            "input_mode": "directory",
            "field_name_policy": "lower_alpha",
        },
    )

    rows = pq.read_table(out).to_pylist()
    assert registry_stream_calls == 1
    assert [row["id"] for row in rows] == [2]
    assert [row["name"] for row in rows] == ["normal"]


def test_pipeline_warm_up_directory_parquet_coalesces_source_file_batches(
    tmp_path: Path,
) -> None:
    """Verify many tiny source files do not become many Parquet row groups."""
    pq = pytest.importorskip("pyarrow.parquet")

    warm = tmp_path / "warm-coalesce"
    normal = tmp_path / "normal-coalesce"
    warm.mkdir()
    normal.mkdir()
    for index in range(8):
        (warm / f"warm-{index}.jsonl").write_text(
            json.dumps({"id": index, "name": "warm"}) + "\n",
            encoding="utf-8",
        )
        (normal / f"normal-{index}.jsonl").write_text(
            json.dumps({"id": index, "name": "normal"}) + "\n",
            encoding="utf-8",
        )

    registry = infer_warm_up_schema_registry(
        [PartitionRunPlan(date(2026, 1, 1), str(warm), str(tmp_path / "warm.parquet"))],
        input_format="jsonl",
        input_mode="directory",
        options={"field_name_policy": "lower_snake"},
        schema_registry={},
        field_name_policy="lower_snake",
    )
    out = tmp_path / "normal.parquet"
    run_partitioned_to_parquet(
        [PartitionRunPlan(date(2026, 1, 2), str(normal), str(out))],
        initial_schema_registry=registry,
        to_parquet_kwargs={
            "input_format": "jsonl",
            "input_mode": "directory",
            "field_name_policy": "lower_snake",
        },
    )

    parquet_file = pq.ParquetFile(out)
    assert parquet_file.metadata.num_rows == 8
    assert parquet_file.metadata.num_row_groups == 1


def test_pipeline_native_registry_stream_handles_normal_partition_drift(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Verify native registry streams update the registry when normal partitions drift."""
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import source_plan

    warm = tmp_path / "warm-drift"
    normal = tmp_path / "normal-drift"
    warm.mkdir()
    normal.mkdir()
    (warm / "warm.json").write_text('{"id": 1}\n', encoding="utf-8")
    (normal / "normal.json").write_text('{"id": 2, "late_field": 3}\n', encoding="utf-8")

    registry = infer_warm_up_schema_registry(
        [PartitionRunPlan(date(2026, 1, 1), str(warm), str(tmp_path / "warm-drift.parquet"))],
        input_format="json",
        input_mode="directory",
        options={"field_name_policy": "lower_alpha"},
        schema_registry={},
        field_name_policy="lower_alpha",
    )
    registry_stream_calls = 0
    real_registry_stream = source_plan.open_source_plan_registry_stream

    def tracking_registry_stream(*args, **kwargs):
        """Track native registry stream use while preserving behavior."""
        nonlocal registry_stream_calls
        registry_stream_calls += 1
        return real_registry_stream(*args, **kwargs)

    monkeypatch.setattr(
        source_plan,
        "open_source_plan_registry_stream",
        tracking_registry_stream,
    )

    out = tmp_path / "normal-drift.parquet"
    result = run_partitioned_to_parquet(
        [PartitionRunPlan(date(2026, 4, 10), str(normal), str(out))],
        initial_schema_registry=registry,
        to_parquet_kwargs={
            "input_format": "json",
            "input_mode": "directory",
            "field_name_policy": "lower_alpha",
        },
    )

    rows = pq.read_table(out).to_pylist()
    assert registry_stream_calls == 1
    assert rows[0]["latefield"] == 3
    assert "latefield" in {
        field["name"] for field in result.final_schema_registry["canonical_schema"]["fields"]
    }


def test_pipeline_remote_warm_up_registry_does_not_inject_rows_into_normal_partitions(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Verify lazy remote warm-up chunks are not replayed into normal outputs."""
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl import public_input
    from schema_sanitizer.api_impl.async_remote_io import RemoteFile, StagedPath

    payloads = {
        "s3://bucket/warm/warm.json": {"id": "warm-row", "warm_only": 1},
        "s3://bucket/normal/normal.json": {"id": "normal-row", "normal_only": 2},
    }

    def fake_list_remote_directory_files(uri, suffixes):
        """Return a single child for each fake remote directory."""
        assert suffixes == (".json",)
        if uri == "s3://bucket/warm/":
            return [RemoteFile("s3://bucket/warm/warm.json", "warm.json", None)]
        if uri == "s3://bucket/normal/":
            return [RemoteFile("s3://bucket/normal/normal.json", "normal.json", None)]
        raise AssertionError(uri)

    def fake_stage_remote_files_to_directory(files, **kwargs):
        """Stage only requested fake remote files."""
        assert kwargs["memory_limit_bytes"] is None
        staged_dir = tmp_path / f"stage-{len(list(tmp_path.glob('stage-*'))) + 1}"
        staged_dir.mkdir()
        for file in files:
            (staged_dir / file.name).write_text(
                json.dumps(payloads[file.uri]) + "\n",
                encoding="utf-8",
            )
        return StagedPath(
            str(staged_dir),
            is_dir=True,
            source_file_by_name={file.name: file.uri for file in files},
        )

    monkeypatch.setattr(
        public_input,
        "list_remote_directory_files",
        fake_list_remote_directory_files,
    )
    monkeypatch.setattr(
        public_input,
        "stage_remote_files_to_directory",
        fake_stage_remote_files_to_directory,
    )
    monkeypatch.setenv("SCHEMA_SANITIZER_REMOTE_STAGE_FILES", "1")

    registry = infer_warm_up_schema_registry(
        [
            PartitionRunPlan(
                date(2026, 1, 1),
                "s3://bucket/warm/",
                "s3://bucket/out/warm.parquet",
            )
        ],
        input_format="json",
        input_mode="directory",
        options={"field_name_policy": "lower_alpha"},
        schema_registry={},
        field_name_policy="lower_alpha",
    )
    out = tmp_path / "normal.parquet"

    run_partitioned_to_parquet(
        [PartitionRunPlan(date(2026, 1, 2), "s3://bucket/normal/", str(out))],
        initial_schema_registry=registry,
        to_parquet_kwargs={
            "input_format": "json",
            "input_mode": "directory",
            "field_name_policy": "lower_alpha",
        },
    )

    rows = pq.read_table(out).to_pylist()
    assert [row["id"] for row in rows] == ["normal-row"]
    assert rows[0]["normalonly"] == 2
    assert rows[0]["warmonly"] is None
    assert rows[0]["source_file"] == "s3://bucket/normal/normal.json"


def test_partition_runner_passes_schema_registry_by_value(monkeypatch, tmp_path: Path) -> None:
    """Verify partition registry state is passed as immutable JSON, not mutable dicts."""
    from schema_sanitizer.pipeline import runner

    seen_registries: list[str] = []

    def fake_to_parquet(source_uri, output_uri, **kwargs):
        """Record the registry passed to the writer."""
        del source_uri, output_uri
        registry = kwargs["schema_registry"]
        assert isinstance(registry, str)
        seen_registries.append(registry)
        return SimpleNamespace(
            stats={},
            schema_registry_json=json.dumps(
                {
                    "canonical_schema": {"fields": []},
                    "generation": len(seen_registries),
                },
                separators=(",", ":"),
            ),
            schema_drifts_json="[]",
        )

    monkeypatch.setattr(runner, "to_parquet", fake_to_parquet)
    initial = {"canonical_schema": {"fields": [{"name": "warmonly"}]}}

    result = run_partitioned_to_parquet(
        [
            PartitionRunPlan(date(2026, 1, 1), "source-a", str(tmp_path / "a.parquet")),
            PartitionRunPlan(date(2026, 1, 2), "source-b", str(tmp_path / "b.parquet")),
        ],
        initial_schema_registry=initial,
        to_parquet_kwargs={"input_format": "jsonl"},
    )

    assert seen_registries[0] == '{"canonical_schema":{"fields":[{"name":"warmonly"}]}}'
    assert seen_registries[1] == '{"canonical_schema":{"fields":[]},"generation":1}'
    assert result.final_schema_registry == {"canonical_schema": {"fields": []}, "generation": 2}


def test_pipeline_diff_flat_schema_paths_reports_added_removed_changed() -> None:
    """Verify reusable schema drift diff logic."""
    diff = diff_flat_schema_paths(
        {"id": "int64", "old": "string", "value": "int64"},
        {"id": "int64", "new": "string", "value": "double"},
    )

    assert diff.added_paths == ["new"]
    assert diff.removed_paths == ["old"]
    assert diff.changed_paths == ["value"]
    assert diff.has_changes


def test_pipeline_observability_helpers_are_reusable() -> None:
    """Verify compact logging helpers are exported from the pipeline package."""
    assert format_duration(65.2) == "1m 5s"
    assert compact_stats_for_log({"input_rows": 3, "bytes_written": 42}) == "in=3 bytes_written=42"


def test_pipeline_read_parquet_schema_handles_local_paths(tmp_path) -> None:
    """Verify reusable Parquet schema reads local output schemas."""
    pa = __import__("pyarrow")
    pq = __import__("pyarrow.parquet").parquet
    target = tmp_path / "rows.parquet"
    schema = pa.schema([pa.field("id", pa.int64())])
    pq.write_table(pa.table({"id": [1]}, schema=schema), target)

    assert read_parquet_schema(str(target)).equals(schema)


def test_bigquery_integration_builds_external_table_ddl() -> None:
    """Verify BigQuery schema/DDL helpers are package-owned."""
    pa = __import__("pyarrow")
    from schema_sanitizer.integrations.bigquery import (
        BigQueryTableRef,
        ExternalTableSpec,
        external_table_ddl,
        parse_hive_partition_column,
    )

    ddl, skipped = external_table_ddl(
        BigQueryTableRef("project", "dataset", "events"),
        pa.schema([pa.field("id", pa.int64()), pa.field("date", pa.date32())]),
        ExternalTableSpec(
            source_uris=["gs://silver/events/*"],
            hive_uri_prefix="gs://silver/events",
            partition_columns=(("date", "DATE"),),
        ),
    )

    assert skipped == ["date"]
    assert "CREATE OR REPLACE EXTERNAL TABLE" in ddl
    assert "`id` INT64" in ddl
    assert parse_hive_partition_column("hour:INT64") == ("hour", "INT64")


def test_bigquery_external_table_ddl_can_sort_nested_fields_alphabetically() -> None:
    """Verify BigQuery DDL can mirror column_order='alphabetically' recursively."""
    pa = __import__("pyarrow")
    from schema_sanitizer.integrations.bigquery import (
        BigQueryTableRef,
        ExternalTableSpec,
        external_table_ddl,
    )

    ddl, _skipped = external_table_ddl(
        BigQueryTableRef("project", "dataset", "events"),
        pa.schema(
            [
                pa.field("z", pa.int64()),
                pa.field(
                    "variables",
                    pa.struct(
                        [
                            pa.field("email", pa.string()),
                            pa.field("phone", pa.string()),
                            pa.field("birthday", pa.string()),
                            pa.field("company", pa.string()),
                        ]
                    ),
                ),
                pa.field("a", pa.string()),
            ]
        ),
        ExternalTableSpec(
            source_uris=["gs://silver/events/*"],
            hive_uri_prefix="gs://silver/events",
            partition_columns=(("date", "DATE"),),
        ),
        sort_fields_alphabetically=True,
    )

    assert ddl.index("`a` STRING") < ddl.index("`variables` STRUCT") < ddl.index("`z` INT64")
    assert (
        "`variables` STRUCT<`birthday` STRING, `company` STRING, "
        "`email` STRING, `phone` STRING>"
    ) in ddl


def test_bigquery_registry_sidecar_partition_queries() -> None:
    """Verify BigQuery registry sidecar SQL uses encoded Hive partition keys."""
    from schema_sanitizer.integrations.bigquery import (
        BigQueryTableRef,
        latest_schema_registry_query,
        partition_key_from_uri,
        sidecar_table_ddl,
        sidecar_upsert_query,
    )

    external = BigQueryTableRef("project", "dataset", "events_ext")
    sidecar = BigQueryTableRef("project", "dataset", "events_registry_state")
    partition_columns = (
        ("year", "INT64"),
        ("month", "INT64"),
        ("date", "DATE"),
        ("hour", "INT64"),
    )
    partition_key = partition_key_from_uri(
        "gs://silver/events/year=2026/month=07/date=2026-07-05/hour=08/file.parquet",
        partition_columns,
    )

    assert partition_key == "year=2026/month=07/date=2026-07-05/hour=08"
    lookup = latest_schema_registry_query(
        external,
        partition_columns,
        partition_key=partition_key,
    )
    assert "`year` = 2026" in lookup
    assert "`month` = 7" in lookup
    assert "`date` = DATE '2026-07-05'" in lookup
    assert "`hour` = 8" in lookup
    assert "CREATE TABLE IF NOT EXISTS `project.dataset.events_registry_state`" in (
        sidecar_table_ddl(sidecar)
    )
    upsert = sidecar_upsert_query(
        sidecar,
        external,
        last_ingested_partition=partition_key,
    )
    assert "MERGE `project.dataset.events_registry_state` AS target" in upsert
    assert "'project.dataset.events_ext'" in upsert
    assert "'year=2026/month=07/date=2026-07-05/hour=08'" in upsert


def test_bigquery_registry_sidecar_fetch_fast_path_and_missing_fallback(caplog) -> None:
    """Verify sidecar lookup narrows registry scans and missing sidecars fallback."""
    from schema_sanitizer.integrations.bigquery import (
        BigQueryTableRef,
        fetch_latest_schema_registry,
    )

    class FakeCursor:
        """Minimal cursor returning configured BigQuery query results."""

        def __init__(self, dbapi):
            """Store fake DB-API state."""
            self._dbapi = dbapi
            self._result = None

        def __enter__(self):
            """Return cursor for context-manager use."""
            return self

        def __exit__(self, _exc_type, _exc, _tb):
            """Propagate context-manager exceptions."""
            return False

        def execute(self, query):
            """Capture query text and choose the matching fake result."""
            self._dbapi.queries.append(query)
            if "INFORMATION_SCHEMA.TABLES" in query:
                self._result = self._dbapi.table_type
            elif "FROM `project.dataset.registry_state`" in query:
                self._result = self._dbapi.sidecar_partition
            elif "`hour` = 8" in query:
                self._result = '{"schema_generation":3,"canonical_schema":{}}'
            else:
                self._result = '{"schema_generation":2,"canonical_schema":{}}'

        def fetchone(self):
            """Return one fake result row."""
            if self._result is None:
                return None
            return (self._result,)

    class FakeConnection:
        """Minimal connection returning fake cursors."""

        def __init__(self, dbapi):
            """Store fake DB-API state."""
            self._dbapi = dbapi

        def __enter__(self):
            """Return connection for context-manager use."""
            return self

        def __exit__(self, _exc_type, _exc, _tb):
            """Propagate context-manager exceptions."""
            return False

        def cursor(self):
            """Return a fake cursor."""
            return FakeCursor(self._dbapi)

    class FakeDbapi:
        """Minimal DB-API facade for sidecar fetch tests."""

        def __init__(self, *, table_type, sidecar_partition):
            """Store configured fake query results."""
            self.table_type = table_type
            self.sidecar_partition = sidecar_partition
            self.queries = []

        def connect(self, *, db_kwargs):
            """Return a fake connection after checking connection options."""
            assert db_kwargs == {"project": "project"}
            return FakeConnection(self)

    external = BigQueryTableRef("project", "dataset", "events_ext")
    sidecar = BigQueryTableRef("project", "dataset", "registry_state")
    partition_columns = (
        ("year", "INT64"),
        ("month", "INT64"),
        ("date", "DATE"),
        ("hour", "INT64"),
    )

    with caplog.at_level(logging.INFO, logger="schema_sanitizer.integrations.bigquery"):
        dbapi = FakeDbapi(
            table_type="BASE TABLE",
            sidecar_partition="year=2026/month=07/date=2026-07-05/hour=08",
        )
        registry = fetch_latest_schema_registry(
            dbapi=dbapi,
            db_kwargs={"project": "project"},
            table_ref=external,
            partition_columns=partition_columns,
            sidecar_table_ref=sidecar,
        )
    assert registry["schema_generation"] == 3
    assert any("`hour` = 8" in query for query in dbapi.queries)
    assert "Sidecar lookup table=project.dataset.registry_state" in caplog.text
    assert "Sidecar lookup table=project.dataset.registry_state status=exists" in caplog.text
    assert (
        "Sidecar lookup table=project.dataset.registry_state "
        "external=project.dataset.events_ext status=hit "
        "partition=year=2026/month=07/date=2026-07-05/hour=08" in caplog.text
    )

    caplog.clear()
    missing_sidecar = FakeDbapi(table_type=None, sidecar_partition=None)
    with caplog.at_level(logging.INFO, logger="schema_sanitizer.integrations.bigquery"):
        registry = fetch_latest_schema_registry(
            dbapi=missing_sidecar,
            db_kwargs={"project": "project"},
            table_ref=external,
            partition_columns=partition_columns,
            sidecar_table_ref=sidecar,
        )
    assert registry["schema_generation"] == 2
    assert not any("`hour` = 8" in query for query in missing_sidecar.queries)
    assert (
        "Sidecar lookup table=project.dataset.registry_state "
        "external=project.dataset.events_ext status=missing fallback=external_scan" in caplog.text
    )


def test_bigquery_registry_sidecar_update_logs_create_and_upsert(caplog) -> None:
    """Verify sidecar updates log table creation checks and content updates."""
    from schema_sanitizer.integrations.bigquery import (
        BigQueryTableRef,
        update_registry_sidecar_table,
    )

    class FakeCursor:
        """Minimal cursor for sidecar update logging tests."""

        def __init__(self, dbapi):
            """Store fake DB-API state."""
            self._dbapi = dbapi
            self._result = None

        def __enter__(self):
            """Return cursor for context-manager use."""
            return self

        def __exit__(self, _exc_type, _exc, _tb):
            """Propagate context-manager exceptions."""
            return False

        def execute(self, query):
            """Capture query text and return table existence state."""
            self._dbapi.queries.append(query)
            if "INFORMATION_SCHEMA.TABLES" in query:
                self._result = self._dbapi.table_type
            else:
                self._result = None

        def fetchone(self):
            """Return one fake table type row."""
            if self._result is None:
                return None
            return (self._result,)

    class FakeConnection:
        """Minimal fake BigQuery connection."""

        def __init__(self, dbapi):
            """Store fake DB-API state."""
            self._dbapi = dbapi

        def __enter__(self):
            """Return connection for context-manager use."""
            return self

        def __exit__(self, _exc_type, _exc, _tb):
            """Propagate context-manager exceptions."""
            return False

        def cursor(self):
            """Return a fake cursor."""
            return FakeCursor(self._dbapi)

    class FakeDbapi:
        """Minimal DB-API facade for sidecar update tests."""

        def __init__(self, table_type):
            """Store configured table type."""
            self.table_type = table_type
            self.queries = []

        def connect(self, *, db_kwargs):
            """Return a fake connection after checking connection options."""
            assert db_kwargs == {"project": "project"}
            return FakeConnection(self)

    external = BigQueryTableRef("project", "dataset", "events_ext")
    sidecar = BigQueryTableRef("project", "dataset", "registry_state")
    dbapi = FakeDbapi(table_type=None)

    with caplog.at_level(logging.INFO, logger="schema_sanitizer.integrations.bigquery"):
        update_registry_sidecar_table(
            dbapi=dbapi,
            db_kwargs={"project": "project"},
            sidecar_table_ref=sidecar,
            external_table_ref=external,
            last_ingested_partition="year=2026/month=07/date=2026-07-05/hour=08",
        )

    assert any("INFORMATION_SCHEMA.TABLES" in query for query in dbapi.queries)
    assert any("CREATE TABLE IF NOT EXISTS" in query for query in dbapi.queries)
    assert any(
        "MERGE `project.dataset.registry_state` AS target" in query for query in dbapi.queries
    )
    assert (
        "Sidecar update table=project.dataset.registry_state status=missing action=create"
        in caplog.text
    )
    assert "Sidecar update table=project.dataset.registry_state action=ensure_table" in caplog.text
    assert (
        "Sidecar update table=project.dataset.registry_state action=upsert "
        "external=project.dataset.events_ext partition=year=2026/month=07/date=2026-07-05/hour=08"
        in caplog.text
    )

    caplog.clear()
    existing_dbapi = FakeDbapi(table_type="BASE TABLE")
    with caplog.at_level(logging.INFO, logger="schema_sanitizer.integrations.bigquery"):
        update_registry_sidecar_table(
            dbapi=existing_dbapi,
            db_kwargs={"project": "project"},
            sidecar_table_ref=sidecar,
            external_table_ref=external,
            last_ingested_partition="year=2026/month=07/date=2026-07-05/hour=09",
        )

    assert (
        "Sidecar update table=project.dataset.registry_state status=exists table_type=BASE TABLE"
        in caplog.text
    )
    assert any(
        "MERGE `project.dataset.registry_state` AS target" in query
        for query in existing_dbapi.queries
    )
