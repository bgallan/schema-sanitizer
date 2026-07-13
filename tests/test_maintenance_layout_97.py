"""Protect ownership and hot-path changes introduced by maintenance layout 97."""

from __future__ import annotations

import importlib.util
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

from schema_sanitizer.pipeline import partition_execution
from schema_sanitizer.pipeline.types import PartitionRunPlan, SchemaRegistryState

ROOT = Path(__file__).resolve().parents[1]


def test_partition_execution_has_one_direct_owner_without_package_facade() -> None:
    """Partition loop, state result, and bootstrap stay in one bounded module."""
    pipeline = ROOT / "src/schema_sanitizer/pipeline"
    owner = pipeline / "partition_execution.py"
    assert importlib.util.find_spec("schema_sanitizer.pipeline.execution") is None
    assert owner.is_file()
    assert not (pipeline / "execution").exists()
    source = owner.read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 500
    assert "class PartitionPipelineResult" in source
    assert "def _compile_native_registry_state" in source
    assert "def run_partitioned_to_parquet_registry_json" in source


def test_static_partition_kwargs_avoid_redundant_copy_and_remain_live(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Static kwargs avoid an extra dict copy while preserving live mutations."""
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

    def fake_to_parquet(*_args, **kwargs):
        """Record the current mapping value for each partition."""
        seen_policies.append(kwargs["field_name_policy"])
        return SimpleNamespace(
            stats={},
            schema_registry_json=None,
            schema_drifts_json="[]",
            native_registry_state=initial_state,
        )

    def mutate_options(*_args) -> None:
        """Change the shared mapping after the first partition."""
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

    source = (ROOT / "src/schema_sanitizer/pipeline/partition_execution.py").read_text(
        encoding="utf-8"
    )
    assert "dict(to_parquet_kwargs)" not in source
    assert seen_policies == ["preserve", "lower_snake"]


def test_csv_frontend_has_one_lifecycle_owner_and_no_header_allocations() -> None:
    """CSV batching stays cohesive and no-header rows avoid empty string headers."""
    package = ROOT / "cpp/src/frontends/csv"
    assert {path.name for path in package.iterdir() if path.is_file()} == {
        "column_projection.cc",
        "column_projection.hh",
        "frontend.cc",
        "frontend_internal.hh",
    }
    frontend = (package / "frontend.cc").read_text(encoding="utf-8")
    projection = (package / "column_projection.cc").read_text(encoding="utf-8")
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "storage->cells.reserve(projection_.column_count_hint())" in frontend
    assert "cells.reserve(projection_.column_count_hint())" in frontend
    assert "if (has_header_ && cells.size() > headers_.size())" in projection
    assert "frontends/csv/frontend_batch.cc" not in manifest
    assert "frontends/csv/frontend_lifecycle.cc" not in manifest


def test_csv_nested_stream_allocates_state_only_for_nested_columns() -> None:
    """Per-batch nested array state scales with nested, not total, columns."""
    package = ROOT / "cpp/src/api/python_abi3/csv/nested_stream"
    owner = (package / "nested_stream.cc").read_text(encoding="utf-8")
    state = (package / "state.hh").read_text(encoding="utf-8")
    assert {path.name for path in package.iterdir() if path.is_file()} == {
        "nested_stream.cc",
        "state.hh",
    }
    assert "std::optional<std::size_t> nested_slot" in state
    assert "std::size_t nested_column_count = 0" in state
    assert "nested_arrays.resize(stream_state->nested_column_count)" in owner
    assert "nested_fields.reserve(stream_state->nested_column_count)" in owner
    assert "nested_arrays.resize(stream_state->columns.size())" not in owner
