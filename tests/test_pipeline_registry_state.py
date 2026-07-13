"""Pipeline registry state tests."""

# ruff: noqa: F405

from __future__ import annotations

from pipeline_shared import *  # noqa: F403


def test_pipeline_runner_carries_registry_forward(monkeypatch, tmp_path) -> None:
    """Verify the reusable runner passes registry JSON to later partitions."""
    from schema_sanitizer.pipeline import partition_execution

    seen_registries: list[str] = []

    def fake_to_parquet(input_path, output_path, **kwargs):
        """Return a fake result while recording the input registry."""
        seen_registries.append(kwargs["schema_registry"])
        generation = len(seen_registries)
        return SimpleNamespace(
            stats={"input": input_path, "output": output_path},
            schema_registry_json=f'{{"schema_generation":{generation}}}',
            schema_drifts_json="[]",
            native_registry_state=None,
        )

    monkeypatch.setattr(partition_execution, "to_parquet", fake_to_parquet)
    plans = [
        PartitionRunPlan(date(2026, 1, 1), "raw/a.jsonl", str(tmp_path / "a.parquet")),
        PartitionRunPlan(date(2026, 1, 2), "raw/b.jsonl", str(tmp_path / "b.parquet")),
    ]

    result = partition_execution.run_partitioned_to_parquet(
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
    from schema_sanitizer.pipeline import partition_execution

    seen_registries = []

    def fake_to_parquet(input_path, output_path, **kwargs):
        """Return a fake JSON registry result while recording registry input."""
        seen_registries.append(kwargs["schema_registry"])
        generation = len(seen_registries)
        return SimpleNamespace(
            stats={"input": input_path, "output": output_path},
            schema_registry_json=f'{{"schema_generation":{generation}}}',
            schema_drifts_json="[]",
            native_registry_state=None,
        )

    monkeypatch.setattr(partition_execution, "to_parquet", fake_to_parquet)
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
    from schema_sanitizer.core_impl.schema_registry import current_native_registry_state
    from schema_sanitizer.pipeline import partition_execution

    states = [object(), object()]
    seen_states = []

    def fake_to_parquet(input_path, output_path, **kwargs):
        """Return a fake result while recording the incoming native state."""
        seen_states.append(current_native_registry_state())
        generation = len(seen_states)
        return SimpleNamespace(
            stats={"input": input_path, "output": output_path},
            schema_registry_json=f'{{"schema_generation":{generation}}}',
            schema_drifts_json="[]",
            native_registry_state=states[generation - 1],
        )

    monkeypatch.setattr(partition_execution, "to_parquet", fake_to_parquet)
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
    from schema_sanitizer.core_impl.schema_registry import current_native_registry_state
    from schema_sanitizer.pipeline import partition_execution

    initial_state = object()
    final_state = object()
    seen_states = []

    def fake_to_parquet(input_path, output_path, **kwargs):
        """Record initial native state handoff and return a replacement state."""
        seen_states.append(current_native_registry_state())
        return SimpleNamespace(
            stats={"input": input_path, "output": output_path},
            schema_registry_json='{"schema_generation":2}',
            schema_drifts_json="[]",
            native_registry_state=final_state,
        )

    monkeypatch.setattr(partition_execution, "to_parquet", fake_to_parquet)
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
    from schema_sanitizer.core_impl.schema_registry import current_native_registry_state
    from schema_sanitizer.pipeline import partition_execution

    compiled_state = object()
    seen_states = []
    compile_calls = []

    def fake_compile(registry_json, **kwargs):
        """Record registry compilation from durable JSON."""
        compile_calls.append((registry_json, kwargs["options"].field_name_policy))
        return compiled_state

    def fake_to_parquet(input_path, output_path, **kwargs):
        """Record native state visible to the first partition."""
        seen_states.append(current_native_registry_state())
        return SimpleNamespace(
            stats={"input": input_path, "output": output_path},
            schema_registry_json='{"schema_generation":2}',
            schema_drifts_json="[]",
            native_registry_state=None,
        )

    monkeypatch.setattr(partition_execution, "native_registry_state_from_json", fake_compile)
    monkeypatch.setattr(partition_execution, "to_parquet", fake_to_parquet)
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
    from schema_sanitizer.pipeline import partition_execution

    seen_to_parquet_kwargs = []

    def fake_compile(registry_json, **kwargs):
        """Return no native state after option normalization has succeeded."""
        assert registry_json == '{"schema_generation":1}'
        assert kwargs["options"] is not None
        assert kwargs["options"].field_name_policy == "lower_snake"
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

    monkeypatch.setattr(partition_execution, "native_registry_state_from_json", fake_compile)
    monkeypatch.setattr(partition_execution, "to_parquet", fake_to_parquet)

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
    from schema_sanitizer.core_impl.schema_registry import current_native_registry_state
    from schema_sanitizer.pipeline import partition_execution

    first_state = object()
    seen_states = []

    def fake_to_parquet(input_path, output_path, **kwargs):
        """Return a native state only for the first partition."""
        seen_states.append(current_native_registry_state())
        generation = len(seen_states)
        return SimpleNamespace(
            stats={"input": input_path, "output": output_path},
            schema_registry_json=f'{{"schema_generation":{generation}}}',
            schema_drifts_json="[]",
            native_registry_state=first_state if generation == 1 else None,
        )

    monkeypatch.setattr(partition_execution, "to_parquet", fake_to_parquet)
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
