"""Remote pipeline observability tests."""

# ruff: noqa: F405

from __future__ import annotations

from pipeline_shared import *  # noqa: F403


def test_pipeline_native_registry_stream_handles_normal_partition_drift(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Verify native registry streams update the registry when normal partitions drift."""
    pq = pytest.importorskip("pyarrow.parquet")
    from schema_sanitizer.api_impl.source_plan import registry as source_plan_registry_stream

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
    real_registry_stream = source_plan_registry_stream.open_source_plan_registry_stream

    def tracking_registry_stream(*args, **kwargs):
        """Track native registry stream use while preserving behavior."""
        nonlocal registry_stream_calls
        registry_stream_calls += 1
        return real_registry_stream(*args, **kwargs)

    monkeypatch.setattr(
        source_plan_registry_stream,
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
    from schema_sanitizer.remote_impl import staging as remote_staging
    from schema_sanitizer.remote_impl import sync_backend
    from schema_sanitizer.remote_impl.staging import StagedPath
    from schema_sanitizer.sources import RemoteFile

    payloads = {
        "s3://bucket/warm/warm.json": {"id": "warm-row", "warm_only": 1},
        "s3://bucket/normal/normal.json": {"id": "normal-row", "normal_only": 2},
    }

    def fake_list_remote_directory_files(uri, suffixes, *, memory_limit_bytes=None):
        """Return a single child for each fake remote directory."""
        assert suffixes == (".json",)
        if uri == "s3://bucket/warm/":
            return [RemoteFile("s3://bucket/warm/warm.json", "warm.json", None)]
        if uri == "s3://bucket/normal/":
            return [RemoteFile("s3://bucket/normal/normal.json", "normal.json", None)]
        raise AssertionError(uri)

    def fake_stage_remote_files_to_directory(files, **kwargs):
        """Stage only requested fake remote files."""
        assert isinstance(kwargs["memory_limit_bytes"], int)
        assert kwargs["memory_limit_bytes"] > 0
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
        sync_backend,
        "list_remote_directory",
        fake_list_remote_directory_files,
    )
    monkeypatch.setattr(
        remote_staging,
        "stage_remote_files_to_directory",
        fake_stage_remote_files_to_directory,
    )

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
    from schema_sanitizer.pipeline import partition_execution

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
            native_registry_state=None,
        )

    monkeypatch.setattr(partition_execution, "to_parquet", fake_to_parquet)
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
    assert estimate_cpu_io_wall_time(4.0, 1.25) == pytest.approx((1.25, 2.75))
    assert estimate_cpu_io_wall_time(4.0, 6.0) == pytest.approx((4.0, 0.0))
    cpu_percent, io_percent = cpu_io_wall_percentages(3.0, 1.0)
    assert (cpu_percent, io_percent) == (33.3, 66.7)
    assert cpu_percent + io_percent == 100.0
    assert cpu_io_wall_percentages(4.0, 6.0) == (100.0, 0.0)
    assert cpu_io_wall_percentages(0.0, 0.0) == (0.0, 100.0)


def test_pipeline_read_parquet_schema_handles_local_paths(tmp_path) -> None:
    """Verify reusable Parquet schema reads local output schemas."""
    pa = pytest.importorskip("pyarrow")
    pq = __import__("pyarrow.parquet").parquet
    target = tmp_path / "rows.parquet"
    schema = pa.schema([pa.field("id", pa.int64())])
    pq.write_table(pa.table({"id": [1]}, schema=schema), target)

    assert read_parquet_schema(str(target)).equals(schema)
