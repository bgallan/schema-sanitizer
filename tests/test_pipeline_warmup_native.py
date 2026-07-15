"""Native pipeline warmup tests."""

# ruff: noqa: F405

from __future__ import annotations

from pipeline_shared import *  # noqa: F403


def test_pipeline_warm_up_prefers_native_auto_registry_stream(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Verify warm-up shares the normal native auto-registry source-plan path."""
    source = tmp_path / "a.jsonl"
    source.write_text('{"id": 1}\n', encoding="utf-8")
    closed: list[str] = []
    calls: list[tuple[object, dict[str, object]]] = []
    created_sources: list[list[tuple[str, str, str]]] = []
    native_plan = object()

    class FakeRaw:
        """Fake native registry stream result."""

        schema_registry_json = '{"schema_generation":1}'
        schema_drifts_json = "[]"
        conversion_timestamp = "2026-01-01T00:00:00Z"
        field_names = ("id",)
        native_registry_state = "compiled-state"

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

    import schema_sanitizer.input_impl.source_plan as path_sources_impl
    from schema_sanitizer.pipeline import registry_warmup

    def create_native_plan(sources, *_args):
        """Capture source descriptors and return one opaque native plan."""
        created_sources.append(list(sources))
        return native_plan

    monkeypatch.setattr(path_sources_impl, "PATH_SOURCE_PLAN_CREATE", create_native_plan)
    monkeypatch.setattr(registry_warmup, "default_execution_context", lambda: FakePool().get()._raw)

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
    assert created_sources == [[("json", str(source), str(source))]]
    assert calls == [
        (
            native_plan,
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
    from schema_sanitizer.pipeline import registry_warmup

    assert not hasattr(registry_warmup, "probe_source_plan_registry")
    assert hasattr(registry_warmup, "probe_prepared_source_plan_registry")


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
    """Verify full to_parquet kwargs do not leak writer options into warm-up registry_warmup."""
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

    from schema_sanitizer.pipeline.registry_warmup import last_warm_up_route

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

    from schema_sanitizer.pipeline.registry_warmup import last_warm_up_route

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

    from schema_sanitizer.pipeline.registry_warmup import last_warm_up_route

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

    from schema_sanitizer.pipeline.registry_warmup import last_warm_up_route

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

    from schema_sanitizer.pipeline import registry_warmup as warm_up_input

    assert not hasattr(warm_up_input, "_route_prepared_inputs_for_warm_up")

    prepared = warm_up_input.prepare_schema_warm_up_input(
        [PartitionRunPlan(date(2026, 1, 1), str(source), str(tmp_path / "out.parquet"))],
        input_format=input_format,
        input_mode="directory",
        input_text_encoding="utf-8",
        xml_row_tag="row",
        csv_delimiter=",",
        csv_has_header=True,
        memory_limit_bytes=None,
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
    from schema_sanitizer.api_impl.input.preparation import prepare_public_input
    from schema_sanitizer.api_impl.source_plan.attached import source_plan_from_data
    from schema_sanitizer.pipeline.registry_warmup import prepare_schema_warm_up_input

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
        memory_limit_bytes=None,
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
    """Verify warm-up data only seeds schema registry_warmup, never normal output rows."""
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
    from schema_sanitizer.api_impl.source_plan import registry as source_plan_registry_stream

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


def test_pipeline_directory_warm_up_reuses_discovered_input(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """A discovered partition must not trigger a second directory listing."""
    source = tmp_path / "part.jsonl"
    source.write_text('{"alpha": 1}\n', encoding="utf-8")

    from schema_sanitizer.input_impl.directory_inputs import DiscoveredDirectoryInput
    from schema_sanitizer.input_impl.selection import single_file_descriptor
    from schema_sanitizer.pipeline import registry_warmup
    from schema_sanitizer.remote_impl import routing

    def fail_listing(*_args, **_kwargs):
        """Fail when warm-up performs a redundant directory listing."""
        raise AssertionError("warm-up relisted an already discovered directory")

    monkeypatch.setattr(routing, "list_remote_directory", fail_listing)
    plan = PartitionRunPlan(
        date(2026, 1, 1),
        "gs://bucket/date=2026-01-01",
        str(tmp_path / "out.parquet"),
        discovered_input=DiscoveredDirectoryInput(
            input_format="jsonl",
            local_files=(single_file_descriptor(source),),
        ),
    )

    prepared = registry_warmup.prepare_schema_warm_up_input(
        [plan],
        input_format="jsonl",
        input_mode="directory",
    )
    try:
        assert prepared.source == "source_plan"
        assert prepared.format == "jsonl"
    finally:
        prepared.close()
