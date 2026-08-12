"""Native Parquet recursive nested grammar runtime tests."""

from __future__ import annotations

from pathlib import Path

from _support.parquet_recursive_cases import (
    _RECURSIVE_FUZZ_SCALARS,
    _recursive_fuzz_cartesian_specs,
    _recursive_fuzz_full_value_factory,
    _recursive_fuzz_phase_value_factory,
    _recursive_fuzz_row_group_phase_labels,
    _recursive_fuzz_row_group_phase_matrix_specs,
)
from _support.parquet_recursive_cases import (
    _recursive_fuzz_empty_value as empty_value,
)
from _support.parquet_runtime import pa
from _support.parquet_runtime import recursive_arrow_type as arrow_type
from _support.parquet_runtime import requires_pyarrow as _requires_pyarrow
from conftest import require_native


@_requires_pyarrow
def test_native_parquet_stream_projects_recursive_row_group_phase_roots(
    tmp_path: Path,
) -> None:
    """Verify projection over several deep recursive roots across phase row groups."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_stream_factory_route,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

    def scalar_value(kind: str, seed: int) -> object:
        """Internal test helper."""
        if kind == "int64":
            return seed * 17
        if kind == "string":
            return f"projected-phase-{seed}"
        if kind == "bool":
            return seed % 2 == 0
        if kind == "float64":
            return seed + 0.5
        raise AssertionError(kind)

    def full_value(spec: object, seed: int) -> object:
        """Internal test helper."""
        kind = spec[0]
        if kind in set(_RECURSIVE_FUZZ_SCALARS):
            return scalar_value(kind, seed)
        if kind == "list":
            return [full_value(spec[1], seed + 1), empty_value(spec[1], seed + 2), None]
        if kind == "map":
            return [
                (f"k-{seed}", full_value(spec[1], seed + 1)),
                (f"empty-{seed}", empty_value(spec[1], seed + 2)),
                (f"none-{seed}", None),
            ]
        if kind == "struct":
            return {
                name: full_value(child, seed + offset + 1)
                for offset, (name, child) in enumerate(spec[1])
            }
        raise AssertionError(kind)

    def sparse_value(spec: object, seed: int) -> object:
        """Internal test helper."""
        kind = spec[0]
        if kind in set(_RECURSIVE_FUZZ_SCALARS):
            return None if seed % 2 else scalar_value(kind, seed)
        if kind == "list":
            return [empty_value(spec[1], seed + 1), None, full_value(spec[1], seed + 2)]
        if kind == "map":
            return [(f"s-{seed}", empty_value(spec[1], seed + 1)), (f"n-{seed}", None)]
        if kind == "struct":
            return {
                name: (
                    full_value(child, seed + offset)
                    if offset % 2 == 0
                    else empty_value(child, seed + offset)
                )
                for offset, (name, child) in enumerate(spec[1])
            }
        raise AssertionError(kind)

    phase_value = _recursive_fuzz_phase_value_factory(sparse_value, full_value)

    require_native()
    specs = _recursive_fuzz_row_group_phase_matrix_specs()[:3]
    field_names = ["alpha", "beta", "gamma"]
    schema = pa.schema(
        [pa.field("id", pa.int64())]
        + [
            pa.field(field_name, arrow_type(spec))
            for field_name, (_, spec, _) in zip(field_names, specs)
        ]
        + [pa.field("tail", pa.string())]
    )
    batches = []
    for offset, phase in enumerate(_recursive_fuzz_row_group_phase_labels()):
        columns = [
            pa.array([offset + 1, offset + 101], type=pa.int64()),
        ]
        for spec_index, (_, spec, _) in enumerate(specs):
            field_type = schema.field(field_names[spec_index]).type
            columns.append(
                pa.array(
                    [
                        phase_value(spec, phase, offset * 100 + spec_index * 10),
                        full_value(spec, offset * 100 + spec_index * 10 + 50),
                    ],
                    type=field_type,
                )
            )
        columns.append(pa.array([f"{phase}-a", f"{phase}-b"], type=pa.string()))
        batches.append(pa.record_batch(columns, schema=schema))

    full_table = pa.Table.from_batches(batches)
    path = tmp_path / "native-recursive-phase-projection-roots.parquet"
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(schema, batches),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )

    info = native_parquet_footer_info(path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    assert info["row_group_count"] == len(_recursive_fuzz_row_group_phase_labels())

    projected_cases = (
        ["gamma"],
        ["beta", "alpha"],
        ["gamma", "id", "alpha"],
        ["tail", "gamma", "beta", "alpha"],
    )
    for columns in projected_cases:
        projected_info = native_parquet_footer_info(path, columns=columns)
        assert projected_info is not None, columns
        assert projected_info["native_reader_ready"] == 1, columns
        for row_group in projected_info["row_groups"]:
            layout = row_group["native_recursive_output_layout"]
            assert layout["decoded"] == 1, columns
            assert [field["name"] for field in layout["fields"]] == columns, columns

        factory = open_parquet_record_batch_stream_factory(
            path,
            source="path",
            feature="test",
            columns=columns,
        )
        reader = pa.RecordBatchReader.from_stream(factory)
        out = reader.read_all()
        expected = full_table.select(columns)

        assert out.schema.equals(expected.schema), columns
        assert out.to_pylist() == expected.to_pylist(), columns
        assert last_parquet_stream_factory_route() == "native_parquet_stream", columns


@_requires_pyarrow
def test_native_parquet_recursive_layout_summary_tracks_projected_noise_roots(
    tmp_path: Path,
) -> None:
    """Verify deep unprojected recursive roots do not perturb a projected root."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import (
        native_parquet_recursive_layout_summary,
    )
    from schema_sanitizer.adapters.parquet.telemetry import (
        last_parquet_stream_factory_route,
    )
    from schema_sanitizer.api_impl.file_conversion.writers import (
        write_parquet_native_first_stream,
    )

    def scalar_value(kind: str, seed: int) -> object:
        """Internal test helper."""
        if kind == "int64":
            return seed * 31
        if kind == "string":
            return f"noise-projection-{seed}"
        if kind == "bool":
            return seed % 2 == 0
        if kind == "float64":
            return seed + 0.125
        raise AssertionError(kind)

    full_value = _recursive_fuzz_full_value_factory(scalar_value, include_null=True)

    def sparse_value(spec: object, seed: int) -> object:
        """Internal test helper."""
        kind = spec[0]
        if kind in set(_RECURSIVE_FUZZ_SCALARS):
            return None if seed % 2 == 0 else scalar_value(kind, seed)
        if kind == "list":
            return [empty_value(spec[1], seed + 1), None, full_value(spec[1], seed + 2)]
        if kind == "map":
            return [(f"s-{seed}", empty_value(spec[1], seed + 1)), (f"n-{seed}", None)]
        if kind == "struct":
            return {
                name: (
                    full_value(child, seed + offset)
                    if offset % 2 == 0
                    else empty_value(child, seed + offset)
                )
                for offset, (name, child) in enumerate(spec[1])
            }
        raise AssertionError(kind)

    phase_value = _recursive_fuzz_phase_value_factory(sparse_value, full_value)

    require_native()
    target_spec = _recursive_fuzz_row_group_phase_matrix_specs()[0][1]
    noise_specs = [spec for _, spec, _ in _recursive_fuzz_cartesian_specs()[:12]]
    schema = pa.schema(
        [pa.field("id", pa.int64()), pa.field("target", arrow_type(target_spec))]
        + [pa.field(f"noise_{index}", arrow_type(spec)) for index, spec in enumerate(noise_specs)]
        + [pa.field("tail", pa.string())]
    )
    batches = []
    for offset, phase in enumerate(_recursive_fuzz_row_group_phase_labels()):
        columns = [
            pa.array([offset, offset + 1000], type=pa.int64()),
            pa.array(
                [
                    phase_value(target_spec, phase, offset * 100),
                    full_value(target_spec, offset * 100 + 50),
                ],
                type=schema.field("target").type,
            ),
        ]
        for noise_index, spec in enumerate(noise_specs):
            columns.append(
                pa.array(
                    [
                        phase_value(spec, phase, offset * 1000 + noise_index * 10),
                        full_value(spec, offset * 1000 + noise_index * 10 + 5),
                    ],
                    type=schema.field(f"noise_{noise_index}").type,
                )
            )
        columns.append(pa.array([f"{phase}-x", f"{phase}-y"], type=pa.string()))
        batches.append(pa.record_batch(columns, schema=schema))

    table = pa.Table.from_batches(batches)
    path = tmp_path / "native-recursive-projection-noise-roots.parquet"
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(schema, batches),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )

    full_summary = native_parquet_recursive_layout_summary(path)
    projected_summary = native_parquet_recursive_layout_summary(path, columns=["target"])

    assert full_summary is not None
    assert projected_summary is not None
    assert full_summary["stable_across_row_groups"] is True
    assert projected_summary["stable_across_row_groups"] is True
    assert full_summary["field_order"] == [field.name for field in schema]
    assert projected_summary["field_order"] == ["target"]
    assert (
        projected_summary["fields"][0]["structural_shape_signature"]
        == (full_summary["fields"][1]["structural_shape_signature"])
    )
    assert projected_summary["fields"][0]["repetition_depth_max"] >= 4
    assert full_summary["fields"][1]["leaf_paths"] == projected_summary["fields"][0]["leaf_paths"]
    assert full_summary["leaf_path_collisions"] == []
    assert projected_summary["leaf_path_collisions"] == []
    assert full_summary["layout_fingerprint"]
    assert projected_summary["layout_fingerprint"]
    assert (
        projected_summary["fields"][0]["field_fingerprint"]
        == (full_summary["fields"][1]["field_fingerprint"])
    )
    assert (
        projected_summary["layout_fingerprint"]
        == (projected_summary["fields"][0]["field_fingerprint"])
    )

    factory = open_parquet_record_batch_stream_factory(
        path,
        source="path",
        feature="test",
        columns=["target"],
    )
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()
    expected = table.select(["target"])

    assert out.schema.equals(expected.schema)
    assert out.to_pylist() == expected.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
