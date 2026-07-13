"""Native Parquet recursive nested grammar runtime tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import require_native

try:
    import pyarrow as pa

    _HAVE_PYARROW = True
except ModuleNotFoundError:  # pragma: no cover
    pa = None
    _HAVE_PYARROW = False

_requires_pyarrow = pytest.mark.skipif(not _HAVE_PYARROW, reason="pyarrow not installed")

from parquet_recursive_fuzz_helpers import (
    _RECURSIVE_FUZZ_SCALARS,
    _recursive_fuzz_cartesian_specs,
    _recursive_fuzz_seeded_specs,
)


@_requires_pyarrow
def test_native_parquet_stream_materializes_cartesian_recursive_grammar_corpus(
    tmp_path: Path,
) -> None:
    """Verify exhaustive bounded list/map/struct operation words stay native."""
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

    def arrow_type(spec: object) -> pa.DataType:
        """Internal test helper."""
        kind = spec[0]
        if kind == "int64":
            return pa.int64()
        if kind == "string":
            return pa.string()
        if kind == "bool":
            return pa.bool_()
        if kind == "float64":
            return pa.float64()
        if kind == "list":
            return pa.list_(arrow_type(spec[1]))
        if kind == "map":
            return pa.map_(pa.string(), arrow_type(spec[1]))
        if kind == "struct":
            return pa.struct([pa.field(name, arrow_type(child)) for name, child in spec[1]])
        raise AssertionError(kind)

    def full_value(spec: object, seed: int) -> object:
        """Internal test helper."""
        kind = spec[0]
        if kind == "int64":
            return seed * 10 + 1
        if kind == "string":
            return f"value-{seed}"
        if kind == "bool":
            return seed % 2 == 0
        if kind == "float64":
            return seed + 0.5
        if kind == "list":
            return [full_value(spec[1], seed + 1), empty_value(spec[1], seed + 2), None]
        if kind == "map":
            return [
                (f"k{seed}", full_value(spec[1], seed + 1)),
                (f"empty{seed}", empty_value(spec[1], seed + 2)),
                (f"none{seed}", None),
            ]
        if kind == "struct":
            return {
                name: full_value(child, seed + offset + 1)
                for offset, (name, child) in enumerate(spec[1])
            }
        raise AssertionError(kind)

    def empty_value(spec: object, seed: int) -> object:
        """Internal test helper."""
        del seed
        kind = spec[0]
        if kind in set(_RECURSIVE_FUZZ_SCALARS):
            return None
        if kind == "list":
            return []
        if kind == "map":
            return []
        if kind == "struct":
            return {
                name: empty_value(child, offset) for offset, (name, child) in enumerate(spec[1])
            }
        raise AssertionError(kind)

    require_native()
    for index, (name, spec, metrics) in enumerate(_recursive_fuzz_cartesian_specs()):
        item_type = arrow_type(spec)
        schema = pa.schema([pa.field("items", item_type)])
        batches = [
            pa.record_batch(
                [pa.array([full_value(spec, index + 10), None], type=item_type)],
                schema=schema,
            ),
            pa.record_batch(
                [
                    pa.array(
                        [empty_value(spec, index + 20), full_value(spec, index + 30)],
                        type=item_type,
                    )
                ],
                schema=schema,
            ),
        ]
        expected = pa.Table.from_batches(batches)
        path = tmp_path / f"native-recursive-cartesian-{name}.parquet"
        write_parquet_native_first_stream(
            pa.RecordBatchReader.from_batches(schema, batches),
            path,
            feature="test",
            parquet_compression="uncompressed",
        )

        info = native_parquet_footer_info(path)

        assert info is not None, name
        assert info["native_reader_ready"] == 1, name
        assert info["native_reader_blockers"] == [], name
        assert info["row_group_count"] == 2, name
        layout = info["row_groups"][0]["native_recursive_output_layout"]
        assert layout["decoded"] == 1, name
        field = layout["fields"][0]
        assert field["shape_signature"], name
        assert field["structural_shape_signature"], name
        assert "#" in field["shape_signature"], name
        assert "#" not in field["structural_shape_signature"], name
        assert len(field["leaf_paths"]) == field["leaf_count"], name
        assert len(field["repeated_node_paths"]) >= metrics["list_count"] + metrics["map_count"], (
            name
        )
        assert field["node_count"] >= metrics["node_count"], name
        assert field["leaf_count"] >= metrics["leaf_count"], name
        assert field["repetition_depth"] >= metrics["repetition_depth"], name
        if metrics["list_count"]:
            assert "list(" in field["shape_signature"], name
        if metrics["map_count"]:
            assert "map(" in field["shape_signature"], name
        if metrics["struct_count"]:
            assert "struct(" in field["shape_signature"], name

        factory = open_parquet_record_batch_stream_factory(
            path,
            source="path",
            feature="test",
        )
        reader = pa.RecordBatchReader.from_stream(factory)
        out = reader.read_all()

        assert out.schema.equals(expected.schema), name
        assert out.to_pylist() == expected.to_pylist(), name
        assert last_parquet_stream_factory_route() == "native_parquet_stream", name


@_requires_pyarrow
def test_native_parquet_stream_materializes_seeded_recursive_fuzzer_corpus(
    tmp_path: Path,
) -> None:
    """Verify irregular seeded recursive shapes round-trip on the native path."""
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

    def arrow_type(spec: object) -> pa.DataType:
        """Internal test helper."""
        kind = spec[0]
        if kind == "int64":
            return pa.int64()
        if kind == "string":
            return pa.string()
        if kind == "bool":
            return pa.bool_()
        if kind == "float64":
            return pa.float64()
        if kind == "list":
            return pa.list_(arrow_type(spec[1]))
        if kind == "map":
            return pa.map_(pa.string(), arrow_type(spec[1]))
        if kind == "struct":
            return pa.struct([pa.field(name, arrow_type(child)) for name, child in spec[1]])
        raise AssertionError(kind)

    def scalar_value(kind: str, seed: int) -> object:
        """Internal test helper."""
        if kind == "int64":
            return seed * 17
        if kind == "string":
            return f"seeded-recursive-{seed}"
        if kind == "bool":
            return seed % 2 == 1
        if kind == "float64":
            return seed + 0.875
        raise AssertionError(kind)

    def empty_value(spec: object, seed: int) -> object:
        """Internal test helper."""
        del seed
        kind = spec[0]
        if kind in set(_RECURSIVE_FUZZ_SCALARS):
            return None
        if kind == "list":
            return []
        if kind == "map":
            return []
        if kind == "struct":
            return {
                name: empty_value(child, child_index)
                for child_index, (name, child) in enumerate(spec[1])
            }
        raise AssertionError(kind)

    def full_value(spec: object, seed: int) -> object:
        """Internal test helper."""
        kind = spec[0]
        if kind in set(_RECURSIVE_FUZZ_SCALARS):
            return scalar_value(kind, seed)
        if kind == "list":
            return [full_value(spec[1], seed + 1), empty_value(spec[1], seed + 2)]
        if kind == "map":
            return [
                (f"k{seed}", full_value(spec[1], seed + 1)),
                (f"empty{seed}", empty_value(spec[1], seed + 2)),
            ]
        if kind == "struct":
            return {
                name: full_value(child, seed + child_index + 1)
                for child_index, (name, child) in enumerate(spec[1])
            }
        raise AssertionError(kind)

    def sparse_value(spec: object, seed: int) -> object:
        """Internal test helper."""
        kind = spec[0]
        if kind in set(_RECURSIVE_FUZZ_SCALARS):
            return None if seed % 3 == 0 else scalar_value(kind, seed)
        if kind == "list":
            return [None, empty_value(spec[1], seed + 1), full_value(spec[1], seed + 2)]
        if kind == "map":
            return [(f"s{seed}", None), (f"v{seed}", full_value(spec[1], seed + 1))]
        if kind == "struct":
            return {
                name: (
                    sparse_value(child, seed + child_index)
                    if child_index % 2 == 0
                    else empty_value(child, seed + child_index)
                )
                for child_index, (name, child) in enumerate(spec[1])
            }
        raise AssertionError(kind)

    require_native()
    selected_indexes = (0, 1, 2, 4, 5, 8, 14, 23)
    cases = [_recursive_fuzz_seeded_specs()[index] for index in selected_indexes]

    for case_index, (name, spec, metrics) in enumerate(cases):
        item_type = arrow_type(spec)
        schema = pa.schema([pa.field("payload", item_type), pa.field("case", pa.string())])
        batch_one = pa.record_batch(
            [
                pa.array([None, empty_value(spec, case_index)], type=item_type),
                pa.array([f"{name}-null", f"{name}-empty"], type=pa.string()),
            ],
            schema=schema,
        )
        batch_two = pa.record_batch(
            [
                pa.array(
                    [sparse_value(spec, case_index * 100), full_value(spec, case_index * 100 + 50)],
                    type=item_type,
                ),
                pa.array([f"{name}-sparse", f"{name}-full"], type=pa.string()),
            ],
            schema=schema,
        )
        expected = pa.Table.from_batches([batch_one, batch_two])
        path = tmp_path / f"native-seeded-recursive-{case_index}.parquet"
        write_parquet_native_first_stream(
            pa.RecordBatchReader.from_batches(schema, [batch_one, batch_two]),
            path,
            feature="test",
            parquet_compression="uncompressed",
        )

        summary = native_parquet_recursive_layout_summary(path, columns=["payload"])
        assert summary is not None, name
        assert summary["stable_across_row_groups"] is True, name
        assert summary["field_order"] == ["payload"], name
        assert summary["layout_fingerprint"] == summary["fields"][0]["field_fingerprint"], name
        assert summary["leaf_path_collisions"] == [], name
        expected_physical_leaves = metrics["leaf_count"] + metrics["map_count"]
        assert summary["fields"][0]["leaf_count_max"] == expected_physical_leaves, name
        assert summary["fields"][0]["repetition_depth_max"] == metrics["repetition_depth"], name

        factory = open_parquet_record_batch_stream_factory(
            path,
            source="path",
            feature="test",
            columns=["payload"],
        )
        out = pa.RecordBatchReader.from_stream(factory).read_all()

        assert out.schema.equals(expected.select(["payload"]).schema), name
        assert out.to_pylist() == expected.select(["payload"]).to_pylist(), name
        assert last_parquet_stream_factory_route() == "native_parquet_stream", name
