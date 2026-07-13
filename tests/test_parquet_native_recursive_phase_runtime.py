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
    _recursive_fuzz_null_empty_matrix_specs,
    _recursive_fuzz_row_group_phase_labels,
    _recursive_fuzz_row_group_phase_matrix_specs,
)


@_requires_pyarrow
def test_native_parquet_stream_materializes_recursive_null_empty_matrix_corpus(
    tmp_path: Path,
) -> None:
    """Verify recursive list/map/struct values survive null/empty/full matrices."""
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

    def scalar_value(kind: str, seed: int) -> object:
        """Internal test helper."""
        if kind == "int64":
            return seed * 100 + 7
        if kind == "string":
            return f"matrix-{seed}"
        if kind == "bool":
            return seed % 2 == 0
        if kind == "float64":
            return seed + 0.875
        raise AssertionError(kind)

    def full_value(spec: object, seed: int) -> object:
        """Internal test helper."""
        kind = spec[0]
        if kind in set(_RECURSIVE_FUZZ_SCALARS):
            return scalar_value(kind, seed)
        if kind == "list":
            return [
                full_value(spec[1], seed + 1),
                sparse_value(spec[1], seed + 2),
                None,
            ]
        if kind == "map":
            return [
                (f"full-{seed}", full_value(spec[1], seed + 1)),
                (f"sparse-{seed}", sparse_value(spec[1], seed + 2)),
                (f"null-{seed}", None),
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

    def sparse_value(spec: object, seed: int) -> object:
        """Internal test helper."""
        kind = spec[0]
        if kind in set(_RECURSIVE_FUZZ_SCALARS):
            return None if seed % 2 == 0 else scalar_value(kind, seed)
        if kind == "list":
            return [empty_value(spec[1], seed + 1), None, full_value(spec[1], seed + 2)]
        if kind == "map":
            return [
                (f"empty-{seed}", empty_value(spec[1], seed + 1)),
                (f"none-{seed}", None),
                (f"full-{seed}", full_value(spec[1], seed + 2)),
            ]
        if kind == "struct":
            return {
                name: (
                    None
                    if offset % 3 == 0
                    else empty_value(child, seed + offset)
                    if offset % 3 == 1
                    else full_value(child, seed + offset)
                )
                for offset, (name, child) in enumerate(spec[1])
            }
        raise AssertionError(kind)

    require_native()
    for index, (name, spec, metrics) in enumerate(_recursive_fuzz_null_empty_matrix_specs()):
        item_type = arrow_type(spec)
        schema = pa.schema([pa.field("items", item_type)])
        batches = [
            pa.record_batch(
                [
                    pa.array(
                        [
                            None,
                            empty_value(spec, index + 10),
                            full_value(spec, index + 20),
                        ],
                        type=item_type,
                    )
                ],
                schema=schema,
            ),
            pa.record_batch(
                [
                    pa.array(
                        [
                            sparse_value(spec, index + 30),
                            full_value(spec, index + 40),
                        ],
                        type=item_type,
                    )
                ],
                schema=schema,
            ),
        ]
        expected = pa.Table.from_batches(batches)
        path = tmp_path / f"native-recursive-null-empty-matrix-{index}.parquet"
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
        assert field["leaf_count"] >= metrics["leaf_count"], name
        assert field["repetition_depth"] >= metrics["repetition_depth"], name
        assert field["shape_signature"], name
        assert field["structural_shape_signature"], name
        assert "#" in field["shape_signature"], name
        assert "#" not in field["structural_shape_signature"], name
        assert len(field["leaf_paths"]) == field["leaf_count"], name
        assert len(field["repeated_node_paths"]) >= metrics["list_count"] + metrics["map_count"], (
            name
        )

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
def test_native_parquet_stream_materializes_recursive_row_group_phase_matrix_corpus(
    tmp_path: Path,
) -> None:
    """Verify each row group can carry a different recursive null/empty phase."""
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

    def scalar_value(kind: str, seed: int) -> object:
        """Internal test helper."""
        if kind == "int64":
            return seed * 1000 + 13
        if kind == "string":
            return f"phase-{seed}"
        if kind == "bool":
            return seed % 2 == 1
        if kind == "float64":
            return seed + 0.0625
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

    def full_value(spec: object, seed: int) -> object:
        """Internal test helper."""
        kind = spec[0]
        if kind in set(_RECURSIVE_FUZZ_SCALARS):
            return scalar_value(kind, seed)
        if kind == "list":
            return [full_value(spec[1], seed + 1), empty_value(spec[1], seed + 2), None]
        if kind == "map":
            return [
                (f"full-{seed}", full_value(spec[1], seed + 1)),
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
            return None if seed % 2 == 0 else scalar_value(kind, seed)
        if kind == "list":
            return [empty_value(spec[1], seed + 1), None, full_value(spec[1], seed + 2)]
        if kind == "map":
            return [
                (f"empty-{seed}", empty_value(spec[1], seed + 1)),
                (f"none-{seed}", None),
                (f"full-{seed}", full_value(spec[1], seed + 2)),
            ]
        if kind == "struct":
            return {
                name: (
                    None
                    if offset % 3 == 0
                    else empty_value(child, seed + offset)
                    if offset % 3 == 1
                    else full_value(child, seed + offset)
                )
                for offset, (name, child) in enumerate(spec[1])
            }
        raise AssertionError(kind)

    def phase_values(spec: object, phase: str, seed: int) -> list[object]:
        """Internal test helper."""
        if phase == "all-null":
            return [None, None]
        if phase == "empty-only":
            return [empty_value(spec, seed), empty_value(spec, seed + 1)]
        if phase == "sparse":
            return [sparse_value(spec, seed), None]
        if phase == "full":
            return [full_value(spec, seed), sparse_value(spec, seed + 1)]
        raise AssertionError(phase)

    require_native()
    for index, (name, spec, metrics) in enumerate(_recursive_fuzz_row_group_phase_matrix_specs()):
        item_type = arrow_type(spec)
        schema = pa.schema([pa.field("items", item_type)])
        batches = [
            pa.record_batch(
                [pa.array(phase_values(spec, phase, index * 100 + offset * 10), type=item_type)],
                schema=schema,
            )
            for offset, phase in enumerate(_recursive_fuzz_row_group_phase_labels())
        ]
        expected = pa.Table.from_batches(batches)
        path = tmp_path / f"native-recursive-phase-matrix-{index}.parquet"
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
        assert info["row_group_count"] == len(_recursive_fuzz_row_group_phase_labels()), name
        structural_signatures: set[str] = set()
        physical_signatures: set[str] = set()
        for row_group in info["row_groups"]:
            layout = row_group["native_recursive_output_layout"]
            assert layout["decoded"] == 1, name
            assert layout["field_count"] == 1, name
            field = layout["fields"][0]
            assert field["name"] == "items", name
            assert field["leaf_count"] >= metrics["leaf_count"], name
            assert field["node_count"] >= metrics["node_count"], name
            assert field["repetition_depth"] >= metrics["repetition_depth"], name
            assert len(field["leaf_paths"]) == field["leaf_count"], name
            assert (
                len(field["repeated_node_paths"]) >= metrics["list_count"] + metrics["map_count"]
            ), name
            structural_signatures.add(field["structural_shape_signature"])
            physical_signatures.add(field["shape_signature"])
        assert len(structural_signatures) == 1, name
        assert len(physical_signatures) == 1, name

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
def test_native_parquet_stream_preserves_recursive_segmentation_invariants(
    tmp_path: Path,
) -> None:
    """Verify deep recursive payloads read identically under row-group resegmentation."""
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
            return seed * 29 + 7
        if kind == "string":
            return f"segment-{seed}"
        if kind == "bool":
            return seed % 2 == 0
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
            return [full_value(spec[1], seed + 1), empty_value(spec[1], seed + 2), None]
        if kind == "map":
            return [
                (f"full-{seed}", full_value(spec[1], seed + 1)),
                (f"empty-{seed}", empty_value(spec[1], seed + 2)),
                (f"none-{seed}", None),
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
            return [(f"none-{seed}", None), (f"full-{seed}", full_value(spec[1], seed + 1))]
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

    def phase_value(spec: object, phase: str, seed: int) -> object:
        """Internal test helper."""
        if phase == "all-null":
            return None
        if phase == "empty-only":
            return empty_value(spec, seed)
        if phase == "sparse":
            return sparse_value(spec, seed)
        if phase == "full":
            return full_value(spec, seed)
        raise AssertionError(phase)

    def write_segments(path: Path, lengths: tuple[int, ...]) -> None:
        """Internal test helper."""
        batches = []
        offset = 0
        for length in lengths:
            batches.extend(table.slice(offset, length).to_batches(max_chunksize=length))
            offset += length
        assert offset == table.num_rows
        write_parquet_native_first_stream(
            pa.RecordBatchReader.from_batches(schema, batches),
            path,
            feature="test",
            parquet_compression="uncompressed",
        )

    require_native()
    _, spec, metrics = _recursive_fuzz_row_group_phase_matrix_specs()[4]
    phases = _recursive_fuzz_row_group_phase_labels() * 2
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("payload", arrow_type(spec)),
        ]
    )
    table = pa.Table.from_pylist(
        [
            {
                "id": row_index,
                "payload": phase_value(spec, phase, row_index * 100),
            }
            for row_index, phase in enumerate(phases)
        ],
        schema=schema,
    )
    expected = table.select(["payload"])
    segmentations = {
        "single": (table.num_rows,),
        "phase_pairs": (2, 2, 2, 2),
        "irregular": (1, 3, 1, 3),
        "per_row": (1, 1, 1, 1, 1, 1, 1, 1),
    }
    summaries = {}

    for label, lengths in segmentations.items():
        path = tmp_path / f"native-recursive-segmentation-{label}.parquet"
        write_segments(path, lengths)
        summary = native_parquet_recursive_layout_summary(path, columns=["payload"])
        assert summary is not None, label
        assert summary["native_reader_ready"] == 1, label
        assert summary["stable_across_row_groups"] is True, label
        assert summary["row_group_count"] == len(lengths), label
        assert summary["row_group_layout_fingerprints_stable"] is True, label
        assert summary["row_group_leaf_level_fingerprints_stable"] is True, label
        assert summary["row_group_repetition_path_fingerprints_stable"] is True, label
        assert summary["row_group_repeated_ancestor_fingerprints_stable"] is True, label
        assert summary["canonical_leaf_repeated_ancestor_fingerprint"], label
        assert summary["fields"][0]["repetition_depth_max"] >= metrics["repetition_depth"], label
        assert set(summary["row_group_canonical_layout_fingerprints"]) == {
            summary["canonical_layout_fingerprint"]
        }, label
        summaries[label] = summary

        factory = open_parquet_record_batch_stream_factory(
            path,
            source="path",
            feature="test",
            columns=["payload"],
        )
        out = pa.RecordBatchReader.from_stream(factory).read_all()
        assert out.schema.equals(expected.schema), label
        assert out.to_pylist() == expected.to_pylist(), label
        assert last_parquet_stream_factory_route() == "native_parquet_stream", label

    baseline = summaries["single"]
    for label, summary in summaries.items():
        assert (
            summary["canonical_layout_fingerprint"] == (baseline["canonical_layout_fingerprint"])
        ), label
        assert (
            summary["canonical_leaf_level_fingerprint"]
            == (baseline["canonical_leaf_level_fingerprint"])
        ), label
        assert (
            summary["canonical_leaf_repetition_path_fingerprint"]
            == (baseline["canonical_leaf_repetition_path_fingerprint"])
        ), label
        assert (
            summary["canonical_leaf_repeated_ancestor_fingerprint"]
            == (baseline["canonical_leaf_repeated_ancestor_fingerprint"])
        ), label
