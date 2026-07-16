"""Native Parquet recursive nested grammar runtime tests."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native
from parquet_recursive_fuzz_helpers import (
    _RECURSIVE_FUZZ_SCALARS,
    _recursive_fuzz_projection_permutation_specs,
)
from parquet_runtime_shared import pa
from parquet_runtime_shared import recursive_arrow_type as arrow_type
from parquet_runtime_shared import requires_pyarrow as _requires_pyarrow

# Split from test_parquet_native_recursive_projection_runtime.py: test_native_parquet_stream_preserves_recursive_root_fingerprints_under_projection_permutations, test_native_parquet_stream_projects_multiple_recursive_roots


@_requires_pyarrow
def test_native_parquet_stream_preserves_recursive_root_fingerprints_under_projection_permutations(
    tmp_path: Path,
) -> None:
    """Verify deep recursive roots are projection-isolated across permutations."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import (
        native_parquet_recursive_layout_summary,
        native_parquet_recursive_projection_chain_contract_audit,
        native_parquet_recursive_projection_contract_audit,
        native_parquet_recursive_projection_coverage_contract_audit,
        native_parquet_recursive_projection_partition_contract_audit,
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
            return f"projection-permutation-{seed}"
        if kind == "bool":
            return seed % 2 == 0
        if kind == "float64":
            return seed + 0.3125
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
            return None if seed % 4 == 0 else scalar_value(kind, seed)
        if kind == "list":
            return [None, empty_value(spec[1], seed + 1), full_value(spec[1], seed + 2)]
        if kind == "map":
            return [(f"none{seed}", None), (f"full{seed}", full_value(spec[1], seed + 1))]
        if kind == "struct":
            return {
                name: (
                    sparse_value(child, seed + child_index)
                    if child_index % 2 == 0
                    else full_value(child, seed + child_index)
                )
                for child_index, (name, child) in enumerate(spec[1])
            }
        raise AssertionError(kind)

    require_native()
    cases = _recursive_fuzz_projection_permutation_specs()
    schema = pa.schema([pa.field(name, arrow_type(spec)) for name, spec, _ in cases])
    batch_one = pa.record_batch(
        [
            pa.array([None, empty_value(spec, index)], type=schema.field(name).type)
            for index, (name, spec, _) in enumerate(cases)
        ],
        schema=schema,
    )
    batch_two = pa.record_batch(
        [
            pa.array(
                [sparse_value(spec, index * 100), full_value(spec, index * 100 + 50)],
                type=schema.field(name).type,
            )
            for index, (name, spec, _) in enumerate(cases)
        ],
        schema=schema,
    )
    expected = pa.Table.from_batches([batch_one, batch_two])
    path = tmp_path / "native-recursive-projection-permutations.parquet"
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(schema, [batch_one, batch_two]),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )

    full_summary = native_parquet_recursive_layout_summary(path)
    assert full_summary is not None
    assert full_summary["stable_across_row_groups"] is True
    assert full_summary["field_order"] == [name for name, _, _ in cases]
    assert full_summary["leaf_path_collisions"] == []
    assert full_summary["repeated_node_path_collisions"] == []
    full_fingerprints = full_summary["field_fingerprints_by_name"]

    partition_audit = native_parquet_recursive_projection_partition_contract_audit(
        path,
        partitions=[["gamma", "alpha"], ["zeta", "beta"], ["epsilon", "delta"]],
    )
    assert partition_audit["stable"] is True
    assert partition_audit["coverage_exact"] is True
    assert partition_audit["partition_audits_stable"] is True
    assert partition_audit["missing_partition_columns"] == []
    assert partition_audit["duplicate_partition_columns"] == []
    assert partition_audit["unknown_partition_columns"] == []
    assert partition_audit["root_contract_fingerprint_matches_full"] is True
    assert partition_audit["leaf_contract_fingerprint_matches_full"] is True
    assert partition_audit["field_fingerprint_matches_full"] is True

    coverage_audit = native_parquet_recursive_projection_coverage_contract_audit(
        path,
        projections=[["gamma", "alpha"], ["alpha", "beta"], ["zeta"]],
        require_full_coverage=False,
        allow_overlaps=True,
    )
    assert coverage_audit["stable"] is True
    assert coverage_audit["coverage_complete"] is False
    assert coverage_audit["coverage_partial"] is True
    assert coverage_audit["uncovered_full_columns"] == ["delta", "epsilon"]
    assert coverage_audit["overlapping_projection_columns"] == ["alpha"]
    assert coverage_audit["projection_audits_stable"] is True
    assert coverage_audit["root_contracts_consistent"] is True
    assert coverage_audit["leaf_contracts_consistent"] is True
    assert coverage_audit["field_contracts_consistent"] is True

    projections = (
        ("gamma", "alpha"),
        ("zeta", "beta", "epsilon"),
        ("delta", "alpha", "gamma", "beta"),
        ("epsilon", "delta", "zeta", "alpha"),
    )
    for projection in projections:
        projected_summary = native_parquet_recursive_layout_summary(path, columns=list(projection))
        assert projected_summary is not None, projection
        assert projected_summary["stable_across_row_groups"] is True, projection
        assert projected_summary["field_order"] == list(projection), projection
        assert projected_summary["field_fingerprints_by_name"] == {
            name: full_fingerprints[name] for name in sorted(projection)
        }, projection
        assert projected_summary["canonical_layout_fingerprint"] == ";".join(
            f"{name}={full_fingerprints[name]}" for name in sorted(projection)
        ), projection
        assert projected_summary["leaf_path_collisions"] == [], projection
        assert projected_summary["repeated_node_path_collisions"] == [], projection

        audit = native_parquet_recursive_projection_contract_audit(
            path,
            columns=list(projection),
        )
        assert audit["stable"] is True, projection
        assert audit["projection_order_matches"] is True, projection
        assert audit["root_contract_matches_by_name"] == {name: True for name in projection}, (
            projection
        )
        assert audit["leaf_contract_matches_by_name"] == {name: True for name in projection}, (
            projection
        )
        assert audit["field_fingerprint_matches_by_name"] == {name: True for name in projection}, (
            projection
        )
        assert (
            audit["canonical_expected_root_contract_fingerprint"]
            == audit["canonical_actual_root_contract_fingerprint"]
        ), projection

        source_projection = [
            name for name, _, _ in reversed(cases) if name in set(projection) | {"alpha", "delta"}
        ]
        chain_audit = native_parquet_recursive_projection_chain_contract_audit(
            path,
            source_columns=source_projection,
            columns=list(projection),
        )
        assert chain_audit["stable"] is True, projection
        assert chain_audit["projected_columns_subset_of_source"] is True, projection
        assert chain_audit["direct_vs_chained_root_contract_fingerprint_matches"] is True, (
            projection
        )
        assert chain_audit["direct_vs_chained_leaf_contract_fingerprint_matches"] is True, (
            projection
        )
        assert chain_audit["direct_vs_chained_field_fingerprint_matches"] is True, projection
        assert chain_audit["root_contract_transitive_matches_by_name"] == {
            name: True for name in projection
        }, projection

        factory = open_parquet_record_batch_stream_factory(
            path,
            source="path",
            feature="test",
            columns=list(projection),
        )
        out = pa.RecordBatchReader.from_stream(factory).read_all()
        selected = expected.select(list(projection))

        assert out.schema.equals(selected.schema), projection
        assert out.to_pylist() == selected.to_pylist(), projection
        assert last_parquet_stream_factory_route() == "native_parquet_stream", projection


@_requires_pyarrow
def test_native_parquet_stream_projects_multiple_recursive_roots(
    tmp_path: Path,
) -> None:
    """Verify independent recursive roots survive projection and column reorder."""
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

    require_native()
    left_type = pa.list_(
        pa.struct(
            [
                pa.field("a", pa.list_(pa.int64())),
                pa.field("b", pa.map_(pa.string(), pa.list_(pa.string()))),
            ]
        )
    )
    right_type = pa.map_(
        pa.string(),
        pa.struct(
            [
                pa.field("x", pa.list_(pa.map_(pa.string(), pa.int64()))),
                pa.field("y", pa.list_(pa.list_(pa.float64()))),
            ]
        ),
    )
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("left", left_type),
            pa.field("right", right_type),
            pa.field("tail", pa.string()),
        ]
    )
    batches = [
        pa.record_batch(
            [
                pa.array([1, 2], type=pa.int64()),
                pa.array(
                    [
                        [{"a": [1, None], "b": [("k", ["v"])]}],
                        None,
                    ],
                    type=left_type,
                ),
                pa.array(
                    [
                        [
                            (
                                "r",
                                {
                                    "x": [[("m", 3)], []],
                                    "y": [[1.25], [], None],
                                },
                            )
                        ],
                        [],
                    ],
                    type=right_type,
                ),
                pa.array(["one", "two"]),
            ],
            schema=schema,
        ),
        pa.record_batch(
            [
                pa.array([3], type=pa.int64()),
                pa.array([[{"a": [], "b": []}]], type=left_type),
                pa.array([None], type=right_type),
                pa.array(["three"]),
            ],
            schema=schema,
        ),
    ]
    full_table = pa.Table.from_batches(batches)
    expected = full_table.select(["right", "left"])
    path = tmp_path / "native-multiple-recursive-roots.parquet"
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
    assert info["row_group_count"] == 2
    factory = open_parquet_record_batch_stream_factory(
        path,
        source="path",
        feature="test",
        columns=["right", "left"],
    )
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(expected.schema)
    assert out.to_pylist() == expected.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"
