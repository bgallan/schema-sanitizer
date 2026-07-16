"""Native Parquet recursive nested grammar runtime tests."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native
from parquet_recursive_fuzz_helpers import (
    _recursive_fuzz_structural_metrics,
)
from parquet_runtime_shared import pa
from parquet_runtime_shared import recursive_arrow_type as arrow_type
from parquet_runtime_shared import requires_pyarrow as _requires_pyarrow


def test_native_parquet_stream_materializes_generated_extreme_recursive_shapes(
    tmp_path: Path,
) -> None:
    """Verify deterministic high-depth and high-branch recursive shapes."""
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

    def list_type(value_type: pa.DataType, depth: int) -> pa.DataType:
        """Return a nested list type around value_type."""
        out = value_type
        for _ in range(depth):
            out = pa.list_(out)
        return out

    def nested_list_value(value: object, depth: int) -> object:
        """Return value wrapped in depth nested one-element lists."""
        out = value
        for _ in range(depth):
            out = [out]
        return out

    def nested_empty_list(depth: int) -> object:
        """Return an empty list nested at the requested list depth."""
        out: object = []
        for _ in range(depth - 1):
            out = [out]
        return out

    require_native()
    leaf = pa.struct(
        [
            pa.field("id", pa.int64()),
            pa.field("labels", pa.list_(pa.string())),
        ]
    )
    scalar_deep_list_type = list_type(pa.int64(), 10)
    deep_list_type = list_type(leaf, 8)
    alternating_type = pa.list_(
        pa.map_(
            pa.string(),
            pa.list_(
                pa.map_(
                    pa.string(),
                    pa.list_(pa.map_(pa.string(), pa.list_(leaf))),
                )
            ),
        )
    )
    branch_type = pa.struct(
        [
            pa.field("a", list_type(leaf, 4)),
            pa.field("b", pa.map_(pa.string(), list_type(leaf, 3))),
            pa.field("c", pa.list_(pa.map_(pa.string(), list_type(leaf, 2)))),
            pa.field(
                "d",
                pa.map_(
                    pa.string(),
                    pa.struct(
                        [
                            pa.field("left", list_type(leaf, 2)),
                            pa.field("right", pa.map_(pa.string(), leaf)),
                        ]
                    ),
                ),
            ),
        ]
    )
    cases = [
        (
            "scalar-list-depth-10",
            scalar_deep_list_type,
            [
                nested_list_value(11, 10),
                None,
                nested_empty_list(10),
                nested_list_value(None, 10),
            ],
            10,
        ),
        (
            "list-depth-8",
            deep_list_type,
            [
                nested_list_value({"id": 1, "labels": ["deep", None]}, 8),
                None,
                nested_empty_list(8),
                nested_list_value(None, 8),
            ],
            8,
        ),
        (
            "alternating-list-map-depth",
            alternating_type,
            [
                [
                    [
                        (
                            "outer",
                            [
                                [
                                    (
                                        "middle",
                                        [
                                            [
                                                (
                                                    "inner",
                                                    [
                                                        {
                                                            "id": 2,
                                                            "labels": ["x"],
                                                        },
                                                        None,
                                                    ],
                                                )
                                            ]
                                        ],
                                    )
                                ],
                                [],
                            ],
                        )
                    ]
                ],
                None,
                [],
                [[("empty", [])]],
            ],
            6,
        ),
        (
            "wide-branch-count-4",
            branch_type,
            [
                {
                    "a": nested_list_value({"id": 3, "labels": []}, 4),
                    "b": [
                        ("b0", nested_list_value({"id": 4, "labels": ["b"]}, 3)),
                        ("b1", None),
                    ],
                    "c": [
                        [
                            (
                                "c0",
                                nested_list_value({"id": None, "labels": [None]}, 2),
                            )
                        ],
                        [],
                    ],
                    "d": [
                        (
                            "d0",
                            {
                                "left": nested_empty_list(2),
                                "right": [("r", {"id": 5, "labels": None})],
                            },
                        )
                    ],
                },
                None,
                {"a": None, "b": [], "c": None, "d": []},
            ],
            5,
        ),
    ]

    for name, item_type, values, min_repeated_depth in cases:
        path = tmp_path / f"native-recursive-generated-extreme-{name}.parquet"
        table = pa.table({"items": pa.array(values, type=item_type)})
        write_parquet_native_first_stream(
            pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
            path,
            feature="test",
            parquet_compression="uncompressed",
        )

        info = native_parquet_footer_info(path)

        assert info is not None
        assert info["native_reader_ready"] == 1, name
        assert info["native_reader_blockers"] == [], name
        assert (
            max(
                len(column["repeated_level_layouts"])
                for column in info["row_groups"][0]["columns"]
                if column["max_repetition_level"] > 0
            )
            >= min_repeated_depth
        ), name
        factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
        reader = pa.RecordBatchReader.from_stream(factory)
        out = reader.read_all()

        assert out.schema.equals(table.schema), name
        assert out.to_pylist() == table.to_pylist(), name
        assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_generated_recursive_shape_fuzzer(
    tmp_path: Path,
) -> None:
    """Verify a generated bounded recursive grammar corpus stays native."""
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

    def scalar_spec(seed: int) -> tuple[str]:
        """Internal test helper."""
        return (("int64",), ("string",), ("bool",), ("float64",))[seed % 4]

    def chain_spec(ops: list[str], leaf: object) -> object:
        """Internal test helper."""
        spec = leaf
        for op_index, op in enumerate(reversed(ops)):
            if op == "list":
                spec = ("list", spec)
            elif op == "map":
                spec = ("map", spec)
            elif op == "struct":
                spec = (
                    "struct",
                    (
                        (f"field_{op_index}", spec),
                        (f"side_{op_index}", scalar_spec(op_index + len(ops))),
                    ),
                )
            else:  # pragma: no cover - local generator invariant
                raise AssertionError(op)
        return spec

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
            return seed + 0.25
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
        kind = spec[0]
        if kind in {"int64", "string", "bool", "float64"}:
            return None
        if kind == "list":
            return []
        if kind == "map":
            return []
        if kind == "struct":
            return {
                name: empty_value(child, seed + offset + 1)
                for offset, (name, child) in enumerate(spec[1])
            }
        raise AssertionError(kind)

    def generated_specs() -> list[tuple[str, object, int]]:
        """Internal test helper."""
        op_pool = ["list", "map", "struct"]
        cases: list[tuple[str, object, int]] = []
        for seed in range(18):
            depth = 5 + (seed % 5)
            ops = [op_pool[(seed * 7 + index * 5 + index // 2) % 3] for index in range(depth)]
            leaf = scalar_spec(seed)
            if seed % 4 == 0:
                leaf = (
                    "struct",
                    (
                        ("left", chain_spec(["list", "map"], scalar_spec(seed + 1))),
                        ("right", chain_spec(["map", "list"], scalar_spec(seed + 2))),
                    ),
                )
            spec = chain_spec(ops, leaf)
            repeated_depth = _recursive_fuzz_structural_metrics(spec)["repetition_depth"]
            cases.append(("-".join(ops) + f"-{seed}", spec, repeated_depth))
        return cases

    require_native()
    covered_root_kinds: set[str] = set()
    deepest_reported_depth = 0
    widest_reported_branch_count = 0

    for name, spec, min_repeated_depth in generated_specs():
        item_type = arrow_type(spec)
        values = [
            full_value(spec, 10),
            None,
            empty_value(spec, 20),
            full_value(spec, 30),
        ]
        table = pa.table({"items": pa.array(values, type=item_type)})
        path = tmp_path / f"native-recursive-fuzz-{name}.parquet"
        write_parquet_native_first_stream(
            pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
            path,
            feature="test",
            parquet_compression="uncompressed",
        )

        info = native_parquet_footer_info(path)

        assert info is not None, name
        assert info["native_reader_ready"] == 1, name
        assert info["native_reader_blockers"] == [], name
        layout = info["row_groups"][0]["native_recursive_output_layout"]
        assert layout["decoded"] == 1, name
        assert layout["field_count"] == 1, name
        field = layout["fields"][0]
        covered_root_kinds.add(field["root_kind"])
        deepest_reported_depth = max(deepest_reported_depth, field["repetition_depth"])
        widest_reported_branch_count = max(widest_reported_branch_count, field["max_child_count"])
        assert field["name"] == "items", name
        assert field["leaf_count"] == len(field["column_indices"]), name
        assert field["repetition_depth"] >= min_repeated_depth, name
        assert field["node_count"] >= field["leaf_count"], name

        factory = open_parquet_record_batch_stream_factory(
            path,
            source="path",
            feature="test",
        )
        reader = pa.RecordBatchReader.from_stream(factory)
        out = reader.read_all()

        assert out.schema.equals(table.schema), name
        assert out.to_pylist() == table.to_pylist(), name
        assert last_parquet_stream_factory_route() == "native_parquet_stream", name

    assert covered_root_kinds == {"list", "map", "struct"}
    assert deepest_reported_depth >= 8
    assert widest_reported_branch_count >= 2
