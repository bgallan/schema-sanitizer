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


# Split from test_parquet_native_recursive_shapes.py: test_native_parquet_stream_materializes_adversarial_recursive_struct_siblings, test_native_parquet_stream_projects_recursive_shapes_across_row_groups, test_native_parquet_stream_materializes_recursive_null_empty_matrix


@_requires_pyarrow
def test_native_parquet_stream_materializes_adversarial_recursive_struct_siblings(
    tmp_path: Path,
) -> None:
    """Verify nullable struct siblings under different repeated ancestors."""
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
    primitive_map = pa.map_(pa.string(), pa.int64())
    leaf_struct = pa.struct(
        [
            pa.field("id", pa.int64()),
            pa.field("tags", pa.list_(pa.string())),
        ]
    )
    branch_struct = pa.struct(
        [
            pa.field("alpha", pa.list_(leaf_struct)),
            pa.field("beta", pa.map_(pa.string(), leaf_struct)),
            pa.field("gamma", pa.list_(pa.map_(pa.string(), pa.list_(leaf_struct)))),
        ]
    )
    root_struct = pa.struct(
        [
            pa.field("left", pa.list_(branch_struct)),
            pa.field("middle", pa.map_(pa.string(), branch_struct)),
            pa.field("right", pa.list_(pa.list_(primitive_map))),
        ]
    )
    cases = [
        (
            "root-struct-siblings",
            root_struct,
            [
                {
                    "left": [
                        {
                            "alpha": [{"id": 1, "tags": ["a", None]}, None],
                            "beta": [
                                ("b", {"id": 2, "tags": []}),
                                ("n", None),
                            ],
                            "gamma": [
                                [
                                    (
                                        "g",
                                        [
                                            {"id": 3, "tags": ["x"]},
                                            None,
                                        ],
                                    )
                                ]
                            ],
                        },
                        None,
                    ],
                    "middle": [
                        (
                            "m",
                            {
                                "alpha": [],
                                "beta": [],
                                "gamma": [[("z", [])]],
                            },
                        )
                    ],
                    "right": [[{"r": 4}], [], None],
                },
                None,
                {"left": None, "middle": [], "right": None},
            ],
        ),
        (
            "list-struct-siblings",
            pa.list_(branch_struct),
            [
                [
                    {
                        "alpha": [{"id": 10, "tags": None}],
                        "beta": [("x", None)],
                        "gamma": [],
                    },
                    None,
                ],
                None,
                [],
                [{"alpha": None, "beta": [], "gamma": [[("deep", None)]]}],
            ],
        ),
        (
            "map-struct-siblings",
            pa.map_(pa.string(), branch_struct),
            [
                [
                    (
                        "root",
                        {
                            "alpha": [None, {"id": 20, "tags": ["m"]}],
                            "beta": [("k", {"id": None, "tags": [None]})],
                            "gamma": [[("inner", [{"id": 21, "tags": []}])]],
                        },
                    )
                ],
                None,
                [("empty", {"alpha": [], "beta": [], "gamma": []})],
            ],
        ),
    ]

    for name, item_type, values in cases:
        path = tmp_path / f"native-adversarial-{name}.parquet"
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
        factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
        reader = pa.RecordBatchReader.from_stream(factory)
        out = reader.read_all()

        assert out.schema.equals(table.schema), name
        assert out.to_pylist() == table.to_pylist(), name
        assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_projects_recursive_shapes_across_row_groups(
    tmp_path: Path,
) -> None:
    """Verify projected recursive shapes materialize independently per row group."""
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
    path = tmp_path / "native-recursive-projection-row-groups.parquet"
    payload_type = pa.list_(
        pa.struct(
            [
                pa.field("attrs", pa.map_(pa.string(), pa.list_(pa.int64()))),
                pa.field(
                    "children",
                    pa.list_(
                        pa.struct(
                            [
                                pa.field("name", pa.string()),
                                pa.field("scores", pa.list_(pa.float64())),
                            ]
                        )
                    ),
                ),
            ]
        )
    )
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("payload", payload_type),
        ]
    )
    batches = [
        pa.record_batch(
            [
                pa.array([1, 2], type=pa.int64()),
                pa.array(
                    [
                        [
                            {
                                "attrs": [("a", [1, None]), ("empty", [])],
                                "children": [
                                    {"name": "x", "scores": [1.5]},
                                    None,
                                ],
                            }
                        ],
                        None,
                    ],
                    type=payload_type,
                ),
            ],
            schema=schema,
        ),
        pa.record_batch(
            [
                pa.array([3, 4], type=pa.int64()),
                pa.array(
                    [
                        [],
                        [
                            {
                                "attrs": None,
                                "children": [
                                    {"name": None, "scores": []},
                                ],
                            }
                        ],
                    ],
                    type=payload_type,
                ),
            ],
            schema=schema,
        ),
    ]
    expected = pa.Table.from_batches(batches).select(["payload"])
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
        columns=["payload"],
    )
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()

    assert out.schema.equals(expected.schema)
    assert out.to_pylist() == expected.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_recursive_null_empty_matrix(
    tmp_path: Path,
) -> None:
    """Verify recursive native materialization over generated null/empty cases."""
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
    leaf = pa.struct(
        [
            pa.field("value", pa.int64()),
            pa.field("notes", pa.list_(pa.string())),
        ]
    )
    branch = pa.struct(
        [
            pa.field("list_leaf", pa.list_(leaf)),
            pa.field("map_leaf", pa.map_(pa.string(), leaf)),
            pa.field("list_map_leaf", pa.list_(pa.map_(pa.string(), pa.list_(leaf)))),
        ]
    )
    cases = [
        (
            "list-branch",
            pa.list_(branch),
            [
                [
                    {
                        "list_leaf": [
                            {"value": 1, "notes": ["a", None]},
                            None,
                            {"value": None, "notes": []},
                        ],
                        "map_leaf": [
                            ("x", {"value": 2, "notes": None}),
                            ("empty", None),
                        ],
                        "list_map_leaf": [
                            [("lm", [{"value": 3, "notes": ["z"]}, None])],
                            [],
                        ],
                    },
                    None,
                ],
                None,
                [],
                [{"list_leaf": None, "map_leaf": [], "list_map_leaf": None}],
            ],
        ),
        (
            "map-branch",
            pa.map_(pa.string(), branch),
            [
                [
                    (
                        "root",
                        {
                            "list_leaf": [],
                            "map_leaf": [("k", {"value": 4, "notes": []})],
                            "list_map_leaf": [[("deep", [])]],
                        },
                    ),
                    ("null_value", None),
                ],
                None,
                [],
                [
                    (
                        "mixed",
                        {
                            "list_leaf": [None],
                            "map_leaf": [],
                            "list_map_leaf": [[("none", None)]],
                        },
                    )
                ],
            ],
        ),
        (
            "struct-branch",
            pa.struct(
                [
                    pa.field("left", pa.list_(branch)),
                    pa.field("right", pa.map_(pa.string(), branch)),
                ]
            ),
            [
                {
                    "left": [
                        {
                            "list_leaf": [{"value": 5, "notes": ["left"]}],
                            "map_leaf": [],
                            "list_map_leaf": [],
                        }
                    ],
                    "right": [
                        (
                            "r",
                            {
                                "list_leaf": None,
                                "map_leaf": [("leaf", None)],
                                "list_map_leaf": None,
                            },
                        )
                    ],
                },
                None,
                {"left": [], "right": []},
            ],
        ),
    ]

    for name, item_type, values in cases:
        path = tmp_path / f"native-recursive-null-empty-{name}.parquet"
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
        factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
        reader = pa.RecordBatchReader.from_stream(factory)
        out = reader.read_all()

        assert out.schema.equals(table.schema), name
        assert out.to_pylist() == table.to_pylist(), name
        assert last_parquet_stream_factory_route() == "native_parquet_stream"
