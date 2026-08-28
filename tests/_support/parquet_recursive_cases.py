"""Build deterministic recursive Parquet schemas and values for native runtime tests.

The bounded and seeded corpora cover irregular list, map, struct, scalar, empty, and null
combinations while keeping failures reproducible.
"""

from __future__ import annotations

import itertools
from collections.abc import Callable
from pathlib import Path

from _support.parquet_runtime import pa, write_read_native_parquet
from _support.parquet_runtime import recursive_arrow_type as arrow_type
from _support.parquet_runtime import requires_pyarrow as _requires_pyarrow

_RECURSIVE_FUZZ_OPS = ("list", "map", "struct")
_RECURSIVE_FUZZ_SCALARS = ("int64", "string", "bool", "float64")

_ScalarValue = Callable[[str, int], object]
_RecursiveValue = Callable[[object, int], object]


def _recursive_fuzz_empty_value(spec: object, seed: int) -> object:
    """Return the canonical empty/null value for a recursive grammar spec."""
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
            name: _recursive_fuzz_empty_value(child, child_index)
            for child_index, (name, child) in enumerate(spec[1])
        }
    raise AssertionError(kind)


# Runtime case functions use the concise fixture name from their former modules.
empty_value = _recursive_fuzz_empty_value


def _recursive_fuzz_full_value_factory(
    scalar_value: _ScalarValue,
    *,
    include_null: bool,
) -> _RecursiveValue:
    """Build a full-value generator with optional explicit null children."""

    def full_value(spec: object, seed: int) -> object:
        """Return one full recursive value."""
        kind = spec[0]
        if kind in set(_RECURSIVE_FUZZ_SCALARS):
            return scalar_value(kind, seed)
        if kind == "list":
            values = [
                full_value(spec[1], seed + 1),
                _recursive_fuzz_empty_value(spec[1], seed + 2),
            ]
            return [*values, None] if include_null else values
        if kind == "map":
            if include_null:
                return [
                    (f"full-{seed}", full_value(spec[1], seed + 1)),
                    (f"empty-{seed}", _recursive_fuzz_empty_value(spec[1], seed + 2)),
                    (f"none-{seed}", None),
                ]
            return [
                (f"k{seed}", full_value(spec[1], seed + 1)),
                (f"empty{seed}", _recursive_fuzz_empty_value(spec[1], seed + 2)),
            ]
        if kind == "struct":
            return {
                name: full_value(child, seed + child_index + 1)
                for child_index, (name, child) in enumerate(spec[1])
            }
        raise AssertionError(kind)

    return full_value


def _recursive_fuzz_sparse_value_factory(
    scalar_value: _ScalarValue,
    full_value: _RecursiveValue,
) -> _RecursiveValue:
    """Build the common sparse-value generator for null/empty matrix tests."""

    def sparse_value(spec: object, seed: int) -> object:
        """Return one sparse recursive value."""
        kind = spec[0]
        if kind in set(_RECURSIVE_FUZZ_SCALARS):
            return None if seed % 2 == 0 else scalar_value(kind, seed)
        if kind == "list":
            return [
                _recursive_fuzz_empty_value(spec[1], seed + 1),
                None,
                full_value(spec[1], seed + 2),
            ]
        if kind == "map":
            return [
                (f"empty-{seed}", _recursive_fuzz_empty_value(spec[1], seed + 1)),
                (f"none-{seed}", None),
                (f"full-{seed}", full_value(spec[1], seed + 2)),
            ]
        if kind == "struct":
            return {
                name: (
                    None
                    if offset % 3 == 0
                    else _recursive_fuzz_empty_value(child, seed + offset)
                    if offset % 3 == 1
                    else full_value(child, seed + offset)
                )
                for offset, (name, child) in enumerate(spec[1])
            }
        raise AssertionError(kind)

    return sparse_value


def _recursive_fuzz_phase_value_factory(
    sparse_value: _RecursiveValue,
    full_value: _RecursiveValue,
) -> Callable[[object, str, int], object]:
    """Build the common null/empty/sparse/full phase selector."""

    def phase_value(spec: object, phase: str, seed: int) -> object:
        """Return a recursive value for one named phase."""
        if phase == "all-null":
            return None
        if phase == "empty-only":
            return _recursive_fuzz_empty_value(spec, seed)
        if phase == "sparse":
            return sparse_value(spec, seed)
        if phase == "full":
            return full_value(spec, seed)
        raise AssertionError(phase)

    return phase_value


def _recursive_fuzz_scalar(seed: int) -> tuple[str]:
    """Return a deterministic scalar spec for recursive grammar fuzzing."""
    return (_RECURSIVE_FUZZ_SCALARS[seed % len(_RECURSIVE_FUZZ_SCALARS)],)


def _recursive_fuzz_chain_spec(ops: tuple[str, ...], leaf: object) -> object:
    """Wrap a leaf with a deterministic list/map/struct operation chain."""
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
                    (f"side_scalar_{op_index}", _recursive_fuzz_scalar(op_index + len(ops))),
                    (
                        f"side_list_{op_index}",
                        ("list", _recursive_fuzz_scalar(op_index + len(ops) + 1)),
                    ),
                ),
            )
        else:  # pragma: no cover - local generator invariant
            raise AssertionError(op)
    return spec


def _recursive_fuzz_structural_metrics(spec: object) -> dict[str, int]:
    """Return pure-Python metrics for a generated recursive grammar spec."""
    kind = spec[0]
    if kind in set(_RECURSIVE_FUZZ_SCALARS):
        return {
            "leaf_count": 1,
            "struct_count": 0,
            "list_count": 0,
            "map_count": 0,
            "node_count": 1,
            "repetition_depth": 0,
            "max_child_count": 0,
        }
    if kind in {"list", "map"}:
        child = _recursive_fuzz_structural_metrics(spec[1])
        child["node_count"] += 1
        child["repetition_depth"] += 1
        child["max_child_count"] = max(child["max_child_count"], 1)
        if kind == "list":
            child["list_count"] += 1
        else:
            child["map_count"] += 1
        return child
    if kind == "struct":
        children = [_recursive_fuzz_structural_metrics(child) for _, child in spec[1]]
        return {
            "leaf_count": sum(child["leaf_count"] for child in children),
            "struct_count": 1 + sum(child["struct_count"] for child in children),
            "list_count": sum(child["list_count"] for child in children),
            "map_count": sum(child["map_count"] for child in children),
            "node_count": 1 + sum(child["node_count"] for child in children),
            "repetition_depth": max((child["repetition_depth"] for child in children), default=0),
            "max_child_count": max(
                len(spec[1]),
                *(child["max_child_count"] for child in children),
            ),
        }
    raise AssertionError(kind)


def _recursive_fuzz_signature(spec: object) -> str:
    """Return a deterministic grammar signature independent of PyArrow."""
    kind = spec[0]
    if kind in set(_RECURSIVE_FUZZ_SCALARS):
        return kind
    if kind == "list":
        return f"list<{_recursive_fuzz_signature(spec[1])}>"
    if kind == "map":
        return f"map<string,{_recursive_fuzz_signature(spec[1])}>"
    if kind == "struct":
        fields = ",".join(f"{name}:{_recursive_fuzz_signature(child)}" for name, child in spec[1])
        return f"struct<{fields}>"
    raise AssertionError(kind)


def _recursive_fuzz_cartesian_specs() -> list[tuple[str, object, dict[str, int]]]:
    """Return a bounded Cartesian corpus over list/map/struct operation words."""
    cases: list[tuple[str, object, dict[str, int]]] = []
    for depth in range(1, 4):
        for index, ops in enumerate(itertools.product(_RECURSIVE_FUZZ_OPS, repeat=depth)):
            leaf = _recursive_fuzz_scalar(index + depth)
            if index % 5 == 0:
                leaf = (
                    "struct",
                    (
                        ("left", _recursive_fuzz_chain_spec(("list", "map"), leaf)),
                        (
                            "right",
                            _recursive_fuzz_chain_spec(
                                ("map", "list"), _recursive_fuzz_scalar(index + 7)
                            ),
                        ),
                    ),
                )
            spec = _recursive_fuzz_chain_spec(ops, leaf)
            name = f"d{depth}-{'-'.join(ops)}-{index}"
            cases.append((name, spec, _recursive_fuzz_structural_metrics(spec)))
    # Add a deterministic deeper frontier without making CI pay for every 3**5 case.
    frontier = (
        ("list", "map", "struct", "list", "map"),
        ("map", "list", "struct", "map", "list"),
        ("struct", "list", "map", "struct", "list"),
        ("struct", "struct", "list", "map", "map"),
        ("map", "struct", "list", "struct", "map"),
        ("list", "map", "struct", "list", "map", "struct", "list"),
        ("struct", "map", "list", "struct", "map", "list", "map"),
        ("list", "map", "list", "map", "list", "map", "list"),
    )
    for index, ops in enumerate(frontier):
        leaf = (
            "struct",
            (
                ("payload", _recursive_fuzz_scalar(index + 41)),
                ("labels", ("list", _recursive_fuzz_scalar(index + 42))),
            ),
        )
        spec = _recursive_fuzz_chain_spec(ops, leaf)
        name = f"frontier-{'-'.join(ops)}-{index}"
        cases.append((name, spec, _recursive_fuzz_structural_metrics(spec)))
    return cases


def _recursive_fuzz_null_empty_matrix_specs() -> list[tuple[str, object, dict[str, int]]]:
    """Return adversarial recursive shapes for null/empty/full matrix coverage."""
    operation_words = (
        ("list", "map", "struct", "list"),
        ("map", "list", "struct", "map"),
        ("struct", "list", "map", "struct"),
        ("list", "struct", "map", "list", "struct"),
        ("map", "struct", "list", "map", "list"),
        ("struct", "map", "list", "struct", "map"),
        ("list", "map", "list", "struct", "map", "list"),
        ("map", "list", "map", "struct", "list", "map"),
        ("struct", "list", "struct", "map", "list", "struct"),
    )
    cases: list[tuple[str, object, dict[str, int]]] = []
    for index, ops in enumerate(operation_words):
        leaf = (
            "struct",
            (
                ("scalar", _recursive_fuzz_scalar(index)),
                ("maybe_list", ("list", _recursive_fuzz_scalar(index + 1))),
                (
                    "maybe_map",
                    (
                        "map",
                        (
                            "struct",
                            (
                                ("inner", _recursive_fuzz_scalar(index + 2)),
                                ("inner_list", ("list", _recursive_fuzz_scalar(index + 3))),
                            ),
                        ),
                    ),
                ),
            ),
        )
        spec = _recursive_fuzz_chain_spec(ops, leaf)
        name = f"null-empty-{'-'.join(ops)}-{index}"
        cases.append((name, spec, _recursive_fuzz_structural_metrics(spec)))
    return cases


def _recursive_fuzz_profile_labels(spec: object) -> set[str]:
    """Return value-profile labels that a null/empty matrix can exercise."""
    kind = spec[0]
    if kind in set(_RECURSIVE_FUZZ_SCALARS):
        return {"scalar-full", "scalar-null"}
    if kind == "list":
        child = _recursive_fuzz_profile_labels(spec[1])
        return {"list-null", "list-empty", "list-with-null-element", "list-full"} | child
    if kind == "map":
        child = _recursive_fuzz_profile_labels(spec[1])
        return {"map-null", "map-empty", "map-with-null-value", "map-full"} | child
    if kind == "struct":
        labels = {"struct-null", "struct-full", "struct-sparse"}
        for _, child in spec[1]:
            labels |= _recursive_fuzz_profile_labels(child)
        return labels
    raise AssertionError(kind)


def _recursive_fuzz_row_group_phase_matrix_specs() -> list[tuple[str, object, dict[str, int]]]:
    """Return representative deep shapes for per-row-group phase coverage."""
    base = _recursive_fuzz_null_empty_matrix_specs()
    # Keep the runtime corpus bounded but force every root kind and several deep
    # alternating list/map/struct paths. The core test below verifies the surface.
    selected_indexes = (0, 1, 2, 4, 6, 8)
    return [
        (f"phase-{index}-{base[index][0]}", base[index][1], base[index][2])
        for index in selected_indexes
    ]


def _recursive_fuzz_row_group_phase_labels() -> tuple[str, ...]:
    """Value phases that should be isolated in distinct row groups."""
    return ("all-null", "empty-only", "sparse", "full")


def _recursive_fuzz_seeded_specs() -> list[tuple[str, object, dict[str, int]]]:
    """Return the deterministic pseudo-random recursive grammar corpus."""

    def next_seed(seed: int, salt: int) -> int:
        """Advance the deterministic recursive-corpus seed."""
        return (seed * 1103515245 + 12345 + salt * 2654435761) & 0x7FFFFFFF

    def node(seed: int, depth: int, max_depth: int, forced: str | None = None) -> object:
        """Build one bounded recursive grammar node from the seed."""
        if depth >= max_depth:
            return _recursive_fuzz_scalar(seed + depth)
        kind = forced or _RECURSIVE_FUZZ_OPS[next_seed(seed, depth) % len(_RECURSIVE_FUZZ_OPS)]
        if kind == "list":
            return ("list", node(next_seed(seed, 11), depth + 1, max_depth))
        if kind == "map":
            return ("map", node(next_seed(seed, 17), depth + 1, max_depth))
        if kind == "struct":
            child_count = 2 + (next_seed(seed, depth + 23) % 3)
            children = []
            for child_index in range(child_count):
                child_seed = next_seed(seed, child_index + 31)
                # Force at least one repeated sibling in wider structs.
                forced_child = None
                if child_index == 1 and depth + 1 < max_depth:
                    forced_child = "list" if seed % 2 == 0 else "map"
                children.append(
                    (
                        f"s{depth}_{child_index}_{child_seed % 97}",
                        node(child_seed, depth + 1, max_depth, forced_child),
                    )
                )
            return ("struct", tuple(children))
        raise AssertionError(kind)

    cases: list[tuple[str, object, dict[str, int]]] = []
    for seed in range(30):
        root_kind = _RECURSIVE_FUZZ_OPS[seed % len(_RECURSIVE_FUZZ_OPS)]
        max_depth = 3 + (seed % 5)
        spec = node(seed + 1009, 0, max_depth, root_kind)
        name = f"seeded-{seed:02d}-{root_kind}-d{max_depth}"
        cases.append((name, spec, _recursive_fuzz_structural_metrics(spec)))
    return cases


def _recursive_fuzz_projection_permutation_specs() -> list[tuple[str, object, dict[str, int]]]:
    """Return independent recursive roots for projection-permutation coverage."""
    seeded = _recursive_fuzz_seeded_specs()
    # Pick deterministic irregular roots with different shapes and root kinds. These
    # become top-level columns in one file so projection order can be permuted
    # without changing each root's internal recursive tree.
    selected_indexes = (0, 1, 2, 8, 14, 23)
    names = ("alpha", "beta", "gamma", "delta", "epsilon", "zeta")
    return [
        (name, seeded[index][1], seeded[index][2]) for name, index in zip(names, selected_indexes)
    ]


@_requires_pyarrow
def test_native_parquet_stream_materializes_adversarial_recursive_struct_siblings(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes adversarial recursive struct siblings."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    primitive_map = pa.map_(pa.string(), pa.int64())
    leaf_struct = pa.struct([pa.field("id", pa.int64()), pa.field("tags", pa.list_(pa.string()))])
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
                            "beta": [("b", {"id": 2, "tags": []}), ("n", None)],
                            "gamma": [[("g", [{"id": 3, "tags": ["x"]}, None])]],
                        },
                        None,
                    ],
                    "middle": [("m", {"alpha": [], "beta": [], "gamma": [[("z", [])]]})],
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
                [{"alpha": [{"id": 10, "tags": None}], "beta": [("x", None)], "gamma": []}, None],
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
def test_native_parquet_stream_projects_recursive_shapes_across_row_groups(tmp_path: Path) -> None:
    """Verify native Parquet stream projects recursive shapes across row groups."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

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
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("payload", payload_type)])
    batches = [
        pa.record_batch(
            [
                pa.array([1, 2], type=pa.int64()),
                pa.array(
                    [
                        [
                            {
                                "attrs": [("a", [1, None]), ("empty", [])],
                                "children": [{"name": "x", "scores": [1.5]}, None],
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
                    [[], [{"attrs": None, "children": [{"name": None, "scores": []}]}]],
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
        path, source="path", feature="test", columns=["payload"]
    )
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()
    assert out.schema.equals(expected.schema)
    assert out.to_pylist() == expected.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_materializes_recursive_null_empty_matrix(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes recursive null empty matrix."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    leaf = pa.struct([pa.field("value", pa.int64()), pa.field("notes", pa.list_(pa.string()))])
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
                        "map_leaf": [("x", {"value": 2, "notes": None}), ("empty", None)],
                        "list_map_leaf": [[("lm", [{"value": 3, "notes": ["z"]}, None])], []],
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
                        {"list_leaf": [None], "map_leaf": [], "list_map_leaf": [[("none", None)]]},
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


@_requires_pyarrow
def test_native_parquet_stream_materializes_generated_extreme_recursive_shapes(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes generated extreme recursive shapes."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

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

    leaf = pa.struct([pa.field("id", pa.int64()), pa.field("labels", pa.list_(pa.string()))])
    scalar_deep_list_type = list_type(pa.int64(), 10)
    deep_list_type = list_type(leaf, 8)
    alternating_type = pa.list_(
        pa.map_(
            pa.string(),
            pa.list_(pa.map_(pa.string(), pa.list_(pa.map_(pa.string(), pa.list_(leaf))))),
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
            [nested_list_value(11, 10), None, nested_empty_list(10), nested_list_value(None, 10)],
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
                            [[("middle", [[("inner", [{"id": 2, "labels": ["x"]}, None])]])], []],
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
                    "b": [("b0", nested_list_value({"id": 4, "labels": ["b"]}, 3)), ("b1", None)],
                    "c": [[("c0", nested_list_value({"id": None, "labels": [None]}, 2))], []],
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
                (
                    len(column["repeated_level_layouts"])
                    for column in info["row_groups"][0]["columns"]
                    if column["max_repetition_level"] > 0
                )
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
    """Verify native Parquet stream materializes generated recursive shape fuzzer."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    def scalar_spec(seed: int) -> tuple[str]:
        """Select a scalar leaf specification from the deterministic seed."""
        return (("int64",), ("string",), ("bool",), ("float64",))[seed % 4]

    def chain_spec(ops: list[str], leaf: object) -> object:
        """Build an alternating recursive chain around one scalar leaf."""
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
            else:
                raise AssertionError(op)
        return spec

    def full_value(spec: object, seed: int) -> object:
        """Build a fully populated value for the recursive specification."""
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
        """Build the canonical empty value for the recursive specification."""
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
        """Generate the bounded recursive specifications used by the fuzzer."""
        op_pool = ["list", "map", "struct"]
        cases: list[tuple[str, object, int]] = []
        for seed in range(18):
            depth = 5 + seed % 5
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

    covered_root_kinds: set[str] = set()
    deepest_reported_depth = 0
    widest_reported_branch_count = 0
    for name, spec, min_repeated_depth in generated_specs():
        item_type = arrow_type(spec)
        values = [full_value(spec, 10), None, empty_value(spec, 20), full_value(spec, 30)]
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
        factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
        reader = pa.RecordBatchReader.from_stream(factory)
        out = reader.read_all()
        assert out.schema.equals(table.schema), name
        assert out.to_pylist() == table.to_pylist(), name
        assert last_parquet_stream_factory_route() == "native_parquet_stream", name
    assert covered_root_kinds == {"list", "map", "struct"}
    assert deepest_reported_depth >= 8
    assert widest_reported_branch_count >= 2


@_requires_pyarrow
def test_native_parquet_stream_materializes_cartesian_recursive_grammar_corpus(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes cartesian recursive grammar corpus."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    def full_value(spec: object, seed: int) -> object:
        """Build a fully populated value for the recursive specification."""
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

    for index, (name, spec, metrics) in enumerate(_recursive_fuzz_cartesian_specs()):
        item_type = arrow_type(spec)
        schema = pa.schema([pa.field("items", item_type)])
        batches = [
            pa.record_batch(
                [pa.array([full_value(spec, index + 10), None], type=item_type)], schema=schema
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
        factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
        reader = pa.RecordBatchReader.from_stream(factory)
        out = reader.read_all()
        assert out.schema.equals(expected.schema), name
        assert out.to_pylist() == expected.to_pylist(), name
        assert last_parquet_stream_factory_route() == "native_parquet_stream", name


@_requires_pyarrow
def test_native_parquet_stream_materializes_seeded_recursive_fuzzer_corpus(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes seeded recursive fuzzer corpus."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_recursive_layout_summary
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    def scalar_value(kind: str, seed: int) -> object:
        """Build a deterministic scalar value for the requested leaf type."""
        if kind == "int64":
            return seed * 17
        if kind == "string":
            return f"seeded-recursive-{seed}"
        if kind == "bool":
            return seed % 2 == 1
        if kind == "float64":
            return seed + 0.875
        raise AssertionError(kind)

    full_value = _recursive_fuzz_full_value_factory(scalar_value, include_null=False)

    def sparse_value(spec: object, seed: int) -> object:
        """Build a sparsely populated value for the recursive specification."""
        kind = spec[0]
        if kind in set(_RECURSIVE_FUZZ_SCALARS):
            return None if seed % 3 == 0 else scalar_value(kind, seed)
        if kind == "list":
            return [None, empty_value(spec[1], seed + 1), full_value(spec[1], seed + 2)]
        if kind == "map":
            return [(f"s{seed}", None), (f"v{seed}", full_value(spec[1], seed + 1))]
        if kind == "struct":
            return {
                name: sparse_value(child, seed + child_index)
                if child_index % 2 == 0
                else empty_value(child, seed + child_index)
                for child_index, (name, child) in enumerate(spec[1])
            }
        raise AssertionError(kind)

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
            path, source="path", feature="test", columns=["payload"]
        )
        out = pa.RecordBatchReader.from_stream(factory).read_all()
        assert out.schema.equals(expected.select(["payload"]).schema), name
        assert out.to_pylist() == expected.select(["payload"]).to_pylist(), name
        assert last_parquet_stream_factory_route() == "native_parquet_stream", name


@_requires_pyarrow
def test_native_parquet_stream_materializes_deep_recursive_mixed_shapes(tmp_path: Path) -> None:
    """Verify native Parquet stream materializes deep recursive mixed shapes."""
    map_type = pa.map_(pa.string(), pa.int64())
    map_list_map_type = pa.map_(pa.string(), pa.list_(map_type))
    inner_struct = pa.struct([pa.field("m", map_type), pa.field("v", pa.int64())])
    nested_map_struct = pa.struct([pa.field("m", map_list_map_type), pa.field("v", pa.int64())])
    list_list_struct_map = pa.list_(pa.list_(inner_struct))
    map_struct_list_list_struct_map_type = pa.map_(
        pa.string(), pa.struct([pa.field("a", list_list_struct_map)])
    )
    map_list_map_struct_type = pa.map_(
        pa.string(),
        pa.list_(pa.map_(pa.string(), pa.struct([pa.field("a", list_list_struct_map)]))),
    )
    list_list_struct_map_list_map = pa.list_(pa.list_(nested_map_struct))
    map_list_list_struct_map = pa.map_(pa.string(), list_list_struct_map)
    deep_list_map_struct = pa.struct(
        [
            pa.field(
                "payload",
                pa.list_(
                    pa.map_(
                        pa.string(),
                        pa.struct([pa.field("scores", pa.list_(pa.map_(pa.string(), pa.int64())))]),
                    )
                ),
            ),
            pa.field("flag", pa.int64()),
        ]
    )
    list_map_list_list_deep_struct = pa.list_(
        pa.map_(pa.string(), pa.list_(pa.list_(deep_list_map_struct)))
    )
    cases = [
        (
            "struct-list-list-struct-map",
            pa.struct([pa.field("a", list_list_struct_map)]),
            [{"a": [[{"m": {"x": 1}, "v": 2}, None], []]}, None, {"a": None}],
        ),
        (
            "list-struct-list-list-struct-map",
            pa.list_(pa.struct([pa.field("a", list_list_struct_map)])),
            [[{"a": [[{"m": {"x": 1}, "v": 2}, None], []]}], None, [], [{"a": None}]],
        ),
        (
            "map-struct-list-list-struct-map",
            map_struct_list_list_struct_map_type,
            [{"root": {"a": [[{"m": {"x": 1}, "v": 2}]]}}, None, {"z": None}],
        ),
        (
            "list-map-struct-list-list-struct-map",
            pa.list_(map_struct_list_list_struct_map_type),
            [[{"root": {"a": [[{"m": {"x": 1}, "v": 2}]]}}], None, [], [{"z": None}]],
        ),
        (
            "list-list-struct-map-list-map",
            list_list_struct_map_list_map,
            [[[{"m": {"a": [{"x": 1}, None], "b": []}, "v": 2}]], None, [[]]],
        ),
        (
            "struct-map-list-list-struct-map",
            pa.struct([pa.field("k", map_list_list_struct_map)]),
            [{"k": {"root": [[{"m": {"x": 1}, "v": 2}]]}}, None, {"k": None}],
        ),
        (
            "struct-map-list-map-struct-list-list-struct-map",
            pa.struct([pa.field("k", map_list_map_struct_type)]),
            [
                {"k": {"root": [{"nested": {"a": [[{"m": {"x": 1}, "v": 2}], []]}}, {}]}},
                None,
                {"k": None},
            ],
        ),
        (
            "list-map-list-list-struct-list-map-struct-list-map",
            list_map_list_list_deep_struct,
            [
                [
                    {
                        "outer": [
                            [{"payload": [{"p": {"scores": [{"s": 1}, {}]}}], "flag": 7}, None],
                            [],
                        ]
                    }
                ],
                None,
                [],
                [{"empty": None}],
            ],
        ),
    ]
    deepest_reported_repeated_layout_count = 0
    for name, item_type, values in cases:
        path = tmp_path / f"native-{name}.parquet"
        table = pa.table({"items": pa.array(values, type=item_type)})
        info = write_read_native_parquet(table, path)
        assert info is not None
        assert info["native_reader_ready"] == 1
        assert info["native_reader_blockers"] == []
        for column in info["row_groups"][0]["columns"]:
            max_repetition_level = column["max_repetition_level"]
            if max_repetition_level <= 0:
                continue
            layouts = column["repeated_level_layouts"]
            deepest_reported_repeated_layout_count = max(
                deepest_reported_repeated_layout_count, len(layouts)
            )
            assert len(layouts) == max_repetition_level, name
            assert [layout["layout_index"] for layout in layouts] == list(
                range(max_repetition_level)
            ), name
            assert all((layout["decoded"] == 1 for layout in layouts)), name
    assert deepest_reported_repeated_layout_count > 3


@_requires_pyarrow
def test_native_parquet_stream_materializes_required_and_optional_root_structs(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes required and optional root structs."""
    root_struct = pa.struct(
        [
            pa.field("id", pa.int64()),
            pa.field("labels", pa.list_(pa.string())),
            pa.field("meta", pa.struct([pa.field("score", pa.float64())])),
        ]
    )
    schema = pa.schema(
        [
            pa.field("required_root", root_struct, nullable=False),
            pa.field("optional_root", root_struct),
        ]
    )
    table = pa.Table.from_pylist(
        [
            {
                "required_root": {"id": 1, "labels": ["a", None], "meta": {"score": 1.5}},
                "optional_root": None,
            },
            {
                "required_root": {"id": None, "labels": [], "meta": None},
                "optional_root": {"id": 2, "labels": None, "meta": {"score": None}},
            },
        ],
        schema=schema,
    )
    path = tmp_path / "native-required-optional-root-structs.parquet"
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []


@_requires_pyarrow
def test_native_parquet_stream_materializes_recursive_sibling_repeated_branches(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes recursive sibling repeated branches."""
    path = tmp_path / "native-recursive-sibling-repeated-branches.parquet"
    left_value_type = pa.list_(
        pa.map_(
            pa.string(),
            pa.list_(
                pa.struct(
                    [
                        pa.field(
                            "meta",
                            pa.struct(
                                [
                                    pa.field("codes", pa.list_(pa.int64())),
                                    pa.field("tag", pa.string()),
                                ]
                            ),
                        ),
                        pa.field("x", pa.int64()),
                        pa.field("ys", pa.list_(pa.int64())),
                    ]
                )
            ),
        )
    )
    right_value_type = pa.map_(pa.string(), pa.list_(pa.list_(pa.map_(pa.string(), pa.int64()))))
    item_type = pa.struct(
        [
            pa.field("left", left_value_type),
            pa.field("plain", pa.int64()),
            pa.field("right", right_value_type),
        ]
    )
    table = pa.table(
        {
            "items": pa.array(
                [
                    {
                        "left": [
                            {
                                "a": [
                                    {
                                        "meta": {"codes": [4, None], "tag": "a"},
                                        "x": 1,
                                        "ys": [1, None],
                                    },
                                    None,
                                    {"meta": None, "x": None, "ys": []},
                                ]
                            },
                            {},
                        ],
                        "plain": 10,
                        "right": {"r": [[{"z": 3}, {}], []], "empty": None},
                    },
                    None,
                    {"left": None, "plain": None, "right": {}},
                    {
                        "left": [{"b": [{"meta": {"codes": [], "tag": None}, "x": 2, "ys": None}]}],
                        "plain": 20,
                        "right": {"s": None},
                    },
                ],
                type=item_type,
            )
        }
    )
    info = write_read_native_parquet(table, path)
    assert info is not None
    assert info["native_reader_ready"] == 1
    assert info["native_reader_blockers"] == []
    assert (
        max(
            (
                len(column["repeated_level_layouts"])
                for column in info["row_groups"][0]["columns"]
                if column["max_repetition_level"] > 0
            )
        )
        > 3
    )


@_requires_pyarrow
def test_native_parquet_stream_materializes_recursive_null_empty_matrix_corpus(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes recursive null empty matrix corpus."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    def scalar_value(kind: str, seed: int) -> object:
        """Build a deterministic scalar value for the requested leaf type."""
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
        """Build a fully populated value for the recursive specification."""
        kind = spec[0]
        if kind in set(_RECURSIVE_FUZZ_SCALARS):
            return scalar_value(kind, seed)
        if kind == "list":
            return [full_value(spec[1], seed + 1), sparse_value(spec[1], seed + 2), None]
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

    sparse_value = _recursive_fuzz_sparse_value_factory(scalar_value, full_value)
    for index, (name, spec, metrics) in enumerate(_recursive_fuzz_null_empty_matrix_specs()):
        item_type = arrow_type(spec)
        schema = pa.schema([pa.field("items", item_type)])
        batches = [
            pa.record_batch(
                [
                    pa.array(
                        [None, empty_value(spec, index + 10), full_value(spec, index + 20)],
                        type=item_type,
                    )
                ],
                schema=schema,
            ),
            pa.record_batch(
                [
                    pa.array(
                        [sparse_value(spec, index + 30), full_value(spec, index + 40)],
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
        factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
        reader = pa.RecordBatchReader.from_stream(factory)
        out = reader.read_all()
        assert out.schema.equals(expected.schema), name
        assert out.to_pylist() == expected.to_pylist(), name
        assert last_parquet_stream_factory_route() == "native_parquet_stream", name


@_requires_pyarrow
def test_native_parquet_stream_materializes_recursive_row_group_phase_matrix_corpus(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream materializes recursive row group phase matrix corpus."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    def scalar_value(kind: str, seed: int) -> object:
        """Build a deterministic scalar value for the requested leaf type."""
        if kind == "int64":
            return seed * 1000 + 13
        if kind == "string":
            return f"phase-{seed}"
        if kind == "bool":
            return seed % 2 == 1
        if kind == "float64":
            return seed + 0.0625
        raise AssertionError(kind)

    full_value = _recursive_fuzz_full_value_factory(scalar_value, include_null=True)
    sparse_value = _recursive_fuzz_sparse_value_factory(scalar_value, full_value)

    def phase_values(spec: object, phase: str, seed: int) -> list[object]:
        """Build values for one null-and-empty row-group phase."""
        if phase == "all-null":
            return [None, None]
        if phase == "empty-only":
            return [empty_value(spec, seed), empty_value(spec, seed + 1)]
        if phase == "sparse":
            return [sparse_value(spec, seed), None]
        if phase == "full":
            return [full_value(spec, seed), sparse_value(spec, seed + 1)]
        raise AssertionError(phase)

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
        factory = open_parquet_record_batch_stream_factory(path, source="path", feature="test")
        reader = pa.RecordBatchReader.from_stream(factory)
        out = reader.read_all()
        assert out.schema.equals(expected.schema), name
        assert out.to_pylist() == expected.to_pylist(), name
        assert last_parquet_stream_factory_route() == "native_parquet_stream", name


@_requires_pyarrow
def test_native_parquet_stream_preserves_recursive_segmentation_invariants(tmp_path: Path) -> None:
    """Verify native Parquet stream preserves recursive segmentation invariants."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_recursive_layout_summary
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    def scalar_value(kind: str, seed: int) -> object:
        """Build a deterministic scalar value for the requested leaf type."""
        if kind == "int64":
            return seed * 29 + 7
        if kind == "string":
            return f"segment-{seed}"
        if kind == "bool":
            return seed % 2 == 0
        if kind == "float64":
            return seed + 0.875
        raise AssertionError(kind)

    def full_value(spec: object, seed: int) -> object:
        """Build a fully populated value for the recursive specification."""
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
        """Build a sparsely populated value for the recursive specification."""
        kind = spec[0]
        if kind in set(_RECURSIVE_FUZZ_SCALARS):
            return None if seed % 3 == 0 else scalar_value(kind, seed)
        if kind == "list":
            return [None, empty_value(spec[1], seed + 1), full_value(spec[1], seed + 2)]
        if kind == "map":
            return [(f"none-{seed}", None), (f"full-{seed}", full_value(spec[1], seed + 1))]
        if kind == "struct":
            return {
                name: sparse_value(child, seed + child_index)
                if child_index % 2 == 0
                else empty_value(child, seed + child_index)
                for child_index, (name, child) in enumerate(spec[1])
            }
        raise AssertionError(kind)

    phase_value = _recursive_fuzz_phase_value_factory(sparse_value, full_value)

    def write_segments(path: Path, lengths: tuple[int, ...]) -> None:
        """Write recursive values using the requested batch segmentation."""
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

    _, spec, metrics = _recursive_fuzz_row_group_phase_matrix_specs()[4]
    phases = _recursive_fuzz_row_group_phase_labels() * 2
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("payload", arrow_type(spec))])
    table = pa.Table.from_pylist(
        [
            {"id": row_index, "payload": phase_value(spec, phase, row_index * 100)}
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
            path, source="path", feature="test", columns=["payload"]
        )
        out = pa.RecordBatchReader.from_stream(factory).read_all()
        assert out.schema.equals(expected.schema), label
        assert out.to_pylist() == expected.to_pylist(), label
        assert last_parquet_stream_factory_route() == "native_parquet_stream", label
    baseline = summaries["single"]
    for label, summary in summaries.items():
        assert (
            summary["canonical_layout_fingerprint"] == baseline["canonical_layout_fingerprint"]
        ), label
        assert (
            summary["canonical_leaf_level_fingerprint"]
            == baseline["canonical_leaf_level_fingerprint"]
        ), label
        assert (
            summary["canonical_leaf_repetition_path_fingerprint"]
            == baseline["canonical_leaf_repetition_path_fingerprint"]
        ), label
        assert (
            summary["canonical_leaf_repeated_ancestor_fingerprint"]
            == baseline["canonical_leaf_repeated_ancestor_fingerprint"]
        ), label


@_requires_pyarrow
def test_native_parquet_stream_preserves_recursive_root_fingerprints_under_projection_permutations(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream preserves recursive root fingerprints under projection permutations."""
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
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    def scalar_value(kind: str, seed: int) -> object:
        """Build a deterministic scalar value for the requested leaf type."""
        if kind == "int64":
            return seed * 31
        if kind == "string":
            return f"projection-permutation-{seed}"
        if kind == "bool":
            return seed % 2 == 0
        if kind == "float64":
            return seed + 0.3125
        raise AssertionError(kind)

    full_value = _recursive_fuzz_full_value_factory(scalar_value, include_null=False)

    def sparse_value(spec: object, seed: int) -> object:
        """Build a sparsely populated value for the recursive specification."""
        kind = spec[0]
        if kind in set(_RECURSIVE_FUZZ_SCALARS):
            return None if seed % 4 == 0 else scalar_value(kind, seed)
        if kind == "list":
            return [None, empty_value(spec[1], seed + 1), full_value(spec[1], seed + 2)]
        if kind == "map":
            return [(f"none{seed}", None), (f"full{seed}", full_value(spec[1], seed + 1))]
        if kind == "struct":
            return {
                name: sparse_value(child, seed + child_index)
                if child_index % 2 == 0
                else full_value(child, seed + child_index)
                for child_index, (name, child) in enumerate(spec[1])
            }
        raise AssertionError(kind)

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
        path, partitions=[["gamma", "alpha"], ["zeta", "beta"], ["epsilon", "delta"]]
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
            (f"{name}={full_fingerprints[name]}" for name in sorted(projection))
        ), projection
        assert projected_summary["leaf_path_collisions"] == [], projection
        assert projected_summary["repeated_node_path_collisions"] == [], projection
        audit = native_parquet_recursive_projection_contract_audit(path, columns=list(projection))
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
            path, source_columns=source_projection, columns=list(projection)
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
            path, source="path", feature="test", columns=list(projection)
        )
        out = pa.RecordBatchReader.from_stream(factory).read_all()
        selected = expected.select(list(projection))
        assert out.schema.equals(selected.schema), projection
        assert out.to_pylist() == selected.to_pylist(), projection
        assert last_parquet_stream_factory_route() == "native_parquet_stream", projection


@_requires_pyarrow
def test_native_parquet_stream_projects_multiple_recursive_roots(tmp_path: Path) -> None:
    """Verify native Parquet stream projects multiple recursive roots."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

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
                pa.array([[{"a": [1, None], "b": [("k", ["v"])]}], None], type=left_type),
                pa.array(
                    [[("r", {"x": [[("m", 3)], []], "y": [[1.25], [], None]})], []], type=right_type
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
        path, source="path", feature="test", columns=["right", "left"]
    )
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()
    assert out.schema.equals(expected.schema)
    assert out.to_pylist() == expected.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_projects_recursive_row_group_phase_roots(tmp_path: Path) -> None:
    """Verify native Parquet stream projects recursive row group phase roots."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    def scalar_value(kind: str, seed: int) -> object:
        """Build a deterministic scalar value for the requested leaf type."""
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
        """Build a fully populated value for the recursive specification."""
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
        """Build a sparsely populated value for the recursive specification."""
        kind = spec[0]
        if kind in set(_RECURSIVE_FUZZ_SCALARS):
            return None if seed % 2 else scalar_value(kind, seed)
        if kind == "list":
            return [empty_value(spec[1], seed + 1), None, full_value(spec[1], seed + 2)]
        if kind == "map":
            return [(f"s-{seed}", empty_value(spec[1], seed + 1)), (f"n-{seed}", None)]
        if kind == "struct":
            return {
                name: full_value(child, seed + offset)
                if offset % 2 == 0
                else empty_value(child, seed + offset)
                for offset, (name, child) in enumerate(spec[1])
            }
        raise AssertionError(kind)

    phase_value = _recursive_fuzz_phase_value_factory(sparse_value, full_value)
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
        columns = [pa.array([offset + 1, offset + 101], type=pa.int64())]
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
            path, source="path", feature="test", columns=columns
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
    """Verify native Parquet recursive layout summary tracks projected noise roots."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_recursive_layout_summary
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    def scalar_value(kind: str, seed: int) -> object:
        """Build a deterministic scalar value for the requested leaf type."""
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
        """Build a sparsely populated value for the recursive specification."""
        kind = spec[0]
        if kind in set(_RECURSIVE_FUZZ_SCALARS):
            return None if seed % 2 == 0 else scalar_value(kind, seed)
        if kind == "list":
            return [empty_value(spec[1], seed + 1), None, full_value(spec[1], seed + 2)]
        if kind == "map":
            return [(f"s-{seed}", empty_value(spec[1], seed + 1)), (f"n-{seed}", None)]
        if kind == "struct":
            return {
                name: full_value(child, seed + offset)
                if offset % 2 == 0
                else empty_value(child, seed + offset)
                for offset, (name, child) in enumerate(spec[1])
            }
        raise AssertionError(kind)

    phase_value = _recursive_fuzz_phase_value_factory(sparse_value, full_value)
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
        == full_summary["fields"][1]["structural_shape_signature"]
    )
    assert projected_summary["fields"][0]["repetition_depth_max"] >= 4
    assert full_summary["fields"][1]["leaf_paths"] == projected_summary["fields"][0]["leaf_paths"]
    assert full_summary["leaf_path_collisions"] == []
    assert projected_summary["leaf_path_collisions"] == []
    assert full_summary["layout_fingerprint"]
    assert projected_summary["layout_fingerprint"]
    assert (
        projected_summary["fields"][0]["field_fingerprint"]
        == full_summary["fields"][1]["field_fingerprint"]
    )
    assert (
        projected_summary["layout_fingerprint"]
        == projected_summary["fields"][0]["field_fingerprint"]
    )
    factory = open_parquet_record_batch_stream_factory(
        path, source="path", feature="test", columns=["target"]
    )
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()
    expected = table.select(["target"])
    assert out.schema.equals(expected.schema)
    assert out.to_pylist() == expected.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_native_parquet_stream_projects_independent_recursive_roots_in_subsets(
    tmp_path: Path,
) -> None:
    """Verify native Parquet stream projects independent recursive roots in subsets."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    alpha_type = pa.list_(
        pa.struct(
            [
                pa.field("items", pa.list_(pa.map_(pa.string(), pa.int64()))),
                pa.field("note", pa.string()),
            ]
        )
    )
    beta_type = pa.struct(
        [
            pa.field("left", pa.map_(pa.string(), pa.list_(pa.float64()))),
            pa.field("right", pa.list_(pa.struct([pa.field("flag", pa.bool_())]))),
        ]
    )
    gamma_type = pa.map_(
        pa.string(),
        pa.list_(
            pa.struct([pa.field("labels", pa.list_(pa.string())), pa.field("score", pa.int64())])
        ),
    )
    schema = pa.schema(
        [
            pa.field("plain", pa.int64()),
            pa.field("alpha", alpha_type),
            pa.field("beta", beta_type),
            pa.field("gamma", gamma_type),
        ]
    )
    batches = [
        pa.record_batch(
            [
                pa.array([1, 2], type=pa.int64()),
                pa.array([[{"items": [[("a", 10)], []], "note": "a"}], None], type=alpha_type),
                pa.array(
                    [{"left": [("x", [1.25, None])], "right": [{"flag": True}, None]}, None],
                    type=beta_type,
                ),
                pa.array([[("g", [{"labels": ["x", None], "score": 7}])], []], type=gamma_type),
            ],
            schema=schema,
        ),
        pa.record_batch(
            [
                pa.array([3], type=pa.int64()),
                pa.array([[{"items": None, "note": None}]], type=alpha_type),
                pa.array([{"left": [], "right": []}], type=beta_type),
                pa.array([None], type=gamma_type),
            ],
            schema=schema,
        ),
    ]
    full_table = pa.Table.from_batches(batches)
    path = tmp_path / "native-recursive-independent-root-subsets.parquet"
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
    for columns in (["gamma"], ["beta", "alpha"], ["gamma", "plain", "alpha"]):
        factory = open_parquet_record_batch_stream_factory(
            path, source="path", feature="test", columns=columns
        )
        reader = pa.RecordBatchReader.from_stream(factory)
        out = reader.read_all()
        expected = full_table.select(columns)
        assert out.schema.equals(expected.schema), columns
        assert out.to_pylist() == expected.to_pylist(), columns
        assert last_parquet_stream_factory_route() == "native_parquet_stream"


@_requires_pyarrow
def test_list_struct_projection_uses_native_reader(tmp_path: Path) -> None:
    """Verify list struct projection uses native reader."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        open_parquet_record_batch_stream_factory,
    )
    from schema_sanitizer.adapters.parquet.telemetry import last_parquet_stream_factory_route
    from schema_sanitizer.api_impl.file_conversion.writers import write_parquet_native_first_stream

    path = tmp_path / "complex-nested-list-projection.parquet"
    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "items": pa.array(
                [
                    [{"score": 1, "label": "a"}, {"score": 2, "label": "b"}],
                    [{"score": 3, "label": "c"}],
                ],
                type=pa.list_(
                    pa.struct([pa.field("score", pa.int64()), pa.field("label", pa.string())])
                ),
            ),
        }
    )
    expected = table.select(["items"])
    write_parquet_native_first_stream(
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
        path,
        feature="test",
        parquet_compression="uncompressed",
    )
    factory = open_parquet_record_batch_stream_factory(
        path, source="path", feature="test", columns=["items"]
    )
    reader = pa.RecordBatchReader.from_stream(factory)
    out = reader.read_all()
    assert out.schema.equals(expected.schema)
    assert out.to_pylist() == expected.to_pylist()
    assert last_parquet_stream_factory_route() == "native_parquet_stream"


RECURSIVE_RUNTIME_CASES = tuple(
    (name.removeprefix("test_"), value)
    for name, value in globals().items()
    if name.startswith("test_") and callable(value)
)
