"""Recursive Parquet nested-shape fixtures shared by runtime tests.

The corpus generators remain separate from the focused assertion modules.
"""

from __future__ import annotations

import itertools

_RECURSIVE_FUZZ_OPS = ("list", "map", "struct")
_RECURSIVE_FUZZ_SCALARS = ("int64", "string", "bool", "float64")


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
    """Return a deterministic pseudo-random recursive grammar corpus.

    This complements the bounded Cartesian corpus with less regular tree shapes:
    varied root kinds, branch widths, repeated siblings, and mixed scalar leaves.
    It is intentionally deterministic so failing production-like shapes can be
    reproduced by corpus name.
    """

    def next_seed(seed: int, salt: int) -> int:
        """Internal recursive fuzz helper."""
        return (seed * 1103515245 + 12345 + salt * 2654435761) & 0x7FFFFFFF

    def node(seed: int, depth: int, max_depth: int, forced: str | None = None) -> object:
        """Internal recursive fuzz helper."""
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
