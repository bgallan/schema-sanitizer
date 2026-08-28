"""Recursive Parquet field accumulation, contracts, and fingerprints."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .path_components import (
    is_component_prefix,
    normalize_path_components,
    path_components_key,
)


def int_list(raw: Any) -> list[int]:
    """Normalize one diagnostic vector to integers."""
    try:
        return [int(value) for value in list(raw or [])]
    except (TypeError, ValueError):
        return []


def int_matrix(raw: Any) -> list[list[int]]:
    """Normalize one diagnostic matrix to integers."""
    out: list[list[int]] = []
    try:
        rows = list(raw or [])
    except TypeError:
        return out
    for row in rows:
        try:
            out.append([int(value) for value in list(row or [])])
        except (TypeError, ValueError):
            out.append([])
    return out


MismatchReporter = Callable[[str], None]


def record_stable_value(
    accumulator: dict[str, Any],
    *,
    key: str,
    value: Any,
    row_group_index: int,
    field_index: int,
    description: str,
    note_mismatch: MismatchReporter,
    stable_key: str | None = None,
    ignore_none: bool = False,
) -> None:
    """Store the first value and report later drift for one field property."""
    if ignore_none and value is None:
        return
    if accumulator[key] is None:
        accumulator[key] = value
        return
    if value == accumulator[key]:
        return
    if stable_key is not None:
        accumulator[stable_key] = False
    note_mismatch(f"row_group[{row_group_index}] field[{field_index}] {description} drifted")


def integer_paths(value: Any) -> list[list[int]]:
    """Normalize nested integer level paths from footer diagnostics."""
    return [[int(level) for level in list(path or [])] for path in list(value or [])]


def update_field_ranges(accumulator: dict[str, Any], field: dict[str, Any]) -> None:
    """Update leaf-count ranges and recursive shape maxima."""
    accumulator["row_groups_seen"] += 1
    leaf_count = int(field.get("leaf_count") or 0)
    current_min = accumulator["leaf_count_min"]
    current_max = accumulator["leaf_count_max"]
    accumulator["leaf_count_min"] = (
        leaf_count if current_min is None else min(current_min, leaf_count)
    )
    accumulator["leaf_count_max"] = (
        leaf_count if current_max is None else max(current_max, leaf_count)
    )
    for target, source in (
        ("node_count_max", "node_count"),
        ("repetition_depth_max", "repetition_depth"),
        ("max_node_depth_max", "max_node_depth"),
        ("max_child_count_max", "max_child_count"),
    ):
        accumulator[target] = max(accumulator[target], int(field.get(source) or 0))


def accumulate_recursive_field(
    accumulator: dict[str, Any],
    field: dict[str, Any],
    *,
    row_group_index: int,
    field_index: int,
    note_mismatch: MismatchReporter,
) -> None:
    """Merge one row-group field diagnostic into its positional accumulator."""
    name = field.get("name")
    if accumulator["name"] is None:
        accumulator["name"] = name
    elif name != accumulator["name"]:
        note_mismatch(
            f"row_group[{row_group_index}] field[{field_index}] name drifted: "
            f"{name!r} != {accumulator['name']!r}"
        )

    record_stable_value(
        accumulator,
        key="root_kind",
        value=field.get("root_kind"),
        row_group_index=row_group_index,
        field_index=field_index,
        description="root kind",
        note_mismatch=note_mismatch,
    )
    record_stable_value(
        accumulator,
        key="structural_shape_signature",
        value=field.get("structural_shape_signature"),
        row_group_index=row_group_index,
        field_index=field_index,
        description="structural recursive shape",
        note_mismatch=note_mismatch,
    )

    physical = field.get("shape_signature")
    if physical is not None and physical not in accumulator["shape_signatures"]:
        accumulator["shape_signatures"].append(physical)
    if len(accumulator["shape_signatures"]) > 1:
        accumulator["stable_shape_signature"] = False
        note_mismatch(
            f"row_group[{row_group_index}] field[{field_index}] physical recursive shape drifted"
        )

    stable_values = (
        (
            "leaf_paths",
            list(field.get("leaf_paths") or []),
            "leaf_paths_stable",
            "leaf paths",
            False,
        ),
        (
            "leaf_path_components",
            normalize_path_components(field.get("leaf_path_components")),
            "leaf_path_components_stable",
            "leaf path components",
            True,
        ),
        (
            "repeated_node_paths",
            list(field.get("repeated_node_paths") or []),
            "repeated_node_paths_stable",
            "repeated node paths",
            False,
        ),
        (
            "repeated_node_path_components",
            normalize_path_components(field.get("repeated_node_path_components")),
            "repeated_node_path_components_stable",
            "repeated node path components",
            True,
        ),
        (
            "leaf_max_definition_levels",
            [int(level) for level in list(field.get("leaf_max_definition_levels") or [])],
            "leaf_max_definition_levels_stable",
            "leaf max definition levels",
            False,
        ),
        (
            "leaf_max_repetition_levels",
            [int(level) for level in list(field.get("leaf_max_repetition_levels") or [])],
            "leaf_max_repetition_levels_stable",
            "leaf max repetition levels",
            False,
        ),
        (
            "leaf_path_definition_levels",
            integer_paths(field.get("leaf_path_definition_levels")),
            "leaf_path_definition_levels_stable",
            "leaf path definition levels",
            False,
        ),
        (
            "leaf_path_repetition_levels",
            integer_paths(field.get("leaf_path_repetition_levels")),
            "leaf_path_repetition_levels_stable",
            "leaf path repetition levels",
            False,
        ),
    )
    for key, value, stable_key, description, ignore_none in stable_values:
        record_stable_value(
            accumulator,
            key=key,
            value=value,
            row_group_index=row_group_index,
            field_index=field_index,
            description=description,
            note_mismatch=note_mismatch,
            stable_key=stable_key,
            ignore_none=ignore_none,
        )

    update_field_ranges(accumulator, field)


def _component_ancestors(
    components: list[str], repeated_components: list[list[str]], rep_levels: list[int]
) -> list[dict[str, Any]]:
    """Return repeated ancestors represented with component-aware paths."""
    ancestors: list[dict[str, Any]] = []
    for repeated in repeated_components:
        if not components or not is_component_prefix(repeated, components):
            continue
        level_index = min(max(len(repeated) - 1, 0), len(rep_levels) - 1) if rep_levels else -1
        ancestors.append(
            {
                "path_components": list(repeated),
                "repetition_level": rep_levels[level_index] if level_index >= 0 else None,
            }
        )
    return ancestors


def leaf_contracts_from_field(field: dict[str, Any]) -> list[dict[str, Any]]:
    """Return explicit per-leaf recursive contracts for production audits."""
    leaf_paths = [str(path) for path in list(field.get("leaf_paths") or [])]
    leaf_components = normalize_path_components(field.get("leaf_path_components"))
    repeated_components = normalize_path_components(field.get("repeated_node_path_components"))
    if leaf_components is None or repeated_components is None:
        raise ValueError("recursive Parquet diagnostics require component-aware paths")
    leaf_max_def = int_list(field.get("leaf_max_definition_levels"))
    leaf_max_rep = int_list(field.get("leaf_max_repetition_levels"))
    leaf_path_def = int_matrix(field.get("leaf_path_definition_levels"))
    leaf_path_rep = int_matrix(field.get("leaf_path_repetition_levels"))
    leaf_count = max(
        len(leaf_paths),
        len(leaf_components),
        len(leaf_max_def),
        len(leaf_max_rep),
        len(leaf_path_def),
        len(leaf_path_rep),
    )

    contracts: list[dict[str, Any]] = []
    for leaf_index in range(leaf_count):
        components = list(leaf_components[leaf_index]) if leaf_index < len(leaf_components) else []
        path_label = (
            leaf_paths[leaf_index]
            if leaf_index < len(leaf_paths)
            else path_components_key(components)
        )
        rep_levels = list(leaf_path_rep[leaf_index]) if leaf_index < len(leaf_path_rep) else []
        repeated_ancestors = _component_ancestors(components, repeated_components, rep_levels)
        contracts.append(
            {
                "leaf_index": leaf_index,
                "leaf_path": path_label,
                "leaf_path_components": components,
                "max_definition_level": (
                    leaf_max_def[leaf_index] if leaf_index < len(leaf_max_def) else None
                ),
                "max_repetition_level": (
                    leaf_max_rep[leaf_index] if leaf_index < len(leaf_max_rep) else None
                ),
                "path_definition_levels": (
                    list(leaf_path_def[leaf_index]) if leaf_index < len(leaf_path_def) else []
                ),
                "path_repetition_levels": rep_levels,
                "repeated_ancestors": repeated_ancestors,
            }
        )
    return contracts


def leaf_level_fingerprint_from_field(field: dict[str, Any]) -> str:
    """Return the definition-level contract for one root field."""
    return (
        f"def={int_list(field.get('leaf_max_definition_levels'))}:"
        f"rep={int_list(field.get('leaf_max_repetition_levels'))}:"
        f"path_def={int_matrix(field.get('leaf_path_definition_levels'))}"
    )


def leaf_repetition_path_fingerprint_from_field(field: dict[str, Any]) -> str:
    """Return the repetition-level path contract for one root field."""
    return (
        f"max_rep={int_list(field.get('leaf_max_repetition_levels'))}:"
        f"path_rep={int_matrix(field.get('leaf_path_repetition_levels'))}"
    )


def _component_ancestor_fingerprint(field: dict[str, Any]) -> str:
    """Build a repeated-ancestor fingerprint from component-aware paths."""
    leaf_components = normalize_path_components(field.get("leaf_path_components"))
    repeated_components = normalize_path_components(field.get("repeated_node_path_components"))
    if leaf_components is None or repeated_components is None:
        raise ValueError("recursive Parquet diagnostics require component-aware paths")
    leaf_rep_levels = int_matrix(field.get("leaf_path_repetition_levels"))
    leaf_parts: list[str] = []
    for leaf_index, components in enumerate(leaf_components):
        levels = leaf_rep_levels[leaf_index] if leaf_index < len(leaf_rep_levels) else []
        ancestors: list[str] = []
        for repeated in repeated_components:
            if not is_component_prefix(repeated, components):
                continue
            level_index = min(max(len(repeated) - 1, 0), len(levels) - 1) if levels else -1
            level = levels[level_index] if level_index >= 0 else "?"
            ancestors.append(f"{path_components_key(repeated)}@{level}")
        level_vector = ",".join(str(level) for level in levels)
        leaf_parts.append(
            f"{path_components_key(components)}[{level_vector}]<" + ",".join(ancestors) + ">"
        )
    return "|".join(leaf_parts)


def leaf_repeated_ancestor_fingerprint_from_field(field: dict[str, Any]) -> str:
    """Map each leaf to repeated ancestors and their path repetition level."""
    return _component_ancestor_fingerprint(field)
