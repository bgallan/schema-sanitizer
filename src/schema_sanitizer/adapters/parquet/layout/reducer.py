"""Recursive native Parquet layout summary reducer.

It reduces nested footer fields into bounded accumulators while tracking containers,
nullability, unsupported forms, and recursive contract evidence.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .fields import accumulate_recursive_field
from .finalization import finalize_recursive_layout_summary
from .fingerprints import field_fingerprint_bundle

MismatchReporter = Callable[[str], None]

_EMPTY_DEFAULTS = (
    "leaf_paths",
    "leaf_path_components",
    "repeated_node_paths",
    "repeated_node_path_components",
    "leaf_max_definition_levels",
    "leaf_max_repetition_levels",
    "leaf_path_definition_levels",
    "leaf_path_repetition_levels",
)

_ROW_GROUP_FINGERPRINT_ATTRS = (
    ("row_group_leaf_level_fingerprints", "leaf_level"),
    ("row_group_repetition_path_fingerprints", "repetition_path"),
    ("row_group_repeated_ancestor_fingerprints", "repeated_ancestor"),
    ("row_group_leaf_contract_fingerprints", "leaf_contract"),
    ("row_group_root_contract_fingerprints", "root_contract_fingerprint"),
)

_ROW_GROUP_STABILITY = (
    (
        "row_group_canonical_layout_fingerprints",
        "row_group_layout_fingerprints_stable",
        "layout",
    ),
    (
        "row_group_leaf_level_fingerprints",
        "row_group_leaf_level_fingerprints_stable",
        "leaf-level",
    ),
    (
        "row_group_repetition_path_fingerprints",
        "row_group_repetition_path_fingerprints_stable",
        "repetition-path",
    ),
    (
        "row_group_repeated_ancestor_fingerprints",
        "row_group_repeated_ancestor_fingerprints_stable",
        "repeated-ancestor",
    ),
    (
        "row_group_leaf_contract_fingerprints",
        "row_group_leaf_contract_fingerprints_stable",
        "leaf-contract",
    ),
    (
        "row_group_root_contract_fingerprints",
        "row_group_root_contract_fingerprints_stable",
        "root-contract",
    ),
)


def _new_recursive_layout_summary(
    info: dict[str, Any], row_groups: list[dict[str, Any]]
) -> dict[str, Any]:
    """Return the initial folded summary shape for recursive layout diagnostics."""
    return {
        "native_reader_ready": info.get("native_reader_ready"),
        "row_group_count": info.get("row_group_count", len(row_groups)),
        "decoded_row_group_count": 0,
        "field_order": [],
        "field_order_per_row_group": [],
        "fields": [],
        "stable_across_row_groups": True,
        "mismatches": [],
        "layout_fingerprint": "",
        "canonical_layout_fingerprint": "",
        "row_group_layout_fingerprints": [],
        "row_group_canonical_layout_fingerprints": [],
        "row_group_layout_fingerprints_stable": True,
        "row_group_leaf_level_fingerprints": [],
        "row_group_leaf_level_fingerprints_stable": True,
        "row_group_repetition_path_fingerprints": [],
        "row_group_repetition_path_fingerprints_stable": True,
        "row_group_repeated_ancestor_fingerprints": [],
        "row_group_repeated_ancestor_fingerprints_stable": True,
        "row_group_leaf_contract_fingerprints": [],
        "row_group_leaf_contract_fingerprints_stable": True,
        "row_group_root_contract_fingerprints": [],
        "row_group_root_contract_fingerprints_stable": True,
        "field_fingerprints_by_name": {},
        "leaf_level_fingerprints_by_name": {},
        "canonical_leaf_level_fingerprint": "",
        "leaf_repetition_path_fingerprints_by_name": {},
        "canonical_leaf_repetition_path_fingerprint": "",
        "leaf_repeated_ancestor_fingerprints_by_name": {},
        "canonical_leaf_repeated_ancestor_fingerprint": "",
        "leaf_contracts_by_name": {},
        "leaf_contract_fingerprints_by_name": {},
        "canonical_leaf_contract_fingerprint": "",
        "root_contracts_by_name": {},
        "root_contract_fingerprints_by_name": {},
        "canonical_root_contract_fingerprint": "",
        "leaf_path_owner_map": {},
        "leaf_path_component_owner_map": {},
        "leaf_path_collisions": [],
        "repeated_node_path_owner_map": {},
        "repeated_node_path_component_owner_map": {},
        "repeated_node_path_collisions": [],
    }


def _new_field_accumulator() -> dict[str, Any]:
    """Return the initial row-group accumulator for a field position."""
    return {
        "name": None,
        "row_groups_seen": 0,
        "root_kind": None,
        "structural_shape_signature": None,
        "shape_signatures": [],
        "stable_shape_signature": True,
        "leaf_paths": None,
        "leaf_paths_stable": True,
        "leaf_path_components": None,
        "leaf_path_components_stable": True,
        "repeated_node_paths": None,
        "repeated_node_paths_stable": True,
        "repeated_node_path_components": None,
        "repeated_node_path_components_stable": True,
        "leaf_max_definition_levels": None,
        "leaf_max_definition_levels_stable": True,
        "leaf_max_repetition_levels": None,
        "leaf_max_repetition_levels_stable": True,
        "leaf_path_definition_levels": None,
        "leaf_path_definition_levels_stable": True,
        "leaf_path_repetition_levels": None,
        "leaf_path_repetition_levels_stable": True,
        "leaf_count_min": None,
        "leaf_count_max": None,
        "node_count_max": 0,
        "repetition_depth_max": 0,
        "max_node_depth_max": 0,
        "max_child_count_max": 0,
    }


def _record_row_group_fingerprints(
    summary: dict[str, Any], fields: list[dict[str, Any]]
) -> list[str]:
    """Append every canonical row-group fingerprint and return field names."""
    names = [str(field.get("name") or "") for field in fields]
    summary["field_order_per_row_group"].append(names)

    bundles = [field_fingerprint_bundle(field) for field in fields]
    named_bundles = list(zip(names, bundles, strict=True))
    canonical_bundles = sorted(named_bundles, key=lambda item: item[0])

    summary["row_group_layout_fingerprints"].append(";".join(bundle.layout for bundle in bundles))
    summary["row_group_canonical_layout_fingerprints"].append(
        ";".join(f"{name}={bundle.layout}" for name, bundle in canonical_bundles)
    )
    for key, attribute in _ROW_GROUP_FINGERPRINT_ATTRS:
        summary[key].append(
            ";".join(f"{name}={getattr(bundle, attribute)}" for name, bundle in canonical_bundles)
        )
    return names


def _validate_recursive_layout_accumulators(
    summary: dict[str, Any],
    fields_by_position: list[dict[str, Any]],
    note_mismatch: MismatchReporter,
) -> None:
    """Validate row-group coverage and fill accumulator defaults."""
    expected_row_groups = int(summary.get("row_group_count") or 0)
    if expected_row_groups and summary["decoded_row_group_count"] != expected_row_groups:
        note_mismatch(
            "decoded recursive row-group count mismatch: "
            f"{summary['decoded_row_group_count']} != {expected_row_groups}"
        )
    for field_index, accumulator in enumerate(fields_by_position):
        if expected_row_groups and accumulator["row_groups_seen"] != expected_row_groups:
            note_mismatch(
                f"field[{field_index}] was seen in {accumulator['row_groups_seen']} "
                f"row groups, expected {expected_row_groups}"
            )
        for key in _EMPTY_DEFAULTS:
            if accumulator[key] is None:
                accumulator[key] = []
        if accumulator["leaf_count_min"] is None:
            accumulator["leaf_count_min"] = 0
        if accumulator["leaf_count_max"] is None:
            accumulator["leaf_count_max"] = 0


def _validate_row_group_fingerprint_stability(
    summary: dict[str, Any], note_mismatch: MismatchReporter
) -> None:
    """Mark row-group fingerprint families unstable when values drift."""
    for key, stable_key, description in _ROW_GROUP_STABILITY:
        values = summary.get(key) or []
        if values and any(value != values[0] for value in values[1:]):
            summary[stable_key] = False
            note_mismatch(f"recursive row-group {description} fingerprints drifted")


def _native_recursive_layout_summary_from_footer_info(
    info: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Summarize recursive native layout diagnostics across row groups."""
    if info is None:
        return None
    row_groups = list(info.get("row_groups") or [])
    summary = _new_recursive_layout_summary(info, row_groups)
    fields_by_position: list[dict[str, Any]] = []

    def note_mismatch(message: str) -> None:
        """Record one recursive-layout mismatch in the shared summary."""
        summary["stable_across_row_groups"] = False
        summary["mismatches"].append(message)

    for row_group_index, row_group in enumerate(row_groups):
        layout = row_group.get("native_recursive_output_layout") or {}
        if layout.get("decoded") != 1:
            note_mismatch(f"row_group[{row_group_index}] recursive layout was not decoded")
            continue
        summary["decoded_row_group_count"] += 1
        fields = list(layout.get("fields") or [])
        names = _record_row_group_fingerprints(summary, fields)
        if row_group_index == 0:
            summary["field_order"] = list(names)
        elif names != summary["field_order"]:
            note_mismatch(
                f"row_group[{row_group_index}] field order drifted: "
                f"{names!r} != {summary['field_order']!r}"
            )

        while len(fields_by_position) < len(fields):
            fields_by_position.append(_new_field_accumulator())
        for field_index, field in enumerate(fields):
            accumulate_recursive_field(
                fields_by_position[field_index],
                field,
                row_group_index=row_group_index,
                field_index=field_index,
                note_mismatch=note_mismatch,
            )

    _validate_recursive_layout_accumulators(summary, fields_by_position, note_mismatch)
    _validate_row_group_fingerprint_stability(summary, note_mismatch)
    return finalize_recursive_layout_summary(summary, fields_by_position, note_mismatch)
