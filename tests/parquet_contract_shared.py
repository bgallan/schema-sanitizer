"""Stable contract fixtures shared by the split Parquet gate tests."""

from __future__ import annotations


def filter_rejecting_writer_status(
    captured: dict[str, object],
    *_args: object,
    **kwargs: object,
) -> dict[str, object]:
    """Capture writer-gate arguments and return a filter-rejection status."""
    captured.update(kwargs)
    return {
        "applicable": True,
        "satisfied": False,
        "issues": ["native reader filter contract: predicate filters require PyArrow"],
        "created_by": "schema-sanitizer native parquet writer",
        "native_reader_ready": True,
        "filters_present": True,
        "filter_contract_satisfied": False,
        "nested_contract_applicable": True,
        "nested_contract_satisfied": True,
    }


def stable_native_nested_contract_summary() -> dict[str, object]:
    """Return a compact recursive summary with every contract family stable."""
    return {
        "native_reader_ready": 1,
        "row_group_count": 2,
        "decoded_row_group_count": 2,
        "field_order": ["payload"],
        "fields": [{"name": "payload"}],
        "stable_across_row_groups": True,
        "mismatches": [],
        "row_group_layout_fingerprints_stable": True,
        "row_group_leaf_level_fingerprints_stable": True,
        "row_group_repetition_path_fingerprints_stable": True,
        "row_group_repeated_ancestor_fingerprints_stable": True,
        "row_group_leaf_contract_fingerprints_stable": True,
        "row_group_root_contract_fingerprints_stable": True,
        "field_fingerprints_by_name": {"payload": "field-fp"},
        "leaf_contract_fingerprints_by_name": {"payload": "leaf-fp"},
        "root_contract_fingerprints_by_name": {"payload": "root-fp"},
        "leaf_path_collisions": [],
        "repeated_node_path_collisions": [],
        "canonical_layout_fingerprint": "payload=field-fp",
        "canonical_leaf_level_fingerprint": "payload=levels-fp",
        "canonical_leaf_repetition_path_fingerprint": "payload=rep-fp",
        "canonical_leaf_repeated_ancestor_fingerprint": "payload=ancestor-fp",
        "canonical_leaf_contract_fingerprint": "payload=leaf-fp",
        "canonical_root_contract_fingerprint": "payload=root-fp",
    }


def stable_native_writer_footer_info() -> dict[str, object]:
    """Return footer diagnostics for a stable schema-sanitizer nested file."""

    def field() -> dict[str, object]:
        """Return one stable nested field diagnostic."""
        return {
            "name": "payload",
            "root_kind": "list",
            "structural_shape_signature": "list<struct<items:list<int64>>>",
            "shape_signature": "list<struct<items:list<#0:int64>>>",
            "leaf_paths": ["payload.list.element.items.list.element"],
            "leaf_path_components": [["payload", "list", "element", "items", "list", "element"]],
            "repeated_node_paths": [
                "payload.list",
                "payload.list.element.items.list",
            ],
            "repeated_node_path_components": [
                ["payload", "list"],
                ["payload", "list", "element", "items", "list"],
            ],
            "leaf_max_definition_levels": [5],
            "leaf_max_repetition_levels": [2],
            "leaf_path_definition_levels": [[0, 1, 2, 3, 4, 5]],
            "leaf_path_repetition_levels": [[0, 1, 1, 1, 2, 2]],
            "leaf_count": 1,
            "node_count": 6,
            "repetition_depth": 2,
            "max_node_depth": 5,
            "max_child_count": 1,
        }

    return {
        "created_by": "schema-sanitizer native parquet writer",
        "native_reader_ready": 1,
        "native_reader_blockers": [],
        "row_group_count": 2,
        "num_rows": 4,
        "row_groups": [
            {"native_recursive_output_layout": {"decoded": 1, "fields": [field()]}},
            {"native_recursive_output_layout": {"decoded": 1, "fields": [field()]}},
        ],
    }


def recursive_projection_field(
    name: str,
    root_kind: str,
    leaf_suffix: str,
    rep_depth: int,
) -> dict[str, object]:
    """Build one recursive field diagnostic for projection composition tests."""
    components = [name, *leaf_suffix.split(".")]
    repeated_components = [components[:2]] if rep_depth else []
    return {
        "name": name,
        "root_kind": root_kind,
        "structural_shape_signature": f"{root_kind}<payload:int64>",
        "shape_signature": f"{root_kind}<payload:#0:int64>",
        "leaf_paths": [f"{name}.{leaf_suffix}"],
        "leaf_path_components": [components],
        "repeated_node_paths": [".".join(components[:2])] if rep_depth else [],
        "repeated_node_path_components": repeated_components,
        "leaf_max_definition_levels": [2 + rep_depth],
        "leaf_max_repetition_levels": [rep_depth],
        "leaf_path_definition_levels": [[0, 1, 2 + rep_depth]],
        "leaf_path_repetition_levels": [[0, *([1] * rep_depth), rep_depth]],
        "leaf_count": 1,
        "node_count": 2 + rep_depth,
        "repetition_depth": rep_depth,
        "max_node_depth": 2 + rep_depth,
        "max_child_count": 1,
    }


def list_projection_field(
    name: str,
    leaf_suffix: str,
    max_def: int,
) -> dict[str, object]:
    """Build one list field diagnostic for projection failure tests."""
    components = [name, *leaf_suffix.split(".")]
    return {
        "name": name,
        "root_kind": "list",
        "structural_shape_signature": f"list<struct<{leaf_suffix}:int64>>",
        "shape_signature": f"list<struct<{leaf_suffix}:#0:int64>>",
        "leaf_paths": [f"{name}.{leaf_suffix}"],
        "leaf_path_components": [components],
        "repeated_node_paths": [f"{name}.list"],
        "repeated_node_path_components": [[name, "list"]],
        "leaf_max_definition_levels": [max_def],
        "leaf_max_repetition_levels": [1],
        "leaf_path_definition_levels": [[0, 1, max_def]],
        "leaf_path_repetition_levels": [[0, 1, 1]],
        "leaf_count": 1,
        "node_count": 3,
        "repetition_depth": 1,
        "max_node_depth": 2,
        "max_child_count": 1,
    }


def repeated_ancestor_field(path_rep: list[list[int]]) -> dict[str, object]:
    """Build a nested field diagnostic with configurable repetition levels."""
    return {
        "name": "payload",
        "root_kind": "list",
        "structural_shape_signature": "list<map<string,list<int64>>>",
        "shape_signature": "list<map<string,list<#0:int64>>>",
        "leaf_paths": ["payload.list.element.entries.value.list.element"],
        "leaf_path_components": [
            ["payload", "list", "element", "entries", "value", "list", "element"]
        ],
        "repeated_node_paths": [
            "payload.list",
            "payload.list.element.entries",
            "payload.list.element.entries.value.list",
        ],
        "repeated_node_path_components": [
            ["payload", "list"],
            ["payload", "list", "element", "entries"],
            ["payload", "list", "element", "entries", "value", "list"],
        ],
        "leaf_max_definition_levels": [6],
        "leaf_max_repetition_levels": [3],
        "leaf_path_definition_levels": [[0, 1, 2, 3, 4, 5, 6]],
        "leaf_path_repetition_levels": path_rep,
        "leaf_count": 1,
        "node_count": 7,
        "repetition_depth": 3,
        "max_node_depth": 6,
        "max_child_count": 1,
    }
