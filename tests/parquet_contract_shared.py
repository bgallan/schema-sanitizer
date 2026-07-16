"""Stable contract fixtures shared by the split Parquet gate tests."""

from __future__ import annotations


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
