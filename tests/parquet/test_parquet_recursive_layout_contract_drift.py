"""Core tests for recursive Parquet nested-layout contracts.

These tests exercise pure diagnostics and recursive corpus generators without
requiring PyArrow. Runtime materialization lives in focused native modules.
"""

from __future__ import annotations

from _support.parquet_contracts import repeated_ancestor_field


def test_native_recursive_layout_summary_detects_root_contract_drift() -> None:
    """Verify root-level contract drift is visible even for one projected root."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    def field(extra_leaf: bool) -> dict[str, object]:
        """Internal test helper."""
        leaf_paths = ["payload.list.element.items.entries.value"]
        leaf_components = [["payload", "list", "element", "items", "entries", "value"]]
        leaf_max_definition_levels = [5]
        leaf_max_repetition_levels = [2]
        leaf_path_definition_levels = [[0, 1, 2, 3, 4, 5]]
        leaf_path_repetition_levels = [[0, 1, 1, 1, 2, 2]]
        if extra_leaf:
            leaf_paths.append("payload.list.element.audit")
            leaf_components.append(["payload", "list", "element", "audit"])
            leaf_max_definition_levels.append(3)
            leaf_max_repetition_levels.append(1)
            leaf_path_definition_levels.append([0, 1, 2, 3])
            leaf_path_repetition_levels.append([0, 1, 1, 1])
        return {
            "name": "payload",
            "root_kind": "list",
            "structural_shape_signature": (
                "list<struct<items:map<string,int64>,audit:string>>"
                if extra_leaf
                else "list<struct<items:map<string,int64>>>"
            ),
            "shape_signature": (
                "list<struct<items:map<string,#0:int64>,audit:#1:string>>"
                if extra_leaf
                else "list<struct<items:map<string,#0:int64>>>"
            ),
            "leaf_paths": leaf_paths,
            "leaf_path_components": leaf_components,
            "repeated_node_paths": [
                "payload.list",
                "payload.list.element.items.entries",
            ],
            "repeated_node_path_components": [
                ["payload", "list"],
                ["payload", "list", "element", "items", "entries"],
            ],
            "leaf_max_definition_levels": leaf_max_definition_levels,
            "leaf_max_repetition_levels": leaf_max_repetition_levels,
            "leaf_path_definition_levels": leaf_path_definition_levels,
            "leaf_path_repetition_levels": leaf_path_repetition_levels,
            "leaf_count": len(leaf_paths),
            "node_count": 6 + (1 if extra_leaf else 0),
            "repetition_depth": 2,
            "max_node_depth": 5,
            "max_child_count": 2 if extra_leaf else 1,
        }

    summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 2,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [field(False)]}},
                {"native_recursive_output_layout": {"decoded": 1, "fields": [field(True)]}},
            ],
        }
    )

    assert summary is not None
    assert summary["stable_across_row_groups"] is False
    assert summary["row_group_root_contract_fingerprints_stable"] is False
    assert len(set(summary["row_group_root_contract_fingerprints"])) == 2
    assert any(
        "row-group root-contract fingerprints drifted" in item for item in summary["mismatches"]
    )


def test_native_recursive_layout_summary_detects_repeated_ancestor_drift() -> None:
    """Verify repeated ancestry drift is called out even when paths are present."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    field = repeated_ancestor_field

    summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 2,
            "row_groups": [
                {
                    "native_recursive_output_layout": {
                        "decoded": 1,
                        "fields": [field([[0, 1, 1, 2, 2, 3, 3]])],
                    }
                },
                {
                    "native_recursive_output_layout": {
                        "decoded": 1,
                        "fields": [field([[0, 1, 2, 2, 2, 3, 3]])],
                    }
                },
            ],
        }
    )

    assert summary is not None
    assert summary["stable_across_row_groups"] is False
    assert summary["row_group_repeated_ancestor_fingerprints_stable"] is False
    assert len(set(summary["row_group_repeated_ancestor_fingerprints"])) == 2
    assert any(
        "row-group repeated-ancestor fingerprints drifted" in item for item in summary["mismatches"]
    )
