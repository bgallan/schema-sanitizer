"""Core tests for recursive Parquet nested-layout contracts.

These tests exercise pure diagnostics and recursive corpus generators without
requiring PyArrow. Runtime materialization lives in focused native modules.
"""

from __future__ import annotations

from _support.parquet_contracts import repeated_ancestor_field


def test_native_recursive_layout_summary_tracks_repeated_ancestor_profiles() -> None:
    """Verify leaf-to-repeated-container ancestry is exposed in diagnostics."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    def field() -> dict[str, object]:
        """Internal test helper."""
        return {
            "name": "payload",
            "root_kind": "struct",
            "structural_shape_signature": "struct<items:list<map<string,list<int64>>>>",
            "shape_signature": "struct<items:list<map<string,list<#0:int64>>>>",
            "leaf_paths": ["payload.items.list.element.entries.value.list.element"],
            "leaf_path_components": [
                [
                    "payload",
                    "items",
                    "list",
                    "element",
                    "entries",
                    "value",
                    "list",
                    "element",
                ]
            ],
            "repeated_node_paths": [
                "payload.items.list",
                "payload.items.list.element.entries",
                "payload.items.list.element.entries.value.list",
            ],
            "repeated_node_path_components": [
                ["payload", "items", "list"],
                ["payload", "items", "list", "element", "entries"],
                [
                    "payload",
                    "items",
                    "list",
                    "element",
                    "entries",
                    "value",
                    "list",
                ],
            ],
            "leaf_max_definition_levels": [7],
            "leaf_max_repetition_levels": [3],
            "leaf_path_definition_levels": [[0, 1, 2, 3, 4, 5, 6, 7]],
            "leaf_path_repetition_levels": [[0, 1, 1, 1, 2, 2, 3, 3]],
            "leaf_count": 1,
            "node_count": 8,
            "repetition_depth": 3,
            "max_node_depth": 7,
            "max_child_count": 1,
        }

    summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 2,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [field()]}},
                {"native_recursive_output_layout": {"decoded": 1, "fields": [field()]}},
            ],
        }
    )

    assert summary is not None
    assert summary["stable_across_row_groups"] is True
    assert summary["row_group_repeated_ancestor_fingerprints_stable"] is True
    assert summary["row_group_repeated_ancestor_fingerprints"] == [
        summary["canonical_leaf_repeated_ancestor_fingerprint"],
        summary["canonical_leaf_repeated_ancestor_fingerprint"],
    ]
    fingerprint = summary["leaf_repeated_ancestor_fingerprints_by_name"]["payload"]
    assert '["payload","items","list"]@1' in fingerprint
    assert '["payload","items","list","element","entries"]@2' in fingerprint
    assert '["payload","items","list","element","entries","value","list"]@3' in fingerprint
    assert "repeated_ancestors=" in summary["field_fingerprints_by_name"]["payload"]


def test_native_recursive_layout_summary_detects_repeated_ancestor_level_assignment_drift() -> None:
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
                        "fields": [field([[0, 2, 2, 2, 2, 3, 3]])],
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


def test_native_recursive_layout_summary_exposes_leaf_contracts() -> None:
    """Verify per-leaf contracts preserve levels, components, and ownership."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    field = {
        "name": "payload",
        "root_kind": "struct",
        "structural_shape_signature": "struct<a.b:list<int64>,a/map:map<string,string>>",
        "shape_signature": "struct<a.b:list<#0:int64>,a/map:map<string,#1:string>>",
        "leaf_paths": [
            "payload.a.b.list.element",
            "payload.a/map.entries.value",
        ],
        "leaf_path_components": [
            ["payload", "a.b", "list", "element"],
            ["payload", "a/map", "entries", "value"],
        ],
        "repeated_node_paths": [
            "payload.a.b.list",
            "payload.a/map.entries",
        ],
        "repeated_node_path_components": [
            ["payload", "a.b", "list"],
            ["payload", "a/map", "entries"],
        ],
        "leaf_max_definition_levels": [3, 3],
        "leaf_max_repetition_levels": [1, 1],
        "leaf_path_definition_levels": [[0, 1, 2, 3], [0, 1, 2, 3]],
        "leaf_path_repetition_levels": [[0, 1, 1, 1], [0, 1, 1, 1]],
        "leaf_count": 2,
        "node_count": 7,
        "repetition_depth": 1,
        "max_node_depth": 3,
        "max_child_count": 2,
    }

    summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 2,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [field]}},
                {"native_recursive_output_layout": {"decoded": 1, "fields": [field]}},
            ],
        }
    )

    assert summary is not None
    assert summary["stable_across_row_groups"] is True
    assert summary["row_group_leaf_contract_fingerprints_stable"] is True
    assert summary["row_group_leaf_contract_fingerprints"] == [
        summary["canonical_leaf_contract_fingerprint"],
        summary["canonical_leaf_contract_fingerprint"],
    ]
    contracts = summary["leaf_contracts_by_name"]["payload"]
    assert len(contracts) == 2
    assert contracts[0]["leaf_path_components"] == ["payload", "a.b", "list", "element"]
    assert contracts[0]["max_definition_level"] == 3
    assert contracts[0]["max_repetition_level"] == 1
    assert contracts[0]["path_definition_levels"] == [0, 1, 2, 3]
    assert contracts[0]["path_repetition_levels"] == [0, 1, 1, 1]
    assert contracts[0]["repeated_ancestors"] == [
        {"path_components": ["payload", "a.b", "list"], "repetition_level": 1}
    ]
    assert contracts[1]["leaf_path_components"] == ["payload", "a/map", "entries", "value"]
    assert contracts[1]["repeated_ancestors"] == [
        {"path_components": ["payload", "a/map", "entries"], "repetition_level": 1}
    ]
    assert "leaf_contracts=" in summary["field_fingerprints_by_name"]["payload"]


def test_native_recursive_layout_summary_detects_leaf_contract_drift() -> None:
    """Verify per-leaf contract drift is reported across row groups."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    def field(path_def: list[list[int]], path_rep: list[list[int]]) -> dict[str, object]:
        """Internal test helper."""
        return {
            "name": "payload",
            "root_kind": "list",
            "structural_shape_signature": "list<struct<x:list<int64>>>",
            "shape_signature": "list<struct<x:list<#0:int64>>>",
            "leaf_paths": ["payload.list.element.x.list.element"],
            "leaf_path_components": [["payload", "list", "element", "x", "list", "element"]],
            "repeated_node_paths": ["payload.list", "payload.list.element.x.list"],
            "repeated_node_path_components": [
                ["payload", "list"],
                ["payload", "list", "element", "x", "list"],
            ],
            "leaf_max_definition_levels": [5],
            "leaf_max_repetition_levels": [2],
            "leaf_path_definition_levels": path_def,
            "leaf_path_repetition_levels": path_rep,
            "leaf_count": 1,
            "node_count": 6,
            "repetition_depth": 2,
            "max_node_depth": 5,
            "max_child_count": 1,
        }

    summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 2,
            "row_groups": [
                {
                    "native_recursive_output_layout": {
                        "decoded": 1,
                        "fields": [field([[0, 1, 2, 3, 4, 5]], [[0, 1, 1, 1, 2, 2]])],
                    }
                },
                {
                    "native_recursive_output_layout": {
                        "decoded": 1,
                        "fields": [field([[0, 1, 2, 4, 4, 5]], [[0, 1, 1, 2, 2, 2]])],
                    }
                },
            ],
        }
    )

    assert summary is not None
    assert summary["stable_across_row_groups"] is False
    assert summary["row_group_leaf_contract_fingerprints_stable"] is False
    assert len(set(summary["row_group_leaf_contract_fingerprints"])) == 2
    assert any(
        "row-group leaf-contract fingerprints drifted" in item for item in summary["mismatches"]
    )


def test_native_recursive_layout_summary_exposes_root_contracts() -> None:
    """Verify root contracts aggregate all nested leaf contracts per projected root."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    alpha = {
        "name": "alpha",
        "root_kind": "struct",
        "structural_shape_signature": "struct<items:list<map<string,int64>>,flag:bool>",
        "shape_signature": "struct<items:list<map<string,#0:int64>>,flag:#1:bool>",
        "leaf_paths": [
            "alpha.items.list.element.entries.value",
            "alpha.flag",
        ],
        "leaf_path_components": [
            ["alpha", "items", "list", "element", "entries", "value"],
            ["alpha", "flag"],
        ],
        "repeated_node_paths": [
            "alpha.items.list",
            "alpha.items.list.element.entries",
        ],
        "repeated_node_path_components": [
            ["alpha", "items", "list"],
            ["alpha", "items", "list", "element", "entries"],
        ],
        "leaf_max_definition_levels": [5, 1],
        "leaf_max_repetition_levels": [2, 0],
        "leaf_path_definition_levels": [[0, 1, 2, 3, 4, 5], [0, 1]],
        "leaf_path_repetition_levels": [[0, 1, 1, 1, 2, 2], [0, 0]],
        "leaf_count": 2,
        "node_count": 7,
        "repetition_depth": 2,
        "max_node_depth": 5,
        "max_child_count": 2,
    }
    beta = {
        "name": "beta",
        "root_kind": "list",
        "structural_shape_signature": "list<struct<k:string,v:map<string,list<int64>>>>",
        "shape_signature": "list<struct<k:#2:string,v:map<string,list<#3:int64>>>>",
        "leaf_paths": [
            "beta.list.element.k",
            "beta.list.element.v.entries.value.list.element",
        ],
        "leaf_path_components": [
            ["beta", "list", "element", "k"],
            ["beta", "list", "element", "v", "entries", "value", "list", "element"],
        ],
        "repeated_node_paths": [
            "beta.list",
            "beta.list.element.v.entries",
            "beta.list.element.v.entries.value.list",
        ],
        "repeated_node_path_components": [
            ["beta", "list"],
            ["beta", "list", "element", "v", "entries"],
            ["beta", "list", "element", "v", "entries", "value", "list"],
        ],
        "leaf_max_definition_levels": [3, 7],
        "leaf_max_repetition_levels": [1, 3],
        "leaf_path_definition_levels": [[0, 1, 2, 3], [0, 1, 2, 3, 4, 5, 6, 7]],
        "leaf_path_repetition_levels": [[0, 1, 1, 1], [0, 1, 1, 1, 2, 2, 3, 3]],
        "leaf_count": 2,
        "node_count": 8,
        "repetition_depth": 3,
        "max_node_depth": 7,
        "max_child_count": 2,
    }

    summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 2,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [alpha, beta]}},
                {"native_recursive_output_layout": {"decoded": 1, "fields": [alpha, beta]}},
            ],
        }
    )

    assert summary is not None
    assert summary["stable_across_row_groups"] is True
    assert summary["row_group_root_contract_fingerprints_stable"] is True
    assert summary["row_group_root_contract_fingerprints"] == [
        summary["canonical_root_contract_fingerprint"],
        summary["canonical_root_contract_fingerprint"],
    ]
    assert set(summary["root_contracts_by_name"]) == {"alpha", "beta"}
    alpha_contract = summary["root_contracts_by_name"]["alpha"]
    beta_contract = summary["root_contracts_by_name"]["beta"]
    assert alpha_contract["root_kind"] == "struct"
    assert beta_contract["root_kind"] == "list"
    assert alpha_contract["leaf_count_min"] == 2
    assert alpha_contract["leaf_count_max"] == 2
    assert beta_contract["repetition_depth_max"] == 3
    assert len(alpha_contract["leaf_contracts"]) == 2
    assert len(beta_contract["leaf_contracts"]) == 2
    assert alpha_contract["leaf_contracts"][0]["repeated_ancestors"] == [
        {"path_components": ["alpha", "items", "list"], "repetition_level": 1},
        {
            "path_components": ["alpha", "items", "list", "element", "entries"],
            "repetition_level": 2,
        },
    ]
    assert (
        summary["root_contract_fingerprints_by_name"]["alpha"]
        in summary["canonical_root_contract_fingerprint"]
    )
