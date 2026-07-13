"""Core tests for recursive Parquet nested-layout contracts.

These tests exercise pure diagnostics and recursive corpus generators without
requiring PyArrow. Runtime materialization lives in focused native modules.
"""

from __future__ import annotations

# Split from test_parquet_recursive_layout_summary.py: test_native_recursive_layout_summary_detects_row_group_shape_drift, test_native_recursive_layout_summary_is_defensive_for_partial_diagnostics, test_native_recursive_layout_summary_fingerprints_and_collision_checks, ...


def test_native_recursive_layout_summary_detects_row_group_shape_drift() -> None:
    """Verify recursive layout summaries surface row-group shape drift."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    info = {
        "native_reader_ready": 1,
        "row_group_count": 2,
        "row_groups": [
            {
                "native_recursive_output_layout": {
                    "decoded": 1,
                    "fields": [
                        {
                            "name": "items",
                            "root_kind": "list",
                            "structural_shape_signature": "list<struct<a:int64>>",
                            "shape_signature": "list<struct<a:#0:int64>>",
                            "leaf_paths": ["items.list.element.a"],
                            "repeated_node_paths": ["items"],
                            "leaf_count": 1,
                            "node_count": 3,
                            "repetition_depth": 1,
                            "max_node_depth": 2,
                            "max_child_count": 1,
                        }
                    ],
                }
            },
            {
                "native_recursive_output_layout": {
                    "decoded": 1,
                    "fields": [
                        {
                            "name": "items",
                            "root_kind": "list",
                            "structural_shape_signature": "list<struct<a:int64,b:string>>",
                            "shape_signature": "list<struct<a:#0:int64,b:#1:string>>",
                            "leaf_paths": [
                                "items.list.element.a",
                                "items.list.element.b",
                            ],
                            "repeated_node_paths": ["items"],
                            "leaf_count": 2,
                            "node_count": 4,
                            "repetition_depth": 1,
                            "max_node_depth": 2,
                            "max_child_count": 2,
                        }
                    ],
                }
            },
        ],
    }

    summary = _native_recursive_layout_summary_from_footer_info(info)

    assert summary is not None
    assert summary["decoded_row_group_count"] == 2
    assert summary["field_order"] == ["items"]
    assert summary["stable_across_row_groups"] is False
    assert any("structural recursive shape drifted" in m for m in summary["mismatches"])
    assert any("leaf paths drifted" in m for m in summary["mismatches"])
    assert summary["fields"][0]["leaf_count_min"] == 1
    assert summary["fields"][0]["leaf_count_max"] == 2
    assert summary["fields"][0]["max_child_count_max"] == 2
    assert summary["fields"][0]["stable_shape_signature"] is False


def test_native_recursive_layout_summary_is_defensive_for_partial_diagnostics() -> None:
    """Verify recursive layout summaries do not trust incomplete diagnostics."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 0,
            "row_group_count": 2,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 0, "error": "bad"}},
                {"native_recursive_output_layout": {"decoded": 1, "fields": []}},
            ],
        }
    )

    assert summary is not None
    assert summary["native_reader_ready"] == 0
    assert summary["decoded_row_group_count"] == 1
    assert summary["stable_across_row_groups"] is False
    assert summary["fields"] == []
    assert any("not decoded" in m for m in summary["mismatches"])
    assert any("decoded recursive row-group count mismatch" in m for m in summary["mismatches"])


def test_native_recursive_layout_summary_fingerprints_and_collision_checks() -> None:
    """Verify recursive layout summaries provide stable fingerprints and collisions."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    info = {
        "native_reader_ready": 1,
        "row_group_count": 1,
        "row_groups": [
            {
                "native_recursive_output_layout": {
                    "decoded": 1,
                    "fields": [
                        {
                            "name": "left",
                            "root_kind": "struct",
                            "structural_shape_signature": "struct<a:int64>",
                            "shape_signature": "struct<a:#0:int64>",
                            "leaf_paths": ["shared.leaf"],
                            "repeated_node_paths": [],
                            "leaf_count": 1,
                            "node_count": 2,
                            "repetition_depth": 0,
                            "max_node_depth": 1,
                            "max_child_count": 1,
                        },
                        {
                            "name": "right",
                            "root_kind": "struct",
                            "structural_shape_signature": "struct<b:int64>",
                            "shape_signature": "struct<b:#1:int64>",
                            "leaf_paths": ["shared.leaf"],
                            "repeated_node_paths": [],
                            "leaf_count": 1,
                            "node_count": 2,
                            "repetition_depth": 0,
                            "max_node_depth": 1,
                            "max_child_count": 1,
                        },
                    ],
                }
            }
        ],
    }

    summary = _native_recursive_layout_summary_from_footer_info(info)

    assert summary is not None
    assert summary["stable_across_row_groups"] is False
    assert summary["field_order"] == ["left", "right"]
    assert summary["layout_fingerprint"].startswith(
        "left:struct:struct<a:int64>:leaves=[shared.leaf]"
    )
    assert summary["fields"][0]["field_fingerprint"] in summary["layout_fingerprint"]
    assert summary["fields"][1]["field_fingerprint"] in summary["layout_fingerprint"]
    assert summary["leaf_path_collisions"] == [
        {
            "leaf_path": "shared.leaf",
            "first_field": "left",
            "other_field": "right",
        }
    ]
    assert any("leaf path collisions" in m for m in summary["mismatches"])


def test_native_recursive_layout_summary_exposes_order_independent_fingerprints() -> None:
    """Verify canonical fingerprints stay stable across projected root reorder."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    alpha = {
        "name": "alpha",
        "root_kind": "list",
        "structural_shape_signature": "list<struct<a:int64>>",
        "shape_signature": "list<struct<a:#0:int64>>",
        "leaf_paths": ["alpha.list.element.a"],
        "repeated_node_paths": ["alpha"],
        "leaf_count": 1,
        "node_count": 3,
        "repetition_depth": 1,
        "max_node_depth": 2,
        "max_child_count": 1,
    }
    beta = {
        "name": "beta",
        "root_kind": "map",
        "structural_shape_signature": "map<string,list<string>>",
        "shape_signature": "map<string,list<#1:string>>",
        "leaf_paths": ["beta.key_value.key", "beta.key_value.value.list.element"],
        "repeated_node_paths": ["beta", "beta.key_value.value"],
        "leaf_count": 2,
        "node_count": 4,
        "repetition_depth": 2,
        "max_node_depth": 3,
        "max_child_count": 2,
    }

    forward = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 1,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [alpha, beta]}}
            ],
        }
    )
    reversed_summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 1,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [beta, alpha]}}
            ],
        }
    )

    assert forward is not None
    assert reversed_summary is not None
    assert forward["field_order"] == ["alpha", "beta"]
    assert reversed_summary["field_order"] == ["beta", "alpha"]
    assert forward["layout_fingerprint"] != reversed_summary["layout_fingerprint"]
    assert (
        forward["canonical_layout_fingerprint"]
        == (reversed_summary["canonical_layout_fingerprint"])
    )
    assert forward["field_fingerprints_by_name"] == (reversed_summary["field_fingerprints_by_name"])
    assert forward["leaf_path_owner_map"] == {
        "alpha.list.element.a": "alpha",
        "beta.key_value.key": "beta",
        "beta.key_value.value.list.element": "beta",
    }
    assert forward["repeated_node_path_owner_map"] == {
        "alpha": "alpha",
        "beta": "beta",
        "beta.key_value.value": "beta",
    }
    assert forward["stable_across_row_groups"] is True
    assert reversed_summary["stable_across_row_groups"] is True


def test_native_recursive_layout_summary_detects_repeated_node_collisions() -> None:
    """Verify repeated-node path ownership is also collision checked."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 1,
            "row_groups": [
                {
                    "native_recursive_output_layout": {
                        "decoded": 1,
                        "fields": [
                            {
                                "name": "left",
                                "root_kind": "list",
                                "structural_shape_signature": "list<int64>",
                                "shape_signature": "list<#0:int64>",
                                "leaf_paths": ["left.list.element"],
                                "repeated_node_paths": ["shared.repeated"],
                                "leaf_count": 1,
                                "node_count": 2,
                                "repetition_depth": 1,
                                "max_node_depth": 1,
                                "max_child_count": 1,
                            },
                            {
                                "name": "right",
                                "root_kind": "list",
                                "structural_shape_signature": "list<string>",
                                "shape_signature": "list<#1:string>",
                                "leaf_paths": ["right.list.element"],
                                "repeated_node_paths": ["shared.repeated"],
                                "leaf_count": 1,
                                "node_count": 2,
                                "repetition_depth": 1,
                                "max_node_depth": 1,
                                "max_child_count": 1,
                            },
                        ],
                    }
                }
            ],
        }
    )

    assert summary is not None
    assert summary["stable_across_row_groups"] is False
    assert summary["leaf_path_collisions"] == []
    assert summary["repeated_node_path_collisions"] == [
        {
            "repeated_node_path": "shared.repeated",
            "first_field": "left",
            "other_field": "right",
        }
    ]
    assert any("repeated-node path collisions" in m for m in summary["mismatches"])


def test_native_recursive_layout_summary_uses_component_paths_for_arbitrary_names() -> None:
    """Verify path components avoid false collisions for arbitrary field names."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 1,
            "row_groups": [
                {
                    "native_recursive_output_layout": {
                        "decoded": 1,
                        "fields": [
                            {
                                "name": "left.root",
                                "root_kind": "struct",
                                "structural_shape_signature": "struct(a.b,c)",
                                "shape_signature": "struct(a.b,#0)",
                                "leaf_paths": ["payload.a.b.c"],
                                "leaf_path_components": [["payload.a", "b", "c"]],
                                "repeated_node_paths": ["payload.a.b"],
                                "repeated_node_path_components": [["payload.a", "b"]],
                                "leaf_count": 1,
                                "node_count": 3,
                                "repetition_depth": 1,
                                "max_node_depth": 2,
                                "max_child_count": 1,
                            },
                            {
                                "name": "right.root",
                                "root_kind": "struct",
                                "structural_shape_signature": "struct(a,b.c)",
                                "shape_signature": "struct(a,#1)",
                                "leaf_paths": ["payload.a.b.c"],
                                "leaf_path_components": [["payload", "a.b", "c"]],
                                "repeated_node_paths": ["payload.a.b"],
                                "repeated_node_path_components": [["payload", "a.b"]],
                                "leaf_count": 1,
                                "node_count": 3,
                                "repetition_depth": 1,
                                "max_node_depth": 2,
                                "max_child_count": 1,
                            },
                        ],
                    }
                }
            ],
        }
    )

    assert summary is not None
    assert summary["stable_across_row_groups"] is True
    assert summary["leaf_path_collisions"] == []
    assert summary["repeated_node_path_collisions"] == []
    assert summary["leaf_path_component_owner_map"] == {
        '["payload","a.b","c"]': "right.root",
        '["payload.a","b","c"]': "left.root",
    }
    assert summary["repeated_node_path_component_owner_map"] == {
        '["payload","a.b"]': "right.root",
        '["payload.a","b"]': "left.root",
    }
    assert "leaf_components=" in summary["layout_fingerprint"]
    assert "repeated_components=" in summary["layout_fingerprint"]
