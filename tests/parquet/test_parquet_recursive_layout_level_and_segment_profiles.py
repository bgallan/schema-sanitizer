"""Core tests for recursive Parquet nested-layout contracts.

These tests exercise pure diagnostics and recursive corpus generators without
requiring PyArrow. Runtime materialization lives in focused native modules.
"""

from __future__ import annotations


def test_native_recursive_layout_summary_component_collision_is_authoritative() -> None:
    """Verify identical component paths collide even if labels differ."""
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
                                "leaf_paths": ["x.y"],
                                "leaf_path_components": [["x", "y"]],
                                "repeated_node_paths": ["x"],
                                "repeated_node_path_components": [["x"]],
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
                                "leaf_paths": ["x/y"],
                                "leaf_path_components": [["x", "y"]],
                                "repeated_node_paths": ["x"],
                                "repeated_node_path_components": [["x"]],
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
    assert summary["leaf_path_collisions"] == [
        {
            "leaf_path": "x/y",
            "leaf_path_components": ["x", "y"],
            "first_field": "left",
            "other_field": "right",
        }
    ]
    assert summary["repeated_node_path_collisions"] == [
        {
            "repeated_node_path": "x",
            "repeated_node_path_components": ["x"],
            "first_field": "left",
            "other_field": "right",
        }
    ]


def test_native_recursive_layout_summary_tracks_definition_level_profiles() -> None:
    """Verify nullable/required leaf level profiles are part of the layout contract."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    def field(def_levels: list[int], path_defs: list[list[int]]) -> dict[str, object]:
        """Internal test helper."""
        return {
            "name": "payload",
            "root_kind": "struct",
            "structural_shape_signature": "struct(required_list,list_optional)",
            "shape_signature": "struct(#0,#1)",
            "leaf_paths": [
                "payload.required_list.element.score",
                "payload.optional_list.element.name",
            ],
            "leaf_path_components": [
                ["payload", "required_list", "element", "score"],
                ["payload", "optional_list", "element", "name"],
            ],
            "repeated_node_paths": ["payload.required_list", "payload.optional_list"],
            "repeated_node_path_components": [
                ["payload", "required_list"],
                ["payload", "optional_list"],
            ],
            "leaf_max_definition_levels": def_levels,
            "leaf_max_repetition_levels": [1, 1],
            "leaf_path_definition_levels": path_defs,
            "leaf_count": 2,
            "node_count": 6,
            "repetition_depth": 1,
            "max_node_depth": 3,
            "max_child_count": 2,
        }

    summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 2,
            "row_groups": [
                {
                    "native_recursive_output_layout": {
                        "decoded": 1,
                        "fields": [field([1, 3], [[0, 1, 1, 1], [0, 1, 2, 3]])],
                    }
                },
                {
                    "native_recursive_output_layout": {
                        "decoded": 1,
                        "fields": [field([1, 3], [[0, 1, 1, 1], [0, 1, 2, 3]])],
                    }
                },
            ],
        }
    )

    assert summary is not None
    assert summary["stable_across_row_groups"] is True
    assert summary["leaf_level_fingerprints_by_name"] == {
        "payload": "def=[1, 3]:rep=[1, 1]:path_def=[[0, 1, 1, 1], [0, 1, 2, 3]]"
    }
    assert summary["canonical_leaf_level_fingerprint"] == (
        "payload=def=[1, 3]:rep=[1, 1]:path_def=[[0, 1, 1, 1], [0, 1, 2, 3]]"
    )
    assert "def=[1,3]" in summary["layout_fingerprint"]
    assert summary["fields"][0]["leaf_max_definition_levels"] == [1, 3]
    assert summary["fields"][0]["leaf_path_definition_levels_stable"] is True


def test_native_recursive_layout_summary_detects_definition_level_drift() -> None:
    """Verify row-group drift in nullable/repeated level profiles is explicit."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    def field(
        def_levels: list[int], rep_levels: list[int], path_defs: list[list[int]]
    ) -> dict[str, object]:
        """Internal test helper."""
        return {
            "name": "payload",
            "root_kind": "list",
            "structural_shape_signature": "list<struct<score:int64>>",
            "shape_signature": "list<struct<#0:int64>>",
            "leaf_paths": ["payload.list.element.score"],
            "leaf_path_components": [["payload", "list", "element", "score"]],
            "repeated_node_paths": ["payload.list"],
            "repeated_node_path_components": [["payload", "list"]],
            "leaf_max_definition_levels": def_levels,
            "leaf_max_repetition_levels": rep_levels,
            "leaf_path_definition_levels": path_defs,
            "leaf_count": 1,
            "node_count": 3,
            "repetition_depth": 1,
            "max_node_depth": 2,
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
                        "fields": [field([2], [1], [[0, 1, 2, 2]])],
                    }
                },
                {
                    "native_recursive_output_layout": {
                        "decoded": 1,
                        "fields": [field([3], [2], [[0, 1, 2, 3]])],
                    }
                },
            ],
        }
    )

    assert summary is not None
    assert summary["stable_across_row_groups"] is False
    assert summary["fields"][0]["leaf_max_definition_levels_stable"] is False
    assert summary["fields"][0]["leaf_max_repetition_levels_stable"] is False
    assert summary["fields"][0]["leaf_path_definition_levels_stable"] is False
    assert any("leaf max definition levels drifted" in item for item in summary["mismatches"])
    assert any("leaf max repetition levels drifted" in item for item in summary["mismatches"])
    assert any("leaf path definition levels drifted" in item for item in summary["mismatches"])


def test_native_recursive_layout_summary_tracks_path_repetition_profiles() -> None:
    """Verify per-leaf repetition paths are part of the recursive contract."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    field = {
        "name": "payload",
        "root_kind": "map",
        "structural_shape_signature": "map<string,list<struct<score:int64>>>",
        "shape_signature": "map<string,list<struct<#0:int64>>>",
        "leaf_paths": ["payload.entries.value.list.element.score"],
        "leaf_path_components": [["payload", "entries", "value", "list", "element", "score"]],
        "repeated_node_paths": ["payload.entries", "payload.entries.value.list"],
        "repeated_node_path_components": [
            ["payload", "entries"],
            ["payload", "entries", "value", "list"],
        ],
        "leaf_max_definition_levels": [5],
        "leaf_max_repetition_levels": [2],
        "leaf_path_definition_levels": [[0, 1, 2, 3, 4, 5]],
        "leaf_path_repetition_levels": [[0, 1, 1, 2, 2, 2]],
        "leaf_count": 1,
        "node_count": 5,
        "repetition_depth": 2,
        "max_node_depth": 5,
        "max_child_count": 1,
    }

    summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 2,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [field]}},
                {"native_recursive_output_layout": {"decoded": 1, "fields": [dict(field)]}},
            ],
        }
    )

    assert summary is not None
    assert summary["stable_across_row_groups"] is True
    assert summary["fields"][0]["leaf_path_repetition_levels"] == [[0, 1, 1, 2, 2, 2]]
    assert summary["fields"][0]["leaf_path_repetition_levels_stable"] is True
    assert summary["leaf_repetition_path_fingerprints_by_name"] == {
        "payload": "max_rep=[2]:path_rep=[[0, 1, 1, 2, 2, 2]]"
    }
    assert summary["canonical_leaf_repetition_path_fingerprint"] == (
        "payload=max_rep=[2]:path_rep=[[0, 1, 1, 2, 2, 2]]"
    )
    assert "path_rep=[[0,1,1,2,2,2]]" in summary["layout_fingerprint"]


def test_native_recursive_layout_summary_detects_path_repetition_drift() -> None:
    """Verify repeated-container topology drift is reported explicitly."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    def field(path_rep: list[list[int]]) -> dict[str, object]:
        """Internal test helper."""
        return {
            "name": "payload",
            "root_kind": "list",
            "structural_shape_signature": "list<map<string,int64>>",
            "shape_signature": "list<map<string,#0:int64>>",
            "leaf_paths": ["payload.list.element.entries.value"],
            "leaf_path_components": [["payload", "list", "element", "entries", "value"]],
            "repeated_node_paths": ["payload.list", "payload.list.element.entries"],
            "repeated_node_path_components": [
                ["payload", "list"],
                ["payload", "list", "element", "entries"],
            ],
            "leaf_max_definition_levels": [4],
            "leaf_max_repetition_levels": [2],
            "leaf_path_definition_levels": [[0, 1, 2, 3, 4]],
            "leaf_path_repetition_levels": path_rep,
            "leaf_count": 1,
            "node_count": 4,
            "repetition_depth": 2,
            "max_node_depth": 4,
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
                        "fields": [field([[0, 1, 1, 2, 2]])],
                    }
                },
                {
                    "native_recursive_output_layout": {
                        "decoded": 1,
                        "fields": [field([[0, 1, 2, 2, 2]])],
                    }
                },
            ],
        }
    )

    assert summary is not None
    assert summary["stable_across_row_groups"] is False
    assert summary["fields"][0]["leaf_path_repetition_levels_stable"] is False
    assert any("leaf path repetition levels drifted" in item for item in summary["mismatches"])


def test_native_recursive_layout_summary_tracks_row_group_segment_fingerprints() -> None:
    """Verify row-group fingerprints make nested segmentation stability visible."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    def field(name: str) -> dict[str, object]:
        """Internal test helper."""
        return {
            "name": name,
            "root_kind": "struct",
            "structural_shape_signature": "struct<items:list<map<string,int64>>>",
            "shape_signature": "struct<items:list<map<string,#0:int64>>>",
            "leaf_paths": [f"{name}.items.list.element.entries.value"],
            "leaf_path_components": [[name, "items", "list", "element", "entries", "value"]],
            "repeated_node_paths": [
                f"{name}.items.list",
                f"{name}.items.list.element.entries",
            ],
            "repeated_node_path_components": [
                [name, "items", "list"],
                [name, "items", "list", "element", "entries"],
            ],
            "leaf_max_definition_levels": [5],
            "leaf_max_repetition_levels": [2],
            "leaf_path_definition_levels": [[0, 1, 2, 3, 4, 5]],
            "leaf_path_repetition_levels": [[0, 1, 1, 2, 2, 2]],
            "leaf_count": 1,
            "node_count": 6,
            "repetition_depth": 2,
            "max_node_depth": 5,
            "max_child_count": 1,
        }

    summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 3,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [field("payload")]}},
                {"native_recursive_output_layout": {"decoded": 1, "fields": [field("payload")]}},
                {"native_recursive_output_layout": {"decoded": 1, "fields": [field("payload")]}},
            ],
        }
    )

    assert summary is not None
    assert summary["stable_across_row_groups"] is True
    assert summary["row_group_layout_fingerprints_stable"] is True
    assert summary["row_group_leaf_level_fingerprints_stable"] is True
    assert summary["row_group_repetition_path_fingerprints_stable"] is True
    assert len(summary["row_group_canonical_layout_fingerprints"]) == 3
    assert len(set(summary["row_group_canonical_layout_fingerprints"])) == 1
    assert (
        summary["row_group_canonical_layout_fingerprints"][0]
        == (summary["canonical_layout_fingerprint"])
    )
    assert summary["row_group_leaf_level_fingerprints"] == [
        summary["canonical_leaf_level_fingerprint"],
        summary["canonical_leaf_level_fingerprint"],
        summary["canonical_leaf_level_fingerprint"],
    ]
    assert summary["row_group_repetition_path_fingerprints"] == [
        summary["canonical_leaf_repetition_path_fingerprint"],
        summary["canonical_leaf_repetition_path_fingerprint"],
        summary["canonical_leaf_repetition_path_fingerprint"],
    ]


def test_native_recursive_layout_summary_detects_segment_fingerprint_drift() -> None:
    """Verify segment-level fingerprints catch row-group topology drift."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    def field(path_rep: list[list[int]]) -> dict[str, object]:
        """Internal test helper."""
        return {
            "name": "payload",
            "root_kind": "map",
            "structural_shape_signature": "map<string,list<int64>>",
            "shape_signature": "map<string,list<#0:int64>>",
            "leaf_paths": ["payload.entries.value.list.element"],
            "leaf_path_components": [["payload", "entries", "value", "list", "element"]],
            "repeated_node_paths": ["payload.entries", "payload.entries.value.list"],
            "repeated_node_path_components": [
                ["payload", "entries"],
                ["payload", "entries", "value", "list"],
            ],
            "leaf_max_definition_levels": [4],
            "leaf_max_repetition_levels": [2],
            "leaf_path_definition_levels": [[0, 1, 2, 3, 4]],
            "leaf_path_repetition_levels": path_rep,
            "leaf_count": 1,
            "node_count": 5,
            "repetition_depth": 2,
            "max_node_depth": 4,
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
                        "fields": [field([[0, 1, 1, 2, 2]])],
                    }
                },
                {
                    "native_recursive_output_layout": {
                        "decoded": 1,
                        "fields": [field([[0, 1, 2, 2, 2]])],
                    }
                },
            ],
        }
    )

    assert summary is not None
    assert summary["stable_across_row_groups"] is False
    assert summary["row_group_layout_fingerprints_stable"] is False
    assert summary["row_group_repetition_path_fingerprints_stable"] is False
    assert len(set(summary["row_group_repetition_path_fingerprints"])) == 2
    assert any(
        "row-group repetition-path fingerprints drifted" in item for item in summary["mismatches"]
    )
