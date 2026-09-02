"""Provide stable callbacks and fixtures for Parquet gate and telemetry contracts.

The helpers capture writer decisions, synthesize status outcomes, and isolate recording state
across cases.
"""

from __future__ import annotations

from functools import partial

import pytest

from schema_sanitizer.adapters.parquet import telemetry as recording


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


_stable_native_nested_contract_summary = stable_native_nested_contract_summary
_stable_native_writer_footer_info = stable_native_writer_footer_info


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


def test_native_recursive_layout_summary_detects_root_contract_drift() -> None:
    """Verify native recursive layout summary detects root contract drift."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    def field(extra_leaf: bool) -> dict[str, object]:
        """Build the recursive field descriptor used by the contract assertion."""
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
            "structural_shape_signature": "list<struct<items:map<string,int64>,audit:string>>"
            if extra_leaf
            else "list<struct<items:map<string,int64>>>",
            "shape_signature": "list<struct<items:map<string,#0:int64>,audit:#1:string>>"
            if extra_leaf
            else "list<struct<items:map<string,#0:int64>>>",
            "leaf_paths": leaf_paths,
            "leaf_path_components": leaf_components,
            "repeated_node_paths": ["payload.list", "payload.list.element.items.entries"],
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
        ("row-group root-contract fingerprints drifted" in item for item in summary["mismatches"])
    )


def test_native_recursive_layout_summary_detects_repeated_ancestor_drift() -> None:
    """Verify native recursive layout summary detects repeated ancestor drift."""
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
        (
            "row-group repeated-ancestor fingerprints drifted" in item
            for item in summary["mismatches"]
        )
    )


def test_native_recursive_layout_summary_detects_row_group_shape_drift() -> None:
    """Verify native recursive layout summary detects row group shape drift."""
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
                            "leaf_path_components": [["items", "list", "element", "a"]],
                            "repeated_node_paths": ["items"],
                            "repeated_node_path_components": [["items"]],
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
                            "leaf_paths": ["items.list.element.a", "items.list.element.b"],
                            "leaf_path_components": [
                                ["items", "list", "element", "a"],
                                ["items", "list", "element", "b"],
                            ],
                            "repeated_node_paths": ["items"],
                            "repeated_node_path_components": [["items"]],
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
    assert any(("structural recursive shape drifted" in m for m in summary["mismatches"]))
    assert any(("leaf paths drifted" in m for m in summary["mismatches"]))
    assert summary["fields"][0]["leaf_count_min"] == 1
    assert summary["fields"][0]["leaf_count_max"] == 2
    assert summary["fields"][0]["max_child_count_max"] == 2
    assert summary["fields"][0]["stable_shape_signature"] is False


def test_native_recursive_layout_summary_is_defensive_for_partial_diagnostics() -> None:
    """Verify native recursive layout summary is defensive for partial diagnostics."""
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
    assert any(("not decoded" in m for m in summary["mismatches"]))
    assert any(("decoded recursive row-group count mismatch" in m for m in summary["mismatches"]))


def test_native_recursive_layout_summary_fingerprints_and_collision_checks() -> None:
    """Verify native recursive layout summary fingerprints and collision checks."""
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
                            "leaf_path_components": [["shared", "leaf"]],
                            "repeated_node_paths": [],
                            "repeated_node_path_components": [],
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
                            "leaf_path_components": [["shared", "leaf"]],
                            "repeated_node_paths": [],
                            "repeated_node_path_components": [],
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
            "leaf_path_components": ["shared", "leaf"],
            "first_field": "left",
            "other_field": "right",
        }
    ]
    assert any(("leaf path collisions" in m for m in summary["mismatches"]))


def test_native_recursive_layout_summary_exposes_order_independent_fingerprints() -> None:
    """Verify native recursive layout summary exposes order independent fingerprints."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    alpha = {
        "name": "alpha",
        "root_kind": "list",
        "structural_shape_signature": "list<struct<a:int64>>",
        "shape_signature": "list<struct<a:#0:int64>>",
        "leaf_paths": ["alpha.list.element.a"],
        "leaf_path_components": [["alpha", "list", "element", "a"]],
        "repeated_node_paths": ["alpha"],
        "repeated_node_path_components": [["alpha"]],
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
        "leaf_path_components": [
            ["beta", "key_value", "key"],
            ["beta", "key_value", "value", "list", "element"],
        ],
        "repeated_node_paths": ["beta", "beta.key_value.value"],
        "repeated_node_path_components": [["beta"], ["beta", "key_value", "value"]],
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
        forward["canonical_layout_fingerprint"] == reversed_summary["canonical_layout_fingerprint"]
    )
    assert forward["field_fingerprints_by_name"] == reversed_summary["field_fingerprints_by_name"]
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
    """Verify native recursive layout summary detects repeated node collisions."""
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
                                "leaf_path_components": [["left", "list", "element"]],
                                "repeated_node_paths": ["shared.repeated"],
                                "repeated_node_path_components": [["shared", "repeated"]],
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
                                "leaf_path_components": [["right", "list", "element"]],
                                "repeated_node_paths": ["shared.repeated"],
                                "repeated_node_path_components": [["shared", "repeated"]],
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
            "repeated_node_path_components": ["shared", "repeated"],
            "first_field": "left",
            "other_field": "right",
        }
    ]
    assert any(("repeated-node path collisions" in m for m in summary["mismatches"]))


def test_native_recursive_layout_summary_uses_component_paths_for_arbitrary_names() -> None:
    """Verify native recursive layout summary uses component paths for arbitrary names."""
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


def test_native_recursive_layout_summary_tracks_repeated_ancestor_profiles() -> None:
    """Verify native recursive layout summary tracks repeated ancestor profiles."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    def field() -> dict[str, object]:
        """Build the recursive field descriptor with repeated-path profiles."""
        return {
            "name": "payload",
            "root_kind": "struct",
            "structural_shape_signature": "struct<items:list<map<string,list<int64>>>>",
            "shape_signature": "struct<items:list<map<string,list<#0:int64>>>>",
            "leaf_paths": ["payload.items.list.element.entries.value.list.element"],
            "leaf_path_components": [
                ["payload", "items", "list", "element", "entries", "value", "list", "element"]
            ],
            "repeated_node_paths": [
                "payload.items.list",
                "payload.items.list.element.entries",
                "payload.items.list.element.entries.value.list",
            ],
            "repeated_node_path_components": [
                ["payload", "items", "list"],
                ["payload", "items", "list", "element", "entries"],
                ["payload", "items", "list", "element", "entries", "value", "list"],
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
    """Verify native recursive layout summary detects repeated ancestor level assignment drift."""
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
        (
            "row-group repeated-ancestor fingerprints drifted" in item
            for item in summary["mismatches"]
        )
    )


def test_native_recursive_layout_summary_exposes_leaf_contracts() -> None:
    """Verify native recursive layout summary exposes leaf contracts."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    field = {
        "name": "payload",
        "root_kind": "struct",
        "structural_shape_signature": "struct<a.b:list<int64>,a/map:map<string,string>>",
        "shape_signature": "struct<a.b:list<#0:int64>,a/map:map<string,#1:string>>",
        "leaf_paths": ["payload.a.b.list.element", "payload.a/map.entries.value"],
        "leaf_path_components": [
            ["payload", "a.b", "list", "element"],
            ["payload", "a/map", "entries", "value"],
        ],
        "repeated_node_paths": ["payload.a.b.list", "payload.a/map.entries"],
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
    """Verify native recursive layout summary detects leaf contract drift."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    def field(path_def: list[list[int]], path_rep: list[list[int]]) -> dict[str, object]:
        """Build the recursive field descriptor used for leaf-drift mutation."""
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
        ("row-group leaf-contract fingerprints drifted" in item for item in summary["mismatches"])
    )


def test_native_recursive_layout_summary_exposes_root_contracts() -> None:
    """Verify native recursive layout summary exposes root contracts."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    alpha = {
        "name": "alpha",
        "root_kind": "struct",
        "structural_shape_signature": "struct<items:list<map<string,int64>>,flag:bool>",
        "shape_signature": "struct<items:list<map<string,#0:int64>>,flag:#1:bool>",
        "leaf_paths": ["alpha.items.list.element.entries.value", "alpha.flag"],
        "leaf_path_components": [
            ["alpha", "items", "list", "element", "entries", "value"],
            ["alpha", "flag"],
        ],
        "repeated_node_paths": ["alpha.items.list", "alpha.items.list.element.entries"],
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
        "leaf_paths": ["beta.list.element.k", "beta.list.element.v.entries.value.list.element"],
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


def test_native_recursive_layout_summary_component_collision_is_authoritative() -> None:
    """Verify native recursive layout summary component collision is authoritative."""
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
    """Verify native recursive layout summary tracks definition level profiles."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    def field(def_levels: list[int], path_defs: list[list[int]]) -> dict[str, object]:
        """Build the recursive field descriptor with definition-level profiles."""
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
    assert (
        summary["canonical_leaf_level_fingerprint"]
        == "payload=def=[1, 3]:rep=[1, 1]:path_def=[[0, 1, 1, 1], [0, 1, 2, 3]]"
    )
    assert "def=[1,3]" in summary["layout_fingerprint"]
    assert summary["fields"][0]["leaf_max_definition_levels"] == [1, 3]
    assert summary["fields"][0]["leaf_path_definition_levels_stable"] is True


def test_native_recursive_layout_summary_detects_definition_level_drift() -> None:
    """Verify native recursive layout summary detects definition level drift."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    def field(
        def_levels: list[int], rep_levels: list[int], path_defs: list[list[int]]
    ) -> dict[str, object]:
        """Build the recursive field descriptor with definition-level profiles."""
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
    assert any(("leaf max definition levels drifted" in item for item in summary["mismatches"]))
    assert any(("leaf max repetition levels drifted" in item for item in summary["mismatches"]))
    assert any(("leaf path definition levels drifted" in item for item in summary["mismatches"]))


def test_native_recursive_layout_summary_tracks_path_repetition_profiles() -> None:
    """Verify native recursive layout summary tracks path repetition profiles."""
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
    assert (
        summary["canonical_leaf_repetition_path_fingerprint"]
        == "payload=max_rep=[2]:path_rep=[[0, 1, 1, 2, 2, 2]]"
    )
    assert "path_rep=[[0,1,1,2,2,2]]" in summary["layout_fingerprint"]


def test_native_recursive_layout_summary_detects_path_repetition_drift() -> None:
    """Verify native recursive layout summary detects path repetition drift."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    def field(path_rep: list[list[int]]) -> dict[str, object]:
        """Build the recursive field descriptor with repeated-path profiles."""
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
    assert any(("leaf path repetition levels drifted" in item for item in summary["mismatches"]))


def test_native_recursive_layout_summary_tracks_row_group_segment_fingerprints() -> None:
    """Verify native recursive layout summary tracks row group segment fingerprints."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    def field(name: str) -> dict[str, object]:
        """Build the recursive field descriptor with row-group fingerprints."""
        return {
            "name": name,
            "root_kind": "struct",
            "structural_shape_signature": "struct<items:list<map<string,int64>>>",
            "shape_signature": "struct<items:list<map<string,#0:int64>>>",
            "leaf_paths": [f"{name}.items.list.element.entries.value"],
            "leaf_path_components": [[name, "items", "list", "element", "entries", "value"]],
            "repeated_node_paths": [f"{name}.items.list", f"{name}.items.list.element.entries"],
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
        == summary["canonical_layout_fingerprint"]
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
    """Verify native recursive layout summary detects segment fingerprint drift."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )

    def field(path_rep: list[list[int]]) -> dict[str, object]:
        """Build the recursive field descriptor with row-group fingerprints."""
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
        ("row-group repetition-path fingerprints drifted" in item for item in summary["mismatches"])
    )


def test_duplicate_projection_names_are_sorted_and_unique() -> None:
    """Verify duplicate projection names are sorted and unique."""
    from schema_sanitizer.adapters.parquet.projection.audits.summary import duplicate_names

    assert duplicate_names(["b", "a", "b", "c", "a", "b"]) == ["a", "b"]
    assert duplicate_names(iter(["only", "once"])) == []


def test_native_recursive_projection_partition_contract_audit_recomposes_full_layout() -> None:
    """Verify native recursive projection partition contract audit recomposes full layout."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.projection.audits.partitions import (
        _native_recursive_projection_partition_contract_audit_from_summaries,
    )

    field = recursive_projection_field
    alpha = field("alpha", "list", "list.element.value", 1)
    beta = field("beta", "map", "entries.value", 1)
    gamma = field("gamma", "struct", "payload.value", 0)
    delta = field("delta", "list", "list.element.items.list.element.value", 2)
    full_summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 2,
            "row_groups": [
                {
                    "native_recursive_output_layout": {
                        "decoded": 1,
                        "fields": [alpha, beta, gamma, delta],
                    }
                },
                {
                    "native_recursive_output_layout": {
                        "decoded": 1,
                        "fields": [alpha, beta, gamma, delta],
                    }
                },
            ],
        }
    )
    first_partition = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 2,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [gamma, alpha]}},
                {"native_recursive_output_layout": {"decoded": 1, "fields": [gamma, alpha]}},
            ],
        }
    )
    second_partition = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 2,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [delta, beta]}},
                {"native_recursive_output_layout": {"decoded": 1, "fields": [delta, beta]}},
            ],
        }
    )
    audit = _native_recursive_projection_partition_contract_audit_from_summaries(
        full_summary,
        [first_partition, second_partition],
        partitions=[["gamma", "alpha"], ["delta", "beta"]],
    )
    assert audit["stable"] is True
    assert audit["mismatches"] == []
    assert audit["coverage_exact"] is True
    assert audit["missing_partition_columns"] == []
    assert audit["unknown_partition_columns"] == []
    assert audit["duplicate_partition_columns"] == []
    assert audit["partition_audits_stable"] is True
    assert audit["field_fingerprint_matches_full"] is True
    assert audit["leaf_contract_fingerprint_matches_full"] is True
    assert audit["root_contract_fingerprint_matches_full"] is True
    assert (
        audit["canonical_full_root_contract_fingerprint"]
        == audit["canonical_recomposed_root_contract_fingerprint"]
    )
    assert (
        audit["recomposed_root_contract_fingerprints_by_name"]
        == full_summary["root_contract_fingerprints_by_name"]
    )


def test_native_recursive_projection_partition_contract_audit_detects_gaps_duplicates_and_drift() -> (
    None
):
    """Verify native recursive projection partition contract audit detects gaps duplicates and drift."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.projection.audits.partitions import (
        _native_recursive_projection_partition_contract_audit_from_summaries,
    )

    field = list_projection_field
    alpha = field("alpha", "list.element.value", 3)
    beta = field("beta", "list.element.value", 3)
    beta_drifted = field("beta", "list.element.changed", 4)
    gamma = field("gamma", "list.element.value", 3)
    delta = field("delta", "list.element.value", 3)
    full_summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 1,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [alpha, beta, gamma]}}
            ],
        }
    )
    first_partition = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 1,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [alpha, beta_drifted]}}
            ],
        }
    )
    second_partition = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 1,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [beta_drifted, delta]}}
            ],
        }
    )
    audit = _native_recursive_projection_partition_contract_audit_from_summaries(
        full_summary,
        [first_partition, second_partition],
        partitions=[["alpha", "beta"], ["beta", "delta"]],
    )
    assert audit["stable"] is False
    assert audit["coverage_exact"] is False
    assert audit["missing_partition_columns"] == ["gamma"]
    assert audit["unknown_partition_columns"] == ["delta"]
    assert audit["duplicate_partition_columns"] == ["beta"]
    assert audit["partition_audits_stable"] is False
    assert audit["field_fingerprint_matches_full"] is False
    assert audit["leaf_contract_fingerprint_matches_full"] is False
    assert audit["root_contract_fingerprint_matches_full"] is False
    assert any(("duplicate columns" in item for item in audit["mismatches"]))
    assert any(("do not cover full layout" in item for item in audit["mismatches"]))
    assert any(("absent from full layout" in item for item in audit["mismatches"]))
    assert any(("partition[0] audit is not stable" in item for item in audit["mismatches"]))


def test_native_recursive_projection_coverage_contract_audit_allows_partial_overlaps() -> None:
    """Verify native recursive projection coverage contract audit allows partial overlaps."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.projection.audits.coverage import (
        _native_recursive_projection_coverage_contract_audit_from_summaries,
    )

    field = recursive_projection_field
    alpha = field("alpha", "list", "list.element.value", 1)
    beta = field("beta", "map", "entries.value", 1)
    gamma = field("gamma", "struct", "payload.value", 0)
    delta = field("delta", "list", "list.element.items.list.element.value", 2)
    full_summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 1,
            "row_groups": [
                {
                    "native_recursive_output_layout": {
                        "decoded": 1,
                        "fields": [alpha, beta, gamma, delta],
                    }
                }
            ],
        }
    )
    first_projection = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 1,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [gamma, alpha]}}
            ],
        }
    )
    second_projection = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 1,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [alpha, beta]}}
            ],
        }
    )
    audit = _native_recursive_projection_coverage_contract_audit_from_summaries(
        full_summary,
        [first_projection, second_projection],
        projections=[["gamma", "alpha"], ["alpha", "beta"]],
        require_full_coverage=False,
        allow_overlaps=True,
    )
    assert audit["stable"] is True
    assert audit["mismatches"] == []
    assert audit["coverage_complete"] is False
    assert audit["coverage_partial"] is True
    assert audit["coverage_exact_by_set"] is False
    assert audit["uncovered_full_columns"] == ["delta"]
    assert audit["overlapping_projection_columns"] == ["alpha"]
    assert audit["overlap_counts_by_name"] == {"alpha": 2}
    assert audit["unknown_projection_columns"] == []
    assert audit["projection_audits_stable"] is True
    assert audit["field_contracts_consistent"] is True
    assert audit["leaf_contracts_consistent"] is True
    assert audit["root_contracts_consistent"] is True
    assert audit["covered_root_contract_fingerprints_by_name"] == {
        name: full_summary["root_contract_fingerprints_by_name"][name]
        for name in ["alpha", "beta", "gamma"]
    }


def test_native_recursive_projection_coverage_contract_audit_enforces_requested_guards() -> None:
    """Verify native recursive projection coverage contract audit enforces requested guards."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.projection.audits.coverage import (
        _native_recursive_projection_coverage_contract_audit_from_summaries,
    )

    field = list_projection_field
    alpha = field("alpha", "list.element.value", 3)
    beta = field("beta", "list.element.value", 3)
    beta_drifted = field("beta", "list.element.changed", 4)
    gamma = field("gamma", "list.element.value", 3)
    delta = field("delta", "list.element.value", 3)
    full_summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 1,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [alpha, beta, gamma]}}
            ],
        }
    )
    first_projection = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 1,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [alpha, beta_drifted]}}
            ],
        }
    )
    second_projection = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 1,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [beta_drifted, delta]}}
            ],
        }
    )
    audit = _native_recursive_projection_coverage_contract_audit_from_summaries(
        full_summary,
        [first_projection, second_projection],
        projections=[["alpha", "beta"], ["beta", "delta"]],
        require_full_coverage=True,
        allow_overlaps=False,
    )
    assert audit["stable"] is False
    assert audit["coverage_complete"] is False
    assert audit["uncovered_full_columns"] == ["gamma"]
    assert audit["unknown_projection_columns"] == ["delta"]
    assert audit["overlapping_projection_columns"] == ["beta"]
    assert audit["projection_audits_stable"] is False
    assert audit["field_contracts_consistent"] is False
    assert audit["leaf_contracts_consistent"] is False
    assert audit["root_contracts_consistent"] is False
    assert audit["root_contract_consistency_by_name"] == {"alpha": True, "beta": False}
    assert any(("does not cover full layout" in item for item in audit["mismatches"]))
    assert any(("overlapping columns" in item for item in audit["mismatches"]))
    assert any(("absent from full layout" in item for item in audit["mismatches"]))
    assert any(("root contract drifted" in item for item in audit["mismatches"]))


def test_native_recursive_projection_contract_audit_matches_reordered_root_subset() -> None:
    """Verify native recursive projection contract audit matches reordered root subset."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.projection.audits.subset import (
        _native_recursive_projection_contract_audit_from_summaries,
    )

    def field(name: str, root_kind: str, leaf_suffix: str, rep_depth: int) -> dict[str, object]:
        """Build the projected recursive field descriptor used by the audit."""
        components = [name, *leaf_suffix.split(".")]
        repeated_components = [[name, "list"]] if rep_depth else []
        return {
            "name": name,
            "root_kind": root_kind,
            "structural_shape_signature": f"{root_kind}<payload:int64>",
            "shape_signature": f"{root_kind}<payload:#0:int64>",
            "leaf_paths": [f"{name}.{leaf_suffix}"],
            "leaf_path_components": [components],
            "repeated_node_paths": [f"{name}.list"] if rep_depth else [],
            "repeated_node_path_components": repeated_components,
            "leaf_max_definition_levels": [2 + rep_depth],
            "leaf_max_repetition_levels": [rep_depth],
            "leaf_path_definition_levels": [[0, 1, 2 + rep_depth]],
            "leaf_path_repetition_levels": [[0, *[1] * rep_depth]],
            "leaf_count": 1,
            "node_count": 2 + rep_depth,
            "repetition_depth": rep_depth,
            "max_node_depth": 2 + rep_depth,
            "max_child_count": 1,
        }

    alpha = field("alpha", "list", "list.element.value", 1)
    beta = field("beta", "map", "entries.value", 1)
    gamma = field("gamma", "struct", "payload.value", 0)
    full_summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 2,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [alpha, beta, gamma]}},
                {"native_recursive_output_layout": {"decoded": 1, "fields": [alpha, beta, gamma]}},
            ],
        }
    )
    projected_summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 2,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [gamma, alpha]}},
                {"native_recursive_output_layout": {"decoded": 1, "fields": [gamma, alpha]}},
            ],
        }
    )
    audit = _native_recursive_projection_contract_audit_from_summaries(
        full_summary, projected_summary, columns=["gamma", "alpha"]
    )
    assert full_summary is not None
    assert projected_summary is not None
    assert audit["stable"] is True
    assert audit["mismatches"] == []
    assert audit["expected_columns"] == ["gamma", "alpha"]
    assert audit["projected_field_order"] == ["gamma", "alpha"]
    assert audit["projection_order_matches"] is True
    assert audit["missing_source_columns"] == []
    assert audit["missing_projected_columns"] == []
    assert audit["unexpected_projected_columns"] == []
    assert audit["field_fingerprint_matches_by_name"] == {"gamma": True, "alpha": True}
    assert audit["leaf_contract_matches_by_name"] == {"gamma": True, "alpha": True}
    assert audit["root_contract_matches_by_name"] == {"gamma": True, "alpha": True}
    assert audit["expected_root_contract_fingerprints_by_name"] == {
        name: full_summary["root_contract_fingerprints_by_name"][name]
        for name in ["alpha", "gamma"]
    }
    assert audit["projected_root_contract_fingerprints_by_name"] == {
        name: projected_summary["root_contract_fingerprints_by_name"][name]
        for name in ["alpha", "gamma"]
    }
    assert (
        audit["canonical_expected_root_contract_fingerprint"]
        == audit["canonical_actual_root_contract_fingerprint"]
    )
    assert audit["expected_projection_root_contract_fingerprint"].startswith("gamma=")


def test_native_recursive_projection_contract_audit_detects_drift_and_column_mismatch() -> None:
    """Verify native recursive projection contract audit detects drift and column mismatch."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.projection.audits.subset import (
        _native_recursive_projection_contract_audit_from_summaries,
    )

    def field(name: str, leaf_path: str, max_def: int, max_rep: int) -> dict[str, object]:
        """Build the projected recursive field descriptor used by the audit."""
        return {
            "name": name,
            "root_kind": "list",
            "structural_shape_signature": f"list<struct<{leaf_path}:int64>>",
            "shape_signature": f"list<struct<{leaf_path}:#0:int64>>",
            "leaf_paths": [f"{name}.{leaf_path}"],
            "leaf_path_components": [[name, *leaf_path.split(".")]],
            "repeated_node_paths": [f"{name}.list"],
            "repeated_node_path_components": [[name, "list"]],
            "leaf_max_definition_levels": [max_def],
            "leaf_max_repetition_levels": [max_rep],
            "leaf_path_definition_levels": [[0, 1, max_def]],
            "leaf_path_repetition_levels": [[0, max_rep, max_rep]],
            "leaf_count": 1,
            "node_count": 3,
            "repetition_depth": 1,
            "max_node_depth": 2,
            "max_child_count": 1,
        }

    alpha = field("alpha", "list.element.value", 3, 1)
    beta = field("beta", "list.element.value", 3, 1)
    beta_drifted = field("beta", "list.element.changed", 4, 1)
    gamma = field("gamma", "list.element.extra", 3, 1)
    full_summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 1,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [alpha, beta]}}
            ],
        }
    )
    projected_summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 1,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [beta_drifted, gamma]}}
            ],
        }
    )
    audit = _native_recursive_projection_contract_audit_from_summaries(
        full_summary, projected_summary, columns=["beta", "alpha"]
    )
    assert audit["stable"] is False
    assert audit["projected_field_order"] == ["beta", "gamma"]
    assert audit["projection_order_matches"] is False
    assert audit["missing_projected_columns"] == ["alpha"]
    assert audit["unexpected_projected_columns"] == ["gamma"]
    assert audit["field_fingerprint_matches_by_name"] == {"beta": False}
    assert audit["leaf_contract_matches_by_name"] == {"beta": False}
    assert audit["root_contract_matches_by_name"] == {"beta": False}
    assert (
        audit["canonical_expected_root_contract_fingerprint"]
        != audit["canonical_actual_root_contract_fingerprint"]
    )
    assert any(("field order" in item for item in audit["mismatches"]))
    assert any(("omitted requested columns" in item for item in audit["mismatches"]))
    assert any(("returned unexpected columns" in item for item in audit["mismatches"]))
    assert any(("root contract drifted" in item for item in audit["mismatches"]))


def test_native_recursive_projection_chain_contract_audit_composes_subprojections() -> None:
    """Verify native recursive projection chain contract audit composes subprojections."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.projection.audits.composition import (
        _native_recursive_projection_chain_contract_audit_from_summaries,
    )

    field = recursive_projection_field
    alpha = field("alpha", "list", "list.element.value", 1)
    beta = field("beta", "map", "entries.value", 1)
    gamma = field("gamma", "struct", "payload.value", 0)
    delta = field("delta", "list", "list.element.items.list.element.value", 2)
    full_summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 2,
            "row_groups": [
                {
                    "native_recursive_output_layout": {
                        "decoded": 1,
                        "fields": [alpha, beta, gamma, delta],
                    }
                },
                {
                    "native_recursive_output_layout": {
                        "decoded": 1,
                        "fields": [alpha, beta, gamma, delta],
                    }
                },
            ],
        }
    )
    source_summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 2,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [delta, beta, alpha]}},
                {"native_recursive_output_layout": {"decoded": 1, "fields": [delta, beta, alpha]}},
            ],
        }
    )
    projected_summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 2,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [beta, alpha]}},
                {"native_recursive_output_layout": {"decoded": 1, "fields": [beta, alpha]}},
            ],
        }
    )
    audit = _native_recursive_projection_chain_contract_audit_from_summaries(
        full_summary,
        source_summary,
        projected_summary,
        source_columns=["delta", "beta", "alpha"],
        columns=["beta", "alpha"],
    )
    assert audit["stable"] is True
    assert audit["mismatches"] == []
    assert audit["projected_columns_subset_of_source"] is True
    assert audit["projected_columns_missing_from_source"] == []
    assert audit["full_to_source"]["stable"] is True
    assert audit["source_to_projected"]["stable"] is True
    assert audit["full_to_projected"]["stable"] is True
    assert audit["direct_vs_chained_root_contract_fingerprint_matches"] is True
    assert audit["direct_vs_chained_leaf_contract_fingerprint_matches"] is True
    assert audit["direct_vs_chained_field_fingerprint_matches"] is True
    assert audit["root_contract_transitive_matches_by_name"] == {"beta": True, "alpha": True}
    assert audit["leaf_contract_transitive_matches_by_name"] == {"beta": True, "alpha": True}
    assert audit["field_fingerprint_transitive_matches_by_name"] == {"beta": True, "alpha": True}


def test_native_recursive_projection_chain_contract_audit_detects_non_subset_and_drift() -> None:
    """Verify native recursive projection chain contract audit detects non subset and drift."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.projection.audits.composition import (
        _native_recursive_projection_chain_contract_audit_from_summaries,
    )

    field = list_projection_field
    alpha = field("alpha", "list.element.value", 3)
    beta = field("beta", "list.element.value", 3)
    beta_drifted = field("beta", "list.element.changed", 4)
    gamma = field("gamma", "list.element.value", 3)
    full_summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 1,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [alpha, beta, gamma]}}
            ],
        }
    )
    source_summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 1,
            "row_groups": [{"native_recursive_output_layout": {"decoded": 1, "fields": [alpha]}}],
        }
    )
    projected_summary = _native_recursive_layout_summary_from_footer_info(
        {
            "native_reader_ready": 1,
            "row_group_count": 1,
            "row_groups": [
                {"native_recursive_output_layout": {"decoded": 1, "fields": [beta_drifted]}}
            ],
        }
    )
    audit = _native_recursive_projection_chain_contract_audit_from_summaries(
        full_summary, source_summary, projected_summary, source_columns=["alpha"], columns=["beta"]
    )
    assert audit["stable"] is False
    assert audit["projected_columns_subset_of_source"] is False
    assert audit["projected_columns_missing_from_source"] == ["beta"]
    assert audit["source_to_projected"]["stable"] is False
    assert audit["full_to_projected"]["stable"] is False
    assert audit["root_contract_transitive_matches_by_name"] == {"beta": False}
    assert audit["leaf_contract_transitive_matches_by_name"] == {"beta": False}
    assert audit["field_fingerprint_transitive_matches_by_name"] == {"beta": False}
    assert any(("not a subset" in item for item in audit["mismatches"]))
    assert any(
        ("root_contract_fingerprints_by_name drifted" in item for item in audit["mismatches"])
    )


RECURSIVE_LAYOUT_CASES = tuple(
    (name.removeprefix("test_"), value)
    for name, value in globals().items()
    if name.startswith("test_") and callable(value)
)


def test_parquet_contract_certification_status_certifies_native_writer_contract() -> None:
    """Verify Parquet contract certification status certifies native writer contract."""
    from schema_sanitizer.adapters.parquet.contract_gates import (
        _native_parquet_writer_contract_status_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_contract_certification_status_from_parts,
        _parquet_preflight_contract_status_from_writer_status,
    )

    writer_status = _native_parquet_writer_contract_status_from_footer_info(
        _stable_native_writer_footer_info(), native_stream_available=True
    )
    preflight_status = _parquet_preflight_contract_status_from_writer_status(
        writer_status, pyarrow_available=False
    )
    certificate = _parquet_contract_certification_status_from_parts(
        preflight_status=preflight_status, writer_status=writer_status
    )
    assert certificate["satisfied"] is True
    assert certificate["route"] == "native_parquet_stream"
    assert certificate["pipeline_safe_with_fallback"] is True
    assert certificate["native_writer_contract_applicable"] is True
    assert certificate["native_writer_contract_satisfied"] is True
    assert certificate["nested_contract_applicable"] is True
    assert certificate["nested_contract_satisfied"] is True
    assert certificate["safe_fallback_contract_satisfied"] is False
    assert certificate["issues"] == []


def test_parquet_contract_certification_status_certifies_external_pyarrow_fallback() -> None:
    """Verify Parquet contract certification status certifies external PyArrow fallback."""
    from schema_sanitizer.adapters.parquet.contract_gates import (
        _native_parquet_writer_contract_status_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_contract_certification_status_from_parts,
        _parquet_preflight_contract_status_from_writer_status,
    )

    info = _stable_native_writer_footer_info()
    info["created_by"] = "spark-3.x"
    writer_status = _native_parquet_writer_contract_status_from_footer_info(
        info, native_stream_available=True
    )
    preflight_status = _parquet_preflight_contract_status_from_writer_status(
        writer_status, pyarrow_available=True
    )
    certificate = _parquet_contract_certification_status_from_parts(
        preflight_status=preflight_status, writer_status=writer_status
    )
    assert certificate["satisfied"] is True
    assert certificate["route"] == "pyarrow_fallback_available"
    assert certificate["pipeline_safe_with_fallback"] is True
    assert certificate["native_writer_contract_applicable"] is False
    assert certificate["native_writer_contract_satisfied"] is False
    assert certificate["safe_fallback_contract_satisfied"] is True
    assert certificate["issues"] == []


def test_parquet_contract_certification_status_fails_native_nested_drift() -> None:
    """Verify Parquet contract certification status fails native nested drift."""
    from schema_sanitizer.adapters.parquet.contract_gates import (
        _native_parquet_writer_contract_status_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_contract_certification_status_from_parts,
        _parquet_preflight_contract_status_from_writer_status,
    )

    info = _stable_native_writer_footer_info()
    second_layout = info["row_groups"][1]["native_recursive_output_layout"]
    second_layout["fields"][0]["leaf_path_repetition_levels"] = [[0, 1, 1, 1, 1, 1]]
    writer_status = _native_parquet_writer_contract_status_from_footer_info(
        info, native_stream_available=True
    )
    preflight_status = _parquet_preflight_contract_status_from_writer_status(
        writer_status, pyarrow_available=True
    )
    certificate = _parquet_contract_certification_status_from_parts(
        preflight_status=preflight_status, writer_status=writer_status
    )
    assert certificate["satisfied"] is False
    assert certificate["pipeline_safe_with_fallback"] is True
    assert certificate["native_writer_contract_applicable"] is True
    assert certificate["native_writer_contract_satisfied"] is False
    assert certificate["nested_contract_applicable"] is True
    assert certificate["nested_contract_satisfied"] is False
    assert any(("native-writer:" in issue for issue in certificate["issues"]))
    assert any(("nested" in issue for issue in certificate["issues"]))


def test_parquet_contract_certification_status_fails_projection_drift() -> None:
    """Verify Parquet contract certification status fails projection drift."""
    from schema_sanitizer.adapters.parquet.contract_gates import (
        _native_parquet_writer_contract_status_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_contract_certification_status_from_parts,
        _parquet_preflight_contract_status_from_writer_status,
    )

    writer_status = _native_parquet_writer_contract_status_from_footer_info(
        _stable_native_writer_footer_info(), native_stream_available=True
    )
    preflight_status = _parquet_preflight_contract_status_from_writer_status(
        writer_status, pyarrow_available=False
    )
    certificate = _parquet_contract_certification_status_from_parts(
        preflight_status=preflight_status,
        writer_status=writer_status,
        projection_audit={"stable": False, "mismatches": ["root contract drifted"]},
    )
    assert certificate["satisfied"] is False
    assert certificate["projection_contract_applicable"] is True
    assert certificate["projection_contract_satisfied"] is False
    assert "projection: root contract drifted" in certificate["issues"]


def test_parquet_contract_certification_status_public_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify Parquet contract certification status public gate."""
    from schema_sanitizer.adapters.parquet import status as parquet_runtime
    from schema_sanitizer.adapters.parquet.contract_gates import (
        _native_parquet_writer_contract_status_from_footer_info,
    )

    writer_status = _native_parquet_writer_contract_status_from_footer_info(
        _stable_native_writer_footer_info(), native_stream_available=True
    )
    monkeypatch.setattr(
        parquet_runtime,
        "native_parquet_writer_contract_status",
        lambda *args, **kwargs: writer_status,
    )
    monkeypatch.setattr(parquet_runtime, "pyarrow_importable", lambda: False)
    monkeypatch.setattr(
        parquet_runtime,
        "native_parquet_recursive_projection_coverage_contract_audit",
        lambda *args, **kwargs: {"stable": True, "mismatches": []},
    )
    certificate = parquet_runtime.parquet_contract_certification_status(
        "writer-native.parquet",
        projections=[["payload"]],
        require_full_projection_coverage=True,
        allow_projection_overlaps=False,
    )
    assert certificate["satisfied"] is True
    assert certificate["route"] == "native_parquet_stream"
    assert certificate["projection_contract_applicable"] is True
    assert certificate["projection_contract_satisfied"] is True


def test_native_parquet_writer_contract_status_enforces_runtime_batch_size() -> None:
    """Verify native Parquet writer contract status enforces runtime batch size."""
    from schema_sanitizer.adapters.parquet.contract_gates import (
        _native_parquet_writer_contract_status_from_footer_info,
    )

    info = _stable_native_writer_footer_info()
    info["row_groups"][0]["num_rows"] = 3
    info["row_groups"][1]["num_rows"] = 1
    blocked = _native_parquet_writer_contract_status_from_footer_info(
        info, native_stream_available=True, batch_size=2
    )
    allowed = _native_parquet_writer_contract_status_from_footer_info(
        info, native_stream_available=True, batch_size=3
    )
    assert blocked["applicable"] is True
    assert blocked["satisfied"] is False
    assert blocked["batch_size"] == 2
    assert blocked["max_row_group_rows"] == 3
    assert blocked["batch_size_contract_satisfied"] is False
    assert any(("batch-size contract" in issue for issue in blocked["issues"]))
    assert allowed["satisfied"] is True
    assert allowed["batch_size_contract_satisfied"] is True


def test_parquet_contract_certification_status_fails_native_batch_size_contract() -> None:
    """Verify Parquet contract certification status fails native batch size contract."""
    from schema_sanitizer.adapters.parquet.contract_gates import (
        _native_parquet_writer_contract_status_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_contract_certification_status_from_parts,
        _parquet_preflight_contract_status_from_writer_status,
    )

    info = _stable_native_writer_footer_info()
    info["row_groups"][0]["num_rows"] = 4
    info["row_groups"][1]["num_rows"] = 1
    writer_status = _native_parquet_writer_contract_status_from_footer_info(
        info, native_stream_available=True, batch_size=2
    )
    preflight_status = _parquet_preflight_contract_status_from_writer_status(
        writer_status, pyarrow_available=True
    )
    certificate = _parquet_contract_certification_status_from_parts(
        preflight_status=preflight_status, writer_status=writer_status
    )
    assert preflight_status["satisfied"] is True
    assert preflight_status["route"] == "pyarrow_fallback_available"
    assert certificate["satisfied"] is False
    assert certificate["pipeline_safe_with_fallback"] is True
    assert certificate["native_writer_contract_applicable"] is True
    assert certificate["native_writer_contract_satisfied"] is False
    assert certificate["safe_fallback_contract_satisfied"] is True
    assert certificate["batch_size"] == 2
    assert certificate["max_row_group_rows"] == 4
    assert certificate["batch_size_contract_satisfied"] is False
    assert any(("batch-size contract" in issue for issue in certificate["issues"]))


def test_parquet_preflight_contract_status_passes_batch_size_to_writer_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Parquet preflight contract status passes batch size to writer gate."""
    from schema_sanitizer.adapters.parquet import status as parquet_runtime

    captured: dict[str, object] = {}

    def fake_writer_status(*args: object, **kwargs: object) -> dict[str, object]:
        """Capture writer-gate arguments and return an accepted status."""
        captured.update(kwargs)
        return {
            "applicable": True,
            "satisfied": False,
            "issues": ["native reader batch-size contract: too small"],
            "created_by": "schema-sanitizer native parquet writer",
            "native_reader_ready": True,
            "nested_contract_applicable": True,
            "nested_contract_satisfied": True,
        }

    monkeypatch.setattr(
        parquet_runtime, "native_parquet_writer_contract_status", fake_writer_status
    )
    monkeypatch.setattr(parquet_runtime, "pyarrow_importable", lambda: True)
    status = parquet_runtime.parquet_preflight_contract_status(
        "native.parquet", columns=["payload"], batch_size=128
    )
    assert captured["columns"] == ["payload"]
    assert captured["batch_size"] == 128
    assert status["satisfied"] is True
    assert status["route"] == "pyarrow_fallback_available"
    assert status["native_writer_contract_satisfied"] is False
    assert status["safe_fallback_contract_satisfied"] is True


def test_native_parquet_writer_contract_status_rejects_runtime_filters() -> None:
    """Verify native Parquet writer contract status rejects runtime filters."""
    from schema_sanitizer.adapters.parquet.contract_gates import (
        _native_parquet_writer_contract_status_from_footer_info,
    )

    filtered = _native_parquet_writer_contract_status_from_footer_info(
        _stable_native_writer_footer_info(), native_stream_available=True, filters=object()
    )
    unfiltered = _native_parquet_writer_contract_status_from_footer_info(
        _stable_native_writer_footer_info(), native_stream_available=True, filters=None
    )
    assert filtered["applicable"] is True
    assert filtered["satisfied"] is False
    assert filtered["filters_present"] is True
    assert filtered["filter_contract_satisfied"] is False
    assert any(("filter contract" in issue for issue in filtered["issues"]))
    assert unfiltered["satisfied"] is True
    assert unfiltered["filter_contract_satisfied"] is True


def test_parquet_contract_certification_status_fails_native_filter_contract() -> None:
    """Verify Parquet contract certification status fails native filter contract."""
    from schema_sanitizer.adapters.parquet.contract_gates import (
        _native_parquet_writer_contract_status_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_contract_certification_status_from_parts,
        _parquet_preflight_contract_status_from_writer_status,
    )

    writer_status = _native_parquet_writer_contract_status_from_footer_info(
        _stable_native_writer_footer_info(), native_stream_available=True, filters=object()
    )
    preflight_status = _parquet_preflight_contract_status_from_writer_status(
        writer_status, pyarrow_available=True
    )
    certificate = _parquet_contract_certification_status_from_parts(
        preflight_status=preflight_status, writer_status=writer_status
    )
    assert preflight_status["satisfied"] is True
    assert preflight_status["route"] == "pyarrow_fallback_available"
    assert preflight_status["filters_present"] is True
    assert preflight_status["filter_contract_satisfied"] is False
    assert certificate["satisfied"] is False
    assert certificate["pipeline_safe_with_fallback"] is True
    assert certificate["native_writer_contract_applicable"] is True
    assert certificate["native_writer_contract_satisfied"] is False
    assert certificate["safe_fallback_contract_satisfied"] is True
    assert certificate["filters_present"] is True
    assert certificate["filter_contract_satisfied"] is False
    assert any(("filter contract" in issue for issue in certificate["issues"]))


def test_parquet_preflight_contract_status_passes_filters_to_writer_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Parquet preflight contract status passes filters to writer gate."""
    from schema_sanitizer.adapters.parquet import status as parquet_runtime

    captured: dict[str, object] = {}
    sentinel_filter = object()
    fake_writer_status = partial(filter_rejecting_writer_status, captured)
    monkeypatch.setattr(
        parquet_runtime, "native_parquet_writer_contract_status", fake_writer_status
    )
    monkeypatch.setattr(parquet_runtime, "pyarrow_importable", lambda: True)
    status = parquet_runtime.parquet_preflight_contract_status(
        "native.parquet", columns=["payload"], batch_size=128, filters=sentinel_filter
    )
    assert captured["columns"] == ["payload"]
    assert captured["batch_size"] == 128
    assert captured["filters"] is sentinel_filter
    assert status["satisfied"] is True
    assert status["route"] == "pyarrow_fallback_available"
    assert status["native_writer_contract_satisfied"] is False
    assert status["safe_fallback_contract_satisfied"] is True
    assert status["filters_present"] is True
    assert status["filter_contract_satisfied"] is False


def test_parquet_contract_certification_status_passes_filters_to_writer_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Parquet contract certification status passes filters to writer gate."""
    from schema_sanitizer.adapters.parquet import status as parquet_runtime

    captured: dict[str, object] = {}
    sentinel_filter = object()
    fake_writer_status = partial(filter_rejecting_writer_status, captured)
    monkeypatch.setattr(
        parquet_runtime, "native_parquet_writer_contract_status", fake_writer_status
    )
    monkeypatch.setattr(parquet_runtime, "pyarrow_importable", lambda: True)
    certificate = parquet_runtime.parquet_contract_certification_status(
        "native.parquet", columns=["payload"], batch_size=128, filters=sentinel_filter
    )
    assert captured["filters"] is sentinel_filter
    assert certificate["pipeline_safe_with_fallback"] is True
    assert certificate["native_writer_contract_applicable"] is True
    assert certificate["native_writer_contract_satisfied"] is False
    assert certificate["safe_fallback_contract_satisfied"] is True
    assert certificate["filters_present"] is True
    assert certificate["filter_contract_satisfied"] is False
    assert certificate["satisfied"] is False


def test_parquet_contract_runtime_readiness_status_from_capabilities_accepts_full_runtime() -> None:
    """Verify Parquet contract runtime readiness status from capabilities accepts full runtime."""
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_contract_runtime_readiness_status_from_capabilities,
    )

    status = _parquet_contract_runtime_readiness_status_from_capabilities(
        pyarrow_available=True, native_footer_available=True, native_stream_available=True
    )
    assert status["satisfied"] is True
    assert status["issues"] == []
    assert status["safe_fallback_runtime_available"] is True
    assert status["native_reader_runtime_available"] is True
    assert status["schema_sanitizer_native_contracts_gateable"] is True
    assert status["nested_native_contracts_gateable"] is True


def test_parquet_contract_runtime_readiness_status_fails_without_pyarrow() -> None:
    """Verify Parquet contract runtime readiness status fails without PyArrow."""
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_contract_runtime_readiness_status_from_capabilities,
    )

    status = _parquet_contract_runtime_readiness_status_from_capabilities(
        pyarrow_available=False,
        native_footer_available=True,
        native_stream_available=True,
        require_pyarrow=True,
    )
    assert status["satisfied"] is False
    assert status["safe_fallback_runtime_available"] is False
    assert any(("PyArrow is required" in issue for issue in status["issues"]))


def test_parquet_contract_runtime_readiness_status_fails_without_native_gates() -> None:
    """Verify Parquet contract runtime readiness status fails without native gates."""
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_contract_runtime_readiness_status_from_capabilities,
    )

    status = _parquet_contract_runtime_readiness_status_from_capabilities(
        pyarrow_available=True,
        native_footer_available=False,
        native_stream_available=False,
        require_native=True,
    )
    assert status["satisfied"] is False
    assert status["safe_fallback_runtime_available"] is True
    assert status["native_reader_runtime_available"] is False
    assert status["schema_sanitizer_native_contracts_gateable"] is False
    assert status["nested_native_contracts_gateable"] is False
    assert any(("footer diagnostics" in issue for issue in status["issues"]))
    assert any(("stream reader" in issue for issue in status["issues"]))


def test_parquet_contract_runtime_readiness_status_can_relax_native_requirement() -> None:
    """Verify Parquet contract runtime readiness status can relax native requirement."""
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_contract_runtime_readiness_status_from_capabilities,
    )

    status = _parquet_contract_runtime_readiness_status_from_capabilities(
        pyarrow_available=True,
        native_footer_available=False,
        native_stream_available=False,
        require_native=False,
    )
    assert status["satisfied"] is True
    assert status["safe_fallback_runtime_available"] is True
    assert status["native_reader_runtime_available"] is False


def test_parquet_contract_runtime_readiness_status_public_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Parquet contract runtime readiness status public gate."""
    from schema_sanitizer.adapters.parquet import status as parquet_runtime

    monkeypatch.setattr(parquet_runtime, "pyarrow_importable", lambda: True)
    status = parquet_runtime.parquet_contract_runtime_readiness_status()
    assert status["satisfied"] is True
    assert status["pyarrow_available"] is True
    assert status["native_footer_available"] is True
    assert status["native_stream_available"] is True


def test_runtime_parquet_gate_snapshots_keep_inputs_defensive() -> None:
    """Verify runtime Parquet gate snapshots keep inputs defensive."""
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_contract_certification_status_from_parts,
        _parquet_preflight_contract_status_from_writer_status,
    )

    writer = {
        "applicable": False,
        "satisfied": False,
        "issues": ["external writer"],
        "nested_contract_issues": ["not applicable"],
    }
    preflight = _parquet_preflight_contract_status_from_writer_status(
        writer, pyarrow_available=True
    )
    projection = {"stable": False, "mismatches": ["drift"]}
    certificate = _parquet_contract_certification_status_from_parts(
        preflight_status=preflight, writer_status=writer, projection_audit=projection
    )
    certificate["preflight_status"]["issues"].append("caller mutation")
    certificate["native_writer_status"]["issues"].append("caller mutation")
    certificate["projection_audit"]["mismatches"].append("caller mutation")
    assert preflight["issues"] == []
    assert writer["issues"] == ["external writer"]
    assert projection["mismatches"] == ["drift"]


def test_native_nested_contract_status_certifies_stable_recursive_summary() -> None:
    """Verify native nested contract status certifies stable recursive summary."""
    from schema_sanitizer.adapters.parquet.contract_gates import (
        _native_nested_contract_status_from_summary,
    )

    status = _native_nested_contract_status_from_summary(_stable_native_nested_contract_summary())
    assert status["applicable"] is True
    assert status["satisfied"] is True
    assert status["issues"] == []
    assert status["row_group_count"] == 2
    assert status["decoded_row_group_count"] == 2
    assert status["field_count"] == 1
    assert status["canonical_layout_fingerprint"] == "payload=field-fp"
    assert status["canonical_leaf_contract_fingerprint"] == "payload=leaf-fp"
    assert status["canonical_root_contract_fingerprint"] == "payload=root-fp"


def test_native_nested_contract_status_fails_closed_on_recursive_drift() -> None:
    """Verify native nested contract status fails closed on recursive drift."""
    from schema_sanitizer.adapters.parquet.contract_gates import (
        _native_nested_contract_status_from_summary,
    )

    summary = _stable_native_nested_contract_summary()
    summary.update(
        {
            "decoded_row_group_count": 1,
            "stable_across_row_groups": False,
            "mismatches": ["root-contract drifted"],
            "row_group_leaf_contract_fingerprints_stable": False,
            "leaf_path_collisions": [
                {"leaf_path": "payload.list.element", "first_field": "a", "other_field": "b"}
            ],
        }
    )
    status = _native_nested_contract_status_from_summary(summary)
    assert status["applicable"] is True
    assert status["satisfied"] is False
    assert any(("decoded row-group count" in issue for issue in status["issues"]))
    assert "recursive layout is not stable across row groups" in status["issues"]
    assert "root-contract drifted" in status["issues"]
    assert "leaf contract fingerprints drifted" in status["issues"]
    assert "leaf path ownership collisions detected" in status["issues"]


def test_native_parquet_nested_contract_status_uses_public_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify native Parquet nested contract status uses public summary."""
    from schema_sanitizer.adapters.parquet import status as parquet_footer

    monkeypatch.setattr(
        parquet_footer,
        "native_parquet_recursive_layout_summary",
        lambda *args, **kwargs: _stable_native_nested_contract_summary(),
    )
    status = parquet_footer.native_parquet_nested_contract_status(
        "nested.parquet", columns=["payload"]
    )
    assert status["applicable"] is True
    assert status["satisfied"] is True
    assert status["field_order"] == ["payload"]


def test_parquet_fallback_failure_marks_pipeline_contract_failed() -> None:
    """Verify Parquet fallback failure marks pipeline contract failed."""
    from schema_sanitizer.adapters.parquet import telemetry as observability

    observability.reset_parquet_stream_factory_observability()
    recording.set_parquet_native_reader_diagnostics(
        attempted=True,
        ready=False,
        reason="not_ready",
        blockers=["external writer"],
        fallback_expected=True,
        fallback_route="pyarrow_dataset_scanner",
    )
    recording.record_parquet_fallback_attempt("pyarrow_dataset_scanner")
    recording.record_parquet_fallback_failure(
        "pyarrow_dataset_scanner", OSError("dataset scanner unavailable")
    )
    diagnostics = observability.last_parquet_native_reader_diagnostics()
    assert diagnostics["pipeline_contract_satisfied"] is False
    assert diagnostics["pipeline_contract_route"] == "pyarrow_dataset_scanner"
    assert diagnostics["pipeline_contract_error"] == "OSError: dataset scanner unavailable"
    assert diagnostics["safe_fallback_contract_satisfied"] is False
    assert diagnostics["fallback_succeeded"] is False


def test_last_parquet_pipeline_contract_status_certifies_native_success() -> None:
    """Verify last Parquet pipeline contract status certifies native success."""
    from schema_sanitizer.adapters.parquet import telemetry as observability

    observability.reset_parquet_stream_factory_observability()
    recording.set_parquet_native_reader_diagnostics(
        attempted=True,
        ready=True,
        reason="native_stream",
        fallback_expected=False,
        fallback_attempted=False,
        fallback_succeeded=False,
        pipeline_contract_satisfied=True,
        pipeline_contract_route="native_parquet_stream",
        pipeline_contract_error=None,
        native_reader_contract_satisfied=True,
        safe_fallback_contract_satisfied=False,
    )
    status = observability.last_parquet_pipeline_contract_status()
    assert status["satisfied"] is True
    assert status["route"] == "native_parquet_stream"
    assert status["issues"] == []
    assert status["native_reader_contract_satisfied"] is True
    assert status["safe_fallback_contract_satisfied"] is False


def test_last_parquet_pipeline_contract_status_certifies_safe_fallback_success() -> None:
    """Verify last Parquet pipeline contract status certifies safe fallback success."""
    from schema_sanitizer.adapters.parquet import telemetry as observability

    observability.reset_parquet_stream_factory_observability()
    recording.set_parquet_native_reader_diagnostics(
        attempted=True,
        ready=False,
        reason="not_ready",
        blockers=["external writer"],
        fallback_expected=True,
        fallback_route="pyarrow_dataset_scanner",
    )
    recording.record_parquet_fallback_attempt("pyarrow_dataset_scanner")
    recording.record_parquet_fallback_success("pyarrow_dataset_scanner")
    status = observability.last_parquet_pipeline_contract_status()
    assert status["satisfied"] is True
    assert status["route"] == "pyarrow_dataset_scanner"
    assert status["issues"] == []
    assert status["fallback_attempted"] is True
    assert status["fallback_succeeded"] is True
    assert status["safe_fallback_contract_satisfied"] is True


def test_last_parquet_pipeline_contract_status_fails_closed_on_inconsistent_state() -> None:
    """Verify last Parquet pipeline contract status fails closed on inconsistent state."""
    from schema_sanitizer.adapters.parquet import telemetry as observability

    observability.reset_parquet_stream_factory_observability()
    recording.set_parquet_native_reader_diagnostics(
        attempted=True,
        ready=False,
        reason="not_ready",
        fallback_expected=True,
        fallback_route="pyarrow_dataset_scanner",
        pipeline_contract_satisfied=True,
        pipeline_contract_route="pyarrow_dataset_scanner",
        safe_fallback_contract_satisfied=True,
        fallback_attempted=False,
        fallback_succeeded=True,
    )
    status = observability.last_parquet_pipeline_contract_status()
    assert status["satisfied"] is False
    assert "PyArrow fallback route did not record an attempt" in status["issues"]


def test_native_parquet_writer_contract_status_certifies_stable_native_nested_file() -> None:
    """Verify native Parquet writer contract status certifies stable native nested file."""
    from schema_sanitizer.adapters.parquet.contract_gates import (
        _native_parquet_writer_contract_status_from_footer_info,
    )

    status = _native_parquet_writer_contract_status_from_footer_info(
        _stable_native_writer_footer_info(), native_stream_available=True
    )
    assert status["applicable"] is True
    assert status["satisfied"] is True
    assert status["issues"] == []
    assert status["native_writer_detected"] is True
    assert status["native_reader_ready"] is True
    assert status["native_stream_available"] is True
    assert status["nested_contract_applicable"] is True
    assert status["nested_contract_satisfied"] is True
    assert status["canonical_leaf_contract_fingerprint"]
    assert status["canonical_root_contract_fingerprint"]


def test_native_parquet_writer_contract_status_fails_closed_on_missing_native_stream() -> None:
    """Verify native Parquet writer contract status fails closed on missing native stream."""
    from schema_sanitizer.adapters.parquet.contract_gates import (
        _native_parquet_writer_contract_status_from_footer_info,
    )

    status = _native_parquet_writer_contract_status_from_footer_info(
        _stable_native_writer_footer_info(), native_stream_available=False
    )
    assert status["applicable"] is True
    assert status["satisfied"] is False
    assert "native Parquet stream function is unavailable" in status["issues"]


def test_native_parquet_writer_contract_status_fails_closed_on_external_writer() -> None:
    """Verify native Parquet writer contract status fails closed on external writer."""
    from schema_sanitizer.adapters.parquet.contract_gates import (
        _native_parquet_writer_contract_status_from_footer_info,
    )

    info = _stable_native_writer_footer_info()
    info["created_by"] = "spark-3.x"
    status = _native_parquet_writer_contract_status_from_footer_info(
        info, native_stream_available=True
    )
    assert status["applicable"] is False
    assert status["satisfied"] is False
    assert any(("not created by schema-sanitizer" in issue for issue in status["issues"]))


def test_native_parquet_writer_contract_status_uses_public_footer_info(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify native Parquet writer contract status uses public footer info."""
    from schema_sanitizer.adapters.parquet import status as parquet_footer

    monkeypatch.setattr(
        parquet_footer,
        "native_parquet_footer_info",
        lambda *args, **kwargs: _stable_native_writer_footer_info(),
    )
    status = parquet_footer.native_parquet_writer_contract_status(
        "writer-native.parquet", columns=["payload"]
    )
    assert status["satisfied"] is True
    assert status["native_writer_detected"] is True
    assert status["nested_contract_satisfied"] is True


def test_parquet_preflight_contract_status_certifies_native_without_pyarrow() -> None:
    """Verify Parquet preflight contract status certifies native without PyArrow."""
    from schema_sanitizer.adapters.parquet.contract_gates import (
        _native_parquet_writer_contract_status_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_preflight_contract_status_from_writer_status,
    )

    writer_status = _native_parquet_writer_contract_status_from_footer_info(
        _stable_native_writer_footer_info(), native_stream_available=True
    )
    status = _parquet_preflight_contract_status_from_writer_status(
        writer_status, pyarrow_available=False
    )
    assert status["satisfied"] is True
    assert status["route"] == "native_parquet_stream"
    assert status["pyarrow_available"] is False
    assert status["native_writer_contract_satisfied"] is True
    assert status["safe_fallback_contract_satisfied"] is False
    assert status["nested_contract_satisfied"] is True
    assert status["issues"] == []


def test_parquet_preflight_contract_status_certifies_external_with_pyarrow() -> None:
    """Verify Parquet preflight contract status certifies external with PyArrow."""
    from schema_sanitizer.adapters.parquet.contract_gates import (
        _native_parquet_writer_contract_status_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_preflight_contract_status_from_writer_status,
    )

    info = _stable_native_writer_footer_info()
    info["created_by"] = "spark-3.x"
    writer_status = _native_parquet_writer_contract_status_from_footer_info(
        info, native_stream_available=True
    )
    status = _parquet_preflight_contract_status_from_writer_status(
        writer_status, pyarrow_available=True
    )
    assert status["satisfied"] is True
    assert status["route"] == "pyarrow_fallback_available"
    assert status["pyarrow_available"] is True
    assert status["native_writer_contract_satisfied"] is False
    assert status["safe_fallback_contract_satisfied"] is True
    assert any(
        ("not created by schema-sanitizer" in issue for issue in status["native_writer_issues"])
    )
    assert status["issues"] == []


def test_parquet_preflight_contract_status_fails_without_native_or_pyarrow() -> None:
    """Verify Parquet preflight contract status fails without native or PyArrow."""
    from schema_sanitizer.adapters.parquet.contract_gates import (
        _native_parquet_writer_contract_status_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.status import (
        _parquet_preflight_contract_status_from_writer_status,
    )

    info = _stable_native_writer_footer_info()
    info["created_by"] = "spark-3.x"
    writer_status = _native_parquet_writer_contract_status_from_footer_info(
        info, native_stream_available=True
    )
    status = _parquet_preflight_contract_status_from_writer_status(
        writer_status, pyarrow_available=False
    )
    assert status["satisfied"] is False
    assert status["route"] is None
    assert status["pyarrow_available"] is False
    assert status["native_writer_contract_satisfied"] is False
    assert status["safe_fallback_contract_satisfied"] is False
    assert any(("PyArrow is not installed" in issue for issue in status["issues"]))
    assert any(("not created by schema-sanitizer" in issue for issue in status["issues"]))


def test_parquet_preflight_contract_status_uses_public_writer_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Parquet preflight contract status uses public writer gate."""
    from schema_sanitizer.adapters.parquet import status as parquet_runtime

    monkeypatch.setattr(
        parquet_runtime,
        "native_parquet_writer_contract_status",
        lambda *args, **kwargs: {
            "applicable": False,
            "satisfied": False,
            "issues": ["external writer"],
            "created_by": "external",
            "native_reader_ready": False,
            "nested_contract_applicable": False,
            "nested_contract_satisfied": False,
        },
    )
    monkeypatch.setattr(parquet_runtime, "pyarrow_importable", lambda: True)
    status = parquet_runtime.parquet_preflight_contract_status("external.parquet")
    assert status["satisfied"] is True
    assert status["route"] == "pyarrow_fallback_available"
    assert status["safe_fallback_contract_satisfied"] is True


def test_native_writer_nested_contract_blockers_fail_closed_on_drift() -> None:
    """Verify native writer nested contract blockers fail closed on drift."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import (
        ParquetRecordBatchStreamFactory,
    )

    info = _stable_native_writer_footer_info()
    second_layout = info["row_groups"][1]["native_recursive_output_layout"]
    second_layout["fields"][0]["leaf_path_repetition_levels"] = [[0, 1, 1, 1, 1, 1]]
    blockers = ParquetRecordBatchStreamFactory._native_nested_contract_blockers(info)
    assert blockers
    assert any(("native nested contract" in blocker for blocker in blockers))
    assert any(("repetition" in blocker or "stable" in blocker for blocker in blockers))


PARQUET_CONTRACT_CASES = tuple(
    (name.removeprefix("test_"), value)
    for name, value in globals().items()
    if name.startswith("test_")
    and callable(value)
    and value not in {case for _case_name, case in RECURSIVE_LAYOUT_CASES}
)
