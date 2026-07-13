"""Core tests for recursive Parquet nested-layout contracts.

These tests exercise pure diagnostics and recursive corpus generators without
requiring PyArrow. Runtime materialization lives in focused native modules.
"""

from __future__ import annotations

# Split from test_parquet_recursive_projection_audits.py: test_native_recursive_projection_contract_audit_matches_reordered_root_subset, test_native_recursive_projection_contract_audit_detects_drift_and_column_mismatch, test_native_recursive_projection_chain_contract_audit_composes_subprojections, ...


def test_native_recursive_projection_contract_audit_matches_reordered_root_subset() -> None:
    """Verify projection audits compare root/leaf contracts against full layouts."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.projection.audits.subset import (
        _native_recursive_projection_contract_audit_from_summaries,
    )

    def field(name: str, root_kind: str, leaf_suffix: str, rep_depth: int) -> dict[str, object]:
        """Internal test helper."""
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
            "leaf_path_repetition_levels": [[0, *([1] * rep_depth)]],
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
        full_summary,
        projected_summary,
        columns=["gamma", "alpha"],
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
    """Verify projection audits fail closed on missing roots and contract drift."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.projection.audits.subset import (
        _native_recursive_projection_contract_audit_from_summaries,
    )

    def field(name: str, leaf_path: str, max_def: int, max_rep: int) -> dict[str, object]:
        """Internal test helper."""
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
        full_summary,
        projected_summary,
        columns=["beta", "alpha"],
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
    assert any("field order" in item for item in audit["mismatches"])
    assert any("omitted requested columns" in item for item in audit["mismatches"])
    assert any("returned unexpected columns" in item for item in audit["mismatches"])
    assert any("root contract drifted" in item for item in audit["mismatches"])


def test_native_recursive_projection_chain_contract_audit_composes_subprojections() -> None:
    """Verify nested root projection contracts are transitive through subprojections."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.projection.audits.composition import (
        _native_recursive_projection_chain_contract_audit_from_summaries,
    )

    def field(name: str, root_kind: str, leaf_suffix: str, rep_depth: int) -> dict[str, object]:
        """Internal test helper."""
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
    assert audit["root_contract_transitive_matches_by_name"] == {
        "beta": True,
        "alpha": True,
    }
    assert audit["leaf_contract_transitive_matches_by_name"] == {
        "beta": True,
        "alpha": True,
    }
    assert audit["field_fingerprint_transitive_matches_by_name"] == {
        "beta": True,
        "alpha": True,
    }


def test_native_recursive_projection_chain_contract_audit_detects_non_subset_and_drift() -> None:
    """Verify projection-chain audit fails closed on non-subsets and drift."""
    from schema_sanitizer.adapters.parquet.layout.reducer import (
        _native_recursive_layout_summary_from_footer_info,
    )
    from schema_sanitizer.adapters.parquet.projection.audits.composition import (
        _native_recursive_projection_chain_contract_audit_from_summaries,
    )

    def field(name: str, leaf_suffix: str, max_def: int) -> dict[str, object]:
        """Internal test helper."""
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
        full_summary,
        source_summary,
        projected_summary,
        source_columns=["alpha"],
        columns=["beta"],
    )

    assert audit["stable"] is False
    assert audit["projected_columns_subset_of_source"] is False
    assert audit["projected_columns_missing_from_source"] == ["beta"]
    assert audit["source_to_projected"]["stable"] is False
    assert audit["full_to_projected"]["stable"] is False
    assert audit["root_contract_transitive_matches_by_name"] == {"beta": False}
    assert audit["leaf_contract_transitive_matches_by_name"] == {"beta": False}
    assert audit["field_fingerprint_transitive_matches_by_name"] == {"beta": False}
    assert any("not a subset" in item for item in audit["mismatches"])
    assert any("root_contract_fingerprints_by_name drifted" in item for item in audit["mismatches"])
