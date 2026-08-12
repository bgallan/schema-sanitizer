"""Core tests for recursive Parquet nested-layout contracts.

These tests exercise pure diagnostics and recursive corpus generators without
requiring PyArrow. Runtime materialization lives in focused native modules.
"""

from __future__ import annotations

from _support.parquet_contracts import list_projection_field, recursive_projection_field


def test_native_recursive_projection_partition_contract_audit_recomposes_full_layout() -> None:
    """Verify disjoint projected roots recombine to the full recursive contract."""
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
    """Verify partition audits fail closed on incomplete or overlapping roots."""
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
    assert any("duplicate columns" in item for item in audit["mismatches"])
    assert any("do not cover full layout" in item for item in audit["mismatches"])
    assert any("absent from full layout" in item for item in audit["mismatches"])
    assert any("partition[0] audit is not stable" in item for item in audit["mismatches"])


def test_native_recursive_projection_coverage_contract_audit_allows_partial_overlaps() -> None:
    """Verify coverage audits can allow partial and overlapping nested reads."""
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
    """Verify coverage audits can fail closed for gaps, overlaps, and drift."""
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
    assert any("does not cover full layout" in item for item in audit["mismatches"])
    assert any("overlapping columns" in item for item in audit["mismatches"])
    assert any("absent from full layout" in item for item in audit["mismatches"])
    assert any("root contract drifted" in item for item in audit["mismatches"])
