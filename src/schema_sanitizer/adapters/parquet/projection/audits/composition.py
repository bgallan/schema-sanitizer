"""Projection contract audits for native Parquet recursive layouts.

The nested-layout summary is intentionally rich. These helpers compare those
summaries across projections, chained projections, partitions, and partial
coverage plans without pulling projection-audit code into the main runtime
adapter.
"""

from __future__ import annotations

from typing import Any

from .subset import _native_recursive_projection_contract_audit_from_summaries
from .summary import duplicate_names, note_mismatch, summary_dict


def _native_recursive_projection_chain_contract_audit_from_summaries(
    full_summary: dict[str, Any] | None,
    source_summary: dict[str, Any] | None,
    projected_summary: dict[str, Any] | None,
    *,
    source_columns: list[str] | tuple[str, ...],
    columns: list[str] | tuple[str, ...],
) -> dict[str, Any]:
    """Audit that recursive projections compose without changing contracts.

    A production reader should treat projection as a pure subset/reorder operation.
    For nested roots this should be transitive: planning ``full -> source`` and
    then ``source -> columns`` must preserve the same root, leaf, and field
    contracts as planning ``full -> columns`` directly. This helper exposes that
    invariant in a JSON-friendly report so deep list/map/struct projection bugs
    can be diagnosed without materializing the file.
    """

    source_order = [str(column) for column in source_columns]
    projected_order = [str(column) for column in columns]
    source_set = set(source_order)
    projected_set = set(projected_order)
    full_to_source = _native_recursive_projection_contract_audit_from_summaries(
        full_summary,
        source_summary,
        columns=source_order,
    )
    source_to_projected = _native_recursive_projection_contract_audit_from_summaries(
        source_summary,
        projected_summary,
        columns=projected_order,
    )
    full_to_projected = _native_recursive_projection_contract_audit_from_summaries(
        full_summary,
        projected_summary,
        columns=projected_order,
    )

    audit: dict[str, Any] = {
        "stable": True,
        "mismatches": [],
        "source_columns": source_order,
        "columns": projected_order,
        "duplicate_source_columns": duplicate_names(source_order),
        "duplicate_projected_columns": duplicate_names(projected_order),
        "projected_columns_subset_of_source": projected_set.issubset(source_set),
        "projected_columns_missing_from_source": sorted(projected_set - source_set),
        "full_to_source": full_to_source,
        "source_to_projected": source_to_projected,
        "full_to_projected": full_to_projected,
        "direct_vs_chained_root_contract_fingerprint_matches": False,
        "direct_vs_chained_leaf_contract_fingerprint_matches": False,
        "direct_vs_chained_field_fingerprint_matches": False,
        "root_contract_transitive_matches_by_name": {},
        "leaf_contract_transitive_matches_by_name": {},
        "field_fingerprint_transitive_matches_by_name": {},
    }

    if audit["duplicate_source_columns"]:
        note_mismatch(
            audit,
            "recursive projection chain source contains duplicate columns: "
            f"{audit['duplicate_source_columns']!r}",
        )
    if audit["duplicate_projected_columns"]:
        note_mismatch(
            audit,
            "recursive projection chain target contains duplicate columns: "
            f"{audit['duplicate_projected_columns']!r}",
        )
    if not audit["projected_columns_subset_of_source"]:
        note_mismatch(
            audit,
            "recursive projection chain target is not a subset of the source projection: "
            f"{audit['projected_columns_missing_from_source']!r}",
        )
    for label, child in (
        ("full-to-source", full_to_source),
        ("source-to-projected", source_to_projected),
        ("full-to-projected", full_to_projected),
    ):
        if not child.get("stable"):
            note_mismatch(audit, f"recursive projection chain {label} audit is not stable")

    for contract_key, output_key in (
        ("root_contract_fingerprints_by_name", "root_contract_transitive_matches_by_name"),
        ("leaf_contract_fingerprints_by_name", "leaf_contract_transitive_matches_by_name"),
        ("field_fingerprints_by_name", "field_fingerprint_transitive_matches_by_name"),
    ):
        full_contracts = summary_dict(full_summary, contract_key)
        source_contracts = summary_dict(source_summary, contract_key)
        projected_contracts = summary_dict(projected_summary, contract_key)
        matches: dict[str, bool] = {}
        for name in projected_order:
            matches[name] = (
                name in full_contracts
                and full_contracts.get(name) == source_contracts.get(name)
                and full_contracts.get(name) == projected_contracts.get(name)
            )
            if not matches[name]:
                note_mismatch(
                    audit,
                    f"recursive projection chain {contract_key} drifted for {name!r}",
                )
        audit[output_key] = matches

    audit["direct_vs_chained_root_contract_fingerprint_matches"] = (
        full_to_projected.get("actual_projection_root_contract_fingerprint")
        == source_to_projected.get("actual_projection_root_contract_fingerprint")
        == full_to_projected.get("expected_projection_root_contract_fingerprint")
    )
    audit["direct_vs_chained_leaf_contract_fingerprint_matches"] = (
        full_to_projected.get("actual_projection_leaf_contract_fingerprint")
        == source_to_projected.get("actual_projection_leaf_contract_fingerprint")
        == full_to_projected.get("expected_projection_leaf_contract_fingerprint")
    )
    audit["direct_vs_chained_field_fingerprint_matches"] = (
        full_to_projected.get("actual_projection_field_fingerprint")
        == source_to_projected.get("actual_projection_field_fingerprint")
        == full_to_projected.get("expected_projection_field_fingerprint")
    )
    for flag_name in (
        "direct_vs_chained_root_contract_fingerprint_matches",
        "direct_vs_chained_leaf_contract_fingerprint_matches",
        "direct_vs_chained_field_fingerprint_matches",
    ):
        if not audit[flag_name]:
            note_mismatch(audit, f"recursive projection chain {flag_name} is false")
    return audit
