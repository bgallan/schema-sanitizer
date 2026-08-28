"""Partition audits for native Parquet recursive projection layouts.

It verifies that projection partitions are disjoint, contract-compatible, and together
reproduce the full recursive layout.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any

from .subset import _native_recursive_projection_contract_audit_from_summaries
from .summary import (
    canonical_fingerprint,
    duplicate_names,
    note_mismatch,
    ordered_fingerprint,
    summary_dict,
    summary_list,
)


@dataclass(frozen=True)
class PartitionAuditInputs:
    """Canonical partition specs and full-layout contracts used by an audit."""

    partitions: list[list[str]]
    summaries: list[dict[str, Any] | None]
    full_order: list[str]
    requested_columns: list[str]
    duplicate_columns: list[str]
    partition_duplicate_columns: list[list[str]]
    missing_columns: list[str]
    unknown_columns: list[str]
    full_fields: dict[str, Any]
    full_leaf_contracts: dict[str, Any]
    full_roots: dict[str, Any]


def prepare_partition_audit_inputs(
    full_summary: dict[str, Any] | None,
    partition_summaries: list[dict[str, Any] | None] | tuple[dict[str, Any] | None, ...],
    partitions: list[list[str] | tuple[str, ...]] | tuple[list[str] | tuple[str, ...], ...],
) -> PartitionAuditInputs:
    """Normalize partition specs and derive coverage diagnostics."""
    normalized = [[str(column) for column in partition] for partition in partitions]
    summaries = list(partition_summaries)
    full_order = summary_list(full_summary, "field_order")
    requested = [column for partition in normalized for column in partition]
    requested_counts = Counter(requested)
    requested_set = set(requested_counts)
    full_set = set(full_order)
    return PartitionAuditInputs(
        partitions=normalized,
        summaries=summaries,
        full_order=full_order,
        requested_columns=requested,
        duplicate_columns=duplicate_names(requested),
        partition_duplicate_columns=[duplicate_names(partition) for partition in normalized],
        missing_columns=sorted(full_set - requested_set),
        unknown_columns=sorted(requested_set - full_set),
        full_fields=summary_dict(full_summary, "field_fingerprints_by_name"),
        full_leaf_contracts=summary_dict(full_summary, "leaf_contract_fingerprints_by_name"),
        full_roots=summary_dict(full_summary, "root_contract_fingerprints_by_name"),
    )


def collect_recomposed_contracts(
    partition_audits: list[dict[str, Any]],
) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    """Collect field, leaf, and root contracts from child partition audits."""
    recomposed_fields: dict[str, str] = {}
    recomposed_leaf_contracts: dict[str, str] = {}
    recomposed_roots: dict[str, str] = {}
    for child in partition_audits:
        for key, target in (
            ("projected_field_fingerprints_by_name", recomposed_fields),
            ("projected_leaf_contract_fingerprints_by_name", recomposed_leaf_contracts),
            ("projected_root_contract_fingerprints_by_name", recomposed_roots),
        ):
            raw = child.get(key) or {}
            if not isinstance(raw, dict):
                continue
            for name, fingerprint in raw.items():
                target[str(name)] = str(fingerprint)
    return recomposed_fields, recomposed_leaf_contracts, recomposed_roots


def apply_recomposed_contracts(
    audit: dict[str, Any],
    inputs: PartitionAuditInputs,
    partition_audits: list[dict[str, Any]],
) -> None:
    """Attach recomposed contracts, fingerprints, and full-layout comparisons."""
    fields, leaf_contracts, roots = collect_recomposed_contracts(partition_audits)
    audit["recomposed_field_fingerprints_by_name"] = dict(sorted(fields.items()))
    audit["recomposed_leaf_contract_fingerprints_by_name"] = dict(sorted(leaf_contracts.items()))
    audit["recomposed_root_contract_fingerprints_by_name"] = dict(sorted(roots.items()))

    for kind, values in (
        ("field", fields),
        ("leaf_contract", leaf_contracts),
        ("root_contract", roots),
    ):
        audit[f"canonical_recomposed_{kind}_fingerprint"] = canonical_fingerprint(
            values,
            inputs.full_order,
        )
        audit[f"ordered_recomposed_{kind}_fingerprint"] = ordered_fingerprint(
            values,
            inputs.requested_columns,
        )
        audit[f"{kind}_fingerprint_matches_full"] = (
            audit[f"canonical_full_{kind}_fingerprint"]
            == audit[f"canonical_recomposed_{kind}_fingerprint"]
        )


def _native_recursive_projection_partition_contract_audit_from_summaries(
    full_summary: dict[str, Any] | None,
    partition_summaries: list[dict[str, Any] | None] | tuple[dict[str, Any] | None, ...],
    *,
    partitions: list[list[str] | tuple[str, ...]] | tuple[list[str] | tuple[str, ...], ...],
) -> dict[str, Any]:
    """Audit that disjoint recursive projections exactly partition a full layout."""
    inputs = prepare_partition_audit_inputs(full_summary, partition_summaries, partitions)
    partition_audits = [
        _native_recursive_projection_contract_audit_from_summaries(
            full_summary,
            inputs.summaries[index] if index < len(inputs.summaries) else None,
            columns=partition,
        )
        for index, partition in enumerate(inputs.partitions)
    ]
    audit: dict[str, Any] = {
        "stable": True,
        "mismatches": [],
        "full_summary_ready": isinstance(full_summary, dict),
        "full_stable_across_row_groups": bool(
            full_summary.get("stable_across_row_groups")
            if isinstance(full_summary, dict)
            else False
        ),
        "full_field_order": inputs.full_order,
        "partitions": inputs.partitions,
        "partition_count": len(inputs.partitions),
        "partition_summary_count": len(inputs.summaries),
        "partition_audits": partition_audits,
        "partition_audits_stable": True,
        "partition_duplicate_columns": inputs.partition_duplicate_columns,
        "duplicate_partition_columns": inputs.duplicate_columns,
        "missing_partition_columns": inputs.missing_columns,
        "unknown_partition_columns": inputs.unknown_columns,
        "coverage_exact": not (
            inputs.missing_columns or inputs.unknown_columns or inputs.duplicate_columns
        ),
        "recomposed_field_fingerprints_by_name": {},
        "recomposed_leaf_contract_fingerprints_by_name": {},
        "recomposed_root_contract_fingerprints_by_name": {},
        "field_fingerprint_matches_full": False,
        "leaf_contract_fingerprint_matches_full": False,
        "root_contract_fingerprint_matches_full": False,
        "canonical_full_field_fingerprint": canonical_fingerprint(
            inputs.full_fields, inputs.full_order
        ),
        "canonical_recomposed_field_fingerprint": "",
        "canonical_full_leaf_contract_fingerprint": canonical_fingerprint(
            inputs.full_leaf_contracts, inputs.full_order
        ),
        "canonical_recomposed_leaf_contract_fingerprint": "",
        "canonical_full_root_contract_fingerprint": canonical_fingerprint(
            inputs.full_roots, inputs.full_order
        ),
        "canonical_recomposed_root_contract_fingerprint": "",
        "ordered_recomposed_field_fingerprint": "",
        "ordered_recomposed_leaf_contract_fingerprint": "",
        "ordered_recomposed_root_contract_fingerprint": "",
    }

    if full_summary is None:
        note_mismatch(audit, "full recursive layout summary is unavailable")
        return audit
    if not audit["full_stable_across_row_groups"]:
        note_mismatch(audit, "full recursive layout summary is not stable across row groups")
    if len(inputs.summaries) != len(inputs.partitions):
        note_mismatch(
            audit,
            "recursive projection partition audit received a different number of "
            "partition summaries and partition specs",
        )
    if inputs.duplicate_columns:
        note_mismatch(
            audit,
            "recursive projection partitions contain duplicate columns: "
            f"{inputs.duplicate_columns!r}",
        )
    for index, duplicates in enumerate(inputs.partition_duplicate_columns):
        if duplicates:
            note_mismatch(
                audit,
                f"recursive projection partition[{index}] contains duplicate columns: "
                f"{duplicates!r}",
            )
    if inputs.missing_columns:
        note_mismatch(
            audit,
            "recursive projection partitions do not cover full layout columns: "
            f"{inputs.missing_columns!r}",
        )
    if inputs.unknown_columns:
        note_mismatch(
            audit,
            "recursive projection partitions reference columns absent from full layout: "
            f"{inputs.unknown_columns!r}",
        )
    for index, child in enumerate(partition_audits):
        if not child.get("stable"):
            audit["partition_audits_stable"] = False
            note_mismatch(audit, f"recursive projection partition[{index}] audit is not stable")

    apply_recomposed_contracts(audit, inputs, partition_audits)
    for flag_name in (
        "field_fingerprint_matches_full",
        "leaf_contract_fingerprint_matches_full",
        "root_contract_fingerprint_matches_full",
    ):
        if not audit[flag_name]:
            note_mismatch(audit, f"recursive projection partition {flag_name} is false")
    return audit
