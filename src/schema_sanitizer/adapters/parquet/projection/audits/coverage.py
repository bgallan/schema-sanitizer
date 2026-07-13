"""Partial and overlapping recursive projection coverage audits."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any, Callable

from .subset import _native_recursive_projection_contract_audit_from_summaries
from .summary import (
    canonical_fingerprint,
    duplicate_names,
    note_mismatch,
    summary_dict,
    summary_list,
)


@dataclass(frozen=True, slots=True)
class ProjectionCoverageInputs:
    """Normalized full-layout and projected-layout coverage inputs."""

    normalized_projections: list[list[str]]
    projection_summaries: list[dict[str, Any] | None]
    full_order: list[str]
    full_set: set[str]
    requested_columns: list[str]
    requested_set: set[str]
    covered_set: set[str]
    unknown_columns: list[str]
    uncovered_columns: list[str]
    overlap_counts: dict[str, int]
    projection_duplicate_columns: list[list[str]]
    full_fields: dict[str, str]
    full_leaf_contracts: dict[str, str]
    full_roots: dict[str, str]
    projection_audits: list[dict[str, Any]]


def normalize_projection_coverage_inputs(
    full_summary: dict[str, Any] | None,
    projection_summaries: list[dict[str, Any] | None] | tuple[dict[str, Any] | None, ...],
    projections: list[list[str] | tuple[str, ...]] | tuple[list[str] | tuple[str, ...], ...],
) -> ProjectionCoverageInputs:
    """Normalize projection specifications and derive coverage relationships."""
    normalized_projections = [[str(column) for column in projection] for projection in projections]
    summaries = list(projection_summaries)
    full_order = summary_list(full_summary, "field_order")
    full_set = set(full_order)
    requested_columns = [column for projection in normalized_projections for column in projection]
    requested_counts = Counter(requested_columns)
    requested_set = set(requested_counts)
    covered_set = requested_set & full_set
    overlap_counts = {
        name: requested_counts[name] for name in sorted(requested_set) if requested_counts[name] > 1
    }
    duplicate_columns = [duplicate_names(projection) for projection in normalized_projections]
    projection_audits = [
        _native_recursive_projection_contract_audit_from_summaries(
            full_summary,
            summaries[index] if index < len(summaries) else None,
            columns=projection,
        )
        for index, projection in enumerate(normalized_projections)
    ]
    return ProjectionCoverageInputs(
        normalized_projections=normalized_projections,
        projection_summaries=summaries,
        full_order=full_order,
        full_set=full_set,
        requested_columns=requested_columns,
        requested_set=requested_set,
        covered_set=covered_set,
        unknown_columns=sorted(requested_set - full_set),
        uncovered_columns=sorted(full_set - covered_set),
        overlap_counts=overlap_counts,
        projection_duplicate_columns=duplicate_columns,
        full_fields=summary_dict(full_summary, "field_fingerprints_by_name"),
        full_leaf_contracts=summary_dict(full_summary, "leaf_contract_fingerprints_by_name"),
        full_roots=summary_dict(full_summary, "root_contract_fingerprints_by_name"),
        projection_audits=projection_audits,
    )


MismatchReporter = Callable[[dict[str, Any], str], None]


def apply_projection_contract_consistency(
    audit: dict[str, Any],
    inputs: ProjectionCoverageInputs,
    *,
    note_mismatch: MismatchReporter,
) -> None:
    """Populate covered fingerprints and consistency verdicts on an audit."""
    occurrences: dict[str, dict[str, list[str]]] = {
        name: {"field": [], "leaf": [], "root": []} for name in sorted(inputs.covered_set)
    }
    for child in inputs.projection_audits:
        for source_key, bucket in (
            ("projected_field_fingerprints_by_name", "field"),
            ("projected_leaf_contract_fingerprints_by_name", "leaf"),
            ("projected_root_contract_fingerprints_by_name", "root"),
        ):
            raw = child.get(source_key) or {}
            if not isinstance(raw, dict):
                continue
            for name, fingerprint in raw.items():
                normalized_name = str(name)
                if normalized_name in occurrences:
                    occurrences[normalized_name][bucket].append(str(fingerprint))

    covered_fields: dict[str, str] = {}
    covered_leaf_contracts: dict[str, str] = {}
    covered_roots: dict[str, str] = {}
    for name in sorted(inputs.covered_set):
        field_values = occurrences[name]["field"]
        leaf_values = occurrences[name]["leaf"]
        root_values = occurrences[name]["root"]
        field_consistent = _values_match_full(field_values, inputs.full_fields.get(name))
        leaf_consistent = _values_match_full(leaf_values, inputs.full_leaf_contracts.get(name))
        root_consistent = _values_match_full(root_values, inputs.full_roots.get(name))
        audit["field_contract_consistency_by_name"][name] = field_consistent
        audit["leaf_contract_consistency_by_name"][name] = leaf_consistent
        audit["root_contract_consistency_by_name"][name] = root_consistent
        _capture_first(covered_fields, name, field_values)
        _capture_first(covered_leaf_contracts, name, leaf_values)
        _capture_first(covered_roots, name, root_values)
        _record_consistency(
            audit,
            name=name,
            contract="field",
            consistent=field_consistent,
            note_mismatch=note_mismatch,
        )
        _record_consistency(
            audit,
            name=name,
            contract="leaf",
            consistent=leaf_consistent,
            note_mismatch=note_mismatch,
        )
        _record_consistency(
            audit,
            name=name,
            contract="root",
            consistent=root_consistent,
            note_mismatch=note_mismatch,
        )

    covered_names = sorted(inputs.covered_set)
    audit["covered_field_fingerprints_by_name"] = dict(sorted(covered_fields.items()))
    audit["covered_leaf_contract_fingerprints_by_name"] = dict(
        sorted(covered_leaf_contracts.items())
    )
    audit["covered_root_contract_fingerprints_by_name"] = dict(sorted(covered_roots.items()))
    audit["canonical_covered_field_fingerprint"] = canonical_fingerprint(
        covered_fields, covered_names
    )
    audit["canonical_covered_leaf_contract_fingerprint"] = canonical_fingerprint(
        covered_leaf_contracts, covered_names
    )
    audit["canonical_covered_root_contract_fingerprint"] = canonical_fingerprint(
        covered_roots, covered_names
    )


def _values_match_full(values: list[str], expected: str | None) -> bool:
    """Return whether repeated projected values agree with the full contract."""
    return bool(values) and len(set(values)) == 1 and values[0] == expected


def _capture_first(target: dict[str, str], name: str, values: list[str]) -> None:
    """Capture the first projected fingerprint when one exists."""
    if values:
        target[name] = values[0]


def _record_consistency(
    audit: dict[str, Any],
    *,
    name: str,
    contract: str,
    consistent: bool,
    note_mismatch: MismatchReporter,
) -> None:
    """Update the aggregate consistency flag and mismatch report."""
    if consistent:
        return
    audit[f"{contract}_contracts_consistent"] = False
    note_mismatch(audit, f"recursive projection coverage {contract} contract drifted for {name!r}")


def _native_recursive_projection_coverage_contract_audit_from_summaries(
    full_summary: dict[str, Any] | None,
    projection_summaries: list[dict[str, Any] | None] | tuple[dict[str, Any] | None, ...],
    *,
    projections: list[list[str] | tuple[str, ...]] | tuple[list[str] | tuple[str, ...], ...],
    require_full_coverage: bool = False,
    allow_overlaps: bool = True,
) -> dict[str, Any]:
    """Audit partial and overlapping recursive projection coverage."""
    inputs = normalize_projection_coverage_inputs(full_summary, projection_summaries, projections)
    audit: dict[str, Any] = {
        "stable": True,
        "mismatches": [],
        "full_summary_ready": isinstance(full_summary, dict),
        "full_stable_across_row_groups": bool(
            full_summary.get("stable_across_row_groups")
            if isinstance(full_summary, dict)
            else False
        ),
        "require_full_coverage": bool(require_full_coverage),
        "allow_overlaps": bool(allow_overlaps),
        "full_field_order": inputs.full_order,
        "projections": inputs.normalized_projections,
        "projection_count": len(inputs.normalized_projections),
        "projection_summary_count": len(inputs.projection_summaries),
        "projection_audits": inputs.projection_audits,
        "projection_audits_stable": True,
        "projection_duplicate_columns": inputs.projection_duplicate_columns,
        "requested_columns": inputs.requested_columns,
        "covered_columns": sorted(inputs.covered_set),
        "uncovered_full_columns": inputs.uncovered_columns,
        "unknown_projection_columns": inputs.unknown_columns,
        "overlapping_projection_columns": sorted(inputs.overlap_counts),
        "overlap_counts_by_name": inputs.overlap_counts,
        "coverage_complete": inputs.full_set.issubset(inputs.requested_set),
        "coverage_exact_by_set": inputs.requested_set == inputs.full_set,
        "coverage_partial": bool(inputs.uncovered_columns),
        "coverage_has_overlaps": bool(inputs.overlap_counts),
        "covered_field_fingerprints_by_name": {},
        "covered_leaf_contract_fingerprints_by_name": {},
        "covered_root_contract_fingerprints_by_name": {},
        "field_contract_consistency_by_name": {},
        "leaf_contract_consistency_by_name": {},
        "root_contract_consistency_by_name": {},
        "field_contracts_consistent": True,
        "leaf_contracts_consistent": True,
        "root_contracts_consistent": True,
        "canonical_full_field_fingerprint": canonical_fingerprint(
            inputs.full_fields, inputs.full_order
        ),
        "canonical_covered_field_fingerprint": "",
        "canonical_full_leaf_contract_fingerprint": canonical_fingerprint(
            inputs.full_leaf_contracts, inputs.full_order
        ),
        "canonical_covered_leaf_contract_fingerprint": "",
        "canonical_full_root_contract_fingerprint": canonical_fingerprint(
            inputs.full_roots, inputs.full_order
        ),
        "canonical_covered_root_contract_fingerprint": "",
    }

    if full_summary is None:
        note_mismatch(audit, "full recursive layout summary is unavailable")
        return audit
    if not audit["full_stable_across_row_groups"]:
        note_mismatch(audit, "full recursive layout summary is not stable across row groups")
    if len(inputs.projection_summaries) != len(inputs.normalized_projections):
        note_mismatch(
            audit,
            "recursive projection coverage audit received a different number of "
            "projection summaries and projection specs",
        )
    for index, duplicates in enumerate(inputs.projection_duplicate_columns):
        if duplicates:
            note_mismatch(
                audit,
                f"recursive projection coverage projection[{index}] contains duplicate columns: "
                f"{duplicates!r}",
            )
    if inputs.unknown_columns:
        note_mismatch(
            audit,
            "recursive projection coverage references columns absent from full layout: "
            f"{inputs.unknown_columns!r}",
        )
    if require_full_coverage and inputs.uncovered_columns:
        note_mismatch(
            audit,
            "recursive projection coverage does not cover full layout columns: "
            f"{inputs.uncovered_columns!r}",
        )
    if not allow_overlaps and inputs.overlap_counts:
        note_mismatch(
            audit,
            "recursive projection coverage contains overlapping columns: "
            f"{sorted(inputs.overlap_counts)!r}",
        )

    for index, child in enumerate(inputs.projection_audits):
        if not child.get("stable"):
            audit["projection_audits_stable"] = False
            note_mismatch(
                audit,
                f"recursive projection coverage projection[{index}] audit is not stable",
            )

    apply_projection_contract_consistency(audit, inputs, note_mismatch=note_mismatch)
    return audit
