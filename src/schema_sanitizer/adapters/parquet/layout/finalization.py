"""Final assembly of recursive Parquet layout summaries.

It turns field accumulators into deterministic path maps, collision reports, contract
totals, and the final diagnostic summary.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .fingerprints import field_fingerprint_bundle
from .path_components import path_components_key


def _collect_path_collisions(
    fields: list[dict[str, Any]],
    *,
    path_key: str,
    components_key: str,
    collision_path_key: str,
) -> tuple[dict[str, str], dict[str, str], list[dict[str, Any]]]:
    """Return string/component owner maps and cross-field collisions."""
    path_owners: dict[str, str] = {}
    component_owners: dict[str, str] = {}
    collisions: list[dict[str, Any]] = []
    for field in fields:
        field_name = str(field.get("name") or "")
        paths = [str(path) for path in field.get(path_key) or []]
        components_list = list(field.get(components_key) or [])
        if components_list:
            for path_index, components in enumerate(components_list):
                component_key = path_components_key(components)
                component_owners.setdefault(component_key, field_name)
                component_owner = component_owners[component_key]
                if component_owner != field_name:
                    collisions.append(
                        {
                            collision_path_key: (
                                paths[path_index] if path_index < len(paths) else component_key
                            ),
                            components_key: list(components),
                            "first_field": component_owner,
                            "other_field": field_name,
                        }
                    )
                if path_index < len(paths):
                    path_owners.setdefault(paths[path_index], field_name)
            continue
        for path in paths:
            path_owner = path_owners.get(path)
            if path_owner is None:
                path_owners[path] = field_name
            elif path_owner != field_name:
                collisions.append(
                    {
                        collision_path_key: path,
                        "first_field": path_owner,
                        "other_field": field_name,
                    }
                )
    return path_owners, component_owners, collisions


def collect_layout_path_collisions(
    fields: list[dict[str, Any]],
) -> tuple[
    dict[str, str],
    dict[str, str],
    list[dict[str, Any]],
    dict[str, str],
    dict[str, str],
    list[dict[str, Any]],
]:
    """Return leaf and repeated-node ownership maps with collisions."""
    leaf = _collect_path_collisions(
        fields,
        path_key="leaf_paths",
        components_key="leaf_path_components",
        collision_path_key="leaf_path",
    )
    repeated = _collect_path_collisions(
        fields,
        path_key="repeated_node_paths",
        components_key="repeated_node_path_components",
        collision_path_key="repeated_node_path",
    )
    return (*leaf, *repeated)


def build_layout_contract_maps(
    fields: list[dict[str, Any]],
) -> tuple[dict[str, Any], set[str]]:
    """Build all name-indexed contract maps and report duplicate names."""
    maps: dict[str, Any] = {
        "field_fingerprints_by_name": {},
        "leaf_level_fingerprints_by_name": {},
        "leaf_repetition_path_fingerprints_by_name": {},
        "leaf_repeated_ancestor_fingerprints_by_name": {},
        "leaf_contracts_by_name": {},
        "leaf_contract_fingerprints_by_name": {},
        "root_contracts_by_name": {},
        "root_contract_fingerprints_by_name": {},
    }
    duplicate_field_names: set[str] = set()
    for field in fields:
        bundle = field_fingerprint_bundle(field)
        field["field_fingerprint"] = bundle.layout
        field_name = str(field.get("name") or "")
        existing = maps["field_fingerprints_by_name"].get(field_name)
        if existing is not None and existing != bundle.layout:
            duplicate_field_names.add(field_name)
        maps["field_fingerprints_by_name"][field_name] = bundle.layout
        maps["leaf_level_fingerprints_by_name"][field_name] = bundle.leaf_level
        maps["leaf_repetition_path_fingerprints_by_name"][field_name] = bundle.repetition_path
        maps["leaf_repeated_ancestor_fingerprints_by_name"][field_name] = bundle.repeated_ancestor
        maps["leaf_contracts_by_name"][field_name] = bundle.leaf_contracts
        maps["leaf_contract_fingerprints_by_name"][field_name] = bundle.leaf_contract
        maps["root_contracts_by_name"][field_name] = bundle.root_contract
        maps["root_contract_fingerprints_by_name"][field_name] = bundle.root_contract_fingerprint
    return {key: dict(sorted(value.items())) for key, value in maps.items()}, duplicate_field_names


def _canonical_fingerprint(values: dict[str, str]) -> str:
    """Return a stable name-indexed fingerprint string."""
    return ";".join(f"{name}={fingerprint}" for name, fingerprint in values.items())


def finalize_recursive_layout_summary(
    summary: dict[str, Any],
    fields_by_position: list[dict[str, Any]],
    note_mismatch: Callable[[str], None],
) -> dict[str, Any]:
    """Populate collision maps and canonical fingerprints for a layout summary."""
    contract_maps, duplicate_field_names = build_layout_contract_maps(fields_by_position)
    (
        leaf_path_owners,
        leaf_component_owners,
        leaf_collisions,
        repeated_path_owners,
        repeated_component_owners,
        repeated_collisions,
    ) = collect_layout_path_collisions(fields_by_position)

    if duplicate_field_names:
        note_mismatch(
            "recursive field fingerprint collisions detected for field names: "
            f"{sorted(duplicate_field_names)!r}"
        )
    if leaf_collisions:
        summary["leaf_path_collisions"] = leaf_collisions
        note_mismatch(f"recursive leaf path collisions detected: {leaf_collisions!r}")
    if repeated_collisions:
        summary["repeated_node_path_collisions"] = repeated_collisions
        note_mismatch(f"recursive repeated-node path collisions detected: {repeated_collisions!r}")

    summary["fields"] = fields_by_position
    summary.update(contract_maps)
    summary["canonical_leaf_level_fingerprint"] = _canonical_fingerprint(
        contract_maps["leaf_level_fingerprints_by_name"]
    )
    summary["canonical_leaf_repetition_path_fingerprint"] = _canonical_fingerprint(
        contract_maps["leaf_repetition_path_fingerprints_by_name"]
    )
    summary["canonical_leaf_repeated_ancestor_fingerprint"] = _canonical_fingerprint(
        contract_maps["leaf_repeated_ancestor_fingerprints_by_name"]
    )
    summary["canonical_leaf_contract_fingerprint"] = _canonical_fingerprint(
        contract_maps["leaf_contract_fingerprints_by_name"]
    )
    summary["canonical_root_contract_fingerprint"] = _canonical_fingerprint(
        contract_maps["root_contract_fingerprints_by_name"]
    )
    summary["leaf_path_owner_map"] = dict(sorted(leaf_path_owners.items()))
    summary["leaf_path_component_owner_map"] = dict(sorted(leaf_component_owners.items()))
    summary["repeated_node_path_owner_map"] = dict(sorted(repeated_path_owners.items()))
    summary["repeated_node_path_component_owner_map"] = dict(
        sorted(repeated_component_owners.items())
    )
    summary["layout_fingerprint"] = ";".join(
        str(field.get("field_fingerprint") or "") for field in fields_by_position
    )
    summary["canonical_layout_fingerprint"] = _canonical_fingerprint(
        contract_maps["field_fingerprints_by_name"]
    )
    return summary
