"""Root-level fingerprints for recursive Parquet layouts.

It derives canonical root and leaf fingerprints so independently decoded layouts can be
compared without relying on object identity.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from .fields import (
    int_list,
    int_matrix,
    leaf_contracts_from_field,
    leaf_level_fingerprint_from_field,
    leaf_repeated_ancestor_fingerprint_from_field,
    leaf_repetition_path_fingerprint_from_field,
)
from .path_components import component_fingerprint, normalize_path_components

_PHYSICAL_LEAF_ID_RE = re.compile(r"#\d+")


@dataclass(slots=True)
class FieldFingerprintBundle:
    """Fingerprints and contracts calculated once for one recursive field."""

    layout: str
    leaf_level: str
    repetition_path: str
    repeated_ancestor: str
    leaf_contracts: list[dict[str, Any]]
    leaf_contract: str
    root_contract: dict[str, Any]
    root_contract_fingerprint: str


def _projection_stable_shape_signature(value: Any) -> str:
    """Remove physical column ids that change under root projection."""
    return _PHYSICAL_LEAF_ID_RE.sub("", str(value))


def _leaf_contract_fingerprint(contracts: list[dict[str, Any]]) -> str:
    """Return a canonical JSON fingerprint for normalized leaf contracts."""
    return "|".join(
        json.dumps(contract, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        for contract in contracts
    )


def _root_contract_from_parts(
    field: dict[str, Any],
    *,
    leaf_contracts: list[dict[str, Any]],
    leaf_level_fingerprint: str,
    repetition_path_fingerprint: str,
    repeated_ancestor_fingerprint: str,
) -> dict[str, Any]:
    """Build one root contract from already-normalized leaf fingerprints."""
    leaf_paths = [str(path) for path in list(field.get("leaf_paths") or [])]
    leaf_components = normalize_path_components(field.get("leaf_path_components"))
    repeated_paths = [str(path) for path in list(field.get("repeated_node_paths") or [])]
    repeated_components = normalize_path_components(field.get("repeated_node_path_components"))
    if leaf_components is None or repeated_components is None:
        raise ValueError("recursive Parquet diagnostics require component-aware paths")
    return {
        "name": str(field.get("name") or ""),
        "root_kind": str(field.get("root_kind") or ""),
        "structural_shape_signature": str(field.get("structural_shape_signature") or ""),
        "shape_signatures": [
            _projection_stable_shape_signature(value)
            for value in list(field.get("shape_signatures") or [])
        ]
        or (
            [_projection_stable_shape_signature(field.get("shape_signature"))]
            if field.get("shape_signature") is not None
            else []
        ),
        "leaf_count_min": int(field.get("leaf_count_min") or field.get("leaf_count") or 0),
        "leaf_count_max": int(field.get("leaf_count_max") or field.get("leaf_count") or 0),
        "node_count_max": int(field.get("node_count_max") or field.get("node_count") or 0),
        "repetition_depth_max": int(
            field.get("repetition_depth_max") or field.get("repetition_depth") or 0
        ),
        "max_node_depth_max": int(
            field.get("max_node_depth_max") or field.get("max_node_depth") or 0
        ),
        "max_child_count_max": int(
            field.get("max_child_count_max") or field.get("max_child_count") or 0
        ),
        "leaf_paths": leaf_paths,
        "leaf_path_components": leaf_components,
        "repeated_node_paths": repeated_paths,
        "repeated_node_path_components": repeated_components,
        "leaf_level_fingerprint": leaf_level_fingerprint,
        "leaf_repetition_path_fingerprint": repetition_path_fingerprint,
        "leaf_repeated_ancestor_fingerprint": repeated_ancestor_fingerprint,
        "leaf_contracts": leaf_contracts,
    }


def _recursive_field_fingerprint_from_parts(
    field: dict[str, Any],
    *,
    repeated_ancestor_fingerprint: str,
    leaf_contract_fingerprint: str,
) -> str:
    """Build one layout fingerprint while reusing expensive nested contracts."""
    name = str(field.get("name") or "")
    root_kind = str(field.get("root_kind") or "")
    structural = str(field.get("structural_shape_signature") or "")
    leaves = "|".join(str(path) for path in list(field.get("leaf_paths") or []))
    leaf_components = component_fingerprint(
        normalize_path_components(field.get("leaf_path_components")) or []
    )
    repeated = "|".join(str(path) for path in list(field.get("repeated_node_paths") or []))
    repeated_components = component_fingerprint(
        normalize_path_components(field.get("repeated_node_path_components")) or []
    )
    definition_levels = ",".join(
        str(level) for level in int_list(field.get("leaf_max_definition_levels"))
    )
    repetition_levels = ",".join(
        str(level) for level in int_list(field.get("leaf_max_repetition_levels"))
    )
    path_definition_levels = "|".join(
        "[" + ",".join(str(level) for level in levels) + "]"
        for levels in int_matrix(field.get("leaf_path_definition_levels"))
    )
    path_repetition_levels = "|".join(
        "[" + ",".join(str(level) for level in levels) + "]"
        for levels in int_matrix(field.get("leaf_path_repetition_levels"))
    )
    return (
        f"{name}:{root_kind}:{structural}:"
        f"leaves=[{leaves}]:leaf_components=[{leaf_components}]:"
        f"repeated=[{repeated}]:repeated_components=[{repeated_components}]:"
        f"def=[{definition_levels}]:rep=[{repetition_levels}]:"
        f"path_def=[{path_definition_levels}]:path_rep=[{path_repetition_levels}]:"
        f"repeated_ancestors=[{repeated_ancestor_fingerprint}]:"
        f"leaf_contracts=[{leaf_contract_fingerprint}]"
    )


def field_fingerprint_bundle(field: dict[str, Any]) -> FieldFingerprintBundle:
    """Return every recursive field fingerprint without duplicate normalization."""
    leaf_level = leaf_level_fingerprint_from_field(field)
    repetition_path = leaf_repetition_path_fingerprint_from_field(field)
    repeated_ancestor = leaf_repeated_ancestor_fingerprint_from_field(field)
    leaf_contracts = leaf_contracts_from_field(field)
    leaf_contract = _leaf_contract_fingerprint(leaf_contracts)
    root_contract = _root_contract_from_parts(
        field,
        leaf_contracts=leaf_contracts,
        leaf_level_fingerprint=leaf_level,
        repetition_path_fingerprint=repetition_path,
        repeated_ancestor_fingerprint=repeated_ancestor,
    )
    return FieldFingerprintBundle(
        layout=_recursive_field_fingerprint_from_parts(
            field,
            repeated_ancestor_fingerprint=repeated_ancestor,
            leaf_contract_fingerprint=leaf_contract,
        ),
        leaf_level=leaf_level,
        repetition_path=repetition_path,
        repeated_ancestor=repeated_ancestor,
        leaf_contracts=leaf_contracts,
        leaf_contract=leaf_contract,
        root_contract=root_contract,
        root_contract_fingerprint=json.dumps(
            root_contract,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
    )
