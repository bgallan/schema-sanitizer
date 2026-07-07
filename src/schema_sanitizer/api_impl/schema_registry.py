"""Reusable native schema-registry helpers."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from ..adapters.pyarrow_common import ensure_pyarrow
from ..core_impl.native import _native
from ..core_impl.native_functions import REGISTRY_STATE_FROM_JSON
from ..core_impl.options_logical_schema import (
    LogicalSchemaPayload,
    _encode_logical_schema_payload_from_schema,
    _pyarrow_schema_from_logical_schema_payload,
)
from ..options_impl.call_option_validators import normalize_field_name_policy_option
from .shared import _call_core, _unwrap_options


@dataclass(frozen=True, init=False)
class SchemaRegistryMergeResult:
    """Merged schema-registry state returned by the native engine."""

    schema: Any
    schema_registry_json: str
    schema_drifts_json: str
    _schema_registry_cache: dict[str, Any] | None
    _schema_drifts_cache: list[dict[str, Any]] | None

    def __init__(
        self,
        *,
        schema: Any,
        schema_registry_json: str,
        schema_drifts_json: str,
        schema_registry: dict[str, Any] | None = None,
        schema_drifts: list[dict[str, Any]] | None = None,
    ):
        """Create a merge result with lazily parsed registry JSON."""
        object.__setattr__(self, "schema", schema)
        object.__setattr__(self, "schema_registry_json", schema_registry_json)
        object.__setattr__(self, "schema_drifts_json", schema_drifts_json)
        object.__setattr__(self, "_schema_registry_cache", schema_registry)
        object.__setattr__(self, "_schema_drifts_cache", schema_drifts)

    @property
    def schema_registry(self) -> dict[str, Any]:
        """Return the parsed schema registry, parsing JSON on first access."""
        cached = self._schema_registry_cache
        if cached is None:
            cached = json.loads(self.schema_registry_json or "{}")
            object.__setattr__(self, "_schema_registry_cache", cached)
        return cached

    @property
    def schema_drifts(self) -> list[dict[str, Any]]:
        """Return parsed schema drifts, parsing JSON on first access."""
        cached = self._schema_drifts_cache
        if cached is None:
            cached = json.loads(self.schema_drifts_json or "[]")
            object.__setattr__(self, "_schema_drifts_cache", cached)
        return cached


def _normalize_registry_json(registry: Mapping[str, Any] | str | None) -> str:
    """Return a compact JSON object string for registry input."""
    if registry is None:
        return "{}"
    if isinstance(registry, str):
        raw = registry.strip()
        if not raw:
            return "{}"
        if _can_reuse_registry_json_string(raw, original=registry):
            return raw
        compact = _compact_registry_json_string(raw)
        if not (compact.startswith("{") and compact.endswith("}")):
            raise ValueError("schema_registry must be a JSON object")
        return compact
    if not isinstance(registry, Mapping):
        raise TypeError("schema_registry must be a mapping, JSON string, or None")
    return _dump_registry_mapping(registry)


def _dump_registry_mapping(registry: Mapping[str, Any]) -> str:
    """Return compact JSON for a registry mapping."""
    return json.dumps(dict(registry), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _compact_registry_json_string(raw: str) -> str:
    """Validate and compact a registry JSON string using the native parser."""
    compact_bytes = _call_core(_native.json_compact_bytes, raw.encode("utf-8"))
    return compact_bytes.decode("utf-8")


def _can_reuse_registry_json_string(raw: str, *, original: str) -> bool:
    """Return whether a registry string can skip Python parse/dump canonicalization."""
    if raw == "{}":
        return True
    if raw != original:
        return False
    if not (raw.startswith("{") and raw.endswith("}")):
        return False
    return (
        '"registry_version"' in raw
        or '"schema_generation"' in raw
        or '"canonical_schema"' in raw
        or '"variants"' in raw
    )


def _normalize_schema(schema: Any, *, name: str) -> Any:
    """Validate an optional PyArrow schema."""
    if schema is None:
        return None
    pa = ensure_pyarrow(feature=name)
    if not isinstance(schema, pa.Schema):
        raise TypeError(f"{name} must be a pyarrow.Schema or None")
    return schema


def _registry_has_canonical_schema(registry: Mapping[str, Any] | str | None) -> bool:
    """Return whether a registry input carries a reusable canonical schema."""
    return _registry_json_has_canonical_schema(_normalize_registry_json(registry))


def _registry_json_has_canonical_schema(registry_json: str) -> bool:
    """Return whether normalized registry JSON carries a reusable canonical schema."""
    return bool(
        _call_core(
            _native.schema_registry_has_canonical_schema,
            registry_json,
        )
    )


def schema_contract_from_registry_json(
    registry_json: str,
    *,
    field_name_policy: str,
) -> Any | None:
    """Return a native schema contract from registry JSON, if one is available."""
    if not _registry_json_has_canonical_schema(registry_json):
        return None
    native_contract = getattr(_native, "schema_registry_contract_payload", None)
    if callable(native_contract):
        contract_payload = _call_core(native_contract, registry_json)
        return LogicalSchemaPayload(bytes(contract_payload))

    pa = ensure_pyarrow(feature="schema registry contract")
    merged = _merge_schema_registry_json(
        inferred_schema=pa.schema([]),
        registry_json=registry_json,
        field_name_policy=field_name_policy,
    )
    return LogicalSchemaPayload(_encode_logical_schema_payload_from_schema(merged.schema))


def native_registry_state_from_json(
    registry: Mapping[str, Any] | str | None,
    *,
    field_name_policy: str = "lower_snake",
    options: Any = None,
    drifts_json: str = "[]",
    conversion_timestamp: str = "",
) -> Any | None:
    """Compile registry JSON into a native state capsule when possible."""
    registry_json = _normalize_registry_json(registry)
    if not _registry_json_has_canonical_schema(registry_json):
        return None
    native_compile = REGISTRY_STATE_FROM_JSON.get()
    if native_compile is None:
        return None
    return _call_core(
        native_compile,
        _unwrap_options(options),
        registry_json,
        normalize_field_name_policy_option(field_name_policy),
        drifts_json,
        conversion_timestamp,
    )


def new_schema_registry(*, field_name_policy: str = "lower_snake") -> dict[str, Any]:
    """Return a fresh empty schema-registry document."""
    normalized_policy = normalize_field_name_policy_option(field_name_policy)
    native_empty = getattr(_native, "schema_registry_empty", None)
    if callable(native_empty):
        raw = _call_core(native_empty, normalized_policy)
        return json.loads(str(raw))
    return {
        "field_name_policy": normalized_policy,
        "registry_version": 1,
        "schema_generation": 1,
        "variants": {},
    }


def merge_schema_registry(
    *,
    inferred_schema: Any,
    schema_registry: Mapping[str, Any] | str | None = None,
    field_name_policy: str = "lower_snake",
) -> SchemaRegistryMergeResult:
    """Merge an inferred schema into registry-backed canonical schema state."""
    registry_json = _normalize_registry_json(schema_registry)

    return _merge_schema_registry_json(
        inferred_schema=inferred_schema,
        registry_json=registry_json,
        field_name_policy=field_name_policy,
    )


def _merge_schema_registry_json(
    *,
    inferred_schema: Any,
    registry_json: str,
    field_name_policy: str = "lower_snake",
) -> SchemaRegistryMergeResult:
    """Merge an inferred schema into an already-normalized registry JSON string."""
    inferred_schema = _normalize_schema(inferred_schema, name="inferred_schema")
    if inferred_schema is None:
        raise TypeError("inferred_schema must be a pyarrow.Schema")
    inferred_payload = _encode_logical_schema_payload_from_schema(inferred_schema)

    schema_payload, out_registry_json, out_drifts_json = _call_core(
        _native.schema_registry_merge,
        inferred_payload,
        registry_json,
        field_name_policy,
    )

    return SchemaRegistryMergeResult(
        schema=_pyarrow_schema_from_logical_schema_payload(schema_payload),
        schema_registry_json=out_registry_json,
        schema_drifts_json=out_drifts_json,
    )


__all__ = [
    "SchemaRegistryMergeResult",
    "merge_schema_registry",
    "native_registry_state_from_json",
    "new_schema_registry",
    "schema_contract_from_registry_json",
]
