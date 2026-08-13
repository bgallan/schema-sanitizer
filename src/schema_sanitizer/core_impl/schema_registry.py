"""Schema-registry documents, native state, merge results, and runtime context."""

from __future__ import annotations

import json
import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

from .dependencies import ensure_pyarrow
from .error_translation import call_core
from .fork_safety import quarantine_inherited_state
from .logical_schema import (
    LogicalSchemaPayload,
    encode_arrow_schema_payload,
    pyarrow_schema_from_payload,
)
from .native_options import normalize_field_name_policy_option
from .native_runtime import native_core as _native


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
    return json.dumps(dict(registry), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _compact_registry_json_string(raw: str) -> str:
    """Validate and compact a registry JSON string using the native parser."""
    compact_bytes = call_core(_native.json_compact_bytes, raw.encode("utf-8"))
    return compact_bytes.decode("utf-8")


def _can_reuse_registry_json_string(raw: str, *, original: str) -> bool:
    """Return whether a registry string can skip canonicalization."""
    if raw == "{}":
        return True
    if raw != original or not (raw.startswith("{") and raw.endswith("}")):
        return False
    return any(
        marker in raw
        for marker in (
            '"registry_version"',
            '"schema_generation"',
            '"canonical_schema"',
            '"variants"',
        )
    )


def new_schema_registry(*, field_name_policy: str = "lower_snake") -> dict[str, Any]:
    """Return a fresh empty schema-registry document."""
    normalized_policy = normalize_field_name_policy_option(field_name_policy)
    raw = call_core(_native.schema_registry_empty, normalized_policy)
    return json.loads(str(raw))


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


def _normalize_schema(schema: Any, *, name: str) -> Any:
    """Validate an optional PyArrow schema."""
    if schema is None:
        return None
    pa = ensure_pyarrow(feature=name)
    if not isinstance(schema, pa.Schema):
        raise TypeError(f"{name} must be a pyarrow.Schema or None")
    return schema


def merge_schema_registry(
    *,
    inferred_schema: Any,
    schema_registry: Mapping[str, Any] | str | None = None,
    field_name_policy: str = "lower_snake",
    detected_at: str = "",
) -> SchemaRegistryMergeResult:
    """Merge an inferred schema into registry-backed canonical schema state."""
    return _merge_schema_registry_json(
        inferred_schema=inferred_schema,
        registry_json=_normalize_registry_json(schema_registry),
        field_name_policy=field_name_policy,
        detected_at=detected_at,
    )


def _merge_schema_registry_json(
    *,
    inferred_schema: Any,
    registry_json: str,
    field_name_policy: str = "lower_snake",
    detected_at: str = "",
) -> SchemaRegistryMergeResult:
    """Merge an inferred schema into an already-normalized registry JSON string."""
    inferred_schema = _normalize_schema(inferred_schema, name="inferred_schema")
    if inferred_schema is None:
        raise TypeError("inferred_schema must be a pyarrow.Schema")
    schema_payload, out_registry_json, out_drifts_json = call_core(
        _native.schema_registry_merge,
        encode_arrow_schema_payload(inferred_schema),
        registry_json,
        field_name_policy,
        detected_at,
    )
    return SchemaRegistryMergeResult(
        schema=pyarrow_schema_from_payload(schema_payload),
        schema_registry_json=out_registry_json,
        schema_drifts_json=out_drifts_json,
    )


def schema_contract_from_registry_json(registry_json: str) -> Any | None:
    """Return the canonical native schema contract carried by registry JSON."""
    contract_payload = call_core(_native.schema_registry_contract_payload, registry_json)
    if contract_payload is None:
        return None
    return LogicalSchemaPayload(bytes(contract_payload))


def native_registry_state_from_json(
    registry: Mapping[str, Any] | str | None,
    *,
    options: Any = None,
    drifts_json: str = "[]",
    conversion_timestamp: str = "",
) -> Any | None:
    """Compile canonical registry JSON with already-prepared native options."""
    return call_core(
        _native.registry_state_from_json,
        options,
        _normalize_registry_json(registry),
        drifts_json,
        conversion_timestamp,
    )


_NATIVE_REGISTRY_STATE: ContextVar[Any | None] = ContextVar(
    "schema_sanitizer_schema_registry_native_state",
    default=None,
)
_FORKED_NATIVE_REGISTRY_KEEPALIVE: list[Any] = []


@contextmanager
def native_registry_state_context(native_state: Any) -> Iterator[None]:
    """Temporarily seed file conversion with a compiled registry-state capsule."""
    owner_pid = os.getpid()
    token = _NATIVE_REGISTRY_STATE.set(native_state)
    try:
        yield
    finally:
        if os.getpid() == owner_pid:
            _NATIVE_REGISTRY_STATE.reset(token)
        else:
            _reset_native_registry_state_after_fork()


def _reset_native_registry_state_after_fork() -> None:
    """Detach inherited ABI3 capsules without invoking child-side finalizers."""
    inherited = _NATIVE_REGISTRY_STATE.get()
    if inherited is not None:
        quarantine_inherited_state("native-schema-registry", inherited)
    _NATIVE_REGISTRY_STATE.set(None)


from .fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler("schema-registry", mode="quarantine_only")


def current_native_registry_state() -> Any | None:
    """Return the compiled registry state active for the current context."""
    return _NATIVE_REGISTRY_STATE.get()
