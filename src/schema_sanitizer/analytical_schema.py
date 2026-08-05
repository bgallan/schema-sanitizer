"""Helpers that separate wide ingress schemas from normalized analytical schemas."""

from __future__ import annotations

import base64
import binascii
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .core_impl.dependencies import ensure_optional_dependency, ensure_pyarrow
from .core_impl.generated_metadata import (
    ETL_GENERATED_COLUMN_NAMES,
    SCHEMA_DRIFTS_COLUMN,
    SCHEMA_REGISTRY_COLUMN,
)
from .core_impl.logical_schema import pyarrow_schema_from_payload
from .core_impl.schema_registry import (
    SchemaRegistryMergeResult,
    merge_schema_registry,
    schema_contract_from_registry_json,
)

_ARROW_SCHEMA_IPC_KEY = "arrow_schema_ipc_base64"
_MAX_ARROW_SCHEMA_IPC_BYTES = 16 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class AnalyticalValidationResult:
    """Successful validation details for one analytical table or dataframe."""

    row_count: int
    schema: Any


@dataclass(frozen=True, slots=True)
class FinalizedAnalyticalOutput:
    """Normalized analytical data plus its regenerated final registry metadata."""

    clean_data: Any
    schema_registry: dict[str, Any]
    schema_registry_json: str
    schema_drifts: list[dict[str, Any]]
    schema_drifts_json: str


def schema_registry_from_arrow_schema(
    schema: Any,
    *,
    field_name_policy: str = "lower_snake",
    detected_at: str = "",
) -> dict[str, Any]:
    """Create a fresh schema-sanitizer registry from a PyArrow schema."""
    pa = ensure_pyarrow(feature="schema_registry_from_arrow_schema")
    if not isinstance(schema, pa.Schema):
        raise TypeError("schema must be a pyarrow.Schema")
    registry = _registry_merge_from_arrow_schema(
        schema,
        field_name_policy=field_name_policy,
        detected_at=detected_at,
    ).schema_registry
    serialized = schema.serialize().to_pybytes()
    if len(serialized) > _MAX_ARROW_SCHEMA_IPC_BYTES:
        raise ValueError("serialized Arrow schema exceeds safety limit")
    registry[_ARROW_SCHEMA_IPC_KEY] = base64.b64encode(serialized).decode("ascii")
    return registry


def arrow_schema_from_schema_registry(schema_registry: Mapping[str, Any] | str) -> Any:
    """Expose the canonical PyArrow schema carried by a schema registry."""
    if isinstance(schema_registry, Mapping):
        registry = dict(schema_registry)
    elif isinstance(schema_registry, str):
        try:
            registry = json.loads(schema_registry)
        except json.JSONDecodeError as exc:
            raise ValueError("schema_registry must contain valid JSON") from exc
        if not isinstance(registry, dict):
            raise ValueError("schema_registry must be a JSON object")
    else:
        raise TypeError("schema_registry must be a mapping or JSON string")

    embedded = registry.get(_ARROW_SCHEMA_IPC_KEY)
    if embedded is not None:
        if not isinstance(embedded, str):
            raise ValueError("embedded Arrow schema must be base64 text")
        if len(embedded) > ((_MAX_ARROW_SCHEMA_IPC_BYTES + 2) // 3) * 4:
            raise ValueError("embedded Arrow schema exceeds safety limit")
        try:
            serialized = base64.b64decode(embedded, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ValueError("embedded Arrow schema is not valid base64") from exc
        if len(serialized) > _MAX_ARROW_SCHEMA_IPC_BYTES:
            raise ValueError("embedded Arrow schema exceeds safety limit")
        pa = ensure_pyarrow(feature="arrow_schema_from_schema_registry")
        try:
            return pa.ipc.read_schema(pa.BufferReader(serialized))
        except Exception as exc:
            raise ValueError("embedded Arrow schema is invalid") from exc

    registry_json = json.dumps(
        registry,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    payload = schema_contract_from_registry_json(registry_json)
    if payload is None:
        raise ValueError("schema_registry does not contain a canonical schema")
    return pyarrow_schema_from_payload(payload.payload)


def project_ingress_scalar_schema(
    final_schema: Any,
    *,
    include_fields: set[str] | frozenset[str] | None = None,
    exclude_generated: bool = True,
) -> Any:
    """Project a final Arrow schema to scalar fields suitable for wide CSV ingress.

    Nested list, struct, map, and union fields are excluded. Generated ETL fields
    are excluded by default because the reader recreates them itself.
    """
    pa = ensure_pyarrow(feature="project_ingress_scalar_schema")
    if not isinstance(final_schema, pa.Schema):
        raise TypeError("final_schema must be a pyarrow.Schema")
    requested = None if include_fields is None else frozenset(include_fields)
    known = {field.name for field in final_schema}
    if requested is not None:
        missing = sorted(requested - known)
        if missing:
            raise ValueError(f"include_fields contains unknown fields: {missing!r}")
    generated = set(ETL_GENERATED_COLUMN_NAMES) if exclude_generated else set()
    fields = [
        field
        for field in final_schema
        if field.name not in generated
        and (requested is None or field.name in requested)
        and _is_ingress_scalar_type(pa, field.type)
    ]
    return pa.schema(fields, metadata=final_schema.metadata)


def validate_analytical_result(
    data: Any,
    expected_schema: Any,
    *,
    check_metadata: bool = False,
) -> AnalyticalValidationResult:
    """Validate an Arrow or Polars analytical result against its final schema.

    Validation is exact for field order, names, nullability, and data types.
    Schema metadata is ignored by default because adapters may not preserve it.
    """
    pa = ensure_pyarrow(feature="validate_analytical_result")
    if not isinstance(expected_schema, pa.Schema):
        raise TypeError("expected_schema must be a pyarrow.Schema")
    schema, row_count = _arrow_schema_and_row_count(data, pa=pa)
    if not schema.equals(expected_schema, check_metadata=check_metadata):
        raise ValueError(_schema_mismatch_message(schema, expected_schema))
    return AnalyticalValidationResult(row_count=row_count, schema=schema)


def finalize_analytical_output(
    data: Any,
    final_schema: Any,
    *,
    field_name_policy: str = "lower_snake",
    detected_at: str = "",
    validate: bool = True,
) -> FinalizedAnalyticalOutput:
    """Replace intermediate schema metadata with registry state for final data.

    The final registry is built from the normalized schema without the two
    self-referential registry columns. Existing ``source_file`` and
    ``ingestion_timestamp`` values are preserved.
    """
    pa = ensure_pyarrow(feature="finalize_analytical_output")
    if not isinstance(final_schema, pa.Schema):
        raise TypeError("final_schema must be a pyarrow.Schema")
    registry_schema = pa.schema(
        [
            field
            for field in final_schema
            if field.name not in {SCHEMA_REGISTRY_COLUMN, SCHEMA_DRIFTS_COLUMN}
        ],
        metadata=final_schema.metadata,
    )
    merged = _registry_merge_from_arrow_schema(
        registry_schema,
        field_name_policy=field_name_policy,
        detected_at=detected_at,
    )
    finalized = _replace_metadata_columns(
        data,
        registry_json=merged.schema_registry_json,
        drifts_json=merged.schema_drifts_json,
        pa=pa,
    )
    finalized = _select_final_field_order(finalized, final_schema, pa=pa)
    if validate:
        validate_analytical_result(finalized, final_schema)
    return FinalizedAnalyticalOutput(
        clean_data=finalized,
        schema_registry=merged.schema_registry,
        schema_registry_json=merged.schema_registry_json,
        schema_drifts=merged.schema_drifts,
        schema_drifts_json=merged.schema_drifts_json,
    )


def _registry_merge_from_arrow_schema(
    schema: Any,
    *,
    field_name_policy: str,
    detected_at: str,
) -> SchemaRegistryMergeResult:
    """Build one fresh native registry merge result from an Arrow schema."""
    return merge_schema_registry(
        inferred_schema=schema,
        schema_registry=None,
        field_name_policy=field_name_policy,
        detected_at=detected_at,
    )


def _is_ingress_scalar_type(pa: Any, data_type: Any) -> bool:
    """Return whether a type is scalar enough to originate in one CSV cell."""
    return not (
        pa.types.is_struct(data_type)
        or pa.types.is_list(data_type)
        or pa.types.is_large_list(data_type)
        or pa.types.is_map(data_type)
        or pa.types.is_union(data_type)
        or (hasattr(pa.types, "is_fixed_size_list") and pa.types.is_fixed_size_list(data_type))
    )


def _arrow_schema_and_row_count(data: Any, *, pa: Any) -> tuple[Any, int]:
    """Return an Arrow schema and row count from supported analytical objects."""
    if isinstance(data, pa.Schema):
        return data, 0
    if isinstance(data, (pa.Table, pa.RecordBatch)):
        return data.schema, data.num_rows
    module_name = type(data).__module__.split(".", 1)[0]
    if module_name == "polars" and hasattr(data, "to_arrow"):
        table = data.to_arrow()
        return table.schema, table.num_rows
    raise TypeError("data must be a pyarrow Table/RecordBatch/Schema or polars DataFrame")


def _schema_mismatch_message(actual: Any, expected: Any) -> str:
    """Return a compact deterministic schema mismatch explanation."""
    actual_fields = [(field.name, str(field.type), field.nullable) for field in actual]
    expected_fields = [(field.name, str(field.type), field.nullable) for field in expected]
    return f"analytical schema mismatch: actual={actual_fields!r}, expected={expected_fields!r}"


def _replace_metadata_columns(
    data: Any,
    *,
    registry_json: str,
    drifts_json: str,
    pa: Any,
) -> Any:
    """Replace or append final registry columns on Arrow or Polars data."""
    if isinstance(data, pa.Table):
        return _replace_arrow_metadata_columns(
            data,
            registry_json=registry_json,
            drifts_json=drifts_json,
            pa=pa,
        )
    module_name = type(data).__module__.split(".", 1)[0]
    if module_name == "polars" and hasattr(data, "with_columns"):
        pl = ensure_optional_dependency(
            "polars", extra="polars", feature="finalize_analytical_output"
        )
        return data.with_columns(
            pl.lit(registry_json).alias(SCHEMA_REGISTRY_COLUMN),
            pl.lit(drifts_json).alias(SCHEMA_DRIFTS_COLUMN),
        )
    raise TypeError("data must be a pyarrow.Table or polars DataFrame")


def _replace_arrow_metadata_columns(
    table: Any,
    *,
    registry_json: str,
    drifts_json: str,
    pa: Any,
) -> Any:
    """Replace or append registry columns on one PyArrow table."""
    values = {
        SCHEMA_REGISTRY_COLUMN: registry_json,
        SCHEMA_DRIFTS_COLUMN: drifts_json,
    }
    out = table
    for name, value in values.items():
        array = pa.array([value] * out.num_rows, type=pa.string())
        index = out.schema.get_field_index(name)
        field = pa.field(name, pa.string(), nullable=False)
        if index < 0:
            out = out.append_column(field, array)
        else:
            out = out.set_column(index, field, array)
    return out


def _select_final_field_order(data: Any, final_schema: Any, *, pa: Any) -> Any:
    """Select final columns in canonical order and cast Arrow data when needed."""
    names = final_schema.names
    data_names = list(data.column_names if hasattr(data, "column_names") else data.columns)
    missing = [name for name in names if name not in data_names]
    extra = [name for name in data_names if name not in names]
    if missing or extra:
        raise ValueError(f"final analytical columns mismatch: missing={missing!r}, extra={extra!r}")
    if isinstance(data, pa.Table):
        selected = data.select(names)
        if not selected.schema.equals(final_schema, check_metadata=False):
            selected = selected.cast(final_schema, safe=True)
        return selected
    return data.select(names)
