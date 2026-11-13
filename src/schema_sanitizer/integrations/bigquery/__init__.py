"""High-level BigQuery external-table and registry API.

It presents the stable table-reference, external-table, query, and registry surface
while leaving lower-level workflows in the advanced namespace.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from . import advanced
    from .arrow_schema import read_external_table_arrow_schema, resolve_bigquery_arrow_schema
    from .external_table import (
        ExternalTableSpec,
        create_or_replace_external_table_from_schema,
        external_table_ddl,
    )
    from .registry import fetch_latest_schema_registry
    from .sidecar import update_registry_sidecar_table
    from .table_ref import BigQueryTableRef, parse_table_ref


_LAZY: dict[str, tuple[str, str]] = {
    "BigQueryTableRef": (".table_ref", "BigQueryTableRef"),
    "ExternalTableSpec": (".external_table", "ExternalTableSpec"),
    "create_or_replace_external_table_from_schema": (
        ".external_table",
        "create_or_replace_external_table_from_schema",
    ),
    "external_table_ddl": (".external_table", "external_table_ddl"),
    "fetch_latest_schema_registry": (".registry", "fetch_latest_schema_registry"),
    "parse_table_ref": (".table_ref", "parse_table_ref"),
    "read_external_table_arrow_schema": (
        ".arrow_schema",
        "read_external_table_arrow_schema",
    ),
    "resolve_bigquery_arrow_schema": (".arrow_schema", "resolve_bigquery_arrow_schema"),
    "update_registry_sidecar_table": (".sidecar", "update_registry_sidecar_table"),
}


def __getattr__(name: str) -> Any:
    """Load one public BigQuery symbol only when callers request it."""
    if name == "advanced":
        value = import_module(f"{__name__}.advanced")
    else:
        spec = _LAZY.get(name)
        if spec is None:
            raise AttributeError(name)
        module_name, attribute = spec
        value = getattr(import_module(module_name, __name__), attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Include deferred public names in interactive module discovery."""
    return sorted({*globals(), *__all__})


__all__ = [
    "BigQueryTableRef",
    "ExternalTableSpec",
    "advanced",
    "create_or_replace_external_table_from_schema",
    "external_table_ddl",
    "fetch_latest_schema_registry",
    "parse_table_ref",
    "read_external_table_arrow_schema",
    "resolve_bigquery_arrow_schema",
    "update_registry_sidecar_table",
]
