"""High-level BigQuery external-table and registry API.

It presents the stable table-reference, external-table, query, and registry surface
while leaving lower-level workflows in the advanced namespace.
"""

from __future__ import annotations

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
