"""Small DB-API query helpers for BigQuery ADBC usage."""

from __future__ import annotations

from typing import Any

from .sql import normalize_bq_type, quote_bq_string
from .table_ref import BigQueryTableRef


def fetch_one_cell(cursor: Any) -> Any:
    """Return the first column from the first row of a DB-API cursor."""
    row = cursor.fetchone()
    if row is None:
        return None
    if isinstance(row, dict):
        return next(iter(row.values()), None)
    return row[0]


def execute_bigquery_sql(
    *,
    dbapi: Any,
    db_kwargs: dict[str, str],
    query: str,
) -> None:
    """Execute one BigQuery SQL statement through DB-API."""
    with dbapi.connect(db_kwargs=db_kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute(query)


def table_type_query(table_ref: BigQueryTableRef) -> str:
    """Return SQL that checks BigQuery table type."""
    return "\n".join(
        [
            "SELECT table_type",
            f"FROM {table_ref.information_schema_tables_identifier}",
            f"WHERE table_catalog = {quote_bq_string(table_ref.project)}",
            f"  AND table_schema = {quote_bq_string(table_ref.dataset)}",
            f"  AND table_name = {quote_bq_string(table_ref.table)}",
            "LIMIT 1",
        ]
    )


def table_columns_query(
    table_ref: BigQueryTableRef,
    partition_columns: tuple[tuple[str, str], ...],
) -> str:
    """Return SQL that fetches configured BigQuery columns."""
    expected_names_sql = ", ".join(quote_bq_string(name) for name, _type in partition_columns)
    return "\n".join(
        [
            "SELECT column_name, data_type",
            f"FROM {table_ref.information_schema_columns_identifier}",
            f"WHERE table_catalog = {quote_bq_string(table_ref.project)}",
            f"  AND table_schema = {quote_bq_string(table_ref.dataset)}",
            f"  AND table_name = {quote_bq_string(table_ref.table)}",
            f"  AND column_name IN ({expected_names_sql})",
        ]
    )


def fetch_table_type(
    *,
    dbapi: Any,
    db_kwargs: dict[str, str],
    table_ref: BigQueryTableRef,
) -> str | None:
    """Return BigQuery table_type, or None if the table does not exist."""
    query = table_type_query(table_ref)
    with dbapi.connect(db_kwargs=db_kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            raw = fetch_one_cell(cur)
    return None if raw is None else str(raw)


def fetch_columns(
    *,
    dbapi: Any,
    db_kwargs: dict[str, str],
    table_ref: BigQueryTableRef,
    partition_columns: tuple[tuple[str, str], ...],
) -> dict[str, str]:
    """Return existing BigQuery columns as {lower_column_name: normalized_data_type}."""
    query = table_columns_query(table_ref, partition_columns)
    with dbapi.connect(db_kwargs=db_kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
    out: dict[str, str] = {}
    for row in rows:
        if isinstance(row, dict):
            name = row.get("column_name")
            data_type = row.get("data_type")
        else:
            name, data_type = row[0], row[1]
        if name is not None and data_type is not None:
            out[str(name).lower()] = normalize_bq_type(str(data_type))
    return out


def validate_existing_external_table_has_hive_partition_columns(
    *,
    existing_columns: dict[str, str],
    partition_columns: tuple[tuple[str, str], ...],
) -> list[str]:
    """Return validation errors for expected Hive partition columns."""
    errors: list[str] = []
    for column_name, expected_type in partition_columns:
        actual_type = existing_columns.get(column_name.lower())
        if actual_type is None:
            errors.append(f"missing column {column_name!r}")
        elif actual_type != expected_type:
            errors.append(
                f"column {column_name!r} has type {actual_type!r}, expected {expected_type!r}"
            )
    return errors
