"""BigQuery Hive external-table adapter for example 08.

It adapts ADBC connections to schema lookup and atomic external-table replacement
operations used by the workflow.
"""

from __future__ import annotations

from typing import Any

from schema_sanitizer.integrations.bigquery.advanced import (
    ExternalTableSpec,
    bigquery_db_kwargs_from_namespace,
    execute_bigquery_sql,
    external_table_ddl,
    import_bigquery_adbc,
    parse_table_ref,
)


class AdbcBigQueryWorkflowClient:
    """ADBC adapter for target-schema lookup and Hive external-table DDL."""

    def __init__(self, args: Any) -> None:
        """Resolve the target reference and ADBC connection options once."""
        self._args = args
        self._table_ref = parse_table_ref(
            args.target_table,
            default_project=getattr(args, "bigquery_project", None),
        )
        self._dbapi, _database_options = import_bigquery_adbc()
        self._db_kwargs = bigquery_db_kwargs_from_namespace(args, self._table_ref)

    def read_target_schema(self, target_table: str) -> Any:
        """Read the existing target schema through a zero-row ADBC query."""
        table_ref = self._resolved_table_ref(target_table)
        query = f"SELECT * FROM {table_ref.sql_identifier} LIMIT 0"
        with self._dbapi.connect(db_kwargs=self._db_kwargs) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                if hasattr(cursor, "fetch_arrow_table"):
                    return cursor.fetch_arrow_table().schema
                if hasattr(cursor, "fetch_record_batch"):
                    reader = cursor.fetch_record_batch()
                    try:
                        return reader.schema
                    finally:
                        close = getattr(reader, "close", None)
                        if callable(close):
                            close()
        raise RuntimeError("ADBC BigQuery cursor did not expose an Arrow schema")

    def replace_external_table(
        self,
        target_table: str,
        *,
        source_uri_pattern: str,
        hive_uri_prefix: str,
        partition_columns: tuple[tuple[str, str], ...],
        reference_file_schema_uri: str,
        final_schema: Any,
    ) -> None:
        """Replace one Hive-partitioned Parquet table after publication."""
        table_ref = self._resolved_table_ref(target_table)
        ddl, _skipped = external_table_ddl(
            table_ref,
            final_schema,
            ExternalTableSpec(
                source_uris=[source_uri_pattern],
                hive_uri_prefix=hive_uri_prefix,
                partition_columns=partition_columns,
                reference_file_schema_uri=reference_file_schema_uri,
            ),
        )
        execute_bigquery_sql(
            dbapi=self._dbapi,
            db_kwargs=self._db_kwargs,
            query=ddl,
        )

    def _resolved_table_ref(self, target_table: str) -> Any:
        """Resolve one table against the configured default project."""
        return parse_table_ref(
            target_table,
            default_project=getattr(self._args, "bigquery_project", None),
        )


__all__ = ["AdbcBigQueryWorkflowClient"]
