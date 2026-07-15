"""Credential-gated interoperability checks against real GCS and BigQuery."""

from __future__ import annotations

import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

@pytest.fixture(autouse=True)
def _require_real_gcp(pytestconfig: pytest.Config) -> None:
    """Run only when explicitly enabled on the pytest command line."""
    if not pytestconfig.getoption("--run-real-gcp"):
        pytest.skip("real GCP interoperability is not configured")


def _required_option(pytestconfig: pytest.Config, name: str) -> str:
    """Return one required non-empty real-service pytest option."""
    value = str(pytestconfig.getoption(name)).strip()
    if not value:
        pytest.fail(f"{name} is required when --run-real-gcp is enabled")
    return value


def _bigquery_modules() -> tuple[Any, Any]:
    """Import the ADBC BigQuery modules used by production integration code."""
    try:
        import adbc_driver_bigquery
        import adbc_driver_bigquery.dbapi as dbapi
    except ImportError as exc:  # pragma: no cover - credential-gated environment contract
        pytest.fail(f"adbc-driver-bigquery is required for real GCP tests: {exc}")
    return dbapi, adbc_driver_bigquery.DatabaseOptions


def _query_one(dbapi: Any, db_kwargs: dict[str, str], query: str) -> Any:
    """Execute a query and return its first cell."""
    with dbapi.connect(db_kwargs=db_kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()
    return None if row is None else row[0]


def test_real_gcs_to_bigquery_external_table_and_sidecar(
    tmp_path: Path, pytestconfig: pytest.Config
) -> None:
    """Run GCS directory ingest, external-table DDL, and sidecar concurrency."""
    import pyarrow.parquet as pq

    import schema_sanitizer as ss
    from schema_sanitizer.integrations.bigquery import (
        BigQueryTableRef,
        ExternalTableSpec,
        create_or_replace_external_table_from_schema,
        fetch_sidecar_last_ingested_partition,
        sidecar_table_ddl,
        update_registry_sidecar_table,
    )
    from schema_sanitizer.integrations.bigquery.client import execute_bigquery_sql
    from schema_sanitizer.remote_impl.providers import gcs
    from schema_sanitizer.remote_impl.transport import run_sync

    project = _required_option(pytestconfig, "--real-gcp-project")
    bucket = _required_option(pytestconfig, "--real-gcs-bucket")
    dataset = _required_option(pytestconfig, "--real-bigquery-dataset")
    location = str(pytestconfig.getoption("--real-bigquery-location")).strip()
    run_token = uuid.uuid4().hex[:16]
    prefix = f"schema-sanitizer-ci/{run_token}"
    input_uri = f"gs://{bucket}/{prefix}/input/"
    hive_prefix = f"gs://{bucket}/{prefix}/silver"
    output_uri = f"{hive_prefix}/year=2026/month=07/date=2026-07-14/result.parquet"
    source_uris = [f"{input_uri}a.json", f"{input_uri}b.json"]
    cleanup_uris = [*source_uris, output_uri]

    dbapi, database_options = _bigquery_modules()
    db_kwargs = {
        database_options.PROJECT_ID.value: project,
        database_options.DATASET_ID.value: dataset,
    }
    if location:
        db_kwargs[database_options.LOCATION.value] = location

    external = BigQueryTableRef(project, dataset, f"ss_ext_{run_token}")
    sidecar = BigQueryTableRef(project, dataset, f"ss_sidecar_{run_token}")
    source_a = tmp_path / "a.json"
    source_b = tmp_path / "b.json"
    source_a.write_text('{"id":1,"name":"alpha"}\n', encoding="utf-8")
    source_b.write_text('{"id":2,"name":"beta"}\n', encoding="utf-8")

    try:
        run_sync(gcs.upload_file(str(source_a), source_uris[0]))
        run_sync(gcs.upload_file(str(source_b), source_uris[1]))

        discovered = run_sync(gcs.list_directory(input_uri, (".json",)))
        assert [item.name for item in discovered] == ["a.json", "b.json"]

        ss.to_parquet(
            input_uri,
            output_uri,
            input_format="json",
            input_mode="directory",
            parquet_compression="gzip",
        )
        assert run_sync(gcs.file_exists(output_uri)) is True

        local_output = tmp_path / "result.parquet"
        run_sync(gcs.download_file(output_uri, str(local_output)))
        schema = pq.read_schema(local_output)
        assert pq.read_table(local_output).num_rows == 2

        create_or_replace_external_table_from_schema(
            dbapi=dbapi,
            db_kwargs=db_kwargs,
            table_ref=external,
            schema=schema,
            spec=ExternalTableSpec(
                source_uris=[f"{hive_prefix}/*"],
                hive_uri_prefix=hive_prefix,
                partition_columns=(("year", "INT64"), ("month", "INT64"), ("date", "DATE")),
            ),
        )
        row_count = _query_one(
            dbapi,
            db_kwargs,
            f"SELECT COUNT(*) FROM {external.sql_identifier} "
            "WHERE `year` = 2026 AND `month` = 7 AND `date` = DATE '2026-07-14'",
        )
        assert int(row_count) == 2

        # Simulate a previous attempt that stopped after table creation.  The
        # normal update must safely resume and remain idempotent.
        execute_bigquery_sql(
            dbapi=dbapi,
            db_kwargs=db_kwargs,
            query=sidecar_table_ddl(sidecar),
        )
        partition = "year=2026/month=07/date=2026-07-14"
        for _ in range(2):
            update_registry_sidecar_table(
                dbapi=dbapi,
                db_kwargs=db_kwargs,
                sidecar_table_ref=sidecar,
                external_table_ref=external,
                last_ingested_partition=partition,
            )
        assert (
            fetch_sidecar_last_ingested_partition(
                dbapi=dbapi,
                db_kwargs=db_kwargs,
                sidecar_table_ref=sidecar,
                external_table_ref=external,
            )
            == partition
        )

        # Concurrent workers target the same logical row.  MERGE must retain a
        # single row rather than creating duplicate sidecar state.
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(
                    update_registry_sidecar_table,
                    dbapi=dbapi,
                    db_kwargs=db_kwargs,
                    sidecar_table_ref=sidecar,
                    external_table_ref=external,
                    last_ingested_partition=partition,
                )
                for _ in range(2)
            ]
            for future in futures:
                future.result()
        sidecar_rows = _query_one(
            dbapi,
            db_kwargs,
            f"SELECT COUNT(*) FROM {sidecar.sql_identifier} "
            f"WHERE external_table_name = '{external.display_name}'",
        )
        assert int(sidecar_rows) == 1
    finally:
        for table in (external, sidecar):
            try:
                execute_bigquery_sql(
                    dbapi=dbapi,
                    db_kwargs=db_kwargs,
                    query=f"DROP TABLE IF EXISTS {table.sql_identifier}",
                )
            except Exception:
                pass
        for uri in cleanup_uris:
            try:
                run_sync(gcs.delete_file(uri))
            except Exception:
                pass
