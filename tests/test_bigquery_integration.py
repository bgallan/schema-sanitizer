"""BigQuery integration tests."""

# ruff: noqa: F405

from __future__ import annotations

from pipeline_shared import *  # noqa: F403


def test_bigquery_integration_builds_external_table_ddl() -> None:
    """Verify BigQuery schema/DDL helpers are package-owned."""
    pa = __import__("pyarrow")
    from schema_sanitizer.integrations.bigquery import (
        BigQueryTableRef,
        ExternalTableSpec,
        external_table_ddl,
        parse_hive_partition_column,
    )

    ddl, skipped = external_table_ddl(
        BigQueryTableRef("project", "dataset", "events"),
        pa.schema([pa.field("id", pa.int64()), pa.field("date", pa.date32())]),
        ExternalTableSpec(
            source_uris=["gs://silver/events/*"],
            hive_uri_prefix="gs://silver/events",
            partition_columns=(("date", "DATE"),),
        ),
    )

    assert skipped == ["date"]
    assert "CREATE OR REPLACE EXTERNAL TABLE" in ddl
    assert "`id` INT64" in ddl
    assert "source_column_match = 'NAME'" in ddl
    assert parse_hive_partition_column("hour:INT64") == ("hour", "INT64")


def test_bigquery_external_table_ddl_can_sort_nested_fields_alphabetically() -> None:
    """Verify BigQuery DDL can mirror column_order='alphabetically' recursively."""
    pa = __import__("pyarrow")
    from schema_sanitizer.integrations.bigquery import (
        BigQueryTableRef,
        ExternalTableSpec,
        external_table_ddl,
    )

    ddl, _skipped = external_table_ddl(
        BigQueryTableRef("project", "dataset", "events"),
        pa.schema(
            [
                pa.field("z", pa.int64()),
                pa.field(
                    "variables",
                    pa.struct(
                        [
                            pa.field("email", pa.string()),
                            pa.field("phone", pa.string()),
                            pa.field("birthday", pa.string()),
                            pa.field("company", pa.string()),
                        ]
                    ),
                ),
                pa.field("a", pa.string()),
            ]
        ),
        ExternalTableSpec(
            source_uris=["gs://silver/events/*"],
            hive_uri_prefix="gs://silver/events",
            partition_columns=(("date", "DATE"),),
        ),
        sort_fields_alphabetically=True,
    )

    assert ddl.index("`a` STRING") < ddl.index("`variables` STRUCT") < ddl.index("`z` INT64")
    assert (
        "`variables` STRUCT<`birthday` STRING, `company` STRING, `email` STRING, `phone` STRING>"
    ) in ddl


@pytest.mark.parametrize("sort_fields_alphabetically", [False, True])
def test_bigquery_external_table_ddl_keeps_etl_columns_last(
    sort_fields_alphabetically: bool,
) -> None:
    """Verify generated ETL columns trail user columns in canonical order."""
    pa = __import__("pyarrow")
    from schema_sanitizer.integrations.bigquery import (
        BigQueryTableRef,
        ExternalTableSpec,
        external_table_ddl,
    )

    ddl, _skipped = external_table_ddl(
        BigQueryTableRef("project", "dataset", "events"),
        pa.schema(
            [
                pa.field("source_file", pa.string()),
                pa.field("z", pa.int64()),
                pa.field("ingestion_timestamp", pa.timestamp("us")),
                pa.field("schema_drifts", pa.string()),
                pa.field("a", pa.string()),
                pa.field("schema_registry", pa.string()),
            ]
        ),
        ExternalTableSpec(
            source_uris=["gs://silver/events/*"],
            hive_uri_prefix="gs://silver/events",
            partition_columns=(("date", "DATE"),),
        ),
        sort_fields_alphabetically=sort_fields_alphabetically,
    )

    data_names = ["a", "z"] if sort_fields_alphabetically else ["z", "a"]
    expected_names = [
        *data_names,
        "schema_registry",
        "schema_drifts",
        "source_file",
        "ingestion_timestamp",
    ]
    assert [ddl.index(f"`{name}`") for name in expected_names] == sorted(
        ddl.index(f"`{name}`") for name in expected_names
    )


def test_bigquery_registry_sidecar_partition_queries() -> None:
    """Verify BigQuery registry sidecar SQL uses encoded Hive partition keys."""
    from schema_sanitizer.integrations.bigquery import (
        BigQueryTableRef,
        latest_schema_registry_query,
        partition_key_from_uri,
        sidecar_table_ddl,
        sidecar_upsert_query,
    )

    external = BigQueryTableRef("project", "dataset", "events_ext")
    sidecar = BigQueryTableRef("project", "dataset", "events_registry_state")
    partition_columns = (
        ("year", "INT64"),
        ("month", "INT64"),
        ("date", "DATE"),
        ("hour", "INT64"),
    )
    partition_key = partition_key_from_uri(
        "gs://silver/events/year=2026/month=07/date=2026-07-05/hour=08/file.parquet",
        partition_columns,
    )

    assert partition_key == "year=2026/month=07/date=2026-07-05/hour=08"
    lookup = latest_schema_registry_query(
        external,
        partition_columns,
        partition_key=partition_key,
    )
    assert "`year` = 2026" in lookup
    assert "`month` = 7" in lookup
    assert "`date` = DATE '2026-07-05'" in lookup
    assert "`hour` = 8" in lookup
    assert "CREATE TABLE IF NOT EXISTS `project.dataset.events_registry_state`" in (
        sidecar_table_ddl(sidecar)
    )
    upsert = sidecar_upsert_query(
        sidecar,
        external,
        last_ingested_partition=partition_key,
    )
    assert "MERGE `project.dataset.events_registry_state` AS target" in upsert
    assert "'project.dataset.events_ext'" in upsert
    assert "'year=2026/month=07/date=2026-07-05/hour=08'" in upsert


def test_bigquery_registry_sidecar_fetch_fast_path_and_missing_fallback(caplog) -> None:
    """Verify sidecar lookup narrows registry scans and missing sidecars fallback."""
    from schema_sanitizer.integrations.bigquery import (
        BigQueryTableRef,
        fetch_latest_schema_registry,
    )

    class FakeCursor:
        """Minimal cursor returning configured BigQuery query results."""

        def __init__(self, dbapi):
            """Store fake DB-API state."""
            self._dbapi = dbapi
            self._result = None

        def __enter__(self):
            """Return cursor for context-manager use."""
            return self

        def __exit__(self, _exc_type, _exc, _tb):
            """Propagate context-manager exceptions."""
            return False

        def execute(self, query):
            """Capture query text and choose the matching fake result."""
            self._dbapi.queries.append(query)
            if "INFORMATION_SCHEMA.TABLES" in query:
                self._result = self._dbapi.table_type
            elif "FROM `project.dataset.registry_state`" in query:
                self._result = self._dbapi.sidecar_partition
            elif "`hour` = 8" in query:
                self._result = '{"schema_generation":3,"canonical_schema":{}}'
            else:
                self._result = '{"schema_generation":2,"canonical_schema":{}}'

        def fetchone(self):
            """Return one fake result row."""
            if self._result is None:
                return None
            return (self._result,)

    class FakeConnection:
        """Minimal connection returning fake cursors."""

        def __init__(self, dbapi):
            """Store fake DB-API state."""
            self._dbapi = dbapi

        def __enter__(self):
            """Return connection for context-manager use."""
            return self

        def __exit__(self, _exc_type, _exc, _tb):
            """Propagate context-manager exceptions."""
            return False

        def cursor(self):
            """Return a fake cursor."""
            return FakeCursor(self._dbapi)

    class FakeDbapi:
        """Minimal DB-API facade for sidecar fetch tests."""

        def __init__(self, *, table_type, sidecar_partition):
            """Store configured fake query results."""
            self.table_type = table_type
            self.sidecar_partition = sidecar_partition
            self.queries = []

        def connect(self, *, db_kwargs):
            """Return a fake connection after checking connection options."""
            assert db_kwargs == {"project": "project"}
            return FakeConnection(self)

    external = BigQueryTableRef("project", "dataset", "events_ext")
    sidecar = BigQueryTableRef("project", "dataset", "registry_state")
    partition_columns = (
        ("year", "INT64"),
        ("month", "INT64"),
        ("date", "DATE"),
        ("hour", "INT64"),
    )

    with caplog.at_level(logging.INFO, logger="schema_sanitizer.integrations.bigquery"):
        dbapi = FakeDbapi(
            table_type="BASE TABLE",
            sidecar_partition="year=2026/month=07/date=2026-07-05/hour=08",
        )
        registry = fetch_latest_schema_registry(
            dbapi=dbapi,
            db_kwargs={"project": "project"},
            table_ref=external,
            partition_columns=partition_columns,
            sidecar_table_ref=sidecar,
        )
    assert registry["schema_generation"] == 3
    assert any("`hour` = 8" in query for query in dbapi.queries)
    assert "Sidecar lookup table=project.dataset.registry_state" in caplog.text
    assert "Sidecar lookup table=project.dataset.registry_state status=exists" in caplog.text
    assert (
        "Sidecar lookup table=project.dataset.registry_state "
        "external=project.dataset.events_ext status=hit "
        "partition=year=2026/month=07/date=2026-07-05/hour=08" in caplog.text
    )

    caplog.clear()
    missing_sidecar = FakeDbapi(table_type=None, sidecar_partition=None)
    with caplog.at_level(logging.INFO, logger="schema_sanitizer.integrations.bigquery"):
        registry = fetch_latest_schema_registry(
            dbapi=missing_sidecar,
            db_kwargs={"project": "project"},
            table_ref=external,
            partition_columns=partition_columns,
            sidecar_table_ref=sidecar,
        )
    assert registry["schema_generation"] == 2
    assert not any("`hour` = 8" in query for query in missing_sidecar.queries)
    assert (
        "Sidecar lookup table=project.dataset.registry_state "
        "external=project.dataset.events_ext status=missing fallback=external_scan" in caplog.text
    )


def test_bigquery_registry_sidecar_update_logs_create_and_upsert(caplog) -> None:
    """Verify sidecar updates log table creation checks and content updates."""
    from schema_sanitizer.integrations.bigquery import (
        BigQueryTableRef,
        update_registry_sidecar_table,
    )

    class FakeCursor:
        """Minimal cursor for sidecar update logging tests."""

        def __init__(self, dbapi):
            """Store fake DB-API state."""
            self._dbapi = dbapi
            self._result = None

        def __enter__(self):
            """Return cursor for context-manager use."""
            return self

        def __exit__(self, _exc_type, _exc, _tb):
            """Propagate context-manager exceptions."""
            return False

        def execute(self, query):
            """Capture query text and return table existence state."""
            self._dbapi.queries.append(query)
            if "INFORMATION_SCHEMA.TABLES" in query:
                self._result = self._dbapi.table_type
            else:
                self._result = None

        def fetchone(self):
            """Return one fake table type row."""
            if self._result is None:
                return None
            return (self._result,)

    class FakeConnection:
        """Minimal fake BigQuery connection."""

        def __init__(self, dbapi):
            """Store fake DB-API state."""
            self._dbapi = dbapi

        def __enter__(self):
            """Return connection for context-manager use."""
            return self

        def __exit__(self, _exc_type, _exc, _tb):
            """Propagate context-manager exceptions."""
            return False

        def cursor(self):
            """Return a fake cursor."""
            return FakeCursor(self._dbapi)

    class FakeDbapi:
        """Minimal DB-API facade for sidecar update tests."""

        def __init__(self, table_type):
            """Store configured table type."""
            self.table_type = table_type
            self.queries = []

        def connect(self, *, db_kwargs):
            """Return a fake connection after checking connection options."""
            assert db_kwargs == {"project": "project"}
            return FakeConnection(self)

    external = BigQueryTableRef("project", "dataset", "events_ext")
    sidecar = BigQueryTableRef("project", "dataset", "registry_state")
    dbapi = FakeDbapi(table_type=None)

    with caplog.at_level(logging.INFO, logger="schema_sanitizer.integrations.bigquery"):
        update_registry_sidecar_table(
            dbapi=dbapi,
            db_kwargs={"project": "project"},
            sidecar_table_ref=sidecar,
            external_table_ref=external,
            last_ingested_partition="year=2026/month=07/date=2026-07-05/hour=08",
        )

    assert any("INFORMATION_SCHEMA.TABLES" in query for query in dbapi.queries)
    assert any("CREATE TABLE IF NOT EXISTS" in query for query in dbapi.queries)
    assert any(
        "MERGE `project.dataset.registry_state` AS target" in query for query in dbapi.queries
    )
    assert (
        "Sidecar update table=project.dataset.registry_state status=missing action=create"
        in caplog.text
    )
    assert "Sidecar update table=project.dataset.registry_state action=ensure_table" in caplog.text
    assert (
        "Sidecar update table=project.dataset.registry_state action=upsert "
        "external=project.dataset.events_ext partition=year=2026/month=07/date=2026-07-05/hour=08"
        in caplog.text
    )

    caplog.clear()
    existing_dbapi = FakeDbapi(table_type="BASE TABLE")
    with caplog.at_level(logging.INFO, logger="schema_sanitizer.integrations.bigquery"):
        update_registry_sidecar_table(
            dbapi=existing_dbapi,
            db_kwargs={"project": "project"},
            sidecar_table_ref=sidecar,
            external_table_ref=external,
            last_ingested_partition="year=2026/month=07/date=2026-07-05/hour=09",
        )

    assert (
        "Sidecar update table=project.dataset.registry_state status=exists table_type=BASE TABLE"
        in caplog.text
    )
    assert any(
        "MERGE `project.dataset.registry_state` AS target" in query
        for query in existing_dbapi.queries
    )
