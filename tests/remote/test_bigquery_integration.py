"""BigQuery integration tests.

It validates external-table DDL, nested and canonical schemas, Hive collisions, registry
queries, sidecars, client ownership, and namespace configuration.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

import schema_sanitizer.integrations.bigquery.external_table as external_table_owner


def _pyarrow():
    """Load PyArrow or skip integration tests when it is unavailable."""
    return pytest.importorskip("pyarrow")


def test_bigquery_integration_builds_external_table_ddl() -> None:
    """Verify BigQuery integration builds external table DDL."""
    pa = _pyarrow()
    from schema_sanitizer.integrations.bigquery import (
        BigQueryTableRef,
        ExternalTableSpec,
        external_table_ddl,
    )
    from schema_sanitizer.integrations.bigquery.advanced import parse_hive_partition_column

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
    assert "source_column_match" not in ddl
    assert parse_hive_partition_column("hour:INT64") == ("hour", "INT64")


def test_bigquery_external_table_ddl_can_sort_nested_fields_alphabetically() -> None:
    """Verify BigQuery external table DDL can sort nested fields alphabetically."""
    pa = _pyarrow()
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
                            pa.field("middle", pa.string()),
                            pa.field("tail", pa.string()),
                            pa.field("alpha", pa.string()),
                            pa.field("beta", pa.string()),
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
        "`variables` STRUCT<`alpha` STRING, `beta` STRING, `middle` STRING, `tail` STRING>"
    ) in ddl


def test_bigquery_external_table_uses_canonical_parquet_reference_schema() -> None:
    """Reference-file mode must avoid explicit positional schema matching."""
    pa = _pyarrow()
    from schema_sanitizer.integrations.bigquery import (
        BigQueryTableRef,
        ExternalTableSpec,
        external_table_ddl,
    )

    ddl, skipped = external_table_ddl(
        BigQueryTableRef("project", "dataset", "events"),
        pa.schema(
            [
                pa.field(
                    "variables",
                    pa.struct(
                        [
                            pa.field("birthday", pa.string()),
                            pa.field("email", pa.string()),
                        ]
                    ),
                )
            ]
        ),
        ExternalTableSpec(
            source_uris=["gs://silver/events/*"],
            hive_uri_prefix="gs://silver/events",
            partition_columns=(("date", "DATE"),),
            reference_file_schema_uri="gs://silver/events/date=2026-07-17/final.parquet",
        ),
    )

    assert skipped == []
    assert "reference_file_schema_uri = " in ddl
    assert "gs://silver/events/date=2026-07-17/final.parquet" in ddl
    assert "source_column_match" not in ddl
    assert "`variables` STRUCT" not in ddl
    assert "WITH PARTITION COLUMNS" in ddl


def test_bigquery_reference_schema_rejects_hive_column_collisions() -> None:
    """A reference file cannot hide fields that duplicate Hive partition keys."""
    pa = _pyarrow()
    from schema_sanitizer.integrations.bigquery import (
        BigQueryTableRef,
        ExternalTableSpec,
        external_table_ddl,
    )

    with pytest.raises(RuntimeError, match="collide with Hive partition columns"):
        external_table_ddl(
            BigQueryTableRef("project", "dataset", "events"),
            pa.schema([pa.field("id", pa.int64()), pa.field("date", pa.date32())]),
            ExternalTableSpec(
                source_uris=["gs://silver/events/*"],
                hive_uri_prefix="gs://silver/events",
                partition_columns=(("date", "DATE"),),
                reference_file_schema_uri="gs://silver/events/date=2026-07-17/final.parquet",
            ),
        )


def test_bigquery_namespace_uses_requested_parquet_reference_file(monkeypatch) -> None:
    """The Example 07 namespace path must forward its final Parquet reference."""
    from schema_sanitizer.integrations.bigquery import namespace_ops
    from schema_sanitizer.integrations.bigquery.external_table import (
        ExternalTableSpec,
    )
    from schema_sanitizer.integrations.bigquery.table_ref import BigQueryTableRef

    reference_uri = "gs://silver/events/date=2026-07-17/final.parquet"
    base_spec = ExternalTableSpec(
        source_uris=["gs://silver/events/*"],
        hive_uri_prefix="gs://silver/events",
        partition_columns=(("date", "DATE"),),
    )
    seen_specs = []

    monkeypatch.setattr(namespace_ops, "import_bigquery_adbc", lambda: (object(), object()))
    monkeypatch.setattr(namespace_ops, "bigquery_db_kwargs_from_namespace", lambda *_: {})
    monkeypatch.setattr(namespace_ops, "external_table_spec_from_namespace", lambda *_: base_spec)

    def fake_ddl(_table_ref, _schema, spec, **_kwargs):
        """Capture the validation DDL spec."""
        seen_specs.append(spec)
        return "DDL", []

    def fake_create(**kwargs):
        """Capture the execution DDL spec."""
        seen_specs.append(kwargs["spec"])
        return []

    monkeypatch.setattr(namespace_ops, "external_table_ddl", fake_ddl)
    monkeypatch.setattr(
        namespace_ops,
        "create_or_replace_external_table_from_schema",
        fake_create,
    )

    namespace_ops.create_or_replace_external_bigquery_table_from_namespace(
        SimpleNamespace(column_order="alphabetically"),
        BigQueryTableRef("project", "dataset", "events"),
        object(),
        reference_file_schema_uri=reference_uri,
    )

    assert len(seen_specs) == 2
    assert all(spec.reference_file_schema_uri == reference_uri for spec in seen_specs)


@pytest.mark.parametrize("sort_fields_alphabetically", [False, True])
def test_bigquery_external_table_ddl_keeps_etl_columns_last(
    sort_fields_alphabetically: bool,
) -> None:
    """Verify BigQuery external table DDL keeps etl columns last."""
    pa = _pyarrow()
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
    """Verify BigQuery registry sidecar partition queries."""
    from schema_sanitizer.integrations.bigquery.advanced import (
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
    """Verify BigQuery registry sidecar fetch fast path and missing fallback."""
    from schema_sanitizer.integrations.bigquery import (
        BigQueryTableRef,
        fetch_latest_schema_registry,
    )

    class FakeCursor:
        """Minimal cursor returning configured BigQuery query results."""

        def __init__(self, dbapi):
            """Initialize fake cursor state for dbapi and result."""
            self._dbapi = dbapi
            self._result = None

        def __enter__(self):
            """Return the managed fake cursor value from context entry."""
            return self

        def __exit__(self, _exc_type, _exc, _tb):
            """Finalize the fake cursor context without suppressing exceptions."""
            return False

        def execute(self, query):
            """Record the submitted SQL statement and return the recording cursor."""
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
            """Return the configured single-row database result."""
            if self._result is None:
                return None
            return (self._result,)

    class FakeConnection:
        """Minimal connection returning fake cursors."""

        def __init__(self, dbapi):
            """Initialize fake connection state for dbapi."""
            self._dbapi = dbapi

        def __enter__(self):
            """Return the managed fake connection value from context entry."""
            return self

        def __exit__(self, _exc_type, _exc, _tb):
            """Finalize the fake connection context without suppressing exceptions."""
            return False

        def cursor(self):
            """Return the recording database cursor used by this scenario."""
            return FakeCursor(self._dbapi)

    class FakeDbapi:
        """Minimal DB-API facade for sidecar fetch tests."""

        def __init__(self, *, table_type, sidecar_partition):
            """Initialize fake dbapi state for table type, sidecar partition, and queries."""
            self.table_type = table_type
            self.sidecar_partition = sidecar_partition
            self.queries = []

        def connect(self, *, db_kwargs):
            """Return the configured recording database connection."""
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
    """Verify BigQuery registry sidecar update logs create and upsert."""
    from schema_sanitizer.integrations.bigquery import (
        BigQueryTableRef,
        update_registry_sidecar_table,
    )

    class FakeCursor:
        """Minimal cursor for sidecar update logging tests."""

        def __init__(self, dbapi):
            """Initialize fake cursor state for dbapi and result."""
            self._dbapi = dbapi
            self._result = None

        def __enter__(self):
            """Return the managed fake cursor value from context entry."""
            return self

        def __exit__(self, _exc_type, _exc, _tb):
            """Finalize the fake cursor context without suppressing exceptions."""
            return False

        def execute(self, query):
            """Record the submitted SQL statement and return the recording cursor."""
            self._dbapi.queries.append(query)
            if "INFORMATION_SCHEMA.TABLES" in query:
                self._result = self._dbapi.table_type
            else:
                self._result = None

        def fetchone(self):
            """Return the configured single-row database result."""
            if self._result is None:
                return None
            return (self._result,)

    class FakeConnection:
        """Minimal fake BigQuery connection."""

        def __init__(self, dbapi):
            """Initialize fake connection state for dbapi."""
            self._dbapi = dbapi

        def __enter__(self):
            """Return the managed fake connection value from context entry."""
            return self

        def __exit__(self, _exc_type, _exc, _tb):
            """Finalize the fake connection context without suppressing exceptions."""
            return False

        def cursor(self):
            """Return the recording database cursor used by this scenario."""
            return FakeCursor(self._dbapi)

    class FakeDbapi:
        """Minimal DB-API facade for sidecar update tests."""

        def __init__(self, table_type):
            """Initialize fake dbapi state for table type and queries."""
            self.table_type = table_type
            self.queries = []

        def connect(self, *, db_kwargs):
            """Return the configured recording database connection."""
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


def test_external_table_spec_resolves_partition_location_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Spec construction computes partition columns and URI prefixes once."""
    calls = 0
    original = external_table_owner.external_table_hive_uri_prefix

    def counted_prefix(**kwargs: object) -> str:
        """Return a prefix object that records string coercion."""
        nonlocal calls
        calls += 1
        return original(**kwargs)

    monkeypatch.setattr(external_table_owner, "external_table_hive_uri_prefix", counted_prefix)
    spec = external_table_owner.external_table_spec_from_namespace(
        SimpleNamespace(
            silver_parquet_prefix="gs://bucket/table",
            partition_granularity="hourly",
            external_table_source_uri=None,
        )
    )

    assert calls == 1
    assert spec.hive_uri_prefix == "gs://bucket/table"
    assert spec.source_uris == ["gs://bucket/table/*"]
    assert spec.partition_columns[-1] == ("hour", "INT64")
