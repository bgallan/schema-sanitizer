# BigQuery integration

`schema_sanitizer.integrations.bigquery` turns a final PyArrow schema into
explicit external-table DDL and maintains optional schema-registry lookup state.
Install it with:

```bash
pip install "schema-sanitizer[bigquery]"
```

The application supplies ADBC connections. The package does not keep a hidden
global BigQuery client.

## Index

- [Table references and DDL](#table-references-and-ddl)
- [Arrow schema translation](#arrow-schema-translation)
- [Embedded registry lookup](#embedded-registry-lookup)
- [Registry sidecar](#registry-sidecar)
- [Curated API](#curated-api)
- [Advanced API](#advanced-api)
- [Safe sequencing](#safe-sequencing)

## [Table references and DDL](#index)

```python
from schema_sanitizer.integrations.bigquery import (
    BigQueryTableRef,
    ExternalTableSpec,
    external_table_ddl,
)

table_ref = BigQueryTableRef("my-project", "analytics", "events")
spec = ExternalTableSpec(
    source_uris=["gs://silver/events/*"],
    hive_uri_prefix="gs://silver/events",
    partition_columns=(
        ("year", "INT64"),
        ("month", "INT64"),
        ("date", "DATE"),
    ),
)

ddl, skipped_partition_fields = external_table_ddl(
    table_ref,
    final_arrow_schema,
    spec,
)
```

`parse_table_ref("project.dataset.table")` provides the same validated model.
`create_or_replace_external_table_from_schema` executes the generated DDL using
an application-provided DB-API driver (`dbapi`) and its connection parameters
(`db_kwargs`); the helper opens and closes the connection and cursor.

The DDL removes fields supplied by Hive path partitioning and preserves the
physical root order. If `sort_fields_alphabetically=True`, only source fields
are sorted. In both modes the generated ETL fields remain at the root tail in
their canonical order:

```text
schema_registry, schema_drifts, source_file, ingestion_timestamp
```

## [Arrow schema translation](#index)

`resolve_bigquery_arrow_schema` selects the final Arrow schema used for DDL.
It prefers a usable schema-sanitizer registry and falls back to the table's
declared schema only when no canonical registry is available.
`read_external_table_arrow_schema` reads that table shape through the
application-provided, duck-typed client. The client must expose `get_table`;
this is distinct from the DB-API driver used to execute DDL.

Translation rules include:

- dictionaries use their value type;
- structs become BigQuery `STRUCT`/`RECORD`;
- lists become `ARRAY`/`REPEATED`;
- maps become arrays of key/value structs;
- Arrow null and unsupported soft-fallback types become `STRING` with a warning;
- Hive partition fields are excluded from the physical Parquet schema.

See [Schema and registry](schema-and-registry.md#generated-etl-fields) for the
upstream generated-field order.

## [Embedded registry lookup](#index)

`fetch_latest_schema_registry` retrieves the latest non-null embedded registry.
Without a sidecar, ordering uses ingestion timestamp, registry generation, and
configured Hive partition columns.

The external data remains authoritative. A missing or invalid result must not
be replaced with an invented schema.

## [Registry sidecar](#index)

The optional native BigQuery sidecar avoids scanning a large external table
only to locate its latest registry. It stores one pointer per external table:

```sql
external_table_name STRING NOT NULL
last_ingested_partition STRING NOT NULL
```

The pointer follows Hive key order:

```text
year=2026/month=08/date=2026-08-05
year=2026/month=08/date=2026-08-05/hour=09
```

Lookup proceeds as follows:

1. Verify that the sidecar exists and is a native `BASE TABLE`.
1. Read the target external-table row.
1. Parse and validate the Hive key.
1. Query embedded registry data in that partition.
1. Fall back to a normal external-table scan if any step fails.

After Parquet publication and external-table update succeed,
`update_registry_sidecar_table` creates the sidecar if necessary and executes
an idempotent `MERGE`. The pointer records the last partition completed by that
run, not necessarily the chronologically greatest partition after a historical
rerun. Losing the sidecar only makes lookup slower; the embedded registry
remains authoritative.

## [Curated API](#index)

The normal namespace intentionally contains only:

- `BigQueryTableRef` and `parse_table_ref`;
- `ExternalTableSpec`, `external_table_ddl`, and
  `create_or_replace_external_table_from_schema`;
- `read_external_table_arrow_schema` and `resolve_bigquery_arrow_schema`;
- `fetch_latest_schema_registry`;
- `update_registry_sidecar_table`.

## [Advanced API](#index)

Lower-level helpers live only in
`schema_sanitizer.integrations.bigquery.advanced`. They cover:

- Arrow-to-BigQuery type and column conversion;
- identifier, string, type, and format normalization;
- raw SQL execution and information-schema inspection;
- Hive partition parsing, formatting, URI derivation, and coverage checks;
- external-table option construction;
- CLI namespace-to-configuration adapters used by the examples;
- partition-key encoding and registry query construction;
- sidecar DDL, lookup, and `MERGE` SQL.

Use the curated API for application code unless custom SQL orchestration needs
one of those primitives. Advanced names are not duplicated at the package root.

## [Safe sequencing](#index)

A production run should:

1. Discover and freeze inputs.
1. Bootstrap the current registry.
1. Write and verify Parquet outputs.
1. Create or replace the external table from the final schema.
1. Update the sidecar last.

If a run fails before the final step, the existing sidecar remains usable. If
sidecar update fails, the next bootstrap falls back to the external data.
