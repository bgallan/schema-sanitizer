# Ingress and final analytical schemas

Modified-time CSV ingestion has two deliberately different schema contracts.

The **ingress schema** describes only stable scalar columns that can be read
directly from each wide CSV row. Dynamic event columns are discovered by
`csv_header_mode="union"` and remain temporary. Nested target fields such as
`event: list<struct<...>>` do not belong to the CSV ingress registry.
Generated columns (`schema_registry`, `schema_drifts`, `source_file`, and
`ingestion_timestamp`) are also excluded from the ingress projection because
the reader creates them.

The **final schema** describes the normalized analytical result that will be
published. It includes nested fields and the generated provenance columns, but
must not contain any raw dynamic event header.

## Index

- [Public helpers](#public-helpers)

## [Public helpers](#index)

```python
import schema_sanitizer as ss

final_registry = ss.schema_registry_from_arrow_schema(final_schema)
restored_schema = ss.arrow_schema_from_schema_registry(final_registry)
ingress_schema = ss.project_ingress_scalar_schema(final_schema)

finalized = ss.finalize_analytical_output(
    normalized_frame,
    final_schema,
    field_name_policy="preserve",
)
ss.validate_analytical_result(finalized.clean_data, final_schema)
```

`finalize_analytical_output` replaces the intermediate wide-CSV
`schema_registry` and `schema_drifts` values with metadata generated from the
normalized final schema. It preserves `source_file` and
`ingestion_timestamp`, rejects extra raw columns, and returns both the finalized
data and the regenerated registry documents.

For BigQuery external tables, use the existing embedded-registry reader first.
`resolve_bigquery_arrow_schema` accepts that registry and falls back to the
table's declared schema through a duck-typed BigQuery client only when no
canonical schema-sanitizer registry is available.
