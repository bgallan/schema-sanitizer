# Flat-prefix CSV ingestion by modification time

This workflow is for a GCS prefix whose CSV objects are not partitioned into
Hive-style date directories. It lists the prefix once, freezes the exact object
versions in immutable manifests, assigns each object to one UTC day from its
GCS `updated` timestamp, reconciles heterogeneous CSV headers, and publishes
one validated Parquet object per non-empty day.

The complete executable reference is
`examples/example_08/08_gcs_csv_modified_window_to_polars_parquet.py`.

## Window semantics

Command-line `--start-date` and `--end-date` are inclusive UTC calendar dates.
Internally each date becomes a half-open window:

```text
[start at 00:00:00Z, next day at 00:00:00Z)
```

An object updated exactly at the start belongs to that day. An object updated
exactly at the end belongs to the next day and is excluded from the earlier
window. Naive datetimes are rejected; aware datetimes are normalized to UTC
before comparison. One completed listing can be reused to build any number of
non-overlapping daily manifests without another network listing.

Selection is based on object modification time, not a timestamp inside the CSV
and not the object name. Rewriting an object creates a new GCS generation and
may also move it into a different modification-time window.

## Immutable GCS generations

Every selected GCS object must have a non-empty `generation`. A
`SourceManifest` stores `(uri, generation)` identities in deterministic order.
Staging requests the exact generation with a matching generation precondition.
An object replaced after discovery therefore cannot silently change the bytes
processed by the run.

If the listed generation is deleted before download, the operation fails. It
does not fall back to the newest generation. Reusing the same manifest for
inference and materialization therefore preserves the same source snapshot.

## Late arrivals and reruns

The listing is a point-in-time snapshot. Objects created or rewritten after the
single listing are not visible to that run, even when their `updated` timestamp
would fall inside a requested day. Modification time also cannot express an
event-time correction for data whose business date differs from its upload
date.

Production scheduling should use an explicit late-arrival policy, for example:

- rerun a bounded lookback of recent UTC days;
- publish deterministic daily object names so a successful rerun replaces the
  complete day rather than appending a partial result;
- persist the listing/run watermark and operational metrics outside the data;
- choose the lookback from the source system's observed delivery delay.

A rerun performs a new listing and may intentionally select newer generations.
A previously persisted manifest instead reproduces its original generations.

## CSV header reconciliation

Use `csv_header_mode="union"` when files for one day can reorder columns, omit
question columns, or introduce new ones. The engine pre-reads each header,
builds immutable per-source projections, preserves first-appearance column
order, and emits nulls for missing fields. Duplicate headers, mixed
header/no-header sources, and rows wider than their own header remain errors.

The default `csv_header_mode="exact"` is unchanged and continues to reject
header mismatches. Existing local paths, directories, URIs, partition plans,
and file converters keep their previous behavior unless `SourceManifest` or
`union` is selected explicitly.

## Analytical memory versus file-output memory

`to_polars`, `to_pyarrow`, `to_pandas`, and `to_duckdb` bound parsing,
inference, staging, and native materialization with `memory_limit_bytes`, but
the final analytical object returned to Python is outside the operation ledger.
A large daily dataframe can therefore exhaust process memory even though the
reader itself stays within budget. Example 8 intentionally uses `to_polars`
because it performs a custom vectorized wide-to-nested transformation; its day
size must be chosen with that dataframe risk in mind.

Direct file converters such as `to_parquet` do not retain a final analytical
table and are the bounded-memory choice when no custom dataframe transform is
required. `iter_batches` avoids building one complete table, but retained
batches become caller-owned memory and can still accumulate.

Example 8 writes the transformed dataframe to a local temporary Parquet,
reopens it, validates schema and row count, and only then uploads the complete
object. The BigQuery external table is replaced only after all requested days
have published successfully, so validation failures do not expose a partial
run.

## Minimal invocation

```bash
pip install "schema-sanitizer[polars,pyarrow,cloud]" adbc-driver-bigquery[dbapi]

python examples/example_08/08_gcs_csv_modified_window_to_polars_parquet.py \
  --source-csv-prefix gs://raw-bucket/responses \
  --silver-parquet-prefix gs://silver-bucket/responses \
  --start-date 2026-07-01 \
  --end-date 2026-07-07 \
  --target-table project_id.dataset_id.external_responses \
  --omit-null-answers \
  --memory-limit-bytes 268435456 \
  --multi-threading
```

The target table schema is the final analytical contract. The example derives
an ingress scalar schema for the wide CSVs, normalizes headers matching
`<integer>/<question text>` into one `list<struct>` column, replaces the
intermediate registry metadata, validates the final schema, and publishes
`YYYY-MM-DD.parquet` for every non-empty UTC day.
