# Compatibility contract

## Runtime platforms

The release artifacts support:

- CPython 3.11, 3.12, 3.13, and 3.14 through one ABI3 wheel per platform;
- Windows AMD64;
- Linux x86-64 on the manylinux 2.28 baseline;
- macOS x86-64 and Apple Silicon.

Linux ARM64 and musllinux remain conditional targets and are not compatibility
commitments until release CI publishes those wheels.

## Optional dependencies

The core package has no mandatory Python dependency. Supported adapter ranges
begin at PyArrow 14, pandas 2, Polars 0.20, and DuckDB 1. Cloud operation uses
the minimum versions declared in `pyproject.toml`; release CI validates each
extra independently. New minimums require a minor release and changelog entry.

## Public API

Public names exported from `schema_sanitizer` and documented pipeline APIs
follow the deprecation policy in `RELEASING.md`. Modules below `api_impl`,
`core_impl`, `input_impl`, `options_impl`, `remote_impl`, and `adapters` are
internal unless explicitly documented otherwise.

Memory and resource configuration has one public control:
`memory_limit_bytes`. Internal chunk, batch, spool, concurrency, Arrow, and
Parquet limits are derived by the native extension. Process-environment
overrides and the former independent resource options are not part of the
compatibility contract.

## Serialized schema registries

A registry document written by a released version must remain readable by later
minor and patch releases in the same major line. Readers must ignore unknown
additive keys. Removing or changing the meaning/type of an existing key is a
major-version change. Writers may add optional keys in a minor release.

The canonical schema and field-name policy remain authoritative. A registry
that cannot supply a usable canonical schema must produce an explicit fallback
or validation result; it must not be silently reinterpreted as a different
schema contract.

## BigQuery sidecar state

The sidecar table is keyed by `external_table_name` and stores
`last_ingested_partition`. Re-running creation or an upsert is idempotent, and a
failure after table creation can be resumed safely. Additive nullable columns
are permitted in minor releases. Renaming/removing these columns or changing
the partition-key encoding requires a major release or an explicit migration.

The external Parquet data remains the source of truth. Missing, invalid, or
unavailable sidecar state falls back to scanning embedded registry metadata.
