# Compatibility contract

This document defines which surfaces applications may rely on and what changes
to expect while `schema-sanitizer` remains in the 0.x series.

## Index

- [Project status](#project-status)
- [Runtime platforms](#runtime-platforms)
- [Optional dependencies](#optional-dependencies)
- [Public Python API](#public-python-api)
- [Deterministic behavior](#deterministic-behavior)
- [Formats and input selection](#formats-and-input-selection)
- [Schema registries](#schema-registries)
- [Output publication](#output-publication)
- [BigQuery state](#bigquery-state)
- [Errors and diagnostics](#errors-and-diagnostics)

## [Project status](#index)

The package is alpha software. A 0.x minor release may change APIs when that
simplifies the contract or fixes a security or correctness problem. Release
notes must describe any required migration.

Aliases for retired names and modules are not retained. Documentation always
uses the current path, and consumers are expected to update to it.

## [Runtime platforms](#index)

Release wheels use the CPython 3.11 stable ABI and support CPython 3.11, 3.12,
3.13, and 3.14.

The published matrix covers:

- Windows AMD64;
- Linux x86-64 on the manylinux 2.28 baseline;
- macOS 11 or newer on x86-64 and Apple Silicon.

Linux ARM64 and musllinux are not commitments until release CI publishes those
wheels regularly.

Temporary-path cleanup uses descriptor-relative, no-follow filesystem
operations on POSIX. Windows does not expose the same `dir_fd` primitives, so
it uses bounded `scandir` traversal with before/after fingerprints and never
descends into link entries. The Windows wheel tests this fallback directly;
POSIX race, FIFO, hard-link, and `fork` attack contracts run on Linux and macOS.

## [Optional dependencies](#index)

The core has no mandatory Python dependency. Current minimums are declared in
`pyproject.toml`:

- PyArrow 14;
- pandas 2;
- Polars 0.20;
- DuckDB 1;
- cloud clients selected by the `gcs`, `s3`, `azure`, or `cloud` extras;
- PyArrow and BigQuery ADBC through the `bigquery` extra.

The installed release metadata is the exact dependency reference.

## [Public Python API](#index)

Only surfaces listed in the [Python API guide](python-api.md) are public:

- names exported by `schema_sanitizer`;
- `schema_sanitizer.sources`;
- `schema_sanitizer.pipeline` and `schema_sanitizer.pipeline.advanced`;
- `schema_sanitizer.integrations.bigquery` and its `advanced` namespace.

The `api_impl`, `core_impl`, `input_impl`, `options_impl`, `remote_impl`, and
`adapters` packages are internal. Their names, signatures, and file layout may
change without notice.

`threading_mode` is also internal. The public concurrency control is the
`multi_threading` boolean. The only public resource control is
`memory_limit_bytes`; queues, packets, workers, staging, and per-stage limits are
derived internally.

## [Deterministic behavior](#index)

Single- and multi-threaded modes must produce the same schema, logical order,
diagnostics, and first failure. Concurrency may change timings and memory peaks,
not the logical result.

There is no fixed worker ceiling. Effective width depends on CPU capacity,
memory budget, process pressure, and available work. A concrete thread count is
therefore not part of the contract.

## [Formats and input selection](#index)

`input_format` is explicit for paths and URIs. It is never inferred from an
extension or file contents. `input_mode="directory"` is non-recursive and uses a
deterministic order.

`csv_header_mode="exact"` remains the default. The opt-in `union` mode accepts
additive or reordered headers without accepting duplicates or post-sanitization
collisions.

GCS manifests freeze every `(uri, generation)` identity. Downloading a different
generation or silently relisting the prefix would violate the contract.

## [Schema registries](#index)

A registry written by a published release must remain readable by later
releases in the same major line. Readers ignore unknown additive keys. Changing
the meaning or type of an existing key requires an explicit migration.

The canonical schema and field-name policy remain authoritative. A registry
without a usable canonical schema produces an explicit error or fallback; it is
never silently reinterpreted as another contract.

## [Output publication](#index)

Local outputs are built in a sibling staging file and replace the destination
only after the writer closes successfully. Remote outputs are uploaded after
local conversion completes.

This prevents partial publication during normal failures. It is not a power-loss
durability guarantee unless the filesystem and application add their own
`fsync` policy.

## [BigQuery state](#index)

External Parquet data retains the authoritative registry. The sidecar stores
only `last_ingested_partition`; when it is missing, invalid, or unavailable,
the integration falls back to searching the external table.

The sidecar key and Hive partition order are persistent formats. Changing them
requires an explicit migration.

## [Errors and diagnostics](#index)

Public exception classes distinguish invalid arguments, integrity failures,
memory, resources, cancellation, and imports. Messages may improve between
releases; applications should branch on exception types and structured fields,
not parse free-form text.

Statistics dictionaries may gain fields. Consumers must ignore unknown keys.
