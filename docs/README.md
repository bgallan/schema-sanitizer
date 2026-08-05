# schema-sanitizer documentation

The main [README](../README.md) is a short introduction. This directory contains
the detailed guides and contracts.

## Index

- [Start here](#start-here)
- [Data and schemas](#data-and-schemas)
- [Operations](#operations)
- [Security and compatibility](#security-and-compatibility)
- [Project](#project)

## [Start here](#index)

- [Getting started](getting-started.md): installation, a first conversion, and
  choosing an output.
- [Python API](python-api.md): functions, reusable configuration, results,
  streaming, cancellation, and analytical helpers.
- [Options](options.md): complete parameter and default-value reference.

## [Data and schemas](#index)

- [Inputs and filesystems](inputs-and-filesystems.md): formats, directories,
  cloud providers, staging, and publication.
- [Heuristics](heuristics.md): inference, names, types, depth, evolution,
  registries, drift records, and adaptive execution.
- [CSV header modes](csv-header-modes.md): `exact` and `union` contracts.
- [Source manifests](source-manifests.md): immutable remote selections.
- [Final analytical schemas](analytical-schema-finalization.md): separating wide
  ingress schemas from normalized outputs.

## [Operations](#index)

- [Partitioned pipelines](pipelines.md): the high-level API and advanced
  primitives.
- [Modification-time CSV](flat-prefix-modified-time-csv.md): UTC windows over a
  flat GCS prefix.
- [BigQuery](bigquery.md): external-table DDL, registries, and sidecars.
- [Concurrency and memory](concurrency-memory-hardening.md): budgets, workers,
  cancellation, and shutdown.
- [Reader memory accounting](reader-memory-accounting.md): what the global
  limit owns.

## [Security and compatibility](#index)

- [Reader security limits](reader-security-limits.md): per-format limits and the
  threat model.
- [Reader complexity](reader-complexity.md): algorithmic guarantees.
- [Compatibility](compatibility.md): platforms, public APIs, formats, and
  serialized state.

## [Project](#index)

- [Development](development.md): environment, tests, builds, benchmarks, and CI.

Modules ending in `_impl` and native implementation details are not public API,
even when a document explains their invariants for auditing purposes.
