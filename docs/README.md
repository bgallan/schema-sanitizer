# schema-sanitizer documentation

The main [README](../README.md) is a short introduction. This directory contains
the detailed guides and contracts.

## Index

- [Start here](#start-here)
- [Data and schemas](#data-and-schemas)
- [Operations](#operations)
- [Security and compatibility](#security-and-compatibility)
- [Hardening audit records](#hardening-audit-records)
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
- [CI/CD pipeline](ci-cd.md): shared validation gates, artifacts, and
  publication.
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

## [Hardening audit records](#index)

The implementation audit trail is kept with the documentation instead of at
the repository root. These records describe internal invariants rather than
public API:

- [Pass 47](memory-concurrency-hardening-pass47.md), [48](memory-concurrency-hardening-pass48.md),
  [49](memory-concurrency-hardening-pass49.md), [50](memory-concurrency-hardening-pass50.md),
  [51](memory-concurrency-hardening-pass51.md), [52](memory-concurrency-hardening-pass52.md),
  [53](memory-concurrency-hardening-pass53.md), [54](memory-concurrency-hardening-pass54.md),
  [55](memory-concurrency-hardening-pass55.md), [56](memory-concurrency-hardening-pass56.md),
  [58](memory-concurrency-hardening-pass58.md), [59](memory-concurrency-hardening-pass59.md),
  [60](memory-concurrency-hardening-pass60.md), and [61](memory-concurrency-hardening-pass61.md).
- [Pass 66](memory-concurrency-hardening-pass66.md), [67](memory-concurrency-hardening-pass67.md),
  [68](memory-concurrency-hardening-pass68.md), [69](memory-concurrency-hardening-pass69.md),
  [70](memory-concurrency-hardening-pass70.md), [71](memory-concurrency-hardening-pass71.md),
  [72](memory-concurrency-hardening-pass72.md), [73](memory-concurrency-hardening-pass73.md),
  [74](memory-concurrency-hardening-pass74.md), [75](memory-concurrency-hardening-pass75.md),
  [76](memory-concurrency-hardening-pass76.md), [77](memory-concurrency-hardening-pass77.md),
  [78](memory-concurrency-hardening-pass78.md), [79](memory-concurrency-hardening-pass79.md),
  [80](memory-concurrency-hardening-pass80.md), [81](memory-concurrency-hardening-pass81.md),
  [82](memory-concurrency-hardening-pass82.md), [83](memory-concurrency-hardening-pass83.md),
  [84](memory-concurrency-hardening-pass84.md), [85](memory-concurrency-hardening-pass85.md),
  and [86](memory-concurrency-hardening-pass86.md).

## [Project](#index)

- [Development](development.md): environment, tests, builds, benchmarks, and CI.

Modules ending in `_impl` and native implementation details are not public API,
even when a document explains their invariants for auditing purposes.
