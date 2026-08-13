# schema-sanitizer documentation

The main [README](../README.md) is a short introduction. The documentation is
organized by task so public guides, operational contracts, and implementation
invariants remain easy to distinguish.

## Index

- [Guides](#guides)
- [Reference](#reference)
- [Operations](#operations)
- [Internals](#internals)
- [Project](#project)

## [Guides](#index)

- [Getting started](guides/getting-started.md): installation, a first conversion,
  and choosing an output.
- [Partitioned pipelines](guides/partitioned-pipelines.md): high-level pipelines,
  planning, ordering, and lookahead.
- [Modification-time CSV](guides/flat-prefix-modified-time-csv.md): immutable GCS
  selections and UTC windows over a flat prefix.

## [Reference](#index)

- [Python API](reference/python-api.md): functions, reusable configuration,
  results, streaming, cancellation, and analytical helpers.
- [Options](reference/options.md): parameters, defaults, and configured API
  mappings.
- [Inputs and filesystems](reference/inputs-and-filesystems.md): formats,
  directories, cloud providers, staging, and publication.
- [Schemas and registries](reference/schema-and-registry.md): inference, CSV
  headers, manifests, final schemas, evolution, and drift records.
- [BigQuery](reference/bigquery.md): external-table DDL, registries, and sidecars.
- [Compatibility](reference/compatibility.md): platforms, public APIs, formats,
  and serialized state.

## [Operations](#index)

- [Resources and concurrency](operations/resources-and-concurrency.md): memory,
  temporary storage, workers, cancellation, and shutdown.
- [Reader security limits](operations/reader-security-limits.md): per-format
  ceilings and the threat model.
- [Reader complexity](operations/reader-complexity.md): algorithmic guarantees
  and scaling evidence.

## [Internals](#index)

- [Concurrency lifecycle](internals/concurrency-lifecycle.md): stable ownership,
  admission, publication, retirement, and teardown invariants.
- [Execution heuristics](internals/execution-heuristics.md): adaptive task arenas,
  packet sizing, output routes, and remote staging.

Modules ending in `_impl` and native implementation details are not public API,
even when an internal document explains their invariants for auditing purposes.

## [Project](#index)

- [Development](project/development.md): environment, tests, native builds, and
  benchmarks.
- [CI/CD](project/ci-cd.md): validation gates, artifacts, and publication.
