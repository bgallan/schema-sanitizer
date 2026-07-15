# Changelog

## Unreleased

### Changed

- Preflight every selected partition before additive Example 07 writes so
  numeric promotion is settled before the first Parquet file is committed.
  Integer-only early partitions now materialize as `DOUBLE` when a later
  partition widens the same field to float.
- Consolidated public resource configuration under `memory_limit_bytes` and
  propagated derived native budgets through ingestion, materialization,
  Parquet, remote scheduling, and output paths.
- Added repository-wide environment-independence checks and removed runtime,
  build, test, example, and workflow environment overrides.

### Security

- Added non-bypassable Arrow, schema, parser, Parquet reader/writer, allocator,
  scratch-retention, and remote-scheduler limits.
- Added ASan/UBSan, native fuzz campaigns, deterministic crash regressions,
  cloud-emulator coverage, and real-service smoke workflows.

### Packaging

- Added downstream sdist-to-wheel validation, optional-extra smoke tests,
  cross-platform ABI3 wheel checks, source distribution cleanliness checks,
  and pinned static zlib parity.

## [0.3.6] - 2026-07-14

### Added

- Cross-platform native Parquet compression parity for GZIP, Snappy, and
  uncompressed output.
- Digest-pinned bundled zlib builds for Windows, Linux, and macOS wheels.
- ASan/UBSan, libFuzzer, Python branch coverage, LLVM coverage, downstream
  package validation, cloud-emulator integration, and durable benchmark CI.

### Changed

- Snappy output now uses copy commands for real compression rather than valid
  literal-only blocks.
- Source distributions reject caches, build trees, and compiled scratch files.
