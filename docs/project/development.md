# Development

## Index

- [Environment](#environment)
- [Checks](#checks)
- [Native build](#native-build)
- [Benchmarks](#benchmarks)
- [Security work](#security-work)
- [CI](#ci)
- [Documentation changes](#documentation-changes)

## [Environment](#index)

Install development dependencies and build the native extension in editable
mode:

```bash
python -m pip install -e ".[dev]"
```

The project requires CPython 3.11 or newer, a C++23 compiler, CMake 4.3, and a
build backend supported by scikit-build-core. Ninja is recommended for local
native work.

## [Checks](#index)

Run the complete Python suite and all repository checks:

```bash
pytest -q
pre-commit run --all-files
```

Tests are grouped by domain under [`tests/`](../../tests/README.md):

```bash
pytest -q tests/io
pytest -q tests/concurrency
pytest -q tests/memory
pytest -q tests/parquet
pytest -q tests/remote
```

Do not remove deterministic single-threaded coverage when adding a parallel
path. Concurrency tests should compare logical output, diagnostics, ordering,
and error selection across both modes.

## [Native build](#index)

Build the standalone CMake target while iterating on C++:

```bash
cmake -S . -B build/dev -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build/dev --parallel
```

Build a distributable wheel with:

```bash
python -m build --wheel
```

Release wheels use CPython's 3.11 stable ABI and bundle pinned zlib for a
consistent Parquet compression matrix.

## [Benchmarks](#index)

Benchmarks live in [`benchmarks/`](../../benchmarks/). Start with a small smoke run
before increasing rows, width, workers, or memory:

```bash
python -m benchmarks.ingestion.cli --rows 100 --width 4 --repeats 1
```

Performance changes must preserve schema, drift, error-order, and memory
contracts. Record enough context to reproduce the input shape, mode, worker
policy, memory limit, and platform.

## [Security work](#index)

Reader changes should be checked against:

- [Reader security limits](../operations/reader-security-limits.md);
- [Resource and concurrency accounting](../operations/resources-and-concurrency.md);
- [Reader complexity](../operations/reader-complexity.md);
- fuzz regressions and sanitizer jobs in CI.

Do not include source values, credentials, or unbounded payloads in public
errors or operation diagnostics.

## [CI](#index)

The [CI/CD pipeline guide](ci-cd.md) explains the shared PR, merge, and publish
gate. Workflows under [`.github/workflows/`](../../.github/workflows/) and the
owned helper map under [`meta/ci/`](../../meta/ci/README.md) are the source of truth. CI covers:

- lint, formatting, typing, and tests;
- native builds and ABI3 wheel installation;
- Linux, Windows, and macOS packaging;
- address, undefined-behavior, and thread sanitizers where supported;
- reader security and fuzz regressions;
- remote fault handling and Parquet contracts.

Keep jobs broad enough to catch platform-specific native failures while
avoiding duplicate jobs that exercise the same contract.

## [Documentation changes](#index)

Keep the root [README](../../README.md) introductory. Detailed contracts belong in
this directory and should be linked from [the documentation index](../README.md).
Avoid release-pass histories in user documentation; retain only current
behavior and migration-relevant compatibility information.
