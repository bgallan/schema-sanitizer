# CI helper ownership

`meta/ci` contains repository-internal entry points used by GitHub Actions,
pre-commit, CMake, packaging tests, and release audits. It is not a public
Python package: invoke a helper through the path recorded by its owning
workflow or configuration file.

## Areas

| Directory | Owner | Typical entry points |
|---|---|---|
| [`quality/`](quality/) | Source, secret, cleanup, and coverage policy | `check_detect_secrets_report.py`, `check_primary_cleanup.py`, `report_risk_coverage.py` |
| [`native/`](native/) | Native source and binary linkage policy | `check_no_arrow_cpp.sh`, `check_no_libarrow_linkage.sh`, the manual CMake documentation target |
| [`parquet/`](parquet/) | Installed-wheel and runtime Parquet certification | compression and fail-closed contract suites |
| [`fuzz/`](fuzz/) | Corpus integrity and bounded regression campaigns | `check_fuzz_corpus.py`, `run_fuzz_regressions.py` |
| [`sanitizers/`](sanitizers/) | ASan/UBSan/TSan process launch and orchestration | CPython launchers and the TSan extension suite |
| [`release/`](release/) | Distribution identity, downstream installation, provenance, and PyPI preflight | archive checker, release manifest, isolated consumer checks, environment/version checks |

The reader-limit evidence aggregator is a benchmark analysis tool rather than
a CI gate and therefore lives at
[`benchmarks/readers/review_limits.py`](../../benchmarks/readers/review_limits.py).

## Invocation contract

- [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml) is the canonical
  automated caller. The manual publication workflow reuses it unchanged.
- [`.pre-commit-config.yaml`](../../.pre-commit-config.yaml) owns the local
  cleanup and fuzz-integrity gates.
- [`CMakeLists.txt`](../../CMakeLists.txt) exposes
  `schema_sanitizer_check_cpp_documentation` as an optional developer target;
  it is not a release-blocking CI lane.
- [`pyproject.toml`](../../pyproject.toml) invokes the Parquet compression check
  from the wheel-test environment.

Helpers that import siblings stay together in `release/`: the manifest uses
the archive validator, the PyPI check uses the version validator, and the
downstream installer launches its smoke and type-check programs by path.
Do not add compatibility wrappers at the `meta/ci` root. Update every
versioned caller atomically when moving or renaming an entry point.

## Local checks

From the repository root:

```bash
python meta/ci/fuzz/check_fuzz_corpus.py
python meta/ci/quality/check_primary_cleanup.py
python meta/ci/release/validate_release_version.py
python -m pytest -q tests/quality/test_ci_helper_layout.py
pre-commit run --all-files
```

Release publication additionally requires the protected GitHub and PyPI
configuration documented in [`docs/project/ci-cd.md`](../../docs/project/ci-cd.md).
