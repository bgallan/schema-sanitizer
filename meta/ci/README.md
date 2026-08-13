# CI helper ownership

`meta/ci` contains repository-internal entry points used by GitHub Actions,
pre-commit, CMake, packaging tests, and release audits. It is not a public
Python package: invoke a helper through the path recorded by its owning
workflow or configuration file.

## Areas

| Directory | Owner | Typical entry points |
|---|---|---|
| [`quality/`](quality/) | Source, secret, cleanup, coverage, and runner-evidence policy | `check_detect_secrets_report.py`, `check_primary_cleanup.py`, `record_runner_environment.py`, `report_risk_coverage.py` |
| [`native/`](native/) | Native source and binary linkage policy | `check_no_arrow_cpp.sh`, `check_no_libarrow_linkage.sh`, the manual CMake documentation target |
| [`parquet/`](parquet/) | Installed-wheel and runtime Parquet certification | compression and fail-closed contract suites |
| [`fuzz/`](fuzz/) | Corpus integrity and bounded regression campaigns | `check_fuzz_corpus.py`, `run_fuzz_regressions.py` |
| [`sanitizers/`](sanitizers/) | ASan/UBSan/TSan process launch and orchestration | CPython launchers and the TSan extension suite |
| [`release/`](release/) | Distribution identity, downstream installation, provenance, and PyPI preflight | archive checker, release manifest, isolated consumer checks, version and remote-main checks |
| [`requirements/`](requirements/) | Reproducible CI-only dependency sets and their cache identity | pinned platform-test adapters, quality tools, and isolated downstream extras |

The reader-limit evidence aggregator is a benchmark analysis tool rather than
a CI gate and therefore lives at
[`benchmarks/readers/review_limits.py`](../../benchmarks/readers/review_limits.py).

## Invocation contract

- [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml) is the canonical
  automated caller. Its `validation-matrix` dispatches the `quality`,
  `source-distribution`, `native-llvm-coverage`, `thread-sanitizer`, and
  `platform-sanitizer` workloads through repository-owned composite actions.
  The manual publication workflow reuses it unchanged.
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

CI package downloads use bounded pip retries and complete exact locks. Apt-owned
toolchains add repository, connection, and dpkg-lock bounds. Release preflight
retries transport failures, HTTP 429, server errors, and HTTP 403 only with an
official GitHub rate-limit header; its server-requested delay is capped at 30
seconds per attempt, while semantic client errors fail immediately. Platform
and release-set consumers retry artifact downloads only after clearing their
exact partial destination. Intermediate wheels and the sdist are retained
for seven days so delayed failed-job reruns can still consume their producers.
The canonical sdist encodes the checked-out commit time through
`SOURCE_DATE_EPOCH`, which its archive validator checks explicitly.

## Local checks

From the repository root:

```bash
python meta/ci/fuzz/check_fuzz_corpus.py
python meta/ci/quality/check_primary_cleanup.py
python meta/ci/release/validate_release_version.py
python -m pytest -q tests/quality/test_ci_helper_layout.py
pre-commit run --all-files
```

Release publication additionally requires the PyPI Trusted Publisher
configuration documented in [`docs/project/ci-cd.md`](../../docs/project/ci-cd.md).
The current workflow grants OIDC only to its final job and does not attach that
job to a GitHub Environment, so the Trusted Publisher's optional environment is
unset.
