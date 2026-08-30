# CI helper ownership

`meta/ci` contains repository-internal entry points used by GitHub Actions,
pre-commit, CMake, packaging tests, and release audits. It is not a public
Python package: invoke a helper through the path recorded by its owning
workflow or configuration file.

## Areas

| Directory | Owner | Typical entry points |
|---|---|---|
| [`quality/`](quality/) | Source, secret, cleanup, coverage, and runner-evidence policy | `check_detect_secrets_report.py`, `check_primary_cleanup.py`, `record_runner_environment.py`, `report_risk_coverage.py`, `run_coverage_suite.py` |
| [`native/`](native/) | Native source and binary linkage policy | `check_no_arrow_cpp.sh`, `check_no_libarrow_linkage.sh`, the manual CMake documentation target |
| [`parquet/`](parquet/) | Installed-wheel and runtime Parquet certification | compression and fail-closed contract suites |
| [`fuzz/`](fuzz/) | Corpus integrity and bounded regression campaigns | `check_fuzz_corpus.py`, `run_fuzz_regressions.sh`, `run_fuzz_regressions.py` |
| [`sanitizers/`](sanitizers/) | ASan/UBSan/TSan process launch and orchestration | CPython launchers and the TSan extension suite |
| [`release/`](release/) | Distribution identity, downstream installation, provenance, and PyPI preflight | archive checker, release manifest, isolated consumer checks, version and remote-main checks |
| [`requirements/`](requirements/) | Reproducible CI-only dependency sets and their cache identity | exact build, hook, platform-test, quality, downstream, and release-verification environments |

The reader-limit evidence aggregator is a benchmark analysis tool rather than
a CI gate and therefore lives at
[`benchmarks/readers/review_limits.py`](../../benchmarks/readers/review_limits.py).

## Invocation contract

- [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml) is the canonical
  automated caller. Its `validation-matrix` dispatches the `quality`,
  `source-distribution`, `native-llvm-coverage`, `thread-sanitizer`, and
  `platform-sanitizer` workloads through repository-owned composite actions.
  The manual publication workflow reuses it unchanged.
- [`.github/actions/restore-pip-cache/action.yml`](../../.github/actions/restore-pip-cache/action.yml)
  owns optional validation-cache identity. Its caller supplies one workload
  owner, exact Python patch, and the complete dependency-input files; the action
  adds the runner operating system and architecture and hashes those inputs
  before accessing an exact cache key.
- [`.pre-commit-config.yaml`](../../.pre-commit-config.yaml) owns the local
  cleanup and fuzz-integrity gates.
- [`CMakeLists.txt`](../../CMakeLists.txt) exposes
  `schema_sanitizer_check_cpp_documentation` as an optional developer target;
  its Clang AST audit validates maintained native file summaries and named
  callables, but is not a release-blocking CI lane. Running it requires matching
  `clang++`, `llvm-config`, and `libclang-cpp` development tools; run it on each
  supported platform or configuration to cover conditional source branches.
- [`pyproject.toml`](../../pyproject.toml) invokes the Parquet compression check
  from the wheel-test environment.

Helpers that import siblings stay together in `release/`: the manifest uses
the archive validator, the PyPI check uses the version validator, and the
downstream installer launches its smoke and type-check programs by path.
Do not add compatibility wrappers at the `meta/ci` root. Update every
versioned caller atomically when moving or renaming an entry point.

CI package downloads use bounded pip retries and complete exact owner locks.
`build-tools.txt` constrains ordinary and isolated builds, including
cibuildwheel's container and test environment; `pre-commit-hooks.txt` constrains
each independently bootstrapped hook environment. The dependency audit evaluates
each real lock separately and statically rejects a declared project or CI tool
without a compatible owner pin, rather than resolving a floating synthetic union.
Validation pip caches are disposable download accelerators, not environments:
their exact keys include workload, operating system, architecture, Python patch,
and a digest over that workload's lock-owning inputs, without partial restore
prefixes. Installation and `pip check` remain authoritative after every restore.
The exact pre-commit environment cache follows the same boundary; every hook is
still bootstrapped and run, and a failed bootstrap clears only its owned cache
before retrying.
Apt-owned toolchains add repository, connection, and dpkg-lock bounds. Release preflight
retries transport failures, HTTP 429, server errors, and HTTP 403 only with an
official GitHub rate-limit header; its server-requested delay is capped at 30
seconds per attempt, while semantic client errors fail immediately. Platform
and release-set consumers retry artifact downloads only after clearing their
exact partial destination. Intermediate wheels, the sdist, and the audited
`release-distributions` set are retained for seven days. This keeps delayed
failed-job reruns possible without granting artifact-deletion permissions or
modifying a completed run.
The canonical sdist encodes the checked-out commit time through
`SOURCE_DATE_EPOCH`, which its archive validator checks explicitly. CI builds it
twice from clean owned directories and requires byte-identical archives before
downstream validation.

The downstream installer creates one environment per published extra through
the pinned `virtualenv` app-data seeder. Its source-distribution owner prepares
exactly one SHA-256-verified pip wheel; environment creation is offline, uses
copies rather than links, and checks the interpreter patch, pointer width, and
pip version before installation. App-data and environment roots are distinct,
owned cleanup locations, while the wheel, constraints, scripts, seed directory,
and generated command file must remain outside them.

Native build acceleration stays outside `meta/ci` helper semantics. Production
wheel actions alone enable target-private Release PCH profiles, while sanitizer,
coverage, and include-hygiene graphs explicitly keep PCH disabled. Linux
ASan/UBSan installs the extension and builds its executor and fuzz targets from
one named, configuration-certified CMake graph. Windows may restore the exact
CPython NuGet package, but verifies its digest, link safety, AMD64 PE identity,
interpreter patch, and pointer width before cibuildwheel may consume it. None of
these paths remove a test, sanitizer target, platform cell, or release artifact;
cache availability and runner speed never determine a gate result.

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
