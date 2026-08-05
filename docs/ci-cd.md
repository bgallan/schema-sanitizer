# CI/CD pipeline

The repository has one validation workflow and one publication wrapper. Pull
requests, merges to `main`, and publication all execute the same validation
definition.

## Index

- [Workflow model](#workflow-model)
- [Validation triggers](#validation-triggers)
- [Validation jobs](#validation-jobs)
- [Build and artifact flow](#build-and-artifact-flow)
- [Publication](#publication)
- [Local reproduction](#local-reproduction)
- [Maintenance rules](#maintenance-rules)

## [Workflow model](#index)

| Workflow | Role | Side effects |
|---|---|---|
| `.github/workflows/ci.yml` | Canonical release validation | Builds and uploads run artifacts only. |
| `.github/workflows/publish.yml` | Manual publication wrapper | May upload the validated artifacts to TestPyPI or PyPI. |

`publish.yml` calls `ci.yml` with `workflow_call`; it does not reimplement or
shorten validation. Consequently, a candidate cannot reach a package index
through a different test path from the one used by pull requests and `main`.

## [Validation triggers](#index)

The canonical workflow runs on:

- non-draft pull-request updates (`opened`, `synchronize`, `reopened`, and
  `ready_for_review`);
- every push to `main`, including a merged pull request;
- manual `workflow_dispatch` runs;
- `workflow_call` from the publication workflow.

Every trigger executes the same jobs and matrices. There are no event-specific
reduced gates. Superseded pull-request runs are cancelled through a concurrency
group; `main` and publish runs are never cancelled automatically.

Python dependency caches are keyed from `pyproject.toml` and shared by
compatible runner/Python combinations. Build products are not cached: release
artifacts are always rebuilt from the checked-out commit.

## [Validation jobs](#index)

The workflow has six auditable job definitions:

| Job | Coverage |
|---|---|
| `checks` | Pre-commit, dependency audit, Bandit, secret scan, benchmark smoke, contextual Python branch coverage, and risk report. |
| `platform-wheels` | Release ABI3 wheels and full test suite on Linux x86-64, Windows AMD64, macOS x86-64, and macOS ARM64; also tests Python 3.14 compatibility. |
| `distribution` | Version validation, sdist build, complete release-set checks, rebuild from sdist, and isolated downstream installation of every optional extra. |
| `coverage-native` | LLVM branch/line coverage over regular, adversarial, and integration domains. |
| `platform-sanitizers` | Linux ASan/UBSan full-extension tests plus native fuzzing, Windows ASan native fuzzing, and macOS x86-64/ARM64 ASan/UBSan probes. |
| `thread-sanitizer` | GCC ThreadSanitizer fuzzing and full-extension concurrency domains. |

The platform jobs are matrices, but each concern has one owner. Python coverage
shares the already built quality environment, and the sdist is built inside the
release-artifact job rather than passed through an extra job.

## [Build and artifact flow](#index)

Each `platform-wheels` matrix entry builds its release wheel once. That same
wheel is installed without optional adapters, certified for the Parquet
contract, exercised by the complete suite with adapters, smoke-benchmarked, and
loaded under the newest supported CPython.

The `distribution` job waits for all platform wheels and then:

1. validates `meta/VERSION`;
1. builds and checks the sdist;
1. downloads the four release wheels;
1. validates filenames, versions, contents, and metadata as one release set;
1. rebuilds a wheel from the sdist;
1. runs isolated downstream installation checks.

Published artifacts are therefore exactly the objects produced by validation:

| Artifact | Producer | Consumer |
|---|---|---|
| `dist-wheels-*` | Platform wheel matrix | Distribution validation and publish. |
| `dist-sdist` | Distribution job | Publish. |
| `python-branch-coverage` | Checks job | Human review and coverage tooling. |
| `native-llvm-coverage` | Native coverage job | Human review and coverage tooling. |
| `platform-evidence-*` | Platform wheel matrix | Performance and Parquet-contract review. |

Publication never rebuilds a wheel or sdist.

## [Publication](#index)

Publishing is manual and restricted to the `main` branch. The operator chooses
`check-only`, `testpypi`, or `pypi` and may provide a release tag.

The workflow performs three phases:

1. validate the branch, version, tag, and request;
1. call the complete canonical validation workflow;
1. after all jobs succeed, download `dist-*` artifacts and upload them with
   PyPI Trusted Publishing.

An actual upload additionally requires the exact confirmation phrase
`publish schema-sanitizer`. The publish job alone receives `id-token: write`;
validation jobs retain read-only repository permissions.

Use `check-only` to exercise the exact release gate without external side
effects.

## [Local reproduction](#index)

Run the fast source and Python gates with:

```bash
python -m pip install -e ".[dev]"
pre-commit run --all-files
pytest -q
```

Build release artifacts with:

```bash
python -m build --sdist
python -m cibuildwheel --output-dir wheelhouse
python meta/ci/check_distribution_contents.py --release-set \
  dist/*.tar.gz wheelhouse/*.whl
```

Sanitizer, fuzzing, coverage, and downstream scripts live in `meta/ci/`. Their
arguments in `ci.yml` are the authoritative release configuration.

## [Maintenance rules](#index)

- Add release-blocking checks to `ci.yml`, never directly to `publish.yml`.
- Do not use `pull_request_target` for repository code from forks.
- Build each release artifact once and reuse it for tests and publication.
- Prefer a matrix when platforms exercise the same contract.
- Keep one job owner per concern; merge jobs that require the same environment
  and split jobs only when toolchains or failure domains differ materially.
- Update this document and CI contract tests whenever triggers, jobs, matrices,
  or artifact names change.
