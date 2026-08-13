"""Documentation, compatibility, and CI contracts for modified-time CSV flows."""

from __future__ import annotations

import inspect
import re
from pathlib import Path

import schema_sanitizer as ss

ROOT = Path(__file__).resolve().parents[2]
CONVERTERS = (
    ss.iter_batches,
    ss.to_pyarrow,
    ss.to_pandas,
    ss.to_polars,
    ss.to_duckdb,
    ss.to_csv,
    ss.to_jsonl,
    ss.to_parquet,
)


def _read(relative: str) -> str:
    """Read one UTF-8 project file."""
    return (ROOT / relative).read_text(encoding="utf-8")


def test_example_08_is_linked_from_both_documentation_indexes() -> None:
    """The executable workflow remains discoverable from both main indexes."""
    readme = _read("README.md")
    examples = _read("examples/README.md")
    path = "examples/example_08/08_gcs_csv_modified_window_to_polars_parquet.py"

    assert path in readme
    assert "example_08/08_gcs_csv_modified_window_to_polars_parquet.py" in examples
    assert "flat GCS prefix" in readme
    assert "Example 08: flat GCS CSV prefix by modification time" in examples


def test_flat_prefix_guide_documents_snapshot_and_window_contracts() -> None:
    """UTC boundaries, immutable generations, and late arrivals stay explicit."""
    guide = _read("docs/guides/flat-prefix-modified-time-csv.md")

    for required in (
        "inclusive UTC calendar dates",
        "half-open window",
        "[start at 00:00:00Z, next day at 00:00:00Z)",
        "(uri, generation)",
        "matching generation precondition",
        "does not fall back to the newest generation",
        "point-in-time snapshot",
        "rerun a bounded lookback",
        "persisted manifest",
    ):
        assert required in guide


def test_memory_guidance_distinguishes_analytical_and_file_outputs() -> None:
    """Users must not mistake the native ledger for a dataframe-size limit."""
    guide = _read("docs/guides/flat-prefix-modified-time-csv.md")
    readme = _read("README.md")
    readme_words = " ".join(readme.split())
    examples = _read("examples/README.md")

    for document in (guide, readme, examples):
        assert "memory_limit_bytes" in document
    assert "PyArrow, pandas, and Polars outputs become caller-owned" in guide
    assert "A lazy DuckDB relation instead retains its governed" in guide
    assert "upstream chain until the final related proxy closes" in guide
    assert "result lifetime contract" in guide
    assert "Direct file converters such as `to_parquet`" in guide
    assert "returned dataframe is caller-owned" in examples
    assert "direct file output is the safe choice" in readme_words


def test_example_08_documents_parameterized_hive_output() -> None:
    """Source selection and row partitioning remain distinct and discoverable."""
    guide = _read("docs/guides/flat-prefix-modified-time-csv.md")
    examples = _read("examples/README.md")
    guide_words = " ".join(guide.split())

    for document in (guide, examples):
        assert "--partition-timestamp-column event_timestamp" in document
        assert "--parquet-file-prefix records" in document
        assert "year=<Y>/month=<M>/day=<D>" in document
    assert "records_20260701_20260703.gz.parquet" in guide
    assert "object modification time" in guide
    assert "configurable data timestamp" in guide
    assert "Aware timestamps are converted to UTC" in guide
    assert "path fields are not serialized into Parquet" in guide
    assert "Several source windows can therefore coexist" in guide
    assert "publication of multiple objects is not one atomic transaction" in guide_words


def test_exact_mode_and_existing_inputs_remain_default_compatible() -> None:
    """Every public converter keeps exact CSV reconciliation as its default."""
    compatibility = _read("docs/reference/compatibility.md")

    for converter in CONVERTERS:
        parameter = inspect.signature(converter).parameters["csv_header_mode"]
        assert parameter.default == "exact"
    assert 'csv_header_mode="exact"' in compatibility
    assert '`input_mode="directory"` is non-recursive' in compatibility
    assert "GCS manifests freeze every `(uri, generation)` identity" in compatibility


def test_docs_do_not_introduce_numbered_design_files() -> None:
    """Stable topics own docs instead of pass-, phase-, or version-numbered files."""
    docs_root = ROOT / "docs"
    forbidden = re.compile(r"(?:^|[-_])(?:pass|phase|version|v)[-_]?\d", re.IGNORECASE)
    offenders = sorted(
        path.relative_to(docs_root).as_posix()
        for path in docs_root.rglob("*.md")
        if forbidden.search(path.stem)
    )

    assert offenders == []


def test_modified_time_csv_tests_use_topic_oriented_modules() -> None:
    """Completed implementation stages must not return as numbered test modules."""
    pipeline_tests = ROOT / "tests/pipeline"
    numbered_stage = re.compile(r"(?:^|_)phase\d+(?:_|$)", re.IGNORECASE)
    numbered = sorted(
        path.name for path in pipeline_tests.glob("test_*.py") if numbered_stage.search(path.stem)
    )

    assert numbered == []


def test_consolidated_ci_owns_modified_time_csv_validation() -> None:
    """Focused suites run inside the existing compact CI and sanitizer jobs."""
    workflow = _read(".github/workflows/ci.yml")
    quality = _read(".github/actions/quality-validation/action.yml")
    platform_sanitizer = _read(".github/actions/platform-sanitizer/action.yml")
    source_distribution = _read(".github/actions/source-distribution/action.yml")
    platform_tests = _read(".github/actions/test-platform-wheel/action.yml")
    tsan_runner = _read("meta/ci/sanitizers/run_tsan_extension_suite.sh")
    native_probe = _read("cpp/tests/ordered_executor_tsan.cc")

    focused_suites = (
        "tests/pipeline/test_modified_time_csv_discovery.py",
        "tests/pipeline/test_modified_time_csv_planning.py",
        "tests/pipeline/test_source_manifest_inputs.py",
        "tests/pipeline/test_csv_header_modes.py",
        "tests/pipeline/test_csv_union_projection.py",
        "tests/pipeline/test_modified_time_csv_schema.py",
        "tests/pipeline/test_example_08_orchestration.py",
        "tests/pipeline/test_example_08_fake_cloud.py",
        "tests/pipeline/test_modified_time_csv_contracts.py",
    )

    validation_sources = workflow + quality + platform_sanitizer + source_distribution
    for suite in focused_suites:
        assert suite in validation_sources
    assert (validation_sources + tsan_runner).count(
        "tests/pipeline/test_csv_union_projection.py"
    ) >= 3
    assert "tests/pipeline" in platform_tests
    assert "pytest -q -o pythonpath=." in platform_tests
    assert "check_downstream_install.py" in source_distribution
    assert "tests/pipeline/test_csv_union_projection.py" in tsan_runner
    assert '#include "ordered_executor_tsan_csv_projection.cc.inc"' in native_probe
