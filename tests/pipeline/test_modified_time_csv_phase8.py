"""Phase-8 documentation, compatibility, and consolidated-CI contracts."""

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
    guide = _read("docs/flat-prefix-modified-time-csv.md")

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
    guide = _read("docs/flat-prefix-modified-time-csv.md")
    readme = _read("README.md")
    readme_words = " ".join(readme.split())
    examples = _read("examples/README.md")

    for document in (guide, readme, examples):
        assert "memory_limit_bytes" in document
    assert "final analytical object returned to Python is outside" in guide
    assert "Direct file converters such as `to_parquet`" in guide
    assert "returned dataframe is caller-owned" in examples
    assert "direct file output is the safe choice" in readme_words


def test_exact_mode_and_existing_inputs_remain_default_compatible() -> None:
    """Every public converter keeps exact CSV reconciliation as its default."""
    compatibility = _read("docs/compatibility.md")

    for converter in CONVERTERS:
        parameter = inspect.signature(converter).parameters["csv_header_mode"]
        assert parameter.default == "exact"
    assert 'csv_header_mode="exact"' in compatibility
    assert '`input_mode="directory"` is non-recursive' in compatibility
    assert "GCS manifests freeze every `(uri, generation)` identity" in compatibility


def test_docs_do_not_introduce_version_numbered_design_files() -> None:
    """Stable topics own docs instead of phase- or version-numbered design files."""
    forbidden = re.compile(r"(?:^|[-_])(?:phase|version|v)[-_]?\d", re.IGNORECASE)
    offenders = [path.name for path in (ROOT / "docs").glob("*.md") if forbidden.search(path.stem)]

    assert offenders == []


def test_consolidated_ci_owns_modified_time_csv_validation() -> None:
    """Focused suites run inside the existing compact CI and sanitizer jobs."""
    workflow = _read(".github/workflows/ci.yml")
    tsan_runner = _read("meta/ci/run_tsan_extension_suite.sh")
    native_probe = _read("cpp/tests/ordered_executor_tsan.cc")

    for phase in range(1, 9):
        assert f"tests/pipeline/test_modified_time_csv_phase{phase}.py" in workflow
    assert "tests/pipeline/test_modified_time_csv_phase7_integration.py" in workflow
    assert workflow.count("tests/pipeline/test_modified_time_csv_phase5.py") >= 3
    assert "pytest -q -o pythonpath=." in workflow
    assert "check_downstream_install.py" in workflow
    assert "tests/pipeline/test_modified_time_csv_phase5.py" in tsan_runner
    assert '#include "ordered_executor_tsan_csv_projection.cc.inc"' in native_probe
