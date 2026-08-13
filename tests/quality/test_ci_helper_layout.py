"""Keep CI helper ownership thematic, explicit, and free of root-level scripts."""

from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CI_ROOT = ROOT / "meta" / "ci"

EXPECTED_HELPERS = {
    "fuzz/check_fuzz_corpus.py",
    "fuzz/run_fuzz_regressions.py",
    "native/check_cpp_documentation.py",
    "native/check_no_arrow_cpp.sh",
    "native/check_no_libarrow_linkage.sh",
    "parquet/check_parquet_compression_matrix.py",
    "parquet/check_parquet_contract_runtime.py",
    "parquet/check_parquet_contract_runtime_suite.py",
    "quality/check_detect_secrets_report.py",
    "quality/check_primary_cleanup.py",
    "quality/record_runner_environment.py",
    "quality/report_risk_coverage.py",
    "release/check_distribution_contents.py",
    "release/check_downstream_install.py",
    "release/check_github_release_environment.py",
    "release/check_pypi_version.py",
    "release/downstream_smoke.py",
    "release/downstream_typecheck.py",
    "release/release_manifest.py",
    "release/validate_release_version.py",
    "sanitizers/asan_python_launcher.cc",
    "sanitizers/run_tsan_extension_suite.sh",
    "sanitizers/tsan_python_launcher.cc",
}


def test_ci_helpers_are_grouped_by_owner() -> None:
    """All executable helpers live in one known owner directory."""
    helpers = {
        path.relative_to(CI_ROOT).as_posix()
        for path in CI_ROOT.rglob("*")
        if path.is_file() and path.suffix in {".cc", ".py", ".sh"}
    }

    assert helpers == EXPECTED_HELPERS
    assert {path.name for path in CI_ROOT.iterdir() if path.is_file()} == {"README.md"}
    assert {
        path.name for path in CI_ROOT.iterdir() if path.is_dir() and path.name != "__pycache__"
    } == {
        "fuzz",
        "native",
        "parquet",
        "quality",
        "release",
        "requirements",
        "sanitizers",
    }


def test_ci_shell_entry_points_remain_executable() -> None:
    """Moved shell gates retain the executable bit expected by workflows."""
    scripts = tuple(CI_ROOT.rglob("*.sh"))

    assert scripts
    assert all(os.access(script, os.X_OK) for script in scripts)


def test_retired_source_zip_pipeline_stays_absent() -> None:
    """The obsolete ZIP chain must not return beside the canonical sdist flow."""
    retired = {
        "check_cmake_sources_exist.sh",
        "check_zip_contains_cmake_sources.sh",
        "create_source_zip.sh",
    }

    assert not any(path.name in retired for path in CI_ROOT.rglob("*"))
