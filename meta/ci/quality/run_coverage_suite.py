#!/usr/bin/env python3
"""Run one centrally declared Python or native coverage suite.

It resolves centrally declared test selections and launches the requested Python or
native coverage workflow.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Sequence

SHARED = {
    "adversarial": (
        "tests/io/test_contracts.py",
        "tests/concurrency/test_runtime_stream_materialization_and_registry_probes.py",
        "tests/concurrency/test_runtime_registry_round_trips_and_result_materialization.py",
        "tests/concurrency/test_runtime_resource_close_and_finalization.py",
        "tests/sinks/test_streaming_writer_lifecycle.py",
        "tests/remote/test_async_remote_scheduler.py",
        "tests/remote/test_remote_http_fault_injection.py",
    ),
    "integration": (
        "tests/io/test_input_remote_plans.py",
        "tests/remote/test_cloud_emulator_configuration.py",
        "tests/remote/test_bigquery_integration.py",
        "tests/remote/test_remote_multipart_uploads.py",
        "tests/pipeline/test_example_08_orchestration.py",
        "tests/pipeline/test_example_08_fake_cloud.py",
    ),
}

SUITES = {
    "python": {
        "regular": (
            "tests/io/test_api.py",
            "tests/io/test_input_python_and_local.py",
            "tests/io/test_input_xml_and_public_api.py",
            "tests/sinks/test_sinks_csv_jsonl.py",
            "tests/pipeline/test_modified_time_csv_discovery.py",
            "tests/pipeline/test_modified_time_csv_planning.py",
            "tests/pipeline/test_source_manifest_inputs.py",
            "tests/pipeline/test_csv_header_modes.py",
            "tests/pipeline/test_csv_union_projection.py",
            "tests/pipeline/test_modified_time_csv_schema.py",
            "tests/pipeline/test_modified_time_csv_contracts.py",
        ),
        **SHARED,
    },
    "native": {
        "regular": (
            "tests/io/test_api.py",
            "tests/io/test_input_xml_and_public_api.py",
            "tests/sinks/test_sinks_csv_jsonl.py",
            "tests/pipeline/test_csv_header_modes.py",
            "tests/pipeline/test_csv_union_projection.py",
        ),
        "adversarial": SHARED["adversarial"],
        "integration": SHARED["integration"]
        + (
            "tests/pipeline/test_modified_time_csv_discovery.py",
            "tests/pipeline/test_modified_time_csv_planning.py",
            "tests/pipeline/test_source_manifest_inputs.py",
            "tests/pipeline/test_modified_time_csv_schema.py",
            "tests/pipeline/test_modified_time_csv_contracts.py",
        ),
    },
}
_COVERAGE_DIRECTORY = Path(".work/coverage")


def selected_tests(profile: str, suite: str, *, root: Path = Path(".")) -> tuple[str, ...]:
    """Return one stable suite after checking that every selection exists."""
    paths = SUITES[profile][suite]
    missing = [path for path in paths if not (root / path).is_file()]
    if missing:
        raise FileNotFoundError(f"missing {profile}/{suite} coverage tests: {missing}")
    return paths


def coverage_data_file(profile: str, suite: str) -> Path:
    """Return the deterministic input file consumed by coverage combine."""
    return _COVERAGE_DIRECTORY / f".coverage.{profile}.{suite}"


def validate_coverage_inputs(profile: str) -> tuple[Path, ...]:
    """Require the exact regular files produced by every declared profile suite."""
    expected = tuple(coverage_data_file(profile, suite) for suite in SUITES[profile])
    actual = (
        tuple(sorted(_COVERAGE_DIRECTORY.glob(".coverage.*")))
        if _COVERAGE_DIRECTORY.is_dir()
        else ()
    )
    if actual != tuple(sorted(expected)):
        missing = sorted(set(expected) - set(actual))
        unexpected = sorted(set(actual) - set(expected))
        raise RuntimeError(
            f"coverage inputs for {profile} differ: missing={missing}, unexpected={unexpected}"
        )
    invalid = [path for path in expected if path.is_symlink() or not path.is_file()]
    if invalid:
        raise RuntimeError(f"coverage inputs must be regular files: {invalid}")
    return expected


def coverage_command(profile: str, suite: str) -> tuple[str, ...]:
    """Return one validated coverage command as an immutable argument vector."""
    tests = selected_tests(profile, suite)
    if profile == "python":
        command = (
            sys.executable,
            "-m",
            "coverage",
            "run",
            "--source=src/schema_sanitizer",
            "--branch",
            f"--data-file={coverage_data_file(profile, suite).as_posix()}",
            f"--context={suite}",
            "-m",
            "pytest",
            "-q",
        )
    else:
        command = (sys.executable, "-m", "pytest", "-q")
    return (*command, *tests)


def run(profile: str, suite: str) -> None:
    """Replace this helper with the selected argv-only coverage command."""
    if profile == "python":
        data_file = coverage_data_file(profile, suite)
        if data_file.is_symlink() or (data_file.exists() and not data_file.is_file()):
            raise ValueError(f"coverage data output must be a regular file: {data_file}")
        if data_file.parent.is_symlink():
            raise ValueError(
                f"coverage output root must be a regular directory: {data_file.parent}"
            )
        data_file.parent.mkdir(parents=True, exist_ok=True)
    command = coverage_command(profile, suite)
    os.execv(command[0], command)


def main(argv: Sequence[str] | None = None) -> None:
    """Parse and execute a declared suite."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", choices=tuple(SUITES))
    parser.add_argument("suite", nargs="?", choices=("regular", "adversarial", "integration"))
    parser.add_argument("--validate-inputs", action="store_true")
    args = parser.parse_args(argv)
    if args.validate_inputs:
        if args.suite is not None:
            parser.error("suite cannot be combined with --validate-inputs")
        validate_coverage_inputs(args.profile)
        return
    if args.suite is None:
        parser.error("suite is required unless --validate-inputs is used")
    run(args.profile, args.suite)


if __name__ == "__main__":
    main()
