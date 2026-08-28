#!/usr/bin/env python3
"""Run one centrally declared Python or native coverage suite.

It resolves centrally declared test selections and launches the requested Python or
native coverage workflow.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

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


def selected_tests(profile: str, suite: str, *, root: Path = Path(".")) -> tuple[str, ...]:
    """Return one stable suite after checking that every selection exists."""
    paths = SUITES[profile][suite]
    missing = [path for path in paths if not (root / path).is_file()]
    if missing:
        raise FileNotFoundError(f"missing {profile}/{suite} coverage tests: {missing}")
    return paths


def run(profile: str, suite: str) -> None:
    """Execute one suite with the instrumentation appropriate to its profile."""
    tests = selected_tests(profile, suite)
    if profile == "python":
        command = [
            sys.executable,
            "-m",
            "coverage",
            "run",
            "--source=src/schema_sanitizer",
            "--branch",
            "--parallel-mode",
            f"--context={suite}",
            "-m",
            "pytest",
            "-q",
        ]
    else:
        command = [sys.executable, "-m", "pytest", "-q"]
    subprocess.run([*command, *tests], check=True)


def main() -> None:
    """Parse and execute a declared suite."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", choices=tuple(SUITES))
    parser.add_argument("suite", choices=("regular", "adversarial", "integration"))
    args = parser.parse_args()
    run(args.profile, args.suite)


if __name__ == "__main__":
    main()
