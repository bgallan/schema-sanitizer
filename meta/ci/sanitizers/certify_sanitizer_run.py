#!/usr/bin/env python3
"""Create and verify provenance-bound sanitizer evidence certificates.

The certificate closes platform-specific blind spots by recording the exact
sanitizer policy and authenticating bounded subprocess evidence where a native
platform executes the concurrency probe and instrumented extension.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
import tomllib
from pathlib import Path
from typing import Any, Mapping

CERTIFICATE_FORMAT = "schema-sanitizer-sanitizer-run-v2"
WATCHDOG_FORMAT = "schema-sanitizer-sanitizer-watchdog-v1"
WINDOWS_TOOLCHAIN_FORMAT = "schema-sanitizer-windows-toolchain-certificate-v1"
WINDOWS_TOOLCHAIN_FILENAME = "windows-toolchain-Windows-X64.json"
WINDOWS_TOOLCHAIN_POLICY = (
    Path(__file__).resolve().parents[1] / "native/windows-release-toolchain.json"
)
PROJECT_CONFIGURATION = Path(__file__).resolve().parents[3] / "pyproject.toml"
_GIT_SHA = re.compile(r"[0-9a-f]{40}(?:[0-9a-f]{24})?")
_ASAN_NO_LEAKS = "detect_leaks=0:halt_on_error=1:strict_string_checks=1"
_ASAN_WITH_LEAKS = "detect_leaks=1:halt_on_error=1:strict_string_checks=1"
_LSAN_FAIL_FAST = "exitcode=23"
_TSAN_FAIL_FAST = "halt_on_error=1:history_size=7:second_deadlock_stack=1"
_UBSAN_FAIL_FAST = "halt_on_error=1:print_stacktrace=1"
EXPECTED_RUNS = {
    ("asan-ubsan", "linux-full", "Linux", "X64"),
    ("asan", "native", "Windows", "X64"),
    ("asan-ubsan", "native", "macOS", "X64"),
    ("asan-ubsan", "native", "macOS", "ARM64"),
    ("tsan", "thread", "Linux", "X64"),
}
_LINUX_EXTENSION_TESTS = (
    "tests/io/test_input_python_and_local.py",
    "tests/schema/test_options_bytes.py",
    "tests/concurrency/test_runtime_stream_materialization_and_registry_probes.py",
    "tests/concurrency/test_runtime_registry_round_trips_and_result_materialization.py",
    "tests/concurrency/test_runtime_resource_close_and_finalization.py",
    "tests/parquet/test_parquet_direct_nested_projection.py",
    "tests/parquet/test_parquet_native_nested_cases.py::test_native_nested_case[native_parquet_stream_materializes_list_map_nested_struct_list_values]",
    "tests/parquet/test_parquet_native_nested_cases.py::test_native_nested_case[native_parquet_stream_materializes_list_map_list_struct_values]",
    "tests/parquet/test_parquet_native_nested_cases.py::test_native_nested_case[native_parquet_stream_materializes_list_map_with_struct_values]",
    "tests/parquet/test_parquet_native_nested_cases.py::test_native_nested_case[native_parquet_stream_materializes_list_map_with_nested_struct_values]",
    "tests/parquet/test_parquet_native_nested_cases.py::test_native_nested_case[native_parquet_stream_materializes_list_map_with_struct_list_values]",
    "tests/parquet/test_parquet_native_nested_cases.py::test_native_nested_case[native_parquet_stream_materializes_list_map_with_struct_list_chain_values]",
    "tests/parquet/test_parquet_native_nested_cases.py::test_native_nested_case[native_parquet_stream_materializes_list_list_struct_values]",
    "tests/parquet/test_parquet_native_nested_cases.py::test_native_nested_case[native_parquet_stream_materializes_deep_list_chain_struct_values]",
    "tests/parquet/test_parquet_native_recursive_cases.py::test_native_recursive_case[native_parquet_stream_materializes_adversarial_recursive_struct_siblings]",
    "tests/parquet/test_parquet_native_recursive_cases.py::test_native_recursive_case[native_parquet_stream_projects_recursive_shapes_across_row_groups]",
    "tests/parquet/test_parquet_native_recursive_cases.py::test_native_recursive_case[native_parquet_stream_materializes_recursive_null_empty_matrix]",
    "tests/parquet/test_parquet_native_recursive_cases.py::test_native_recursive_case[native_parquet_stream_materializes_generated_extreme_recursive_shapes]",
    "tests/parquet/test_parquet_native_recursive_cases.py::test_native_recursive_case[native_parquet_stream_materializes_generated_recursive_shape_fuzzer]",
    "tests/pipeline/test_csv_union_projection.py",
)


def _canonical_json(payload: Mapping[str, Any]) -> str:
    """Serialize sanitizer evidence with canonical whitespace and key order."""
    return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def _regular_file(path: Path, label: str) -> None:
    """Require one evidence input to be a non-symlinked regular file."""
    if path.is_symlink() or not path.is_file():
        raise AssertionError(f"{label} must be a regular file: {path}")


def _write_atomically(destination: Path, content: str) -> None:
    """Write one certificate atomically without following output symlinks."""
    if destination.is_symlink() or (destination.exists() and not destination.is_file()):
        raise AssertionError(f"sanitizer certificate output is unsafe: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def _runtime_environment(
    sanitizer: str,
    mode: str,
    runner_os: str,
    kind: str,
) -> dict[str, str]:
    """Return the complete ambient-proof sanitizer environment for one process."""
    identity = (sanitizer, mode, runner_os)
    if identity == ("asan-ubsan", "linux-full", "Linux"):
        if kind not in {"extension", "fuzz"}:
            raise ValueError(f"unsupported Linux sanitizer process kind: {kind}")
        return {
            "ASAN_OPTIONS": _ASAN_WITH_LEAKS if kind == "fuzz" else _ASAN_NO_LEAKS,
            "LSAN_OPTIONS": _LSAN_FAIL_FAST if kind == "fuzz" else "",
            "TSAN_OPTIONS": "",
            "UBSAN_OPTIONS": _UBSAN_FAIL_FAST,
        }
    if identity in {
        ("asan", "native", "Windows"),
        ("asan-ubsan", "native", "macOS"),
    }:
        allowed_kinds = {"concurrency", "extension", "fuzz"}
        if runner_os == "macOS":
            allowed_kinds.add("lane-stealing")
        if kind not in allowed_kinds:
            raise ValueError(f"unsupported native sanitizer process kind: {kind}")
        return {
            "ASAN_OPTIONS": _ASAN_NO_LEAKS,
            "LSAN_OPTIONS": "",
            "TSAN_OPTIONS": "",
            "UBSAN_OPTIONS": _UBSAN_FAIL_FAST if runner_os == "macOS" else "",
        }
    if identity == ("tsan", "thread", "Linux"):
        if kind not in {"extension", "fuzz"}:
            raise ValueError(f"unsupported ThreadSanitizer process kind: {kind}")
        return {
            "ASAN_OPTIONS": "",
            "LSAN_OPTIONS": "",
            "TSAN_OPTIONS": _TSAN_FAIL_FAST,
            "UBSAN_OPTIONS": "",
        }
    raise ValueError(f"unsupported sanitizer evidence tuple: {identity}")


def _policy(sanitizer: str, mode: str, runner_os: str) -> dict[str, Any]:
    """Return the closed sanitizer policy for one supported workload."""
    key = (sanitizer, mode, runner_os)
    if key == ("asan-ubsan", "linux-full", "Linux"):
        return {
            "extension_runtime": "asan-ubsan-first-launcher",
            "leak_detection": {
                "extension": "disabled-for-noninstrumented-cpython",
                "standalone_fuzzers": "enabled",
            },
            "process_tree_watchdog": True,
            "runtime_environments": {
                kind: _runtime_environment(sanitizer, mode, runner_os, kind)
                for kind in ("extension", "fuzz")
            },
            "suppressions": [],
            "watchdog_modes": {
                "extension": "external-process-tree",
                "fuzz": "external-process-tree",
            },
        }
    if key == ("asan", "native", "Windows"):
        return {
            "extension_runtime": "msvc-asan-first-launcher",
            "leak_detection": "unsupported-by-msvc-asan",
            "process_tree_watchdog": True,
            "runtime_environments": {
                kind: _runtime_environment(sanitizer, mode, runner_os, kind)
                for kind in ("concurrency", "extension", "fuzz")
            },
            "suppressions": [],
            "watchdog_modes": {
                "concurrency": "external-process-tree",
                "extension": "external-process-tree",
                "fuzz": "external-process-tree",
            },
        }
    if key == ("asan-ubsan", "native", "macOS"):
        return {
            "extension_runtime": "asan-ubsan-first-launcher",
            "leak_detection": "disabled-for-noninstrumented-cpython",
            "process_tree_watchdog": True,
            "runtime_environments": {
                kind: _runtime_environment(sanitizer, mode, runner_os, kind)
                for kind in ("concurrency", "lane-stealing", "extension", "fuzz")
            },
            "suppressions": [],
            "watchdog_modes": {
                "concurrency": "external-process-tree",
                "lane-stealing": "external-process-tree",
                "extension": "external-process-tree",
                "fuzz": "external-process-tree",
            },
        }
    if key == ("tsan", "thread", "Linux"):
        return {
            "extension_runtime": "tsan-first-launcher",
            "leak_detection": "not-applicable",
            "process_tree_watchdog": True,
            "runtime_environments": {
                kind: _runtime_environment(sanitizer, mode, runner_os, kind)
                for kind in ("extension", "fuzz")
            },
            "suppressions": [],
            "suite": {"domains": 8, "rounds": 2},
            "watchdog_modes": {
                "extension": "internal-per-domain",
                "fuzz": "external-process-tree",
            },
        }
    raise ValueError(f"unsupported sanitizer evidence tuple: {key}")


def _fuzz_command(build_root: str, campaign_runs: int, *, engine: str | None = None) -> list[str]:
    """Return the exact externally bounded fuzz campaign command."""
    command = [
        "bash",
        "meta/ci/fuzz/run_fuzz_regressions.sh",
        "--build-root",
        build_root,
    ]
    if engine is not None:
        command.extend(("--engine", engine))
    command.extend(
        (
            "--campaign-runs",
            str(campaign_runs),
            "--seed",
            "15172026",
            "--max-len",
            "262144",
        )
    )
    return command


def _watchdog_specs(
    sanitizer: str,
    mode: str,
    runner_os: str,
    runner_arch: str,
) -> dict[str, dict[str, Any]]:
    """Return the exact raw watchdog filenames and payloads for one CI tuple."""
    identity = (sanitizer, mode, runner_os, runner_arch)
    if identity not in EXPECTED_RUNS:
        raise ValueError(f"unsupported sanitizer evidence tuple: {identity}")
    stem = f"watchdog-{sanitizer}-{mode}-{runner_os}-{runner_arch}"
    label_stem = f"{runner_os}-{runner_arch}-{sanitizer}-{mode}"
    sys_platform = {"Linux": "linux", "macOS": "darwin", "Windows": "win32"}[runner_os]

    def spec(kind: str, command: list[str], timeout_seconds: int) -> dict[str, Any]:
        """Build one canonical expected watchdog payload."""
        return {
            "command": command,
            "format": WATCHDOG_FORMAT,
            "label": f"{label_stem}-{kind}",
            "platform": sys_platform,
            "sanitizer_environment": _runtime_environment(sanitizer, mode, runner_os, kind),
            "status": "passed",
            "timeout_seconds": timeout_seconds,
        }

    if mode == "linux-full":
        return {
            f"{stem}-extension.json": spec(
                "extension",
                [".work/bin/python-asan", "-m", "pytest", "-q", *_LINUX_EXTENSION_TESTS],
                900,
            ),
            f"{stem}-fuzz.json": spec(
                "fuzz",
                _fuzz_command(".work/build/platform-sanitizer/fuzz", 1_000, engine="libfuzzer"),
                900,
            ),
        }
    if mode == "native":
        executable_suffix = ".exe" if runner_os == "Windows" else ""
        concurrency_args = (
            ["--case", "arena_backpressure_deadline"]
            if runner_os == "Windows"
            else ["--fixed-cpu-capacity", "3", "--rounds", "100"]
        )
        concurrency_timeout = 60 if runner_os == "Windows" else 300
        build_root = ".work/build/platform-sanitizer"
        specifications = {
            f"{stem}-concurrency.json": spec(
                "concurrency",
                [
                    f"{build_root}/schema_sanitizer_sanitized_ordered_executor{executable_suffix}",
                    *concurrency_args,
                ],
                concurrency_timeout,
            ),
            f"{stem}-extension.json": spec(
                "extension",
                [
                    f"{build_root}/schema_sanitizer_sanitizer_python_launcher{executable_suffix}",
                    "-u",
                    "meta/ci/release/abi_public_smoke.py",
                ],
                120,
            ),
            f"{stem}-fuzz.json": spec("fuzz", _fuzz_command(f"{build_root}/fuzz", 500), 900),
        }
        if runner_os == "macOS":
            specifications[f"{stem}-lane-stealing.json"] = spec(
                "lane-stealing",
                [
                    f"{build_root}/schema_sanitizer_sanitized_ordered_executor",
                    "--fixed-cpu-capacity",
                    "4",
                    "--case",
                    "lane_stealing",
                ],
                60,
            )
        return specifications
    return {
        f"{stem}-fuzz.json": spec("fuzz", _fuzz_command(".work/build/tsan/fuzz", 1_000), 900),
    }


def _watchdog_evidence(
    directory: Path,
    *,
    sanitizer: str,
    mode: str,
    runner_os: str,
    runner_arch: str,
) -> list[dict[str, Any]]:
    """Validate and authenticate every raw watchdog certificate in a directory."""
    if directory.is_symlink() or not directory.is_dir():
        raise AssertionError(f"sanitizer evidence directory is unsafe: {directory}")
    expected = _watchdog_specs(sanitizer, mode, runner_os, runner_arch)
    paths = [directory / name for name in sorted(expected)]
    evidence: list[dict[str, Any]] = []
    for path in paths:
        _regular_file(path, "watchdog certificate")
        serialized = path.read_text(encoding="utf-8")
        payload = json.loads(serialized)
        if not isinstance(payload, Mapping) or serialized != _canonical_json(payload):
            raise AssertionError(f"watchdog certificate is not canonical JSON: {path}")
        if payload != expected[path.name]:
            raise AssertionError(f"watchdog certificate policy mismatch: {path}")
        evidence.append(
            {
                "filename": path.name,
                "label": payload["label"],
                "sha256": hashlib.sha256(serialized.encode("utf-8")).hexdigest(),
            }
        )
    return evidence


def _windows_toolchain_payload() -> dict[str, Any]:
    """Return the exact Windows sanitizer toolchain certificate payload."""
    _regular_file(WINDOWS_TOOLCHAIN_POLICY, "Windows toolchain policy")
    _regular_file(PROJECT_CONFIGURATION, "project configuration")
    policy_serialized = WINDOWS_TOOLCHAIN_POLICY.read_text(encoding="utf-8")
    policy = json.loads(policy_serialized)
    if not isinstance(policy, dict) or policy_serialized != _canonical_json(policy):
        raise AssertionError("Windows toolchain policy is not canonical JSON")
    try:
        cmake_spec = tomllib.loads(PROJECT_CONFIGURATION.read_text(encoding="utf-8"))["tool"][
            "scikit-build"
        ]["cmake"]["version"]
    except (KeyError, TypeError, tomllib.TOMLDecodeError) as error:
        raise AssertionError("project configuration has no CMake pin") from error
    if (
        not isinstance(cmake_spec, str)
        or re.fullmatch(r"==[0-9]+(?:\.[0-9]+){2}", cmake_spec) is None
    ):
        raise AssertionError("project configuration has no exact CMake pin")
    compiler = (
        f"{policy['generator_instance']}/VC/Tools/MSVC/{policy['vc_tools_version']}"
        "/bin/Hostx64/x64/cl.exe"
    )
    return {
        "build_directory": ".work/build/platform-sanitizer",
        "cmake_version": cmake_spec.removeprefix("=="),
        "compiler": {"path": compiler, "version": policy["compiler_version"]},
        "format": WINDOWS_TOOLCHAIN_FORMAT,
        "generator": policy["generator"],
        "generator_instance": policy["generator_instance"],
        "platform": "x64",
        "sdk_version": policy["sdk_version"],
        "toolset": policy["toolset"],
        "vc_tools_version": policy["vc_tools_version"],
    }


def _windows_toolchain_evidence(
    directory: Path, *, runner_os: str, runner_arch: str
) -> list[dict[str, Any]]:
    """Authenticate the exact generated Windows compiler certificate."""
    if (runner_os, runner_arch) != ("Windows", "X64"):
        return []
    path = directory / WINDOWS_TOOLCHAIN_FILENAME
    _regular_file(path, "Windows toolchain certificate")
    serialized = path.read_text(encoding="utf-8")
    payload = json.loads(serialized)
    if not isinstance(payload, Mapping) or serialized != _canonical_json(payload):
        raise AssertionError("Windows toolchain certificate is not canonical JSON")
    if payload != _windows_toolchain_payload():
        raise AssertionError("Windows toolchain certificate policy mismatch")
    return [
        {
            "filename": path.name,
            "label": "Windows-X64-reviewed-toolchain",
            "sha256": hashlib.sha256(serialized.encode("utf-8")).hexdigest(),
        }
    ]


def build_certificate(
    *,
    evidence_directory: Path,
    sanitizer: str,
    mode: str,
    runner_os: str,
    runner_arch: str,
    github_sha: str,
    github_run_id: int,
    github_run_attempt: int,
) -> dict[str, Any]:
    """Return a validated sanitizer policy and authenticated execution evidence."""
    if _GIT_SHA.fullmatch(github_sha) is None:
        raise ValueError("github_sha must be a lowercase 40- or 64-character Git object ID")
    if github_run_id < 1 or github_run_attempt < 1:
        raise ValueError("GitHub run identity values must be positive integers")
    policy = _policy(sanitizer, mode, runner_os)
    evidence = _windows_toolchain_evidence(
        evidence_directory,
        runner_os=runner_os,
        runner_arch=runner_arch,
    ) + _watchdog_evidence(
        evidence_directory,
        sanitizer=sanitizer,
        mode=mode,
        runner_os=runner_os,
        runner_arch=runner_arch,
    )
    return {
        "evidence": evidence,
        "format": CERTIFICATE_FORMAT,
        "mode": mode,
        "platform": {"arch": runner_arch, "os": runner_os},
        "policy": policy,
        "provenance": {
            "git_sha": github_sha,
            "github_run_attempt": github_run_attempt,
            "github_run_id": github_run_id,
        },
        "sanitizer": sanitizer,
        "status": "passed",
    }


def create_certificate(certificate: Path, **options: Any) -> None:
    """Create and immediately verify one sanitizer run certificate."""
    payload = build_certificate(**options)
    _write_atomically(certificate, _canonical_json(payload))
    verify_certificate(certificate, **options)


def verify_certificate(certificate: Path, **options: Any) -> None:
    """Rebuild sanitizer evidence and compare it with a canonical certificate."""
    _regular_file(certificate, "sanitizer run certificate")
    serialized = certificate.read_text(encoding="utf-8")
    payload = json.loads(serialized)
    if not isinstance(payload, Mapping) or serialized != _canonical_json(payload):
        raise AssertionError("sanitizer run certificate is not canonical JSON")
    if payload != build_certificate(**options):
        raise AssertionError("sanitizer run certificate, policy, or evidence changed")


def verify_directory(
    directory: Path,
    *,
    github_sha: str,
    github_run_id: int,
    github_run_attempt: int,
) -> None:
    """Verify the exact five sanitizer certificates required by the release gate."""
    if directory.is_symlink() or not directory.is_dir():
        raise AssertionError(f"sanitizer certificate directory is unsafe: {directory}")
    expected_certificates = {
        f"sanitizer-run-{runner_os}-{runner_arch}-{sanitizer}.json"
        for sanitizer, _mode, runner_os, runner_arch in EXPECTED_RUNS
    }
    expected_watchdogs = {
        filename
        for sanitizer, mode, runner_os, runner_arch in EXPECTED_RUNS
        for filename in _watchdog_specs(sanitizer, mode, runner_os, runner_arch)
    }
    evidence_paths = sorted(directory.iterdir())
    for path in evidence_paths:
        _regular_file(path, "sanitizer evidence")
    observed_files = {path.name for path in evidence_paths}
    expected_files = expected_certificates | expected_watchdogs
    expected_files.add(WINDOWS_TOOLCHAIN_FILENAME)
    if observed_files != expected_files:
        raise AssertionError(
            "sanitizer evidence inventory mismatch: "
            f"missing={sorted(expected_files - observed_files)}, "
            f"extra={sorted(observed_files - expected_files)}"
        )
    certificates = [directory / name for name in sorted(expected_certificates)]
    observed: set[tuple[str, str, str, str]] = set()
    for certificate in certificates:
        _regular_file(certificate, "sanitizer run certificate")
        payload = json.loads(certificate.read_text(encoding="utf-8"))
        platform = payload.get("platform")
        if not isinstance(platform, Mapping):
            raise AssertionError(f"sanitizer certificate omits platform: {certificate}")
        identity = (
            str(payload.get("sanitizer")),
            str(payload.get("mode")),
            str(platform.get("os")),
            str(platform.get("arch")),
        )
        expected_filename = f"sanitizer-run-{identity[2]}-{identity[3]}-{identity[0]}.json"
        if certificate.name != expected_filename:
            raise AssertionError(f"sanitizer certificate filename mismatch: {certificate.name}")
        if identity in observed:
            raise AssertionError(f"duplicate sanitizer certificate tuple: {identity}")
        observed.add(identity)
        verify_certificate(
            certificate,
            evidence_directory=directory,
            sanitizer=identity[0],
            mode=identity[1],
            runner_os=identity[2],
            runner_arch=identity[3],
            github_sha=github_sha,
            github_run_id=github_run_id,
            github_run_attempt=github_run_attempt,
        )
    if observed != EXPECTED_RUNS:
        raise AssertionError(
            f"sanitizer certificate matrix mismatch: missing={sorted(EXPECTED_RUNS - observed)}, "
            f"extra={sorted(observed - EXPECTED_RUNS)}"
        )


def _positive_integer(raw: str) -> int:
    """Parse one strictly positive command-line integer."""
    value = int(raw)
    if value < 1:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return value


def main() -> None:
    """Run the sanitizer certificate create-or-verify command-line interface."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("create", "verify", "verify-directory"))
    parser.add_argument("--certificate", type=Path)
    parser.add_argument("--evidence-directory", required=True, type=Path)
    parser.add_argument("--sanitizer")
    parser.add_argument("--mode")
    parser.add_argument("--runner-os")
    parser.add_argument("--runner-arch")
    parser.add_argument("--github-sha", required=True)
    parser.add_argument("--github-run-id", required=True, type=_positive_integer)
    parser.add_argument("--github-run-attempt", required=True, type=_positive_integer)
    args = parser.parse_args()
    options = {
        "evidence_directory": args.evidence_directory,
        "sanitizer": args.sanitizer,
        "mode": args.mode,
        "runner_os": args.runner_os,
        "runner_arch": args.runner_arch,
        "github_sha": args.github_sha,
        "github_run_id": args.github_run_id,
        "github_run_attempt": args.github_run_attempt,
    }
    try:
        if args.operation == "create":
            if args.certificate is None or None in (
                args.sanitizer,
                args.mode,
                args.runner_os,
                args.runner_arch,
            ):
                parser.error("create requires --certificate and one sanitizer tuple")
            create_certificate(args.certificate, **options)
        elif args.operation == "verify":
            if args.certificate is None or None in (
                args.sanitizer,
                args.mode,
                args.runner_os,
                args.runner_arch,
            ):
                parser.error("verify requires --certificate and one sanitizer tuple")
            verify_certificate(args.certificate, **options)
        else:
            verify_directory(
                args.evidence_directory,
                github_sha=args.github_sha,
                github_run_id=args.github_run_id,
                github_run_attempt=args.github_run_attempt,
            )
        print(f"sanitizer run certificate {args.operation} passed: {args.certificate}")
    except (AssertionError, json.JSONDecodeError, OSError, TypeError, ValueError) as error:
        raise SystemExit(str(error)) from error


if __name__ == "__main__":
    main()
