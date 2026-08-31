"""Behavioral architecture contracts for the CI helper tree.

The check keeps fuzz, native, Parquet, quality, and release helpers under explicit owner
directories and rejects obsolete flat-script locations.
"""

from __future__ import annotations

import importlib.util
import json
import os
import struct
import sys
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CI_ROOT = ROOT / "meta" / "ci"

OWNER_DIRECTORIES = {
    "fuzz",
    "native",
    "parquet",
    "quality",
    "release",
    "requirements",
    "sanitizers",
}


def _load_ci_helper(relative_path: str):
    """Load one standalone CI helper without making meta a runtime package."""
    path = CI_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(f"test_{path.stem}_{id(path)}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_ci_helpers_are_grouped_by_owner() -> None:
    """Runnable helpers belong to a thematic owner, not a filename inventory."""
    helpers = [
        path.relative_to(CI_ROOT)
        for path in CI_ROOT.rglob("*")
        if path.is_file() and path.suffix in {".cc", ".py", ".sh"}
    ]

    assert helpers
    assert all(len(path.parts) >= 2 for path in helpers)
    assert {path.parts[0] for path in helpers} <= OWNER_DIRECTORIES
    assert all(path.suffix == ".md" for path in CI_ROOT.iterdir() if path.is_file())


def test_ci_shell_entry_points_remain_executable() -> None:
    """Moved shell gates retain the executable bit expected by workflows."""
    scripts = tuple(CI_ROOT.rglob("*.sh"))

    assert scripts
    assert all(os.access(script, os.X_OK) for script in scripts)


def test_only_the_bounded_sanitizer_watchdog_imports_subprocess() -> None:
    """The sole subprocess owner applies fixed bounds and process-tree cleanup."""
    production_roots = (CI_ROOT, ROOT / "benchmarks")
    offenders = {
        path.relative_to(ROOT).as_posix()
        for root in production_roots
        for path in root.rglob("*.py")
        if "import subprocess" in path.read_text(encoding="utf-8")
        or "from subprocess import" in path.read_text(encoding="utf-8")
    }

    assert offenders == {"meta/ci/sanitizers/run_with_watchdog.py"}
    watchdog = (ROOT / next(iter(offenders))).read_text(encoding="utf-8")
    assert 'start_new_session=os.name != "nt"' in watchdog
    assert "subprocess.CREATE_NEW_PROCESS_GROUP" in watchdog
    assert '"System32" / "taskkill.exe"' in watchdog
    assert "signal.SIGTERM" in watchdog
    assert "signal.SIGKILL" in watchdog
    assert '["/bin/ps", "-e", "-o", "pgid=,stat="]' in watchdog
    assert 'not fields[1].startswith("Z")' in watchdog
    assert "process.wait(timeout=timeout_seconds)" in watchdog
    assert "TERMINATION_GRACE_SECONDS = 10" in watchdog
    assert "shell=True" not in watchdog
    assert not (CI_ROOT / "quality" / "run_process.py").exists()


def test_sanitizer_watchdog_overrides_and_records_all_runtime_options(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A hostile ambient sanitizer environment cannot reach a guarded child."""
    helper = _load_ci_helper("sanitizers/run_with_watchdog.py")
    expected = {
        "ASAN_OPTIONS": "detect_leaks=0:halt_on_error=1",
        "LSAN_OPTIONS": "",
        "TSAN_OPTIONS": "",
        "UBSAN_OPTIONS": "halt_on_error=1:print_stacktrace=1",
    }
    for name in helper.SANITIZER_ENVIRONMENT_NAMES:
        monkeypatch.setenv(name, "ambient-runner-value")
    observed_path = tmp_path / "observed.json"
    certificate = tmp_path / "watchdog.json"
    child = (
        "import json, os, pathlib, sys; "
        "names=('ASAN_OPTIONS','LSAN_OPTIONS','TSAN_OPTIONS','UBSAN_OPTIONS'); "
        "pathlib.Path(sys.argv[1]).write_text("
        "json.dumps({name: os.environ.get(name) for name in names}, sort_keys=True), "
        "encoding='utf-8')"
    )

    status = helper.run_guarded(
        [sys.executable, "-c", child, str(observed_path)],
        timeout_seconds=10,
        label="runtime-environment-test",
        certificate=certificate,
        sanitizer_environment=expected,
    )

    assert status == 0
    assert json.loads(observed_path.read_text(encoding="utf-8")) == expected
    evidence = json.loads(certificate.read_text(encoding="utf-8"))
    assert evidence["sanitizer_environment"] == expected
    with pytest.raises(ValueError, match="inventory mismatch"):
        helper.run_guarded(
            [sys.executable, "-c", "pass"],
            timeout_seconds=10,
            label="incomplete-environment-test",
            certificate=certificate,
            sanitizer_environment={"ASAN_OPTIONS": ""},
        )


def test_sanitizer_watchdog_uses_only_fixed_git_bash_on_windows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Windows Bash resolution ignores PATH, SystemRoot, and explicit shim paths."""
    helper = _load_ci_helper("sanitizers/run_with_watchdog.py")
    assert helper.WINDOWS_GIT_BASH.as_posix() == "C:/Program Files/Git/bin/bash.exe"

    trusted = tmp_path / "Program Files" / "Git" / "bin" / "bash.exe"
    ambient = tmp_path / "ambient" / "bash.exe"
    system = tmp_path / "Windows" / "System32" / "bash.exe"
    for executable in (trusted, ambient, system):
        executable.parent.mkdir(parents=True, exist_ok=True)
        executable.write_bytes(b"MZcontrolled-test-executable")
        executable.chmod(0o700)
    monkeypatch.setattr(helper, "WINDOWS_GIT_BASH", trusted)
    monkeypatch.setenv("PATH", os.fspath(ambient.parent))
    monkeypatch.setenv("SystemRoot", os.fspath(system.parents[1]))

    logical = ["bash", "meta/ci/fuzz/run_fuzz_regressions.sh"]
    execution = helper._execution_command(logical, windows=True)

    assert execution == [
        os.fspath(trusted.resolve()),
        "meta/ci/fuzz/run_fuzz_regressions.sh",
    ]
    assert logical == ["bash", "meta/ci/fuzz/run_fuzz_regressions.sh"]
    assert os.fspath(ambient) not in execution
    assert os.fspath(system) not in execution
    for forbidden in ("bash.exe", os.fspath(ambient), os.fspath(system)):
        with pytest.raises(ValueError, match="logical 'bash' command token"):
            helper._execution_command([forbidden, "controlled.sh"], windows=True)

    symlink = tmp_path / "Program Files" / "Git" / "symlink" / "bash.exe"
    symlink.parent.mkdir(parents=True)
    symlink.symlink_to(ambient)
    monkeypatch.setattr(helper, "WINDOWS_GIT_BASH", symlink)
    with pytest.raises(RuntimeError, match="symlinked"):
        helper._execution_command(logical, windows=True)


def test_sanitizer_watchdog_records_logical_argv_after_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Evidence keeps portable logical argv while execution uses a resolved binary."""
    helper = _load_ci_helper("sanitizers/run_with_watchdog.py")
    logical = ["bash", "controlled-script.sh"]
    resolved = [sys.executable, "-c", "pass"]
    observed: list[list[str]] = []

    def resolve(command: list[str]) -> list[str]:
        """Record logical argv and substitute a harmless real executable."""
        observed.append(list(command))
        return resolved

    monkeypatch.setattr(helper, "_execution_command", resolve)
    certificate = tmp_path / "watchdog.json"
    environment = {name: "" for name in helper.SANITIZER_ENVIRONMENT_NAMES}

    status = helper.run_guarded(
        logical,
        timeout_seconds=10,
        label="logical-command-test",
        certificate=certificate,
        sanitizer_environment=environment,
    )

    assert status == 0
    assert observed == [logical]
    assert json.loads(certificate.read_text(encoding="utf-8"))["command"] == logical


def test_retired_source_zip_pipeline_stays_absent() -> None:
    """The obsolete ZIP chain must not return beside the canonical sdist flow."""
    retired = {
        "check_cmake_sources_exist.sh",
        "check_zip_contains_cmake_sources.sh",
        "create_source_zip.sh",
    }

    assert not any(path.name in retired for path in CI_ROOT.rglob("*"))


def test_native_coverage_certificate_closes_inventory_floors_and_provenance(
    tmp_path: Path,
) -> None:
    """Coverage evidence fails on omitted sources, low totals, or run rebinding."""
    helper = _load_ci_helper("native/check_llvm_coverage.py")
    summaries = {metric: {"count": 1, "covered": 1, "percent": 100.0} for metric in helper.METRICS}
    sources = sorted(
        path
        for path in (ROOT / "cpp/src").rglob("*")
        if path.is_file() and path.suffix.lower() in helper.TRANSLATION_UNIT_SUFFIXES
    )
    mapped_sources = [
        path
        for path in sources
        if path.relative_to(ROOT).as_posix() not in helper.ZERO_REGION_TRANSLATION_UNITS
    ]
    document = {
        "type": "llvm.coverage.json.export",
        "version": "2.0.1",
        "data": [
            {
                "files": [{"filename": str(path), "summary": summaries} for path in mapped_sources],
                "totals": summaries,
            }
        ],
    }
    report = tmp_path / "coverage.json"
    certificate = tmp_path / "certificate.json"
    report.write_text(json.dumps(document), encoding="utf-8")
    options = {
        "repository": ROOT,
        "source_root": ROOT / "cpp/src",
        "github_sha": "a" * 40,
        "github_run_id": 7,
        "github_run_attempt": 1,
    }
    helper.create_certificate(report, certificate, **options)
    helper.verify_certificate(
        certificate,
        repository=ROOT,
        source_root=ROOT / "cpp/src",
        github_sha="a" * 40,
        github_run_id=7,
        github_run_attempt=1,
    )
    with pytest.raises(AssertionError, match="provenance mismatch"):
        helper.verify_certificate(
            certificate,
            repository=ROOT,
            source_root=ROOT / "cpp/src",
            github_sha="b" * 40,
            github_run_id=7,
            github_run_attempt=1,
        )

    payload = json.loads(certificate.read_text(encoding="utf-8"))
    payload["source_tree_sha256"] = "0" * 64
    certificate.write_text(helper._canonical_json(payload), encoding="utf-8")
    with pytest.raises(AssertionError, match="source-tree digest mismatch"):
        helper.verify_certificate(certificate, **options)
    helper.create_certificate(report, certificate, **options)

    payload = json.loads(certificate.read_text(encoding="utf-8"))
    payload["zero_region_translation_units"] = []
    certificate.write_text(helper._canonical_json(payload), encoding="utf-8")
    with pytest.raises(AssertionError, match="zero-region policy mismatch"):
        helper.verify_certificate(certificate, **options)
    helper.create_certificate(report, certificate, **options)

    payload = json.loads(certificate.read_text(encoding="utf-8"))
    payload["observed_translation_units"] += 1
    certificate.write_text(helper._canonical_json(payload), encoding="utf-8")
    with pytest.raises(AssertionError, match="observed translation-unit count"):
        helper.verify_certificate(certificate, **options)
    helper.create_certificate(report, certificate, **options)

    native_sources = helper._native_source_files(ROOT / "cpp/src", ROOT)
    assert any(source.endswith(".cc.inc") for source in native_sources)

    boundary = {
        metric: {"count": 20_001, "covered": 8_000, "percent": 40.0} for metric in helper.METRICS
    }
    with pytest.raises(AssertionError, match="native coverage floor failed"):
        helper._require_floor("rounding-boundary", boundary, {"regions": 40.0})

    omitted_entry = document["data"][0]["files"].pop()
    report.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(AssertionError, match="translation-unit inventory mismatch"):
        helper.build_certificate(report, **options)
    document["data"][0]["files"].append(omitted_entry)
    document["data"][0]["totals"] = {
        metric: {"count": 1, "covered": 0, "percent": 0.0} for metric in helper.METRICS
    }
    report.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(AssertionError, match="native coverage floor failed"):
        helper.build_certificate(report, **options)


def test_native_coverage_rejects_foreign_paths_and_source_tree_symlinks(tmp_path: Path) -> None:
    """Foreign LLVM paths and unauthenticated linked sources fail closed."""
    helper = _load_ci_helper("native/check_llvm_coverage.py")
    repository = tmp_path / "repository"
    source_root = repository / "cpp" / "src"
    source_root.mkdir(parents=True)
    (source_root / "owned.cc").write_text("int owned;\n", encoding="utf-8")
    foreign = tmp_path / "foreign" / "cpp" / "src" / "owned.cc"

    assert helper._repository_filename(str(source_root / "owned.cc"), repository) == (
        "cpp/src/owned.cc"
    )
    assert helper._repository_filename(str(foreign), repository) is None

    linked_file = source_root / "linked.hh"
    linked_file.symlink_to(source_root / "owned.cc")
    with pytest.raises(AssertionError, match="source tree contains a symlink"):
        helper._native_source_files(source_root, repository)
    linked_file.unlink()

    external_directory = tmp_path / "external-sources"
    external_directory.mkdir()
    linked_directory = source_root / "linked-directory"
    linked_directory.symlink_to(external_directory, target_is_directory=True)
    with pytest.raises(AssertionError, match="source tree contains a symlink"):
        helper._expected_sources(source_root, repository)


def test_platform_wheel_certificate_rejects_binary_and_run_tampering(tmp_path: Path) -> None:
    """Native architecture bytes and workflow identity remain authenticated."""
    helper = _load_ci_helper("native/certify_platform_wheel.py")
    wheel = tmp_path / "schema_sanitizer-0.4.3-cp311-abi3-manylinux_2_28_x86_64.whl"
    elf = bytearray(64)
    elf[:6] = b"\x7fELF\x02\x01"
    struct.pack_into("<HH", elf, 16, 3, 62)
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("schema_sanitizer/_core_abi3.abi3.so", elf)
    certificate = tmp_path / "wheel-certificate.json"
    options = {
        "platform": "linux-x86_64",
        "github_sha": "a" * 40,
        "github_run_id": 9,
        "github_run_attempt": 1,
    }
    helper.create_certificate(wheel, certificate, **options)
    helper.verify_certificate(
        certificate,
        wheel_directory=tmp_path,
        platform="linux-x86_64",
        github_sha="a" * 40,
        github_run_id=9,
        github_run_attempt=1,
    )
    with pytest.raises(AssertionError, match="changed"):
        helper.verify_certificate(
            certificate,
            wheel_directory=tmp_path,
            platform="linux-x86_64",
            github_sha="a" * 40,
            github_run_id=10,
            github_run_attempt=1,
        )
    payload = json.loads(certificate.read_text(encoding="utf-8"))
    payload["platform"] = "macos-x86_64"
    certificate.write_text(helper._canonical_json(payload), encoding="utf-8")
    with pytest.raises(AssertionError, match="platform mismatch"):
        helper.verify_certificate(
            certificate,
            wheel_directory=tmp_path,
            platform="linux-x86_64",
            github_sha="a" * 40,
            github_run_id=9,
            github_run_attempt=1,
        )
    payload["platform"] = "linux-x86_64"
    payload["wheel"]["filename"] = "../" + wheel.name
    certificate.write_text(helper._canonical_json(payload), encoding="utf-8")
    with pytest.raises(AssertionError, match="unsafe wheel filename"):
        helper.verify_certificate(
            certificate,
            wheel_directory=tmp_path,
            platform="linux-x86_64",
            github_sha="a" * 40,
            github_run_id=9,
            github_run_attempt=1,
        )
    payload["wheel"]["filename"] = wheel.name
    certificate.write_text(helper._canonical_json(payload), encoding="utf-8")
    real_wheel = tmp_path / "real-wheel.whl"
    wheel.rename(real_wheel)
    try:
        wheel.symlink_to(real_wheel)
    except OSError as error:
        pytest.skip(f"symlinks are unavailable: {error}")
    with pytest.raises(AssertionError, match="must not be a symlink"):
        helper.verify_certificate(
            certificate,
            wheel_directory=tmp_path,
            platform="linux-x86_64",
            github_sha="a" * 40,
            github_run_id=9,
            github_run_attempt=1,
        )
    malformed = tmp_path / "schema_sanitizer-0.4.4-cp311-abi3-manylinux_2_28_x86_64.whl"
    with zipfile.ZipFile(malformed, "w") as archive:
        archive.writestr("schema_sanitizer/_core_abi3.abi3.so", b"not-elf")
    with pytest.raises(AssertionError, match="not an ELF"):
        helper.build_certificate(malformed, **options)


def test_sanitizer_certificates_require_exact_matrix_and_watchdog_bytes(tmp_path: Path) -> None:
    """The gate consumes five run certificates and authenticates native watchdogs."""
    helper = _load_ci_helper("sanitizers/certify_sanitizer_run.py")
    for sanitizer, mode, runner_os, runner_arch in sorted(helper.EXPECTED_RUNS):
        for filename, raw in helper._watchdog_specs(
            sanitizer, mode, runner_os, runner_arch
        ).items():
            (tmp_path / filename).write_text(helper._canonical_json(raw), encoding="utf-8")
        certificate = tmp_path / f"sanitizer-run-{runner_os}-{runner_arch}-{sanitizer}.json"
        helper.create_certificate(
            certificate,
            evidence_directory=tmp_path,
            sanitizer=sanitizer,
            mode=mode,
            runner_os=runner_os,
            runner_arch=runner_arch,
            github_sha="a" * 40,
            github_run_id=11,
            github_run_attempt=1,
        )
    helper.verify_directory(
        tmp_path,
        github_sha="a" * 40,
        github_run_id=11,
        github_run_attempt=1,
    )
    extra_evidence = tmp_path / "unreviewed-evidence.txt"
    extra_evidence.write_text("unexpected\n", encoding="utf-8")
    with pytest.raises(AssertionError, match="evidence inventory mismatch"):
        helper.verify_directory(
            tmp_path,
            github_sha="a" * 40,
            github_run_id=11,
            github_run_attempt=1,
        )
    extra_evidence.unlink()
    raw_certificate = tmp_path / "watchdog-asan-native-Windows-X64-extension.json"
    raw = json.loads(raw_certificate.read_text(encoding="utf-8"))
    raw["timeout_seconds"] += 1
    raw_certificate.write_text(helper._canonical_json(raw), encoding="utf-8")
    with pytest.raises(AssertionError, match="policy mismatch"):
        helper.verify_directory(
            tmp_path,
            github_sha="a" * 40,
            github_run_id=11,
            github_run_attempt=1,
        )
    raw["timeout_seconds"] -= 1
    raw_certificate.write_text(helper._canonical_json(raw), encoding="utf-8")
    raw["sanitizer_environment"]["ASAN_OPTIONS"] = "ambient-runner-value"
    raw_certificate.write_text(helper._canonical_json(raw), encoding="utf-8")
    with pytest.raises(AssertionError, match="policy mismatch"):
        helper.verify_directory(
            tmp_path,
            github_sha="a" * 40,
            github_run_id=11,
            github_run_attempt=1,
        )
    raw["sanitizer_environment"] = helper._runtime_environment(
        "asan", "native", "Windows", "extension"
    )
    raw_certificate.write_text(helper._canonical_json(raw), encoding="utf-8")
    raw_certificate.write_text(raw_certificate.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(AssertionError, match="not canonical JSON"):
        helper.verify_directory(
            tmp_path,
            github_sha="a" * 40,
            github_run_id=11,
            github_run_attempt=1,
        )
