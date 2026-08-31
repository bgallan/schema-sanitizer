"""Behavioral architecture contracts for the CI helper tree.

The check keeps fuzz, native, Parquet, quality, and release helpers under explicit owner
directories and rejects obsolete flat-script locations.
"""

from __future__ import annotations

import contextlib
import importlib.util
import json
import os
import re
import struct
import sys
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CI_ROOT = ROOT / "meta" / "ci"
WINDOWS_COMPILER = (
    "C:/Program Files/Microsoft Visual Studio/2022/Enterprise/VC/Tools/"
    "MSVC/14.44.35207/bin/Hostx64/x64/cl.exe"
)

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


def _cmake_assignment(key: str, value: str) -> str:
    """Render one representative CMake-generated assignment."""
    encoded = value if value in {"1", "TRUE"} else f'"{value}"'
    return f"set({key} {encoded})"


def _windows_toolchain_tree(tmp_path: Path) -> tuple[Path, dict[str, Path]]:
    """Create one valid Visual Studio cache and generated compiler metadata pair."""
    build_root = tmp_path / "build"
    binary = build_root / "cp311-abi3-win_amd64"
    metadata = binary / "CMakeFiles" / "4.3.4"
    metadata.mkdir(parents=True)
    cache = binary / "CMakeCache.txt"
    cache.write_text(
        "\r\n".join(
            (
                "# Representative Visual Studio cache without compiler cache entries.",
                "CMAKE_PROJECT_NAME:STATIC=schema_sanitizer",
                "CMAKE_GENERATOR:INTERNAL=Visual Studio 17 2022",
                "CMAKE_GENERATOR_INSTANCE:INTERNAL=C:/Program Files/Microsoft Visual Studio/2022/Enterprise",
                "CMAKE_GENERATOR_PLATFORM:INTERNAL=x64",
                "CMAKE_GENERATOR_TOOLSET:INTERNAL=v143,host=x64,version=14.44.35207",
                "SCHEMA_SANITIZER_MSVC_COMPILE_PROCESSES:STRING=auto",
                "",
            )
        ),
        encoding="utf-8",
    )
    log = tmp_path / "cibuildwheel.log"
    log.write_text(
        "*** scikit-build-core 0.11.6 using CMake 4.3.4 (wheel)\r\n"
        "-- Selecting Windows SDK version 10.0.26100.0 to target Windows 10.0.20348.\r\n",
        encoding="utf-8",
    )
    paths = {"cache": cache, "log": log}
    for language in ("C", "CXX"):
        prefix = f"CMAKE_{language}_"
        values = {
            f"{prefix}ABI_COMPILED": "TRUE",
            f"{prefix}COMPILER": WINDOWS_COMPILER,
            f"{prefix}COMPILER_ARCHITECTURE_ID": "x64",
            f"{prefix}COMPILER_ID": "MSVC",
            f"{prefix}COMPILER_LOADED": "1",
            f"{prefix}COMPILER_VERSION": "19.44.35228.0",
            f"{prefix}COMPILER_WORKS": "TRUE",
            f"{prefix}PLATFORM_ID": "Windows",
            f"{prefix}SIZEOF_DATA_PTR": "8",
        }
        path = metadata / f"CMake{language}Compiler.cmake"
        path.write_text(
            "\n".join(_cmake_assignment(key, value) for key, value in values.items()) + "\n",
            encoding="utf-8",
        )
        paths[language] = path
    return build_root, paths


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


def test_pre_commit_dispatcher_and_configuration_have_one_exact_allowlist() -> None:
    """Every local hook routes through exactly one repository-owned dispatch key."""
    helper = _load_ci_helper("quality/run_pre_commit_tool.py")
    config = (ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8")
    hook_ids = set(re.findall(r"^      - id: ([a-z0-9-]+)$", config, re.MULTILINE))

    assert hook_ids == set(helper._TOOL_COMMANDS)
    assert config.count("entry: python meta/ci/quality/run_pre_commit_tool.py ") == len(hook_ids)


def test_pre_commit_dispatcher_reuses_an_exact_active_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An exact CI environment dispatches directly without touching the local cache."""
    helper = _load_ci_helper("quality/run_pre_commit_tool.py")
    observed: list[tuple[Path, str, tuple[str, ...]]] = []
    monkeypatch.setattr(helper, "_current_environment_is_exact", lambda: True)
    monkeypatch.setenv(helper.CERTIFIED_CURRENT_ENVIRONMENT, "1")

    def unexpected_fingerprint() -> str:
        """Fail if the exact-environment fast path consults the bootstrap cache."""
        raise AssertionError("exact active environment unexpectedly consulted the cache")

    def execute(python: Path, hook_id: str, arguments: tuple[str, ...]) -> int:
        """Record the selected interpreter and preserve a representative hook status."""
        observed.append((python, hook_id, arguments))
        return 17

    monkeypatch.setattr(helper, "_environment_fingerprint", unexpected_fingerprint)
    monkeypatch.setattr(helper, "_execute_hook", execute)

    assert helper.run_hook("ruff-check", ("file with spaces.py",)) == 17
    assert observed == [(Path(sys.executable).absolute(), "ruff-check", ("file with spaces.py",))]


def test_pre_commit_dispatcher_bootstraps_once_then_reuses_the_owned_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing tool selects one isolated environment and warm runs reuse it."""
    helper = _load_ci_helper("quality/run_pre_commit_tool.py")
    fingerprint = "a" * 64
    environment = tmp_path / "pre-commit-tools" / "owned"
    bootstraps: list[tuple[Path, str]] = []
    executions: list[Path] = []
    monkeypatch.setattr(helper, "_current_environment_is_exact", lambda: True)
    monkeypatch.delenv(helper.CERTIFIED_CURRENT_ENVIRONMENT, raising=False)
    monkeypatch.setattr(helper, "_environment_fingerprint", lambda: fingerprint)
    monkeypatch.setattr(helper, "_environment_path", lambda _fingerprint: environment)
    monkeypatch.setattr(helper, "_environment_lock", lambda _fingerprint: contextlib.nullcontext())
    monkeypatch.setattr(
        helper,
        "_external_environment_is_exact",
        lambda _environment, _fingerprint: bool(bootstraps),
    )

    def bootstrap(path: Path, observed_fingerprint: str) -> None:
        """Record the sole cold bootstrap without changing the active environment."""
        bootstraps.append((path, observed_fingerprint))

    def execute(python: Path, _hook_id: str, _arguments: tuple[str, ...]) -> int:
        """Record the interpreter selected for each cold or warm dispatch."""
        executions.append(python)
        return 0

    monkeypatch.setattr(helper, "_bootstrap_environment", bootstrap)
    monkeypatch.setattr(helper, "_execute_hook", execute)

    assert helper.run_hook("mypy", ()) == 0
    assert helper.run_hook("mypy", ()) == 0
    assert bootstraps == [(environment, fingerprint)]
    assert executions == [helper._environment_python(environment)] * 2
    assert all(python != Path(sys.executable).absolute() for python in executions)


def test_pre_commit_bootstrap_uses_the_exact_hash_locked_sequence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cold setup pins pip, installs prerequisites and the full lock, then certifies."""
    helper = _load_ci_helper("quality/run_pre_commit_tool.py")
    monkeypatch.setattr(helper, "ROOT", tmp_path)
    environment = tmp_path / ".work/pre-commit-tools/owned"
    bounded: list[tuple[tuple[str, ...], str]] = []
    helpers: list[tuple[str, tuple[str, ...]]] = []
    finalization: list[str] = []
    monkeypatch.setattr(helper, "_reset_environment", lambda _environment: None)
    monkeypatch.setattr(
        helper,
        "_isolated_install_environment",
        lambda _environment: contextlib.nullcontext(),
    )

    def bounded_run(command: tuple[str, ...], *, timeout_seconds: int, label: str) -> None:
        """Record bounded local setup commands without creating a real environment."""
        assert timeout_seconds > 0
        bounded.append((tuple(command), label))

    def run_helper(python: Path, script: Path, arguments: tuple[str, ...]) -> None:
        """Record each target-interpreter bootstrap helper invocation."""
        assert python == helper._environment_python(environment)
        helpers.append((script.name, tuple(arguments)))

    def write_ready(_environment: Path, _fingerprint: str) -> None:
        """Record that readiness is published only after every installer check."""
        finalization.append("write")

    def validate(_environment: Path, _fingerprint: str) -> bool:
        """Record the post-publication independent environment certification."""
        finalization.append("validate")
        return True

    monkeypatch.setattr(helper, "run_bounded", bounded_run)
    monkeypatch.setattr(helper, "_run_helper", run_helper)
    monkeypatch.setattr(helper, "_write_ready", write_ready)
    monkeypatch.setattr(helper, "_external_environment_is_exact", validate)

    helper._bootstrap_environment(environment, "b" * 64)

    assert bounded[0][0] == (
        sys.executable,
        "-I",
        "-m",
        "venv",
        os.fspath(environment),
    )
    assert bounded[-1][0] == (
        os.fspath(helper._environment_python(environment)),
        "-m",
        "pip",
        "check",
    )
    assert helpers[0][0] == "ensure_pinned_pip.py"
    assert helpers[1][1][-5:] == (
        "--packages",
        "setuptools",
        "wheel",
        "requests",
        "semver",
    )
    assert helpers[2][1][-3:] == ("--all", "--allow-sdist", "actionlint-py")
    assert finalization == ["write", "validate"]


def test_pre_commit_tool_lookup_never_falls_back_to_ambient_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Console-script resolution is confined to the selected venv on every platform."""
    helper = _load_ci_helper("quality/run_pre_commit_tool.py")
    environment = tmp_path / "environment"
    python = helper._environment_python(environment, windows=False)
    ambient = tmp_path / "ambient" / "fixture-tool"
    ambient.parent.mkdir(parents=True)
    ambient.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    ambient.chmod(0o700)
    monkeypatch.setenv("PATH", os.fspath(ambient.parent))

    assert helper._environment_python(environment, windows=True) == (
        environment / "Scripts/python.exe"
    )
    assert helper._executable_path(python, "fixture-tool") is None

    selected = python.parent / "fixture-tool"
    selected.parent.mkdir(parents=True)
    selected.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    selected.chmod(0o700)
    assert helper._executable_path(python, "fixture-tool") == selected.resolve()


def test_pre_commit_lock_retries_contention_only_until_its_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Genuine lock contention retries briefly and then reports one bounded timeout."""
    helper = _load_ci_helper("quality/run_pre_commit_tool.py")
    attempts = 0
    sleeps: list[float] = []
    times = iter((0.0, 0.0, 2.0))
    monkeypatch.setattr(helper, "LOCK_TIMEOUT_SECONDS", 1)
    monkeypatch.setattr(helper.time, "monotonic", lambda: next(times))
    monkeypatch.setattr(helper.time, "sleep", sleeps.append)

    def contend(_handle: object) -> None:
        """Model a lock held beyond the configured acquisition deadline."""
        nonlocal attempts
        attempts += 1
        raise OSError(helper.errno.EAGAIN, "lock is held")

    monkeypatch.setattr(helper, "_try_platform_lock", contend)
    with (tmp_path / "lock").open("w+b") as handle:
        with pytest.raises(TimeoutError, match="tool-cache lock"):
            helper._acquire_lock(handle)

    assert attempts == 2
    assert sleeps == [0.1]


def test_pre_commit_lock_rejects_noncontention_errors_immediately(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unsupported or invalid lock operations never look like hour-long contention."""
    helper = _load_ci_helper("quality/run_pre_commit_tool.py")
    attempts = 0

    def unsupported(_handle: object) -> None:
        """Model a filesystem that cannot provide the requested lock operation."""
        nonlocal attempts
        attempts += 1
        raise OSError(helper.errno.ENOLCK, "locks are unsupported")

    def unexpected_sleep(_seconds: float) -> None:
        """Fail if a noncontention error enters the retry loop."""
        raise AssertionError("noncontention lock error unexpectedly retried")

    monkeypatch.setattr(helper, "_try_platform_lock", unsupported)
    monkeypatch.setattr(helper.time, "sleep", unexpected_sleep)
    with (tmp_path / "lock").open("w+b") as handle:
        with pytest.raises(OSError) as raised:
            helper._acquire_lock(handle)

    assert raised.value.errno == helper.errno.ENOLCK
    assert attempts == 1


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


def test_windows_toolchain_certificate_accepts_visual_studio_generated_metadata(
    tmp_path: Path,
) -> None:
    """A valid VS tree needs no C or CXX compiler entries in CMakeCache."""
    helper = _load_ci_helper("native/certify_windows_toolchain.py")
    build_root, paths = _windows_toolchain_tree(tmp_path)

    assert "CMAKE_C_COMPILER:" not in paths["cache"].read_text(encoding="utf-8")
    assert helper.certify_windows_toolchain(build_root, paths["log"]) == paths["cache"].parent


def test_windows_toolchain_certificate_supports_direct_sanitizer_builds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The direct ASan tree emits the canonical toolchain record bound into evidence."""
    helper = _load_ci_helper("native/certify_windows_toolchain.py")
    _build_root, paths = _windows_toolchain_tree(tmp_path)
    direct_root = paths["cache"].parent
    certificate = tmp_path / "windows-toolchain-Windows-X64.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "certify_windows_toolchain.py",
            str(direct_root),
            str(paths["log"]),
            "--direct-build",
            "--certificate",
            str(certificate),
        ],
    )

    assert helper.main() == 0
    assert json.loads(certificate.read_text(encoding="utf-8")) == helper._certificate_payload(
        helper._load_policy(helper.WINDOWS_TOOLCHAIN),
        "4.3.4",
        direct_root,
    )


@pytest.mark.parametrize(
    ("entry", "replacement"),
    (
        ("CMAKE_PROJECT_NAME:STATIC=schema_sanitizer", "CMAKE_PROJECT_NAME:STATIC=other"),
        (
            "CMAKE_GENERATOR:INTERNAL=Visual Studio 17 2022",
            "CMAKE_GENERATOR:INTERNAL=Ninja",
        ),
        (
            "CMAKE_GENERATOR_INSTANCE:INTERNAL=C:/Program Files/Microsoft Visual Studio/2022/Enterprise",
            "CMAKE_GENERATOR_INSTANCE:INTERNAL=C:/unreviewed",
        ),
        ("CMAKE_GENERATOR_PLATFORM:INTERNAL=x64", "CMAKE_GENERATOR_PLATFORM:INTERNAL=ARM64"),
        (
            "CMAKE_GENERATOR_TOOLSET:INTERNAL=v143,host=x64,version=14.44.35207",
            "CMAKE_GENERATOR_TOOLSET:INTERNAL=ClangCL",
        ),
        (
            "SCHEMA_SANITIZER_MSVC_COMPILE_PROCESSES:STRING=auto",
            "SCHEMA_SANITIZER_MSVC_COMPILE_PROCESSES:STRING=1",
        ),
    ),
)
def test_windows_toolchain_certificate_rejects_cache_mismatches(
    tmp_path: Path, entry: str, replacement: str
) -> None:
    """Every persisted Visual Studio cache constraint remains exact."""
    helper = _load_ci_helper("native/certify_windows_toolchain.py")
    build_root, paths = _windows_toolchain_tree(tmp_path)
    cache = paths["cache"]
    cache.write_text(
        cache.read_text(encoding="utf-8").replace(entry, replacement), encoding="utf-8"
    )

    with pytest.raises(AssertionError, match=entry.partition(":")[0]):
        helper.certify_windows_toolchain(build_root, paths["log"])


@pytest.mark.parametrize(
    ("suffix", "expected", "replacement"),
    (
        ("COMPILER", WINDOWS_COMPILER, "C:/unreviewed/cl.exe"),
        ("COMPILER_ID", "MSVC", "Clang"),
        ("COMPILER_VERSION", "19.44.35228.0", "19.45.0"),
        ("PLATFORM_ID", "Windows", "Linux"),
        ("COMPILER_ARCHITECTURE_ID", "x64", "ARM64"),
        ("COMPILER_LOADED", "1", "0"),
        ("COMPILER_WORKS", "TRUE", "FALSE"),
        ("ABI_COMPILED", "TRUE", "FALSE"),
        ("SIZEOF_DATA_PTR", "8", "4"),
    ),
)
@pytest.mark.parametrize("language", ("C", "CXX"))
def test_windows_toolchain_certificate_rejects_compiler_metadata_mismatches(
    tmp_path: Path, language: str, suffix: str, expected: str, replacement: str
) -> None:
    """Both languages fail closed on identity, target, or probe corruption."""
    helper = _load_ci_helper("native/certify_windows_toolchain.py")
    build_root, paths = _windows_toolchain_tree(tmp_path)
    key = f"CMAKE_{language}_{suffix}"
    metadata = paths[language]
    content = metadata.read_text(encoding="utf-8")
    content = content.replace(_cmake_assignment(key, expected), _cmake_assignment(key, replacement))
    metadata.write_text(content, encoding="utf-8")

    with pytest.raises(AssertionError, match=key):
        helper.certify_windows_toolchain(build_root, paths["log"])


def test_windows_toolchain_certificate_rejects_ambiguous_or_split_metadata(
    tmp_path: Path,
) -> None:
    """Missing, duplicate, and cross-version language metadata cannot certify."""
    helper = _load_ci_helper("native/certify_windows_toolchain.py")
    build_root, paths = _windows_toolchain_tree(tmp_path)
    cxx = paths["CXX"]
    cxx.unlink()
    with pytest.raises(AssertionError, match="exactly one generated CXX"):
        helper.certify_windows_toolchain(build_root, paths["log"])

    build_root, paths = _windows_toolchain_tree(tmp_path / "duplicate")
    duplicate = paths["C"].parents[1] / "4.3.5" / paths["C"].name
    duplicate.parent.mkdir()
    duplicate.write_text(paths["C"].read_text(encoding="utf-8"), encoding="utf-8")
    with pytest.raises(AssertionError, match="exactly one generated C compiler"):
        helper.certify_windows_toolchain(build_root, paths["log"])

    build_root, paths = _windows_toolchain_tree(tmp_path / "split")
    split = paths["CXX"].parents[1] / "4.3.5" / paths["CXX"].name
    split.parent.mkdir()
    paths["CXX"].replace(split)
    with pytest.raises(AssertionError, match="different CMake versions"):
        helper.certify_windows_toolchain(build_root, paths["log"])


def test_windows_toolchain_certificate_rejects_duplicates_and_symlinks(tmp_path: Path) -> None:
    """Generated inputs must be unique regular files below the fresh build root."""
    helper = _load_ci_helper("native/certify_windows_toolchain.py")
    build_root, paths = _windows_toolchain_tree(tmp_path)
    cache = paths["cache"]
    cache.write_text(
        cache.read_text(encoding="utf-8") + "CMAKE_GENERATOR:INTERNAL=duplicate\n",
        encoding="utf-8",
    )
    with pytest.raises(AssertionError, match="duplicate CMake cache entry"):
        helper.certify_windows_toolchain(build_root, paths["log"])

    build_root, paths = _windows_toolchain_tree(tmp_path / "assignment")
    metadata = paths["C"]
    metadata.write_text(
        metadata.read_text(encoding="utf-8")
        + _cmake_assignment("CMAKE_C_COMPILER", WINDOWS_COMPILER)
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(AssertionError, match="duplicate generated CMake assignment"):
        helper.certify_windows_toolchain(build_root, paths["log"])

    build_root, paths = _windows_toolchain_tree(tmp_path / "symlink")
    metadata = paths["CXX"]
    outside = tmp_path / "outside-CMakeCXXCompiler.cmake"
    metadata.replace(outside)
    metadata.symlink_to(outside)
    with pytest.raises(AssertionError, match="crosses a symlink"):
        helper.certify_windows_toolchain(build_root, paths["log"])


def test_windows_toolchain_certificate_rejects_ambiguous_or_symlinked_root_cache(
    tmp_path: Path,
) -> None:
    """Only one real root cache may serve as authoritative build evidence."""
    helper = _load_ci_helper("native/certify_windows_toolchain.py")
    build_root, paths = _windows_toolchain_tree(tmp_path)
    duplicate = build_root / "unexpected" / "CMakeCache.txt"
    duplicate.parent.mkdir()
    duplicate.write_text(paths["cache"].read_text(encoding="utf-8"), encoding="utf-8")
    with pytest.raises(AssertionError, match="exactly one root CMake cache"):
        helper.certify_windows_toolchain(build_root, paths["log"])

    build_root, paths = _windows_toolchain_tree(tmp_path / "symlink-cache")
    cache = paths["cache"]
    outside = tmp_path / "outside-CMakeCache.txt"
    cache.replace(outside)
    cache.symlink_to(outside)
    with pytest.raises(AssertionError, match="crosses a symlink"):
        helper.certify_windows_toolchain(build_root, paths["log"])


def test_windows_toolchain_certificate_requires_the_pinned_cmake_producer(tmp_path: Path) -> None:
    """Compiler metadata must come from the exact CMake release pinned for wheels."""
    helper = _load_ci_helper("native/certify_windows_toolchain.py")
    build_root, paths = _windows_toolchain_tree(tmp_path)
    metadata = paths["C"].parent
    metadata.rename(metadata.with_name("4.3.5"))

    with pytest.raises(AssertionError, match="CMake"):
        helper.certify_windows_toolchain(build_root, paths["log"])


@pytest.mark.parametrize(
    "sdk_lines",
    (
        "",
        "-- Selecting Windows SDK version 10.0.22621.0 to target Windows 10.0.20348.\n",
        (
            "-- Selecting Windows SDK version 10.0.26100.0 to target Windows 10.0.20348.\n"
            "-- Selecting Windows SDK version 10.0.22621.0 to target Windows 10.0.20348.\n"
        ),
        "-- Selecting Windows SDK version unknown to target Windows 10.0.20348.\n",
    ),
)
def test_windows_toolchain_certificate_rejects_unreviewed_sdk_inventory(
    tmp_path: Path, sdk_lines: str
) -> None:
    """SDK evidence must contain one well-formed selection of the reviewed version."""
    helper = _load_ci_helper("native/certify_windows_toolchain.py")
    build_root, paths = _windows_toolchain_tree(tmp_path)
    paths["log"].write_text(sdk_lines, encoding="utf-8")

    with pytest.raises(AssertionError, match="SDK"):
        helper.certify_windows_toolchain(build_root, paths["log"])


def test_windows_toolchain_certificate_rejects_a_symlinked_build_log(tmp_path: Path) -> None:
    """SDK evidence must be read from the owned regular log, never a symlink."""
    helper = _load_ci_helper("native/certify_windows_toolchain.py")
    build_root, paths = _windows_toolchain_tree(tmp_path)
    log = paths["log"]
    outside = tmp_path / "outside-cibuildwheel.log"
    log.replace(outside)
    log.symlink_to(outside)

    with pytest.raises(AssertionError, match="cibuildwheel log"):
        helper.certify_windows_toolchain(build_root, log)


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
        if runner_os == "Windows":
            (tmp_path / helper.WINDOWS_TOOLCHAIN_FILENAME).write_text(
                helper._canonical_json(helper._windows_toolchain_payload()),
                encoding="utf-8",
            )
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
