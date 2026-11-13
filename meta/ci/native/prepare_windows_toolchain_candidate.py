#!/usr/bin/env python3
"""Prepare a reviewable Windows toolchain policy patch from a fresh CMake probe.

This maintenance helper never changes the checked-out policy. The release jobs
continue to certify against the reviewed, checked-in version.
"""

from __future__ import annotations

import argparse
import difflib
import hashlib
import importlib
import importlib.metadata
import json
import platform
import re
import shlex
import tempfile
from pathlib import Path
from typing import Any

from meta.ci.native import certify_windows_toolchain as certification
from meta.ci.quality.run_bounded_command import run_bounded

POLICY_RELATIVE = Path("meta/ci/native/windows-release-toolchain.json")
_VERSION = re.compile(r"[0-9]+(?:\.[0-9]+){3}")
_RUNTIME = re.compile(
    r"schema_sanitizer\.libs/(?P<stem>msvcp140(?:_1|_atomic_wait)?)"
    r"-[0-9a-f]{32}\.dll"
)
_REQUIRED_RUNTIME_STEMS = {"msvcp140", "msvcp140_1", "msvcp140_atomic_wait"}


def _canonical_json(payload: dict[str, Any]) -> str:
    """Serialize one candidate or observation with a stable representation."""
    return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def _probe_cmake(build_root: Path, policy: dict[str, Any]) -> Path:
    """Configure a small C/C++ project with the release generator and pins."""
    installed_version = importlib.metadata.version("cmake")
    expected_version = certification._cmake_version_pin(certification.PROJECT_CONFIGURATION)
    if installed_version != expected_version:
        raise AssertionError(f"CMake probe version mismatch: {installed_version!r}")
    cmake_package = importlib.import_module("cmake")
    cmake = Path(cmake_package.CMAKE_BIN_DIR) / "cmake.exe"
    if not cmake.is_file():
        raise AssertionError("the hash-locked CMake executable is unavailable")

    source = build_root / "source"
    build = build_root / "build"
    source.mkdir(parents=True)
    source.joinpath("CMakeLists.txt").write_text(
        "cmake_minimum_required(VERSION 3.21)\n"
        "project(schema_sanitizer LANGUAGES C CXX)\n"
        'set(SCHEMA_SANITIZER_MSVC_COMPILE_PROCESSES auto CACHE STRING "" FORCE)\n'
        "set(SCHEMA_SANITIZER_WINDOWS_SDK_VERSION "
        '"${CMAKE_VS_WINDOWS_TARGET_PLATFORM_VERSION}" CACHE INTERNAL "" FORCE)\n',
        encoding="utf-8",
    )
    command = [
        cmake.as_posix(),
        "-S",
        source.as_posix(),
        "-B",
        build.as_posix(),
        "-G",
        policy["generator"],
        "-A",
        f"x64,version={policy['sdk_version']}",
        "-T",
        policy["toolset"],
        f"-DCMAKE_GENERATOR_INSTANCE:INTERNAL={policy['generator_instance']}",
    ]
    configure = build_root / "configure.sh"
    configure.write_text(
        "#!/usr/bin/env bash\nset -euo pipefail\n" + shlex.join(command) + "\n",
        encoding="utf-8",
    )
    run_bounded(
        ["bash", configure.as_posix()],
        timeout_seconds=180,
        label="windows-toolchain-cmake-probe",
    )
    return build


def _compiler_version(build: Path) -> str:
    """Read both CMake language records and require the same MSVC version."""
    versions = []
    for language in ("C", "CXX"):
        path = certification._single_path(
            list((build / "CMakeFiles").glob(f"*/CMake{language}Compiler.cmake")),
            f"generated {language} compiler metadata",
        )
        values = certification._metadata_values(path, language)
        versions.append(values[f"CMAKE_{language}_COMPILER_VERSION"])
    if len(set(versions)) != 1 or _VERSION.fullmatch(versions[0]) is None:
        raise AssertionError(f"C and CXX did not select one valid MSVC version: {versions}")
    return versions[0]


def _runtime_inventory(policy: dict[str, Any], redist: Path) -> dict[str, str]:
    """Record source DLL hashes without guessing their post-repair wheel names."""
    current = policy["wheel_runtime_dlls"]
    stems = set()
    for name in current:
        match = _RUNTIME.fullmatch(name)
        if match is None or match.group("stem") in stems:
            raise AssertionError(f"unexpected reviewed runtime entry: {name}")
        stems.add(match.group("stem"))
    if stems != _REQUIRED_RUNTIME_STEMS:
        raise AssertionError(f"incomplete reviewed runtime inventory: {sorted(stems)}")
    for stem in ("vcruntime140", "vcruntime140_1"):
        if not (redist / f"{stem}.dll").is_file():
            raise AssertionError(f"required Windows runtime is missing: {stem}.dll")
    inventory = {}
    for stem in sorted(stems):
        path = redist / f"{stem}.dll"
        if path.is_symlink() or not path.is_file():
            raise AssertionError(f"reviewed Windows runtime is missing or unsafe: {path}")
        inventory[f"{stem}.dll"] = hashlib.sha256(path.read_bytes()).hexdigest()
    return inventory


def _diff(relative: Path, previous: str, candidate: str) -> str:
    """Render an applyable unified diff for a reviewed repository file."""
    return "".join(
        difflib.unified_diff(
            previous.splitlines(keepends=True),
            candidate.splitlines(keepends=True),
            fromfile=f"a/{relative.as_posix()}",
            tofile=f"b/{relative.as_posix()}",
        )
    )


def prepare_candidate(build: Path, redist: Path, output: Path) -> bool:
    """Validate observed inputs and write a candidate patch without applying it."""
    policy = certification._load_policy(certification.WINDOWS_TOOLCHAIN)
    observed_version = _compiler_version(build)
    reviewed_versions = policy["compiler_versions"]
    if observed_version.split(".")[:2] != reviewed_versions[0].split(".")[:2]:
        raise AssertionError(f"MSVC feature version changed: {observed_version!r}; review manually")
    candidate = dict(policy)
    candidate["compiler_versions"] = sorted(
        set(reviewed_versions) | {observed_version},
        key=lambda version: tuple(map(int, version.split("."))),
    )
    source_runtimes = _runtime_inventory(policy, redist)
    candidate_text = _canonical_json(candidate)
    previous_text = certification.WINDOWS_TOOLCHAIN.read_text(encoding="utf-8")

    output.mkdir(parents=True, exist_ok=True)
    candidate_policy = output / "windows-release-toolchain.json"
    candidate_policy.write_text(candidate_text, encoding="utf-8")
    certification.certify_windows_toolchain(build, policy_path=candidate_policy, direct_build=True)
    patch = _diff(POLICY_RELATIVE, previous_text, candidate_text)
    (output / "windows-toolchain.patch").write_text(patch, encoding="utf-8")
    changed = bool(patch)
    (output / "observed.json").write_text(
        _canonical_json(
            {
                "compiler_version": observed_version,
                "drift": changed,
                "redist_dlls": source_runtimes,
                "redistributable_version": policy["redistributable_version"],
                "sdk_version": policy["sdk_version"],
                "toolset": policy["toolset"],
            }
        ),
        encoding="utf-8",
    )
    if changed:
        print(
            "::warning title=Windows toolchain policy drift::"
            "Review windows-toolchain.patch and run the complete CI before merging."
        )
    else:
        print("Windows compiler version matches the reviewed policy")
    return changed


def main() -> None:
    """Probe the hosted Windows runner and upload no changes to the repository."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path(".work/windows-toolchain-candidate"))
    args = parser.parse_args()
    if platform.system() != "Windows":
        parser.error("the live toolchain probe requires a Windows runner")
    policy = certification._load_policy(certification.WINDOWS_TOOLCHAIN)
    work = Path(".work")
    if work.is_symlink() or (work.exists() and not work.is_dir()):
        raise AssertionError("maintenance work directory is unsafe")
    work.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="windows-toolchain-probe-", dir=work) as name:
        build = _probe_cmake(Path(name), policy)
        redist = (
            Path(policy["generator_instance"])
            / "VC"
            / "Redist"
            / "MSVC"
            / policy["redistributable_version"]
            / "x64"
            / "Microsoft.VC143.CRT"
        )
        prepare_candidate(build, redist, args.output)


if __name__ == "__main__":
    main()
