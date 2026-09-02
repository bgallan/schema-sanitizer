#!/usr/bin/env python3
"""Certify the exact Windows toolchain selected by CMake's Visual Studio generator.

Visual Studio computes its C and C++ compiler variables instead of reliably
storing them in ``CMakeCache.txt``, so this gate validates the persisted cache
fields and generated per-language metadata to prove the build used the reviewed
compiler, SDK, target, and ABI probes. The project persists CMake's authoritative
Visual Studio SDK selection, avoiding optional human-readable configure output.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
import tomllib
from pathlib import Path
from typing import Any

WINDOWS_TOOLCHAIN = Path(__file__).with_name("windows-release-toolchain.json")
PROJECT_CONFIGURATION = Path(__file__).resolve().parents[3] / "pyproject.toml"
_POLICY_FORMAT = "schema-sanitizer-windows-toolchain-v1"
_POLICY_KEYS = {
    "compiler_version",
    "format",
    "generator",
    "generator_instance",
    "redistributable_version",
    "sdk_version",
    "toolset",
    "vc_tools_version",
    "wheel_runtime_dlls",
}
_CACHE_LINE = re.compile(r"^(?P<key>[^:=#][^:=]*):(?P<type>[^=]+)=(?P<value>.*)$")
_BARE_CMAKE_VALUE = re.compile(r"[A-Za-z0-9_.:+/\\-]+")
_EXACT_CMAKE_PIN = re.compile(r"==(?P<version>[0-9]+(?:\.[0-9]+){2})")
_CERTIFICATE_FORMAT = "schema-sanitizer-windows-toolchain-certificate-v1"


def _canonical_json(payload: dict[str, Any]) -> str:
    """Serialize policy data with its required stable representation."""
    return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def _write_atomically(destination: Path, payload: dict[str, Any]) -> None:
    """Publish one canonical certificate without following an output symlink."""
    if destination.is_symlink() or (destination.exists() and not destination.is_file()):
        raise AssertionError(f"Windows toolchain certificate output is unsafe: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(_canonical_json(payload))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def _require_regular_file(path: Path, label: str) -> None:
    """Require one real, non-symlinked regular input file."""
    if path.is_symlink() or not path.is_file():
        raise AssertionError(f"{label} must be a regular non-symlinked file: {path}")


def _load_policy(path: Path) -> dict[str, Any]:
    """Load and structurally validate the canonical Windows toolchain policy."""
    _require_regular_file(path, "Windows toolchain policy")
    serialized = path.read_text(encoding="utf-8")
    payload = json.loads(serialized)
    if not isinstance(payload, dict) or serialized != _canonical_json(payload):
        raise AssertionError("Windows toolchain policy is not canonical JSON")
    if set(payload) != _POLICY_KEYS or payload.get("format") != _POLICY_FORMAT:
        raise AssertionError("Windows toolchain policy has an invalid schema")
    scalar_keys = _POLICY_KEYS - {"wheel_runtime_dlls"}
    if any(not isinstance(payload[key], str) or not payload[key] for key in scalar_keys):
        raise AssertionError("Windows toolchain policy contains an invalid scalar")
    runtimes = payload["wheel_runtime_dlls"]
    if not isinstance(runtimes, dict) or not runtimes:
        raise AssertionError("Windows toolchain policy has no runtime inventory")
    return payload


def _cmake_version_pin(path: Path) -> str:
    """Read the sole exact CMake version pin from project configuration."""
    _require_regular_file(path, "project configuration")
    try:
        payload = tomllib.loads(path.read_text(encoding="utf-8"))
        version_spec = payload["tool"]["scikit-build"]["cmake"]["version"]
    except (KeyError, TypeError, tomllib.TOMLDecodeError) as error:
        raise AssertionError("project configuration has no valid CMake version pin") from error
    if not isinstance(version_spec, str):
        raise AssertionError("project CMake version pin must be a string")
    match = _EXACT_CMAKE_PIN.fullmatch(version_spec)
    if match is None:
        raise AssertionError(f"project CMake version is not an exact pin: {version_spec!r}")
    return match.group("version")


def _require_directory(path: Path, label: str) -> None:
    """Require one real, non-symlinked directory."""
    if path.is_symlink() or not path.is_dir():
        raise AssertionError(f"{label} must be a non-symlinked directory: {path}")


def _require_tree_file(path: Path, root: Path, label: str) -> None:
    """Require a regular file reached without crossing a symlink below root."""
    try:
        relative = path.relative_to(root)
    except ValueError as error:
        raise AssertionError(f"{label} escapes the build root: {path}") from error
    current = root
    for component in relative.parts:
        current /= component
        if current.is_symlink():
            raise AssertionError(f"{label} crosses a symlink: {current}")
    if not path.is_file():
        raise AssertionError(f"{label} must be a regular file: {path}")


def _single_path(paths: list[Path], label: str) -> Path:
    """Return the sole sorted path or reject an ambiguous inventory."""
    ordered = sorted(paths)
    if len(ordered) != 1:
        raise AssertionError(f"expected exactly one {label}, found {len(ordered)}: {ordered}")
    return ordered[0]


def _cache_entries(path: Path) -> dict[str, tuple[str, str]]:
    """Parse unique typed entries from one generated CMake cache."""
    entries: dict[str, tuple[str, str]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        match = _CACHE_LINE.fullmatch(line)
        if match is None:
            continue
        key = match.group("key")
        if key in entries:
            raise AssertionError(f"duplicate CMake cache entry: {key}")
        entries[key] = (match.group("type"), match.group("value"))
    return entries


def _cmake_value(line: str, key: str) -> str | None:
    """Parse one exact generated ``set(KEY value)`` assignment."""
    prefix = f"set({key} "
    if not line.startswith(prefix):
        return None
    if not line.endswith(")"):
        raise AssertionError(f"malformed generated CMake assignment for {key}")
    encoded = line[len(prefix) : -1]
    if len(encoded) >= 2 and encoded.startswith('"') and encoded.endswith('"'):
        value = encoded[1:-1]
        if '"' in value:
            raise AssertionError(f"unsupported quoted generated CMake value for {key}")
        return value
    if _BARE_CMAKE_VALUE.fullmatch(encoded) is None:
        raise AssertionError(f"unsupported generated CMake value for {key}")
    return encoded


def _metadata_values(path: Path, language: str) -> dict[str, str]:
    """Read the required unique fields from one CMake language metadata file."""
    prefix = f"CMAKE_{language}_"
    required = {
        f"{prefix}ABI_COMPILED",
        f"{prefix}COMPILER",
        f"{prefix}COMPILER_ARCHITECTURE_ID",
        f"{prefix}COMPILER_ID",
        f"{prefix}COMPILER_LOADED",
        f"{prefix}COMPILER_VERSION",
        f"{prefix}COMPILER_WORKS",
        f"{prefix}PLATFORM_ID",
        f"{prefix}SIZEOF_DATA_PTR",
    }
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        for key in required:
            value = _cmake_value(line, key)
            if value is None:
                continue
            if key in values:
                raise AssertionError(f"duplicate generated CMake assignment: {key}")
            values[key] = value
    missing = sorted(required - values.keys())
    if missing:
        raise AssertionError(f"generated {language} compiler metadata is incomplete: {missing}")
    return values


def _expected_metadata(language: str, policy: dict[str, Any]) -> dict[str, str]:
    """Build the exact reviewed metadata values for one language."""
    prefix = f"CMAKE_{language}_"
    compiler = (
        f"{policy['generator_instance']}/VC/Tools/MSVC/{policy['vc_tools_version']}"
        "/bin/Hostx64/x64/cl.exe"
    )
    return {
        f"{prefix}ABI_COMPILED": "TRUE",
        f"{prefix}COMPILER": compiler,
        f"{prefix}COMPILER_ARCHITECTURE_ID": "x64",
        f"{prefix}COMPILER_ID": "MSVC",
        f"{prefix}COMPILER_LOADED": "1",
        f"{prefix}COMPILER_VERSION": policy["compiler_version"],
        f"{prefix}COMPILER_WORKS": "TRUE",
        f"{prefix}PLATFORM_ID": "Windows",
        f"{prefix}SIZEOF_DATA_PTR": "8",
    }


def _certificate_payload(
    policy: dict[str, Any], cmake_version: str, build_directory: Path
) -> dict[str, Any]:
    """Describe the exact certified compiler selection in portable JSON."""
    compiler = (
        f"{policy['generator_instance']}/VC/Tools/MSVC/{policy['vc_tools_version']}"
        "/bin/Hostx64/x64/cl.exe"
    )
    return {
        "build_directory": build_directory.as_posix(),
        "cmake_version": cmake_version,
        "compiler": {"path": compiler, "version": policy["compiler_version"]},
        "format": _CERTIFICATE_FORMAT,
        "generator": policy["generator"],
        "generator_instance": policy["generator_instance"],
        "platform": "x64",
        "sdk_version": policy["sdk_version"],
        "toolset": policy["toolset"],
        "vc_tools_version": policy["vc_tools_version"],
    }


def certify_windows_toolchain(
    build_root: Path,
    policy_path: Path = WINDOWS_TOOLCHAIN,
    project_configuration: Path = PROJECT_CONFIGURATION,
    *,
    direct_build: bool = False,
) -> Path:
    """Certify one Visual Studio build tree and return its binary directory."""
    policy = _load_policy(policy_path)
    cmake_version = _cmake_version_pin(project_configuration)
    _require_directory(build_root, "Windows build root")
    cache = (
        build_root / "CMakeCache.txt"
        if direct_build
        else _single_path(list(build_root.glob("*/CMakeCache.txt")), "root CMake cache")
    )
    _require_tree_file(cache, build_root, "root CMake cache")
    build_directory = cache.parent

    cache_values = _cache_entries(cache)
    expected_cache = {
        "CMAKE_GENERATOR": ("INTERNAL", policy["generator"]),
        "CMAKE_GENERATOR_INSTANCE": ("INTERNAL", policy["generator_instance"]),
        "CMAKE_GENERATOR_PLATFORM": (
            "INTERNAL",
            f"x64,version={policy['sdk_version']}",
        ),
        "CMAKE_GENERATOR_TOOLSET": ("INTERNAL", policy["toolset"]),
        "CMAKE_PROJECT_NAME": ("STATIC", "schema_sanitizer"),
        "SCHEMA_SANITIZER_MSVC_COMPILE_PROCESSES": ("STRING", "auto"),
        "SCHEMA_SANITIZER_WINDOWS_SDK_VERSION": ("INTERNAL", policy["sdk_version"]),
    }
    for key, expected_entry in expected_cache.items():
        if cache_values.get(key) != expected_entry:
            raise AssertionError(
                "Windows release cache "
                f"{key} mismatch: {cache_values.get(key)!r} != {expected_entry!r}"
            )

    metadata_root = build_directory / "CMakeFiles"
    _require_directory(metadata_root, "CMake metadata root")
    compiler_files = {
        language: _single_path(
            list(metadata_root.glob(f"*/CMake{language}Compiler.cmake")),
            f"generated {language} compiler metadata",
        )
        for language in ("C", "CXX")
    }
    if compiler_files["C"].parent != compiler_files["CXX"].parent:
        raise AssertionError("C and CXX compiler metadata come from different CMake versions")
    metadata_version = compiler_files["C"].parent.name
    if metadata_version != cmake_version:
        raise AssertionError(
            f"generated CMake metadata version mismatch: {metadata_version!r} != {cmake_version!r}"
        )
    for language, metadata in compiler_files.items():
        _require_tree_file(metadata, build_root, f"generated {language} compiler metadata")
        values = _metadata_values(metadata, language)
        expected_metadata = _expected_metadata(language, policy)
        if values != expected_metadata:
            mismatches = {
                key: (values.get(key), expected_value)
                for key, expected_value in expected_metadata.items()
                if values.get(key) != expected_value
            }
            raise AssertionError(f"Windows {language} compiler metadata mismatch: {mismatches}")
    return build_directory


def _parser() -> argparse.ArgumentParser:
    """Build the command-line parser for the Windows toolchain gate."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("build_root", type=Path)
    parser.add_argument("--policy", type=Path, default=WINDOWS_TOOLCHAIN)
    parser.add_argument("--certificate", type=Path)
    parser.add_argument(
        "--direct-build",
        action="store_true",
        help="certify a CMakeCache.txt directly below BUILD_ROOT",
    )
    return parser


def main() -> int:
    """Certify the requested build tree for command-line callers."""
    args = _parser().parse_args()
    build_directory = certify_windows_toolchain(
        args.build_root,
        args.policy,
        direct_build=args.direct_build,
    )
    if args.certificate is not None:
        policy = _load_policy(args.policy)
        _write_atomically(
            args.certificate,
            _certificate_payload(
                policy, _cmake_version_pin(PROJECT_CONFIGURATION), build_directory
            ),
        )
    print(f"Certified Windows toolchain metadata: {build_directory.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
