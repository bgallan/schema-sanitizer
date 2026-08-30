#!/usr/bin/env python3
"""Certify native wheel identity beyond its packaging-level compatibility tags.

The certificate authenticates the wheel and extension bytes, proves the exact
binary architecture and macOS deployment floor, audits Windows PE imports and
reviewed runtime DLLs, and binds all evidence to one workflow run and commit.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import struct
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Mapping

CERTIFICATE_FORMAT = "schema-sanitizer-platform-wheel-v1"
PLATFORMS = {
    "linux-x86_64": {"tag": "manylinux", "suffix": ".so"},
    "macos-x86_64": {"tag": "macosx_11_0_x86_64", "suffix": ".so"},
    "macos-arm64": {"tag": "macosx_11_0_arm64", "suffix": ".so"},
    "windows-amd64": {"tag": "win_amd64", "suffix": ".pyd"},
}
WINDOWS_TOOLCHAIN = Path(__file__).with_name("windows-release-toolchain.json")
_GIT_SHA = re.compile(r"[0-9a-f]{40}(?:[0-9a-f]{24})?")
_WINDOWS_SYSTEM_IMPORT = re.compile(
    r"(?:ADVAPI32|KERNEL32|VCRUNTIME140(?:_1)?|api-ms-win-crt-[a-z0-9-]+-l1-1-0)\.dll",
    re.IGNORECASE,
)


def _sha256(payload: bytes) -> str:
    """Return the lowercase SHA-256 digest for one byte payload."""
    return hashlib.sha256(payload).hexdigest()


def _canonical_json(payload: Mapping[str, Any]) -> str:
    """Serialize certificate or policy data with canonical whitespace."""
    return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def _regular_file(path: Path, label: str) -> None:
    """Require a non-symlinked regular input file."""
    if path.is_symlink() or not path.is_file():
        raise AssertionError(f"{label} must be a regular file: {path}")


def _write_atomically(destination: Path, content: str) -> None:
    """Write a certificate atomically without following output symlinks."""
    if destination.is_symlink() or (destination.exists() and not destination.is_file()):
        raise AssertionError(f"wheel certificate output is unsafe: {destination}")
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


def _unpack_from(fmt: str, payload: bytes, offset: int, label: str) -> tuple[Any, ...]:
    """Unpack a bounded native-binary structure with a clear failure."""
    size = struct.calcsize(fmt)
    if offset < 0 or offset + size > len(payload):
        raise AssertionError(f"{label} extends beyond the native binary")
    return struct.unpack_from(fmt, payload, offset)


def _certify_elf(payload: bytes) -> dict[str, Any]:
    """Require one little-endian 64-bit x86-64 ELF shared object."""
    if len(payload) < 64 or payload[:4] != b"\x7fELF":
        raise AssertionError("Linux extension is not an ELF binary")
    if payload[4] != 2 or payload[5] != 1:
        raise AssertionError("Linux extension must be little-endian ELF64")
    file_type, machine = _unpack_from("<HH", payload, 16, "ELF header")
    if file_type != 3 or machine != 62:
        raise AssertionError(
            f"Linux extension must be an x86-64 shared object, got type={file_type}, machine={machine}"
        )
    return {"architecture": "x86_64", "format": "ELF64", "object_type": "shared"}


def _encoded_macos_version(value: int) -> str:
    """Render one Mach-O packed operating-system version."""
    return f"{value >> 16}.{(value >> 8) & 0xFF}.{value & 0xFF}"


def _certify_macho(payload: bytes, platform: str) -> dict[str, Any]:
    """Require one thin 64-bit Mach-O bundle with a macOS 11 deployment floor."""
    header = _unpack_from("<IiiIIIII", payload, 0, "Mach-O header")
    magic, cpu_type, _cpu_subtype, file_type, command_count, command_bytes, _flags, _reserved = (
        header
    )
    expected_cpu = 0x01000007 if platform == "macos-x86_64" else 0x0100000C
    architecture = "x86_64" if platform == "macos-x86_64" else "arm64"
    if magic != 0xFEEDFACF or cpu_type != expected_cpu or file_type != 8:
        raise AssertionError(
            f"macOS extension must be a thin {architecture} Mach-O bundle; "
            f"got magic={magic:#x}, cpu={cpu_type:#x}, type={file_type}"
        )
    offset = 32
    end = offset + command_bytes
    if end > len(payload):
        raise AssertionError("Mach-O load-command table extends beyond the extension")
    minimum_versions: list[int] = []
    for _index in range(command_count):
        command, size = _unpack_from("<II", payload, offset, "Mach-O load command")
        if size < 8 or offset + size > end:
            raise AssertionError("Mach-O contains an invalid load-command size")
        if command == 0x32:
            _command, _size, target_platform, minimum, _sdk, _tools = _unpack_from(
                "<IIIIII", payload, offset, "LC_BUILD_VERSION"
            )
            if target_platform != 1:
                raise AssertionError(f"Mach-O build target is not macOS: {target_platform}")
            minimum_versions.append(minimum)
        offset += size
    if offset != end or minimum_versions != [0x000B0000]:
        raise AssertionError(
            "macOS extension must contain exactly one LC_BUILD_VERSION with minos 11.0.0"
        )
    return {
        "architecture": architecture,
        "format": "Mach-O-64",
        "minimum_macos": _encoded_macos_version(minimum_versions[0]),
        "object_type": "bundle",
    }


def _pe_sections(
    payload: bytes, pe_offset: int, count: int, optional_size: int
) -> list[tuple[int, int, int]]:
    """Return PE section RVA spans and raw-file offsets."""
    section_offset = pe_offset + 24 + optional_size
    sections: list[tuple[int, int, int]] = []
    for index in range(count):
        offset = section_offset + index * 40
        virtual_size, virtual_address, raw_size, raw_offset = _unpack_from(
            "<IIII", payload, offset + 8, "PE section"
        )
        sections.append((virtual_address, max(virtual_size, raw_size), raw_offset))
    return sections


def _pe_rva_offset(rva: int, sections: list[tuple[int, int, int]], payload_size: int) -> int:
    """Translate a PE relative virtual address into a bounded file offset."""
    for virtual_address, span, raw_offset in sections:
        if virtual_address <= rva < virtual_address + span:
            offset = raw_offset + rva - virtual_address
            if offset >= payload_size:
                break
            return offset
    raise AssertionError(f"PE RVA is outside every section: {rva:#x}")


def _pe_c_string(payload: bytes, rva: int, sections: list[tuple[int, int, int]]) -> str:
    """Read a bounded ASCII string addressed by a PE RVA."""
    offset = _pe_rva_offset(rva, sections, len(payload))
    end = payload.find(b"\0", offset, min(len(payload), offset + 4096))
    if end < 0:
        raise AssertionError("PE import name is not NUL terminated")
    try:
        return payload[offset:end].decode("ascii")
    except UnicodeDecodeError as error:
        raise AssertionError("PE import name is not ASCII") from error


def _pe_imports(
    payload: bytes, directory_offset: int, sections: list[tuple[int, int, int]]
) -> list[str]:
    """Return the canonical regular-import DLL inventory for a PE image."""
    import_rva, import_size = _unpack_from(
        "<II", payload, directory_offset + 8, "PE import directory"
    )
    if import_rva == 0 or import_size < 20:
        raise AssertionError("PE extension has no import directory")
    offset = _pe_rva_offset(import_rva, sections, len(payload))
    imports: list[str] = []
    while True:
        descriptor = _unpack_from("<IIIII", payload, offset, "PE import descriptor")
        if descriptor == (0, 0, 0, 0, 0):
            break
        imports.append(_pe_c_string(payload, descriptor[3], sections))
        offset += 20
        if len(imports) > 256:
            raise AssertionError("PE import table exceeds its safety bound")
    if len(imports) != len(set(name.lower() for name in imports)):
        raise AssertionError("PE import table repeats a DLL name")
    return imports


def _certify_pe(payload: bytes, bundled_dll_names: set[str]) -> dict[str, Any]:
    """Require an AMD64 PE extension with hardened flags and closed imports."""
    if len(payload) < 64 or payload[:2] != b"MZ":
        raise AssertionError("Windows extension is not a PE binary")
    (pe_offset,) = _unpack_from("<I", payload, 0x3C, "DOS header")
    if payload[pe_offset : pe_offset + 4] != b"PE\0\0":
        raise AssertionError("Windows extension has an invalid PE signature")
    machine, section_count, _timestamp, _symbols, _symbol_count, optional_size, characteristics = (
        _unpack_from("<HHIIIHH", payload, pe_offset + 4, "PE COFF header")
    )
    optional_offset = pe_offset + 24
    (magic,) = _unpack_from("<H", payload, optional_offset, "PE optional header")
    if machine != 0x8664 or magic != 0x20B or not characteristics & 0x2000:
        raise AssertionError("Windows extension must be an AMD64 PE32+ DLL")
    (dll_characteristics,) = _unpack_from(
        "<H", payload, optional_offset + 70, "PE DLL characteristics"
    )
    required_hardening = 0x20 | 0x40 | 0x100
    if dll_characteristics & required_hardening != required_hardening:
        raise AssertionError(
            f"Windows extension omits ASLR/DEP hardening flags: {dll_characteristics:#x}"
        )
    sections = _pe_sections(payload, pe_offset, section_count, optional_size)
    directory_offset = optional_offset + 112
    imports = _pe_imports(payload, directory_offset, sections)
    bundled_lower = {name.lower() for name in bundled_dll_names}
    unsupported = sorted(
        name
        for name in imports
        if name.lower() != "python3.dll"
        and name.lower() not in bundled_lower
        and _WINDOWS_SYSTEM_IMPORT.fullmatch(name) is None
    )
    if unsupported:
        raise AssertionError(f"Windows extension imports unapproved DLLs: {unsupported}")
    if not bundled_lower.issubset({name.lower() for name in imports}):
        raise AssertionError("Windows extension does not import every certified bundled runtime")
    if any("arrow" in name.lower() for name in imports):
        raise AssertionError(f"Windows extension imports an Arrow library: {imports}")
    return {
        "architecture": "AMD64",
        "dll_characteristics": f"0x{dll_characteristics:04x}",
        "format": "PE32+",
        "imports": imports,
        "object_type": "DLL",
    }


def _windows_runtime_policy() -> tuple[dict[str, str], str]:
    """Read the canonical reviewed Windows toolchain and runtime policy."""
    _regular_file(WINDOWS_TOOLCHAIN, "Windows toolchain policy")
    serialized = WINDOWS_TOOLCHAIN.read_text(encoding="utf-8")
    payload = json.loads(serialized)
    if serialized != _canonical_json(payload):
        raise AssertionError("Windows toolchain policy is not canonical JSON")
    runtimes = payload.get("wheel_runtime_dlls")
    if payload.get("format") != "schema-sanitizer-windows-toolchain-v1" or not isinstance(
        runtimes, dict
    ):
        raise AssertionError("Windows toolchain policy has an invalid format")
    if any(re.fullmatch(r"[0-9a-f]{64}", value) is None for value in runtimes.values()):
        raise AssertionError("Windows toolchain policy contains an invalid runtime digest")
    return dict(runtimes), _sha256(serialized.encode("utf-8"))


def _native_payload_evidence(archive: zipfile.ZipFile, platform: str) -> dict[str, Any]:
    """Certify the only ABI3 extension and platform-specific native dependencies."""
    expected_suffix = str(PLATFORMS[platform]["suffix"])
    extensions = sorted(
        name
        for name in archive.namelist()
        if Path(name).parent.as_posix() == "schema_sanitizer"
        and Path(name).name.startswith("_core_abi3")
        and name.endswith((".so", ".pyd"))
    )
    if len(extensions) != 1 or not extensions[0].endswith(expected_suffix):
        raise AssertionError(f"expected one {expected_suffix} ABI3 extension, found {extensions}")
    extension_name = extensions[0]
    extension = archive.read(extension_name)
    evidence: dict[str, Any]
    if platform == "linux-x86_64":
        evidence = _certify_elf(extension)
    elif platform.startswith("macos-"):
        evidence = _certify_macho(extension, platform)
    else:
        expected_runtimes, policy_digest = _windows_runtime_policy()
        actual_runtimes = {
            name: _sha256(archive.read(name))
            for name in archive.namelist()
            if name.startswith("schema_sanitizer.libs/") and name.lower().endswith(".dll")
        }
        if actual_runtimes != expected_runtimes:
            raise AssertionError(
                f"Windows runtime DLL inventory/digest mismatch: {actual_runtimes!r}"
            )
        evidence = _certify_pe(extension, {Path(name).name for name in actual_runtimes})
        evidence["runtime_dlls"] = actual_runtimes
        evidence["toolchain_policy_sha256"] = policy_digest
    evidence["member"] = extension_name
    evidence["sha256"] = _sha256(extension)
    evidence["size"] = len(extension)
    return evidence


def build_certificate(
    wheel: Path,
    *,
    platform: str,
    github_sha: str,
    github_run_id: int,
    github_run_attempt: int,
) -> dict[str, Any]:
    """Inspect a wheel and return its deterministic native-binary certificate."""
    _regular_file(wheel, "platform wheel")
    if platform not in PLATFORMS:
        raise ValueError(f"unsupported platform certificate: {platform}")
    if _GIT_SHA.fullmatch(github_sha) is None:
        raise ValueError("github_sha must be a lowercase 40- or 64-character Git object ID")
    if github_run_id < 1 or github_run_attempt < 1:
        raise ValueError("GitHub run identity values must be positive integers")
    required_tag = str(PLATFORMS[platform]["tag"])
    if required_tag not in wheel.name or not wheel.name.endswith(".whl"):
        raise AssertionError(f"{wheel.name}: filename does not encode {platform}")
    with zipfile.ZipFile(wheel) as archive:
        native = _native_payload_evidence(archive, platform)
    return {
        "format": CERTIFICATE_FORMAT,
        "native": native,
        "platform": platform,
        "provenance": {
            "git_sha": github_sha,
            "github_run_attempt": github_run_attempt,
            "github_run_id": github_run_id,
        },
        "wheel": {
            "filename": wheel.name,
            "sha256": _sha256(wheel.read_bytes()),
            "size": wheel.stat().st_size,
        },
    }


def create_certificate(wheel: Path, certificate: Path, **options: Any) -> None:
    """Create and immediately re-verify one platform-wheel certificate."""
    payload = build_certificate(wheel, **options)
    _write_atomically(certificate, _canonical_json(payload))
    verify_certificate(
        certificate,
        wheel_directory=wheel.parent,
        platform=options["platform"],
        github_sha=options["github_sha"],
        github_run_id=options["github_run_id"],
        github_run_attempt=options["github_run_attempt"],
    )


def verify_certificate(
    certificate: Path,
    *,
    wheel_directory: Path,
    platform: str,
    github_sha: str,
    github_run_id: int,
    github_run_attempt: int,
) -> None:
    """Rebuild a platform certificate from its authenticated downloaded wheel."""
    _regular_file(certificate, "platform-wheel certificate")
    serialized = certificate.read_text(encoding="utf-8")
    payload = json.loads(serialized)
    if not isinstance(payload, Mapping) or serialized != _canonical_json(payload):
        raise AssertionError("platform-wheel certificate is not canonical JSON")
    wheel_payload = payload.get("wheel")
    if payload.get("format") != CERTIFICATE_FORMAT or not isinstance(wheel_payload, Mapping):
        raise AssertionError("platform-wheel certificate has an invalid format")
    filename = wheel_payload.get("filename")
    if not isinstance(filename, str):
        raise AssertionError("platform-wheel certificate omits wheel identity")
    if platform not in PLATFORMS or payload.get("platform") != platform:
        raise AssertionError("platform-wheel certificate platform mismatch")
    if (
        filename in {"", ".", ".."}
        or "/" in filename
        or "\\" in filename
        or Path(filename).name != filename
    ):
        raise AssertionError("platform-wheel certificate contains an unsafe wheel filename")
    if wheel_directory.is_symlink() or not wheel_directory.is_dir():
        raise AssertionError(f"wheel directory must be a directory: {wheel_directory}")
    resolved_directory = wheel_directory.resolve(strict=True)
    candidate = resolved_directory / filename
    if candidate.is_symlink():
        raise AssertionError("certified wheel must not be a symlink")
    wheel = candidate.resolve(strict=True)
    if wheel.parent != resolved_directory:
        raise AssertionError("certified wheel must be directly inside the wheel directory")
    expected = build_certificate(
        wheel,
        platform=platform,
        github_sha=github_sha,
        github_run_id=github_run_id,
        github_run_attempt=github_run_attempt,
    )
    if payload != expected:
        raise AssertionError("platform-wheel certificate or wheel bytes changed")


def _positive_integer(raw: str) -> int:
    """Parse one strictly positive command-line integer."""
    value = int(raw)
    if value < 1:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return value


def main() -> None:
    """Run the platform-wheel certificate create-or-verify interface."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("create", "verify"))
    parser.add_argument("--certificate", required=True, type=Path)
    parser.add_argument("--wheel", type=Path)
    parser.add_argument("--wheel-directory", type=Path)
    parser.add_argument("--platform", choices=tuple(PLATFORMS))
    parser.add_argument("--github-sha", required=True)
    parser.add_argument("--github-run-id", required=True, type=_positive_integer)
    parser.add_argument("--github-run-attempt", required=True, type=_positive_integer)
    args = parser.parse_args()
    try:
        if args.operation == "create":
            if args.wheel is None or args.platform is None:
                parser.error("create requires --wheel and --platform")
            create_certificate(
                args.wheel,
                args.certificate,
                platform=args.platform,
                github_sha=args.github_sha,
                github_run_id=args.github_run_id,
                github_run_attempt=args.github_run_attempt,
            )
        else:
            if args.wheel_directory is None or args.platform is None:
                parser.error("verify requires --wheel-directory and --platform")
            verify_certificate(
                args.certificate,
                wheel_directory=args.wheel_directory,
                platform=args.platform,
                github_sha=args.github_sha,
                github_run_id=args.github_run_id,
                github_run_attempt=args.github_run_attempt,
            )
        print(f"platform-wheel certificate {args.operation} passed: {args.certificate}")
    except (
        AssertionError,
        json.JSONDecodeError,
        KeyError,
        OSError,
        struct.error,
        TypeError,
        ValueError,
        zipfile.BadZipFile,
    ) as error:
        raise SystemExit(str(error)) from error


if __name__ == "__main__":
    main()
