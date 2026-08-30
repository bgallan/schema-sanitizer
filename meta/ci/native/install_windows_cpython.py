"""Securely install cibuildwheel's pinned Windows CPython runtime from its NuGet ZIP.

The helper downloads only the immutable digest-pinned package, validates every archive
member before extraction, and atomically publishes a structurally certified AMD64 tree.
The owning action then verifies the interpreter runtime without invoking NuGet.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import ssl
import stat
import struct
import sys
import tempfile
import time
import zipfile
from collections.abc import Callable, Sequence
from http.client import HTTPException, HTTPSConnection
from pathlib import Path, PurePosixPath

import certifi

PYTHON_VERSION = "3.11.9"
PYTHON_PACKAGE_HOST = "api.nuget.org"
PYTHON_PACKAGE_PATH = "/v3-flatcontainer/python/3.11.9/python.3.11.9.nupkg"
MAX_ARCHIVE_MEMBERS = 10_000
MAX_EXPANDED_BYTES = 1_073_741_824
MAX_DOWNLOAD_BYTES = 268_435_456
DOWNLOAD_ATTEMPTS = 5
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
RETRYABLE_HTTP_STATUSES = frozenset({408, 429, *range(500, 600)})
INVALID_WINDOWS_CHARACTERS = frozenset('<>:"|?*')
RESERVED_WINDOWS_STEMS = frozenset(
    {"con", "prn", "aux", "nul"}
    | {f"com{index}" for index in range(1, 10)}
    | {f"lpt{index}" for index in range(1, 10)}
    | {f"com{index}" for index in "¹²³"}
    | {f"lpt{index}" for index in "¹²³"}
)


class WindowsCpythonInstallError(RuntimeError):
    """Report an unsafe package, failed certification, or publication error."""


def _require_sha256(expected: str) -> None:
    """Reject a malformed SHA-256 pin before it reaches a trust decision."""
    if len(expected) != 64 or any(character not in "0123456789abcdef" for character in expected):
        raise WindowsCpythonInstallError(f"invalid lowercase SHA-256 pin: {expected!r}")


def _sha256(path: Path) -> str:
    """Return the streaming SHA-256 digest of one regular file."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(DOWNLOAD_CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def verified_payload(path: Path, expected: str) -> bool:
    """Return whether a non-link regular file has the exact trusted digest."""
    _require_sha256(expected)
    return not path.is_symlink() and path.is_file() and _sha256(path) == expected


def download_verified(target: Path, expected: str) -> None:
    """Download the pinned package and atomically publish its verified bytes."""
    _require_sha256(expected)
    if target.parent.is_symlink() or not target.parent.is_dir():
        raise WindowsCpythonInstallError(f"download parent is unsafe: {target.parent}")
    if target.is_symlink() or (target.exists() and not target.is_file()):
        raise WindowsCpythonInstallError(f"download target is unsafe: {target}")

    context = ssl.create_default_context(cafile=certifi.where())
    for attempt in range(DOWNLOAD_ATTEMPTS):
        descriptor, temporary_name = tempfile.mkstemp(
            dir=target.parent,
            prefix=f".{target.name}.",
            suffix=".download",
        )
        os.close(descriptor)
        temporary = Path(temporary_name)
        try:
            digest = hashlib.sha256()
            downloaded_bytes = 0
            connection = HTTPSConnection(PYTHON_PACKAGE_HOST, context=context, timeout=60)
            try:
                connection.request(
                    "GET",
                    PYTHON_PACKAGE_PATH,
                    headers={"Accept-Encoding": "identity"},
                )
                response = connection.getresponse()
                if response.status != 200:
                    message = f"CPython package download returned HTTP status {response.status}"
                    if response.status in RETRYABLE_HTTP_STATUSES:
                        raise OSError(message)
                    raise WindowsCpythonInstallError(message)
                with temporary.open("wb") as stream:
                    while chunk := response.read(DOWNLOAD_CHUNK_BYTES):
                        downloaded_bytes += len(chunk)
                        if downloaded_bytes > MAX_DOWNLOAD_BYTES:
                            raise WindowsCpythonInstallError(
                                "CPython package download exceeds the configured byte limit"
                            )
                        digest.update(chunk)
                        stream.write(chunk)
            finally:
                connection.close()
            actual = digest.hexdigest()
            if actual != expected:
                raise WindowsCpythonInstallError(
                    f"CPython package SHA-256 mismatch: expected {expected}, got {actual}"
                )
            os.replace(temporary, target)
            return
        except WindowsCpythonInstallError:
            raise
        except (HTTPException, OSError):
            if attempt + 1 == DOWNLOAD_ATTEMPTS:
                raise
            time.sleep(2**attempt)
        finally:
            temporary.unlink(missing_ok=True)
    raise AssertionError("bounded download loop terminated without a result")


def ensure_verified_archive(path: Path, expected: str) -> None:
    """Reuse an exact cached package or replace it with a verified download."""
    if verified_payload(path, expected):
        return
    download_verified(path, expected)
    if not verified_payload(path, expected):
        raise WindowsCpythonInstallError(f"CPython package was not installed safely: {path}")


def _validated_member_path(member: zipfile.ZipInfo) -> PurePosixPath:
    """Return one safe portable member path after rejecting Windows aliases."""
    name = member.orig_filename
    is_directory = member.is_dir()
    candidate = name[:-1] if is_directory and name.endswith("/") else name
    if not candidate or "\x00" in candidate or "\\" in candidate or candidate.startswith("/"):
        raise WindowsCpythonInstallError(f"unsafe archive member path: {name!r}")

    components = candidate.split("/")
    for component in components:
        stem = component.split(".", 1)[0].casefold()
        if (
            component in {"", ".", ".."}
            or component[-1] in {" ", "."}
            or any(character in INVALID_WINDOWS_CHARACTERS for character in component)
            or stem in RESERVED_WINDOWS_STEMS
        ):
            raise WindowsCpythonInstallError(f"unsafe archive member path: {name!r}")
    return PurePosixPath(*components)


def _validated_archive_members(archive: zipfile.ZipFile) -> list[tuple[zipfile.ZipInfo, Path]]:
    """Return a collision-free bounded inventory containing only files and directories."""
    members = archive.infolist()
    if len(members) > MAX_ARCHIVE_MEMBERS:
        raise WindowsCpythonInstallError(
            f"CPython package has too many members: {len(members)} > {MAX_ARCHIVE_MEMBERS}"
        )

    inventory: list[tuple[zipfile.ZipInfo, Path]] = []
    kinds: dict[str, bool] = {}
    spellings: dict[str, str] = {}
    expanded_bytes = 0
    for member in members:
        path = _validated_member_path(member)
        key = path.as_posix().casefold()
        if key in kinds:
            raise WindowsCpythonInstallError(
                f"duplicate or case-colliding archive member: {member.orig_filename!r}"
            )
        for component_count in range(1, len(path.parts) + 1):
            spelling = PurePosixPath(*path.parts[:component_count]).as_posix()
            folded = spelling.casefold()
            prior = spellings.setdefault(folded, spelling)
            if prior != spelling:
                raise WindowsCpythonInstallError(
                    f"case-colliding archive path prefixes: {prior!r} and {spelling!r}"
                )

        mode_type = stat.S_IFMT(member.external_attr >> 16)
        is_directory = member.is_dir()
        expected_types = {0, stat.S_IFDIR} if is_directory else {0, stat.S_IFREG}
        if (
            member.flag_bits & 0x1
            or mode_type not in expected_types
            or (is_directory and member.file_size != 0)
        ):
            raise WindowsCpythonInstallError(
                f"encrypted, linked, or non-regular archive member: {member.orig_filename!r}"
            )
        if not is_directory:
            expanded_bytes += member.file_size
            if expanded_bytes > MAX_EXPANDED_BYTES:
                raise WindowsCpythonInstallError(
                    "CPython package expands beyond the configured byte limit"
                )
        kinds[key] = is_directory
        inventory.append((member, Path(*path.parts)))

    for _member, relative in inventory:
        for parent in relative.parents:
            if parent == Path("."):
                continue
            parent_kind = kinds.get(parent.as_posix().casefold())
            if parent_kind is False:
                raise WindowsCpythonInstallError(
                    f"archive file is also a member parent: {parent.as_posix()!r}"
                )
    return inventory


def extract_archive(archive_path: Path, destination: Path) -> None:
    """Extract a validated NuGet ZIP into one new, empty owned directory."""
    if destination.is_symlink() or not destination.is_dir() or any(destination.iterdir()):
        raise WindowsCpythonInstallError(f"extraction destination is unsafe: {destination}")
    try:
        with zipfile.ZipFile(archive_path) as archive:
            inventory = _validated_archive_members(archive)
            for member, relative in inventory:
                target = destination / relative
                if member.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(member) as source, target.open("xb") as output:
                    shutil.copyfileobj(source, output, DOWNLOAD_CHUNK_BYTES)
                if target.stat().st_size != member.file_size:
                    raise WindowsCpythonInstallError(
                        f"archive member size changed during extraction: {member.orig_filename!r}"
                    )
    except (OSError, RuntimeError, zipfile.BadZipFile) as error:
        if isinstance(error, WindowsCpythonInstallError):
            raise
        raise WindowsCpythonInstallError(
            f"invalid CPython package archive: {archive_path}"
        ) from error


def validate_amd64_pe(executable: Path) -> None:
    """Require a non-link PE executable whose COFF machine is AMD64."""
    if executable.is_symlink() or not executable.is_file():
        raise WindowsCpythonInstallError(
            f"pinned CPython executable is missing or unsafe: {executable}"
        )
    try:
        with executable.open("rb") as stream:
            if stream.read(2) != b"MZ":
                raise WindowsCpythonInstallError("pinned CPython executable is not a PE image")
            stream.seek(0x3C)
            offset_bytes = stream.read(4)
            if len(offset_bytes) != 4:
                raise WindowsCpythonInstallError("pinned CPython PE header is truncated")
            pe_offset = struct.unpack("<I", offset_bytes)[0]
            stream.seek(pe_offset)
            signature = stream.read(4)
            machine_bytes = stream.read(2)
            if signature != b"PE\0\0" or len(machine_bytes) != 2:
                raise WindowsCpythonInstallError("pinned CPython executable is not a PE image")
            if struct.unpack("<H", machine_bytes)[0] != 0x8664:
                raise WindowsCpythonInstallError("pinned CPython executable is not AMD64")
    except OSError as error:
        raise WindowsCpythonInstallError(
            f"cannot inspect CPython executable: {executable}"
        ) from error


def validate_package_tree(package_root: Path) -> Path:
    """Certify the extracted tree and return its AMD64 Python executable."""
    if package_root.is_symlink() or not package_root.is_dir():
        raise WindowsCpythonInstallError(f"pinned CPython package root is unsafe: {package_root}")
    for candidate in package_root.rglob("*"):
        if candidate.is_symlink() or not (candidate.is_file() or candidate.is_dir()):
            raise WindowsCpythonInstallError(
                f"pinned CPython package contains a link or non-regular entry: {candidate}"
            )
    executable = package_root / "tools" / "python.exe"
    validate_amd64_pe(executable)
    return executable


def _remove_owned_path(path: Path) -> None:
    """Remove only the exact package publication path without following links."""
    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.exists():
        shutil.rmtree(path)


def install_archive(
    archive_path: Path,
    package_root: Path,
    expected: str,
    *,
    candidate_validator: Callable[[Path], None] | None = None,
) -> Path:
    """Validate, extract, certify, and atomically publish a pinned CPython package."""
    if not verified_payload(archive_path, expected):
        raise WindowsCpythonInstallError(
            f"refusing to extract an unverified CPython package: {archive_path}"
        )
    parent = package_root.parent
    if parent.is_symlink() or not parent.is_dir():
        raise WindowsCpythonInstallError(f"CPython publication parent is unsafe: {parent}")
    if package_root.is_symlink() or (package_root.exists() and not package_root.is_dir()):
        raise WindowsCpythonInstallError(f"CPython publication target is unsafe: {package_root}")

    temporary = Path(tempfile.mkdtemp(dir=parent, prefix=f".{package_root.name}.extract-"))
    published = False
    try:
        extract_archive(archive_path, temporary)
        executable = validate_package_tree(temporary)
        if candidate_validator is not None:
            candidate_validator(executable)
        _remove_owned_path(package_root)
        os.replace(temporary, package_root)
        published = True
    finally:
        if not published:
            _remove_owned_path(temporary)

    executable = validate_package_tree(package_root)
    return executable


def _workspace_archive(workspace: Path) -> Path:
    """Return the owned package-cache file after rejecting workspace escape."""
    cache_root = workspace / ".work" / "cache" / "nuget"
    if cache_root.is_symlink() or not cache_root.is_dir():
        raise WindowsCpythonInstallError(f"CPython package-cache root is unsafe: {cache_root}")
    if cache_root.resolve() != cache_root:
        raise WindowsCpythonInstallError(
            f"CPython package-cache root escaped the workspace: {cache_root}"
        )
    return cache_root / f"python.{PYTHON_VERSION}.nupkg"


def _parser() -> argparse.ArgumentParser:
    """Build the small command-line parser for the immutable digest pin."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sha256", help="expected lowercase SHA-256 of the CPython NuGet package")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Install the verified CPython runtime into cibuildwheel's private cache."""
    arguments = _parser().parse_args(argv)
    workspace = Path.cwd().resolve()
    archive_path = _workspace_archive(workspace)
    ensure_verified_archive(archive_path, arguments.sha256)

    from cibuildwheel.util.file import CIBW_CACHE_PATH

    if CIBW_CACHE_PATH.is_symlink() or (CIBW_CACHE_PATH.exists() and not CIBW_CACHE_PATH.is_dir()):
        raise WindowsCpythonInstallError(f"cibuildwheel cache root is unsafe: {CIBW_CACHE_PATH}")
    CIBW_CACHE_PATH.mkdir(parents=True, exist_ok=True)
    package_parent = CIBW_CACHE_PATH / "nuget-cpython"
    if package_parent.is_symlink() or (package_parent.exists() and not package_parent.is_dir()):
        raise WindowsCpythonInstallError(
            f"cibuildwheel CPython cache root is unsafe: {package_parent}"
        )
    package_parent.mkdir(parents=True, exist_ok=True)
    executable = install_archive(
        archive_path,
        package_parent / f"python.{PYTHON_VERSION}",
        arguments.sha256,
    )
    sys.stdout.write(executable.as_posix())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
