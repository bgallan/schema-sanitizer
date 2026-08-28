"""Read byte-exact fuzz inputs from loose files and deterministic archives."""

from __future__ import annotations

import hashlib
import stat
import zipfile
from pathlib import Path
from typing import NamedTuple

TARGETS = ("json", "csv", "xml", "parquet")
SHA1_NAME_LENGTH = 40
ARCHIVE_SUFFIX = ".sha1.zip"
ARCHIVE_TIMESTAMP = (1980, 1, 1, 0, 0, 0)
MAX_ARCHIVE_INPUTS = 10_000
MAX_INPUT_BYTES = 16 * 1024 * 1024
MAX_ARCHIVE_BYTES = 256 * 1024 * 1024


class FuzzInput(NamedTuple):
    """One logical fuzz input, independent of its repository storage."""

    name: str
    data: bytes
    origin: Path
    archived: bool


class FuzzInputError(ValueError):
    """Raised when fuzz input storage is unsafe or malformed."""


def archive_path(role_root: Path, target: str) -> Path:
    """Return the deterministic content-addressed archive for one target."""
    return role_root / f"{target}{ARCHIVE_SUFFIX}"


def _sha1(data: bytes) -> str:
    return hashlib.sha1(data, usedforsecurity=False).hexdigest()


def is_content_addressed_name(name: str) -> bool:
    """Return whether *name* is a canonical libFuzzer SHA-1 identifier."""
    return len(name) == SHA1_NAME_LENGTH and all(
        character in "0123456789abcdef" for character in name
    )


def _record_error(errors: list[str] | None, message: str) -> None:
    if errors is None:
        raise FuzzInputError(message)
    errors.append(message)


def _archive_inputs(
    path: Path,
    *,
    errors: list[str] | None,
) -> list[FuzzInput]:
    if not path.exists():
        return []
    if path.is_symlink() or not path.is_file():
        _record_error(errors, f"fuzz archive must be a regular file: {path}")
        return []

    inputs: list[FuzzInput] = []
    try:
        with zipfile.ZipFile(path) as archive:
            members = archive.infolist()
            if len(members) > MAX_ARCHIVE_INPUTS:
                _record_error(errors, f"fuzz archive has too many inputs: {path}")
                return []
            names = [member.filename for member in members]
            if names != sorted(names) or len(names) != len(set(names)):
                _record_error(errors, f"fuzz archive members must be unique and sorted: {path}")
            total_bytes = 0
            for member in members:
                name = member.filename
                mode = member.external_attr >> 16
                invalid_name = not is_content_addressed_name(name) or "/" in name or "\\" in name
                if invalid_name or member.is_dir() or stat.S_ISLNK(mode) or member.flag_bits & 0x1:
                    _record_error(errors, f"unsafe fuzz archive member: {path}!{name}")
                    continue
                if member.date_time != ARCHIVE_TIMESTAMP:
                    _record_error(
                        errors, f"non-deterministic fuzz archive timestamp: {path}!{name}"
                    )
                if member.compress_type != zipfile.ZIP_DEFLATED:
                    _record_error(errors, f"unexpected fuzz archive compression: {path}!{name}")
                if member.file_size > MAX_INPUT_BYTES:
                    _record_error(errors, f"fuzz archive member exceeds byte limit: {path}!{name}")
                    continue
                total_bytes += member.file_size
                if total_bytes > MAX_ARCHIVE_BYTES:
                    _record_error(errors, f"fuzz archive exceeds byte limit: {path}")
                    break
                try:
                    data = archive.read(member)
                except (OSError, RuntimeError, zipfile.BadZipFile) as error:
                    _record_error(errors, f"cannot read fuzz archive member {path}!{name}: {error}")
                    continue
                actual = _sha1(data)
                if actual != name:
                    _record_error(
                        errors,
                        f"content hash mismatch: {path}!{name} (actual SHA-1: {actual})",
                    )
                    continue
                inputs.append(FuzzInput(name, data, path, True))
    except (OSError, zipfile.BadZipFile) as error:
        _record_error(errors, f"invalid fuzz archive {path}: {error}")
    return inputs


def target_inputs(
    role_root: Path,
    target: str,
    *,
    errors: list[str] | None = None,
) -> list[FuzzInput]:
    """Return stable logical inputs from one target directory and optional archive."""
    root = role_root / target
    if not root.is_dir() or root.is_symlink():
        _record_error(errors, f"missing regular fuzz target directory: {root}")
        return []

    inputs: list[FuzzInput] = []
    for path in sorted(root.iterdir()):
        if path.is_symlink() or not path.is_file():
            _record_error(errors, f"fuzz target directories must stay flat: {path}")
            continue
        if path.name.startswith("."):
            _record_error(errors, f"hidden fuzz input is not allowed: {path}")
            continue
        try:
            data = path.read_bytes()
        except OSError as error:
            _record_error(errors, f"cannot read fuzz input {path}: {error}")
            continue
        if len(data) > MAX_INPUT_BYTES:
            _record_error(errors, f"fuzz input exceeds byte limit: {path}")
            continue
        if is_content_addressed_name(path.name):
            actual = _sha1(data)
            if actual != path.name:
                _record_error(
                    errors,
                    f"content hash mismatch: {path} (actual SHA-1: {actual})",
                )
        inputs.append(FuzzInput(path.name, data, path, False))

    packed = _archive_inputs(archive_path(role_root, target), errors=errors)
    names = {case.name for case in inputs}
    for case in packed:
        if case.name in names:
            _record_error(errors, f"duplicate loose and archived fuzz input: {root / case.name}")
            continue
        names.add(case.name)
        inputs.append(case)
    return sorted(inputs, key=lambda case: case.name)
