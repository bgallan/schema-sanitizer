"""Folder listing and bounded file-reading helpers for folder inputs."""

from __future__ import annotations

import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from ..errors import SchemaSanitizerResourceError
from .async_remote_io import looks_like_remote_uri

FOLDER_READ_CHUNK_BYTES = 1024 * 1024


class BinaryReader(Protocol):
    """Minimal binary stream interface used by folder readers."""

    def read(self, size: int = -1, /) -> bytes:
        """Read up to size bytes from the stream."""
        ...

    def close(self) -> object:
        """Close the stream."""
        ...


@dataclass(frozen=True)
class FolderFile:
    """A direct folder child that can be read as bytes."""

    display_name: str
    name: str
    size: int | None
    open_binary: Callable[[], BinaryReader]
    native_path: str | None = None


def _path_size(path: Path) -> int | None:
    """Return a local file size, or None if the platform cannot provide it."""
    try:
        return path.stat().st_size
    except OSError:
        return None


def _open_local_binary(path: Path) -> BinaryReader:
    """Open a local folder child as a binary stream."""
    return path.open("rb")


def _local_binary_opener(path: Path) -> Callable[[], BinaryReader]:
    """Return a stable opener for a local folder child."""

    def _open() -> BinaryReader:
        """Open the captured local child path."""
        return _open_local_binary(path)

    return _open


def _normalized_suffixes(suffixes: str | Sequence[str]) -> tuple[str, ...]:
    """Return normalized lowercase suffixes with leading dots."""
    values = (suffixes,) if isinstance(suffixes, str) else tuple(suffixes)
    if not values:
        raise ValueError("At least one file suffix is required")
    normalized: list[str] = []
    for value in values:
        suffix = value.lower()
        normalized.append(suffix if suffix.startswith(".") else f".{suffix}")
    return tuple(normalized)


def _suffix_description(suffixes: tuple[str, ...]) -> str:
    """Return a concise suffix list for diagnostics."""
    return " or ".join(suffixes)


def _local_folder_files(
    path: str | os.PathLike[str], *, suffixes: str | Sequence[str], reader_name: str
) -> list[FolderFile]:
    """Return deterministic non-recursive local files with accepted suffixes."""
    accepted = _normalized_suffixes(suffixes)
    folder = Path(os.fspath(path))
    if not folder.is_dir():
        raise NotADirectoryError(f"{reader_name} requires a directory: {folder}")
    paths = sorted(
        (
            child
            for child in folder.iterdir()
            if child.is_file() and child.suffix.lower() in accepted
        ),
        key=lambda child: child.name,
    )
    if not paths:
        raise ValueError(
            f"{reader_name} found no {_suffix_description(accepted)} files in: {folder}"
        )
    return [
        FolderFile(
            display_name=str(child),
            name=child.name,
            size=_path_size(child),
            open_binary=_local_binary_opener(child),
            native_path=os.fspath(child),
        )
        for child in paths
    ]


def folder_files(
    path: str | os.PathLike[str],
    *,
    suffix: str | Sequence[str],
    reader_name: str,
) -> list[FolderFile]:
    """Return deterministic non-recursive files from a local or URI folder."""
    if looks_like_remote_uri(path):
        raise ValueError(f"{reader_name} remote directories must be staged before listing")
    return _local_folder_files(path, suffixes=suffix, reader_name=reader_name)


def check_document_size(
    display_name: str,
    size: int | None,
    *,
    memory_limit_bytes: int | None,
    stage: str,
) -> None:
    """Reject a folder child that exceeds the configured memory budget."""
    if memory_limit_bytes is None or memory_limit_bytes <= 0:
        return
    if size is None:
        return
    if size <= memory_limit_bytes:
        return

    raise SchemaSanitizerResourceError(
        f"memory_limit_bytes limit exceeded during {stage}: "
        f"{size} bytes > {memory_limit_bytes} bytes; file: {display_name}",
        detail={
            "stage": stage,
            "limit_name": "memory_limit_bytes",
            "limit_bytes": memory_limit_bytes,
            "actual_bytes": size,
            "file": display_name,
        },
    )


def read_folder_file_bytes(
    file: FolderFile,
    *,
    memory_limit_bytes: int | None,
    stage: str,
) -> bytes:
    """Read one folder child without crossing the configured byte budget."""
    if memory_limit_bytes is None or memory_limit_bytes <= 0:
        stream = file.open_binary()
        try:
            return stream.read()
        finally:
            close = getattr(stream, "close", None)
            if close is not None:
                close()

    chunks: list[bytes] = []
    total = 0
    stream = file.open_binary()
    try:
        while True:
            remaining = memory_limit_bytes + 1 - total
            chunk_size = min(FOLDER_READ_CHUNK_BYTES, remaining)
            chunk = stream.read(chunk_size)
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            check_document_size(
                file.display_name,
                total,
                memory_limit_bytes=memory_limit_bytes,
                stage=stage,
            )
    finally:
        close = getattr(stream, "close", None)
        if close is not None:
            close()
    return b"".join(chunks)
