"""Shared native-folder helpers for JSON and XML folder readers."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .folder_listing import check_document_size


def is_utf8_encoding(name: str) -> bool:
    """Return whether an encoding name means UTF-8."""
    return name.lower().replace("_", "-").replace("-", "") == "utf8"


def native_text_encoding_supported(name: str) -> bool:
    """Return whether native path readers can transcode this text encoding."""
    normalized = name.lower().replace("_", "-").replace(" ", "-")
    compact = normalized.replace("-", "")
    return compact in {"utf8", "utf16", "utf16le", "utf16be", "latin1", "iso88591", "cp819"}


def memory_limit_arg(memory_limit_bytes: int | None) -> int:
    """Return the native ABI sentinel for an optional memory limit."""
    return -1 if memory_limit_bytes is None else memory_limit_bytes


def all_local_files(files: list[Any]) -> bool:
    """Return whether all listed folder files expose a native local path."""
    return all(getattr(file, "native_path", None) is not None for file in files)


class NativeFolderBatcher:
    """Batch deterministic local UTF-8 files through one native folder function."""

    def __init__(
        self,
        files: list[Any],
        *,
        native_func: Any,
        memory_limit_bytes: int | None,
        stage: str,
        validate_sizes: bool,
        max_files_per_batch: int = 64,
    ):
        """Create a native batcher over local files."""
        self._files = files
        self._native_func = native_func
        self._memory_limit_bytes = memory_limit_bytes
        self._stage = stage
        self._validate_sizes = validate_sizes
        self._max_files_per_batch = max_files_per_batch
        self._native_paths = [file.native_path or file.display_name for file in files]
        self._sizes = [max(0, file.size or 0) for file in files]
        self.index = 0

    @classmethod
    def from_files(
        cls,
        files: list[Any],
        *,
        input_text_encoding: str,
        memory_limit_bytes: int | None,
        native_func: Any | None,
        stage: str,
        validate_sizes: bool,
        max_files_per_batch: int = 64,
    ) -> "NativeFolderBatcher | None":
        """Return a native batcher when files and encoding are native-compatible."""
        if native_func is None:
            return None
        if not is_utf8_encoding(input_text_encoding):
            return None
        if not all_local_files(files):
            return None
        return cls(
            files,
            native_func=native_func,
            memory_limit_bytes=memory_limit_bytes,
            stage=stage,
            validate_sizes=validate_sizes,
            max_files_per_batch=max_files_per_batch,
        )

    def append_next_batch(
        self,
        buffer: bytearray,
        target_bytes: int,
        *,
        error_mapper: Callable[[Exception], Exception] | None = None,
    ) -> bool:
        """Append the next native-produced byte batch to a byte buffer."""
        if self.index >= len(self._files):
            return False

        start = self.index
        paths: list[str] = []
        estimated_bytes = 0
        while self.index < len(self._files) and len(paths) < self._max_files_per_batch:
            src = self._files[self.index]
            if self._validate_sizes:
                check_document_size(
                    src.display_name,
                    src.size,
                    memory_limit_bytes=self._memory_limit_bytes,
                    stage=self._stage,
                )
            paths.append(self._native_paths[self.index])
            estimated_bytes += self._sizes[self.index]
            self.index += 1
            if estimated_bytes >= target_bytes and paths:
                break

        try:
            payload = self._native_func(paths, memory_limit_arg(self._memory_limit_bytes))
        except MemoryError:
            self.index = start
            raise
        except Exception as exc:
            if error_mapper is not None:
                raise error_mapper(exc) from exc
            raise
        buffer.extend(payload)
        return True
