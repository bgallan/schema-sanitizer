"""Expose JSON-array object elements as native JSON Lines bytes."""

from __future__ import annotations

from ..core_impl.byte_reader_base import BufferedSeekableByteReader
from ..core_impl.native_functions import JSON_ARRAY_FILES_TO_JSONL_BYTES, JSON_ARRAY_TO_JSONL_BYTES
from .folder_listing import FOLDER_READ_CHUNK_BYTES, FolderFile, read_folder_file_bytes
from .native_folder_common import memory_limit_arg


class JsonArrayJsonlByteReader(BufferedSeekableByteReader):
    """Seekable byte reader that splits JSON-array object elements natively."""

    def __init__(self, source: FolderFile, *, memory_limit_bytes: int | None = None):
        """Store a reopenable source and initialize native conversion state."""
        self._source = source
        self._memory_limit_bytes = memory_limit_bytes
        self._native_json_array_files_to_jsonl = JSON_ARRAY_FILES_TO_JSONL_BYTES.get()
        self._native_json_array_to_jsonl = JSON_ARRAY_TO_JSONL_BYTES.get()
        if source.native_path is None and memory_limit_bytes is None:
            raise RuntimeError(
                "non-path json_array conversion requires memory_limit_bytes for native bytes conversion"
            )
        self._done = False
        super().__init__("JsonArrayJsonlByteReader", default_chunk_bytes=FOLDER_READ_CHUNK_BYTES)
        self.seek(0)

    def _append_native_payload(self) -> bool:
        """Append native-produced JSONL for the source."""
        native_path = self._source.native_path
        if native_path is not None:
            if self._native_json_array_files_to_jsonl is None:
                raise RuntimeError(
                    "json_array path conversion requires native JSON-array file support"
                )
            payload = self._native_json_array_files_to_jsonl(
                [native_path],
                memory_limit_arg(self._memory_limit_bytes),
            )
        elif self._native_json_array_to_jsonl is not None:
            raw = read_folder_file_bytes(
                self._source,
                memory_limit_bytes=self._memory_limit_bytes,
                stage="json_parse",
            )
            try:
                payload = self._native_json_array_to_jsonl(raw)
            except Exception as exc:
                raise ValueError(
                    f"Invalid json_array file {self._source.display_name}: {exc}"
                ) from exc
        else:
            raise RuntimeError("json_array conversion requires native JSON-array bytes support")
        self._buffer.extend(payload)
        self._done = True
        return bool(payload)

    def _append_next(self, target_bytes: int) -> bool:
        """Append the native JSONL payload once."""
        del target_bytes
        if self._done:
            return False
        return self._append_native_payload()

    def _reset_reader(self) -> None:
        """Reset native conversion state."""
        self._done = False
