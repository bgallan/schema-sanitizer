"""Replayable path reader that transcodes text files to UTF-8 chunks."""

from __future__ import annotations

import codecs
import os
from typing import Any

from .byte_reader_base import BufferedSeekableByteReader


class TranscodingPathByteReader(BufferedSeekableByteReader):
    """Seekable byte reader for non-UTF-8 local path inputs."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        encoding: str,
        read_chunk_bytes: int = 1 << 20,
    ):
        """Open a path and initialize strict incremental transcoding."""
        self._path = os.fspath(path)
        self._encoding = codecs.lookup(encoding).name
        self._read_chunk_bytes = max(1, int(read_chunk_bytes))
        self._stream: Any = None
        self._decoder: Any = None
        self._source_eof = False
        super().__init__("TranscodingPathByteReader", default_chunk_bytes=self._read_chunk_bytes)
        self.seek(0)

    def _close_stream(self) -> None:
        """Close the current binary source stream."""
        stream = self._stream
        self._stream = None
        if stream is not None:
            stream.close()

    def _open_stream(self) -> None:
        """Open the binary path source and reset decoder state."""
        self._close_stream()
        self._stream = open(self._path, "rb")
        self._decoder = codecs.getincrementaldecoder(self._encoding)("strict")
        self._source_eof = False

    def _append_utf8(self, text: str) -> bool:
        """Append decoded text as UTF-8 bytes."""
        if not text:
            return False
        self._buffer.extend(text.encode("utf-8"))
        return True

    def _append_next(self, target_bytes: int) -> bool:
        """Append the next transcoded UTF-8 chunk."""
        del target_bytes
        if self._source_eof:
            return False
        if self._stream is None or self._decoder is None:
            self._open_stream()
        raw = self._stream.read(self._read_chunk_bytes)
        if raw:
            return self._append_utf8(self._decoder.decode(raw, final=False))
        self._source_eof = True
        try:
            return self._append_utf8(self._decoder.decode(b"", final=True))
        finally:
            self._close_stream()

    def _reset_reader(self) -> None:
        """Reset transcoding to the beginning of the path."""
        self._open_stream()

    def close(self) -> None:
        """Release buffered bytes and the open path handle."""
        self._close_stream()
        super().close()
