"""Shared lifecycle helpers for PyArrow file sinks."""

from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from typing import Any

from ..core_impl.path_uris import local_path_or_reject_remote


@dataclass(slots=True)
class PyArrowOutputSink:
    """Own a PyArrow output target, optional stream, and writer."""

    target: Any
    output_stream: Any | None
    writer: Any | None = None

    def close(self) -> None:
        """Close the writer and filesystem stream in the correct order."""
        try:
            if self.writer is not None:
                self.writer.close()
        finally:
            self.writer = None
            if self.output_stream is not None:
                with suppress(Exception):
                    self.output_stream.close()
                self.output_stream = None


def open_pyarrow_output_sink(out_path: Any, *, feature: str) -> PyArrowOutputSink:
    """Open a PyArrow output target with unified close handling."""
    del feature
    if isinstance(out_path, str):
        out_path = local_path_or_reject_remote(
            out_path,
            remote_error="Remote outputs must be staged before PyArrow sink writing",
        )
    return PyArrowOutputSink(target=out_path, output_stream=None)
