"""Canonical input selectors, format rules, path validation, and text preparation."""

from __future__ import annotations

import codecs
import os
from pathlib import Path
from typing import Any, Literal, TypeAlias, cast

from ..core_impl.generated_bytes import BufferedGeneratedBytesReader
from ..core_impl.uris import (
    local_path_from_file_uri,
    looks_like_file_uri,
    looks_like_remote_uri,
    looks_like_supported_uri,
    suffix_from_uri,
)
from .directory_inputs import FolderFile

_Format: TypeAlias = str
_Source: TypeAlias = Literal["auto", "text", "path", "python", "uri", "stream"]

FORMAT_SUFFIXES: dict[str, tuple[str, ...]] = {
    "csv": (".csv",),
    "json": (".json",),
    "json_array": (".json",),
    "jsonl": (".jsonl",),
    "ndjson": (".ndjson",),
    "xml": (".xml",),
    "parquet": (".parquet", ".pq"),
}
PUBLIC_INPUT_FORMATS = frozenset(FORMAT_SUFFIXES)
PUBLIC_INPUT_MODES = frozenset({"single_file", "directory"})

_AUTO_FORMAT_BY_SUFFIX = {
    suffix: ("json" if format_name in {"jsonl", "ndjson"} else format_name)
    for format_name, suffixes in FORMAT_SUFFIXES.items()
    for suffix in suffixes
}
_SORTED_PATH_SUFFIXES = tuple(sorted(_AUTO_FORMAT_BY_SUFFIX))
_SUPPORTED_PATH_EXTENSIONS = ", ".join(
    (*_SORTED_PATH_SUFFIXES[:-1], f"or {_SORTED_PATH_SUFFIXES[-1]}")
)
_FILE_FORMATS = frozenset({"json", "json_array", "xml", "csv", "parquet"})
_SOURCE_VALUES = frozenset({"auto", "path", "text", "python", "uri", "stream"})
_SOURCE_SELECTOR_MESSAGE = "source must be 'auto', 'path', 'uri', 'stream', 'text', or 'python'"
SUPPORTED_INPUT_MESSAGE = (
    "Pass a local .json/.jsonl/.ndjson/.xml/.csv/.parquet file path, "
    "a supported remote URI, or a list of dicts."
)
_NATIVE_TEXT_ENCODINGS = frozenset({"utf-8", "utf-16", "utf-16-le", "utf-16-be", "iso8859-1"})
_PARQUET_MAGIC = b"PAR1"
_ARROW_IPC_MAGIC = b"ARROW1"


class TranscodingPathByteReader(BufferedGeneratedBytesReader):
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
        super().__init__(
            "TranscodingPathByteReader",
            default_chunk_bytes=self._read_chunk_bytes,
        )
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


def normalize_input_text_encoding(encoding: str | None) -> str:
    """Validate and canonicalize a text encoding name."""
    if encoding is not None and not isinstance(encoding, str):
        raise TypeError("io.input_text_encoding must be a string")
    normalized = "utf-8" if encoding is None else encoding.strip()
    if not normalized:
        raise ValueError("io.input_text_encoding must not be empty")
    try:
        return codecs.lookup(normalized).name
    except LookupError as error:
        raise ValueError(f"Unknown io.input_text_encoding: {encoding!r}") from error


def decode_text_bytes(buf: bytes | bytearray | memoryview, *, encoding: str) -> str:
    """Decode a byte buffer using strict error handling."""
    return bytes(buf).decode(encoding, "strict")


def is_utf8_encoding(name: str) -> bool:
    """Return whether one canonical encoding name is UTF-8."""
    return name == "utf-8"


def native_text_encoding_supported(name: str) -> bool:
    """Return whether native path readers support one canonical encoding."""
    return name in _NATIVE_TEXT_ENCODINGS


def native_input_format(format_name: str) -> str:
    """Return the canonical native frontend name for one public input format."""
    return "json" if format_name in {"jsonl", "ndjson"} else format_name


def input_format_extensions(format_name: str) -> tuple[str, ...]:
    """Return accepted extensions without leading dots for one input format."""
    return tuple(suffix.removeprefix(".") for suffix in FORMAT_SUFFIXES[format_name])


def is_filelike(obj: Any) -> bool:
    """Return whether an object exposes a callable read method."""
    return callable(getattr(obj, "read", None))


def looks_like_uri_string(value: str) -> bool:
    """Return whether a string is a supported local or remote URI."""
    try:
        return looks_like_supported_uri(value)
    except (TypeError, ValueError):
        return False


def normalize_source_selector(source: _Source) -> _Source:
    """Normalize and validate an input source selector."""
    if not isinstance(source, str):
        raise TypeError("source must be a string")
    source_text = source.strip().lower()
    if source_text not in _SOURCE_VALUES:
        raise ValueError(_SOURCE_SELECTOR_MESSAGE)
    return cast("_Source", source_text)


def resolve_auto_source(data: Any) -> _Source:
    """Infer the source selector from an input object."""
    if isinstance(data, os.PathLike):
        return "path"
    if isinstance(data, str) and looks_like_uri_string(data):
        return "uri"
    if isinstance(data, str):
        return "path"
    if isinstance(data, list):
        return "python"
    raise TypeError(f"Unsupported input. {SUPPORTED_INPUT_MESSAGE}")


def validate_source_data(data: Any, source: _Source) -> None:
    """Validate the source container; native encoding validates Python rows lazily."""
    if source == "path":
        if not isinstance(data, (str, os.PathLike)):
            raise TypeError("source='path' requires a local filesystem path.")
        if isinstance(data, str) and looks_like_uri_string(data):
            raise ValueError("source='path' requires a local filesystem path; use source='uri'.")
    if source == "uri" and (not isinstance(data, str) or not looks_like_uri_string(data)):
        raise TypeError("source='uri' requires a supported filesystem URI string.")
    if source == "stream" and not is_filelike(data):
        raise TypeError("source='stream' requires an object exposing read(max_bytes).")
    if source == "text" and not isinstance(data, (str, bytes, bytearray, memoryview)):
        raise TypeError("source='text' requires str or bytes input.")
    if source == "python" and not isinstance(data, list):
        raise TypeError("source='python' only supports list[dict] inputs.")


def source_for_file_input(input_path: object) -> _Source:
    """Return the known selector for a converter input path or URI."""
    return "uri" if looks_like_remote_uri(input_path) else "path"


def sniff_text_format(text: str) -> str:
    """Infer an input format from text content."""
    stripped = text.lstrip()
    if not stripped or stripped[0] in "[{":
        return "json"
    if stripped[0] == "<":
        return "xml"
    return "csv"


def sniff_bytes_format(buf: bytes | bytearray | memoryview) -> str:
    """Infer an input format from a byte buffer prefix."""
    view = memoryview(buf)
    index = 0
    while index < len(view) and view[index] in b" \t\r\n":
        index += 1
    if index >= len(view):
        return "json"
    remaining = len(view) - index
    if remaining >= len(_PARQUET_MAGIC) and bytes(view[index : index + 4]) == _PARQUET_MAGIC:
        return "parquet"
    if remaining >= len(_ARROW_IPC_MAGIC) and bytes(view[index : index + 6]) == _ARROW_IPC_MAGIC:
        return "ipc"
    first = view[index]
    if first in {ord("["), ord("{")}:
        return "json"
    if first == ord("<"):
        return "xml"
    return "csv"


def normalize_format_selector(format_name: _Format) -> _Format:
    """Normalize a public input format selector."""
    if not isinstance(format_name, str):
        raise TypeError("format must be a string")
    normalized = format_name.strip().lower()
    if normalized in {"arrow", "feather", "ipc"}:
        raise ValueError(
            "IPC/Arrow/Feather inputs are not supported. Use a .json, .jsonl, .xml, "
            ".csv, or .parquet file, or a list of dicts."
        )
    return "json" if normalized in {"jsonl", "ndjson"} else normalized


def resolve_auto_format(data: Any, source: _Source) -> _Format:
    """Infer the input format from a validated source."""
    if source == "python":
        return "python"
    if source == "path":
        suffix = Path(os.fspath(data)).suffix.lower()
    elif source == "uri":
        suffix = suffix_from_uri(data).lower()
    elif source == "text":
        return sniff_text_format(data) if isinstance(data, str) else sniff_bytes_format(data)
    else:
        raise ValueError("format='auto' is only supported for file paths, URIs, or list[dict].")
    format_name = _AUTO_FORMAT_BY_SUFFIX.get(suffix)
    if format_name is not None:
        return format_name
    label = "file" if source == "path" else "URI"
    raise ValueError(
        f"Unsupported input {label} extension: {suffix!r}. Expected {_SUPPORTED_PATH_EXTENSIONS}."
    )


def validate_source_format_pair(source: _Source, format_name: _Format) -> None:
    """Validate a source and format combination."""
    if source == "python" and format_name != "python":
        raise ValueError("list[dict] inputs require format='python' or format='auto'.")
    if source in {"path", "text", "uri", "stream"} and format_name not in _FILE_FORMATS:
        label = "file" if source == "path" else source
        raise ValueError(
            f"{label} inputs only support format='json', 'json_array', 'xml', 'csv', or 'parquet'."
        )


def resolve_source_and_format(
    data: Any,
    *,
    format: _Format,
    source: _Source,
) -> tuple[Any, _Source, _Format]:
    """Resolve and validate source and format selectors."""
    format_name = normalize_format_selector(format)
    source_name = normalize_source_selector(source)
    if is_filelike(data) and source_name != "stream":
        raise TypeError(f"file-like inputs are not supported. {SUPPORTED_INPUT_MESSAGE}")
    if source_name == "auto":
        source_name = resolve_auto_source(data)
    validate_source_data(data, source_name)
    if format_name == "auto":
        format_name = resolve_auto_format(data, source_name)
    validate_source_format_pair(source_name, format_name)
    return data, source_name, format_name


def prepare_native_text_data(
    data: Any,
    *,
    source: _Source,
    format_name: _Format,
    input_text_encoding: str,
) -> tuple[Any, _Source]:
    """Prepare encoded text input for native ingestion."""
    if format_name not in {"csv", "json", "json_array", "xml"}:
        return data, source
    encoding = normalize_input_text_encoding(input_text_encoding)
    if source == "path" and encoding != "utf-8":
        if native_text_encoding_supported(encoding):
            return data, source
        return TranscodingPathByteReader(data, encoding=encoding), "stream"
    if source != "path" and isinstance(data, (bytes, bytearray, memoryview)):
        return decode_text_bytes(data, encoding=encoding), source
    return data, source


def normalize_public_input_format(input_format: str | None) -> str:
    """Normalize an explicit public input format."""
    if input_format is None:
        raise ValueError(
            "input_format is required; select csv, json, json_array, jsonl, ndjson, xml, or parquet"
        )
    if not isinstance(input_format, str):
        raise TypeError("input_format must be a string")
    value = input_format.strip().lower()
    if value == "auto":
        raise ValueError("input_format='auto' is not supported; select an explicit format")
    if value not in PUBLIC_INPUT_FORMATS:
        accepted = ", ".join(sorted(PUBLIC_INPUT_FORMATS))
        raise ValueError(f"input_format must be one of: {accepted}")
    return value


def normalize_public_input_mode(input_mode: str) -> str:
    """Normalize a public path mode."""
    if not isinstance(input_mode, str):
        raise TypeError("input_mode must be a string")
    value = input_mode.strip().lower()
    if value not in PUBLIC_INPUT_MODES:
        raise ValueError("input_mode must be 'single_file' or 'directory'")
    return value


def path_suffix(value: str | os.PathLike[str]) -> str:
    """Return a local or URI path suffix."""
    raw = os.fspath(value)
    if looks_like_file_uri(raw):
        return Path(local_path_from_file_uri(raw)).suffix.lower()
    if looks_like_remote_uri(raw):
        return suffix_from_uri(raw).lower()
    return Path(raw).suffix.lower()


def validate_suffix(path: str | os.PathLike[str], input_format: str) -> None:
    """Require the exact extension associated with an input format."""
    suffix = path_suffix(path)
    accepted = FORMAT_SUFFIXES[input_format]
    if suffix not in accepted:
        expected = " or ".join(accepted)
        raise ValueError(
            f"input_format={input_format!r} requires extension {expected}; got {suffix!r}"
        )


def validate_local_path_mode(path: str | os.PathLike[str], input_mode: str) -> None:
    """Require a local file or directory matching input_mode."""
    local = Path(os.fspath(path))
    if input_mode == "single_file":
        if not local.is_file():
            raise FileNotFoundError(f"single_file input requires a file: {local}")
    elif not local.is_dir():
        raise NotADirectoryError(f"directory input requires a directory: {local}")


def single_file_descriptor(path: str | os.PathLike[str]) -> FolderFile:
    """Return a reopenable descriptor for one local or file-URI input."""
    raw = os.fspath(path)
    if looks_like_file_uri(raw):
        raw = local_path_from_file_uri(raw)
    local = Path(raw)
    return FolderFile(
        display_name=str(local),
        name=local.name,
        size=local.stat().st_size,
        open_binary=lambda: local.open("rb"),
        native_path=str(local),
    )


def display_source_file(path: str | os.PathLike[str]) -> str:
    """Return the full displayed source path for generated ETL metadata."""
    raw = os.fspath(path)
    if looks_like_remote_uri(raw) or looks_like_file_uri(raw):
        return raw
    return str(Path(raw).resolve())


def folder_file_source(
    file: FolderFile,
    source_file_by_name: dict[str, str] | None = None,
) -> str:
    """Return the full displayed path for one folder child."""
    if source_file_by_name is not None:
        source_file = source_file_by_name.get(file.name)
        if source_file is not None:
            return source_file
    raw = file.native_path or file.display_name
    if looks_like_remote_uri(raw):
        return raw
    return str(Path(raw).resolve())


def unsupported_native_directory_ingestion(reason: str | None = None) -> RuntimeError:
    """Return the public error for unsupported native directory ingestion."""
    message = (
        "Directory input requires the native C++ path-source ingestion path; "
        "this directory source or option set is not supported by native directory ingestion."
    )
    if reason:
        message = f"{message} {reason}"
    return RuntimeError(message)
