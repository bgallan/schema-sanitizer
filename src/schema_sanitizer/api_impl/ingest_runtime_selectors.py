"""Source, format, and text-encoding selectors for ingest runtime inputs."""

from __future__ import annotations

import codecs
import os
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import urlparse

from ..core_impl.path_uris import local_path_from_file_uri
from .ingest_runtime_binary import _sniff_bytes_format

_URI_SCHEMES = {
    "file",
    "s3",
    "gs",
    "gcs",
    "abfs",
    "adl",
    "hdfs",
    "https",
    "http",
}
_PATH_EXTENSION_TO_FORMAT = {
    ".json": "json",
    ".jsonl": "json",
    ".ndjson": "json",
    ".xml": "xml",
    ".csv": "csv",
    ".parquet": "parquet",
    ".pq": "parquet",
}
_SUPPORTED_PATH_EXTENSIONS = ".json, .jsonl, .ndjson, .xml, .csv, .parquet, or .pq"

_Format = str
_Source = Literal["auto", "text", "path", "python", "uri", "stream"]
_SOURCE_VALUES = {"auto", "path", "text", "python", "uri", "stream"}
_FILE_FORMATS = {"json", "json_array", "xml", "csv", "parquet"}
_SOURCE_SELECTOR_MESSAGE = "source must be 'auto', 'path', 'uri', 'stream', 'text', or 'python'"
_SUPPORTED_INPUT_MESSAGE = (
    "Pass a local .json/.jsonl/.ndjson/.xml/.csv/.parquet file path, "
    "a supported remote URI, or a list of dicts."
)


def _sniff_text_format(text: str) -> str:
    """Infer an input format from text content."""
    t = text.lstrip()
    if not t:
        return "json"
    if t[0] in "[{":
        return "json"
    if t[0] == "<":
        return "xml"
    return "csv"


def _normalize_input_text_encoding(encoding: str | None) -> str:
    """Validate and canonicalize a text encoding name."""
    if encoding is not None and not isinstance(encoding, str):
        raise TypeError("io.input_text_encoding must be a string")
    enc = "utf-8" if encoding is None else encoding.strip()
    if not enc:
        raise ValueError("io.input_text_encoding must not be empty")
    try:
        return codecs.lookup(enc).name
    except LookupError as e:
        raise ValueError(f"Unknown io.input_text_encoding: {encoding!r}") from e


def _decode_text_bytes(buf: bytes | bytearray | memoryview, *, encoding: str) -> str:
    """Decode a byte buffer using strict error handling."""
    return bytes(buf).decode(encoding, "strict")


def _is_filelike(obj: Any) -> bool:
    """Return whether an object exposes a callable read method."""
    return callable(getattr(obj, "read", None))


def _looks_like_uri_str(s: str) -> bool:
    """Heuristic for URI-like strings."""
    try:
        u = urlparse(s)
    except Exception:
        return False
    if not u.scheme:
        return False
    scheme = u.scheme.lower()
    if scheme not in _URI_SCHEMES:
        return False
    return scheme == "file" or bool(u.netloc)


def _normalize_format_selector(format: _Format) -> _Format:
    """Normalize a public input format selector."""
    if not isinstance(format, str):
        raise TypeError("format must be a string")
    format = format.strip().lower()
    if format in {"arrow", "feather", "ipc"}:
        raise ValueError(
            "IPC/Arrow/Feather inputs are not supported. Use a .json, .jsonl, .xml, .csv, or .parquet file, or a list of dicts."
        )
    return "json" if format in {"jsonl", "ndjson"} else format


def _normalize_source_selector(source: _Source) -> _Source:
    """Normalize and validate an input source selector."""
    if not isinstance(source, str):
        raise TypeError("source must be a string")
    source_text = source.strip().lower()
    if source_text not in _SOURCE_VALUES:
        raise ValueError(_SOURCE_SELECTOR_MESSAGE)
    return cast("_Source", source_text)


def _resolve_auto_source(data: Any) -> _Source:
    """Infer the source selector from an input object."""
    if isinstance(data, os.PathLike):
        return "path"
    if isinstance(data, str) and _looks_like_uri_str(data):
        return "uri"
    if isinstance(data, str):
        return "path"
    if isinstance(data, list):
        return "python"
    raise TypeError(f"Unsupported input. {_SUPPORTED_INPUT_MESSAGE}")


def _validate_source_data(data: Any, src: _Source) -> None:
    """Validate that input data matches its source selector."""
    if src == "path":
        if not isinstance(data, (str, os.PathLike)):
            raise TypeError("source='path' requires a local filesystem path.")
        if isinstance(data, str) and _looks_like_uri_str(data):
            raise ValueError("source='path' requires a local filesystem path; use source='uri'.")
    if src == "uri" and (not isinstance(data, str) or not _looks_like_uri_str(data)):
        raise TypeError("source='uri' requires a supported filesystem URI string.")
    if src == "stream" and not _is_filelike(data):
        raise TypeError("source='stream' requires an object exposing read(max_bytes).")
    if src == "text" and not isinstance(data, (str, bytes, bytearray, memoryview)):
        raise TypeError("source='text' requires str or bytes input.")
    if src == "python" and (
        not isinstance(data, list) or not all(isinstance(row, dict) for row in data)
    ):
        raise TypeError("source='python' only supports list[dict] inputs.")


def _resolve_auto_format(data: Any, src: _Source) -> _Format:
    """Infer the input format from a validated source."""
    if src == "python":
        return "python"
    if src == "path":
        p = Path(os.fspath(data))
        fmt = _PATH_EXTENSION_TO_FORMAT.get(p.suffix.lower())
        if fmt is not None:
            return fmt
        raise ValueError(
            f"Unsupported input file extension: {p.suffix!r}. Expected {_SUPPORTED_PATH_EXTENSIONS}."
        )
    if src == "uri":
        parsed = urlparse(data)
        path = local_path_from_file_uri(data) if parsed.scheme.lower() == "file" else parsed.path
        suffix = Path(path).suffix.lower()
        fmt = _PATH_EXTENSION_TO_FORMAT.get(suffix)
        if fmt is not None:
            return fmt
        raise ValueError(
            f"Unsupported input URI extension: {suffix!r}. Expected {_SUPPORTED_PATH_EXTENSIONS}."
        )
    if src == "text":
        return _sniff_text_format(data) if isinstance(data, str) else _sniff_bytes_format(data)
    raise ValueError("format='auto' is only supported for file paths, URIs, or list[dict].")


def _validate_source_format_pair(src: _Source, fmt: _Format) -> None:
    """Validate a source and format combination."""
    if src == "python" and fmt != "python":
        raise ValueError("list[dict] inputs require format='python' or format='auto'.")
    if src in {"path", "text", "uri", "stream"} and fmt not in _FILE_FORMATS:
        raise ValueError(
            f"{'file' if src == 'path' else src} inputs only support format='json', "
            "'json_array', 'xml', 'csv', or 'parquet'."
        )


def _resolve_source_and_format(
    data: Any,
    *,
    format: _Format,
    source: _Source,
) -> tuple[Any, _Source, _Format]:
    """Resolve and validate source and format selectors."""
    fmt = _normalize_format_selector(format)
    src = _normalize_source_selector(source)

    if _is_filelike(data) and src != "stream":
        raise TypeError(f"file-like inputs are not supported. {_SUPPORTED_INPUT_MESSAGE}")

    if src == "auto":
        src = _resolve_auto_source(data)

    _validate_source_data(data, src)

    if fmt == "auto":
        fmt = _resolve_auto_format(data, src)

    _validate_source_format_pair(src, fmt)

    return data, src, fmt
