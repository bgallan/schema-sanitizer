"""Cross-mode malformed-input rejection contracts for hardened readers.

It requires malformed inputs to produce the same typed, privacy-safe failure across
local files, directories, and remote transport modes.
"""

from __future__ import annotations

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Thread
from typing import Iterator

import pytest

import schema_sanitizer as ss

pytestmark = pytest.mark.usefixtures("require_native")


@contextmanager
def _payload_server(payload: bytes, suffix: str) -> Iterator[str]:
    """Serve one deterministic hostile object through the public HTTP route."""

    class Handler(BaseHTTPRequestHandler):
        """Serve deterministic hostile bytes without logging test traffic."""

        def do_HEAD(self) -> None:  # noqa: N802
            """Handle HTTP HEAD requests for the local test server."""
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()

        def do_GET(self) -> None:  # noqa: N802
            """Handle HTTP GET requests for the local test server."""
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, _format: str, *_args: object) -> None:
            """Suppress routine request logging from the local test server."""
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, name="reader-rejection-http")
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/hostile.{suffix}"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def _error_fingerprint(
    error: BaseException,
) -> tuple[type[BaseException], tuple[tuple[str, object], ...]]:
    """Return stable public error context while excluding source path identity."""
    detail = dict(getattr(error, "detail", {}) or {})
    detail.pop("source", None)
    return type(error), tuple(sorted(detail.items()))


_CASES = [
    pytest.param(
        "csv",
        "csv",
        b'a,b\n1,"unterminated',
        {},
        id="csv-truncated-quote",
    ),
    pytest.param(
        "jsonl",
        "jsonl",
        b'{"a":"\\x"}\n',
        {},
        id="jsonl-invalid-escape",
    ),
    pytest.param(
        "xml",
        "xml",
        b"<root><a>&unknown;</a></root>",
        {},
        id="xml-document-unknown-entity",
    ),
    pytest.param(
        "xml",
        "xml",
        b"<rows><row><a>&unknown;</a></row></rows>",
        {"xml_row_tag": "row"},
        id="xml-row-tag-unknown-entity",
    ),
]


@pytest.mark.parametrize(("input_format", "suffix", "payload", "format_options"), _CASES)
def test_malformed_inputs_have_one_contract_across_local_directory_and_remote_modes(
    tmp_path: Path,
    input_format: str,
    suffix: str,
    payload: bytes,
    format_options: dict[str, object],
) -> None:
    """Every public source/mode rejects the same hostile bytes at the same stage."""

    local = tmp_path / f"hostile.{suffix}"
    local.write_bytes(payload)
    directory = tmp_path / "directory"
    directory.mkdir()
    (directory / f"hostile.{suffix}").write_bytes(payload)

    observed: list[tuple[type[BaseException], tuple[tuple[str, object], ...]]] = []

    def run(source: object, *, input_mode: str, multi_threading: bool, label: str) -> None:
        """Execute one source/mode case while preserving an existing destination."""
        output = tmp_path / f"output-{label}-{multi_threading}.jsonl"
        output.write_bytes(b"existing-output\n")
        with pytest.raises(ss.SchemaSanitizerInvalidArgumentError) as caught:
            ss.to_jsonl(
                source,
                output,
                input_format=input_format,
                input_mode=input_mode,
                multi_threading=multi_threading,
                on_error="stop",
                memory_limit_bytes=16 << 20,
                **format_options,
            )
        observed.append(_error_fingerprint(caught.value))
        assert output.read_bytes() == b"existing-output\n"

    for multi_threading in (False, True):
        run(local, input_mode="single_file", multi_threading=multi_threading, label="local")
        run(directory, input_mode="directory", multi_threading=multi_threading, label="directory")

    with _payload_server(payload, suffix) as remote:
        for multi_threading in (False, True):
            run(remote, input_mode="single_file", multi_threading=multi_threading, label="remote")

    assert observed
    assert all(item == observed[0] for item in observed[1:])
