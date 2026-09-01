"""Cross-mode malformed-input rejection contracts for hardened readers.

It requires malformed inputs to produce the same typed, privacy-safe failure across
local files, directories, and remote transport modes.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Event, Thread
from time import monotonic_ns

import pytest
from _support.synchronization import join_thread_or_fail, wait_event_or_fail

import schema_sanitizer as ss
from schema_sanitizer.api_impl.operation_context import operation_finalizer_snapshot
from schema_sanitizer.core_impl.finalizer_cleanup import finalizer_cleanup_snapshot
from schema_sanitizer.core_impl.runtime_registry import runtime_service_snapshot
from schema_sanitizer.core_impl.terminal_ownership import terminal_ownership_snapshot

pytestmark = pytest.mark.usefixtures("require_native")


@contextmanager
def _payload_server(payload: bytes, suffix: str, phase_durations: dict[str, int]) -> Iterator[str]:
    """Serve hostile bytes until an explicit, bounded test-owned stop handshake."""

    class Handler(BaseHTTPRequestHandler):
        """Serve deterministic hostile bytes without logging test traffic."""

        def do_HEAD(self) -> None:  # noqa: N802
            """Handle HTTP HEAD requests for the local test server."""
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Connection", "close")
            self.end_headers()

        def do_GET(self) -> None:  # noqa: N802
            """Handle HTTP GET requests for the local test server."""
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, _format: str, *_args: object) -> None:
            """Suppress routine request logging from the local test server."""
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    # ``BaseServer.shutdown()`` has an unbounded coordination wait. The old
    # teardown region exhibited a 35-second macOS runner stall after otherwise
    # subsecond rejections. A short ``handle_request`` timeout plus an explicit
    # stop/join handshake keeps the owner non-daemon and turns a real leaked
    # request handler into a deterministic test failure.
    server.timeout = 0.05
    ready = Event()
    stop = Event()
    server_errors: list[BaseException] = []

    def serve_until_stopped() -> None:
        """Handle complete requests until the controlling test signals stop."""
        ready.set()
        try:
            while not stop.is_set():
                server.handle_request()
        except BaseException as exc:
            server_errors.append(exc)

    thread = Thread(
        target=serve_until_stopped,
        name="reader-rejection-http",
        daemon=False,
    )
    thread.start()
    wait_event_or_fail(ready)
    try:
        yield f"http://127.0.0.1:{server.server_port}/hostile.{suffix}"
    finally:
        shutdown_started_ns = monotonic_ns()
        stop.set()
        try:
            join_thread_or_fail(thread)
        finally:
            server.server_close()
        phase_durations["server_shutdown_ns"] = monotonic_ns() - shutdown_started_ns
        assert server_errors == []


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
    request: pytest.FixtureRequest,
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
        operation_finalizers_before = operation_finalizer_snapshot()
        prepared_finalizers_before = finalizer_cleanup_snapshot()
        runtime_services_before = dict(runtime_service_snapshot().service_kinds)
        terminal_owners_before = dict(terminal_ownership_snapshot().categories)
        remote_threads_before = {
            thread
            for thread in threading.enumerate()
            if thread.name.startswith("schema-sanitizer-remote-io") and thread.is_alive()
        }
        started_ns = monotonic_ns()
        try:
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
        finally:
            threading_label = "multi" if multi_threading else "single"
            request.node.user_properties.append(
                (f"duration_ns_{label}_{threading_label}", monotonic_ns() - started_ns)
            )
        assert not getattr(caught.value, "__notes__", ())
        observed.append(_error_fingerprint(caught.value))
        assert output.read_bytes() == b"existing-output\n"

        if label == "remote" and multi_threading:
            postconditions_started_ns = monotonic_ns()
            operation_finalizers_after = operation_finalizer_snapshot()
            prepared_finalizers_after = finalizer_cleanup_snapshot()
            assert all(
                after <= before
                for after, before in zip(
                    operation_finalizers_after[:2],
                    operation_finalizers_before[:2],
                    strict=True,
                )
            )
            assert operation_finalizers_after[2] == operation_finalizers_before[2]
            assert prepared_finalizers_after[0] <= prepared_finalizers_before[0]
            assert prepared_finalizers_after[1] == prepared_finalizers_before[1]
            runtime_services_after = dict(runtime_service_snapshot().service_kinds)
            assert runtime_services_after.get(
                "remote_io_coordinator", 0
            ) <= runtime_services_before.get("remote_io_coordinator", 0)
            terminal_owners_after = dict(terminal_ownership_snapshot().categories)
            assert all(
                count <= terminal_owners_before.get(category, 0)
                for category, count in terminal_owners_after.items()
                if category.startswith("remote_")
            )
            remote_threads_after = {
                thread
                for thread in threading.enumerate()
                if thread.name.startswith("schema-sanitizer-remote-io") and thread.is_alive()
            }
            assert remote_threads_after <= remote_threads_before
            request.node.user_properties.append(
                (
                    "duration_ns_remote_multi_postconditions",
                    monotonic_ns() - postconditions_started_ns,
                )
            )

    for multi_threading in (False, True):
        run(local, input_mode="single_file", multi_threading=multi_threading, label="local")
        run(directory, input_mode="directory", multi_threading=multi_threading, label="directory")

    server_phase_durations: dict[str, int] = {}
    with _payload_server(payload, suffix, server_phase_durations) as remote:
        for multi_threading in (False, True):
            run(remote, input_mode="single_file", multi_threading=multi_threading, label="remote")
    request.node.user_properties.extend(server_phase_durations.items())

    assert observed
    assert all(item == observed[0] for item in observed[1:])
