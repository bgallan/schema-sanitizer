"""Process-signal and abrupt-shutdown contracts for remote concurrency.

It proves SIGINT drains an active remote operation and unclosed contexts do not prevent
interpreter shutdown.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _wait_for_file(path: Path, process: subprocess.Popen[str]) -> None:
    """Wait until a child publishes readiness or exits unexpectedly."""
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        try:
            if path.read_text(encoding="utf-8") == "ready":
                return
        except FileNotFoundError:
            pass
        if process.poll() is not None:
            break
        time.sleep(0.005)
    process.terminate()
    stdout, stderr = process.communicate(timeout=5)
    pytest.fail(
        f"child did not initialize: returncode={process.returncode} "
        f"stdout={stdout!r} stderr={stderr!r}"
    )


@pytest.mark.skipif(os.name != "posix", reason="POSIX SIGINT delivery required")
def test_sigint_interrupt_drains_remote_operation_context(tmp_path: Path) -> None:
    """SIGINT must unwind the waiter and still cancel/join remote work."""
    ready = tmp_path / "ready"
    closed = tmp_path / "closed"
    script = r"""
import asyncio
from pathlib import Path
import sys
import threading

sys.path.insert(0, str(Path.cwd() / "src"))
from schema_sanitizer.api_impl.operation_context import OperationExecutionContext

ready = Path(sys.argv[1])
closed = Path(sys.argv[2])
started = threading.Event()
operation = OperationExecutionContext(
    threading_mode="multi",
    memory_limit_bytes=64 << 20,
)

async def block() -> None:
    started.set()
    await asyncio.Event().wait()

future = operation.submit_remote(block)
assert started.wait(timeout=5)
ready.write_text("ready", encoding="utf-8")
try:
    future.result()
except KeyboardInterrupt:
    pass
else:
    raise AssertionError("SIGINT did not interrupt the synchronous waiter")
finally:
    operation.close()
    closed.write_text("closed", encoding="utf-8")
"""
    process = subprocess.Popen(
        [sys.executable, "-c", script, str(ready), str(closed)],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    _wait_for_file(ready, process)
    process.send_signal(signal.SIGINT)
    stdout, stderr = process.communicate(timeout=10)

    assert process.returncode == 0, f"stdout={stdout!r} stderr={stderr!r}"
    assert closed.read_text(encoding="utf-8") == "closed"
    assert "Task was destroyed" not in stderr


def test_unclosed_remote_context_does_not_block_interpreter_shutdown(
    tmp_path: Path,
) -> None:
    """An abandoned active remote context must not hang CPython shutdown."""
    ready = tmp_path / "ready"
    script = r"""
import asyncio
from pathlib import Path
import sys
import threading

sys.path.insert(0, str(Path.cwd() / "src"))
from schema_sanitizer.api_impl.operation_context import OperationExecutionContext

ready = Path(sys.argv[1])
started = threading.Event()
operation = OperationExecutionContext(
    threading_mode="multi",
    memory_limit_bytes=64 << 20,
)

async def block() -> None:
    started.set()
    await asyncio.Event().wait()

operation.submit_remote(block)
assert started.wait(timeout=5)
ready.write_text("ready", encoding="utf-8")
# Intentionally omit operation.close(). The coordinator host is daemonized so
# an abrupt interpreter exit cannot wait forever for its event loop.
"""
    process = subprocess.Popen(
        [sys.executable, "-c", script, str(ready)],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    _wait_for_file(ready, process)
    stdout, stderr = process.communicate(timeout=10)

    assert process.returncode == 0, f"stdout={stdout!r} stderr={stderr!r}"
