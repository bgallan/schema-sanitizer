"""Provide deterministic synchronization primitives for runner-independent concurrency tests.

Observable conditions and events establish scheduling handshakes while retaining a bounded
timeout only as a deadlock fuse.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
from pathlib import Path
from threading import Condition, Event, Lock
from time import monotonic
from typing import Any, Protocol

# Deadlock fuse for scheduling another test thread on a contended CI host.
# Correctness must still be established by an Event/Condition/Barrier handshake.
SCHEDULER_TIMEOUT_SECONDS = 10.0
_WAIT_NOHANG = getattr(os, "WNOHANG", None)


class _WaitableSignal(Protocol):
    """Describe an event-like synchronization signal used by tests."""

    def wait(self, timeout: float | None = None) -> bool:
        """Wait for the signal and report whether it arrived."""
        ...


class _JoinableThread(Protocol):
    """Describe a thread-like worker with bounded join support."""

    def join(self, timeout: float | None = None) -> None:
        """Wait up to the requested timeout for worker termination."""
        ...

    def is_alive(self) -> bool:
        """Report whether the worker is still running."""
        ...


class _JoinableProcess(_JoinableThread, Protocol):
    """Describe a child process that can be stopped after a missed deadline."""

    def terminate(self) -> None:
        """Request termination of the child process."""
        ...


def wait_event_or_fail(
    event: _WaitableSignal,
    *,
    timeout: float = SCHEDULER_TIMEOUT_SECONDS,
) -> None:
    """Fail when an expected synchronization event misses its deadlock fuse."""
    if not event.wait(timeout):
        raise TimeoutError(f"test event was not signaled within {timeout:g} seconds")


def join_thread_or_fail(
    thread: _JoinableThread,
    *,
    timeout: float = SCHEDULER_TIMEOUT_SECONDS,
) -> None:
    """Join a test thread within the deadlock fuse and require its termination."""
    thread.join(timeout=timeout)
    if thread.is_alive():
        raise TimeoutError(f"test thread did not exit within {timeout:g} seconds")


def join_process_or_fail(
    process: _JoinableProcess,
    *,
    timeout: float = SCHEDULER_TIMEOUT_SECONDS,
) -> None:
    """Join a test child by its deadline and reap it after emergency termination."""
    process.join(timeout=timeout)
    if not process.is_alive():
        return
    try:
        process.terminate()
    except ProcessLookupError:
        pass
    process.join(timeout=timeout)
    if process.is_alive():
        kill = getattr(process, "kill", None)
        if kill is not None:
            try:
                kill()
            except ProcessLookupError:
                pass
            process.join(timeout=timeout)
    if process.is_alive():
        raise TimeoutError(f"test child did not exit or terminate within {timeout * 3:g} seconds")
    raise TimeoutError(f"test child did not exit within {timeout:g} seconds")


def wait_for_process_exit(pid: int) -> int:
    """Reap one test child or terminate it after the shared deadlock fuse."""
    if _WAIT_NOHANG is None:
        raise RuntimeError("nonblocking process reaping requires POSIX waitpid support")
    deadline = monotonic() + SCHEDULER_TIMEOUT_SECONDS
    while True:
        waited_pid, status = os.waitpid(pid, _WAIT_NOHANG)
        if waited_pid == pid:
            return status
        remaining = deadline - monotonic()
        if remaining <= 0:
            try:
                os.kill(pid, getattr(signal, "SIGKILL", signal.SIGTERM))
            except ProcessLookupError:
                pass
            reap_deadline = monotonic() + SCHEDULER_TIMEOUT_SECONDS
            while True:
                waited_pid, _status = os.waitpid(pid, _WAIT_NOHANG)
                if waited_pid == pid:
                    break
                reap_remaining = reap_deadline - monotonic()
                if reap_remaining <= 0:
                    raise TimeoutError(
                        f"terminated test child {pid} could not be reaped within "
                        f"{SCHEDULER_TIMEOUT_SECONDS:g} seconds"
                    )
                Event().wait(min(0.01, reap_remaining))
            raise TimeoutError(
                f"test child {pid} did not exit within {SCHEDULER_TIMEOUT_SECONDS:g} seconds"
            )
        Event().wait(min(0.01, remaining))


def run_isolated_python_probe(test_file: str | os.PathLike[str], probe_name: str) -> None:
    """Run one uncollected test helper in a fresh interpreter.

    Real-fork probes intentionally create inherited-lock states that require a
    multithreaded parent. Keeping that parent inside a short-lived interpreter
    prevents both its at-fork mutations and Python's multithreaded-fork warning
    from leaking into the shared pytest process.
    """
    import schema_sanitizer

    resolved = Path(test_file).resolve()
    repository_root = resolved.parents[2]
    package_root = Path(schema_sanitizer.__file__).resolve().parent
    source = (
        "import runpy, sys\n"
        "from pathlib import Path\n"
        "root = Path(sys.argv[1]).resolve().parents[2]\n"
        "expected_package = Path(sys.argv[3]).resolve()\n"
        "probe_paths = [str(root), str(root / 'tests')]\n"
        "if expected_package.is_relative_to(root):\n"
        "    probe_paths.insert(1, str(root / 'src'))\n"
        "sys.path[:0] = probe_paths\n"
        "import schema_sanitizer\n"
        "actual_package = Path(schema_sanitizer.__file__).resolve().parent\n"
        "if actual_package != expected_package:\n"
        "    raise RuntimeError(f'isolated probe package mismatch: {actual_package} != {expected_package}')\n"
        "namespace = runpy.run_path(sys.argv[1])\n"
        "probe = namespace[sys.argv[2]]\n"
        "probe()\n"
    )
    process = subprocess.Popen(
        [sys.executable, "-c", source, str(resolved), probe_name, str(package_root)],
        cwd=repository_root,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=os.name != "nt",
    )
    try:
        stdout, stderr = process.communicate(timeout=SCHEDULER_TIMEOUT_SECONDS * 3)
    except subprocess.TimeoutExpired as error:
        if os.name == "nt":
            process.kill()
        else:
            os.killpg(process.pid, signal.SIGKILL)
        stdout, stderr = process.communicate()
        details = "\n".join(part for part in (stdout.strip(), stderr.strip()) if part)
        raise AssertionError(
            f"isolated probe {resolved.name}:{probe_name} exceeded its bounded timeout"
            + (f"\n{details}" if details else "")
        ) from error
    if process.returncode == 0:
        return
    details = "\n".join(part for part in (stdout.strip(), stderr.strip()) if part)
    raise AssertionError(
        f"isolated probe {resolved.name}:{probe_name} failed with "
        f"status {process.returncode}\n{details}"
    )


class WaitObservedCondition(Condition):
    """Expose when another test thread enters or waits on a condition."""

    def __init__(self, lock: Any | None = None) -> None:
        """Initialize the wait observed condition test double."""
        super().__init__(lock)
        self.enter_observed = Event()
        self.wait_entered = Event()

    def __enter__(self) -> bool:
        """Enter the context managed by the wait observed condition test double."""
        entered = super().__enter__()
        self.enter_observed.set()
        return entered

    def wait(self, timeout: float | None = None) -> bool:
        """Wait for the wait observed condition test double to reach its terminal state."""
        self.wait_entered.set()
        return super().wait(timeout)


class ContentionObservedLock:
    """Lock wrapper exposing the first blocking acquisition attempt."""

    def __init__(self) -> None:
        """Initialize the contention observed lock test double."""
        self._lock = Lock()
        self.contention_entered = Event()

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        """Acquire the resource represented by the contention observed lock test double."""
        if not blocking:
            return self._lock.acquire(False)
        if self._lock.acquire(False):
            return True
        self.contention_entered.set()
        if timeout == -1:
            return self._lock.acquire()
        return self._lock.acquire(timeout=timeout)

    def release(self) -> None:
        """Release the resource held by the contention observed lock test double."""
        self._lock.release()

    def __enter__(self) -> ContentionObservedLock:
        """Enter the context managed by the contention observed lock test double."""
        self.acquire()
        return self

    def __exit__(self, *_exc: object) -> None:
        """Exit the context managed by the contention observed lock test double and run cleanup."""
        self.release()


__all__ = [
    "ContentionObservedLock",
    "SCHEDULER_TIMEOUT_SECONDS",
    "WaitObservedCondition",
    "join_process_or_fail",
    "join_thread_or_fail",
    "run_isolated_python_probe",
    "wait_event_or_fail",
    "wait_for_process_exit",
]
