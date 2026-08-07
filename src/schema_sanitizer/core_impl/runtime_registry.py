"""Transactional, phased registry for runtime helpers participating in shutdown."""

from __future__ import annotations

import inspect
import os
import threading
from dataclasses import dataclass
from enum import Enum, IntEnum, auto
from time import monotonic_ns
from typing import Callable

from .diagnostic_epoch import diagnostic_transition
from .durations import remaining_seconds
from .fork_safety import ensure_runtime_fork_safe
from .safe_errors import clear_exception_traceback


class RuntimeServicePhase(IntEnum):
    """Order runtime services by their shutdown dependency phase."""

    PRODUCER = 10
    CLEANUP_PRODUCER = 20
    CONSUMER = 30


class RuntimeCloseStatus(Enum):
    """Report one runtime service's progress toward quiescence."""

    QUIESCENT = auto()
    PROGRESS = auto()
    RETRY = auto()
    PARKED = auto()
    FAILED = auto()


class _ServiceState(Enum):
    RESERVED = auto()
    START_AUTHORIZED = auto()
    ACTIVE = auto()
    CANCELLED = auto()
    QUIESCENT = auto()


@dataclass(frozen=True, slots=True)
class RuntimeServiceSnapshot:
    """Describe registered runtime services and admission state."""

    registered_services: int
    service_kinds: tuple[tuple[str, int], ...]
    generation: int
    progress_epoch: int
    last_progress_ns: int
    reserved_services: int = 0
    admission_closed: bool = False
    capacity: int = 256
    rejected_services: int = 0
    circuit_open: bool = False


class _ServiceEntry:
    __slots__ = (
        "owner",
        "close_call",
        "kind",
        "close_name",
        "generation",
        "phase",
        "priority",
        "active",
        "state",
    )

    def __init__(
        self,
        owner: object,
        *,
        kind: str,
        close_name: str,
        generation: int,
        phase: RuntimeServicePhase,
        priority: int,
        close_call: Callable[[float], object],
    ) -> None:
        # The registry owns one strong control block until quiescence.  A weak
        # reference alone can make a live host disappear from shutdown.
        self.owner = owner
        self.close_call = close_call
        self.kind = kind
        self.close_name = close_name
        self.generation = generation
        self.phase = phase
        self.priority = priority
        self.active = False
        self.state = _ServiceState.RESERVED


class RuntimeServiceRegistration:
    """Idempotent two-phase registration token."""

    __slots__ = ("_registry", "_token", "_pid", "_closed", "_active")

    def __init__(self, registry: "_RuntimeServiceRegistry", token: int) -> None:
        self._registry = registry
        self._token = token
        self._pid = os.getpid()
        self._closed = False
        self._active = False

    def activate(self) -> None:
        if self._closed or self._active or os.getpid() != self._pid:
            return
        self._registry.activate(self._token)
        self._active = True

    def start_thread(self, thread: threading.Thread) -> None:
        """Atomically authorize, start and activate one exact internal thread."""
        if self._closed or self._active or os.getpid() != self._pid:
            raise RuntimeError("runtime service registration is not startable")
        self._registry.start_thread(self._token, thread)
        self._active = True

    def close(self) -> None:
        if self._closed or os.getpid() != self._pid:
            return
        self._closed = True
        self._registry.unregister(self._token)


class _RuntimeServiceRegistry:
    def __init__(self) -> None:
        self._reset(os.getpid())

    def _reset(self, pid: int) -> None:
        self._pid = pid
        self._lock = threading.Lock()
        self._entries: dict[int, _ServiceEntry] = {}
        self._sequence = 0
        self._generation = 1
        self._progress_epoch = 0
        self._last_progress_ns = monotonic_ns()
        self._admission_closed = False
        self._capacity = 256
        self._rejected_services = 0
        self._circuit_open = False

    def _ensure_process(self) -> None:
        if self._pid != os.getpid():
            self._reset(os.getpid())

    def _mark_progress_locked(self) -> None:
        self._progress_epoch += 1
        self._last_progress_ns = monotonic_ns()
        diagnostic_transition()

    def _remove_dead(self, token: int, generation: int) -> None:
        with self._lock:
            entry = self._entries.get(token)
            if entry is not None and entry.generation == generation:
                self._entries.pop(token, None)
                if len(self._entries) < self._capacity:
                    self._circuit_open = False
                self._mark_progress_locked()

    @staticmethod
    def _prepare_close(owner: object, close_name: str) -> Callable[[float], object]:
        try:
            inspect.getattr_static(owner, close_name)
        except AttributeError:
            raise TypeError(
                f"runtime service has no declared close method {close_name!r}"
            ) from None
        method = getattr(owner, close_name)
        if not callable(method):
            raise TypeError("runtime service close method must be callable")
        try:
            signature = inspect.signature(method)
            signature.bind(deadline_seconds=0.0)
        except (TypeError, ValueError):
            try:
                signature = inspect.signature(method)
                signature.bind()
            except (TypeError, ValueError):
                raise TypeError(
                    "runtime service close method must accept deadline_seconds or no arguments"
                ) from None

            def close_without_deadline(_remaining: float) -> object:
                return method()

            return close_without_deadline

        def close_with_deadline(remaining: float) -> object:
            return method(deadline_seconds=remaining)

        return close_with_deadline

    def reserve(
        self,
        owner: object,
        *,
        kind: str,
        close_name: str,
        phase: RuntimeServicePhase = RuntimeServicePhase.PRODUCER,
        priority: int = 0,
    ) -> RuntimeServiceRegistration:
        self._ensure_process()
        ensure_runtime_fork_safe()
        if type(kind) is not str or type(close_name) is not str:
            raise TypeError("runtime service metadata must be exact strings")
        if not isinstance(phase, RuntimeServicePhase):
            raise TypeError("runtime service phase must be RuntimeServicePhase")
        if isinstance(priority, bool) or not isinstance(priority, int):
            raise TypeError("runtime service priority must be an integer")
        close_call = self._prepare_close(owner, close_name)
        with self._lock:
            if self._admission_closed or self._circuit_open:
                raise RuntimeError("runtime service admission is closed")
            if len(self._entries) >= self._capacity:
                self._rejected_services += 1
                self._circuit_open = True
                self._mark_progress_locked()
                raise RuntimeError("runtime service registry capacity exhausted")
            self._sequence += 1
            token = self._sequence
            generation = self._generation
            self._entries[token] = _ServiceEntry(
                owner,
                kind=kind,
                close_name=close_name,
                generation=generation,
                phase=phase,
                priority=priority,
                close_call=close_call,
            )
            self._mark_progress_locked()
        return RuntimeServiceRegistration(self, token)

    def register(
        self,
        owner: object,
        *,
        kind: str,
        close_name: str,
        phase: RuntimeServicePhase = RuntimeServicePhase.PRODUCER,
        priority: int = 0,
    ) -> RuntimeServiceRegistration:
        registration = self.reserve(
            owner,
            kind=kind,
            close_name=close_name,
            phase=phase,
            priority=priority,
        )
        registration.activate()
        return registration

    def activate(self, token: int) -> None:
        self._ensure_process()
        with self._lock:
            entry = self._entries.get(int(token))
            if entry is None:
                raise RuntimeError("runtime service reservation is no longer valid")
            if entry.state is _ServiceState.CANCELLED:
                raise RuntimeError("runtime service reservation was cancelled")
            if not entry.active:
                entry.active = True
                entry.state = _ServiceState.ACTIVE
                self._mark_progress_locked()

    def start_thread(self, token: int, thread: threading.Thread) -> None:
        """Hold the registry gate across thread.start() to prevent late starts."""
        self._ensure_process()
        if type(thread) is not threading.Thread:
            raise TypeError("runtime service start requires an exact Thread")
        with self._lock:
            entry = self._entries.get(int(token))
            if entry is None or entry.state is not _ServiceState.RESERVED:
                raise RuntimeError("runtime service reservation is no longer startable")
            if self._admission_closed:
                entry.state = _ServiceState.CANCELLED
                self._entries.pop(int(token), None)
                self._mark_progress_locked()
                raise RuntimeError("runtime service admission closed before thread start")
            entry.state = _ServiceState.START_AUTHORIZED
            self._mark_progress_locked()
            try:
                thread.start()
            except BaseException:
                entry.state = _ServiceState.RESERVED
                self._mark_progress_locked()
                raise
            entry.active = True
            entry.state = _ServiceState.ACTIVE
            self._mark_progress_locked()

    def unregister(self, token: int) -> None:
        self._ensure_process()
        with self._lock:
            entry = self._entries.pop(int(token), None)
            if entry is not None:
                entry.state = _ServiceState.QUIESCENT
                if len(self._entries) < self._capacity:
                    self._circuit_open = False
                self._mark_progress_locked()

    def close_admission(self) -> None:
        self._ensure_process()
        with self._lock:
            if not self._admission_closed:
                self._admission_closed = True
                self._mark_progress_locked()

    def reopen_admission_for_tests(self) -> None:
        with self._lock:
            self._admission_closed = False
            if not self._entries:
                self._circuit_open = False
            self._mark_progress_locked()

    @staticmethod
    def _call_close(method: Callable[..., object], remaining: float) -> object:
        try:
            signature = inspect.signature(method)
            signature.bind(deadline_seconds=remaining)
        except (TypeError, ValueError):
            return method()
        return method(deadline_seconds=remaining)

    @staticmethod
    def _is_quiescent_result(result: object) -> bool:
        return result is True or result is RuntimeCloseStatus.QUIESCENT

    def close_all(
        self,
        *,
        deadline_ns: int,
        phase: RuntimeServicePhase | None = None,
    ) -> tuple[int, int]:
        """Close services in declared phase/priority order until the deadline."""
        self._ensure_process()
        closed = 0
        while remaining_seconds(deadline_ns) > 0:
            with self._lock:
                snapshot = tuple(
                    sorted(
                        (
                            entry.phase,
                            entry.priority,
                            token,
                            entry.generation,
                            entry.owner,
                            entry.close_call,
                        )
                        for token, entry in self._entries.items()
                        if phase is None or entry.phase is phase
                    )
                )
            if not snapshot:
                break
            progressed = False
            for _entry_phase, _priority, token, generation, owner, close_call in snapshot:
                remaining = remaining_seconds(deadline_ns)
                if remaining <= 0:
                    break
                try:
                    result = close_call(remaining)
                except Exception as exc:
                    clear_exception_traceback(exc)
                    result = RuntimeCloseStatus.RETRY
                if self._is_quiescent_result(result):
                    self._remove_dead(token, generation)
                    closed += 1
                    progressed = True
                elif result is RuntimeCloseStatus.PROGRESS:
                    progressed = True
            if not progressed:
                # Services may be waiting for their own worker completions. Do
                # not prematurely conclude that a retry cannot make progress.
                threading.Event().wait(min(0.005, remaining_seconds(deadline_ns)))
        with self._lock:
            remaining_services = sum(
                1 for entry in self._entries.values() if phase is None or entry.phase is phase
            )
            return closed, remaining_services

    def snapshot(self) -> RuntimeServiceSnapshot:
        self._ensure_process()
        with self._lock:
            counts: dict[str, int] = {}
            reserved = 0
            for entry in self._entries.values():
                counts[entry.kind] = counts.get(entry.kind, 0) + 1
                if entry.state is _ServiceState.RESERVED:
                    reserved += 1
            return RuntimeServiceSnapshot(
                len(self._entries),
                tuple(sorted(counts.items())),
                self._generation,
                self._progress_epoch,
                self._last_progress_ns,
                reserved,
                self._admission_closed,
                self._capacity,
                self._rejected_services,
                self._circuit_open,
            )

    def reset_after_fork(self) -> None:
        self._reset(os.getpid())
        self._admission_closed = True


_RUNTIME_SERVICES = _RuntimeServiceRegistry()


def reserve_runtime_service(
    owner: object,
    *,
    kind: str,
    close_name: str = "close",
    phase: RuntimeServicePhase = RuntimeServicePhase.PRODUCER,
    priority: int = 0,
) -> RuntimeServiceRegistration:
    """Reserve a runtime-service slot before its worker is published."""
    return _RUNTIME_SERVICES.reserve(
        owner,
        kind=kind,
        close_name=close_name,
        phase=phase,
        priority=priority,
    )


def register_runtime_service(
    owner: object,
    *,
    kind: str,
    close_name: str = "close",
    phase: RuntimeServicePhase = RuntimeServicePhase.PRODUCER,
    priority: int = 0,
) -> RuntimeServiceRegistration:
    """Register and activate a process-wide runtime service."""
    return _RUNTIME_SERVICES.register(
        owner,
        kind=kind,
        close_name=close_name,
        phase=phase,
        priority=priority,
    )


def runtime_service_snapshot() -> RuntimeServiceSnapshot:
    """Return current runtime-service registry diagnostics."""
    return _RUNTIME_SERVICES.snapshot()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_RUNTIME_SERVICES.reset_after_fork)


__all__ = [
    "RuntimeCloseStatus",
    "RuntimeServicePhase",
    "RuntimeServiceRegistration",
    "RuntimeServiceSnapshot",
    "register_runtime_service",
    "reserve_runtime_service",
    "runtime_service_snapshot",
]
