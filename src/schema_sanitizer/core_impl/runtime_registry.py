"""Register runtime services transactionally for phased shutdown.

Services are reserved and published through close phases, checked for quiescence, and repaired
after a fork without exposing partially registered helpers.
"""

from __future__ import annotations

import inspect
import os
import threading
from dataclasses import dataclass
from enum import Enum, IntEnum, auto
from time import monotonic_ns
from typing import Callable

from .bounded_generation import BoundedGenerationPool
from .control_plane_budget import ControlPlaneTicket, release_control_plane, reserve_control_plane
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
    RETIRING = auto()
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
    post_commit_failures: int = 0


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
        "thread",
        "control_ticket",
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
        control_ticket: ControlPlaneTicket,
    ) -> None:
        # The registry owns one strong control block until quiescence.  A weak
        # reference alone can make a live host disappear from shutdown.
        """Initialize the service entry and its owned runtime state."""
        self.owner = owner
        self.close_call = close_call
        self.kind = kind
        self.close_name = close_name
        self.generation = generation
        self.phase = phase
        self.priority = priority
        self.active = False
        self.state = _ServiceState.RESERVED
        self.thread: threading.Thread | None = None
        self.control_ticket = control_ticket


class RuntimeServiceRegistration:
    """Idempotent two-phase registration token."""

    __slots__ = ("_registry", "_token", "_pid", "_lock", "_closed", "_active")

    def __init__(self, registry: "_RuntimeServiceRegistry", token: int) -> None:
        """Initialize the runtime service registration and its owned runtime state."""
        self._registry = registry
        self._token = token
        self._pid = os.getpid()
        self._lock = threading.Lock()
        self._closed = False
        self._active = False

    def activate(self) -> None:
        """Activate the reserved runtime entry."""
        if os.getpid() != self._pid:
            return
        with self._lock:
            if self._closed or self._active:
                return
            self._registry.activate(self._token)
            self._active = True

    def start_thread(self, thread: threading.Thread) -> None:
        """Atomically authorize, start and activate one exact internal thread."""
        if os.getpid() != self._pid:
            raise RuntimeError("runtime service registration is not startable")
        with self._lock:
            if self._closed or self._active:
                raise RuntimeError("runtime service registration is not startable")
            self._registry.start_thread(self._token, thread)
            self._active = True

    def close(self) -> None:
        """Release resources owned by this runtime service registration."""
        if os.getpid() != self._pid:
            return
        with self._lock:
            if self._closed:
                # A thread-backed registration can remain RETIRING until its
                # physical host exits. Re-probe so repeated close/snapshot
                # paths can retire it without reopening token ownership.
                self._registry.unregister(self._token)
                return
            self._closed = True
            self._registry.unregister(self._token)


class _RuntimeServiceRegistry:
    def __init__(self) -> None:
        """Initialize the runtime service registry and its owned runtime state."""
        self._reset(os.getpid())

    def _reset(self, pid: int) -> None:
        """Reset process-local state owned by this runtime service registry."""
        self._pid = pid
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._entries: dict[int, _ServiceEntry] = {}
        self._token_pool = BoundedGenerationPool(256)
        self._generation = 1
        self._progress_epoch = 0
        self._last_progress_ns = monotonic_ns()
        self._admission_closed = False
        self._capacity = 256
        self._rejected_services = 0
        self._circuit_open = False
        self._post_commit_failures = 0

    def _ensure_process(self) -> None:
        """Ensure the owner still belongs to the active process."""
        if self._pid != os.getpid():
            self._reset(os.getpid())

    def _mark_progress_locked(self) -> None:
        """Mark progress while holding the governing lock."""
        self._progress_epoch = min((1 << 63) - 1, self._progress_epoch + 1)
        self._last_progress_ns = monotonic_ns()
        diagnostic_transition()
        self._condition.notify_all()

    def _mark_progress_noexcept_locked(self) -> None:
        """Publish diagnostics after an irreversible lifecycle commit."""
        try:
            self._mark_progress_locked()
        except BaseException as exc:
            clear_exception_traceback(exc)
            try:
                self._post_commit_failures += 1
            except BaseException:
                pass

    def _retire_entry_locked(self, token: int, entry: _ServiceEntry) -> None:
        """Return the control-plane charge before the allocation-free map pop."""
        if not release_control_plane(entry.control_ticket):
            raise RuntimeError("runtime-service control-plane retirement did not commit")
        if not self._token_pool.release_for(entry):
            # Exact owner identity remains the retirement authority even when
            # the bounded token handoff/mirror is stale.
            raise RuntimeError("runtime-service generation retirement did not commit")
        self._entries.pop(token, None)

    def _remove_dead(self, token: int, generation: int) -> bool:
        """Retire one logically quiescent service only after physical thread exit."""
        with self._lock:
            entry = self._entries.get(token)
            if entry is None or entry.generation != generation:
                return False
            thread = entry.thread
            if thread is not None and thread.is_alive():
                if entry.state is not _ServiceState.RETIRING:
                    entry.state = _ServiceState.RETIRING
                    self._mark_progress_noexcept_locked()
                return False
            entry.state = _ServiceState.QUIESCENT
            self._retire_entry_locked(token, entry)
            if len(self._entries) < self._capacity:
                self._circuit_open = False
            self._mark_progress_noexcept_locked()
            return True

    def _prune_retiring_threads_locked(self) -> int:
        """Remove exited registrations without copying the bounded registry."""
        removed = 0
        while True:
            selected_token: int | None = None
            selected_entry: _ServiceEntry | None = None
            for token, entry in self._entries.items():
                if entry.state is not _ServiceState.RETIRING:
                    continue
                thread = entry.thread
                if thread is not None and thread.is_alive():
                    continue
                selected_token = token
                selected_entry = entry
                break
            if selected_entry is None or selected_token is None:
                break
            selected_entry.state = _ServiceState.QUIESCENT
            self._retire_entry_locked(selected_token, selected_entry)
            removed += 1
        if removed:
            if len(self._entries) < self._capacity:
                self._circuit_open = False
            self._mark_progress_noexcept_locked()
        return removed

    @staticmethod
    def _prepare_close(owner: object, close_name: str) -> Callable[[float], object]:
        """Prepare one runtime service entry for bounded shutdown."""
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
            raise TypeError(
                "runtime service close method must accept keyword deadline_seconds"
            ) from None

        def close_with_deadline(remaining: float) -> object:
            """Close the runtime registry within the supplied deadline."""
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
        """Reserve governed capacity through this runtime service registry."""
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
                self._mark_progress_noexcept_locked()
                raise RuntimeError("runtime service registry capacity exhausted")
            generation = self._generation
            # Construct the registry entry before asking the bounded namespace
            # for a generation. An interruption
            # after acquire_for() returns but before token STORE is recoverable
            # with release_for(entry).
            control_ticket = reserve_control_plane("runtime_service", 512)
            entry = _ServiceEntry(
                owner,
                kind=kind,
                close_name=close_name,
                generation=generation,
                phase=phase,
                priority=priority,
                close_call=close_call,
                control_ticket=control_ticket,
            )
            token: int | None = None
            try:
                token = self._token_pool.acquire_for(entry)
                if token is None:
                    self._rejected_services += 1
                    self._circuit_open = True
                    self._mark_progress_noexcept_locked()
                    raise RuntimeError("runtime service token namespace exhausted")
                registration = RuntimeServiceRegistration(self, token)
                self._entries[token] = entry
                self._mark_progress_locked()
            except BaseException as primary:
                try:
                    generation_released = self._token_pool.release_for(entry)
                except BaseException:
                    generation_released = False
                if token is not None and generation_released:
                    self._entries.pop(token, None)
                elif not generation_released:
                    from .safe_errors import add_bounded_note

                    add_bounded_note(
                        primary,
                        "runtime-service generation rollback did not commit",
                        RuntimeError("bounded generation owner retirement failed"),
                    )
                release_control_plane(control_ticket)
                raise
        return registration

    def register(
        self,
        owner: object,
        *,
        kind: str,
        close_name: str,
        phase: RuntimeServicePhase = RuntimeServicePhase.PRODUCER,
        priority: int = 0,
    ) -> RuntimeServiceRegistration:
        """Register one reserved entry with this runtime service registry."""
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
        """Activate the reserved runtime entry."""
        self._ensure_process()
        if type(token) is not int:
            raise TypeError("runtime service token must be an exact integer")
        with self._condition:
            entry = self._entries.get(token)
            if entry is None:
                if self._admission_closed:
                    raise RuntimeError("runtime service admission closed before activation")
                raise RuntimeError("runtime service reservation is no longer valid")
            if entry.state is _ServiceState.CANCELLED:
                raise RuntimeError("runtime service reservation was cancelled")
            if entry.state is not _ServiceState.RESERVED:
                if entry.state is _ServiceState.ACTIVE:
                    return
                raise RuntimeError("runtime service reservation is not activatable")
            if self._admission_closed:
                entry.state = _ServiceState.CANCELLED
                self._retire_entry_locked(token, entry)
                self._mark_progress_noexcept_locked()
                raise RuntimeError("runtime service admission closed before activation")
            entry.active = True
            entry.state = _ServiceState.ACTIVE
            self._mark_progress_noexcept_locked()

    def start_thread(self, token: int, thread: threading.Thread) -> None:
        """Hold the registry gate across thread.start() to prevent late starts."""
        self._ensure_process()
        # Authenticate the only internal Thread subclass by exact class
        # identity. Module/name checks are forgeable metadata and must not be
        # used as a capability at this lifecycle boundary.
        from .governed_thread import RetirementAwareThread

        thread_type = type(thread)
        if thread_type not in (threading.Thread, RetirementAwareThread):
            raise TypeError("runtime service start requires an exact governed Thread")
        if type(token) is not int:
            raise TypeError("runtime service token must be an exact integer")
        with self._condition:
            entry = self._entries.get(token)
            if entry is None or entry.state is not _ServiceState.RESERVED:
                if self._admission_closed:
                    raise RuntimeError("runtime service admission closed before thread start")
                raise RuntimeError("runtime service reservation is no longer startable")
            if self._admission_closed:
                entry.state = _ServiceState.CANCELLED
                self._retire_entry_locked(token, entry)
                self._mark_progress_noexcept_locked()
                raise RuntimeError("runtime service admission closed before thread start")
            entry.state = _ServiceState.START_AUTHORIZED
            self._mark_progress_noexcept_locked()
            try:
                thread.start()
            except BaseException:
                entry.state = _ServiceState.RESERVED
                self._mark_progress_noexcept_locked()
                raise
            # Physical start is the irreversible commit point. Everything after
            # it must be publication-only and must never report start failure.
            entry.active = True
            entry.thread = thread
            entry.state = _ServiceState.ACTIVE
            self._mark_progress_noexcept_locked()

    def unregister(self, token: int) -> None:
        """Remove one registered entry from this runtime service registry."""
        self._ensure_process()
        if type(token) is not int:
            raise TypeError("runtime service token must be an exact integer")
        with self._condition:
            self._prune_retiring_threads_locked()
            entry = self._entries.get(token)
            if entry is None:
                return
            thread = entry.thread
            if thread is not None and thread.is_alive():
                # Never make a physically-live governed thread invisible to
                # shutdown. The token may be logically closed while the
                # registry retains the exact host until is_alive() is false.
                entry.state = _ServiceState.RETIRING
                entry.active = True
                self._mark_progress_noexcept_locked()
                return
            self._retire_entry_locked(token, entry)
            entry.state = _ServiceState.QUIESCENT
            if len(self._entries) < self._capacity:
                self._circuit_open = False
            self._mark_progress_noexcept_locked()

    def close_admission(self) -> None:
        """Reject new runtime-service registrations before shutdown."""
        self._ensure_process()
        with self._condition:
            changed = not self._admission_closed
            self._admission_closed = True
            # A reservation is not ownership of a running service. Once the
            # terminal admission barrier commits, no RESERVED entry may later
            # cross into ACTIVE. Retire them atomically under the same gate.
            cancelled = 0
            # Retire one reservation per scan: no O(services) tuple allocation
            # is needed after the terminal admission barrier has committed.
            while True:
                selected_token: int | None = None
                selected_entry: _ServiceEntry | None = None
                for token, entry in self._entries.items():
                    if entry.state is _ServiceState.RESERVED:
                        selected_token = token
                        selected_entry = entry
                        break
                if selected_token is None or selected_entry is None:
                    break
                selected_entry.state = _ServiceState.CANCELLED
                self._retire_entry_locked(selected_token, selected_entry)
                cancelled += 1
            if changed or cancelled:
                if len(self._entries) < self._capacity:
                    self._circuit_open = False
                self._mark_progress_noexcept_locked()
            try:
                self._condition.notify_all()
            except BaseException as exc:
                clear_exception_traceback(exc)

    def reopen_admission_for_tests(self) -> None:
        """Reopen admission for an isolated test run."""
        with self._condition:
            self._admission_closed = False
            if not self._entries:
                self._circuit_open = False
            self._mark_progress_locked()

    @staticmethod
    def _is_quiescent_result(result: object) -> bool:
        """Return whether a shutdown result proves runtime quiescence."""
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
            with self._condition:
                self._prune_retiring_threads_locked()
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
                        if entry.state in {_ServiceState.ACTIVE, _ServiceState.RETIRING}
                        and (phase is None or entry.phase is phase)
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
                    removed = self._remove_dead(token, generation)
                    if removed:
                        closed += 1
                        progressed = True
                    # A logically quiescent but physically-live thread remains
                    # RETIRING. Do not call close() in a tight loop; the bounded
                    # epoch wait below re-probes physical exit without burning CPU.
                elif result is RuntimeCloseStatus.PROGRESS:
                    progressed = True
            if not progressed:
                # Wait on the registry progress epoch rather than allocating a
                # fresh Event and spin-polling every few milliseconds. Some
                # services only reveal worker completion on the next close()
                # probe, so retain a bounded 50 ms retry ceiling.
                with self._condition:
                    epoch = self._progress_epoch
                    timeout = min(0.05, remaining_seconds(deadline_ns))
                    if timeout > 0 and self._progress_epoch == epoch:
                        self._condition.wait_for(
                            lambda: self._progress_epoch != epoch,
                            timeout=timeout,
                        )
        with self._condition:
            self._prune_retiring_threads_locked()
            remaining_services = sum(
                1 for entry in self._entries.values() if phase is None or entry.phase is phase
            )
            return closed, remaining_services

    def snapshot(self) -> RuntimeServiceSnapshot:
        """Return a bounded snapshot of the current runtime service registry."""
        self._ensure_process()
        with self._condition:
            self._prune_retiring_threads_locked()
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
                self._post_commit_failures,
            )

    def reset_after_fork(self) -> None:
        """Reset process-local state inherited across a fork."""
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


from .fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler("runtime-services", mode="quarantine_only")


__all__ = [
    "RuntimeCloseStatus",
    "RuntimeServicePhase",
    "RuntimeServiceRegistration",
    "RuntimeServiceSnapshot",
    "register_runtime_service",
    "reserve_runtime_service",
    "runtime_service_snapshot",
]
