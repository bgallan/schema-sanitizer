"""Shared transaction helpers for governed runtime thread ownership."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock, Thread, current_thread
from typing import Any

from ..errors import SchemaSanitizerResourceError

_MAX_RETIREMENT_DEBTS = 4096
_RETIREMENT_LOCK = Lock()


@dataclass(slots=True)
class _RetirementDebtSlot:
    thread: Thread | None = None
    release_permit: Callable[[], bool | None] | None = None
    registration: Any = None
    token: int = 0


_RETIREMENT_DEBTS: list[_RetirementDebtSlot] = [
    _RetirementDebtSlot() for _ in range(_MAX_RETIREMENT_DEBTS)
]
_RETIREMENT_DEBT_COUNT = 0
_RETIREMENT_OVERFLOWS = 0
_RETIREMENT_OVERFLOWED = False

from .safe_errors import add_bounded_note  # noqa: E402


def reap_governed_thread_retirements() -> int:
    """Return permits only after their physical Python thread has exited.

    Debt slots are preallocated. One dead owner is claimed under the lock and
    released outside it, so the terminal path never materializes an O(n) tuple
    and recursive availability callbacks cannot reclaim the same slot twice.
    """
    global _RETIREMENT_DEBT_COUNT
    reaped = 0
    while True:
        claimed_index = -1
        claimed_thread: Thread | None = None
        claimed_release: Callable[[], bool | None] | None = None
        claimed_registration: Any = None
        with _RETIREMENT_LOCK:
            for index, slot in enumerate(_RETIREMENT_DEBTS):
                thread = slot.thread
                release_permit = slot.release_permit
                if thread is None or release_permit is None or thread.is_alive():
                    continue
                claimed_index = index
                claimed_thread = thread
                claimed_release = release_permit
                claimed_registration = slot.registration
                slot.thread = None
                slot.release_permit = None
                slot.registration = None
                slot.token = 0
                _RETIREMENT_DEBT_COUNT = max(0, _RETIREMENT_DEBT_COUNT - 1)
                break
        if claimed_thread is None or claimed_release is None:
            return reaped
        index = claimed_index
        thread = claimed_thread
        release_permit = claimed_release
        registration = claimed_registration
        committed = False
        try:
            result = release_permit()
            committed = result is not False
        except BaseException:
            try:
                from .retry_scheduler import adopt_failed_release

                owner = getattr(release_permit, "__self__", None)
                committed = bool(
                    owner is not None and adopt_failed_release(owner, retained_bytes=256)
                )
            except BaseException:
                committed = False
        if not committed:
            with _RETIREMENT_LOCK:
                slot = _RETIREMENT_DEBTS[index]
                if slot.thread is None:
                    slot.thread = thread
                    slot.release_permit = release_permit
                    slot.registration = registration
                    slot.token = id(thread)
                    _RETIREMENT_DEBT_COUNT += 1
            continue
        if registration is not None:
            try:
                registration.close()
            except BaseException:
                pass
        reaped += 1


def defer_governed_thread_retirement(
    thread: Thread,
    release_permit: Callable[[], bool | None],
    *,
    registration: Any = None,
) -> bool:
    """Transfer a live current thread's permit into preallocated retirement debt."""
    global _RETIREMENT_OVERFLOWS, _RETIREMENT_OVERFLOWED, _RETIREMENT_DEBT_COUNT
    reap_governed_thread_retirements()
    if not thread.is_alive():
        result = release_permit()
        if result is False:
            return False
        if registration is not None:
            registration.close()
        return True
    token = id(thread)
    with _RETIREMENT_LOCK:
        free: _RetirementDebtSlot | None = None
        for slot in _RETIREMENT_DEBTS:
            if slot.thread is thread and slot.token == token:
                return True
            if free is None and slot.thread is None:
                free = slot
        if free is None:
            _RETIREMENT_OVERFLOWED = True
            _RETIREMENT_OVERFLOWS += 1
            return False
        free.thread = thread
        free.release_permit = release_permit
        free.registration = registration
        free.token = token
        _RETIREMENT_DEBT_COUNT += 1
    return True


class RetirementAwareThread(Thread):
    """Thread whose successful join is also a safe retirement-debt reap point."""

    def join(self, timeout: float | None = None) -> None:
        super().join(timeout=timeout)
        if not self.is_alive():
            reap_governed_thread_retirements()


def governed_thread_retirement_snapshot() -> tuple[int, int]:
    """Return live retirement debts and irreversible publication overflows."""
    reap_governed_thread_retirements()
    with _RETIREMENT_LOCK:
        return (
            _RETIREMENT_DEBT_COUNT,
            max(1, _RETIREMENT_OVERFLOWS) if _RETIREMENT_OVERFLOWED else _RETIREMENT_OVERFLOWS,
        )


def _reset_governed_thread_retirements_after_fork() -> None:
    global _RETIREMENT_OVERFLOWS, _RETIREMENT_OVERFLOWED, _RETIREMENT_DEBT_COUNT
    # The module is quarantine-only after fork; do not allocate replacement
    # locks or banks in the child.
    with _RETIREMENT_LOCK:
        for slot in _RETIREMENT_DEBTS:
            slot.thread = None
            slot.release_permit = None
            slot.registration = None
            slot.token = 0
        _RETIREMENT_DEBT_COUNT = 0
        _RETIREMENT_OVERFLOWS = 0
        _RETIREMENT_OVERFLOWED = False


def _native_physical_thread_api() -> Any:
    """Return the canonical shared physical-thread permit API."""
    from .native_runtime import native_core

    return native_core


def _acquire_native_physical_thread_permit(thread: object) -> Any:
    if not isinstance(thread, Thread):
        raise TypeError("governed runtime host must be a threading.Thread")
    native = _native_physical_thread_api()
    raw_granted = native.process_physical_thread_permits_acquire(1, 1)
    granted = int(raw_granted)
    if granted != 1:
        raise SchemaSanitizerResourceError(
            "process physical thread capacity exhausted",
            detail={
                "stage": "physical_threads",
                "limit_name": "process_physical_threads",
                "actual_items": 1,
            },
        )
    return native


def _release_native_physical_thread_permit(native: Any) -> None:
    native.process_physical_thread_permits_release(1)


def start_governed_thread(thread: Thread, *, registration: Any = None) -> None:
    """Start one governed host exactly once, optionally through the runtime barrier.

    Binary runtimes reserve the same native physical-thread capability used by
    C++ workers immediately before the irreversible ``Thread.start()`` commit.
    The capability remains owned until the Python host physically exits, while
    the existing logical project-thread lease remains an independent lifecycle
    capability. This closes the former Python↔C++ admission race.
    """
    reap_governed_thread_retirements()
    native = _acquire_native_physical_thread_permit(thread)
    original_run = thread.run
    start_committed = False

    def _run_with_physical_permit() -> Any:
        running_marked = False
        try:
            try:
                native.process_physical_thread_mark_running()
                running_marked = True
            except BaseException:
                # Physical start is already committed. Instrumentation must
                # never prevent the authorized host from executing.
                running_marked = False
            return original_run()
        finally:
            if running_marked:
                try:
                    native.process_physical_thread_mark_stopped()
                except BaseException:
                    pass
            try:
                native.process_physical_thread_permits_release(1)
            except BaseException:
                # The native release operation is noexcept internally; a
                # Python-level failure here must not strand thread teardown.
                pass
            try:
                thread.run = original_run  # type: ignore[method-assign]
            except BaseException:
                pass

    thread.run = _run_with_physical_permit  # type: ignore[method-assign]

    try:
        if registration is None:
            thread.start()
            start_committed = True
            return
        registration.start_thread(thread)
        start_committed = True
    except BaseException:
        # Before a successful Thread.start(), no host can consume the permit.
        # Restore the original entry point before returning the capability.
        if not start_committed:
            try:
                thread.run = original_run  # type: ignore[method-assign]
            finally:
                try:
                    _release_native_physical_thread_permit(native)
                except BaseException:
                    pass
        raise


def rollback_unstarted_runtime_thread(
    registration: Any,
    release_permit: Callable[[], bool | None],
    *,
    primary: BaseException,
    note_prefix: str,
) -> None:
    """Rollback a host that provably never became a live thread.

    The registration is removed before returning the permit, preventing a
    capacity slot from being reused while an observable RESERVED owner remains.
    Failures are attached to the caller's primary exception without replacing
    it; ownership-specific fallback remains the publisher's responsibility.
    """
    if registration is not None:
        try:
            registration.close()
        except BaseException as exc:
            add_bounded_note(primary, f"{note_prefix} registry rollback", exc)
    try:
        release_permit()
    except BaseException as exc:
        add_bounded_note(primary, f"{note_prefix} permit rollback", exc)


def retire_governed_runtime_thread(
    thread: Thread | None,
    registration: Any,
    release_permit: Callable[[], bool],
    *,
    terminal_from_current: bool = False,
) -> bool:
    """Retire a stopped host without losing permit/registry observability.

    Permit ownership is committed first.  Only then is the runtime service
    unregistered.  A worker may invoke this from its final ``finally`` block
    immediately before returning when ``terminal_from_current`` is true.
    """
    if thread is None:
        return False
    if thread.is_alive():
        if not (terminal_from_current and thread is current_thread()):
            return False
        return defer_governed_thread_retirement(thread, release_permit, registration=registration)
    result = release_permit()
    if result is False:
        return False
    if registration is not None:
        registration.close()
    return True


try:
    from .fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

    _register_fork_handler("governed-thread", mode="quarantine_only")
except BaseException:
    pass


from .shutdown_observers import (  # noqa: E402
    register_shutdown_observer as _register_shutdown_observer,
)

_register_shutdown_observer("governed_thread_retirement", governed_thread_retirement_snapshot)


__all__ = [
    "defer_governed_thread_retirement",
    "governed_thread_retirement_snapshot",
    "reap_governed_thread_retirements",
    "RetirementAwareThread",
    "retire_governed_runtime_thread",
    "rollback_unstarted_runtime_thread",
    "start_governed_thread",
]
