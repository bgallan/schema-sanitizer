"""Bounded non-blocking handoff for resource finalizers that may otherwise block."""

from __future__ import annotations

from typing import Any

from .finalizer_escrow import FinalizerEscrow, ReservedFinalizerEscrow
from .rooted_finalizer import RootedFinalizerAuthority

_MAX_FINALIZER_CLEANUP_OWNERS = 16384
_FINALIZER_CLEANUP_ESCROW: FinalizerEscrow[object] = FinalizerEscrow(
    _MAX_FINALIZER_CLEANUP_OWNERS, static_kind="finalizer_cleanup_legacy"
)
_FINALIZER_CLEANUP_OVERFLOWS = 0
_FINALIZER_CLEANUP_OVERFLOWED = False


class PreparedFinalizerCleanup:
    """Mutable wrapper over a separately pre-rooted cleanup authority.

    The escrow roots ``_authority`` rather than this wrapper, so abandoning a
    reserved capsule can still reach ``__del__`` and arm/cancel its exact slot.
    """

    __slots__ = ("_ticket", "_authority")

    def __init__(self, callback: Any) -> None:
        if not callable(callback):
            raise TypeError("prepared finalizer callback must be callable")
        self._ticket = 0
        self._authority = RootedFinalizerAuthority(callback)

    @property
    def ticket(self) -> int:
        ticket = self._ticket
        if ticket:
            return ticket
        authority = self._authority
        return int(getattr(authority, "ticket", 0) or 0)

    @property
    def callback(self) -> Any:
        return self._authority.callback

    @callback.setter
    def callback(self, value: Any) -> None:
        self._authority.callback = value

    @property
    def _escrow_armed(self) -> bool:
        return self._authority._escrow_armed

    @_escrow_armed.setter
    def _escrow_armed(self, value: bool) -> None:
        if value:
            ticket = self.ticket
            if ticket:
                self._authority.arm_for_ticket(ticket)
            else:
                self._authority._escrow_armed = True
        else:
            self._authority.disarm_ticket()

    @property
    def arg0(self) -> object | None:
        return self._authority.arg0

    @arg0.setter
    def arg0(self, value: object | None) -> None:
        self._authority.arg0 = value

    @property
    def arg1(self) -> object | None:
        return self._authority.arg1

    @arg1.setter
    def arg1(self, value: object | None) -> None:
        self._authority.arg1 = value

    @property
    def arg2(self) -> object | None:
        return self._authority.arg2

    @arg2.setter
    def arg2(self, value: object | None) -> None:
        self._authority.arg2 = value

    @property
    def arg3(self) -> object | None:
        return self._authority.arg3

    @arg3.setter
    def arg3(self, value: object | None) -> None:
        self._authority.arg3 = value

    @property
    def arg4(self) -> object | None:
        return self._authority.arg4

    @arg4.setter
    def arg4(self, value: object | None) -> None:
        self._authority.arg4 = value

    @property
    def arg5(self) -> object | None:
        return self._authority.arg5

    @arg5.setter
    def arg5(self, value: object | None) -> None:
        self._authority.arg5 = value

    @property
    def arg6(self) -> object | None:
        return self._authority.arg6

    @arg6.setter
    def arg6(self, value: object | None) -> None:
        self._authority.arg6 = value

    @property
    def arg7(self) -> object | None:
        return self._authority.arg7

    @arg7.setter
    def arg7(self, value: object | None) -> None:
        self._authority.arg7 = value

    def run(self) -> None:
        # Live/manual compatibility path. Safe-point execution runs the rooted
        # authority directly because the wrapper may already have been GC'd.
        if not self._authority._ack_only:
            self.callback(self)

    def clear(self) -> None:
        self._authority.clear()

    def __del__(self) -> None:
        """Arm an orphaned authority without any blocking escrow operation."""
        ticket = self.ticket
        if not ticket:
            return
        try:
            accepted = defer_prepared_finalizer_cleanup(self)
            if not accepted:
                _mark_prepared_finalizer_overflow()
        except BaseException:
            try:
                _mark_prepared_finalizer_overflow()
            except BaseException:
                pass


_MAX_PREPARED_FINALIZER_CLEANUPS = 32768
_PREPARED_FINALIZER_ESCROW: ReservedFinalizerEscrow[RootedFinalizerAuthority] = (
    ReservedFinalizerEscrow(
        _MAX_PREPARED_FINALIZER_CLEANUPS, static_kind="finalizer_cleanup_prepared"
    )
)
_PREPARED_FINALIZER_OVERFLOWS = 0
_PREPARED_FINALIZER_OVERFLOWED = False


def _mark_prepared_finalizer_overflow() -> None:
    """Latch publication loss even when the diagnostic counter cannot grow."""
    global _PREPARED_FINALIZER_OVERFLOWS, _PREPARED_FINALIZER_OVERFLOWED
    _PREPARED_FINALIZER_OVERFLOWED = True
    try:
        _PREPARED_FINALIZER_OVERFLOWS += 1
    except MemoryError:
        pass


_PREPARED_ARG_NAMES = ("arg0", "arg1", "arg2", "arg3", "arg4", "arg5", "arg6", "arg7")


def _cleanup_detached_resources_capsule(capsule: PreparedFinalizerCleanup) -> None:
    """Close only detached resources, clearing successful aliases in place.

    The fixed eight-slot scan allocates no owner container.  Successful
    resources are cleared before moving on, while a failure leaves that
    resource and all later owners rooted for the next safe point.
    """
    for name in _PREPARED_ARG_NAMES:
        resource = getattr(capsule, name)
        if resource is None:
            continue
        _cleanup_owner(resource)
        for alias in _PREPARED_ARG_NAMES:
            if getattr(capsule, alias) is resource:
                setattr(capsule, alias, None)


def _drop_detached_references_capsule(_capsule: PreparedFinalizerCleanup) -> None:
    """Acknowledge references whose destruction itself must occur at a safe point."""
    return


def reserve_finalizer_cleanup(callback: Any) -> PreparedFinalizerCleanup:
    """Allocate and owner-first root a separate authority before exposure."""
    capsule = PreparedFinalizerCleanup(callback)
    try:
        ticket = _PREPARED_FINALIZER_ESCROW.reserve_rooted(capsule._authority)
        if ticket is None:
            raise RuntimeError("prepared finalizer cleanup capacity exhausted")
        # ``reserve_rooted`` already writes authority.ticket before handoff.  If
        # an async exception lands before this mirror STORE, capsule.__del__
        # reads the exact ticket from the authority and can still arm cleanup.
        capsule._ticket = ticket
        return capsule
    except BaseException:
        try:
            _PREPARED_FINALIZER_ESCROW.release_rooted_owner(capsule._authority)
        except BaseException:
            pass
        raise


def reserve_detached_resources_finalizer_cleanup() -> PreparedFinalizerCleanup:
    """Pre-reserve cleanup for a detached sequence of resources."""
    return reserve_finalizer_cleanup(_cleanup_detached_resources_capsule)


def reserve_reference_finalizer_cleanup() -> PreparedFinalizerCleanup:
    """Pre-reserve cleanup that drops detached object references safely."""
    return reserve_finalizer_cleanup(_drop_detached_references_capsule)


def _cleanup_prepared_resource_capsule(capsule: PreparedFinalizerCleanup) -> None:
    resource = capsule.arg0
    if resource is not None:
        _cleanup_owner(resource)


def reserve_resource_finalizer_cleanup(resource: object) -> PreparedFinalizerCleanup:
    """Pre-reserve cleanup for one already-created resource owner."""
    capsule = reserve_finalizer_cleanup(_cleanup_prepared_resource_capsule)
    capsule.arg0 = resource
    return capsule


def reserve_owner_finalizer_cleanup() -> PreparedFinalizerCleanup:
    """Pre-reserve an initially empty single-owner cleanup capsule."""
    return reserve_finalizer_cleanup(_cleanup_prepared_resource_capsule)


# Legacy tuple-returning wrappers remain for focused compatibility only. Production
# code uses the single-capsule reserve_* API so no container is allocated after
# escrow ownership commits.
def prepare_detached_resources_finalizer_cleanup() -> tuple[int, PreparedFinalizerCleanup]:
    """Return the legacy ticket-and-capsule detached-resource reservation."""
    capsule = reserve_detached_resources_finalizer_cleanup()
    return capsule.ticket, capsule


def prepare_reference_finalizer_cleanup() -> tuple[int, PreparedFinalizerCleanup]:
    """Return the legacy ticket-and-capsule reference reservation."""
    capsule = reserve_reference_finalizer_cleanup()
    return capsule.ticket, capsule


def prepare_resource_finalizer_cleanup(
    resource: object,
) -> tuple[int, PreparedFinalizerCleanup]:
    """Return the legacy reservation tuple for one resource."""
    capsule = reserve_resource_finalizer_cleanup(resource)
    return capsule.ticket, capsule


def prepare_owner_finalizer_cleanup() -> tuple[int, PreparedFinalizerCleanup]:
    """Return the legacy reservation tuple for a later owner."""
    capsule = reserve_owner_finalizer_cleanup()
    return capsule.ticket, capsule


def defer_owner_finalizer_cleanup(owner: object, capsule: PreparedFinalizerCleanup) -> bool:
    """Publish ``owner`` into its pre-reserved deferred cleanup capsule."""
    capsule.arg0 = owner
    return defer_prepared_finalizer_cleanup(capsule)


def prepare_finalizer_cleanup(callback: Any) -> tuple[int, PreparedFinalizerCleanup]:
    """Return the legacy reservation tuple for an arbitrary cleanup callback."""
    capsule = reserve_finalizer_cleanup(callback)
    return capsule.ticket, capsule


def acknowledge_prepared_finalizer_cleanup(
    capsule_or_ticket: PreparedFinalizerCleanup | int,
    legacy_capsule: PreparedFinalizerCleanup | None = None,
) -> None:
    """Retire a prepared slot after primary cleanup already committed."""
    if legacy_capsule is None:
        if not isinstance(capsule_or_ticket, PreparedFinalizerCleanup):
            raise TypeError("prepared finalizer cleanup capsule required")
        capsule = capsule_or_ticket
    else:
        capsule = legacy_capsule
        if type(capsule_or_ticket) is not int or capsule_or_ticket != capsule.ticket:
            raise RuntimeError("prepared finalizer ticket/capsule authority mismatch")
    ticket = capsule.ticket
    if not ticket:
        return
    authority = capsule._authority
    if authority._escrow_armed and (
        authority._ack_only or capsule.callback is _drop_detached_references_capsule
    ):
        # A prior attempt already durably transferred ACK-only ownership. The
        # local wrapper may relinquish its duplicate exact ticket immediately.
        capsule._ticket = 0
        authority.ticket = 0
        return
    # PRIMARY -> ACK_ONLY is irreversible and happens before any potentially
    # failing retirement call. Preserve the historical static callback marker
    # as well as the explicit authority bit for source/runtime compatibility.
    capsule.callback = _drop_detached_references_capsule
    authority.make_ack_only()
    try:
        retired = _PREPARED_FINALIZER_ESCROW.release_ticket(ticket)
    except BaseException:
        retired = False
    if not retired:
        if not _PREPARED_FINALIZER_ESCROW.publish_rooted(ticket, authority):
            _mark_prepared_finalizer_overflow()
        # Keep the local ticket so an explicit retry/defer remains observable;
        # the separately rooted ACK owner already prevents primary replay.
        raise RuntimeError("prepared finalizer cleanup acknowledgement did not commit")
    capsule._ticket = 0
    authority.ticket = 0
    authority.clear()


def cancel_prepared_finalizer_cleanup(
    capsule_or_ticket: PreparedFinalizerCleanup | int,
    legacy_capsule: PreparedFinalizerCleanup | None = None,
) -> None:
    """Cancel an unused prepared slot without ever permitting primary replay."""
    if legacy_capsule is None:
        if not isinstance(capsule_or_ticket, PreparedFinalizerCleanup):
            raise TypeError("prepared finalizer cleanup capsule required")
        capsule = capsule_or_ticket
    else:
        capsule = legacy_capsule
        if type(capsule_or_ticket) is not int or capsule_or_ticket != capsule.ticket:
            raise RuntimeError("prepared finalizer ticket/capsule authority mismatch")
    ticket = capsule.ticket
    if not ticket:
        return
    authority = capsule._authority
    # Cancellation semantically commits before bookkeeping. Even an exception
    # from release_ticket therefore leaves only an ACK owner behind.
    capsule.callback = _drop_detached_references_capsule
    authority.make_ack_only()
    try:
        retired = _PREPARED_FINALIZER_ESCROW.release_ticket(ticket)
    except BaseException:
        retired = False
    if not retired:
        if _PREPARED_FINALIZER_ESCROW.publish_rooted(ticket, authority):
            capsule._ticket = 0
            authority.ticket = 0
            return
        _mark_prepared_finalizer_overflow()
        raise RuntimeError("prepared finalizer cleanup slot is not authoritative")
    capsule._ticket = 0
    authority.ticket = 0
    authority.clear()


def defer_prepared_finalizer_cleanup(
    capsule_or_ticket: PreparedFinalizerCleanup | int,
    legacy_capsule: PreparedFinalizerCleanup | None = None,
) -> bool:
    """Arm the separately rooted authority using embedded exact capability."""
    if legacy_capsule is None:
        if not isinstance(capsule_or_ticket, PreparedFinalizerCleanup):
            return False
        capsule = capsule_or_ticket
    else:
        capsule = legacy_capsule
        if type(capsule_or_ticket) is not int or capsule_or_ticket != capsule.ticket:
            _mark_prepared_finalizer_overflow()
            return False
    ticket = capsule.ticket
    if not ticket:
        return False
    authority = capsule._authority
    accepted = _PREPARED_FINALIZER_ESCROW.publish_rooted(ticket, authority)
    if accepted:
        capsule._ticket = 0
        authority.ticket = 0
    else:
        _mark_prepared_finalizer_overflow()
    return accepted


def defer_finalizer_cleanup(owner: object) -> bool:
    """Transfer *owner* without waiting, allocating container nodes, or doing I/O."""
    global _FINALIZER_CLEANUP_OVERFLOWS, _FINALIZER_CLEANUP_OVERFLOWED
    try:
        accepted = _FINALIZER_CLEANUP_ESCROW.try_publish(owner)
    except BaseException:
        accepted = False
    if not accepted:
        _FINALIZER_CLEANUP_OVERFLOWED = True
        try:
            _FINALIZER_CLEANUP_OVERFLOWS += 1
        except MemoryError:
            pass
    return accepted


def _cleanup_owner(owner: object) -> None:
    custom = getattr(owner, "_finalizer_cleanup_from_escrow", None)
    if callable(custom):
        custom()
        return
    # Preserve object-specific abandonment semantics where they exist.
    abandon = getattr(owner, "abandon", None)
    if callable(abandon):
        abandon()
        return
    close_all = getattr(owner, "close_all", None)
    if callable(close_all):
        close_all()
        return
    close = getattr(owner, "close", None)
    if callable(close):
        close()
        return
    release = getattr(owner, "release", None)
    if callable(release):
        release()
        return
    raise TypeError("finalizer cleanup owner exposes no cleanup method")


def drain_finalizer_cleanup() -> int:
    """Run prepared capsules and legacy rich-owner cleanup one rooted slot at a time."""
    progressed = 0

    def process_prepared(_ticket: int, authority: RootedFinalizerAuthority) -> None:
        nonlocal progressed
        authority.run()
        authority.clear()
        authority.ticket = 0
        progressed += 1

    # Attempt every owner that was published when this safe point began. A
    # persistent failure must not head-of-line block unrelated cleanup. The
    # escrow's consume cursor advances on claim, so each failed generation is
    # retried at most once per safe point.
    # Armed pre-rooted owners may still be RESERVED when their publishing
    # finalizer lost the slot-lock race. Scan bounded active generations so the
    # escrow can promote those durable owners at this safe point.
    prepared_attempts = _PREPARED_FINALIZER_ESCROW.active_count()
    for _ in range(prepared_attempts):
        try:
            if not _PREPARED_FINALIZER_ESCROW.process_one(process_prepared):
                break
        except BaseException:
            continue

    def process(owner: object) -> None:
        nonlocal progressed
        _cleanup_owner(owner)
        progressed += 1

    legacy_attempts = _FINALIZER_CLEANUP_ESCROW.size()
    for _ in range(legacy_attempts):
        try:
            if not _FINALIZER_CLEANUP_ESCROW.process_one(process):
                break
        except BaseException:
            # The failed owner remains published, but the consume cursor has
            # advanced so unrelated owners still make progress this safe point.
            continue
    return progressed


def finalizer_cleanup_snapshot() -> tuple[int, int]:
    """Return pending rich owners and irreversible publication failures."""
    overflow = _FINALIZER_CLEANUP_OVERFLOWS
    if _FINALIZER_CLEANUP_OVERFLOWED or _FINALIZER_CLEANUP_ESCROW.overflowed:
        overflow = max(1, overflow)
    prepared_overflow = _PREPARED_FINALIZER_OVERFLOWS
    if _PREPARED_FINALIZER_OVERFLOWED or _PREPARED_FINALIZER_ESCROW.overflowed:
        prepared_overflow = max(1, prepared_overflow)
    return (
        _FINALIZER_CLEANUP_ESCROW.size() + _PREPARED_FINALIZER_ESCROW.active_count(),
        overflow + prepared_overflow,
    )


def finalizer_cleanup_activity_snapshot() -> tuple[int, int, int, int]:
    """Return ABA-resistant activity for the legacy cleanup ring only.

    The prepared escrow is registered explicitly in ``finalizer_registry`` and
    is therefore accounted there exactly once.
    """
    return _FINALIZER_CLEANUP_ESCROW.activity_snapshot()


def prepared_finalizer_capacity_snapshot() -> tuple[int, int, int, int]:
    """Return capacity, active generations, available slots and retired slots.

    Reserving one prepared slot is an admission prerequisite for every owner
    using ``reserve_finalizer_cleanup``; therefore active finalizable owners can
    never exceed the teardown capacity represented here.
    """
    active = _PREPARED_FINALIZER_ESCROW.active_count()
    retired = _PREPARED_FINALIZER_ESCROW.retired_count()
    available = max(0, _PREPARED_FINALIZER_ESCROW.capacity - active - retired)
    return (_PREPARED_FINALIZER_ESCROW.capacity, active, available, retired)


def _reset_finalizer_cleanup_after_fork() -> None:
    global _FINALIZER_CLEANUP_OVERFLOWS, _FINALIZER_CLEANUP_OVERFLOWED
    global _PREPARED_FINALIZER_OVERFLOWS, _PREPARED_FINALIZER_OVERFLOWED
    # FinalizerEscrow pre-roots its inherited ring in the before-fork hook, so
    # the child can swap synchronization without allocating or decrefing it.
    _FINALIZER_CLEANUP_ESCROW.reset_after_fork()
    _PREPARED_FINALIZER_ESCROW.reset_after_fork()
    _PREPARED_FINALIZER_OVERFLOWS = 0
    _PREPARED_FINALIZER_OVERFLOWED = False
    _FINALIZER_CLEANUP_OVERFLOWS = 0
    _FINALIZER_CLEANUP_OVERFLOWED = False


from .fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler("finalizer-cleanup", mode="quarantine_only")


from .finalizer_registry import (  # noqa: E402
    register_finalizer_domain as _register_finalizer_domain,
)

_register_finalizer_domain(
    "finalizer_cleanup",
    drain=drain_finalizer_cleanup,
    snapshot=finalizer_cleanup_snapshot,
    activity=finalizer_cleanup_activity_snapshot,
    escrows=(("prepared_cleanup", _PREPARED_FINALIZER_ESCROW),),
    legacy_escrows=(("generic_cleanup", _FINALIZER_CLEANUP_ESCROW),),
)


__all__ = [
    "PreparedFinalizerCleanup",
    "reserve_finalizer_cleanup",
    "reserve_detached_resources_finalizer_cleanup",
    "reserve_reference_finalizer_cleanup",
    "reserve_resource_finalizer_cleanup",
    "reserve_owner_finalizer_cleanup",
    "cancel_prepared_finalizer_cleanup",
    "acknowledge_prepared_finalizer_cleanup",
    "defer_prepared_finalizer_cleanup",
    "defer_owner_finalizer_cleanup",
    "defer_finalizer_cleanup",
    "prepare_finalizer_cleanup",
    "prepare_detached_resources_finalizer_cleanup",
    "prepare_reference_finalizer_cleanup",
    "prepare_resource_finalizer_cleanup",
    "prepare_owner_finalizer_cleanup",
    "drain_finalizer_cleanup",
    "finalizer_cleanup_snapshot",
    "prepared_finalizer_capacity_snapshot",
]
