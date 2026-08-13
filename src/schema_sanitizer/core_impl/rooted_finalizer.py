"""Pre-rooted finalizer authorities for allocation-free GC handoff.

A :class:`RootedFinalizerAuthority` is deliberately separate from the Python
wrapper whose ``__del__`` arms it.  The escrow may therefore keep cleanup
authority strongly rooted without keeping the wrapper alive.  The wrapper's
GC tail performs only a non-blocking arm/publication; exact slot retirement
remains a normal safe-point/explicit-release operation.
"""

from __future__ import annotations

from typing import Callable

from .finalizer_escrow import ReservedFinalizerEscrow


class RootedFinalizerAuthority:
    """Compact mutable cleanup state rooted before external ownership exists."""

    __slots__ = (
        "callback",
        "arg0",
        "arg1",
        "arg2",
        "arg3",
        "arg4",
        "arg5",
        "arg6",
        "arg7",
        "arg8",
        "arg9",
        "arg10",
        "arg11",
        "ticket",
        "_escrow_armed",
        "_escrow_armed_ticket",
        "_ack_only",
    )

    def __init__(self, callback: Callable[["RootedFinalizerAuthority"], object]) -> None:
        if not callable(callback):
            raise TypeError("rooted finalizer callback must be callable")
        self.callback = callback
        self.arg0: object | None = None
        self.arg1: object | None = None
        self.arg2: object | None = None
        self.arg3: object | None = None
        self.arg4: object | None = None
        self.arg5: object | None = None
        self.arg6: object | None = None
        self.arg7: object | None = None
        self.arg8: object | None = None
        self.arg9: object | None = None
        self.arg10: object | None = None
        self.arg11: object | None = None
        self.ticket = 0
        self._escrow_armed = False
        # The generation, not a process-global boolean, is the durable arm
        # authority.  ``_escrow_armed`` remains as a compatibility mirror for
        # older focused doubles/source contracts.
        self._escrow_armed_ticket = 0
        self._ack_only = False

    def run(self) -> object | None:
        if self._ack_only:
            return None
        return self.callback(self)

    def clear(self) -> None:
        self.arg0 = None
        self.arg1 = None
        self.arg2 = None
        self.arg3 = None
        self.arg4 = None
        self.arg5 = None
        self.arg6 = None
        self.arg7 = None
        self.arg8 = None
        self.arg9 = None
        self.arg10 = None
        self.arg11 = None

    def make_ack_only(self) -> None:
        """Irreversibly disarm primary cleanup before secondary bookkeeping."""
        self._ack_only = True
        self._escrow_armed = False
        self._escrow_armed_ticket = 0

    def arm_for_ticket(self, ticket: int) -> None:
        """Durably arm exactly one escrow generation."""
        exact = int(ticket)
        if exact <= 0:
            raise ValueError("rooted finalizer arm ticket must be positive")
        self._escrow_armed_ticket = exact
        self._escrow_armed = True

    def disarm_ticket(self, ticket: int | None = None) -> None:
        """Disarm the matching generation without affecting a newer arm."""
        if ticket is not None and self._escrow_armed_ticket != int(ticket):
            return
        self._escrow_armed_ticket = 0
        self._escrow_armed = False

    def is_armed_for(self, ticket: int) -> bool:
        return bool(self._escrow_armed and self._escrow_armed_ticket == int(ticket))


class FinalizerReplayCapability:
    """Identity capability that remembers an already-committed exact release.

    Finalizer callbacks can be interrupted after the resource ledger has
    retired the owner but before the callback clears its authority arguments.
    Keeping this one-bit postcondition on the exact capability turns that replay
    into an acknowledgement rather than a second physical/logical release.
    """

    __slots__ = ("released",)

    def __init__(self) -> None:
        self.released = False


def reserve_rooted_finalizer_authority(
    escrow: ReservedFinalizerEscrow[RootedFinalizerAuthority],
    callback: Callable[[RootedFinalizerAuthority], object],
) -> tuple[int, RootedFinalizerAuthority]:
    """Reserve and root a separate cleanup authority before wrapper exposure."""
    # Allocate the owner first.  Once reserve_ticket commits, no allocation is
    # required to establish the escrow's physical ownership of this authority.
    authority = RootedFinalizerAuthority(callback)
    try:
        ticket = escrow.reserve_rooted(authority)
        if ticket is None:
            raise RuntimeError("rooted finalizer escrow exhausted")
        return ticket, authority
    except BaseException:
        try:
            escrow.release_rooted_owner(authority)
        except BaseException:
            pass
        authority.ticket = 0
        raise


def arm_rooted_finalizer_authority(
    escrow: ReservedFinalizerEscrow[RootedFinalizerAuthority],
    ticket: int,
    authority: RootedFinalizerAuthority,
) -> bool:
    """Durably hand off cleanup without blocking the GC thread."""
    if type(ticket) is not int or ticket <= 0 or authority.ticket != ticket:
        return False
    return bool(escrow.publish_rooted(ticket, authority))


def retire_or_ack_rooted_finalizer_authority(
    escrow: ReservedFinalizerEscrow[RootedFinalizerAuthority],
    ticket: int,
    authority: RootedFinalizerAuthority,
) -> bool:
    """Retire exact finalizer capacity, or durably publish ACK-only authority.

    ``make_ack_only`` happens *before* any potentially failing retirement call.
    Therefore an exception, stale race, or injected fault can never re-arm the
    primary cleanup.  ``True`` means the slot retired synchronously; ``False``
    means ACK ownership was durably handed to the escrow for a safe point.
    """
    if type(ticket) is not int or ticket <= 0 or authority.ticket != ticket:
        return True
    authority.make_ack_only()
    try:
        if escrow.release_rooted_ticket(ticket, authority):
            authority.ticket = 0
            authority.clear()
            return True
    except BaseException:
        # The semantic transition is already ACK-only.  Fall through and arm
        # that rooted ACK before re-raising would risk losing cleanup capacity.
        pass
    if escrow.publish_rooted(ticket, authority):
        return False
    raise RuntimeError("rooted finalizer acknowledgement did not commit")


def cancel_rooted_finalizer_authority(
    escrow: ReservedFinalizerEscrow[RootedFinalizerAuthority],
    ticket: int,
    authority: RootedFinalizerAuthority,
) -> bool:
    """Cancel an unused authority with the same irreversible ACK transition."""
    return retire_or_ack_rooted_finalizer_authority(escrow, ticket, authority)


__all__ = [
    "FinalizerReplayCapability",
    "RootedFinalizerAuthority",
    "arm_rooted_finalizer_authority",
    "cancel_rooted_finalizer_authority",
    "reserve_rooted_finalizer_authority",
    "retire_or_ack_rooted_finalizer_authority",
]
