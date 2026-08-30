"""Tests reserved-escrow generation identity, rollback or recycle states, replayed
finalizer admission, exact releases for memory, storage, and cross-process domains,
snapshots, source capabilities, and control-plane headroom. One owner has one
generation; replay is acknowledgement-only, stale tickets clear on rollback, and
recycle-pending state remains recoverable capacity."""

from __future__ import annotations

from pathlib import Path
from threading import Event, Lock, Thread

import pytest
from _support.synchronization import join_thread_or_fail


def test_reserved_escrow_same_owner_has_one_generation() -> None:
    """Verify reserved escrow same owner has one generation."""
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow
    from schema_sanitizer.core_impl.rooted_finalizer import RootedFinalizerAuthority

    owner = RootedFinalizerAuthority(lambda _owner: None)
    escrow: ReservedFinalizerEscrow[RootedFinalizerAuthority] = ReservedFinalizerEscrow(2)
    first = escrow.reserve_rooted(owner)
    assert first is not None
    # Lost-return retry of an unarmed RESERVED owner is idempotent.
    second = escrow.reserve_rooted(owner)
    assert second == first
    assert escrow.active_count() == 1
    # Public wrappers may clear this handoff field after publication; the
    # derived index remains exact and prevents a duplicate rooted generation.
    owner.ticket = 0
    assert escrow.reserve_rooted(owner) == first
    assert escrow.active_count() == 1

    assert escrow.publish_rooted(first, owner)
    assert owner._escrow_armed_ticket == first
    with pytest.raises(RuntimeError, match="already has an active generation"):
        escrow.reserve_rooted(owner)
    assert escrow.active_count() == 1

    assert escrow.process_one(lambda _ticket, value: value.run())
    assert escrow.active_count() == 0


def test_reserved_escrow_clean_owner_path_never_rebuilds_capacity(monkeypatch) -> None:
    """Keep rooted reservation and rollback on identity hints and the free ring."""
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow
    from schema_sanitizer.core_impl.rooted_finalizer import RootedFinalizerAuthority

    escrow: ReservedFinalizerEscrow[RootedFinalizerAuthority] = ReservedFinalizerEscrow(4_096)

    def unexpected_rebuild(_self: ReservedFinalizerEscrow[RootedFinalizerAuthority]) -> None:
        """Fail if a clean rooted-owner path attempts bounded reconstruction."""
        raise AssertionError("clean reserved-escrow path rebuilt capacity")

    monkeypatch.setattr(
        ReservedFinalizerEscrow, "_rebuild_capacity_mirrors_locked", unexpected_rebuild
    )
    for _ in range(32):
        owner = RootedFinalizerAuthority(lambda _owner: None)
        assert escrow.reserve_rooted(owner) is not None
        assert escrow.release_rooted_owner(owner)


def test_reserved_escrow_dirty_ticket_map_recovers_exact_lost_handoff() -> None:
    """Rebuild a missing ticket mirror from its exact rooted slot."""
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow
    from schema_sanitizer.core_impl.rooted_finalizer import RootedFinalizerAuthority

    escrow: ReservedFinalizerEscrow[RootedFinalizerAuthority] = ReservedFinalizerEscrow(4)
    owner = RootedFinalizerAuthority(lambda _owner: None)
    ticket = escrow.reserve_rooted(owner)
    assert ticket is not None
    escrow._ticket_slots.clear()
    escrow._capacity_mirrors_dirty = True

    assert escrow.reserve_rooted(owner) == ticket
    assert escrow.active_count() == 1
    assert escrow.release_rooted_owner(owner)


def test_reserved_escrow_rejects_stale_owner_ticket_hint_by_exact_identity() -> None:
    """Validate an owner ticket hint against exact slot identity before accepting it."""
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow
    from schema_sanitizer.core_impl.rooted_finalizer import RootedFinalizerAuthority

    escrow: ReservedFinalizerEscrow[RootedFinalizerAuthority] = ReservedFinalizerEscrow(2)
    first = RootedFinalizerAuthority(lambda _owner: None)
    second = RootedFinalizerAuthority(lambda _owner: None)
    first_ticket = escrow.reserve_rooted(first)
    second_ticket = escrow.reserve_rooted(second)
    assert first_ticket is not None
    assert second_ticket is not None
    first.ticket = second_ticket

    # The stale ticket points at a live but different owner. Exact identity wins,
    # reconstruction finds the original generation, and no duplicate is created.
    assert escrow.reserve_rooted(first) == first_ticket
    assert escrow.active_count() == 2
    assert escrow.release_rooted_owner(first)
    assert escrow.release_rooted_owner(second)


def test_split_root_rejects_the_same_exact_owner_in_a_second_ticket() -> None:
    """Prevent split admission from orphaning one of two duplicate owner slots."""
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow
    from schema_sanitizer.core_impl.rooted_finalizer import RootedFinalizerAuthority

    escrow: ReservedFinalizerEscrow[RootedFinalizerAuthority] = ReservedFinalizerEscrow(2)
    first = escrow.reserve_ticket()
    second = escrow.reserve_ticket()
    assert first is not None
    assert second is not None
    owner = RootedFinalizerAuthority(lambda _owner: None)

    assert escrow.root_reserved(first, owner)
    assert not escrow.root_reserved(second, owner)
    assert escrow.release_rooted_owner(owner)
    assert escrow.release_rooted_owner(owner)
    assert escrow.release_ticket(second)
    assert escrow.active_count() == 0
    assert escrow.capacity_snapshot().available == 2


def test_direct_ticket_publish_keeps_capacity_clean_for_consecutive_tickets() -> None:
    """Direct-ticket publication dirties only its derived owner index."""
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    escrow: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(2)
    first = escrow.reserve_ticket()
    second = escrow.reserve_ticket()
    assert first is not None and second is not None
    first_owner = object()
    second_owner = object()

    assert escrow.publish_reserved(first, first_owner)
    assert escrow.publish_reserved(first, first_owner)
    assert not escrow._capacity_mirrors_dirty
    assert escrow._owner_slots_dirty
    assert escrow.publish_reserved(second, second_owner)
    assert escrow.published_count() == 2
    assert not escrow.overflowed
    assert escrow.process_one(lambda _ticket, _value: None)
    assert escrow.process_one(lambda _ticket, _value: None)


def test_direct_ticket_publish_rejects_duplicate_owner_while_index_dirty() -> None:
    """Scan bounded authority when direct publication has not rebuilt its index."""
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    escrow: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(2)
    first = escrow.reserve_ticket()
    second = escrow.reserve_ticket()
    assert first is not None and second is not None
    owner = object()

    assert escrow.publish_reserved(first, owner)
    assert not escrow.publish_reserved(second, owner)
    assert escrow.process_one(lambda _ticket, _value: None)
    assert escrow.release_ticket(second)


def test_direct_ticket_publish_recovers_post_owner_assignment_interruption() -> None:
    """Promote an exactly retained owner after interrupted direct publication."""
    import schema_sanitizer.core_impl.finalizer_escrow as module

    escrow: module.ReservedFinalizerEscrow[object] = module.ReservedFinalizerEscrow(1)
    ticket = escrow.reserve_ticket()
    assert ticket is not None
    owner = object()

    class AssignThenInterrupt(list[object]):
        """Commit owner retention before injecting one asynchronous exception."""

        failed = False

        def __setitem__(self, index, value) -> None:
            """Raise once after the target owner is durably stored."""
            super().__setitem__(index, value)
            if value is owner and not self.failed:
                self.failed = True
                raise KeyboardInterrupt("direct-ticket publication post-owner commit")

    escrow._slots = AssignThenInterrupt(escrow._slots)
    with pytest.raises(KeyboardInterrupt, match="post-owner commit"):
        escrow.publish_reserved(ticket, owner)

    assert escrow.publish_reserved(ticket, owner)
    seen: list[object] = []
    assert escrow.process_one(lambda _ticket, value: seen.append(value))
    assert seen == [owner]
    assert escrow.active_count() == 0
    assert not escrow.overflowed


def test_direct_ticket_publish_acknowledges_post_marker_clear_interruption() -> None:
    """Acknowledge exact publication when marker clearing committed before raising."""
    import schema_sanitizer.core_impl.finalizer_escrow as module

    escrow: module.ReservedFinalizerEscrow[object] = module.ReservedFinalizerEscrow(1)
    ticket = escrow.reserve_ticket()
    assert ticket is not None
    owner = object()

    class ClearThenInterrupt(bytearray):
        """Interrupt once after clearing a live publication marker."""

        failed = False

        def __setitem__(self, index, value) -> None:
            """Commit marker clearing before raising the injected exception."""
            previous = self[index]
            super().__setitem__(index, value)
            if previous == 1 and value == 0 and not self.failed:
                self.failed = True
                raise KeyboardInterrupt("direct-ticket publication post-marker clear")

    escrow._owner_publications = ClearThenInterrupt(escrow._owner_publications)
    with pytest.raises(KeyboardInterrupt, match="post-marker clear"):
        escrow.publish_reserved(ticket, owner)

    assert escrow.publish_reserved(ticket, owner)
    assert not escrow.overflowed
    assert escrow.process_one(lambda _ticket, _value: None)
    assert escrow.active_count() == 0


def test_direct_ticket_publish_recovers_or_cancels_post_marker_set_interruption() -> None:
    """Finish an exact retry or fail closed when publication never retained its owner."""
    import schema_sanitizer.core_impl.finalizer_escrow as module

    class SetThenInterrupt(bytearray):
        """Interrupt once after publishing an in-progress marker."""

        failed = False

        def __setitem__(self, index, value) -> None:
            """Commit marker publication before raising the injected exception."""
            previous = self[index]
            super().__setitem__(index, value)
            if previous == 0 and value == 1 and not self.failed:
                self.failed = True
                raise KeyboardInterrupt("direct-ticket publication post-marker set")

    for retry in (True, False):
        escrow: module.ReservedFinalizerEscrow[object] = module.ReservedFinalizerEscrow(1)
        ticket = escrow.reserve_ticket()
        assert ticket is not None
        owner = object()
        escrow._owner_publications = SetThenInterrupt(escrow._owner_publications)
        with pytest.raises(KeyboardInterrupt, match="post-marker set"):
            escrow.publish_reserved(ticket, owner)

        if retry:
            assert escrow.publish_reserved(ticket, owner)
            assert escrow.process_one(lambda _ticket, _value: None)
            assert not escrow.overflowed
            assert escrow.active_count() == 0
            continue

        assert not escrow.process_one(lambda _ticket, _value: None)
        snapshot = escrow.capacity_snapshot()
        assert snapshot.active == 0
        assert snapshot.available == 1
        assert snapshot.overflowed
        assert snapshot.publication_failures == 1
        assert not escrow.activity_is_quiescent()


def test_orphan_publication_recovery_preserves_concurrent_failure_increments() -> None:
    """Never lower failures published while orphan recovery holds admission."""
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    escrow: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(1)
    ticket = escrow.reserve_ticket()
    assert ticket is not None
    slot = escrow._ticket_slots[ticket]
    escrow._capacity_mirrors_dirty = True
    escrow._owner_slots_dirty = True
    escrow._owner_publications[slot] = 1
    counter = escrow._publication_failures_counter
    commit_entered = Event()
    continue_rebuild = Event()
    original_increment_marked = counter._inc_marked
    assert original_increment_marked is not None

    def pause_before_commit(capsule: object, markers: bytearray, index: int) -> object:
        """Pause before the indivisible counter-and-marker native commit."""
        commit_entered.set()
        assert continue_rebuild.wait(30)
        return original_increment_marked(capsule, markers, index)

    counter._inc_marked = pause_before_commit
    result: list[bool] = []
    worker = Thread(target=lambda: result.append(escrow.process_one(lambda *_args: None)))
    worker.start()
    assert commit_entered.wait(30)

    owner = object()
    assert not escrow.publish_reserved(ticket, owner)
    assert not escrow.publish_reserved(ticket, owner)
    continue_rebuild.set()
    join_thread_or_fail(worker)

    assert result == [False]
    snapshot = escrow.capacity_snapshot()
    assert snapshot.publication_failures == 3
    assert snapshot.active == 0
    assert snapshot.available == 1


def test_orphan_publication_recovery_counts_after_an_unrelated_failure() -> None:
    """Count an orphan independently of an earlier publication failure."""
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    escrow: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(1)
    ticket = escrow.reserve_ticket()
    assert ticket is not None
    slot = escrow._ticket_slots[ticket]
    escrow._capacity_mirrors_dirty = True
    escrow._owner_slots_dirty = True
    escrow._owner_publications[slot] = 1

    assert not escrow.publish_reserved(ticket + 100, object())
    assert escrow._publication_failures_counter.value() == 1
    assert not escrow.process_one(lambda *_args: None)
    snapshot = escrow.capacity_snapshot()
    assert snapshot.publication_failures == 2
    assert snapshot.active == 0
    assert snapshot.available == 1


def test_orphan_publication_recovery_retries_pre_commit_interruption() -> None:
    """Keep orphan authority active until its failure record can commit."""
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    escrow: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(1)
    ticket = escrow.reserve_ticket()
    assert ticket is not None
    slot = escrow._ticket_slots[ticket]
    escrow._capacity_mirrors_dirty = True
    escrow._owner_slots_dirty = True
    escrow._owner_publications[slot] = 1
    counter = escrow._publication_failures_counter
    original_increment_marked = counter._inc_marked
    assert original_increment_marked is not None

    def interrupt_before_commit(_capsule: object, _markers: bytearray, _index: int) -> object:
        """Raise without changing either member of the native commit pair."""
        raise KeyboardInterrupt("pre marked increment")

    counter._inc_marked = interrupt_before_commit
    with pytest.raises(RuntimeError, match="counter is unavailable"):
        escrow.process_one(lambda *_args: None)
    assert escrow._states[slot] == 5
    assert escrow._owner_publications[slot] == 1
    assert escrow._capacity_mirrors_dirty
    assert escrow._active_counter.value() == 1
    assert counter.value() == 0

    counter._inc_marked = original_increment_marked
    assert not escrow.process_one(lambda *_args: None)
    snapshot = escrow.capacity_snapshot()
    assert snapshot.publication_failures == 1
    assert snapshot.active == 0
    assert snapshot.available == 1


def test_orphan_publication_recovery_survives_post_commit_interruption() -> None:
    """Trust the marker when native failure publication commits before raising."""
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    escrow: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(1)
    ticket = escrow.reserve_ticket()
    assert ticket is not None
    slot = escrow._ticket_slots[ticket]
    escrow._capacity_mirrors_dirty = True
    escrow._owner_slots_dirty = True
    escrow._owner_publications[slot] = 1
    counter = escrow._publication_failures_counter
    original_increment_marked = counter._inc_marked
    assert original_increment_marked is not None

    def commit_then_interrupt(capsule: object, markers: bytearray, index: int) -> object:
        """Raise after the native counter and recorded marker both commit."""
        original_increment_marked(capsule, markers, index)
        raise KeyboardInterrupt("post marked increment")

    counter._inc_marked = commit_then_interrupt
    assert not escrow.process_one(lambda *_args: None)
    snapshot = escrow.capacity_snapshot()
    assert snapshot.publication_failures == 1
    assert snapshot.active == 0
    assert snapshot.available == 1


def test_orphan_publication_recovery_retries_post_return_interruption() -> None:
    """Do not recount when control stops after the paired method returns."""
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    escrow: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(1)
    ticket = escrow.reserve_ticket()
    assert ticket is not None
    slot = escrow._ticket_slots[ticket]
    escrow._capacity_mirrors_dirty = True
    escrow._owner_slots_dirty = True
    escrow._owner_publications[slot] = 1
    base_counter = escrow._publication_failures_counter

    class ReturnThenInterrupt:
        """Raise once after the wrapped composite method reports success."""

        failed = False

        def increment_marked(self, markers: bytearray, index: int) -> bool:
            """Commit through the base counter before interrupting its caller."""
            result = base_counter.increment_marked(markers, index)
            if not self.failed:
                self.failed = True
                raise KeyboardInterrupt("post marked method return")
            return result

        def __getattr__(self, name: str) -> object:
            """Delegate every other counter operation to the base counter."""
            return getattr(base_counter, name)

    escrow._publication_failures_counter = ReturnThenInterrupt()  # type: ignore[assignment]
    with pytest.raises(KeyboardInterrupt, match="post marked method return"):
        escrow.process_one(lambda *_args: None)
    assert base_counter.value() == 1
    assert escrow._owner_publications[slot] == 2
    assert escrow._capacity_mirrors_dirty
    assert escrow._active_counter.value() == 1

    assert not escrow.process_one(lambda *_args: None)
    snapshot = escrow.capacity_snapshot()
    assert snapshot.publication_failures == 1
    assert snapshot.active == 0
    assert snapshot.available == 1


def test_orphan_publication_recovery_retries_post_marker_clear_interruption() -> None:
    """Never recount an orphan after its recorded marker was durably cleared."""
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    class ClearThenInterrupt(bytearray):
        """Interrupt once after clearing a recorded orphan marker."""

        failed = False

        def __setitem__(self, index, value) -> None:
            """Commit marker clearing before raising the injected exception."""
            previous = self[index]
            super().__setitem__(index, value)
            if previous == 2 and value == 0 and not self.failed:
                self.failed = True
                raise KeyboardInterrupt("post recorded marker clear")

    escrow: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(1)
    ticket = escrow.reserve_ticket()
    assert ticket is not None
    slot = escrow._ticket_slots[ticket]
    escrow._capacity_mirrors_dirty = True
    escrow._owner_slots_dirty = True
    escrow._owner_publications = ClearThenInterrupt(escrow._owner_publications)
    escrow._owner_publications[slot] = 1

    with pytest.raises(KeyboardInterrupt, match="post recorded marker clear"):
        escrow.process_one(lambda *_args: None)
    assert escrow._publication_failures_counter.value() == 1
    assert escrow._owner_publications[slot] == 0
    assert escrow._capacity_mirrors_dirty
    assert escrow._active_counter.value() == 1

    assert not escrow.process_one(lambda *_args: None)
    snapshot = escrow.capacity_snapshot()
    assert snapshot.publication_failures == 1
    assert snapshot.active == 0
    assert snapshot.available == 1


def test_direct_ticket_publish_acknowledges_post_clean_commit_interruption() -> None:
    """Acknowledge exact publication after the final clean commit raises."""
    import schema_sanitizer.core_impl.finalizer_escrow as module

    class CleanThenInterruptEscrow(module.ReservedFinalizerEscrow[object]):
        """Inject one exception after capacity mirrors become clean."""

        __slots__ = ("interrupt_next_clean",)

        def __init__(self) -> None:
            """Initialize the escrow with clean-commit injection disabled."""
            self.interrupt_next_clean = False
            super().__init__(1)

        def __setattr__(self, name: str, value: object) -> None:
            """Commit the requested attribute before the selected exception."""
            previous = getattr(self, name, None)
            super().__setattr__(name, value)
            if (
                name == "_capacity_mirrors_dirty"
                and previous is True
                and value is False
                and self.interrupt_next_clean
            ):
                self.interrupt_next_clean = False
                raise KeyboardInterrupt("direct-ticket publication post-clean commit")

    escrow = CleanThenInterruptEscrow()
    ticket = escrow.reserve_ticket()
    assert ticket is not None
    owner = object()
    escrow.interrupt_next_clean = True
    with pytest.raises(KeyboardInterrupt, match="post-clean commit"):
        escrow.publish_reserved(ticket, owner)

    assert escrow.publish_reserved(ticket, owner)
    assert not escrow.overflowed
    assert escrow.process_one(lambda _ticket, _value: None)
    assert escrow.active_count() == 0


def test_dirty_owner_index_cannot_ack_a_live_owner_as_retired() -> None:
    """Rebuild a dirty owner index before accepting a missing-ticket release."""
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    escrow: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(2)
    ticket = escrow.reserve_ticket()
    assert ticket is not None
    owner = object()
    assert escrow.publish_reserved(ticket, owner)
    assert escrow._owner_slots_dirty

    assert not escrow.release_rooted_ticket(ticket + 1_000, owner)
    assert any(candidate is owner for candidate in escrow._slots)
    assert escrow.process_one(lambda _ticket, _value: None)


def test_rooted_publish_accepts_post_arm_assignment_interruption() -> None:
    """Treat an exact post-exception arm state as a successful handoff."""
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    class Owner:
        """Expose one finalizer arm that raises after its exact commit."""

        def __init__(self) -> None:
            """Initialize unarmed ticket state for the test owner."""
            self.ticket = 0
            self._escrow_armed_ticket = 0
            self.failed = False

        def arm_for_ticket(self, ticket: int) -> None:
            """Commit the arm and interrupt its caller once."""
            self._escrow_armed_ticket = ticket
            if not self.failed:
                self.failed = True
                raise KeyboardInterrupt("rooted publication post-arm commit")

        def disarm_ticket(self, ticket: int | None = None) -> None:
            """Clear the matching arm during escrow retirement."""
            if ticket is None or self._escrow_armed_ticket == ticket:
                self._escrow_armed_ticket = 0

    escrow: ReservedFinalizerEscrow[Owner] = ReservedFinalizerEscrow(1)
    owner = Owner()
    ticket = escrow.reserve_rooted(owner)
    assert ticket is not None

    assert escrow.publish_rooted(ticket, owner)
    assert not escrow.overflowed
    assert escrow.process_one(lambda _ticket, _value: None)
    assert escrow.active_count() == 0


def test_split_root_cannot_race_owner_hint_recovery_into_a_duplicate() -> None:
    """Serialize split rooting through the exact derived owner index."""
    import schema_sanitizer.core_impl.finalizer_escrow as module
    from schema_sanitizer.core_impl.rooted_finalizer import RootedFinalizerAuthority

    escrow: module.ReservedFinalizerEscrow[RootedFinalizerAuthority] = (
        module.ReservedFinalizerEscrow(2)
    )
    split_ticket = escrow.reserve_ticket()
    assert split_ticket is not None
    owner = RootedFinalizerAuthority(lambda _owner: None)
    assert escrow.root_reserved(split_ticket, owner)
    assert escrow.reserve_rooted(owner) == split_ticket
    assert sum(candidate is owner for candidate in escrow._slots) == 1
    assert escrow.release_rooted_owner(owner)


def test_rooted_release_interrupt_after_owner_removal_repairs_ticket_mirrors() -> None:
    """Repair stale ticket metadata after interruption at the owner-removal commit."""
    import schema_sanitizer.core_impl.finalizer_escrow as module
    from schema_sanitizer.core_impl.rooted_finalizer import RootedFinalizerAuthority

    escrow: module.ReservedFinalizerEscrow[RootedFinalizerAuthority] = (
        module.ReservedFinalizerEscrow(1)
    )
    owner = RootedFinalizerAuthority(lambda _owner: None)
    ticket = escrow.reserve_rooted(owner)
    assert ticket is not None

    class RemoveThenInterrupt(list[object]):
        """Interrupt immediately after the exact rooted owner becomes absent."""

        failed = False

        def __setitem__(self, index, value) -> None:
            """Commit owner removal and inject one asynchronous interruption."""
            super().__setitem__(index, value)
            if value is module._EMPTY and not self.failed:
                self.failed = True
                raise KeyboardInterrupt("rooted release post-owner-removal")

    escrow._slots = RemoveThenInterrupt(escrow._slots)
    assert escrow.release_rooted_ticket(ticket, owner)

    assert ticket not in escrow._ticket_slots
    assert escrow.active_count() == 0
    replacement = RootedFinalizerAuthority(lambda _owner: None)
    assert escrow.reserve_rooted(replacement) is not None
    assert escrow.release_rooted_owner(replacement)


def test_ticket_release_interrupt_after_owner_removal_repairs_ticket_mirrors() -> None:
    """Repair a rooted release-ticket commit interrupted after owner removal."""
    import schema_sanitizer.core_impl.finalizer_escrow as module
    from schema_sanitizer.core_impl.rooted_finalizer import RootedFinalizerAuthority

    escrow: module.ReservedFinalizerEscrow[RootedFinalizerAuthority] = (
        module.ReservedFinalizerEscrow(1)
    )
    owner = RootedFinalizerAuthority(lambda _owner: None)
    ticket = escrow.reserve_rooted(owner)
    assert ticket is not None

    class RemoveThenInterrupt(list[object]):
        """Interrupt after release_ticket removes its exact rooted owner."""

        failed = False

        def __setitem__(self, index, value) -> None:
            """Commit owner removal and interrupt before ticket-map cleanup."""
            super().__setitem__(index, value)
            if value is module._EMPTY and not self.failed:
                self.failed = True
                raise KeyboardInterrupt("ticket release post-owner-removal")

    escrow._slots = RemoveThenInterrupt(escrow._slots)
    assert escrow.release_ticket(ticket)

    assert ticket not in escrow._ticket_slots
    assert escrow.active_count() == 0
    assert escrow.capacity_snapshot().available == 1


def test_process_retirement_interrupt_after_owner_removal_rebuilds_every_mirror() -> None:
    """Retry a processed-owner retirement interrupted at its authority commit."""
    import schema_sanitizer.core_impl.finalizer_escrow as module
    from schema_sanitizer.core_impl.rooted_finalizer import RootedFinalizerAuthority

    escrow: module.ReservedFinalizerEscrow[RootedFinalizerAuthority] = (
        module.ReservedFinalizerEscrow(1)
    )
    owner = RootedFinalizerAuthority(lambda _owner: None)
    ticket = escrow.reserve_rooted(owner)
    assert ticket is not None
    assert escrow.publish_rooted(ticket, owner)

    class RemoveThenInterrupt(list[object]):
        """Inject one exact post-removal interrupt into processed retirement."""

        failed = False

        def __setitem__(self, index, value) -> None:
            """Commit the owner removal before delivering the interruption."""
            super().__setitem__(index, value)
            if value is module._EMPTY and not self.failed:
                self.failed = True
                raise KeyboardInterrupt("processed retirement post-owner-removal")

    escrow._slots = RemoveThenInterrupt(escrow._slots)
    with pytest.raises(KeyboardInterrupt, match="processed retirement post-owner-removal"):
        escrow.process_one(lambda _ticket, value: value.run())

    assert ticket not in escrow._ticket_slots
    assert escrow.active_count() == 0
    assert escrow.published_count() == 0
    assert escrow.capacity_snapshot().available == 1


def test_reserved_escrow_rollback_clears_stale_ticket_and_arm(monkeypatch) -> None:
    """Verify reserved escrow rollback clears stale ticket and arm."""
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow
    from schema_sanitizer.core_impl.rooted_finalizer import RootedFinalizerAuthority

    escrow: ReservedFinalizerEscrow[RootedFinalizerAuthority] = ReservedFinalizerEscrow(1)
    owner = RootedFinalizerAuthority(lambda _owner: None)
    original = ReservedFinalizerEscrow._bump_progress
    failed = False
    stale_ticket = 0

    def interrupt(self: ReservedFinalizerEscrow[RootedFinalizerAuthority]) -> None:
        """Inject the interruption at the controlled handoff point."""
        nonlocal failed, stale_ticket
        if self is escrow and not failed:
            failed = True
            stale_ticket = owner.ticket
            raise KeyboardInterrupt("reserved-escrow-same-owner-has-one reserve rollback")
        original(self)

    monkeypatch.setattr(ReservedFinalizerEscrow, "_bump_progress", interrupt)
    with pytest.raises(KeyboardInterrupt, match="reserve rollback"):
        escrow.reserve_rooted(owner)

    assert owner.ticket == 0
    assert owner._escrow_armed_ticket == 0
    assert stale_ticket > 0
    assert escrow.publish_rooted(stale_ticket, owner)
    assert not escrow.overflowed
    snap = escrow.capacity_snapshot()
    assert snap.active == 0
    assert snap.invariant_ok


def test_recycle_pending_is_recoverable_capacity_not_overflow(monkeypatch) -> None:
    """Verify recycle pending is recoverable capacity not overflow."""
    import schema_sanitizer.core_impl.finalizer_escrow as module
    from schema_sanitizer.core_impl.rooted_finalizer import RootedFinalizerAuthority

    escrow: module.ReservedFinalizerEscrow[RootedFinalizerAuthority] = (
        module.ReservedFinalizerEscrow(1)
    )
    owner = RootedFinalizerAuthority(lambda _owner: None)
    ticket = escrow.reserve_rooted(owner)
    assert ticket is not None
    assert escrow.publish_rooted(ticket, owner)

    original = module.ReservedFinalizerEscrow._recycle_one_pending_locked

    def interrupt(self: module.ReservedFinalizerEscrow[RootedFinalizerAuthority]) -> bool:
        """Inject the interruption at the controlled handoff point."""
        if self is escrow:
            raise KeyboardInterrupt("reserved-escrow-same-owner-has-one recycle")
        return original(self)

    monkeypatch.setattr(module.ReservedFinalizerEscrow, "_recycle_one_pending_locked", interrupt)
    assert escrow.process_one(lambda _ticket, value: value.run())

    snap = escrow.capacity_snapshot()
    assert snap.active == 0
    assert snap.available == 0
    assert snap.recycle_pending == 1
    assert not snap.overflowed
    assert snap.invariant_ok


def test_path_claim_admission_finalizer_is_replay_idempotent() -> None:
    """Verify path claim admission finalizer is replay idempotent."""
    from schema_sanitizer.core_impl import path_identity as module

    before = module._PATH_CLAIM_ADMISSION_OWNERS.exact_active_count()
    admission = module._acquire_path_claim_admission()
    authority = admission.finalizer_owner
    assert module._PATH_CLAIM_ADMISSION_OWNERS.exact_active_count() == before + 1

    module._run_path_claim_admission_finalizer(authority)
    assert module._PATH_CLAIM_ADMISSION_OWNERS.exact_active_count() == before
    # Simulate callback replay before process_one could publish PROCESSED.
    module._run_path_claim_admission_finalizer(authority)
    assert module._PATH_CLAIM_ADMISSION_OWNERS.exact_active_count() == before

    admission.counted = False
    admission.released = True
    module._PATH_CLAIM_FINALIZER_ESCROW.release_rooted_owner(authority)


def test_operation_memory_exact_release_replay_is_ack(monkeypatch) -> None:
    """Verify operation memory exact release replay is ack."""
    from schema_sanitizer.core_impl import memory_budget as module

    ledger = object.__new__(module.OperationMemoryLedger)
    ledger._lock = Lock()
    ledger._python_leases = {}
    ledger._unknown_python_lease_releases = 0
    ledger._post_release_observation_failures = 0
    ledger._cross_process_release_deferred = False
    capability = module.FinalizerReplayCapability()
    lease_id = 17
    owner_id = 23
    entry = module._PythonMemoryLeaseEntry(
        owner_id,
        capability,
        0,
        None,
        physical_released=True,
        physical_size_bytes=0,
        native_receipt=None,
    )
    ledger._python_leases[lease_id] = entry
    monkeypatch.setattr(
        module.OperationMemoryLedger, "_maybe_finish_deferred_close", lambda self: None
    )
    monkeypatch.setattr(
        module.OperationMemoryLedger, "_schedule_deferred_close_cleanup_noexcept", lambda self: None
    )

    ledger._release_python_lease_authority(lease_id, owner_id, capability)
    assert capability.released
    assert lease_id not in ledger._python_leases
    # Same exact capability is an ACK, not an unknown/double release.
    ledger._release_python_lease_authority(lease_id, owner_id, capability)
    assert ledger._unknown_python_lease_releases == 0


def test_temporary_storage_exact_release_replay_is_ack(monkeypatch, tmp_path: Path) -> None:
    """Verify temporary storage exact release replay is ack."""
    from schema_sanitizer.core_impl import temporary_storage as module

    pool = module.TemporaryStoragePermitPool(None)
    capability = module.FinalizerReplayCapability()
    lease_id = 9
    owner_id = 11
    entry = module._StorageLeaseEntry(
        owner_id,
        capability,
        0,
        0,
        tmp_path,
        0,
        module.ProcessTemporaryStorageCapability(module._PROCESS_TEMPORARY_STORAGE),
        None,
        process_released=True,
        local_released=True,
    )
    pool._leases[lease_id] = entry
    monkeypatch.setattr(
        module.TemporaryStoragePermitPool, "_finish_pending_resize_locked", lambda self, entry: None
    )

    pool._release_lease_authority(lease_id, owner_id, capability)
    assert capability.released
    assert lease_id not in pool._leases
    pool._release_lease_authority(lease_id, owner_id, capability)
    assert pool._unknown_lease_releases == 0


def test_direct_cross_memory_exact_release_replay_is_ack() -> None:
    """Verify direct cross memory exact release replay is ack."""
    from schema_sanitizer.core_impl import cross_process_memory as module

    owner = object()
    free_before = module._DIRECT_LEASE_FREE_COUNT
    registration = module._register_direct_lease(owner)
    lease_id = registration.lease_id
    capability = registration.capability
    module._update_direct_lease_reserved(owner, lease_id, capability, 123)

    assert module._retire_direct_lease_authority(id(owner), lease_id, capability) == 123
    assert capability.released
    assert module._DIRECT_LEASE_FREE_COUNT == free_before
    unknown_before = module._DIRECT_LEASE_UNKNOWN_RELEASES
    assert module._retire_direct_lease_authority(id(owner), lease_id, capability) == 0
    assert module._DIRECT_LEASE_UNKNOWN_RELEASES == unknown_before


def test_finalizer_admission_snapshot_counts_recycle_pending(monkeypatch) -> None:
    """Verify finalizer admission snapshot counts recycle pending."""
    import schema_sanitizer.core_impl.finalizer_escrow as module
    from schema_sanitizer.core_impl.finalizer_admission import _domain
    from schema_sanitizer.core_impl.rooted_finalizer import RootedFinalizerAuthority

    escrow: module.ReservedFinalizerEscrow[RootedFinalizerAuthority] = (
        module.ReservedFinalizerEscrow(1)
    )
    owner = RootedFinalizerAuthority(lambda _owner: None)
    ticket = escrow.reserve_rooted(owner)
    assert ticket is not None
    assert escrow.publish_rooted(ticket, owner)

    original = module.ReservedFinalizerEscrow._recycle_one_pending_locked
    monkeypatch.setattr(
        module.ReservedFinalizerEscrow,
        "_recycle_one_pending_locked",
        lambda self: (
            (_ for _ in ()).throw(KeyboardInterrupt("pending"))
            if self is escrow
            else original(self)
        ),
    )
    assert escrow.process_one(lambda _ticket, value: value.run())
    domain = _domain("reserved-escrow-same-owner-has-one", escrow)  # type: ignore[arg-type]
    assert domain.recycle_pending == 1
    assert domain.invariant_ok


def test_source_contract_replay_capabilities_and_exact_arms() -> None:
    """Verify source contract replay capabilities and exact arms."""
    root = Path(__file__).resolve().parents[2] / "src/schema_sanitizer/core_impl"
    escrow = (root / "finalizer_escrow.py").read_text(encoding="utf-8")
    rooted = (root / "rooted_finalizer.py").read_text(encoding="utf-8")
    memory = (root / "memory_budget.py").read_text(encoding="utf-8")
    storage = (root / "temporary_storage.py").read_text(encoding="utf-8")
    cross = (root / "cross_process_memory.py").read_text(encoding="utf-8")
    path = (root / "path_identity.py").read_text(encoding="utf-8")

    assert "is_armed_for" in rooted
    assert "reserved finalizer owner already has an active generation" in escrow
    assert "recycle_pending" in escrow
    assert "FinalizerReplayCapability()" in memory
    assert "FinalizerReplayCapability()" in storage
    assert "FinalizerReplayCapability()" in cross
    assert "_PATH_CLAIM_ADMISSION_OWNERS = BoundedGenerationPool" in path
    assert "_PATH_CLAIM_ADMISSION_OWNERS_FORK_FRESH" in path


def test_default_control_plane_capacity_leaves_dynamic_headroom() -> None:
    """Verify default control plane capacity leaves dynamic headroom."""
    from schema_sanitizer.core_impl.control_plane_budget import (
        _DEFAULT_CAPACITY_BYTES,
        _MAX_CAPACITY_BYTES,
        process_control_plane_snapshot,
        release_control_plane,
        reserve_control_plane,
    )

    snapshot = process_control_plane_snapshot()
    assert snapshot.static_baseline_bytes < _DEFAULT_CAPACITY_BYTES
    assert snapshot.capacity_bytes == _DEFAULT_CAPACITY_BYTES
    assert _DEFAULT_CAPACITY_BYTES <= _MAX_CAPACITY_BYTES

    ticket = reserve_control_plane("reserved-escrow-same-owner-has-one_default_headroom", 256)
    assert release_control_plane(ticket)
