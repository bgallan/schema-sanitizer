"""Regression coverage for memory prepared finalizer ack disarms primary before retirement failure."""

from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src" / "schema_sanitizer"


def _fail_first_release_for(monkeypatch: pytest.MonkeyPatch, target: object) -> None:
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    original = ReservedFinalizerEscrow.release_ticket
    failed = False

    def flaky(self: object, ticket: int) -> bool:
        nonlocal failed
        if self is target and not failed:
            failed = True
            return False
        return original(self, ticket)  # type: ignore[arg-type]

    monkeypatch.setattr(ReservedFinalizerEscrow, "release_ticket", flaky)


def test_prepared_finalizer_ack_disarms_primary_before_retirement_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import finalizer_cleanup as module

    calls = 0

    def primary(_capsule: object) -> None:
        nonlocal calls
        calls += 1

    capsule = module.reserve_finalizer_cleanup(primary)
    _fail_first_release_for(monkeypatch, module._PREPARED_FINALIZER_ESCROW)

    with pytest.raises(RuntimeError, match="acknowledgement did not commit"):
        module.acknowledge_prepared_finalizer_cleanup(capsule)

    assert capsule.ticket != 0
    assert capsule.callback is module._drop_detached_references_capsule
    assert module.defer_prepared_finalizer_cleanup(capsule)
    module.drain_finalizer_cleanup()
    assert calls == 0


def test_remote_permit_ack_failure_retries_only_secondary_tail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import finalizer_cleanup
    from schema_sanitizer.remote_impl.io_permits import RemoteIoPermit

    class Governor:
        def __init__(self) -> None:
            self.calls = 0

        def _release(self, _weight: int) -> None:
            self.calls += 1

    governor = Governor()
    permit = RemoteIoPermit(governor, 1, "prepared-finalizer-ack-disarms-primary-before")  # type: ignore[arg-type]
    _fail_first_release_for(monkeypatch, finalizer_cleanup._PREPARED_FINALIZER_ESCROW)

    with pytest.raises(RuntimeError, match="acknowledgement did not commit"):
        permit.release()
    assert permit._released is True
    assert governor.calls == 1

    permit.release()
    assert governor.calls == 1
    assert permit._finalizer_ticket == 0


def test_provider_control_tail_failure_keeps_exact_owner_without_double_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.remote_impl import provider_throttle as module

    governor = module.ProviderThrottleGovernor(max_tracked_keys=4)
    lease, _delay = governor.try_acquire("prepared-finalizer-ack-disarms-primary-before")
    assert lease is not None
    lease_entry = governor._active_leases[lease._lease_id]
    lease_ticket = lease_entry.control_ticket
    real_release = module.release_control_plane
    failed = False

    def flaky(ticket: object) -> bool:
        nonlocal failed
        if ticket is lease_ticket and not failed:
            failed = True
            return False
        return bool(real_release(ticket))  # type: ignore[arg-type]

    monkeypatch.setattr(module, "release_control_plane", flaky)
    with pytest.raises(RuntimeError, match="control-plane retirement did not commit"):
        lease.release()

    assert governor._states["prepared-finalizer-ack-disarms-primary-before"].in_flight == 0
    assert governor._active_leases[lease._lease_id].resource_released is True
    assert lease._state == "active"

    lease.release()
    assert governor._states["prepared-finalizer-ack-disarms-primary-before"].in_flight == 0
    assert lease._lease_id not in governor._active_leases
    assert lease._state == "released"

    # Do not leave the per-key control owner live in the process-global budget.
    state = governor._states["prepared-finalizer-ack-disarms-primary-before"]
    assert flaky(state.control_ticket)
    state.control_ticket = None


def test_cross_process_exact_stale_authority_fails_closed() -> None:
    from schema_sanitizer.core_impl.cross_process_memory import _ProcessCrossMemoryCoordinator

    coordinator = _ProcessCrossMemoryCoordinator(16 << 20)
    reservation = coordinator.acquire(64)
    token = reservation._token
    capability = reservation._capability
    original = reservation.reserved_bytes

    with pytest.raises(RuntimeError, match="not authoritative"):
        coordinator.resize(token, id(reservation) + 1, capability, original + 1)
    assert reservation.reserved_bytes == original
    assert coordinator._contributions[token] == original

    with pytest.raises(RuntimeError, match="not authoritative"):
        coordinator.release(
            token,
            id(reservation) + 1,
            capability,
            nonblocking=False,
        )
    assert token in coordinator._contributions
    reservation.release()
    assert token not in coordinator._contributions


def test_cross_process_finalizer_ticket_failure_transfers_ack_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl.cross_process_memory import _ProcessCrossMemoryCoordinator

    coordinator = _ProcessCrossMemoryCoordinator(16 << 20)
    escrow = coordinator._finalizer_releases
    ticket = escrow.reserve_ticket()
    assert ticket is not None
    before = escrow.published_count()
    _fail_first_release_for(monkeypatch, escrow)

    assert coordinator.release_finalizer_ticket(ticket)
    assert escrow.published_count() == before + 1
    coordinator.reconcile_pending()
    assert escrow.published_count() == before


def test_stage_construction_ticket_failure_transfers_ack_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import memory_budget as module

    escrow = module._STAGE_ADMISSION_CONSTRUCTION_ESCROW
    ticket = escrow.reserve_ticket()
    assert ticket is not None
    before = escrow.published_count()
    _fail_first_release_for(monkeypatch, escrow)

    assert module._retire_stage_admission_construction_ticket(ticket)
    assert escrow.published_count() == before + 1
    module.drain_abandoned_memory_finalizers()
    assert escrow.published_count() == before


def test_path_claim_capacity_rollback_never_decrements_uncommitted_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import path_identity as module

    baseline = module._PATH_CLAIM_ADMISSIONS
    escrow = module._PATH_CLAIM_FINALIZER_ESCROW
    before = escrow.published_count()
    monkeypatch.setattr(module, "_MAX_LIVE_PATH_CLAIMS", baseline)
    _fail_first_release_for(monkeypatch, escrow)

    with pytest.raises(OSError, match="path-claim capacity exhausted"):
        module._acquire_path_claim_admission()

    assert module._PATH_CLAIM_ADMISSIONS == baseline
    assert escrow.published_count() == before + 1
    module._drain_path_claim_finalizers(limit=1)
    assert escrow.published_count() == before


def test_quiescence_counters_do_not_use_saturating_decrements() -> None:
    sources = {
        "janitor": SRC / "core_impl" / "temporary_janitor.py",
        "prefetch": SRC / "api_impl" / "source_plan" / "remote.py",
        "cleanup": SRC / "core_impl" / "cleanup_dispatcher.py",
        "retry": SRC / "core_impl" / "retry_scheduler.py",
        "provider_pool": SRC / "remote_impl" / "provider_session_pool.py",
        "remote_io": SRC / "remote_impl" / "io_coordinator.py",
    }
    text = {name: path.read_text(encoding="utf-8") for name, path in sources.items()}
    assert "self._quarantine_inflight = max(0" not in text["janitor"]
    assert "self._admissions_inflight = max(0" not in text["prefetch"]
    assert "self._cleanup_callbacks_inflight = max(0" not in text["prefetch"]
    assert "self._workers_starting = max(0" not in text["cleanup"]
    assert "self._execution_starting = max(0" not in text["retry"]
    assert "self._active_retries = max(0" not in text["retry"]
    assert "gate.users = max(0" not in text["provider_pool"]
    assert "self._submission_callbacks_inflight = max(" not in text["remote_io"]


def test_temporary_storage_replacement_cleanup_is_outside_pool_condition() -> None:
    source = (SRC / "core_impl" / "temporary_storage.py").read_text(encoding="utf-8")
    start = source.index("    def _resize_lease(")
    end = source.index("\n    def ", start + 10)
    body = source[start:end]
    assert "entry.resize_replacement = replacement" in body
    assert "cleanup_replacement = entry.resize_replacement" in body
    cleanup = "_PROCESS_TEMPORARY_STORAGE.release_capability(cleanup_replacement)"
    assert cleanup in body
    assert body.index("cleanup_replacement = entry.resize_replacement") < body.index(cleanup)
