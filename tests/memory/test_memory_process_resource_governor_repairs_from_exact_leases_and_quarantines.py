"""Regression coverage for memory process resource governor repairs from exact leases and quarantines."""

from __future__ import annotations

from pathlib import Path

import pytest


def test_process_resource_governor_repairs_from_exact_leases_and_quarantines() -> None:
    from schema_sanitizer.core_impl.process_resources import _Governor
    from schema_sanitizer.errors import SchemaSanitizerResourceError

    governor = _Governor(3, "process-resource-governor-repairs-from-exact-governor")
    lease = governor.try_acquire_up_to(2, minimum=2)
    assert governor._in_use == 2

    governor._in_use = 0  # low-cache fault: exact lease still owns two.
    lease.shrink(1)
    assert governor._in_use == 1
    assert sum(entry.amount for entry in governor._active_leases.values()) == 1
    assert governor._corrupted is True
    with pytest.raises(SchemaSanitizerResourceError, match="admission is closed"):
        governor.try_acquire_up_to(1, minimum=1)

    # Quarantine must preserve exact cleanup authority.
    lease.release()
    assert governor._in_use == 0
    assert not governor._active_leases


def test_dynamic_control_plane_repairs_derived_mirrors_from_exact_owner_ledger() -> None:
    from schema_sanitizer.core_impl.control_plane_budget import _ProcessControlPlaneBudget

    budget = _ProcessControlPlaneBudget(include_static_baseline=False)
    budget.configure(2048)
    first = budget.reserve("process-resource-governor-repairs-from-exact", 512)
    budget._reserved = 0
    budget._active = 0

    second = budget.reserve("process-resource-governor-repairs-from-exact-second", 512)
    assert budget._reserved == 1024
    assert budget._active == 2
    assert budget._corrupted is False
    assert budget.release(first)
    assert budget.release(second)
    assert budget._reserved == 0
    assert budget._active == 0


def test_temporary_storage_low_counter_corruption_closes_admission_but_cleanup_survives(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from schema_sanitizer.core_impl import temporary_storage_governor as module
    from schema_sanitizer.errors import SchemaSanitizerResourceError

    governor = module._ProcessTemporaryStorageGovernor()
    free = module._MINIMUM_FREE_BYTES + 4096
    monkeypatch.setattr(governor, "filesystem", lambda _path: (78, tmp_path, free))
    monkeypatch.setattr(governor, "free_inodes", lambda _path: 100_000)
    monkeypatch.setattr(module, "cross_process_storage_enabled", lambda: False)

    first = governor.reserve_capability(1024, path=tmp_path, label="first")
    state = governor._states[78]
    state.reserved_bytes = 0

    with pytest.raises(SchemaSanitizerResourceError, match="quarantined"):
        governor.reserve_capability(512, path=tmp_path, label="second")
    assert state.corrupted is True
    assert state.reserved_bytes == 1024
    assert governor._protocol_violations >= 1
    assert governor.release_capability(first)
    assert state.reserved_bytes == 0


def test_cleanup_dispatcher_exact_index_quarantines_low_admission_cache() -> None:
    from schema_sanitizer.core_impl.cleanup_dispatcher import _CleanupDispatcher

    dispatcher = _CleanupDispatcher()
    assert dispatcher.submit(lambda: None, start_worker=False)
    assert len(dispatcher._owned_index) == 1
    dispatcher._owned_calls = 0
    dispatcher._owned_bytes = 0

    assert not dispatcher.submit(lambda: None, start_worker=False)
    assert dispatcher._corrupted is True
    assert dispatcher._circuit_open is True
    assert dispatcher._owned_calls == 1
    assert dispatcher._owned_bytes > 0
    assert len(dispatcher._owned_index) == 1


def test_release_guardian_never_turns_low_counter_corruption_into_headroom(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl.retry_scheduler import _ReleaseGuardian

    class Owner:
        def release(self) -> None:
            return None

    guardian = _ReleaseGuardian()
    monkeypatch.setattr(guardian, "_ensure_workers", lambda: None)
    first = Owner()
    assert guardian.adopt(first, retained_bytes=512)
    guardian._retained_bytes = 0

    assert not guardian.adopt(Owner(), retained_bytes=512)
    assert guardian._corrupted is True
    assert guardian._circuit_open is True
    assert guardian._retained_bytes == 512
    assert len(guardian._items) == 1


def test_retry_scheduler_rebuilds_admission_from_exact_owner_mappings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl.retry_scheduler import _RetryScheduler

    scheduler = _RetryScheduler()
    monkeypatch.setattr(scheduler, "_ensure_workers", lambda: None)
    assert scheduler.schedule(
        ("process-resource-governor-repairs-from-exact", "a"),
        lambda: None,
        delay_seconds=60,
        retained_bytes=512,
    )
    scheduler._pending_bytes = 0
    scheduler._subsystem_bytes.clear()

    assert not scheduler.schedule(
        ("process-resource-governor-repairs-from-exact", "b"),
        lambda: None,
        delay_seconds=60,
        retained_bytes=512,
    )
    assert scheduler._admission_corrupted is True
    assert scheduler._admission_paused is True
    assert scheduler._pending_bytes == 512
    assert scheduler._subsystem_bytes


def test_cross_process_storage_release_commits_local_authority_before_fallible_tail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import cross_process_storage as module

    account = module.open_cross_process_storage_account(7801)
    module.reserve_cross_process_account(
        account,
        32,
        128,
        enabled=False,
    )

    def fail_release(*_args, **_kwargs):
        raise KeyboardInterrupt("injected post-authority cleanup fault")

    # Once local exact authority commits, the stale host side is conservative
    # debt. Preserve the primary async exception, but a caller retry cannot debit
    # the old amount because local exact authority has already moved to zero.
    monkeypatch.setattr(module, "_release_cross_process_raw", fail_release)
    with pytest.raises(KeyboardInterrupt, match="injected post-authority cleanup fault"):
        module.release_cross_process_account(account, 32, enabled=False)
    assert account.reserved_bytes == 0
    assert account.reconciliation_pending is True
    assert account.reconciliation_failures == 1
    module.close_cross_process_storage_account(account)
    assert account.closed is True


def test_fork_quarantine_is_generation_scoped_and_does_not_dedupe_same_label(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import fork_safety

    monkeypatch.setattr(
        fork_safety,
        "_FORK_LABELS",
        [None] * (fork_safety._MAX_FORK_CAPSULE_ENTRIES * 2),
    )
    monkeypatch.setattr(
        fork_safety,
        "_FORK_OWNERS",
        [None] * (fork_safety._MAX_FORK_CAPSULE_ENTRIES * 2 * fork_safety._MAX_INLINE_OWNERS),
    )
    monkeypatch.setattr(fork_safety, "_FORK_CAPSULE_COUNTS", [0, 0])
    monkeypatch.setattr(fork_safety, "_FORK_CAPSULE_COUNT", 0)

    owner_a = object()
    owner_b = object()
    monkeypatch.setattr(fork_safety, "_FORK_GENERATION", 1)
    assert fork_safety.quarantine_inherited_state("same-label", owner_a)
    monkeypatch.setattr(fork_safety, "_FORK_GENERATION", 2)
    assert fork_safety.quarantine_inherited_state("same-label", owner_b)

    second_index = fork_safety._MAX_FORK_CAPSULE_ENTRIES * fork_safety._MAX_INLINE_OWNERS
    assert fork_safety._FORK_OWNERS[0] is owner_a
    assert fork_safety._FORK_OWNERS[second_index] is owner_b
    assert fork_safety._FORK_CAPSULE_COUNTS == [1, 1]
    assert fork_safety._FORK_CAPSULE_COUNT == 2


def test_temporary_storage_finishes_exact_local_commit_before_propagating_cross_tail_fault(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from schema_sanitizer.core_impl import cross_process_storage
    from schema_sanitizer.core_impl import temporary_storage_governor as module

    governor = module._ProcessTemporaryStorageGovernor()
    free = module._MINIMUM_FREE_BYTES + 4096
    monkeypatch.setattr(governor, "filesystem", lambda _path: (7803, tmp_path, free))
    monkeypatch.setattr(governor, "free_inodes", lambda _path: 100_000)
    monkeypatch.setattr(module, "cross_process_storage_enabled", lambda: True)
    monkeypatch.setattr(module, "cross_process_storage_directory", lambda: tmp_path)

    capability = governor.reserve_capability(128, path=tmp_path, label="cross")
    state = governor._states[7803]
    account = state.cross_account
    assert account is not None

    def fail_release(*_args, **_kwargs):
        raise KeyboardInterrupt("injected cross tail")

    monkeypatch.setattr(cross_process_storage, "_release_cross_process_raw", fail_release)
    with pytest.raises(KeyboardInterrupt, match="injected cross tail"):
        governor.release_capability(capability)

    assert capability.active is False
    assert state.reserved_bytes == 0
    assert account.reserved_bytes == 0
    # Returning the now-idle device immediately retries reconciliation and can
    # retire the exact account; the original KeyboardInterrupt is still propagated.
    assert account.closed is True
    assert account.reconciliation_pending is False
    assert account.reconciliation_failures == 1


def test_native_amount_permit_underflow_poison_is_sticky_and_fail_closed() -> None:
    root = Path(__file__).resolve().parents[2]
    source = (root / "cpp/src/internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")

    assert "g_process_thread_permit_corrupted" in source
    assert "g_process_file_descriptor_permit_corrupted" in source
    assert "TakePermitDomainOrQuarantine" in source

    thread_acquire = source[
        source.index("TryAcquireProcessThreadPermitsUpTo") : source.index(
            "TryAcquireProcessPhysicalThreadPermitsUpTo"
        )
    ]
    assert "g_process_thread_permit_corrupted.load" in thread_acquire
    assert "commit_domain(granted)" in thread_acquire
    assert "const bool poisoned" in thread_acquire
    assert "terminal debt" in thread_acquire

    thread_release = source[
        source.index("release_process_physical_thread_permits(") : source.index(
            "acquire_process_external_runtime_thread_permits("
        )
    ]
    assert "TakePermitDomainOrQuarantine" in thread_release

    fd_acquire = source[
        source.index("TryAcquireProcessFdPermitsUpTo") : source.index(
            "ConfiguredProcessThreadCapacity"
        )
    ]
    assert "g_process_file_descriptor_permit_corrupted.load" in fd_acquire
    assert "Retain it as conservative terminal debt" in fd_acquire
    fd_release = source[
        source.index("void release_process_file_descriptor_permits") : source.index(
            "void mark_process_file_descriptors_opened"
        )
    ]
    assert "TakePermitDomainOrQuarantine" in fd_release
    assert "g_process_file_descriptor_protocol_violations" in fd_release


def test_cross_process_storage_reconciliation_preserves_same_device_sibling_account(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from schema_sanitizer.core_impl import cross_process_storage as module

    if module.fcntl is None:
        pytest.skip("cross-process coordination requires POSIX flock")
    monkeypatch.setenv(module._ENV_ENABLED, "1")
    monkeypatch.setenv(module._ENV_DIRECTORY, str(tmp_path))
    first = module.open_cross_process_storage_account(7803)
    second = module.open_cross_process_storage_account(7803)
    module.reserve_cross_process_account(first, 30, 100)
    module.reserve_cross_process_account(second, 40, 100)
    assert module.cross_process_reserved_bytes(7803) == 70

    def fail_release(*_args, **_kwargs):
        raise KeyboardInterrupt("injected before shared release")

    real_release = module._release_cross_process_raw
    monkeypatch.setattr(module, "_release_cross_process_raw", fail_release)
    with pytest.raises(KeyboardInterrupt, match="before shared release"):
        module.release_cross_process_account(first, 30)
    assert first.reserved_bytes == 0
    assert second.reserved_bytes == 40
    assert first.reconciliation_pending is True
    monkeypatch.setattr(module, "_release_cross_process_raw", real_release)
    assert module.cross_process_reserved_bytes(7803) == 70

    # A zero growth is a safe point: recovery must reconcile the process+device
    # journal record to the aggregate authority of *both* local accounts, not to
    # the recovering account's zero balance.
    module.reserve_cross_process_account(first, 0, 100)
    assert first.reconciliation_pending is False
    assert module.cross_process_reserved_bytes(7803) == 40
    assert second.reserved_bytes == 40

    module.release_cross_process_account(second, 40)
    assert module.cross_process_reserved_bytes(7803) == 0
    module.close_cross_process_storage_account(first)
    module.close_cross_process_storage_account(second)
