"""Exercises external-runtime construction failures, parent and standalone claims,
alignment escrow, setter observability, generation low-water marks, route profiles,
Parquet lifetime, and native completion corruption. Construction rolls back both logical
and physical sides exactly; pool configuration fails conservatively while partial width
and route identity remain supported."""

from __future__ import annotations

import os
import threading
from pathlib import Path

import pytest
from _support.synchronization import (
    SCHEDULER_TIMEOUT_SECONDS,
    join_thread_or_fail,
    run_isolated_python_probe,
    wait_event_or_fail,
    wait_for_process_exit,
)

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src" / "schema_sanitizer"
CPP = ROOT / "cpp" / "src"


def _reset_external_pool_state(module, monkeypatch: pytest.MonkeyPatch) -> None:
    """Install an empty synthetic registry without discarding resident runtimes."""
    module.drain_finalizer_cleanup()
    with module._EXTERNAL_RUNTIME_POOL_COORDINATOR_LOCK:
        assert module._EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS == 0
        assert module._EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS == 0
        assert all(
            not entry.physical_claims and not entry.logical_claims and not entry.config_inflight
            for entry in module._EXTERNAL_RUNTIME_POOL_COORDINATOR.values()
        )
    monkeypatch.setattr(
        module,
        "_EXTERNAL_RUNTIME_POOL_COORDINATOR",
        module._ExternalRuntimeCoordinator(),
    )


def test_external_runtime_construction_native_exception_rolls_back_standalone_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify external runtime construction native exception rolls back standalone claim."""
    from schema_sanitizer.core_impl import concurrency_contracts
    from schema_sanitizer.core_impl import process_resources as module

    _reset_external_pool_state(module, monkeypatch)
    governor = module._Governor(
        8, "external-runtime-construction-native-exception-rolls_constructor_native_failure"
    )
    logical_lease_ids: list[int] = []

    class Runtime:
        value = 8

        @classmethod
        def cpu_count(cls) -> int:
            """Return the controlled CPU count reported by the runtime."""
            return cls.value

        @classmethod
        def set_cpu_count(cls, value: int) -> None:
            """Record the CPU count selected by the controlled runtime."""
            cls.value = value

    class Native:
        def acquire_exact_permit_lease(self, desired: int, minimum: int):
            """Acquire the fake exact-permit lease requested by the resource owner."""
            del minimum
            with governor._condition:
                logical_lease_ids.extend(
                    lease_id
                    for lease_id, entry in governor._active_leases.items()
                    if entry.amount == desired
                )
            raise RuntimeError("injected native acquisition failure")

    monkeypatch.setattr(module, "_THREAD_GOVERNOR", governor)
    monkeypatch.setattr(module, "_refresh_thread_governor_capacity", lambda: None)
    monkeypatch.setattr(module, "_native_external_thread_api", lambda: Native())
    monkeypatch.setattr(concurrency_contracts, "current_runtime_execution_lease", lambda: None)

    with pytest.raises(RuntimeError, match="injected native acquisition failure"):
        module.acquire_external_runtime_threads(8, allow_parallel=True, runtime=Runtime)
    # The cleanup queue may drain synchronously; peak usage proves the logical
    # claim committed before the injected native failure.
    assert governor.snapshot().peak_in_use == 8
    assert len(logical_lease_ids) == 1
    module.drain_finalizer_cleanup()
    with governor._condition:
        assert logical_lease_ids[0] not in governor._active_leases
    assert module.external_runtime_pool_snapshot()["coordinator_entries"] == 0


def test_external_runtime_construction_native_exception_rolls_back_parent_borrow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify external runtime construction native exception rolls back parent borrow."""
    from schema_sanitizer.core_impl import concurrency_contracts
    from schema_sanitizer.core_impl import process_resources as module

    _reset_external_pool_state(module, monkeypatch)
    governor = module._Governor(
        12, "external-runtime-construction-native-exception-rolls_constructor_borrow_failure"
    )
    operation = governor.try_acquire_up_to(9, minimum=9)

    class Runtime:
        value = 8

        @classmethod
        def cpu_count(cls) -> int:
            """Return the controlled CPU count reported by the runtime."""
            return cls.value

        @classmethod
        def set_cpu_count(cls, value: int) -> None:
            """Record the CPU count selected by the controlled runtime."""
            cls.value = value

    class Native:
        def acquire_exact_permit_lease(self, desired: int, minimum: int):
            """Acquire the fake exact-permit lease requested by the resource owner."""
            del desired, minimum
            raise RuntimeError("injected borrowed native failure")

    monkeypatch.setattr(module, "_THREAD_GOVERNOR", governor)
    monkeypatch.setattr(module, "_native_external_thread_api", lambda: Native())
    monkeypatch.setattr(concurrency_contracts, "current_runtime_execution_lease", lambda: operation)

    with pytest.raises(RuntimeError, match="injected borrowed native failure"):
        module.acquire_external_runtime_threads(8, allow_parallel=True, runtime=Runtime)
    budget = operation.__dict__["_external_runtime_borrow_budget"]
    assert budget.borrowed == 8
    module.drain_finalizer_cleanup()
    assert budget.borrowed == 0
    operation.release()
    # Finalizer draining can start an unrelated cleanup worker while the test
    # governor is installed globally.  The exact parent capability, rather than
    # aggregate process usage, is the authority this rollback must retire.
    with governor._condition:
        assert operation.lease_id not in governor._active_leases


def test_external_runtime_construction_alignment_failure_escrows_both_sides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify external runtime construction alignment failure escrows both sides."""
    from schema_sanitizer.core_impl import concurrency_contracts
    from schema_sanitizer.core_impl import process_resources as module

    _reset_external_pool_state(module, monkeypatch)
    events: list[tuple[str, int]] = []

    class Runtime:
        value = 8

        @classmethod
        def cpu_count(cls) -> int:
            """Return the controlled CPU count reported by the runtime."""
            return cls.value

        @classmethod
        def set_cpu_count(cls, value: int) -> None:
            """Record the CPU count selected by the controlled runtime."""
            cls.value = value

    class Logical:
        amount = 8

        def shrink(self, amount: int) -> None:
            """Raise the deliberate failure for the shrink path."""
            raise RuntimeError("injected logical alignment failure")

        def release(self) -> None:
            """Release the resource held by the logical test double."""
            events.append(("logical-release", self.amount))
            self.amount = 0

    class Native:
        class Receipt:
            amount = 4

        def acquire_exact_permit_lease(self, desired: int, minimum: int):
            """Acquire the fake exact-permit lease requested by the resource owner."""
            del minimum
            events.append(("native-acquire", desired))
            return self.Receipt(), 4

        def exact_permit_lease_amount(self, receipt: Receipt) -> int:
            """Return the exact permit amount tracked by the fake lease."""
            return receipt.amount

        def resize_exact_permit_lease(self, receipt: Receipt, target: int) -> int:
            """Resize the fake exact-permit lease to the requested amount."""
            events.append(("native-release", receipt.amount - target))
            receipt.amount = target
            return target

    logical = Logical()
    monkeypatch.setattr(
        module, "_acquire_shared_external_logical_thread_lease", lambda *a, **k: logical
    )
    monkeypatch.setattr(module, "_native_external_thread_api", lambda: Native())
    monkeypatch.setattr(concurrency_contracts, "current_runtime_execution_lease", lambda: None)

    with pytest.raises(RuntimeError, match="logical alignment failure"):
        module.acquire_external_runtime_threads(8, allow_parallel=True, runtime=Runtime)
    module.drain_finalizer_cleanup()
    assert ("native-release", 4) in events
    assert ("logical-release", 8) in events
    assert module.external_runtime_pool_snapshot()["physical_permits"] == 0


def test_constrain_external_pool_fails_closed_on_unobservable_or_ignored_setter() -> None:
    """Verify constrain external pool fails closed on unobservable or ignored setter."""
    from schema_sanitizer.core_impl import process_resources as module
    from schema_sanitizer.errors import SchemaSanitizerResourceError

    class BrokenGetter:
        @staticmethod
        def cpu_count() -> int:
            """Raise the deliberate failure for the CPU count path."""
            raise RuntimeError("cannot inspect")

        @staticmethod
        def set_cpu_count(value: int) -> None:
            """Record the CPU count selected by the controlled runtime."""
            pass

    with pytest.raises(SchemaSanitizerResourceError, match="could not be observed"):
        module.constrain_external_runtime_worker_pool(BrokenGetter, 4)

    class IgnoredSetter:
        value = 16

        @classmethod
        def cpu_count(cls) -> int:
            """Return the controlled CPU count reported by the runtime."""
            return cls.value

        @classmethod
        def set_cpu_count(cls, value: int) -> None:
            """Record the CPU count selected by the controlled runtime."""
            pass

    with pytest.raises(SchemaSanitizerResourceError, match="exceeds admitted physical width"):
        module.constrain_external_runtime_worker_pool(IgnoredSetter, 4)


def test_fresh_runtime_generation_can_reexpand_after_prior_low_water_mark(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify fresh runtime generation can reexpand after prior low water mark."""
    from schema_sanitizer.core_impl import concurrency_contracts
    from schema_sanitizer.core_impl import process_resources as module

    _reset_external_pool_state(module, monkeypatch)
    governor = module._Governor(
        16, "external-runtime-construction-native-exception-rolls_reexpand_generation"
    )
    native_calls: list[tuple[str, int, int]] = []

    class Runtime:
        value = 8

        @classmethod
        def cpu_count(cls) -> int:
            """Return the controlled CPU count reported by the runtime."""
            return cls.value

        @classmethod
        def set_cpu_count(cls, value: int) -> None:
            """Record the CPU count selected by the controlled runtime."""
            cls.value = value

    class Native:
        class Receipt:
            def __init__(self, amount: int) -> None:
                """Initialize the receipt test double."""
                self.amount = amount

        def acquire_exact_permit_lease(self, desired: int, minimum: int):
            """Acquire the fake exact-permit lease requested by the resource owner."""
            native_calls.append(("acquire", desired, minimum))
            return self.Receipt(desired), desired

        def exact_permit_lease_amount(self, receipt: Receipt) -> int:
            """Return the exact permit amount tracked by the fake lease."""
            return receipt.amount

        def resize_exact_permit_lease(self, receipt: Receipt, target: int) -> int:
            """Resize the fake exact-permit lease to the requested amount."""
            native_calls.append(("release", receipt.amount - target, receipt.amount - target))
            receipt.amount = target
            return target

    monkeypatch.setattr(module, "_THREAD_GOVERNOR", governor)
    monkeypatch.setattr(module, "_refresh_thread_governor_capacity", lambda: None)
    monkeypatch.setattr(module, "_native_external_thread_api", lambda: Native())
    monkeypatch.setattr(concurrency_contracts, "current_runtime_execution_lease", lambda: None)

    first = module.acquire_external_runtime_threads(8, allow_parallel=True, runtime=Runtime)
    assert module.constrain_external_runtime_worker_pool(Runtime, 2) == 2
    first.shrink_to(2)
    first_claim = first._lease
    assert first_claim is not None
    with module._EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION:
        first_pool_entry = module._EXTERNAL_RUNTIME_POOL_COORDINATOR[first_claim._runtime_id]
        first_owner = first_pool_entry.logical_lease
        assert first_owner is not None
    first_owner_lease_id = first_owner.lease_id
    with governor._condition:
        first_owner_entry = governor._active_leases[first_owner_lease_id]
        assert first_owner_entry.owner_id == id(first_owner)
        assert first_owner_entry.capability is first_owner._capability
        assert first_owner_entry.amount == 2
        assert not first_owner_entry.resource_released
    first.close()
    with governor._condition:
        assert first_owner_lease_id not in governor._active_leases
    assert Runtime.value == 2
    assert module.external_runtime_pool_snapshot()["coordinator_entries"] == 0

    second = module.acquire_external_runtime_threads(8, allow_parallel=True, runtime=Runtime)
    assert second.workers == 8
    assert module.constrain_external_runtime_worker_pool(Runtime, second.workers) == 8
    assert Runtime.value == 8
    second.close()


def test_configurable_standalone_runtime_degrades_partially_not_to_serial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify configurable standalone runtime degrades partially not to serial."""
    from schema_sanitizer.core_impl import concurrency_contracts
    from schema_sanitizer.core_impl import process_resources as module

    _reset_external_pool_state(module, monkeypatch)
    governor = module._Governor(
        4, "external-runtime-construction-native-exception-rolls_partial_external"
    )

    class Runtime:
        value = 16

        @classmethod
        def cpu_count(cls) -> int:
            """Return the controlled CPU count reported by the runtime."""
            return cls.value

        @classmethod
        def set_cpu_count(cls, value: int) -> None:
            """Record the CPU count selected by the controlled runtime."""
            cls.value = value

    class Native:
        class Receipt:
            amount = 4

        def acquire_exact_permit_lease(self, desired: int, minimum: int):
            """Acquire the fake exact-permit lease requested by the resource owner."""
            assert (desired, minimum) == (4, 2)
            return self.Receipt(), 4

        def exact_permit_lease_amount(self, receipt: Receipt) -> int:
            """Return the exact permit amount tracked by the fake lease."""
            return receipt.amount

        def resize_exact_permit_lease(self, receipt: Receipt, target: int) -> int:
            """Resize the fake exact-permit lease to the requested amount."""
            receipt.amount = target
            return target

    monkeypatch.setattr(module, "_THREAD_GOVERNOR", governor)
    monkeypatch.setattr(module, "_refresh_thread_governor_capacity", lambda: None)
    monkeypatch.setattr(module, "_native_external_thread_api", lambda: Native())
    monkeypatch.setattr(concurrency_contracts, "current_runtime_execution_lease", lambda: None)

    lease = module.acquire_external_runtime_threads(8, allow_parallel=True, runtime=Runtime)
    assert lease.parallel is True
    assert lease.workers == 4
    logical_claim = lease._lease
    assert logical_claim is not None
    with module._EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION:
        pool_entry = module._EXTERNAL_RUNTIME_POOL_COORDINATOR[logical_claim._runtime_id]
        logical_owner = pool_entry.logical_lease
        assert logical_owner is not None
    logical_owner_lease_id = logical_owner.lease_id
    with governor._condition:
        logical_owner_entry = governor._active_leases[logical_owner_lease_id]
        assert logical_owner_entry.owner_id == id(logical_owner)
        assert logical_owner_entry.capability is logical_owner._capability
        assert logical_owner_entry.amount == 4
        assert not logical_owner_entry.resource_released
    assert module.constrain_external_runtime_worker_pool(Runtime, lease.workers) == 4
    lease.close()
    with governor._condition:
        assert logical_owner_lease_id not in governor._active_leases


def _probe_parquet_dataset_lifetime_lease_after_fork() -> None:
    """Exercise the inherited lifetime lock in a disposable process."""
    from schema_sanitizer.adapters.parquet import record_batch_factory as module

    class Capability:
        def close(self) -> None:
            """Close the resources owned by the capability test double."""
            pass

    owner = module._DatasetLifetimeOwner(object(), Capability(), None)
    lease = owner.acquire()
    owner.close()  # retain only stream lease
    held = threading.Event()
    release = threading.Event()

    def hold() -> None:
        """Hold the synchronization point until the competing path arrives."""
        with lease._lock:
            held.set()
            wait_event_or_fail(release)

    thread = threading.Thread(target=hold)
    thread.start()
    assert held.wait(SCHEDULER_TIMEOUT_SECONDS)
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:  # pragma: no cover
        try:
            os.close(read_fd)
            try:
                lease.close()
            except RuntimeError as exc:
                os.write(write_fd, ("ok:" + str(exc)).encode())
                os._exit(0)
            os.write(write_fd, b"unexpected-success")
            os._exit(2)
        except BaseException as exc:
            os.write(write_fd, ("child-error:" + repr(exc)).encode())
            os._exit(3)

    os.close(write_fd)
    try:
        status = wait_for_process_exit(pid)
        message = os.read(read_fd, 4096).decode()
    finally:
        os.close(read_fd)
        release.set()
        join_thread_or_fail(thread)
        lease.close()
    assert os.waitstatus_to_exitcode(status) == 0
    assert message.startswith("ok:")
    assert "cannot be reused after fork" in message


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
def test_parquet_dataset_lifetime_lease_fails_before_inherited_lock_after_fork() -> None:
    """Verify Parquet dataset lifetime lease fails before inherited lock after fork."""
    run_isolated_python_probe(__file__, "_probe_parquet_dataset_lifetime_lease_after_fork")


def test_route_profiles_are_orthogonal_and_payload_contract_backed() -> None:
    """Verify route profiles are orthogonal and payload contract backed."""
    from schema_sanitizer.core_impl import concurrency_contracts as contracts
    from schema_sanitizer.core_impl.concurrency_route_evidence import (
        INPUT_ROUTE_PROFILE_REQUIREMENTS,
        OUTPUT_ROUTE_PROFILE_REQUIREMENTS,
    )

    assert set(INPUT_ROUTE_PROFILE_REQUIREMENTS) == {
        "local_path",
        "remote_chunks",
        "directory_source_plan",
        "materialized_memory",
        "python_iterator",
        "staged_remote",
    }
    assert set(OUTPUT_ROUTE_PROFILE_REQUIREMENTS) == {
        "local_file",
        "remote_staged_commit",
        "stream",
        "analytical_adapter",
    }
    registered = contracts.runtime_concurrency_contracts()
    assert registered
    contract_name = next(iter(registered))
    pair_token = contracts.activate_runtime_concurrency_pair("csv", "csv")
    route_token = contracts._CURRENT_ROUTE_PROFILES.set(("local_path", "local_file"))
    try:
        contracts.observe_runtime_concurrency_contract(contract_name)
    finally:
        contracts._CURRENT_ROUTE_PROFILES.reset(route_token)
        contracts.reset_runtime_concurrency_pair(pair_token)
    observed = contracts.runtime_route_profile_contract_observations()
    assert observed["local_path"][contract_name] >= 1
    assert observed["local_file"][contract_name] >= 1
    source = (SRC / "core_impl/concurrency_coverage.py").read_text(encoding="utf-8")
    assert "validate_route_profile_runtime_contracts()" in source
    assert "validate_safety_critical_runtime_contracts()" in source
    contracts_source = (SRC / "core_impl/concurrency_contracts.py").read_text(encoding="utf-8")
    assert "and self.route_token is None" in contracts_source


def test_route_profile_classifier_covers_transport_and_lifetime_shapes() -> None:
    """Verify route profile classifier covers transport and lifetime shapes."""
    from types import SimpleNamespace

    from schema_sanitizer.core_impl.concurrency_route_evidence import (
        analytical_output_route_profile,
        input_route_profile,
        output_file_route_profile,
    )
    from schema_sanitizer.input_impl.prepared import PreparedPublicInput

    assert input_route_profile(PreparedPublicInput([], "python", "python")) == "python_iterator"
    assert input_route_profile(PreparedPublicInput(b"x", "csv", "buffer")) == "materialized_memory"
    assert (
        input_route_profile(
            PreparedPublicInput("/tmp/a.csv", "csv", "file", source_file="/tmp/a.csv")
        )
        == "local_path"
    )
    assert (
        input_route_profile(
            PreparedPublicInput("/tmp/a.csv", "csv", "file", source_file="s3://bucket/a.csv")
        )
        == "staged_remote"
    )
    assert (
        input_route_profile(
            PreparedPublicInput(
                SimpleNamespace(remote_native_multisource_manifest=object()),
                "csv",
                "manifest",
                source_file="s3://bucket/prefix/",
            )
        )
        == "remote_chunks"
    )
    assert (
        input_route_profile(
            PreparedPublicInput(
                SimpleNamespace(native_multisource_manifest=object()),
                "csv",
                "manifest",
                source_file="/tmp/dir",
            )
        )
        == "directory_source_plan"
    )
    assert output_file_route_profile("/tmp/out.parquet") == "local_file"
    assert output_file_route_profile("gs://bucket/out.parquet") == "remote_staged_commit"
    assert analytical_output_route_profile("pyarrow_reader") == "stream"
    assert analytical_output_route_profile("polars") == "analytical_adapter"


def test_single_staged_parquet_preserves_remote_route_identity() -> None:
    """Verify single staged Parquet preserves remote route identity."""
    from schema_sanitizer.api_impl.input.directory_preparation import (
        prepare_single_parquet_file,
    )
    from schema_sanitizer.core_impl.concurrency_route_evidence import input_route_profile

    source_uri = "s3://bucket/input.parquet"
    prepared = prepare_single_parquet_file(
        "/tmp/schema-sanitizer-staged.parquet",
        source_file=source_uri,
        keepalive=None,
        memory_limit_bytes=64 << 20,
    )
    try:
        assert prepared.source_file == source_uri
        assert input_route_profile(prepared) == "staged_remote"
    finally:
        prepared.close()


def test_native_runtime_separates_external_pool_permits_and_surfaces_completion_corruption() -> (
    None
):
    """Verify native runtime separates external pool permits and surfaces completion corruption."""
    header = (CPP / "internal/runtime/operation_task_arena.hh").read_text(encoding="utf-8")
    arena = (CPP / "internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    probe = (CPP / "api/python_abi3/runtime/ordered_executor_probe.cc").read_text(encoding="utf-8")
    diagnostics = (SRC / "core_impl/runtime_diagnostics.py").read_text(encoding="utf-8")

    assert "acquire_process_external_runtime_thread_permits" in header
    assert "g_process_external_runtime_thread_permits" in arena
    assert "unaccounted_external" in arena
    assert "g_completion_memory_protocol_violations.fetch_add" in arena
    assert "py_process_external_runtime_thread_permit_lease_acquire" in probe
    assert "completion_memory_protocol_violations" in diagnostics
    assert "if len(values) != 30" in diagnostics


def test_external_runtime_uses_one_coordinator_not_split_logical_physical_ledgers() -> None:
    """Verify external runtime uses one coordinator not split logical physical ledgers."""
    source = (SRC / "core_impl/process_resources.py").read_text(encoding="utf-8")
    assert "class _ExternalRuntimePoolCoordinatorEntry" in source
    assert "_EXTERNAL_RUNTIME_POOL_COORDINATOR" in source
    assert "_EXTERNAL_RUNTIME_LOGICAL_POOLS" not in source
    assert "_EXTERNAL_RUNTIME_PHYSICAL_POOLS" not in source
    assert "class _ExternalRuntimeConstructionEscrow" in source
