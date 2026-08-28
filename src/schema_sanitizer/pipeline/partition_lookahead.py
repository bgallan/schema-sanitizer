"""Bounded one-partition source preparation for multi-mode pipelines."""

from __future__ import annotations

import os
from collections.abc import Mapping
from concurrent.futures import Future
from dataclasses import dataclass
from threading import Condition, Lock
from time import monotonic
from typing import Any, Protocol, cast

from ..api_impl.input.preparation import prepare_public_input
from ..api_impl.operation_context import OperationExecutionContext
from ..api_impl.partition_resources import BorrowedPartitionResources
from ..api_impl.source_plan.attached import remote_native_multisource_manifest_from_data
from ..core_impl.durations import normalize_duration
from ..core_impl.execution_policy import (
    execution_policy,
    threading_mode_from_multi_threading,
)
from ..core_impl.finalization import runtime_is_finalizing
from ..core_impl.finalizer_escrow import ReservedFinalizerEscrow
from ..core_impl.memory_budget import (
    acquire_stage_concurrency_admission,
    normalize_memory_limit,
)
from ..core_impl.memory_budget import (
    adaptive_parallel_slots as _adaptive_parallel_slots,
)
from ..core_impl.process_resources import acquire_project_threads
from ..core_impl.resource_lifecycle import _cleanup_with_note
from ..core_impl.rooted_finalizer import RootedFinalizerAuthority
from ..core_impl.runtime_registry import RuntimeServiceRegistration, register_runtime_service
from ..core_impl.safe_errors import add_bounded_note
from ..errors import SchemaSanitizerResourceError
from ..input_impl.directory_inputs import discovered_directory_input_context
from ..input_impl.prepared import PreparedPublicInput
from .partition_lookahead_worker import ThreadPoolExecutor as _DaemonThreadPoolExecutor
from .types import PartitionRunPlan

_LOOKAHEAD_FINALIZER_ESCROW: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(
    1024, static_kind="partition_lookahead"
)
_LOOKAHEAD_FINALIZER_OVERFLOWS = 0
_LOOKAHEAD_FINALIZER_OVERFLOWED = False


class _Closeable(Protocol):
    def close(self) -> object:
        """Close an exact speculative owner."""


class _LookaheadFuture(Protocol):
    def cancel(self) -> bool:
        """Cancel pending work when it has not started."""

    def done(self) -> bool:
        """Return whether the future reached a terminal state."""

    def result(self) -> _Closeable:
        """Return the closeable speculative result."""


class _LookaheadExecutor(Protocol):
    def shutdown(self, *, wait: bool, cancel_futures: bool) -> object:
        """Stop the speculative worker."""


def _run_partition_lookahead_finalizer(
    authority: RootedFinalizerAuthority,
) -> None:
    """Close abandoned speculative owners without retaining the controller."""
    future = cast(_LookaheadFuture | None, authority.arg0)
    future_context = cast(_Closeable | None, authority.arg1)
    if future is None:
        if future_context is not None:
            future_context.close()
            authority.arg1 = None
    else:
        if future.cancel():
            if future_context is not None:
                future_context.close()
            authority.arg0 = None
            authority.arg1 = None
        elif not future.done():
            raise RuntimeError("partition lookahead finalizer still has running work")
        else:
            try:
                prepared = future.result()
            except BaseException:
                if future_context is not None:
                    try:
                        future_context.close()
                    except BaseException:
                        # Retain both exact owners in the rooted authority so a
                        # later safe point can retry context cleanup.
                        raise
            else:
                prepared.close()
            authority.arg0 = None
            authority.arg1 = None
    executor = cast(_LookaheadExecutor | None, authority.arg2)
    if executor is not None:
        executor.shutdown(wait=True, cancel_futures=True)
        authority.arg2 = None
    registration = cast(_Closeable | None, authority.arg3)
    if registration is not None:
        registration.close()
        authority.arg3 = None


def drain_partition_lookahead_finalizers() -> int:
    """Close lookahead owners while the escrow remains authoritative."""
    progressed = 0

    def process(ticket: int, owner: object) -> None:
        nonlocal progressed
        if isinstance(owner, RootedFinalizerAuthority):
            owner.ticket = ticket
            owner.run()
            owner.clear()
            progressed += 1
            return
        if not isinstance(owner, PartitionSourceLookahead):
            return
        owner._finalizer_ticket = ticket
        owner.close()
        progressed += 1

    attempts = _LOOKAHEAD_FINALIZER_ESCROW.active_count()
    for _ in range(attempts):
        try:
            if not _LOOKAHEAD_FINALIZER_ESCROW.process_one(process):
                break
        except BaseException:
            continue
    return progressed


def partition_lookahead_finalizer_snapshot() -> tuple[int, int]:
    """Return published lookahead cleanups and finalizer overflow count."""
    return (
        _LOOKAHEAD_FINALIZER_ESCROW.published_count(),
        max(1, _LOOKAHEAD_FINALIZER_OVERFLOWS)
        if (_LOOKAHEAD_FINALIZER_OVERFLOWED or _LOOKAHEAD_FINALIZER_ESCROW.overflowed)
        else _LOOKAHEAD_FINALIZER_OVERFLOWS,
    )


def _release_parallel_admission(future: Future[Any]) -> None:
    admission = getattr(future, "_schema_sanitizer_parallel_admission", None)
    if admission is None:
        return
    try:
        delattr(future, "_schema_sanitizer_parallel_admission")
    except BaseException:
        pass
    try:
        admission.close()
    except BaseException:
        pass


@dataclass(frozen=True, slots=True)
class _PreparationOptions:
    """Input preparation settings that must match the eventual conversion."""

    input_format: str | None
    input_mode: str
    input_text_encoding: str
    xml_row_tag: str | None
    csv_delimiter: str
    csv_has_header: bool
    memory_limit_bytes: int | None
    threading_mode: str

    @classmethod
    def from_kwargs(
        cls,
        kwargs: Mapping[str, Any],
        *,
        memory_limit_bytes: int,
    ) -> _PreparationOptions:
        """Extract canonical public input settings from converter keyword arguments."""
        return cls(
            input_format=kwargs.get("input_format"),
            input_mode=str(kwargs.get("input_mode", "single_file")),
            input_text_encoding=str(kwargs.get("input_text_encoding", "utf-8")),
            xml_row_tag=kwargs.get("xml_row_tag"),
            csv_delimiter=str(kwargs.get("csv_delimiter", ",")),
            csv_has_header=bool(kwargs.get("csv_has_header", True)),
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode_from_multi_threading(
                kwargs.get("multi_threading", False)
            ),
        )


@dataclass(slots=True)
class _PreparedPartition:
    """Prepared immutable source bytes plus their timestamp-owning child context."""

    plan: PartitionRunPlan
    options: _PreparationOptions
    prepared_input: PreparedPublicInput
    operation_context: OperationExecutionContext
    allow_early_lookahead: bool

    def resources(self, trigger: Any) -> BorrowedPartitionResources:
        """Build the one-shot converter handoff for this partition."""
        return BorrowedPartitionResources(
            input_path=self.plan.source_uri,
            prepared_input=self.prepared_input,
            operation_context=self.operation_context,
            allow_early_lookahead=self.allow_early_lookahead,
            lookahead_trigger=trigger,
        )

    def close(self) -> None:
        """Close an unconsumed prepared partition."""
        primary: BaseException | None = None
        try:
            self.prepared_input.close()
        except BaseException as exc:
            primary = exc
        try:
            self.operation_context.close()
        except BaseException as exc:
            if primary is None:
                primary = exc
            else:
                add_bounded_note(
                    primary,
                    "partition operation-context cleanup also failed",
                    exc,
                )
        if primary is not None:
            raise primary


@dataclass(slots=True)
class _DeferredPartition:
    """A source whose speculative staging must be retried at its own ordinal."""

    plan: PartitionRunPlan
    options: _PreparationOptions
    operation_context: OperationExecutionContext

    def close(self) -> None:
        """Release the retained child context."""
        self.operation_context.close()


PreparedOrDeferred = _PreparedPartition | _DeferredPartition


class PartitionSourceLookahead:
    """Prepare at most partition ``N + 1`` while ``N`` converts or publishes."""

    def __init__(
        self,
        kwargs: Mapping[str, Any],
        *,
        memory_limit_bytes: int | None = None,
    ) -> None:
        """Create a lazy one-slot worker when the static pipeline policy allows it."""
        drain_partition_lookahead_finalizers()
        self._finalizer_owner = RootedFinalizerAuthority(_run_partition_lookahead_finalizer)
        self._finalizer_ticket = -1
        try:
            ticket = _LOOKAHEAD_FINALIZER_ESCROW.reserve_rooted(self._finalizer_owner)
            if ticket is None:
                raise SchemaSanitizerResourceError(
                    "partition lookahead finalizer escrow capacity exhausted",
                    detail={
                        "stage": "partition_lookahead",
                        "limit_name": "finalizer_escrow_slots",
                        "limit_items": _LOOKAHEAD_FINALIZER_ESCROW.capacity,
                        "actual_items": _LOOKAHEAD_FINALIZER_ESCROW.capacity + 1,
                    },
                )
            self._finalizer_ticket = ticket
        except BaseException:
            try:
                _LOOKAHEAD_FINALIZER_ESCROW.release_rooted_owner(self._finalizer_owner)
            except BaseException:
                pass
            raise
        # Initialize every primary owner before later construction can fail.
        self._executor: _DaemonThreadPoolExecutor | None = None
        self._future: Future[PreparedOrDeferred] | None = None
        self._future_context: OperationExecutionContext | None = None
        self._runtime_registration: RuntimeServiceRegistration | None = None
        self._closed = False
        self._kwargs = kwargs
        self._pid = os.getpid()
        self._memory_limit_bytes = (
            normalize_memory_limit(kwargs.get("memory_limit_bytes"))
            if memory_limit_bytes is None
            else memory_limit_bytes
        )
        initial_options = self._current_options()
        policy = execution_policy(
            initial_options.threading_mode,
            initial_options.memory_limit_bytes,
        )
        self.enabled = not policy.is_single and policy.effective_workers > 1
        if self.enabled:
            try:
                self._executor = _DaemonThreadPoolExecutor(
                    max_workers=1,
                    thread_name_prefix="schema-sanitizer-partition-lookahead",
                    permit_factory=acquire_project_threads,
                )
            except SchemaSanitizerResourceError:
                # Lookahead is an optimization. When the process-wide thread
                # window is already committed to conversion or remote I/O,
                # preserve correctness by preparing partitions synchronously.
                self.enabled = False
        self._armed: tuple[PartitionRunPlan, OperationExecutionContext] | None = None
        self._close_lock = Lock()
        self._close_condition = Condition(self._close_lock)
        self._close_in_progress = False
        self._submissions_inflight = 0
        self._protocol_violations = 0
        self._consumer_inflight = False
        self._close_started = False
        self._close_timeout_seconds = 30.0
        self._late_close_registered = False
        self._runtime_registration = register_runtime_service(
            self, kind="partition_lookahead", close_name="_runtime_shutdown"
        )
        self._sync_finalizer_owner()

    def _sync_finalizer_owner(self) -> None:
        """Mirror primary owners into the pre-rooted allocation-free authority."""
        owner = getattr(self, "_finalizer_owner", None)
        if not isinstance(owner, RootedFinalizerAuthority):
            return
        owner.arg0 = getattr(self, "_future", None)
        owner.arg1 = getattr(self, "_future_context", None)
        owner.arg2 = getattr(self, "_executor", None)
        owner.arg3 = getattr(self, "_runtime_registration", None)

    def _lifecycle_condition(self) -> Condition:
        """Return the lifecycle condition created with the controller."""
        return self._close_condition

    def _finish_submission_locked(self) -> None:
        """Release one lifecycle claim without hiding a double completion."""
        if self._submissions_inflight <= 0:
            self._protocol_violations += 1
            return
        self._submissions_inflight -= 1

    def _resume_close_if_quiescent(self) -> None:
        """Resume a timed-out close after the last claimed admission exits."""
        condition = self._lifecycle_condition()
        with condition:
            retry = (
                self._close_started
                and not self._close_in_progress
                and not self._closed
                and self._submissions_inflight == 0
                and self._protocol_violations == 0
            )
        if retry:
            try:
                self.close()
            except BaseException:
                # Every owner remains reachable on failure. A later explicit
                # close or finalizer can retry without masking the admission's
                # own primary result.
                pass

    def prepare_first(self, plan: PartitionRunPlan) -> _PreparedPartition:
        """Prepare the first partition inside the same close admission barrier."""
        condition = self._lifecycle_condition()
        with condition:
            if not self.enabled:
                raise RuntimeError("partition source lookahead is disabled")
            if self._close_started:
                raise RuntimeError("partition source lookahead is closing")
            next_submissions = self._submissions_inflight + 1
            self._submissions_inflight = next_submissions
        try:
            options = self._current_options()
            prepared = self._prepare_with_new_context(plan, options)
            return self._materialize_deferred(prepared, options)
        finally:
            with condition:
                self._finish_submission_locked()
                condition.notify_all()
            self._resume_close_if_quiescent()

    def arm(
        self,
        plan: PartitionRunPlan | None,
        parent_context: OperationExecutionContext,
    ) -> None:
        """Record the next partition under the lifecycle transaction."""
        condition = self._lifecycle_condition()
        with condition:
            if not self.enabled or self._close_started or plan is None:
                return
            if self._future is not None or self._armed is not None:
                raise RuntimeError("partition lookahead window already contains work")
            self._armed = (plan, parent_context)

    def trigger(self) -> None:
        """Submit one claimed partition and publish it before close can commit."""
        condition = self._lifecycle_condition()
        with condition:
            if not self.enabled or self._close_started or self._armed is None:
                return
            executor = self._executor
            if executor is None:
                raise RuntimeError("partition lookahead worker is unavailable")
            plan, parent_context = self._armed
            next_submissions = self._submissions_inflight + 1
            self._submissions_inflight = next_submissions
            self._armed = None

        child_context: OperationExecutionContext | None = None
        future: Future[PreparedOrDeferred] | None = None
        primary: BaseException | None = None
        disable = False
        try:
            options = self._current_options()
            helper_bytes = max(8 << 20, self._memory_limit_bytes // 8)
            target = _adaptive_parallel_slots(2, per_slot_bytes=helper_bytes)
            if target < 2:
                return
            # Borrow the daemon executor's physical helper slot first, then
            # attach resident bytes so no stage waits while retaining memory.
            execution_lease = executor._thread_lease
            if execution_lease is None:
                return
            admission = acquire_stage_concurrency_admission(
                target,
                per_slot_bytes=helper_bytes,
                stage="partition_lookahead_admission",
                execution_lease=execution_lease,
                require_memory=True,
                memory_ledger=parent_context.memory_ledger,
            )
            if admission.slots < 2:
                admission.close()
                return
            child_context = parent_context.fork()
            try:
                future = executor.submit(self._prepare, plan, child_context, options)
            except BaseException as submit_error:
                _cleanup_with_note(
                    submit_error,
                    admission,
                    label="partition lookahead admission rollback also failed",
                )
                raise
            setattr(future, "_schema_sanitizer_parallel_admission", admission)
            future.add_done_callback(_release_parallel_admission)
        except Exception as exc:
            primary = exc
            disable = True
        except BaseException as exc:
            primary = exc
            disable = True
        finally:
            if future is None and child_context is not None:
                try:
                    child_context.close()
                except BaseException as cleanup_error:
                    if primary is None:
                        primary = cleanup_error
                    else:
                        add_bounded_note(
                            primary,
                            "partition lookahead child-context rollback also failed",
                            cleanup_error,
                        )
                    with condition:
                        self._future_context = child_context
            with condition:
                if future is not None:
                    # Publish even after close starts: close is waiting for this
                    # admission claim and will own the just-created resources.
                    self._future = future
                    self._future_context = child_context
                if disable:
                    self.enabled = False
                self._sync_finalizer_owner()
                self._finish_submission_locked()
                condition.notify_all()

        if primary is not None:
            try:
                executor.shutdown(wait=False, cancel_futures=True)
            except BaseException as cleanup_error:
                add_bounded_note(
                    primary,
                    "partition lookahead executor rollback also failed",
                    cleanup_error,
                )
            else:
                with condition:
                    if self._executor is executor and self._future is None:
                        self._executor = None
                        self._sync_finalizer_owner()
            if isinstance(primary, Exception):
                self._resume_close_if_quiescent()
                return
            self._resume_close_if_quiescent()
            raise primary
        self._resume_close_if_quiescent()

    def take_next(self, plan: PartitionRunPlan) -> _PreparedPartition:
        """Consume one result through an exclusive lifecycle claim."""
        condition = self._lifecycle_condition()
        with condition:
            if not self.enabled:
                raise RuntimeError("partition source lookahead is disabled")
            if self._close_started:
                raise RuntimeError("partition source lookahead is closing")
            if self._consumer_inflight:
                raise RuntimeError("partition lookahead result is already being consumed")
            # Materialize the next counter before publishing either lifecycle
            # latch. A MemoryError in PyLong growth therefore leaves no phantom
            # consumer/submission claim.
            next_submissions = self._submissions_inflight + 1
            future = self._future
            future_context = self._future_context
            try:
                self._submissions_inflight = next_submissions
                self._consumer_inflight = True
                if future is not None:
                    # Transfer ownership to this consumer. close() waits for the
                    # admission claim and therefore cannot clean these resources in
                    # parallel. A failed cleanup republishes them before the claim
                    # is released.
                    self._future = None
                    self._future_context = None
            except BaseException:
                self._finish_submission_locked()
                self._consumer_inflight = False
                raise

        restore_claim = future is not None
        try:
            options = self._current_options()
            if future is None:
                prepared = self._prepare_with_new_context(plan, options)
                return self._materialize_deferred(prepared, options)
            try:
                prepared = future.result()
            except BaseException as exc:
                cleanup_failed = False
                if future_context is not None:
                    try:
                        future_context.close()
                    except BaseException as cleanup_error:
                        cleanup_failed = True
                        add_bounded_note(
                            exc,
                            "partition lookahead context cleanup also failed",
                            cleanup_error,
                        )
                if not cleanup_failed:
                    restore_claim = False
                raise
            if prepared.plan != plan:
                prepared.close()
                restore_claim = False
                raise RuntimeError("partition lookahead returned a different source ordinal")
            if prepared.options != options:
                restore_claim = False
                return self._replace_stale_preparation(prepared, options)
            materialized = self._materialize_deferred(prepared, options)
            restore_claim = False
            return materialized
        finally:
            with condition:
                if restore_claim and self._future is None:
                    self._future = future
                    self._future_context = future_context
                self._sync_finalizer_owner()
                self._consumer_inflight = False
                self._finish_submission_locked()
                condition.notify_all()
            self._resume_close_if_quiescent()

    def _current_options(self) -> _PreparationOptions:
        """Capture the live static mapping at one preparation boundary."""
        return _PreparationOptions.from_kwargs(
            self._kwargs,
            memory_limit_bytes=self._memory_limit_bytes,
        )

    def _prepare_with_new_context(
        self,
        plan: PartitionRunPlan,
        options: _PreparationOptions | None = None,
    ) -> PreparedOrDeferred:
        """Create partition metadata, prepare source bytes, and retain one packet."""
        options = self._current_options() if options is None else options
        context = OperationExecutionContext(
            threading_mode=options.threading_mode,
            memory_limit_bytes=options.memory_limit_bytes,
        )
        return self._prepare(plan, context, options)

    def _prepare(
        self,
        plan: PartitionRunPlan,
        context: OperationExecutionContext,
        options: _PreparationOptions | None = None,
    ) -> PreparedOrDeferred:
        """Prepare one partition, deferring only temporary-window contention."""
        options = self._current_options() if options is None else options
        prepared: PreparedPublicInput | None = None
        try:
            with discovered_directory_input_context(plan.source_uri, plan.discovered_input):
                prepared = prepare_public_input(
                    plan.source_uri,
                    input_format=options.input_format,
                    input_mode=options.input_mode,
                    input_text_encoding=options.input_text_encoding,
                    xml_row_tag=options.xml_row_tag,
                    csv_delimiter=options.csv_delimiter,
                    csv_has_header=options.csv_has_header,
                    memory_limit_bytes=options.memory_limit_bytes,
                    threading_mode=options.threading_mode,
                    operation_context=context,
                )
            remote_manifest = remote_native_multisource_manifest_from_data(prepared.data)
            if remote_manifest is not None:
                remote_manifest.prefetch_first_chunk()
            return _PreparedPartition(
                plan=plan,
                options=options,
                prepared_input=prepared,
                operation_context=context,
                allow_early_lookahead=remote_manifest is None,
            )
        except SchemaSanitizerResourceError as exc:
            if prepared is not None:
                prepared.close()
            if self._is_temporary_window_contention(exc, context):
                return _DeferredPartition(plan, options, context)
            context.close()
            raise
        except BaseException as exc:
            if prepared is not None:
                _cleanup_with_note(
                    exc,
                    prepared,
                    label="partition prepared-input cleanup also failed",
                )
            _cleanup_with_note(
                exc,
                context,
                label="partition operation-context cleanup also failed",
            )
            raise

    def _replace_stale_preparation(
        self, value: PreparedOrDeferred, options: _PreparationOptions
    ) -> _PreparedPartition:
        """Reprepare stale options without opening a second resource domain.

        The speculative context already shares the parent operation's physical
        thread and memory capabilities. Fork it before retiring the stale
        packet so the replacement retains those exact credits instead of
        transiently competing with its predecessor for a new project-thread
        lease under a tight process cap.
        """
        replacement_context = value.operation_context.fork()
        try:
            value.close()
        except BaseException as exc:
            _cleanup_with_note(
                exc,
                replacement_context,
                label="stale lookahead replacement-context rollback also failed",
            )
            raise
        prepared = self._prepare(value.plan, replacement_context, options)
        return self._materialize_deferred(prepared, options)

    def _materialize_deferred(
        self,
        value: PreparedOrDeferred,
        options: _PreparationOptions | None = None,
    ) -> _PreparedPartition:
        """Retry a capacity-only miss after the preceding partition has drained."""
        options = self._current_options() if options is None else options
        if isinstance(value, _PreparedPartition):
            if value.options == options:
                return value
            return self._replace_stale_preparation(value, options)
        if value.options != options:
            return self._replace_stale_preparation(value, options)
        prepared = self._prepare(value.plan, value.operation_context, options)
        if isinstance(prepared, _DeferredPartition):
            prepared.close()
            raise RuntimeError(
                "partition source preparation remained deferred at its execution ordinal"
            )
        return prepared

    @staticmethod
    def _is_temporary_window_contention(
        exc: SchemaSanitizerResourceError,
        context: OperationExecutionContext,
    ) -> bool:
        """Return whether earlier artifacts alone caused a retryable permit miss."""
        detail = exc.detail or {}
        return (
            detail.get("limit_name") == "temporary_storage_bytes"
            and "window exhausted" in str(exc)
            and context.temporary_storage.snapshot().reserved_bytes > 0
        )

    def _late_future_completed(self, _future: Future[Any]) -> None:
        """Resume a deferred close without reentering a live close transaction."""
        condition = self._lifecycle_condition()
        with condition:
            self._late_close_registered = False
            if self._close_in_progress:
                condition.notify_all()
                return
        try:
            self.close()
        except BaseException:
            # Cleanup ownership remains published for an explicit/finalizer
            # retry; Future callbacks must not raise into executor internals.
            pass

    def _release_finalizer_ticket(self) -> None:
        ticket = getattr(self, "_finalizer_ticket", -1)
        owner = getattr(self, "_finalizer_owner", None)
        if type(ticket) is int and ticket >= 0:
            if isinstance(owner, RootedFinalizerAuthority):
                owner.make_ack_only()
            try:
                retired = _LOOKAHEAD_FINALIZER_ESCROW.release_ticket(ticket)
            except BaseException:
                retired = False
            if retired:
                self._finalizer_ticket = -1
                if isinstance(owner, RootedFinalizerAuthority):
                    owner.clear()
            elif isinstance(owner, RootedFinalizerAuthority):
                _LOOKAHEAD_FINALIZER_ESCROW.publish_rooted(ticket, owner)

    def close(self) -> None:
        """Stop admission and clean owners outside the lifecycle lock."""
        if os.getpid() != self._pid:
            return
        condition = self._lifecycle_condition()
        deadline = monotonic() + self._close_timeout_seconds
        with condition:
            while self._close_in_progress:
                remaining = deadline - monotonic()
                if remaining <= 0 or not condition.wait(timeout=remaining):
                    raise RuntimeError("partition lookahead concurrent close exceeded its deadline")
            if self._closed:
                self._release_finalizer_ticket()
                return
            self._close_in_progress = True
            self._close_started = True
            self.enabled = False
            self._armed = None
            while self._submissions_inflight:
                remaining = deadline - monotonic()
                if remaining <= 0 or not condition.wait(timeout=remaining):
                    self._close_in_progress = False
                    condition.notify_all()
                    raise RuntimeError(
                        "partition lookahead admissions exceeded their close deadline"
                    )
            if self._protocol_violations:
                self._close_in_progress = False
                condition.notify_all()
                raise RuntimeError("partition lookahead lifecycle protocol violation")
            future = self._future
            future_context = self._future_context
            executor = self._executor

        cleanup_committed = False
        executor_committed = executor is None
        deferred = False
        cleanup_failed = False
        try:
            if future is None and future_context is not None:
                future_context.close()
                cleanup_committed = True
            elif future is not None:
                if future.cancel():
                    if future_context is not None:
                        future_context.close()
                    cleanup_committed = True
                elif future.done():
                    try:
                        prepared = future.result()
                    except BaseException as exc:
                        if future_context is not None:
                            try:
                                future_context.close()
                            except BaseException as cleanup_error:
                                add_bounded_note(
                                    exc,
                                    "partition lookahead context cleanup also failed",
                                    cleanup_error,
                                )
                                raise exc
                    else:
                        prepared.close()
                    cleanup_committed = True
                else:
                    register = False
                    with condition:
                        if not self._late_close_registered:
                            self._late_close_registered = True
                            register = True
                    if register:
                        # Registration is outside the lock because an already
                        # completed Future invokes callbacks synchronously.
                        try:
                            future.add_done_callback(self._late_future_completed)
                        except BaseException:
                            with condition:
                                self._late_close_registered = False
                                condition.notify_all()
                            # A non-standard Future may invoke the callback and
                            # then raise. A terminal result can still be drained
                            # by this generation; otherwise retain it for retry.
                            if not future.done():
                                raise
                    deferred = not future.done()

            if not deferred and executor is not None:
                executor.shutdown(wait=True, cancel_futures=True)
                executor_committed = True

            # A no-op/custom shutdown may leave a running Future. Keep every
            # owner reachable and let its callback start the next generation.
            if future is not None and not cleanup_committed and future.done():
                try:
                    prepared = future.result()
                except BaseException as exc:
                    if future_context is not None:
                        try:
                            future_context.close()
                        except BaseException as cleanup_error:
                            add_bounded_note(
                                exc,
                                "partition lookahead context cleanup also failed",
                                cleanup_error,
                            )
                            raise exc
                else:
                    prepared.close()
                cleanup_committed = True
                deferred = False

            with condition:
                if cleanup_committed:
                    if self._future is future:
                        self._future = None
                    if self._future_context is future_context:
                        self._future_context = None
                    self._late_close_registered = False
                if executor_committed and self._executor is executor:
                    self._executor = None
                if self._future is None and self._future_context is None:
                    self._closed = True
                self._sync_finalizer_owner()
        except BaseException:
            cleanup_failed = True
            raise
        finally:
            with condition:
                self._close_in_progress = False
                retry_now = (
                    not self._closed
                    and self._future is not None
                    and self._future.done()
                    and not self._late_close_registered
                    and not cleanup_failed
                )
                condition.notify_all()
        if retry_now:
            self.close()
        if self._closed:
            self._release_finalizer_ticket()

    def _runtime_shutdown(self, *, deadline_seconds: float) -> bool:
        normalized = normalize_duration(
            deadline_seconds,
            name="partition lookahead shutdown deadline",
            allow_zero=True,
        )
        assert normalized is not None
        previous = self._close_timeout_seconds
        self._close_timeout_seconds = min(previous, normalized)
        try:
            self.close()
        except BaseException:
            return False
        finally:
            self._close_timeout_seconds = previous
        stopped = bool(self._closed)
        if stopped:
            registration = self._runtime_registration
            self._runtime_registration = None
            self._sync_finalizer_owner()
            if registration is not None:
                registration.close()
        return stopped

    def __enter__(self) -> PartitionSourceLookahead:
        """Return the active lookahead controller."""
        return self

    def __exit__(self, *_exc: object) -> None:
        """Close speculative work."""
        self.close()

    def __del__(self) -> None:
        """Arm the pre-rooted lookahead authority without waiting in GC."""
        global _LOOKAHEAD_FINALIZER_OVERFLOWS, _LOOKAHEAD_FINALIZER_OVERFLOWED
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", -1)
            owner = getattr(self, "_finalizer_owner", None)
            if not (type(ticket) is int and ticket >= 0):
                return
            if not isinstance(owner, RootedFinalizerAuthority):
                return
            self._sync_finalizer_owner()
            if getattr(self, "_closed", False):
                owner.make_ack_only()
            if _LOOKAHEAD_FINALIZER_ESCROW.publish_rooted(ticket, owner):
                self._finalizer_ticket = -1
                return
            _LOOKAHEAD_FINALIZER_OVERFLOWED = True
            try:
                _LOOKAHEAD_FINALIZER_OVERFLOWS += 1
            except MemoryError:
                pass
        except BaseException:
            _LOOKAHEAD_FINALIZER_OVERFLOWED = True
            try:
                _LOOKAHEAD_FINALIZER_OVERFLOWS += 1
            except MemoryError:
                pass


def _reset_partition_lookahead_finalizers_after_fork() -> None:
    global _LOOKAHEAD_FINALIZER_OVERFLOWS, _LOOKAHEAD_FINALIZER_OVERFLOWED
    _LOOKAHEAD_FINALIZER_ESCROW.reset_after_fork()
    _LOOKAHEAD_FINALIZER_OVERFLOWS = 0
    _LOOKAHEAD_FINALIZER_OVERFLOWED = False


from ..core_impl.fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler("partition-lookahead", mode="quarantine_only")


from ..core_impl.finalizer_registry import (  # noqa: E402
    register_finalizer_domain as _register_finalizer_domain,
)

_register_finalizer_domain(
    "partition_lookahead",
    drain=drain_partition_lookahead_finalizers,
    snapshot=partition_lookahead_finalizer_snapshot,
    escrows=(("partition_lookahead", _LOOKAHEAD_FINALIZER_ESCROW),),
)


__all__ = [
    "PartitionSourceLookahead",
    "drain_partition_lookahead_finalizers",
    "partition_lookahead_finalizer_snapshot",
]
