"""Bounded one-partition source preparation for multi-mode pipelines."""

from __future__ import annotations

from collections.abc import Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

from ..api_impl.input.preparation import prepare_public_input
from ..api_impl.operation_context import OperationExecutionContext
from ..api_impl.partition_resources import BorrowedPartitionResources
from ..api_impl.source_plan.attached import remote_native_multisource_manifest_from_data
from ..core_impl.execution_policy import (
    execution_policy,
    threading_mode_from_multi_threading,
)
from ..errors import SchemaSanitizerResourceError
from ..input_impl.directory_inputs import discovered_directory_input_context
from ..input_impl.prepared import PreparedPublicInput
from .types import PartitionRunPlan


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
    def from_kwargs(cls, kwargs: Mapping[str, Any]) -> _PreparationOptions:
        """Extract canonical public input settings from converter keyword arguments."""
        return cls(
            input_format=kwargs.get("input_format"),
            input_mode=str(kwargs.get("input_mode", "single_file")),
            input_text_encoding=str(kwargs.get("input_text_encoding", "utf-8")),
            xml_row_tag=kwargs.get("xml_row_tag"),
            csv_delimiter=str(kwargs.get("csv_delimiter", ",")),
            csv_has_header=bool(kwargs.get("csv_has_header", True)),
            memory_limit_bytes=kwargs.get("memory_limit_bytes"),
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
        self.prepared_input.close()
        self.operation_context.close()


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

    def __init__(self, kwargs: Mapping[str, Any]) -> None:
        """Create a lazy one-slot worker when the static pipeline policy allows it."""
        self._kwargs = kwargs
        initial_options = self._current_options()
        policy = execution_policy(
            initial_options.threading_mode,
            initial_options.memory_limit_bytes,
        )
        self.enabled = not policy.is_single and policy.effective_workers > 1
        self._executor = (
            ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix="schema-sanitizer-partition-lookahead",
            )
            if self.enabled
            else None
        )
        self._armed: tuple[PartitionRunPlan, OperationExecutionContext] | None = None
        self._future: Future[PreparedOrDeferred] | None = None
        self._closed = False

    def prepare_first(self, plan: PartitionRunPlan) -> _PreparedPartition:
        """Prepare the first partition synchronously before any lookahead exists."""
        if not self.enabled:
            raise RuntimeError("partition source lookahead is disabled")
        options = self._current_options()
        prepared = self._prepare_with_new_context(plan, options)
        return self._materialize_deferred(prepared, options)

    def arm(
        self,
        plan: PartitionRunPlan | None,
        parent_context: OperationExecutionContext,
    ) -> None:
        """Record the only next partition and its resource-sharing parent."""
        if not self.enabled or self._closed or plan is None:
            return
        if self._future is not None or self._armed is not None:
            raise RuntimeError("partition lookahead window already contains work")
        self._armed = (plan, parent_context)

    def trigger(self) -> None:
        """Submit the armed partition once a conversion reaches a safe boundary."""
        if not self.enabled or self._closed or self._armed is None:
            return
        executor = self._executor
        if executor is None:
            raise RuntimeError("partition lookahead worker is unavailable")
        plan, parent_context = self._armed
        self._armed = None
        options = self._current_options()
        try:
            child_context = parent_context.fork()
            self._future = executor.submit(self._prepare, plan, child_context, options)
        except Exception:
            child = locals().get("child_context")
            if child is not None:
                child.close()
            self.enabled = False
            self._executor = None
            executor.shutdown(wait=False, cancel_futures=True)

    def take_next(self, plan: PartitionRunPlan) -> _PreparedPartition:
        """Consume the retained next result, raising its error only at this ordinal."""
        if not self.enabled:
            raise RuntimeError("partition source lookahead is disabled")
        future = self._future
        self._future = None
        options = self._current_options()
        if future is None:
            prepared = self._prepare_with_new_context(plan, options)
        else:
            prepared = future.result()
            if prepared.plan != plan:
                prepared.close()
                raise RuntimeError("partition lookahead returned a different source ordinal")
            if prepared.options != options:
                prepared.close()
                prepared = self._prepare_with_new_context(plan, options)
        return self._materialize_deferred(prepared, options)

    def _current_options(self) -> _PreparationOptions:
        """Capture the live static mapping at one preparation boundary."""
        return _PreparationOptions.from_kwargs(self._kwargs)

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
        except BaseException:
            if prepared is not None:
                prepared.close()
            context.close()
            raise

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
            value.close()
            return self._materialize_deferred(
                self._prepare_with_new_context(value.plan, options),
                options,
            )
        if value.options != options:
            value.close()
            return self._materialize_deferred(
                self._prepare_with_new_context(value.plan, options),
                options,
            )
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

    def close(self) -> None:
        """Cancel or drain speculative work and release every retained resource."""
        if self._closed:
            return
        self._closed = True
        self._armed = None
        future = self._future
        self._future = None
        if future is not None:
            future.cancel()
            if not future.cancelled():
                try:
                    future.result().close()
                except BaseException:
                    pass
        executor = self._executor
        self._executor = None
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)

    def __enter__(self) -> PartitionSourceLookahead:
        """Return the active lookahead controller."""
        return self

    def __exit__(self, *_exc: object) -> None:
        """Close speculative work."""
        self.close()


__all__ = ["PartitionSourceLookahead"]
