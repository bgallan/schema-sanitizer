"""Native Parquet reader preflight, contracts, and stream opening."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ...errors import SchemaSanitizerResourceError
from .contract_gates.native import (
    _NATIVE_PARQUET_WRITER_CREATED_BY,
    _native_nested_contract_diagnostics,
    _native_nested_contract_status_from_summary,
)
from .layout.reducer import _native_recursive_layout_summary_from_footer_info
from .memory import _native_parquet_batch_size_contract_issue
from .telemetry import (
    set_parquet_native_reader_diagnostics,
    set_parquet_stream_factory_route,
)


def native_writer_detected(info: dict[str, Any]) -> bool:
    """Return whether footer diagnostics identify schema-sanitizer's writer."""
    return info.get("created_by") == _NATIVE_PARQUET_WRITER_CREATED_BY


def native_writer_diagnostics(info: dict[str, Any]) -> dict[str, Any]:
    """Return common diagnostics proving native-writer detection."""
    return {
        "created_by": info.get("created_by"),
        "native_writer_detected": native_writer_detected(info),
    }


def parquet_resource_diagnostics(info: dict[str, Any]) -> dict[str, Any]:
    """Return bounded compression counters derived from footer metadata."""
    compressed = 0
    decompressed = 0
    for row_group in list(info.get("row_groups") or []):
        if not isinstance(row_group, dict):
            continue
        for column in list(row_group.get("columns") or []):
            if not isinstance(column, dict):
                continue
            try:
                compressed += max(0, int(column.get("total_compressed_size") or 0))
            except Exception:
                pass
            try:
                decompressed += max(0, int(column.get("total_uncompressed_size") or 0))
            except Exception:
                pass
    return {
        "compressed_bytes": compressed,
        "decompressed_bytes": decompressed,
        "decompression_ratio": (float(decompressed) / float(compressed) if compressed > 0 else 0.0),
    }


def native_nested_contract_blockers(info: dict[str, Any]) -> list[str]:
    """Return blockers when a native-writer nested contract is unsafe."""
    if not native_writer_detected(info):
        return []
    if info.get("bounded_preflight") == 1:
        if info.get("native_reader_ready") == 1:
            return []
        issues = [str(issue) for issue in list(info.get("native_reader_blockers") or [])]
        if not issues:
            issues = ["bounded native stream preflight was not satisfied"]
        return [f"native nested contract: {issue}" for issue in issues]
    status = _native_nested_contract_status_from_summary(
        _native_recursive_layout_summary_from_footer_info(info)
    )
    if not status.get("applicable") or status.get("satisfied") is True:
        return []
    issues = [str(issue) for issue in list(status.get("issues") or [])]
    if not issues:
        issues = ["native nested contract was not satisfied"]
    return [f"native nested contract: {issue}" for issue in issues]


def _raise_explicit_memory_limit(
    factory: Any, blockers: list[str], *, cause: BaseException | None = None
) -> None:
    """Fail closed when a configured budget made the native route unsafe.

    The PyArrow fallback does not participate in the native operation ledger,
    so using it after a memory-related preflight rejection would silently
    bypass ``memory_limit_bytes``.
    """
    if factory._memory_limit_bytes is None:
        return
    joined = "; ".join(blockers) or "native Parquet memory preflight failed"
    lowered = joined.lower()
    memory_related = any(
        marker in lowered
        for marker in (
            "memory",
            "configured limit",
            "metadata capacity",
            "buffer estimate",
            "footer length",
            "retained arrow capacity",
            "operation budget",
        )
    )
    if not memory_related:
        return
    error = SchemaSanitizerResourceError(
        f"memory_limit_bytes limit exceeded during Parquet input preflight: {joined}",
        detail={
            "stage": "parquet_read",
            "limit_name": "memory_limit_bytes",
            "limit_bytes": factory._memory_limit_bytes,
        },
    )
    if cause is not None:
        raise error from cause
    raise error


@dataclass(frozen=True)
class NativeParquetReadPlan:
    """Native read callable and validated footer diagnostics."""

    read: Any
    footer_info: dict[str, Any]


def prepare_native_parquet_read(
    factory: Any,
    *,
    native_stream_read_hook: Any,
    footer_info: Callable[..., dict[str, Any] | None],
    logger: Any,
) -> NativeParquetReadPlan | None:
    """Return a validated native-read plan or record the fallback reason."""
    if factory._filters is not None:
        set_parquet_native_reader_diagnostics(
            attempted=False,
            ready=False,
            reason="filter_requires_dataset_scanner",
            fallback_expected=True,
            fallback_route="pyarrow_dataset_scanner",
        )
        return None
    if factory._local_path is None:
        set_parquet_native_reader_diagnostics(
            attempted=False,
            ready=False,
            reason="source_not_path",
            fallback_expected=True,
            fallback_route="pyarrow_parquetfile_iter_batches",
            source=factory._source,
            native_source_kind=factory._native_source_kind,
        )
        return None
    native_read = native_stream_read_hook
    try:
        info = footer_info(
            factory._local_path,
            columns=factory._columns,
            memory_limit_bytes=factory._memory_limit_bytes,
        )
    except Exception as exc:
        _raise_explicit_memory_limit(factory, [f"{type(exc).__name__}: {exc}"], cause=exc)
        set_parquet_native_reader_diagnostics(
            attempted=True,
            ready=False,
            reason="footer_info_error",
            blockers=[f"{type(exc).__name__}: {exc}"],
            fallback_expected=True,
            fallback_route="pyarrow_dataset_scanner",
            source=factory._source,
            native_source_kind=factory._native_source_kind,
        )
        logger.debug(
            "Native Parquet reader skipped; footer info failed; retrying input with PyArrow: %s",
            exc,
        )
        return None
    if not info:
        set_parquet_native_reader_diagnostics(
            attempted=True,
            ready=False,
            reason="footer_info_unavailable",
            fallback_expected=True,
            fallback_route="pyarrow_dataset_scanner",
            source=factory._source,
            native_source_kind=factory._native_source_kind,
        )
        logger.debug("Native Parquet reader skipped: footer info unavailable")
        return None
    batch_size_blocker = _native_parquet_batch_size_contract_issue(info, factory._batch_size)
    if batch_size_blocker is not None:
        _record_not_ready(factory, info, [batch_size_blocker])
        logger.debug(
            "Native Parquet reader skipped; retrying input with PyArrow: %s",
            batch_size_blocker,
        )
        return None
    nested_blockers = native_nested_contract_blockers(info)
    if nested_blockers:
        _raise_explicit_memory_limit(factory, nested_blockers)
        set_parquet_native_reader_diagnostics(
            attempted=True,
            ready=False,
            reason="native_nested_contract_failed",
            blockers=nested_blockers,
            fallback_expected=True,
            fallback_route="pyarrow_dataset_scanner",
            row_group_count=info.get("row_group_count"),
            num_rows=info.get("num_rows"),
            **native_writer_diagnostics(info),
            **parquet_resource_diagnostics(info),
            native_writer_contract_satisfied=False,
            **_native_nested_contract_diagnostics(info),
            source=factory._source,
            native_source_kind=factory._native_source_kind,
        )
        logger.debug(
            "Native Parquet reader skipped; nested contract failed; "
            "retrying input with PyArrow: %s",
            nested_blockers[0],
        )
        return None
    blockers = list(info.get("native_reader_blockers") or [])
    bounded_preflight = info.get("bounded_preflight") == 1
    if info.get("native_reader_ready") != 1 and (bounded_preflight or factory._columns is None):
        _raise_explicit_memory_limit(factory, [str(item) for item in blockers])
        _record_not_ready(factory, info, blockers)
        logger.debug(
            "Native Parquet reader skipped; retrying input with PyArrow: %s",
            blockers[0] if blockers else "unknown blocker",
        )
        return None
    if info.get("native_reader_ready") != 1:
        logger.debug(
            "Native Parquet reader full footer was not ready; trying projected "
            "native read before PyArrow fallback: %s",
            blockers[0] if blockers else "unknown blocker",
        )
    return NativeParquetReadPlan(read=native_read, footer_info=info)


def _record_not_ready(factory: Any, info: dict[str, Any], blockers: list[str]) -> None:
    """Record one native reader preflight rejection."""
    set_parquet_native_reader_diagnostics(
        attempted=True,
        ready=False,
        reason="not_ready",
        blockers=blockers,
        fallback_expected=True,
        fallback_route="pyarrow_dataset_scanner",
        row_group_count=info.get("row_group_count"),
        num_rows=info.get("num_rows"),
        **native_writer_diagnostics(info),
        **parquet_resource_diagnostics(info),
        source=factory._source,
        native_source_kind=factory._native_source_kind,
    )


def try_native_parquet_stream(
    factory: Any,
    *,
    native_stream_read_hook: Any,
    footer_info: Callable[..., dict[str, Any] | None],
    logger: Any,
) -> Any | None:
    """Return a native Parquet Arrow C stream capsule when supported."""
    plan = prepare_native_parquet_read(
        factory,
        native_stream_read_hook=native_stream_read_hook,
        footer_info=footer_info,
        logger=logger,
    )
    if plan is None:
        return None
    info = plan.footer_info
    try:
        capsule = plan.read(
            factory._local_path,
            None if factory._columns is None else list(factory._columns),
            -1 if factory._memory_limit_bytes is None else factory._memory_limit_bytes,
        )
    except Exception as exc:
        _raise_explicit_memory_limit(factory, [f"{type(exc).__name__}: {exc}"], cause=exc)
        _record_native_open_error(factory, info, exc, logger)
        return None
    set_parquet_native_reader_diagnostics(
        attempted=True,
        ready=True,
        reason="native_stream",
        blockers=[],
        fallback_expected=False,
        fallback_attempted=False,
        fallback_succeeded=False,
        pipeline_contract_satisfied=True,
        pipeline_contract_route="native_parquet_stream",
        pipeline_contract_error=None,
        native_reader_contract_satisfied=True,
        safe_fallback_contract_satisfied=False,
        row_group_count=info.get("row_group_count"),
        num_rows=info.get("num_rows"),
        **native_writer_diagnostics(info),
        **parquet_resource_diagnostics(info),
        native_writer_contract_satisfied=native_writer_detected(info),
        **_native_nested_contract_diagnostics(info),
        source=factory._source,
        native_source_kind=factory._native_source_kind,
    )
    set_parquet_stream_factory_route("native_parquet_stream")
    factory._keepalive = (capsule,)
    return capsule


def _record_native_open_error(
    factory: Any,
    info: dict[str, Any],
    exc: Exception,
    logger: Any,
) -> None:
    """Record a native stream-open failure before PyArrow fallback."""
    message = f"{type(exc).__name__}: {exc}"
    reason = "not_ready" if "file is not ready" in str(exc) else "native_error"
    set_parquet_native_reader_diagnostics(
        attempted=True,
        ready=False,
        reason=reason,
        blockers=[message] if reason == "not_ready" else [],
        error=message,
        fallback_expected=True,
        fallback_route="pyarrow_dataset_scanner",
        row_group_count=info.get("row_group_count"),
        num_rows=info.get("num_rows"),
        **native_writer_diagnostics(info),
        **parquet_resource_diagnostics(info),
        source=factory._source,
        native_source_kind=factory._native_source_kind,
    )
    if reason == "not_ready":
        logger.debug(
            "Native Parquet reader skipped after projected readiness check; "
            "retrying input with PyArrow: %s",
            message,
        )
    else:
        logger.error(
            "Native Parquet reader failed; retrying input with PyArrow",
            exc_info=exc,
        )
