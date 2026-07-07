"""Registry warm-up helpers for partitioned pipelines."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from time import perf_counter
from typing import Any

from ..api_impl.file_api_helpers import _call_options_from_locals
from ..api_impl.file_api_option_sets import CONVERTER_HELPER_KEYS, PARQUET_WRITER_OPTION_KEYS
from ..api_impl.file_convert_core import options_for_schema_probe
from ..api_impl.pool import default_pool
from ..api_impl.public_input import PreparedPublicInput, prepare_public_input
from ..api_impl.schema_registry import _normalize_registry_json
from ..api_impl.shared import _unwrap_options
from ..api_impl.source_plan import (
    probe_prepared_source_plan_registry,
    source_plan_from_prepared_inputs,
)
from ..options_impl.call_options import normalize_call_options_or_none
from .types import PartitionRunPlan, SchemaRegistryState

SUPPORTED_WARM_UP_INPUT_FORMATS = frozenset(
    {"csv", "json", "json_array", "jsonl", "ndjson", "parquet", "xml"}
)
_LAST_WARM_UP_ROUTE = "none"
WarmUpProgressCallback = Callable[[int, int, PartitionRunPlan, float], None]


def last_warm_up_route() -> str:
    """Return the route used by the most recent schema warm-up inference."""
    return _LAST_WARM_UP_ROUTE


def prepare_schema_warm_up_input(
    plans: list[PartitionRunPlan],
    *,
    input_format: str,
    input_mode: str,
    input_text_encoding: str = "utf-8",
    xml_row_tag: str | None = None,
    csv_delimiter: str = ",",
    csv_has_header: bool = True,
    batch_memory_limit_bytes: int | None = None,
    call_options: Any = None,
    _enable_parquet_native: bool = True,
    after_source_prepared: WarmUpProgressCallback | None = None,
) -> PreparedPublicInput:
    """Prepare warm-up sources as one native source plan."""
    if input_format not in SUPPORTED_WARM_UP_INPUT_FORMATS:
        accepted = ", ".join(sorted(SUPPORTED_WARM_UP_INPUT_FORMATS))
        raise ValueError(
            f"Schema warm-up currently supports input_format={accepted}. "
            "Other formats need a format-specific multi-source reader."
        )
    if not plans:
        raise ValueError("Schema warm-up requires at least one source partition")

    prepared_inputs: list[PreparedPublicInput] = []
    try:
        total = len(plans)
        for index, plan in enumerate(plans, start=1):
            start = perf_counter()
            prepared = prepare_public_input(
                plan.source_uri,
                input_format=input_format,
                input_mode=input_mode,
                input_text_encoding=input_text_encoding,
                xml_row_tag=xml_row_tag,
                csv_delimiter=csv_delimiter,
                csv_has_header=csv_has_header,
                memory_limit_bytes=batch_memory_limit_bytes,
            )
            prepared_inputs.append(prepared)
            if after_source_prepared is not None:
                after_source_prepared(index, total, plan, perf_counter() - start)
    except Exception:
        while prepared_inputs:
            prepared_inputs.pop().close()
        raise

    plan = None
    if input_format != "parquet" or _enable_parquet_native:
        plan = source_plan_from_prepared_inputs(
            prepared_inputs,
            input_format=input_format,
            input_mode=input_mode,
            input_text_encoding=input_text_encoding,
            xml_row_tag=xml_row_tag,
            csv_delimiter=csv_delimiter,
            csv_has_header=csv_has_header,
            memory_limit_bytes=batch_memory_limit_bytes,
            call_options=call_options,
        )
    if plan is None:
        prepared_formats = sorted({prepared.format for prepared in prepared_inputs})
        while prepared_inputs:
            prepared_inputs.pop().close()
        if input_format == "parquet":
            raise ValueError(
                "Parquet schema warm-up requires native Arrow-source probing; "
                "ensure PyArrow is available and all Parquet sources have compatible schemas."
            )
        raise ValueError(
            "Schema warm-up sources must be native path-source compatible; "
            "use UTF-8 text input and native-supported formats "
            f"(got prepared formats {prepared_formats})."
        )
    plan.close_items.extend(prepared_inputs)
    return PreparedPublicInput(
        plan,
        plan.input_format,
        "source_plan",
        keepalive=plan,
        xml_row_tag=plan.xml_row_tag,
    )


def _warm_up_call_options(
    options: Mapping[str, Any],
) -> tuple[dict[str, Any], Any]:
    """Return normalized warm-up call-option input and object."""
    call_options_input = dict(options)
    call_options_input["schema_mode"] = "additive"
    call_options_input = _call_options_from_locals(
        call_options_input,
        CONVERTER_HELPER_KEYS | PARQUET_WRITER_OPTION_KEYS,
    )
    return (
        call_options_input,
        normalize_call_options_or_none(**options_for_schema_probe(call_options_input)),
    )


def infer_warm_up_schema_registry_state(
    plans: list[PartitionRunPlan],
    *,
    input_format: str,
    input_mode: str,
    options: Mapping[str, Any],
    schema_registry: Mapping[str, Any] | str | None,
    field_name_policy: str,
    after_source_prepared: WarmUpProgressCallback | None = None,
) -> SchemaRegistryState:
    """Run additive schema warm-up and return JSON plus native registry state."""
    global _LAST_WARM_UP_ROUTE
    _LAST_WARM_UP_ROUTE = "none"
    call_options_input, call_options = _warm_up_call_options(options)
    registry_json = _normalize_registry_json(schema_registry)
    prepared_input = prepare_schema_warm_up_input(
        plans,
        input_format=input_format,
        input_mode=input_mode,
        input_text_encoding=str(options.get("input_text_encoding", "utf-8")),
        xml_row_tag=options.get("xml_row_tag"),
        csv_delimiter=str(options.get("csv_delimiter", ",")),
        csv_has_header=bool(options.get("csv_has_header", True)),
        batch_memory_limit_bytes=options.get("batch_memory_limit_bytes"),
        call_options=call_options,
        after_source_prepared=after_source_prepared,
    )
    try:
        if prepared_input.xml_row_tag is not None:
            call_options_input = dict(call_options_input)
            call_options_input["xml_row_tag"] = prepared_input.xml_row_tag
            call_options_input["input_text_encoding"] = "utf-8"
            call_options = normalize_call_options_or_none(
                **options_for_schema_probe(call_options_input)
            )
        if prepared_input.source == "source_plan":
            probe = probe_prepared_source_plan_registry(
                default_pool().get()._raw,
                prepared_input,
                _unwrap_options(call_options),
                registry_json=registry_json,
                field_name_policy=field_name_policy,
                schema_mode="additive",
            )
            raw = probe.raw
            if raw is not None:
                _LAST_WARM_UP_ROUTE = probe.route_name
                return SchemaRegistryState(
                    schema_registry_json=raw.schema_registry_json or "{}",
                    native_registry_state=getattr(raw, "native_registry_state", None),
                )
            raise ValueError("Schema warm-up native probing is unavailable for these sources.")
        raise ValueError(f"Unsupported prepared schema warm-up source: {prepared_input.source!r}")
    finally:
        prepared_input.close()


def infer_warm_up_schema_registry_json(
    plans: list[PartitionRunPlan],
    *,
    input_format: str,
    input_mode: str,
    options: Mapping[str, Any],
    schema_registry: Mapping[str, Any] | str | None,
    field_name_policy: str,
    after_source_prepared: WarmUpProgressCallback | None = None,
) -> str:
    """Run additive schema warm-up and return the updated canonical registry JSON."""
    return infer_warm_up_schema_registry_state(
        plans,
        input_format=input_format,
        input_mode=input_mode,
        options=options,
        schema_registry=schema_registry,
        field_name_policy=field_name_policy,
        after_source_prepared=after_source_prepared,
    ).schema_registry_json


def infer_warm_up_schema_registry(
    plans: list[PartitionRunPlan],
    *,
    input_format: str,
    input_mode: str,
    options: Mapping[str, Any],
    schema_registry: Mapping[str, Any] | str | None,
    field_name_policy: str,
    after_source_prepared: WarmUpProgressCallback | None = None,
) -> dict[str, Any]:
    """Run additive schema warm-up and return the updated registry."""
    registry_json = infer_warm_up_schema_registry_json(
        plans,
        input_format=input_format,
        input_mode=input_mode,
        options=options,
        schema_registry=schema_registry,
        field_name_policy=field_name_policy,
        after_source_prepared=after_source_prepared,
    )
    return json.loads(registry_json or "{}")
