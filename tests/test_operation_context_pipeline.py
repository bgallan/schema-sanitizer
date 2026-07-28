"""Whole-operation concurrency context propagation tests."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest


def test_file_conversion_shares_one_context_from_input_through_output(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Input preparation and output publication receive one context instance."""
    from schema_sanitizer.api_impl.file_conversion import converters
    from schema_sanitizer.api_impl.results import Result
    from schema_sanitizer.input_impl.prepared import PreparedPublicInput
    from schema_sanitizer.remote_impl.staging import RemoteOutputTarget

    observed: dict[str, object] = {}
    output_path = tmp_path / "out.jsonl"

    def prepare_input(_input_path, **kwargs):
        """Capture the context passed to source preparation."""
        observed["input"] = kwargs["operation_context"]
        return PreparedPublicInput(data=b"{}\n", format="jsonl", source="bytes")

    def prepare_output(path, **kwargs):
        """Capture the context passed to output preparation."""
        observed["output"] = kwargs["operation_context"]
        return RemoteOutputTarget(
            local_path=str(path),
            operation_context=kwargs["operation_context"],
            threading_mode=kwargs["threading_mode"],
            memory_limit_bytes=kwargs["memory_limit_bytes"],
        )

    def write(_data, path, **_kwargs):
        """Create a deterministic output marker."""
        Path(path).write_text("ok\n", encoding="utf-8")
        return Result(SimpleNamespace(diagnostics=None), schema_registry_json="{}")

    def finalize(target):
        """Capture the context retained by the output target."""
        observed["finalize"] = target.operation_context

    monkeypatch.setattr(converters, "prepare_public_input", prepare_input)
    monkeypatch.setattr(converters, "prepare_output_target", prepare_output)
    monkeypatch.setattr(converters, "finalize_output_target", finalize)
    monkeypatch.setattr(converters, "try_convert_source_plan_with_options", lambda *_a, **_k: None)

    result = converters.convert_file_with_options(
        "s3://bucket/in.jsonl",
        output_path,
        input_format="jsonl",
        input_mode="single_file",
        options={
            "multi_threading": True,
            "memory_limit_bytes": 64 * 1024 * 1024,
        },
        writer=write,
        source_plan_writer=write,
        feature="to_jsonl",
        schema_registry=None,
    )

    assert observed["input"] is observed["output"] is observed["finalize"]
    assert result.execution_policy is not None
    assert result.execution_policy["requested_mode"] == "multi"
    assert output_path.read_text(encoding="utf-8") == "ok\n"


def test_context_construction_releases_resources_when_clock_capture_fails(
    monkeypatch,
) -> None:
    """A failed metadata capture cannot leak the newly created resource domain."""
    from schema_sanitizer.api_impl import operation_context as context_module

    released: list[bool] = []

    class Resources:
        """Minimal resource-domain double recording final release."""

        def release(self) -> None:
            """Record cleanup after constructor failure."""
            released.append(True)

    resources = Resources()
    monkeypatch.setattr(
        context_module,
        "_OperationExecutionResources",
        lambda _policy, _memory_limit_bytes: resources,
    )

    def fail_clock() -> None:
        """Model an unexpected wall-clock provider failure."""
        raise RuntimeError("clock unavailable")

    monkeypatch.setattr(context_module, "capture_operation_timestamps", fail_clock)
    with pytest.raises(RuntimeError, match="clock unavailable"):
        context_module.OperationExecutionContext(
            threading_mode="multi",
            memory_limit_bytes=64 << 20,
        )

    assert released == [True]
