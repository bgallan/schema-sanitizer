"""Phase-4 contracts for typed CSV header-mode API plumbing."""

from __future__ import annotations

import inspect
import json
from pathlib import Path
from typing import get_args, get_type_hints

import pytest

import schema_sanitizer as ss
from schema_sanitizer.core_impl.native_options import validate_options
from schema_sanitizer.errors import SchemaSanitizerInvalidArgumentError
from schema_sanitizer.options_impl.call_options import normalize_call_options
from schema_sanitizer.options_impl.options import (
    Options as InternalOptions,
)
from schema_sanitizer.options_impl.options import (
    require_implemented_csv_header_mode,
)

PUBLIC_CONVERTERS = (
    ss.iter_batches,
    ss.to_duckdb,
    ss.to_pandas,
    ss.to_polars,
    ss.to_pyarrow,
    ss.to_csv,
    ss.to_jsonl,
    ss.to_parquet,
)


def test_all_public_converters_expose_typed_exact_default() -> None:
    """Every public reader/writer exposes the same typed header-mode option."""
    for converter in PUBLIC_CONVERTERS:
        parameter = inspect.signature(converter).parameters["csv_header_mode"]
        assert parameter.default == "exact"
        annotation = get_type_hints(converter)["csv_header_mode"]
        assert get_args(annotation) == ("exact", "union")


def test_call_options_carry_exact_and_union_modes() -> None:
    """The normalized native option object retains either public mode."""
    assert normalize_call_options().csv.csv_header_mode == "exact"
    assert normalize_call_options(csv_header_mode=" exact ").csv.csv_header_mode == "exact"
    assert normalize_call_options(csv_header_mode="UNION").csv.csv_header_mode == "union"


@pytest.mark.parametrize("value", [None, True, 1])
def test_csv_header_mode_rejects_non_strings(value: object) -> None:
    """Header-mode validation does not coerce unrelated scalar types."""
    with pytest.raises(TypeError, match="csv_header_mode"):
        normalize_call_options(csv_header_mode=value)


def test_csv_header_mode_rejects_unknown_strings() -> None:
    """Only the two documented header policies are accepted."""
    with pytest.raises(ValueError, match="csv_header_mode"):
        normalize_call_options(csv_header_mode="merge")


def test_union_mode_reaches_the_normal_input_lifecycle(tmp_path: Path) -> None:
    """Union is implemented and therefore performs ordinary input validation."""
    missing = tmp_path / "not-opened.csv"
    with pytest.raises(FileNotFoundError, match="single_file input requires a file"):
        ss.to_jsonl(
            missing,
            tmp_path / "unused.jsonl",
            input_format="csv",
            csv_header_mode="union",
        )


def _business_rows(path: Path) -> list[dict[str, object]]:
    """Return stable user/provenance fields from generated JSON Lines."""
    return [
        {
            key: value
            for key, value in json.loads(line).items()
            if key in {"id", "name", "source_file"}
        }
        for line in path.read_text(encoding="utf-8").splitlines()
    ]


def test_exact_default_and_explicit_mode_materialize_the_same_rows(tmp_path: Path) -> None:
    """Spelling out exact mode leaves the historical successful path unchanged."""
    folder = tmp_path / "input"
    folder.mkdir()
    (folder / "a.csv").write_text("id,name\n1,Ana\n", encoding="utf-8")
    (folder / "b.csv").write_text("id,name\n2,Luis\n", encoding="utf-8")
    default_output = tmp_path / "default.jsonl"
    exact_output = tmp_path / "exact.jsonl"

    ss.to_jsonl(folder, default_output, input_format="csv", input_mode="directory")
    ss.to_jsonl(
        folder,
        exact_output,
        input_format="csv",
        input_mode="directory",
        csv_header_mode="exact",
    )

    assert _business_rows(default_output) == _business_rows(exact_output)


def test_exact_default_and_explicit_mode_reject_the_same_mismatch(tmp_path: Path) -> None:
    """Reordered headers remain invalid in both implicit and explicit exact mode."""
    folder = tmp_path / "input"
    folder.mkdir()
    (folder / "a.csv").write_text("id,name\n1,Ana\n", encoding="utf-8")
    (folder / "b.csv").write_text("name,id\nLuis,2\n", encoding="utf-8")

    messages: list[str] = []
    for suffix, options in (("default", {}), ("exact", {"csv_header_mode": "exact"})):
        with pytest.raises(SchemaSanitizerInvalidArgumentError) as error:
            ss.to_jsonl(
                folder,
                tmp_path / f"{suffix}.jsonl",
                input_format="csv",
                input_mode="directory",
                **options,
            )
        messages.append(str(error.value))

    assert messages[0] == messages[1]
    assert "CSV directory header mismatch" in messages[0]


def test_native_catalog_validates_csv_header_mode() -> None:
    """The ABI3 option catalog carries and validates the staged mode field."""
    for mode in ("exact", "union"):
        validate_options(InternalOptions(csv={"csv_header_mode": mode}).raw)

    with pytest.raises(RuntimeError, match="csv_header_mode must be exact or union"):
        validate_options(InternalOptions(csv={"csv_header_mode": "merge"}).raw)


def test_both_header_modes_are_implemented() -> None:
    """The native reader accepts both public reconciliation policies."""
    assert require_implemented_csv_header_mode("exact") == "exact"
    assert require_implemented_csv_header_mode("union") == "union"
