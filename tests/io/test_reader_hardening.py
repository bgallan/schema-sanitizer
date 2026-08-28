"""Regression tests for hostile XML, CSV, and JSON reader inputs."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

import schema_sanitizer as ss

pytestmark = pytest.mark.usefixtures("require_native")

_GENERATED_COLUMNS = {
    "schema_registry",
    "schema_drifts",
    "source_file",
    "ingestion_timestamp",
}


def _convert(
    tmp_path: Path,
    payload: bytes,
    *,
    input_format: str,
    suffix: str,
    **options: object,
) -> list[dict[str, object]]:
    """Convert one hostile-reader fixture through the public JSONL sink."""
    source = tmp_path / f"input.{suffix}"
    output = tmp_path / "output.jsonl"
    source.write_bytes(payload)
    ss.to_jsonl(source, output, input_format=input_format, **options)
    rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    return [
        {key: value for key, value in row.items() if key not in _GENERATED_COLUMNS} for row in rows
    ]


def _nested_xml(depth: int) -> bytes:
    """Build one XML document with the requested nesting depth."""
    return (("<n>" * depth) + "x" + ("</n>" * depth)).encode()


def _row_nested_xml(inner_depth: int) -> bytes:
    """Build one row-tag XML document with the requested nesting depth."""
    return (
        "<rows><row>" + ("<n>" * inner_depth) + "x" + ("</n>" * inner_depth) + "</row></rows>"
    ).encode()


def _capture_error(
    tmp_path: Path,
    payload: bytes,
    *,
    input_format: str,
    suffix: str,
    **options: object,
) -> tuple[type[BaseException], str]:
    """Run one conversion and return its stable public error fingerprint."""
    source = tmp_path / f"bad-{options.get('multi_threading', False)}.{suffix}"
    output = tmp_path / f"bad-{options.get('multi_threading', False)}.jsonl"
    source.write_bytes(payload)
    with pytest.raises(ss.SchemaSanitizerError) as caught:
        ss.to_jsonl(source, output, input_format=input_format, **options)
    return type(caught.value), str(caught.value)


@pytest.mark.parametrize(
    ("xml_row_tag", "accepted", "rejected"),
    [
        (None, _nested_xml(512), _nested_xml(513)),
        ("row", _row_nested_xml(510), _row_nested_xml(511)),
    ],
)
def test_xml_internal_depth_boundary_is_stable(
    tmp_path: Path,
    xml_row_tag: str | None,
    accepted: bytes,
    rejected: bytes,
) -> None:
    """Depth 512 succeeds and depth 513 is rejected without recursion failure."""

    _convert(
        tmp_path,
        accepted,
        input_format="xml",
        suffix="xml",
        xml_row_tag=xml_row_tag,
    )
    errors = [
        _capture_error(
            tmp_path,
            rejected,
            input_format="xml",
            suffix="xml",
            xml_row_tag=xml_row_tag,
            multi_threading=enabled,
        )
        for enabled in (False, True)
    ]

    assert errors[0] == errors[1]
    assert errors[0][0] is ss.SchemaSanitizerInvalidArgumentError
    assert "nesting depth 513" in errors[0][1]
    assert "safety limit 512" in errors[0][1]


@pytest.mark.parametrize("xml_row_tag", [None, "row"])
def test_xml_twenty_thousand_levels_fail_in_subprocess(
    tmp_path: Path, xml_row_tag: str | None
) -> None:
    """Extreme nesting returns a Python error rather than crashing the process."""

    source = tmp_path / "deep.xml"
    output = tmp_path / "deep.jsonl"
    payload = _nested_xml(20_000) if xml_row_tag is None else _row_nested_xml(19_998)
    source.write_bytes(payload)
    script = """
import sys
sys.path.insert(0, sys.argv[4])
import schema_sanitizer as ss
try:
    ss.to_jsonl(sys.argv[1], sys.argv[2], input_format="xml", xml_row_tag=None if sys.argv[3] == "-" else sys.argv[3])
except ss.SchemaSanitizerInvalidArgumentError as exc:
    if "safety limit 512" not in str(exc):
        raise
else:
    raise SystemExit("hostile nesting unexpectedly succeeded")
"""
    repo_root = Path(__file__).parents[2]
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(source),
            str(output),
            xml_row_tag or "-",
            str(repo_root / "src"),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
        cwd=repo_root,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout


@pytest.mark.parametrize("xml_row_tag", [None, "row"])
@pytest.mark.parametrize(
    "entity",
    [b"&#x110000;", b"&#xD800;", b"&#xFFFFFFFF;", b"&#0;", b"&unknown;", b"&amp"],
)
def test_xml_rejects_invalid_entities_in_text_and_attributes(
    tmp_path: Path, xml_row_tag: str | None, entity: bytes
) -> None:
    """Entity validation is strict and shared by text and attributes."""

    for payload in (
        b"<rows><row><value>" + entity + b"</value></row></rows>",
        b'<rows><row value="' + entity + b'"/></rows>',
    ):
        error_type, message = _capture_error(
            tmp_path,
            payload,
            input_format="xml",
            suffix="xml",
            xml_row_tag=xml_row_tag,
        )
        assert error_type is ss.SchemaSanitizerInvalidArgumentError
        assert "XML parse error at byte" in message


@pytest.mark.parametrize("multi_threading", [False, True])
def test_xml_entity_split_across_stream_chunk_is_decoded_once(
    tmp_path: Path, multi_threading: bool
) -> None:
    """An entity spanning a row-scanner refill has the same decoded value."""

    chunk_bytes = (1024 * 1024) // 64
    prefix = b"<rows><row><value>"
    padding = b" " * (chunk_bytes - len(prefix) - 2)
    payload = prefix + padding + b"&amp;</value></row></rows>"

    rows = _convert(
        tmp_path,
        payload,
        input_format="xml",
        suffix="xml",
        xml_row_tag="row",
        memory_limit_bytes=1024 * 1024,
        multi_threading=multi_threading,
    )
    assert rows == [{"value": "&"}]


@pytest.mark.parametrize("xml_row_tag", [None, "row"])
@pytest.mark.parametrize("multi_threading", [False, True])
def test_xml_node_expansion_obeys_shared_operation_budget(
    tmp_path: Path, xml_row_tag: str | None, multi_threading: bool
) -> None:
    """Document trees stay bounded while streamed row trees release early."""

    row_count = 1_000
    payload = ("<rows>" + ("<row><a/><b/><c/><d/></row>" * row_count) + "</rows>").encode()
    source = tmp_path / f"expanded-{xml_row_tag}-{multi_threading}.xml"
    output = tmp_path / f"expanded-{xml_row_tag}-{multi_threading}.jsonl"
    source.write_bytes(payload)

    if xml_row_tag is None:
        with pytest.raises(ss.SchemaSanitizerOutOfMemoryError):
            ss.to_jsonl(
                source,
                output,
                input_format="xml",
                xml_row_tag=xml_row_tag,
                memory_limit_bytes=1024 * 1024,
                multi_threading=multi_threading,
            )
    else:
        result = ss.to_polars(
            source,
            input_format="xml",
            xml_row_tag=xml_row_tag,
            memory_limit_bytes=1024 * 1024,
            multi_threading=multi_threading,
        )
        assert result.clean_data.height == row_count


@pytest.mark.parametrize(
    "payload",
    [
        b'<root a="1" a="2"/>',
        b'<root a="x<y"/>',
        b"<root><!-- bad--comment --></root>",
        b'<!DoCtYpE root [<!EnTiTy x "boom">]><root>&x;</root>',
        b"<root><child></root>",
        b"<root>\xff</root>",
    ],
)
def test_xml_strict_syntax_is_rejected_consistently(tmp_path: Path, payload: bytes) -> None:
    """Document and row-tag scanners reject malformed XML syntax."""

    row_payload = b"<rows><row>" + payload + b"</row></rows>"
    for selected_payload, xml_row_tag in ((payload, None), (row_payload, "row")):
        error_type, message = _capture_error(
            tmp_path,
            selected_payload,
            input_format="xml",
            suffix="xml",
            xml_row_tag=xml_row_tag,
        )
        assert error_type is ss.SchemaSanitizerInvalidArgumentError
        assert "XML parse error at byte" in message or "DTD" in message or "UTF-8" in message


def test_xml_rejects_trailing_document_content_in_both_modes(tmp_path: Path) -> None:
    """Non-whitespace bytes after the root are never silently ignored."""

    for payload, xml_row_tag in (
        (b"<root/>trailing", None),
        (b"<rows><row><root/></row></rows>trailing", "row"),
    ):
        error_type, message = _capture_error(
            tmp_path,
            payload,
            input_format="xml",
            suffix="xml",
            xml_row_tag=xml_row_tag,
        )
        assert error_type is ss.SchemaSanitizerInvalidArgumentError
        assert "outside root" in message or "trailing content" in message


@pytest.mark.parametrize(
    ("payload", "offset_fragment"),
    [
        (b'a,b\n1,"x', "byte 6"),
        (b'a,b\n1,"x"junk\n', "byte 9"),
        (b'a,b\n1,x"y\n', "byte 7"),
    ],
)
def test_csv_strict_quote_errors_match_across_threading_modes(
    tmp_path: Path, payload: bytes, offset_fragment: str
) -> None:
    """CSV quote failures are structured and preserve exact source offsets."""

    errors = [
        _capture_error(
            tmp_path,
            payload,
            input_format="csv",
            suffix="csv",
            multi_threading=enabled,
        )
        for enabled in (False, True)
    ]

    assert errors[0] == errors[1]
    assert errors[0][0] is ss.SchemaSanitizerInvalidArgumentError
    assert offset_fragment in errors[0][1]


def test_csv_valid_strict_controls(tmp_path: Path) -> None:
    """Embedded newlines, doubled quotes, empty fields, and delimiters remain valid."""

    rows = _convert(
        tmp_path,
        b'a;b;c;d\r\n"x\r\ny";"a""b";;z\r\n',
        input_format="csv",
        suffix="csv",
        csv_delimiter=";",
    )
    assert rows == [{"a": "x\ny", "b": 'a"b', "d": "z"}]


def _nested_json_object(depth: int) -> bytes:
    """Build one JSON object with the requested nesting depth."""
    return (b'{"n":' * depth) + b"0" + (b"}" * depth)


@pytest.mark.parametrize("input_format", ["json", "jsonl"])
@pytest.mark.parametrize("multi_threading", [False, True])
@pytest.mark.parametrize(
    "payload",
    [
        b'{"keep":1,"bad":"\\q"}',
        b'{"keep":1,"bad":"\\uZZZZ"}',
        b'{"keep":1,"bad":"\\uD800"}',
        b'{"keep":1,"bad":"\\uDC00"}',
        b'{"keep":1,"bad":"\xc0\xaf"}',
        b'{"keep":1e+}',
        b'{"keep":1} trailing',
    ],
)
def test_json_rejects_malformed_values_on_all_text_paths(
    tmp_path: Path,
    input_format: str,
    multi_threading: bool,
    payload: bytes,
) -> None:
    """Optimized JSON paths validate escapes, Unicode, numbers, and tails."""

    error_type, message = _capture_error(
        tmp_path,
        payload + (b"\n" if input_format == "jsonl" else b""),
        input_format=input_format,
        suffix=input_format,
        multi_threading=multi_threading,
        on_error="stop",
    )
    assert error_type is ss.SchemaSanitizerInvalidArgumentError
    assert "byte" in message


def test_json_strict_projection_still_validates_unknown_fields(tmp_path: Path) -> None:
    """Malformed unprojected fields cannot bypass strict/lazy validation."""

    seed = tmp_path / "seed.jsonl"
    seed_out = tmp_path / "seed-out.jsonl"
    seed.write_text('{"keep":1}\n', encoding="utf-8")
    registry = ss.to_jsonl(seed, seed_out, input_format="jsonl").schema_registry

    payload = b'{"keep":2,"ignored":"\\q"}\n'
    errors = [
        _capture_error(
            tmp_path,
            payload,
            input_format="jsonl",
            suffix="jsonl",
            schema_mode="strict",
            schema_registry=registry,
            on_error="stop",
            multi_threading=enabled,
        )
        for enabled in (False, True)
    ]
    assert errors[0] == errors[1]
    assert errors[0][0] is ss.SchemaSanitizerInvalidArgumentError
    assert "invalid escape" in errors[0][1]


@pytest.mark.parametrize("input_format", ["json", "jsonl"])
def test_json_internal_depth_boundary_is_stable(tmp_path: Path, input_format: str) -> None:
    """Depth 512 succeeds and depth 513 fails without recursive crashes."""

    accepted = _nested_json_object(512)
    rejected = _nested_json_object(513)
    _convert(
        tmp_path,
        accepted + (b"\n" if input_format == "jsonl" else b""),
        input_format=input_format,
        suffix=input_format,
    )
    errors = [
        _capture_error(
            tmp_path,
            rejected + (b"\n" if input_format == "jsonl" else b""),
            input_format=input_format,
            suffix=input_format,
            on_error="stop",
            multi_threading=enabled,
        )
        for enabled in (False, True)
    ]
    assert errors[0] == errors[1]
    assert errors[0][0] is ss.SchemaSanitizerInvalidArgumentError
    assert "nesting" in errors[0][1]
    assert "safety limit 512" in errors[0][1]


@pytest.mark.parametrize("multi_threading", [False, True])
def test_json_surrogate_pair_crossing_chunk_boundary(tmp_path: Path, multi_threading: bool) -> None:
    """A valid surrogate pair remains valid across a scanner refill."""

    chunk_bytes = (1024 * 1024) // 64
    prefix = b'{"value":"'
    padding = b"x" * (chunk_bytes - len(prefix) - len(b"\\uD83D"))
    payload = prefix + padding + b'\\uD83D\\uDE00"}\n'
    rows = _convert(
        tmp_path,
        payload,
        input_format="jsonl",
        suffix="jsonl",
        memory_limit_bytes=1024 * 1024,
        multi_threading=multi_threading,
    )
    assert len(rows) == 1
    assert isinstance(rows[0]["value"], str)
    assert rows[0]["value"].endswith("😀")


@pytest.mark.parametrize("multi_threading", [False, True])
def test_jsonl_error_policy_recovers_without_losing_offsets(
    tmp_path: Path, multi_threading: bool
) -> None:
    """JSONL malformed rows stop, skip, or emit null as requested."""

    payload = b'{"a":1}\n{"a":"\\q"}\n{"a":3}\n'
    error_type, message = _capture_error(
        tmp_path,
        payload,
        input_format="jsonl",
        suffix="jsonl",
        on_error="stop",
        multi_threading=multi_threading,
    )
    assert error_type is ss.SchemaSanitizerInvalidArgumentError
    assert "byte 15" in message

    skipped = _convert(
        tmp_path,
        payload,
        input_format="jsonl",
        suffix="jsonl",
        on_error="skip_row",
        multi_threading=multi_threading,
    )
    assert skipped == [{"a": 1}, {"a": 3}]

    emitted = _convert(
        tmp_path,
        payload,
        input_format="jsonl",
        suffix="jsonl",
        on_error="emit_null_row",
        multi_threading=multi_threading,
    )
    assert emitted == [{"a": 1}, {"a": None}, {"a": 3}]


@pytest.mark.parametrize("multi_threading", [False, True])
def test_json_duplicate_keys_and_long_numbers_are_deterministic(
    tmp_path: Path, multi_threading: bool
) -> None:
    """Duplicate keys are first-wins and oversized numbers fail predictably."""

    rows = _convert(
        tmp_path,
        b'{"a":1,"a":2}\n',
        input_format="jsonl",
        suffix="jsonl",
        multi_threading=multi_threading,
    )
    assert rows == [{"a": 1}]

    error_type, message = _capture_error(
        tmp_path,
        b'{"a":' + (b"9" * 10_000) + b"}\n",
        input_format="jsonl",
        suffix="jsonl",
        on_error="stop",
        multi_threading=multi_threading,
    )
    assert error_type is ss.SchemaSanitizerInvalidArgumentError
    assert "invalid float" in message or "invalid integer" in message


def test_parquet_footer_budget_precedes_global_hard_ceiling(tmp_path: Path) -> None:
    """A low operation budget rejects an oversized footer before reading it."""
    from schema_sanitizer.adapters.parquet.status import (
        native_parquet_stream_preflight_info,
    )

    footer_length = 100 * 1024
    path = tmp_path / "oversized-footer.parquet"
    path.write_bytes(
        b"PAR1" + (b"\0" * (footer_length - 4)) + footer_length.to_bytes(4, "little") + b"PAR1"
    )

    with pytest.raises(
        RuntimeError,
        match=r"footer length 102400.*effective limit 65536",
    ):
        native_parquet_stream_preflight_info(path, memory_limit_bytes=1024 * 1024)


def test_parquet_page_budget_precedes_page_hard_ceiling(tmp_path: Path) -> None:
    """Page verification and decompression use the lower operation limit."""
    from schema_sanitizer.adapters.parquet.status import (
        native_parquet_stream_preflight_info,
    )

    source = tmp_path / "wide.jsonl"
    parquet = tmp_path / "wide.parquet"
    source.write_text(
        "".join(json.dumps({"a": f"{index:020d}"}) + "\n" for index in range(5000)),
        encoding="utf-8",
    )
    ss.to_parquet(
        source,
        parquet,
        input_format="jsonl",
        parquet_compression="uncompressed",
    )

    limited = native_parquet_stream_preflight_info(
        parquet,
        columns=["a"],
        memory_limit_bytes=1024 * 1024,
    )
    assert limited is not None
    assert limited["native_reader_ready"] == 0
    assert any(
        "page exceeds effective operation limit 65536" in blocker
        for blocker in limited["native_reader_blockers"]
    )

    relaxed = native_parquet_stream_preflight_info(
        parquet,
        columns=["a"],
        memory_limit_bytes=4 * 1024 * 1024,
    )
    assert relaxed is not None
    assert relaxed["native_reader_ready"] == 1


@pytest.mark.parametrize("codec", ["uncompressed", "snappy", "gzip"])
def test_parquet_corrupt_supported_codec_payloads_fail_closed(tmp_path: Path, codec: str) -> None:
    """Corrupt payloads from every compiled native codec fail without a crash."""
    from schema_sanitizer.adapters.parquet.status import (
        native_parquet_footer_info,
        native_parquet_stream_preflight_info,
    )

    source = tmp_path / f"codec-{codec}.jsonl"
    parquet = tmp_path / f"codec-{codec}.parquet"
    source.write_text(
        "".join(json.dumps({"a": f"{index:064d}"}) + "\n" for index in range(200)),
        encoding="utf-8",
    )
    ss.to_parquet(
        source,
        parquet,
        input_format="jsonl",
        parquet_compression=codec,
    )

    footer = native_parquet_footer_info(parquet, columns=["a"])
    assert footer is not None
    page = footer["row_groups"][0]["columns"][0]["pages"][0]
    payload_offset = int(page["compressed_payload_offset"])
    mutated = bytearray(parquet.read_bytes())
    mutated[payload_offset] = 0 if mutated[payload_offset] == 0xFF else 0xFF
    corrupt = tmp_path / f"codec-{codec}-corrupt.parquet"
    corrupt.write_bytes(mutated)

    preflight = native_parquet_stream_preflight_info(
        corrupt,
        columns=["a"],
        memory_limit_bytes=16 * 1024 * 1024,
    )
    assert preflight is not None
    assert preflight["native_reader_ready"] == 0
    assert any("row group 0:" in blocker for blocker in preflight["native_reader_blockers"])


@pytest.mark.parametrize("codec", ["snappy", "gzip"])
def test_parquet_high_expansion_page_is_rejected_by_operation_budget(
    tmp_path: Path, codec: str
) -> None:
    """A highly compressible page is rejected before its expansion can escape the budget."""
    from schema_sanitizer.adapters.parquet.status import (
        native_parquet_footer_info,
        native_parquet_stream_preflight_info,
    )

    source = tmp_path / f"{codec}-expansion.jsonl"
    parquet = tmp_path / f"{codec}-expansion.parquet"
    source.write_text(
        "".join(json.dumps({"a": ("x" * 4080) + f"{index:016d}"}) + "\n" for index in range(4000)),
        encoding="utf-8",
    )
    ss.to_parquet(
        source,
        parquet,
        input_format="jsonl",
        parquet_compression=codec,
    )

    footer = native_parquet_footer_info(parquet, columns=["a"])
    assert footer is not None
    page = footer["row_groups"][0]["columns"][0]["pages"][0]
    # The writer may split pages at different valid row boundaries; the
    # security property is that the selected page exceeds the reader's 2-MiB
    # effective operation limit and has a high compression ratio.
    assert int(page["uncompressed_page_size"]) > 2 * 1024 * 1024
    assert int(page["compressed_page_size"]) < int(page["uncompressed_page_size"]) // 10

    preflight = native_parquet_stream_preflight_info(
        parquet,
        columns=["a"],
        memory_limit_bytes=64 * 1024 * 1024,
    )
    assert preflight is not None
    assert preflight["native_reader_ready"] == 0
    assert any(
        "page exceeds effective operation limit 2097152" in blocker
        for blocker in preflight["native_reader_blockers"]
    )


@pytest.mark.parametrize("codec", ["uncompressed", "snappy", "gzip"])
def test_parquet_truncated_supported_codec_payloads_fail_closed(tmp_path: Path, codec: str) -> None:
    """Removing one payload byte from every native codec fails without a crash."""
    from schema_sanitizer.adapters.parquet.status import (
        native_parquet_footer_info,
        native_parquet_stream_preflight_info,
    )

    source = tmp_path / f"truncated-{codec}.jsonl"
    parquet = tmp_path / f"truncated-{codec}.parquet"
    truncated = tmp_path / f"truncated-{codec}-bad.parquet"
    source.write_text(
        "".join(json.dumps({"a": f"{index:064d}"}) + "\n" for index in range(200)),
        encoding="utf-8",
    )
    ss.to_parquet(
        source,
        parquet,
        input_format="jsonl",
        parquet_compression=codec,
    )
    footer = native_parquet_footer_info(parquet, columns=["a"])
    assert footer is not None
    page = footer["row_groups"][0]["columns"][0]["pages"][0]
    payload_offset = int(page["compressed_payload_offset"])
    payload_size = int(page["compressed_page_size"])
    assert payload_size > 1
    original = parquet.read_bytes()
    cut = payload_offset + payload_size - 1
    truncated.write_bytes(original[:cut] + original[cut + 1 :])

    preflight = native_parquet_stream_preflight_info(
        truncated,
        columns=["a"],
        memory_limit_bytes=16 * 1024 * 1024,
    )
    assert preflight is not None
    assert preflight["native_reader_ready"] == 0
    assert any("row group 0:" in blocker for blocker in preflight["native_reader_blockers"])


@pytest.mark.parametrize("multi_threading", [False, True])
def test_csv_duplicate_nonempty_headers_fail_deterministically(
    tmp_path: Path, multi_threading: bool
) -> None:
    """Duplicate source headers cannot silently collapse into one output field."""

    error_type, message = _capture_error(
        tmp_path,
        b"a,a\n1,2\n",
        input_format="csv",
        suffix="csv",
        multi_threading=multi_threading,
    )
    assert error_type is ss.SchemaSanitizerInvalidArgumentError
    assert "duplicate non-empty name at column 2" in message


@pytest.mark.parametrize("xml_row_tag", [None, "row"])
def test_xml_long_unmatched_ampersand_run_is_bounded(
    tmp_path: Path, xml_row_tag: str | None
) -> None:
    """A long malformed entity run terminates promptly without repeated rescans."""

    source = tmp_path / "ampersands.xml"
    output = tmp_path / "ampersands.jsonl"
    payload = b"<rows><row><value>" + (b"&" * 1_000_000) + b"</value></row></rows>"
    source.write_bytes(payload)
    script = """
import sys
sys.path.insert(0, sys.argv[4])
import schema_sanitizer as ss
try:
    ss.to_jsonl(
        sys.argv[1],
        sys.argv[2],
        input_format="xml",
        xml_row_tag=None if sys.argv[3] == "-" else sys.argv[3],
        on_error="stop",
    )
except ss.SchemaSanitizerInvalidArgumentError:
    pass
else:
    raise SystemExit("malformed entity run unexpectedly succeeded")
"""
    repo_root = Path(__file__).parents[2]
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(source),
            str(output),
            xml_row_tag or "-",
            str(repo_root / "src"),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
        cwd=repo_root,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout


@pytest.mark.parametrize("xml_row_tag", [None, "row"])
def test_xml_mixed_case_doctype_crossing_chunk_boundary_is_disabled(
    tmp_path: Path, xml_row_tag: str | None
) -> None:
    """DTD/entity rejection is case-insensitive and stable across scanner refills."""

    chunk_bytes = (1024 * 1024) // 64
    payload = (
        (b" " * (chunk_bytes - 4))
        + b'<!DoCtYpE rows [<!EnTiTy x "boom">]>'
        + b"<rows><row>&x;</row></rows>"
    )
    error_type, message = _capture_error(
        tmp_path,
        payload,
        input_format="xml",
        suffix="xml",
        xml_row_tag=xml_row_tag,
        memory_limit_bytes=1024 * 1024,
        on_error="stop",
    )
    assert error_type is ss.SchemaSanitizerInvalidArgumentError
    assert "DTD and entity declarations are not supported" in message
    assert f"byte {chunk_bytes - 4}" in message


@pytest.mark.parametrize("multi_threading", [False, True])
def test_csv_rejects_truncated_utf8_in_optimized_and_parsed_paths(
    tmp_path: Path, multi_threading: bool
) -> None:
    """CSV validates raw UTF-8 even when rows use direct/raw-only projection."""

    error_type, message = _capture_error(
        tmp_path,
        b"a,b\n1,\xc3\n",
        input_format="csv",
        suffix="csv",
        multi_threading=multi_threading,
        on_error="stop",
    )
    assert error_type is ss.SchemaSanitizerInvalidArgumentError
    assert "truncated UTF-8 sequence" in message
    assert "byte 6" in message


def test_csv_utf8_bom_is_accepted_and_removed_from_header(tmp_path: Path) -> None:
    """A UTF-8 BOM remains supported while malformed UTF-8 is rejected."""

    rows = _convert(
        tmp_path,
        b"\xef\xbb\xbfa,b\n1,2\n",
        input_format="csv",
        suffix="csv",
    )
    assert rows == [{"a": "1", "b": "2"}]


@pytest.mark.parametrize(
    ("field_name_policy", "header"),
    [
        ("lower_alpha", b"A-B,AB\n1,2\n"),
        ("lower_snake", b"A B,A_B\n1,2\n"),
    ],
)
def test_csv_rejects_post_reconciliation_header_collisions(
    tmp_path: Path, field_name_policy: str, header: bytes
) -> None:
    """Distinct source names cannot collapse under the configured name policy."""

    errors = [
        _capture_error(
            tmp_path,
            header,
            input_format="csv",
            suffix="csv",
            field_name_policy=field_name_policy,
            multi_threading=enabled,
        )
        for enabled in (False, True)
    ]
    assert errors[0] == errors[1]
    assert errors[0][0] is ss.SchemaSanitizerInvalidArgumentError
    assert "collide after field-name reconciliation at column 2" in errors[0][1]
    assert f"policy '{field_name_policy}'" in errors[0][1]


def test_csv_preserve_policy_keeps_distinct_source_headers(tmp_path: Path) -> None:
    """Preserve mode accepts names that only normalized policies would merge."""

    rows = _convert(
        tmp_path,
        b"A-B,AB\n1,2\n",
        input_format="csv",
        suffix="csv",
        field_name_policy="preserve",
    )
    assert rows == [{"A-B": "1", "AB": "2"}]


@pytest.mark.parametrize("multi_threading", [False, True])
def test_csv_field_limit_is_derived_from_operation_budget(
    tmp_path: Path, multi_threading: bool
) -> None:
    """A field cannot consume the whole operation budget in any execution mode."""

    payload = b"value\n" + (b"x" * (600 * 1024)) + b"\n"
    error_type, message = _capture_error(
        tmp_path,
        payload,
        input_format="csv",
        suffix="csv",
        memory_limit_bytes=1024 * 1024,
        multi_threading=multi_threading,
        on_error="stop",
    )
    assert error_type is ss.SchemaSanitizerOutOfMemoryError
    assert "CSV field size exceeds effective limit" in message
    assert "614400 > 524288" in message


def test_parquet_level_output_is_bounded_before_vector_allocation(
    tmp_path: Path,
) -> None:
    """A hostile page num_values cannot reserve level vectors before budgeting."""
    from schema_sanitizer.adapters.parquet.status import (
        native_parquet_footer_info,
        native_parquet_stream_preflight_info,
    )

    source = tmp_path / "levels-source.jsonl"
    parquet = tmp_path / "levels-source.parquet"
    corrupt = tmp_path / "levels-corrupt.parquet"
    source.write_text(
        "".join(json.dumps({"a": value}) + "\n" for value in range(10)),
        encoding="utf-8",
    )
    ss.to_parquet(
        source,
        parquet,
        input_format="jsonl",
        parquet_compression="uncompressed",
    )
    info = native_parquet_footer_info(parquet, columns=["a"])
    assert info is not None
    column = info["row_groups"][0]["columns"][0]
    assert int(column["max_definition_level"]) == 1
    page = column["pages"][0]
    header_offset = int(page["header_offset"])
    header_size = int(page["header_size"])
    payload_offset = int(page["compressed_payload_offset"])
    payload_size = int(page["compressed_page_size"])
    original = parquet.read_bytes()
    header = original[header_offset : header_offset + header_size]
    assert header == bytes.fromhex("1500152015202c1514150a150615060000")

    def encode_varint(value: int) -> bytes:
        """Encode one unsigned integer using the Parquet varint representation."""
        encoded = bytearray()
        while value >= 0x80:
            encoded.append((value & 0x7F) | 0x80)
            value >>= 7
        encoded.append(value)
        return bytes(encoded)

    hostile_count = 100_000_000
    encoded_count = encode_varint(hostile_count * 2)  # compact zigzag i32
    mutated_header = bytearray(header)
    mutated_header[3] = 0x1A  # uncompressed size: 13
    mutated_header[5] = 0x1A  # compressed size: 13
    data_page_header = header.index(bytes.fromhex("2c1514"))
    mutated_header = (
        bytes(mutated_header[: data_page_header + 1])
        + b"\x15"
        + encoded_count
        + bytes(mutated_header[data_page_header + 3 :])
    )
    growth = len(mutated_header) - len(header)
    assert growth == 3
    payload = original[payload_offset : payload_offset + payload_size]
    mutated = (
        original[:header_offset]
        + mutated_header
        + payload[:-growth]
        + original[payload_offset + payload_size :]
    )
    assert len(mutated) == len(original)
    corrupt.write_bytes(mutated)

    preflight = native_parquet_stream_preflight_info(
        corrupt,
        columns=["a"],
        memory_limit_bytes=1024 * 1024,
    )
    assert preflight is not None
    assert preflight["native_reader_ready"] == 0
    assert any(
        "derived level output exceeds effective operation limit 524288" in blocker
        for blocker in preflight["native_reader_blockers"]
    )


def test_parquet_page_crc_is_validated_before_decode(tmp_path: Path) -> None:
    """An optional Parquet page CRC mismatch fails before value decoding."""
    from schema_sanitizer.adapters.parquet.status import native_parquet_footer_info

    source = tmp_path / "crc-source.jsonl"
    parquet = tmp_path / "crc-source.parquet"
    corrupt = tmp_path / "crc-corrupt.parquet"
    source.write_text(
        "".join(json.dumps({"a": value}) + "\n" for value in range(10)),
        encoding="utf-8",
    )
    ss.to_parquet(
        source,
        parquet,
        input_format="jsonl",
        parquet_compression="uncompressed",
    )

    info = native_parquet_footer_info(parquet, columns=["a"])
    assert info is not None
    page = info["row_groups"][0]["columns"][0]["pages"][0]
    header_offset = int(page["header_offset"])
    header_size = int(page["header_size"])
    payload_size = int(page["compressed_page_size"])
    assert payload_size == 16

    original = parquet.read_bytes()
    header = original[header_offset : header_offset + header_size]
    # Native writer fixture: fields 1/2/3 followed by DataPageHeader field 5.
    assert header[:7] == bytes.fromhex("1500152015202c")

    # Insert PageHeader.crc (field 4) with an intentionally wrong value. The
    # header grows by two bytes, so shrink both declared page sizes and the
    # payload by two bytes to keep all footer offsets and chunk lengths stable.
    mutated_header = bytearray(header)
    mutated_header[3] = 0x1C  # uncompressed size: 14 (zigzag 28)
    mutated_header[5] = 0x1C  # compressed size: 14
    mutated_header = (
        bytes(mutated_header[:6])
        + bytes.fromhex("15001c")  # field 4 CRC=0, then field 5 struct
        + bytes(mutated_header[7:])
    )
    payload_start = header_offset + header_size
    payload = original[payload_start : payload_start + payload_size]
    mutated = (
        original[:header_offset]
        + mutated_header
        + payload[:-2]
        + original[payload_start + payload_size :]
    )
    assert len(mutated) == len(original)
    corrupt.write_bytes(mutated)

    with pytest.raises(RuntimeError, match=r"CRC32 checksum mismatch at byte"):
        native_parquet_footer_info(corrupt, columns=["a"])


@pytest.mark.parametrize("multi_threading", [False, True])
def test_xml_peak_charged_memory_stays_within_budget_and_releases(
    tmp_path: Path, multi_threading: bool
) -> None:
    """XML success and failure leave no operation-pool bytes charged."""
    from schema_sanitizer.api_impl.execution_context import default_pool

    source = tmp_path / "bounded.xml"
    output = tmp_path / "bounded.jsonl"
    source.write_text(
        "<rows>"
        + "".join(f"<row><id>{index}</id><value>{'x' * 512}</value></row>" for index in range(2000))
        + "</rows>",
        encoding="utf-8",
    )
    limit = 8 * 1024 * 1024
    ss.to_jsonl(
        source,
        output,
        input_format="xml",
        xml_row_tag="row",
        memory_limit_bytes=limit,
        multi_threading=multi_threading,
    )
    memory = default_pool().get().performance_stats()["memory"]
    assert 0 < int(memory["peak_bytes"]) <= limit
    assert int(memory["current_bytes"]) == 0

    hostile = tmp_path / "hostile.xml"
    hostile_output = tmp_path / "hostile.jsonl"
    hostile.write_text(
        "<root>" + "".join(f"<n{i}/>" for i in range(100_000)) + "</root>",
        encoding="utf-8",
    )
    with pytest.raises(ss.SchemaSanitizerResourceError):
        ss.to_jsonl(
            hostile,
            hostile_output,
            input_format="xml",
            memory_limit_bytes=1024 * 1024,
            multi_threading=multi_threading,
        )
    failed_memory = default_pool().get().performance_stats()["memory"]
    assert 0 <= int(failed_memory["peak_bytes"]) <= 1024 * 1024
    assert int(failed_memory["current_bytes"]) == 0
    assert not hostile_output.exists()


@pytest.mark.parametrize("multi_threading", [False, True])
def test_xml_committed_row_trees_release_before_batch_owner(
    tmp_path: Path, multi_threading: bool
) -> None:
    """A long row-tag stream stays below budget by releasing committed trees."""
    from schema_sanitizer.api_impl.execution_context import default_pool

    source = tmp_path / "many-rows.xml"
    output = tmp_path / "many-rows.jsonl"
    row_count = 10_000
    source.write_text(
        "<rows>"
        + "".join(
            f"<row><id>{index}</id><value>{'x' * 512}</value></row>" for index in range(row_count)
        )
        + "</rows>",
        encoding="utf-8",
    )
    limit = 64 * 1024 * 1024

    ss.to_jsonl(
        source,
        output,
        input_format="xml",
        xml_row_tag="row",
        memory_limit_bytes=limit,
        multi_threading=multi_threading,
    )

    assert output.read_bytes().count(b"\n") == row_count
    memory = default_pool().get().performance_stats()["memory"]
    assert 0 < int(memory["peak_bytes"]) <= limit
    assert int(memory["current_bytes"]) == 0


@pytest.mark.parametrize("xml_row_tag", [None, "row"])
@pytest.mark.parametrize("kind", ["comment", "cdata", "pi", "attribute"])
def test_xml_unterminated_markup_is_bounded(
    tmp_path: Path, xml_row_tag: str | None, kind: str
) -> None:
    """Large unterminated scanner states fail promptly without refill rescans."""

    source = tmp_path / "unterminated.xml"
    output = tmp_path / "unterminated.jsonl"
    prefixes = {
        "comment": b"<!--",
        "cdata": b"<![CDATA[",
        "pi": b"<?target ",
        "attribute": b'<node value="',
    }
    fragment = prefixes[kind] + (b"x" * 1_000_000)
    payload = b"<rows><row>" + fragment if xml_row_tag == "row" else b"<root>" + fragment
    source.write_bytes(payload)
    script = """
import sys
sys.path.insert(0, sys.argv[4])
import schema_sanitizer as ss
try:
    ss.to_jsonl(
        sys.argv[1],
        sys.argv[2],
        input_format="xml",
        xml_row_tag=None if sys.argv[3] == "-" else sys.argv[3],
        on_error="stop",
        memory_limit_bytes=4 * 1024 * 1024,
    )
except (ss.SchemaSanitizerInvalidArgumentError, ss.SchemaSanitizerOutOfMemoryError):
    pass
else:
    raise SystemExit("unterminated hostile markup unexpectedly succeeded")
"""
    repo_root = Path(__file__).parents[2]
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(source),
            str(output),
            xml_row_tag or "-",
            str(repo_root / "src"),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
        cwd=repo_root,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_xml_parallel_failure_cancels_workers_and_releases_budget(tmp_path: Path) -> None:
    """A row-tag parse failure drains worker-owned XML trees and pool charges."""
    from schema_sanitizer.api_impl.execution_context import default_pool

    source = tmp_path / "parallel-failure.xml"
    output = tmp_path / "parallel-failure.jsonl"
    rows: list[str] = []
    for index in range(192):
        if index == 17:
            rows.append(f'<row id="{index}" id="duplicate"><v>{"x" * 4096}</v></row>')
        else:
            rows.append(f'<row id="{index}"><v>{"x" * 4096}</v></row>')
    source.write_text("<rows>" + "".join(rows) + "</rows>", encoding="utf-8")

    with pytest.raises(ss.SchemaSanitizerInvalidArgumentError, match="duplicate attribute"):
        ss.to_jsonl(
            source,
            output,
            input_format="xml",
            xml_row_tag="row",
            memory_limit_bytes=16 * 1024 * 1024,
            multi_threading=True,
        )

    memory = default_pool().get().performance_stats()["memory"]
    assert int(memory["current_bytes"]) == 0
    assert 0 < int(memory["peak_bytes"]) <= 16 * 1024 * 1024
    assert not output.exists()


def test_jsonl_path_group_coordination_uses_operation_budget(tmp_path: Path) -> None:
    """Directory prefetch metadata and retained child batches share one pool."""
    from schema_sanitizer.api_impl.execution_context import default_pool

    source = tmp_path / "jsonl-group"
    source.mkdir()
    expected: list[int] = []
    for index in range(48):
        value = index * 3
        expected.append(value)
        (source / f"{index:03d}.jsonl").write_text(
            json.dumps({"value": value}) + "\n", encoding="utf-8"
        )
    output = tmp_path / "grouped.jsonl"
    limit = 8 * 1024 * 1024

    ss.to_jsonl(
        source,
        output,
        input_format="jsonl",
        input_mode="directory",
        memory_limit_bytes=limit,
        multi_threading=True,
    )

    values = [json.loads(line)["value"] for line in output.read_text().splitlines()]
    assert values == expected
    memory = default_pool().get().performance_stats()["memory"]
    assert int(memory["current_bytes"]) == 0
    assert 0 < int(memory["peak_bytes"]) <= limit


@pytest.mark.parametrize("multi_threading", [False, True])
def test_reader_errors_do_not_echo_sensitive_input_contents(
    tmp_path: Path, multi_threading: bool
) -> None:
    """Malformed payload values and XML names stay out of public exceptions."""

    secret = "private_customer_token_7f45c0"  # pragma: allowlist secret
    cases = (
        (
            f"<{secret}><child></{secret}_other>".encode(),
            "xml",
            "xml",
            {},
        ),
        (
            f'key\n"{secret}"junk\n'.encode(),
            "csv",
            "csv",
            {},
        ),
        (
            f'{{"key":"{secret}\\q"}}\n'.encode(),
            "jsonl",
            "jsonl",
            {"on_error": "stop"},
        ),
    )
    for payload, input_format, suffix, options in cases:
        error_type, message = _capture_error(
            tmp_path,
            payload,
            input_format=input_format,
            suffix=suffix,
            multi_threading=multi_threading,
            **options,
        )
        assert error_type is ss.SchemaSanitizerInvalidArgumentError
        assert secret not in message
