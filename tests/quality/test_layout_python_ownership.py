"""Ownership and layout contracts for Python runtime modules."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from schema_sanitizer.core_impl.generated_bytes import BufferedGeneratedBytesReader

ROOT = Path(__file__).resolve().parents[2]


class _ChunkReader(BufferedGeneratedBytesReader):
    """Small deterministic generated reader used to verify cursor semantics."""

    def __init__(self, chunks: list[bytes]):
        """Store deterministic source chunks."""
        self._chunks = chunks
        self._index = 0
        super().__init__("_ChunkReader", default_chunk_bytes=8)

    def _append_next(self, target_bytes: int) -> bool:
        """Append one deterministic source chunk."""
        del target_bytes
        if self._index >= len(self._chunks):
            return False
        self._buffer.extend(self._chunks[self._index])
        self._index += 1
        return True

    def _reset_reader(self) -> None:
        """Reset the deterministic source cursor."""
        self._index = 0


CPP = ROOT / "cpp/src"

SRC = ROOT / "src/schema_sanitizer"


def _production_text() -> str:
    """Return all production Python and C++ source text."""
    return "\n".join(
        (
            path.read_text(encoding="utf-8")
            for root in (SRC, ROOT / "cpp")
            for path in root.rglob("*")
            if path.is_file() and path.suffix in {".py", ".cc", ".hh", ".inc"}
        )
    )


def test_async_scheduler_has_a_neutral_core_owner() -> None:
    """Generic async planning must not live below the remote transport package."""
    owner = SRC / "core_impl/async_scheduler.py"
    source = owner.read_text(encoding="utf-8")
    assert owner.is_file()
    assert not (SRC / "remote_impl/scheduler.py").exists()
    assert "async def unordered_indexed_results" in source
    assert "async def ordered_indexed_results" in source
    assert "DirectoryDownloadTuning" not in source
    pipeline = (SRC / "pipeline/source_discovery.py").read_text(encoding="utf-8")
    assert "core_impl.async_scheduler" in pipeline
    assert "remote_impl.scheduler" not in pipeline
    for relative in ("api_impl/source_plan/remote.py", "api_impl/parquet/arrow_sources.py"):
        consumer = (SRC / relative).read_text(encoding="utf-8")
        assert "core_impl.execution_policy" in consumer
        assert "core_impl.async_scheduler" not in consumer
        assert "remote_impl.scheduler" not in consumer


def test_call_options_have_one_canonical_python_owner() -> None:
    """Wrapper filtering and normalization must not return to parallel modules."""
    package = ROOT / "src/schema_sanitizer"
    owner = package / "options_impl/call_options.py"
    source = owner.read_text(encoding="utf-8")
    assert owner.is_file()
    assert len(source.splitlines()) <= 500
    assert "FILE_CONVERSION_HELPER_KEYS" in source
    assert "ANALYTICAL_HELPER_KEYS" in source
    assert "def call_options_from_locals(" in source
    assert "def normalize_call_options(" in source
    assert not (package / "core_impl/call_options.py").exists()
    production = "\n".join((path.read_text(encoding="utf-8") for path in package.rglob("*.py")))
    assert "core_impl.call_options" not in production


def test_generated_byte_reader_uses_an_amortized_cursor() -> None:
    """Small reads advance a cursor instead of deleting the bytearray prefix."""
    reader = _ChunkReader([b"abcdefghij"])
    assert reader.read(2) == b"ab"
    assert len(reader._buffer) == 10
    assert reader._buffer_offset == 2
    assert b"".join(iter(lambda: reader.read(2), b"")) == b"cdefghij"
    assert reader._buffer == bytearray()
    assert reader._buffer_offset == 0
    assert reader.seek(0) == 0
    assert reader.read(10) == b"abcdefghij"
    source = (ROOT / "src/schema_sanitizer/core_impl/generated_bytes.py").read_text(
        encoding="utf-8"
    )
    assert "self._buffer_offset" in source
    assert "del self._buffer[:max_bytes]" not in source


def test_generated_reader_returns_slices_without_an_intermediate_bytearray() -> None:
    """Generated reads copy once from a released memoryview before compaction."""
    source = (SRC / "core_impl/generated_bytes.py").read_text(encoding="utf-8")
    assert "view = memoryview(self._buffer)" in source
    assert "out = view.tobytes()" in source
    assert "view.release()" in source
    assert "bytes(self._buffer[start : self._buffer_offset])" not in source


def test_native_symbol_caches_have_one_python_owner() -> None:
    """Tiny native symbol declarations must not regress into data-only facades."""
    core = ROOT / "src/schema_sanitizer/core_impl"
    assert (core / "native_symbols.py").is_file()
    assert not (core / "native_symbols").exists()
    for retired in ("arrow", "delimited", "parquet", "registry", "sources"):
        assert not (core / "native_symbols" / f"{retired}.py").exists()


def test_new_python_owner_packages_do_not_flatten_symbols() -> None:
    """Owner packages must not recreate the removed modules through reexports."""
    from schema_sanitizer.adapters.parquet import record_batch_factory, telemetry

    assert importlib.util.find_spec("schema_sanitizer.api_impl.file_conversion.public") is None
    assert not hasattr(record_batch_factory, "iter_parquet_record_batches")
    assert not hasattr(telemetry, "record_native_reader_result")


def test_optional_dependencies_have_one_canonical_owner() -> None:
    """Optional dependency loading must have one cached canonical owner."""
    dependencies = SRC / "core_impl/dependencies.py"
    text = dependencies.read_text(encoding="utf-8")
    assert len(text.splitlines()) <= 500
    assert "def ensure_optional_dependency(" in text
    assert "def ensure_pyarrow(" in text
    assert "def pyarrow_importable(" in text
    assert "@lru_cache(maxsize=1)" in text
    assert not (SRC / "core_impl/optional_dependencies.py").exists()
    assert not (SRC / "core_impl/pyarrow_dependency.py").exists()
    production = _production_text()
    assert "core_impl.optional_dependencies" not in production
    assert "core_impl.pyarrow_dependency" not in production


def test_python_reader_adapter_has_one_bounded_owner() -> None:
    """One small adapter should not be split by method implementation."""
    sources = ROOT / "cpp/src/api/python_abi3/sources"
    owner = sources / "python_reader.cc"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (sources / "python_reader").exists()
    assert not (sources / "_core_abi3_python_reader.cc").exists()


def test_python_reader_adapter_has_one_translation_unit() -> None:
    """The small Python reader class must compile from one cohesive owner."""
    sources = CPP / "api/python_abi3/sources"
    owner = sources / "python_reader.cc"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (sources / "python_reader").exists()
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert manifest.count("sources/python_reader.cc") == 1
    assert "sources/python_reader/" not in manifest


def test_python_row_shape_validation_has_one_native_owner() -> None:
    """Python orchestration validates the container; C++ validates each row."""
    selection = (SRC / "input_impl/selection.py").read_text(encoding="utf-8")
    native = (CPP / "api/python_abi3/json/_core_abi3_python_rows.cc").read_text(encoding="utf-8")
    assert "all(isinstance(row, dict)" not in selection
    assert "PyDict_Check(item)" in native
    assert '"%zd is not a dict"' in native


def test_python_rows_have_one_bounded_core_owner() -> None:
    """Python-row streaming remains centralized after native iterator batching."""
    execution = (SRC / "core_impl/execution.py").read_text(encoding="utf-8")
    owner = SRC / "core_impl/python_rows.py"
    source = owner.read_text(encoding="utf-8")
    assert "from .python_rows import" in execution
    assert "class PythonRowsJsonlByteReader" in source
    assert "def last_python_rows_route" in source
    assert "PYTHON_ITER_ROWS_JSONL_BYTES" in source
    assert len(execution.splitlines()) <= 500
    assert len(source.splitlines()) <= 600


def test_runtime_python_owners_are_direct_and_bounded() -> None:
    """Execution contexts and stream wrappers must not regress into micro-packages."""
    owners = [
        ROOT / "src/schema_sanitizer/api_impl/execution_context.py",
        ROOT / "src/schema_sanitizer/api_impl/streams.py",
    ]
    for owner in owners:
        assert owner.is_file()
        assert len(owner.read_text(encoding="utf-8").splitlines()) <= 600
    assert not (ROOT / "src/schema_sanitizer/api_impl/execution_context").exists()
    assert not (ROOT / "src/schema_sanitizer/stream_impl.py").exists()
    assert not (ROOT / "src/schema_sanitizer/api_impl/streams").exists()


def test_small_python_domains_are_direct_modules() -> None:
    """Call options and source plans stay cohesive without package facades."""
    package = ROOT / "src/schema_sanitizer"
    owners = {
        package / "options_impl/call_options.py": 500,
        package / "input_impl/source_plan.py": 500,
    }
    retired = (
        package / "options_impl/call_options",
        package / "source_plan_impl.py",
        package / "source_plan_impl",
        package / "api_impl/source_plan/plan.py",
        package / "input_impl/source_plan",
    )
    for owner, limit in owners.items():
        assert owner.is_file()
        assert len(owner.read_text(encoding="utf-8").splitlines()) <= limit
    assert all((not path.exists() for path in retired))
    source = "\n".join((path.read_text(encoding="utf-8") for path in package.rglob("*.py")))
    assert "options_impl.call_options.normalize" not in source
    assert "schema_sanitizer.source_plan_impl" not in source
    assert "api_impl.source_plan.plan.model" not in source
    assert "api_impl.source_plan.plan.path_sources" not in source


def test_small_python_domains_have_direct_owners() -> None:
    """Registry, result, and file metadata code must not return to micro-packages."""
    owners = (
        ROOT / "src/schema_sanitizer/core_impl/schema_registry.py",
        ROOT / "src/schema_sanitizer/api_impl/results.py",
        ROOT / "src/schema_sanitizer/adapters/pyarrow/file_metadata.py",
    )
    owner_limits = (500, 800, 500)
    for owner, limit in zip(owners, owner_limits, strict=True):
        assert owner.is_file()
        assert len(owner.read_text(encoding="utf-8").splitlines()) <= limit
        assert not owner.with_suffix("").is_dir()
    retired_paths = (
        ROOT / "src/schema_sanitizer/core_impl/schema_registry/document.py",
        ROOT / "src/schema_sanitizer/core_impl/schema_registry/native_state.py",
        ROOT / "src/schema_sanitizer/api_impl/results/result.py",
        ROOT / "src/schema_sanitizer/api_impl/results/sink.py",
        ROOT / "src/schema_sanitizer/adapters/pyarrow/file_metadata/stream.py",
    )
    assert not [path for path in retired_paths if path.exists()]


def test_temporal_capture_helpers_have_explicit_owners() -> None:
    """Calendar conversion and regex-capture parsing remain separate units."""
    planning = ROOT / "cpp/src/planning"
    assert not (planning / "options_temporal_regex_parts.cc").exists()
    assert not (ROOT / "cpp/src/internal/planning/options_temporal_regex_parts.hh").exists()
    assert {path.name for path in (planning / "temporal").glob("*.cc")} == {
        "calendar.cc",
        "regex_captures.cc",
    }
    assert (ROOT / "cpp/src/internal/planning/temporal/parts.hh").is_file()


def test_temporal_primitives_are_split_by_value_domain() -> None:
    """Date, time, and timestamp parsers must compile as separate units."""
    core = ROOT / "cpp/src/core"
    package = core / "temporal"
    assert {path.name for path in package.iterdir() if path.is_file()} == {
        "date.cpp",
        "parse_internal.hh",
        "time.cpp",
        "timestamp.cpp",
    }
    assert not (core / "primitives_temporal.cpp").exists()
