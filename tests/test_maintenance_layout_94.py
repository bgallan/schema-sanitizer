"""Protect maintenance layout revision 94."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_options_have_two_bounded_owners_without_helper_facades() -> None:
    """Catalog options and per-call options remain cohesive direct modules."""
    package = ROOT / "src/schema_sanitizer/options_impl"
    owners = {"options.py", "call_options.py", "__init__.py"}
    assert {path.name for path in package.iterdir() if path.is_file()} == owners
    assert len((package / "options.py").read_text(encoding="utf-8").splitlines()) <= 500
    assert len((package / "call_options.py").read_text(encoding="utf-8").splitlines()) <= 500

    production = "\n".join(
        path.read_text(encoding="utf-8") for path in (ROOT / "src/schema_sanitizer").rglob("*.py")
    )
    for retired in ("options_groups", "call_option_validators", "options_impl.native_call"):
        assert retired not in production


def test_remote_staging_and_directory_downloads_are_bounded_owners() -> None:
    """Temporary paths and shared directory transfers have cohesive owners."""
    remote = ROOT / "src/schema_sanitizer/remote_impl"
    staging = remote / "staging.py"
    downloads = remote / "directory_downloads.py"
    assert staging.is_file()
    assert downloads.is_file()
    assert not (remote / "staging").exists()
    assert len(staging.read_text(encoding="utf-8").splitlines()) <= 500
    source = downloads.read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 500
    assert "class DownloadContext" in source
    assert "class RemoteDirectoryDownloadSession" in source
    assert "download_file_bytes" not in source
    bulk = source[source.index("async def _download_files_with_context") :]
    assert "remote_provider(file.uri)" not in bulk
    assert "await download_file_to_path(context, file" in bulk


def test_csv_projection_has_one_owner_and_cached_column_metadata() -> None:
    """CSV projection must not re-scan every planned column for every row."""
    frontend = ROOT / "cpp/src/frontends/csv"
    owner = frontend / "column_projection.cc"
    assert owner.is_file()
    assert not (frontend / "column_projection").exists()
    source = owner.read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 500
    assert "std::ranges::equal" in source
    assert "std::to_chars" in source
    assert "keep_mask_ready_ && keep_mask_.size() >= column_count" in source
    assert "first_new_column = keep_mask_.size()" in source
    assert "ensure_column_hashes(cells.size())" in source
    assert ".key_hash = column_hashes_[i]" in source
    header = (frontend / "column_projection.hh").read_text(encoding="utf-8")
    assert "std::vector<std::uint64_t> column_hashes_" in header

    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert "frontends/csv/column_projection.cc" in manifest
    assert "frontends/csv/column_projection/" not in manifest
