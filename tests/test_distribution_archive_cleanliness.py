"""Tests for source archive completeness and scratch-file rejection."""

from __future__ import annotations

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType

import pytest


def _load_validator() -> ModuleType:
    """Load the distribution validator without packaging ``meta``."""
    path = Path(__file__).parents[1] / "meta" / "ci" / "check_distribution_contents.py"
    spec = spec_from_file_location("check_distribution_contents", path)
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _minimal_source_names(validator: ModuleType) -> list[str]:
    """Return a complete minimal source-ZIP member set."""
    return sorted(
        {
            *validator._SDIST_REQUIRED,
            "cpp/src/minimal.cpp",
            "tests/test_minimal.py",
        }
    )


def test_source_zip_validator_accepts_clean_source_tree(tmp_path: Path) -> None:
    """A complete source ZIP without generated files is accepted."""
    validator = _load_validator()

    validator._validate_source_zip(
        tmp_path / "source.zip",
        _minimal_source_names(validator),
    )


@pytest.mark.parametrize(
    "scratch_name",
    [
        ".coverage",
        "coverage.xml",
        "profiles/parser.profraw",
        "profiles/parser.profdata",
        "coverage/native.gcda",
        "coverage/native.gcno",
        "build-memsec/libsanitize_core.a",
        "build-asan/CMakeCache.txt",
        "cmake-build-debug/module.obj",
        ".build-local/generated.o",
    ],
)
def test_source_zip_validator_rejects_scratch_artifacts(
    tmp_path: Path,
    scratch_name: str,
) -> None:
    """Coverage, profiler, and build scratch files never ship in source archives."""
    validator = _load_validator()
    names = [*_minimal_source_names(validator), scratch_name]

    with pytest.raises(AssertionError, match="contains scratch/build files"):
        validator._validate_source_zip(tmp_path / "source.zip", names)


def test_release_filename_validator_requires_all_supported_wheels() -> None:
    """One consistent sdist and the four release platforms form a valid set."""
    validator = _load_validator()
    validator.validate_release_filenames(
        [
            "schema_sanitizer-0.4.0.tar.gz",
            "schema_sanitizer-0.4.0-cp311-abi3-manylinux_2_28_x86_64.whl",
            "schema_sanitizer-0.4.0-cp311-abi3-win_amd64.whl",
            "schema_sanitizer-0.4.0-cp311-abi3-macosx_11_0_x86_64.whl",
            "schema_sanitizer-0.4.0-cp311-abi3-macosx_11_0_arm64.whl",
        ]
    )


def test_release_filename_validator_rejects_version_drift() -> None:
    """A stale platform wheel cannot enter a release set."""
    validator = _load_validator()

    with pytest.raises(AssertionError, match="mismatched distribution versions"):
        validator.validate_release_filenames(
            [
                "schema_sanitizer-0.4.0.tar.gz",
                "schema_sanitizer-0.4.0-cp311-abi3-manylinux_2_28_x86_64.whl",
                "schema_sanitizer-0.4.0-cp311-abi3-win_amd64.whl",
                "schema_sanitizer-0.4.0-cp311-abi3-macosx_11_0_x86_64.whl",
                "schema_sanitizer-0.3.9-cp311-abi3-macosx_11_0_arm64.whl",
            ]
        )
