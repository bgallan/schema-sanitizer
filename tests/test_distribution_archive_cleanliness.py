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
    ],
)
def test_source_zip_validator_rejects_coverage_and_profile_artifacts(
    tmp_path: Path,
    scratch_name: str,
) -> None:
    """Coverage databases and profiler output never ship in source archives."""
    validator = _load_validator()
    names = [*_minimal_source_names(validator), scratch_name]

    with pytest.raises(AssertionError, match="contains scratch/build files"):
        validator._validate_source_zip(tmp_path / "source.zip", names)


@pytest.mark.parametrize(
    "scratch_name",
    [
        "build-memsec/libsanitize_core.a",
        "build-asan/CMakeCache.txt",
        "cmake-build-debug/module.obj",
        ".build-local/generated.o",
    ],
)
def test_source_zip_validator_rejects_root_build_variants(
    tmp_path: Path,
    scratch_name: str,
) -> None:
    """Root build directories remain scratch even when their names carry suffixes."""
    validator = _load_validator()
    names = [*_minimal_source_names(validator), scratch_name]

    with pytest.raises(AssertionError, match="contains scratch/build files"):
        validator._validate_source_zip(tmp_path / "source.zip", names)
