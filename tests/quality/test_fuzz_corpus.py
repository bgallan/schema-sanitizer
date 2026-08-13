"""Contracts for byte-exact fuzz inputs and isolated campaign staging."""

from __future__ import annotations

import hashlib
import importlib.util
import shutil
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CHECKER = ROOT / "meta" / "ci" / "fuzz" / "check_fuzz_corpus.py"
RUNNER = ROOT / "meta" / "ci" / "fuzz" / "run_fuzz_regressions.py"


def _module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _minimal_fuzz_tree(root: Path, targets: tuple[str, ...]) -> None:
    (root / "README.md").parent.mkdir(parents=True)
    (root / "README.md").write_text("fuzz\n", encoding="utf-8")
    (root / "regressions" / "README.md").parent.mkdir(parents=True)
    (root / "regressions" / "README.md").write_text("regressions\n", encoding="utf-8")
    for role in ("corpus", "regressions"):
        for target in targets:
            target_root = root / role / target
            target_root.mkdir(parents=True)
            data = f"{role}-{target}".encode()
            if role == "regressions":
                name = hashlib.sha1(data, usedforsecurity=False).hexdigest()
            else:
                name = "seed"
            (target_root / name).write_bytes(data)


def _copy_repository_fuzz_tree(destination: Path) -> Path:
    return shutil.copytree(ROOT / "fuzz", destination / "fuzz")


def test_repository_fuzz_tree_has_valid_content_addresses() -> None:
    """Committed libFuzzer-style names must describe their exact bytes."""
    checker = _module(CHECKER, "check_fuzz_corpus_repository")

    inventory = checker.check_fuzz_tree(ROOT / "fuzz")

    assert inventory.corpus_inputs == 25
    assert inventory.regression_inputs == 275
    assert inventory.content_addressed_inputs == 265
    assert inventory.unique_campaign_inputs == 295


def test_fuzz_checker_rejects_hash_drift_and_nested_inputs(tmp_path: Path) -> None:
    """Formatting damage and layout changes must fail with actionable paths."""
    checker = _module(CHECKER, "check_fuzz_corpus_invalid")
    fuzz_root = tmp_path / "fuzz"
    _minimal_fuzz_tree(fuzz_root, checker.TARGETS)
    regression = fuzz_root / "regressions" / checker.TARGETS[0]
    hashed = next(regression.iterdir())
    hashed.write_bytes(hashed.read_bytes() + b"\n")
    nested = fuzz_root / "corpus" / checker.TARGETS[1] / "nested"
    nested.mkdir()
    (nested / "seed").write_bytes(b"nested")

    with pytest.raises(checker.FuzzCorpusError) as raised:
        checker.check_fuzz_tree(fuzz_root)

    message = str(raised.value)
    assert "content hash mismatch" in message
    assert "fuzz target directories must stay flat" in message


def test_fuzz_checker_rejects_an_empty_target(tmp_path: Path) -> None:
    """Every parser keeps both curated seeds and promoted regressions."""
    checker = _module(CHECKER, "check_fuzz_corpus_empty_target")
    fuzz_root = tmp_path / "fuzz"
    _minimal_fuzz_tree(fuzz_root, checker.TARGETS)
    target = checker.TARGETS[0]
    for path in (fuzz_root / "corpus" / target).iterdir():
        path.unlink()

    with pytest.raises(
        checker.FuzzCorpusError,
        match=rf"no fuzz inputs found under corpus/{target}",
    ):
        checker.check_fuzz_tree(fuzz_root)


def test_fuzz_checker_rejects_descriptive_corpus_drift(tmp_path: Path) -> None:
    """The tree fingerprint protects curated seeds without content-addressed names."""
    checker = _module(CHECKER, "check_fuzz_corpus_descriptive_seed")
    fuzz_root = _copy_repository_fuzz_tree(tmp_path)
    fixture = fuzz_root / "corpus" / "json" / "invalid-utf8.json"
    fixture.write_bytes(fixture.read_bytes() + b"\x00")

    with pytest.raises(checker.FuzzCorpusError, match="fuzz tree fingerprint mismatch"):
        checker.check_fuzz_tree(fuzz_root)


def test_fuzz_checker_rejects_unmanifested_regression_drift(tmp_path: Path) -> None:
    """The tree fingerprint also protects every descriptive regression."""
    checker = _module(CHECKER, "check_fuzz_corpus_descriptive_regression")
    fuzz_root = _copy_repository_fuzz_tree(tmp_path)
    fixture = fuzz_root / "regressions" / "xml" / "duplicate-attribute.xml"
    fixture.write_bytes(fixture.read_bytes() + b"\x00")

    with pytest.raises(checker.FuzzCorpusError, match="fuzz tree fingerprint mismatch"):
        checker.check_fuzz_tree(fuzz_root)


def test_fuzz_checker_rejects_deleted_content_addressed_input(tmp_path: Path) -> None:
    """Exact per-target counts prevent silent regression-case removal."""
    checker = _module(CHECKER, "check_fuzz_corpus_deleted_regression")
    fuzz_root = _copy_repository_fuzz_tree(tmp_path)
    target_root = fuzz_root / "regressions" / "csv"
    fixture = next(
        path for path in sorted(target_root.iterdir()) if checker.SHA1_NAME.fullmatch(path.name)
    )
    fixture.unlink()

    with pytest.raises(checker.FuzzCorpusError) as raised:
        checker.check_fuzz_tree(fuzz_root)

    message = str(raised.value)
    assert "unexpected input count under regressions/csv" in message
    assert "fuzz tree fingerprint mismatch" in message


@pytest.mark.parametrize(
    ("role", "replacement"),
    [("corpus", "file"), ("regressions", "symlink")],
)
def test_fuzz_checker_rejects_non_regular_target_directories(
    role: str,
    replacement: str,
    tmp_path: Path,
) -> None:
    """Target containers cannot redirect or hide their checked inputs."""
    checker = _module(CHECKER, f"check_fuzz_corpus_target_{replacement}")
    fuzz_root = _copy_repository_fuzz_tree(tmp_path)
    target_root = fuzz_root / role / "json"
    shutil.rmtree(target_root)
    if replacement == "file":
        target_root.write_bytes(b"not a directory")
    else:
        try:
            target_root.symlink_to(fuzz_root / role / "csv", target_is_directory=True)
        except OSError as error:
            pytest.skip(f"directory symlinks are unavailable: {error}")

    with pytest.raises(
        checker.FuzzCorpusError,
        match=rf"missing regular directory: {role}/json",
    ):
        checker.check_fuzz_tree(fuzz_root)


@pytest.mark.parametrize(
    "fixture",
    [
        "csv/unterminated.csv",
        "json/truncated.json",
        "parquet/truncated.parquet",
        "xml/mismatched.xml",
    ],
)
def test_fuzz_checker_rejects_descriptive_fixture_drift(
    fixture: str,
    tmp_path: Path,
) -> None:
    """The descriptive regressions required in sdists remain byte-exact."""
    checker = _module(CHECKER, f"check_fuzz_corpus_fixture_{fixture.replace('/', '_')}")
    fuzz_root = tmp_path / "fuzz"
    _minimal_fuzz_tree(fuzz_root, checker.TARGETS)
    for relative, expected in checker.DESCRIPTIVE_REGRESSION_SHA256.items():
        source = ROOT / "fuzz" / "regressions" / relative
        destination = fuzz_root / "regressions" / relative
        destination.write_bytes(source.read_bytes())
        assert hashlib.sha256(destination.read_bytes()).hexdigest() == expected
    target = fuzz_root / "regressions" / fixture
    target.write_bytes(target.read_bytes() + b"\n")

    with pytest.raises(checker.FuzzCorpusError, match="descriptive fixture hash mismatch"):
        checker.check_fuzz_tree(fuzz_root)


def test_campaigns_stage_a_temporary_deduplicated_union(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Campaign writes stay outside the checkout while both seed roles are used."""
    runner = _module(RUNNER, "run_fuzz_regressions_staging")
    build_root = tmp_path / "build"
    regression_root = tmp_path / "regressions"
    corpus_root = tmp_path / "corpus"
    calls: list[list[str]] = []
    staged_roots: list[Path] = []

    build_root.mkdir()
    for target in runner.TARGETS:
        runner.fuzzer_binary(build_root, target).write_bytes(b"binary")
        regression_target = regression_root / target
        corpus_target = corpus_root / target
        regression_target.mkdir(parents=True)
        corpus_target.mkdir(parents=True)
        shared = f"shared-{target}".encode()
        (regression_target / "regression").write_bytes(f"regression-{target}".encode())
        (regression_target / "shared").write_bytes(shared)
        (corpus_target / "corpus").write_bytes(f"corpus-{target}".encode())
        (corpus_target / "shared-copy").write_bytes(shared)

    def fake_run(command: list[str], *, check: bool) -> None:
        assert check is True
        staged = Path(command[-1])
        staged_roots.append(staged)
        assert staged.parent not in {regression_root, corpus_root}
        assert len(list(staged.iterdir())) == 3
        calls.append(command)

    monkeypatch.setattr(runner.subprocess, "run", fake_run)
    assert runner.run_campaigns(
        build_root,
        regression_root,
        corpus_root=corpus_root,
        runs=17,
        seed=41,
        max_length=4096,
    ) == 17 * len(runner.TARGETS)

    assert len(calls) == len(runner.TARGETS)
    assert all(not staged.exists() for staged in staged_roots)
    assert all(len(list((regression_root / target).iterdir())) == 2 for target in runner.TARGETS)
    assert all(len(list((corpus_root / target).iterdir())) == 2 for target in runner.TARGETS)
