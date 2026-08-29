"""Contracts for byte-exact fuzz inputs and isolated campaign staging.

It checks content-addressed corpus structure and manifests, rejects drift and unsafe
archives, and builds isolated deduplicated campaigns.
"""

from __future__ import annotations

import hashlib
import importlib.util
import shutil
import stat
import zipfile
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]
CHECKER = ROOT / "meta" / "ci" / "fuzz" / "check_fuzz_corpus.py"
RUNNER = ROOT / "meta" / "ci" / "fuzz" / "run_fuzz_regressions.py"
PACKER = ROOT / "meta" / "ci" / "fuzz" / "pack_fuzz_regressions.py"


def _module(path: Path, name: str) -> ModuleType:
    """Load the CI helper module under test from its repository path."""
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _minimal_fuzz_tree(root: Path, targets: tuple[str, ...]) -> None:
    """Create the smallest valid native fuzz corpus layout."""
    (root / "README.md").parent.mkdir(parents=True)
    (root / "README.md").write_text("fuzz\n", encoding="utf-8")
    (root / "regressions" / "README.md").parent.mkdir(parents=True)
    (root / "regressions" / "README.md").write_text("regressions\n", encoding="utf-8")
    for role in ("corpus", "regressions"):
        for target in targets:
            target_root = root / role / target
            target_root.mkdir(parents=True)
            data = f"{role}-{target}".encode()
            name = "regression" if role == "regressions" else "seed"
            (target_root / name).write_bytes(data)


def _copy_repository_fuzz_tree(destination: Path) -> Path:
    """Copy the maintained fuzz corpus into an isolated test directory."""
    return shutil.copytree(ROOT / "fuzz", destination / "fuzz")


def test_repository_fuzz_tree_has_a_valid_sha256_manifest() -> None:
    """The canonical SHA-256 tree fingerprint protects every committed input."""
    checker = _module(CHECKER, "check_fuzz_corpus_repository")

    inventory = checker.check_fuzz_tree(ROOT / "fuzz")

    assert inventory.corpus_inputs == 25
    assert inventory.regression_inputs == 275
    assert inventory.legacy_libfuzzer_inputs == 265
    assert inventory.unique_campaign_inputs == 295


def test_deep_json_seed_keeps_its_boundary_without_generated_whitespace() -> None:
    """The depth-limit seed stays compact while retaining 513 nested members."""
    data = (ROOT / "fuzz" / "corpus" / "json" / "depth-513.json").read_bytes()

    assert len(data) < 4_096
    assert data.count(b'{"n":') == 513
    assert data.count(b"{") == data.count(b"}") == 514


def test_fuzz_checker_rejects_byte_drift_and_nested_inputs(tmp_path: Path) -> None:
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
    assert "fuzz tree fingerprint mismatch" in message
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


def test_fuzz_checker_rejects_deleted_archived_inputs(tmp_path: Path) -> None:
    """Exact per-target counts prevent silent regression-case removal."""
    checker = _module(CHECKER, "check_fuzz_corpus_deleted_regression")
    fuzz_root = _copy_repository_fuzz_tree(tmp_path)
    (fuzz_root / "regressions" / "csv.sha1.zip").unlink()

    with pytest.raises(checker.FuzzCorpusError) as raised:
        checker.check_fuzz_tree(fuzz_root)

    message = str(raised.value)
    assert "unexpected input count under regressions/csv" in message
    assert "fuzz tree fingerprint mismatch" in message


def test_fuzz_packer_is_deterministic_and_removes_only_hashed_files(tmp_path: Path) -> None:
    """Packed regressions retain stable names and byte-exact contents."""
    packer = _module(PACKER, "pack_fuzz_regressions_determinism")
    regression_root = tmp_path / "regressions"
    target = packer.TARGETS[0]
    target_root = regression_root / target
    target_root.mkdir(parents=True)
    expected: dict[str, bytes] = {}
    for data in (b"first regression", b"second regression"):
        name = hashlib.sha1(data, usedforsecurity=False).hexdigest()
        expected[name] = data
        (target_root / name).write_bytes(data)
    descriptive = target_root / "hand-maintained.json"
    descriptive.write_bytes(b"descriptive")

    assert packer.pack_target(regression_root, target, remove_loose=True) == 2
    archive = regression_root / f"{target}.sha1.zip"
    first_bytes = archive.read_bytes()
    first_mtime = archive.stat().st_mtime_ns
    assert packer.pack_target(regression_root, target, remove_loose=True) == 2
    assert archive.read_bytes() == first_bytes
    assert archive.stat().st_mtime_ns == first_mtime
    assert stat.S_IMODE(archive.stat().st_mode) == 0o644
    assert descriptive.read_bytes() == b"descriptive"
    assert sorted(path.name for path in target_root.iterdir()) == [descriptive.name]
    with zipfile.ZipFile(archive) as packed:
        assert packed.namelist() == sorted(expected)
        assert {name: packed.read(name) for name in packed.namelist()} == expected


def test_fuzz_packer_recovers_after_interrupted_loose_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cleanup failure leaves a complete archive that remove-loose can resume."""
    packer = _module(PACKER, "pack_fuzz_regressions_cleanup_retry")
    regression_root = tmp_path / "regressions"
    target = packer.TARGETS[0]
    target_root = regression_root / target
    target_root.mkdir(parents=True)
    expected: dict[str, bytes] = {}
    for data in (b"first retry case", b"second retry case"):
        name = hashlib.sha1(data, usedforsecurity=False).hexdigest()
        expected[name] = data
        (target_root / name).write_bytes(data)

    failing_path = target_root / sorted(expected)[1]
    original_unlink = Path.unlink
    failure_injected = False

    def unlink_with_one_failure(path: Path, *, missing_ok: bool = False) -> None:
        """Fail exactly once while removing the second archived loose input."""
        nonlocal failure_injected
        if path == failing_path and not failure_injected:
            failure_injected = True
            raise OSError("injected loose cleanup failure")
        original_unlink(path, missing_ok=missing_ok)

    with monkeypatch.context() as cleanup_patch:
        cleanup_patch.setattr(Path, "unlink", unlink_with_one_failure)
        with pytest.raises(OSError, match="injected loose cleanup failure"):
            packer.pack_target(regression_root, target, remove_loose=True)

    archive = regression_root / f"{target}.sha1.zip"
    committed_bytes = archive.read_bytes()
    with zipfile.ZipFile(archive) as packed:
        assert {name: packed.read(name) for name in packed.namelist()} == expected
    assert sorted(path.name for path in target_root.iterdir()) == [failing_path.name]
    with pytest.raises(ValueError, match="duplicate loose and archived fuzz input"):
        packer.pack_target(regression_root, target, remove_loose=False)

    assert packer.pack_target(regression_root, target, remove_loose=True) == 2
    assert archive.read_bytes() == committed_bytes
    assert not any(target_root.iterdir())


def test_fuzz_packer_keeps_loose_inputs_when_archive_commit_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed atomic archive replacement cannot start destructive cleanup."""
    packer = _module(PACKER, "pack_fuzz_regressions_commit_failure")
    regression_root = tmp_path / "regressions"
    target = packer.TARGETS[0]
    target_root = regression_root / target
    target_root.mkdir(parents=True)
    data = b"preserved until archive commit"
    name = hashlib.sha1(data, usedforsecurity=False).hexdigest()
    loose = target_root / name
    loose.write_bytes(data)

    def fail_replace(source: Path, destination: Path) -> None:
        """Inject one archive commit failure before loose cleanup begins."""
        raise OSError(f"injected archive commit failure: {source} -> {destination}")

    with monkeypatch.context() as commit_patch:
        commit_patch.setattr(packer.os, "replace", fail_replace)
        with pytest.raises(OSError, match="injected archive commit failure"):
            packer.pack_target(regression_root, target, remove_loose=True)

    archive = regression_root / f"{target}.sha1.zip"
    assert loose.read_bytes() == data
    assert not archive.exists()
    assert not any(path.name.endswith(".tmp") for path in regression_root.iterdir())

    assert packer.pack_target(regression_root, target, remove_loose=True) == 1
    assert archive.is_file()
    assert not loose.exists()


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


def test_fuzz_checker_rejects_a_symlinked_role_without_traversing_it(tmp_path: Path) -> None:
    """Corpus roles cannot redirect validation outside the declared fuzz tree."""
    checker = _module(CHECKER, "check_fuzz_corpus_role_symlink")
    fuzz_root = _copy_repository_fuzz_tree(tmp_path)
    corpus_root = fuzz_root / "corpus"
    external = tmp_path / "external-corpus"
    corpus_root.rename(external)
    try:
        corpus_root.symlink_to(external, target_is_directory=True)
    except OSError as error:
        pytest.skip(f"directory symlinks are unavailable: {error}")

    with pytest.raises(checker.FuzzCorpusError) as raised:
        checker.check_fuzz_tree(fuzz_root)

    message = str(raised.value)
    assert "missing regular directory: corpus" in message
    assert "corpus/json" not in message


def test_fuzz_checker_rejects_a_symlinked_input(tmp_path: Path) -> None:
    """Individual corpus files cannot redirect byte reads outside their target."""
    checker = _module(CHECKER, "check_fuzz_corpus_input_symlink")
    fuzz_root = _copy_repository_fuzz_tree(tmp_path)
    fixture = fuzz_root / "corpus" / "json" / "array.json"
    external = tmp_path / "outside-input"
    external.write_bytes(fixture.read_bytes())
    fixture.unlink()
    try:
        fixture.symlink_to(external)
    except OSError as error:
        pytest.skip(f"file symlinks are unavailable: {error}")

    with pytest.raises(
        checker.FuzzCorpusError,
        match="fuzz target directories must stay flat",
    ):
        checker.check_fuzz_tree(fuzz_root)


def test_fuzz_checker_rejects_a_dangling_archive_symlink(tmp_path: Path) -> None:
    """A missing archive cannot be disguised as an allowed dangling link."""
    checker = _module(CHECKER, "check_fuzz_corpus_archive_symlink")
    fuzz_root = _copy_repository_fuzz_tree(tmp_path)
    archive = fuzz_root / "regressions" / "json.sha1.zip"
    archive.unlink()
    try:
        archive.symlink_to(tmp_path / "missing-archive")
    except OSError as error:
        pytest.skip(f"file symlinks are unavailable: {error}")

    with pytest.raises(
        checker.FuzzCorpusError,
        match="fuzz archive must be a regular file",
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
    tmp_path: Path,
) -> None:
    """Campaign writes stay outside the checkout while both seed roles are used."""
    runner = _module(RUNNER, "run_fuzz_regressions_staging")
    build_root = tmp_path / "build"
    regression_root = tmp_path / "regressions"
    corpus_root = tmp_path / "corpus"
    campaign_root = tmp_path / "staged"
    campaign_root.mkdir()

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

    commands = runner.campaign_commands(
        build_root,
        regression_root,
        campaign_root,
        corpus_root=corpus_root,
        runs=17,
        seed=41,
        max_length=4096,
    )

    assert len(commands) == len(runner.TARGETS)
    staged_roots = [Path(command[-1]) for command in commands]
    assert all(staged.parent == campaign_root for staged in staged_roots)
    assert all(len(list(staged.iterdir())) == 3 for staged in staged_roots)
    assert all(len(list((regression_root / target).iterdir())) == 2 for target in runner.TARGETS)
    assert all(len(list((corpus_root / target).iterdir())) == 2 for target in runner.TARGETS)
