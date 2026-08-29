"""Tests for deterministic planning of promoted fuzz crashes.

They cover packed-input staging, bounded engine flags, symlink rejection, stable
campaign seeds, shell quoting, cleanup ownership, and delayed evidence publication.
"""

from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "meta" / "ci" / "fuzz" / "run_fuzz_regressions.py"


def _module() -> ModuleType:
    """Load the CI script as a testable module."""
    spec = importlib.util.spec_from_file_location("run_fuzz_regressions", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _runner_tree(module: ModuleType, root: Path) -> tuple[Path, Path]:
    """Create regular fuzzer binaries and one loose regression per target."""
    build_root = root / "build"
    regression_root = root / "regressions"
    build_root.mkdir()
    for target in module.TARGETS:
        module.fuzzer_binary(build_root, target).write_bytes(b"binary")
        target_root = regression_root / target
        target_root.mkdir(parents=True)
        (target_root / "case.bin").write_bytes(target.encode())
    return build_root, regression_root


def test_regression_planner_stages_stable_cases_and_commands(tmp_path: Path) -> None:
    """Every logical input is staged and assigned to its matching fuzzer."""
    module = _module()
    build_root, regression_root = _runner_tree(module, tmp_path)
    staging_root = tmp_path / "staged"

    commands = module.regression_commands(build_root, regression_root, staging_root)

    assert len(commands) == len(module.TARGETS)
    for target, command in zip(module.TARGETS, commands, strict=True):
        assert Path(command[0]) == module.fuzzer_binary(build_root, target)
        assert command[1:4] == (
            "-runs=1",
            f"-max_input_ms={module.DEFAULT_MAX_INPUT_MS}",
            f"-max_rss_mb={module.DEFAULT_MAX_RSS_MB}",
        )
        staged = Path(command[4])
        assert staged == staging_root / target / "case.bin"
        assert staged.read_bytes() == target.encode()
        assert staged.parent != regression_root / target


def test_regression_planner_materializes_repository_archives(tmp_path: Path) -> None:
    """Packed regressions become ordinary staged files before shell execution."""
    module = _module()
    build_root = tmp_path / "build"
    build_root.mkdir()
    for target in module.TARGETS:
        module.fuzzer_binary(build_root, target).write_bytes(b"binary")

    commands = module.regression_commands(
        build_root,
        ROOT / "fuzz" / "regressions",
        tmp_path / "staged",
    )

    assert len(commands) == 275
    assert all(Path(command[-1]).is_file() for command in commands)
    assert all(Path(command[-1]).is_relative_to(tmp_path / "staged") for command in commands)


def test_regression_planner_rejects_empty_regression_set(tmp_path: Path) -> None:
    """CI fails rather than claiming regression coverage with no inputs."""
    module = _module()
    build_root, regression_root = _runner_tree(module, tmp_path)
    first_target = module.TARGETS[0]
    (regression_root / first_target / "case.bin").unlink()

    with pytest.raises(RuntimeError, match="no fuzz regression inputs"):
        module.regression_commands(build_root, regression_root, tmp_path / "staged")


def test_regression_planner_rejects_a_missing_target_directory(tmp_path: Path) -> None:
    """A populated sibling cannot hide a missing parser regression set."""
    module = _module()
    build_root, regression_root = _runner_tree(module, tmp_path)
    missing = module.TARGETS[0]
    (regression_root / missing / "case.bin").unlink()
    (regression_root / missing).rmdir()

    with pytest.raises(
        FileNotFoundError,
        match=rf"missing regular fuzz target directory: .*{missing}",
    ):
        module.regression_commands(build_root, regression_root, tmp_path / "staged")


def test_target_cases_reject_a_symlinked_target(tmp_path: Path) -> None:
    """Custom roots cannot redirect one parser target outside its declared tree."""
    module = _module()
    regression_root = tmp_path / "regressions"
    real_target = tmp_path / "real-target"
    real_target.mkdir()
    regression_root.mkdir()
    target = module.TARGETS[0]
    try:
        (regression_root / target).symlink_to(real_target, target_is_directory=True)
    except OSError as error:
        pytest.skip(f"directory symlinks are unavailable: {error}")

    with pytest.raises(FileNotFoundError, match="missing regular fuzz target directory"):
        module.target_cases(regression_root, target)


def test_target_cases_reject_a_symlinked_input(tmp_path: Path) -> None:
    """A corpus entry cannot redirect reads outside its regular target directory."""
    module = _module()
    target = module.TARGETS[0]
    target_root = tmp_path / "regressions" / target
    target_root.mkdir(parents=True)
    external = tmp_path / "external"
    external.write_bytes(b"outside")
    try:
        (target_root / "case").symlink_to(external)
    except OSError as error:
        pytest.skip(f"file symlinks are unavailable: {error}")

    with pytest.raises(ValueError, match="fuzz target directories must stay flat"):
        module.target_cases(tmp_path / "regressions", target)


def test_regression_planner_rejects_a_symlinked_binary(tmp_path: Path) -> None:
    """The execution plan accepts only regular build outputs as fuzzers."""
    module = _module()
    build_root, regression_root = _runner_tree(module, tmp_path)
    target = module.TARGETS[0]
    binary = module.fuzzer_binary(build_root, target)
    replacement = build_root / "replacement"
    replacement.write_bytes(b"binary")
    binary.unlink()
    try:
        binary.symlink_to(replacement)
    except OSError as error:
        pytest.skip(f"file symlinks are unavailable: {error}")

    with pytest.raises(FileNotFoundError, match="missing regular fuzzer binary"):
        module.regression_commands(build_root, regression_root, tmp_path / "staged")


def test_campaign_planner_uses_stable_target_seeds_and_limits(tmp_path: Path) -> None:
    """Mutation campaigns remain bounded and reproducible per parser."""
    module = _module()
    build_root, regression_root = _runner_tree(module, tmp_path)
    campaign_root = tmp_path / "campaign"

    commands = module.campaign_commands(
        build_root,
        regression_root,
        campaign_root,
        runs=17,
        seed=41,
        max_length=4096,
    )

    assert len(commands) == len(module.TARGETS)
    for ordinal, (target, command) in enumerate(zip(module.TARGETS, commands, strict=True)):
        assert Path(command[0]) == module.fuzzer_binary(build_root, target)
        assert command[1:6] == (
            "-runs=17",
            f"-seed={41 + ordinal}",
            "-max_len=4096",
            f"-max_input_ms={module.DEFAULT_MAX_INPUT_MS}",
            f"-max_rss_mb={module.DEFAULT_MAX_RSS_MB}",
        )
        assert Path(command[6]) == campaign_root / target
        assert Path(command[6]).is_dir()


def test_libfuzzer_planner_uses_native_timeout_and_rss_flags(tmp_path: Path) -> None:
    """ASan and UBSan libFuzzer jobs avoid standalone-only options."""
    module = _module()
    build_root, regression_root = _runner_tree(module, tmp_path)

    commands = module.regression_commands(
        build_root,
        regression_root,
        tmp_path / "staged",
        max_input_ms=5_001,
        max_rss_mb=768,
        engine="libfuzzer",
    )

    for command in commands:
        assert command[1:4] == ("-runs=1", "-timeout=6", "-rss_limit_mb=768")
        assert all("max_input_ms" not in argument for argument in command)
        assert all("max_rss_mb" not in argument for argument in command)


def test_guard_arguments_reject_unknown_engine() -> None:
    """Configuration mistakes fail before any fuzz command is emitted."""
    module = _module()

    with pytest.raises(ValueError, match="unsupported fuzz engine"):
        module.guard_arguments("unknown", max_input_ms=1_000, max_rss_mb=128)


def test_build_plan_owns_all_staging_until_shell_execution(tmp_path: Path) -> None:
    """The generated shell, rather than the short-lived planner, owns cleanup."""
    module = _module()
    build_root, regression_root = _runner_tree(module, tmp_path)
    args = argparse.Namespace(
        build_root=build_root,
        regression_root=regression_root,
        corpus_root=regression_root,
        work_root=tmp_path / "work",
        campaign_runs=0,
        seed=41,
        max_len=4096,
        max_input_ms=5000,
        max_rss_mb=768,
        engine="standalone",
    )

    plan = module.build_plan(args)

    assert plan.staging_root.is_dir()
    assert all(Path(command[-1]).is_relative_to(plan.staging_root) for command in plan.commands)
    assert plan.regression_inputs == len(module.TARGETS)
    assert plan.mutation_runs == 0


def test_shell_plan_quotes_commands_and_publishes_evidence_last(tmp_path: Path) -> None:
    """Passing evidence appears only after quoted commands and owned cleanup setup."""
    module = _module()
    output = tmp_path / "evidence.json"
    script = tmp_path / "plan.sh"
    staging_root = tmp_path / "staging root"
    staging_root.mkdir()
    plan = module.FuzzPlan(
        commands=(("/usr/bin/true", "value; touch injected"),),
        regression_inputs=10,
        mutation_runs=4000,
        staging_root=staging_root,
    )
    evidence = {
        "status": "passed",
        "regression_inputs": 10,
        "mutation_runs": 4000,
        "sanitizer_findings": 0,
    }

    module.write_shell_plan(plan, script, evidence_output=output, evidence=evidence)

    content = script.read_text(encoding="utf-8")
    assert "trap cleanup_staging EXIT" in content
    assert "'value; touch injected'" in content
    assert content.index("/usr/bin/true") < content.index(output.as_posix())
    assert '"status": "passed"' in content
    assert not output.exists()
