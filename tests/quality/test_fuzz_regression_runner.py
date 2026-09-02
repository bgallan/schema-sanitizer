"""Tests for deterministic planning of promoted fuzz crashes.

They cover packed-input staging, bounded engine flags, symlink rejection, stable
campaign seeds, shell quoting, cleanup ownership, and delayed evidence publication.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shutil
import subprocess
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "meta" / "ci" / "fuzz" / "run_fuzz_regressions.py"
STANDALONE_RUNNER = ROOT / "cpp" / "fuzz" / "standalone_main.cc"


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


def _compile_recording_standalone_runner(tmp_path: Path) -> Path:
    """Build the standalone driver with a target that records every input."""
    compiler = shutil.which("c++") or shutil.which("clang++") or shutil.which("g++")
    assert compiler is not None, "a C++ compiler is required for the mutation golden test"
    target_source = tmp_path / "record_fuzz_inputs.cc"
    target_source.write_text(
        """\
#include <array>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>

#define main schema_sanitizer_standalone_main
#include "cpp/fuzz/standalone_main.cc"
#undef main

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data,
                                      std::size_t size) {
  std::cout << std::hex << std::setfill('0');
  for (std::size_t index = 0; index < size; ++index) {
    std::cout << std::setw(2) << static_cast<unsigned>(data[index]);
  }
  std::cout << std::dec << '\\n';
  return 0;
}

int main(int argc, char **argv) {
  if (argc == 2 && std::string_view(argv[1]) == "--bounded-random-golden") {
    std::mt19937_64 random(41U);
    constexpr std::array<std::uint64_t, 5> bounds{
        3U,
        10U,
        17U,
        (std::uint64_t{1} << 63U) + 1U,
        std::numeric_limits<std::uint64_t>::max(),
    };
    for (const auto bound : bounds) {
      for (unsigned sample = 0; sample < 4U; ++sample) {
        std::cout << random_below(random, bound) << '\\n';
      }
    }
    return 0;
  }
  return schema_sanitizer_standalone_main(argc, argv);
}
""",
        encoding="utf-8",
    )
    executable = tmp_path / ("record_fuzz_inputs.exe" if os.name == "nt" else "record_fuzz_inputs")
    command = [
        compiler,
        "-std=c++23",
        "-O0",
        "-I",
        str(ROOT),
        str(target_source),
        "-o",
        str(executable),
    ]
    if os.name == "nt":
        command.extend(("-static-libgcc", "-static-libstdc++", "-lpsapi"))
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        cwd=ROOT,
        timeout=120,
    )
    assert completed.returncode == 0, completed.stderr
    return executable


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


def test_standalone_mutation_stream_matches_its_cross_library_golden(
    tmp_path: Path,
) -> None:
    """Fixed seed mutation bytes stay independent of standard-library distributions."""
    executable = _compile_recording_standalone_runner(tmp_path)
    seed = tmp_path / "seed"
    seed.write_bytes(b"abcd")
    command = [
        str(executable),
        "-runs=16",
        "-seed=41",
        "-max_len=16",
        "-max_input_ms=5000",
        "-max_rss_mb=0",
        str(seed),
    ]
    outputs: list[list[str]] = []
    for _ in range(2):
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert completed.returncode == 0, completed.stderr
        outputs.append(completed.stdout.splitlines())

    assert outputs[0] == outputs[1]
    runner_source = STANDALONE_RUNNER.read_text(encoding="utf-8")
    assert "uniform_int_distribution" not in runner_source
    assert "GetProcessMemoryInfo" in runner_source

    guarded_command = command.copy()
    guarded_command[1] = "-runs=1"
    guarded_command[5] = "-max_rss_mb=1000000"
    guarded = subprocess.run(
        guarded_command,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert guarded.returncode == 0, guarded.stderr

    overflow = subprocess.run(
        [
            str(executable),
            "-runs=1",
            "-max_rss_mb=17592186044416",
            str(seed),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert overflow.returncode == 2
    assert "RSS limit is too large" in overflow.stderr

    bounded = subprocess.run(
        [str(executable), "--bounded-random-golden"],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert bounded.returncode == 0, bounded.stderr
    assert bounded.stdout.splitlines() == [
        "1",
        "2",
        "2",
        "1",
        "5",
        "7",
        "5",
        "0",
        "4",
        "1",
        "5",
        "10",
        "3776483655231910937",
        "4614963366957433368",
        "3795442861040559494",
        "4082647064111992146",
        "6628579074409753543",
        "13884036159728665350",
        "12874508976105387110",
        "6827436633806237924",
    ]
    assert outputs[0] == [
        "61626364",
        "61636364",
        "196173628e64",  # pragma: allowlist secret
        "6161426462636464",
        "6161626343646243646263646162",
        "",
        "",
        "82da207ee5a99c896263641b",  # pragma: allowlist secret
        "616263",
        "0197422d497f8d4301",  # pragma: allowlist secret
        "6162636446",
        "71524c63",
        "907507bee6df9861",  # pragma: allowlist secret
        "",
        "85148a5c85c5c6d6",
        "b34ebaaaf8",
        "standalone fuzz runs completed: 16",
    ]


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

    stale = plan.staging_root / "stale"
    stale.write_text("old run", encoding="utf-8")
    replacement = module.build_plan(args)
    assert replacement.staging_root == plan.staging_root
    assert not stale.exists()


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
    assert content.index("/usr/bin/true") < content.index("mv -f --")
    assert '"status": "passed"' in content
    assert not output.exists()

    bash = shutil.which("bash")
    assert bash is not None
    completed = subprocess.run(
        [bash, script.as_posix()], check=False, capture_output=True, text=True
    )
    assert completed.returncode == 0, completed.stderr
    assert json.loads(output.read_text(encoding="utf-8")) == evidence
    assert not staging_root.exists()


def test_failed_fuzz_plan_removes_stale_passing_evidence(tmp_path: Path) -> None:
    """A rerun clears old success evidence before executing a failing command."""
    module = _module()
    output = tmp_path / "evidence.json"
    output.write_text('{"status":"passed"}\n', encoding="utf-8")
    script = tmp_path / "plan.sh"
    staging_root = tmp_path / "staging"
    staging_root.mkdir()
    plan = module.FuzzPlan(
        commands=(("/usr/bin/false",),),
        regression_inputs=1,
        mutation_runs=0,
        staging_root=staging_root,
    )

    module.write_shell_plan(
        plan,
        script,
        evidence_output=output,
        evidence={"status": "passed"},
    )
    bash = shutil.which("bash")
    assert bash is not None
    completed = subprocess.run([bash, script.as_posix()], check=False)

    assert completed.returncode != 0
    assert not output.exists()
    assert not staging_root.exists()


def test_fuzz_outputs_cannot_overlap_staging_executables_or_inputs(tmp_path: Path) -> None:
    """Plan and evidence publication cannot overwrite any cleanup or command owner."""
    module = _module()
    staging_root = tmp_path / "staging"
    staging_root.mkdir()
    binary = tmp_path / "fuzzer"
    binary.write_bytes(b"binary")
    command_input = tmp_path / "external-input"
    command_input.write_bytes(b"input")
    regression_root = tmp_path / "regression-source"
    regression_root.mkdir()
    source_input = regression_root / "source.bin"
    source_input.write_bytes(b"source")
    plan = module.FuzzPlan(
        commands=((binary.as_posix(), command_input.as_posix()),),
        regression_inputs=1,
        mutation_runs=0,
        staging_root=staging_root,
        source_roots=(("regression root", regression_root),),
    )
    valid_output = tmp_path / "commands.sh"
    invalid = (
        (staging_root / "commands.sh", None, "outside owned staging"),
        (binary, None, "command 0 executable"),
        (valid_output, command_input, "command 0 input"),
        (source_input, None, "stay outside regression root"),
        (valid_output, valid_output / "evidence.json", "outputs must be disjoint"),
    )

    for command_output, evidence_output, message in invalid:
        with pytest.raises(ValueError, match=message):
            module.write_shell_plan(
                plan,
                command_output,
                evidence_output=evidence_output,
                evidence={"status": "passed"},
            )
    assert binary.read_bytes() == b"binary"
    assert command_input.read_bytes() == b"input"
    assert source_input.read_bytes() == b"source"


def test_fuzz_source_overlap_fails_before_staging_cleanup(tmp_path: Path) -> None:
    """A cleanup-owned build root is rejected without deleting its fuzzer."""
    module = _module()
    work_root = tmp_path / "work"
    staging_root = work_root / "staging"
    build_root = staging_root / "build"
    regression_root = tmp_path / "regressions"
    build_root.mkdir(parents=True)
    regression_root.mkdir()
    sentinel = build_root / "preserve"
    sentinel.write_bytes(b"binary")
    args = argparse.Namespace(
        build_root=build_root,
        regression_root=regression_root,
        corpus_root=regression_root,
        work_root=work_root,
        campaign_runs=0,
        seed=41,
        max_len=4096,
        max_input_ms=5000,
        max_rss_mb=768,
        engine="standalone",
    )

    with pytest.raises(ValueError, match="build root must stay outside owned staging"):
        module.build_plan(args)
    assert sentinel.read_bytes() == b"binary"


@pytest.mark.parametrize(("primary_status", "expected_status"), ((0, 23), (7, 7)))
def test_fuzz_cleanup_traps_report_cleanup_only_failures(
    tmp_path: Path,
    primary_status: int,
    expected_status: int,
) -> None:
    """Generated and outer traps fail success but preserve an existing failure."""
    module = _module()
    staging_root = tmp_path / f"staging-{primary_status}"
    staging_root.mkdir()
    generated = tmp_path / f"generated-{primary_status}.sh"
    module.write_shell_plan(
        module.FuzzPlan((), 0, 0, staging_root),
        generated,
        evidence_output=None,
        evidence={},
    )
    wrapper = SCRIPT.with_suffix(".sh")
    scripts = (
        (generated.read_text(encoding="utf-8"), "trap cleanup_staging EXIT"),
        (wrapper.read_text(encoding="utf-8"), "trap cleanup_plan EXIT"),
    )
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir(exist_ok=True)
    fake_rm = fake_bin / "rm"
    required_commands = {
        command: shutil.which(command) for command in ("bash", "dirname", "mktemp")
    }
    assert all(required_commands.values())
    bash = required_commands["bash"]
    assert bash is not None
    fake_rm.write_text("#!/usr/bin/env bash\nexit 23\n", encoding="utf-8")
    fake_rm.chmod(0o755)
    temporary_root = tmp_path / "temporary"
    temporary_root.mkdir(exist_ok=True)
    tool_directories = sorted(
        {str(Path(command).parent) for command in required_commands.values() if command is not None}
    )
    environment = {
        "LC_ALL": "C",
        "PATH": os.pathsep.join((str(fake_bin), *tool_directories)),
        "TMPDIR": str(temporary_root),
        "TZ": "UTC",
    }

    for index, (content, marker) in enumerate(scripts):
        end = content.index(marker) + len(marker)
        preamble = content[:end] + f"\nexit {primary_status}\n"
        preamble_path = tmp_path / f"cleanup-preamble-{primary_status}-{index}.sh"
        preamble_path.write_text(preamble, encoding="utf-8")
        completed = subprocess.run(
            [bash, preamble_path.as_posix()],
            check=False,
            cwd=ROOT,
            env=environment,
        )
        assert completed.returncode == expected_status


def test_fuzz_wrapper_keeps_owned_plan_locations_authoritative() -> None:
    """Caller options cannot override the wrapper's process-owned output paths."""
    wrapper = SCRIPT.with_suffix(".sh").read_text(encoding="utf-8")

    assert wrapper.index('"$@"') < wrapper.index("--work-root")
    assert wrapper.index('"$@"') < wrapper.index("--command-output")
