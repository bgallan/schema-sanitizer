"""Tests for deterministic execution of promoted fuzz crashes.

It validates stable crash discovery, bounded campaign commands, symlink rejection,
temporary staging, and machine-readable evidence.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "meta" / "ci" / "fuzz" / "run_fuzz_regressions.py"


def _module():
    """Load the CI script as a testable module."""
    spec = importlib.util.spec_from_file_location("run_fuzz_regressions", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_regression_runner_discovers_stable_cases_and_commands(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Every parser directory is executed against its matching fuzzer binary."""
    module = _module()
    build_root = tmp_path / "build"
    regression_root = tmp_path / "regressions"
    calls: list[list[str]] = []
    staged_inputs: list[tuple[Path, bytes]] = []

    for target in module.TARGETS:
        build_root.mkdir(exist_ok=True)
        module.fuzzer_binary(build_root, target).write_bytes(b"binary")
        target_root = regression_root / target
        target_root.mkdir(parents=True)
        (target_root / "case.bin").write_bytes(target.encode())

    def fake_run(command: list[str], *, check: bool) -> None:
        """Capture one deterministic fuzzer invocation."""
        assert check is True
        path = Path(command[-1])
        staged_inputs.append((path, path.read_bytes()))
        calls.append(command)

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    assert module.run_regressions(build_root, regression_root) == len(module.TARGETS)
    assert len(calls) == len(module.TARGETS)
    for target, command in zip(module.TARGETS, calls, strict=True):
        assert Path(command[0]) == module.fuzzer_binary(build_root, target)
        assert command[1] == "-runs=1"
        assert command[2:4] == [
            f"-max_input_ms={module.DEFAULT_MAX_INPUT_MS}",
            f"-max_rss_mb={module.DEFAULT_MAX_RSS_MB}",
        ]
        assert Path(command[4]).name == "case.bin"
        assert Path(command[4]).parent != regression_root / target
    assert [data for _path, data in staged_inputs] == [target.encode() for target in module.TARGETS]
    assert all(not path.exists() for path, _data in staged_inputs)


def test_regression_runner_rejects_empty_regression_set(tmp_path: Path) -> None:
    """CI fails rather than silently claiming regression coverage with no inputs."""
    module = _module()
    build_root = tmp_path / "build"
    regression_root = tmp_path / "regressions"
    build_root.mkdir()
    for target in module.TARGETS:
        module.fuzzer_binary(build_root, target).write_bytes(b"binary")
        (regression_root / target).mkdir(parents=True)

    with pytest.raises(RuntimeError, match="no fuzz regression inputs"):
        module.run_regressions(build_root, regression_root)


def test_regression_runner_rejects_a_missing_target_directory(tmp_path: Path) -> None:
    """A populated sibling target cannot hide a missing parser regression set."""
    module = _module()
    build_root = tmp_path / "build"
    regression_root = tmp_path / "regressions"
    build_root.mkdir()
    missing = module.TARGETS[0]
    for target in module.TARGETS:
        module.fuzzer_binary(build_root, target).write_bytes(b"binary")
        if target == missing:
            continue
        target_root = regression_root / target
        target_root.mkdir(parents=True)
        (target_root / "case.bin").write_bytes(target.encode())

    with pytest.raises(
        FileNotFoundError,
        match=rf"missing regular fuzz target directory: .*{missing}",
    ):
        module.run_regressions(build_root, regression_root)


def test_regression_cases_reject_a_symlinked_target(tmp_path: Path) -> None:
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
        module.regression_cases(regression_root, target)


def test_campaign_runner_uses_stable_target_seeds_and_limits(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Mutation campaigns must be bounded and reproducible per parser."""
    module = _module()
    build_root = tmp_path / "build"
    regression_root = tmp_path / "regressions"
    calls: list[list[str]] = []
    staged_corpora: list[Path] = []

    build_root.mkdir()
    for target in module.TARGETS:
        module.fuzzer_binary(build_root, target).write_bytes(b"binary")
        target_root = regression_root / target
        target_root.mkdir(parents=True)
        (target_root / "case.bin").write_bytes(target.encode())

    def fake_run(command: list[str], *, check: bool) -> None:
        """Capture one bounded mutation campaign."""
        assert check is True
        staged_corpora.append(Path(command[-1]))
        calls.append(command)

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    assert module.run_campaigns(
        build_root,
        regression_root,
        runs=17,
        seed=41,
        max_length=4096,
    ) == 17 * len(module.TARGETS)
    assert len(calls) == len(module.TARGETS)
    for ordinal, (target, command) in enumerate(zip(module.TARGETS, calls, strict=True)):
        assert Path(command[0]) == module.fuzzer_binary(build_root, target)
        assert command[1:6] == [
            "-runs=17",
            f"-seed={41 + ordinal}",
            "-max_len=4096",
            f"-max_input_ms={module.DEFAULT_MAX_INPUT_MS}",
            f"-max_rss_mb={module.DEFAULT_MAX_RSS_MB}",
        ]
        assert Path(command[6]).name == target
        assert Path(command[6]) != regression_root / target
    assert all(not corpus.exists() for corpus in staged_corpora)


def test_libfuzzer_runner_uses_native_timeout_and_rss_flags(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """ASan/UBSan libFuzzer jobs must not receive standalone-only options."""
    module = _module()
    build_root = tmp_path / "build"
    regression_root = tmp_path / "regressions"
    calls: list[list[str]] = []

    build_root.mkdir()
    for target in module.TARGETS:
        module.fuzzer_binary(build_root, target).write_bytes(b"binary")
        target_root = regression_root / target
        target_root.mkdir(parents=True)
        (target_root / "case.bin").write_bytes(target.encode())

    def fake_run(command: list[str], *, check: bool) -> None:
        """Record one deterministic fuzz-runner subprocess invocation."""
        assert check is True
        calls.append(command)

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    assert module.run_regressions(
        build_root,
        regression_root,
        max_input_ms=5_001,
        max_rss_mb=768,
        engine="libfuzzer",
    ) == len(module.TARGETS)

    for command in calls:
        assert command[1:4] == ["-runs=1", "-timeout=6", "-rss_limit_mb=768"]
        assert all("max_input_ms" not in argument for argument in command)
        assert all("max_rss_mb" not in argument for argument in command)


def test_guard_arguments_reject_unknown_engine() -> None:
    """Configuration mistakes must fail before starting a fuzz subprocess."""
    module = _module()

    with pytest.raises(ValueError, match="unsupported fuzz engine"):
        module.guard_arguments("unknown", max_input_ms=1_000, max_rss_mb=128)


def test_main_can_publish_machine_readable_fuzz_evidence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Scheduled campaigns emit canonical evidence for limit review automation."""
    module = _module()
    output = tmp_path / "evidence.json"
    monkeypatch.setattr(module, "run_regressions", lambda *args, **kwargs: 10)
    monkeypatch.setattr(module, "run_campaigns", lambda *args, **kwargs: 4000)
    monkeypatch.setattr(
        module,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "build_root": tmp_path,
                "regression_root": tmp_path,
                "campaign_runs": 1000,
                "seed": 41,
                "max_len": 4096,
                "max_input_ms": 5000,
                "max_rss_mb": 768,
                "engine": "libfuzzer",
                "evidence_output": output,
            },
        )(),
    )

    module.main()
    import json

    evidence = json.loads(output.read_text(encoding="utf-8"))
    assert evidence["status"] == "passed"
    assert evidence["regression_inputs"] == 10
    assert evidence["mutation_runs"] == 4000
    assert evidence["sanitizer_findings"] == 0
