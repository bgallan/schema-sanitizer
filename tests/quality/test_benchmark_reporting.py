"""Regression tests for the benchmark reporting harness.

It validates shell-free command isolation, package ownership, timing records,
machine-readable reports, route details, and privacy-safe limit reviews.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from benchmarks.concurrency import assets as concurrency_assets
from benchmarks.concurrency.assets import load_catalog, stage_probes
from benchmarks.concurrency.threading import matrix as threading_matrix
from benchmarks.concurrency.threading import modes
from benchmarks.concurrency.threading.dimensions import expected_benchmark_case_names
from benchmarks.ingestion.reporting import write_report
from benchmarks.ingestion.timing import records, reset_records, set_default_warmups, time_call
from benchmarks.support import command as benchmark_command
from benchmarks.support.command import CAPTURE, DISCARD, MERGE_WITH_STDOUT, run_command

ROOT = Path(__file__).resolve().parents[2]


def test_run_command_uses_argv_and_captures_text() -> None:
    """A validated interpreter runs without a shell and returns captured streams."""
    completed = run_command(
        [sys.executable, "-c", "import sys; print('out'); print('err', file=sys.stderr)"],
        check=True,
        stdout=CAPTURE,
        stderr=CAPTURE,
        text=True,
        timeout=10,
    )

    assert completed.stdout.splitlines() == ["out"]
    assert completed.stderr.splitlines() == ["err"]


def test_run_command_supports_discard_and_stderr_merge() -> None:
    """Only the explicitly supported stream combinations are accepted."""
    completed = run_command(
        [sys.executable, "-c", "import sys; print('out'); print('err', file=sys.stderr)"],
        check=True,
        stdout=DISCARD,
        stderr=MERGE_WITH_STDOUT,
        text=True,
        timeout=10,
    )

    assert completed.stdout is None
    assert completed.stderr is None


def test_run_command_preserves_python_environment_prefix() -> None:
    """Executable validation must not resolve a virtualenv launcher out of its env."""
    completed = run_command(
        [sys.executable, "-c", "import sys; print(sys.prefix)"],
        check=True,
        stdout=CAPTURE,
        text=True,
        timeout=10,
    )

    assert isinstance(completed.stdout, str)
    assert Path(completed.stdout.strip()).resolve() == Path(sys.prefix).resolve()


@pytest.mark.parametrize(
    ("post_kill_reaps", "error_type"),
    ((True, TimeoutError), (False, benchmark_command.CommandCleanupFailed)),
)
def test_run_command_timeout_kills_domain_and_bounds_post_kill_reap(
    monkeypatch: pytest.MonkeyPatch,
    post_kill_reaps: bool,
    error_type: type[Exception],
) -> None:
    """Timeout cleanup kills the process group and bounds both reaping outcomes."""
    spawned: dict[str, object] = {}
    timeouts: list[float] = []
    killed: list[tuple[int, int]] = []

    class FakeProcess:
        """Provide the process surface used by the bounded executor."""

        pid = 31_415

        async def wait(self) -> int:
            """Return only if the mocked bounded waiter elects to await it."""
            return 0

    async def fake_create(*_argv: str, **kwargs: object) -> FakeProcess:
        """Record process-domain creation and return the controlled child."""
        spawned.update(kwargs)
        return FakeProcess()

    async def fake_wait_for(awaitable: object, *, timeout: float) -> int:
        """Expire execution and select the controlled post-kill reap outcome."""
        timeouts.append(timeout)
        close = getattr(awaitable, "close", None)
        if callable(close):
            close()
        if len(timeouts) == 2 and post_kill_reaps:
            return 0
        raise TimeoutError

    monkeypatch.setattr(benchmark_command.asyncio, "create_subprocess_exec", fake_create)
    monkeypatch.setattr(benchmark_command.asyncio, "wait_for", fake_wait_for)
    monkeypatch.setattr(benchmark_command, "_PLATFORM_FAMILY", "posix")
    monkeypatch.setattr(
        benchmark_command.os,
        "killpg",
        lambda pid, action: killed.append((pid, action)),
        raising=False,
    )

    with pytest.raises(error_type):
        asyncio.run(
            benchmark_command._execute(
                (sys.executable, "-c", "pass"),
                cwd=None,
                stdout=None,
                stderr=None,
                timeout=7,
            )
        )

    assert spawned["start_new_session"] is True
    assert killed == [(FakeProcess.pid, benchmark_command._PROCESS_DOMAIN_KILL_SIGNAL)]
    assert timeouts == [7, benchmark_command._POST_KILL_REAP_TIMEOUT_SECONDS]


@pytest.mark.parametrize("already_exited", [True, False])
def test_windows_tree_kill_race_distinguishes_terminal_from_direct_fallback(
    monkeypatch: pytest.MonkeyPatch,
    already_exited: bool,
) -> None:
    """A raced child exit is accepted, but direct-only tree cleanup fails closed."""
    direct_kills: list[int] = []

    class FakePath:
        """Represent the controlled Windows system tree-kill executable."""

        def is_file(self) -> bool:
            """Report that the system executable exists."""
            return True

        def __fspath__(self) -> str:
            """Return a stable executable path for the fake launcher."""
            return "C:/Windows/System32/taskkill.exe"

    class FakeTreeKiller:
        """Return the no-longer-present status from the tree-kill helper."""

        returncode = 128

        async def wait(self) -> int:
            """Complete immediately with the controlled nonzero status."""
            return self.returncode

        def kill(self) -> None:
            """Reject unexpected cleanup of an already terminal helper."""
            raise AssertionError("terminal taskkill helper was killed")

    class FakeProcess:
        """Model either the race-to-exit or the live direct-kill fallback."""

        pid = 27_182

        def __init__(self) -> None:
            """Select the direct child's state at tree-kill completion."""
            self.returncode = 0 if already_exited else None
            self.kill_requested = False

        async def wait(self) -> int:
            """Return the terminal status after any required direct kill."""
            if self.kill_requested:
                self.returncode = -9
            assert self.returncode is not None
            return self.returncode

        def kill(self) -> None:
            """Record that only the direct child could be killed."""
            direct_kills.append(self.pid)
            self.kill_requested = True

    async def fake_create(*_argv: str, **_kwargs: object) -> FakeTreeKiller:
        """Return the controlled nonzero tree-kill process."""
        return FakeTreeKiller()

    with monkeypatch.context() as patch:
        patch.setattr(benchmark_command, "_PLATFORM_FAMILY", "nt")
        patch.setattr(benchmark_command, "_windows_taskkill_path", lambda: FakePath())
        patch.setattr(benchmark_command.asyncio, "create_subprocess_exec", fake_create)
        process = FakeProcess()
        if already_exited:
            asyncio.run(
                benchmark_command._kill_process_domain_and_reap(
                    process,  # type: ignore[arg-type]
                    argv=("benchmark.exe",),
                )
            )
        else:
            with pytest.raises(
                benchmark_command.CommandCleanupFailed,
                match="could not be reaped",
            ):
                asyncio.run(
                    benchmark_command._kill_process_domain_and_reap(
                        process,  # type: ignore[arg-type]
                        argv=("benchmark.exe",),
                    )
                )

    assert direct_kills == ([] if already_exited else [FakeProcess.pid])


def test_windows_tree_kill_path_comes_from_the_system_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Windows cleanup resolves taskkill through the system API instead of PATH."""
    system_directory = tmp_path / "System32"
    system_directory.mkdir()
    taskkill = system_directory / "taskkill.exe"
    taskkill.write_bytes(b"controlled executable")

    class FakeGetSystemDirectory:
        """Write the controlled system directory into a WinAPI output buffer."""

        argtypes: object = None
        restype: object = None

        def __call__(self, buffer: object, size: int) -> int:
            """Populate a sufficiently large output buffer and return its length."""
            assert size == benchmark_command._WINDOWS_SYSTEM_DIRECTORY_BUFFER_CHARS
            setattr(buffer, "value", str(system_directory))
            return len(str(system_directory))

    class FakeKernel32:
        """Expose the one system-directory function required by the resolver."""

        GetSystemDirectoryW = FakeGetSystemDirectory()

    monkeypatch.setattr(
        benchmark_command.ctypes,
        "WinDLL",
        lambda library, **options: FakeKernel32(),
        raising=False,
    )

    assert benchmark_command._windows_taskkill_path() == taskkill


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
def test_run_command_rejects_unbounded_or_invalid_timeouts(timeout: float) -> None:
    """Every command must declare one finite positive execution deadline."""
    with pytest.raises(ValueError, match="finite and positive"):
        run_command([sys.executable, "-c", "pass"], timeout=timeout)


@pytest.mark.parametrize("command", [[], "echo value", [""], ["python\0evil"]])
def test_run_command_rejects_non_argv_commands(command: object) -> None:
    """Shell strings, empty commands, and NUL-bearing arguments fail before execution."""
    with pytest.raises((TypeError, ValueError)):
        run_command(command, timeout=10)  # type: ignore[arg-type]


def test_run_command_rejects_stdout_merge() -> None:
    """The stderr-only merge sentinel cannot be misapplied to stdout."""
    with pytest.raises(ValueError, match="valid only for stderr"):
        run_command(
            [sys.executable, "-c", "pass"],
            stdout=MERGE_WITH_STDOUT,
            timeout=10,
        )


def test_threading_matrix_plan_replaces_only_owned_stale_state(tmp_path: Path) -> None:
    """Plan reruns use one stable directory and discard its incomplete prior state."""
    args = argparse.Namespace(
        profile="ci",
        rows=8,
        warmups=0,
        repeats=1,
        only="parquet",
        pipeline_shape="all",
        pipeline_format="all",
        output=None,
    )
    work_root = tmp_path / "work root"
    command_output = tmp_path / "commands.sh"
    first = threading_matrix._shell_plan(args, work_root, command_output)
    stale = work_root / "matrix" / "stale.json"
    stale.write_text("old run\n", encoding="utf-8")
    second = threading_matrix._shell_plan(args, work_root, command_output)

    assert first == second
    assert "umask 077" in first
    assert not stale.exists()
    assert (ROOT / "benchmarks/concurrency/threading/run_matrix.sh").read_text(
        encoding="utf-8"
    ).count("umask 077") == 1


def test_benchmark_output_writer_is_idempotent_and_rejects_symlinks(tmp_path: Path) -> None:
    """Canonical report rewrites preserve timestamps and cannot follow aliases."""
    output = tmp_path / "report.json"
    threading_matrix._write_text_atomically(output, '{"stable": true}\n')
    first_mtime = output.stat().st_mtime_ns
    threading_matrix._write_text_atomically(output, '{"stable": true}\n')
    assert output.stat().st_mtime_ns == first_mtime

    target = tmp_path / "target.json"
    target.write_text("preserve\n", encoding="utf-8")
    output.unlink()
    try:
        output.symlink_to(target)
    except OSError:
        pytest.skip("symlinks are unavailable")
    with pytest.raises(ValueError, match="symlinked benchmark output"):
        threading_matrix._write_text_atomically(output, "replace\n")
    assert target.read_text(encoding="utf-8") == "preserve\n"


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("missing", "case contract mismatch"),
        ("extra", "case contract mismatch"),
        ("non_boolean", "equivalence must be a boolean"),
    ),
)
def test_threading_matrix_requires_the_exact_strict_case_contract(
    tmp_path: Path,
    mutation: str,
    message: str,
) -> None:
    """Missing, extra, or truthy non-boolean child evidence fails closed."""
    case = threading_matrix._cases("ci")[0]
    names = expected_benchmark_case_names("parquet")
    results = {name: {"equivalent": True} for name in names}
    dimensions = threading_matrix._case_dimensions(
        case,
        rows=8,
        warmups=0,
        repeats=1,
        selection="parquet",
        pipeline_shape="all",
        pipeline_format="all",
    )
    if mutation == "missing":
        results.pop(names[0])
    elif mutation == "extra":
        results["uncontracted_case"] = {"equivalent": True}
    else:
        results[names[0]]["equivalent"] = 1
    output = tmp_path / "case.json"
    output.write_text(json.dumps({"dimensions": dimensions, "cases": results}), encoding="utf-8")

    with pytest.raises(RuntimeError, match=message):
        threading_matrix._load_case(
            case,
            output,
            rows=8,
            warmups=0,
            repeats=1,
            selection="parquet",
            pipeline_shape="all",
            pipeline_format="all",
        )


@pytest.mark.parametrize(
    ("dimension", "replacement"),
    (("parquet_compression", "gzip"), ("pipeline_shape", "scalar"), ("pipeline_format", "csv")),
)
def test_threading_matrix_rejects_mislabeled_child_dimensions(
    tmp_path: Path,
    dimension: str,
    replacement: str,
) -> None:
    """A child cannot ignore or relabel any requested semantic dimension."""
    case = threading_matrix._cases("ci")[0]
    dimensions = threading_matrix._case_dimensions(
        case,
        rows=8,
        warmups=0,
        repeats=1,
        selection="parquet",
        pipeline_shape="all",
        pipeline_format="all",
    )
    dimensions[dimension] = replacement
    cases = {name: {"equivalent": True} for name in expected_benchmark_case_names("parquet")}
    output = tmp_path / "case.json"
    output.write_text(json.dumps({"dimensions": dimensions, "cases": cases}), encoding="utf-8")

    with pytest.raises(RuntimeError, match="dimension contract mismatch"):
        threading_matrix._load_case(
            case,
            output,
            rows=8,
            warmups=0,
            repeats=1,
            selection="parquet",
            pipeline_shape="all",
            pipeline_format="all",
        )


def test_threading_plan_rejects_overlapping_outputs_before_cleanup(tmp_path: Path) -> None:
    """Invalid path overlap cannot trigger deletion of an existing owned plan."""
    work_root = tmp_path / "work"
    plan_root = work_root / "matrix"
    plan_root.mkdir(parents=True)
    stale = plan_root / "preserve.json"
    stale.write_text("preserve\n", encoding="utf-8")
    outside = tmp_path / "outside.sh"
    args = argparse.Namespace(
        profile="ci",
        rows=8,
        warmups=0,
        repeats=1,
        only="parquet",
        pipeline_shape="all",
        pipeline_format="all",
        output=None,
    )

    invalid = (
        (plan_root / "commands.sh", None),
        (outside, plan_root / "report.json"),
        (outside, outside),
    )
    for command_output, report_output in invalid:
        args.output = report_output
        with pytest.raises(ValueError, match="must be disjoint"):
            threading_matrix._shell_plan(args, work_root, command_output)
        assert stale.read_text(encoding="utf-8") == "preserve\n"


def test_threading_plan_allows_outputs_beside_its_owned_directory(tmp_path: Path) -> None:
    """Caller-owned siblings remain valid because cleanup removes only ``matrix``."""
    work_root = tmp_path / "work"
    command_output = work_root / "commands.sh"
    report_output = work_root / "report.json"
    args = argparse.Namespace(
        profile="ci",
        rows=8,
        warmups=0,
        repeats=1,
        only="parquet",
        pipeline_shape="all",
        pipeline_format="all",
        output=report_output,
    )

    plan = threading_matrix._shell_plan(args, work_root, command_output)

    assert f"matrix_root={(work_root / 'matrix').as_posix()}" in plan
    assert command_output.parent == report_output.parent == work_root


@pytest.mark.parametrize(("primary_status", "expected_status"), ((0, 23), (7, 7)))
def test_benchmark_cleanup_traps_report_cleanup_only_failures(
    tmp_path: Path,
    primary_status: int,
    expected_status: int,
) -> None:
    """Generated and outer traps fail success but preserve an existing failure."""
    args = argparse.Namespace(
        profile="ci",
        rows=8,
        warmups=0,
        repeats=1,
        only="parquet",
        pipeline_shape="all",
        pipeline_format="all",
        output=None,
    )
    generated = threading_matrix._shell_plan(
        args,
        tmp_path / f"work-{primary_status}",
        tmp_path / f"commands-{primary_status}.sh",
    )
    wrapper = (ROOT / "benchmarks/concurrency/threading/run_matrix.sh").read_text(encoding="utf-8")
    scripts = (
        (generated, "trap cleanup_matrix EXIT"),
        (wrapper, "trap cleanup_plan EXIT"),
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


def test_threading_parquet_digest_closes_reader_before_cleanup(tmp_path: Path, monkeypatch) -> None:
    """The Windows benchmark must release its Parquet handle before unlinking."""
    output = tmp_path / "output.parquet"
    output.write_bytes(b"placeholder")
    lifecycle: list[str] = []

    class FakeTable:
        def to_pylist(self) -> list[dict[str, int]]:
            """Return one deterministic row after recording materialization."""
            lifecycle.append("materialize")
            return [{"value": 1}]

    class FakeParquetFile:
        def __init__(self, path: Path) -> None:
            """Record construction for the expected output path."""
            assert path == output
            lifecycle.append("construct")

        def __enter__(self) -> "FakeParquetFile":
            """Open the fake reader and return its context value."""
            lifecycle.append("open")
            return self

        def read(self) -> FakeTable:
            """Record the read and return a materializable table."""
            lifecycle.append("read")
            return FakeTable()

        def __exit__(self, *_exc: object) -> None:
            """Record that the reader closed before fixture cleanup."""
            lifecycle.append("close")

    monkeypatch.setattr(pq, "ParquetFile", FakeParquetFile)

    assert len(modes._logical_digest(output)) == 64
    assert lifecycle == ["construct", "open", "read", "materialize", "close"]
    output.unlink()


def test_threading_output_equivalence_checks_every_iteration(tmp_path: Path) -> None:
    """An early divergent output cannot be hidden by a matching final repeat."""
    iterations = {"single": 0, "multi": 0}

    def run(mode: str, path: Path) -> None:
        """Diverge only the first warmup and converge every later output."""
        iteration = iterations[mode]
        iterations[mode] += 1
        payload = b"divergent" if mode == "single" and iteration == 0 else b"stable"
        path.write_bytes(payload)

    with pytest.raises(RuntimeError, match="differ at iteration 0"):
        modes._time_case(
            run,
            directory=tmp_path,
            name="early-divergence",
            suffix="bin",
            warmups=1,
            repeats=1,
            verification=modes._sha256,
            equivalence_kind="bytes",
        )

    assert not list(tmp_path.iterdir())


def test_benchmark_python_modules_are_grouped_by_domain() -> None:
    """Keep executable implementations out of the benchmark package root."""
    root = ROOT / "benchmarks"

    assert not [path for path in root.glob("*.py") if path.name != "__init__.py"]
    modules = [
        path
        for path in root.rglob("*.py")
        if path != root / "__init__.py" and "__pycache__" not in path.parts
    ]
    assert modules
    assert all(len(path.relative_to(root).parts) >= 2 for path in modules)
    assert all((path.parent / "__init__.py").is_file() for path in modules)


def test_concurrency_catalog_stages_every_probe(tmp_path: Path) -> None:
    """The current catalog is complete and every indexed probe remains compilable source."""
    catalog = load_catalog()
    expected = {probe for record in catalog["records"] for probe in record["probes"]}
    staged = stage_probes(tmp_path)

    assert {path.relative_to(tmp_path).as_posix() for path in staged} == expected
    assert all("main(" in path.read_text(encoding="utf-8") for path in staged)


def test_concurrency_probe_archive_repacking_is_safe_and_deterministic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A staged tree round-trips exactly and rejects path traversal lookups."""
    staged_root = tmp_path / "staged"
    stage_probes(staged_root)
    archive = tmp_path / "concurrency.zip"
    monkeypatch.setattr(concurrency_assets, "PROBE_ARCHIVE", archive)

    concurrency_assets.pack_staged_probes(staged_root)
    first = archive.read_bytes()
    concurrency_assets.pack_staged_probes(staged_root)

    assert archive.read_bytes() == first
    assert "main(" in concurrency_assets.load_probe("layout/compact-queued-task-tsan.cc")
    with pytest.raises(ValueError, match="unsafe"):
        concurrency_assets.load_probe("../escape.cc")


def test_retained_benchmark_evidence_is_valid_json() -> None:
    """Keep committed evidence machine-readable after moves and consolidation."""
    evidence_root = ROOT / "benchmarks" / "evidence"
    for path in evidence_root.rglob("*.json"):
        json.loads(path.read_text(encoding="utf-8"))


def test_quality_benchmark_harness_smoke_command() -> None:
    """The quality action's exact ingestion smoke workload completes end to end."""
    completed = run_command(
        [
            sys.executable,
            "-m",
            "benchmarks.ingestion.cli",
            "--rows",
            "8",
            "--width",
            "2",
            "--repeats",
            "1",
        ],
        cwd=ROOT,
        check=True,
        stdout=CAPTURE,
        stderr=CAPTURE,
        text=True,
        timeout=120,
    )

    assert isinstance(completed.stdout, str)
    assert completed.stderr == ""
    lines = {line.split(":", 1)[0]: line for line in completed.stdout.splitlines()}
    assert "input_source_route=path" in lines["to_pyarrow jsonl"]
    for label in (
        "to_pyarrow json directory",
        "to_pyarrow json directory many files",
        "to_pyarrow xml directory",
    ):
        assert "input_source_route=path_sources" in lines[label]
        assert "input_plan_route=native_manifest_paths" in lines[label]
    assert "parquet_input_route=" not in lines["to_pyarrow jsonl"]
    for label in ("to_jsonl", "to_csv"):
        assert "input_source_route=path" in lines[label]
    for label in (
        "to_jsonl parquet direct",
        "to_jsonl parquet direct wide",
        "to_jsonl registry parquet direct",
    ):
        assert "parquet_input_route=native_registry" in lines[label]
        assert "file_output_route=native_direct" in lines[label]


def test_time_call_records_median_p95_sizes_and_warmups(tmp_path: Path) -> None:
    """Record robust timings, sizes, repeats, and warmup counts."""
    calls = 0
    output = tmp_path / "out.bin"

    def work() -> object:
        """Write a deterministic output for each measured invocation."""
        nonlocal calls
        calls += 1
        output.write_bytes(b"result")
        return object()

    reset_records()
    set_default_warmups(2)
    record = time_call(
        "case",
        work,
        rows=10,
        repeats=3,
        input_bytes=100,
        output_bytes=output,
    )

    assert calls == 5
    assert record.warmups == 2
    assert record.repeats == 3
    assert record.input_bytes == 100
    assert record.output_bytes == len(b"result")
    assert record.median_seconds >= 0
    assert record.p95_seconds >= record.median_seconds
    assert records() == [record]


def test_write_report_contains_platform_fixture_and_records(tmp_path: Path) -> None:
    """Persist benchmark records together with fixture and platform metadata."""
    reset_records()
    set_default_warmups(0)
    record = time_call("noop", lambda: None, rows=1, repeats=1)
    output = tmp_path / "benchmark.json"

    write_report(output, [record], fixture_metadata={"rows": 1, "case": "noop"})

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 1
    assert payload["fixture"] == {"rows": 1, "case": "noop"}
    assert payload["platform"]["python"]
    assert payload["benchmarks"][0]["label"] == "noop"


def _review_module():
    """Load the reader-limit review utility directly from its repository path."""
    path = ROOT / "benchmarks/readers/review_limits.py"
    spec = importlib.util.spec_from_file_location("review_reader_limits", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_review_waits_for_production_telemetry_without_hiding_clean_fuzzing() -> None:
    """Verify review waits for production telemetry without hiding clean fuzzing."""
    review = _review_module().build_review(
        {"status": "passed", "sanitizer_findings": 0, "mutation_runs": 40000}, []
    )
    assert review["review_status"] == "awaiting_production_telemetry"
    assert review["production_telemetry_present"] is False
    assert review["automatic_limit_change"] is False


def test_review_aggregates_only_privacy_safe_resource_counters() -> None:
    """Verify review aggregates only privacy safe resource counters."""
    review = _review_module().build_review(
        {"status": "passed", "sanitizer_findings": 0},
        [
            {
                "peak_charged_memory_bytes": 75,
                "operation_memory_limit_bytes": 100,
                "parser_max_depth": 12,
                "decompression_ratio": 4.0,
                "cancellation_reason": "consumer_close",
                "secret": "must-not-propagate",  # pragma: allowlist secret
            },
            {
                "peak_charged_memory_bytes": 20,
                "operation_memory_limit_bytes": 100,
                "parser_max_depth": 7,
                "cancellation_reason": "consumer_close",
            },
        ],
    )
    assert review["review_status"] == "complete"
    assert review["telemetry"]["max_peak_to_limit_ratio"] == 0.75
    assert review["telemetry"]["maxima"]["parser_max_depth"] == 12
    assert review["telemetry"]["cancellation_reasons"] == {"consumer_close": 2}
    assert "secret" not in str(review)
