"""Exercises composite asynchronous admission, cancellation after success, out-of-order
fixed rings, task-domain release, native byte backpressure, cgroup ancestry, and stage
ordering. Memory always publishes before worker capacity, failures never touch worker
slots, and delivery or close retains one exact generation under bounded deadlines."""

from __future__ import annotations

import asyncio
import re
from pathlib import Path

import pytest
from _support.source_contracts import package_source_text
from _support.synchronization import SCHEDULER_TIMEOUT_SECONDS

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src" / "schema_sanitizer"
CPP = ROOT / "cpp" / "src"

_CPP_IGNORED = re.compile(
    r"//[^\n]*|/\*.*?\*/|(?:u8|u|U|L)?\"(?:\\.|[^\"\\])*\"|"
    r"(?:u8|u|U|L)?'(?:\\.|[^'\\])*'",
    re.DOTALL,
)
_CPP_TOKEN = re.compile(
    r"[A-Za-z_]\w*|\d+(?:'\d+)*(?:[A-Za-z_]\w*)?|::|->|&&|\|\||"
    r"==|!=|<=|>=|\+\+|--|[{}()\[\],;.&*!=<>+\-/]"
)


def _cpp_scope(source: str, signature: str) -> str:
    """Extract C++ scope from the production source contract."""
    code = _CPP_IGNORED.sub(" ", source)
    match = re.search(signature, code)
    assert match is not None, signature
    opening = code.find("{", match.end())
    assert opening >= 0, signature
    depth = 0
    for index in range(opening, len(code)):
        if code[index] == "{":
            depth += 1
        elif code[index] == "}":
            depth -= 1
            if depth == 0:
                return code[match.start() : index + 1]
    raise AssertionError(f"unterminated C++ scope: {signature}")


def _cpp_tokens(source: str) -> tuple[str, ...]:
    """Return the tokens extracted from the production C++ source."""
    return tuple(_CPP_TOKEN.findall(_CPP_IGNORED.sub(" ", source)))


def _token_index(tokens: tuple[str, ...], needle: tuple[str, ...], *, start: int = 0) -> int:
    """Extract token index from the production source contract."""
    width = len(needle)
    for index in range(start, len(tokens) - width + 1):
        if tokens[index : index + width] == needle:
            return index
    raise AssertionError(f"missing C++ token sequence: {' '.join(needle)}")


def test_parallel_admission_reserves_memory_before_workers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify parallel admission reserves memory before workers."""
    from schema_sanitizer.core_impl import memory_budget, process_resources

    order: list[str] = []

    class MemoryLease:
        def resize(self, _amount: int) -> None:
            """Resize the resource represented by the memory lease test double."""
            pass

        def close(self) -> None:
            """Close the resources owned by the memory lease test double."""
            order.append("memory-release")

    class Ledger:
        def acquire(self, _amount: int, *, stage: str) -> MemoryLease:
            """Acquire the resource represented by the ledger test double."""
            order.append("memory-acquire")
            return MemoryLease()

    class Execution:
        amount = 1

        def release(self) -> None:
            """Release the resource held by the execution test double."""
            order.append("worker-release")

    monkeypatch.setattr(memory_budget, "adaptive_parallel_slots", lambda *_a, **_k: 2)
    monkeypatch.setattr(memory_budget, "current_operation_memory_ledger", lambda: Ledger())
    monkeypatch.setattr(
        process_resources,
        "acquire_project_threads",
        lambda *_a, **_k: order.append("worker-acquire") or Execution(),
    )

    admission = memory_budget.acquire_parallel_admission(
        2,
        per_slot_bytes=1024,
        stage="parallel-admission-reserves-memory-before-workers",
        require_memory=True,
    )
    assert order[:2] == ["memory-acquire", "worker-acquire"]
    admission.close()
    assert order[-2:] == ["worker-release", "memory-release"]


def test_required_memory_failure_never_touches_worker_capacity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify required memory failure never touches worker capacity."""
    from schema_sanitizer.core_impl import memory_budget, process_resources

    touched = False
    monkeypatch.setattr(memory_budget, "adaptive_parallel_slots", lambda *_a, **_k: 2)
    monkeypatch.setattr(memory_budget, "current_operation_memory_ledger", lambda: None)

    def acquire_threads(*_a: object, **_k: object) -> object:
        """Acquire the worker-thread permits before starting work."""
        nonlocal touched
        touched = True
        raise AssertionError("worker admission must not be reached")

    monkeypatch.setattr(process_resources, "acquire_project_threads", acquire_threads)
    with pytest.raises(RuntimeError, match="requires an operation memory ledger"):
        memory_budget.acquire_parallel_admission(
            2,
            per_slot_bytes=1024,
            stage="parallel-admission-reserves-memory-before-workers",
            require_memory=True,
        )
    assert touched is False


def test_async_admission_close_retains_failed_generation_for_retry() -> None:
    """Verify async admission close retains failed generation for retry."""
    from schema_sanitizer.core_impl.async_scheduler import _AsyncSchedulerAdmission

    events: list[str] = []

    class FailsOnce:
        calls = 0

        def close(self) -> None:
            """Close the resources owned by the fails once test double."""
            self.calls += 1
            events.append(f"borrowed-{self.calls}")
            if self.calls == 1:
                raise RuntimeError("retry me")

    class Stage:
        def close(self) -> None:
            """Close the resources owned by the stage test double."""
            events.append("stage")

    borrowed = FailsOnce()
    stage = Stage()
    admission = _AsyncSchedulerAdmission(2, stage, borrowed)
    with pytest.raises(RuntimeError, match="retry me"):
        admission.close()
    assert admission.borrowed_stage_admission is borrowed
    assert admission.stage_admission is stage
    assert admission.slots == 2

    admission.close()
    assert events == ["borrowed-1", "borrowed-2", "stage"]
    assert admission.borrowed_stage_admission is None
    assert admission.stage_admission is None
    assert admission.slots == 0


def test_retry_success_is_delivery_commit_even_if_cancellation_arrives_after_operation() -> None:
    """Verify retry success is delivery commit even if cancellation arrives after operation."""
    from schema_sanitizer.core_impl.async_scheduler import retry_async
    from schema_sanitizer.core_impl.cancellation import operation_cancellation

    async def run() -> str:
        """Cancel the token after success and return the committed retry result."""
        with operation_cancellation() as token:

            async def operation() -> str:
                """Run the controlled operation under test."""
                token.cancel()
                return "committed"

            return await retry_async(operation, retries=3)

    assert asyncio.run(run()) == "committed"


def test_ordered_async_pending_storage_is_fixed_ring_and_handles_out_of_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify ordered async pending storage is fixed ring and handles out of order."""
    from schema_sanitizer.core_impl import async_scheduler as scheduler

    source = package_source_text("core_impl/async_scheduler.py")
    ordered = source[
        source.index("async def ordered_indexed_results") : source.index(
            "async def unordered_indexed_results"
        )
    ]
    assert "pending: dict" not in ordered
    assert "_AsyncPendingResultSlot" in ordered
    assert "index % worker_count" in ordered

    async def run() -> tuple[list[int], list[int]]:
        """Collect out-of-order fetches through a two-slot pending ring."""
        monkeypatch.setattr(
            scheduler,
            "_acquire_async_scheduler_admission",
            lambda _requested: scheduler._AsyncSchedulerAdmission(2),
        )
        later_completed = asyncio.Event()
        completion_order: list[int] = []

        async def fetch(index: int) -> int:
            """Fetch the controlled asynchronous result."""
            if index == 0:
                await asyncio.wait_for(later_completed.wait(), timeout=SCHEDULER_TIMEOUT_SECONDS)
            else:
                later_completed.set()
            completion_order.append(index)
            return index

        values = [
            value
            async for _index, value in scheduler.ordered_indexed_results(
                8,
                fetch,
                window=2,
                memory_contract=scheduler.AsyncResultMemoryContract(preflight_bytes=64),
            )
        ]
        return values, completion_order

    values, completion_order = asyncio.run(run())
    assert values == list(range(8))
    assert completion_order.index(1) < completion_order.index(0)


def test_async_task_domain_release_commits_exactly_once_under_one_lock() -> None:
    """Verify async task domain release commits exactly once under one lock."""
    source = package_source_text("core_impl/async_scheduler.py")
    block = source[
        source.index("class _AsyncTaskDomainLease") : source.index(
            "@dataclass(slots=True)\nclass _AsyncSchedulerAdmission"
        )
    ]
    assert "with _ASYNC_ADMISSION_CONDITION" in block
    assert block.index("if self._released") > block.index("with _ASYNC_ADMISSION_CONDITION")
    assert block.index("self._released = True") > block.index("_ASYNC_TASK_SLOTS_IN_USE =")


def test_native_retained_byte_backpressure_has_deadline_and_no_queue_mutex_wait() -> None:
    """Verify native retained byte backpressure has deadline and no queue mutex wait."""
    source = (CPP / "internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    acquire = _cpp_tokens(
        _cpp_scope(
            source,
            r"\bsanitize\s*::\s*Status\s+AcquireRetainedSubmitCredit\s*\(",
        )
    )

    hard_limit = _token_index(
        acquire,
        (
            "constexpr",
            "auto",
            "kRetainedBackpressureDeadline",
            "=",
            "std",
            "::",
            "chrono",
            "::",
            "seconds",
            "(",
        ),
    )
    hard_deadline = _token_index(
        acquire,
        (
            "hard_wait_deadline",
            "=",
            "backpressure_started_at",
            "+",
            "kRetainedBackpressureDeadline",
        ),
    )
    effective_deadline = _token_index(
        acquire,
        ("auto", "retained_wait_deadline", "=", "hard_wait_deadline", ";"),
    )
    retained_lock = _token_index(
        acquire,
        (
            "std",
            "::",
            "unique_lock",
            "retained_lock",
            "(",
            "state",
            "->",
            "retained_wait_mutex",
            ")",
        ),
    )
    wait = _token_index(
        acquire,
        (
            "state",
            "->",
            "retained_ready",
            ".",
            "wait_until",
            "(",
            "retained_lock",
            ",",
            "retained_wait_deadline",
            ")",
        ),
    )
    timeout_record = _token_index(
        acquire,
        ("state", "->", "backpressure_timeouts", ".", "fetch_add", "("),
        start=wait,
    )
    hard_timeout = _token_index(
        acquire,
        ("return", "sanitize", "::", "Status", "::", "OutOfMemory", "("),
        start=timeout_record,
    )

    assert hard_limit < hard_deadline < effective_deadline < retained_lock < wait
    assert wait < timeout_record < hard_timeout
    assert acquire.count("wait_until") == 1
    # Worker queues are protected by a member literally named ``mutex``. The
    # only blocking lock in this producer path must be the dedicated retained
    # credit lock passed to the condition variable above.
    assert "mutex" not in acquire
    assert not {"tasks", "abandoned_tasks", "start_mutex", "inline_mutex"}.intersection(acquire)


def test_native_cgroup_limits_are_tristate_and_effective_across_ancestors() -> None:
    """Verify native cgroup limits are tristate and effective across ancestors."""
    view = (CPP / "internal/runtime/cgroup_view.hh").read_text(encoding="utf-8")
    assert "enum class ValueState" in view
    assert "kValue, kUnbounded, kUnknown" in view
    assert "effective_unsigned" in view
    assert "effective_headroom" in view
    assert "parent_directory_in_place" in view

    memory = (CPP / "internal/memory/memory_budget.cc").read_text(encoding="utf-8")
    arena = (CPP / "internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    cpu = (CPP / "internal/runtime/cpu_capacity.hh").read_text(encoding="utf-8")
    registry = (CPP / "internal/memory/memory_pool_registry.cc.inc").read_text(encoding="utf-8")
    assert "effective_headroom" in memory
    # Unknown Linux cgroup discovery must remain genuinely fail-closed.  Zero
    # would feed automatic_memory_limit_from_available() and select its 512 MiB
    # compatibility default instead.
    assert memory.count("return std::uint64_t{1U};") >= 2
    assert "mount_length == 1U && mountpoint[0] == '/'" in view
    assert "effective_headroom" in arena
    assert "resolve_directory" in cpu and "parent_directory_in_place" in cpu
    assert "effective_unsigned" in registry


def test_stage_domain_order_publishes_memory_before_workers() -> None:
    """Verify stage domain order publishes memory before workers."""
    source = package_source_text("core_impl/memory_budget.py")
    mapping = source[
        source.index("_STAGE_DOMAIN_ORDER") : source.index("def _stage_domain_order_key")
    ]
    assert mapping.index('"resident_memory": 10') < mapping.index('"physical_thread": 20')
