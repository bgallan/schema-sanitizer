"""Prevent tests from turning runner load into a correctness signal."""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
TESTS = ROOT / "tests"
CPP_TESTS = ROOT / "cpp" / "tests"
_CLOCK_NAMES = frozenset(
    {
        "monotonic",
        "monotonic_ns",
        "perf_counter",
        "perf_counter_ns",
        "process_time",
        "process_time_ns",
        "time",
        "time_ns",
    }
)
_DURATION_NAME_PARTS = (
    "elapsed",
    "duration",
    "latency",
    "runtime_seconds",
    "wall_clock",
    "wall_time",
)
_CPP_ASSIGNMENT = re.compile(
    r"\b(?P<name>[A-Za-z_]\w*)\s*=\s*(?P<expression>[^;]+);",
    re.DOTALL,
)
_CPP_CLOCK_EXPRESSION = re.compile(r"(?:steady_clock|high_resolution_clock|system_clock)::now\s*\(")
_CPP_DURATION_CONSTRUCTOR = re.compile(
    r"(?:std::chrono::)?(?:nanoseconds|microseconds|milliseconds|seconds|minutes|hours)"
    r"\s*\([^()]*\)"
)
_CPP_INFIX_COMPARISON = re.compile(
    r"(?P<left>[^;\n{}]+?)\s*(?P<operator><=|>=|<|>)\s*"
    r"(?P<right>[^;\n{}]+)",
)
_CPP_TEST_MACRO = re.compile(r"\b(?:ASSERT|EXPECT)_(?:LT|LE|GT|GE)\s*\(")


def _clock_aliases(
    nodes: tuple[ast.AST, ...],
    *,
    inherited_callables: set[str] | None = None,
    inherited_modules: set[str] | None = None,
) -> tuple[set[str], set[str]]:
    callables = set(_CLOCK_NAMES if inherited_callables is None else inherited_callables)
    modules = set({"time"} if inherited_modules is None else inherited_modules)
    for node in nodes:
        if isinstance(node, ast.Import):
            modules.update(
                alias.asname or alias.name for alias in node.names if alias.name == "time"
            )
        elif isinstance(node, ast.ImportFrom) and node.module == "time":
            callables.update(
                alias.asname or alias.name for alias in node.names if alias.name in _CLOCK_NAMES
            )
    changed = True
    while changed:
        changed = False
        for node in nodes:
            if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                continue
            value = node.value
            if value is None:
                continue
            aliases_clock = (
                isinstance(value, ast.Name)
                and value.id in callables
                or isinstance(value, ast.Attribute)
                and isinstance(value.value, ast.Name)
                and value.value.id in modules
                and value.attr in _CLOCK_NAMES
            )
            if not aliases_clock:
                continue
            targets = node.targets if isinstance(node, ast.Assign) else (node.target,)
            assigned = set().union(*(_assigned_names(target) for target in targets))
            if not assigned <= callables:
                callables.update(assigned)
                changed = True
    return callables, modules


def _is_clock_call(node: ast.AST, *, callables: set[str], modules: set[str]) -> bool:
    if not isinstance(node, ast.Call):
        return False
    function = node.func
    return (
        isinstance(function, ast.Name)
        and function.id in callables
        or isinstance(function, ast.Attribute)
        and isinstance(function.value, ast.Name)
        and function.value.id in modules
        and function.attr in _CLOCK_NAMES
    )


def _contains_clock_call(node: ast.AST, *, callables: set[str], modules: set[str]) -> bool:
    return any(
        _is_clock_call(candidate, callables=callables, modules=modules)
        for candidate in ast.walk(node)
    )


def _contains_temporal_call(node: ast.AST, temporal_functions: set[str]) -> bool:
    return any(
        isinstance(candidate, ast.Call)
        and isinstance(candidate.func, ast.Name)
        and candidate.func.id in temporal_functions
        for candidate in ast.walk(node)
    )


def _loaded_names(node: ast.AST) -> set[str]:
    return {
        candidate.id
        for candidate in ast.walk(node)
        if isinstance(candidate, ast.Name) and isinstance(candidate.ctx, ast.Load)
    }


def _called_name_identifiers(node: ast.AST) -> set[str]:
    return {
        call.func.id
        for call in ast.walk(node)
        if isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
    }


def _temporal_value_names(node: ast.AST) -> set[str]:
    """Return data names, attributes and mapping keys that denote a duration."""
    values: set[str] = set()

    class Visitor(ast.NodeVisitor):
        def visit_Call(self, candidate: ast.Call) -> None:  # noqa: N802
            # A helper named ``format_duration`` does not make its result a clock
            # value. Only its data arguments can carry temporal provenance.
            for argument in candidate.args:
                self.visit(argument)
            for keyword in candidate.keywords:
                self.visit(keyword.value)

        def visit_Name(self, candidate: ast.Name) -> None:  # noqa: N802
            if isinstance(candidate.ctx, ast.Load):
                values.add(candidate.id)

        def visit_Attribute(self, candidate: ast.Attribute) -> None:  # noqa: N802
            values.add(candidate.attr)
            self.visit(candidate.value)

        def visit_Subscript(self, candidate: ast.Subscript) -> None:  # noqa: N802
            key = candidate.slice
            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                values.add(key.value)
            self.visit(candidate.value)

    Visitor().visit(node)
    return {
        value for value in values if any(part in value.lower() for part in _DURATION_NAME_PARTS)
    }


def _assigned_names(node: ast.AST) -> set[str]:
    return {
        candidate.id
        for candidate in ast.walk(node)
        if isinstance(candidate, ast.Name) and isinstance(candidate.ctx, ast.Store)
    }


def _is_numeric_literal(node: ast.AST) -> bool:
    if isinstance(node, ast.Constant):
        return type(node.value) in (int, float)
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
        return _is_numeric_literal(node.operand)
    return False


def _scopes(tree: ast.Module) -> tuple[ast.Module | ast.FunctionDef | ast.AsyncFunctionDef, ...]:
    return (tree,) + tuple(
        node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    )


def _scope_nodes(
    scope: ast.Module | ast.FunctionDef | ast.AsyncFunctionDef,
) -> tuple[ast.AST, ...]:
    nested = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)
    collected: list[ast.AST] = []

    def visit(node: ast.AST) -> None:
        if node is not scope and isinstance(node, nested):
            return
        collected.append(node)
        for child in ast.iter_child_nodes(node):
            visit(child)

    visit(scope)
    return tuple(collected)


def _clock_values(
    nodes: tuple[ast.AST, ...], *, clock_callables: set[str], clock_modules: set[str]
) -> set[str]:
    values: set[str] = set()
    changed = True
    while changed:
        changed = False
        for node in nodes:
            if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                continue
            value = node.value
            if value is None or not (
                _contains_clock_call(value, callables=clock_callables, modules=clock_modules)
                or _loaded_names(value) & values
            ):
                continue
            targets = node.targets if isinstance(node, ast.Assign) else (node.target,)
            assigned = set().union(*(_assigned_names(target) for target in targets))
            if not assigned <= values:
                values.update(assigned)
                changed = True
    return values


def _temporal_functions(
    tree: ast.Module, *, module_callables: set[str], module_modules: set[str]
) -> set[str]:
    functions: set[str] = set()
    changed = True
    while changed:
        changed = False
        for function in (
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        ):
            nodes = _scope_nodes(function)
            callables, modules = _clock_aliases(
                nodes,
                inherited_callables=module_callables,
                inherited_modules=module_modules,
            )
            values = _clock_values(nodes, clock_callables=callables, clock_modules=modules)
            temporal_return = any(
                isinstance(node, ast.Return)
                and node.value is not None
                and (
                    _contains_clock_call(node.value, callables=callables, modules=modules)
                    or bool(_loaded_names(node.value) & values)
                    or bool(_temporal_value_names(node.value))
                    or _contains_temporal_call(node.value, functions)
                )
                for node in nodes
            )
            if temporal_return and function.name not in functions:
                functions.add(function.name)
                changed = True
    return functions


def _fragile_assertions(path: Path) -> tuple[tuple[int, str], ...]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    module_nodes = _scope_nodes(tree)
    module_callables, module_modules = _clock_aliases(module_nodes)
    temporal_functions = _temporal_functions(
        tree,
        module_callables=module_callables,
        module_modules=module_modules,
    )
    violations: list[tuple[int, str]] = []
    for scope in _scopes(tree):
        nodes = _scope_nodes(scope)
        clock_callables, clock_modules = _clock_aliases(
            nodes,
            inherited_callables=module_callables,
            inherited_modules=module_modules,
        )
        clock_values = _clock_values(
            nodes,
            clock_callables=clock_callables,
            clock_modules=clock_modules,
        )

        for node in nodes:
            if not isinstance(node, ast.Assert):
                continue
            for comparison in (
                candidate for candidate in ast.walk(node.test) if isinstance(candidate, ast.Compare)
            ):
                names = _loaded_names(comparison)
                value_names = names - _called_name_identifiers(comparison)
                duration_named = bool(_temporal_value_names(comparison)) or any(
                    part in name.lower() for name in value_names for part in _DURATION_NAME_PARTS
                )
                clock_derived = (
                    _contains_clock_call(
                        comparison, callables=clock_callables, modules=clock_modules
                    )
                    or _contains_temporal_call(comparison, temporal_functions)
                    or bool(names & clock_values)
                )
                measured = duration_named or clock_derived
                operands = (comparison.left, *comparison.comparators)
                ceiling = any(
                    measured
                    and (
                        isinstance(operator, (ast.Lt, ast.LtE))
                        and _is_numeric_literal(right)
                        or isinstance(operator, (ast.Gt, ast.GtE))
                        and _is_numeric_literal(left)
                    )
                    for left, operator, right in zip(
                        operands[:-1], comparison.ops, operands[1:], strict=True
                    )
                )
                if ceiling:
                    violations.append((node.lineno, ast.unparse(comparison)))
    return tuple(violations)


def _cpp_words(expression: str) -> set[str]:
    return set(re.findall(r"\b[A-Za-z_]\w*\b", expression))


def _cpp_clock_dataflow(source: str) -> tuple[set[str], set[str]]:
    """Find clock points and values derived from a clock-point difference."""
    assignments = tuple(_CPP_ASSIGNMENT.finditer(source))
    clock_points = {
        match.group("name")
        for match in assignments
        if _CPP_CLOCK_EXPRESSION.search(match.group("expression"))
        and "-" not in match.group("expression")
    }
    durations: set[str] = set()
    changed = True
    while changed:
        changed = False
        for match in assignments:
            name = match.group("name")
            expression = match.group("expression")
            words = _cpp_words(expression)
            temporal_operands = len(words & clock_points) + len(
                _CPP_CLOCK_EXPRESSION.findall(expression)
            )
            clock_difference = "-" in expression and temporal_operands >= 2
            derived = clock_difference or bool(words & durations)
            if derived and name not in durations:
                durations.add(name)
                changed = True
    return clock_points, durations


def _cpp_temporal_expression(
    expression: str, *, clock_points: set[str], durations: set[str]
) -> bool:
    words = _cpp_words(expression)
    if words & durations:
        return True
    temporal_operands = len(words & clock_points) + len(_CPP_CLOCK_EXPRESSION.findall(expression))
    return "-" in expression and temporal_operands >= 2


def _cpp_limit_expression(expression: str) -> bool:
    normalized = expression.strip()
    return bool(
        _CPP_DURATION_CONSTRUCTOR.search(normalized)
        or re.search(r"(?<![\w.])[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[uUlLfF]*)(?![\w.])", normalized)
        or re.search(r"(?<![\w.])\d+(?:ns|us|ms|s|min|h)(?!\w)", normalized)
    )


def _balanced_cpp_arguments(source: str, opening: int) -> tuple[str, str, int] | None:
    """Return the two top-level arguments of one ASSERT/EXPECT comparison."""
    depth = 0
    comma = -1
    for index in range(opening, len(source)):
        character = source[index]
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0:
                if comma < 0:
                    return None
                return source[opening + 1 : comma], source[comma + 1 : index], index + 1
        elif character == "," and depth == 1 and comma < 0:
            comma = index
    return None


def _mask_cpp_comments_and_literals(source: str) -> str:
    """Replace comments and quoted literals while preserving byte offsets/newlines."""
    masked = list(source)
    index = 0
    while index < len(source):
        if source.startswith("//", index):
            end = source.find("\n", index)
            end = len(source) if end < 0 else end
            for position in range(index, end):
                masked[position] = " "
            index = end
            continue
        if source.startswith("/*", index):
            end = source.find("*/", index + 2)
            end = len(source) if end < 0 else end + 2
            for position in range(index, end):
                if masked[position] != "\n":
                    masked[position] = " "
            index = end
            continue
        if source[index] in {'"', "'"}:
            quote = source[index]
            position = index
            while position < len(source):
                character = source[position]
                if character != "\n":
                    masked[position] = " "
                position += 1
                if character == "\\" and position < len(source):
                    if source[position] != "\n":
                        masked[position] = " "
                    position += 1
                    continue
                if position > index + 1 and character == quote:
                    break
            index = position
            continue
        index += 1
    return "".join(masked)


def _fragile_cpp_assertions(path: Path) -> tuple[tuple[int, str], ...]:
    source = path.read_text(encoding="utf-8")
    code = _mask_cpp_comments_and_literals(source)
    clock_points, durations = _cpp_clock_dataflow(code)
    violations: list[tuple[int, str]] = []

    def record_if_ceiling(
        left: str, operator: str, right: str, *, start: int, expression: str
    ) -> None:
        forward = (
            operator in {"<", "<="}
            and _cpp_temporal_expression(left, clock_points=clock_points, durations=durations)
            and _cpp_limit_expression(right)
        )
        reverse = (
            operator in {">", ">="}
            and _cpp_limit_expression(left)
            and _cpp_temporal_expression(right, clock_points=clock_points, durations=durations)
        )
        if forward or reverse:
            line = source.count("\n", 0, start) + 1
            violations.append((line, " ".join(expression.split())))

    for match in _CPP_INFIX_COMPARISON.finditer(code):
        record_if_ceiling(
            match.group("left"),
            match.group("operator"),
            match.group("right"),
            start=match.start(),
            expression=source[match.start() : match.end()],
        )

    for match in _CPP_TEST_MACRO.finditer(code):
        parsed = _balanced_cpp_arguments(code, match.end() - 1)
        if parsed is None:
            continue
        left, right, end = parsed
        macro = match.group(0)
        relation = re.search(r"_(LT|LE|GT|GE)", macro)
        assert relation is not None
        operator = {"LT": "<", "LE": "<=", "GT": ">", "GE": ">="}[relation.group(1)]
        record_if_ceiling(
            left,
            operator,
            right,
            start=match.start(),
            expression=source[match.start() : end],
        )
    return tuple(dict.fromkeys(violations))


@pytest.mark.parametrize(
    "source",
    (
        "import time\nstarted = time.monotonic()\nelapsed = time.monotonic() - started\nassert elapsed < 0.25\n",
        "from time import perf_counter as now\nbefore = now()\nassert now() - before <= 1.0\n",
        "from time import perf_counter as now\ndef test_case():\n before=now()\n assert now()-before < .5\n",
        "import time as clock\ntick = clock.monotonic\nbefore = tick()\nassert 0.5 > tick() - before\n",
        "import time as clock\ndef test_case():\n before=clock.monotonic()\n assert clock.monotonic()-before < .5\n",
        "import asyncio, time\nasync def run():\n before=time.monotonic()\n return time.monotonic()-before\nassert asyncio.run(run()) < 0.5\n",
        "_elapsed_us = probe()\nassert _elapsed_us < 500\n",
        "import time\ndef test_case():\n before=time.monotonic()\n elapsed=time.monotonic()-before\n assert elapsed < .5 and ok\n",
        "def test_case():\n durations = samples()\n assert all(duration < .5 for duration in durations)\n",
        "def test_case(report):\n assert report['elapsed_seconds'] < .5\n",
        "def test_case(result):\n assert result.elapsed < .5\n",
    ),
)
def test_wall_clock_guard_detects_speed_ceilings(tmp_path: Path, source: str) -> None:
    path = tmp_path / "test_fragile.py"
    path.write_text(source, encoding="utf-8")
    assert _fragile_assertions(path)


@pytest.mark.parametrize(
    "source",
    (
        "assert event.wait(timeout=0.5)\n",
        "thread.join(timeout=0.5)\nassert not thread.is_alive()\n",
        "subprocess.run(command, timeout=0.5)\n",
        "import time\ndeadline = time.monotonic() + 1\nwhile time.monotonic() < deadline: pass\nassert done\n",
        "elapsed_ns = report['elapsed_ns']\nassert elapsed_ns >= 0\n",
        "recorded_latency = evidence['latency']\nassert recorded_latency == 0.5\n",
        "import asyncio\nasync def run(): return 0.1\nassert asyncio.run(run()) < 0.5\n",
    ),
)
def test_wall_clock_guard_preserves_safety_timeouts_and_evidence(
    tmp_path: Path, source: str
) -> None:
    path = tmp_path / "test_safe.py"
    path.write_text(source, encoding="utf-8")
    assert _fragile_assertions(path) == ()


@pytest.mark.parametrize(
    "body",
    (
        "const auto elapsed = std::chrono::steady_clock::now() - before;\n"
        "if (elapsed < std::chrono::milliseconds(250)) return true;",
        "if (std::chrono::steady_clock::now() - before < "
        "std::chrono::milliseconds(5)) return true;",
        "const auto elapsed = std::chrono::steady_clock::now() - before;\n"
        "ASSERT_LT(elapsed, std::chrono::milliseconds(5));",
        "const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>("
        "std::chrono::steady_clock::now() - before).count();\n"
        "if (elapsed < 500) return true;",
        "std::chrono::milliseconds elapsed = std::chrono::duration_cast<"
        "std::chrono::milliseconds>(std::chrono::steady_clock::now() - before);\n"
        "EXPECT_LE(elapsed, std::chrono::milliseconds(5));",
        "using namespace std::chrono_literals;\n"
        "const auto elapsed = std::chrono::steady_clock::now() - before;\n"
        "ASSERT_LT(elapsed, 250ms);",
    ),
)
def test_wall_clock_guard_detects_cpp_speed_ceiling(tmp_path: Path, body: str) -> None:
    path = tmp_path / "fragile.cc"
    path.write_text(
        "const auto before = std::chrono::steady_clock::now();\noperation();\n" + body,
        encoding="utf-8",
    )
    assert _fragile_cpp_assertions(path)


def test_wall_clock_guard_preserves_cpp_deadline_and_wait_timeout(tmp_path: Path) -> None:
    path = tmp_path / "safe.cc"
    path.write_text(
        """
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::seconds(5);
        while (!done && std::chrono::steady_clock::now() < deadline) yield();
        ready.wait_for(lock, std::chrono::seconds(30));
        const auto retries = 2;
        const auto payload = build(before, retries - 1);
        ASSERT_LT(payload.size(), 10);
        // elapsed < std::chrono::milliseconds(1) is only documentation.
        log("elapsed < std::chrono::milliseconds(1)");
        """,
        encoding="utf-8",
    )
    assert _fragile_cpp_assertions(path) == ()


def test_tests_do_not_assert_wall_clock_speed() -> None:
    """Use logical work, fake clocks and barriers instead of elapsed-time ceilings."""
    violations = [
        f"{path.relative_to(ROOT)}:{line}: {expression}"
        for path in sorted(TESTS.rglob("test_*.py"))
        for line, expression in _fragile_assertions(path)
    ]
    assert not violations, (
        "wall-clock speed is not a correctness contract; assert deterministic work, "
        "state or a fake-clock deadline instead:\n" + "\n".join(violations)
    )


def test_cpp_tests_do_not_assert_wall_clock_speed() -> None:
    """Keep native correctness probes independent from runner scheduling speed."""
    paths = sorted(CPP_TESTS.rglob("*.cc")) + sorted(CPP_TESTS.rglob("*.cc.inc"))
    violations = [
        f"{path.relative_to(ROOT)}:{line}: {expression}"
        for path in paths
        for line, expression in _fragile_cpp_assertions(path)
    ]
    assert not violations, (
        "native wall-clock speed is not a correctness contract; use logical work, "
        "barriers or retained benchmark evidence instead:\n" + "\n".join(violations)
    )
