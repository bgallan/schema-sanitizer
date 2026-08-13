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


def _fixed_thread_sleeps(path: Path) -> tuple[tuple[int, str], ...]:
    """Find real-time sleeps used outside a bounded polling loop.

    A safety polling loop may sleep briefly between observations.  A standalone
    sleep, however, is commonly an attempt to let another thread run and turns
    host load into part of the test contract.  Async sleeps are excluded: long
    cancellable coroutines are deliberate lifecycle stimuli in several tests.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    sleep_callables: set[str] = set()
    time_modules = {"time"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            time_modules.update(
                alias.asname or alias.name for alias in node.names if alias.name == "time"
            )
        elif isinstance(node, ast.ImportFrom) and node.module == "time":
            sleep_callables.update(
                alias.asname or alias.name for alias in node.names if alias.name == "sleep"
            )

    violations: list[tuple[int, str]] = []
    scope_nodes = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)

    def polls_observable(loop: ast.While) -> bool:
        if not (isinstance(loop.test, ast.Constant) and bool(loop.test.value)):
            return True
        return any(
            isinstance(candidate, ast.If)
            and any(isinstance(child, ast.Break) for child in ast.walk(candidate))
            for candidate in ast.walk(loop)
        )

    class Visitor(ast.NodeVisitor):
        def __init__(self) -> None:
            self.ancestors: list[ast.AST] = []

        def generic_visit(self, node: ast.AST) -> None:
            self.ancestors.append(node)
            super().generic_visit(node)
            self.ancestors.pop()

        def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
            function = node.func
            is_sleep = (
                isinstance(function, ast.Name)
                and function.id in sleep_callables
                or isinstance(function, ast.Attribute)
                and isinstance(function.value, ast.Name)
                and function.value.id in time_modules
                and function.attr == "sleep"
            )
            if is_sleep:
                inside_polling_loop = False
                for ancestor in reversed(self.ancestors):
                    if isinstance(ancestor, ast.While) and polls_observable(ancestor):
                        inside_polling_loop = True
                        break
                    if isinstance(ancestor, scope_nodes):
                        break
                if not inside_polling_loop:
                    violations.append((node.lineno, ast.unparse(node)))
            self.generic_visit(node)

    Visitor().visit(tree)
    return tuple(violations)


def _fixed_async_sleeps(path: Path) -> tuple[tuple[int, str], ...]:
    """Find fixed async delays used as implicit scheduler synchronization.

    ``sleep(0)`` is an event-loop yield, while 60/3600-second sleeps model a
    deliberately blocked coroutine whose cancellation is under test.  Short
    sleeps inside a loop are also permitted when the loop observes state on
    every iteration.  Every other positive literal delay needs an explicit
    Event, Condition, hook, or fake clock instead.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    sleep_callables: set[str] = set()
    asyncio_modules = {"asyncio"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            asyncio_modules.update(
                alias.asname or alias.name for alias in node.names if alias.name == "asyncio"
            )
        elif isinstance(node, ast.ImportFrom) and node.module == "asyncio":
            sleep_callables.update(
                alias.asname or alias.name for alias in node.names if alias.name == "sleep"
            )

    violations: list[tuple[int, str]] = []
    scope_nodes = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)

    def polls_observable(loop: ast.While) -> bool:
        if not (isinstance(loop.test, ast.Constant) and bool(loop.test.value)):
            return True
        return any(
            isinstance(candidate, ast.If)
            and any(isinstance(child, ast.Break) for child in ast.walk(candidate))
            for candidate in ast.walk(loop)
        )

    class Visitor(ast.NodeVisitor):
        def __init__(self) -> None:
            self.ancestors: list[ast.AST] = []

        def generic_visit(self, node: ast.AST) -> None:
            self.ancestors.append(node)
            super().generic_visit(node)
            self.ancestors.pop()

        def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
            function = node.func
            is_sleep = (
                isinstance(function, ast.Name)
                and function.id in sleep_callables
                or isinstance(function, ast.Attribute)
                and isinstance(function.value, ast.Name)
                and function.value.id in asyncio_modules
                and function.attr == "sleep"
            )
            if is_sleep and node.args:
                delay = node.args[0]
                value: int | float | None = None
                if isinstance(delay, ast.Constant) and type(delay.value) in (int, float):
                    value = delay.value
                elif (
                    isinstance(delay, ast.UnaryOp)
                    and isinstance(delay.op, (ast.UAdd, ast.USub))
                    and isinstance(delay.operand, ast.Constant)
                    and type(delay.operand.value) in (int, float)
                ):
                    sign = -1 if isinstance(delay.op, ast.USub) else 1
                    value = sign * delay.operand.value
                fixed_delay = value is not None and value > 0 and value not in {60, 3600}
                if fixed_delay:
                    inside_polling_loop = False
                    for ancestor in reversed(self.ancestors):
                        if isinstance(ancestor, ast.While) and polls_observable(ancestor):
                            inside_polling_loop = True
                            break
                        if isinstance(ancestor, scope_nodes):
                            break
                    if not inside_polling_loop:
                        violations.append((node.lineno, ast.unparse(node)))
            self.generic_visit(node)

    Visitor().visit(tree)
    return tuple(violations)


def _telemetry_field_name(node: ast.AST) -> str | None:
    """Return a literal telemetry field selected by an expression."""
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Subscript):
        key = node.slice
        if isinstance(key, ast.Constant) and isinstance(key.value, str):
            return key.value
    return None


_STARTED_WORKER_NAMES = frozenset({"started", "started_workers", "workers_started"})
_WORKER_CAPACITY_NAMES = frozenset(
    {
        "capacity",
        "configured_workers",
        "effective_workers",
        "requested_workers",
        "worker_capacity",
        "workers",
    }
)
_PROBE_STARTED_RESULT_INDEX = {
    "operation_task_arena_mixed_lane_probe": 2,
    "operation_task_arena_output_preference_probe": 3,
    "operation_task_arena_output_steal_probe": 4,
}
_EXPLICIT_PREWARM_STARTED_ASSERTIONS = {
    (
        "tests/concurrency/test_concurrency_output_preference_is_dormant_through_eight_workers.py",
        "test_output_preference_is_dormant_through_eight_workers",
        "started == 8",
    ),
    (
        "tests/concurrency/test_concurrency_shallow_local_output_progress_at_four_workers.py",
        "test_shallow_local_output_progress_at_four_workers",
        "started == 4",
    ),
}


def _worker_name_roles(name: str) -> set[str]:
    normalized = name.lower().lstrip("_")
    if normalized in _STARTED_WORKER_NAMES:
        return {"started"}
    if normalized in _WORKER_CAPACITY_NAMES:
        return {"capacity"}
    return set()


def _worker_expression_roles(node: ast.AST, aliases: dict[str, set[str]]) -> set[str]:
    """Track simple worker-count values without inferring numeric relationships."""
    if isinstance(node, ast.Name):
        return set(aliases.get(node.id, _worker_name_roles(node.id)))
    if isinstance(node, ast.Constant):
        if type(node.value) is int and node.value > 0:
            return {"positive_constant"}
        return set()
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.UAdd):
        return _worker_expression_roles(node.operand, aliases)

    field = _telemetry_field_name(node)
    if field is not None:
        roles = _worker_name_roles(field)
        if roles:
            return roles
    if isinstance(node, ast.Subscript):
        container_roles = _worker_expression_roles(node.value, aliases)
        if isinstance(node.slice, ast.Constant) and type(node.slice.value) is int:
            marker = f"probe_started_at:{node.slice.value}"
            if marker in container_roles:
                return {"started"}
        return set()
    if not isinstance(node, ast.Call):
        return set()

    function_name = (
        node.func.id
        if isinstance(node.func, ast.Name)
        else node.func.attr
        if isinstance(node.func, ast.Attribute)
        else ""
    )
    if function_name in {"int", "operator.index"} and len(node.args) == 1:
        return _worker_expression_roles(node.args[0], aliases)
    if (
        isinstance(node.func, ast.Attribute)
        and node.func.attr == "get"
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and isinstance(node.args[0].value, str)
    ):
        return _worker_name_roles(node.args[0].value)
    started_index = _PROBE_STARTED_RESULT_INDEX.get(function_name)
    if started_index is not None:
        return {f"probe_started_at:{started_index}"}
    return set()


def _bind_worker_alias(
    target: ast.AST,
    value: ast.AST,
    aliases: dict[str, set[str]],
) -> None:
    """Apply one simple assignment to the local worker-value environment."""
    if isinstance(target, (ast.Tuple, ast.List)):
        if isinstance(value, (ast.Tuple, ast.List)) and len(target.elts) == len(value.elts):
            for element, element_value in zip(target.elts, value.elts, strict=True):
                _bind_worker_alias(element, element_value, aliases)
            return
        value_roles = _worker_expression_roles(value, aliases)
        probe_indices = {
            int(role.removeprefix("probe_started_at:"))
            for role in value_roles
            if role.startswith("probe_started_at:")
        }
        for index, element in enumerate(target.elts):
            if isinstance(element, ast.Starred):
                element = element.value
            if isinstance(element, ast.Name):
                aliases[element.id] = {"started"} if index in probe_indices else set()
        return
    if isinstance(target, ast.Name):
        aliases[target.id] = _worker_expression_roles(value, aliases)


def _local_worker_guard_nodes(
    scope: ast.Module | ast.FunctionDef | ast.AsyncFunctionDef,
) -> tuple[ast.AST, ...]:
    """Return source-ordered nodes without leaking aliases across nested scopes."""
    nested = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)
    nodes: list[ast.AST] = []

    def visit(node: ast.AST) -> None:
        if node is not scope and isinstance(node, nested):
            return
        if isinstance(node, (ast.Assign, ast.AnnAssign, ast.Assert)):
            nodes.append(node)
        for child in ast.iter_child_nodes(node):
            visit(child)

    visit(scope)
    return tuple(sorted(nodes, key=lambda node: (node.lineno, node.col_offset)))


def _lazy_worker_exact_equalities(path: Path) -> tuple[tuple[str, int, str], ...]:
    """Find exact lazy-start counts equated to a capacity or positive constant."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    violations: list[tuple[str, int, str]] = []
    for scope in _scopes(tree):
        aliases: dict[str, set[str]] = {}
        scope_name = scope.name if not isinstance(scope, ast.Module) else "<module>"
        for node in _local_worker_guard_nodes(scope):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    _bind_worker_alias(target, node.value, aliases)
                continue
            if isinstance(node, ast.AnnAssign) and node.value is not None:
                _bind_worker_alias(node.target, node.value, aliases)
                continue
            if isinstance(node, ast.AnnAssign):
                continue
            assert isinstance(node, ast.Assert)
            for comparison in (
                candidate for candidate in ast.walk(node.test) if isinstance(candidate, ast.Compare)
            ):
                operands = (comparison.left, *comparison.comparators)
                for left, operator, right in zip(
                    operands[:-1], comparison.ops, operands[1:], strict=True
                ):
                    if not isinstance(operator, ast.Eq):
                        continue
                    left_roles = _worker_expression_roles(left, aliases)
                    right_roles = _worker_expression_roles(right, aliases)
                    exact_target = {"capacity", "positive_constant"}
                    if (
                        "started" in left_roles
                        and bool(right_roles & exact_target)
                        or "started" in right_roles
                        and bool(left_roles & exact_target)
                    ):
                        violations.append((scope_name, node.lineno, ast.unparse(comparison)))
    return tuple(dict.fromkeys(violations))


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
    "source",
    (
        "import time\ntime.sleep(0.01)\n",
        "from time import sleep as pause\ndef test_case():\n pause(.05)\n",
        "import time as clock\ndef worker():\n clock.sleep(1)\n",
        "from time import sleep\nwhile True:\n sleep(.1)\n",
    ),
)
def test_thread_sleep_guard_detects_scheduler_delays(tmp_path: Path, source: str) -> None:
    path = tmp_path / "test_fragile_sleep.py"
    path.write_text(source, encoding="utf-8")
    assert _fixed_thread_sleeps(path)


@pytest.mark.parametrize(
    "source",
    (
        "import time\nwhile not ready():\n time.sleep(0.01)\n",
        "import asyncio\nasync def worker():\n await asyncio.sleep(60)\n",
    ),
)
def test_thread_sleep_guard_preserves_polling_and_async_stimuli(
    tmp_path: Path, source: str
) -> None:
    path = tmp_path / "test_safe_sleep.py"
    path.write_text(source, encoding="utf-8")
    assert _fixed_thread_sleeps(path) == ()


@pytest.mark.parametrize(
    "source",
    (
        "import asyncio\nasync def worker():\n await asyncio.sleep(0.01)\n",
        "import asyncio as aio\nasync def worker():\n await aio.sleep(.05)\n",
        "from asyncio import sleep as pause\nasync def worker():\n await pause(1)\n",
    ),
)
def test_async_sleep_guard_detects_scheduler_delays(tmp_path: Path, source: str) -> None:
    path = tmp_path / "test_fragile_async_sleep.py"
    path.write_text(source, encoding="utf-8")
    assert _fixed_async_sleeps(path)


@pytest.mark.parametrize(
    "source",
    (
        "import asyncio\nasync def worker():\n await asyncio.sleep(0)\n",
        "import asyncio\nasync def worker():\n await asyncio.sleep(60)\n",
        "import asyncio\nasync def worker():\n await asyncio.sleep(3600)\n",
        "import asyncio\nasync def worker(done):\n"
        " while not done.is_set():\n  await asyncio.sleep(0.01)\n",
    ),
)
def test_async_sleep_guard_preserves_yields_blockers_and_polling(
    tmp_path: Path, source: str
) -> None:
    path = tmp_path / "test_safe_async_sleep.py"
    path.write_text(source, encoding="utf-8")
    assert _fixed_async_sleeps(path) == ()


@pytest.mark.parametrize(
    "source",
    (
        "assert stats['started_workers'] == stats['effective_workers']\n",
        "assert report.effective_workers == report.started_workers\n",
        "def test_case(report):\n observed = report.started_workers\n capacity = "
        "report.effective_workers\n assert observed == capacity\n",
        "def test_case(stats):\n first = stats['started_workers']\n observed = first\n "
        "limit = 8\n assert limit == observed\n",
        "def test_case(stats, workers):\n assert int(stats.get('started_workers')) == workers\n",
        "def test_case():\n result = operation_task_arena_output_steal_probe(8)\n "
        "observed = result[4]\n assert observed == 8\n",
        "def test_case():\n _, _, _, observed, _, _ = "
        "operation_task_arena_output_preference_probe(4)\n assert observed == 4\n",
    ),
)
def test_lazy_worker_guard_detects_exact_capacity_assumptions(tmp_path: Path, source: str) -> None:
    path = tmp_path / "test_fragile_workers.py"
    path.write_text(source, encoding="utf-8")
    assert _lazy_worker_exact_equalities(path)


@pytest.mark.parametrize(
    "source",
    (
        "assert 1 <= counters['started_workers'] <= stats['effective_workers']\n",
        "assert stats['started_workers'] == 0\n",
        "def test_case(stats):\n observed = stats['started_workers']\n "
        "assert observed == finished\n",
        "def test_case():\n started = Event()\n capacity = queue.capacity\n "
        "assert started.is_set() == (capacity > 0)\n",
        "def test_case(stats):\n observed = stats['finished_workers']\n assert observed == 8\n",
    ),
)
def test_lazy_worker_guard_preserves_bounds_inline_and_unrelated_counts(
    tmp_path: Path, source: str
) -> None:
    path = tmp_path / "test_safe_workers.py"
    path.write_text(source, encoding="utf-8")
    assert _lazy_worker_exact_equalities(path) == ()


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


def test_tests_do_not_use_fixed_thread_sleeps_as_synchronization() -> None:
    """Require an observable handshake when one test thread waits for another."""
    violations = [
        f"{path.relative_to(ROOT)}:{line}: {expression}"
        for path in sorted(TESTS.rglob("test_*.py"))
        for line, expression in _fixed_thread_sleeps(path)
    ]
    assert not violations, (
        "fixed sleeps cannot prove another thread reached the intended state; "
        "use an Event, Condition, Barrier, or bounded polling observation instead:\n"
        + "\n".join(violations)
    )


def test_tests_do_not_use_fixed_async_sleeps_as_synchronization() -> None:
    """Require an observable async handshake instead of elapsed wall time."""
    violations = [
        f"{path.relative_to(ROOT)}:{line}: {expression}"
        for path in sorted(TESTS.rglob("test_*.py"))
        for line, expression in _fixed_async_sleeps(path)
    ]
    assert not violations, (
        "fixed async delays cannot prove another task reached the intended state; "
        "use an Event, Condition, hook, fake clock, or observable polling loop instead:\n"
        + "\n".join(violations)
    )


def test_tests_do_not_require_every_lazy_worker_to_start() -> None:
    """Configured worker capacity is a ceiling unless a probe forces every lane."""
    violations = [
        f"{path.relative_to(ROOT)}:{line}: {expression}"
        for path in sorted(TESTS.rglob("test_*.py"))
        for function, line, expression in _lazy_worker_exact_equalities(path)
        if (path.relative_to(ROOT).as_posix(), function, expression)
        not in _EXPLICIT_PREWARM_STARTED_ASSERTIONS
    ]
    assert not violations, (
        "lazy worker startup is scheduler-dependent; compare actual starts to "
        "effective capacity with an upper bound, or use an explicit all-worker "
        "barrier probe:\n" + "\n".join(violations)
    )


def test_exact_started_aliases_are_limited_to_explicit_prewarm_probes() -> None:
    """Only the probe that blocks every low-core lane may require exact starts."""
    observed = {
        (path.relative_to(ROOT).as_posix(), function, expression)
        for path in sorted((TESTS / "concurrency").glob("test_*.py"))
        for function, _line, expression in _lazy_worker_exact_equalities(path)
    }
    assert observed == _EXPLICIT_PREWARM_STARTED_ASSERTIONS

    expected_calls = {
        (path, function): int(expression.rpartition(" ")[2])
        for path, function, expression in _EXPLICIT_PREWARM_STARTED_ASSERTIONS
    }
    for (relative_path, function_name), expected_workers in expected_calls.items():
        tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
        function = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == function_name
        )
        calls = [
            call
            for call in ast.walk(function)
            if isinstance(call, ast.Call)
            and isinstance(call.func, ast.Attribute)
            and call.func.attr == "operation_task_arena_output_preference_probe"
        ]
        assert len(calls) == 1
        assert calls[0].args
        assert isinstance(calls[0].args[0], ast.Constant)
        assert calls[0].args[0].value == expected_workers

    probe = (ROOT / "cpp/src/api/python_abi3/runtime/arena_scheduler_probe.cc").read_text(
        encoding="utf-8"
    )
    probe_function = probe.index("py_operation_task_arena_output_preference_probe")
    prewarm = probe[
        probe.index("// The low-core contract reports", probe_function) : probe.index(
            "std::atomic<std::size_t> blockers_started", probe_function
        )
    ]
    assert "if (workers <= 8U)" in prewarm
    assert "for (std::size_t ordinal = 0; ordinal < workers; ++ordinal)" in prewarm
    assert "arena->started_workers() != workers" in prewarm
    assert "output preference probe did not prewarm every worker" in prewarm


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
