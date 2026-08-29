#!/usr/bin/env python3
"""Reject test contracts that depend on chance, wall-clock speed, or scheduler timing.

It analyzes Python and C++ tests for nondeterministic randomness, vacuous assertions,
speed ceilings, scheduler sleeps, and worker-capacity assumptions while allowing
bounded safety waits.
"""

from __future__ import annotations

import argparse
import ast
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
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
_OVERLAP_TELEMETRY_NAMES = frozenset({"peak_active_tasks"})
_PROMOTION_TELEMETRY_NAMES = frozenset(
    {
        "output_preference_bypasses",
        "outputs_before_broad",
        "promoted",
    }
)
_NONDETERMINISTIC_FUNCTIONS = {
    "os": frozenset({"getrandom", "urandom"}),
    "random": frozenset(
        {
            "betavariate",
            "binomialvariate",
            "choice",
            "choices",
            "expovariate",
            "gammavariate",
            "gauss",
            "getrandbits",
            "lognormvariate",
            "normalvariate",
            "paretovariate",
            "randbytes",
            "randint",
            "random",
            "randrange",
            "sample",
            "shuffle",
            "triangular",
            "uniform",
            "vonmisesvariate",
            "weibullvariate",
        }
    ),
    "secrets": frozenset(
        {"choice", "randbelow", "randbits", "token_bytes", "token_hex", "token_urlsafe"}
    ),
    "uuid": frozenset({"uuid1", "uuid4", "uuid6", "uuid7", "uuid8"}),
}
_PROBE_STARTED_RESULT_INDEX = {
    "operation_task_arena_mixed_lane_probe": 2,
    "operation_task_arena_output_preference_probe": 3,
    "operation_task_arena_output_steal_probe": 4,
}
_CPP_ASSIGNMENT = re.compile(r"\b(?P<name>[A-Za-z_]\w*)\s*=\s*(?P<expression>.*)", re.DOTALL)
_CPP_CLOCK_EXPRESSION = re.compile(r"(?:steady_clock|high_resolution_clock|system_clock)::now\s*\(")
_CPP_DURATION_CONSTRUCTOR = re.compile(
    r"(?:std::chrono::)?(?:nanoseconds|microseconds|milliseconds|seconds|minutes|hours)"
    r"\s*\([^()]*\)"
)
_CPP_TEST_MACRO = re.compile(r"\b(?:ASSERT|EXPECT)_(?:LT|LE|GT|GE)\s*\(")
_CPP_NUMBER = re.compile(r"(?<![\w.])[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[uUlLfF]*)(?![\w.])")
_CPP_LITERAL_DURATION = re.compile(r"(?<![\w.])\d+(?:ns|us|ms|s|min|h)(?!\w)")


@dataclass(frozen=True)
class PythonFindings:
    """Determinism violations found in one Python test module."""

    nondeterministic_randomness: tuple[tuple[int, str], ...] = ()
    vacuous_assertions: tuple[tuple[int, str], ...] = ()
    wall_clock: tuple[tuple[int, str], ...] = ()
    thread_sleeps: tuple[tuple[int, str], ...] = ()
    async_sleeps: tuple[tuple[int, str], ...] = ()
    lazy_workers: tuple[tuple[str, int, str], ...] = ()
    prewarmed_lazy_workers: tuple[tuple[str, int, str], ...] = ()
    incidental_overlap: tuple[tuple[int, str], ...] = ()
    incidental_promotions: tuple[tuple[int, str], ...] = ()

    def empty(self) -> bool:
        """Return whether the module has no determinism violations."""
        return not (
            self.nondeterministic_randomness
            or self.vacuous_assertions
            or self.wall_clock
            or self.thread_sleeps
            or self.async_sleeps
            or self.lazy_workers
            or self.incidental_overlap
            or self.incidental_promotions
        )


def _assigned_names(node: ast.AST) -> set[str]:
    """Collect names assigned within the selected AST scope."""
    return {
        child.id
        for child in ast.walk(node)
        if isinstance(child, ast.Name) and isinstance(child.ctx, ast.Store)
    }


def _loaded_names(node: ast.AST) -> set[str]:
    """Collect names loaded within the selected AST scope."""
    return {
        child.id
        for child in ast.walk(node)
        if isinstance(child, ast.Name) and isinstance(child.ctx, ast.Load)
    }


def _called_names(node: ast.AST) -> set[str]:
    """Collect directly invoked function names within the selected AST scope."""
    return {
        child.func.id
        for child in ast.walk(node)
        if isinstance(child, ast.Call) and isinstance(child.func, ast.Name)
    }


def _numeric_literal(node: ast.AST) -> int | float | None:
    """Evaluate a signed numeric literal without executing source code."""
    if isinstance(node, ast.Constant) and type(node.value) in (int, float):
        return node.value
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
        value = _numeric_literal(node.operand)
        if value is not None:
            return -value if isinstance(node.op, ast.USub) else value
    return None


def _nondeterministic_random_findings(tree: ast.Module) -> tuple[tuple[int, str], ...]:
    """Find entropy-backed or implicitly seeded randomness in Python tests."""
    module_aliases: dict[str, str] = {}
    callable_aliases: dict[str, tuple[str, str]] = {}
    findings: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in _NONDETERMINISTIC_FUNCTIONS or alias.name == "random":
                    module_aliases[alias.asname or alias.name] = alias.name
        elif isinstance(node, ast.ImportFrom) and node.module in {
            *_NONDETERMINISTIC_FUNCTIONS,
            "random",
        }:
            for alias in node.names:
                if alias.name == "*":
                    findings.append((node.lineno, ast.unparse(node)))
                    continue
                callable_aliases[alias.asname or alias.name] = (node.module, alias.name)

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        resolved: tuple[str, str] | None = None
        if isinstance(node.func, ast.Name):
            resolved = callable_aliases.get(node.func.id)
        elif isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name):
            module = module_aliases.get(node.func.value.id)
            if module is not None:
                resolved = (module, node.func.attr)
        if resolved is None:
            continue
        module, member = resolved
        nondeterministic = member in _NONDETERMINISTIC_FUNCTIONS.get(module, ())
        if module == "random" and member == "SystemRandom":
            nondeterministic = True
        elif module == "random" and member == "Random":
            nondeterministic = not node.args or (
                isinstance(node.args[0], ast.Constant) and node.args[0].value is None
            )
        elif module == "random" and member == "seed":
            nondeterministic = not node.args or (
                isinstance(node.args[0], ast.Constant) and node.args[0].value is None
            )
        if nondeterministic:
            findings.append((node.lineno, ast.unparse(node)))
    return tuple(dict.fromkeys(findings))


def _vacuous_assertion_findings(tree: ast.Module) -> tuple[tuple[int, str], ...]:
    """Find OR assertions whose empty-input branch can skip the claimed evidence."""
    findings: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assert) or not isinstance(node.test, ast.BoolOp):
            continue
        if not isinstance(node.test.op, ast.Or):
            continue
        empty_guards = {
            ast.dump(value.operand, include_attributes=False)
            for value in node.test.values
            if isinstance(value, ast.UnaryOp) and isinstance(value.op, ast.Not)
        }
        guarded_iterations = {
            ast.dump(generator.iter, include_attributes=False)
            for value in node.test.values
            for call in ast.walk(value)
            if isinstance(call, ast.Call)
            and isinstance(call.func, ast.Name)
            and call.func.id in {"all", "any"}
            for argument in call.args
            if isinstance(argument, (ast.GeneratorExp, ast.ListComp, ast.SetComp))
            for generator in argument.generators
        }
        if empty_guards & guarded_iterations:
            findings.append((node.lineno, ast.unparse(node.test)))
    return tuple(dict.fromkeys(findings))


def _scope_nodes(
    scope: ast.Module | ast.FunctionDef | ast.AsyncFunctionDef,
) -> tuple[ast.AST, ...]:
    """Collect nodes in one lexical scope while skipping nested definitions."""
    nested = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)
    collected: list[ast.AST] = []

    def visit(node: ast.AST) -> None:
        """Collect nodes in the current scope while skipping nested scopes."""
        if node is not scope and isinstance(node, nested):
            return
        collected.append(node)
        for child in ast.iter_child_nodes(node):
            visit(child)

    visit(scope)
    return tuple(collected)


def _scopes(tree: ast.Module) -> tuple[ast.Module | ast.FunctionDef | ast.AsyncFunctionDef, ...]:
    """Return the module and every nested function scope to analyze independently."""
    return (tree,) + tuple(
        node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    )


def _aliases(
    nodes: tuple[ast.AST, ...],
    module: str,
    member_names: frozenset[str],
    *,
    inherited_callables: set[str] | None = None,
    inherited_modules: set[str] | None = None,
) -> tuple[set[str], set[str]]:
    """Resolve local aliases for the selected module members."""
    callables = set(member_names if inherited_callables is None else inherited_callables)
    modules = set({module} if inherited_modules is None else inherited_modules)
    for node in nodes:
        if isinstance(node, ast.Import):
            modules.update(
                alias.asname or alias.name for alias in node.names if alias.name == module
            )
        elif isinstance(node, ast.ImportFrom) and node.module == module:
            callables.update(
                alias.asname or alias.name for alias in node.names if alias.name in member_names
            )
    changed = True
    while changed:
        changed = False
        for node in nodes:
            if not isinstance(node, (ast.Assign, ast.AnnAssign)) or node.value is None:
                continue
            value = node.value
            aliases_member = (
                isinstance(value, ast.Name)
                and value.id in callables
                or isinstance(value, ast.Attribute)
                and isinstance(value.value, ast.Name)
                and value.value.id in modules
                and value.attr in member_names
            )
            if not aliases_member:
                continue
            targets = node.targets if isinstance(node, ast.Assign) else (node.target,)
            assigned = set().union(*(_assigned_names(target) for target in targets))
            if not assigned <= callables:
                callables.update(assigned)
                changed = True
    return callables, modules


def _call_matches(
    node: ast.AST,
    *,
    callables: set[str],
    modules: set[str],
    members: frozenset[str],
) -> bool:
    """Return whether a call targets one of the selected functions."""
    if not isinstance(node, ast.Call):
        return False
    function = node.func
    return (
        isinstance(function, ast.Name)
        and function.id in callables
        or isinstance(function, ast.Attribute)
        and isinstance(function.value, ast.Name)
        and function.value.id in modules
        and function.attr in members
    )


def _contains_call(
    node: ast.AST,
    *,
    callables: set[str],
    modules: set[str],
    members: frozenset[str],
) -> bool:
    """Return whether the nodes contain a direct call to any selected name."""
    return any(
        _call_matches(child, callables=callables, modules=modules, members=members)
        for child in ast.walk(node)
    )


def _temporal_value_names(node: ast.AST) -> set[str]:
    """Infer names that carry values derived from a clock call."""
    values: set[str] = set()

    class Visitor(ast.NodeVisitor):
        def visit_Call(self, candidate: ast.Call) -> None:  # noqa: N802
            """Visit call nodes while collecting findings."""
            for argument in candidate.args:
                self.visit(argument)
            for keyword in candidate.keywords:
                self.visit(keyword.value)

        def visit_Name(self, candidate: ast.Name) -> None:  # noqa: N802
            """Visit name nodes while collecting findings."""
            if isinstance(candidate.ctx, ast.Load):
                values.add(candidate.id)

        def visit_Attribute(self, candidate: ast.Attribute) -> None:  # noqa: N802
            """Visit attribute nodes while collecting findings."""
            values.add(candidate.attr)
            self.visit(candidate.value)

        def visit_Subscript(self, candidate: ast.Subscript) -> None:  # noqa: N802
            """Visit subscript nodes while collecting findings."""
            if isinstance(candidate.slice, ast.Constant) and isinstance(candidate.slice.value, str):
                values.add(candidate.slice.value)
            self.visit(candidate.value)

    Visitor().visit(node)
    return {
        value for value in values if any(part in value.lower() for part in _DURATION_NAME_PARTS)
    }


def _clock_values(
    nodes: tuple[ast.AST, ...], *, callables: set[str], modules: set[str]
) -> set[str]:
    """Collect expressions whose values originate from wall-clock functions."""
    values: set[str] = set()
    changed = True
    while changed:
        changed = False
        for node in nodes:
            if not isinstance(node, (ast.Assign, ast.AnnAssign)) or node.value is None:
                continue
            if not (
                _contains_call(
                    node.value,
                    callables=callables,
                    modules=modules,
                    members=_CLOCK_NAMES,
                )
                or _loaded_names(node.value) & values
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
    """Identify helper functions that return or transform temporal values."""
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
            callables, modules = _aliases(
                nodes,
                "time",
                _CLOCK_NAMES,
                inherited_callables=module_callables,
                inherited_modules=module_modules,
            )
            values = _clock_values(nodes, callables=callables, modules=modules)
            returns_time = any(
                isinstance(node, ast.Return)
                and node.value is not None
                and (
                    _contains_call(
                        node.value,
                        callables=callables,
                        modules=modules,
                        members=_CLOCK_NAMES,
                    )
                    or bool(_loaded_names(node.value) & values)
                    or bool(_temporal_value_names(node.value))
                    or any(
                        isinstance(child, ast.Call)
                        and isinstance(child.func, ast.Name)
                        and child.func.id in functions
                        for child in ast.walk(node.value)
                    )
                )
                for node in nodes
            )
            if returns_time and function.name not in functions:
                functions.add(function.name)
                changed = True
    return functions


def _wall_clock_findings(tree: ast.Module) -> tuple[tuple[int, str], ...]:
    """Find assertions that impose fragile wall-clock speed ceilings."""
    module_nodes = _scope_nodes(tree)
    module_callables, module_modules = _aliases(module_nodes, "time", _CLOCK_NAMES)
    temporal_functions = _temporal_functions(
        tree,
        module_callables=module_callables,
        module_modules=module_modules,
    )
    findings: list[tuple[int, str]] = []
    for scope in _scopes(tree):
        nodes = _scope_nodes(scope)
        callables, modules = _aliases(
            nodes,
            "time",
            _CLOCK_NAMES,
            inherited_callables=module_callables,
            inherited_modules=module_modules,
        )
        values = _clock_values(nodes, callables=callables, modules=modules)
        for node in nodes:
            if not isinstance(node, ast.Assert):
                continue
            for comparison in (
                child for child in ast.walk(node.test) if isinstance(child, ast.Compare)
            ):
                names = _loaded_names(comparison)
                measured = (
                    bool(_temporal_value_names(comparison))
                    or any(
                        part in name.lower()
                        for name in names - _called_names(comparison)
                        for part in _DURATION_NAME_PARTS
                    )
                    or _contains_call(
                        comparison,
                        callables=callables,
                        modules=modules,
                        members=_CLOCK_NAMES,
                    )
                    or bool(names & values)
                    or any(
                        isinstance(child, ast.Call)
                        and isinstance(child.func, ast.Name)
                        and child.func.id in temporal_functions
                        for child in ast.walk(comparison)
                    )
                )
                operands = (comparison.left, *comparison.comparators)
                ceiling = any(
                    measured
                    and (
                        isinstance(operator, (ast.Lt, ast.LtE))
                        and _numeric_literal(right) is not None
                        or isinstance(operator, (ast.Gt, ast.GtE))
                        and _numeric_literal(left) is not None
                    )
                    for left, operator, right in zip(
                        operands[:-1], comparison.ops, operands[1:], strict=True
                    )
                )
                if ceiling:
                    findings.append((node.lineno, ast.unparse(comparison)))
    return tuple(dict.fromkeys(findings))


def _polls_observable(loop: ast.While) -> bool:
    """Return whether a polling loop checks an observable state transition."""
    if not (isinstance(loop.test, ast.Constant) and bool(loop.test.value)):
        return True
    return any(
        isinstance(candidate, ast.If)
        and any(isinstance(child, ast.Break) for child in ast.walk(candidate))
        for candidate in ast.walk(loop)
    )


def _sleep_findings(
    tree: ast.Module,
) -> tuple[tuple[tuple[int, str], ...], tuple[tuple[int, str], ...]]:
    """Classify sleeps as scheduler dependencies, polling, yielding, or safety waits."""
    nodes = tuple(ast.walk(tree))
    thread_callables, time_modules = _aliases(nodes, "time", frozenset({"sleep"}))
    async_callables, asyncio_modules = _aliases(nodes, "asyncio", frozenset({"sleep"}))
    thread: list[tuple[int, str]] = []
    asynchronous: list[tuple[int, str]] = []
    scope_nodes = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)

    class Visitor(ast.NodeVisitor):
        def __init__(self) -> None:
            """Initialize visitor state for ancestors."""
            self.ancestors: list[ast.AST] = []

        def generic_visit(self, node: ast.AST) -> None:
            """Visit child nodes while preserving the current analysis context."""
            self.ancestors.append(node)
            super().generic_visit(node)
            self.ancestors.pop()

        def inside_poll(self) -> bool:
            """Return whether traversal is currently inside a recognized polling loop."""
            for ancestor in reversed(self.ancestors):
                if isinstance(ancestor, ast.While) and _polls_observable(ancestor):
                    return True
                if isinstance(ancestor, scope_nodes):
                    break
            return False

        def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
            """Visit call nodes while collecting findings."""
            if _call_matches(
                node,
                callables=thread_callables,
                modules=time_modules,
                members=frozenset({"sleep"}),
            ):
                if not self.inside_poll():
                    thread.append((node.lineno, ast.unparse(node)))
            elif (
                _call_matches(
                    node,
                    callables=async_callables,
                    modules=asyncio_modules,
                    members=frozenset({"sleep"}),
                )
                and node.args
            ):
                delay = _numeric_literal(node.args[0])
                if (
                    delay is not None
                    and delay > 0
                    and delay not in {60, 3600}
                    and not self.inside_poll()
                ):
                    asynchronous.append((node.lineno, ast.unparse(node)))
            self.generic_visit(node)

    Visitor().visit(tree)
    return tuple(thread), tuple(asynchronous)


def _field_name(node: ast.AST) -> str | None:
    """Return the terminal field name referenced by an AST expression."""
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant):
        return node.slice.value if isinstance(node.slice.value, str) else None
    return None


def _worker_roles(name: str) -> set[str]:
    """Infer capacity, started-worker, and prewarm roles for local names."""
    normalized = name.lower().lstrip("_")
    if normalized in _STARTED_WORKER_NAMES:
        return {"started"}
    if normalized in _WORKER_CAPACITY_NAMES:
        return {"capacity"}
    return set()


def _worker_expression_roles(node: ast.AST, aliases: dict[str, set[str]]) -> set[str]:
    """Infer worker roles carried by an arbitrary expression."""
    if isinstance(node, ast.Name):
        return set(aliases.get(node.id, _worker_roles(node.id)))
    value = _numeric_literal(node)
    if value is not None:
        return {"positive_constant"} if value > 0 else set()
    field = _field_name(node)
    if field is not None and (roles := _worker_roles(field)):
        return roles
    if isinstance(node, ast.Subscript):
        container = _worker_expression_roles(node.value, aliases)
        index = node.slice.value if isinstance(node.slice, ast.Constant) else None
        return {"started"} if f"probe_started_at:{index}" in container else set()
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
        return _worker_roles(node.args[0].value)
    started_index = _PROBE_STARTED_RESULT_INDEX.get(function_name)
    return {f"probe_started_at:{started_index}"} if started_index is not None else set()


def _bind_worker_alias(target: ast.AST, value: ast.AST, aliases: dict[str, set[str]]) -> None:
    """Propagate worker-role information through one assignment target."""
    if isinstance(target, (ast.Tuple, ast.List)):
        if isinstance(value, (ast.Tuple, ast.List)) and len(target.elts) == len(value.elts):
            for element, element_value in zip(target.elts, value.elts, strict=True):
                _bind_worker_alias(element, element_value, aliases)
            return
        indices = {
            int(role.removeprefix("probe_started_at:"))
            for role in _worker_expression_roles(value, aliases)
            if role.startswith("probe_started_at:")
        }
        for index, element in enumerate(target.elts):
            element = element.value if isinstance(element, ast.Starred) else element
            if isinstance(element, ast.Name):
                aliases[element.id] = {"started"} if index in indices else set()
    elif isinstance(target, ast.Name):
        aliases[target.id] = _worker_expression_roles(value, aliases)


def _local_guard_nodes(
    scope: ast.Module | ast.FunctionDef | ast.AsyncFunctionDef,
) -> tuple[ast.AST, ...]:
    """Collect nodes belonging to the current control-flow guard."""
    return tuple(
        sorted(
            (
                node
                for node in _scope_nodes(scope)
                if isinstance(node, (ast.Assign, ast.AnnAssign, ast.Assert))
            ),
            key=lambda node: (node.lineno, node.col_offset),
        )
    )


def _prewarm_counts(scope: ast.AST) -> set[int]:
    """Find explicit prewarm counts associated with lazy worker pools."""
    return {
        call.args[0].value
        for call in ast.walk(scope)
        if isinstance(call, ast.Call)
        and isinstance(call.func, (ast.Name, ast.Attribute))
        and (call.func.id if isinstance(call.func, ast.Name) else call.func.attr)
        == "operation_task_arena_output_preference_probe"
        and call.args
        and isinstance(call.args[0], ast.Constant)
        and type(call.args[0].value) is int
    }


def _lazy_worker_findings(
    tree: ast.Module,
) -> tuple[tuple[tuple[str, int, str], ...], tuple[tuple[str, int, str], ...]]:
    """Find assertions that assume lazy pools immediately reach capacity."""
    findings: list[tuple[str, int, str]] = []
    prewarmed_findings: list[tuple[str, int, str]] = []
    for scope in _scopes(tree):
        aliases: dict[str, set[str]] = {}
        scope_name = scope.name if not isinstance(scope, ast.Module) else "<module>"
        prewarmed = _prewarm_counts(scope)
        for node in _local_guard_nodes(scope):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    _bind_worker_alias(target, node.value, aliases)
                continue
            if isinstance(node, ast.AnnAssign):
                if node.value is not None:
                    _bind_worker_alias(node.target, node.value, aliases)
                continue
            for comparison in (
                child for child in ast.walk(node.test) if isinstance(child, ast.Compare)
            ):
                operands = (comparison.left, *comparison.comparators)
                for left, operator, right in zip(
                    operands[:-1], comparison.ops, operands[1:], strict=True
                ):
                    if not isinstance(operator, ast.Eq):
                        continue
                    left_roles = _worker_expression_roles(left, aliases)
                    right_roles = _worker_expression_roles(right, aliases)
                    target_roles = {"capacity", "positive_constant"}
                    fragile = (
                        "started" in left_roles
                        and bool(right_roles & target_roles)
                        or "started" in right_roles
                        and bool(left_roles & target_roles)
                    )
                    compared = {
                        child.value
                        for child in ast.walk(comparison)
                        if isinstance(child, ast.Constant) and type(child.value) is int
                    }
                    if fragile:
                        finding = (scope_name, node.lineno, ast.unparse(comparison))
                        findings.append(finding)
                        if prewarmed & compared:
                            prewarmed_findings.append(finding)
    return tuple(dict.fromkeys(findings)), tuple(dict.fromkeys(prewarmed_findings))


def _telemetry_expression(
    node: ast.AST,
    aliases: set[str],
    field_names: frozenset[str],
) -> bool:
    """Return whether an expression reads selected scheduler telemetry."""
    if isinstance(node, ast.Name):
        return node.id in aliases or node.id in field_names
    if _field_name(node) in field_names:
        return True
    if not isinstance(node, ast.Call):
        return False
    function_name = (
        node.func.id
        if isinstance(node.func, ast.Name)
        else node.func.attr
        if isinstance(node.func, ast.Attribute)
        else ""
    )
    if function_name in {"int", "operator.index"} and len(node.args) == 1:
        return _telemetry_expression(node.args[0], aliases, field_names)
    return bool(
        isinstance(node.func, ast.Attribute)
        and node.func.attr == "get"
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and node.args[0].value in field_names
    )


def _requires_scheduler_observation(
    left: ast.AST,
    operator: ast.cmpop,
    right: ast.AST,
    aliases: set[str],
    field_names: frozenset[str],
    incidental_threshold: float,
) -> bool:
    """Return whether a comparison requires scheduler evidence above a safe bound."""
    left_value = _numeric_literal(left)
    right_value = _numeric_literal(right)
    if _telemetry_expression(left, aliases, field_names) and right_value is not None:
        return bool(
            isinstance(operator, ast.Gt)
            and right_value >= incidental_threshold
            or isinstance(operator, ast.GtE)
            and right_value > incidental_threshold
            or isinstance(operator, ast.NotEq)
            and incidental_threshold == 0
            and right_value == 0
        )
    if _telemetry_expression(right, aliases, field_names) and left_value is not None:
        return bool(
            isinstance(operator, ast.Lt)
            and left_value >= incidental_threshold
            or isinstance(operator, ast.LtE)
            and left_value > incidental_threshold
            or isinstance(operator, ast.NotEq)
            and incidental_threshold == 0
            and left_value == 0
        )
    return False


def _incidental_scheduler_findings(
    tree: ast.Module,
    field_names: frozenset[str],
    incidental_threshold: float,
) -> tuple[tuple[int, str], ...]:
    """Find assertions that require nondeterministic scheduler observations."""
    findings: list[tuple[int, str]] = []
    for scope in _scopes(tree):
        aliases: set[str] = set()
        for node in _local_guard_nodes(scope):
            if isinstance(node, ast.Assign):
                assigned = set().union(*(_assigned_names(target) for target in node.targets))
                derived = _telemetry_expression(node.value, aliases, field_names)
                aliases.difference_update(assigned)
                if derived:
                    aliases.update(assigned)
                continue
            if isinstance(node, ast.AnnAssign):
                assigned = _assigned_names(node.target)
                derived = node.value is not None and _telemetry_expression(
                    node.value, aliases, field_names
                )
                aliases.difference_update(assigned)
                if derived:
                    aliases.update(assigned)
                continue
            for comparison in (
                child for child in ast.walk(node.test) if isinstance(child, ast.Compare)
            ):
                operands = (comparison.left, *comparison.comparators)
                if any(
                    _requires_scheduler_observation(
                        left,
                        operator,
                        right,
                        aliases,
                        field_names,
                        incidental_threshold,
                    )
                    for left, operator, right in zip(
                        operands[:-1], comparison.ops, operands[1:], strict=True
                    )
                ):
                    findings.append((node.lineno, ast.unparse(comparison)))
    return tuple(dict.fromkeys(findings))


@lru_cache(maxsize=None)
def analyze_python(path: Path) -> PythonFindings:
    """Parse and apply every Python determinism rule to one test module."""
    try:
        source = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return PythonFindings()
    tree = ast.parse(source, filename=str(path))
    thread_sleeps, async_sleeps = _sleep_findings(tree)
    lazy_workers, prewarmed_lazy_workers = _lazy_worker_findings(tree)
    return PythonFindings(
        nondeterministic_randomness=_nondeterministic_random_findings(tree),
        vacuous_assertions=_vacuous_assertion_findings(tree),
        wall_clock=_wall_clock_findings(tree),
        thread_sleeps=thread_sleeps,
        async_sleeps=async_sleeps,
        lazy_workers=lazy_workers,
        prewarmed_lazy_workers=prewarmed_lazy_workers,
        incidental_overlap=_incidental_scheduler_findings(tree, _OVERLAP_TELEMETRY_NAMES, 1),
        incidental_promotions=_incidental_scheduler_findings(tree, _PROMOTION_TELEMETRY_NAMES, 0),
    )


def unapproved_lazy_workers(path: Path) -> tuple[tuple[str, int, str], ...]:
    """Return lazy-worker equalities not backed by an explicit prewarm probe."""
    findings = analyze_python(path)
    allowed = set(findings.prewarmed_lazy_workers)
    return tuple(finding for finding in findings.lazy_workers if finding not in allowed)


def incidental_overlap_assertions(path: Path) -> tuple[tuple[int, str], ...]:
    """Return assertions that require live tasks to overlap incidentally."""
    return analyze_python(path).incidental_overlap


def incidental_promotion_assertions(path: Path) -> tuple[tuple[int, str], ...]:
    """Return assertions that require a scheduler-dependent promotion."""
    return analyze_python(path).incidental_promotions


def _cpp_words(expression: str) -> set[str]:
    """Tokenize the C++ source while retaining source offsets."""
    return set(re.findall(r"\b[A-Za-z_]\w*\b", expression))


def _cpp_statements(code: str) -> tuple[tuple[int, str], ...]:
    """Split C++ source into offset-preserving statements."""
    statements: list[tuple[int, str]] = []
    start = 0
    for index, character in enumerate(code):
        if character == ";":
            statements.append((start, code[start:index]))
            start = index + 1
    if start < len(code):
        statements.append((start, code[start:]))
    return tuple(statements)


def _cpp_clock_dataflow(code: str) -> tuple[set[str], set[str]]:
    """Track C++ variables whose values derive from clock reads."""
    assignments: list[tuple[str, str]] = []
    for _start, statement in _cpp_statements(code):
        match = _CPP_ASSIGNMENT.search(statement)
        if match is not None:
            assignments.append((match.group("name"), match.group("expression")))
    clock_points = {
        name
        for name, expression in assignments
        if _CPP_CLOCK_EXPRESSION.search(expression) and "-" not in expression
    }
    durations: set[str] = set()
    changed = True
    while changed:
        changed = False
        for name, expression in assignments:
            words = _cpp_words(expression)
            temporal_operands = len(words & clock_points) + len(
                _CPP_CLOCK_EXPRESSION.findall(expression)
            )
            if (
                "-" in expression and temporal_operands >= 2 or words & durations
            ) and name not in durations:
                durations.add(name)
                changed = True
    return clock_points, durations


def _cpp_temporal(expression: str, clock_points: set[str], durations: set[str]) -> bool:
    """Return whether a C++ expression depends on timing data."""
    words = _cpp_words(expression)
    temporal_operands = len(words & clock_points) + len(_CPP_CLOCK_EXPRESSION.findall(expression))
    return bool(words & durations) or "-" in expression and temporal_operands >= 2


def _cpp_limit(expression: str) -> bool:
    """Return whether a C++ expression contains a literal duration limit."""
    return bool(
        _CPP_DURATION_CONSTRUCTOR.search(expression)
        or _CPP_NUMBER.search(expression)
        or _CPP_LITERAL_DURATION.search(expression)
    )


def _balanced_cpp_arguments(source: str, opening: int) -> tuple[str, str, int] | None:
    """Split macro arguments while respecting nested C++ delimiters."""
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


def _mask_cpp(source: str) -> str:
    """Mask C++ comments and literals without shifting source offsets."""
    masked = list(source)
    index = 0
    while index < len(source):
        if source.startswith("//", index):
            end = source.find("\n", index)
            end = len(source) if end < 0 else end
        elif source.startswith("/*", index):
            end = source.find("*/", index + 2)
            end = len(source) if end < 0 else end + 2
        elif source[index] in {'"', "'"}:
            quote = source[index]
            end = index + 1
            while end < len(source):
                character = source[end]
                end += 1
                if character == "\\" and end < len(source):
                    end += 1
                elif character == quote:
                    break
        else:
            index += 1
            continue
        for position in range(index, end):
            if masked[position] != "\n":
                masked[position] = " "
        index = end
    return "".join(masked)


def fragile_cpp_assertions(path: Path) -> tuple[tuple[int, str], ...]:
    """Return native assertions that treat a speed ceiling as correctness."""
    source = path.read_text(encoding="utf-8")
    code = _mask_cpp(source)
    clock_points, durations = _cpp_clock_dataflow(code)
    findings: list[tuple[int, str]] = []

    def record(left: str, operator: str, right: str, start: int, expression: str) -> None:
        """Record one result in the current analysis."""
        fragile = (
            operator in {"<", "<="}
            and _cpp_temporal(left, clock_points, durations)
            and _cpp_limit(right)
            or operator in {">", ">="}
            and _cpp_limit(left)
            and _cpp_temporal(right, clock_points, durations)
        )
        if fragile:
            findings.append((source.count("\n", 0, start) + 1, " ".join(expression.split())))

    offset = 0
    for line in code.splitlines(keepends=True):
        segment = line.strip()
        for match in re.finditer(r"<=|>=|<|>", segment):
            record(
                segment[: match.start()],
                match.group(),
                segment[match.end() :],
                offset + line.find(segment) + match.start(),
                source[offset + line.find(segment) : offset + len(line.rstrip("\n"))],
            )
        offset += len(line)

    for match in _CPP_TEST_MACRO.finditer(code):
        parsed = _balanced_cpp_arguments(code, match.end() - 1)
        if parsed is None:
            continue
        left, right, end = parsed
        relation = re.search(r"_(LT|LE|GT|GE)", match.group())
        assert relation is not None
        operator = {"LT": "<", "LE": "<=", "GT": ">", "GE": ">="}[relation.group(1)]
        record(left, operator, right, match.start(), source[match.start() : end])
    return tuple(dict.fromkeys(findings))


def repository_findings(root: Path = ROOT) -> tuple[str, ...]:
    """Return every determinism violation in the repository test trees."""
    findings: list[str] = []
    for path in sorted((root / "tests").rglob("*.py")):
        result = analyze_python(path)
        relative = path.relative_to(root)
        findings.extend(
            f"{relative}:{line}: {expression}"
            for line, expression in result.nondeterministic_randomness
        )
        findings.extend(
            f"{relative}:{line}: {expression}" for line, expression in result.vacuous_assertions
        )
        findings.extend(
            f"{relative}:{line}: {expression}" for line, expression in result.wall_clock
        )
        findings.extend(
            f"{relative}:{line}: {expression}" for line, expression in result.thread_sleeps
        )
        findings.extend(
            f"{relative}:{line}: {expression}" for line, expression in result.async_sleeps
        )
        findings.extend(
            f"{relative}:{line}: {expression}"
            for _function, line, expression in unapproved_lazy_workers(path)
        )
        findings.extend(
            f"{relative}:{line}: {expression}" for line, expression in result.incidental_overlap
        )
        findings.extend(
            f"{relative}:{line}: {expression}" for line, expression in result.incidental_promotions
        )
    cpp_root = root / "cpp" / "tests"
    paths = sorted(cpp_root.rglob("*.cc")) + sorted(cpp_root.rglob("*.cc.inc"))
    for path in paths:
        findings.extend(
            f"{path.relative_to(root)}:{line}: {expression}"
            for line, expression in fragile_cpp_assertions(path)
        )
    return tuple(findings)


def main(argv: list[str] | None = None) -> int:
    """Check the repository or a caller-selected root."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args(argv)
    findings = repository_findings(args.root.resolve())
    if findings:
        print("Test determinism check failed:")
        for finding in findings:
            print(f"  {finding}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
