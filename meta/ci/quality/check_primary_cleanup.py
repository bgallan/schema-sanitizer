#!/usr/bin/env python3
"""Reject exception-masking cleanup in hardened ownership boundaries.

It inspects Python lifecycle handlers for broad catches, unsafe cleanup, reentrant
callbacks, and masked primary failures.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
CRITICAL_PATHS = (
    "src/schema_sanitizer/api_impl/execution_context.py",
    "src/schema_sanitizer/api_impl/input/preparation.py",
    "src/schema_sanitizer/pipeline/partition_lookahead.py",
    "src/schema_sanitizer/pipeline/registry_warmup.py",
    "src/schema_sanitizer/remote_impl/staging.py",
    "src/schema_sanitizer/remote_impl/io_coordinator.py",
    "src/schema_sanitizer/remote_impl/session_lifecycle.py",
    "src/schema_sanitizer/remote_impl/staged_ownership.py",
    "src/schema_sanitizer/remote_impl/staging_paths.py",
    "src/schema_sanitizer/remote_impl/provider_throttle.py",
    "src/schema_sanitizer/core_impl/temporary_janitor.py",
    "src/schema_sanitizer/api_impl/source_plan/remote.py",
    "src/schema_sanitizer/api_impl/operation_context.py",
    "src/schema_sanitizer/core_impl/path_identity.py",
    "src/schema_sanitizer/core_impl/cleanup_dispatcher.py",
    "src/schema_sanitizer/remote_impl/async_bridge.py",
    "src/schema_sanitizer/remote_impl/io_permits.py",
    "src/schema_sanitizer/core_impl/retry_scheduler.py",
    "src/schema_sanitizer/core_impl/process_resources.py",
    "src/schema_sanitizer/core_impl/memory_budget.py",
    "src/schema_sanitizer/core_impl/temporary_storage.py",
)
CLEANUP_METHODS = frozenset({"__aexit__", "__exit__", "close", "release", "shutdown", "unlink"})
KNOWN_NON_THROWING_TARGETS = frozenset({"loop.close"})


def _catches_broad_exception(handler: ast.ExceptHandler) -> bool:
    """Return whether the handler catches Exception or BaseException broadly."""
    caught = handler.type
    if caught is None:
        return True
    if isinstance(caught, ast.Name):
        return caught.id in {"BaseException", "Exception"}
    if isinstance(caught, ast.Tuple):
        return any(
            isinstance(item, ast.Name) and item.id in {"BaseException", "Exception"}
            for item in caught.elts
        )
    return False


def _cleanup_call(node: ast.AST) -> ast.Call | None:
    """Return the cleanup call represented by an AST statement."""
    if not isinstance(node, ast.Expr) or not isinstance(node.value, ast.Call):
        return None
    call = node.value
    if isinstance(call.func, ast.Attribute) and call.func.attr in CLEANUP_METHODS:
        if ast.unparse(call.func) in KNOWN_NON_THROWING_TARGETS:
            return None
        return call
    return None


def _contains_cleanup_helper(node: ast.AST) -> bool:
    """Return whether the node invokes a recognized cleanup helper."""
    return any(
        isinstance(child, ast.Call)
        and isinstance(child.func, ast.Name)
        and child.func.id == "_cleanup_with_note"
        for child in ast.walk(node)
    )


def _guarded_try(node: ast.Try) -> bool:
    """Locate the try statement that guards a cleanup operation."""
    return any(_catches_broad_exception(handler) for handler in node.handlers)


def _unsafe_handler_calls(statements: list[ast.stmt]) -> list[ast.Call]:
    """Collect cleanup calls that can mask a primary handler failure."""
    unsafe: list[ast.Call] = []
    for statement in statements:
        cleanup = _cleanup_call(statement)
        if cleanup is not None:
            unsafe.append(cleanup)
            continue
        if isinstance(statement, ast.Try) and _guarded_try(statement):
            continue
        if isinstance(statement, ast.If):
            unsafe.extend(_unsafe_handler_calls(statement.body))
            unsafe.extend(_unsafe_handler_calls(statement.orelse))
        elif isinstance(statement, (ast.For, ast.AsyncFor, ast.While)):
            unsafe.extend(_unsafe_handler_calls(statement.body))
            unsafe.extend(_unsafe_handler_calls(statement.orelse))
        elif isinstance(statement, (ast.With, ast.AsyncWith)):
            unsafe.extend(_unsafe_handler_calls(statement.body))
    return unsafe


def _primary_error_branches(
    node: ast.If,
) -> tuple[list[ast.stmt], list[ast.stmt]] | None:
    """Return ``(success, error)`` branches for an explicit error/None test."""
    test = node.test
    if not isinstance(test, ast.Compare) or len(test.ops) != 1:
        return None
    if len(test.comparators) != 1:
        return None
    left_name = ast.unparse(test.left).lower()
    checks_primary = any(token in left_name for token in ("error", "exception", "exc"))
    comparator = test.comparators[0]
    if not checks_primary or not (
        isinstance(comparator, ast.Constant) and comparator.value is None
    ):
        return None

    operator = test.ops[0]
    if isinstance(operator, (ast.Is, ast.Eq)):
        return node.body, node.orelse
    if isinstance(operator, (ast.IsNot, ast.NotEq)):
        return node.orelse, node.body
    return None


def _callback_reenters_lifecycle(call: ast.Call) -> bool:
    """Return whether an add_done_callback body re-enters owner cleanup."""
    if not isinstance(call.func, ast.Attribute) or call.func.attr != "add_done_callback":
        return False
    if not call.args:
        return False
    callback = call.args[0]
    return any(
        isinstance(child, ast.Call)
        and isinstance(child.func, ast.Attribute)
        and isinstance(child.func.value, ast.Name)
        and child.func.value.id == "self"
        and child.func.attr in {"close", "shutdown", "release", "abandon"}
        for child in ast.walk(callback)
    )


def _reentrant_callbacks_under_lock(tree: ast.AST, path: Path) -> list[str]:
    """Find lifecycle callbacks invoked while their owning lock is held."""
    errors: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.With, ast.AsyncWith)):
            continue
        contexts = " ".join(ast.unparse(item.context_expr).lower() for item in node.items)
        if not any(token in contexts for token in ("lock", "condition")):
            continue
        for child in ast.walk(ast.Module(body=node.body, type_ignores=[])):
            if isinstance(child, ast.Call) and _callback_reenters_lifecycle(child):
                relative = path.relative_to(ROOT)
                errors.append(
                    f"{relative}:{child.lineno}: lifecycle callback registered under lock"
                )
    return errors


def _unsafe_finally_calls(statements: list[ast.stmt]) -> list[ast.Call]:
    """Collect unguarded cleanup calls from finally blocks."""
    unsafe: list[ast.Call] = []
    for statement in statements:
        cleanup = _cleanup_call(statement)
        if cleanup is not None:
            unsafe.append(cleanup)
            continue
        if isinstance(statement, ast.Try) and _guarded_try(statement):
            continue
        if isinstance(statement, ast.If):
            branches = _primary_error_branches(statement)
            if branches is not None:
                _success_branch, error_branch = branches
                # Cleanup in the success branch cannot mask the primary
                # exception. The error branch is still inspected recursively,
                # so a helper in one nested path cannot hide a raw close in
                # another path.
                unsafe.extend(_unsafe_finally_calls(error_branch))
                continue
            unsafe.extend(_unsafe_finally_calls(statement.body))
            unsafe.extend(_unsafe_finally_calls(statement.orelse))
        elif isinstance(statement, (ast.For, ast.AsyncFor, ast.While)):
            unsafe.extend(_unsafe_finally_calls(statement.body))
            unsafe.extend(_unsafe_finally_calls(statement.orelse))
        elif isinstance(statement, (ast.With, ast.AsyncWith)):
            unsafe.extend(_unsafe_finally_calls(statement.body))
    return unsafe


def _format_call(path: Path, kind: str, call: ast.Call) -> str:
    """Render one cleanup call with its source line for diagnostics."""
    target = ast.unparse(call.func)
    relative = path.relative_to(ROOT)
    return f"{relative}:{call.lineno}: unsafe {target}() in {kind} cleanup"


def check_path(path: Path) -> list[str]:
    """Analyze one Python source file and return unsafe cleanup findings."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    errors: list[str] = []
    errors.extend(_reentrant_callbacks_under_lock(tree, path))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Try):
            continue
        for handler in node.handlers:
            if not _catches_broad_exception(handler):
                continue
            errors.extend(
                _format_call(path, "broad-exception", call)
                for call in _unsafe_handler_calls(handler.body)
            )
        errors.extend(
            _format_call(path, "finally", call) for call in _unsafe_finally_calls(node.finalbody)
        )
    return errors


def main() -> int:
    """Scan requested ownership modules and report unsafe cleanup branches."""
    errors = [error for relative in CRITICAL_PATHS for error in check_path(ROOT / relative)]
    if errors:
        print("Primary-exception cleanup safety check failed:", file=sys.stderr)
        for error in errors:
            print(f"  {error}", file=sys.stderr)
        print(
            "Use _cleanup_with_note(), or guard retryable cleanup in its own "
            "try/except while retaining the owner.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
