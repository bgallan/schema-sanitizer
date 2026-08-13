"""Shared fixtures for memory-hardening tests."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from typing import Any, cast

import pytest


def _purge_module(name: str) -> None:
    """Remove one module and its cached parent-package attribute."""
    sys.modules.pop(name, None)
    parent_name, _, attribute = name.rpartition(".")
    parent = sys.modules.get(parent_name)
    if parent is not None and hasattr(parent, attribute):
        delattr(parent, attribute)


@pytest.fixture
def native_stub(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> Iterator[None]:
    """Provide isolated import-time native metadata for Python-only tests."""
    from schema_sanitizer.core_impl import native_runtime

    class Stub:
        """Minimal native metadata provider."""

        def options_catalog(self) -> tuple[object, ...]:
            """Return an empty option catalog."""
            return ()

        def __getattr__(self, _name: str) -> Any:
            """Return no-op native entry points."""
            return lambda *_args, **_kwargs: None

    module_names = cast(
        tuple[str, ...],
        getattr(request.module, "_NATIVE_STUB_MODULES"),
    )
    real_native = native_runtime.native_core
    preexisting_modules = set(sys.modules)
    sentinel = object()
    saved = {name: sys.modules.get(name, sentinel) for name in module_names}
    monkeypatch.setattr(native_runtime, "native_core", Stub())
    for name in reversed(module_names):
        _purge_module(name)
    try:
        yield
    finally:
        native_runtime.native_core = real_native
        created_modules = sorted(
            (
                name
                for name in tuple(sys.modules)
                if name.startswith("schema_sanitizer.") and name not in preexisting_modules
            ),
            key=lambda name: name.count("."),
            reverse=True,
        )
        for name in created_modules:
            _purge_module(name)
        for name in reversed(module_names):
            _purge_module(name)
        for name, module in saved.items():
            if module is sentinel:
                continue
            sys.modules[name] = module
            parent_name, _, attribute = name.rpartition(".")
            parent = sys.modules.get(parent_name)
            if parent is not None:
                setattr(parent, attribute, module)
