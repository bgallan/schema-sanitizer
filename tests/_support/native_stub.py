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
    from schema_sanitizer.core_impl import finalizer_registry, native_runtime

    with finalizer_registry._REGISTRY_LOCK:
        finalizer_registry_state = (
            tuple(finalizer_registry._REGISTRY),
            dict(finalizer_registry._REGISTRY_NAMES),
            bytes(finalizer_registry._REGISTRY_EPOCH),
            finalizer_registry._REGISTRY_CORRUPTED,
            finalizer_registry._REGISTRY_FROZEN,
            finalizer_registry._FROZEN_DOMAINS,
            finalizer_registry._FROZEN_ESCROWS,
            finalizer_registry._FROZEN_ACTIVITY_NATIVE_CAPSULES,
        )

    class Stub:
        """Minimal native metadata provider."""

        def __init__(self) -> None:
            self._memory_ledgers: dict[int, list[int]] = {}

        def process_resident_memory_stats(self) -> tuple[int, int, int]:
            return (1 << 40, 0, 0)

        def process_physical_thread_permits_acquire(self, amount: int, minimum: int) -> int:
            return amount if amount >= minimum else 0

        def process_physical_thread_permits_release(self, _amount: int) -> None:
            return None

        def process_file_descriptor_permits_snapshot(
            self,
        ) -> tuple[int, int, int, int, int, int]:
            return (0, 0, 4096, 0, 0, 0)

        def operation_memory_ledger_create(self, limit_bytes: int) -> object:
            capsule = object()
            self._memory_ledgers[id(capsule)] = [limit_bytes, 0, 0]
            return capsule

        def operation_memory_ledger_reserve_snapshot(
            self, capsule: object, amount: int, _stage: str
        ) -> tuple[int, int, int]:
            values = self._memory_ledgers[id(capsule)]
            values[1] += amount
            values[2] = max(values[2], values[1])
            return tuple(values)  # type: ignore[return-value]

        def operation_memory_ledger_release(self, capsule: object, amount: int) -> None:
            self._memory_ledgers[id(capsule)][1] -= amount

        def operation_memory_ledger_snapshot(self, capsule: object) -> tuple[int, int, int]:
            return tuple(self._memory_ledgers[id(capsule)])  # type: ignore[return-value]

        def options_catalog(self) -> tuple[object, ...]:
            return ()

        def __getattr__(self, _name: str) -> Any:
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
        (
            registered,
            registered_names,
            registry_epoch,
            registry_corrupted,
            registry_frozen,
            frozen_domains,
            frozen_escrows,
            frozen_activity_capsules,
        ) = finalizer_registry_state
        with finalizer_registry._REGISTRY_LOCK:
            finalizer_registry._REGISTRY[:] = registered
            finalizer_registry._REGISTRY_NAMES.clear()
            finalizer_registry._REGISTRY_NAMES.update(registered_names)
            finalizer_registry._REGISTRY_EPOCH[:] = registry_epoch
            finalizer_registry._REGISTRY_CORRUPTED = registry_corrupted
            finalizer_registry._REGISTRY_FROZEN = registry_frozen
            finalizer_registry._FROZEN_DOMAINS = frozen_domains
            finalizer_registry._FROZEN_ESCROWS = frozen_escrows
            finalizer_registry._FROZEN_ACTIVITY_NATIVE_CAPSULES = frozen_activity_capsules
