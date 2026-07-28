"""Operation-lifetime provider session pooling contracts."""

from __future__ import annotations

from typing import Any

import pytest


class _FakeClient:
    """Record provider client closure without real network resources."""

    def __init__(self) -> None:
        """Initialize close accounting."""
        self.close_calls = 0
        self.uses = 0

    async def close(self) -> None:
        """Record the operation-final close."""
        self.close_calls += 1

    async def __aenter__(self) -> _FakeClient:
        """Record one borrowed use."""
        self.uses += 1
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """The raw client would normally close per use."""
        await self.close()


def test_operation_context_reuses_aiohttp_session_and_closes_once(monkeypatch) -> None:
    """Repeated remote stages borrow one connector pool for the whole operation."""
    from schema_sanitizer.api_impl.operation_context import OperationExecutionContext
    from schema_sanitizer.remote_impl import transport

    created: list[_FakeClient] = []

    async def create_raw(*_args: Any, **_kwargs: Any) -> _FakeClient:
        """Create one fake raw session."""
        client = _FakeClient()
        created.append(client)
        return client

    monkeypatch.setattr(transport, "_open_aiohttp_session_unpooled", create_raw)
    operation = OperationExecutionContext(
        threading_mode="multi",
        memory_limit_bytes=64 << 20,
    )

    async def use_session() -> int:
        """Borrow and leave one pooled aiohttp session."""
        async with await transport.open_aiohttp_session(
            {"Authorization": "Bearer stable"},
            memory_limit_bytes=64 << 20,
            threading_mode="multi",
        ) as session:
            session.uses += 1
            return session.uses

    try:
        assert operation.run_remote(use_session) == 1
        assert operation.run_remote(use_session) == 2
        assert len(created) == 1
        assert created[0].close_calls == 0
    finally:
        operation.close()

    assert created[0].close_calls == 1


def test_provider_pool_separates_incompatible_http_header_sets(monkeypatch) -> None:
    """Sessions with different authorization/default headers are never aliased."""
    from schema_sanitizer.api_impl.operation_context import OperationExecutionContext
    from schema_sanitizer.remote_impl import transport

    created: list[_FakeClient] = []

    async def create_raw(*_args: Any, **_kwargs: Any) -> _FakeClient:
        """Create one fake raw session per distinct key."""
        client = _FakeClient()
        created.append(client)
        return client

    monkeypatch.setattr(transport, "_open_aiohttp_session_unpooled", create_raw)
    operation = OperationExecutionContext(
        threading_mode="multi",
        memory_limit_bytes=64 << 20,
    )

    async def borrow(token: str) -> None:
        """Borrow one keyed session."""
        async with await transport.open_aiohttp_session(
            {"Authorization": f"Bearer {token}"},
            memory_limit_bytes=64 << 20,
            threading_mode="multi",
        ):
            return

    try:
        operation.run_remote(lambda: borrow("a"))
        operation.run_remote(lambda: borrow("b"))
        operation.run_remote(lambda: borrow("a"))
        assert len(created) == 2
    finally:
        operation.close()

    assert [client.close_calls for client in created] == [1, 1]


def test_single_operation_does_not_construct_provider_pool(monkeypatch) -> None:
    """Single mode rejects async sessions and creates no host thread or pool."""
    from schema_sanitizer.api_impl.operation_context import OperationExecutionContext
    from schema_sanitizer.remote_impl import transport

    async def forbidden_open(*_args: Any, **_kwargs: Any) -> _FakeClient:
        """Fail if strict single mode reaches the asynchronous transport."""
        raise AssertionError("single mode opened an asynchronous provider session")

    monkeypatch.setattr(transport, "_open_aiohttp_session_unpooled", forbidden_open)
    operation = OperationExecutionContext(
        threading_mode="single",
        memory_limit_bytes=64 << 20,
    )

    async def forbidden_async_operation() -> None:
        """Represent a provider coroutine that single mode must reject."""

    try:
        assert operation.run_remote_sync(lambda: "inline") == "inline"
        with pytest.raises(RuntimeError, match="strict single-mode"):
            operation.run_remote(forbidden_async_operation)
        assert operation.remote_coordinator is None
    finally:
        operation.close()


def test_operation_close_cancels_active_use_before_closing_provider(monkeypatch) -> None:
    """Pool shutdown drains active work before closing the shared client."""
    import asyncio
    import threading

    from schema_sanitizer.api_impl.operation_context import OperationExecutionContext
    from schema_sanitizer.remote_impl import transport

    client = _FakeClient()
    cancelled: list[bool] = []
    started = threading.Event()

    async def create_raw(*_args: Any, **_kwargs: Any) -> _FakeClient:
        """Return one shared fake session."""
        return client

    monkeypatch.setattr(transport, "_open_aiohttp_session_unpooled", create_raw)
    operation = OperationExecutionContext(
        threading_mode="multi",
        memory_limit_bytes=64 << 20,
    )

    async def block() -> None:
        """Hold the pooled client until coordinator shutdown cancels the task."""
        async with await transport.open_aiohttp_session(
            memory_limit_bytes=64 << 20,
            threading_mode="multi",
        ):
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.append(True)
                raise

    future = operation.submit_remote(block)
    assert started.wait(timeout=2)
    operation.close()

    assert future.cancelled() or future.done()
    assert cancelled == [True]
    assert client.close_calls == 1


class _FakeManager:
    """Record one provider manager entry and final exit."""

    def __init__(self, value: Any) -> None:
        """Store the entered provider value."""
        self.value = value
        self.enter_calls = 0
        self.exit_calls = 0

    async def __aenter__(self) -> Any:
        """Enter the provider manager once."""
        self.enter_calls += 1
        return self.value

    async def __aexit__(self, *_exc: object) -> None:
        """Close the provider manager once."""
        self.exit_calls += 1


def test_operation_context_reuses_s3_manager(monkeypatch) -> None:
    """S3 list, download, and upload calls can share one entered SDK client."""
    from schema_sanitizer.api_impl.operation_context import OperationExecutionContext
    from schema_sanitizer.remote_impl.providers import s3

    client = object()
    manager = _FakeManager(client)
    raw_calls = 0

    async def create_raw() -> _FakeManager:
        """Create the operation-owned S3 manager."""
        nonlocal raw_calls
        raw_calls += 1
        return manager

    monkeypatch.setattr(s3, "_open_client_unpooled", create_raw)
    operation = OperationExecutionContext(
        threading_mode="multi",
        memory_limit_bytes=64 << 20,
    )

    async def borrow() -> bool:
        """Enter and leave one borrowed S3 manager."""
        async with await s3.open_client() as borrowed:
            return borrowed is client

    try:
        assert operation.run_remote(borrow) is True
        assert operation.run_remote(borrow) is True
        assert raw_calls == 1
        assert manager.enter_calls == 1
        assert manager.exit_calls == 0
    finally:
        operation.close()

    assert manager.exit_calls == 1


def test_operation_context_reuses_azure_owner_per_account(monkeypatch) -> None:
    """Azure service/credential owners are pooled per account and closed once."""
    from schema_sanitizer.api_impl.operation_context import OperationExecutionContext
    from schema_sanitizer.remote_impl.providers import azure

    created: list[_FakeClient] = []

    async def create_raw(_ref: object) -> _FakeClient:
        """Create one fake combined Azure owner per account key."""
        owner = _FakeClient()
        created.append(owner)
        return owner

    monkeypatch.setattr(azure, "_open_service_unpooled", create_raw)
    first = azure.parse_uri("az://one/container/input/a.json")
    second = azure.parse_uri("az://two/container/input/b.json")
    operation = OperationExecutionContext(
        threading_mode="multi",
        memory_limit_bytes=64 << 20,
    )

    async def borrow(ref: object) -> None:
        """Borrow and release one account-scoped Azure owner."""
        service = await azure.open_service(ref)
        service.uses += 1
        await service.close()

    try:
        operation.run_remote(lambda: borrow(first))
        operation.run_remote(lambda: borrow(first))
        operation.run_remote(lambda: borrow(second))
        assert len(created) == 2
        assert [owner.uses for owner in created] == [2, 1]
        assert [owner.close_calls for owner in created] == [0, 0]
    finally:
        operation.close()

    assert [owner.close_calls for owner in created] == [1, 1]
