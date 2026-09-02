"""Operation-lifetime provider session pooling contracts.

It checks key compatibility, concurrent single-flight construction, borrower
cancellation, ordered closure, and provider-owner reuse per operation.
"""

from __future__ import annotations

from typing import Any

import pytest
from _support.synchronization import SCHEDULER_TIMEOUT_SECONDS


class _FakeClient:
    """Record provider client closure without real network resources."""

    def __init__(self) -> None:
        """Initialize fake client state for close calls and uses."""
        self.close_calls = 0
        self.uses = 0

    async def close(self) -> None:
        """Close the fake client and update close calls."""
        self.close_calls += 1

    async def __aenter__(self) -> _FakeClient:
        """Return the managed fake client value from context entry."""
        self.uses += 1
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Finalize the fake client context without suppressing exceptions."""
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


def test_provider_pool_initializes_distinct_keys_concurrently() -> None:
    """Slow factories for unrelated provider keys do not share one await lock."""
    import asyncio

    from schema_sanitizer.remote_impl.provider_session_pool import (
        RemoteProviderSessionPool,
    )

    async def exercise() -> tuple[int, list[_FakeClient]]:
        """Borrow two distinct keys and measure concurrent factory execution."""
        active = 0
        peak = 0
        both_started = asyncio.Event()
        release = asyncio.Event()
        clients: list[_FakeClient] = []

        async def create() -> _FakeClient:
            """Create one tracked provider session."""
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            if active == 2:
                both_started.set()
            await release.wait()
            active -= 1
            client = _FakeClient()
            clients.append(client)
            return client

        async with RemoteProviderSessionPool() as pool:
            first = asyncio.create_task(pool.borrow_client(("http", "a"), create))
            second = asyncio.create_task(pool.borrow_client(("http", "b"), create))
            await asyncio.wait_for(
                both_started.wait(),
                timeout=SCHEDULER_TIMEOUT_SECONDS,
            )
            release.set()
            await asyncio.gather(first, second)
        return peak, clients

    peak, clients = asyncio.run(exercise())
    assert peak == 2
    assert len(clients) == 2
    assert [client.close_calls for client in clients] == [1, 1]


def test_provider_pool_keeps_single_flight_for_one_key() -> None:
    """Concurrent borrows of one key still create and close one client."""
    import asyncio

    from schema_sanitizer.remote_impl.provider_session_pool import (
        RemoteProviderSessionPool,
    )

    async def exercise() -> tuple[int, _FakeClient]:
        """Borrow one key concurrently and count single-flight client creation."""
        calls = 0
        client = _FakeClient()

        async def create() -> _FakeClient:
            """Create one tracked provider session."""
            nonlocal calls
            calls += 1
            await asyncio.sleep(0)
            return client

        async with RemoteProviderSessionPool() as pool:
            first, second = await asyncio.gather(
                pool.borrow_client(("http", "same"), create),
                pool.borrow_client(("http", "same"), create),
            )
            assert first._value is second._value  # noqa: SLF001
        return calls, client

    calls, client = asyncio.run(exercise())
    assert calls == 1
    assert client.close_calls == 1


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
    assert started.wait(timeout=SCHEDULER_TIMEOUT_SECONDS)
    operation.close()

    assert future.done()
    assert cancelled == [True]
    assert client.close_calls == 1


class _FakeManager:
    """Record one provider manager entry and final exit."""

    def __init__(self, value: Any) -> None:
        """Initialize fake manager state for value, enter calls, and exit calls."""
        self.value = value
        self.enter_calls = 0
        self.exit_calls = 0

    async def __aenter__(self) -> Any:
        """Return the managed fake manager value from context entry."""
        self.enter_calls += 1
        return self.value

    async def __aexit__(self, *_exc: object) -> None:
        """Finalize the fake manager context without suppressing exceptions."""
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
