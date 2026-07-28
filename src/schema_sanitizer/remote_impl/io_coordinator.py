"""One operation-owned event-loop thread for bounded remote I/O."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Awaitable, Callable
from concurrent.futures import CancelledError, Future
from contextlib import asynccontextmanager, suppress
from typing import Any, AsyncContextManager, TypeVar

from .provider_session_pool import activate_provider_session_pool

T = TypeVar("T")


@asynccontextmanager
async def _empty_async_context() -> Any:
    """Provide a contextless event loop for operation-wide remote work."""
    yield None


class RemoteIoCoordinator:
    """Run remote coroutines on one owned event loop and context."""

    def __init__(
        self,
        context_factory: Callable[[], AsyncContextManager[Any]] | None = None,
        *,
        thread_name: str = "schema-sanitizer-remote-io",
    ) -> None:
        """Start the coordinator and enter its shared async context."""
        self._context_factory = context_factory or _empty_async_context
        self._ready = threading.Event()
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._context_manager: AsyncContextManager[Any] | None = None
        self._context: Any = None
        self._startup_error: BaseException | None = None
        self._futures: set[Future[Any]] = set()
        self._closed = False
        self._thread = threading.Thread(
            target=self._run,
            name=thread_name,
            daemon=True,
        )
        self._thread.start()
        self._ready.wait()
        self._raise_startup_error()

    def _raise_startup_error(self) -> None:
        """Raise an error captured while the coordinator thread was starting."""
        startup_error = self._startup_error
        if startup_error is None:
            return
        self._thread.join()
        raise startup_error

    def submit(self, operation: Callable[[Any], Awaitable[T]]) -> Future[T]:
        """Schedule one operation against the shared remote context."""
        with self._lock:
            if self._closed:
                raise RuntimeError("remote I/O coordinator is closed")
            loop = self._loop
            if loop is None:
                raise RuntimeError("remote I/O coordinator did not start")

            async def invoke() -> T:
                """Invoke one submitted operation inside the owned loop."""
                with activate_provider_session_pool(self._context):
                    return await operation(self._context)

            future = asyncio.run_coroutine_threadsafe(invoke(), loop)
            self._futures.add(future)
            future.add_done_callback(self._forget_future)
            return future

    def close(self) -> None:
        """Cancel, drain, close the shared context, and join the host thread."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            loop = self._loop
            futures = tuple(self._futures)
        for future in futures:
            future.cancel()
        for future in futures:
            with suppress(CancelledError, Exception):
                future.result()

        close_error: BaseException | None = None
        if loop is not None and loop.is_running():
            close_future = asyncio.run_coroutine_threadsafe(self._shutdown(), loop)
            try:
                close_future.result()
            except BaseException as exc:
                close_error = exc
            finally:
                loop.call_soon_threadsafe(loop.stop)
        self._thread.join()
        if close_error is not None:
            raise close_error

    def __enter__(self) -> RemoteIoCoordinator:
        """Return the started coordinator."""
        return self

    def __exit__(self, *_exc: object) -> None:
        """Close the coordinator."""
        self.close()

    @property
    def thread_ident(self) -> int | None:
        """Return the owned host thread identifier for diagnostics/tests."""
        return self._thread.ident

    def _forget_future(self, future: Future[Any]) -> None:
        """Drop one completed future from coordinator-owned bookkeeping."""
        with self._lock:
            self._futures.discard(future)

    def _run(self) -> None:
        """Own the event loop for the complete coordinator lifetime."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        try:
            manager = self._context_factory()
            self._context_manager = manager
            self._context = loop.run_until_complete(manager.__aenter__())
        except BaseException as exc:
            self._startup_error = exc
            self._ready.set()
            loop.close()
            return

        self._ready.set()
        try:
            loop.run_forever()
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()

    async def _shutdown(self) -> None:
        """Drain submitted tasks before closing the shared provider context."""
        current = asyncio.current_task()
        pending = [task for task in asyncio.all_tasks() if task is not current]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        manager = self._context_manager
        self._context_manager = None
        self._context = None
        if manager is not None:
            await manager.__aexit__(None, None, None)
