"""Provide strict asynchronous protocol doubles for remote transport tests.

The response, context, and manifest helpers model bounded reads, cleanup, and multipart state
without real network traffic.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path


class AsyncValueContext:
    def __init__(self, value: object) -> None:
        """Initialize the async value context test double."""
        self.value = value

    async def __aenter__(self) -> object:
        """Enter the asynchronous context managed by the async value context test double."""
        return self.value

    async def __aexit__(self, *_exc: object) -> None:
        """Exit the asynchronous context managed by the async value context test double and run cleanup."""
        return None


class BoundedResponse:
    """Aiohttp-like response that permits only explicitly sized reads."""

    def __init__(
        self,
        status_or_payload: int | Mapping[str, object] = 200,
        *,
        headers: dict[str, str] | None = None,
        body: bytes | str = b"",
        enter_error: BaseException | None = None,
    ) -> None:
        """Initialize the bounded response test double."""
        if isinstance(status_or_payload, Mapping):
            self.status = 200
            self._body = json.dumps(status_or_payload).encode()
        else:
            self.status = status_or_payload
            self._body = body.encode() if isinstance(body, str) else body
        self.headers = headers or {}
        self._offset = 0
        self.content = self
        self._enter_error = enter_error

    async def __aenter__(self) -> BoundedResponse:
        """Enter the asynchronous context managed by the bounded response test double."""
        if self._enter_error is not None:
            raise self._enter_error
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Exit the asynchronous context managed by the bounded response test double and run cleanup."""
        return None

    async def read(self, size: int) -> bytes:
        """Read bounded data from the bounded response test double."""
        end = min(len(self._body), self._offset + size)
        chunk = self._body[self._offset : end]
        self._offset = end
        return chunk

    def at_eof(self) -> bool:
        """Report whether the controlled response reached end of input."""
        return self._offset == len(self._body)


def sparse_file(path: Path, size: int) -> None:
    """Create a deterministic sparse file without allocating its payload."""
    with path.open("wb") as handle:
        handle.truncate(size)
