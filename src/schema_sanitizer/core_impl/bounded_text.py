"""Allocation-bounded text measurement helpers.

It measures UTF-8 in fixed character chunks and returns a one-byte-over-limit sentinel without
constructing the full encoded payload.
"""

from __future__ import annotations

_UTF8_SIZE_CHUNK_CHARS = 4096


def utf8_size_bounded(value: str, maximum_bytes: int) -> int:
    """Measure UTF-8 incrementally and stop after one byte ceiling is exceeded."""
    maximum = max(0, int(maximum_bytes))
    total = 0
    for offset in range(0, len(value), _UTF8_SIZE_CHUNK_CHARS):
        total += len(
            value[offset : offset + _UTF8_SIZE_CHUNK_CHARS].encode("utf-8", errors="surrogatepass")
        )
        if total > maximum:
            return maximum + 1
    return total


__all__ = ["utf8_size_bounded"]
