"""Small release-consumer program type-checked against the installed package."""

from pathlib import Path

import schema_sanitizer as ss


def convert(source: Path, destination: Path) -> None:
    """Convert JSONL while exercising installed public type information."""
    result: ss.Result = ss.to_jsonl(source, destination, input_format="jsonl")
    print(result.stats)
