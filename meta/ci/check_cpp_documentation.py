"""Check C/C++ function documentation using clang-doc output."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CPP_SOURCE_PREFIX = "cpp/src/"


def _find_clang_doc() -> str:
    """Return an available clang-doc executable."""
    executable = shutil.which("clang-doc")
    if executable is not None:
        return executable
    candidates = sorted(Path("/usr/lib").glob("llvm-*/bin/clang-doc"), reverse=True)
    if candidates:
        return str(candidates[0])
    raise FileNotFoundError("clang-doc was not found")


def _find_compile_database() -> Path:
    """Return the most recently modified compile database."""
    candidates = sorted((_REPO_ROOT / "build").glob("*/compile_commands.json"))
    if not candidates:
        raise FileNotFoundError("no build/*/compile_commands.json file was found")
    return max(candidates, key=lambda path: path.stat().st_mtime_ns)


def _walk_functions(value: Any) -> Iterator[dict[str, Any]]:
    """Yield function records from nested clang-doc JSON."""
    if isinstance(value, dict):
        if value.get("InfoType") == "function":
            yield value
        for child in value.values():
            yield from _walk_functions(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_functions(child)


def _documentation_gaps(output_dir: Path) -> list[str]:
    """Return project function locations without descriptions."""
    functions: dict[tuple[Any, ...], dict[str, Any]] = {}
    for path in output_dir.rglob("*.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        for function in _walk_functions(payload):
            location = function.get("Location") or {}
            key = (
                function.get("USR"),
                location.get("Filename"),
                location.get("LineNumber"),
                function.get("Name"),
            )
            functions[key] = function

    gaps = []
    for function in functions.values():
        location = function.get("Location") or {}
        filename = location.get("Filename", "")
        if filename.startswith(_CPP_SOURCE_PREFIX) and not function.get("Description"):
            gaps.append(f"{filename}:{location.get('LineNumber')}: {function.get('Name')}")
    return sorted(gaps)


def main() -> int:
    """Run clang-doc and report undocumented project functions."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--compile-commands",
        type=Path,
        default=None,
        help="Path to compile_commands.json; defaults to the latest build directory.",
    )
    args = parser.parse_args()

    compile_commands = (args.compile_commands or _find_compile_database()).resolve()
    with tempfile.TemporaryDirectory(prefix="schema-sanitizer-clang-doc-") as tmp:
        output_dir = Path(tmp)
        subprocess.run(
            [
                _find_clang_doc(),
                "--executor=all-TUs",
                "--format=json",
                f"--output={output_dir}",
                f"--source-root={_REPO_ROOT}",
                str(compile_commands),
            ],
            cwd=_REPO_ROOT,
            check=True,
        )
        gaps = _documentation_gaps(output_dir)

    if gaps:
        print("Undocumented C/C++ functions:")
        print("\n".join(gaps))
        return 1
    print("All C/C++ functions discovered by clang-doc are documented.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
