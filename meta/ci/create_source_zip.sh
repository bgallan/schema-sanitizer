#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "Usage: $0 <output-zip>" >&2
  exit 2
fi

out="$1"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
out_abs="$(
  python - "${out}" <<'PY'
from pathlib import Path
import sys
print(Path(sys.argv[1]).resolve())
PY
)"
mkdir -p "$(dirname "${out_abs}")"

python - "${root}" "${out_abs}" <<'PY'
from __future__ import annotations

from pathlib import Path
import sys
import zipfile

root = Path(sys.argv[1])
out = Path(sys.argv[2])

# Root build/cache outputs are excluded. Do not exclude every path segment named
# "build": cpp/src/internal/materialization contains real source files used by CMake.
ROOT_DIR_EXCLUDES = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "build",
    "dist",
    "wheelhouse",
}
ANY_DIR_EXCLUDES = {"__pycache__"}
SUFFIX_EXCLUDES = {
    ".a",
    ".dylib",
    ".dll",
    ".exp",
    ".lib",
    ".ninja",
    ".o",
    ".obj",
    ".pyd",
    ".pyc",
    ".pyo",
    ".so",
}
FILE_EXCLUDES = {
    ".DS_Store",
    "CMakeCache.txt",
    "CTestTestfile.cmake",
    "cmake_install.cmake",
    "install_manifest.txt",
    ".ninja_deps",
    ".ninja_log",
}

entries: list[Path] = []
for path in root.rglob("*"):
    rel = path.relative_to(root)
    if not rel.parts:
        continue
    root_dir = rel.parts[0]
    if (
        root_dir in ROOT_DIR_EXCLUDES
        or root_dir.startswith(".build")
        or root_dir.startswith("build-")
        or root_dir.startswith("cmake-build-")
    ):
        continue
    if any(part in ANY_DIR_EXCLUDES or part.endswith(".egg-info") for part in rel.parts):
        continue
    if path.is_dir():
        continue
    if path.name in FILE_EXCLUDES:
        continue
    if path.suffix in SUFFIX_EXCLUDES:
        continue
    if path.resolve() == out.resolve():
        continue
    entries.append(rel)

with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    for rel in sorted(entries):
        zf.write(root / rel, rel.as_posix())
PY

bash "${root}/meta/ci/check_zip_contains_cmake_sources.sh" "${out_abs}"
echo "OK: wrote ${out_abs}"
