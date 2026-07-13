#!/usr/bin/env bash
set -euo pipefail

# Ensure the source tree is internally complete before configure/build. This
# catches incomplete release/working ZIPs where CMake sources or local quoted
# headers were accidentally omitted.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT}"

missing=0
report_missing() {
  echo "ERROR: missing source tree file: $1" >&2
  missing=1
}

cmake_files="$(
  {
    printf '%s\n' CMakeLists.txt
    find cmake -type f \( -name '*.cmake' -o -name 'CMakeLists.txt' \) 2>/dev/null
  } | sort -u
)"

cmake_sources="$(
  xargs grep -Eho 'cpp/[^ )]+\.(cc|cpp|cxx)' <<<"${cmake_files}" | sort -u
)"

while IFS= read -r path; do
  [[ -z "${path}" ]] && continue
  [[ -f "${path}" ]] || report_missing "${path}"
done <<<"${cmake_sources}"

# Every production translation unit must be owned by a CMake target. This
# catches new source files that work in a developer tree but are never built.
while IFS= read -r path; do
  [[ -z "${path}" ]] && continue
  if ! grep -Fxq "${path}" <<<"${cmake_sources}"; then
    echo "ERROR: production source is not listed in CMake sources: ${path}" >&2
    missing=1
  fi
done < <(
  find cpp/src -type f \( -name '*.cc' -o -name '*.cpp' -o -name '*.cxx' \) |
    sort
)

# Check quoted project includes against both configured include roots and the
# directory of the including file. Local .cc.inc fragments are intentionally
# colocated with their translation unit and must not be resolved from cpp/src.
if ! python - "${ROOT}" <<'PYINCLUDES'; then
from __future__ import annotations

from pathlib import Path
import re
import sys

root = Path(sys.argv[1])
source_root = root / "cpp" / "src"
thirdparty_root = root / "cpp" / "thirdparty"
pattern = re.compile(r'^\s*#\s*include\s+"([^"]+)"')
extensions = {".cc", ".cpp", ".cxx", ".hh", ".hpp", ".h"}
missing: list[tuple[Path, str]] = []

for source in sorted(path for path in source_root.rglob("*") if path.suffix in extensions):
    for line in source.read_text(encoding="utf-8", errors="replace").splitlines():
        match = pattern.match(line)
        if match is None:
            continue
        include_path = match.group(1)
        candidates = (
            source.parent / include_path,
            source_root / include_path,
            thirdparty_root / include_path,
        )
        if not any(candidate.is_file() for candidate in candidates):
            missing.append((source.relative_to(root), include_path))

for source, include_path in missing:
    print(
        f"ERROR: missing quoted include {include_path!r} referenced by {source}",
        file=sys.stderr,
    )
raise SystemExit(bool(missing))
PYINCLUDES
  missing=1
fi

if [[ "${missing}" -ne 0 ]]; then
  exit 1
fi

echo "OK: CMake sources and quoted project includes exist"
