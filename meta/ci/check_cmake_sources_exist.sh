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

# Check quoted project includes against the configured include roots. System
# includes (<...>) are intentionally ignored.
while IFS= read -r include_path; do
  [[ -z "${include_path}" ]] && continue
  if [[ -f "cpp/src/${include_path}" || -f "cpp/thirdparty/${include_path}" ]]; then
    continue
  fi
  report_missing "include ${include_path} (searched cpp/src and cpp/thirdparty)"
done < <(
  find cpp/src -type f \( -name '*.cc' -o -name '*.cpp' -o -name '*.cxx' -o -name '*.hh' -o -name '*.hpp' -o -name '*.h' \) \
    -print0 |
    xargs -0 grep -hoE '^[[:space:]]*#[[:space:]]*include[[:space:]]+"[^"]+"' |
    sed -E 's/.*"([^"]+)".*/\1/' |
    sort -u
)

if [[ "${missing}" -ne 0 ]]; then
  exit 1
fi

echo "OK: CMake sources and quoted project includes exist"
