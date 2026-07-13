#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "Usage: $0 <source-zip>" >&2
  exit 2
fi

archive="$1"
if [[ ! -f "${archive}" ]]; then
  echo "ERROR: archive not found: ${archive}" >&2
  exit 2
fi

tmp="$(mktemp -d)"
cleanup() {
  rm -rf "${tmp}"
}
trap cleanup EXIT

python - "${archive}" "${tmp}" <<'PY'
from pathlib import Path
import sys
import zipfile

archive = Path(sys.argv[1])
dest = Path(sys.argv[2])
with zipfile.ZipFile(archive) as zf:
    zf.extractall(dest)
PY

root="${tmp}"
if [[ ! -f "${root}/CMakeLists.txt" ]]; then
  # Support archives that contain a single top-level directory.
  shopt -s nullglob
  entries=("${tmp}"/*)
  shopt -u nullglob
  if [[ "${#entries[@]}" -eq 1 && -d "${entries[0]}" && -f "${entries[0]}/CMakeLists.txt" ]]; then
    root="${entries[0]}"
  fi
fi

if [[ ! -f "${root}/meta/ci/check_cmake_sources_exist.sh" ]]; then
  echo "ERROR: archive does not contain meta/ci/check_cmake_sources_exist.sh" >&2
  exit 1
fi

(cd "${root}" && bash meta/ci/check_cmake_sources_exist.sh)

artifact="$(
  find "${root}" \
    \( -path "${root}/build/*" \
    -o -path "${root}/.build*" \
    -o -path "${root}/build-*" \
    -o -path "${root}/cmake-build-*" \
    -o -path "${root}/dist/*" \
    -o -path "${root}/wheelhouse/*" \
    -o -path "${root}/.git/*" \
    -o -path "*/__pycache__/*" \
    -o -path "*.egg-info/*" \
    -o -path "*/.pytest_cache/*" \
    -o -path "*/.mypy_cache/*" \
    -o -path "*/.ruff_cache/*" \
    -o -name '*.pyc' \
    -o -name '*.pyo' \
    -o -name '*.so' \
    -o -name '*.pyd' \
    -o -name '*.dll' \
    -o -name '*.dylib' \
    -o -name '*.o' \
    -o -name '*.obj' \
    -o -name '*.a' \
    -o -name '*.lib' \
    -o -name '*.exp' \
    -o -name '*.dSYM' \
    -o -name '.DS_Store' \
    -o -name '.ninja_deps' \
    -o -name '.ninja_log' \
    -o -name 'CMakeCache.txt' \
    -o -name 'cmake_install.cmake' \
    \) -print -quit
)"
if [[ -n "${artifact}" ]]; then
  echo "ERROR: archive contains generated artifact: ${artifact#"${root}"/}" >&2
  exit 1
fi

echo "OK: archive contains no generated build/cache artifacts"
