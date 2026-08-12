#!/usr/bin/env bash
set -euo pipefail

# Native linkage gate. Usage: check_no_libarrow_linkage.sh <path-to-extension>

EXT_PATH="${1:-}"
if [[ -z "${EXT_PATH}" ]]; then
  echo "Usage: $0 <path-to-extension>" >&2
  exit 2
fi
if [[ ! -f "${EXT_PATH}" ]]; then
  echo "ERROR: extension file not found: ${EXT_PATH}" >&2
  exit 2
fi

tmp_report="$(mktemp)"
trap 'rm -f "${tmp_report}"' EXIT

if command -v otool >/dev/null 2>&1; then
  echo "-- otool -L ${EXT_PATH}"
  otool -L "${EXT_PATH}" | tee "${tmp_report}"
  if grep -E "libarrow|libparquet" "${tmp_report}"; then
    echo "ERROR: Arrow C++ runtime libs detected in linkage." >&2
    exit 1
  fi
elif command -v ldd >/dev/null 2>&1; then
  echo "-- ldd ${EXT_PATH}"
  ldd "${EXT_PATH}" | tee "${tmp_report}"
  if grep -E "libarrow|libparquet" "${tmp_report}"; then
    echo "ERROR: Arrow C++ runtime libs detected in linkage." >&2
    exit 1
  fi
else
  echo "WARN: neither otool nor ldd found; skipping linkage check" >&2
fi

echo "OK: no libarrow/libparquet in extension linkage"
