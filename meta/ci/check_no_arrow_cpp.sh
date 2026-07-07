#!/usr/bin/env bash
set -euo pipefail

# Fail if Arrow C++ headers are referenced in core C++ sources/headers.
# Arrow C Data interop is provided via the vendored NanoArrow C header.

if grep -R --line-number --fixed-strings "<arrow/" cpp/src; then
  echo "ERROR: Arrow C++ headers detected (include <arrow/...>)." >&2
  exit 1
fi

echo "OK: no <arrow/...> includes in cpp/src"
