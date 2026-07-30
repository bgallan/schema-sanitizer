#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
build_dir="${1:-${repo_root}/build/tsan}"
python_launcher="${2:-${repo_root}/python-tsan}"
rounds="${3:-1}"
site_packages="${4:-}"
test_target="${5:-}"
domain_timeout_seconds=300
domain_shutdown_grace_seconds=5

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "The full-extension ThreadSanitizer gate is supported only on Linux." >&2
  exit 2
fi
if [[ ! -d "${build_dir}" ]]; then
  echo "TSan build directory does not exist: ${build_dir}" >&2
  exit 2
fi
if [[ ! -x "${python_launcher}" ]]; then
  echo "TSan CPython launcher is not executable: ${python_launcher}" >&2
  exit 2
fi
if ! [[ "${rounds}" =~ ^[1-9][0-9]*$ ]]; then
  echo "TSan rounds must be a positive integer." >&2
  exit 2
fi
if [[ -z "${site_packages}" || ! -d "${site_packages}" ]]; then
  echo "Python site-packages directory is required: ${site_packages:-<empty>}" >&2
  exit 2
fi
if ! command -v setsid >/dev/null 2>&1; then
  echo "The Linux util-linux setsid command is required." >&2
  exit 2
fi

cd "${repo_root}"

"${python_launcher}" -u -c '
from pathlib import Path
import sys

sys.path.insert(0, sys.argv[2])
sys.path.insert(0, "src")
from schema_sanitizer.core_impl.native_runtime import native_core

expected = Path(sys.argv[1]).resolve()
loaded = Path(native_core.__file__).resolve()
if expected not in loaded.parents:
    raise SystemExit(f"TSan gate loaded {loaded}, expected an extension under {expected}")
print(f"TSan extension: {loaded}")
' "${build_dir}" "${site_packages}"

if [[ -z "${test_target}" || "${test_target}" == "--verify-only" ]]; then
  ctest --test-dir "${build_dir}" \
    -R schema_sanitizer_tsan_ordered_executor --output-on-failure
fi
if [[ "${test_target}" == "--verify-only" ]]; then
  exit 0
fi
if [[ -n "${test_target}" && ! -f "${test_target}" ]]; then
  echo "TSan test domain does not exist: ${test_target}" >&2
  exit 2
fi

process_is_live() {
  local process_state
  process_state="$(ps -o stat= -p "$1" 2>/dev/null | tr -d "[:space:]")"
  [[ -n "${process_state}" && "${process_state}" != Z* ]]
}

run_tsan_domain() {
  local test_file="$1"
  local marker
  marker="$(mktemp "${TMPDIR:-/tmp}/schema-sanitizer-tsan.XXXXXX")"

  setsid "${python_launcher}" -u -c '
import pathlib
import sys

sys.path.insert(0, sys.argv[1])
sys.path.insert(0, "src")
import pytest


class _RecordSessionResult:
    @pytest.hookimpl(trylast=True)
    def pytest_sessionfinish(self, session, exitstatus):
        del session
        pathlib.Path(sys.argv[3]).write_text(str(int(exitstatus)), encoding="ascii")


raise SystemExit(pytest.main(["-q", sys.argv[2]], plugins=[_RecordSessionResult()]))
' "${site_packages}" "${test_file}" "${marker}" &
  local domain_pid=$!
  local deadline=$((SECONDS + domain_timeout_seconds))
  local session_status=""
  local process_status=0

  while process_is_live "${domain_pid}"; do
    if [[ -s "${marker}" ]]; then
      session_status="$(cat "${marker}")"
      local grace_deadline=$((SECONDS + domain_shutdown_grace_seconds))
      while process_is_live "${domain_pid}" && ((SECONDS < grace_deadline)); do
        sleep 0.1
      done
      if process_is_live "${domain_pid}"; then
        echo "TSan domain completed; terminating non-instrumented interpreter teardown: ${test_file}"
        kill -TERM -- "-${domain_pid}" 2>/dev/null || true
        sleep 1
        kill -KILL -- "-${domain_pid}" 2>/dev/null || true
      fi
      wait "${domain_pid}" 2>/dev/null || true
      rm -f "${marker}"
      return "${session_status}"
    fi
    if ((SECONDS >= deadline)); then
      echo "TSan domain timed out before pytest completed: ${test_file}" >&2
      kill -TERM -- "-${domain_pid}" 2>/dev/null || true
      sleep 1
      kill -KILL -- "-${domain_pid}" 2>/dev/null || true
      wait "${domain_pid}" 2>/dev/null || true
      rm -f "${marker}"
      return 124
    fi
    sleep 0.1
  done

  if wait "${domain_pid}"; then
    process_status=0
  else
    process_status=$?
  fi
  if [[ -s "${marker}" ]]; then
    session_status="$(cat "${marker}")"
    rm -f "${marker}"
    return "${session_status}"
  fi
  rm -f "${marker}"
  return "${process_status}"
}

if [[ -n "${test_target}" ]]; then
  tests=("${test_target}")
else
  tests=(
    tests/test_threading_native_executor.py
    tests/test_threading_inference.py
    tests/test_threading_materialization.py
    tests/test_threading_output.py
    tests/test_threading_parquet_output.py
    tests/test_threading_golden_matrix.py
    tests/test_partition_lookahead.py
  )
fi
for ((round = 1; round <= rounds; ++round)); do
  echo "Full-extension TSan differential round ${round}/${rounds}"
  for test_file in "${tests[@]}"; do
    echo "TSan domain: ${test_file}"
    run_tsan_domain "${test_file}"
  done
done
