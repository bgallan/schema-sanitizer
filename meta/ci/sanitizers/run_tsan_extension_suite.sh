#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
build_dir="${1:-${repo_root}/.work/build/tsan}"
python_launcher="${2:-${repo_root}/.work/bin/python-tsan}"
rounds="${3:-1}"
site_packages="${4:-}"
test_target="${5:-}"
domain_timeout_seconds=300
domain_launch_timeout_seconds=5
domain_shutdown_grace_seconds=5
domain_kill_timeout_seconds=5

# Override every sanitizer option at the execution boundary. Empty values are
# deliberate: unrelated runner-level sanitizer settings must never leak in.
readonly ASAN_OPTIONS=''
readonly LSAN_OPTIONS=''
readonly TSAN_OPTIONS='halt_on_error=1:history_size=7:second_deadlock_stack=1'
readonly UBSAN_OPTIONS=''
export ASAN_OPTIONS LSAN_OPTIONS TSAN_OPTIONS UBSAN_OPTIONS

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
if [[ -z "${site_packages}" ]]; then
  site_packages="$(python -c 'import site; print(site.getsitepackages()[0])')"
fi
if [[ ! -d "${site_packages}" ]]; then
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
    -R schema_sanitizer_tsan_ordered_executor --output-on-failure \
    --timeout "${domain_timeout_seconds}"
fi
if [[ "${test_target}" == "--verify-only" ]]; then
  exit 0
fi
if [[ -n "${test_target}" && ! -f "${test_target}" ]]; then
  echo "TSan test domain does not exist: ${test_target}" >&2
  exit 2
fi

process_group_is_live() {
  local domain_pgid="$1"
  local process_pgid
  local process_state
  local process_table

  # Inspection errors must fail closed: treating an unknown group as dead could
  # turn a cleanup failure into a green sanitizer result.
  if ! process_table="$(ps -e -o pgid=,stat= 2>/dev/null)"; then
    return 0
  fi
  while read -r process_pgid process_state; do
    if [[ "${process_pgid}" == "${domain_pgid}" && "${process_state}" != Z* ]]; then
      return 0
    fi
  done <<<"${process_table}"
  return 1
}

terminate_tsan_domain() {
  local domain_pgid="$1"
  local domain_wait_pid="$2"
  local test_file="$3"
  local termination_deadline

  kill -TERM -- "-${domain_pgid}" 2>/dev/null || true
  termination_deadline=$((SECONDS + domain_shutdown_grace_seconds))
  while process_group_is_live "${domain_pgid}" && ((SECONDS < termination_deadline)); do
    sleep 0.1
  done
  if process_group_is_live "${domain_pgid}"; then
    echo "TSan domain ignored TERM; escalating to KILL: ${test_file}" >&2
    kill -KILL -- "-${domain_pgid}" 2>/dev/null || true
    termination_deadline=$((SECONDS + domain_kill_timeout_seconds))
    while process_group_is_live "${domain_pgid}" && ((SECONDS < termination_deadline)); do
      sleep 0.1
    done
  fi
  if process_group_is_live "${domain_pgid}"; then
    echo "TSan domain resisted bounded termination: ${test_file}" >&2
    return 124
  fi
  wait "${domain_wait_pid}" 2>/dev/null || true
}

run_tsan_domain() {
  local test_file="$1"
  local identity
  local marker
  identity="$(mktemp "${TMPDIR:-/tmp}/schema-sanitizer-tsan-identity.XXXXXX")"
  marker="$(mktemp "${TMPDIR:-/tmp}/schema-sanitizer-tsan.XXXXXX")"

  # The single-quoted variables below are expanded by the new session's shell.
  # shellcheck disable=SC2016
  setsid --wait bash -c '
identity_file="$1"
shift
printf "%s\n" "$$" > "${identity_file}.pending"
mv -- "${identity_file}.pending" "${identity_file}"
exec "$@"
' schema-sanitizer-tsan-domain "${identity}" "${python_launcher}" -u -c '
import pathlib
import sys

sys.path.insert(0, sys.argv[1])
sys.path.insert(0, "src")
import pytest

_PYARROW_DATASET_FALLBACK = (
    "tests/concurrency/test_threading_golden_matrix.py::"
    "test_fixed_clock_parquet_input_fallback_equivalence"
)


class _RecordSessionResult:
    @pytest.hookimpl(trylast=True)
    def pytest_sessionfinish(self, session, exitstatus):
        del session
        pathlib.Path(sys.argv[3]).write_text(str(int(exitstatus)), encoding="ascii")


pytest_args = ["-q", "--capture=no", sys.argv[2]]
if sys.argv[2] == "tests/concurrency/test_threading_golden_matrix.py":
    # This functional fallback contract intentionally owns the PyArrow asynchronous
    # dataset scanner. Its uninstrumented worker pool is covered in regular CI,
    # while this gate remains focused on instrumented project concurrency.
    pytest_args.extend(["--deselect", _PYARROW_DATASET_FALLBACK])
raise SystemExit(pytest.main(pytest_args, plugins=[_RecordSessionResult()]))
' "${site_packages}" "${test_file}" "${marker}" &
  local domain_wait_pid=$!
  local launch_deadline=$((SECONDS + domain_launch_timeout_seconds))
  while [[ ! -s "${identity}" ]] && ((SECONDS < launch_deadline)); do
    sleep 0.1
  done
  if [[ ! -s "${identity}" ]]; then
    echo "TSan domain did not publish its process-group identity: ${test_file}" >&2
    kill -KILL "${domain_wait_pid}" 2>/dev/null || true
    wait "${domain_wait_pid}" 2>/dev/null || true
    rm -f "${identity}" "${identity}.pending" "${marker}"
    return 124
  fi
  local domain_pgid
  domain_pgid="$(cat "${identity}")"
  if ! [[ "${domain_pgid}" =~ ^[1-9][0-9]*$ ]] || ((domain_pgid <= 1)); then
    echo "TSan domain published an invalid process-group identity: ${domain_pgid}" >&2
    kill -KILL "${domain_wait_pid}" 2>/dev/null || true
    wait "${domain_wait_pid}" 2>/dev/null || true
    rm -f "${identity}" "${identity}.pending" "${marker}"
    return 124
  fi
  local deadline=$((SECONDS + domain_timeout_seconds))
  local session_status=""
  local process_status=0

  while process_group_is_live "${domain_pgid}"; do
    if [[ -s "${marker}" ]]; then
      session_status="$(cat "${marker}")"
      local grace_deadline=$((SECONDS + domain_shutdown_grace_seconds))
      while process_group_is_live "${domain_pgid}" && ((SECONDS < grace_deadline)); do
        sleep 0.1
      done
      if process_group_is_live "${domain_pgid}"; then
        echo "TSan domain completed; terminating non-instrumented interpreter teardown: ${test_file}"
        if ! terminate_tsan_domain "${domain_pgid}" "${domain_wait_pid}" "${test_file}"; then
          rm -f "${identity}" "${identity}.pending" "${marker}"
          return 124
        fi
      else
        wait "${domain_wait_pid}" 2>/dev/null || true
      fi
      rm -f "${identity}" "${identity}.pending" "${marker}"
      return "${session_status}"
    fi
    if ((SECONDS >= deadline)); then
      echo "TSan domain timed out before pytest completed: ${test_file}" >&2
      terminate_tsan_domain "${domain_pgid}" "${domain_wait_pid}" "${test_file}" || true
      rm -f "${identity}" "${identity}.pending" "${marker}"
      return 124
    fi
    sleep 0.1
  done

  if wait "${domain_wait_pid}"; then
    process_status=0
  else
    process_status=$?
  fi
  if [[ -s "${marker}" ]]; then
    session_status="$(cat "${marker}")"
    rm -f "${identity}" "${identity}.pending" "${marker}"
    return "${session_status}"
  fi
  rm -f "${identity}" "${identity}.pending" "${marker}"
  return "${process_status}"
}

if [[ -n "${test_target}" ]]; then
  tests=("${test_target}")
else
  tests=(
    tests/concurrency/test_threading_native_executor.py
    tests/concurrency/test_threading_inference.py
    tests/concurrency/test_threading_materialization.py
    tests/concurrency/test_threading_output.py
    tests/concurrency/test_threading_parquet_output.py
    tests/concurrency/test_threading_golden_matrix.py
    tests/pipeline/test_partition_lookahead.py
    tests/pipeline/test_csv_union_projection.py
  )
fi
for ((round = 1; round <= rounds; ++round)); do
  echo "Full-extension TSan differential round ${round}/${rounds}"
  for test_file in "${tests[@]}"; do
    echo "TSan domain: ${test_file}"
    run_tsan_domain "${test_file}"
  done
done
