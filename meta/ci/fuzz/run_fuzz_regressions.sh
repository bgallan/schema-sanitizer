#!/usr/bin/env bash
# Build and execute a deterministic fuzz plan without Python process orchestration.
# The wrapper owns every temporary path and removes it after success or failure.
set -euo pipefail
umask 077

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
repository_root=$(cd -- "${script_dir}/../../.." && pwd -P)
plan_root=$(mktemp -d)
cleanup_plan() {
  local status=$?
  local cleanup_status=0
  trap - EXIT
  rm -rf -- "${plan_root}" || cleanup_status=$?
  if ((status != 0)); then
    exit "${status}"
  fi
  exit "${cleanup_status}"
}
trap cleanup_plan EXIT

(
  cd -- "${repository_root}"
  python "${script_dir}/run_fuzz_regressions.py" \
    "$@" \
    --work-root "${plan_root}/work" \
    --command-output "${plan_root}/commands.sh"
)
(
  cd -- "${repository_root}"
  bash "${plan_root}/commands.sh"
)
