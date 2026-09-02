#!/usr/bin/env bash
# Build one deterministic benchmark command plan inside a process-owned workspace.
# The wrapper removes every staged case report after success, failure, or interruption.
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

cd -- "${repository_root}"
python -m benchmarks.concurrency.threading.matrix \
  "$@" \
  --work-root "${plan_root}/work" \
  --command-output "${plan_root}/commands.sh"
bash "${plan_root}/commands.sh"
