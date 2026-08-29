#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
repository_root=$(cd -- "${script_dir}/../../.." && pwd -P)
plan_root=$(mktemp -d)
cleanup_plan() { rm -rf -- "${plan_root}"; }
trap cleanup_plan EXIT

cd -- "${repository_root}"
python -m benchmarks.concurrency.threading.matrix \
  "$@" \
  --work-root "${plan_root}/work" \
  --command-output "${plan_root}/commands.sh"
bash "${plan_root}/commands.sh"
