#!/usr/bin/env bash
# Build and execute an isolated downstream-install plan with strict argument boundaries.
# All temporary paths are process-owned and removed after success or failure.
set -euo pipefail
umask 077

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
plan_root=$(mktemp -d)
cleanup_plan() { rm -rf -- "${plan_root}"; }
trap cleanup_plan EXIT

python "${script_dir}/check_downstream_install.py" \
  "$@" \
  --work-root "${plan_root}/work" \
  --command-output "${plan_root}/commands.sh"
bash "${plan_root}/commands.sh"
