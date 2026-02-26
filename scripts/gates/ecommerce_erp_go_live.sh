#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  bash scripts/gates/ecommerce_erp_go_live.sh [--out-dir <path>|--out-dir=<path>]

Runs DHA-69 ecommerce/ERP go-live gates in deterministic order and writes logs.
USAGE
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

out_dir_override=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --out-dir)
      shift
      if [ "$#" -eq 0 ]; then
        echo "error: missing path after --out-dir" >&2
        exit 2
      fi
      out_dir_override="$1"
      ;;
    --out-dir=*)
      out_dir_override="${1#--out-dir=}"
      if [ -z "${out_dir_override}" ]; then
        echo "error: missing path after --out-dir=" >&2
        exit 2
      fi
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

timestamp_utc="$(date -u +"%Y%m%dT%H%M%SZ")"
if [ -n "${out_dir_override}" ]; then
  out_dir="${out_dir_override}"
else
  out_dir="var/gates/ecommerce-erp-go-live/${timestamp_utc}"
fi
mkdir -p "${out_dir}"

summary_file="${out_dir}/summary.txt"
git_sha="$(git rev-parse HEAD)"

cat > "${summary_file}" <<SUMMARY
DHA-69 Ecommerce/ERP Go-Live Gate Summary
started_at_utc=${timestamp_utc}
git_sha=${git_sha}
repo_root=${REPO_ROOT}
out_dir=${out_dir}
SUMMARY

run_step() {
  local step_name="$1"
  local log_name="$2"
  local log_path="${out_dir}/${log_name}"
  local status=0
  shift 2

  {
    echo "step=${step_name}"
    echo "started_at_utc=$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    printf 'command='
    printf '%q ' "$@"
    echo
  } >"${log_path}"

  if "$@" >>"${log_path}" 2>&1; then
    echo "finished_at_utc=$(date -u +"%Y-%m-%dT%H:%M:%SZ")" >>"${log_path}"
    echo "${step_name}=PASS log=${log_name}" | tee -a "${summary_file}" >/dev/null
    return 0
  else
    status=$?
  fi

  {
    echo "finished_at_utc=$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo "exit_code=${status}"
  } >>"${log_path}"
  echo "${step_name}=FAIL exit_code=${status} log=${log_name}" | tee -a "${summary_file}"
  exit "${status}"
}

run_step "compile_supplier" "compile_supplier.log" \
  cargo run -p dharma-cli -- compile contracts/std/commerce_inventory_supplier.dhl
run_step "compile_warehouse" "compile_warehouse.log" \
  cargo run -p dharma-cli -- compile contracts/std/commerce_logistics_warehouse.dhl
run_step "compile_sellable" "compile_sellable.log" \
  cargo run -p dharma-cli -- compile contracts/std/commerce_inventory_sellable.dhl
run_step "compile_order_line" "compile_order_line.log" \
  cargo run -p dharma-cli -- compile contracts/std/commerce_order_line.dhl
run_step "rebuild_gate" "rebuild.log" \
  cargo test -p dharma-cli cmd::ops::tests::project_rebuild_populates_commerce_projections -- --exact --nocapture
run_step "key_queries_gate" "key_queries.log" \
  cargo test -p dharma-cli cmd::ops::tests::ecommerce_key_queries_return_expected_rows -- --exact --nocapture
run_step "watch_gate" "watch.log" \
  cargo test -p dharma-cli cmd::ops::tests::project_watch_applies_incremental_update -- --exact --nocapture

echo "completed_at_utc=$(date -u +"%Y-%m-%dT%H:%M:%SZ")" >> "${summary_file}"
echo "overall=PASS" >> "${summary_file}"
