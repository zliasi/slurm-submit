#!/usr/bin/env bash
# HPC integration test validator
#
# Reads job manifest from run-tests.sh, queries sacct for each job,
# inspects output/log files, checks expected patterns, reports pass/fail.
#
# Usage:
#   ./check-results.sh [--wait SECONDS]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
source "${SCRIPT_DIR}/lib/common.sh"

WAIT_TIMEOUT=0
if [[ "${1:-}" == "--wait" ]]; then
  WAIT_TIMEOUT="${2:-300}"
fi

# Wait for all jobs in manifest to complete
#
# Arguments:
#   $1 - timeout_seconds: Max seconds to wait (0 = no wait)
#
# Returns:
#   0 - All jobs finished
#   1 - Timeout
wait_for_jobs() {
  local timeout_seconds="$1"
  [[ "${timeout_seconds}" -gt 0 ]] || return 0

  local job_ids
  job_ids=$(cut -f2 "${MANIFEST_FILE}" | tr '\n' ',' | sed 's/,$//')
  [[ -n "${job_ids}" ]] || return 0

  printf "Waiting for jobs to complete (timeout: %ds)\n" "${timeout_seconds}"

  local elapsed=0
  local interval=10
  while [[ ${elapsed} -lt ${timeout_seconds} ]]; do
    local pending
    pending=$(squeue -j "${job_ids}" --noheader 2>/dev/null | wc -l)
    if [[ "${pending}" -eq 0 ]]; then
      printf "All jobs completed after %ds\n" "${elapsed}"
      return 0
    fi
    printf "  %d jobs still running (%ds elapsed)\n" \
      "${pending}" "${elapsed}"
    sleep "${interval}"
    elapsed=$((elapsed + interval))
  done

  printf "Timeout: some jobs still running after %ds\n" "${timeout_seconds}"
  return 1
}

# Validate a single job entry from the manifest
#
# Arguments:
#   $1 - test_name: Test identifier
#   $2 - job_id: SLURM job ID
#   $3 - expected_state: Expected sacct state
#   $4 - expected_exit: Expected exit code
#   $5 - output_dir: Output directory path
#
# Returns:
#   0 - All checks passed
#   1 - At least one check failed
validate_job() {
  local test_name="$1"
  local job_id="$2"
  local expected_state="$3"
  local expected_exit="$4"
  local output_dir="$5"

  local failures=0
  local detail

  detail=$(check_sacct_status "${job_id}" "${expected_state}" 2>&1) \
    || { failures=$((failures + 1))
         printf "    sacct state: %s\n" "${detail}"; }

  detail=$(check_exit_code "${job_id}" "${expected_exit}" 2>&1) \
    || { failures=$((failures + 1))
         printf "    exit code: %s\n" "${detail}"; }

  if [[ "${expected_state}" == "COMPLETED" ]]; then
    local has_output=false
    for f in "${output_dir}"/*.out "${output_dir}"/*.log; do
      if [[ -f "${f}" ]]; then
        has_output=true
        break
      fi
    done
    if [[ "${has_output}" == false ]]; then
      printf "    no output files in %s\n" "${output_dir}"
      failures=$((failures + 1))
    fi
  fi

  check_patterns "${test_name}" "${output_dir}" \
    || failures=$((failures + $?))

  if [[ "${failures}" -eq 0 ]]; then
    report "${test_name}" "PASS" "job=${job_id}"
  else
    report "${test_name}" "FAIL" \
      "job=${job_id} (${failures} check(s) failed)"
  fi

  return "${failures}"
}


# Validate specific cross-cutting post-submission checks
#
# Returns:
#   0 - Always (reports inline)
validate_cross_cutting() {
  local out_dir="${RESULTS_DIR}/output"

  if [[ -d "${out_dir}/common-no-archive" ]]; then
    local archive_count
    archive_count=$(find "${out_dir}/common-no-archive" \
      -name "*.tar.xz" 2>/dev/null | wc -l)
    if [[ "${archive_count}" -eq 0 ]]; then
      report "common-no-archive-check" "PASS" "no archive created"
    else
      report "common-no-archive-check" "FAIL" \
        "unexpected .tar.xz found"
    fi
  fi

  if [[ -d "${out_dir}/common-custom-jobname" ]]; then
    local job_id
    job_id=$(grep "^common-custom-jobname" "${MANIFEST_FILE}" \
      | cut -f2 || true)
    if [[ -n "${job_id}" ]]; then
      local actual_name
      actual_name=$(sacct -j "${job_id}" --format=JobName \
        --noheader --parsable2 2>/dev/null | head -1 | tr -d '[:space:]')
      if [[ "${actual_name}" == "myjob" ]]; then
        report "common-custom-jobname-check" "PASS" \
          "job name = myjob"
      else
        report "common-custom-jobname-check" "FAIL" \
          "expected name=myjob got=${actual_name}"
      fi
    fi
  fi

  if [[ -d "${out_dir}/common-output-dir/custom-out" ]]; then
    local custom_out="${out_dir}/common-output-dir/custom-out"
    local has_files=false
    for f in "${custom_out}"/*.out "${custom_out}"/*.log; do
      if [[ -f "${f}" ]]; then
        has_files=true
        break
      fi
    done
    if [[ "${has_files}" == true ]]; then
      report "common-output-dir-check" "PASS" \
        "outputs in custom dir"
    else
      report "common-output-dir-check" "FAIL" \
        "no outputs in custom dir"
    fi
  fi
}

main() {
  [[ -f "${MANIFEST_FILE}" ]] \
    || { printf "No manifest found at %s\n" "${MANIFEST_FILE}" >&2
         printf "Run run-tests.sh first\n" >&2
         exit 1; }

  printf "HPC Integration Test Validator\n\n"

  wait_for_jobs "${WAIT_TIMEOUT}" || true

  : >"${REPORT_FILE}"

  printf "Checking submitted jobs\n\n"

  local test_name job_id expected_state expected_exit output_dir
  while IFS=$'\t' read -r test_name job_id expected_state \
    expected_exit output_dir; do
    [[ -n "${test_name}" ]] || continue
    validate_job "${test_name}" "${job_id}" "${expected_state}" \
      "${expected_exit}" "${output_dir}" || true
  done <"${MANIFEST_FILE}"

  printf "\nCross-cutting checks\n\n"
  validate_cross_cutting

  printf "\n"
  print_summary || exit 1
}

main "$@"
