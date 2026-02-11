#!/usr/bin/env bash
set -euo pipefail

# Parse submission test results.
# Reads jobs.log from a results directory, queries sacct for each job,
# and checks exit codes against expected results.
#
# Usage:
#   ./tests/parse-results.sh tests/results-bash
#   ./tests/parse-results.sh tests/results-python

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"
source "${SCRIPT_DIR}/register.sh"

RESULTS_DIR="${1:-}"

if [[ -z "${RESULTS_DIR}" || ! -d "${RESULTS_DIR}" ]]; then
  printf "Usage: %s <results-directory>\n" "$(basename "$0")"
  exit 1
fi

JOB_LOG="${RESULTS_DIR}/jobs.log"
if [[ ! -f "${JOB_LOG}" ]]; then
  printf "Error: %s not found\n" "${JOB_LOG}"
  exit 1
fi

PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0

log_pass() {
  PASS_COUNT=$((PASS_COUNT + 1))
  printf "  PASS: %s\n" "$1"
}

log_fail() {
  FAIL_COUNT=$((FAIL_COUNT + 1))
  printf "  FAIL: %s -- %s\n" "$1" "$2"
}

# Query sacct for a job's exit code and state
#
# Arguments:
#   $1 - job ID
#
# Outputs:
#   "STATE EXIT_CODE" on stdout
query_job() {
  local job_id="$1"
  /usr/bin/sacct -n -j "${job_id}" \
    --format=State,ExitCode \
    --parsable2 \
    | head -1
}

# Check a single job result against expected outcome
#
# Arguments:
#   $1 - test name
#   $2 - job ID
#   $3 - expected: "success", "software-error", "timeout"
check_job() {
  local test_name="$1"
  local job_id="$2"
  local expected="$3"

  printf "\n[%s] job=%s expected=%s\n" "${test_name}" "${job_id}" "${expected}"

  if [[ "${job_id}" == "SUBMIT_FAILED" ]]; then
    log_fail "${test_name}" "submission failed"
    return
  fi
  if [[ "${job_id}" == "UNKNOWN" ]]; then
    log_fail "${test_name}" "unknown job ID"
    return
  fi

  local sacct_line
  sacct_line=$(query_job "${job_id}" 2>/dev/null || true)

  if [[ -z "${sacct_line}" ]]; then
    log_fail "${test_name}" "sacct returned no data (job may still be running)"
    return
  fi

  local state exit_code
  state=$(printf "%s" "${sacct_line}" | cut -d'|' -f1)
  exit_code=$(printf "%s" "${sacct_line}" | cut -d'|' -f2)

  printf "  state=%s exit=%s\n" "${state}" "${exit_code}"

  case "${expected}" in
    success)
      if [[ "${state}" == "COMPLETED" && "${exit_code}" == "0:0" ]]; then
        log_pass "${test_name}"
      else
        log_fail "${test_name}" "expected COMPLETED/0:0, got ${state}/${exit_code}"
      fi
      ;;
    software-error)
      if [[ "${state}" == "FAILED" || "${exit_code}" != "0:0" ]]; then
        log_pass "${test_name}"
      else
        log_fail "${test_name}" "expected non-zero exit, got ${state}/${exit_code}"
      fi
      ;;
    timeout)
      if [[ "${state}" == "TIMEOUT" || "${state}" == "CANCELLED" ]]; then
        log_pass "${test_name}"
      else
        log_fail "${test_name}" "expected TIMEOUT/CANCELLED, got ${state}/${exit_code}"
      fi
      ;;
    *)
      log_fail "${test_name}" "unknown expected result: ${expected}"
      ;;
  esac

  # Check for output files in test directory
  local test_dir="${RESULTS_DIR}/${test_name}"
  if [[ -d "${test_dir}" ]]; then
    local output_count
    output_count=$(find "${test_dir}" -name "*.log" -o -name "*.out" 2>/dev/null | wc -l)
    printf "  output files: %d\n" "${output_count}"
  fi
}

printf "Parsing results from: %s\n" "${RESULTS_DIR}"

while IFS= read -r line; do
  [[ "${line}" =~ ^#.*$ ]] && continue
  [[ -z "${line}" ]] && continue

  local_name=$(printf "%s" "${line}" | awk '{print $1}')
  local_job=$(printf "%s" "${line}" | awk '{print $2}')
  local_expected=$(printf "%s" "${line}" | awk '{print $3}')

  check_job "${local_name}" "${local_job}" "${local_expected}"
done < "${JOB_LOG}"

printf "\nResults: %d passed, %d failed, %d skipped\n" \
  "${PASS_COUNT}" "${FAIL_COUNT}" "${SKIP_COUNT}"

if [[ ${FAIL_COUNT} -gt 0 ]]; then
  exit 1
fi

# Extract mode from results directory name (results-bash -> bash)
REGISTER_MODE=$(basename "${RESULTS_DIR}" | sed 's/^results-//')
register_result "parse-results" "${REGISTER_MODE}" "${PASS_COUNT}" "${FAIL_COUNT}" "${SKIP_COUNT}"
