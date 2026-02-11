#!/usr/bin/env bash
# Shared helpers for HPC integration tests
#
# Provides functions for submitting test jobs, recording results,
# and validating outputs against expected patterns.
#
# Sourced by run-tests.sh and check-results.sh; not executable standalone.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/results"
MANIFEST_FILE="${RESULTS_DIR}/jobs.manifest"
REPORT_FILE="${RESULTS_DIR}/report.txt"
EXPECTED_DIR="${SCRIPT_DIR}/expected"
export INPUTS_DIR="${SCRIPT_DIR}/inputs"
export SUBMIT_BIN="${SCRIPT_DIR}/../../bin/submit"

PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0

# Ensure results directory exists
#
# Returns:
#   0 - Success
init_results() {
  mkdir -p "${RESULTS_DIR}"
  : >"${MANIFEST_FILE}"
  : >"${REPORT_FILE}"
}

# Submit a job and record its ID to the manifest
#
# Arguments:
#   $1 - test_name: Unique test identifier
#   $2 - expected_state: Expected sacct state (COMPLETED, FAILED, etc.)
#   $3 - expected_exit: Expected exit code (0, non-zero, etc.)
#   $4 - output_dir: Where outputs land
#   $5.. - submit command arguments
#
# Returns:
#   0 - Job submitted and recorded
#   1 - Submission failed
submit_and_record() {
  local test_name="$1"
  local expected_state="$2"
  local expected_exit="$3"
  local output_dir="$4"
  shift 4

  local submit_output
  if ! submit_output=$("$@" 2>&1); then
    printf "SUBMIT_FAIL\t%s\t%s\n" "${test_name}" "${submit_output}" \
      >>"${REPORT_FILE}"
    printf "  SUBMIT FAILED: %s\n" "${test_name}" >&2
    return 1
  fi

  local job_id
  job_id=$(printf "%s" "${submit_output}" \
    | grep -oP 'Submitted batch job \K[0-9]+' || true)

  if [[ -z "${job_id}" ]]; then
    printf "SUBMIT_FAIL\t%s\tno job ID in output: %s\n" \
      "${test_name}" "${submit_output}" >>"${REPORT_FILE}"
    printf "  SUBMIT FAILED (no job ID): %s\n" "${test_name}" >&2
    return 1
  fi

  printf "%s\t%s\t%s\t%s\t%s\n" \
    "${test_name}" "${job_id}" "${expected_state}" \
    "${expected_exit}" "${output_dir}" \
    >>"${MANIFEST_FILE}"

  printf "  submitted: %-30s  job=%s\n" "${test_name}" "${job_id}"
  return 0
}

# Run a pre-submission validation test (no SLURM job)
#
# Arguments:
#   $1 - test_name: Unique test identifier
#   $2 - expected_pattern: Grep pattern expected in stderr/stdout
#   $3.. - command to run
#
# Returns:
#   0 - Test passed
#   1 - Test failed
test_pre_submit() {
  local test_name="$1"
  local expected_pattern="$2"
  shift 2

  local cmd_output
  local cmd_exit=0
  cmd_output=$("$@" 2>&1) || cmd_exit=$?

  if [[ ${cmd_exit} -eq 0 ]]; then
    report "${test_name}" "FAIL" "expected non-zero exit, got 0"
    return 1
  fi

  if printf "%s" "${cmd_output}" | grep -qP "${expected_pattern}"; then
    report "${test_name}" "PASS" "validation error matched"
    return 0
  fi

  report "${test_name}" "FAIL" \
    "pattern '${expected_pattern}' not in: ${cmd_output}"
  return 1
}

# Check sacct state for a completed job
#
# Arguments:
#   $1 - job_id: SLURM job ID
#   $2 - expected_state: Expected state string
#
# Returns:
#   0 - State matches
#   1 - State mismatch or query failed
check_sacct_status() {
  local job_id="$1"
  local expected_state="$2"

  local sacct_output
  sacct_output=$(sacct -j "${job_id}" \
    --format=State --noheader --parsable2 2>/dev/null \
    | head -1 | tr -d '[:space:]')

  if [[ "${sacct_output}" == "${expected_state}" ]]; then
    return 0
  fi

  printf "expected state=%s got=%s" "${expected_state}" "${sacct_output}"
  return 1
}

# Check sacct exit code for a completed job
#
# Arguments:
#   $1 - job_id: SLURM job ID
#   $2 - expected_exit: Expected exit code (integer or "non-zero")
#
# Returns:
#   0 - Exit code matches
#   1 - Mismatch
check_exit_code() {
  local job_id="$1"
  local expected_exit="$2"

  local exit_str
  exit_str=$(sacct -j "${job_id}" \
    --format=ExitCode --noheader --parsable2 2>/dev/null \
    | head -1 | tr -d '[:space:]')

  local actual_code="${exit_str%%:*}"

  if [[ "${expected_exit}" == "non-zero" ]]; then
    [[ "${actual_code}" != "0" ]] && return 0
    printf "expected non-zero exit, got 0"
    return 1
  fi

  if [[ "${actual_code}" == "${expected_exit}" ]]; then
    return 0
  fi

  printf "expected exit=%s got=%s" "${expected_exit}" "${actual_code}"
  return 1
}

# Assert a file exists
#
# Arguments:
#   $1 - filepath: Path to check
#
# Returns:
#   0 - File exists
#   1 - File missing
check_file_exists() {
  local filepath="$1"
  [[ -f "${filepath}" ]] && return 0
  printf "file missing: %s" "${filepath}"
  return 1
}

# Assert a file contains a pattern
#
# Arguments:
#   $1 - filepath: File to search
#   $2 - pattern: Grep pattern (extended regex)
#
# Returns:
#   0 - Pattern found
#   1 - Pattern not found
check_file_contains() {
  local filepath="$1"
  local pattern="$2"

  if [[ ! -f "${filepath}" ]]; then
    printf "file missing: %s" "${filepath}"
    return 1
  fi

  if grep -qE "${pattern}" "${filepath}" 2>/dev/null; then
    return 0
  fi

  printf "pattern '%s' not in %s" "${pattern}" "${filepath}"
  return 1
}

# Assert a file does NOT contain a pattern
#
# Arguments:
#   $1 - filepath: File to search
#   $2 - pattern: Grep pattern (extended regex)
#
# Returns:
#   0 - Pattern absent
#   1 - Pattern found (unexpected)
check_file_not_contains() {
  local filepath="$1"
  local pattern="$2"

  [[ ! -f "${filepath}" ]] && return 0

  if grep -qE "${pattern}" "${filepath}" 2>/dev/null; then
    printf "unexpected pattern '%s' found in %s" "${pattern}" "${filepath}"
    return 1
  fi

  return 0
}

# Log a test result
#
# Arguments:
#   $1 - test_name: Test identifier
#   $2 - status: PASS, FAIL, or SKIP
#   $3 - message: Detail message
#
# Returns:
#   0 - Always
report() {
  local test_name="$1"
  local status="$2"
  local message="${3:-}"

  printf "%s\t%s\t%s\n" "${status}" "${test_name}" "${message}" \
    >>"${REPORT_FILE}"

  case "${status}" in
    PASS) PASS_COUNT=$((PASS_COUNT + 1)) ;;
    FAIL) FAIL_COUNT=$((FAIL_COUNT + 1)) ;;
    SKIP) SKIP_COUNT=$((SKIP_COUNT + 1)) ;;
  esac

  printf "  %-6s %s" "${status}" "${test_name}"
  [[ -n "${message}" ]] && printf "  (%s)" "${message}"
  printf "\n"
}

# Validate a job against expected patterns file
#
# Arguments:
#   $1 - test_name: Test identifier
#   $2 - output_dir: Output directory path
#
# Returns:
#   0 - All patterns matched
#   1 - At least one pattern check failed
check_patterns() {
  local test_name="$1"
  local output_dir="$2"
  local pattern_file="${EXPECTED_DIR}/${test_name}.patterns"

  [[ -f "${pattern_file}" ]] || return 0

  local failures=0
  local line prefix pattern
  while IFS= read -r line || [[ -n "${line}" ]]; do
    [[ "${line}" =~ ^[[:space:]]*$ ]] && continue
    [[ "${line}" =~ ^[[:space:]]*# ]] && continue

    prefix="${line:0:1}"
    pattern="${line:1}"

    case "${prefix}" in
      +)
        local found=false
        for f in "${output_dir}"/*.out "${output_dir}"/*.log; do
          [[ -f "${f}" ]] || continue
          if grep -qE "${pattern}" "${f}" 2>/dev/null; then
            found=true
            break
          fi
        done
        if [[ "${found}" == false ]]; then
          printf "    missing pattern: +%s\n" "${pattern}" >&2
          failures=$((failures + 1))
        fi
        ;;
      -)
        for f in "${output_dir}"/*.out "${output_dir}"/*.log; do
          [[ -f "${f}" ]] || continue
          if grep -qE "${pattern}" "${f}" 2>/dev/null; then
            printf "    unexpected pattern: -%s in %s\n" \
              "${pattern}" "${f}" >&2
            failures=$((failures + 1))
          fi
        done
        ;;
    esac
  done <"${pattern_file}"

  return "${failures}"
}

# Print final summary
#
# Returns:
#   0 - All tests passed
#   1 - At least one failure
print_summary() {
  local total=$((PASS_COUNT + FAIL_COUNT + SKIP_COUNT))
  printf "\nResults: %d passed, %d failed, %d skipped (total %d)\n" \
    "${PASS_COUNT}" "${FAIL_COUNT}" "${SKIP_COUNT}" "${total}"
  [[ "${FAIL_COUNT}" -eq 0 ]] && return 0
  return 1
}
