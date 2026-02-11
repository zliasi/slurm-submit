#!/usr/bin/env bash
# Shared test register helper.
# Source this from test scripts, then call register_result on success.
#
# Register file: tests/test-register.log
# Format: timestamp  commit  test  mode  pass  fail  skip

REGISTER_FILE="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)/test-register.log"

# Append a result line to the register file.
#
# Arguments:
#   $1 - test type (export-validate, submit, parse-results)
#   $2 - mode (bash, python, both)
#   $3 - pass count
#   $4 - fail count
#   $5 - skip count
register_result() {
  local test_type="$1"
  local mode="$2"
  local pass="$3"
  local fail="$4"
  local skip="$5"

  local timestamp
  timestamp=$(date -Iseconds)

  local commit="unknown"
  if command -v git >/dev/null 2>&1; then
    local hash
    hash=$(git -C "$(dirname "${REGISTER_FILE}")" rev-parse --short HEAD 2>/dev/null || true)
    if [[ -n "${hash}" ]]; then
      commit="${hash}"
      if ! git -C "$(dirname "${REGISTER_FILE}")" diff --quiet HEAD 2>/dev/null; then
        commit="${hash}-dirty"
      fi
    fi
  fi

  if [[ ! -f "${REGISTER_FILE}" ]]; then
    printf "# test-register.log\n" > "${REGISTER_FILE}"
    printf "# timestamp\tcommit\ttest\tmode\tpass\tfail\tskip\n" >> "${REGISTER_FILE}"
  fi

  printf "%s\t%s\t%s\t%s\t%d\t%d\t%d\n" \
    "${timestamp}" "${commit}" "${test_type}" "${mode}" \
    "${pass}" "${fail}" "${skip}" >> "${REGISTER_FILE}"

  printf "Registered: %s %s %s (%d pass, %d fail, %d skip)\n" \
    "${test_type}" "${mode}" "${commit}" "${pass}" "${fail}" "${skip}"
}
