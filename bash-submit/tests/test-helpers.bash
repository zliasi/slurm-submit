#!/usr/bin/env bash
# Shared test setup for bats tests
#
# Source this in each .bats file after loading bats libraries.

readonly TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_ROOT="$(cd "${TEST_DIR}/.." && pwd)"
readonly LIB_DIR="${PROJECT_ROOT}/lib"
readonly BIN_DIR="${PROJECT_ROOT}/bin"
readonly MODULES_DIR="${PROJECT_ROOT}/modules"
readonly INPUTS_DIR="${TEST_DIR}/inputs"

# Load bats libraries
load "${TEST_DIR}/lib/bats-support/load"
load "${TEST_DIR}/lib/bats-assert/load"

# Source all library files for unit testing
#
# Returns:
#   0 - Success
source_libraries() {
  source "${LIB_DIR}/core.sh"
  source "${PROJECT_ROOT}/config/defaults.sh"
  source "${LIB_DIR}/config.sh"
  source "${LIB_DIR}/args.sh"
  source "${LIB_DIR}/backup.sh"
  source "${LIB_DIR}/manifest.sh"
  source "${LIB_DIR}/partition.sh"
  source "${LIB_DIR}/scratch.sh"
  source "${LIB_DIR}/sbatch.sh"
}

# Source a module file
#
# Arguments:
#   $1 - module_name: Module to source (e.g. "orca")
#
# Returns:
#   0 - Success
source_module() {
  local module_name="$1"
  source "${MODULES_DIR}/${module_name}.sh"
}

# Create a temporary directory for test isolation
#
# Sets TEST_TMPDIR global.
#
# Returns:
#   0 - Success
setup_tmpdir() {
  TEST_TMPDIR=$(mktemp -d)
}

# Remove temporary directory
#
# Returns:
#   0 - Success
teardown_tmpdir() {
  [[ -n "${TEST_TMPDIR:-}" && -d "${TEST_TMPDIR}" ]] \
    && rm -rf "${TEST_TMPDIR}"
}

# Create a mock sbatch that captures the script
#
# Arguments:
#   $1 - capture_dir: Directory to store captured script
#
# Returns:
#   0 - Success
create_mock_sbatch() {
  local capture_dir="$1"
  local mock_sbatch="${capture_dir}/sbatch"

  cat >"${mock_sbatch}" <<'MOCK'
#!/usr/bin/env bash
cat >"${MOCK_SBATCH_CAPTURE:-/dev/null}"
printf "Submitted batch job 12345\n"
MOCK
  chmod +x "${mock_sbatch}"
  export PATH="${capture_dir}:${PATH}"
  export MOCK_SBATCH_CAPTURE="${capture_dir}/captured-script.sh"
}

# Create dummy input files for testing
#
# Returns:
#   0 - Success
create_test_inputs() {
  mkdir -p "${INPUTS_DIR}"

  [[ -f "${INPUTS_DIR}/test.inp" ]] \
    || printf "test input\n" >"${INPUTS_DIR}/test.inp"
  [[ -f "${INPUTS_DIR}/test.xyz" ]] \
    || printf "2\ntest\nH 0 0 0\nH 0 0 1\n" >"${INPUTS_DIR}/test.xyz"
  [[ -f "${INPUTS_DIR}/test.com" ]] \
    || printf "test gaussian\n" >"${INPUTS_DIR}/test.com"
  [[ -f "${INPUTS_DIR}/test.dal" ]] \
    || printf "**DALTON INPUT\n" >"${INPUTS_DIR}/test.dal"
  [[ -f "${INPUTS_DIR}/test.mol" ]] \
    || printf "MOL\n" >"${INPUTS_DIR}/test.mol"
  [[ -f "${INPUTS_DIR}/test.py" ]] \
    || printf "print('hello')\n" >"${INPUTS_DIR}/test.py"
  [[ -f "${INPUTS_DIR}/test.molden" ]] \
    || printf "molden\n" >"${INPUTS_DIR}/test.molden"
}
