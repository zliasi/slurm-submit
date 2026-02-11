#!/usr/bin/env bash
# HPC integration test submitter
#
# Submits all test jobs via bash-submit, records job IDs to manifest.
# Run check-results.sh after jobs complete to validate outcomes.
#
# Usage:
#   ./run-tests.sh [--module MODULE]  # submit all or one module's tests

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
source "${SCRIPT_DIR}/lib/common.sh"

BIN_DIR="${SCRIPT_DIR}/../../bin"
INPUTS="${SCRIPT_DIR}/inputs"

FILTER_MODULE="${1:-}"
if [[ "${FILTER_MODULE}" == "--module" ]]; then
  FILTER_MODULE="${2:-}"
  [[ -n "${FILTER_MODULE}" ]] \
    || { printf "Usage: %s [--module MODULE]\n" "$0" >&2; exit 1; }
elif [[ -n "${FILTER_MODULE}" ]]; then
  FILTER_MODULE=""
fi

# Check that submit binary and sbatch are available
#
# Returns:
#   0 - All dependencies present
#   1 - Missing dependency (exits)
check_dependencies() {
  [[ -x "${BIN_DIR}/submit" ]] \
    || { printf "submit binary not found at %s\n" "${BIN_DIR}" >&2; exit 1; }
  command -v sbatch >/dev/null 2>&1 \
    || { printf "sbatch not found; run on a SLURM cluster\n" >&2; exit 1; }
}

# Determine if module should run based on filter
#
# Arguments:
#   $1 - module_name: Module to check
#
# Returns:
#   0 - Should run
#   1 - Filtered out
should_run() {
  local module_name="$1"
  [[ -z "${FILTER_MODULE}" || "${FILTER_MODULE}" == "${module_name}" ]]
}

# Create a per-test output directory
#
# Arguments:
#   $1 - test_name: Test identifier
#
# Outputs:
#   Absolute path to output dir
#
# Returns:
#   0 - Success
make_test_output_dir() {
  local test_name="$1"
  local dir="${RESULTS_DIR}/output/${test_name}"
  mkdir -p "${dir}"
  printf "%s/" "${dir}"
}

run_pre_submission_tests() {
  printf "\nPre-submission validation tests\n\n"

  local orca_inp="${INPUTS}/orca/hf-h2.inp"

  test_pre_submit "common-bad-time" \
    "Invalid time format" \
    "${BIN_DIR}/sorca" "${orca_inp}" -t abc -c 1 -m 2 || true

  test_pre_submit "common-bad-cpus" \
    "must be positive integer" \
    "${BIN_DIR}/sorca" "${orca_inp}" -c 0 -m 2 || true

  test_pre_submit "common-missing-arg" \
    "requires a value" \
    "${BIN_DIR}/sorca" "${orca_inp}" -c || true
}

submit_dalton_tests() {
  should_run "dalton" || return 0
  printf "\nDalton tests\n\n"

  local d="${INPUTS}/dalton"
  local out

  out=$(make_test_output_dir "dalton-single")
  submit_and_record "dalton-single" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/sdalton" "${d}/hf-h2.dal" "${d}/h2.mol" \
    -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "dalton-array")
  submit_and_record "dalton-array" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/sdalton" "${d}/hf-h2.dal" "${d}/h2.mol" "${d}/h2o.mol" \
    -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "dalton-pot")
  submit_and_record "dalton-pot" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/sdalton" "${d}/hf-h2o.dal" "${d}/h2o.mol" "${d}/h2o.pot" \
    -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "dalton-loprop")
  submit_and_record "dalton-loprop" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/sdalton" "${d}/hf-h2.dal" "${d}/h2.mol" \
    -l -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "dalton-embedded")
  submit_and_record "dalton-embedded" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/sdalton" "${d}/embedded.dal" \
    -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "dalton-64i")
  submit_and_record "dalton-64i" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/sdalton" "${d}/hf-h2.dal" "${d}/h2.mol" \
    -m 20 -c 1 -o "${out}" || true

  out=$(make_test_output_dir "dalton-bad-input")
  submit_and_record "dalton-bad-input" "FAILED" "non-zero" "${out}" \
    "${BIN_DIR}/sdalton" "${d}/bad-keyword.dal" "${d}/h2.mol" \
    -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "dalton-oom")
  submit_and_record "dalton-oom" "OUT_OF_MEMORY" "non-zero" "${out}" \
    "${BIN_DIR}/sdalton" "${d}/hf-h2.dal" "${d}/h2.mol" \
    -c 1 -m 1 -o "${out}" || true

  out=$(make_test_output_dir "dalton-timeout")
  submit_and_record "dalton-timeout" "TIMEOUT" "non-zero" "${out}" \
    "${BIN_DIR}/sdalton" "${d}/hf-h2.dal" "${d}/h2.mol" \
    -c 1 -m 2 -t 0:00:05 -o "${out}" || true
}

submit_dirac_tests() {
  should_run "dirac" || return 0
  printf "\nDIRAC tests\n\n"

  local d="${INPUTS}/dirac"
  local out

  out=$(make_test_output_dir "dirac-single")
  submit_and_record "dirac-single" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/sdirac" "${d}/hf-h2.inp" "${d}/h2.mol" \
    -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "dirac-array")
  submit_and_record "dirac-array" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/sdirac" "${d}/hf-h2.inp" "${d}/h2.mol" \
    "${d}/hf-h2.inp" "${d}/h2.mol" \
    -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "dirac-bad-input")
  submit_and_record "dirac-bad-input" "FAILED" "non-zero" "${out}" \
    "${BIN_DIR}/sdirac" "${d}/bad-method.inp" "${d}/h2.mol" \
    -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "dirac-oom")
  submit_and_record "dirac-oom" "OUT_OF_MEMORY" "non-zero" "${out}" \
    "${BIN_DIR}/sdirac" "${d}/hf-h2.inp" "${d}/h2.mol" \
    -c 1 -m 1 -o "${out}" || true

  out=$(make_test_output_dir "dirac-timeout")
  submit_and_record "dirac-timeout" "TIMEOUT" "non-zero" "${out}" \
    "${BIN_DIR}/sdirac" "${d}/hf-h2.inp" "${d}/h2.mol" \
    -c 1 -m 2 -t 0:00:05 -o "${out}" || true
}

submit_turbomole_tests() {
  should_run "turbomole" || return 0
  printf "\nTurbomole tests\n\n"

  local d="${INPUTS}/turbomole"
  local out

  out=$(make_test_output_dir "turbomole-single")
  submit_and_record "turbomole-single" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/sturbomole" "${d}/happy/control" "${d}/happy/coord" \
    -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "turbomole-bad")
  submit_and_record "turbomole-bad" "FAILED" "non-zero" "${out}" \
    "${BIN_DIR}/sturbomole" "${d}/bad/control" "${d}/bad/coord" \
    -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "turbomole-timeout")
  submit_and_record "turbomole-timeout" "TIMEOUT" "non-zero" "${out}" \
    "${BIN_DIR}/sturbomole" "${d}/happy/control" "${d}/happy/coord" \
    -c 1 -m 2 -t 0:00:05 -o "${out}" || true
}

submit_cfour_tests() {
  should_run "cfour" || return 0
  printf "\nCFOUR tests\n\n"

  local d="${INPUTS}/cfour"
  local out

  out=$(make_test_output_dir "cfour-single")
  submit_and_record "cfour-single" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/scfour" "${d}/hf-h2.inp" \
    -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "cfour-genbas")
  submit_and_record "cfour-genbas" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/scfour" "${d}/hf-h2.inp" \
    -g "${d}/custom-GENBAS" -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "cfour-bad-basis")
  submit_and_record "cfour-bad-basis" "FAILED" "non-zero" "${out}" \
    "${BIN_DIR}/scfour" "${d}/bad-basis.inp" \
    -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "cfour-oom")
  submit_and_record "cfour-oom" "OUT_OF_MEMORY" "non-zero" "${out}" \
    "${BIN_DIR}/scfour" "${d}/hf-h2.inp" \
    -c 1 -m 1 -o "${out}" || true
}

submit_gaussian_tests() {
  should_run "gaussian" || return 0
  printf "\nGaussian tests\n\n"

  local d="${INPUTS}/gaussian"
  local out

  out=$(make_test_output_dir "gaussian-single")
  submit_and_record "gaussian-single" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/sgaussian" "${d}/hf-h2.com" \
    -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "gaussian-gjf")
  submit_and_record "gaussian-gjf" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/sgaussian" "${d}/opt-h2o.gjf" \
    -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "gaussian-bad-route")
  submit_and_record "gaussian-bad-route" "FAILED" "non-zero" "${out}" \
    "${BIN_DIR}/sgaussian" "${d}/bad-route.com" \
    -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "gaussian-oom")
  submit_and_record "gaussian-oom" "OUT_OF_MEMORY" "non-zero" "${out}" \
    "${BIN_DIR}/sgaussian" "${d}/hf-h2.com" \
    -c 1 -m 1 -o "${out}" || true

  out=$(make_test_output_dir "gaussian-timeout")
  submit_and_record "gaussian-timeout" "TIMEOUT" "non-zero" "${out}" \
    "${BIN_DIR}/sgaussian" "${d}/hf-h2.com" \
    -c 1 -m 2 -t 0:00:05 -o "${out}" || true
}

submit_orca_tests() {
  should_run "orca" || return 0
  printf "\nORCA tests\n\n"

  local d="${INPUTS}/orca"
  local out

  out=$(make_test_output_dir "orca-single")
  submit_and_record "orca-single" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/sorca" "${d}/hf-h2.inp" \
    -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "orca-bad")
  submit_and_record "orca-bad" "FAILED" "non-zero" "${out}" \
    "${BIN_DIR}/sorca" "${d}/bad-keyword.inp" \
    -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "orca-oom")
  submit_and_record "orca-oom" "OUT_OF_MEMORY" "non-zero" "${out}" \
    "${BIN_DIR}/sorca" "${d}/hf-h2.inp" \
    -c 1 -m 1 -o "${out}" || true

  out=$(make_test_output_dir "orca-archive")
  submit_and_record "orca-archive" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/sorca" "${d}/hf-h2.inp" \
    -c 1 -m 2 -o "${out}" || true
}

submit_nwchem_tests() {
  should_run "nwchem" || return 0
  printf "\nNWChem tests\n\n"

  local d="${INPUTS}/nwchem"
  local out

  out=$(make_test_output_dir "nwchem-single")
  submit_and_record "nwchem-single" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/snwchem" "${d}/hf-h2.nw" \
    -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "nwchem-bad")
  submit_and_record "nwchem-bad" "FAILED" "non-zero" "${out}" \
    "${BIN_DIR}/snwchem" "${d}/bad-task.nw" \
    -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "nwchem-oom")
  submit_and_record "nwchem-oom" "OUT_OF_MEMORY" "non-zero" "${out}" \
    "${BIN_DIR}/snwchem" "${d}/hf-h2.nw" \
    -c 1 -m 1 -o "${out}" || true
}

submit_molpro_tests() {
  should_run "molpro" || return 0
  printf "\nMolpro tests\n\n"

  local d="${INPUTS}/molpro"
  local out

  out=$(make_test_output_dir "molpro-single")
  submit_and_record "molpro-single" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/smolpro" "${d}/hf-h2.inp" \
    -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "molpro-bad")
  submit_and_record "molpro-bad" "FAILED" "non-zero" "${out}" \
    "${BIN_DIR}/smolpro" "${d}/bad-method.inp" \
    -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "molpro-oom")
  submit_and_record "molpro-oom" "OUT_OF_MEMORY" "non-zero" "${out}" \
    "${BIN_DIR}/smolpro" "${d}/hf-h2.inp" \
    -c 1 -m 1 -o "${out}" || true
}

submit_sharc_tests() {
  should_run "sharc" || return 0
  printf "\nSHARC tests (best-effort)\n\n"

  local d="${INPUTS}/sharc"
  local out

  out=$(make_test_output_dir "sharc-single")
  submit_and_record "sharc-single" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/ssharc" "${d}/minimal.inp" \
    -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "sharc-oom")
  submit_and_record "sharc-oom" "OUT_OF_MEMORY" "non-zero" "${out}" \
    "${BIN_DIR}/ssharc" "${d}/minimal.inp" \
    -c 1 -m 1 -o "${out}" || true
}

submit_xtb_tests() {
  should_run "xtb" || return 0
  printf "\nxTB tests\n\n"

  local d="${INPUTS}/xtb"
  local out

  out=$(make_test_output_dir "xtb-single")
  submit_and_record "xtb-single" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/sxtb" "${d}/h2.xyz" --opt \
    -c 1 -m 0.5 -o "${out}" || true

  out=$(make_test_output_dir "xtb-passthrough")
  submit_and_record "xtb-passthrough" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/sxtb" "${d}/h2o.xyz" --opt --chrg 0 --gfn 2 \
    -c 1 -m 0.5 -o "${out}" || true

  out=$(make_test_output_dir "xtb-omp")
  submit_and_record "xtb-omp" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/sxtb" "${d}/h2.xyz" --omp-threads 2 \
    -c 2 -m 1 -o "${out}" || true

  out=$(make_test_output_dir "xtb-bad")
  submit_and_record "xtb-bad" "FAILED" "non-zero" "${out}" \
    "${BIN_DIR}/sxtb" "${d}/broken.xyz" --opt \
    -c 1 -m 0.5 -o "${out}" || true

  out=$(make_test_output_dir "xtb-array")
  submit_and_record "xtb-array" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/sxtb" "${d}/h2.xyz" "${d}/h2o.xyz" --opt \
    -c 1 -m 0.5 -o "${out}" || true
}

submit_std2_tests() {
  should_run "std2" || return 0
  printf "\nSTD2 tests\n\n"

  local d="${INPUTS}/std2"
  local out

  out=$(make_test_output_dir "std2-molden")
  submit_and_record "std2-molden" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/sstd2" "${d}/h2o.molden" \
    -ax 0.25 -e 10 -c 1 -m 0.5 -o "${out}" || true

  out=$(make_test_output_dir "std2-xtb")
  submit_and_record "std2-xtb" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/sstd2" "${d}/h2.xyz" \
    -e 8 -c 1 -m 0.5 -o "${out}" || true

  out=$(make_test_output_dir "std2-functional")
  submit_and_record "std2-functional" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/sstd2" "${d}/h2o.molden" \
    --PBE0 -c 1 -m 0.5 -o "${out}" || true

  out=$(make_test_output_dir "std2-bad")
  submit_and_record "std2-bad" "FAILED" "non-zero" "${out}" \
    "${BIN_DIR}/sstd2" "${d}/bad.molden" \
    -c 1 -m 0.5 -o "${out}" || true
}

submit_python_tests() {
  should_run "python" || return 0
  printf "\nPython tests\n\n"

  local d="${INPUTS}/python"
  local out

  out=$(make_test_output_dir "python-single")
  submit_and_record "python-single" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/spython" "${d}/hello.py" \
    -c 1 -m 1 -o "${out}" || true

  out=$(make_test_output_dir "python-uv")
  submit_and_record "python-uv" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/spython" "${d}/hello.py" --uv \
    -c 1 -m 1 -o "${out}" || true

  out=$(make_test_output_dir "python-args")
  submit_and_record "python-args" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/spython" "${d}/hello.py" --args "--verbose" \
    -c 1 -m 1 -o "${out}" || true

  out=$(make_test_output_dir "python-crash")
  submit_and_record "python-crash" "FAILED" "non-zero" "${out}" \
    "${BIN_DIR}/spython" "${d}/crash.py" \
    -c 1 -m 1 -o "${out}" || true

  out=$(make_test_output_dir "python-oom")
  submit_and_record "python-oom" "OUT_OF_MEMORY" "non-zero" "${out}" \
    "${BIN_DIR}/spython" "${d}/oom.py" \
    -c 1 -m 1 -o "${out}" || true
}

submit_exec_tests() {
  should_run "exec" || return 0
  printf "\nExec tests\n\n"

  local d="${INPUTS}/exec"
  local out

  out=$(make_test_output_dir "exec-basic")
  submit_and_record "exec-basic" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/sexec" -c 1 -m 1 -o "${out}" \
    -- "${d}/ok.sh" || true

  out=$(make_test_output_dir "exec-mpi")
  submit_and_record "exec-mpi" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/sexec" -c 2 -m 2 --mpi -x "${d}/ok.sh" \
    -o "${out}" || true

  out=$(make_test_output_dir "exec-fail")
  submit_and_record "exec-fail" "FAILED" "non-zero" "${out}" \
    "${BIN_DIR}/sexec" -c 1 -m 1 -o "${out}" \
    -- "${d}/fail.sh" || true
}

submit_cross_cutting_tests() {
  printf "\nCross-cutting tests\n\n"

  local orca_inp="${INPUTS}/orca/hf-h2.inp"
  local out

  out=$(make_test_output_dir "common-no-archive")
  submit_and_record "common-no-archive" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/sorca" "${orca_inp}" --no-archive \
    -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "common-custom-jobname")
  submit_and_record "common-custom-jobname" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/sorca" "${orca_inp}" -j myjob \
    -c 1 -m 2 -o "${out}" || true

  out=$(make_test_output_dir "common-output-dir")
  local custom_out="${RESULTS_DIR}/output/common-output-dir/custom-out/"
  mkdir -p "${custom_out}"
  submit_and_record "common-output-dir" "COMPLETED" "0" "${custom_out}" \
    "${BIN_DIR}/sorca" "${orca_inp}" \
    -c 1 -m 2 -o "${custom_out}" || true

  out=$(make_test_output_dir "common-double-dash")
  submit_and_record "common-double-dash" "COMPLETED" "0" "${out}" \
    "${BIN_DIR}/sexec" -c 1 -m 1 -o "${out}" \
    -- echo hello || true
}

main() {
  check_dependencies
  init_results

  printf "HPC Integration Test Submitter\n"

  run_pre_submission_tests

  submit_dalton_tests
  submit_dirac_tests
  submit_turbomole_tests
  submit_cfour_tests
  submit_gaussian_tests
  submit_orca_tests
  submit_nwchem_tests
  submit_molpro_tests
  submit_sharc_tests
  submit_xtb_tests
  submit_std2_tests
  submit_python_tests
  submit_exec_tests
  submit_cross_cutting_tests

  local job_count
  job_count=$(wc -l <"${MANIFEST_FILE}" 2>/dev/null || printf "0")

  printf "\nSubmitted %s jobs\n" "${job_count}"
  printf "Manifest: %s\n" "${MANIFEST_FILE}"
  printf "Monitor:  squeue -u %s\n" "${USER}"
  printf "Validate: %s/check-results.sh\n" "${SCRIPT_DIR}"
}

main "$@"
