#!/usr/bin/env bash
set -euo pipefail

# Submission tests: submits real jobs for each module via Slurm.
# Run with --mode bash or --mode python.
# Tests: success cases, software error cases, Slurm timeout cases.
#
# Usage:
#   ./tests/submit-tests.sh --mode bash
#   ./tests/submit-tests.sh --mode python
#
# Results go to tests/results-{mode}/ with a job log for tracking.

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
INPUT_DIR="${SCRIPT_DIR}/inputs"

source "${SCRIPT_DIR}/register.sh"

MODE=""
TIMEOUT_LIMIT="0-00:00:30"
SUBMIT_COUNT=0
SKIP_COUNT=0

usage() {
  printf "Usage: %s --mode bash|python [--modules mod1,mod2,...]\n" "$(basename "$0")"
  exit 1
}

FILTER_MODULES=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="$2"; shift 2 ;;
    --modules) FILTER_MODULES="$2"; shift 2 ;;
    *) usage ;;
  esac
done

[[ -n "${MODE}" ]] || usage
[[ "${MODE}" =~ ^(bash|python)$ ]] || usage

RESULTS_DIR="${SCRIPT_DIR}/results-${MODE}"
rm -rf "${RESULTS_DIR}"
mkdir -p "${RESULTS_DIR}"

JOB_LOG="${RESULTS_DIR}/jobs.log"
printf "# Submit test job log -- mode: %s -- %s\n" "${MODE}" "$(date)" > "${JOB_LOG}"
printf "# format: test_name job_id expected_result\n" >> "${JOB_LOG}"

if [[ "${MODE}" == "bash" ]]; then
  export PATH="${PROJECT_ROOT}/bash-submit/bin:${PATH}"
fi

# Check if a module should be tested
#
# Arguments:
#   $1 - module name
#
# Returns:
#   0 if module should be tested
should_test() {
  local mod="$1"
  if [[ -z "${FILTER_MODULES}" ]]; then
    return 0
  fi
  [[ ",${FILTER_MODULES}," == *",${mod},"* ]]
}

# Submit a test job and record the job ID
#
# Arguments:
#   $1 - test name (for logging)
#   $2 - expected result: "success", "software-error", "timeout"
#   $3.. - command to run
submit_test() {
  local test_name="$1"
  local expected="$2"
  shift 2

  local out_dir="${RESULTS_DIR}/${test_name}"
  mkdir -p "${out_dir}"

  printf "Submitting: %s\n" "${test_name}"
  local sbatch_output
  if sbatch_output=$("$@" -o "${out_dir}" 2>&1); then
    local job_id
    job_id=$(printf "%s" "${sbatch_output}" | grep -oP '\d+' | head -1)
    if [[ -n "${job_id}" ]]; then
      printf "%s %s %s\n" "${test_name}" "${job_id}" "${expected}" >> "${JOB_LOG}"
      printf "  Job ID: %s (expected: %s)\n" "${job_id}" "${expected}"
      SUBMIT_COUNT=$((SUBMIT_COUNT + 1))
    else
      printf "  Warning: could not extract job ID from: %s\n" "${sbatch_output}"
      printf "%s UNKNOWN %s\n" "${test_name}" "${expected}" >> "${JOB_LOG}"
    fi
  else
    printf "  Submission failed: %s\n" "${sbatch_output}"
    printf "%s SUBMIT_FAILED %s\n" "${test_name}" "${expected}" >> "${JOB_LOG}"
  fi
}

# Copy input files to a working directory inside results
#
# Arguments:
#   $1 - test name
#   $2.. - source files to copy
#
# Outputs:
#   path to work dir
setup_work_dir() {
  local test_name="$1"
  shift
  local work_dir="${RESULTS_DIR}/${test_name}/work"
  mkdir -p "${work_dir}"
  for src in "$@"; do
    cp "${src}" "${work_dir}/"
  done
  printf "%s" "${work_dir}"
}

# Create temporary variant config for testing
#
# Returns:
#   0 on success
setup_variant_config() {
  if [[ "${MODE}" == "python" ]]; then
    printf '[paths]\norca_path = "/tmp/orca-test-variant"\n' \
      > "${PROJECT_ROOT}/python-submit/config/software/orca-test.toml"
  else
    printf 'ORCA_PATH="/tmp/orca-test-variant"\n' \
      > "${PROJECT_ROOT}/bash-submit/config/software/orca-test.sh"
  fi
}

# Remove temporary variant config
#
# Returns:
#   0 on success
cleanup_variant_config() {
  rm -f "${PROJECT_ROOT}/python-submit/config/software/orca-test.toml"
  rm -f "${PROJECT_ROOT}/bash-submit/config/software/orca-test.sh"
}

# orca tests
if should_test "orca" && command -v sorca >/dev/null 2>&1; then
  printf "\n== orca ==\n"

  wd=$(setup_work_dir "orca-success" "${INPUT_DIR}/orca/hf-h2.inp")
  submit_test "orca-success" "success" sorca "${wd}/hf-h2.inp"

  wd=$(setup_work_dir "orca-bad-keyword" "${INPUT_DIR}/orca/bad-keyword.inp")
  submit_test "orca-bad-keyword" "software-error" sorca "${wd}/bad-keyword.inp"

  wd=$(setup_work_dir "orca-timeout" "${INPUT_DIR}/orca/hf-h2.inp")
  submit_test "orca-timeout" "timeout" sorca "${wd}/hf-h2.inp" -t "${TIMEOUT_LIMIT}"

  # variant test
  setup_variant_config
  wd=$(setup_work_dir "orca-variant" "${INPUT_DIR}/orca/hf-h2.inp")
  submit_test "orca-variant" "software-error" sorca "${wd}/hf-h2.inp" --variant test
  cleanup_variant_config
else
  printf "\nSKIP: orca (not available)\n"
  SKIP_COUNT=$((SKIP_COUNT + 1))
fi

# gaussian tests
if should_test "gaussian" && command -v sgaussian >/dev/null 2>&1; then
  printf "\n== gaussian ==\n"

  wd=$(setup_work_dir "gaussian-success" "${INPUT_DIR}/gaussian/hf-h2.com")
  submit_test "gaussian-success" "success" sgaussian "${wd}/hf-h2.com"

  wd=$(setup_work_dir "gaussian-gjf" "${INPUT_DIR}/gaussian/hf-h2.gjf")
  submit_test "gaussian-gjf" "success" sgaussian "${wd}/hf-h2.gjf"

  wd=$(setup_work_dir "gaussian-bad-route" "${INPUT_DIR}/gaussian/bad-route.com")
  submit_test "gaussian-bad-route" "software-error" sgaussian "${wd}/bad-route.com"

  wd=$(setup_work_dir "gaussian-timeout" "${INPUT_DIR}/gaussian/hf-h2.com")
  submit_test "gaussian-timeout" "timeout" sgaussian "${wd}/hf-h2.com" -t "${TIMEOUT_LIMIT}"
else
  printf "\nSKIP: gaussian (not available)\n"
  SKIP_COUNT=$((SKIP_COUNT + 1))
fi

# dalton tests
if should_test "dalton" && command -v sdalton >/dev/null 2>&1; then
  printf "\n== dalton ==\n"

  wd=$(setup_work_dir "dalton-pair" \
    "${INPUT_DIR}/dalton/hf.dal" "${INPUT_DIR}/dalton/sto-3g-h2.mol")
  submit_test "dalton-pair" "success" sdalton "${wd}/hf.dal" "${wd}/sto-3g-h2.mol"

  wd=$(setup_work_dir "dalton-pot" \
    "${INPUT_DIR}/dalton/hf.dal" "${INPUT_DIR}/dalton/sto-3g-h2.mol" "${INPUT_DIR}/dalton/h2.pot")
  submit_test "dalton-pot" "success" sdalton "${wd}/hf.dal" "${wd}/sto-3g-h2.mol" "${wd}/h2.pot"

  wd=$(setup_work_dir "dalton-embedded" "${INPUT_DIR}/dalton/hf-sto-3g-h2.dal")
  submit_test "dalton-embedded" "success" sdalton "${wd}/hf-sto-3g-h2.dal"

  wd=$(setup_work_dir "dalton-bad-keyword" \
    "${INPUT_DIR}/dalton/bad-keyword.dal" "${INPUT_DIR}/dalton/sto-3g-h2.mol")
  submit_test "dalton-bad-keyword" "software-error" sdalton "${wd}/bad-keyword.dal" "${wd}/sto-3g-h2.mol"

  wd=$(setup_work_dir "dalton-timeout" \
    "${INPUT_DIR}/dalton/hf.dal" "${INPUT_DIR}/dalton/sto-3g-h2.mol")
  submit_test "dalton-timeout" "timeout" sdalton "${wd}/hf.dal" "${wd}/sto-3g-h2.mol" -t "${TIMEOUT_LIMIT}"
else
  printf "\nSKIP: dalton (not available)\n"
  SKIP_COUNT=$((SKIP_COUNT + 1))
fi

# dirac tests
if should_test "dirac" && command -v sdirac >/dev/null 2>&1; then
  printf "\n== dirac ==\n"

  wd=$(setup_work_dir "dirac-success" \
    "${INPUT_DIR}/dirac/hf-h2.inp" "${INPUT_DIR}/dirac/h2.mol")
  submit_test "dirac-success" "success" sdirac "${wd}/hf-h2.inp" "${wd}/h2.mol"

  wd=$(setup_work_dir "dirac-bad-method" \
    "${INPUT_DIR}/dirac/bad-method.inp" "${INPUT_DIR}/dirac/h2.mol")
  submit_test "dirac-bad-method" "software-error" sdirac "${wd}/bad-method.inp" "${wd}/h2.mol"

  wd=$(setup_work_dir "dirac-timeout" \
    "${INPUT_DIR}/dirac/hf-h2.inp" "${INPUT_DIR}/dirac/h2.mol")
  submit_test "dirac-timeout" "timeout" sdirac "${wd}/hf-h2.inp" "${wd}/h2.mol" -t "${TIMEOUT_LIMIT}"
else
  printf "\nSKIP: dirac (not available)\n"
  SKIP_COUNT=$((SKIP_COUNT + 1))
fi

# cfour tests
if should_test "cfour" && command -v scfour >/dev/null 2>&1; then
  printf "\n== cfour ==\n"

  wd=$(setup_work_dir "cfour-success" "${INPUT_DIR}/cfour/hf-h2.inp")
  submit_test "cfour-success" "success" scfour "${wd}/hf-h2.inp"

  wd=$(setup_work_dir "cfour-bad-basis" "${INPUT_DIR}/cfour/bad-basis.inp")
  submit_test "cfour-bad-basis" "software-error" scfour "${wd}/bad-basis.inp"

  wd=$(setup_work_dir "cfour-timeout" "${INPUT_DIR}/cfour/hf-h2.inp")
  submit_test "cfour-timeout" "timeout" scfour "${wd}/hf-h2.inp" -t "${TIMEOUT_LIMIT}"
else
  printf "\nSKIP: cfour (not available)\n"
  SKIP_COUNT=$((SKIP_COUNT + 1))
fi

# molpro tests
if should_test "molpro" && command -v smolpro >/dev/null 2>&1; then
  printf "\n== molpro ==\n"

  wd=$(setup_work_dir "molpro-success" "${INPUT_DIR}/molpro/hf-h2.inp")
  submit_test "molpro-success" "success" smolpro "${wd}/hf-h2.inp"

  wd=$(setup_work_dir "molpro-bad-method" "${INPUT_DIR}/molpro/bad-method.inp")
  submit_test "molpro-bad-method" "software-error" smolpro "${wd}/bad-method.inp"

  wd=$(setup_work_dir "molpro-timeout" "${INPUT_DIR}/molpro/hf-h2.inp")
  submit_test "molpro-timeout" "timeout" smolpro "${wd}/hf-h2.inp" -t "${TIMEOUT_LIMIT}"
else
  printf "\nSKIP: molpro (not available)\n"
  SKIP_COUNT=$((SKIP_COUNT + 1))
fi

# nwchem tests
if should_test "nwchem" && command -v snwchem >/dev/null 2>&1; then
  printf "\n== nwchem ==\n"

  wd=$(setup_work_dir "nwchem-success" "${INPUT_DIR}/nwchem/hf-h2.nw")
  submit_test "nwchem-success" "success" snwchem "${wd}/hf-h2.nw"

  wd=$(setup_work_dir "nwchem-bad-task" "${INPUT_DIR}/nwchem/bad-task.nw")
  submit_test "nwchem-bad-task" "software-error" snwchem "${wd}/bad-task.nw"

  wd=$(setup_work_dir "nwchem-timeout" "${INPUT_DIR}/nwchem/hf-h2.nw")
  submit_test "nwchem-timeout" "timeout" snwchem "${wd}/hf-h2.nw" -t "${TIMEOUT_LIMIT}"
else
  printf "\nSKIP: nwchem (not available)\n"
  SKIP_COUNT=$((SKIP_COUNT + 1))
fi

# xtb tests
if should_test "xtb" && command -v sxtb >/dev/null 2>&1; then
  printf "\n== xtb ==\n"

  wd=$(setup_work_dir "xtb-success" "${INPUT_DIR}/xtb/h2.xyz")
  submit_test "xtb-success" "success" sxtb "${wd}/h2.xyz"

  wd=$(setup_work_dir "xtb-broken" "${INPUT_DIR}/xtb/broken.xyz")
  submit_test "xtb-broken" "software-error" sxtb "${wd}/broken.xyz"

  wd=$(setup_work_dir "xtb-timeout" "${INPUT_DIR}/xtb/h2.xyz")
  submit_test "xtb-timeout" "timeout" sxtb "${wd}/h2.xyz" -t "${TIMEOUT_LIMIT}"
else
  printf "\nSKIP: xtb (not available)\n"
  SKIP_COUNT=$((SKIP_COUNT + 1))
fi

# std2 tests
if should_test "std2" && command -v sstd2 >/dev/null 2>&1; then
  printf "\n== std2 ==\n"

  wd=$(setup_work_dir "std2-molden" "${INPUT_DIR}/std2/h2.molden")
  submit_test "std2-molden" "success" sstd2 "${wd}/h2.molden"

  wd=$(setup_work_dir "std2-xyz" "${INPUT_DIR}/std2/h2.xyz")
  submit_test "std2-xyz" "success" sstd2 "${wd}/h2.xyz"

  wd=$(setup_work_dir "std2-bad" "${INPUT_DIR}/std2/bad.molden")
  submit_test "std2-bad" "software-error" sstd2 "${wd}/bad.molden"

  wd=$(setup_work_dir "std2-timeout" "${INPUT_DIR}/std2/h2.molden")
  submit_test "std2-timeout" "timeout" sstd2 "${wd}/h2.molden" -t "${TIMEOUT_LIMIT}"
else
  printf "\nSKIP: std2 (not available)\n"
  SKIP_COUNT=$((SKIP_COUNT + 1))
fi

# turbomole tests
if should_test "turbomole" && command -v sturbomole >/dev/null 2>&1; then
  printf "\n== turbomole ==\n"

  submit_test "turbomole-success" "success" \
    sturbomole "${INPUT_DIR}/turbomole/hf-h2/control" "${INPUT_DIR}/turbomole/hf-h2/coord"

  submit_test "turbomole-bad" "software-error" \
    sturbomole "${INPUT_DIR}/turbomole/bad/control" "${INPUT_DIR}/turbomole/bad/coord"

  submit_test "turbomole-timeout" "timeout" \
    sturbomole "${INPUT_DIR}/turbomole/hf-h2/control" "${INPUT_DIR}/turbomole/hf-h2/coord" -t "${TIMEOUT_LIMIT}"
else
  printf "\nSKIP: turbomole (not available)\n"
  SKIP_COUNT=$((SKIP_COUNT + 1))
fi

# sharc tests
if should_test "sharc" && command -v ssharc >/dev/null 2>&1; then
  printf "\n== sharc ==\n"

  wd=$(setup_work_dir "sharc-success" "${INPUT_DIR}/sharc/hf-h2.inp")
  submit_test "sharc-success" "success" ssharc "${wd}/hf-h2.inp"

  wd=$(setup_work_dir "sharc-timeout" "${INPUT_DIR}/sharc/hf-h2.inp")
  submit_test "sharc-timeout" "timeout" ssharc "${wd}/hf-h2.inp" -t "${TIMEOUT_LIMIT}"
else
  printf "\nSKIP: sharc (not available)\n"
  SKIP_COUNT=$((SKIP_COUNT + 1))
fi

# python tests
if should_test "python" && command -v spython >/dev/null 2>&1; then
  printf "\n== python ==\n"

  wd=$(setup_work_dir "python-success" "${INPUT_DIR}/python/hello.py")
  submit_test "python-success" "success" spython "${wd}/hello.py"

  wd=$(setup_work_dir "python-crash" "${INPUT_DIR}/python/crash.py")
  submit_test "python-crash" "software-error" spython "${wd}/crash.py"

  wd=$(setup_work_dir "python-timeout" "${INPUT_DIR}/python/hello.py")
  submit_test "python-timeout" "timeout" spython "${wd}/hello.py" -t "${TIMEOUT_LIMIT}"
else
  printf "\nSKIP: python (not available)\n"
  SKIP_COUNT=$((SKIP_COUNT + 1))
fi

# exec tests
if should_test "exec" && command -v sexec >/dev/null 2>&1; then
  printf "\n== exec ==\n"

  wd=$(setup_work_dir "exec-success" "${INPUT_DIR}/exec/ok.sh")
  chmod +x "${wd}/ok.sh"
  submit_test "exec-success" "success" sexec -- "${wd}/ok.sh"

  wd=$(setup_work_dir "exec-fail" "${INPUT_DIR}/exec/fail.sh")
  chmod +x "${wd}/fail.sh"
  submit_test "exec-fail" "software-error" sexec -- "${wd}/fail.sh"

  wd=$(setup_work_dir "exec-timeout" "${INPUT_DIR}/exec/ok.sh")
  chmod +x "${wd}/ok.sh"
  submit_test "exec-timeout" "timeout" sexec -t "${TIMEOUT_LIMIT}" -- "${wd}/ok.sh"
else
  printf "\nSKIP: exec (not available)\n"
  SKIP_COUNT=$((SKIP_COUNT + 1))
fi

printf "\nSubmitted: %d jobs, Skipped: %d modules\n" "${SUBMIT_COUNT}" "${SKIP_COUNT}"
printf "Job log: %s\n" "${JOB_LOG}"
printf "Run parse-results.sh after jobs complete.\n"

register_result "submit" "${MODE}" "${SUBMIT_COUNT}" "0" "${SKIP_COUNT}"
