#!/usr/bin/env bash
set -euo pipefail

# Export validation: generates --export for each module and checks sbatch directives.
# No Slurm needed. Run with --mode bash or --mode python.
#
# Usage:
#   ./tests/export-validate.sh --mode bash
#   ./tests/export-validate.sh --mode python
#   ./tests/export-validate.sh --mode both

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
INPUT_DIR="${SCRIPT_DIR}/inputs"

source "${SCRIPT_DIR}/register.sh"

MODE=""
PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0

usage() {
  printf "Usage: %s --mode bash|python|both\n" "$(basename "$0")"
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="$2"; shift 2 ;;
    *) usage ;;
  esac
done

[[ -n "${MODE}" ]] || usage
[[ "${MODE}" =~ ^(bash|python|both)$ ]] || usage

log_pass() {
  PASS_COUNT=$((PASS_COUNT + 1))
  printf "  PASS: %s\n" "$1"
}

log_fail() {
  FAIL_COUNT=$((FAIL_COUNT + 1))
  printf "  FAIL: %s -- %s\n" "$1" "$2"
}

log_skip() {
  SKIP_COUNT=$((SKIP_COUNT + 1))
  printf "  SKIP: %s -- %s\n" "$1" "$2"
}

# Check that a string appears in a file
#
# Arguments:
#   $1 - test label
#   $2 - file path
#   $3 - expected string
assert_contains() {
  local label="$1"
  local filepath="$2"
  local expected="$3"

  if grep -qF "${expected}" "${filepath}" 2>/dev/null; then
    log_pass "${label}"
  else
    log_fail "${label}" "expected '${expected}'"
  fi
}

# Check that a string does NOT appear in a file
#
# Arguments:
#   $1 - test label
#   $2 - file path
#   $3 - unexpected string
assert_not_contains() {
  local label="$1"
  local filepath="$2"
  local unexpected="$3"

  if grep -qF "${unexpected}" "${filepath}" 2>/dev/null; then
    log_fail "${label}" "unexpected '${unexpected}'"
  else
    log_pass "${label}"
  fi
}

# Run export validation for a single mode
#
# Arguments:
#   $1 - mode: "bash" or "python"
run_mode() {
  local mode="$1"
  local out_dir="${SCRIPT_DIR}/export-${mode}"
  rm -rf "${out_dir}"
  mkdir -p "${out_dir}"

  printf "\nMode: %s\n" "${mode}"

  if [[ "${mode}" == "bash" ]]; then
    export PATH="${PROJECT_ROOT}/bash-submit/bin:${PATH}"
  fi

  local work_dir
  work_dir=$(mktemp -d)
  trap "rm -rf '${work_dir}'" RETURN

  # orca
  printf "\n[orca]\n"
  if command -v sorca >/dev/null 2>&1; then
    cp "${INPUT_DIR}/orca/hf-h2.inp" "${work_dir}/"
    (cd "${work_dir}" && sorca hf-h2.inp --export "${out_dir}/orca.slurm") 2>&1 || true
    if [[ -f "${out_dir}/orca.slurm" ]]; then
      assert_contains "shebang" "${out_dir}/orca.slurm" "#!/bin/bash"
      assert_contains "sbatch header" "${out_dir}/orca.slurm" "#SBATCH --job-name="
      assert_contains "cpus directive" "${out_dir}/orca.slurm" "#SBATCH --cpus-per-task="
      assert_contains "mem directive" "${out_dir}/orca.slurm" "#SBATCH --mem="
      assert_contains "partition" "${out_dir}/orca.slurm" "#SBATCH --partition="
      assert_contains "export none" "${out_dir}/orca.slurm" "#SBATCH --export=NONE"
      assert_contains "set pipefail" "${out_dir}/orca.slurm" "set -euo pipefail"
      assert_contains "sacct footer" "${out_dir}/orca.slurm" "sacct"
      assert_contains "orca command" "${out_dir}/orca.slurm" "orca"
      assert_contains "scratch setup" "${out_dir}/orca.slurm" "scratch"
    else
      log_fail "orca export" "file not created"
    fi
  else
    log_skip "orca" "sorca not found"
  fi

  # gaussian
  printf "\n[gaussian]\n"
  if command -v sgaussian >/dev/null 2>&1; then
    cp "${INPUT_DIR}/gaussian/hf-h2.com" "${work_dir}/"
    cp "${INPUT_DIR}/gaussian/hf-h2.gjf" "${work_dir}/"
    (cd "${work_dir}" && sgaussian hf-h2.com --export "${out_dir}/gaussian-com.slurm") 2>&1 || true
    (cd "${work_dir}" && sgaussian hf-h2.gjf --export "${out_dir}/gaussian-gjf.slurm") 2>&1 || true
    for ext in com gjf; do
      local f="${out_dir}/gaussian-${ext}.slurm"
      if [[ -f "${f}" ]]; then
        assert_contains "gaussian-${ext} header" "${f}" "#SBATCH --job-name="
        assert_contains "gaussian-${ext} export=NONE" "${f}" "#SBATCH --export=NONE"
      else
        log_fail "gaussian-${ext} export" "file not created"
      fi
    done
  else
    log_skip "gaussian" "sgaussian not found"
  fi

  # dalton: dal+mol pair
  printf "\n[dalton]\n"
  if command -v sdalton >/dev/null 2>&1; then
    cp "${INPUT_DIR}/dalton/hf.dal" "${work_dir}/"
    cp "${INPUT_DIR}/dalton/sto-3g-h2.mol" "${work_dir}/"
    cp "${INPUT_DIR}/dalton/h2.pot" "${work_dir}/"
    cp "${INPUT_DIR}/dalton/hf-sto-3g-h2.dal" "${work_dir}/"
    (cd "${work_dir}" && sdalton hf.dal sto-3g-h2.mol --export "${out_dir}/dalton-pair.slurm") 2>&1 || true
    if [[ -f "${out_dir}/dalton-pair.slurm" ]]; then
      assert_contains "dalton pair header" "${out_dir}/dalton-pair.slurm" "#SBATCH --job-name="
      assert_contains "dalton pair dalton cmd" "${out_dir}/dalton-pair.slurm" "dalton"
    else
      log_fail "dalton pair export" "file not created"
    fi
    (cd "${work_dir}" && sdalton hf.dal sto-3g-h2.mol h2.pot --export "${out_dir}/dalton-pot.slurm") 2>&1 || true
    if [[ -f "${out_dir}/dalton-pot.slurm" ]]; then
      assert_contains "dalton pot header" "${out_dir}/dalton-pot.slurm" "#SBATCH --job-name="
    else
      log_fail "dalton pot export" "file not created"
    fi
    (cd "${work_dir}" && sdalton hf-sto-3g-h2.dal --export "${out_dir}/dalton-embedded.slurm") 2>&1 || true
    if [[ -f "${out_dir}/dalton-embedded.slurm" ]]; then
      assert_contains "dalton embedded header" "${out_dir}/dalton-embedded.slurm" "#SBATCH --job-name="
    else
      log_fail "dalton embedded export" "file not created"
    fi
  else
    log_skip "dalton" "sdalton not found"
  fi

  # dirac: inp+mol pair
  printf "\n[dirac]\n"
  if command -v sdirac >/dev/null 2>&1; then
    cp "${INPUT_DIR}/dirac/hf-h2.inp" "${work_dir}/dirac-hf-h2.inp"
    cp "${INPUT_DIR}/dirac/h2.mol" "${work_dir}/dirac-h2.mol"
    (cd "${work_dir}" && sdirac dirac-hf-h2.inp dirac-h2.mol --export "${out_dir}/dirac.slurm") 2>&1 || true
    if [[ -f "${out_dir}/dirac.slurm" ]]; then
      assert_contains "dirac header" "${out_dir}/dirac.slurm" "#SBATCH --job-name="
    else
      log_fail "dirac export" "file not created"
    fi
  else
    log_skip "dirac" "sdirac not found"
  fi

  # cfour
  printf "\n[cfour]\n"
  if command -v scfour >/dev/null 2>&1; then
    cp "${INPUT_DIR}/cfour/hf-h2.inp" "${work_dir}/cfour-hf-h2.inp"
    (cd "${work_dir}" && scfour cfour-hf-h2.inp --export "${out_dir}/cfour.slurm") 2>&1 || true
    if [[ -f "${out_dir}/cfour.slurm" ]]; then
      assert_contains "cfour header" "${out_dir}/cfour.slurm" "#SBATCH --job-name="
    else
      log_fail "cfour export" "file not created"
    fi
  else
    log_skip "cfour" "scfour not found"
  fi

  # molpro
  printf "\n[molpro]\n"
  if command -v smolpro >/dev/null 2>&1; then
    cp "${INPUT_DIR}/molpro/hf-h2.inp" "${work_dir}/molpro-hf-h2.inp"
    (cd "${work_dir}" && smolpro molpro-hf-h2.inp --export "${out_dir}/molpro.slurm") 2>&1 || true
    if [[ -f "${out_dir}/molpro.slurm" ]]; then
      assert_contains "molpro header" "${out_dir}/molpro.slurm" "#SBATCH --job-name="
    else
      log_fail "molpro export" "file not created"
    fi
  else
    log_skip "molpro" "smolpro not found"
  fi

  # nwchem
  printf "\n[nwchem]\n"
  if command -v snwchem >/dev/null 2>&1; then
    cp "${INPUT_DIR}/nwchem/hf-h2.nw" "${work_dir}/"
    (cd "${work_dir}" && snwchem hf-h2.nw --export "${out_dir}/nwchem.slurm") 2>&1 || true
    if [[ -f "${out_dir}/nwchem.slurm" ]]; then
      assert_contains "nwchem header" "${out_dir}/nwchem.slurm" "#SBATCH --job-name="
    else
      log_fail "nwchem export" "file not created"
    fi
  else
    log_skip "nwchem" "snwchem not found"
  fi

  # xtb
  printf "\n[xtb]\n"
  if command -v sxtb >/dev/null 2>&1; then
    cp "${INPUT_DIR}/xtb/h2.xyz" "${work_dir}/"
    (cd "${work_dir}" && sxtb h2.xyz --export "${out_dir}/xtb.slurm") 2>&1 || true
    if [[ -f "${out_dir}/xtb.slurm" ]]; then
      assert_contains "xtb header" "${out_dir}/xtb.slurm" "#SBATCH --job-name="
    else
      log_fail "xtb export" "file not created"
    fi
  else
    log_skip "xtb" "sxtb not found"
  fi

  # std2 molden mode
  printf "\n[std2]\n"
  if command -v sstd2 >/dev/null 2>&1; then
    cp "${INPUT_DIR}/std2/h2.molden" "${work_dir}/"
    cp "${INPUT_DIR}/std2/h2.xyz" "${work_dir}/std2-h2.xyz"
    (cd "${work_dir}" && sstd2 h2.molden --export "${out_dir}/std2-molden.slurm") 2>&1 || true
    if [[ -f "${out_dir}/std2-molden.slurm" ]]; then
      assert_contains "std2 molden header" "${out_dir}/std2-molden.slurm" "#SBATCH --job-name="
    else
      log_fail "std2 molden export" "file not created"
    fi
    (cd "${work_dir}" && sstd2 std2-h2.xyz --export "${out_dir}/std2-xyz.slurm") 2>&1 || true
    if [[ -f "${out_dir}/std2-xyz.slurm" ]]; then
      assert_contains "std2 xyz header" "${out_dir}/std2-xyz.slurm" "#SBATCH --job-name="
    else
      log_fail "std2 xyz export" "file not created"
    fi
  else
    log_skip "std2" "sstd2 not found"
  fi

  # turbomole
  printf "\n[turbomole]\n"
  if command -v sturbomole >/dev/null 2>&1; then
    (cd "${work_dir}" && sturbomole "${INPUT_DIR}/turbomole/hf-h2/control" "${INPUT_DIR}/turbomole/hf-h2/coord" --export "${out_dir}/turbomole.slurm") 2>&1 || true
    if [[ -f "${out_dir}/turbomole.slurm" ]]; then
      assert_contains "turbomole header" "${out_dir}/turbomole.slurm" "#SBATCH --job-name="
    else
      log_fail "turbomole export" "file not created"
    fi
  else
    log_skip "turbomole" "sturbomole not found"
  fi

  # sharc
  printf "\n[sharc]\n"
  if command -v ssharc >/dev/null 2>&1; then
    cp "${INPUT_DIR}/sharc/hf-h2.inp" "${work_dir}/sharc-hf-h2.inp"
    (cd "${work_dir}" && ssharc sharc-hf-h2.inp --export "${out_dir}/sharc.slurm") 2>&1 || true
    if [[ -f "${out_dir}/sharc.slurm" ]]; then
      assert_contains "sharc header" "${out_dir}/sharc.slurm" "#SBATCH --job-name="
    else
      log_fail "sharc export" "file not created"
    fi
  else
    log_skip "sharc" "ssharc not found"
  fi

  # python
  printf "\n[python]\n"
  if command -v spython >/dev/null 2>&1; then
    cp "${INPUT_DIR}/python/hello.py" "${work_dir}/"
    (cd "${work_dir}" && spython hello.py --export "${out_dir}/python.slurm") 2>&1 || true
    if [[ -f "${out_dir}/python.slurm" ]]; then
      assert_contains "python header" "${out_dir}/python.slurm" "#SBATCH --job-name="
      assert_contains "python cmd" "${out_dir}/python.slurm" "python"
    else
      log_fail "python export" "file not created"
    fi
  else
    log_skip "python" "spython not found"
  fi

  # exec
  printf "\n[exec]\n"
  if command -v sexec >/dev/null 2>&1; then
    cp "${INPUT_DIR}/exec/ok.sh" "${work_dir}/"
    chmod +x "${work_dir}/ok.sh"
    (cd "${work_dir}" && sexec --export "${out_dir}/exec.slurm" -- ./ok.sh) 2>&1 || true
    if [[ -f "${out_dir}/exec.slurm" ]]; then
      assert_contains "exec header" "${out_dir}/exec.slurm" "#SBATCH --job-name="
      assert_contains "exec cmd" "${out_dir}/exec.slurm" "ok.sh"
    else
      log_fail "exec export" "file not created"
    fi
  else
    log_skip "exec" "sexec not found"
  fi

  # --variant test: create temp orca-test config, validate, remove
  printf "\n[variant]\n"
  if command -v sorca >/dev/null 2>&1; then
    local variant_toml variant_sh
    if [[ "${mode}" == "python" ]]; then
      variant_toml="${PROJECT_ROOT}/python-submit/config/software/orca-test.toml"
      printf '[paths]\norca_path = "/tmp/orca-test"\n' > "${variant_toml}"
      cp "${INPUT_DIR}/orca/hf-h2.inp" "${work_dir}/variant-test.inp"
      (cd "${work_dir}" && sorca variant-test.inp --variant test --export "${out_dir}/variant.slurm") 2>&1 || true
      rm -f "${variant_toml}"
    else
      variant_sh="${PROJECT_ROOT}/bash-submit/config/software/orca-test.sh"
      printf 'ORCA_PATH="/tmp/orca-test"\n' > "${variant_sh}"
      cp "${INPUT_DIR}/orca/hf-h2.inp" "${work_dir}/variant-test.inp"
      (cd "${work_dir}" && sorca variant-test.inp --variant test --export "${out_dir}/variant.slurm") 2>&1 || true
      rm -f "${variant_sh}"
    fi
    if [[ -f "${out_dir}/variant.slurm" ]]; then
      assert_contains "variant paths" "${out_dir}/variant.slurm" "/tmp/orca-test"
      log_pass "variant config loaded"
    else
      log_fail "variant export" "file not created"
    fi

    # missing variant should error
    (cd "${work_dir}" && sorca variant-test.inp --variant nonexistent --export "${out_dir}/variant-missing.slurm" 2>&1) && {
      log_fail "missing variant" "should have errored"
    } || {
      log_pass "missing variant errors"
    }
  else
    log_skip "variant" "sorca not found"
  fi

  # --export default filename test
  printf "\n[export-default]\n"
  if command -v sorca >/dev/null 2>&1; then
    cp "${INPUT_DIR}/orca/hf-h2.inp" "${work_dir}/"
    (cd "${work_dir}" && sorca hf-h2.inp --export) 2>&1 || true
    if [[ -f "${work_dir}/orca.slurm" ]]; then
      log_pass "default export filename orca.slurm"
    else
      log_fail "default export filename" "orca.slurm not created"
    fi
  else
    log_skip "export-default" "sorca not found"
  fi

  # common flags test
  printf "\n[common-flags]\n"
  if command -v sorca >/dev/null 2>&1; then
    cp "${INPUT_DIR}/orca/hf-h2.inp" "${work_dir}/flags-test.inp"
    (cd "${work_dir}" && sorca flags-test.inp -c 8 -m 16 -t 1-00:00:00 -p kemi6 --export "${out_dir}/flags.slurm") 2>&1 || true
    if [[ -f "${out_dir}/flags.slurm" ]]; then
      assert_contains "cpus=8" "${out_dir}/flags.slurm" "--cpus-per-task=8"
      assert_contains "mem=16gb" "${out_dir}/flags.slurm" "--mem=16gb"
      assert_contains "time limit" "${out_dir}/flags.slurm" "--time=1-00:00:00"
      assert_contains "partition kemi6" "${out_dir}/flags.slurm" "--partition=kemi6"
    else
      log_fail "common flags export" "file not created"
    fi
  else
    log_skip "common-flags" "sorca not found"
  fi
}

# Diff bash vs python exported scripts
#
# Arguments: none (uses export-bash/ and export-python/ dirs)
diff_modes() {
  local bash_dir="${SCRIPT_DIR}/export-bash"
  local python_dir="${SCRIPT_DIR}/export-python"

  printf "\n[parity check]\n"
  if [[ ! -d "${bash_dir}" || ! -d "${python_dir}" ]]; then
    printf "  SKIP: need both export-bash/ and export-python/ for parity check\n"
    return
  fi

  local file
  for file in "${bash_dir}"/*.slurm; do
    [[ -f "${file}" ]] || continue
    local name
    name=$(basename "${file}")
    if [[ -f "${python_dir}/${name}" ]]; then
      if diff -q "${file}" "${python_dir}/${name}" >/dev/null 2>&1; then
        log_pass "parity: ${name}"
      else
        printf "  DIFF: %s\n" "${name}"
        diff --unified=1 "${file}" "${python_dir}/${name}" | head -20
        printf "\n"
      fi
    else
      printf "  SKIP: %s missing from python export\n" "${name}"
    fi
  done
}

if [[ "${MODE}" == "both" ]]; then
  run_mode "bash"
  run_mode "python"
  diff_modes
else
  run_mode "${MODE}"
fi

printf "\nResults: %d passed, %d failed, %d skipped\n" \
  "${PASS_COUNT}" "${FAIL_COUNT}" "${SKIP_COUNT}"

if [[ ${FAIL_COUNT} -gt 0 ]]; then
  exit 1
fi

register_result "export-validate" "${MODE}" "${PASS_COUNT}" "${FAIL_COUNT}" "${SKIP_COUNT}"
