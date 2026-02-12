#!/usr/bin/env bash
set -euo pipefail

# Export validation: runs --export for each module and validates content.
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
    --mode)
      MODE="$2"
      shift 2
      ;;
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

  if grep -qF -- "${expected}" "${filepath}" 2>/dev/null; then
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

  if grep -qF -- "${unexpected}" "${filepath}" 2>/dev/null; then
    log_fail "${label}" "unexpected '${unexpected}'"
  else
    log_pass "${label}"
  fi
}

# Validate common sbatch headers present in all exported scripts
#
# Arguments:
#   $1 - label prefix
#   $2 - file path
assert_common_headers() {
  local prefix="$1"
  local filepath="$2"

  assert_contains "${prefix} shebang" "${filepath}" "#!/bin/bash"
  assert_contains "${prefix} job-name" "${filepath}" "#SBATCH --job-name="
  assert_contains "${prefix} cpus" "${filepath}" "#SBATCH --cpus-per-task="
  assert_contains "${prefix} mem" "${filepath}" "#SBATCH --mem="
  assert_contains "${prefix} partition" "${filepath}" "#SBATCH --partition="
  assert_contains "${prefix} export=NONE" "${filepath}" "#SBATCH --export=NONE"
  assert_contains "${prefix} pipefail" "${filepath}" "set -euo pipefail"
  assert_contains "${prefix} omp" "${filepath}" "export OMP_NUM_THREADS="
  assert_contains "${prefix} sacct" "${filepath}" "sacct"
}

# Read a path from module config
#
# Arguments:
#   $1 - module name (e.g. "orca")
#   $2 - bash variable name (e.g. "ORCA_PATH")
#   $3 - toml key (e.g. "orca_path")
config_path() {
  local module="$1" bash_var="$2" toml_key="$3"
  if [[ "${mode}" == "bash" ]]; then
    local cfg="${PROJECT_ROOT}/bash-submit/config/software/${module}.sh"
    [[ -f "${cfg}" ]] || return 1
    # shellcheck disable=SC1090
    (source "${cfg}" && printf '%s' "${!bash_var}")
  else
    local cfg="${PROJECT_ROOT}/python-submit/config/software/${module}.toml"
    [[ -f "${cfg}" ]] || return 1
    grep "^${toml_key} " "${cfg}" | head -1 | cut -d'"' -f2
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
  # shellcheck disable=SC2064
  trap "rm -rf '${work_dir}'" RETURN

  # orca
  printf "\n[orca]\n"
  if command -v sorca >/dev/null 2>&1; then
    cp "${INPUT_DIR}/orca/hf-h2.inp" "${work_dir}/"
    if ! (cd "${work_dir}" && sorca hf-h2.inp \
      --export "${out_dir}/orca.slurm") 2>&1; then
      log_fail "orca export" "command failed"
    fi
    local f="${out_dir}/orca.slurm"
    if [[ -f "${f}" ]]; then
      assert_common_headers "orca" "${f}"
      local orca_path
      orca_path=$(config_path "orca" "ORCA_PATH" "orca_path")
      assert_contains "orca path" "${f}" "${orca_path}"
      assert_contains "orca module purge" "${f}" "module purge"
      assert_contains "orca scratch" "${f}" 'scratch_directory="/scratch/'
      assert_contains "orca archive" "${f}" "tar -cJf"
      assert_contains "orca cleanup" "${f}" 'rm -rf "$scratch_directory"'
      assert_contains "orca job-name val" "${f}" "#SBATCH --job-name=hf-h2"
      assert_contains "orca input ref" "${f}" "hf-h2.inp"
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
    if ! (cd "${work_dir}" && sgaussian hf-h2.com \
      --export "${out_dir}/gaussian-com.slurm") 2>&1; then
      log_fail "gaussian-com export" "command failed"
    fi
    if ! (cd "${work_dir}" && sgaussian hf-h2.gjf \
      --export "${out_dir}/gaussian-gjf.slurm") 2>&1; then
      log_fail "gaussian-gjf export" "command failed"
    fi
    for ext in com gjf; do
      local f="${out_dir}/gaussian-${ext}.slurm"
      if [[ -f "${f}" ]]; then
        assert_common_headers "gaussian-${ext}" "${f}"
        assert_contains "gaussian-${ext} module purge" "${f}" "module purge"
        assert_contains "gaussian-${ext} oneapi" "${f}" "setvars.sh"
        assert_contains "gaussian-${ext} scratch" "${f}" "GAUSS_SCRDIR"
        assert_contains "gaussian-${ext} g16" "${f}" "g16"
        assert_contains "gaussian-${ext} archive" "${f}" "tar -cJf"
        assert_contains "gaussian-${ext} job-name val" "${f}" \
          "#SBATCH --job-name=hf-h2"
        assert_contains "gaussian-${ext} input ref" "${f}" "hf-h2.${ext}"
      else
        log_fail "gaussian-${ext} export" "file not created"
      fi
    done
  else
    log_skip "gaussian" "sgaussian not found"
  fi

  # dalton
  printf "\n[dalton]\n"
  if command -v sdalton >/dev/null 2>&1; then
    cp "${INPUT_DIR}/dalton/hf.dal" "${work_dir}/"
    cp "${INPUT_DIR}/dalton/sto-3g-h2.mol" "${work_dir}/"
    cp "${INPUT_DIR}/dalton/h2.pot" "${work_dir}/"
    cp "${INPUT_DIR}/dalton/hf-sto-3g-h2.dal" "${work_dir}/"
    if ! (cd "${work_dir}" && sdalton hf.dal sto-3g-h2.mol \
      --export "${out_dir}/dalton-pair.slurm") 2>&1; then
      log_fail "dalton-pair export" "command failed"
    fi
    if ! (cd "${work_dir}" && sdalton hf.dal sto-3g-h2.mol h2.pot \
      --export "${out_dir}/dalton-pot.slurm") 2>&1; then
      log_fail "dalton-pot export" "command failed"
    fi
    if ! (cd "${work_dir}" && sdalton hf-sto-3g-h2.dal \
      --export "${out_dir}/dalton-embedded.slurm") 2>&1; then
      log_fail "dalton-embedded export" "command failed"
    fi
    local f="${out_dir}/dalton-pair.slurm"
    if [[ -f "${f}" ]]; then
      assert_common_headers "dalton" "${f}"
      assert_contains "dalton module purge" "${f}" "module purge"
      assert_contains "dalton oneapi" "${f}" "setvars.sh"
      assert_contains "dalton scratch" "${f}" "DALTON_TMPDIR"
      assert_contains "dalton cmd" "${f}" "dalton"
      assert_contains "dalton archive" "${f}" "tar -cJf"
      assert_contains "dalton cleanup" "${f}" 'rm -rf "$DALTON_TMPDIR"'
      assert_contains "dalton job-name val" "${f}" \
        "#SBATCH --job-name=hf_sto-3g-h2"
      assert_contains "dalton input ref dal" "${f}" "hf.dal"
      assert_contains "dalton input ref mol" "${f}" "sto-3g-h2.mol"
    else
      log_fail "dalton-pair export" "file not created"
    fi
    f="${out_dir}/dalton-pot.slurm"
    if [[ -f "${f}" ]]; then
      assert_common_headers "dalton-pot" "${f}"
      assert_contains "dalton-pot cmd" "${f}" "dalton"
      assert_contains "dalton-pot job-name val" "${f}" \
        "#SBATCH --job-name=hf_sto-3g-h2_h2"
      assert_contains "dalton-pot input ref dal" "${f}" "hf.dal"
      assert_contains "dalton-pot input ref mol" "${f}" "sto-3g-h2.mol"
      assert_contains "dalton-pot input ref pot" "${f}" "h2.pot"
    else
      log_fail "dalton-pot export" "file not created"
    fi
    f="${out_dir}/dalton-embedded.slurm"
    if [[ -f "${f}" ]]; then
      assert_common_headers "dalton-embedded" "${f}"
      assert_contains "dalton-embedded cmd" "${f}" "dalton"
      assert_contains "dalton-embedded job-name val" "${f}" \
        "#SBATCH --job-name=hf-sto-3g-h2"
      assert_contains "dalton-embedded input ref" "${f}" "hf-sto-3g-h2.dal"
    else
      log_fail "dalton-embedded export" "file not created"
    fi
  else
    log_skip "dalton" "sdalton not found"
  fi

  # dirac
  printf "\n[dirac]\n"
  if command -v sdirac >/dev/null 2>&1; then
    cp "${INPUT_DIR}/dirac/hf-h2.inp" "${work_dir}/dirac-hf-h2.inp"
    cp "${INPUT_DIR}/dirac/h2.mol" "${work_dir}/dirac-h2.mol"
    if ! (cd "${work_dir}" && sdirac dirac-hf-h2.inp dirac-h2.mol \
      --export "${out_dir}/dirac.slurm") 2>&1; then
      log_fail "dirac export" "command failed"
    fi
    local f="${out_dir}/dirac.slurm"
    if [[ -f "${f}" ]]; then
      assert_common_headers "dirac" "${f}"
      assert_contains "dirac module purge" "${f}" "module purge"
      assert_contains "dirac oneapi" "${f}" "setvars.sh"
      assert_contains "dirac scratch" "${f}" "DIRAC_SCRATCH"
      assert_contains "dirac pam" "${f}" "pam"
      assert_contains "dirac archive" "${f}" "tar -cJf"
      assert_contains "dirac cleanup" "${f}" 'rm -rf "$DIRAC_SCRATCH"'
      assert_contains "dirac job-name val" "${f}" \
        "#SBATCH --job-name=dirac-hf-h2_dirac-h2"
      assert_contains "dirac input ref inp" "${f}" "dirac-hf-h2.inp"
      assert_contains "dirac input ref mol" "${f}" "dirac-h2.mol"
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
    if ! (cd "${work_dir}" && scfour cfour-hf-h2.inp \
      --export "${out_dir}/cfour.slurm") 2>&1; then
      log_fail "cfour export" "command failed"
    fi
    local f="${out_dir}/cfour.slurm"
    if [[ -f "${f}" ]]; then
      assert_common_headers "cfour" "${f}"
      assert_contains "cfour scratch" "${f}" 'scratch_directory="/scratch/'
      assert_contains "cfour path" "${f}" 'CFOUR='
      assert_contains "cfour cmd" "${f}" "xcfour"
      assert_contains "cfour archive" "${f}" "tar -cJf"
      assert_contains "cfour cleanup" "${f}" 'rm -rf "$scratch_directory"'
      assert_contains "cfour job-name val" "${f}" \
        "#SBATCH --job-name=cfour-hf-h2"
      assert_contains "cfour input ref" "${f}" "cfour-hf-h2.inp"
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
    if ! (cd "${work_dir}" && smolpro molpro-hf-h2.inp \
      --export "${out_dir}/molpro.slurm") 2>&1; then
      log_fail "molpro export" "command failed"
    fi
    local f="${out_dir}/molpro.slurm"
    if [[ -f "${f}" ]]; then
      assert_common_headers "molpro" "${f}"
      assert_contains "molpro cmd" "${f}" "molpro"
      assert_contains "molpro job-name val" "${f}" \
        "#SBATCH --job-name=molpro-hf-h2"
      assert_contains "molpro input ref" "${f}" "molpro-hf-h2.inp"
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
    if ! (cd "${work_dir}" && snwchem hf-h2.nw \
      --export "${out_dir}/nwchem.slurm") 2>&1; then
      log_fail "nwchem export" "command failed"
    fi
    local f="${out_dir}/nwchem.slurm"
    if [[ -f "${f}" ]]; then
      assert_common_headers "nwchem" "${f}"
      assert_contains "nwchem cmd" "${f}" "nwchem"
      assert_contains "nwchem job-name val" "${f}" "#SBATCH --job-name=hf-h2"
      assert_contains "nwchem input ref" "${f}" "hf-h2.nw"
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
    if ! (cd "${work_dir}" && sxtb h2.xyz \
      --export "${out_dir}/xtb.slurm") 2>&1; then
      log_fail "xtb export" "command failed"
    fi
    local f="${out_dir}/xtb.slurm"
    if [[ -f "${f}" ]]; then
      assert_common_headers "xtb" "${f}"
      assert_contains "xtb cmd" "${f}" "xtb"
      assert_contains "xtb job-name val" "${f}" "#SBATCH --job-name=h2"
      assert_contains "xtb input ref" "${f}" "h2.xyz"
    else
      log_fail "xtb export" "file not created"
    fi
  else
    log_skip "xtb" "sxtb not found"
  fi

  # std2
  printf "\n[std2]\n"
  if command -v sstd2 >/dev/null 2>&1; then
    cp "${INPUT_DIR}/std2/h2.molden" "${work_dir}/"
    cp "${INPUT_DIR}/std2/h2.xyz" "${work_dir}/std2-h2.xyz"
    if ! (cd "${work_dir}" && sstd2 h2.molden \
      --export "${out_dir}/std2-molden.slurm") 2>&1; then
      log_fail "std2-molden export" "command failed"
    fi
    if ! (cd "${work_dir}" && sstd2 std2-h2.xyz \
      --export "${out_dir}/std2-xyz.slurm") 2>&1; then
      log_fail "std2-xyz export" "command failed"
    fi
    local f="${out_dir}/std2-molden.slurm"
    if [[ -f "${f}" ]]; then
      assert_common_headers "std2" "${f}"
      assert_contains "std2 module purge" "${f}" "module purge"
      assert_contains "std2 omp override" "${f}" "export OMP_NUM_THREADS="
      assert_contains "std2 mkl" "${f}" "MKL_NUM_THREADS="
      assert_contains "std2 cmd" "${f}" "std2"
      assert_contains "std2 job-name val" "${f}" "#SBATCH --job-name=h2"
      assert_contains "std2 input ref" "${f}" "h2.molden"
    else
      log_fail "std2-molden export" "file not created"
    fi
    f="${out_dir}/std2-xyz.slurm"
    if [[ -f "${f}" ]]; then
      assert_common_headers "std2-xyz" "${f}"
      assert_contains "std2-xyz cmd" "${f}" "std2"
      assert_contains "std2-xyz job-name val" "${f}" \
        "#SBATCH --job-name=std2-h2"
      assert_contains "std2-xyz input ref" "${f}" "std2-h2.xyz"
    else
      log_fail "std2-xyz export" "file not created"
    fi
  else
    log_skip "std2" "sstd2 not found"
  fi

  # turbomole
  printf "\n[turbomole]\n"
  if command -v sturbomole >/dev/null 2>&1; then
    if ! (cd "${work_dir}" && sturbomole \
      "${INPUT_DIR}/turbomole/hf-h2/control" \
      "${INPUT_DIR}/turbomole/hf-h2/coord" \
      --export "${out_dir}/turbomole.slurm") 2>&1; then
      log_fail "turbomole export" "command failed"
    fi
    local f="${out_dir}/turbomole.slurm"
    if [[ -f "${f}" ]]; then
      assert_common_headers "turbomole" "${f}"
      assert_contains "turbomole module purge" "${f}" "module purge"
      assert_contains "turbomole turbodir" "${f}" "TURBODIR="
      assert_contains "turbomole parnodes" "${f}" "PARNODES="
      assert_contains "turbomole omp override" "${f}" \
        'export OMP_NUM_THREADS="'
      assert_contains "turbomole dscf" "${f}" "dscf"
      assert_contains "turbomole job-name val" "${f}" \
        "#SBATCH --job-name=control"
      assert_contains "turbomole input ref ctrl" "${f}" "control"
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
    if ! (cd "${work_dir}" && ssharc sharc-hf-h2.inp \
      --export "${out_dir}/sharc.slurm") 2>&1; then
      log_fail "sharc export" "command failed"
    fi
    local f="${out_dir}/sharc.slurm"
    if [[ -f "${f}" ]]; then
      assert_common_headers "sharc" "${f}"
      assert_contains "sharc module purge" "${f}" "module purge"
      assert_contains "sharc oneapi" "${f}" "setvars.sh"
      assert_contains "sharc scratch" "${f}" 'scratch_directory="/scratch/'
      assert_contains "sharc cmd" "${f}" "sharc.x"
      assert_contains "sharc archive" "${f}" "tar -cJf"
      assert_contains "sharc job-name val" "${f}" \
        "#SBATCH --job-name=sharc-hf-h2"
      assert_contains "sharc input ref" "${f}" "sharc-hf-h2.inp"
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
    if ! (cd "${work_dir}" && spython hello.py \
      --export "${out_dir}/python.slurm") 2>&1; then
      log_fail "python export" "command failed"
    fi
    local f="${out_dir}/python.slurm"
    if [[ -f "${f}" ]]; then
      assert_common_headers "python" "${f}"
      assert_contains "python cmd" "${f}" "python3"
      assert_contains "python job-name val" "${f}" "#SBATCH --job-name=hello"
      assert_contains "python input ref" "${f}" "hello.py"
    else
      log_fail "python export" "file not created"
    fi
  else
    log_skip "python" "spython not found"
  fi

  # exec (ok.sh passed as positional arg for resolve_inputs)
  printf "\n[exec]\n"
  if command -v sexec >/dev/null 2>&1; then
    cp "${INPUT_DIR}/exec/ok.sh" "${work_dir}/"
    chmod +x "${work_dir}/ok.sh"
    if ! (cd "${work_dir}" && sexec ok.sh \
      --export "${out_dir}/exec.slurm" -- ./ok.sh) 2>&1; then
      log_fail "exec export" "command failed"
    fi
    local f="${out_dir}/exec.slurm"
    if [[ -f "${f}" ]]; then
      assert_common_headers "exec" "${f}"
      assert_contains "exec cmd" "${f}" "ok.sh"
      assert_contains "exec job-name val" "${f}" "#SBATCH --job-name=ok.sh"
    else
      log_fail "exec export" "file not created"
    fi
  else
    log_skip "exec" "sexec not found"
  fi

  # variant test
  printf "\n[variant]\n"
  if command -v sorca >/dev/null 2>&1; then
    local variant_toml variant_sh
    if [[ "${mode}" == "python" ]]; then
      variant_toml="${PROJECT_ROOT}/python-submit/config/software/orca-test.toml"
      printf '[paths]\norca_path = "/software/kemi/Orca/orca_6_0_1"\n' \
        >"${variant_toml}"
      cp "${INPUT_DIR}/orca/hf-h2.inp" "${work_dir}/variant-test.inp"
      if ! (cd "${work_dir}" && sorca variant-test.inp --variant test \
        --export "${out_dir}/variant.slurm") 2>&1; then
        log_fail "variant export" "command failed"
      fi
      rm -f "${variant_toml}"
    else
      variant_sh="${PROJECT_ROOT}/bash-submit/config/software/orca-test.sh"
      printf 'ORCA_PATH="/software/kemi/Orca/orca_6_0_1"\n' \
        >"${variant_sh}"
      cp "${INPUT_DIR}/orca/hf-h2.inp" "${work_dir}/variant-test.inp"
      if ! (cd "${work_dir}" && sorca variant-test.inp --variant test \
        --export "${out_dir}/variant.slurm") 2>&1; then
        log_fail "variant export" "command failed"
      fi
      rm -f "${variant_sh}"
    fi
    if [[ -f "${out_dir}/variant.slurm" ]]; then
      assert_contains "variant path" "${out_dir}/variant.slurm" \
        "/software/kemi/Orca/orca_6_0_1"
      local default_orca_path
      default_orca_path=$(config_path "orca" "ORCA_PATH" "orca_path")
      assert_not_contains "variant not default" "${out_dir}/variant.slurm" \
        "${default_orca_path}"
    else
      log_fail "variant export" "file not created"
    fi

    # missing variant should error
    (cd "${work_dir}" && sorca variant-test.inp --variant nonexistent \
      --export "${out_dir}/variant-missing.slurm" 2>&1) && {
      log_fail "missing variant" "should have errored"
    } || {
      log_pass "missing variant errors"
    }
  else
    log_skip "variant" "sorca not found"
  fi

  # default export filename test
  printf "\n[export-default]\n"
  if command -v sorca >/dev/null 2>&1; then
    cp "${INPUT_DIR}/orca/hf-h2.inp" "${work_dir}/"
    if ! (cd "${work_dir}" && sorca hf-h2.inp --export) 2>&1; then
      log_fail "export-default orca" "command failed"
    fi
    if [[ -f "${work_dir}/orca.slurm" ]]; then
      log_pass "default filename orca"
    else
      log_fail "default filename orca" "orca.slurm not created"
    fi
    rm -f "${work_dir}/orca.slurm"
  else
    log_skip "export-default orca" "sorca not found"
  fi
  if command -v sgaussian >/dev/null 2>&1; then
    cp "${INPUT_DIR}/gaussian/hf-h2.com" "${work_dir}/"
    if ! (cd "${work_dir}" && sgaussian hf-h2.com --export) 2>&1; then
      log_fail "export-default gaussian" "command failed"
    fi
    if [[ -f "${work_dir}/gaussian.slurm" ]]; then
      log_pass "default filename gaussian"
    else
      log_fail "default filename gaussian" "gaussian.slurm not created"
    fi
    rm -f "${work_dir}/gaussian.slurm"
  else
    log_skip "export-default gaussian" "sgaussian not found"
  fi
  if command -v sxtb >/dev/null 2>&1; then
    cp "${INPUT_DIR}/xtb/h2.xyz" "${work_dir}/"
    if ! (cd "${work_dir}" && sxtb h2.xyz --export) 2>&1; then
      log_fail "export-default xtb" "command failed"
    fi
    if [[ -f "${work_dir}/xtb.slurm" ]]; then
      log_pass "default filename xtb"
    else
      log_fail "default filename xtb" "xtb.slurm not created"
    fi
    rm -f "${work_dir}/xtb.slurm"
  else
    log_skip "export-default xtb" "sxtb not found"
  fi

  # common flags test (-c 8 -m 16 -t -p)
  printf "\n[common-flags]\n"
  if command -v sorca >/dev/null 2>&1; then
    cp "${INPUT_DIR}/orca/hf-h2.inp" "${work_dir}/flags-test.inp"
    if ! (cd "${work_dir}" && sorca flags-test.inp \
      -c 8 -m 16 -t 1-00:00:00 -p kemi6 \
      --export "${out_dir}/flags.slurm") 2>&1; then
      log_fail "common-flags export" "command failed"
    fi
    local f="${out_dir}/flags.slurm"
    if [[ -f "${f}" ]]; then
      assert_contains "flags cpus=8" "${f}" "--cpus-per-task=8"
      assert_contains "flags mem=16gb" "${f}" "--mem=16gb"
      assert_contains "flags time" "${f}" "--time=1-00:00:00"
      assert_contains "flags partition" "${f}" "--partition=kemi6"
      assert_contains "flags omp=1" "${f}" "export OMP_NUM_THREADS=1"
    else
      log_fail "common-flags export" "file not created"
    fi
  else
    log_skip "common-flags" "sorca not found"
  fi

  # omp override test (--omp 4)
  printf "\n[omp-override]\n"
  if command -v sorca >/dev/null 2>&1; then
    cp "${INPUT_DIR}/orca/hf-h2.inp" "${work_dir}/omp-test.inp"
    if ! (cd "${work_dir}" && sorca omp-test.inp -c 8 --omp 4 \
      --export "${out_dir}/omp-override.slurm") 2>&1; then
      log_fail "omp-override export" "command failed"
    fi
    local f="${out_dir}/omp-override.slurm"
    if [[ -f "${f}" ]]; then
      assert_contains "omp=4" "${f}" "export OMP_NUM_THREADS=4"
    else
      log_fail "omp-override export" "file not created"
    fi
  else
    log_skip "omp-override" "sorca not found"
  fi

  # omp cpus test (OpenMP module inherits cpus)
  printf "\n[omp-cpus]\n"
  if command -v sxtb >/dev/null 2>&1; then
    cp "${INPUT_DIR}/xtb/h2.xyz" "${work_dir}/omp-xtb.xyz"
    if ! (cd "${work_dir}" && sxtb omp-xtb.xyz -c 4 \
      --export "${out_dir}/xtb-cpus.slurm") 2>&1; then
      log_fail "omp-cpus export" "command failed"
    fi
    local f="${out_dir}/xtb-cpus.slurm"
    if [[ -f "${f}" ]]; then
      assert_contains "xtb omp=cpus" "${f}" "export OMP_NUM_THREADS=4"
    else
      log_fail "omp-cpus export" "file not created"
    fi
  else
    log_skip "omp-cpus" "sxtb not found"
  fi

  # --no-archive test
  printf "\n[no-archive]\n"
  if command -v sorca >/dev/null 2>&1; then
    cp "${INPUT_DIR}/orca/hf-h2.inp" "${work_dir}/no-archive-test.inp"
    if ! (cd "${work_dir}" && sorca no-archive-test.inp --no-archive \
      --export "${out_dir}/no-archive.slurm") 2>&1; then
      log_fail "no-archive export" "command failed"
    fi
    local f="${out_dir}/no-archive.slurm"
    if [[ -f "${f}" ]]; then
      assert_not_contains "no-archive tar" "${f}" "tar -cJf"
      assert_contains "no-archive msg" "${f}" "Archive creation disabled"
    else
      log_fail "no-archive export" "file not created"
    fi
  else
    log_skip "no-archive" "sorca not found"
  fi

  # -j (custom job name) test
  printf "\n[custom-job-name]\n"
  if command -v sorca >/dev/null 2>&1; then
    cp "${INPUT_DIR}/orca/hf-h2.inp" "${work_dir}/custom-name-test.inp"
    if ! (cd "${work_dir}" && sorca custom-name-test.inp -j my-custom-job \
      --export "${out_dir}/custom-name.slurm") 2>&1; then
      log_fail "custom-job-name export" "command failed"
    fi
    local f="${out_dir}/custom-name.slurm"
    if [[ -f "${f}" ]]; then
      assert_contains "custom name" "${f}" "#SBATCH --job-name=my-custom-job"
    else
      log_fail "custom-job-name export" "file not created"
    fi
  else
    log_skip "custom-job-name" "sorca not found"
  fi

  # -o (output dir) test
  printf "\n[output-dir]\n"
  if command -v sorca >/dev/null 2>&1; then
    cp "${INPUT_DIR}/orca/hf-h2.inp" "${work_dir}/output-dir-test.inp"
    if ! (cd "${work_dir}" && sorca output-dir-test.inp -o results/ \
      --export "${out_dir}/output-dir.slurm") 2>&1; then
      log_fail "output-dir export" "command failed"
    fi
    local f="${out_dir}/output-dir.slurm"
    if [[ -f "${f}" ]]; then
      assert_contains "output dir" "${f}" 'output_directory="results/"'
    else
      log_fail "output-dir export" "file not created"
    fi
  else
    log_skip "output-dir" "sorca not found"
  fi

  # array mode test
  printf "\n[array]\n"
  if command -v sorca >/dev/null 2>&1; then
    cp "${INPUT_DIR}/orca/hf-h2.inp" "${work_dir}/array-h2a.inp"
    cp "${INPUT_DIR}/orca/hf-h2.inp" "${work_dir}/array-h2b.inp"
    if ! (cd "${work_dir}" && sorca array-h2a.inp array-h2b.inp \
      --export "${out_dir}/array.slurm") 2>&1; then
      log_fail "array export" "command failed"
    fi
    local f="${out_dir}/array.slurm"
    if [[ -f "${f}" ]]; then
      assert_contains "array directive" "${f}" "#SBATCH --array=1-2%5"
      assert_contains "array devnull" "${f}" '#SBATCH --output="/dev/null"'
      assert_contains "array task id" "${f}" "SLURM_ARRAY_TASK_ID"
      assert_contains "array sed" "${f}" 'sed -n'
      assert_contains "array job-name" "${f}" \
        "#SBATCH --job-name=orca-array-2t5"
    else
      log_fail "array export" "file not created"
    fi
    rm -f "${work_dir}/.orca-array-2t5.manifest"
  else
    log_skip "array" "sorca not found"
  fi

  # array mode with explicit throttle
  printf "\n[array-throttle]\n"
  if command -v sorca >/dev/null 2>&1; then
    cp "${INPUT_DIR}/orca/hf-h2.inp" "${work_dir}/throttle-a.inp"
    cp "${INPUT_DIR}/orca/hf-h2.inp" "${work_dir}/throttle-b.inp"
    if ! (cd "${work_dir}" && sorca throttle-a.inp throttle-b.inp \
      -T 1 --export "${out_dir}/array-throttle.slurm") 2>&1; then
      log_fail "array-throttle export" "command failed"
    fi
    local f="${out_dir}/array-throttle.slurm"
    if [[ -f "${f}" ]]; then
      assert_contains "throttle directive" "${f}" "#SBATCH --array=1-2%1"
      assert_contains "throttle job-name" "${f}" \
        "#SBATCH --job-name=orca-array-2t1"
    else
      log_fail "array-throttle export" "file not created"
    fi
    rm -f "${work_dir}/.orca-array-2t1.manifest"
  else
    log_skip "array-throttle" "sorca not found"
  fi

  # export to nonexistent parent directory
  printf "\n[export-bad-path]\n"
  if command -v sorca >/dev/null 2>&1; then
    cp "${INPUT_DIR}/orca/hf-h2.inp" "${work_dir}/badpath.inp"
    if (cd "${work_dir}" && sorca badpath.inp \
      --export "${work_dir}/nonexistent/test.slurm") 2>&1; then
      log_fail "export-bad-path" "should have errored"
    else
      log_pass "export-bad-path errors"
    fi
  else
    log_skip "export-bad-path" "sorca not found"
  fi

  # syntax check on all exported files
  printf "\n[syntax]\n"
  for slurm_file in "${out_dir}"/*.slurm; do
    [[ -f "${slurm_file}" ]] || continue
    local name
    name=$(basename "${slurm_file}")
    if bash -n "${slurm_file}" 2>/dev/null; then
      log_pass "syntax: ${name}"
    else
      log_fail "syntax: ${name}" "bash -n failed"
    fi
  done
}

# Diff bash vs python exported scripts
#
# Arguments: none (uses export-bash/ and export-python/ dirs)
diff_modes() {
  local bash_dir="${SCRIPT_DIR}/export-bash"
  local python_dir="${SCRIPT_DIR}/export-python"

  printf "\n[parity check]\n"
  if [[ ! -d "${bash_dir}" || ! -d "${python_dir}" ]]; then
    printf "  SKIP: need both export-bash/ and export-python/\n"
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
        log_fail "parity: ${name}" "bash/python output differs"
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

register_result "export-validate" "${MODE}" \
  "${PASS_COUNT}" "${FAIL_COUNT}" "${SKIP_COUNT}"
