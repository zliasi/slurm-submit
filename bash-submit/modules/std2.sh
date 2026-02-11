#!/usr/bin/env bash
# Module: STD2 (simplified TD-DFT)
#
# Category B: passthrough args, dual mode (molden vs xtb), float memory

MOD_NAME="std2"
MOD_INPUT_EXTENSIONS=(".molden" ".molden.inp" ".xyz" ".coord")
MOD_OUTPUT_EXTENSIONS=(".out")
MOD_RETRIEVE_EXTENSIONS=()
MOD_DEFAULT_CPUS=1
MOD_DEFAULT_MEMORY_GB="0.5"
MOD_DEFAULT_THROTTLE=10
MOD_DEFAULT_OUTPUT_DIR="."
MOD_USES_SCRATCH=false
MOD_USES_ARCHIVE=false
MOD_MEMORY_UNIT="gb_float"

STD2_EXEC="${STD2_EXEC:-std2}"
XTB4STDA_EXEC="${XTB4STDA_EXEC:-xtb4stda}"

STD2_OPTIONS=()
STD2_AX="${STD2_DEFAULT_AX:-0.25}"
STD2_ENERGY="${STD2_DEFAULT_E:-7.0}"
STD2_STY=3
STD2_XTB_MODE=false
STD2_MOLDEN_MODE=false
USE_SPECTRUM=false

mod_print_usage() {
  cat <<'EOF'
 STD2 submission (Molden + xTB modes)

 Molden options:
   -ax FLOAT            Fock exchange (default: 0.25)
   -e FLOAT             Energy threshold eV (default: 7.0)
   -sty INT             Molden style (default: 3)
   --PBE0, --B3LYP, --CAMB3LYP, --wB97XD2, --wB97XD3
   --wB97MV, --SRC2R1, --SRC2R2
   -rpa, -t, -vectm N, -nto N, -sf, -oldtda

 xTB mode (auto for .xyz/.coord):
   -e FLOAT, -rpa

 Other:
   --spectrum           Run g_spec after completion

 Examples:
   sstd2 molecule.molden -ax 0.25 -e 10
   sstd2 geometry.xyz -e 8 -rpa
   sstd2 *.molden --PBE0 --throttle 5
EOF
}

# Parse module-specific + std2 passthrough args
#
# Arguments:
#   $@ - Remaining args
#
# Returns:
#   0 - Success
mod_parse_args() {
  local args=("$@")
  STD2_OPTIONS=()
  local i=0
  while [[ ${i} -lt ${#args[@]} ]]; do
    case "${args[i]}" in
      -ax)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        STD2_AX="${args[i+1]}"; i=$((i + 1)) ;;
      -e)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        STD2_ENERGY="${args[i+1]}"; i=$((i + 1)) ;;
      -sty)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        STD2_STY="${args[i+1]}"; i=$((i + 1)) ;;
      --PBE0)
        STD2_AX="0.25" ;;
      --B3LYP)
        STD2_AX="0.20" ;;
      --CAMB3LYP)
        STD2_OPTIONS+=("-CAMB3LYP") ;;
      --wB97XD2)
        STD2_OPTIONS+=("-wB97XD2") ;;
      --wB97XD3)
        STD2_OPTIONS+=("-wB97XD3") ;;
      --wB97MV)
        STD2_OPTIONS+=("-wB97MV") ;;
      --SRC2R1)
        STD2_OPTIONS+=("-SRC2R1") ;;
      --SRC2R2)
        STD2_OPTIONS+=("-SRC2R2") ;;
      --spectrum)
        USE_SPECTRUM=true ;;
      -*)
        STD2_OPTIONS+=("${args[i]}") ;;
      *)
        die_usage "Unknown positional arg: ${args[i]}" ;;
    esac
    i=$((i + 1))
  done
}

mod_validate() {
  validate_positive_number "${STD2_AX}" "Fock exchange"
  validate_positive_number "${STD2_ENERGY}" "energy threshold"

  local input_file
  for input_file in "${INPUTS[@]:-}"; do
    if [[ "${input_file}" =~ \.(molden|molden\.inp)$ ]]; then
      STD2_MOLDEN_MODE=true
    elif [[ "${input_file}" =~ \.(xyz|coord)$ ]]; then
      STD2_XTB_MODE=true
    fi
  done

  if [[ "${STD2_MOLDEN_MODE}" == true \
    && "${STD2_XTB_MODE}" == true ]]; then
    die_usage "Cannot mix Molden and xTB files in same job"
  fi
}

mod_emit_dependencies() {
  [[ -n "${STD2_DEPS:-}" ]] && printf "%s\n" "${STD2_DEPS}"
}

# Emit std2 run command (branches on molden vs xtb mode)
#
# Arguments:
#   $1 - input: Input file path
#   $2 - stem: Input stem
#
# Returns:
#   0 - Success
mod_emit_run_command() {
  local input="$1"
  local stem="$2"
  local opts_str=""
  [[ ${#STD2_OPTIONS[@]} -gt 0 ]] && opts_str="${STD2_OPTIONS[*]}"

  printf "export OMP_NUM_THREADS=%s\n" "${NUM_CPUS}"
  printf "export MKL_NUM_THREADS=%s\n" "${NUM_CPUS}"
  printf "\ncd \"%s\"\n" "${OUTPUT_DIR}"

  if [[ "${STD2_XTB_MODE}" == true ]]; then
    cat <<EOF
${XTB4STDA_EXEC} "${input}" > "${stem}.xtb.out" 2>&1
if [[ -f wfn.xtb ]]; then
  ${STD2_EXEC} -xtb -e ${STD2_ENERGY} ${opts_str} > "${stem}.out" 2>&1
  mv wfn.xtb "${stem}.wfn.xtb"
else
  printf "Error: xtb4stda failed to generate wfn.xtb\n"
  exit 1
fi
EOF
  else
    cat <<EOF
${STD2_EXEC} -f "${input}" -sty ${STD2_STY} \\
  -ax ${STD2_AX} -e ${STD2_ENERGY} ${opts_str} > "${stem}.out" 2>&1
EOF
  fi
}

mod_emit_retrieve_outputs() {
  return 0
}

# Strip extension handling multiple possible suffixes
#
# Arguments:
#   $1 - input_file: Input file path
#
# Returns:
#   0 - Success
mod_job_name() {
  local base
  base=$(basename "$1")
  printf "%s\n" "${base%%.*}"
}

mod_backup_targets() {
  local stem="$1"
  local output_dir="$2"
  printf "%s\n" "${output_dir}${stem}.out"
  printf "%s\n" "${output_dir}${stem}${LOG_EXTENSION}"
  printf "%s\n" "${output_dir}${stem}.tda.dat"
}
