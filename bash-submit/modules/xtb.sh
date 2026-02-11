#!/usr/bin/env bash
# Module: xTB (semiempirical quantum chemistry)
#
# Category B: passthrough args, no scratch, float memory

MOD_NAME="xtb"
MOD_INPUT_EXTENSIONS=(".xyz")
MOD_OUTPUT_EXTENSIONS=()
MOD_RETRIEVE_EXTENSIONS=()
MOD_DEFAULT_CPUS=1
MOD_DEFAULT_MEMORY_GB="0.5"
MOD_DEFAULT_THROTTLE=10
MOD_DEFAULT_OUTPUT_DIR="output"
MOD_USES_SCRATCH=false
MOD_USES_ARCHIVE=false
MOD_MEMORY_UNIT="gb_float"

XTB_EXEC="${XTB_EXEC:-xtb}"
XTB_OPTIONS=()
OMP_THREADS=""

mod_print_usage() {
  cat <<'EOF'
 xTB submission

 Module options:
   --omp-threads INT   OMP_NUM_THREADS (default: same as --cpus)

 xTB options (pass through):
   --opt, --md, --chrg INT, --uhf INT, --gfn N, plus any other xtb flags

 Examples:
   sxtb opt.xyz --opt -c 1 -m 0.5
   sxtb *.xyz --opt --throttle 5
EOF
}

# Parse module-specific + passthrough args
#
# Arguments:
#   $@ - Remaining args
#
# Returns:
#   0 - Success
mod_parse_args() {
  local args=("$@")
  XTB_OPTIONS=()
  local i=0
  while [[ ${i} -lt ${#args[@]} ]]; do
    case "${args[i]}" in
      --omp-threads)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        OMP_THREADS="${args[i+1]}"
        i=$((i + 1))
        ;;
      --chrg|--uhf|--gfn|--alpb|--gbsa|--namespace|--input|--copy|--restart)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        XTB_OPTIONS+=("${args[i]}" "${args[i+1]}")
        i=$((i + 1))
        ;;
      *)
        XTB_OPTIONS+=("${args[i]}")
        ;;
    esac
    i=$((i + 1))
  done
}

mod_validate() {
  [[ -z "${OMP_THREADS}" ]] \
    || validate_positive_integer "${OMP_THREADS}" "omp-threads"
}

mod_emit_dependencies() {
  [[ -n "${XTB_DEPS:-}" ]] && printf "%s\n" "${XTB_DEPS}"
}

# Emit xtb run command
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
  local omp="${OMP_THREADS:-${NUM_CPUS}}"
  local opts_str=""
  [[ ${#XTB_OPTIONS[@]} -gt 0 ]] && opts_str="${XTB_OPTIONS[*]}"
  cat <<EOF
export OMP_NUM_THREADS=${omp}
cd "${OUTPUT_DIR}"
${XTB_EXEC} "${input}" ${opts_str} > "${stem}${LOG_EXTENSION}" 2>&1
EOF
}

mod_emit_retrieve_outputs() {
  local stem="$1"
  cat <<EOF
printf "\n"
printf "Retrieving output files:\n"
for file in "${stem}"*.xyz; do
  if [[ -f "\$file" ]]; then
    printf "Retrieved: %s\n" "\$file"
  fi
done
EOF
}

mod_job_name() {
  strip_extension "$1" ".xyz"
}

mod_backup_targets() {
  local stem="$1"
  local output_dir="$2"
  printf "%s\n" "${output_dir}${stem}${LOG_EXTENSION}"
  printf "%s\n" "${output_dir}${stem}.opt.xyz"
  printf "%s\n" "${output_dir}${stem}.md.xyz"
}
