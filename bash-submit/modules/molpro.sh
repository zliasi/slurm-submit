#!/usr/bin/env bash
# Module: Molpro
#
# Category A: single-file, no scratch, module load

MOD_NAME="molpro"
MOD_INPUT_EXTENSIONS=(".inp")
MOD_OUTPUT_EXTENSIONS=(".out")
MOD_RETRIEVE_EXTENSIONS=()
MOD_DEFAULT_CPUS=1
MOD_DEFAULT_MEMORY_GB=2
MOD_DEFAULT_THROTTLE=5
MOD_DEFAULT_OUTPUT_DIR="output"
MOD_USES_SCRATCH=false
MOD_USES_ARCHIVE=false
MOD_MEMORY_UNIT="gb"

MOLPRO_EXEC="${MOLPRO_EXEC:-molpro}"

mod_print_usage() {
  cat <<'EOF'
 Molpro submission

 Examples:
   smolpro opt_dft.inp -c 4 -m 8
   smolpro *.inp --throttle 5 -c 2 -m 4
EOF
}

mod_parse_args() {
  if [[ $# -gt 0 ]]; then
    die_usage "Unknown option: $1"
  fi
}

mod_validate() {
  return 0
}

mod_emit_dependencies() {
  [[ -n "${MOLPRO_DEPS:-}" ]] && printf "%s\n" "${MOLPRO_DEPS}"
}

mod_emit_run_command() {
  local input="$1"
  local stem="$2"
  cat <<EOF
${MOLPRO_EXEC} -n \$SLURM_CPUS_ON_NODE "${input}" \\
  > "\${output_directory:-}${stem}.out" 2>&1
EOF
}

mod_emit_retrieve_outputs() {
  return 0
}

mod_job_name() {
  strip_extension "$1" ".inp"
}

mod_backup_targets() {
  default_mod_backup_targets "$1" "$2"
}
