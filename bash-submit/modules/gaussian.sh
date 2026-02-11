#!/usr/bin/env bash
# Module: Gaussian 16
#
# Category A: single-file, scratch, retrieve .chk

MOD_NAME="gaussian"
MOD_INPUT_EXTENSIONS=(".com" ".gjf")
MOD_OUTPUT_EXTENSIONS=(".out")
MOD_RETRIEVE_EXTENSIONS=(".chk")
MOD_DEFAULT_CPUS=1
MOD_DEFAULT_MEMORY_GB=2
MOD_DEFAULT_THROTTLE=10
MOD_DEFAULT_OUTPUT_DIR="output"
MOD_USES_SCRATCH=true
MOD_USES_ARCHIVE=false
MOD_MEMORY_UNIT="gb"

GAUSSIAN_EXEC="${GAUSSIAN_EXEC:-g16}"

# Print module-specific usage
#
# Returns:
#   0 - Success
mod_print_usage() {
  cat <<'EOF'
 Gaussian 16 submission

 Examples:
   sgaussian opt_b3lyp_ccpvdz_h2o.com -c 2 -m 4 -t 04:00:00
   sgaussian *.com --throttle 5 -c 2 -m 4
EOF
}

# Parse module-specific arguments
#
# Arguments:
#   $@ - Remaining args
#
# Returns:
#   0 - Success
mod_parse_args() {
  if [[ $# -gt 0 ]]; then
    die_usage "Unknown option: $1"
  fi
}

# Validate module state
#
# Returns:
#   0 - Success
mod_validate() {
  return 0
}

# Emit environment setup
#
# Returns:
#   0 - Success
mod_emit_dependencies() {
  [[ -n "${GAUSSIAN_DEPS:-}" ]] && printf "%s\n" "${GAUSSIAN_DEPS}"
}

# Emit run command
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
  cat <<EOF
export GAUSS_SCRDIR="\$scratch_directory"
srun ${GAUSSIAN_EXEC} "${input}" > "\${output_directory}${stem}.out"
EOF
}

# Emit file retrieval
#
# Arguments:
#   $1 - stem: Input stem
#
# Returns:
#   0 - Success
mod_emit_retrieve_outputs() {
  local stem="$1"
  cat <<EOF
if ls ${stem}.chk 1>/dev/null 2>&1; then
  mv ${stem}.chk "\${output_directory}${stem}.chk"
fi
EOF
}

# Compute job name (strip any extension since .com or .gjf)
#
# Arguments:
#   $1 - input_file: Input file path
#
# Returns:
#   0 - Success
mod_job_name() {
  local base
  base=$(basename "$1")
  printf "%s\n" "${base%.*}"
}

# List backup targets
#
# Arguments:
#   $1 - stem: Input stem
#   $2 - output_dir: Output directory
#
# Returns:
#   0 - Success
mod_backup_targets() {
  local stem="$1"
  local output_dir="$2"
  printf "%s\n" "${output_dir}${stem}.out"
  printf "%s\n" "${output_dir}${stem}${LOG_EXTENSION}"
  printf "%s\n" "${output_dir}${stem}.chk"
}
