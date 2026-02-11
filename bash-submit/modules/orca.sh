#!/usr/bin/env bash
# Module: Orca quantum chemistry
#
# Category A: simple single-file, scratch + archive + retrieve

MOD_NAME="orca"
MOD_INPUT_EXTENSIONS=(".inp")
MOD_OUTPUT_EXTENSIONS=(".out")
MOD_RETRIEVE_EXTENSIONS=(".xyz" ".nto" ".cube")
MOD_DEFAULT_CPUS=1
MOD_DEFAULT_MEMORY_GB=2
MOD_DEFAULT_THROTTLE=5
MOD_DEFAULT_OUTPUT_DIR="output"
MOD_USES_SCRATCH=true
MOD_USES_ARCHIVE=true
MOD_MEMORY_UNIT="gb"

ORCA_PATH="${ORCA_PATH:-}"
OPENMPI_BIN="${OPENMPI_BIN:-}"
OPENMPI_LIB="${OPENMPI_LIB:-}"

# Print module-specific usage
#
# Returns:
#   0 - Success
mod_print_usage() {
  cat <<'EOF'
 Orca submission

 Examples:
   sorca opt_b3lyp_def2tzvp.inp -c 8 -m 16 -p kemi6
   sorca *.inp --throttle 5 -c 4 -m 8
   sorca -M manifest.txt --throttle 2 -c 4 -m 8 -p chem
EOF
}

# Parse module-specific arguments (none for orca)
#
# Arguments:
#   $@ - Remaining args after common parsing
#
# Returns:
#   0 - Success
mod_parse_args() {
  if [[ $# -gt 0 ]]; then
    die_usage "Unknown option: $1"
  fi
}

# Validate module state after parsing (no extra validation needed)
#
# Returns:
#   0 - Success
mod_validate() {
  return 0
}

# Emit environment setup lines for sbatch
#
# Outputs:
#   Module load / PATH export lines
#
# Returns:
#   0 - Success
mod_emit_dependencies() {
  [[ -n "${ORCA_DEPS:-}" ]] && printf "%s\n" "${ORCA_DEPS}"
}

# Emit run command for sbatch
#
# Arguments:
#   $1 - input: Input file path (may be a variable reference)
#   $2 - stem: Input stem (may be a variable reference)
#
# Outputs:
#   Execution lines
#
# Returns:
#   0 - Success
mod_emit_run_command() {
  local input="$1"
  local stem="$2"
  cat <<EOF
cp "${input}" "\$scratch_directory/${stem}.inp"
${ORCA_PATH}/orca "\$scratch_directory/${stem}.inp" > "\${output_directory}${stem}.out"
EOF
}

# Emit file retrieval lines for sbatch
#
# Arguments:
#   $1 - stem: Input stem (may be a variable reference)
#
# Outputs:
#   Retrieval lines
#
# Returns:
#   0 - Success
mod_emit_retrieve_outputs() {
  local stem="$1"
  cat <<EOF
printf "\n"
for ext in .xyz .nto .cube; do
  while IFS= read -r -d '' file; do
    filename=\$(basename "\$file")
    if mv "\$file" "\${output_directory}\$filename"; then
      printf "Retrieved: %s\n" "\$filename"
    else
      printf "Warning: Failed to retrieve %s\n" "\$filename"
    fi
  done < <(find "\$scratch_directory" -maxdepth 1 -type f \
    -name "*\$ext" -print0)
done
EOF
}

# Compute job name from input file
#
# Arguments:
#   $1 - input_file: Input file path
#
# Outputs:
#   Job name
#
# Returns:
#   0 - Success
mod_job_name() {
  strip_extension "$1" ".inp"
}

# List backup targets for pre-submit backup
#
# Arguments:
#   $1 - stem: Input stem
#   $2 - output_dir: Output directory path
#
# Outputs:
#   File paths to backup
#
# Returns:
#   0 - Success
mod_backup_targets() {
  default_mod_backup_targets "$1" "$2"
}
