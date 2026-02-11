#!/usr/bin/env bash
# Input resolution and manifest creation/reading
#
# Provides default single-file implementations. Multi-file modules
# (dalton, dirac, turbomole) override mod_build_jobs, mod_read_manifest,
# and mod_create_exec_manifest.
#
# Sourced by bin/submit; not executable standalone.

# Resolve inputs from CLI positional args or manifest file
#
# Sets INPUTS array and ARRAY_MODE. For single-file modules,
# validates extensions and file existence.
#
# Returns:
#   0 - Success
#   1 - No inputs or invalid (exits via die_usage)
resolve_inputs() {
  if [[ -n "${MANIFEST_FILE}" ]]; then
    validate_file_exists "${MANIFEST_FILE}"
    readarray -t INPUTS < <(
      grep -v '^[[:space:]]*$' "${MANIFEST_FILE}" | sed 's/\r$//'
    )
    ARRAY_MODE=true
  elif [[ ${#POSITIONAL_ARGS[@]} -gt 1 ]]; then
    INPUTS=("${POSITIONAL_ARGS[@]}")
    ARRAY_MODE=true
  elif [[ ${#POSITIONAL_ARGS[@]} -eq 1 ]]; then
    INPUTS=("${POSITIONAL_ARGS[0]}")
    ARRAY_MODE=false
  else
    die_usage "No input files specified"
  fi

  local input_file
  for input_file in "${INPUTS[@]}"; do
    validate_file_exists "${input_file}"
    if [[ ${#MOD_INPUT_EXTENSIONS[@]} -gt 0 ]]; then
      validate_file_extension "${input_file}" "${MOD_INPUT_EXTENSIONS[@]}"
    fi
  done
}

# Create a manifest file from INPUTS array
#
# Arguments:
#   $1 - job_name: Used for default manifest filename
#
# Outputs:
#   Manifest file path on stdout
#
# Returns:
#   0 - Success
create_manifest() {
  local job_name="$1"
  local manifest_path

  if [[ -n "${CUSTOM_JOB_NAME}" ]]; then
    manifest_path="${CUSTOM_JOB_NAME}"
  else
    manifest_path=".${job_name}.manifest"
  fi

  : >"${manifest_path}"
  local input_file
  for input_file in "${INPUTS[@]}"; do
    printf "%s\n" "$(to_absolute_path "${input_file}")" >>"${manifest_path}"
  done
  printf "%s" "${manifest_path}"
}

# Default: compute job name from single input file
#
# Arguments:
#   $1 - input_file: Input file path
#
# Outputs:
#   Job name on stdout
#
# Returns:
#   0 - Success
default_mod_job_name() {
  local input_file="$1"
  local ext
  for ext in "${MOD_INPUT_EXTENSIONS[@]}"; do
    if [[ "${input_file}" == *"${ext}" ]]; then
      strip_extension "${input_file}" "${ext}"
      return 0
    fi
  done
  basename "${input_file}"
}

# Default: list backup targets for pre-submit backup
#
# Arguments:
#   $1 - stem: Input stem (basename without extension)
#   $2 - output_dir: Output directory path
#
# Outputs:
#   File paths to backup, one per line
#
# Returns:
#   0 - Success
default_mod_backup_targets() {
  local stem="$1"
  local output_dir="$2"

  local ext
  for ext in "${MOD_OUTPUT_EXTENSIONS[@]}"; do
    printf "%s\n" "${output_dir}${stem}${ext}"
  done
  printf "%s\n" "${output_dir}${stem}${LOG_EXTENSION}"
  if [[ "${CREATE_ARCHIVE}" == true && "${MOD_USES_ARCHIVE:-false}" == true ]]; then
    printf "%s\n" "${output_dir}${stem}.tar.xz"
  fi
}

# Perform pre-submit backups for all inputs
#
# Returns:
#   0 - Success
backup_all_outputs() {
  if declare -f mod_backup_all >/dev/null 2>&1; then
    mod_backup_all
    return
  fi

  local input_file stem backup_target
  for input_file in "${INPUTS[@]}"; do
    stem=$(mod_job_name "${input_file}")
    while IFS= read -r backup_target; do
      backup_existing_file "${backup_target}"
    done < <(mod_backup_targets "${stem}" "${OUTPUT_DIR}")
  done
}

# Determine job name from inputs and settings
#
# Sets JOB_NAME global.
#
# Returns:
#   0 - Success
determine_job_name() {
  if [[ -n "${CUSTOM_JOB_NAME}" ]]; then
    JOB_NAME="${CUSTOM_JOB_NAME}"
  elif declare -f mod_determine_job_name >/dev/null 2>&1; then
    JOB_NAME=$(mod_determine_job_name)
  elif [[ "${ARRAY_MODE}" == true ]]; then
    JOB_NAME="${MOD_NAME}-array-${#INPUTS[@]}t${THROTTLE}"
  else
    JOB_NAME=$(mod_job_name "${INPUTS[0]}")
  fi
}

# Handle manifest creation for array mode
#
# Sets EXEC_MANIFEST global.
#
# Returns:
#   0 - Success
setup_manifest() {
  EXEC_MANIFEST=""
  if [[ "${ARRAY_MODE}" == true ]]; then
    if declare -f mod_create_exec_manifest >/dev/null 2>&1; then
      EXEC_MANIFEST=$(mod_create_exec_manifest "${JOB_NAME}")
      printf "Created manifest file: %s\n" "${EXEC_MANIFEST}"
    elif [[ -n "${MANIFEST_FILE}" ]]; then
      EXEC_MANIFEST="${MANIFEST_FILE}"
      printf "Using manifest file: %s\n" "${EXEC_MANIFEST}"
    else
      EXEC_MANIFEST=$(create_manifest "${JOB_NAME}")
      printf "Created manifest file: %s\n" "${EXEC_MANIFEST}"
    fi
  fi
}
