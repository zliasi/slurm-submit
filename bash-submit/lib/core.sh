#!/usr/bin/env bash
# Core utility functions: error handling, validation, path helpers
#
# Sourced by bin/submit; not executable standalone.

# Print error message and exit
#
# Arguments:
#   $1 - message: Error message to display
#
# Exit codes:
#   1 - Always exits with error
die() {
  local message="$1"
  printf "\nError: %s\n" "${message}" >&2
  exit 1
}

# Print error message with usage hint and exit
#
# Arguments:
#   $1 - message: Error message to display
#
# Exit codes:
#   1 - Always exits with error
die_usage() {
  local message="$1"
  printf "\nError: %s\n" "${message}" >&2
  printf "Use: %s -h for help.\n\n" "${PROGRAM_INVOCATION:-submit}" >&2
  exit 1
}

# Validate that a file exists
#
# Arguments:
#   $1 - filepath: Path to validate
#
# Exit codes:
#   0 - File exists
#   1 - File not found
validate_file_exists() {
  local filepath="$1"
  [[ -f "${filepath}" ]] || die_usage "File not found: ${filepath}"
}

# Validate a positive integer value
#
# Arguments:
#   $1 - value: Value to validate
#   $2 - param_name: Parameter name for error messages
#
# Exit codes:
#   0 - Valid
#   1 - Invalid
validate_positive_integer() {
  local value="$1"
  local param_name="$2"
  [[ "${value}" =~ ^[1-9][0-9]*$ ]] \
    || die_usage "Invalid value for ${param_name}: must be positive integer"
}

# Validate a positive number (integer or float)
#
# Arguments:
#   $1 - value: Value to validate
#   $2 - param_name: Parameter name for error messages
#
# Exit codes:
#   0 - Valid
#   1 - Invalid
validate_positive_number() {
  local value="$1"
  local param_name="$2"
  [[ "${value}" =~ ^[0-9]+(\.[0-9]+)?$ && "${value}" != "0" ]] \
    || die_usage "Invalid value for ${param_name}: must be positive number"
}

# Validate SLURM time format (D-HH:MM:SS or HH:MM:SS)
#
# Arguments:
#   $1 - time_str: Time string to validate
#
# Exit codes:
#   0 - Valid or empty
#   1 - Invalid format
validate_time_format() {
  local time_str="$1"
  local time_regex='^([0-9]+-)?([0-1]?[0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$'
  [[ -z "${time_str}" || "${time_str}" =~ ${time_regex} ]] \
    || die_usage "Invalid time format: ${time_str} (use D-HH:MM:SS)"
}

# Validate that a file has an allowed extension
#
# Arguments:
#   $1 - filepath: File path to check
#   $2..N - allowed extensions (e.g. ".inp" ".xyz")
#
# Exit codes:
#   0 - Extension matches
#   1 - Extension not allowed
validate_file_extension() {
  local filepath="$1"
  shift
  local allowed_extensions=("$@")

  [[ ${#allowed_extensions[@]} -eq 0 ]] && return 0

  local ext
  for ext in "${allowed_extensions[@]}"; do
    [[ "${filepath}" == *"${ext}" ]] && return 0
  done
  die_usage "Invalid extension for ${filepath} (expected: ${allowed_extensions[*]})"
}

# Convert a path to absolute
#
# Arguments:
#   $1 - filepath: Path to convert
#
# Outputs:
#   Absolute path on stdout
#
# Returns:
#   0 - Success
to_absolute_path() {
  local filepath="$1"
  if command -v realpath >/dev/null 2>&1; then
    realpath "${filepath}"
  elif command -v readlink >/dev/null 2>&1; then
    readlink -f "${filepath}"
  else
    [[ "${filepath}" = /* ]] && printf "%s\n" "${filepath}" \
      || printf "%s/%s\n" "${PWD}" "${filepath}"
  fi
}

# Strip extension from a filename
#
# Arguments:
#   $1 - filepath: File path
#   $2 - extension: Extension to strip (e.g. ".inp")
#
# Outputs:
#   Basename without extension
#
# Returns:
#   0 - Success
strip_extension() {
  local filepath="$1"
  local extension="$2"
  local base
  base=$(basename "${filepath}")
  printf "%s\n" "${base%"${extension}"}"
}

# Ensure a directory exists, creating it if needed
#
# Arguments:
#   $1 - dir_path: Directory path
#
# Returns:
#   0 - Success
ensure_directory() {
  local dir_path="$1"
  if [[ ! -d "${dir_path}" ]]; then
    mkdir -p "${dir_path}"
    printf "Created directory: %s\n" "${dir_path}"
  fi
}

# Normalize output directory path (ensure trailing slash)
#
# Arguments:
#   $1 - dir_path: Directory path
#
# Outputs:
#   Normalized path with trailing slash
#
# Returns:
#   0 - Success
_require_arg_value() {
  local flag="$1"
  local next_index="$2"
  local array_length="$3"
  (( next_index < array_length )) \
    || die_usage "Option ${flag} requires a value"
}

normalize_output_dir() {
  local dir_path="$1"
  [[ -n "${dir_path}" && "${dir_path: -1}" != "/" ]] \
    && dir_path="${dir_path}/"
  printf "%s" "${dir_path}"
}
