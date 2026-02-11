#!/usr/bin/env bash
# Layered configuration loading
#
# Priority (low to high):
#   1. config/defaults.sh (shipped defaults)
#   2. modules/<name>.sh (module interface + fallback exec defaults)
#   3. config/software/<name>.sh (site-specific paths + deps)
#   4. Module MOD_DEFAULT_* variables
#   5. CLI arguments
#
# Sourced by bin/submit; not executable standalone.

# Load per-software config, with optional variant support
#
# When VARIANT is set, loads <module>-<variant>.sh instead of <module>.sh.
# Missing variant file is a fatal error; missing base file is silently ignored.
#
# Globals:
#   SUBMIT_ROOT, MODULE_NAME, VARIANT
#
# Returns:
#   0 - Success (even if base file absent)
#   1 - Missing variant file (exits via die)
load_software_config() {
  local config_file
  if [[ -n "${VARIANT}" ]]; then
    config_file="${SUBMIT_ROOT}/config/software/${MODULE_NAME}-${VARIANT}.sh"
    [[ -f "${config_file}" ]] \
      || die "Variant config not found: ${MODULE_NAME}-${VARIANT}.sh"
  else
    config_file="${SUBMIT_ROOT}/config/software/${MODULE_NAME}.sh"
    [[ -f "${config_file}" ]] || return 0
  fi
  # shellcheck source=/dev/null
  source "${config_file}"
}

# Apply module defaults over shipped defaults
#
# Module variables (MOD_DEFAULT_*) override DEFAULT_* when set.
# Called after module and software config are sourced.
#
# Returns:
#   0 - Success
apply_module_defaults() {
  [[ -n "${MOD_DEFAULT_CPUS:-}" ]] && DEFAULT_CPUS="${MOD_DEFAULT_CPUS}"
  [[ -n "${MOD_DEFAULT_MEMORY_GB:-}" ]] \
    && DEFAULT_MEMORY_GB="${MOD_DEFAULT_MEMORY_GB}"
  [[ -n "${MOD_DEFAULT_THROTTLE:-}" ]] \
    && DEFAULT_THROTTLE="${MOD_DEFAULT_THROTTLE}"
  [[ -n "${MOD_DEFAULT_OUTPUT_DIR:-}" ]] \
    && DEFAULT_OUTPUT_DIR="${MOD_DEFAULT_OUTPUT_DIR}"
  return 0
}

# Initialize runtime globals from defaults (post-config-load)
#
# Sets mutable globals that CLI parsing will later override.
#
# Returns:
#   0 - Success
init_runtime_globals() {
  PARTITION="${DEFAULT_PARTITION}"
  NUM_CPUS="${DEFAULT_CPUS}"
  MEMORY_GB="${DEFAULT_MEMORY_GB}"
  NTASKS="${DEFAULT_NTASKS}"
  NODES="${DEFAULT_NODES}"
  THROTTLE="${DEFAULT_THROTTLE}"
  OUTPUT_DIR="${DEFAULT_OUTPUT_DIR}"
  LOG_EXTENSION="${DEFAULT_LOG_EXTENSION}"

  TIME_LIMIT=""
  CUSTOM_JOB_NAME=""
  NICE_FACTOR=""
  MANIFEST_FILE=""

  VARIANT=""
  EXPORT_FILE=""

  ARRAY_MODE=false

  POSITIONAL_ARGS=()
  REMAINING_ARGS=()
  INPUTS=()
}
