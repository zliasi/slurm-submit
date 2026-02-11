#!/usr/bin/env bash
# Module: Turbomole
#
# Category C: multi-file (control/coord pairs), runtime backup

MOD_NAME="turbomole"
MOD_INPUT_EXTENSIONS=()
MOD_OUTPUT_EXTENSIONS=(".out")
MOD_RETRIEVE_EXTENSIONS=()
MOD_DEFAULT_CPUS=1
MOD_DEFAULT_MEMORY_GB=2
MOD_DEFAULT_THROTTLE=10
MOD_DEFAULT_OUTPUT_DIR="output"
MOD_USES_SCRATCH=false
MOD_USES_ARCHIVE=false
MOD_MEMORY_UNIT="gb"

JOBS=()

mod_print_usage() {
  cat <<'EOF'
 Turbomole submission (control/coord pairs)

 Usage:
   sturbomole control coord [control2 coord2 ...] [options]
   sturbomole -M FILE [options]

 Examples:
   sturbomole dft_opt/control dft_opt/coord -c 4 -m 8
   sturbomole opt1/control opt1/coord opt2/control opt2/coord -T 5
   sturbomole -M manifest.txt -c 2 -m 4
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
  [[ -n "${TURBOMOLE_DEPS:-}" ]] && printf "%s\n" "${TURBOMOLE_DEPS}"
}

# Build jobs from positional tokens or manifest
#
# Arguments:
#   $@ - Positional arguments (control coord pairs)
#
# Returns:
#   0 - Success
mod_build_jobs() {
  JOBS=()

  if [[ -n "${MANIFEST_FILE}" ]]; then
    _turbomole_read_manifest "${MANIFEST_FILE}"
  else
    _turbomole_build_from_tokens "$@"
  fi

  (( ${#JOBS[@]} >= 1 )) \
    || die_usage "No jobs assembled (check inputs)"

  INPUTS=("${JOBS[@]}")
  if (( ${#JOBS[@]} > 1 )); then
    ARRAY_MODE=true
  else
    ARRAY_MODE=false
  fi
}

# Parse control/coord pairs from positional tokens
#
# Arguments:
#   $@ - File tokens
#
# Returns:
#   0 - Success
_turbomole_build_from_tokens() {
  local -a tokens=("$@")
  local current_control=""
  local tok

  for tok in "${tokens[@]}"; do
    if [[ "${tok}" == *control ]]; then
      validate_file_exists "${tok}"
      current_control="${tok}"
    elif [[ "${tok}" == *coord ]]; then
      validate_file_exists "${tok}"
      [[ -n "${current_control}" ]] \
        || die_usage "Coord file without preceding control: ${tok}"
      JOBS+=("$(to_absolute_path "${current_control}")"$'\t'"$(to_absolute_path "${tok}")")
    else
      die_usage "Unsupported file (expect *control or *coord): ${tok}"
    fi
  done

  [[ ${#JOBS[@]} -gt 0 ]] \
    || die_usage "No control/coord pairs specified"
}

# Read tab-separated control/coord manifest
#
# Arguments:
#   $1 - file: Manifest file path
#
# Returns:
#   0 - Success
_turbomole_read_manifest() {
  local file="$1"
  validate_file_exists "${file}"
  local line

  while IFS= read -r line || [[ -n "${line}" ]]; do
    line="${line%$'\r'}"
    [[ "${line}" =~ ^[[:space:]]*$ ]] && continue
    [[ "${line}" =~ ^[[:space:]]*# ]] && continue

    local control coord
    IFS=$'\t' read -r control coord <<< "${line}"

    [[ -f "${control}" ]] \
      || die_usage "Invalid control in manifest: ${control}"
    [[ -f "${coord}" ]] \
      || die_usage "Invalid coord in manifest: ${coord}"

    JOBS+=("${control}"$'\t'"${coord}")
  done < "${file}"
}

# Write tab-separated manifest from JOBS array
#
# Arguments:
#   $1 - job_name: Used for manifest filename
#
# Outputs:
#   Manifest file path on stdout
#
# Returns:
#   0 - Success
mod_create_exec_manifest() {
  local job_name="$1"
  local manifest_path=".${job_name}.manifest"
  : >"${manifest_path}"
  local j
  for j in "${JOBS[@]}"; do
    printf "%s\n" "${j}" >>"${manifest_path}"
  done
  printf "%s" "${manifest_path}"
}

# Compute job name from JOBS array
#
# Outputs:
#   Job name on stdout
#
# Returns:
#   0 - Success
mod_determine_job_name() {
  if [[ "${ARRAY_MODE}" == true ]]; then
    printf "%s-array-%dt%s" "${MOD_NAME}" "${#JOBS[@]}" "${THROTTLE}"
  else
    local control coord
    IFS=$'\t' read -r control coord <<< "${JOBS[0]}"
    local control_base
    control_base=$(basename "${control}")
    printf "%s" "${control_base%.*}"
  fi
}

mod_job_name() {
  local base
  base=$(basename "$1")
  printf "%s" "${base%.*}"
}

# Backup all outputs for JOBS array
#
# Returns:
#   0 - Success
mod_backup_all() {
  local line control coord stem
  for line in "${JOBS[@]}"; do
    IFS=$'\t' read -r control coord <<< "${line}"
    stem=$(basename "${control}")
    stem="${stem%.*}"
    backup_existing_file "${OUTPUT_DIR}${stem}.out"
    backup_existing_file "${OUTPUT_DIR}${stem}${LOG_EXTENSION}"
  done
}

mod_backup_targets() {
  local stem="$1"
  local output_dir="$2"
  printf "%s\n" "${output_dir}${stem}.out"
  printf "%s\n" "${output_dir}${stem}${LOG_EXTENSION}"
}

mod_emit_run_command() {
  return 0
}

mod_emit_retrieve_outputs() {
  return 0
}

# Emit array job body for turbomole
#
# Returns:
#   0 - Success
mod_generate_array_body() {
  local exec_manifest
  exec_manifest=$(to_absolute_path "${EXEC_MANIFEST}")
  local time_display="${TIME_LIMIT:-default (partition max)}"
  local mem_per_cpu="$((MEMORY_GB / NUM_CPUS))"

  printf "\n"
  emit_backup_function_inline

  cat <<EOF

line=\$(sed -n "\${SLURM_ARRAY_TASK_ID}p" "${exec_manifest}")
IFS=\$'\\t' read -r CONTROL COORD <<< "\$line"

control_dir=\$(dirname "\$CONTROL")
control_base=\$(basename "\$CONTROL")
stem="\${control_base%.*}"

output_file="${OUTPUT_DIR}\${stem}.out"
log_file="${OUTPUT_DIR}\${stem}${LOG_EXTENSION}"

exec 1>"\$log_file" 2>&1

printf "Job information\\n"
printf "Job name:      %s\\n"   "${JOB_NAME}"
printf "Job ID:        %s_%s\\n" "\$SLURM_ARRAY_JOB_ID" "\$SLURM_ARRAY_TASK_ID"
printf "Output file:   %s\\n"   "\$output_file"
printf "Compute node:  %s\\n"   "\$(hostname)"
printf "Partition:     %s\\n"   "${PARTITION}"
printf "CPU cores:     %s\\n"   "${NUM_CPUS}"
printf "Memory:        %s GB (%s GB per CPU core)\\n" "${MEMORY_GB}" "${mem_per_cpu}"
printf "Time limit:    %s\\n"   "${time_display}"
printf "Submitted by:  %s\\n"   "\${USER:-}"
printf "Submitted on:  %s\\n"   "\$(date)"

backup_existing_files "\$output_file"

export PARNODES="${NUM_CPUS}"
export OMP_NUM_THREADS="${NUM_CPUS}"

cd "\$control_dir" || exit 1

dscf > "\$output_file" 2>&1 && EXIT_CODE=0 || EXIT_CODE=\$?
EOF

  emit_job_footer true

  cat <<'EOF'

exit $EXIT_CODE
EOF
}

# Emit single job body for turbomole
#
# Returns:
#   0 - Success
mod_generate_single_body() {
  local control coord
  IFS=$'\t' read -r control coord <<< "${JOBS[0]}"
  local control_base
  control_base=$(basename "${control}")
  local stem="${control_base%.*}"
  local time_display="${TIME_LIMIT:-default (partition max)}"
  local mem_per_cpu="$((MEMORY_GB / NUM_CPUS))"

  printf "\n"
  emit_backup_function_inline

  cat <<EOF

control_dir=\$(dirname "${control}")
stem="${stem}"

output_file="${OUTPUT_DIR}\${stem}.out"

printf "Job information\\n"
printf "Job name:      %s\\n"   "\${SLURM_JOB_NAME:-${JOB_NAME}}"
printf "Job ID:        %s\\n"   "\${SLURM_JOB_ID:-}"
printf "Output file:   %s\\n"   "\$output_file"
printf "Compute node:  %s\\n"   "\$(hostname)"
printf "Partition:     %s\\n"   "${PARTITION}"
printf "CPU cores:     %s\\n"   "${NUM_CPUS}"
printf "Memory:        %s GB (%s GB per CPU core)\\n" "${MEMORY_GB}" "${mem_per_cpu}"
printf "Time limit:    %s\\n"   "${time_display}"
printf "Submitted by:  %s\\n"   "\${USER:-}"
printf "Submitted on:  %s\\n"   "\$(date)"

backup_existing_files "\$output_file"

export PARNODES="${NUM_CPUS}"
export OMP_NUM_THREADS="${NUM_CPUS}"

cd "\$control_dir" || exit 1

dscf > "\$output_file" 2>&1 && EXIT_CODE=0 || EXIT_CODE=\$?
EOF

  emit_job_footer false

  cat <<'EOF'

exit $EXIT_CODE
EOF
}
