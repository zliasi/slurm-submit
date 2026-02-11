#!/usr/bin/env bash
# Module: DIRAC (relativistic quantum chemistry)
#
# Category C: multi-file (inp/mol pairs), scratch, runtime backup

MOD_NAME="dirac"
MOD_INPUT_EXTENSIONS=()
MOD_OUTPUT_EXTENSIONS=(".out")
MOD_RETRIEVE_EXTENSIONS=()
MOD_DEFAULT_CPUS=1
MOD_DEFAULT_MEMORY_GB=2
MOD_DEFAULT_THROTTLE=10
MOD_DEFAULT_OUTPUT_DIR="output"
MOD_USES_SCRATCH=true
MOD_USES_ARCHIVE=false
MOD_MEMORY_UNIT="gb"

DIRAC_PAM="${DIRAC_PAM:-pam}"
JOBS=()

mod_print_usage() {
  cat <<'EOF'
 DIRAC submission (paired inp/mol)

 Usage:
   sdirac input.inp geom.mol [options]
   sdirac inp1.inp mol1.mol inp2.inp mol2.mol ... [options]
   sdirac -M FILE [options]

 Examples:
   sdirac sp-hf.inp 631g-h2o.mol -c 2 -m 4
   sdirac sp-hf.inp h2o.mol sp-mp2.inp h2o.mol -c 4 -m 8
   sdirac -M manifest.txt -T 5
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
  [[ -n "${DIRAC_DEPS:-}" ]] && printf "%s\n" "${DIRAC_DEPS}"
}

# Build jobs from positional tokens or manifest
#
# Arguments:
#   $@ - Positional arguments (alternating inp mol)
#
# Returns:
#   0 - Success
mod_build_jobs() {
  JOBS=()

  if [[ -n "${MANIFEST_FILE}" ]]; then
    _dirac_read_manifest "${MANIFEST_FILE}"
  else
    _dirac_build_from_tokens "$@"
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

# Parse paired inp/mol tokens
#
# Arguments:
#   $@ - File tokens (alternating inp mol)
#
# Returns:
#   0 - Success
_dirac_build_from_tokens() {
  local -a tokens=("$@")
  local num_tokens=${#tokens[@]}

  if (( num_tokens < 2 )); then
    die_usage "DIRAC requires both an .inp and .mol file"
  fi

  if (( num_tokens % 2 != 0 )); then
    die_usage "Each .inp file must be paired with a .mol file"
  fi

  local i
  for ((i = 0; i < num_tokens; i += 2)); do
    local inp="${tokens[i]}"
    local mol="${tokens[i + 1]}"

    validate_file_exists "${inp}"
    validate_file_exists "${mol}"

    [[ "${inp}" == *.inp ]] \
      || die_usage "Expected .inp file, got: ${inp}"
    [[ "${mol}" == *.mol ]] \
      || die_usage "Expected .mol file, got: ${mol}"

    JOBS+=("$(to_absolute_path "${inp}")"$'\t'"$(to_absolute_path "${mol}")")
  done
}

# Read tab-separated inp/mol manifest
#
# Arguments:
#   $1 - file: Manifest file path
#
# Returns:
#   0 - Success
_dirac_read_manifest() {
  local file="$1"
  validate_file_exists "${file}"
  local line

  while IFS= read -r line || [[ -n "${line}" ]]; do
    line="${line%$'\r'}"
    [[ "${line}" =~ ^[[:space:]]*$ ]] && continue
    [[ "${line}" =~ ^[[:space:]]*# ]] && continue

    local inp mol
    IFS=$'\t' read -r inp mol <<< "${line}"

    [[ "${inp}" == *.inp && -f "${inp}" ]] \
      || die_usage "Invalid INP in manifest: ${inp}"
    [[ "${mol}" == *.mol && -f "${mol}" ]] \
      || die_usage "Invalid MOL in manifest: ${mol}"

    JOBS+=("${inp}"$'\t'"${mol}")
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
    local inp mol
    IFS=$'\t' read -r inp mol <<< "${JOBS[0]}"
    local inp_base mol_base
    inp_base=$(basename "${inp}"); inp_base="${inp_base%.*}"
    mol_base=$(basename "${mol}"); mol_base="${mol_base%.*}"
    printf "%s_%s" "${inp_base}" "${mol_base}"
  fi
}

mod_job_name() {
  strip_extension "$1" ".inp"
}

# Backup all outputs for JOBS array
#
# Returns:
#   0 - Success
mod_backup_all() {
  local line inp mol stem
  for line in "${JOBS[@]}"; do
    IFS=$'\t' read -r inp mol <<< "${line}"
    local inp_base mol_base
    inp_base=$(basename "${inp}"); inp_base="${inp_base%.*}"
    mol_base=$(basename "${mol}"); mol_base="${mol_base%.*}"
    stem="${inp_base}_${mol_base}"
    backup_existing_file "${OUTPUT_DIR}${stem}.out"
    if [[ "${ARRAY_MODE}" == true ]]; then
      backup_existing_file "${OUTPUT_DIR}${stem}${LOG_EXTENSION}"
    fi
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

# Emit array job body for dirac
#
# Returns:
#   0 - Success
mod_generate_array_body() {
  local exec_manifest
  exec_manifest=$(to_absolute_path "${EXEC_MANIFEST}")
  local time_display="${TIME_LIMIT:-default (partition max)}"
  local mem_per_cpu="${MEMORY_GB}"
  local total_mem="$((MEMORY_GB * NUM_CPUS))"

  printf "\n"
  emit_backup_function_inline

  cat <<EOF

line=\$(sed -n "\${SLURM_ARRAY_TASK_ID}p" "${exec_manifest}")
IFS=\$'\\t' read -r INP MOL <<< "\$line"

inp_base=\$(basename "\$INP"); inp_base="\${inp_base%.*}"
mol_base=\$(basename "\$MOL"); mol_base="\${mol_base%.*}"
stem="\${inp_base}_\${mol_base}"

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
printf "Memory:        %s GB (%s GB per CPU core)\\n" "${total_mem}" "${mem_per_cpu}"
printf "Time limit:    %s\\n"   "${time_display}"
printf "Submitted by:  %s\\n"   "\${USER:-}"
printf "Submitted on:  %s\\n"   "\$(date)"

backup_existing_files "\$output_file"

export DIRAC_SCRATCH="${SCRATCH_BASE}/\${SLURM_ARRAY_JOB_ID}/\${SLURM_ARRAY_TASK_ID}"
mkdir -p "\$DIRAC_SCRATCH"

"${DIRAC_PAM}" \\
  --mpi="${NUM_CPUS}" \\
  --ag="${total_mem}" \\
  --gb="${mem_per_cpu}" \\
  --scratch="\$DIRAC_SCRATCH" \\
  --mol="\$MOL" \\
  --inp="\$INP" \\
  && dirac_exit_code=0 || dirac_exit_code=\$?

if [[ "${OUTPUT_DIR}" != "" && "${OUTPUT_DIR}" != "./" ]]; then
  for file in "\${inp_base}_\${mol_base}"*; do
    if [[ -f "\$file" && "\$file" != *.inp && "\$file" != *.mol ]]; then
      mv "\$file" "${OUTPUT_DIR}" 2>/dev/null || true
    fi
  done
fi

rm -rf "\$DIRAC_SCRATCH" || true
EOF

  emit_job_footer true

  cat <<'EOF'

exit $dirac_exit_code
EOF
}

# Emit single job body for dirac
#
# Returns:
#   0 - Success
mod_generate_single_body() {
  local inp mol
  IFS=$'\t' read -r inp mol <<< "${JOBS[0]}"
  local inp_base mol_base
  inp_base=$(basename "${inp}"); inp_base="${inp_base%.*}"
  mol_base=$(basename "${mol}"); mol_base="${mol_base%.*}"
  local stem="${inp_base}_${mol_base}"
  local time_display="${TIME_LIMIT:-default (partition max)}"
  local mem_per_cpu="${MEMORY_GB}"
  local total_mem="$((MEMORY_GB * NUM_CPUS))"

  printf "\n"
  emit_backup_function_inline

  cat <<EOF

stem="${stem}"
output_file="${OUTPUT_DIR}\${stem}.out"

printf "Job information\\n"
printf "Job name:      %s\\n"   "\${SLURM_JOB_NAME:-${JOB_NAME}}"
printf "Job ID:        %s\\n"   "\${SLURM_JOB_ID:-}"
printf "Output file:   %s\\n"   "\$output_file"
printf "Compute node:  %s\\n"   "\$(hostname)"
printf "Partition:     %s\\n"   "${PARTITION}"
printf "CPU cores:     %s\\n"   "${NUM_CPUS}"
printf "Memory:        %s GB (%s GB per CPU core)\\n" "${total_mem}" "${mem_per_cpu}"
printf "Time limit:    %s\\n"   "${time_display}"
printf "Submitted by:  %s\\n"   "\${USER:-}"
printf "Submitted on:  %s\\n"   "\$(date)"

backup_existing_files "\$output_file"

export DIRAC_SCRATCH="${SCRATCH_BASE}/\${SLURM_JOB_ID}"
mkdir -p "\$DIRAC_SCRATCH"

"${DIRAC_PAM}" \\
  --mpi="${NUM_CPUS}" \\
  --ag="${total_mem}" \\
  --gb="${mem_per_cpu}" \\
  --scratch="\$DIRAC_SCRATCH" \\
  --mol="${mol}" \\
  --inp="${inp}" \\
  && dirac_exit_code=0 || dirac_exit_code=\$?

if [[ "${OUTPUT_DIR}" != "" && "${OUTPUT_DIR}" != "./" ]]; then
  for file in "${inp_base}_${mol_base}"*; do
    if [[ -f "\$file" && "\$file" != *.inp && "\$file" != *.mol ]]; then
      mv "\$file" "${OUTPUT_DIR}" 2>/dev/null || true
    fi
  done
fi

rm -rf "\$DIRAC_SCRATCH" || true
EOF

  emit_job_footer false

  cat <<'EOF'

exit $dirac_exit_code
EOF
}
