#!/usr/bin/env bash
# SBATCH script assembly
#
# Builds complete sbatch scripts from header, module emissions, and footer.
#
# Sourced by bin/submit; not executable standalone.

# Emit sbatch header with #SBATCH directives
#
# Outputs:
#   Header lines on stdout
#
# Returns:
#   0 - Success
emit_sbatch_header() {
  local memory_directive
  if [[ "${MOD_MEMORY_UNIT:-gb}" == "gb_float" ]]; then
    memory_directive=$(
      awk "BEGIN {printf \"%.0f\", ${MEMORY_GB} * 1024}"
    )
    memory_directive="${memory_directive}MB"
  else
    memory_directive="${MEMORY_GB}gb"
  fi

  cat <<EOF
#!/bin/bash
#SBATCH --job-name=${JOB_NAME}
EOF

  if [[ "${ARRAY_MODE}" == true ]]; then
    printf "#SBATCH --output=\"/dev/null\"\n"
    printf "#SBATCH --array=1-%d%%%s\n" "${#INPUTS[@]}" "${THROTTLE}"
  else
    printf "#SBATCH --output=\"%s%%x%s\"\n" "${OUTPUT_DIR}" "${LOG_EXTENSION}"
  fi

  cat <<EOF
#SBATCH --nodes=${NODES}
#SBATCH --ntasks=${NTASKS}
#SBATCH --cpus-per-task=${NUM_CPUS}
#SBATCH --mem=${memory_directive}
#SBATCH --partition=${PARTITION}
EOF

  [[ -n "${TIME_LIMIT}" ]] \
    && printf "#SBATCH --time=%s\n" "${TIME_LIMIT}"
  [[ -n "${NICE_FACTOR}" ]] \
    && printf "#SBATCH --nice=%s\n" "${NICE_FACTOR}"
  [[ -n "${NODE_EXCLUDE}" ]] \
    && printf "#SBATCH --exclude=%s\n" "${NODE_EXCLUDE}"

  printf "#SBATCH --export=NONE\n"
}

# Emit job info printf block for sbatch script
#
# Arguments:
#   $1 - array_mode: "true" if array job
#
# Outputs:
#   Printf lines on stdout
#
# Returns:
#   0 - Success
emit_job_info_block() {
  local array_mode="$1"
  local time_display="${TIME_LIMIT:-default (partition max)}"
  local mem_per_cpu
  if [[ "${MOD_MEMORY_UNIT:-gb}" == "gb_float" ]]; then
    mem_per_cpu="${MEMORY_GB}"
  else
    mem_per_cpu="$((MEMORY_GB / NUM_CPUS))"
  fi

  cat <<EOF
printf "Job information\n"
printf "Job name:      %s\n"   "${JOB_NAME}"
EOF

  if [[ "${array_mode}" == true ]]; then
    cat <<'EOF'
printf "Job ID:        %s_%s\n" "$SLURM_ARRAY_JOB_ID" "$SLURM_ARRAY_TASK_ID"
printf "Input file:    %s\n"   "$(basename "$input_file")"
EOF
  else
    cat <<'EOF'
printf "Job ID:        %s\n"   "$SLURM_JOB_ID"
EOF
    printf "printf \"Input file:    %%s\\\\n\"   \"%s\"\n" \
      "$(basename "${INPUTS[0]}")"
  fi

  cat <<EOF
printf "Compute node:  %s\n"   "\$HOSTNAME"
printf "Partition:     %s\n"   "${PARTITION}"
printf "CPU cores:     %s\n"   "${NUM_CPUS}"
EOF

  if [[ "${MOD_MEMORY_UNIT:-gb}" == "gb_float" ]]; then
    printf "printf \"Memory:        %s GB\\\\n\" \"%s\"\n" \
      "${MEMORY_GB}" "${MEMORY_GB}"
  else
    cat <<EOF
printf "Memory:        %s GB (%s GB per CPU core)\n" \\
  "${MEMORY_GB}" "${mem_per_cpu}"
EOF
  fi

  cat <<EOF
printf "Time limit:    %s\n"   "${time_display}"
printf "Submitted by:  %s\n"   "\$USER"
printf "Submitted on:  %s\n"   "\$(date)"
EOF
}

# Emit archive creation block for sbatch script
#
# Outputs:
#   Archive creation lines on stdout
#
# Returns:
#   0 - Success
emit_archive_block() {
  if [[ "${CREATE_ARCHIVE}" == true ]]; then
    cat <<'EOF'
if tar -cJf "$output_directory$stem.tar.xz" -C "$scratch_directory" .; then
  printf "\nArchive \"%s.tar.xz\" has been created in %s\n" \
    "$stem" "$output_directory"
else
  printf "\nError: Failed to create archive %s.tar.xz in %s\n" \
    "$stem" "$output_directory"
fi
EOF
  else
    cat <<'EOF'
printf "\nArchive creation disabled\n"
EOF
  fi
}

# Emit sacct footer for sbatch script
#
# Arguments:
#   $1 - array_mode: "true" if array job
#
# Outputs:
#   Footer lines on stdout
#
# Returns:
#   0 - Success
emit_job_footer() {
  local array_mode="$1"
  cat <<'EOF'

printf "\nEnd of job\n"
printf "      Job ID   Job name     Memory   Wall time   CPU time\n"
sleep 2
EOF

  if [[ "${array_mode}" == true ]]; then
    cat <<'EOF'
/usr/bin/sacct -n \
  -j "${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}" \
  --format=JobID,JobName,MaxRSS,Elapsed,CPUTime --units=MB
EOF
  else
    cat <<'EOF'
/usr/bin/sacct -n -j "$SLURM_JOB_ID" \
  --format=JobID,JobName,MaxRSS,Elapsed,CPUTime --units=MB
EOF
  fi
}

# Generate complete sbatch script
#
# Assembles all sections into a complete sbatch submission script.
#
# Outputs:
#   Complete sbatch script on stdout
#
# Returns:
#   0 - Success
generate_sbatch_script() {
  emit_sbatch_header
  printf "\nset -euo pipefail\n\n"
  mod_emit_dependencies

  if [[ "${ARRAY_MODE}" == true ]]; then
    if declare -f mod_generate_array_body >/dev/null 2>&1; then
      mod_generate_array_body
    else
      _generate_array_body
    fi
  else
    if declare -f mod_generate_single_body >/dev/null 2>&1; then
      mod_generate_single_body
    else
      _generate_single_body
    fi
  fi
}

# Generate array job body (internal)
#
# Outputs:
#   Array dispatch + per-task body on stdout
#
# Returns:
#   0 - Success
_generate_array_body() {
  local input_ext="${MOD_INPUT_EXTENSIONS[0]:-.inp}"
  local exec_manifest
  exec_manifest=$(to_absolute_path "${EXEC_MANIFEST}")

  printf "\n"
  cat <<EOF
input_file=\$(sed -n "\${SLURM_ARRAY_TASK_ID}p" "${exec_manifest}")
stem=\$(basename "\$input_file" ${input_ext})
EOF

  printf "\nexec 1>\"%s\${stem}%s\" 2>&1\n\n" \
    "${OUTPUT_DIR}" "${LOG_EXTENSION}"

  emit_job_info_block true
  printf "\n"

  if [[ "${MOD_USES_SCRATCH:-false}" == true ]]; then
    printf "output_directory=\"%s\"\n" "${OUTPUT_DIR}"
    emit_scratch_setup true
    printf "\n"
  fi

  mod_emit_run_command "\$input_file" "\$stem"
  printf "\n"
  mod_emit_retrieve_outputs "\$stem"

  if [[ "${MOD_USES_ARCHIVE:-false}" == true ]]; then
    printf "\n"
    emit_archive_block
  fi

  if [[ "${MOD_USES_SCRATCH:-false}" == true ]]; then
    printf "\n"
    emit_scratch_cleanup
  fi

  emit_job_footer true
}

# Generate single job body (internal)
#
# Outputs:
#   Single job body on stdout
#
# Returns:
#   0 - Success
_generate_single_body() {
  local input_ext="${MOD_INPUT_EXTENSIONS[0]:-.inp}"
  local single_input
  single_input=$(to_absolute_path "${INPUTS[0]}")
  local stem
  stem=$(mod_job_name "${INPUTS[0]}")

  printf "\nstem=\"%s\"\n" "${stem}"
  printf "\n"
  emit_job_info_block false
  printf "\n"

  if [[ "${MOD_USES_SCRATCH:-false}" == true ]]; then
    printf "output_directory=\"%s\"\n" "${OUTPUT_DIR}"
    emit_scratch_setup false
    printf "\n"
  fi

  mod_emit_run_command "${single_input}" "${stem}"
  printf "\n"
  mod_emit_retrieve_outputs "${stem}"

  if [[ "${MOD_USES_ARCHIVE:-false}" == true ]]; then
    printf "\n"
    emit_archive_block
  fi

  if [[ "${MOD_USES_SCRATCH:-false}" == true ]]; then
    printf "\n"
    emit_scratch_cleanup
  fi

  emit_job_footer false
}

# Write sbatch script to file instead of submitting
#
# Arguments:
#   $1 - script content
#   $2 - output file path
#
# Returns:
#   0 - Success
#   1 - Write failed (exits)
_write_export() {
  local script="$1"
  local filepath="$2"

  if ! printf "%s" "${script}" > "${filepath}"; then
    printf "Error: Failed to write export file %s\n" "${filepath}" >&2
    exit 1
  fi
  chmod 755 "${filepath}"
  printf "Exported sbatch script to %s\n" "${filepath}"
}

# Submit script to sbatch via pipe
#
# Arguments:
#   $1 - script content
#
# Returns:
#   0 - Success
#   1 - Submission failed (exits)
_submit_to_sbatch() {
  local script="$1"

  if ! printf "%s" "${script}" | sbatch; then
    printf "Error: Job submission failed\n" >&2
    exit 1
  fi
  if [[ "${ARRAY_MODE}" == true ]]; then
    printf "Job array: %d subjobs, throttled to %d concurrent\n" \
      "${#INPUTS[@]}" "${THROTTLE}"
  fi
}

# Generate sbatch script and submit or export
#
# If EXPORT_FILE is set, writes to file instead of submitting.
#
# Returns:
#   0 - Success
#   1 - Failure (exits)
submit_job() {
  local sbatch_script
  sbatch_script=$(generate_sbatch_script)

  if [[ -n "${EXPORT_FILE}" ]]; then
    _write_export "${sbatch_script}" "${EXPORT_FILE}"
  else
    _submit_to_sbatch "${sbatch_script}"
  fi
}
