#!/usr/bin/env bash
# Module: Dalton
#
# Category C: multi-file (dal/mol/pot/rst), scratch, runtime backup,
#             sticky pot, 32i/64i binary, LoProp

MOD_NAME="dalton"
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

DALTON_EXEC_32I="${DALTON_EXEC_32I:-dalton}"
DALTON_EXEC_64I="${DALTON_EXEC_64I:-dalton}"
DALTON_LOPROP=false

JOBS=()

mod_print_usage() {
  cat <<'EOF'
 Dalton submission (dal/mol/pot/rst)

 Module options:
   -l, --loprop             Request LoProp files (-get "AOONEINT AOPROPER")

 Usage:
   sdalton input.dal geom.mol [pot.pot] [restart.tar.gz] [options]
   sdalton input.dal geom1.mol geom2.mol ... [options]
   sdalton -M FILE [options]

 Examples:
   sdalton exc_b3lyp.dal augccpvdz_h2o.mol -c 1 -m 4
   sdalton opt.dal ccpvdz_h2o.mol ccpvdz_ethanol.mol -T 5
   sdalton -M manifest.txt -c 4 -m 8
EOF
}

# Parse module-specific args
#
# Arguments:
#   $@ - Remaining args
#
# Returns:
#   0 - Success
mod_parse_args() {
  local args=("$@")
  local i=0
  while [[ ${i} -lt ${#args[@]} ]]; do
    case "${args[i]}" in
      -l|--loprop)
        DALTON_LOPROP=true ;;
      *)
        die_usage "Unknown option: ${args[i]}" ;;
    esac
    i=$((i + 1))
  done
}

mod_validate() {
  return 0
}

mod_emit_dependencies() {
  [[ -n "${DALTON_DEPS:-}" ]] && printf "%s\n" "${DALTON_DEPS}"
}

# Check if DAL file contains embedded geometry
#
# Arguments:
#   $1 - dal_file: Path to DAL file
#
# Returns:
#   0 - Contains geometry
#   1 - Keywords only
_dal_contains_geometry() {
  local dal_file="$1"
  grep -q "^BASIS\|^Atomtypes=" "${dal_file}" 2>/dev/null || return 1
  return 0
}

# Append a job entry to JOBS array
#
# Arguments:
#   $1 - dal: DAL file path
#   $2 - mol: MOL file path (may be empty)
#   $3 - pot: POT file path (may be empty)
#   $4 - rst: Restart archive path (may be empty)
#
# Returns:
#   0 - Success
_dalton_append_job() {
  local dal="$1" mol="$2" pot="$3" rst="$4"
  JOBS+=("${dal}"$'\t'"${mol}"$'\t'"${pot}"$'\t'"${rst}")
}

# Retroactively apply pot file to jobs from segment start
#
# Arguments:
#   $1 - pot: POT file path
#   $2 - start_idx: Starting index in JOBS
#
# Returns:
#   0 - Success
_dalton_retroactive_pot() {
  local pot="$1" start_idx="$2"
  local idx dal mol curpot rst
  for ((idx = start_idx; idx < ${#JOBS[@]}; idx++)); do
    IFS=$'\t' read -r dal mol curpot rst <<< "${JOBS[idx]}"
    if [[ -z "${curpot}" ]]; then
      JOBS[idx]="${dal}"$'\t'"${mol}"$'\t'"${pot}"$'\t'"${rst}"
    fi
  done
}

# Build jobs from positional tokens or manifest
#
# Arguments:
#   $@ - Positional arguments
#
# Returns:
#   0 - Success
mod_build_jobs() {
  JOBS=()

  if [[ -n "${MANIFEST_FILE}" ]]; then
    _dalton_read_manifest "${MANIFEST_FILE}"
  else
    _dalton_build_from_tokens "$@"
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

# Parse dal/mol/pot/rst tokens with sticky pot logic
#
# Arguments:
#   $@ - File tokens
#
# Returns:
#   0 - Success
_dalton_build_from_tokens() {
  local -a tokens=("$@")
  local current_dal=""
  local current_dal_has_mol=false
  local sticky_pot=""
  local seg_start=0
  local next_restart=""
  local pending_global_restart=""
  local pending_pot_before_dal=""

  local tok
  for tok in "${tokens[@]}"; do
    case "${tok}" in
      *.dal)
        validate_file_exists "${tok}"
        current_dal="$(to_absolute_path "${tok}")"
        current_dal_has_mol=false
        sticky_pot=""
        if [[ -n "${pending_pot_before_dal}" ]]; then
          sticky_pot="${pending_pot_before_dal}"
          pending_pot_before_dal=""
        fi
        next_restart=""
        seg_start=${#JOBS[@]}
        ;;
      *.pot)
        validate_file_exists "${tok}"
        local abs_pot
        abs_pot="$(to_absolute_path "${tok}")"
        if [[ -z "${current_dal}" ]]; then
          pending_pot_before_dal="${abs_pot}"
        else
          sticky_pot="${abs_pot}"
          _dalton_retroactive_pot "${sticky_pot}" "${seg_start}"
        fi
        ;;
      *.tar.gz)
        validate_file_exists "${tok}"
        local abs_rst
        abs_rst="$(to_absolute_path "${tok}")"
        if [[ -n "${current_dal}" ]]; then
          next_restart="${abs_rst}"
        else
          pending_global_restart="${abs_rst}"
        fi
        ;;
      *.mol)
        validate_file_exists "${tok}"
        [[ -n "${current_dal}" ]] \
          || die_usage ".mol without a preceding .dal: ${tok}"
        local abs_mol
        abs_mol="$(to_absolute_path "${tok}")"

        local rst_use=""
        if [[ -n "${next_restart}" ]]; then
          rst_use="${next_restart}"; next_restart=""
        elif [[ -n "${pending_global_restart}" ]]; then
          rst_use="${pending_global_restart}"
          pending_global_restart=""
        fi

        _dalton_append_job "${current_dal}" "${abs_mol}" \
          "${sticky_pot}" "${rst_use}"
        current_dal_has_mol=true
        ;;
      *)
        die_usage \
          "Unsupported file type (expect .dal/.mol/.pot/.tar.gz): ${tok}"
        ;;
    esac
  done

  if [[ -n "${current_dal}" \
    && "${current_dal_has_mol}" == false ]]; then
    if _dal_contains_geometry "${current_dal}"; then
      local rst_use=""
      if [[ -n "${next_restart}" ]]; then
        rst_use="${next_restart}"
      elif [[ -n "${pending_global_restart}" ]]; then
        rst_use="${pending_global_restart}"
      fi
      _dalton_append_job "${current_dal}" "" \
        "${sticky_pot}" "${rst_use}"
    else
      die_usage \
        ".dal file without .mol: ${current_dal} (embed geometry or provide .mol)"
    fi
  fi
}

# Read tab-separated dal/mol/pot/rst manifest
#
# Arguments:
#   $1 - file: Manifest file path
#
# Returns:
#   0 - Success
_dalton_read_manifest() {
  local file="$1"
  validate_file_exists "${file}"
  local line manifest_tokens=()

  while IFS= read -r line || [[ -n "${line}" ]]; do
    line="${line%$'\r'}"
    [[ "${line}" =~ ^[[:space:]]*$ ]] && continue
    [[ "${line}" =~ ^[[:space:]]*# ]] && continue

    local -a fields
    read -r -a fields <<< "${line}"

    if (( ${#fields[@]} >= 2 )); then
      local dal="${fields[0]}" mol="${fields[1]}" pot="" rst=""
      [[ -n "${fields[2]:-}" ]] && pot="${fields[2]}"
      [[ -n "${fields[3]:-}" ]] && rst="${fields[3]}"

      [[ "${dal}" == *.dal && -f "${dal}" ]] \
        || die_usage "Invalid DAL in manifest: ${dal}"
      [[ "${mol}" == *.mol && -f "${mol}" ]] \
        || die_usage "Invalid MOL in manifest: ${mol}"
      [[ -z "${pot}" ]] || {
        [[ "${pot}" == *.pot && -f "${pot}" ]] \
          || die_usage "Invalid POT in manifest: ${pot}"
      }
      [[ -z "${rst}" ]] || {
        [[ "${rst}" == *.tar.gz && -f "${rst}" ]] \
          || die_usage "Invalid RESTART in manifest: ${rst}"
      }

      _dalton_append_job "${dal}" "${mol}" "${pot}" "${rst}"
    else
      manifest_tokens+=("${line}")
    fi
  done < "${file}"

  if (( ${#manifest_tokens[@]} > 0 )); then
    _dalton_build_from_tokens "${manifest_tokens[@]}"
  fi
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

# Compute dalton stem from job fields
#
# Arguments:
#   $1 - dal: DAL path
#   $2 - mol: MOL path (may be empty)
#   $3 - pot: POT path (may be empty)
#
# Outputs:
#   Stem string on stdout
#
# Returns:
#   0 - Success
_dalton_stem() {
  local dal="$1" mol="$2" pot="$3"
  local dal_base mol_base pot_base stem
  dal_base=$(basename "${dal}"); dal_base="${dal_base%.*}"

  if [[ -n "${mol}" ]]; then
    mol_base=$(basename "${mol}"); mol_base="${mol_base%.*}"
    if [[ -n "${pot}" ]]; then
      pot_base=$(basename "${pot}"); pot_base="${pot_base%.*}"
      stem="${dal_base}_${mol_base}_${pot_base}"
    else
      stem="${dal_base}_${mol_base}"
    fi
  else
    stem="${dal_base}"
  fi
  printf "%s" "${stem}"
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
    local dal mol pot rst
    IFS=$'\t' read -r dal mol pot rst <<< "${JOBS[0]}"
    _dalton_stem "${dal}" "${mol}" "${pot}"
  fi
}

mod_job_name() {
  strip_extension "$1" ".dal"
}

# Backup all outputs for JOBS array
#
# Returns:
#   0 - Success
mod_backup_all() {
  local line dal mol pot rst stem
  for line in "${JOBS[@]}"; do
    IFS=$'\t' read -r dal mol pot rst <<< "${line}"
    stem=$(_dalton_stem "${dal}" "${mol}" "${pot}")
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

# Emit array job body for dalton
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
IFS=\$'\\t' read -r DAL MOL POT RST <<< "\$line"

dal_base=\$(basename "\$DAL"); dal_base="\${dal_base%.*}"
mol_base=\$(basename "\$MOL"); mol_base="\${mol_base%.*}"
if [[ -n "\$POT" ]]; then
  pot_base=\$(basename "\$POT"); pot_base="\${pot_base%.*}"
  stem="\${dal_base}_\${mol_base}_\${pot_base}"
else
  stem="\${dal_base}_\${mol_base}"
fi

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

export DALTON_TMPDIR="${SCRATCH_BASE}/\${SLURM_ARRAY_JOB_ID}/\${SLURM_ARRAY_TASK_ID}"
mkdir -p "\$DALTON_TMPDIR"

DALTON_BIN="${DALTON_EXEC_32I}"
if [[ -z "\$POT" && "${MEMORY_GB}" -gt 16 ]]; then
  DALTON_BIN="${DALTON_EXEC_64I}"
fi

cmd=( "\$DALTON_BIN" -d -np "${NUM_CPUS}" -gb "${MEMORY_GB}" -t "\$DALTON_TMPDIR" -dal "\$DAL" -o "\$output_file" )
[[ -n "\$MOL" ]] && cmd+=( -mol "\$MOL" )
[[ -n "\$POT" ]] && cmd+=( -pot "\$POT" )
[[ -n "\$RST" ]] && cmd+=( -f "\$RST" )
EOF

  if [[ "${DALTON_LOPROP}" == true ]]; then
    printf "cmd+=( -get \"AOONEINT AOPROPER\" )\n"
  fi

  cat <<EOF

"\${cmd[@]}" && dalton_exit_code=0 || dalton_exit_code=\$?

if [[ "${OUTPUT_DIR}" != "" && "${OUTPUT_DIR}" != "./" ]]; then
  if ls "\${stem}.tar.gz" 1>/dev/null 2>&1; then
    mv "\${stem}.tar.gz" "${OUTPUT_DIR}\${stem}.tar.gz"
  fi
fi

rm -rf "\$DALTON_TMPDIR" || true
EOF

  emit_job_footer true

  cat <<'EOF'

exit $dalton_exit_code
EOF
}

# Emit single job body for dalton
#
# Returns:
#   0 - Success
mod_generate_single_body() {
  local dal mol pot rst
  IFS=$'\t' read -r dal mol pot rst <<< "${JOBS[0]}"
  local stem
  stem=$(_dalton_stem "${dal}" "${mol}" "${pot}")
  local time_display="${TIME_LIMIT:-default (partition max)}"
  local mem_per_cpu="$((MEMORY_GB / NUM_CPUS))"

  printf "\n"
  emit_backup_function_inline

  cat <<EOF

DAL="${dal}"
MOL="${mol}"
POT="${pot}"
RST="${rst}"

dal_base=\$(basename "\$DAL"); dal_base="\${dal_base%.*}"
if [[ -n "\$MOL" ]]; then
  mol_base=\$(basename "\$MOL"); mol_base="\${mol_base%.*}"
  if [[ -n "\$POT" ]]; then
    pot_base=\$(basename "\$POT"); pot_base="\${pot_base%.*}"
    stem="\${dal_base}_\${mol_base}_\${pot_base}"
  else
    stem="\${dal_base}_\${mol_base}"
  fi
else
  stem="\${dal_base}"
fi

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

export DALTON_TMPDIR="${SCRATCH_BASE}/\${SLURM_JOB_ID}"
mkdir -p "\$DALTON_TMPDIR"

DALTON_BIN="${DALTON_EXEC_32I}"
if [[ -z "\$POT" && "${MEMORY_GB}" -gt 16 ]]; then
  DALTON_BIN="${DALTON_EXEC_64I}"
fi

cmd=( "\$DALTON_BIN" -d -np "${NUM_CPUS}" -gb "${MEMORY_GB}" -t "\$DALTON_TMPDIR" -dal "\$DAL" -o "\$output_file" )
[[ -n "\$MOL" ]] && cmd+=( -mol "\$MOL" )
[[ -n "\$POT" ]] && cmd+=( -pot "\$POT" )
[[ -n "\$RST" ]] && cmd+=( -f "\$RST" )
EOF

  if [[ "${DALTON_LOPROP}" == true ]]; then
    printf "cmd+=( -get \"AOONEINT AOPROPER\" )\n"
  fi

  cat <<EOF

"\${cmd[@]}" && dalton_exit_code=0 || dalton_exit_code=\$?

if [[ "${OUTPUT_DIR}" != "" && "${OUTPUT_DIR}" != "./" ]]; then
  if ls "\${stem}.tar.gz" 1>/dev/null 2>&1; then
    mv "\${stem}.tar.gz" "${OUTPUT_DIR}\${stem}.tar.gz"
  fi
fi

rm -rf "\$DALTON_TMPDIR" || true
EOF

  emit_job_footer false

  cat <<'EOF'

exit $dalton_exit_code
EOF
}
