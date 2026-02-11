#!/usr/bin/env bash
# Rotating file backup -- pre-submit and inline sbatch embedding
#
# Sourced by bin/submit; not executable standalone.

# Backup an existing file with numbered rotation
#
# Rotates target -> .0, .0 -> .1, ..., .N deleted.
# Uses flock for concurrent safety.
#
# Arguments:
#   $1 - target_path: File to backup
#
# Returns:
#   0 - Success or file doesn't exist
backup_existing_file() {
  local target_path="$1"
  [[ -z "${target_path}" || ! -f "${target_path}" ]] && return 0

  local dir_path base_name backup_dir backup_base
  dir_path=$(dirname -- "${target_path}")
  base_name=$(basename -- "${target_path}")

  if [[ "${USE_BACKUP_DIR}" == true ]]; then
    backup_dir="${dir_path}/${BACKUP_DIR_NAME}"
    [[ ! -d "${backup_dir}" ]] && mkdir -p "${backup_dir}"
    backup_base="${backup_dir}/${base_name}"
  else
    backup_base="${dir_path}/${base_name}"
  fi

  local width=${#MAX_BACKUPS}
  local from to i

  exec 9>"${backup_base}.lock"
  if ! flock -w 5 9; then
    printf "Warning: could not acquire lock for backup of %s\n" \
      "${target_path}" >&2
    return 0
  fi

  printf -v to "%0${width}d" "${MAX_BACKUPS}"
  [[ -e "${backup_base}.${to}" ]] && rm -f -- "${backup_base}.${to}"

  for ((i = MAX_BACKUPS - 1; i >= 0; i--)); do
    printf -v from "%0${width}d" "${i}"
    printf -v to "%0${width}d" "$((i + 1))"
    if [[ -e "${backup_base}.${from}" ]]; then
      mv -f -- "${backup_base}.${from}" "${backup_base}.${to}"
    fi
  done

  printf -v to "%0${width}d" 0
  mv -f -- "${target_path}" "${backup_base}.${to}"

  flock -u 9
  rm -f "${backup_base}.lock"
}

# Emit backup function for embedding inside sbatch scripts
#
# Modules that need runtime backup (dalton, dirac, cfour, turbomole)
# call this to embed the backup logic into the generated script.
#
# Outputs:
#   Bash function definition on stdout
#
# Returns:
#   0 - Success
emit_backup_function_inline() {
  local width=${#MAX_BACKUPS}
  cat <<EOF
backup_existing_files() {
  local target_path="\$1"
  [[ -z "\$target_path" || ! -f "\$target_path" ]] && return 0

  local dir_path base_name backup_dir backup_base
  dir_path=\$(dirname -- "\$target_path")
  base_name=\$(basename -- "\$target_path")

  if [[ "${USE_BACKUP_DIR}" == true ]]; then
    backup_dir="\${dir_path}/${BACKUP_DIR_NAME}"
    [[ ! -d "\$backup_dir" ]] && mkdir -p "\$backup_dir"
    backup_base="\${backup_dir}/\${base_name}"
  else
    backup_base="\${dir_path}/\${base_name}"
  fi

  local width=${width}
  local from to i

  exec 9>"\${backup_base}.lock" || true
  flock -w 5 9 || true

  printf -v to "%0\${width}d" "${MAX_BACKUPS}"
  [[ -e "\${backup_base}.\${to}" ]] && rm -f -- "\${backup_base}.\${to}"

  for ((i = ${MAX_BACKUPS} - 1; i >= 0; i--)); do
    printf -v from "%0\${width}d" "\$i"
    printf -v to "%0\${width}d" "\$((i + 1))"
    if [[ -e "\${backup_base}.\${from}" ]]; then
      mv -f -- "\${backup_base}.\${from}" "\${backup_base}.\${to}"
    fi
  done

  printf -v to "%0\${width}d" 0
  mv -f -- "\$target_path" "\${backup_base}.\${to}"

  flock -u 9 || true
  rm -f "\${backup_base}.lock" || true
}
EOF
}
