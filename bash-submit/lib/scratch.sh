#!/usr/bin/env bash
# Scratch directory setup and cleanup emission for sbatch scripts
#
# Sourced by bin/submit; not executable standalone.

# Emit scratch directory setup lines for sbatch script
#
# Arguments:
#   $1 - array_mode: "true" if array job
#
# Outputs:
#   Bash lines for scratch setup on stdout
#
# Returns:
#   0 - Success
emit_scratch_setup() {
  local array_mode="$1"

  if [[ "${array_mode}" == true ]]; then
    cat <<EOF
scratch_directory="${SCRATCH_BASE}/\$SLURM_JOB_ID/\$SLURM_ARRAY_TASK_ID"
EOF
  else
    cat <<EOF
scratch_directory="${SCRATCH_BASE}/\$SLURM_JOB_ID"
EOF
  fi
  cat <<'EOF'
mkdir -p "$scratch_directory"
EOF
}

# Emit scratch directory cleanup lines for sbatch script
#
# Outputs:
#   Bash lines for scratch cleanup on stdout
#
# Returns:
#   0 - Success
emit_scratch_cleanup() {
  cat <<'EOF'
rm -rf "$scratch_directory"
EOF
}
