#!/usr/bin/env bash
# Partition-specific settings (node excludes, etc.)
#
# Sourced by bin/submit; not executable standalone.

# Configure partition-specific settings
#
# Sets NODE_EXCLUDE from exclude file when partition matches.
#
# Returns:
#   0 - Success
setup_partition_specifics() {
  NODE_EXCLUDE=""
  if [[ "${PARTITION}" == "${NODE_EXCLUDE_PARTITION}" \
    && -f "${NODE_EXCLUDE_FILE}" ]]; then
    NODE_EXCLUDE=$(paste -sd, "${NODE_EXCLUDE_FILE}")
  fi
}
