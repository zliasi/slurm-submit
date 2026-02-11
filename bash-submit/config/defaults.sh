#!/usr/bin/env bash
# Shipped default configuration values
#
# Sourced by bin/submit before module and user config.
# All values can be overridden by module defaults, user config, or CLI args.

DEFAULT_PARTITION="chem"
DEFAULT_CPUS=1
DEFAULT_MEMORY_GB=2
DEFAULT_NTASKS=1
DEFAULT_NODES=1
DEFAULT_THROTTLE=5
DEFAULT_OUTPUT_DIR="output"
DEFAULT_LOG_EXTENSION=".log"

USE_BACKUP_DIR=true
MAX_BACKUPS=5
BACKUP_DIR_NAME="backup"

SCRATCH_BASE="/scratch"
NODE_EXCLUDE_FILE="/groups/kemi/liasi/lib/chem_node_exclude_list.txt"
NODE_EXCLUDE_PARTITION="chem"

CREATE_ARCHIVE=true
