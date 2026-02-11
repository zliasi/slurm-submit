#!/usr/bin/env bash
# Common CLI argument parsing
#
# Parses flags shared by all modules. Unknown flags are collected in
# REMAINING_ARGS for module-specific parsing via mod_parse_args().
#
# Sourced by bin/submit; not executable standalone.

# Print common option help lines
#
# Outputs:
#   Common usage text on stdout
#
# Returns:
#   0 - Success
print_common_options() {
  cat <<'EOF'
 Common options:
   -c, --cpus INT               CPU cores per task
   -m, --memory NUM             Total memory in GB
   -p, --partition NAME         Partition
   -t, --time D-HH:MM:SS       Time limit
   -o, --output DIR             Output directory
   -M, --manifest FILE          Manifest file (job array)
   -T, --throttle INT           Max concurrent array subjobs
   -N, --nodes INT              Number of nodes
   -n, --ntasks INT             Number of tasks
   -j, --job, --job-name NAME   Custom job name
   -y, --nice INT               SLURM nice factor
   --variant NAME               Software variant (loads <module>-NAME.sh)
   --export [FILE]              Write sbatch script to FILE instead of submitting
   --no-archive                 Disable archive creation
   -h, --help                   Show this help
EOF
}

# Parse common CLI arguments
#
# Consumes known flags, collecting positional args and unknown flags
# into POSITIONAL_ARGS and REMAINING_ARGS respectively.
#
# Arguments:
#   $@ - Command line arguments
#
# Sets globals:
#   PARTITION, NUM_CPUS, MEMORY_GB, OUTPUT_DIR, TIME_LIMIT,
#   MANIFEST_FILE, THROTTLE, NODES, NTASKS, CUSTOM_JOB_NAME,
#   NICE_FACTOR, CREATE_ARCHIVE, POSITIONAL_ARGS, REMAINING_ARGS
#
# Returns:
#   0 - Success
#   1 - Invalid arguments (exits via die_usage)
parse_common_args() {
  local args=("$@")
  POSITIONAL_ARGS=()
  REMAINING_ARGS=()

  local i=0
  while [[ ${i} -lt ${#args[@]} ]]; do
    case "${args[i]}" in
      -o|--output)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        OUTPUT_DIR="${args[i+1]}"
        i=$((i + 1))
        ;;
      -c|--cpus|--cpu)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        NUM_CPUS="${args[i+1]}"
        i=$((i + 1))
        ;;
      -m|--memory|--mem)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        MEMORY_GB="${args[i+1]}"
        i=$((i + 1))
        ;;
      -p|--partition)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        PARTITION="${args[i+1]}"
        i=$((i + 1))
        ;;
      -t|--time)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        TIME_LIMIT="${args[i+1]}"
        i=$((i + 1))
        ;;
      -M|--manifest)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        MANIFEST_FILE="${args[i+1]}"
        i=$((i + 1))
        ;;
      -T|--throttle)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        THROTTLE="${args[i+1]}"
        i=$((i + 1))
        ;;
      -N|--nodes)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        NODES="${args[i+1]}"
        i=$((i + 1))
        ;;
      -n|--ntasks)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        NTASKS="${args[i+1]}"
        i=$((i + 1))
        ;;
      -j|--job|--job-name)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        CUSTOM_JOB_NAME="${args[i+1]}"
        i=$((i + 1))
        ;;
      -y|--nice)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        NICE_FACTOR="${args[i+1]}"
        i=$((i + 1))
        ;;
      --variant)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        VARIANT="${args[i+1]}"
        i=$((i + 1))
        ;;
      --export)
        if [[ $((i + 1)) -lt ${#args[@]} && ! "${args[i+1]}" =~ ^- ]]; then
          EXPORT_FILE="${args[i+1]}"
          i=$((i + 1))
        else
          EXPORT_FILE=":default:"
        fi
        ;;
      --no-archive)
        CREATE_ARCHIVE=false
        ;;
      -h|--help)
        print_usage
        exit 0
        ;;
      --)
        REMAINING_ARGS+=("${args[@]:i}")
        break
        ;;
      -*)
        REMAINING_ARGS+=("${args[i]}")
        if [[ $((i + 1)) -lt ${#args[@]} && ! "${args[i+1]}" =~ ^- ]]; then
          REMAINING_ARGS+=("${args[i+1]}")
          i=$((i + 1))
        fi
        ;;
      *)
        POSITIONAL_ARGS+=("${args[i]}")
        ;;
    esac
    i=$((i + 1))
  done
}

# Validate common arguments after parsing
#
# Returns:
#   0 - All valid
#   1 - Invalid (exits via die_usage)
validate_common_args() {
  validate_positive_integer "${NUM_CPUS}" "CPU cores"

  if [[ "${MOD_MEMORY_UNIT:-gb}" == "gb_float" ]]; then
    validate_positive_number "${MEMORY_GB}" "memory"
  else
    validate_positive_integer "${MEMORY_GB}" "memory"
  fi

  validate_positive_integer "${NTASKS}" "ntasks"
  validate_positive_integer "${NODES}" "nodes"
  validate_positive_integer "${THROTTLE}" "throttle"
  [[ -n "${NICE_FACTOR}" ]] \
    && validate_positive_integer "${NICE_FACTOR}" "nice factor"
  validate_time_format "${TIME_LIMIT}"
}
