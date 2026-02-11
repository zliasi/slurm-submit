#!/usr/bin/env bash
# Module: exec (generic executable submission)
#
# Category D: no input validation, arbitrary commands

MOD_NAME="exec"
MOD_INPUT_EXTENSIONS=()
MOD_OUTPUT_EXTENSIONS=()
MOD_RETRIEVE_EXTENSIONS=()
MOD_DEFAULT_CPUS=1
MOD_DEFAULT_MEMORY_GB=2
MOD_DEFAULT_THROTTLE=5
MOD_DEFAULT_OUTPUT_DIR="output"
MOD_USES_SCRATCH=false
MOD_USES_ARCHIVE=false
MOD_MEMORY_UNIT="gb"

EXEC_USE_MPI=false
EXEC_COMMAND=()

mod_print_usage() {
  cat <<'EOF'
 Generic executable submission

 Module options:
   --mpi                Use mpirun for parallel execution
   -x, --executable PATH  Executable path (alternative to --)

 Usage:
   sexec [options] -- command [args]
   sexec [options] -x executable [args]

 Examples:
   sexec -c 4 -m 8 -- ./myprogram arg1 arg2
   sexec -c 2 -m 4 --mpi -x ./parallel_program input.dat
EOF
}

# Parse module-specific args for exec
#
# Handles --, -x/--executable, --mpi, and collects remaining as command
#
# Arguments:
#   $@ - Remaining args
#
# Returns:
#   0 - Success
mod_parse_args() {
  local args=("$@")
  local executable_path=""
  local i=0
  while [[ ${i} -lt ${#args[@]} ]]; do
    case "${args[i]}" in
      --mpi)
        EXEC_USE_MPI=true ;;
      -x|--executable)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        executable_path="${args[i+1]}"
        i=$((i + 1)) ;;
      --)
        i=$((i + 1))
        EXEC_COMMAND+=("${args[@]:i}")
        break ;;
      *)
        EXEC_COMMAND+=("${args[i]}") ;;
    esac
    i=$((i + 1))
  done

  if [[ -n "${executable_path}" ]]; then
    EXEC_COMMAND=("${executable_path}" "${EXEC_COMMAND[@]}")
  fi
}

mod_validate() {
  [[ ${#EXEC_COMMAND[@]} -gt 0 ]] \
    || die_usage "No command specified (use -- command or -x executable)"
}

mod_emit_dependencies() {
  [[ -n "${EXEC_DEPS:-}" ]] && printf "%s\n" "${EXEC_DEPS}"
}

# Emit the user command execution
#
# Arguments:
#   $1 - input: (unused for exec)
#   $2 - stem: (unused for exec)
#
# Returns:
#   0 - Success
mod_emit_run_command() {
  local cmd_line="${EXEC_COMMAND[0]}"
  local arg
  for arg in "${EXEC_COMMAND[@]:1}"; do
    cmd_line="${cmd_line} $(printf "'%s'" "${arg}")"
  done

  printf "cd \"%s\"\n" "${OUTPUT_DIR}"
  if [[ "${EXEC_USE_MPI}" == true ]]; then
    printf "mpirun -np \$SLURM_NTASKS %s\n" "${cmd_line}"
  else
    printf "%s\n" "${cmd_line}"
  fi
  printf "EXIT_CODE=\$?\n"
}

mod_emit_retrieve_outputs() {
  return 0
}

mod_job_name() {
  basename "${EXEC_COMMAND[0]}"
}

mod_backup_targets() {
  return 0
}
