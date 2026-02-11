#!/usr/bin/env bash
# Module: Python script submission
#
# Category B: passthrough args, multiple env managers, float memory

MOD_NAME="python"
MOD_INPUT_EXTENSIONS=(".py")
MOD_OUTPUT_EXTENSIONS=()
MOD_RETRIEVE_EXTENSIONS=()
MOD_DEFAULT_CPUS=1
MOD_DEFAULT_MEMORY_GB="1.0"
MOD_DEFAULT_THROTTLE=10
MOD_DEFAULT_OUTPUT_DIR="."
MOD_USES_SCRATCH=false
MOD_USES_ARCHIVE=false
MOD_MEMORY_UNIT="gb_float"

PYTHON_EXEC="${PYTHON_EXEC:-python3}"
PYTHON_MODULE=""
CONDA_ENV=""
VENV_PATH=""
CONDA_ACTIVATE=""
UV_ENABLED=false
UV_PROJECT_PATH=""
SCRIPT_ARGS=""

mod_print_usage() {
  cat <<'EOF'
 Python submission

 Environment options:
   --python EXEC          Python executable (default: python3)
   --python-module MOD    Activate conda module
   --conda-env ENV        Activate conda environment
   --venv PATH            Activate virtualenv
   --conda-activate ENV   Source conda activate
   --uv                   Use uv in current directory
   --uv-project PATH      Use uv with project directory

 Script options:
   --args "ARG1 ARG2"     Pass arguments to Python script

 Examples:
   spython analysis.py -c 4 -m 8
   spython script.py --conda-env myenv --args "--verbose"
   spython *.py --uv -T 5
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
      --python)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        PYTHON_EXEC="${args[i+1]}"; i=$((i + 1)) ;;
      --python-module)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        PYTHON_MODULE="${args[i+1]}"; i=$((i + 1)) ;;
      --conda-env)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        CONDA_ENV="${args[i+1]}"; i=$((i + 1)) ;;
      --venv)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        VENV_PATH="${args[i+1]}"; i=$((i + 1)) ;;
      --conda-activate)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        CONDA_ACTIVATE="${args[i+1]}"; i=$((i + 1)) ;;
      --uv)
        UV_ENABLED=true ;;
      --uv-project)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        UV_PROJECT_PATH="${args[i+1]}"; UV_ENABLED=true
        i=$((i + 1)) ;;
      --args)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        SCRIPT_ARGS="${args[i+1]}"; i=$((i + 1)) ;;
      *)
        die_usage "Unknown option: ${args[i]}" ;;
    esac
    i=$((i + 1))
  done
}

mod_validate() {
  if [[ "${UV_ENABLED}" == true && -n "${UV_PROJECT_PATH}" ]]; then
    [[ -d "${UV_PROJECT_PATH}" ]] \
      || die_usage "uv project path not found: ${UV_PROJECT_PATH}"
  fi
}

mod_emit_dependencies() {
  [[ -n "${PYTHON_DEPS:-}" ]] && printf "%s\n" "${PYTHON_DEPS}"
}

# Emit python run command with env setup
#
# Arguments:
#   $1 - input: Script path
#   $2 - stem: Script stem
#
# Returns:
#   0 - Success
mod_emit_run_command() {
  local input="$1"
  local stem="$2"

  printf "export OMP_NUM_THREADS=%s\n" "${NUM_CPUS}"

  if [[ -n "${PYTHON_MODULE}" ]]; then
    printf "module load %s\n" "${PYTHON_MODULE}"
  elif [[ -n "${CONDA_ENV}" ]]; then
    printf "conda activate %s\n" "${CONDA_ENV}"
  elif [[ -n "${VENV_PATH}" ]]; then
    printf "source %s/bin/activate\n" "${VENV_PATH}"
  elif [[ -n "${CONDA_ACTIVATE}" ]]; then
    printf "source \$(conda info --base)/etc/profile.d/conda.sh"
    printf " && conda activate %s\n" "${CONDA_ACTIVATE}"
  fi

  printf "\ncd \"%s\"\n" "${OUTPUT_DIR}"

  if [[ "${UV_ENABLED}" == true ]]; then
    if [[ -n "${UV_PROJECT_PATH}" ]]; then
      printf "uv run --project %s python \"%s\" %s 2>&1\n" \
        "${UV_PROJECT_PATH}" "${input}" "${SCRIPT_ARGS}"
    else
      printf "uv run python \"%s\" %s 2>&1\n" \
        "${input}" "${SCRIPT_ARGS}"
    fi
  else
    printf "%s \"%s\" %s 2>&1\n" \
      "${PYTHON_EXEC}" "${input}" "${SCRIPT_ARGS}"
  fi
}

mod_emit_retrieve_outputs() {
  return 0
}

mod_job_name() {
  strip_extension "$1" ".py"
}

mod_backup_targets() {
  local stem="$1"
  local output_dir="$2"
  printf "%s\n" "${output_dir}${stem}${LOG_EXTENSION}"
}
