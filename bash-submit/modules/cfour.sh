#!/usr/bin/env bash
# Module: CFOUR
#
# Category A: single-file, scratch, runtime backup, tar.gz archive
# CFOUR has complex scratch setup: copies binaries, GENBAS, ECPDATA

MOD_NAME="cfour"
MOD_INPUT_EXTENSIONS=(".inp")
MOD_OUTPUT_EXTENSIONS=(".out")
MOD_RETRIEVE_EXTENSIONS=()
MOD_DEFAULT_CPUS=1
MOD_DEFAULT_MEMORY_GB=2
MOD_DEFAULT_THROTTLE=10
MOD_DEFAULT_OUTPUT_DIR="output"
MOD_USES_SCRATCH=true
MOD_USES_ARCHIVE=false
MOD_MEMORY_UNIT="gb"

CFOUR_DIR="${CFOUR_DIR:-}"
CFOUR_BASIS_FILE="${CFOUR_BASIS_FILE:-}"
CUSTOM_GENBAS=""

mod_print_usage() {
  cat <<'EOF'
 CFOUR submission

 Module options:
   -g, --genbas FILE   Custom GENBAS file

 Examples:
   scfour scf_ccsd.inp -c 1 -m 4
   scfour *.inp -c 2 -m 8 -g custom_GENBAS
EOF
}

# Parse module-specific args (--genbas)
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
      -g|--genbas)
        _require_arg_value "${args[i]}" "$((i + 1))" "${#args[@]}"
        CUSTOM_GENBAS="${args[i+1]}"
        i=$((i + 1))
        ;;
      *)
        die_usage "Unknown option: ${args[i]}"
        ;;
    esac
    i=$((i + 1))
  done
}

# Validate custom genbas exists if specified
#
# Returns:
#   0 - Success
mod_validate() {
  [[ -z "${CUSTOM_GENBAS}" ]] || validate_file_exists "${CUSTOM_GENBAS}"
}

mod_emit_dependencies() {
  [[ -n "${CFOUR_DEPS:-}" ]] && printf "%s\n" "${CFOUR_DEPS}"
}

# Emit CFOUR run command with full scratch setup
#
# Arguments:
#   $1 - input: Input file path
#   $2 - stem: Input stem
#
# Returns:
#   0 - Success
mod_emit_run_command() {
  local input="$1"
  local stem="$2"
  local genbas_abs=""
  [[ -n "${CUSTOM_GENBAS}" ]] \
    && genbas_abs=$(to_absolute_path "${CUSTOM_GENBAS}")
  local basis_abs=""
  [[ -n "${CFOUR_BASIS_FILE}" && -f "${CFOUR_BASIS_FILE}" ]] \
    && basis_abs=$(to_absolute_path "${CFOUR_BASIS_FILE}")

  cat <<EOF
export CFOUR="${CFOUR_DIR}"
export PATH=".:\$PATH:\$scratch_directory"

cd "\$scratch_directory"

lock_file="/tmp/cfour_copy_\${SLURM_JOB_ID}.lock"
exec 200>"\$lock_file"
flock -w 30 200 || printf "Warning: Could not acquire copy lock\n" >&2

cp "\$CFOUR"/bin/* .
cp "${input}" ZMAT

input_dir=\$(dirname "${input}")

EOF

  if [[ -n "${basis_abs}" ]]; then
    cat <<EOF
printf "Using basis file: ${basis_abs}\n"
cp "${basis_abs}" GENBAS
EOF
  elif [[ -n "${genbas_abs}" ]]; then
    cat <<EOF
printf "Using custom GENBAS: ${genbas_abs}\n"
cp "${genbas_abs}" GENBAS
EOF
  else
    cat <<'EOF'
if [[ -f "${input_dir}/GENBAS" ]]; then
  printf "Using GENBAS from input directory\n"
  cp "${input_dir}/GENBAS" GENBAS
else
  printf "Using default GENBAS from CFOUR\n"
  cp "$CFOUR/basis/GENBAS" .
fi
EOF
  fi

  cat <<EOF

cp "\$CFOUR/basis/ECPDATA" .

flock -u 200
exec 200>&-
rm -f "\$lock_file"

printf "%s\n" "\$(hostname)" > nodefile

./xcfour ./ZMAT ./GENBAS > "\${output_directory}${stem}.out" \\
  && cfour_exit_code=0 || cfour_exit_code=\$?

if [[ "\$output_directory" != "" && "\$output_directory" != "./" ]]; then
  tar -zcf "\${output_directory}${stem}.tar.gz" \\
    out* anh* b* c* d* i* j* p* q* zm* 2>/dev/null || true
fi

cd ..
EOF
}

mod_emit_retrieve_outputs() {
  return 0
}

mod_job_name() {
  local base
  base=$(basename "$1")
  printf "%s\n" "${base%.*}"
}

mod_backup_targets() {
  local stem="$1"
  local output_dir="$2"
  printf "%s\n" "${output_dir}${stem}.out"
  printf "%s\n" "${output_dir}${stem}${LOG_EXTENSION}"
  printf "%s\n" "${output_dir}${stem}.tar.gz"
}
