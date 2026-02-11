#!/usr/bin/env bash
# Module: SHARC (Surface Hopping including ARbitrary Couplings)
#
# Category A: single-file, scratch, archive, copies INITCONDS/QM dirs

MOD_NAME="sharc"
MOD_INPUT_EXTENSIONS=(".inp")
MOD_OUTPUT_EXTENSIONS=()
MOD_RETRIEVE_EXTENSIONS=()
MOD_DEFAULT_CPUS=1
MOD_DEFAULT_MEMORY_GB=2
MOD_DEFAULT_THROTTLE=5
MOD_DEFAULT_OUTPUT_DIR="output"
MOD_USES_SCRATCH=true
MOD_USES_ARCHIVE=true
MOD_MEMORY_UNIT="gb"

SHARC_HOME="${SHARC_HOME:-}"

mod_print_usage() {
  cat <<'EOF'
 SHARC submission

 Examples:
   ssharc dynamics.inp -c 4 -m 8 -t 2-00:00:00
   ssharc traj_*.inp --throttle 10 -c 2 -m 4
EOF
}

mod_parse_args() {
  if [[ $# -gt 0 ]]; then
    die_usage "Unknown option: $1"
  fi
}

mod_validate() {
  return 0
}

mod_emit_dependencies() {
  [[ -n "${SHARC_DEPS:-}" ]] && printf "%s\n" "${SHARC_DEPS}"
}

# Emit SHARC run command: copies input + aux dirs, runs sharc.x
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
  cat <<EOF
input_dir=\$(dirname "${input}")
cp "${input}" "\$scratch_directory/input"
if [[ -f "\$input_dir/INITCONDS" ]]; then
  cp "\$input_dir/INITCONDS" "\$scratch_directory/"
fi
if [[ -d "\$input_dir/QM" ]]; then
  cp -r "\$input_dir/QM" "\$scratch_directory/"
fi

cd "\$scratch_directory"
\$SHARC/sharc.x input > output.log 2>&1
EOF
}

# Emit retrieval: output.dat, output.log, restart dir, *.out/*.xyz/*.dat
#
# Arguments:
#   $1 - stem: Input stem
#
# Returns:
#   0 - Success
mod_emit_retrieve_outputs() {
  local stem="$1"
  cat <<EOF
if [[ -f "output.dat" ]]; then
  cp output.dat "\${output_directory}${stem}_output.dat"
fi
if [[ -f "output.log" ]]; then
  cp output.log "\${output_directory}${stem}_output.log"
fi
if [[ -d "restart" ]]; then
  cp -r restart "\${output_directory}${stem}_restart"
fi
for file in *.out *.xyz *.dat; do
  if [[ -f "\$file" ]]; then
    cp "\$file" "\${output_directory}"
  fi
done
cd /
EOF
}

mod_job_name() {
  strip_extension "$1" ".inp"
}

mod_backup_targets() {
  local stem="$1"
  local output_dir="$2"
  printf "%s\n" "${output_dir}${stem}_output.dat"
  printf "%s\n" "${output_dir}${stem}_output.log"
  printf "%s\n" "${output_dir}${stem}${LOG_EXTENSION}"
  if [[ "${CREATE_ARCHIVE}" == true ]]; then
    printf "%s\n" "${output_dir}${stem}.tar.gz"
  fi
}
