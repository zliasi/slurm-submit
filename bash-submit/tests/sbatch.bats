#!/usr/bin/env bats

setup() {
  load test-helpers
  source_libraries
  PROGRAM_INVOCATION="test"

  MOD_NAME="test"
  MOD_INPUT_EXTENSIONS=(".inp")
  MOD_OUTPUT_EXTENSIONS=(".out")
  MOD_MEMORY_UNIT="gb"
  MOD_USES_SCRATCH=false
  MOD_USES_ARCHIVE=false

  apply_module_defaults
  init_runtime_globals
  PARTITION="chem"
  NUM_CPUS=2
  MEMORY_GB=4
  JOB_NAME="testjob"
  ARRAY_MODE=false
  INPUTS=("test.inp")
  NODE_EXCLUDE=""

  mod_emit_dependencies() { return 0; }
  mod_emit_run_command() { printf "echo running\n"; }
  mod_emit_retrieve_outputs() { return 0; }
  mod_job_name() { strip_extension "$1" ".inp"; }
  mod_backup_targets() { return 0; }
}

@test "emit_sbatch_header includes job name" {
  local output
  output=$(emit_sbatch_header)
  [[ "${output}" == *"--job-name=testjob"* ]]
}

@test "emit_sbatch_header includes cpu and memory" {
  local output
  output=$(emit_sbatch_header)
  [[ "${output}" == *"--cpus-per-task=2"* ]]
  [[ "${output}" == *"--mem=4gb"* ]]
}

@test "emit_sbatch_header includes partition" {
  local output
  output=$(emit_sbatch_header)
  [[ "${output}" == *"--partition=chem"* ]]
}

@test "emit_sbatch_header includes time when set" {
  TIME_LIMIT="1-00:00:00"
  local output
  output=$(emit_sbatch_header)
  [[ "${output}" == *"--time=1-00:00:00"* ]]
}

@test "emit_sbatch_header omits time when empty" {
  TIME_LIMIT=""
  local output
  output=$(emit_sbatch_header)
  [[ "${output}" != *"--time"* ]]
}

@test "emit_sbatch_header uses float memory for gb_float modules" {
  MOD_MEMORY_UNIT="gb_float"
  MEMORY_GB="0.5"
  local output
  output=$(emit_sbatch_header)
  [[ "${output}" == *"--mem=512MB"* ]]
}

@test "emit_sbatch_header includes array directive for array mode" {
  ARRAY_MODE=true
  INPUTS=("a.inp" "b.inp" "c.inp")
  THROTTLE=5
  local output
  output=$(emit_sbatch_header)
  [[ "${output}" == *"--array=1-3%5"* ]]
  [[ "${output}" == *"--output=\"/dev/null\""* ]]
}

@test "generate_sbatch_script produces valid bash" {
  local output
  output=$(generate_sbatch_script)
  run bash -n <(printf "%s" "${output}")
  assert_success
}

@test "generate_sbatch_script includes set -euo pipefail" {
  local output
  output=$(generate_sbatch_script)
  [[ "${output}" == *"set -euo pipefail"* ]]
}
