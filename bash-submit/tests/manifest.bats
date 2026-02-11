#!/usr/bin/env bats

setup() {
  load test-helpers
  source_libraries
  PROGRAM_INVOCATION="test"

  MOD_NAME="test"
  MOD_INPUT_EXTENSIONS=(".inp")
  MOD_OUTPUT_EXTENSIONS=(".out")
  MOD_MEMORY_UNIT="gb"
  MOD_USES_ARCHIVE=false

  apply_module_defaults
  init_runtime_globals

  setup_tmpdir
  create_test_inputs

  mod_job_name() { strip_extension "$1" ".inp"; }
  mod_backup_targets() { default_mod_backup_targets "$@"; }
}

teardown() {
  teardown_tmpdir
}

@test "resolve_inputs sets single file" {
  POSITIONAL_ARGS=("${INPUTS_DIR}/test.inp")
  MANIFEST_FILE=""
  resolve_inputs
  assert_equal "${#INPUTS[@]}" "1"
  assert_equal "${ARRAY_MODE}" "false"
}

@test "resolve_inputs sets array mode for multiple files" {
  POSITIONAL_ARGS=("${INPUTS_DIR}/test.inp" "${INPUTS_DIR}/test.inp")
  MANIFEST_FILE=""
  resolve_inputs
  assert_equal "${#INPUTS[@]}" "2"
  assert_equal "${ARRAY_MODE}" "true"
}

@test "resolve_inputs reads manifest file" {
  printf "%s\n" "${INPUTS_DIR}/test.inp" >"${TEST_TMPDIR}/manifest.txt"
  MANIFEST_FILE="${TEST_TMPDIR}/manifest.txt"
  POSITIONAL_ARGS=()
  resolve_inputs
  assert_equal "${#INPUTS[@]}" "1"
  assert_equal "${ARRAY_MODE}" "true"
}

@test "resolve_inputs rejects missing files" {
  POSITIONAL_ARGS=("/nonexistent/file.inp")
  MANIFEST_FILE=""
  run resolve_inputs
  assert_failure
}

@test "resolve_inputs rejects wrong extensions" {
  local wrong="${TEST_TMPDIR}/test.txt"
  printf "test\n" >"${wrong}"
  POSITIONAL_ARGS=("${wrong}")
  MANIFEST_FILE=""
  run resolve_inputs
  assert_failure
}

@test "create_manifest writes absolute paths" {
  INPUTS=("${INPUTS_DIR}/test.inp")
  CUSTOM_JOB_NAME=""
  cd "${TEST_TMPDIR}"
  local manifest
  manifest=$(create_manifest "testjob")
  assert [ -f "${manifest}" ]
  local line
  line=$(head -1 "${manifest}")
  [[ "${line}" = /* ]]
}

@test "determine_job_name uses custom name when set" {
  CUSTOM_JOB_NAME="myjob"
  ARRAY_MODE=false
  INPUTS=("test.inp")
  determine_job_name
  assert_equal "${JOB_NAME}" "myjob"
}

@test "determine_job_name computes from single input" {
  CUSTOM_JOB_NAME=""
  ARRAY_MODE=false
  INPUTS=("molecule.inp")
  determine_job_name
  assert_equal "${JOB_NAME}" "molecule"
}

@test "determine_job_name uses array format for multiple inputs" {
  CUSTOM_JOB_NAME=""
  ARRAY_MODE=true
  INPUTS=("a.inp" "b.inp" "c.inp")
  THROTTLE=5
  determine_job_name
  assert_equal "${JOB_NAME}" "test-array-3t5"
}
